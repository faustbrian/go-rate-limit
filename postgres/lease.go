package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"github.com/jackc/pgx/v5"
)

type leaseExecutor interface {
	acquire(context.Context, []byte, ratelimit.LeaseRequest, string) (ratelimit.Lease, ratelimit.Decision, error)
	release(context.Context, []byte, ratelimit.Lease, string) error
}

// Acquire transactionally obtains a weighted distributed concurrency lease.
func (store *Store) Acquire(ctx context.Context, request ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
	if err := request.Validate(); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	request.Request.Now = time.UnixMicro(request.Request.Now.UnixMicro()).UTC()
	executor, ok := store.executor.(leaseExecutor)
	if !ok {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrUnsupported
	}
	callCtx, cancel := context.WithTimeout(ctx, store.options.Timeout)
	defer cancel()
	key := sha256.Sum256([]byte(request.Request.Policy.ID() + "\x00" + request.Request.Key.String()))
	leaseDigest := sha256.Sum256([]byte(request.LeaseID))
	lease, decision, err := executor.acquire(callCtx, key[:], request, hex.EncodeToString(leaseDigest[:]))
	if err == nil || errors.Is(err, ratelimit.ErrRejected) ||
		errors.Is(err, ratelimit.ErrLeaseNotOwned) {
		return lease, decision, err
	}
	if errors.Is(err, ratelimit.ErrCorrupt) {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrCorrupt
	}
	if errors.Is(callCtx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrDeadline
	}
	return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrUnavailable
}

// Release transactionally verifies and relinquishes an owned lease.
func (store *Store) Release(ctx context.Context, lease ratelimit.Lease) error {
	if lease.ID == "" || lease.PolicyID == "" || lease.Key.String() == "" || lease.Cost == 0 {
		return ratelimit.ErrInvalidRequest
	}
	executor, ok := store.executor.(leaseExecutor)
	if !ok {
		return ratelimit.ErrUnsupported
	}
	callCtx, cancel := context.WithTimeout(ctx, store.options.Timeout)
	defer cancel()
	key := sha256.Sum256([]byte(lease.PolicyID + "\x00" + lease.Key.String()))
	leaseDigest := sha256.Sum256([]byte(lease.ID))
	err := executor.release(callCtx, key[:], lease, hex.EncodeToString(leaseDigest[:]))
	if err == nil || errors.Is(err, ratelimit.ErrLeaseNotFound) ||
		errors.Is(err, ratelimit.ErrLeaseNotOwned) {
		return err
	}
	if errors.Is(err, ratelimit.ErrCorrupt) {
		return ratelimit.ErrCorrupt
	}
	if errors.Is(callCtx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return ratelimit.ErrDeadline
	}
	return ratelimit.ErrUnavailable
}

func (executor *nativeExecutor) acquire(ctx context.Context, key []byte, request ratelimit.LeaseRequest, digest string) (lease ratelimit.Lease, decision ratelimit.Decision, resultErr error) {
	tx, serverNow, err := executor.beginLocked(ctx, key)
	if err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	defer func() { _ = tx.rollback(context.WithoutCancel(ctx)) }()
	if executor.options.Clock == ServerClock {
		request.Request.Now = serverNow.UTC()
	}
	current, err := loadState(ctx, tx, key, request.Request.Now)
	if err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	next, lease, decision, resultErr := mutateLease(current, request, digest)
	if resultErr != nil && !errors.Is(resultErr, ratelimit.ErrRejected) {
		return ratelimit.Lease{}, ratelimit.Decision{}, resultErr
	}
	encoded := encodeState(next)
	ttl := request.Request.Policy.LeaseDuration() * 2
	if ttl < time.Second {
		ttl = time.Second
	}
	if err := tx.exec(ctx, upsertStateSQL, key, encoded, request.Request.Now.Add(ttl), request.Request.Now); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	if err := tx.commit(ctx); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	return lease, decision, resultErr
}

func (executor *nativeExecutor) release(ctx context.Context, key []byte, lease ratelimit.Lease, digest string) error {
	tx, serverNow, err := executor.beginLocked(ctx, key)
	if err != nil {
		return err
	}
	defer func() { _ = tx.rollback(context.WithoutCancel(ctx)) }()
	current, err := loadStateForRelease(ctx, tx, key)
	if err != nil {
		return err
	}
	if current == nil || current.Algorithm != ratelimit.Concurrency {
		return ratelimit.ErrLeaseNotFound
	}
	existing, ok := current.Leases[digest]
	if !ok {
		return ratelimit.ErrLeaseNotFound
	}
	if existing.Cost != lease.Cost || existing.ExpiresMicros != lease.ExpiresAt.UnixMicro() {
		return ratelimit.ErrLeaseNotOwned
	}
	delete(current.Leases, digest)
	if len(current.Leases) == 0 {
		if err := tx.exec(ctx, deleteStateSQL, key); err != nil {
			return err
		}
	} else {
		encoded := encodeState(current)
		expiresAt := latestLeaseExpiry(current.Leases)
		if err := tx.exec(ctx, upsertStateSQL, key, encoded, expiresAt, serverNow.UTC()); err != nil {
			return err
		}
	}
	return tx.commit(ctx)
}

func (executor *nativeExecutor) beginLocked(ctx context.Context, key []byte) (nativeTransaction, time.Time, error) {
	tx, err := executor.database.begin(ctx)
	if err != nil {
		return nil, time.Time{}, err
	}
	lockTimeout := strconv.FormatInt(executor.options.LockTimeout.Milliseconds(), 10) + "ms"
	var ignored string
	if err := tx.queryRow(ctx, setLockTimeoutSQL, lockTimeout).Scan(&ignored); err != nil {
		_ = tx.rollback(context.WithoutCancel(ctx))
		return nil, time.Time{}, err
	}
	var lock any
	var now time.Time
	if err := tx.queryRow(ctx, lockAndTimeSQL, advisoryKey(key)).Scan(&lock, &now); err != nil {
		_ = tx.rollback(context.WithoutCancel(ctx))
		return nil, time.Time{}, err
	}
	return tx, now, nil
}

func latestLeaseExpiry(leases map[string]persistedLease) time.Time {
	var latest int64
	for _, lease := range leases {
		if lease.ExpiresMicros > latest {
			latest = lease.ExpiresMicros
		}
	}
	return time.UnixMicro(latest)
}

func loadStateForRelease(ctx context.Context, tx nativeTransaction, key []byte) (*persistedState, error) {
	var encoded []byte
	var expiresAt time.Time
	err := tx.queryRow(ctx, selectStateSQL, key).Scan(&encoded, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeState(encoded)
}

var _ ratelimit.LeaseBackend = (*Store)(nil)

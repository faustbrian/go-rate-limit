package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StrictOptions configures strict PostgreSQL operations and owned rollback cleanup.
type StrictOptions struct {
	Options
	// RollbackTimeout bounds synchronous cleanup after transaction ownership.
	RollbackTimeout time.Duration
}

// NewStrict constructs a strict Store without checking its schema.
func NewStrict(pool *pgxpool.Pool, options StrictOptions) (*Store, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: PostgreSQL pool is required", ratelimit.ErrInvalidPolicy)
	}
	if options.RollbackTimeout <= 0 {
		return nil, fmt.Errorf("%w: rollback timeout must be positive", ratelimit.ErrInvalidPolicy)
	}
	store, err := New(pool, options.Options)
	if err != nil {
		return nil, err
	}
	store.rollbackTimeout = options.RollbackTimeout
	return store, nil
}

// OpenStrict constructs a strict Store and checks its schema.
func OpenStrict(ctx context.Context, pool *pgxpool.Pool, options StrictOptions) (*Store, error) {
	return openStrictChecked(ctx, func() (*Store, error) { return NewStrict(pool, options) })
}

func openStrictChecked(ctx context.Context, construct func() (*Store, error)) (*Store, error) {
	if nilInterface(ctx) {
		return nil, fmt.Errorf("%w: context is required", ratelimit.ErrInvalidRequest)
	}
	store, err := construct()
	if err != nil {
		return nil, err
	}
	if err := store.CheckStrict(ctx); err != nil {
		return nil, err
	}
	return store, nil
}

// AdmitStrict evaluates one request with explicit transaction outcomes.
func (store *Store) AdmitStrict(ctx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	if err := store.strictReady(ctx); err != nil {
		return ratelimit.Decision{}, err
	}
	if err := request.Validate(); err != nil {
		return ratelimit.Decision{}, err
	}
	if err := postgresStrictContext(ctx); err != nil {
		return ratelimit.Decision{}, err
	}
	if request.Policy.Algorithm() == ratelimit.Concurrency {
		return ratelimit.Decision{}, ratelimit.ErrUnsupported
	}
	native, ok := store.executor.(*nativeExecutor)
	if !ok {
		return ratelimit.Decision{}, fmt.Errorf("%w: backend is nil or uninitialized", ratelimit.ErrInvalidPolicy)
	}
	request.Now = time.UnixMicro(request.Now.UnixMicro()).UTC()
	callCtx, cancel := context.WithTimeout(ctx, store.options.Timeout)
	defer cancel()
	key := sha256.Sum256([]byte(request.Policy.ID() + "\x00" + request.Key.String()))
	return native.admitStrict(callCtx, key[:], request, store.rollbackTimeout)
}

// AcquireStrict obtains one lease with explicit transaction outcomes.
func (store *Store) AcquireStrict(ctx context.Context, request ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
	if err := store.strictReady(ctx); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	if err := request.Validate(); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	if err := postgresStrictContext(ctx); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	native, ok := store.executor.(*nativeExecutor)
	if !ok {
		return ratelimit.Lease{}, ratelimit.Decision{}, fmt.Errorf("%w: backend is nil or uninitialized", ratelimit.ErrInvalidPolicy)
	}
	request.Request.Now = time.UnixMicro(request.Request.Now.UnixMicro()).UTC()
	callCtx, cancel := context.WithTimeout(ctx, store.options.Timeout)
	defer cancel()
	key := sha256.Sum256([]byte(request.Request.Policy.ID() + "\x00" + request.Request.Key.String()))
	digest := sha256.Sum256([]byte(request.LeaseID))
	return native.acquireStrict(callCtx, key[:], request, hex.EncodeToString(digest[:]), store.rollbackTimeout)
}

// ReleaseStrict releases one lease with explicit transaction outcomes.
func (store *Store) ReleaseStrict(ctx context.Context, lease ratelimit.Lease) error {
	if err := store.strictReady(ctx); err != nil {
		return err
	}
	if lease.ID == "" || lease.PolicyID == "" || lease.Key.String() == "" || lease.Cost == 0 || lease.ExpiresAt.IsZero() {
		return ratelimit.ErrInvalidRequest
	}
	if err := postgresStrictContext(ctx); err != nil {
		return err
	}
	native, ok := store.executor.(*nativeExecutor)
	if !ok {
		return fmt.Errorf("%w: backend is nil or uninitialized", ratelimit.ErrInvalidPolicy)
	}
	callCtx, cancel := context.WithTimeout(ctx, store.options.Timeout)
	defer cancel()
	key := sha256.Sum256([]byte(lease.PolicyID + "\x00" + lease.Key.String()))
	digest := sha256.Sum256([]byte(lease.ID))
	return native.releaseStrict(callCtx, key[:], lease, hex.EncodeToString(digest[:]), store.rollbackTimeout)
}

// CheckStrict verifies the package-owned schema without mutation.
func (store *Store) CheckStrict(ctx context.Context) error {
	if err := store.strictReady(ctx); err != nil {
		return err
	}
	if err := postgresStrictContext(ctx); err != nil {
		return err
	}
	native, ok := store.executor.(*nativeExecutor)
	if !ok {
		return fmt.Errorf("%w: backend is nil or uninitialized", ratelimit.ErrInvalidPolicy)
	}
	var table *string
	if err := native.database.queryRow(ctx, "SELECT to_regclass('rate_limit_states')::text").Scan(&table); err != nil {
		return postgresPredispatchCategory(ctx, err)
	}
	if table == nil {
		return ratelimit.ErrUnavailable
	}
	return nil
}

// CleanupStrict deletes a bounded expired-state batch with explicit outcome semantics.
func (store *Store) CleanupStrict(ctx context.Context, batch int) (int64, error) {
	if err := store.strictReady(ctx); err != nil {
		return 0, err
	}
	if batch < 1 || batch > MaxCleanupBatch {
		return 0, fmt.Errorf("%w: cleanup batch must be positive", ratelimit.ErrInvalidRequest)
	}
	if err := postgresStrictContext(ctx); err != nil {
		return 0, err
	}
	native, ok := store.executor.(*nativeExecutor)
	if !ok {
		return 0, fmt.Errorf("%w: backend is nil or uninitialized", ratelimit.ErrInvalidPolicy)
	}
	var count int64
	if err := native.database.queryRow(ctx, cleanupSQL, batch).Scan(&count); err != nil {
		return 0, ratelimit.UnknownOutcome(ctx.Err())
	}
	return count, nil
}

func (store *Store) strictReady(ctx context.Context) error {
	if store == nil || store.executor == nil || store.rollbackTimeout <= 0 {
		return fmt.Errorf("%w: backend is nil or uninitialized", ratelimit.ErrInvalidPolicy)
	}
	if nilInterface(ctx) {
		return fmt.Errorf("%w: context is required", ratelimit.ErrInvalidRequest)
	}
	return nil
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	current := reflect.ValueOf(value)
	switch current.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return current.IsNil()
	default:
		return false
	}
}

func postgresStrictContext(ctx context.Context) error {
	if ctx.Err() == context.Canceled {
		return ratelimit.CanceledOutcome()
	}
	if ctx.Err() == context.DeadlineExceeded {
		return ratelimit.DeadlineOutcome()
	}
	return nil
}

func postgresPredispatchCategory(ctx context.Context, err error) error {
	if ctx.Err() == context.Canceled {
		return ratelimit.CanceledOutcome()
	}
	if ctx.Err() == context.DeadlineExceeded {
		return ratelimit.DeadlineOutcome()
	}
	if err == context.Canceled { //nolint:errorlint // Strict dispatch accepts only the direct cancellation sentinel.
		return ratelimit.CanceledOutcome()
	}
	if err == context.DeadlineExceeded { //nolint:errorlint // Strict dispatch accepts only the direct deadline sentinel.
		return ratelimit.DeadlineOutcome()
	}
	if backendErr, ok := err.(*ratelimit.BackendError); ok { //nolint:errorlint // Strict dispatch accepts only a direct declared BackendError.
		if backendErr == nil {
			return ratelimit.ErrUnavailable
		}
		if backendErr.Category() == nil {
			return ratelimit.ErrUnavailable
		}
		return backendErr
	}
	if err == ratelimit.ErrRejected { //nolint:errorlint // Strict dispatch accepts only the direct rejection sentinel.
		return err
	}
	if err == ratelimit.ErrOverflow || err == ratelimit.ErrCorrupt || //nolint:errorlint // Strict dispatch accepts only direct safe sentinels.
		err == ratelimit.ErrUnsupported || err == ratelimit.ErrLeaseNotFound || err == ratelimit.ErrLeaseNotOwned { //nolint:errorlint // Strict dispatch accepts only direct safe sentinels.
		return err
	}
	return ratelimit.ErrUnavailable
}

func (executor *nativeExecutor) beginLockedStrict(ctx context.Context, key []byte, rollbackTimeout time.Duration) (nativeTransaction, time.Time, time.Time, error) {
	tx, err := executor.database.begin(ctx)
	if err != nil {
		return nil, time.Time{}, time.Time{}, ratelimit.UnknownOutcome(ctx.Err())
	}
	rollbackDeadline := time.Now().Add(rollbackTimeout)
	rollbackOnPanic := true
	defer func() {
		if !rollbackOnPanic {
			return
		}
		recovered := recover()
		_ = rollbackStrict(ctx, tx, rollbackDeadline)
		if recovered != nil {
			panic(recovered)
		}
	}()
	lockMilliseconds := int64(executor.options.LockTimeout / time.Millisecond)
	if executor.options.LockTimeout%time.Millisecond != 0 {
		lockMilliseconds++
	}
	lockTimeout := strconv.FormatInt(lockMilliseconds, 10) + "ms"
	var ignored string
	if err := tx.queryRow(ctx, setLockTimeoutSQL, lockTimeout).Scan(&ignored); err != nil {
		rollbackOnPanic = false
		return tx, time.Time{}, rollbackDeadline, postgresPrecommitCategory(ctx, err)
	}
	var lock any
	var now time.Time
	if err := tx.queryRow(ctx, lockAndTimeSQL, advisoryKey(key)).Scan(&lock, &now); err != nil {
		rollbackOnPanic = false
		return tx, time.Time{}, rollbackDeadline, postgresPrecommitCategory(ctx, err)
	}
	rollbackOnPanic = false
	return tx, now, rollbackDeadline, nil
}

func rollbackStrict(ctx context.Context, tx nativeTransaction, deadline time.Time) error {
	rollbackCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer cancel()
	return tx.rollback(rollbackCtx)
}

func postgresPrecommitCategory(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*persistedStateError); ok { //nolint:errorlint // Only this direct package-owned marker identifies decoder corruption.
		return ratelimit.ErrCorrupt
	}
	if err == ratelimit.ErrRejected || err == ratelimit.ErrOverflow || //nolint:errorlint // Known direct outcomes retain precedence over caller cancellation.
		err == ratelimit.ErrCorrupt || err == ratelimit.ErrLeaseNotFound || err == ratelimit.ErrLeaseNotOwned { //nolint:errorlint // Known direct outcomes retain precedence over caller cancellation.
		return err
	}
	if ctx.Err() == context.Canceled {
		return ratelimit.CanceledOutcome()
	}
	if ctx.Err() == context.DeadlineExceeded {
		return ratelimit.DeadlineOutcome()
	}
	return ratelimit.ErrUnavailable
}

func (executor *nativeExecutor) admitStrict(ctx context.Context, key []byte, request ratelimit.Request, rollbackTimeout time.Duration) (decision ratelimit.Decision, resultErr error) {
	tx, serverNow, rollbackDeadline, err := executor.beginLockedStrict(ctx, key, rollbackTimeout)
	if tx == nil {
		return ratelimit.Decision{}, err
	}
	committed := false
	defer func() {
		if !committed && rollbackStrict(ctx, tx, rollbackDeadline) != nil {
			decision = ratelimit.Decision{}
			resultErr = ratelimit.UnknownOutcome(ctx.Err())
		}
	}()
	if err != nil {
		return ratelimit.Decision{}, err
	}
	if executor.options.Clock == ServerClock {
		request.Now = serverNow.UTC()
		if err := validateServerClockRange(request.Now, request.Policy.Period()); err != nil {
			return ratelimit.Decision{}, err
		}
	}
	current, err := loadStateStrict(ctx, tx, key, request.Now)
	if err != nil {
		return ratelimit.Decision{}, postgresPrecommitCategory(ctx, err)
	}
	next, decision, resultErr := mutateState(current, request)
	decision, resultErr = strictPostgresDecisionResult(ctx, next, decision, resultErr)
	if resultErr != nil && !errors.Is(resultErr, ratelimit.ErrRejected) {
		return ratelimit.Decision{}, resultErr
	}
	effective := time.UnixMicro(next.ObservedMicros).UTC()
	encoded := encodeState(next)
	ttl := postgresStateTTL(request.Policy.Period())
	if err := tx.exec(ctx, upsertStateSQL, key, encoded, effective.Add(ttl), effective); err != nil {
		return ratelimit.Decision{}, postgresPrecommitCategory(ctx, err)
	}
	if err := tx.commit(ctx); err != nil {
		return ratelimit.Decision{}, ratelimit.UnknownOutcome(ctx.Err())
	}
	committed = true
	return decision, postgresPrecommitCategory(ctx, resultErr)
}

func (executor *nativeExecutor) acquireStrict(ctx context.Context, key []byte, request ratelimit.LeaseRequest, digest string, rollbackTimeout time.Duration) (lease ratelimit.Lease, decision ratelimit.Decision, resultErr error) {
	tx, serverNow, rollbackDeadline, err := executor.beginLockedStrict(ctx, key, rollbackTimeout)
	if tx == nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	committed := false
	defer func() {
		if !committed && rollbackStrict(ctx, tx, rollbackDeadline) != nil {
			lease, decision = ratelimit.Lease{}, ratelimit.Decision{}
			resultErr = ratelimit.UnknownOutcome(ctx.Err())
		}
	}()
	if err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	if executor.options.Clock == ServerClock {
		request.Request.Now = serverNow.UTC()
		if err := validateServerClockRange(request.Request.Now, request.Request.Policy.LeaseDuration()); err != nil {
			return ratelimit.Lease{}, ratelimit.Decision{}, err
		}
	}
	current, err := loadStateStrict(ctx, tx, key, request.Request.Now)
	if err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, postgresPrecommitCategory(ctx, err)
	}
	next, lease, decision, resultErr := mutateLease(current, request, digest)
	lease, decision, resultErr = strictPostgresLeaseResult(ctx, next, lease, decision, resultErr)
	if resultErr != nil && !errors.Is(resultErr, ratelimit.ErrRejected) {
		return ratelimit.Lease{}, ratelimit.Decision{}, resultErr
	}
	effective := time.UnixMicro(next.ObservedMicros).UTC()
	encoded := encodeState(next)
	ttl := postgresStateTTL(request.Request.Policy.LeaseDuration())
	if err := tx.exec(ctx, upsertStateSQL, key, encoded, effective.Add(ttl), effective); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, postgresPrecommitCategory(ctx, err)
	}
	if err := tx.commit(ctx); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.UnknownOutcome(ctx.Err())
	}
	committed = true
	return lease, decision, postgresPrecommitCategory(ctx, resultErr)
}

func validateServerClockRange(now time.Time, duration time.Duration) error {
	micros := now.UnixMicro()
	durationMicros := duration.Microseconds()
	if micros < -maxExactMicros || durationMicros <= 0 || micros > maxExactMicros-durationMicros {
		return ratelimit.ErrOverflow
	}
	return nil
}

func (executor *nativeExecutor) releaseStrict(ctx context.Context, key []byte, lease ratelimit.Lease, digest string, rollbackTimeout time.Duration) (resultErr error) {
	tx, serverNow, rollbackDeadline, err := executor.beginLockedStrict(ctx, key, rollbackTimeout)
	if tx == nil {
		return err
	}
	committed := false
	defer func() {
		if !committed && rollbackStrict(ctx, tx, rollbackDeadline) != nil {
			resultErr = ratelimit.UnknownOutcome(ctx.Err())
		}
	}()
	if err != nil {
		return err
	}
	current, err := loadStateForReleaseStrict(ctx, tx, key)
	if err != nil {
		return postgresPrecommitCategory(ctx, err)
	}
	if current == nil {
		return ratelimit.ErrLeaseNotFound
	}
	if err := validateConcurrencyState(current, lease.PolicyID); err != nil {
		return err
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
		err = tx.exec(ctx, deleteStateSQL, key)
	} else {
		err = tx.exec(ctx, upsertStateSQL, key, encodeState(current), latestLeaseExpiry(current.Leases), serverNow.UTC())
	}
	if err != nil {
		return postgresPrecommitCategory(ctx, err)
	}
	if err := tx.commit(ctx); err != nil {
		return ratelimit.UnknownOutcome(ctx.Err())
	}
	committed = true
	return nil
}

func postgresStateTTL(duration time.Duration) time.Duration {
	if duration > time.Duration(math.MaxInt64)/2 {
		return time.Duration(math.MaxInt64)
	}
	return max(duration*2, time.Second)
}

func validateStrictDecisionTime(decision ratelimit.Decision, effective time.Time) error {
	if !decision.Reset.IsZero() && decision.Reset.Before(effective) {
		return ratelimit.ErrCorrupt
	}
	return nil
}

func strictPostgresDecisionResult(ctx context.Context, next *persistedState, decision ratelimit.Decision, err error) (ratelimit.Decision, error) {
	if err != nil && !errors.Is(err, ratelimit.ErrRejected) {
		return ratelimit.Decision{}, postgresPrecommitCategory(ctx, err)
	}
	effective := time.UnixMicro(next.ObservedMicros).UTC()
	if timeErr := validateStrictDecisionTime(decision, effective); timeErr != nil {
		return ratelimit.Decision{}, timeErr
	}
	return decision, err
}

func strictPostgresLeaseResult(ctx context.Context, next *persistedState, lease ratelimit.Lease, decision ratelimit.Decision, err error) (ratelimit.Lease, ratelimit.Decision, error) {
	if err != nil && !errors.Is(err, ratelimit.ErrRejected) {
		return ratelimit.Lease{}, ratelimit.Decision{}, postgresPrecommitCategory(ctx, err)
	}
	effective := time.UnixMicro(next.ObservedMicros).UTC()
	if timeErr := validateStrictLeaseTime(lease, decision, effective); timeErr != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, timeErr
	}
	return lease, decision, err
}

func validateStrictLeaseTime(lease ratelimit.Lease, decision ratelimit.Decision, effective time.Time) error {
	if err := validateStrictDecisionTime(decision, effective); err != nil {
		return err
	}
	if !lease.ExpiresAt.IsZero() && (!lease.ExpiresAt.After(effective) || !lease.ExpiresAt.Equal(decision.Reset)) {
		return ratelimit.ErrCorrupt
	}
	return nil
}

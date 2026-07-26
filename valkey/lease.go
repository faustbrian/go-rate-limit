package valkey

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

type leaseExecutor interface {
	acquire(context.Context, []string, []string) ([]string, error)
	release(context.Context, []string, []string) ([]string, error)
}

// Acquire atomically obtains a weighted distributed concurrency lease.
func (store *Store) Acquire(ctx context.Context, request ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
	if err := request.Validate(); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	executor, ok := store.executor.(leaseExecutor)
	if !ok {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrUnsupported
	}
	callCtx, cancel := context.WithTimeout(ctx, store.options.Timeout)
	defer cancel()
	digest := sha256.Sum256([]byte(request.LeaseID))
	now := request.Request.Now.UnixMicro()
	serverClock := "0"
	if store.options.Clock == ServerClock {
		serverClock = "1"
	}
	ttl := request.Request.Policy.LeaseDuration() * 2
	if ttl < time.Second {
		ttl = time.Second
	}
	args := []string{
		"1", request.Request.Policy.ID(), request.Request.Policy.Revision(),
		strconv.FormatUint(request.Request.Policy.Limit(), 10),
		strconv.FormatUint(request.Request.Cost, 10),
		strconv.FormatInt(now, 10),
		strconv.FormatInt(request.Request.Policy.LeaseDuration().Microseconds(), 10),
		strconv.FormatInt(ttl.Milliseconds(), 10),
		serverClock,
		hex.EncodeToString(digest[:]),
	}
	reply, err := executor.acquire(callCtx, []string{store.key(request.Request)}, args)
	if err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, store.leaseTransportError(callCtx, err)
	}
	return decodeLeaseReply(reply, request)
}

// Release atomically verifies and relinquishes an owned lease.
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
	digest := sha256.Sum256([]byte(lease.ID))
	requestKey := sha256.Sum256([]byte(lease.PolicyID + "\x00" + lease.Key.String()))
	key := store.options.Prefix + ":{" + hex.EncodeToString(requestKey[:]) + "}"
	reply, err := executor.release(callCtx, []string{key}, []string{
		"1", lease.PolicyID, hex.EncodeToString(digest[:]),
		strconv.FormatUint(lease.Cost, 10),
		strconv.FormatInt(lease.ExpiresAt.UnixMicro(), 10),
	})
	if err != nil {
		return store.leaseTransportError(callCtx, err)
	}
	if len(reply) != 1 {
		return ratelimit.ErrCorrupt
	}
	switch reply[0] {
	case "ok":
		return nil
	case "not_found":
		return ratelimit.ErrLeaseNotFound
	case "not_owned":
		return ratelimit.ErrLeaseNotOwned
	default:
		return ratelimit.ErrCorrupt
	}
}

func (store *Store) leaseTransportError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return ratelimit.ErrDeadline
	}
	return ratelimit.ErrUnavailable
}

func decodeLeaseReply(reply []string, request ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
	if len(reply) != 7 {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrCorrupt
	}
	if reply[5] == "not_owned" {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrLeaseNotOwned
	}
	decision, err := decodeDecision(reply[:6])
	expiresMicros, parseErr := strconv.ParseInt(reply[6], 10, 64)
	if parseErr != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrCorrupt
	}
	lease := ratelimit.Lease{
		ID: request.LeaseID, Key: request.Request.Key,
		PolicyID:       request.Request.Policy.ID(),
		PolicyRevision: request.Request.Policy.Revision(),
		Cost:           request.Request.Cost, ExpiresAt: time.UnixMicro(expiresMicros),
		Backend: "valkey",
	}
	return lease, decision, err
}

var _ ratelimit.LeaseBackend = (*Store)(nil)

package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit/v3"
)

// ClockPolicy selects the authoritative time source for transactions.
type ClockPolicy uint8

const (
	// ClientClock uses Request.Now and clamps rollback per key.
	ClientClock ClockPolicy = iota
	// ServerClock uses PostgreSQL clock_timestamp in the locked transaction.
	ServerClock
)

// Options configures transaction deadlines, lock waits, and clock authority.
type Options struct {
	// Timeout bounds each admission and lease operation; checks and cleanup use the caller context.
	Timeout time.Duration
	// LockTimeout bounds PostgreSQL lock acquisition; nonpositive uses Timeout.
	// Strict setup rounds positive sub-millisecond values up to one millisecond.
	LockTimeout time.Duration
	// Clock selects client or PostgreSQL server time.
	Clock ClockPolicy
}

type executor interface {
	admit(context.Context, []byte, ratelimit.Request) (ratelimit.Decision, error)
}

// Store performs locked PostgreSQL transactions through a borrowed pool.
// Construct it before use; no pool close, background cleanup or migration
// application is owned. Strict methods require a strict constructor; released
// methods retain legacy error normalization and unbounded detached rollback.
type Store struct {
	executor        executor
	options         Options
	rollbackTimeout time.Duration
}

func newStore(executor executor, options Options) (*Store, error) {
	if executor == nil {
		return nil, fmt.Errorf("%w: executor, timeout, and clock are required", ratelimit.ErrInvalidPolicy)
	}
	if options.Timeout < 1 {
		return nil, fmt.Errorf("%w: executor, timeout, and clock are required", ratelimit.ErrInvalidPolicy)
	}
	if options.Clock > ServerClock {
		return nil, fmt.Errorf("%w: executor, timeout, and clock are required", ratelimit.ErrInvalidPolicy)
	}
	if options.LockTimeout < 1 {
		options.LockTimeout = options.Timeout
	}
	return &Store{executor: executor, options: options}, nil
}

// Name returns the stable backend identifier.
func (store *Store) Name() string { return "postgres" }

// Admit evaluates one non-concurrency request in a locked transaction.
func (store *Store) Admit(ctx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	if err := request.Validate(); err != nil {
		return ratelimit.Decision{}, err
	}
	if request.Policy.Algorithm() == ratelimit.Concurrency {
		return ratelimit.Decision{}, ratelimit.ErrUnsupported
	}
	request.Now = time.UnixMicro(request.Now.UnixMicro()).UTC()
	callCtx, cancel := context.WithTimeout(ctx, store.options.Timeout)
	defer cancel()
	key := sha256.Sum256([]byte(request.Policy.ID() + "\x00" + request.Key.String()))
	decision, err := store.executor.admit(callCtx, key[:], request)
	if err == nil || errors.Is(err, ratelimit.ErrRejected) {
		return decision, err
	}
	if errors.Is(err, ratelimit.ErrCorrupt) {
		return ratelimit.Decision{}, ratelimit.ErrCorrupt
	}
	if errors.Is(err, ratelimit.ErrOverflow) {
		return ratelimit.Decision{}, ratelimit.ErrOverflow
	}
	if errors.Is(callCtx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return ratelimit.Decision{}, ratelimit.ErrDeadline
	}
	return ratelimit.Decision{}, ratelimit.ErrUnavailable
}

var _ ratelimit.Backend = (*Store)(nil)

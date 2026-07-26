package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
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
	// Timeout bounds each backend operation.
	Timeout time.Duration
	// LockTimeout bounds PostgreSQL lock acquisition; zero uses Timeout.
	LockTimeout time.Duration
	// Clock selects client or PostgreSQL server time.
	Clock ClockPolicy
}

type executor interface {
	admit(context.Context, []byte, ratelimit.Request) (ratelimit.Decision, error)
}

// Store is an atomic pgx-backed admission backend.
type Store struct {
	executor executor
	options  Options
}

func newStore(executor executor, options Options) (*Store, error) {
	if executor == nil || options.Timeout <= 0 ||
		(options.Clock != ClientClock && options.Clock != ServerClock) {
		return nil, fmt.Errorf("%w: executor, timeout, and clock are required", ratelimit.ErrInvalidPolicy)
	}
	if options.LockTimeout <= 0 {
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

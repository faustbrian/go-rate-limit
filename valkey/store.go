package valkey

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

// MaxPrefixBytes bounds the operator-controlled portion of persisted keys.
const MaxPrefixBytes = 64

// ClockPolicy selects the authoritative time source for state transitions.
type ClockPolicy uint8

const (
	// ClientClock uses Request.Now and clamps rollback per key.
	ClientClock ClockPolicy = iota
	// ServerClock uses Valkey's TIME result inside the atomic script.
	ServerClock
)

// Options configures Valkey key names, deadlines, and clock authority.
type Options struct {
	// Prefix namespaces keys and must contain no spaces or hash-tag characters.
	Prefix string
	// Timeout bounds each server-side operation.
	Timeout time.Duration
	// Clock selects client or server time.
	Clock ClockPolicy
}

type executor interface {
	exec(context.Context, []string, []string) ([]string, error)
}

// Store is an atomic native-Valkey admission backend.
type Store struct {
	executor executor
	options  Options
}

func newStore(executor executor, options Options) (*Store, error) {
	if executor == nil || options.Prefix == "" || len(options.Prefix) > MaxPrefixBytes ||
		options.Timeout <= 0 ||
		strings.ContainsAny(options.Prefix, "{}\x00\r\n ") ||
		(options.Clock != ClientClock && options.Clock != ServerClock) {
		return nil, fmt.Errorf("%w: safe prefix, timeout, clock, and executor are required", ratelimit.ErrInvalidPolicy)
	}
	return &Store{executor: executor, options: options}, nil
}

// Name returns the stable backend identifier.
func (store *Store) Name() string { return "valkey" }

// Admit evaluates one non-concurrency request in an atomic Lua script.
func (store *Store) Admit(ctx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	if err := request.Validate(); err != nil {
		return ratelimit.Decision{}, err
	}
	if request.Policy.Algorithm() == ratelimit.Concurrency {
		return ratelimit.Decision{}, ratelimit.ErrUnsupported
	}
	callCtx, cancel := context.WithTimeout(ctx, store.options.Timeout)
	defer cancel()
	reply, err := store.executor.exec(callCtx, []string{store.key(request)}, store.args(request))
	if err != nil {
		if errors.Is(callCtx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
			return ratelimit.Decision{}, ratelimit.ErrDeadline
		}
		return ratelimit.Decision{}, ratelimit.ErrUnavailable
	}
	return decodeDecision(reply)
}

func (store *Store) key(request ratelimit.Request) string {
	digest := sha256.Sum256([]byte(request.Policy.ID() + "\x00" + request.Key.String()))
	return store.options.Prefix + ":{" + hex.EncodeToString(digest[:]) + "}"
}

func (store *Store) args(request ratelimit.Request) []string {
	now := request.Now.UnixMicro()
	serverClock := "0"
	if store.options.Clock == ServerClock {
		serverClock = "1"
	}
	ttl := request.Policy.Period() * 2
	if ttl < time.Second {
		ttl = time.Second
	}
	return []string{
		"1",
		string(request.Policy.Algorithm()),
		request.Policy.ID(),
		request.Policy.Revision(),
		strconv.FormatUint(request.Policy.Capacity(), 10),
		strconv.FormatUint(request.Policy.Burst(), 10),
		strconv.FormatInt(request.Policy.Period().Microseconds(), 10),
		strconv.FormatUint(request.Cost, 10),
		strconv.FormatInt(now, 10),
		serverClock,
		strconv.FormatInt(ttl.Milliseconds(), 10),
	}
}

func decodeDecision(reply []string) (ratelimit.Decision, error) {
	if len(reply) != 6 {
		return ratelimit.Decision{}, fmt.Errorf("%w: decision field count", ratelimit.ErrCorrupt)
	}
	if reply[0] == "-1" {
		if reply[5] == "overflow" {
			return ratelimit.Decision{}, ratelimit.ErrOverflow
		}
		return ratelimit.Decision{}, ratelimit.ErrCorrupt
	}
	allowed, err := strconv.ParseBool(reply[0])
	if err != nil {
		return ratelimit.Decision{}, fmt.Errorf("%w: allowed", ratelimit.ErrCorrupt)
	}
	remaining, err := strconv.ParseUint(reply[1], 10, 64)
	if err != nil {
		return ratelimit.Decision{}, fmt.Errorf("%w: remaining", ratelimit.ErrCorrupt)
	}
	limit, err := strconv.ParseUint(reply[2], 10, 64)
	if err != nil {
		return ratelimit.Decision{}, fmt.Errorf("%w: limit", ratelimit.ErrCorrupt)
	}
	resetMicros, err := strconv.ParseInt(reply[3], 10, 64)
	if err != nil {
		return ratelimit.Decision{}, fmt.Errorf("%w: reset", ratelimit.ErrCorrupt)
	}
	retryMicros, err := strconv.ParseInt(reply[4], 10, 64)
	if err != nil || retryMicros < 0 {
		return ratelimit.Decision{}, fmt.Errorf("%w: retry-after", ratelimit.ErrCorrupt)
	}
	reason := ratelimit.Reason(reply[5])
	if reason != ratelimit.ReasonAllowed && reason != ratelimit.ReasonLimited {
		return ratelimit.Decision{}, fmt.Errorf("%w: reason", ratelimit.ErrCorrupt)
	}
	decision := ratelimit.Decision{
		Allowed: allowed, Remaining: remaining, Limit: limit,
		Reset: time.UnixMicro(resetMicros), RetryAfter: time.Duration(retryMicros) * time.Microsecond,
		Reason: reason,
	}
	if !allowed {
		return decision, ratelimit.ErrRejected
	}
	return decision, nil
}

var _ ratelimit.Backend = (*Store)(nil)

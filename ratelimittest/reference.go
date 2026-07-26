package ratelimittest

import (
	"context"
	"math/big"
	"sync"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

// Reference is a mutex-protected rational-arithmetic algorithm model.
type Reference struct {
	mu     sync.Mutex
	states map[string]*referenceState
}

type referenceState struct {
	algorithm ratelimit.Algorithm
	observed  time.Time
	tokens    *big.Rat
	last      time.Time
	window    int64
	used      uint64
	segments  [16]referenceSegment
	leases    map[string]referenceLease
}

type referenceSegment struct {
	index int64
	used  uint64
}

type referenceLease struct {
	cost      uint64
	expiresAt time.Time
}

// NewReference constructs an empty reference model.
func NewReference() *Reference {
	return &Reference{states: make(map[string]*referenceState)}
}

// Name returns the stable reference backend identifier.
func (reference *Reference) Name() string { return "reference" }

// Admit evaluates a request using rational arithmetic and bounded windows.
func (reference *Reference) Admit(ctx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	if err := ctx.Err(); err != nil {
		return ratelimit.Decision{}, err
	}
	if err := request.Validate(); err != nil {
		return ratelimit.Decision{}, err
	}
	request.Now = time.UnixMicro(request.Now.UnixMicro()).UTC()
	reference.mu.Lock()
	defer reference.mu.Unlock()
	current := reference.state(request)
	request.Now = clampTime(request.Now, current.observed)
	current.observed = request.Now
	switch request.Policy.Algorithm() {
	case ratelimit.TokenBucket:
		return referenceToken(current, request)
	case ratelimit.FixedWindow:
		return referenceFixed(current, request)
	case ratelimit.SlidingWindow:
		return referenceSliding(current, request)
	default:
		return ratelimit.Decision{}, ratelimit.ErrUnsupported
	}
}

// Acquire obtains a deterministic weighted reference lease.
func (reference *Reference) Acquire(ctx context.Context, request ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
	if err := ctx.Err(); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	if err := request.Validate(); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	request.Request.Now = time.UnixMicro(request.Request.Now.UnixMicro()).UTC()
	reference.mu.Lock()
	defer reference.mu.Unlock()
	current := reference.state(request.Request)
	request.Request.Now = clampTime(request.Request.Now, current.observed)
	current.observed = request.Request.Now
	var used uint64
	var reset time.Time
	for id, lease := range current.leases {
		if !lease.expiresAt.After(request.Request.Now) {
			delete(current.leases, id)
			continue
		}
		used += lease.cost
		if reset.IsZero() || lease.expiresAt.Before(reset) {
			reset = lease.expiresAt
		}
	}
	if existing, ok := current.leases[request.LeaseID]; ok {
		if existing.cost != request.Request.Cost {
			return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrLeaseNotOwned
		}
		return referenceLeaseValue(request, existing.expiresAt), ratelimit.Decision{
			Allowed: true, Limit: request.Request.Policy.Limit(),
			Remaining: request.Request.Policy.Limit() - min(used, request.Request.Policy.Limit()),
			Reset:     existing.expiresAt, Reason: ratelimit.ReasonAllowed,
		}, nil
	}
	remaining := request.Request.Policy.Limit() - min(used, request.Request.Policy.Limit())
	if request.Request.Cost > remaining {
		return ratelimit.Lease{}, ratelimit.Decision{
			Allowed: false, Limit: request.Request.Policy.Limit(),
			Remaining: remaining, Reset: reset,
			RetryAfter: max(reset.Sub(request.Request.Now), 0),
			Reason:     ratelimit.ReasonLimited,
		}, ratelimit.ErrRejected
	}
	expiresAt := request.Request.Now.Add(request.Request.Policy.LeaseDuration())
	current.leases[request.LeaseID] = referenceLease{cost: request.Request.Cost, expiresAt: expiresAt}
	return referenceLeaseValue(request, expiresAt), ratelimit.Decision{
		Allowed: true, Limit: request.Request.Policy.Limit(),
		Remaining: remaining - request.Request.Cost,
		Reset:     expiresAt, Reason: ratelimit.ReasonAllowed,
	}, nil
}

// Release verifies and removes a reference lease.
func (reference *Reference) Release(ctx context.Context, lease ratelimit.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	reference.mu.Lock()
	defer reference.mu.Unlock()
	current, ok := reference.states[lease.PolicyID+"\x00"+lease.Key.String()]
	if !ok {
		return ratelimit.ErrLeaseNotFound
	}
	existing, ok := current.leases[lease.ID]
	if !ok {
		return ratelimit.ErrLeaseNotFound
	}
	if existing.cost != lease.Cost || !existing.expiresAt.Equal(lease.ExpiresAt) {
		return ratelimit.ErrLeaseNotOwned
	}
	delete(current.leases, lease.ID)
	return nil
}

func (reference *Reference) state(request ratelimit.Request) *referenceState {
	key := request.Policy.ID() + "\x00" + request.Key.String()
	current, ok := reference.states[key]
	if !ok {
		current = &referenceState{
			algorithm: request.Policy.Algorithm(), observed: request.Now,
			tokens: new(big.Rat).SetInt(new(big.Int).SetUint64(request.Policy.Limit())),
			last:   request.Now, leases: make(map[string]referenceLease),
		}
		reference.states[key] = current
	}
	return current
}

func referenceToken(current *referenceState, request ratelimit.Request) (ratelimit.Decision, error) {
	if request.Now.After(current.last) {
		elapsed := new(big.Rat).SetFrac(
			big.NewInt(request.Now.Sub(current.last).Microseconds()),
			big.NewInt(request.Policy.Period().Microseconds()),
		)
		elapsed.Mul(elapsed, new(big.Rat).SetInt(new(big.Int).SetUint64(request.Policy.Capacity())))
		current.tokens.Add(current.tokens, elapsed)
		limit := new(big.Rat).SetInt(new(big.Int).SetUint64(request.Policy.Limit()))
		if current.tokens.Cmp(limit) > 0 {
			current.tokens.Set(limit)
		}
		current.last = request.Now
	}
	cost := new(big.Rat).SetInt(new(big.Int).SetUint64(request.Cost))
	allowed := current.tokens.Cmp(cost) >= 0
	if allowed {
		current.tokens.Sub(current.tokens, cost)
	}
	remaining := floorRat(current.tokens)
	reset := request.Now.Add(referenceRefillDuration(
		new(big.Rat).Sub(
			new(big.Rat).SetInt(new(big.Int).SetUint64(request.Policy.Limit())),
			new(big.Rat).Set(current.tokens),
		),
		request.Policy,
	))
	decision := ratelimit.Decision{
		Allowed: allowed, Limit: request.Policy.Limit(), Remaining: remaining,
		Reset: reset, Reason: ratelimit.ReasonAllowed,
	}
	if !allowed {
		decision.Reason = ratelimit.ReasonLimited
		decision.RetryAfter = referenceRefillDuration(
			new(big.Rat).Sub(cost, new(big.Rat).Set(current.tokens)),
			request.Policy,
		)
		return decision, ratelimit.ErrRejected
	}
	return decision, nil
}

func referenceFixed(current *referenceState, request ratelimit.Request) (ratelimit.Decision, error) {
	period := request.Policy.Period().Nanoseconds()
	window := floorBoundary(request.Now.UnixNano(), period)
	if current.window != window {
		current.window, current.used = window, 0
	}
	return referenceConsume(current, request, time.Unix(0, window+period))
}

func referenceSliding(current *referenceState, request ratelimit.Request) (ratelimit.Decision, error) {
	period := request.Policy.Period().Nanoseconds()
	width := (period + 15) / 16
	index := floorBoundary(request.Now.UnixNano(), width) / width
	oldestIndex := floorBoundary(request.Now.Add(-request.Policy.Period()).UnixNano(), width) / width
	current.used = 0
	var earliest int64
	for slot := range current.segments {
		if current.segments[slot].index <= oldestIndex {
			current.segments[slot] = referenceSegment{}
			continue
		}
		current.used += current.segments[slot].used
		if current.segments[slot].used > 0 &&
			(earliest == 0 || current.segments[slot].index < earliest) {
			earliest = current.segments[slot].index
		}
	}
	slot := int(index % 16)
	if slot < 0 {
		slot += 16
	}
	if current.segments[slot].index != index {
		current.segments[slot] = referenceSegment{index: index}
	}
	reset := request.Now.Add(request.Policy.Period())
	if earliest != 0 {
		reset = time.Unix(0, (earliest+1)*width).Add(request.Policy.Period())
	}
	decision, err := referenceConsume(current, request, reset)
	if err == nil {
		current.segments[slot].used += request.Cost
	}
	return decision, err
}

func referenceConsume(current *referenceState, request ratelimit.Request, reset time.Time) (ratelimit.Decision, error) {
	remaining := request.Policy.Limit() - min(current.used, request.Policy.Limit())
	if request.Cost > remaining {
		return ratelimit.Decision{
			Allowed: false, Limit: request.Policy.Limit(), Remaining: remaining,
			Reset: reset, RetryAfter: max(reset.Sub(request.Now), 0),
			Reason: ratelimit.ReasonLimited,
		}, ratelimit.ErrRejected
	}
	current.used += request.Cost
	return ratelimit.Decision{
		Allowed: true, Limit: request.Policy.Limit(),
		Remaining: request.Policy.Limit() - current.used,
		Reset:     reset, Reason: ratelimit.ReasonAllowed,
	}, nil
}

func referenceRefillDuration(tokens *big.Rat, policy ratelimit.Policy) time.Duration {
	if tokens.Sign() <= 0 {
		return 0
	}
	value := new(big.Rat).Mul(tokens, new(big.Rat).SetInt64(policy.Period().Microseconds()))
	value.Quo(value, new(big.Rat).SetInt(new(big.Int).SetUint64(policy.Capacity())))
	numerator, denominator := value.Num(), value.Denom()
	quotient := new(big.Int).Quo(numerator, denominator)
	if new(big.Int).Mod(numerator, denominator).Sign() != 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	return time.Duration(quotient.Int64()) * time.Microsecond
}

func floorRat(value *big.Rat) uint64 {
	return new(big.Int).Quo(value.Num(), value.Denom()).Uint64()
}

func referenceLeaseValue(request ratelimit.LeaseRequest, expiresAt time.Time) ratelimit.Lease {
	return ratelimit.Lease{
		ID: request.LeaseID, Key: request.Request.Key,
		PolicyID:       request.Request.Policy.ID(),
		PolicyRevision: request.Request.Policy.Revision(),
		Cost:           request.Request.Cost, ExpiresAt: expiresAt, Backend: "reference",
	}
}

func clampTime(now, observed time.Time) time.Time {
	if now.Before(observed) {
		return observed
	}
	return now
}

func floorBoundary(value, size int64) int64 {
	quotient := value / size
	if value < 0 && value%size != 0 {
		quotient--
	}
	return quotient * size
}

var _ ratelimit.Backend = (*Reference)(nil)
var _ ratelimit.LeaseBackend = (*Reference)(nil)

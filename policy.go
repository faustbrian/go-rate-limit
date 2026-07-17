package ratelimit

import (
	"fmt"
	"math"
	"time"
)

// Algorithm identifies the admission algorithm used by a policy.
type Algorithm string

const (
	// TokenBucket refills capacity continuously using integer arithmetic.
	TokenBucket Algorithm = "token_bucket"
	// FixedWindow resets capacity at deterministic period boundaries.
	FixedWindow Algorithm = "fixed_window"
	// SlidingWindow estimates a rolling window using bounded segments.
	SlidingWindow Algorithm = "sliding_window"
	// Concurrency admits weighted work while an explicit lease is held.
	Concurrency Algorithm = "concurrency"
)

// FailureMode controls the decision returned when a backend cannot decide.
type FailureMode uint8

const (
	// FailClosed rejects admission when the backend is unavailable.
	FailClosed FailureMode = iota
	// FailOpen admits non-concurrency work when the backend is unavailable.
	FailOpen
)

// Consistency describes the scope in which a backend enforces a policy.
type Consistency string

const (
	// ConsistencyProcessLocal limits independently inside one process.
	ConsistencyProcessLocal Consistency = "process_local"
	// ConsistencyStrong requires atomic coordination through shared state.
	ConsistencyStrong Consistency = "strong"
)

// PolicySpec contains the immutable inputs used to construct a Policy.
type PolicySpec struct {
	// ID is a stable, non-secret policy identifier.
	ID string
	// Revision changes whenever admission semantics change.
	Revision string
	// Algorithm selects the state transition model.
	Algorithm Algorithm
	// Capacity is the base number of units available per period or lease set.
	Capacity uint64
	// Period controls refill or window duration for non-concurrency policies.
	Period time.Duration
	// Burst adds bounded capacity above Capacity.
	Burst uint64
	// MaxCost bounds the weight of one request; zero defaults to Limit.
	MaxCost uint64
	// FailureMode controls backend outage behavior.
	FailureMode FailureMode
	// Consistency declares the required coordination scope.
	Consistency Consistency
	// Lease is the maximum concurrency lease duration.
	Lease time.Duration
}

// Policy is an immutable, validated admission policy.
type Policy struct {
	id          string
	revision    string
	algorithm   Algorithm
	capacity    uint64
	period      time.Duration
	burst       uint64
	maxCost     uint64
	failureMode FailureMode
	consistency Consistency
	lease       time.Duration
}

// NewPolicy validates spec and returns an immutable policy.
func NewPolicy(spec PolicySpec) (Policy, error) {
	if spec.ID == "" || spec.Revision == "" || spec.Capacity == 0 {
		return Policy{}, fmt.Errorf("%w: identity and capacity are required", ErrInvalidPolicy)
	}
	if spec.Capacity > math.MaxInt64 || spec.Burst > math.MaxInt64-spec.Capacity {
		return Policy{}, fmt.Errorf("%w: capacity plus burst is too large", ErrInvalidPolicy)
	}
	switch spec.Algorithm {
	case TokenBucket, FixedWindow, SlidingWindow:
		if spec.Period < time.Microsecond || spec.Period%time.Microsecond != 0 {
			return Policy{}, fmt.Errorf("%w: period must use positive microsecond precision", ErrInvalidPolicy)
		}
	case Concurrency:
		if spec.Lease < time.Microsecond || spec.Lease%time.Microsecond != 0 {
			return Policy{}, fmt.Errorf("%w: concurrency lease must use positive microsecond precision", ErrInvalidPolicy)
		}
	default:
		return Policy{}, fmt.Errorf("%w: unknown algorithm", ErrInvalidPolicy)
	}
	limit := spec.Capacity + spec.Burst
	if spec.MaxCost == 0 {
		spec.MaxCost = limit
	}
	if spec.MaxCost > limit {
		return Policy{}, fmt.Errorf("%w: maximum cost exceeds limit", ErrInvalidPolicy)
	}
	if spec.FailureMode != FailClosed && spec.FailureMode != FailOpen {
		return Policy{}, fmt.Errorf("%w: unknown failure mode", ErrInvalidPolicy)
	}
	if spec.Algorithm == Concurrency && spec.FailureMode == FailOpen {
		return Policy{}, fmt.Errorf("%w: concurrency leases cannot fail open", ErrInvalidPolicy)
	}
	if spec.Consistency == "" {
		spec.Consistency = ConsistencyProcessLocal
	}
	if spec.Consistency != ConsistencyProcessLocal && spec.Consistency != ConsistencyStrong {
		return Policy{}, fmt.Errorf("%w: unknown consistency", ErrInvalidPolicy)
	}
	return Policy{
		id: spec.ID, revision: spec.Revision, algorithm: spec.Algorithm,
		capacity: spec.Capacity, period: spec.Period, burst: spec.Burst,
		maxCost: spec.MaxCost, failureMode: spec.FailureMode,
		consistency: spec.Consistency, lease: spec.Lease,
	}, nil
}

// ID returns the stable policy identity.
func (p Policy) ID() string { return p.id }

// Revision returns the immutable policy revision.
func (p Policy) Revision() string { return p.revision }

// Algorithm returns the policy algorithm.
func (p Policy) Algorithm() Algorithm { return p.algorithm }

// Capacity returns base capacity without burst.
func (p Policy) Capacity() uint64 { return p.capacity }

// Period returns the refill or window duration.
func (p Policy) Period() time.Duration { return p.period }

// Burst returns additional bounded capacity.
func (p Policy) Burst() uint64 { return p.burst }

// Limit returns Capacity plus Burst.
func (p Policy) Limit() uint64 { return p.capacity + p.burst }

// MaxCost returns the greatest weight allowed for one operation.
func (p Policy) MaxCost() uint64 { return p.maxCost }

// FailureMode returns the backend outage behavior.
func (p Policy) FailureMode() FailureMode { return p.failureMode }

// Consistency returns the required coordination scope.
func (p Policy) Consistency() Consistency { return p.consistency }

// LeaseDuration returns the concurrency lease duration.
func (p Policy) LeaseDuration() time.Duration { return p.lease }

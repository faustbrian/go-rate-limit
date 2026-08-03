package memory

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

func TestConfiguredBoundsAreInclusive(t *testing.T) {
	t.Parallel()

	if _, err := New(Options{MaxKeys: 0, Shards: 1}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("New(zero keys) error = %v", err)
	}
	store, err := New(Options{MaxKeys: MaxConfiguredKeys, Shards: MaxConfiguredShards})
	if err != nil {
		t.Fatalf("New(maximum bounds) error = %v", err)
	}
	if len(store.shards) != MaxConfiguredShards {
		t.Fatalf("shards = %d", len(store.shards))
	}
}

func TestLeaseAccountingExcludesExpiredAndIncludesEveryActiveLease(t *testing.T) {
	t.Parallel()

	store, err := New(Options{MaxKeys: 1, Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	request := internalLeaseRequest(t, "new", 1)
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "internal", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 3, MaxCost: 3, Lease: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Request.Policy = policy
	key := stateKey(request.Request)
	store.shards[0].states[key] = &state{
		algorithm: ratelimit.Concurrency,
		lastSeen:  request.Request.Now,
		leases: map[string]leaseState{
			"expired":  {cost: 1, expiresAt: request.Request.Now},
			"active-a": {cost: 1, expiresAt: request.Request.Now.Add(time.Second)},
			"active-b": {cost: 1, expiresAt: request.Request.Now.Add(time.Second)},
		},
	}

	_, decision, err := store.Acquire(context.Background(), request)
	if err != nil || !decision.Allowed || decision.Remaining != 0 {
		t.Fatalf("Acquire() = %+v, %v", decision, err)
	}
	if _, exists := store.shards[0].states[key].leases["expired"]; exists {
		t.Fatal("expired lease was retained")
	}
}

func TestSlidingSegmentAccumulatesWithinCurrentSlot(t *testing.T) {
	t.Parallel()

	request := internalRequest(t, ratelimit.SlidingWindow, 1)
	current := &state{}
	for range 2 {
		decision, err := admitSliding(current, request)
		if err != nil || !decision.Allowed {
			t.Fatalf("admitSliding() = %+v, %v", decision, err)
		}
	}
	segmentSize := (int64(request.Policy.Period()) + slidingSegments - 1) / slidingSegments
	index := floorBoundary(request.Now.UnixNano(), segmentSize) / segmentSize
	slot := positiveMod(index, slidingSegments)
	if current.segments[slot].used != 2 {
		t.Fatalf("current segment used = %d", current.segments[slot].used)
	}
}

func TestLenTotalsEveryShard(t *testing.T) {
	t.Parallel()

	store := &Store{shards: []shard{
		{states: map[string]*state{"one": {}}},
		{states: map[string]*state{"two": {}, "three": {}}},
	}}
	if got := store.Len(); got != 3 {
		t.Fatalf("Len() = %d", got)
	}
}

func TestSweepRejectsNonpositiveIdleDuration(t *testing.T) {
	t.Parallel()

	store, err := New(Options{MaxKeys: 1, Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, idleFor := range []time.Duration{0, -time.Nanosecond} {
		if _, err := store.Sweep(time.Unix(1, 0), idleFor); !errors.Is(err, ratelimit.ErrInvalidRequest) {
			t.Fatalf("Sweep(%s) error = %v", idleFor, err)
		}
	}
}

func TestActiveLeaseBoundaryAndEviction(t *testing.T) {
	t.Parallel()

	now := time.Unix(1, 0)
	if hasActiveLease(map[string]leaseState{"equal": {expiresAt: now}}, now) {
		t.Fatal("lease expiring at now was active")
	}
	if !hasActiveLease(map[string]leaseState{"later": {expiresAt: now.Add(time.Nanosecond)}}, now) {
		t.Fatal("future lease was inactive")
	}
	target := shard{states: map[string]*state{
		"active": {lastSeen: now.Add(-time.Hour), leases: map[string]leaseState{"lease": {expiresAt: now.Add(time.Second)}}},
		"idle":   {lastSeen: now},
	}}
	if !target.evictOldest(now) {
		t.Fatal("evictOldest() = false")
	}
	if _, exists := target.states["active"]; !exists {
		t.Fatal("active lease was evicted")
	}
}

func TestRefillExactArithmeticBoundaries(t *testing.T) {
	t.Parallel()

	widePolicy := policyForInternalTest(t, 9_000_000_000_000_000, time.Microsecond)
	start := time.Unix(0, 0)
	wide := &state{lastRefill: start}
	request := internalRequest(t, ratelimit.TokenBucket, 1)
	request.Policy = widePolicy
	request.Now = start.Add(2050 * time.Microsecond)
	refill(wide, request)
	if wide.tokens != widePolicy.Limit() || wide.remainder != 0 {
		t.Fatalf("wide refill = tokens %d, remainder %d", wide.tokens, wide.remainder)
	}

	exactPolicy := policyForInternalTest(t, 3, time.Second)
	exact := &state{lastRefill: start}
	request.Policy = exactPolicy
	request.Now = start.Add(1100 * time.Millisecond)
	refill(exact, request)
	if exact.tokens != exactPolicy.Limit() || exact.remainder != 0 {
		t.Fatalf("exact refill = tokens %d, remainder %d", exact.tokens, exact.remainder)
	}
}

func TestRefillDurationClampBoundaries(t *testing.T) {
	t.Parallel()

	policy := policyForInternalTest(t, 1, 2*time.Microsecond)
	if got := refillDuration(math.MaxUint64, 0, policy); got != time.Duration(math.MaxInt64) {
		t.Fatalf("high-word boundary = %s", got)
	}
	policy = policyForInternalTest(t, 1, time.Microsecond)
	quotient := uint64(math.MaxInt64 / int64(time.Microsecond))
	want := time.Duration(quotient) * time.Microsecond
	if got := refillDuration(quotient, 0, policy); got != want {
		t.Fatalf("quotient boundary = %s, want %s", got, want)
	}
}

func TestSignedHelperBoundaries(t *testing.T) {
	t.Parallel()

	if got := floorBoundary(-10, 10); got != -10 {
		t.Fatalf("floorBoundary(-10, 10) = %d", got)
	}
	if got := floorBoundary(0, 10); got != 0 {
		t.Fatalf("floorBoundary(0, 10) = %d", got)
	}
	if got := nonnegative(0); got != 0 {
		t.Fatalf("nonnegative(0) = %s", got)
	}
}

func policyForInternalTest(t *testing.T, capacity uint64, period time.Duration) ratelimit.Policy {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "boundary", Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Capacity: capacity, Period: period, MaxCost: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

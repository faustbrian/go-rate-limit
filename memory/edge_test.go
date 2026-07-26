package memory

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

func TestStoreValidationCancellationAndClosedEdges(t *testing.T) {
	t.Parallel()

	for _, options := range []Options{
		{}, {MaxKeys: 1}, {MaxKeys: 1, Shards: 2},
		{MaxKeys: 1_000_001, Shards: 1}, {MaxKeys: 1025, Shards: 1025},
	} {
		if _, err := New(options); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
			t.Fatalf("New(%+v) error = %v", options, err)
		}
	}
	store, err := New(Options{MaxKeys: 2, Shards: 2})
	if err != nil {
		t.Fatal(err)
	}
	if store.Name() != "memory" {
		t.Fatalf("Name() = %q", store.Name())
	}
	if uneven, err := New(Options{MaxKeys: 3, Shards: 2}); err != nil ||
		uneven.shards[0].maxKeys != 2 {
		t.Fatalf("uneven New() = %+v, %v", uneven, err)
	}
	if _, err := store.Admit(context.Background(), ratelimit.Request{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid Admit() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := internalRequest(t, ratelimit.FixedWindow, 1)
	concurrency := internalRequest(t, ratelimit.Concurrency, 1)
	if _, err := store.Admit(context.Background(), concurrency); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("concurrency Admit() error = %v", err)
	}
	if _, err := store.Admit(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Admit() error = %v", err)
	}
	leaseRequest := internalLeaseRequest(t, "lease", 1)
	if _, _, err := store.Acquire(context.Background(), ratelimit.LeaseRequest{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid Acquire() error = %v", err)
	}
	if _, _, err := store.Acquire(ctx, leaseRequest); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Acquire() error = %v", err)
	}
	if err := store.Release(ctx, ratelimit.Lease{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Release() error = %v", err)
	}
	if _, err := store.Sweep(time.Time{}, time.Second); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid Sweep() error = %v", err)
	}
	_ = store.Close()
	if _, _, err := store.Acquire(context.Background(), leaseRequest); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("closed Acquire() error = %v", err)
	}
	if err := store.Release(context.Background(), ratelimit.Lease{}); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("closed Release() error = %v", err)
	}
	if _, err := store.Sweep(time.Now(), time.Second); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("closed Sweep() error = %v", err)
	}
}

func TestLeaseIdempotencyExpiryAndOwnershipEdges(t *testing.T) {
	t.Parallel()

	store, err := New(Options{MaxKeys: 4, Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	request := internalLeaseRequest(t, "lease", 1)
	lease, _, err := store.Acquire(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	same, _, err := store.Acquire(context.Background(), request)
	if err != nil || !same.ExpiresAt.Equal(lease.ExpiresAt) {
		t.Fatalf("idempotent Acquire() = %+v, %v", same, err)
	}
	mismatch := request
	mismatch.Request.Cost = 2
	if _, _, err := store.Acquire(context.Background(), mismatch); !errors.Is(err, ratelimit.ErrLeaseNotOwned) {
		t.Fatalf("mismatched idempotent Acquire() error = %v", err)
	}
	rollback := request
	rollback.Request.Now = rollback.Request.Now.Add(-time.Second)
	if _, _, err := store.Acquire(context.Background(), rollback); err != nil {
		t.Fatalf("rollback Acquire() error = %v", err)
	}
	forged := lease
	forged.Cost = 2
	if err := store.Release(context.Background(), forged); !errors.Is(err, ratelimit.ErrLeaseNotOwned) {
		t.Fatalf("forged Release() error = %v", err)
	}
	expired := internalLeaseRequest(t, "next", 1)
	expired.Request.Now = lease.ExpiresAt
	if _, decision, err := store.Acquire(context.Background(), expired); err != nil ||
		!decision.Allowed {
		t.Fatalf("expired takeover = %+v, %v", decision, err)
	}
	missing := lease
	missing.PolicyID = "missing"
	if err := store.Release(context.Background(), missing); !errors.Is(err, ratelimit.ErrLeaseNotFound) {
		t.Fatalf("missing Release() error = %v", err)
	}
}

func TestInternalArithmeticAndEvictionEdges(t *testing.T) {
	t.Parallel()

	policy := internalRequest(t, ratelimit.TokenBucket, 1).Policy
	now := time.Unix(100, 0)
	current := &state{
		tokens: policy.Limit(), lastRefill: now, lastSeen: now,
		revision: "v1", algorithm: ratelimit.TokenBucket,
	}
	request := internalRequest(t, ratelimit.TokenBucket, 1)
	request.Now = now
	refill(current, request)
	request.Now = now.Add(time.Second)
	refill(current, request)
	current.tokens = 0
	current.remainder = uint64(policy.Period()) - 1
	current.lastRefill = now
	refill(current, request)
	if current.tokens != policy.Limit() {
		t.Fatalf("saturating refill tokens = %d", current.tokens)
	}
	if refillDuration(0, 0, policy) != 0 {
		t.Fatal("zero refill duration was nonzero")
	}
	if refillDuration(math.MaxUint64, 0, policy) != time.Duration(math.MaxInt64) {
		t.Fatal("overflow refill duration was not clamped")
	}
	if floorBoundary(-1, 10) != -10 || positiveMod(-1, 16) != 15 ||
		nonnegative(-time.Second) != 0 {
		t.Fatal("negative arithmetic helpers diverged")
	}

	target := shard{maxKeys: 1, states: map[string]*state{
		"b": {lastSeen: now},
		"a": {lastSeen: now},
	}}
	target.evictOldest(now)
	if _, ok := target.states["a"]; ok {
		t.Fatal("lexicographic eviction did not remove a")
	}
	target = shard{maxKeys: 1, states: map[string]*state{
		"old": {lastSeen: now},
		"new": {lastSeen: now.Add(time.Second)},
	}}
	target.evictOldest(now.Add(2 * time.Second))
	if _, ok := target.states["old"]; ok {
		t.Fatal("time-based eviction did not remove old")
	}

	for _, algorithm := range []ratelimit.Algorithm{
		ratelimit.TokenBucket, ratelimit.FixedWindow,
		ratelimit.SlidingWindow, ratelimit.Concurrency,
	} {
		current := &state{
			revision: "v1", algorithm: algorithm, tokens: 10,
			lastRefill: now.Add(-time.Second),
		}
		request := internalRequest(t, algorithm, 1)
		carryRevision(current, request)
		if current.revision != "v1" {
			t.Fatalf("revision = %q", current.revision)
		}
	}

	partialPolicy := internalRequest(t, ratelimit.TokenBucket, 1).Policy
	partial := &state{tokens: 0, lastRefill: now}
	partialRequest := internalRequest(t, ratelimit.TokenBucket, 1)
	partialRequest.Now = now.Add(100 * time.Millisecond)
	refill(partial, partialRequest)
	if partial.remainder == 0 {
		t.Fatal("partial refill remainder was lost")
	}
	hugePolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "huge", Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Capacity: 9_007_199_254_740_991, Period: time.Microsecond, MaxCost: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	huge := &state{tokens: 0, lastRefill: time.Unix(0, 0)}
	refill(huge, ratelimit.Request{
		Policy: hugePolicy, Key: request.Key, Cost: 1,
		Now: time.UnixMicro(9_007_199_254_740_991),
	})
	if huge.tokens != hugePolicy.Limit() {
		t.Fatal("wide refill did not saturate")
	}
	ceilPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "ceil", Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Capacity: 3, Period: time.Second, MaxCost: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if refillDuration(1, 0, ceilPolicy) != 333334*time.Microsecond {
		t.Fatal("refill duration was not rounded up")
	}
	overflowPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "duration-overflow", Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Capacity: 1, Period: 2 * time.Microsecond, MaxCost: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if refillDuration(math.MaxInt64, 0, overflowPolicy) != time.Duration(math.MaxInt64) {
		t.Fatal("large quotient was not clamped")
	}
	_ = partialPolicy
}

func internalRequest(t *testing.T, algorithm ratelimit.Algorithm, cost uint64) ratelimit.Request {
	t.Helper()
	spec := ratelimit.PolicySpec{
		ID: "internal", Revision: "v1", Algorithm: algorithm,
		Capacity: 2, Period: time.Second, MaxCost: 2,
	}
	if algorithm == ratelimit.Concurrency {
		spec.Period, spec.Lease = 0, time.Second
	}
	policy, err := ratelimit.NewPolicy(spec)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "case", Value: "internal"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ratelimit.Request{Policy: policy, Key: key, Cost: cost, Now: time.Unix(100, 0)}
}

func internalLeaseRequest(t *testing.T, id string, cost uint64) ratelimit.LeaseRequest {
	t.Helper()
	return ratelimit.LeaseRequest{Request: internalRequest(t, ratelimit.Concurrency, cost), LeaseID: id}
}

package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"github.com/faustbrian/golib/pkg/rate-limit/memory"
)

func TestTokenBucketUsesIntegerRefillAndWeightedCosts(t *testing.T) {
	t.Parallel()

	store := newStore(t, 8)
	now := time.Unix(10, 0)
	request := requestFor(t, ratelimit.TokenBucket, "token", "v1", 4, 2, time.Second, 3, now)

	first := admit(t, store, request)
	if !first.Allowed || first.Remaining != 3 || first.Limit != 6 {
		t.Fatalf("first = %+v", first)
	}
	request.Now = now.Add(250 * time.Millisecond)
	second := admit(t, store, request)
	if !second.Allowed || second.Remaining != 1 {
		t.Fatalf("second = %+v", second)
	}
	request.Now = now.Add(250 * time.Millisecond)
	decision, err := store.Admit(context.Background(), request)
	if !errors.Is(err, ratelimit.ErrRejected) || decision.Allowed ||
		decision.RetryAfter != 500*time.Millisecond ||
		!decision.Reset.Equal(now.Add(3*time.Second/2)) {
		t.Fatalf("rejected = %+v, %v", decision, err)
	}
}

func TestFixedWindowHasDeterministicBoundaries(t *testing.T) {
	t.Parallel()

	store := newStore(t, 8)
	request := requestFor(t, ratelimit.FixedWindow, "fixed", "v1", 2, 0, time.Second, 2, time.Unix(10, 900_000_000))
	if decision := admit(t, store, request); !decision.Allowed || decision.Remaining != 0 ||
		!decision.Reset.Equal(time.Unix(11, 0)) {
		t.Fatalf("first = %+v", decision)
	}
	request.Cost = 1
	if decision, err := store.Admit(context.Background(), request); !errors.Is(err, ratelimit.ErrRejected) ||
		decision.RetryAfter != 100*time.Millisecond {
		t.Fatalf("rejected = %+v, %v", decision, err)
	}
	request.Now = time.Unix(11, 0)
	if decision := admit(t, store, request); !decision.Allowed || decision.Remaining != 1 {
		t.Fatalf("next window = %+v", decision)
	}
}

func TestSlidingWindowCounterExpiresBoundedSegments(t *testing.T) {
	t.Parallel()

	store := newStore(t, 8)
	start := time.Unix(20, 0)
	request := requestFor(t, ratelimit.SlidingWindow, "slide", "v1", 2, 0, time.Second, 1, start)
	admit(t, store, request)
	request.Now = start.Add(500 * time.Millisecond)
	admit(t, store, request)
	request.Now = start.Add(999 * time.Millisecond)
	if _, err := store.Admit(context.Background(), request); !errors.Is(err, ratelimit.ErrRejected) {
		t.Fatalf("Admit() error = %v", err)
	}
	request.Now = start.Add(time.Second)
	if decision := admit(t, store, request); !decision.Allowed {
		t.Fatalf("expired decision = %+v", decision)
	}
}

func TestPolicyRevisionCarriesPriorConsumption(t *testing.T) {
	t.Parallel()

	store := newStore(t, 8)
	now := time.Unix(30, 0)
	request := requestFor(t, ratelimit.FixedWindow, "revision", "v1", 5, 0, time.Minute, 4, now)
	admit(t, store, request)
	request = requestFor(t, ratelimit.FixedWindow, "revision", "v2", 6, 0, time.Minute, 3, now)
	decision, err := store.Admit(context.Background(), request)
	if !errors.Is(err, ratelimit.ErrRejected) || decision.Remaining != 2 {
		t.Fatalf("revision decision = %+v, %v", decision, err)
	}
}

func TestAlgorithmChangeUnderPolicyIdentityIsCorrupt(t *testing.T) {
	t.Parallel()

	store := newStore(t, 8)
	now := time.Unix(31, 0)
	request := requestFor(t, ratelimit.FixedWindow, "algorithm", "v1", 2, 0, time.Minute, 1, now)
	admit(t, store, request)
	request = requestFor(t, ratelimit.TokenBucket, "algorithm", "v2", 2, 0, time.Minute, 1, now)
	if _, err := store.Admit(context.Background(), request); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("algorithm change error = %v", err)
	}

	concurrency := requestFor(t, ratelimit.Concurrency, "algorithm", "v3", 2, 0, 0, 1, now)
	leaseRequest := ratelimit.LeaseRequest{Request: concurrency, LeaseID: "lease"}
	if _, _, err := store.Acquire(context.Background(), leaseRequest); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("lease algorithm change error = %v", err)
	}
}

func TestStoreEvictsDeterministicallyAndShutsDown(t *testing.T) {
	t.Parallel()

	store := newStore(t, 1)
	first := requestFor(t, ratelimit.FixedWindow, "one", "v1", 1, 0, time.Hour, 1, time.Unix(1, 0))
	admit(t, store, first)
	second := requestFor(t, ratelimit.FixedWindow, "two", "v1", 1, 0, time.Hour, 1, time.Unix(2, 0))
	admit(t, store, second)
	if store.Len() != 1 {
		t.Fatalf("Len() = %d", store.Len())
	}
	if decision := admit(t, store, first); !decision.Allowed {
		t.Fatalf("evicted first decision = %+v", decision)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := store.Admit(context.Background(), first); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("Admit() after Close error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestConcurrencyLeaseIsExplicitAndReleasable(t *testing.T) {
	t.Parallel()

	store := newStore(t, 8)
	request := requestFor(t, ratelimit.Concurrency, "worker", "v1", 2, 0, 0, 1, time.Unix(40, 0))
	lease, decision, err := store.Acquire(context.Background(), ratelimit.LeaseRequest{Request: request, LeaseID: "job-1"})
	if err != nil || !decision.Allowed || lease.ID != "job-1" {
		t.Fatalf("Acquire() = %+v, %+v, %v", lease, decision, err)
	}
	request.Cost = 2
	if _, _, err := store.Acquire(context.Background(), ratelimit.LeaseRequest{Request: request, LeaseID: "job-2"}); !errors.Is(err, ratelimit.ErrRejected) {
		t.Fatalf("second Acquire() error = %v", err)
	}
	if err := store.Release(context.Background(), lease); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if err := store.Release(context.Background(), lease); !errors.Is(err, ratelimit.ErrLeaseNotFound) {
		t.Fatalf("second Release() error = %v", err)
	}
}

func TestClockRollbackCannotOpenAnOlderWindow(t *testing.T) {
	t.Parallel()

	store := newStore(t, 8)
	request := requestFor(t, ratelimit.FixedWindow, "rollback", "v1", 1, 0, time.Second, 1, time.Unix(11, 0))
	admit(t, store, request)
	request.Now = time.Unix(10, 0)
	if _, err := store.Admit(context.Background(), request); !errors.Is(err, ratelimit.ErrRejected) {
		t.Fatalf("rollback Admit() error = %v", err)
	}
}

func TestSweepRemovesIdleStateButPreservesActiveLease(t *testing.T) {
	t.Parallel()

	store := newStore(t, 8)
	request := requestFor(t, ratelimit.FixedWindow, "idle", "v1", 1, 0, time.Second, 1, time.Unix(10, 0))
	admit(t, store, request)
	leaseRequest := requestFor(t, ratelimit.Concurrency, "lease", "v1", 1, 0, 0, 1, time.Unix(10, 0))
	if _, _, err := store.Acquire(context.Background(), ratelimit.LeaseRequest{
		Request: leaseRequest, LeaseID: "active",
	}); err != nil {
		t.Fatal(err)
	}
	removed, err := store.Sweep(time.Unix(10, 500_000_000), 250*time.Millisecond)
	if err != nil || removed != 1 || store.Len() != 1 {
		t.Fatalf("Sweep(active) = %d, %v, len=%d", removed, err, store.Len())
	}
	removed, err = store.Sweep(time.Unix(12, 0), 250*time.Millisecond)
	if err != nil || removed != 1 || store.Len() != 0 {
		t.Fatalf("Sweep(expired) = %d, %v, len=%d", removed, err, store.Len())
	}
}

func TestCardinalityPressureCannotEvictActiveLease(t *testing.T) {
	t.Parallel()

	store, err := memory.New(memory.Options{MaxKeys: 1, Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	active := leaseRequestFor(t, "active", "job-1")
	if _, decision, err := store.Acquire(context.Background(), active); err != nil ||
		!decision.Allowed {
		t.Fatalf("active Acquire() = %+v, %v", decision, err)
	}

	pressure := leaseRequestFor(t, "pressure", "job-2")
	if _, _, err := store.Acquire(context.Background(), pressure); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("pressure Acquire() error = %v, want unavailable", err)
	}
	admission := requestFor(
		t, ratelimit.FixedWindow, "admission", "v1", 1, 0,
		time.Second, 1, time.Unix(10, 0),
	)
	if _, err := store.Admit(context.Background(), admission); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("pressure Admit() error = %v, want unavailable", err)
	}

	active.LeaseID = "job-3"
	if _, decision, err := store.Acquire(context.Background(), active); !errors.Is(err, ratelimit.ErrRejected) || decision.Allowed {
		t.Fatalf("second active Acquire() = %+v, %v", decision, err)
	}
}

func leaseRequestFor(t *testing.T, value, leaseID string) ratelimit.LeaseRequest {
	t.Helper()
	request := requestFor(
		t, ratelimit.Concurrency, value, "v1", 1, 0, 0, 1, time.Unix(10, 0),
	)
	return ratelimit.LeaseRequest{Request: request, LeaseID: leaseID}
}

func newStore(t *testing.T, maxKeys int) *memory.Store {
	t.Helper()
	store, err := memory.New(memory.Options{MaxKeys: maxKeys, Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func requestFor(t *testing.T, algorithm ratelimit.Algorithm, id, revision string, capacity, burst uint64, period time.Duration, cost uint64, now time.Time) ratelimit.Request {
	t.Helper()
	spec := ratelimit.PolicySpec{
		ID: id, Revision: revision, Algorithm: algorithm, Capacity: capacity,
		Burst: burst, Period: period, MaxCost: capacity + burst,
	}
	if algorithm == ratelimit.Concurrency {
		spec.Lease = time.Second
	}
	policy, err := ratelimit.NewPolicy(spec)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "principal", Value: "42"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ratelimit.Request{Policy: policy, Key: key, Cost: cost, Now: now}
}

func admit(t *testing.T, store *memory.Store, request ratelimit.Request) ratelimit.Decision {
	t.Helper()
	decision, err := store.Admit(context.Background(), request)
	if err != nil {
		t.Fatalf("Admit() error = %v", err)
	}
	return decision
}

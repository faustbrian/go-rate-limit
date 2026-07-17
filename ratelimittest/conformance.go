package ratelimittest

import (
	"context"
	"errors"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

// BackendFixture exposes admission and optional lease capabilities.
type BackendFixture struct {
	// Backend is required for algorithm conformance.
	Backend ratelimit.Backend
	// Leases enables concurrency conformance when non-nil.
	Leases ratelimit.LeaseBackend
}

// BackendFactory constructs isolated state for one conformance subtest.
type BackendFactory func(testing.TB) BackendFixture

// RunBackendConformance compares a backend with the rational reference model.
func RunBackendConformance(t *testing.T, factory BackendFactory) {
	t.Helper()
	for _, algorithm := range []ratelimit.Algorithm{
		ratelimit.TokenBucket, ratelimit.FixedWindow, ratelimit.SlidingWindow,
	} {
		t.Run(string(algorithm), func(t *testing.T) {
			fixture := factory(t)
			reference := NewReference()
			for _, request := range scenario(t, algorithm) {
				got, gotErr := fixture.Backend.Admit(context.Background(), request)
				want, wantErr := reference.Admit(context.Background(), request)
				if !sameError(gotErr, wantErr) ||
					got.Allowed != want.Allowed ||
					got.Remaining != want.Remaining ||
					got.Limit != want.Limit ||
					got.Reason != want.Reason ||
					got.RetryAfter != want.RetryAfter {
					t.Fatalf("Admit(%+v) = %+v, %v; want %+v, %v", request, got, gotErr, want, wantErr)
				}
			}
		})
	}
	t.Run("concurrency", func(t *testing.T) {
		fixture := factory(t)
		if fixture.Leases == nil {
			t.Skip("backend does not claim lease support")
		}
		reference := NewReference()
		request := leaseScenario(t)
		gotLease, got, gotErr := fixture.Leases.Acquire(context.Background(), request)
		_, want, wantErr := reference.Acquire(context.Background(), request)
		if !sameError(gotErr, wantErr) || got.Allowed != want.Allowed || got.Remaining != want.Remaining {
			t.Fatalf("Acquire() = %+v, %v; want %+v, %v", got, gotErr, want, wantErr)
		}
		request.LeaseID, request.Request.Cost = "job-2", 2
		_, got, gotErr = fixture.Leases.Acquire(context.Background(), request)
		_, want, wantErr = reference.Acquire(context.Background(), request)
		if !sameError(gotErr, wantErr) || got.Remaining != want.Remaining {
			t.Fatalf("contended Acquire() = %+v, %v; want %+v, %v", got, gotErr, want, wantErr)
		}
		if err := fixture.Leases.Release(context.Background(), gotLease); err != nil {
			t.Fatalf("Release() error = %v", err)
		}
	})
}

func scenario(t testing.TB, algorithm ratelimit.Algorithm) []ratelimit.Request {
	t.Helper()
	start := time.Unix(100, 0)
	capacity, burst := uint64(2), uint64(0)
	period := time.Second
	costs := []uint64{1, 1, 1, 1}
	offsets := []time.Duration{0, 500 * time.Millisecond, 999 * time.Millisecond, time.Second}
	if algorithm == ratelimit.TokenBucket {
		capacity, burst = 4, 2
		costs = []uint64{3, 3, 3}
		offsets = []time.Duration{0, 250*time.Millisecond + 999*time.Nanosecond, 250 * time.Millisecond}
	}
	if algorithm == ratelimit.FixedWindow {
		costs = []uint64{2, 1, 1}
		offsets = []time.Duration{900 * time.Millisecond, 900 * time.Millisecond, time.Second}
	}
	policy := policy(t, algorithm, capacity, burst, period)
	key := key(t)
	requests := make([]ratelimit.Request, len(costs))
	for index := range costs {
		requests[index] = ratelimit.Request{
			Policy: policy, Key: key, Cost: costs[index], Now: start.Add(offsets[index]),
		}
	}
	return requests
}

func leaseScenario(t testing.TB) ratelimit.LeaseRequest {
	t.Helper()
	return ratelimit.LeaseRequest{
		Request: ratelimit.Request{
			Policy: policy(t, ratelimit.Concurrency, 2, 0, time.Second),
			Key:    key(t), Cost: 1, Now: time.Unix(100, 0),
		},
		LeaseID: "job-1",
	}
}

func policy(t testing.TB, algorithm ratelimit.Algorithm, capacity, burst uint64, period time.Duration) ratelimit.Policy {
	t.Helper()
	spec := ratelimit.PolicySpec{
		ID: "conformance-" + string(algorithm), Revision: "v1",
		Algorithm: algorithm, Capacity: capacity, Burst: burst,
		Period: period, MaxCost: capacity + burst,
	}
	if algorithm == ratelimit.Concurrency {
		spec.Period, spec.Lease = 0, period
	}
	policy, err := ratelimit.NewPolicy(spec)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func key(t testing.TB) ratelimit.Key {
	t.Helper()
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "case", Value: t.Name()}, Hash: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func sameError(left, right error) bool {
	for _, sentinel := range []error{
		ratelimit.ErrRejected, ratelimit.ErrInvalidRequest, ratelimit.ErrUnavailable,
		ratelimit.ErrDeadline, ratelimit.ErrOverflow, ratelimit.ErrCorrupt,
	} {
		if errors.Is(left, sentinel) != errors.Is(right, sentinel) {
			return false
		}
	}
	return (left == nil) == (right == nil)
}

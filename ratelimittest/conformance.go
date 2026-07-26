package ratelimittest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

const contentionWorkers = 64

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
					!got.Reset.Equal(want.Reset) ||
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
	for _, algorithm := range []ratelimit.Algorithm{
		ratelimit.TokenBucket, ratelimit.FixedWindow,
		ratelimit.SlidingWindow, ratelimit.Concurrency,
	} {
		t.Run("rolling-revision-"+string(algorithm), func(t *testing.T) {
			runRevisionConformance(t, factory(t), algorithm)
		})
	}
}

func runRevisionConformance(t *testing.T, fixture BackendFixture, algorithm ratelimit.Algorithm) {
	t.Helper()
	reference := NewReference()
	capacities := []uint64{4, 2, 5}
	costs := []uint64{3, 1, 1}
	if algorithm == ratelimit.TokenBucket {
		costs[1] = 2
	}
	if algorithm == ratelimit.Concurrency {
		if fixture.Leases == nil {
			t.Skip("backend does not claim lease support")
		}
		costs[2] = 2
	}
	key := key(t)
	var firstLease ratelimit.LeaseRequest
	for index := range capacities {
		policy := revisionPolicy(t, algorithm, capacities[index], fmt.Sprintf("v%d", index+1))
		request := ratelimit.Request{
			Policy: policy, Key: key, Cost: costs[index], Now: time.Unix(100, 0),
		}
		if algorithm == ratelimit.Concurrency {
			leaseRequest := ratelimit.LeaseRequest{
				Request: request, LeaseID: fmt.Sprintf("revision-%d", index),
			}
			if index == 0 {
				firstLease = leaseRequest
			}
			_, got, gotErr := fixture.Leases.Acquire(context.Background(), leaseRequest)
			_, want, wantErr := reference.Acquire(context.Background(), leaseRequest)
			assertSameDecision(t, got, gotErr, want, wantErr)
			if index == 0 {
				collision := leaseRequest
				collision.Request.Cost = 1
				_, got, gotErr = fixture.Leases.Acquire(context.Background(), collision)
				_, want, wantErr = reference.Acquire(context.Background(), collision)
				assertSameDecision(t, got, gotErr, want, wantErr)
			}
			if index == 1 {
				_, got, gotErr = fixture.Leases.Acquire(context.Background(), firstLease)
				_, want, wantErr = reference.Acquire(context.Background(), firstLease)
				assertSameDecision(t, got, gotErr, want, wantErr)
			}
			continue
		}
		got, gotErr := fixture.Backend.Admit(context.Background(), request)
		want, wantErr := reference.Admit(context.Background(), request)
		assertSameDecision(t, got, gotErr, want, wantErr)
	}
}

func assertSameDecision(t testing.TB, got ratelimit.Decision, gotErr error, want ratelimit.Decision, wantErr error) {
	t.Helper()
	if !sameError(gotErr, wantErr) || got.Allowed != want.Allowed ||
		got.Remaining != want.Remaining || got.Limit != want.Limit ||
		got.Reason != want.Reason || !got.Reset.Equal(want.Reset) ||
		got.RetryAfter != want.RetryAfter {
		t.Fatalf("decision = %+v, %v; want %+v, %v", got, gotErr, want, wantErr)
	}
}

// RunBackendAtomicity proves exact same-key capacity and idempotent lease retry
// behavior under concurrent contention.
func RunBackendAtomicity(t *testing.T, factory BackendFactory) {
	t.Helper()
	t.Run("many-key-admission", func(t *testing.T) {
		fixture := factory(t)
		policy := policy(t, ratelimit.FixedWindow, 1, 0, time.Minute)
		var allowed atomic.Int64
		failures := contend(contentionWorkers, func(index int) error {
			key, err := ratelimit.NewKey(ratelimit.KeySpec{
				Namespace: "test", Version: "v1",
				Subject: ratelimit.Subject{
					Kind: "case", Value: fmt.Sprintf("%s-%d", t.Name(), index),
				},
				Hash: true,
			})
			if err != nil {
				return err
			}
			decision, err := fixture.Backend.Admit(context.Background(), ratelimit.Request{
				Policy: policy, Key: key, Cost: 1, Now: time.Unix(100, 0),
			})
			if err != nil || !decision.Allowed {
				return fmt.Errorf("worker %d decision %+v: %w", index, decision, err)
			}
			allowed.Add(1)
			return nil
		})
		if len(failures) != 0 || allowed.Load() != contentionWorkers {
			t.Fatalf("allowed=%d failures=%v", allowed.Load(), failures)
		}
	})

	t.Run("same-key-admission", func(t *testing.T) {
		fixture := factory(t)
		request := ratelimit.Request{
			Policy: policy(t, ratelimit.FixedWindow, 8, 0, time.Minute),
			Key:    key(t), Cost: 1, Now: time.Unix(100, 0),
		}
		var allowed atomic.Int64
		failures := contend(contentionWorkers, func(index int) error {
			decision, err := fixture.Backend.Admit(context.Background(), request)
			if err == nil && decision.Allowed {
				allowed.Add(1)
				return nil
			}
			if errors.Is(err, ratelimit.ErrRejected) && !decision.Allowed {
				return nil
			}
			return fmt.Errorf("worker %d decision %+v: %w", index, decision, err)
		})
		if len(failures) != 0 || allowed.Load() != 8 {
			t.Fatalf("allowed=%d failures=%v", allowed.Load(), failures)
		}
	})

	t.Run("idempotent-lease-retry", func(t *testing.T) {
		fixture := factory(t)
		if fixture.Leases == nil {
			t.Skip("backend does not claim lease support")
		}
		request := ratelimit.LeaseRequest{
			Request: ratelimit.Request{
				Policy: policy(t, ratelimit.Concurrency, 1, 0, time.Minute),
				Key:    key(t), Cost: 1, Now: time.Unix(100, 0),
			},
			LeaseID: "retry",
		}
		var owned atomic.Uint64
		failures := contend(contentionWorkers, func(index int) error {
			lease, decision, err := fixture.Leases.Acquire(context.Background(), request)
			if err != nil || !decision.Allowed || lease.ID != request.LeaseID {
				return fmt.Errorf("worker %d lease %+v decision %+v: %w", index, lease, decision, err)
			}
			owned.Add(lease.Cost)
			return nil
		})
		if len(failures) != 0 || owned.Load() != uint64(contentionWorkers) {
			t.Fatalf("owned=%d failures=%v", owned.Load(), failures)
		}
		other := request
		other.LeaseID = "other"
		if _, decision, err := fixture.Leases.Acquire(context.Background(), other); !errors.Is(err, ratelimit.ErrRejected) || decision.Allowed {
			t.Fatalf("other Acquire() = %+v, %v", decision, err)
		}
	})
}

func contend(workers int, operation func(int) error) []error {
	start := make(chan struct{})
	failures := make(chan error, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for index := range workers {
		go func() {
			defer group.Done()
			<-start
			if err := operation(index); err != nil {
				failures <- err
			}
		}()
	}
	close(start)
	group.Wait()
	close(failures)
	result := make([]error, 0, len(failures))
	for err := range failures {
		result = append(result, err)
	}
	return result
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
		costs = []uint64{3, 3, 3, 1, 1, 6}
		offsets = []time.Duration{
			0,
			250*time.Millisecond + 999*time.Nanosecond,
			250 * time.Millisecond,
			250*time.Millisecond + 999*time.Nanosecond,
			-time.Hour,
			24 * time.Hour,
		}
	}
	if algorithm == ratelimit.FixedWindow {
		costs = []uint64{2, 1, 1, 1, 1, 2}
		offsets = []time.Duration{
			900 * time.Millisecond,
			900 * time.Millisecond,
			time.Second,
			time.Second,
			-time.Hour,
			24 * time.Hour,
		}
	}
	if algorithm == ratelimit.SlidingWindow {
		costs = []uint64{1, 1, 1, 1, 1, 2}
		offsets = []time.Duration{
			0,
			500 * time.Millisecond,
			999 * time.Millisecond,
			time.Second,
			-time.Hour,
			24 * time.Hour,
		}
	}
	policy := policy(t, algorithm, capacity, burst, period)
	key := key(t)
	requests := make([]ratelimit.Request, len(costs)+1)
	requests[0] = ratelimit.Request{
		Policy: policy, Key: key, Cost: 1, Now: time.Unix(0, 0),
	}
	for index := range costs {
		requests[index+1] = ratelimit.Request{
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

func revisionPolicy(t testing.TB, algorithm ratelimit.Algorithm, capacity uint64, revision string) ratelimit.Policy {
	t.Helper()
	spec := ratelimit.PolicySpec{
		ID: "rolling-" + string(algorithm), Revision: revision,
		Algorithm: algorithm, Capacity: capacity, Period: time.Second,
		MaxCost: capacity,
	}
	if algorithm == ratelimit.Concurrency {
		spec.Period, spec.Lease = 0, time.Minute
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
		ratelimit.ErrLeaseNotFound, ratelimit.ErrLeaseNotOwned,
	} {
		if errors.Is(left, sentinel) != errors.Is(right, sentinel) {
			return false
		}
	}
	return (left == nil) == (right == nil)
}

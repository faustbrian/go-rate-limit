package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

type nilContext struct{}

func (*nilContext) Deadline() (time.Time, bool) { panic("called") }
func (*nilContext) Done() <-chan struct{}       { panic("called") }
func (*nilContext) Err() error                  { panic("called") }
func (*nilContext) Value(any) any               { panic("called") }

func TestStrictTemporalValidationRejectsStaleResults(t *testing.T) {
	effective := time.Unix(100, 0)
	stale := ratelimit.Decision{Allowed: true, Reset: effective.Add(-time.Microsecond)}
	if err := validateStrictDecisionTime(stale, effective); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("stale decision error = %v", err)
	}
	if decision, err := strictDecisionResult(stale, ratelimit.ErrRejected, effective); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("stale decision result = %+v, %v", decision, err)
	}
	lease := ratelimit.Lease{ExpiresAt: effective}
	decision := ratelimit.Decision{Allowed: true, Reset: effective}
	if err := validateStrictLeaseTime(lease, decision, effective); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("nonfuture lease error = %v", err)
	}
	if lease, decision, err := strictLeaseResult(ratelimit.Lease{}, stale, ratelimit.ErrRejected, effective); lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("stale lease result = %+v, %+v, %v", lease, decision, err)
	}
}

func TestStrictMemoryRejectsClampedEffectiveTimeOverflowBeforeMutation(t *testing.T) {
	const maximumExactMicros = int64(9_007_199_254_740_991)
	keyRequest := internalRequest(t, ratelimit.TokenBucket, 1)
	policy := func(revision string, algorithm ratelimit.Algorithm, duration time.Duration) ratelimit.Policy {
		t.Helper()
		spec := ratelimit.PolicySpec{
			ID: keyRequest.Policy.ID(), Revision: revision, Algorithm: algorithm,
			Capacity: 2, MaxCost: 2, Period: duration,
		}
		if algorithm == ratelimit.Concurrency {
			spec.Period = 0
			spec.Lease = duration
		}
		result, err := ratelimit.NewPolicy(spec)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}

	admitStore, _ := New(Options{MaxKeys: 2, Shards: 1})
	admit := keyRequest
	admit.Policy = policy("v1", ratelimit.TokenBucket, time.Microsecond)
	admit.Now = time.UnixMicro(maximumExactMicros - 1)
	if _, err := admitStore.AdmitStrict(context.Background(), admit); err != nil {
		t.Fatal(err)
	}
	admit.Policy = policy("v2", ratelimit.TokenBucket, 2*time.Microsecond)
	admit.Now = time.UnixMicro(maximumExactMicros - 2)
	if decision, err := admitStore.AdmitStrict(context.Background(), admit); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrOverflow) {
		t.Fatalf("clamped admit = %+v, %v", decision, err)
	}
	if current := admitStore.shards[0].states[stateKey(admit)]; current == nil || current.revision != "v1" || current.lastSeen.UnixMicro() != maximumExactMicros-1 {
		t.Fatalf("clamped admit mutated state = %+v", current)
	}

	leaseStore, _ := New(Options{MaxKeys: 2, Shards: 1})
	leaseRequest := internalLeaseRequest(t, "first", 1)
	leaseRequest.Request.Policy = policy("v1", ratelimit.Concurrency, time.Microsecond)
	leaseRequest.Request.Now = time.UnixMicro(maximumExactMicros - 1)
	if _, _, err := leaseStore.AcquireStrict(context.Background(), leaseRequest); err != nil {
		t.Fatal(err)
	}
	leaseRequest.LeaseID = "second"
	leaseRequest.Request.Policy = policy("v2", ratelimit.Concurrency, 2*time.Microsecond)
	leaseRequest.Request.Now = time.UnixMicro(maximumExactMicros - 2)
	if lease, decision, err := leaseStore.AcquireStrict(context.Background(), leaseRequest); lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrOverflow) {
		t.Fatalf("clamped acquire = %+v, %+v, %v", lease, decision, err)
	}
	if current := leaseStore.shards[0].states[stateKey(leaseRequest.Request)]; current == nil || current.revision != "v1" || current.lastSeen.UnixMicro() != maximumExactMicros-1 || len(current.leases) != 1 {
		t.Fatalf("clamped acquire mutated state = %+v", current)
	}
}

func TestValidateStrictEffectiveRangeBoundaries(t *testing.T) {
	const maximumExactMicros = int64(9_007_199_254_740_991)
	for _, test := range []struct {
		name      string
		effective time.Time
		duration  time.Duration
		want      error
	}{
		{name: "exact lower", effective: time.UnixMicro(-maximumExactMicros), duration: time.Microsecond},
		{name: "below lower", effective: time.UnixMicro(-maximumExactMicros - 1), duration: time.Microsecond, want: ratelimit.ErrOverflow},
		{name: "zero duration", effective: time.UnixMicro(0), duration: 0, want: ratelimit.ErrOverflow},
		{name: "exact upper sum", effective: time.UnixMicro(maximumExactMicros - 1), duration: time.Microsecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateStrictEffectiveRange(test.effective, test.duration); !errors.Is(err, test.want) {
				t.Fatalf("validateStrictEffectiveRange() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestStrictMemoryRejectsSameIDWindowPeriodChanges(t *testing.T) {
	for _, algorithm := range []ratelimit.Algorithm{ratelimit.FixedWindow, ratelimit.SlidingWindow} {
		t.Run(string(algorithm), func(t *testing.T) {
			store, err := New(Options{MaxKeys: 1, Shards: 1})
			if err != nil {
				t.Fatal(err)
			}
			request := internalRequest(t, algorithm, 1)
			policy := func(revision string, period time.Duration) ratelimit.Policy {
				t.Helper()
				result, policyErr := ratelimit.NewPolicy(ratelimit.PolicySpec{
					ID: request.Policy.ID(), Revision: revision, Algorithm: algorithm,
					Capacity: 1, MaxCost: 1, Period: period,
				})
				if policyErr != nil {
					t.Fatal(policyErr)
				}
				return result
			}
			request.Policy = policy("v1", 10*time.Second)
			request.Now = time.Unix(15, 0)
			if decision, admitErr := store.AdmitStrict(context.Background(), request); admitErr != nil || !decision.Allowed {
				t.Fatalf("initial admit = %+v, %v", decision, admitErr)
			}
			current := store.shards[0].states[stateKey(request)]
			beforeRevision, beforeUsed, beforeWindow, beforeSegments := current.revision, current.used, current.windowStart, current.segments
			request.Policy = policy("v2", time.Minute)
			if decision, admitErr := store.AdmitStrict(context.Background(), request); decision != (ratelimit.Decision{}) || !errors.Is(admitErr, ratelimit.ErrCorrupt) {
				t.Fatalf("period change = %+v, %v", decision, admitErr)
			}
			if current.revision != beforeRevision || current.used != beforeUsed || current.windowStart != beforeWindow || current.segments != beforeSegments {
				t.Fatalf("period change mutated state = %+v", current)
			}
		})
	}
}

type cancelAfterLockContext struct{ calls int }

func (*cancelAfterLockContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*cancelAfterLockContext) Done() <-chan struct{}       { return nil }
func (ctx *cancelAfterLockContext) Err() error {
	ctx.calls++
	if ctx.calls > 1 {
		return context.Canceled
	}
	return nil
}
func (*cancelAfterLockContext) Value(any) any { return nil }

func TestStrictMemoryCancellationAfterLockDoesNotMutate(t *testing.T) {
	admitStore, _ := New(Options{MaxKeys: 2, Shards: 1})
	if decision, err := admitStore.AdmitStrict(&cancelAfterLockContext{}, internalRequest(t, ratelimit.TokenBucket, 1)); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("admit = %+v, %v", decision, err)
	}
	assertShardUnlocked(t, admitStore)
	if admitStore.Len() != 0 {
		t.Fatalf("admit keys=%d", admitStore.Len())
	}

	acquireStore, _ := New(Options{MaxKeys: 2, Shards: 1})
	if lease, decision, err := acquireStore.AcquireStrict(&cancelAfterLockContext{}, internalLeaseRequest(t, "lease", 1)); lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("acquire = %+v, %+v, %v", lease, decision, err)
	}
	assertShardUnlocked(t, acquireStore)
	if acquireStore.Len() != 0 {
		t.Fatalf("acquire keys=%d", acquireStore.Len())
	}

	store, _ := New(Options{MaxKeys: 2, Shards: 1})
	request := internalLeaseRequest(t, "owned", 1)
	lease, _, err := store.AcquireStrict(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseStrict(&cancelAfterLockContext{}, lease); !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("release = %v", err)
	}
	assertShardUnlocked(t, store)
	if err := store.ReleaseStrict(context.Background(), lease); err != nil {
		t.Fatalf("lease was mutated after cancellation: %v", err)
	}
}

func assertShardUnlocked(t *testing.T, store *Store) {
	t.Helper()
	select {
	case <-store.shards[0].mu.channel():
		store.shards[0].mu.Unlock()
	default:
		t.Fatal("strict operation retained the shard lock")
	}
}

func TestStrictMemoryValidationAndState(t *testing.T) {
	var absent *Store
	var nilCtx *nilContext
	if _, err := absent.AdmitStrict(context.Background(), ratelimit.Request{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil admit=%v", err)
	}
	store, _ := New(Options{MaxKeys: 2, Shards: 1})
	//lint:ignore SA1012 A nil context is the contract under test.
	if _, err := store.AdmitStrict(nil, ratelimit.Request{}); //nolint:staticcheck // A nil context is the contract under test.
	!errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("literal nil=%v", err)
	}
	if _, err := store.AdmitStrict(nilCtx, ratelimit.Request{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("nil context=%v", err)
	}
	if _, err := store.AdmitStrict(context.Background(), ratelimit.Request{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid=%v", err)
	}
	if _, err := store.AdmitStrict(context.Background(), internalRequest(t, ratelimit.Concurrency, 1)); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("unsupported=%v", err)
	}
	request := internalRequest(t, ratelimit.TokenBucket, 1)
	decision, err := store.AdmitStrict(context.Background(), request)
	if err != nil || !decision.Allowed {
		t.Fatalf("admit=%+v,%v", decision, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.AdmitStrict(ctx, request); !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("canceled=%v", err)
	}
	deadline, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	if _, err := store.AdmitStrict(deadline, request); !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("deadline=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AdmitStrict(context.Background(), request); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("closed=%v", err)
	}
}

func TestStrictMemoryLeaseLifecycle(t *testing.T) {
	var absent *Store
	var nilCtx *nilContext
	request := internalLeaseRequest(t, "lease", 1)
	if _, _, err := absent.AcquireStrict(context.Background(), request); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil acquire=%v", err)
	}
	store, _ := New(Options{MaxKeys: 2, Shards: 1})
	if _, _, err := store.AcquireStrict(nilCtx, request); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("nil context=%v", err)
	}
	if _, _, err := store.AcquireStrict(context.Background(), ratelimit.LeaseRequest{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid=%v", err)
	}
	lease, decision, err := store.AcquireStrict(context.Background(), request)
	if err != nil || !decision.Allowed {
		t.Fatalf("acquire=%+v,%+v,%v", lease, decision, err)
	}
	if err := absent.ReleaseStrict(context.Background(), lease); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil release=%v", err)
	}
	if err := store.ReleaseStrict(nilCtx, lease); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("nil context=%v", err)
	}
	if err := store.ReleaseStrict(context.Background(), ratelimit.Lease{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := store.AcquireStrict(ctx, internalLeaseRequest(t, "other", 1)); !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("acquire canceled=%v", err)
	}
	if err := store.ReleaseStrict(ctx, lease); !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("release canceled=%v", err)
	}
	if err := store.ReleaseStrict(context.Background(), lease); err != nil {
		t.Fatalf("release=%v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*ratelimit.Lease)
	}{
		{name: "ID", mutate: func(value *ratelimit.Lease) { value.ID = "" }},
		{name: "policy ID", mutate: func(value *ratelimit.Lease) { value.PolicyID = "" }},
		{name: "key", mutate: func(value *ratelimit.Lease) { value.Key = ratelimit.Key{} }},
		{name: "cost", mutate: func(value *ratelimit.Lease) { value.Cost = 0 }},
		{name: "expiry", mutate: func(value *ratelimit.Lease) { value.ExpiresAt = time.Time{} }},
	} {
		t.Run("invalid release "+test.name, func(t *testing.T) {
			invalid := lease
			test.mutate(&invalid)
			if err := store.ReleaseStrict(context.Background(), invalid); !errors.Is(err, ratelimit.ErrInvalidRequest) {
				t.Fatalf("error=%v", err)
			}
		})
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.AcquireStrict(context.Background(), request); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("closed acquire=%v", err)
	}
	if err := store.ReleaseStrict(context.Background(), lease); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("closed release=%v", err)
	}
	if strictError(context.Background(), ratelimit.ErrCorrupt) != ratelimit.ErrCorrupt { //nolint:errorlint // Stable direct errors preserve identity.
		t.Fatal("stable error changed")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(strictError(context.Background(), context.Canceled), ratelimit.ErrCanceled) || !errors.Is(strictError(canceled, ratelimit.ErrCorrupt), ratelimit.ErrCanceled) {
		t.Fatal("cancellation was not normalized from error and context independently")
	}
	if !errors.Is(strictError(context.Background(), context.DeadlineExceeded), ratelimit.ErrDeadline) || !errors.Is(strictError(deadlineContext(t), ratelimit.ErrCorrupt), ratelimit.ErrDeadline) {
		t.Fatal("deadline was not normalized from error and context independently")
	}
	if !errors.Is(strictError(deadlineContext(t), context.DeadlineExceeded), ratelimit.ErrDeadline) {
		t.Fatal("deadline not normalized")
	}
}

func TestStrictMemoryCapacityFailureReturnsWithoutTemporalPanic(t *testing.T) {
	store, _ := New(Options{MaxKeys: 1, Shards: 1})
	if _, _, err := store.AcquireStrict(context.Background(), internalLeaseRequest(t, "lease", 1)); err != nil {
		t.Fatal(err)
	}
	request := internalRequest(t, ratelimit.TokenBucket, 1)
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: "other", Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 2, Period: time.Second, MaxCost: 2})
	if err != nil {
		t.Fatal(err)
	}
	request.Policy = policy
	blocked := internalLeaseRequest(t, "blocked", 1)
	blockedPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: "other-concurrency", Revision: "v1", Algorithm: ratelimit.Concurrency, Capacity: 2, MaxCost: 2, Lease: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	blocked.Request.Policy = blockedPolicy
	if lease, decision, err := store.AcquireStrict(context.Background(), blocked); lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("acquire capacity result = %+v, %+v, %v", lease, decision, err)
	}
	if decision, err := store.AdmitStrict(context.Background(), request); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("capacity result = %+v, %v", decision, err)
	}
}

func TestStrictMemoryLeaseLockWaitCancellation(t *testing.T) {
	store, _ := New(Options{MaxKeys: 2, Shards: 1})
	request := internalLeaseRequest(t, "lease", 1)
	lease, _, err := store.AcquireStrict(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range []func(context.Context) error{
		func(ctx context.Context) error {
			_, _, err := store.AcquireStrict(ctx, internalLeaseRequest(t, "other", 1))
			return err
		},
		func(ctx context.Context) error { return store.ReleaseStrict(ctx, lease) },
	} {
		store.shards[0].mu.Lock()
		base, cancel := context.WithCancel(context.Background())
		ctx := &lockWaitContext{Context: base, waiting: make(chan struct{})}
		done := make(chan error, 1)
		go func() { done <- call(ctx) }()
		select {
		case <-ctx.waiting:
		case <-time.After(100 * time.Millisecond):
			store.shards[0].mu.Unlock()
			<-done
			t.Fatal("strict lease operation did not enter the lock wait")
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, ratelimit.ErrCanceled) {
				t.Fatalf("error=%v", err)
			}
		case <-time.After(100 * time.Millisecond):
			store.shards[0].mu.Unlock()
			<-done
			t.Fatal("lock wait was not canceled")
		}
		assertCanceledWaiterDidNotReleaseOwner(t, &store.shards[0].mu)
	}
}

func deadlineContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)
	return ctx
}

package ratelimittest

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
	lease := ratelimit.Lease{ExpiresAt: effective}
	decision := ratelimit.Decision{Allowed: true, Reset: effective}
	if err := validateStrictLeaseTime(lease, decision, effective); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("nonfuture lease error = %v", err)
	}
	if decision, err := strictDecisionResult(stale, ratelimit.ErrRejected, effective); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("stale decision result = %+v, %v", decision, err)
	}
	if lease, decision, err := strictLeaseResult(ratelimit.Lease{}, stale, ratelimit.ErrRejected, effective); lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("stale lease result = %+v, %+v, %v", lease, decision, err)
	}
}

func TestStrictReferenceRejectsClampedEffectiveTimeOverflowBeforeMutation(t *testing.T) {
	const maximumExactMicros = int64(9_007_199_254_740_991)
	keyRequest := referenceRequest(t, ratelimit.TokenBucket, 1)
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

	reference := NewReference()
	admit := keyRequest
	admit.Policy = policy("v1", ratelimit.TokenBucket, time.Microsecond)
	admit.Now = time.UnixMicro(maximumExactMicros - 1)
	if _, err := reference.AdmitStrict(context.Background(), admit); err != nil {
		t.Fatal(err)
	}
	admit.Policy = policy("v2", ratelimit.TokenBucket, 2*time.Microsecond)
	admit.Now = time.UnixMicro(maximumExactMicros - 2)
	if decision, err := reference.AdmitStrict(context.Background(), admit); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrOverflow) {
		t.Fatalf("clamped admit = %+v, %v", decision, err)
	}
	if current := reference.states[admit.Policy.ID()+"\x00"+admit.Key.String()]; current == nil || current.revision != "v1" || current.observed.UnixMicro() != maximumExactMicros-1 {
		t.Fatalf("clamped admit mutated state = %+v", current)
	}

	reference = NewReference()
	leaseRequest := referenceLeaseRequest(t, "first")
	leaseRequest.Request.Policy = policy("v1", ratelimit.Concurrency, time.Microsecond)
	leaseRequest.Request.Now = time.UnixMicro(maximumExactMicros - 1)
	if _, _, err := reference.AcquireStrict(context.Background(), leaseRequest); err != nil {
		t.Fatal(err)
	}
	leaseRequest.LeaseID = "second"
	leaseRequest.Request.Policy = policy("v2", ratelimit.Concurrency, 2*time.Microsecond)
	leaseRequest.Request.Now = time.UnixMicro(maximumExactMicros - 2)
	if lease, decision, err := reference.AcquireStrict(context.Background(), leaseRequest); lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrOverflow) {
		t.Fatalf("clamped acquire = %+v, %+v, %v", lease, decision, err)
	}
	if current := reference.states[leaseRequest.Request.Policy.ID()+"\x00"+leaseRequest.Request.Key.String()]; current == nil || current.revision != "v1" || current.observed.UnixMicro() != maximumExactMicros-1 || len(current.leases) != 1 {
		t.Fatalf("clamped acquire mutated state = %+v", current)
	}
}

func TestStrictReferenceRejectsSameIDWindowPeriodChanges(t *testing.T) {
	for _, algorithm := range []ratelimit.Algorithm{ratelimit.FixedWindow, ratelimit.SlidingWindow} {
		t.Run(string(algorithm), func(t *testing.T) {
			reference := NewReference()
			request := referenceRequest(t, algorithm, 1)
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
			if decision, admitErr := reference.AdmitStrict(context.Background(), request); admitErr != nil || !decision.Allowed {
				t.Fatalf("initial admit = %+v, %v", decision, admitErr)
			}
			key := request.Policy.ID() + "\x00" + request.Key.String()
			current := reference.states[key]
			beforeRevision, beforeUsed, beforeWindow, beforeSegments := current.revision, current.used, current.window, current.segments
			request.Policy = policy("v2", time.Minute)
			if decision, admitErr := reference.AdmitStrict(context.Background(), request); decision != (ratelimit.Decision{}) || !errors.Is(admitErr, ratelimit.ErrCorrupt) {
				t.Fatalf("period change = %+v, %v", decision, admitErr)
			}
			if current.revision != beforeRevision || current.used != beforeUsed || current.window != beforeWindow || current.segments != beforeSegments {
				t.Fatalf("period change mutated state = %+v", current)
			}
		})
	}
}

type canceledAfterLockContext struct{}

func (*canceledAfterLockContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*canceledAfterLockContext) Done() <-chan struct{}       { return nil }
func (*canceledAfterLockContext) Err() error                  { return context.Canceled }
func (*canceledAfterLockContext) Value(any) any               { return nil }

type releaseCanceledAfterLockContext struct{ calls int }

func (*releaseCanceledAfterLockContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*releaseCanceledAfterLockContext) Done() <-chan struct{}       { return nil }
func (ctx *releaseCanceledAfterLockContext) Err() error {
	ctx.calls++
	if ctx.calls > 1 {
		return context.Canceled
	}
	return nil
}
func (*releaseCanceledAfterLockContext) Value(any) any { return nil }

func TestStrictReferenceCancellationAfterLockDoesNotMutate(t *testing.T) {
	reference := NewReference()
	if decision, err := reference.AdmitStrict(&canceledAfterLockContext{}, referenceRequest(t, ratelimit.TokenBucket, 1)); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCanceled) || len(reference.states) != 0 {
		t.Fatalf("admit = %+v, %v, states=%d", decision, err, len(reference.states))
	}
	if lease, decision, err := reference.AcquireStrict(&canceledAfterLockContext{}, referenceLeaseRequest(t, "lease")); lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCanceled) || len(reference.states) != 0 {
		t.Fatalf("acquire = %+v, %+v, %v, states=%d", lease, decision, err, len(reference.states))
	}

	lease, _, err := reference.AcquireStrict(context.Background(), referenceLeaseRequest(t, "owned"))
	if err != nil {
		t.Fatal(err)
	}
	if err := reference.ReleaseStrict(&releaseCanceledAfterLockContext{}, lease); !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("release = %v", err)
	}
	if err := reference.ReleaseStrict(context.Background(), lease); err != nil {
		t.Fatalf("lease was mutated after cancellation: %v", err)
	}
}

func TestStrictReferenceValidationAndLifecycle(t *testing.T) {
	var absent *Reference
	var nilCtx *nilContext
	if _, err := absent.AdmitStrict(context.Background(), ratelimit.Request{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil admit=%v", err)
	}
	reference := NewReference()
	if _, err := reference.AdmitStrict(nilCtx, ratelimit.Request{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("nil context=%v", err)
	}
	if _, err := reference.AdmitStrict(context.Background(), ratelimit.Request{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid=%v", err)
	}
	request := referenceRequest(t, ratelimit.TokenBucket, 1)
	decision, err := reference.AdmitStrict(context.Background(), request)
	if err != nil || !decision.Allowed {
		t.Fatalf("admit=%+v,%v", decision, err)
	}
	leaseRequest := referenceLeaseRequest(t, "lease")
	if _, _, err := absent.AcquireStrict(context.Background(), leaseRequest); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil acquire=%v", err)
	}
	if _, _, err := reference.AcquireStrict(nilCtx, leaseRequest); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("nil acquire context=%v", err)
	}
	if _, _, err := reference.AcquireStrict(context.Background(), ratelimit.LeaseRequest{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid acquire=%v", err)
	}
	lease, decision, err := reference.AcquireStrict(context.Background(), leaseRequest)
	if err != nil || !decision.Allowed {
		t.Fatalf("acquire=%+v,%+v,%v", lease, decision, err)
	}
	if err := absent.ReleaseStrict(context.Background(), lease); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil release=%v", err)
	}
	if err := reference.ReleaseStrict(nilCtx, lease); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("nil release context=%v", err)
	}
	if err := reference.ReleaseStrict(context.Background(), ratelimit.Lease{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid release=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reference.AdmitStrict(ctx, request); !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("canceled admit=%v", err)
	}
	if _, _, err := reference.AcquireStrict(ctx, referenceLeaseRequest(t, "other")); !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("canceled acquire=%v", err)
	}
	if err := reference.ReleaseStrict(ctx, lease); !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("canceled release=%v", err)
	}
	if err := reference.ReleaseStrict(context.Background(), lease); err != nil {
		t.Fatalf("release=%v", err)
	}
	deadline, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	if _, err := reference.AdmitStrict(deadline, request); !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("deadline=%v", err)
	}
	if referenceStrictError(context.Background(), ratelimit.ErrCorrupt) != ratelimit.ErrCorrupt { //nolint:errorlint // Stable direct errors preserve identity.
		t.Fatal("stable changed")
	}
	if !nilInterface(nil) {
		t.Fatal("literal nil was not detected")
	}
	if err := reference.ReleaseStrict(deadline, lease); !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("deadline release=%v", err)
	}
}

func TestStrictReferenceTokenRevisionCarryIsConservative(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	policy := func(revision string, capacity uint64) ratelimit.Policy {
		t.Helper()
		result, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
			ID: "reference-revision", Revision: revision, Algorithm: ratelimit.TokenBucket,
			Capacity: capacity, Period: time.Minute, MaxCost: capacity,
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	request := func(revision string, capacity uint64) ratelimit.Request {
		result := referenceRequest(t, ratelimit.TokenBucket, 1)
		result.Policy = policy(revision, capacity)
		result.Now = now
		return result
	}

	reference := NewReference()
	first := request("v1", 10)
	if decision, err := reference.AdmitStrict(context.Background(), first); err != nil || decision.Remaining != 9 {
		t.Fatalf("initial decrease state = %+v, %v", decision, err)
	}
	if decision, err := reference.AdmitStrict(context.Background(), request("v2", 2)); err != nil || !decision.Allowed || decision.Remaining != 1 {
		t.Fatalf("decreased revision = %+v, %v", decision, err)
	}

	reference = NewReference()
	if _, err := reference.AdmitStrict(context.Background(), request("v1", 2)); err != nil {
		t.Fatal(err)
	}
	if decision, err := reference.AdmitStrict(context.Background(), request("v2", 10)); err != nil || !decision.Allowed || decision.Remaining != 0 {
		t.Fatalf("increased revision = %+v, %v", decision, err)
	}

	reference = NewReference()
	malformed := request("v2", 2)
	if _, err := reference.AdmitStrict(context.Background(), malformed); err != nil {
		t.Fatal(err)
	}
	current := reference.state(malformed)
	current.tokens.SetInt64(10)
	if decision, err := reference.AdmitStrict(context.Background(), malformed); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("same-revision malformed state = %+v, %v", decision, err)
	}

	reference = NewReference()
	fractional := request("v1", 2)
	current = reference.state(fractional)
	current.tokens.SetFrac64(19, 10)
	current.revision = "v1"
	transition := request("v2", 2)
	if decision, err := reference.AdmitStrict(context.Background(), transition); err != nil || !decision.Allowed || decision.Remaining != 0 {
		t.Fatalf("fractional transition = %+v, %v", decision, err)
	}
	transition.Now = transition.Now.Add(10 * time.Second)
	if decision, err := reference.AdmitStrict(context.Background(), transition); !errors.Is(err, ratelimit.ErrRejected) || decision.Allowed {
		t.Fatalf("fractional carry was not discarded = %+v, %v", decision, err)
	}
}

func TestStrictReferenceAcquireRejectsAlgorithmReuse(t *testing.T) {
	reference := NewReference()
	request := referenceRequest(t, ratelimit.TokenBucket, 1)
	if _, err := reference.AdmitStrict(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: request.Policy.ID(), Revision: "v2", Algorithm: ratelimit.Concurrency,
		Capacity: 2, MaxCost: 2, Lease: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, decision, err := reference.AcquireStrict(context.Background(), ratelimit.LeaseRequest{
		Request: ratelimit.Request{Policy: policy, Key: request.Key, Cost: 1, Now: request.Now},
		LeaseID: "lease",
	})
	if lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("AcquireStrict() = %+v, %+v, %v", lease, decision, err)
	}
}

func TestStrictReferenceUnsupportedAdmitDoesNotCreateOrAdvanceLeaseState(t *testing.T) {
	reference := NewReference()
	leaseRequest := referenceLeaseRequest(t, "lease")
	unsupported := leaseRequest.Request
	unsupported.Now = unsupported.Now.Add(time.Hour)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	if decision, err := reference.AdmitStrict(canceled, unsupported); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("AdmitStrict(canceled concurrency) = %+v, %v", decision, err)
	}
	if len(reference.states) != 0 {
		t.Fatalf("canceled admission created state: %+v", reference.states)
	}

	//nolint:errorlint // The strict contract requires the direct sentinel, not a wrapper.
	if decision, err := reference.AdmitStrict(context.Background(), unsupported); decision != (ratelimit.Decision{}) || err != ratelimit.ErrUnsupported {
		t.Fatalf("AdmitStrict(concurrency) = %+v, %v", decision, err)
	}
	if len(reference.states) != 0 {
		t.Fatalf("unsupported admission created state: %+v", reference.states)
	}

	lease, decision, err := reference.AcquireStrict(context.Background(), leaseRequest)
	wantExpiry := leaseRequest.Request.Now.Add(leaseRequest.Request.Policy.LeaseDuration())
	if err != nil || !decision.Allowed || !lease.ExpiresAt.Equal(wantExpiry) {
		t.Fatalf("AcquireStrict() = %+v, %+v, %v; want expiry %s", lease, decision, err, wantExpiry)
	}
}

func referenceLeaseRequest(t *testing.T, id string) ratelimit.LeaseRequest {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: "strict-lease", Revision: "v1", Algorithm: ratelimit.Concurrency, Capacity: 2, MaxCost: 2, Lease: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	request := referenceRequest(t, ratelimit.TokenBucket, 1)
	request.Policy = policy
	return ratelimit.LeaseRequest{Request: request, LeaseID: id}
}

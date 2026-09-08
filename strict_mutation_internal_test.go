package ratelimit

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDirectErrorRequiresExactComparableIdentity(t *testing.T) {
	if !directError(nil, nil) || directError(nil, ErrRejected) || directError(ErrRejected, nil) {
		t.Fatal("nil identity comparison changed")
	}
	if !directError(ErrRejected, ErrRejected) || directError(ErrRejected, ErrUnavailable) {
		t.Fatal("sentinel identity comparison changed")
	}
	if directError(strictMutationError("a"), strictMutationError("a")) {
		t.Fatal("non-comparable errors were compared")
	}
}

type strictMutationError []byte

func (strictMutationError) Error() string { return "mutation error" }

func TestStrictBackendNamePredicatesAreIndependent(t *testing.T) {
	valid := []string{"a", "z", "a0", "a9", "a-b", strings.Repeat("a", MaxBackendNameBytes)}
	invalid := []string{"", strings.Repeat("a", MaxBackendNameBytes+1), "0a", "A", "a_", "a--b", "a-", "a/b"}
	for _, name := range valid {
		if !validBackendName(name) {
			t.Fatalf("validBackendName(%q) = false", name)
		}
	}
	for _, name := range invalid {
		if validBackendName(name) {
			t.Fatalf("validBackendName(%q) = true", name)
		}
	}
}

func TestStrictObserverFilteringAndExactMaximum(t *testing.T) {
	request := strictMutationRequest(t, TokenBucket)
	var calls atomic.Int64
	observer := ObserveFunc(func(Observation) { calls.Add(1) })
	maximum := make([]Observer, MaxObservers)
	for index := range maximum {
		maximum[index] = observer
	}
	service, err := NewStrictService(strictMutationBackend{admit: func(_ context.Context, request Request) (Decision, error) {
		return Decision{Allowed: true, Limit: request.Policy.Limit(), Remaining: request.Policy.Limit() - request.Cost, Reset: request.Now.Add(time.Second), Reason: ReasonAllowed}, nil
	}}, append([]Observer{nil}, maximum...)...)
	if err != nil {
		t.Fatalf("maximum observers rejected: %v", err)
	}
	if _, err := service.Admit(context.Background(), request); err != nil || calls.Load() != MaxObservers {
		t.Fatalf("filtered observers called %d times: %v", calls.Load(), err)
	}
	if _, err := NewStrictService(strictMutationBackend{admit: func(context.Context, Request) (Decision, error) { return Decision{}, nil }}, append(maximum, observer)...); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("over maximum observers error = %v", err)
	}
}

func TestCompleteDecisionPredicatesRejectEachMalformedField(t *testing.T) {
	request := strictMutationRequest(t, TokenBucket)
	reset := request.Now.Add(time.Second)
	allowed := Decision{Allowed: true, Limit: request.Policy.Limit(), Remaining: request.Policy.Limit() - request.Cost, Reset: reset, Reason: ReasonAllowed}
	if !completeAllowed(allowed, request) {
		t.Fatal("complete allowed decision rejected")
	}
	allowedMutations := []func(*Decision){
		func(value *Decision) { value.Allowed = false },
		func(value *Decision) { value.Reason = ReasonLimited },
		func(value *Decision) { value.Limit++ },
		func(value *Decision) { value.Remaining++ },
		func(value *Decision) { value.Reset = time.Time{} },
		func(value *Decision) { value.RetryAfter = time.Nanosecond },
	}
	for index, mutate := range allowedMutations {
		candidate := allowed
		mutate(&candidate)
		if completeAllowed(candidate, request) {
			t.Fatalf("malformed allowed decision %d accepted: %+v", index, candidate)
		}
	}

	rejected := Decision{Allowed: false, Limit: request.Policy.Limit(), Remaining: request.Cost - 1, Reset: reset, RetryAfter: 0, Reason: ReasonLimited}
	if !completeRejected(rejected, request) {
		t.Fatal("complete rejected decision rejected")
	}
	rejectedMutations := []func(*Decision){
		func(value *Decision) { value.Allowed = true },
		func(value *Decision) { value.Reason = ReasonAllowed },
		func(value *Decision) { value.Limit++ },
		func(value *Decision) { value.Remaining = request.Cost },
		func(value *Decision) { value.Reset = time.Time{} },
		func(value *Decision) { value.RetryAfter = -time.Nanosecond },
	}
	for index, mutate := range rejectedMutations {
		candidate := rejected
		mutate(&candidate)
		if completeRejected(candidate, request) {
			t.Fatalf("malformed rejected decision %d accepted: %+v", index, candidate)
		}
	}
}

func TestStrictDeadlineFailOpenRejectsTypedNilOutcome(t *testing.T) {
	request := strictMutationRequest(t, TokenBucket)
	var outcome *strictOutcomeError
	var err error = outcome
	if strictDeadlineFailOpen(request, err) {
		t.Fatal("typed-nil outcome enabled fail-open")
	}
}

func TestCompleteLeaseAndValidationRejectEachMalformedField(t *testing.T) {
	request := LeaseRequest{Request: strictMutationRequest(t, Concurrency), LeaseID: "lease"}
	decision := Decision{Allowed: true, Limit: request.Request.Policy.Limit(), Remaining: request.Request.Policy.Limit() - request.Request.Cost, Reset: request.Request.Now.Add(time.Second), Reason: ReasonAllowed}
	lease := Lease{ID: request.LeaseID, Key: request.Request.Key, PolicyID: request.Request.Policy.ID(), PolicyRevision: request.Request.Policy.Revision(), Cost: request.Request.Cost, ExpiresAt: decision.Reset}
	if !completeLease(lease, decision, request) || validateStrictLease(lease) != nil {
		t.Fatal("complete lease rejected")
	}
	mutations := []func(*Lease){
		func(value *Lease) { value.ID = "other" },
		func(value *Lease) { value.Key = Key{} },
		func(value *Lease) { value.PolicyID = "other" },
		func(value *Lease) { value.PolicyRevision = "other" },
		func(value *Lease) { value.Cost++ },
		func(value *Lease) { value.ExpiresAt = time.Time{} },
		func(value *Lease) { value.ExpiresAt = decision.Reset.Add(time.Nanosecond) },
	}
	for index, mutate := range mutations {
		candidate := lease
		mutate(&candidate)
		if completeLease(candidate, decision, request) {
			t.Fatalf("malformed complete lease %d accepted: %+v", index, candidate)
		}
	}
	validationMutations := []func(*Lease){
		func(value *Lease) { value.ID = "" },
		func(value *Lease) { value.Key = Key{} },
		func(value *Lease) { value.PolicyID = "" },
		func(value *Lease) { value.Cost = 0 },
		func(value *Lease) { value.ExpiresAt = time.Time{} },
	}
	for index, mutate := range validationMutations {
		candidate := lease
		mutate(&candidate)
		if err := validateStrictLease(candidate); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("invalid lease %d error = %v", index, err)
		}
	}
}

func TestNormalizeAcquireRequiresExactShapes(t *testing.T) {
	request := LeaseRequest{Request: strictMutationRequest(t, Concurrency), LeaseID: "lease"}
	service := &StrictService{name: "backend"}
	decision := Decision{Allowed: true, Limit: request.Request.Policy.Limit(), Remaining: request.Request.Policy.Limit() - request.Request.Cost, Reset: request.Request.Now.Add(time.Second), Reason: ReasonAllowed}
	lease := Lease{ID: request.LeaseID, Key: request.Request.Key, PolicyID: request.Request.Policy.ID(), PolicyRevision: request.Request.Policy.Revision(), Cost: request.Request.Cost, ExpiresAt: decision.Reset}

	actualLease, actualDecision, err := service.normalizeAcquire(context.Background(), request, lease, decision, nil)
	if err != nil || actualLease.Backend != "backend" || actualDecision.Reason != ReasonAllowed {
		t.Fatalf("complete success = %+v, %+v, %v", actualLease, actualDecision, err)
	}
	for _, test := range []struct {
		lease    Lease
		decision Decision
		err      error
	}{
		{lease: lease, decision: decision, err: ErrUnavailable},
		{lease: lease, err: CanceledOutcome()},
		{decision: decision, err: CanceledOutcome()},
	} {
		actualLease, actualDecision, actualErr := service.normalizeAcquire(context.Background(), request, test.lease, test.decision, test.err)
		if actualLease != (Lease{}) || actualDecision.Reason != ReasonOutcomeUnknown || !errors.Is(actualErr, ErrOutcomeUnknown) {
			t.Fatalf("invalid acquire shape = %+v, %+v, %v", actualLease, actualDecision, actualErr)
		}
	}
	actualLease, actualDecision, err = service.normalizeAcquire(context.Background(), request, Lease{}, Decision{}, CanceledOutcome())
	if actualLease != (Lease{}) || actualDecision.Reason != ReasonCanceled || err.Error() != "rate limit canceled" || !errors.Is(err, ErrCanceled) || errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("declared cancellation = %+v, %+v, %v", actualLease, actualDecision, err)
	}
	rejected := Decision{Limit: request.Request.Policy.Limit(), Remaining: 0, Reset: request.Request.Now.Add(time.Second), Reason: ReasonLimited}
	actualLease, actualDecision, err = service.normalizeAcquire(context.Background(), request, Lease{}, rejected, ErrRejected)
	if actualLease != (Lease{}) || actualDecision.Reason != ReasonLimited || err != ErrRejected { //nolint:errorlint // Known rejection preserves the exact sentinel.
		t.Fatalf("complete rejection = %+v, %+v, %v", actualLease, actualDecision, err)
	}
	for _, test := range []struct {
		lease    Lease
		decision Decision
	}{
		{lease: lease, decision: rejected},
		{decision: Decision{}},
	} {
		actualLease, actualDecision, actualErr := service.normalizeAcquire(context.Background(), request, test.lease, test.decision, ErrRejected)
		if actualLease != (Lease{}) || actualDecision.Reason != ReasonOutcomeUnknown || !errors.Is(actualErr, ErrOutcomeUnknown) {
			t.Fatalf("invalid rejection shape = %+v, %+v, %v", actualLease, actualDecision, actualErr)
		}
	}
	actualLease, actualDecision, err = service.normalizeAcquire(context.Background(), request, Lease{}, rejected, ErrUnavailable)
	if actualLease != (Lease{}) || actualDecision.Reason != ReasonOutcomeUnknown || !errors.Is(err, ErrOutcomeUnknown) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("non-rejection with rejected decision = %+v, %+v, %v", actualLease, actualDecision, err)
	}
}

func TestStrictDeclarationRequiresExactOutcomeTypes(t *testing.T) {
	if declaration, category := strictDeclaration((*strictOutcomeError)(nil)); category != nil || declaration != declarationUnsafe {
		t.Fatalf("nil outcome declaration = %v, %v", category, declaration)
	}
	if declaration, category := strictDeclaration(CanceledOutcome()); category == nil || declaration != declarationOutcome {
		t.Fatalf("canceled outcome declaration = %v, %v", category, declaration)
	}
}

func TestNormalizeAdmitDoesNotTreatSafeErrorsAsRejections(t *testing.T) {
	request := strictMutationRequest(t, TokenBucket)
	service := &StrictService{name: "backend"}
	rejected := Decision{Limit: request.Policy.Limit(), Remaining: 0, Reset: request.Now.Add(time.Second), Reason: ReasonLimited}
	decision, err := service.normalizeAdmit(context.Background(), request, rejected, ErrUnavailable)
	if decision.Reason != ReasonOutcomeUnknown || !errors.Is(err, ErrOutcomeUnknown) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("non-rejection with rejected decision = %+v, %v", decision, err)
	}
}

func TestStrictBatchCancellationIndexesAtBoundaries(t *testing.T) {
	requests := []Request{strictMutationRequest(t, TokenBucket), strictMutationRequest(t, TokenBucket), strictMutationRequest(t, TokenBucket)}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	backend := strictMutationBackend{admit: func(_ context.Context, request Request) (Decision, error) {
		calls++
		if calls == 2 {
			cancel()
		}
		return Decision{Allowed: true, Limit: request.Policy.Limit(), Remaining: request.Policy.Limit() - request.Cost, Reset: request.Now.Add(time.Second), Reason: ReasonAllowed}, nil
	}}
	service, err := NewStrictService(backend)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Batch(ctx, BatchRequest{Requests: requests, Atomicity: AtomicityPerItem})
	var batchErr *StrictBatchError
	if !errors.As(err, &batchErr) || calls != 2 || len(result.Attempted) != 2 || result.Attempted[0] != 0 || result.Attempted[1] != 1 || result.Decisions[2] != (Decision{}) {
		t.Fatalf("batch = %+v, %v, calls=%d", result, err, calls)
	}
	items := batchErr.Items()
	if len(items) != 1 || items[0].Index() != 2 || !errors.Is(items[0], ErrCanceled) {
		t.Fatalf("items = %+v", items)
	}

	ctx, cancel = context.WithCancel(context.Background())
	service, _ = NewStrictService(strictMutationBackend{admit: func(_ context.Context, request Request) (Decision, error) {
		cancel()
		return Decision{Allowed: true, Limit: request.Policy.Limit(), Remaining: request.Policy.Limit() - request.Cost, Reset: request.Now.Add(time.Second), Reason: ReasonAllowed}, nil
	}})
	result, err = service.Batch(ctx, BatchRequest{Requests: requests[:1], Atomicity: AtomicityPerItem})
	if err != nil || len(result.Attempted) != 1 || !result.Decisions[0].Allowed {
		t.Fatalf("final known cancellation race = %+v, %v", result, err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	service, _ = NewStrictService(strictMutationBackend{admit: func(context.Context, Request) (Decision, error) {
		cancel()
		return Decision{}, UnknownOutcome(context.Canceled)
	}})
	result, err = service.Batch(ctx, BatchRequest{Requests: requests[:1], Atomicity: AtomicityPerItem})
	if !errors.As(err, &batchErr) || len(result.Attempted) != 1 || result.Attempted[0] != 0 || result.Decisions[0].Reason != ReasonOutcomeUnknown {
		t.Fatalf("final unknown cancellation race = %+v, %v", result, err)
	}
	items = batchErr.Items()
	if len(items) != 1 || items[0].Index() != 0 || !errors.Is(items[0], ErrOutcomeUnknown) || !errors.Is(items[0], ErrCanceled) {
		t.Fatalf("final unknown items = %+v", items)
	}

	for _, count := range []int{1, 2} {
		ctx, cancel = context.WithCancel(context.Background())
		service, _ = NewStrictService(strictMutationBackend{admit: func(_ context.Context, request Request) (Decision, error) {
			cancel()
			return Decision{
				Allowed: false, Limit: request.Policy.Limit(), Remaining: 0,
				Reset: request.Now.Add(time.Second), RetryAfter: time.Second, Reason: ReasonLimited,
			}, ErrRejected
		}})
		result, err = service.Batch(ctx, BatchRequest{Requests: requests[:count], Atomicity: AtomicityPerItem})
		if !errors.As(err, &batchErr) || len(result.Attempted) != 1 || result.Attempted[0] != 0 || result.Decisions[0].Reason != ReasonLimited {
			t.Fatalf("known terminal cancellation race(%d) = %+v, %v", count, result, err)
		}
		items = batchErr.Items()
		if len(items) != count || items[0].Index() != 0 || !errors.Is(items[0], ErrRejected) || errors.Is(items[0], ErrCanceled) {
			t.Fatalf("known terminal items(%d) = %+v", count, items)
		}
		if count == 2 {
			if result.Decisions[1] != (Decision{}) || items[1].Index() != 1 || !errors.Is(items[1], ErrCanceled) {
				t.Fatalf("known terminal next item = %+v / %+v", result.Decisions[1], items[1])
			}
		}
	}
}

func TestStrictBatchContinuesAfterBackendDeadlineAndUnknownOutcome(t *testing.T) {
	requests := []Request{strictMutationRequest(t, TokenBucket), strictMutationRequest(t, TokenBucket)}
	for _, test := range []struct {
		name       string
		firstError error
		wantReason Reason
		wantError  error
	}{
		{name: "deadline", firstError: DeadlineOutcome(), wantReason: ReasonDeadline, wantError: ErrDeadline},
		{name: "unknown", firstError: UnknownOutcome(nil), wantReason: ReasonOutcomeUnknown, wantError: ErrOutcomeUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			service, err := NewStrictService(strictMutationBackend{admit: func(_ context.Context, request Request) (Decision, error) {
				calls++
				if calls == 1 {
					return Decision{}, test.firstError
				}
				return Decision{
					Allowed: true, Limit: request.Policy.Limit(), Remaining: request.Policy.Limit() - request.Cost,
					Reset: request.Now.Add(time.Second), Reason: ReasonAllowed,
				}, nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := service.Batch(context.Background(), BatchRequest{Requests: requests, Atomicity: AtomicityPerItem})
			var batchErr *StrictBatchError
			if !errors.As(err, &batchErr) || calls != 2 || len(result.Attempted) != 2 || result.Attempted[0] != 0 || result.Attempted[1] != 1 || result.Decisions[0].Reason != test.wantReason || !result.Decisions[1].Allowed {
				t.Fatalf("Batch() = %+v, %v, calls=%d", result, err, calls)
			}
			items := batchErr.Items()
			if len(items) != 1 || items[0].Index() != 0 || !errors.Is(items[0], test.wantError) {
				t.Fatalf("items = %+v", items)
			}
		})
	}
}

func TestStrictContextNormalizationDoesNotDiscloseCancellationCause(t *testing.T) {
	sensitive := errors.New("sensitive cancellation cause")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(sensitive)
	calls := 0
	service, err := NewStrictService(strictMutationBackend{admit: func(context.Context, Request) (Decision, error) {
		calls++
		return Decision{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	decision, err := service.Admit(ctx, strictMutationRequest(t, TokenBucket))
	if calls != 0 || decision.Reason != ReasonCanceled || err == nil || err.Error() != ErrCanceled.Error() ||
		!errors.Is(err, ErrCanceled) || !errors.Is(err, context.Canceled) || errors.Is(err, sensitive) || strings.Contains(err.Error(), "sensitive") {
		t.Fatalf("cause normalization = %+v, %v, calls=%d", decision, err, calls)
	}
}

func TestValidateBatchEnforcesExactSizeBounds(t *testing.T) {
	request := strictMutationRequest(t, TokenBucket)
	if err := validateBatch(BatchRequest{Requests: []Request{request}, Atomicity: AtomicityPerItem}); err != nil {
		t.Fatalf("minimum batch rejected: %v", err)
	}
	if err := validateBatch(BatchRequest{Requests: make([]Request, MaxBatchSize), Atomicity: AtomicityPerItem}); err == nil {
		// Every zero request is invalid; reaching request validation proves the size bound accepted MaxBatchSize.
		t.Fatal("maximum batch bypassed request validation")
	} else if !errors.Is(err, ErrInvalidRequest) || strings.Contains(err.Error(), "batch size") {
		t.Fatalf("maximum batch size error = %v", err)
	}
	for _, size := range []int{0, MaxBatchSize + 1} {
		if err := validateBatch(BatchRequest{Requests: make([]Request, size), Atomicity: AtomicityPerItem}); !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "batch size") {
			t.Fatalf("batch size %d error = %v", size, err)
		}
	}
}

type strictMutationBackend struct {
	admit func(context.Context, Request) (Decision, error)
}

func (backend strictMutationBackend) Name() string { return "backend" }
func (backend strictMutationBackend) Admit(ctx context.Context, request Request) (Decision, error) {
	return backend.admit(ctx, request)
}
func (backend strictMutationBackend) AdmitStrict(ctx context.Context, request Request) (Decision, error) {
	return backend.admit(ctx, request)
}

func strictMutationRequest(t *testing.T, algorithm Algorithm) Request {
	t.Helper()
	spec := PolicySpec{ID: "strict-mutation", Revision: "v1", Algorithm: algorithm, Capacity: 2, MaxCost: 2}
	if algorithm == Concurrency {
		spec.Lease = time.Second
	} else {
		spec.Period = time.Second
	}
	policy, err := NewPolicy(spec)
	if err != nil {
		t.Fatal(err)
	}
	key, err := NewKey(KeySpec{Namespace: "test", Version: "v1", Subject: Subject{Kind: "case", Value: "mutation"}})
	if err != nil {
		t.Fatal(err)
	}
	return Request{Policy: policy, Key: key, Cost: 1, Now: time.Unix(100, 0)}
}

package ratelimit_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

type countingStrictBackend struct {
	nameCalls atomic.Int64
	name      string
	admit     func(context.Context, ratelimit.Request) (ratelimit.Decision, error)
}

func (backend *countingStrictBackend) Name() string {
	backend.nameCalls.Add(1)
	return backend.name
}

func (backend *countingStrictBackend) Admit(ctx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	return backend.admit(ctx, request)
}

func (backend *countingStrictBackend) AdmitStrict(ctx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	return backend.admit(ctx, request)
}

type nilStrictBackend struct{}

func (*nilStrictBackend) Name() string { panic("typed-nil backend invoked") }
func (*nilStrictBackend) Admit(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
	panic("typed-nil backend invoked")
}
func (*nilStrictBackend) AdmitStrict(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
	panic("typed-nil backend invoked")
}

type nilObserver struct{}

func (*nilObserver) Observe(ratelimit.Observation) { panic("typed-nil observer invoked") }

type nilContext struct{}

func (*nilContext) Deadline() (time.Time, bool) { panic("typed-nil context invoked") }
func (*nilContext) Done() <-chan struct{}       { panic("typed-nil context invoked") }
func (*nilContext) Err() error                  { panic("typed-nil context invoked") }
func (*nilContext) Value(any) any               { panic("typed-nil context invoked") }

func TestNewStrictServiceValidatesNilAndBackendNameBeforeRetention(t *testing.T) {
	t.Parallel()
	if _, err := ratelimit.NewStrictService(nil); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("literal nil error=%v", err)
	}

	if _, err := ratelimit.NewStrictService((*nilStrictBackend)(nil)); err == nil ||
		!errors.Is(err, ratelimit.ErrInvalidPolicy) || err.Error() != "invalid rate limit policy: backend is required" {
		t.Fatalf("NewStrictService(typed nil) error = %v", err)
	}
	validObserver := ratelimit.ObserveFunc(func(ratelimit.Observation) {})
	for _, test := range []struct {
		name string
		want bool
	}{
		{"a", true}, {strings.Repeat("a", 64), true}, {"a-0-z", true},
		{"", false}, {strings.Repeat("a", 65), false}, {"0name", false},
		{"Name", false}, {"na_me", false}, {"na--me", false}, {"name-", false},
		{"nåme", false}, {" name", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &countingStrictBackend{name: test.name, admit: completeAllowedAdmission}
			service, err := ratelimit.NewStrictService(backend, nil, validObserver)
			if test.want && (err != nil || service == nil) {
				t.Fatalf("NewStrictService(%q) = %v, %v", test.name, service, err)
			}
			if !test.want && (service != nil || err == nil || !errors.Is(err, ratelimit.ErrInvalidPolicy) ||
				err.Error() != "invalid rate limit policy: backend name must be 1 to 64 lowercase ASCII bytes using letters, digits, and single hyphens") {
				t.Fatalf("NewStrictService(%q) = %v, %v", test.name, service, err)
			}
			if backend.nameCalls.Load() != 1 {
				t.Fatalf("Name calls = %d, want 1", backend.nameCalls.Load())
			}
		})
	}
	backend := &countingStrictBackend{name: "valid", admit: completeAllowedAdmission}
	if _, err := ratelimit.NewStrictService(backend, (*nilObserver)(nil)); err == nil ||
		err.Error() != "invalid rate limit policy: observer at index 0 is nil" || backend.nameCalls.Load() != 0 {
		t.Fatalf("typed-nil observer error/calls = %v/%d", err, backend.nameCalls.Load())
	}
	observers := make([]ratelimit.Observer, ratelimit.MaxObservers+2)
	observers[0] = nil
	for index := 1; index < len(observers); index++ {
		observers[index] = validObserver
	}
	if _, err := ratelimit.NewStrictService(backend, observers...); err == nil ||
		err.Error() != "invalid rate limit policy: at most 16 observers are allowed" || backend.nameCalls.Load() != 0 {
		t.Fatalf("observer bound error/calls = %v/%d", err, backend.nameCalls.Load())
	}
}

func TestStrictAdmitValidationDeadlineAndBackendErrorBranches(t *testing.T) {
	var absent *ratelimit.StrictService
	if _, err := absent.Admit(context.Background(), ratelimit.Request{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil service=%v", err)
	}
	service, _ := ratelimit.NewStrictService(&countingStrictBackend{name: "strict", admit: completeAllowedAdmission})
	if _, err := service.Admit(context.Background(), ratelimit.Request{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid=%v", err)
	}
	request := validRequest(t, ratelimit.FailClosed)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err := service.Admit(ctx, request); !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("deadline=%v", err)
	}
	service, _ = ratelimit.NewStrictService(&countingStrictBackend{name: "strict", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
		return ratelimit.Decision{}, ratelimit.DeadlineOutcome()
	}})
	decision, err := service.Admit(context.Background(), request)
	if !errors.Is(err, ratelimit.ErrDeadline) || decision.Reason != ratelimit.ReasonDeadline {
		t.Fatalf("declared=%+v,%v", decision, err)
	}
	backendErr, _ := ratelimit.NewBackendError(ratelimit.ErrUnavailable)
	service, _ = ratelimit.NewStrictService(&countingStrictBackend{name: "strict", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
		return ratelimit.Decision{}, backendErr
	}})
	if _, err := service.Admit(context.Background(), request); err != backendErr { //nolint:errorlint // Direct BackendError identity is preserved.
		t.Fatalf("backend error=%v", err)
	}
}

func TestStrictServiceAdmitResultAndCategoryMatrix(t *testing.T) {
	t.Parallel()

	request := validRequest(t, ratelimit.FailClosed)
	reset := request.Now.Add(time.Second)
	completeAllowed := ratelimit.Decision{Allowed: true, Limit: request.Policy.Limit(), Remaining: 1, Reset: reset, Reason: ratelimit.ReasonAllowed}
	completeRejected := ratelimit.Decision{Limit: request.Policy.Limit(), Reset: reset, RetryAfter: time.Second, Reason: ratelimit.ReasonLimited}

	tests := []struct {
		name       string
		decision   ratelimit.Decision
		err        error
		wantReason ratelimit.Reason
		wantErr    error
		known      bool
	}{
		{"success", completeAllowed, nil, ratelimit.ReasonAllowed, nil, true},
		{"rejection", completeRejected, ratelimit.ErrRejected, ratelimit.ReasonLimited, ratelimit.ErrRejected, true},
		{"incomplete-rejection", ratelimit.Decision{}, ratelimit.ErrRejected, ratelimit.ReasonOutcomeUnknown, ratelimit.ErrOutcomeUnknown, false},
		{"unavailable", ratelimit.Decision{}, ratelimit.ErrUnavailable, ratelimit.ReasonBackendUnavailable, ratelimit.ErrUnavailable, true},
		{"overflow", ratelimit.Decision{}, ratelimit.ErrOverflow, ratelimit.ReasonBackendUnavailable, ratelimit.ErrOverflow, true},
		{"corrupt", ratelimit.Decision{}, ratelimit.ErrCorrupt, ratelimit.ReasonBackendUnavailable, ratelimit.ErrCorrupt, true},
		{"unsupported", ratelimit.Decision{}, ratelimit.ErrUnsupported, ratelimit.ReasonBackendUnavailable, ratelimit.ErrUnsupported, true},
		{"lease-not-found-inapplicable", ratelimit.Decision{}, ratelimit.ErrLeaseNotFound, ratelimit.ReasonOutcomeUnknown, ratelimit.ErrOutcomeUnknown, false},
		{"lease-not-owned-inapplicable", ratelimit.Decision{}, ratelimit.ErrLeaseNotOwned, ratelimit.ReasonOutcomeUnknown, ratelimit.ErrOutcomeUnknown, false},
		{"empty-success", ratelimit.Decision{}, nil, ratelimit.ReasonOutcomeUnknown, ratelimit.ErrOutcomeUnknown, false},
		{"denied-without-error", completeRejected, nil, ratelimit.ReasonOutcomeUnknown, ratelimit.ErrOutcomeUnknown, false},
		{"allowed-with-rejection", completeAllowed, ratelimit.ErrRejected, ratelimit.ReasonOutcomeUnknown, ratelimit.ErrOutcomeUnknown, false},
		{"nonzero-error-result", completeAllowed, ratelimit.ErrUnavailable, ratelimit.ReasonOutcomeUnknown, ratelimit.ErrOutcomeUnknown, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observed := make(chan ratelimit.Observation, 1)
			backend := &countingStrictBackend{name: "test-backend", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
				return test.decision, test.err
			}}
			service, err := ratelimit.NewStrictService(backend, ratelimit.ObserveFunc(func(value ratelimit.Observation) { observed <- value }))
			if err != nil {
				t.Fatal(err)
			}
			decision, err := service.Admit(context.Background(), request)
			if decision.Reason != test.wantReason || decision.Backend != "test-backend" || decision.PolicyRevision != request.Policy.Revision() {
				t.Fatalf("decision = %+v", decision)
			}
			if test.wantErr == nil && err != nil {
				t.Fatalf("error = %v", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			if test.known && errors.Is(err, ratelimit.ErrOutcomeUnknown) {
				t.Fatalf("known error became unknown: %v", err)
			}
			observation := <-observed
			if observation.Decision != decision || observation.Err != err { //nolint:errorlint // Observer receives the exact returned error object.
				t.Fatalf("observation = %+v/%v, result = %+v/%v", observation.Decision, observation.Err, decision, err)
			}
			if backend.nameCalls.Load() != 1 {
				t.Fatalf("Name calls = %d", backend.nameCalls.Load())
			}
		})
	}
}

func TestStrictServiceAdmitFailOpenAndContextPrecedence(t *testing.T) {
	t.Parallel()

	for _, backendErr := range []error{ratelimit.ErrUnavailable, ratelimit.DeadlineOutcome()} {
		service, err := ratelimit.NewStrictService(&countingStrictBackend{name: "backend", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
			return ratelimit.Decision{}, backendErr
		}})
		if err != nil {
			t.Fatal(err)
		}
		decision, err := service.Admit(context.Background(), validRequest(t, ratelimit.FailOpen))
		if err != nil || !decision.Allowed || decision.Reason != ratelimit.ReasonFailOpen {
			t.Fatalf("Admit(%v) = %+v, %v", backendErr, decision, err)
		}
	}
	for _, backendErr := range []error{ratelimit.CanceledOutcome(), ratelimit.UnknownOutcome(nil), errors.New("unsafe")} {
		service, err := ratelimit.NewStrictService(&countingStrictBackend{name: "backend", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
			return ratelimit.Decision{}, backendErr
		}})
		if err != nil {
			t.Fatal(err)
		}
		decision, err := service.Admit(context.Background(), validRequest(t, ratelimit.FailOpen))
		if err == nil || decision.Allowed || (decision.Reason != ratelimit.ReasonCanceled && decision.Reason != ratelimit.ReasonOutcomeUnknown) {
			t.Fatalf("Admit(%T) = %+v, %v", backendErr, decision, err)
		}
	}

	backendCalls := atomic.Int64{}
	observed := make(chan ratelimit.Observation, 1)
	service, err := ratelimit.NewStrictService(&countingStrictBackend{name: "backend", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
		backendCalls.Add(1)
		return ratelimit.Decision{}, nil
	}}, ratelimit.ObserveFunc(func(observation ratelimit.Observation) { observed <- observation }))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := validRequest(t, ratelimit.FailOpen)
	decision, err := service.Admit(ctx, request)
	if backendCalls.Load() != 0 || !errors.Is(err, ratelimit.ErrCanceled) || !errors.Is(err, context.Canceled) ||
		!errors.Is(err, ratelimit.ErrDeadline) || errors.Is(err, ratelimit.ErrOutcomeUnknown) || decision.Reason != ratelimit.ReasonCanceled ||
		decision.Backend != "backend" || decision.PolicyRevision != request.Policy.Revision() {
		t.Fatalf("pre-dispatch cancellation = %+v, %v, calls=%d", decision, err, backendCalls.Load())
	}
	if observation := <-observed; observation.Decision != decision || observation.Err != err { //nolint:errorlint // Observer receives the exact returned error object.
		t.Fatalf("pre-dispatch cancellation observation = %+v, %v", observation.Decision, observation.Err)
	}
	deadline, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer deadlineCancel()
	decision, err = service.Admit(deadline, validRequest(t, ratelimit.FailOpen))
	if err != nil || backendCalls.Load() != 0 || !decision.Allowed || decision.Reason != ratelimit.ReasonFailOpen {
		t.Fatalf("pre-dispatch deadline fail-open = %+v, %v, calls=%d", decision, err, backendCalls.Load())
	}
	if _, err := service.Admit((*nilContext)(nil), validRequest(t, ratelimit.FailClosed)); err == nil ||
		err.Error() != "invalid rate limit request: context is required" {
		t.Fatalf("typed-nil context error = %v", err)
	}
}

func TestStrictBatchStopsImmediatelyOnExpiredDeadline(t *testing.T) {
	backendCalls := atomic.Int64{}
	service, err := ratelimit.NewStrictService(&countingStrictBackend{name: "backend", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
		backendCalls.Add(1)
		return ratelimit.Decision{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	requests := []ratelimit.Request{validRequest(t, ratelimit.FailOpen), validRequest(t, ratelimit.FailOpen)}
	result, err := service.Batch(ctx, ratelimit.BatchRequest{Requests: requests, Atomicity: ratelimit.AtomicityPerItem})
	var aggregate *ratelimit.StrictBatchError
	if !errors.As(err, &aggregate) || backendCalls.Load() != 0 || len(result.Attempted) != 0 || len(result.Decisions) != 2 {
		t.Fatalf("Batch() = %+v, %v, calls=%d", result, err, backendCalls.Load())
	}
	if result.Decisions[0] != (ratelimit.Decision{}) || result.Decisions[1] != (ratelimit.Decision{}) {
		t.Fatalf("decisions = %+v", result.Decisions)
	}
	if items := aggregate.Items(); len(items) != 1 || items[0].Index() != 0 || !errors.Is(items[0], ratelimit.ErrDeadline) {
		t.Fatalf("items = %v", items)
	}
}

func TestStrictServiceCompleteSuccessWinsCancellationRace(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	request := validRequest(t, ratelimit.FailClosed)
	service, err := ratelimit.NewStrictService(&countingStrictBackend{name: "backend", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
		cancel()
		return completeAllowedAdmission(ctx, request)
	}})
	if err != nil {
		t.Fatal(err)
	}
	decision, err := service.Admit(ctx, request)
	if err != nil || !decision.Allowed || decision.Reason != ratelimit.ReasonAllowed {
		t.Fatalf("Admit() = %+v, %v", decision, err)
	}
}

func TestStrictServiceIsClockAuthorityNeutral(t *testing.T) {
	t.Parallel()

	request := validRequest(t, ratelimit.FailClosed)
	request.Now = time.Unix(300, 0)
	reset := time.Unix(200, 0)
	for _, test := range []struct {
		name     string
		decision ratelimit.Decision
		err      error
	}{
		{
			name: "allowed",
			decision: ratelimit.Decision{
				Allowed: true, Limit: request.Policy.Limit(),
				Remaining: request.Policy.Limit() - request.Cost,
				Reset:     reset, Reason: ratelimit.ReasonAllowed,
			},
		},
		{
			name: "rejected",
			decision: ratelimit.Decision{
				Limit: request.Policy.Limit(), Reset: reset,
				RetryAfter: time.Second, Reason: ratelimit.ReasonLimited,
			},
			err: ratelimit.ErrRejected,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed := make(chan ratelimit.Observation, 1)
			backend := &countingStrictBackend{name: "authority-neutral", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
				return test.decision, test.err
			}}
			service, err := ratelimit.NewStrictService(backend, ratelimit.ObserveFunc(func(observation ratelimit.Observation) {
				observed <- observation
			}))
			if err != nil {
				t.Fatal(err)
			}

			decision, err := service.Admit(context.Background(), request)
			if err != test.err || errors.Is(err, ratelimit.ErrOutcomeUnknown) || !decision.Reset.Equal(reset) { //nolint:errorlint // Known outcomes preserve exact identity.
				t.Fatalf("Admit() = %+v, %v", decision, err)
			}
			observation := <-observed
			if observation.Decision != decision || observation.Err != err { //nolint:errorlint // Observer receives the exact returned error object.
				t.Fatalf("observation = %+v/%v, result = %+v/%v", observation.Decision, observation.Err, decision, err)
			}
			if backend.nameCalls.Load() != 1 {
				t.Fatalf("Name calls = %d, want 1", backend.nameCalls.Load())
			}
		})
	}
}

type hostileStrictError []byte

func (hostileStrictError) Error() string { panic("hostile Error invoked") }
func (hostileStrictError) Is(error) bool { panic("hostile Is invoked") }
func (hostileStrictError) As(any) bool   { panic("hostile As invoked") }

func TestStrictServiceNeverTraversesOrFormatsUnsafeBackendErrors(t *testing.T) {
	t.Parallel()

	unsafeErrors := []error{
		hostileStrictError("secret"),
		fmt.Errorf("wrapped: %w", ratelimit.ErrUnavailable),
		errors.Join(ratelimit.ErrUnavailable, ratelimit.ErrCorrupt),
		context.Canceled,
		context.DeadlineExceeded,
		(*ratelimit.BackendError)(nil),
	}
	for index, backendErr := range unsafeErrors {
		t.Run(fmt.Sprintf("case-%d", index), func(t *testing.T) {
			service, err := ratelimit.NewStrictService(&countingStrictBackend{name: "backend", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
				return ratelimit.Decision{Allowed: true, Limit: 99, Reason: ratelimit.ReasonAllowed}, backendErr
			}})
			if err != nil {
				t.Fatal(err)
			}
			decision, err := service.Admit(context.Background(), validRequest(t, ratelimit.FailOpen))
			if !errors.Is(err, ratelimit.ErrOutcomeUnknown) || errors.Is(err, ratelimit.ErrUnavailable) || decision.Allowed ||
				decision.Reason != ratelimit.ReasonOutcomeUnknown || strings.Contains(err.Error(), "secret") {
				t.Fatalf("unsafe result = %+v, %v", decision, err)
			}
		})
	}
}

func TestBackendErrorAndStrictAggregateZeroContracts(t *testing.T) {
	t.Parallel()

	for _, category := range []error{
		ratelimit.ErrRejected, ratelimit.ErrUnavailable, ratelimit.ErrOverflow,
		ratelimit.ErrCorrupt, ratelimit.ErrUnsupported, ratelimit.ErrLeaseNotFound,
		ratelimit.ErrLeaseNotOwned,
	} {
		backendErr, err := ratelimit.NewBackendError(category)
		if err != nil || backendErr.Category() != category || backendErr.Unwrap() != category || backendErr.Error() != category.Error() { //nolint:errorlint // BackendError accessors preserve exact declared identity.
			t.Fatalf("NewBackendError(%v) = %v, %v", category, backendErr, err)
		}
	}
	for _, invalid := range []error{nil, errors.New("unsafe"), fmt.Errorf("wrapped: %w", ratelimit.ErrRejected), errors.Join(ratelimit.ErrRejected)} {
		if value, err := ratelimit.NewBackendError(invalid); value != nil || err == nil || !errors.Is(err, ratelimit.ErrInvalidPolicy) ||
			err.Error() != "invalid rate limit policy: backend error category is not safe" {
			t.Fatalf("NewBackendError(invalid) = %v, %v", value, err)
		}
	}
	var backendErr *ratelimit.BackendError
	if backendErr.Error() != "rate limit backend error" || backendErr.Unwrap() != nil || backendErr.Category() != nil {
		t.Fatal("nil BackendError is not total")
	}
	var item *ratelimit.BatchItemError
	if item.Error() != "item error" || item.Unwrap() != nil || item.Index() != -1 {
		t.Fatal("nil BatchItemError is not total")
	}
	var batchErr *ratelimit.StrictBatchError
	if batchErr.Error() != "batch error" || batchErr.Unwrap() != nil || batchErr.Items() != nil {
		t.Fatal("nil StrictBatchError is not total")
	}
}

func TestStrictBatchPreservesAttemptedIndexesAndStructuredFailures(t *testing.T) {
	t.Parallel()

	requests := []ratelimit.Request{
		validRequest(t, ratelimit.FailClosed), validRequest(t, ratelimit.FailClosed), validRequest(t, ratelimit.FailClosed),
	}
	calls := 0
	service, err := ratelimit.NewStrictService(&countingStrictBackend{name: "batch", admit: func(callCtx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
		calls++
		if calls == 2 {
			return ratelimit.Decision{}, ratelimit.ErrUnavailable
		}
		return completeAllowedAdmission(callCtx, request)
	}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Batch(context.Background(), ratelimit.BatchRequest{Requests: requests, Atomicity: ratelimit.AtomicityPerItem})
	var aggregate *ratelimit.StrictBatchError
	if !errors.As(err, &aggregate) || calls != 3 || fmt.Sprint(result.Attempted) != "[0 1 2]" || len(result.Decisions) != 3 {
		t.Fatalf("Batch() = %+v, %v, calls=%d", result, err, calls)
	}
	items := aggregate.Items()
	if len(items) != 1 || items[0].Index() != 1 || !errors.Is(items[0], ratelimit.ErrUnavailable) ||
		!errors.Is(aggregate, ratelimit.ErrUnavailable) || aggregate.Error() != "item 1: rate limit backend unavailable" {
		t.Fatalf("aggregate = %#v / %v", items, aggregate)
	}
	items[0] = nil
	if aggregate.Items()[0] == nil {
		t.Fatal("Items returned mutable storage")
	}
}

func TestStrictBatchStopsBeforeDispatchAndLeavesUnattemptedDecisionsZero(t *testing.T) {
	requests := []ratelimit.Request{validRequest(t, ratelimit.FailClosed), validRequest(t, ratelimit.FailClosed)}
	calls := 0
	service, _ := ratelimit.NewStrictService(&countingStrictBackend{name: "batch", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
		calls++
		return ratelimit.Decision{}, nil
	}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := service.Batch(ctx, ratelimit.BatchRequest{Requests: requests, Atomicity: ratelimit.AtomicityPerItem})
	var aggregate *ratelimit.StrictBatchError
	if !errors.As(err, &aggregate) || calls != 0 || len(result.Decisions) != 2 || result.Decisions[0] != (ratelimit.Decision{}) || result.Decisions[1] != (ratelimit.Decision{}) || len(result.Attempted) != 0 {
		t.Fatalf("Batch()=%+v,%v,calls=%d", result, err, calls)
	}
	if items := aggregate.Items(); len(items) != 1 || items[0].Index() != 0 || !errors.Is(items[0], ratelimit.ErrCanceled) {
		t.Fatalf("items=%v", items)
	}
}

func TestStrictBatchRecordsNextCanceledIndexAfterKnownResult(t *testing.T) {
	requests := []ratelimit.Request{validRequest(t, ratelimit.FailClosed), validRequest(t, ratelimit.FailClosed)}
	ctx, cancel := context.WithCancel(context.Background())
	service, _ := ratelimit.NewStrictService(&countingStrictBackend{name: "batch", admit: func(callCtx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
		cancel()
		return completeAllowedAdmission(callCtx, request)
	}})
	result, err := service.Batch(ctx, ratelimit.BatchRequest{Requests: requests, Atomicity: ratelimit.AtomicityPerItem})
	var aggregate *ratelimit.StrictBatchError
	if !errors.As(err, &aggregate) || fmt.Sprint(result.Attempted) != "[0]" || !result.Decisions[0].Allowed || result.Decisions[1] != (ratelimit.Decision{}) {
		t.Fatalf("Batch()=%+v,%v", result, err)
	}
	if items := aggregate.Items(); len(items) != 1 || items[0].Index() != 1 || !errors.Is(items[0], ratelimit.ErrCanceled) {
		t.Fatalf("items=%v", items)
	}
}

type cancellationRaceContext struct{ calls int }

func (*cancellationRaceContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*cancellationRaceContext) Done() <-chan struct{}       { return nil }
func (ctx *cancellationRaceContext) Err() error {
	ctx.calls++
	if ctx.calls > 1 {
		return context.Canceled
	}
	return nil
}
func (*cancellationRaceContext) Value(any) any { return nil }

type deadlineRaceContext struct{ calls int }

func (*deadlineRaceContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*deadlineRaceContext) Done() <-chan struct{}       { return nil }
func (ctx *deadlineRaceContext) Err() error {
	ctx.calls++
	if ctx.calls > 1 {
		return context.DeadlineExceeded
	}
	return nil
}
func (*deadlineRaceContext) Value(any) any { return nil }

func TestStrictBatchCancellationRaceBeforeDispatchLeavesCurrentDecisionZero(t *testing.T) {
	requests := []ratelimit.Request{validRequest(t, ratelimit.FailClosed), validRequest(t, ratelimit.FailClosed)}
	calls := 0
	service, _ := ratelimit.NewStrictService(&countingStrictBackend{name: "batch", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
		calls++
		return ratelimit.Decision{}, nil
	}})
	result, err := service.Batch(&cancellationRaceContext{}, ratelimit.BatchRequest{Requests: requests, Atomicity: ratelimit.AtomicityPerItem})
	var aggregate *ratelimit.StrictBatchError
	if !errors.As(err, &aggregate) || calls != 0 || len(result.Attempted) != 0 || result.Decisions[0] != (ratelimit.Decision{}) || result.Decisions[1] != (ratelimit.Decision{}) {
		t.Fatalf("Batch()=%+v,%v,calls=%d", result, err, calls)
	}
	if items := aggregate.Items(); len(items) != 1 || items[0].Index() != 0 || !errors.Is(items[0], ratelimit.ErrCanceled) {
		t.Fatalf("items=%v", items)
	}
}

func TestStrictBatchDeadlineRaceLeavesFirstUnattemptedDecisionZero(t *testing.T) {
	requests := []ratelimit.Request{validRequest(t, ratelimit.FailOpen), validRequest(t, ratelimit.FailOpen)}
	calls := 0
	var observations atomic.Int64
	service, _ := ratelimit.NewStrictService(&countingStrictBackend{name: "batch", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
		calls++
		return ratelimit.Decision{}, nil
	}}, ratelimit.ObserveFunc(func(ratelimit.Observation) { observations.Add(1) }))
	result, err := service.Batch(&deadlineRaceContext{}, ratelimit.BatchRequest{Requests: requests, Atomicity: ratelimit.AtomicityPerItem})
	var aggregate *ratelimit.StrictBatchError
	if !errors.As(err, &aggregate) || calls != 0 || observations.Load() != 0 || len(result.Attempted) != 0 || len(result.Decisions) != 2 {
		t.Fatalf("Batch()=%+v,%v,calls=%d", result, err, calls)
	}
	if result.Decisions[0] != (ratelimit.Decision{}) || result.Decisions[1] != (ratelimit.Decision{}) {
		t.Fatalf("decisions=%+v", result.Decisions)
	}
	if items := aggregate.Items(); len(items) != 1 || items[0].Index() != 0 || !errors.Is(items[0], ratelimit.ErrDeadline) {
		t.Fatalf("items=%v", items)
	}
}

func TestStrictBatchAmbiguousCancellationStopsAtAttemptedItem(t *testing.T) {
	requests := []ratelimit.Request{validRequest(t, ratelimit.FailClosed), validRequest(t, ratelimit.FailClosed)}
	ctx, cancel := context.WithCancel(context.Background())
	service, _ := ratelimit.NewStrictService(&countingStrictBackend{name: "batch", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
		cancel()
		return ratelimit.Decision{}, errors.New("ambiguous")
	}})
	result, err := service.Batch(ctx, ratelimit.BatchRequest{Requests: requests, Atomicity: ratelimit.AtomicityPerItem})
	var aggregate *ratelimit.StrictBatchError
	if !errors.As(err, &aggregate) || fmt.Sprint(result.Attempted) != "[0]" || result.Decisions[0].Reason != ratelimit.ReasonOutcomeUnknown || result.Decisions[1] != (ratelimit.Decision{}) {
		t.Fatalf("Batch()=%+v,%v", result, err)
	}
	items := aggregate.Items()
	if len(items) != 1 || items[0].Index() != 0 || !errors.Is(items[0], ratelimit.ErrOutcomeUnknown) || !errors.Is(items[0], ratelimit.ErrCanceled) {
		t.Fatalf("items=%v", items)
	}
}

func TestStrictBatchValidationAndAllSuccess(t *testing.T) {
	service, _ := ratelimit.NewStrictService(&countingStrictBackend{name: "batch", admit: completeAllowedAdmission})
	for _, batch := range []ratelimit.BatchRequest{{}, {Requests: make([]ratelimit.Request, ratelimit.MaxBatchSize+1), Atomicity: ratelimit.AtomicityPerItem}, {Requests: []ratelimit.Request{validRequest(t, ratelimit.FailClosed)}, Atomicity: ratelimit.AtomicityAllOrNothing}, {Requests: []ratelimit.Request{validRequest(t, ratelimit.FailClosed)}, Atomicity: ratelimit.Atomicity("unknown")}, {Requests: []ratelimit.Request{{}}, Atomicity: ratelimit.AtomicityPerItem}} {
		if result, err := service.Batch(context.Background(), batch); err == nil || result.Decisions != nil {
			t.Fatalf("invalid Batch()=%+v,%v", result, err)
		}
	}
	request := validRequest(t, ratelimit.FailClosed)
	result, err := service.Batch(context.Background(), ratelimit.BatchRequest{Requests: []ratelimit.Request{request}, Atomicity: ratelimit.AtomicityPerItem})
	if err != nil || fmt.Sprint(result.Attempted) != "[0]" || !result.Decisions[0].Allowed {
		t.Fatalf("success=%+v,%v", result, err)
	}
	var absent *ratelimit.StrictService
	if _, err := absent.Batch(context.Background(), ratelimit.BatchRequest{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil service=%v", err)
	}
	var nilCtx *nilContext
	if _, err := service.Batch(nilCtx, ratelimit.BatchRequest{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("nil context=%v", err)
	}
}

func completeAllowedAdmission(_ context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	return ratelimit.Decision{
		Allowed: true, Limit: request.Policy.Limit(), Remaining: request.Policy.Limit() - request.Cost,
		Reset: request.Now.Add(time.Second), Reason: ratelimit.ReasonAllowed,
	}, nil
}

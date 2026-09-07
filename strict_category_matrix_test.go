package ratelimit_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

type categoryMatrixBackend struct {
	admit   func(context.Context, ratelimit.Request) (ratelimit.Decision, error)
	acquire func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error)
	release func(context.Context, ratelimit.Lease) error
}

func (*categoryMatrixBackend) Name() string { return "matrix" }
func (*categoryMatrixBackend) Admit(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
	panic("legacy backend invoked")
}
func (backend *categoryMatrixBackend) AdmitStrict(ctx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	return backend.admit(ctx, request)
}
func (backend *categoryMatrixBackend) AcquireStrict(ctx context.Context, request ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
	return backend.acquire(ctx, request)
}
func (backend *categoryMatrixBackend) ReleaseStrict(ctx context.Context, lease ratelimit.Lease) error {
	return backend.release(ctx, lease)
}

type unsafeMatchingError struct{}

func (unsafeMatchingError) Error() string { return "sensitive matching error" }
func (unsafeMatchingError) Is(error) bool { return true }
func (unsafeMatchingError) As(any) bool   { return true }

type panicErrorMethod struct{}

func (panicErrorMethod) Error() string { panic("hostile Error invoked") }

type panicIsMethod struct{}

func (panicIsMethod) Error() string { return "hostile-is-marker" }
func (panicIsMethod) Is(error) bool { panic("hostile Is invoked") }

type panicAsMethod struct{}

func (panicAsMethod) Error() string { return "hostile-as-marker" }
func (panicAsMethod) As(any) bool   { panic("hostile As invoked") }

type transitionContext struct{ err error }

func (*transitionContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*transitionContext) Done() <-chan struct{}       { return nil }
func (ctx *transitionContext) Err() error              { return ctx.err }
func (*transitionContext) Value(any) any               { return nil }

func TestStrictServiceCompleteCategoryApplicabilityMatrix(t *testing.T) {
	categories := []struct {
		name      string
		category  error
		admit     bool
		acquire   bool
		release   bool
		rejection bool
	}{
		{name: "rejected", category: ratelimit.ErrRejected, admit: true, acquire: true, rejection: true},
		{name: "unavailable", category: ratelimit.ErrUnavailable, admit: true, acquire: true, release: true},
		{name: "overflow", category: ratelimit.ErrOverflow, admit: true, acquire: true},
		{name: "corrupt", category: ratelimit.ErrCorrupt, admit: true, acquire: true, release: true},
		{name: "unsupported", category: ratelimit.ErrUnsupported, admit: true},
		{name: "lease-not-found", category: ratelimit.ErrLeaseNotFound, release: true},
		{name: "lease-not-owned", category: ratelimit.ErrLeaseNotOwned, acquire: true, release: true},
	}
	for _, category := range categories {
		for _, carrier := range []struct {
			name string
			wrap func(error) error
		}{
			{name: "sentinel", wrap: func(err error) error { return err }},
			{name: "backend-error", wrap: func(err error) error {
				backendErr, constructionErr := ratelimit.NewBackendError(err)
				if constructionErr != nil {
					t.Fatal(constructionErr)
				}
				return backendErr
			}},
		} {
			t.Run(category.name+"/"+carrier.name, func(t *testing.T) {
				declared := carrier.wrap(category.category)
				t.Run("admit", func(t *testing.T) {
					request := validRequest(t, ratelimit.FailClosed)
					raw := ratelimit.Decision{}
					if category.rejection {
						raw = completeRejectedDecision(request)
					}
					observed := make(chan ratelimit.Observation, 2)
					service := matrixService(t, &categoryMatrixBackend{
						admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) { return raw, declared },
						acquire: func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
							return ratelimit.Lease{}, ratelimit.Decision{}, nil
						},
						release: func(context.Context, ratelimit.Lease) error { return nil },
					}, observed)
					decision, err := service.Admit(context.Background(), request)
					assertCategoryResult(t, err, declared, category.category, category.admit)
					if category.admit && category.rejection && decision.Reason != ratelimit.ReasonLimited {
						t.Fatalf("admit rejection decision = %+v", decision)
					}
					if category.admit && !category.rejection && decision.Reason != ratelimit.ReasonBackendUnavailable {
						t.Fatalf("admit known failure decision = %+v", decision)
					}
					if !category.admit && decision.Reason != ratelimit.ReasonOutcomeUnknown {
						t.Fatalf("admit inapplicable decision = %+v", decision)
					}
					assertOneObservation(t, observed, decision, err)
				})

				t.Run("acquire", func(t *testing.T) {
					request := ratelimit.LeaseRequest{Request: concurrencyRequest(t), LeaseID: "lease"}
					raw := ratelimit.Decision{}
					if category.rejection {
						raw = completeRejectedDecision(request.Request)
					}
					observed := make(chan ratelimit.Observation, 2)
					service := matrixService(t, &categoryMatrixBackend{
						admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) { return ratelimit.Decision{}, nil },
						acquire: func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
							return ratelimit.Lease{}, raw, declared
						},
						release: func(context.Context, ratelimit.Lease) error { return nil },
					}, observed)
					lease, decision, err := service.Acquire(context.Background(), request)
					assertCategoryResult(t, err, declared, category.category, category.acquire)
					if lease != (ratelimit.Lease{}) {
						t.Fatalf("acquire failure lease = %+v", lease)
					}
					if category.acquire && category.rejection && decision.Reason != ratelimit.ReasonLimited {
						t.Fatalf("acquire rejection decision = %+v", decision)
					}
					if category.acquire && !category.rejection && decision.Reason != ratelimit.ReasonBackendUnavailable {
						t.Fatalf("acquire known failure decision = %+v", decision)
					}
					if !category.acquire && decision.Reason != ratelimit.ReasonOutcomeUnknown {
						t.Fatalf("acquire inapplicable decision = %+v", decision)
					}
					assertOneObservation(t, observed, decision, err)
				})

				t.Run("release", func(t *testing.T) {
					request := ratelimit.LeaseRequest{Request: concurrencyRequest(t), LeaseID: "lease"}
					lease := completeMatrixLease(request)
					observed := make(chan ratelimit.Observation, 1)
					service := matrixService(t, &categoryMatrixBackend{
						admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) { return ratelimit.Decision{}, nil },
						acquire: func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
							return ratelimit.Lease{}, ratelimit.Decision{}, nil
						},
						release: func(context.Context, ratelimit.Lease) error { return declared },
					}, observed)
					err := service.Release(context.Background(), lease)
					assertCategoryResult(t, err, declared, category.category, category.release)
					if len(observed) != 0 {
						t.Fatal("Release emitted an observation")
					}
				})
			})
		}
	}
}

func TestStrictServiceRejectsUnsafeErrorsAcrossReachableMethods(t *testing.T) {
	unsafeErrors := []error{
		errors.New(strings.Repeat("hostile-marker", 1_048_576/len("hostile-marker")+1)[:1_048_576]),
		fmt.Errorf("wrapped secret: %w", ratelimit.ErrRejected),
		fmt.Errorf("wrapped secret: %w", ratelimit.ErrUnavailable),
		fmt.Errorf("wrapped secret: %w", ratelimit.ErrOverflow),
		fmt.Errorf("wrapped secret: %w", ratelimit.ErrCorrupt),
		fmt.Errorf("wrapped secret: %w", ratelimit.ErrUnsupported),
		fmt.Errorf("wrapped secret: %w", ratelimit.ErrLeaseNotFound),
		fmt.Errorf("wrapped secret: %w", ratelimit.ErrLeaseNotOwned),
		errors.Join(ratelimit.ErrUnavailable),
		errors.Join(ratelimit.ErrUnavailable, ratelimit.ErrCorrupt),
		unsafeMatchingError{},
		panicErrorMethod{},
		panicIsMethod{},
		panicAsMethod{},
		errors.New("arbitrary secret"),
		context.Canceled,
		context.DeadlineExceeded,
		new(ratelimit.BackendError),
		(*ratelimit.BackendError)(nil),
	}
	for index, unsafeErr := range unsafeErrors {
		t.Run(fmt.Sprintf("case-%d", index), func(t *testing.T) {
			leaseRequest := ratelimit.LeaseRequest{Request: concurrencyRequest(t), LeaseID: "lease"}
			lease := completeMatrixLease(leaseRequest)
			observed := make(chan ratelimit.Observation, 16)
			backend := &categoryMatrixBackend{
				admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
					return ratelimit.Decision{Allowed: true, Limit: 99, Reason: ratelimit.ReasonAllowed}, unsafeErr
				},
				acquire: func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
					return lease, ratelimit.Decision{Allowed: true, Limit: 99, Reason: ratelimit.ReasonAllowed}, unsafeErr
				},
				release: func(context.Context, ratelimit.Lease) error { return unsafeErr },
			}
			service := matrixService(t, backend, observed)

			admissionRequests := []ratelimit.Request{
				validRequest(t, ratelimit.FailClosed),
				validRequest(t, ratelimit.FailOpen),
				concurrencyRequest(t),
			}
			for _, admissionRequest := range admissionRequests {
				decision, err := service.Admit(context.Background(), admissionRequest)
				assertFreshUnknown(t, decision, err, unsafeErr)
				assertOneObservation(t, observed, decision, err)
				secondDecision, secondErr := service.Admit(context.Background(), admissionRequest)
				assertFreshUnknown(t, secondDecision, secondErr, unsafeErr)
				if err == secondErr { //nolint:errorlint // Each unsafe result must receive a fresh wrapper identity.
					t.Fatal("unsafe Admit reused an unknown wrapper")
				}
				assertOneObservation(t, observed, secondDecision, secondErr)
			}

			batch, batchErr := service.Batch(context.Background(), ratelimit.BatchRequest{
				Requests: []ratelimit.Request{admissionRequests[0]}, Atomicity: ratelimit.AtomicityPerItem,
			})
			var aggregate *ratelimit.StrictBatchError
			if !errors.As(batchErr, &aggregate) || !errors.Is(batchErr, ratelimit.ErrOutcomeUnknown) ||
				len(batch.Attempted) != 1 || batch.Attempted[0] != 0 || len(batch.Decisions) != 1 ||
				batch.Decisions[0].Reason != ratelimit.ReasonOutcomeUnknown || batchErr.Error() != "item 0: rate limit outcome unknown" ||
				len(batchErr.Error()) > ratelimit.MaxStrictBatchErrorBytes || strings.Contains(batchErr.Error(), "hostile") {
				t.Fatal("unsafe Batch result did not preserve its bounded structured contract")
			}
			items := aggregate.Items()
			if len(items) != 1 || items[0].Index() != 0 || !errors.Is(items[0], ratelimit.ErrOutcomeUnknown) {
				t.Fatal("unsafe Batch item identity was not preserved")
			}
			assertOneObservation(t, observed, batch.Decisions[0], items[0].Unwrap())

			gotLease, leaseDecision, acquireErr := service.Acquire(context.Background(), leaseRequest)
			if gotLease != (ratelimit.Lease{}) {
				t.Fatalf("unsafe Acquire lease = %+v", gotLease)
			}
			assertFreshUnknown(t, leaseDecision, acquireErr, unsafeErr)
			assertOneObservation(t, observed, leaseDecision, acquireErr)

			if releaseErr := service.Release(context.Background(), lease); !freshUnknownError(releaseErr, unsafeErr) {
				t.Fatalf("unsafe Release was not normalized")
			}
			if len(observed) != 0 {
				t.Fatal("Release emitted an observation")
			}
		})
	}
}

func TestUnsafeBackendErrorsUseOnlyCallerContextForRaceCategory(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want error
		not  error
	}{
		{name: "canceled", err: context.Canceled, want: ratelimit.ErrCanceled},
		{name: "deadline", err: context.DeadlineExceeded, want: ratelimit.ErrDeadline, not: ratelimit.ErrCanceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := &transitionContext{}
			unsafeErr := errors.New("hostile race marker")
			observed := make(chan ratelimit.Observation, 2)
			backend := &categoryMatrixBackend{
				admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
					ctx.err = test.err
					return ratelimit.Decision{}, unsafeErr
				},
				acquire: func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
					ctx.err = test.err
					return ratelimit.Lease{}, ratelimit.Decision{}, unsafeErr
				},
				release: func(context.Context, ratelimit.Lease) error {
					ctx.err = test.err
					return unsafeErr
				},
			}
			service := matrixService(t, backend, observed)

			decision, err := service.Admit(ctx, validRequest(t, ratelimit.FailOpen))
			assertContextUnknown(t, decision, err, test.want, test.not)
			assertOneObservation(t, observed, decision, err)

			ctx.err = nil
			request := ratelimit.LeaseRequest{Request: concurrencyRequest(t), LeaseID: "lease"}
			lease, decision, err := service.Acquire(ctx, request)
			if lease != (ratelimit.Lease{}) {
				t.Fatalf("race Acquire lease = %+v", lease)
			}
			assertContextUnknown(t, decision, err, test.want, test.not)
			assertOneObservation(t, observed, decision, err)

			ctx.err = nil
			err = service.Release(ctx, completeMatrixLease(request))
			if !errors.Is(err, ratelimit.ErrOutcomeUnknown) || !errors.Is(err, test.want) ||
				test.not != nil && errors.Is(err, test.not) || strings.Contains(err.Error(), "hostile") {
				t.Fatal("release race did not use only the caller context category")
			}
			if len(observed) != 0 {
				t.Fatal("Release emitted an observation")
			}
		})
	}
}

func matrixService(t *testing.T, backend *categoryMatrixBackend, observed chan<- ratelimit.Observation) *ratelimit.StrictService {
	t.Helper()
	service, err := ratelimit.NewStrictService(backend, ratelimit.ObserveFunc(func(observation ratelimit.Observation) {
		observed <- observation
	}))
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func completeRejectedDecision(request ratelimit.Request) ratelimit.Decision {
	return ratelimit.Decision{
		Allowed: false, Limit: request.Policy.Limit(), Remaining: 0,
		Reset: request.Now.Add(time.Second), RetryAfter: time.Second, Reason: ratelimit.ReasonLimited,
	}
}

func completeMatrixLease(request ratelimit.LeaseRequest) ratelimit.Lease {
	return ratelimit.Lease{
		ID: request.LeaseID, Key: request.Request.Key,
		PolicyID: request.Request.Policy.ID(), PolicyRevision: request.Request.Policy.Revision(),
		Cost: request.Request.Cost, ExpiresAt: request.Request.Now.Add(time.Second), Backend: "matrix",
	}
}

func assertCategoryResult(t *testing.T, got, declared, category error, applicable bool) {
	t.Helper()
	if applicable {
		if got != declared { //nolint:errorlint // Applicable direct categories preserve exact identity.
			t.Fatalf("known category did not preserve direct identity")
		}
		return
	}
	if !errors.Is(got, ratelimit.ErrOutcomeUnknown) || errors.Is(got, category) || got == declared || got.Error() != "rate limit outcome unknown" { //nolint:errorlint // Inapplicable declarations must not preserve identity.
		t.Fatalf("inapplicable category was not replaced by fresh unknown")
	}
}

func assertUnknownResult(t *testing.T, decision ratelimit.Decision, err error) {
	t.Helper()
	if !errors.Is(err, ratelimit.ErrOutcomeUnknown) || decision.Allowed ||
		decision.Reason != ratelimit.ReasonOutcomeUnknown || len(err.Error()) > ratelimit.MaxBackendErrorBytes ||
		strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "hostile") {
		t.Fatalf("unsafe backend result was not bounded and fail closed")
	}
	assertNoUnsafeTypes(t, err)
}

func assertFreshUnknown(t *testing.T, decision ratelimit.Decision, err, unsafeErr error) {
	t.Helper()
	assertUnknownResult(t, decision, err)
	if !freshUnknownError(err, unsafeErr) {
		t.Fatal("unsafe backend identity crossed the strict boundary")
	}
	if unsafeErr == context.Canceled || unsafeErr == context.DeadlineExceeded { //nolint:errorlint // Only direct raw context sentinels select this assertion branch.
		if errors.Is(err, ratelimit.ErrCanceled) || errors.Is(err, ratelimit.ErrDeadline) {
			t.Fatal("raw backend context selected a nested category while caller context was active")
		}
	}
}

func freshUnknownError(err, unsafeErr error) bool {
	return errors.Is(err, ratelimit.ErrOutcomeUnknown) && err != unsafeErr && //nolint:errorlint // Unsafe backend identity must not cross the boundary.
		!errors.Is(err, ratelimit.ErrUnavailable) && !errors.Is(err, ratelimit.ErrRejected) &&
		!errors.Is(err, ratelimit.ErrOverflow) && !errors.Is(err, ratelimit.ErrCorrupt) &&
		!errors.Is(err, ratelimit.ErrUnsupported) && !errors.Is(err, ratelimit.ErrLeaseNotFound) &&
		!errors.Is(err, ratelimit.ErrLeaseNotOwned) && err.Error() == "rate limit outcome unknown"
}

func assertNoUnsafeTypes(t *testing.T, err error) {
	t.Helper()
	var matching unsafeMatchingError
	var panicError panicErrorMethod
	var panicIs panicIsMethod
	var panicAs panicAsMethod
	var hostile hostileStrictError
	if errors.As(err, &matching) || errors.As(err, &panicError) || errors.As(err, &panicIs) ||
		errors.As(err, &panicAs) || errors.As(err, &hostile) {
		t.Fatal("unsafe backend type crossed the strict boundary")
	}
}

func assertContextUnknown(t *testing.T, decision ratelimit.Decision, err, want, not error) {
	t.Helper()
	if !errors.Is(err, ratelimit.ErrOutcomeUnknown) || !errors.Is(err, want) ||
		not != nil && errors.Is(err, not) || decision.Allowed ||
		decision.Reason != ratelimit.ReasonOutcomeUnknown || strings.Contains(err.Error(), "hostile") {
		t.Fatal("unsafe race did not use only the caller context category")
	}
}

func assertOneObservation(t *testing.T, observed <-chan ratelimit.Observation, decision ratelimit.Decision, err error) {
	t.Helper()
	observation := <-observed
	if observation.Decision != decision || observation.Err != err || len(observed) != 0 { //nolint:errorlint // Observation must preserve exact result identity.
		t.Fatalf("observation did not preserve exact normalized result identity")
	}
}

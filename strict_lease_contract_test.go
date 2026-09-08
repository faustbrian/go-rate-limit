package ratelimit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

type strictLeaseBackendFake struct {
	strictBackendFunc
	acquire      func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error)
	release      func(context.Context, ratelimit.Lease) error
	acquireCalls int
	releaseCalls int
}

func (backend *strictLeaseBackendFake) AcquireStrict(ctx context.Context, request ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
	backend.acquireCalls++
	return backend.acquire(ctx, request)
}

func (backend *strictLeaseBackendFake) ReleaseStrict(ctx context.Context, lease ratelimit.Lease) error {
	backend.releaseCalls++
	return backend.release(ctx, lease)
}

func TestStrictReleaseRejectsForeignBackendBeforeDispatch(t *testing.T) {
	t.Parallel()

	backend := &strictLeaseBackendFake{
		strictBackendFunc: strictBackendFunc{name: "owner", admit: completeAllowedAdmission},
		acquire: func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
			return ratelimit.Lease{}, ratelimit.Decision{}, nil
		},
		release: func(context.Context, ratelimit.Lease) error { return nil },
	}
	service, err := ratelimit.NewStrictService(backend)
	if err != nil {
		t.Fatal(err)
	}
	request := concurrencyRequest(t)
	lease := ratelimit.Lease{
		ID: "lease", Key: request.Key, PolicyID: request.Policy.ID(),
		PolicyRevision: request.Policy.Revision(), Cost: request.Cost,
		ExpiresAt: request.Now.Add(time.Second), Backend: "foreign",
	}
	err = service.Release(context.Background(), lease)
	if !errors.Is(err, ratelimit.ErrLeaseNotOwned) || backend.releaseCalls != 0 {
		t.Fatalf("Release(foreign) = %v, calls=%d", err, backend.releaseCalls)
	}
}

func TestStrictLeaseKnownResultsWinCallerCancellationRace(t *testing.T) {
	request := ratelimit.LeaseRequest{Request: concurrencyRequest(t), LeaseID: "lease"}
	reset := request.Request.Now.Add(time.Second)
	allowed := ratelimit.Decision{
		Allowed: true, Limit: request.Request.Policy.Limit(),
		Remaining: request.Request.Policy.Limit() - request.Request.Cost,
		Reset:     reset, Reason: ratelimit.ReasonAllowed,
	}
	lease := ratelimit.Lease{
		ID: request.LeaseID, Key: request.Request.Key,
		PolicyID: request.Request.Policy.ID(), PolicyRevision: request.Request.Policy.Revision(),
		Cost: request.Request.Cost, ExpiresAt: reset,
	}
	ctx, cancel := context.WithCancel(context.Background())
	backend := &strictLeaseBackendFake{
		strictBackendFunc: strictBackendFunc{name: "lease-backend", admit: completeAllowedAdmission},
		acquire: func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
			cancel()
			return lease, allowed, nil
		},
		release: func(context.Context, ratelimit.Lease) error { return nil },
	}
	service, _ := ratelimit.NewStrictService(backend)
	gotLease, decision, err := service.Acquire(ctx, request)
	if err != nil || gotLease.Backend != "lease-backend" || !decision.Allowed || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("Acquire cancellation race = %+v, %+v, %v", gotLease, decision, err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	rejected := ratelimit.Decision{
		Limit: request.Request.Policy.Limit(), Reset: reset,
		RetryAfter: time.Second, Reason: ratelimit.ReasonLimited,
	}
	backend = &strictLeaseBackendFake{
		strictBackendFunc: strictBackendFunc{name: "lease-backend", admit: completeAllowedAdmission},
		acquire: func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
			cancel()
			return ratelimit.Lease{}, rejected, ratelimit.ErrRejected
		},
		release: func(context.Context, ratelimit.Lease) error { return nil },
	}
	service, _ = ratelimit.NewStrictService(backend)
	gotLease, decision, err = service.Acquire(ctx, request)
	if gotLease != (ratelimit.Lease{}) || !errors.Is(err, ratelimit.ErrRejected) || decision.Reason != ratelimit.ReasonLimited || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("Acquire rejection race = %+v, %+v, %v", gotLease, decision, err)
	}

	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "known terminal", err: ratelimit.ErrLeaseNotFound},
	} {
		t.Run("release "+test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			backend := &strictLeaseBackendFake{
				strictBackendFunc: strictBackendFunc{name: "lease-backend", admit: completeAllowedAdmission},
				acquire: func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
					return ratelimit.Lease{}, ratelimit.Decision{}, nil
				},
				release: func(context.Context, ratelimit.Lease) error { cancel(); return test.err },
			}
			service, _ := ratelimit.NewStrictService(backend)
			owned := lease
			owned.Backend = "lease-backend"
			err := service.Release(ctx, owned)
			if err != test.err || !errors.Is(ctx.Err(), context.Canceled) { //nolint:errorlint // Exact known result identity wins the racing cancellation.
				t.Fatalf("Release cancellation race = %v, want %v", err, test.err)
			}
		})
	}
}

func TestStrictAcquireValidatesCompleteLeaseAndCategoryApplicability(t *testing.T) {
	t.Parallel()

	request := ratelimit.LeaseRequest{Request: concurrencyRequest(t), LeaseID: "lease"}
	completeDecision := ratelimit.Decision{
		Allowed: true, Limit: request.Request.Policy.Limit(),
		Remaining: request.Request.Policy.Limit() - request.Request.Cost,
		Reset:     request.Request.Now.Add(time.Second), Reason: ratelimit.ReasonAllowed,
	}
	completeLease := ratelimit.Lease{
		ID: request.LeaseID, Key: request.Request.Key,
		PolicyID: request.Request.Policy.ID(), PolicyRevision: request.Request.Policy.Revision(),
		Cost: request.Request.Cost, ExpiresAt: completeDecision.Reset, Backend: "untrusted",
	}
	tests := []struct {
		name       string
		lease      ratelimit.Lease
		decision   ratelimit.Decision
		err        error
		wantErr    error
		wantReason ratelimit.Reason
	}{
		{"success", completeLease, completeDecision, nil, nil, ratelimit.ReasonAllowed},
		{"incomplete-success", ratelimit.Lease{}, completeDecision, nil, ratelimit.ErrOutcomeUnknown, ratelimit.ReasonOutcomeUnknown},
		{"incomplete-rejection", ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrRejected, ratelimit.ErrOutcomeUnknown, ratelimit.ReasonOutcomeUnknown},
		{"unavailable", ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrUnavailable, ratelimit.ErrUnavailable, ratelimit.ReasonBackendUnavailable},
		{"overflow", ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrOverflow, ratelimit.ErrOverflow, ratelimit.ReasonBackendUnavailable},
		{"corrupt", ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrCorrupt, ratelimit.ErrCorrupt, ratelimit.ReasonBackendUnavailable},
		{"not-owned", ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrLeaseNotOwned, ratelimit.ErrLeaseNotOwned, ratelimit.ReasonBackendUnavailable},
		{"unsupported-inapplicable", ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrUnsupported, ratelimit.ErrOutcomeUnknown, ratelimit.ReasonOutcomeUnknown},
		{"not-found-inapplicable", ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrLeaseNotFound, ratelimit.ErrOutcomeUnknown, ratelimit.ReasonOutcomeUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &strictLeaseBackendFake{
				strictBackendFunc: strictBackendFunc{name: "lease-backend", admit: completeAllowedAdmission},
				acquire: func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
					return test.lease, test.decision, test.err
				},
				release: func(context.Context, ratelimit.Lease) error { return nil },
			}
			service, err := ratelimit.NewStrictService(backend)
			if err != nil {
				t.Fatal(err)
			}
			lease, decision, err := service.Acquire(context.Background(), request)
			if test.wantErr == nil && err != nil {
				t.Fatalf("Acquire() error = %v", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("Acquire() error = %v, want %v", err, test.wantErr)
			}
			if decision.Reason != test.wantReason || decision.Backend != "lease-backend" {
				t.Fatalf("decision = %+v", decision)
			}
			if test.wantErr == nil && lease.Backend != "lease-backend" {
				t.Fatalf("lease backend = %q", lease.Backend)
			}
			if test.wantErr != nil && lease != (ratelimit.Lease{}) {
				t.Fatalf("error lease = %+v", lease)
			}
		})
	}
}

func TestStrictLeaseCapabilityAndReleaseCategoryMatrix(t *testing.T) {
	t.Parallel()

	observed := make(chan ratelimit.Observation, 1)
	admissionOnly, err := ratelimit.NewStrictService(
		strictBackendFunc{name: "admission", admit: completeAllowedAdmission},
		ratelimit.ObserveFunc(func(observation ratelimit.Observation) { observed <- observation }),
	)
	if err != nil {
		t.Fatal(err)
	}
	request := ratelimit.LeaseRequest{Request: concurrencyRequest(t), LeaseID: "lease"}
	leaseResult, decision, acquireErr := admissionOnly.Acquire(context.Background(), request)
	if leaseResult != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(acquireErr, ratelimit.ErrUnsupported) ||
		acquireErr.Error() != "rate limit operation unsupported: backend does not guarantee strict leases" {
		t.Fatalf("Acquire(admission only) = %+v, %+v, %v", leaseResult, decision, acquireErr)
	}
	if len(observed) != 0 {
		t.Fatal("Acquire pre-dispatch capability failure unexpectedly observed")
	}

	lease := ratelimit.Lease{
		ID: request.LeaseID, Key: request.Request.Key, PolicyID: request.Request.Policy.ID(),
		PolicyRevision: request.Request.Policy.Revision(), Cost: request.Request.Cost,
		ExpiresAt: request.Request.Now.Add(time.Second), Backend: "lease-backend",
	}
	admissionLease := lease
	admissionLease.Backend = "admission"
	if err := admissionOnly.Release(context.Background(), admissionLease); !errors.Is(err, ratelimit.ErrUnsupported) || err.Error() != "rate limit operation unsupported: backend does not guarantee strict leases" {
		t.Fatalf("Release(admission only)=%v", err)
	}
	if len(observed) != 0 {
		t.Fatal("Release capability failure unexpectedly observed")
	}
	for _, test := range []struct {
		name    string
		err     error
		wantErr error
	}{
		{"success", nil, nil},
		{"unavailable", ratelimit.ErrUnavailable, ratelimit.ErrUnavailable},
		{"corrupt", ratelimit.ErrCorrupt, ratelimit.ErrCorrupt},
		{"not-found", ratelimit.ErrLeaseNotFound, ratelimit.ErrLeaseNotFound},
		{"not-owned", ratelimit.ErrLeaseNotOwned, ratelimit.ErrLeaseNotOwned},
		{"rejected-inapplicable", ratelimit.ErrRejected, ratelimit.ErrOutcomeUnknown},
		{"overflow-inapplicable", ratelimit.ErrOverflow, ratelimit.ErrOutcomeUnknown},
		{"unsupported-inapplicable", ratelimit.ErrUnsupported, ratelimit.ErrOutcomeUnknown},
		{"unsafe", errors.New("unsafe"), ratelimit.ErrOutcomeUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &strictLeaseBackendFake{
				strictBackendFunc: strictBackendFunc{name: "lease-backend", admit: completeAllowedAdmission},
				acquire: func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
					return ratelimit.Lease{}, ratelimit.Decision{}, nil
				},
				release: func(context.Context, ratelimit.Lease) error { return test.err },
			}
			service, err := ratelimit.NewStrictService(backend)
			if err != nil {
				t.Fatal(err)
			}
			err = service.Release(context.Background(), lease)
			if test.wantErr == nil && err != nil {
				t.Fatalf("Release() error = %v", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("Release() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestStrictLeaseValidationContextAndDeclaredOutcomes(t *testing.T) {
	request := ratelimit.LeaseRequest{Request: concurrencyRequest(t), LeaseID: "lease"}
	validLease := ratelimit.Lease{ID: request.LeaseID, Key: request.Request.Key, PolicyID: request.Request.Policy.ID(), PolicyRevision: request.Request.Policy.Revision(), Cost: request.Request.Cost, ExpiresAt: request.Request.Now.Add(time.Second), Backend: "lease-backend"}
	var absent *ratelimit.StrictService
	if _, _, err := absent.Acquire(context.Background(), request); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil Acquire error=%v", err)
	}
	if err := absent.Release(context.Background(), validLease); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil Release error=%v", err)
	}
	backend := &strictLeaseBackendFake{strictBackendFunc: strictBackendFunc{name: "lease-backend", admit: completeAllowedAdmission}, acquire: func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.CanceledOutcome()
	}, release: func(context.Context, ratelimit.Lease) error { return ratelimit.DeadlineOutcome() }}
	service, _ := ratelimit.NewStrictService(backend)
	var nilCtx *nilContext
	if _, _, err := service.Acquire(nilCtx, request); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("nil context=%v", err)
	}
	if _, _, err := service.Acquire(context.Background(), ratelimit.LeaseRequest{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid request=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, decision, err := service.Acquire(ctx, request); !errors.Is(err, ratelimit.ErrCanceled) || decision.Reason != ratelimit.ReasonCanceled || backend.acquireCalls != 0 {
		t.Fatalf("canceled=%+v,%v,calls=%d", decision, err, backend.acquireCalls)
	}
	if _, decision, err := service.Acquire(context.Background(), request); !errors.Is(err, ratelimit.ErrCanceled) || decision.Reason != ratelimit.ReasonCanceled {
		t.Fatalf("declared canceled=%+v,%v", decision, err)
	}
	if err := service.Release(nilCtx, validLease); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("release nil context=%v", err)
	}
	if err := service.Release(context.Background(), ratelimit.Lease{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("release invalid=%v", err)
	}
	if err := service.Release(ctx, validLease); !errors.Is(err, ratelimit.ErrCanceled) || backend.releaseCalls != 0 {
		t.Fatalf("release canceled=%v calls=%d", err, backend.releaseCalls)
	}
	admissionOnly, _ := ratelimit.NewStrictService(strictBackendFunc{name: "admission", admit: completeAllowedAdmission})
	admissionLease := validLease
	admissionLease.Backend = "admission"
	if _, decision, err := admissionOnly.Acquire(ctx, request); !errors.Is(err, ratelimit.ErrCanceled) ||
		decision.Reason != ratelimit.ReasonCanceled {
		t.Fatalf("admission-only canceled acquire=%+v,%v", decision, err)
	}
	if err := admissionOnly.Release(ctx, admissionLease); !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("admission-only canceled release=%v", err)
	}
	expired, expire := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer expire()
	if _, decision, err := admissionOnly.Acquire(expired, request); !errors.Is(err, ratelimit.ErrDeadline) ||
		decision.Reason != ratelimit.ReasonDeadline {
		t.Fatalf("admission-only expired acquire=%+v,%v", decision, err)
	}
	if err := admissionOnly.Release(expired, admissionLease); !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("admission-only expired release=%v", err)
	}
	if err := service.Release(context.Background(), validLease); !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("declared deadline=%v", err)
	}
	backend.acquire = func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.DeadlineOutcome()
	}
	if _, decision, err := service.Acquire(context.Background(), request); !errors.Is(err, ratelimit.ErrDeadline) || decision.Reason != ratelimit.ReasonDeadline {
		t.Fatalf("declared deadline=%+v,%v", decision, err)
	}
	backend.acquire = func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.UnknownOutcome(context.Canceled)
	}
	if _, decision, err := service.Acquire(context.Background(), request); !errors.Is(err, ratelimit.ErrOutcomeUnknown) || !errors.Is(err, ratelimit.ErrCanceled) || decision.Reason != ratelimit.ReasonOutcomeUnknown {
		t.Fatalf("declared unknown=%+v,%v", decision, err)
	}
	backend.release = func(context.Context, ratelimit.Lease) error { return ratelimit.CanceledOutcome() }
	if err := service.Release(context.Background(), validLease); !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("release declared canceled=%v", err)
	}
	backend.release = func(context.Context, ratelimit.Lease) error {
		return ratelimit.UnknownOutcome(context.DeadlineExceeded)
	}
	if err := service.Release(context.Background(), validLease); !errors.Is(err, ratelimit.ErrOutcomeUnknown) || !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("release unknown=%v", err)
	}
}

func TestStrictAcquireCompleteRejection(t *testing.T) {
	request := ratelimit.LeaseRequest{Request: concurrencyRequest(t), LeaseID: "lease"}
	rejected := ratelimit.Decision{Allowed: false, Limit: request.Request.Policy.Limit(), Remaining: 0, Reset: request.Request.Now.Add(time.Second), RetryAfter: time.Second, Reason: ratelimit.ReasonLimited}
	backendErr, _ := ratelimit.NewBackendError(ratelimit.ErrRejected)
	backend := &strictLeaseBackendFake{strictBackendFunc: strictBackendFunc{name: "lease", admit: completeAllowedAdmission}, acquire: func(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
		return ratelimit.Lease{}, rejected, backendErr
	}, release: func(context.Context, ratelimit.Lease) error { return nil }}
	service, _ := ratelimit.NewStrictService(backend)
	lease, decision, err := service.Acquire(context.Background(), request)
	if lease != (ratelimit.Lease{}) || decision.Reason != ratelimit.ReasonLimited || err != backendErr { //nolint:errorlint // Direct BackendError identity is preserved.
		t.Fatalf("Acquire()=%+v,%+v,%v", lease, decision, err)
	}
}

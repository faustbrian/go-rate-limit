package ratelimit

import (
	"context"
	"fmt"
	"time"
)

// Acquire validates and obtains one strict concurrency lease.
func (service *StrictService) Acquire(ctx context.Context, request LeaseRequest) (Lease, Decision, error) {
	if service == nil || service.backend == nil || service.name == "" {
		return Lease{}, Decision{}, fmt.Errorf("%w: strict service is nil or uninitialized", ErrInvalidPolicy)
	}
	if isNilInterface(ctx) {
		return Lease{}, Decision{}, fmt.Errorf("%w: context is required", ErrInvalidRequest)
	}
	if err := request.Validate(); err != nil {
		return Lease{}, Decision{}, err
	}
	started := time.Now()
	if err := strictCallerContext(ctx); err != nil {
		decision := service.stampDecision(strictDenied(request.Request, contextReason(err)), request.Request)
		service.observe(request.Request, decision, err, started)
		return Lease{}, decision, err
	}
	backend, ok := service.backend.(StrictLeaseBackend)
	if !ok {
		return Lease{}, Decision{}, fmt.Errorf("%w: backend does not guarantee strict leases", ErrUnsupported)
	}
	rawLease, rawDecision, rawErr := backend.AcquireStrict(ctx, request)
	lease, decision, err := service.normalizeAcquire(ctx, request, rawLease, rawDecision, rawErr)
	service.observe(request.Request, decision, err, started)
	return lease, decision, err
}

func (service *StrictService) normalizeAcquire(ctx context.Context, request LeaseRequest, rawLease Lease, rawDecision Decision, rawErr error) (Lease, Decision, error) {
	declaration, category := strictDeclaration(rawErr)
	if rawErr == nil && completeAllowed(rawDecision, request.Request) && completeLease(rawLease, rawDecision, request) {
		rawLease.Backend = service.name
		return rawLease, service.stampDecision(rawDecision, request.Request), nil
	}
	if declaration == declarationSafe && directError(category, ErrRejected) && rawLease == (Lease{}) && completeRejected(rawDecision, request.Request) {
		return Lease{}, service.stampDecision(rawDecision, request.Request), rawErr
	}
	if declaration == declarationOutcome && rawLease == (Lease{}) && zeroDecision(rawDecision) {
		outcome := rawErr.(*strictOutcomeError) //nolint:errorlint // Declaration proved the exact direct strict outcome type.
		if outcome.kind == strictOutcomeUnknown {
			decision := service.stampDecision(strictDenied(request.Request, ReasonOutcomeUnknown), request.Request)
			return Lease{}, decision, reconstructUnknown(outcome)
		}
		decision := service.stampDecision(strictDenied(request.Request, contextReason(outcome)), request.Request)
		if outcome.kind == strictOutcomeCanceled {
			return Lease{}, decision, CanceledOutcome()
		}
		return Lease{}, decision, DeadlineOutcome()
	}
	if declaration == declarationSafe && !directError(category, ErrRejected) && applicableCategory(category, strictMethodAcquire) && rawLease == (Lease{}) && zeroDecision(rawDecision) {
		return Lease{}, service.stampDecision(strictDenied(request.Request, ReasonBackendUnavailable), request.Request), rawErr
	}
	decision, err := service.unknownDecision(ctx, request.Request)
	return Lease{}, decision, err
}

func completeLease(lease Lease, decision Decision, request LeaseRequest) bool {
	return lease.ID == request.LeaseID && lease.Key == request.Request.Key &&
		lease.PolicyID == request.Request.Policy.ID() && lease.PolicyRevision == request.Request.Policy.Revision() &&
		lease.Cost == request.Request.Cost && !lease.ExpiresAt.IsZero() &&
		lease.ExpiresAt.Equal(decision.Reset)
}

// Release relinquishes one strict lease without observing or retaining it.
func (service *StrictService) Release(ctx context.Context, lease Lease) error {
	if service == nil || service.backend == nil || service.name == "" {
		return fmt.Errorf("%w: strict service is nil or uninitialized", ErrInvalidPolicy)
	}
	if isNilInterface(ctx) {
		return fmt.Errorf("%w: context is required", ErrInvalidRequest)
	}
	if err := validateStrictLease(lease); err != nil {
		return err
	}
	if lease.Backend != "" && lease.Backend != service.name {
		return ErrLeaseNotOwned
	}
	if err := strictCallerContext(ctx); err != nil {
		return err
	}
	backend, ok := service.backend.(StrictLeaseBackend)
	if !ok {
		return fmt.Errorf("%w: backend does not guarantee strict leases", ErrUnsupported)
	}
	rawErr := backend.ReleaseStrict(ctx, lease)
	if rawErr == nil {
		return nil
	}
	declaration, category := strictDeclaration(rawErr)
	if declaration == declarationOutcome {
		outcome := rawErr.(*strictOutcomeError) //nolint:errorlint // Declaration proved the exact direct strict outcome type.
		switch outcome.kind {
		case strictOutcomeCanceled:
			return CanceledOutcome()
		case strictOutcomeDeadline:
			return DeadlineOutcome()
		case strictOutcomeUnknown:
			return reconstructUnknown(outcome)
		}
	}
	if declaration == declarationSafe && applicableCategory(category, strictMethodRelease) {
		return rawErr
	}
	return UnknownOutcome(ctx.Err())
}

func validateStrictLease(lease Lease) error {
	if lease.ID == "" || lease.Key.String() == "" || lease.PolicyID == "" || lease.Cost == 0 || lease.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: complete lease is required", ErrInvalidRequest)
	}
	return nil
}

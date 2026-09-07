package ratelimit

import (
	"context"
	"fmt"
	"time"
)

const (
	// ReasonCanceled identifies caller cancellation before backend dispatch.
	ReasonCanceled Reason = "canceled"
	// ReasonDeadline identifies a known deadline before mutation dispatch.
	ReasonDeadline Reason = "deadline"
	// ReasonOutcomeUnknown identifies an ambiguous post-dispatch result.
	ReasonOutcomeUnknown Reason = "outcome_unknown"
)

// StrictBackend exposes explicit dispatch-outcome declarations.
type StrictBackend interface {
	Backend
	AdmitStrict(context.Context, Request) (Decision, error)
}

// StrictLeaseBackend exposes strict admission and lease lifecycle operations.
type StrictLeaseBackend interface {
	StrictBackend
	AcquireStrict(context.Context, LeaseRequest) (Lease, Decision, error)
	ReleaseStrict(context.Context, Lease) error
}

// StrictService validates complete backend results and fails closed on ambiguity.
type StrictService struct {
	backend   StrictBackend
	observers []Observer
	name      string
}

// NewStrictService constructs a strict service without invoking backend operations.
func NewStrictService(backend StrictBackend, observers ...Observer) (*StrictService, error) {
	if isNilInterface(backend) {
		return nil, fmt.Errorf("%w: backend is required", ErrInvalidPolicy)
	}
	filtered := make([]Observer, 0, len(observers))
	for index, observer := range observers {
		if observer == nil {
			continue
		}
		if isNilInterface(observer) {
			return nil, fmt.Errorf("%w: observer at index %d is nil", ErrInvalidPolicy, index)
		}
		filtered = append(filtered, observer)
	}
	if len(filtered) > MaxObservers {
		return nil, fmt.Errorf("%w: at most %d observers are allowed", ErrInvalidPolicy, MaxObservers)
	}
	name := backend.Name()
	if !validBackendName(name) {
		return nil, fmt.Errorf("%w: backend name must be 1 to 64 lowercase ASCII bytes using letters, digits, and single hyphens", ErrInvalidPolicy)
	}
	return &StrictService{backend: backend, observers: filtered, name: name}, nil
}

func validBackendName(name string) bool {
	if len(name) == 0 || len(name) > MaxBackendNameBytes || name[0] < 'a' || name[0] > 'z' {
		return false
	}
	previousHyphen := false
	for _, current := range []byte(name) {
		if current == '-' {
			if previousHyphen {
				return false
			}
			previousHyphen = true
			continue
		}
		if (current < 'a' || current > 'z') && (current < '0' || current > '9') {
			return false
		}
		previousHyphen = false
	}
	return !previousHyphen
}

// Admit validates and executes one strict admission request.
func (service *StrictService) Admit(ctx context.Context, request Request) (Decision, error) {
	decision, err, _ := service.admit(ctx, request, false)
	return decision, err
}

func (service *StrictService) admit(ctx context.Context, request Request, validated bool) (Decision, error, bool) {
	if service == nil || service.backend == nil || service.name == "" {
		return Decision{}, fmt.Errorf("%w: strict service is nil or uninitialized", ErrInvalidPolicy), false
	}
	if isNilInterface(ctx) {
		return Decision{}, fmt.Errorf("%w: context is required", ErrInvalidRequest), false
	}
	if !validated {
		if err := request.Validate(); err != nil {
			return Decision{}, err, false
		}
	}
	started := time.Now()
	if err := strictCallerContext(ctx); err != nil {
		if validated {
			return Decision{}, err, false
		}
		if strictDeadlineFailOpen(request, err) {
			decision := service.stampDecision(strictFailOpen(request), request)
			service.observe(request, decision, nil, started)
			return decision, nil, false
		}
		decision := service.stampDecision(strictDenied(request, contextReason(err)), request)
		service.observe(request, decision, err, started)
		return decision, err, false
	}

	raw, rawErr := service.backend.AdmitStrict(ctx, request)
	decision, err := service.normalizeAdmit(ctx, request, raw, rawErr)
	service.observe(request, decision, err, started)
	return decision, err, true
}

func (service *StrictService) normalizeAdmit(ctx context.Context, request Request, raw Decision, rawErr error) (Decision, error) {
	declaration, category := strictDeclaration(rawErr)
	if rawErr == nil && completeAllowed(raw, request) {
		return service.stampDecision(raw, request), nil
	}
	if declaration == declarationSafe && directError(category, ErrRejected) && completeRejected(raw, request) {
		return service.stampDecision(raw, request), rawErr
	}
	if declaration == declarationOutcome && zeroDecision(raw) {
		outcome := rawErr.(*strictOutcomeError) //nolint:errorlint // Declaration proved the exact direct strict outcome type.
		switch outcome.kind {
		case strictOutcomeCanceled:
			return service.stampDecision(strictDenied(request, ReasonCanceled), request), CanceledOutcome()
		case strictOutcomeDeadline:
			if request.Policy.FailureMode() == FailOpen && request.Policy.Algorithm() != Concurrency {
				return service.stampDecision(strictFailOpen(request), request), nil
			}
			return service.stampDecision(strictDenied(request, ReasonDeadline), request), DeadlineOutcome()
		case strictOutcomeUnknown:
			return service.stampDecision(strictDenied(request, ReasonOutcomeUnknown), request), reconstructUnknown(outcome)
		}
	}
	if declaration == declarationSafe && !directError(category, ErrRejected) && applicableCategory(category, strictMethodAdmit) && zeroDecision(raw) {
		if directError(category, ErrUnavailable) && request.Policy.FailureMode() == FailOpen && request.Policy.Algorithm() != Concurrency {
			return service.stampDecision(strictFailOpen(request), request), nil
		}
		return service.stampDecision(strictDenied(request, ReasonBackendUnavailable), request), rawErr
	}
	return service.unknownDecision(ctx, request)
}

func (service *StrictService) unknownDecision(ctx context.Context, request Request) (Decision, error) {
	return service.stampDecision(strictDenied(request, ReasonOutcomeUnknown), request), UnknownOutcome(ctx.Err())
}

func reconstructUnknown(outcome *strictOutcomeError) error {
	if outcome.context == strictOutcomeCanceled {
		return UnknownOutcome(context.Canceled)
	}
	if outcome.context == strictOutcomeDeadline {
		return UnknownOutcome(context.DeadlineExceeded)
	}
	return UnknownOutcome(nil)
}

func strictFailOpen(request Request) Decision {
	return Decision{Allowed: true, Limit: request.Policy.Limit(), Remaining: request.Policy.Limit(), Reason: ReasonFailOpen}
}

func strictDenied(request Request, reason Reason) Decision {
	return Decision{Allowed: false, Limit: request.Policy.Limit(), Reason: reason}
}

func (service *StrictService) stampDecision(decision Decision, request Request) Decision {
	decision.Backend = service.name
	decision.PolicyRevision = request.Policy.Revision()
	return decision
}

func (service *StrictService) observe(request Request, decision Decision, err error, started time.Time) {
	observation := Observation{
		PolicyID: request.Policy.ID(), SubjectKind: request.Key.SubjectKind(),
		Decision: decision, Err: err, Duration: time.Since(started),
	}
	for _, observer := range service.observers {
		safeObserve(observer, observation)
	}
}

func completeAllowed(decision Decision, request Request) bool {
	return decision.Allowed && decision.Reason == ReasonAllowed &&
		decision.Limit == request.Policy.Limit() &&
		decision.Remaining <= request.Policy.Limit()-request.Cost &&
		!decision.Reset.IsZero() && decision.RetryAfter == 0
}

func completeRejected(decision Decision, request Request) bool {
	return !decision.Allowed && decision.Reason == ReasonLimited &&
		decision.Limit == request.Policy.Limit() && decision.Remaining < request.Cost &&
		!decision.Reset.IsZero() && decision.RetryAfter >= 0
}

func zeroDecision(decision Decision) bool { return decision == (Decision{}) }

type declarationKind uint8

const (
	declarationUnsafe declarationKind = iota
	declarationSafe
	declarationOutcome
)

func strictDeclaration(err error) (declarationKind, error) {
	if err == nil {
		return declarationUnsafe, nil
	}
	if outcome, ok := err.(*strictOutcomeError); ok && outcome != nil { //nolint:errorlint // Strict declarations accept only direct owned outcome errors.
		return declarationOutcome, err
	}
	if safeBackendCategory(err) {
		return declarationSafe, err
	}
	if backendErr, ok := err.(*BackendError); ok && backendErr != nil && safeBackendCategory(backendErr.category) { //nolint:errorlint // BackendError must be a direct explicit declaration.
		return declarationSafe, backendErr.category
	}
	return declarationUnsafe, nil
}

type strictMethod uint8

const (
	strictMethodAdmit strictMethod = iota
	strictMethodAcquire
	strictMethodRelease
)

func applicableCategory(category error, method strictMethod) bool {
	switch method {
	case strictMethodAdmit:
		return directError(category, ErrRejected) || directError(category, ErrUnavailable) ||
			directError(category, ErrOverflow) || directError(category, ErrCorrupt) ||
			directError(category, ErrUnsupported)
	case strictMethodAcquire:
		return directError(category, ErrRejected) || directError(category, ErrUnavailable) ||
			directError(category, ErrOverflow) || directError(category, ErrCorrupt) ||
			directError(category, ErrLeaseNotOwned)
	case strictMethodRelease:
		return directError(category, ErrUnavailable) || directError(category, ErrCorrupt) ||
			directError(category, ErrLeaseNotFound) || directError(category, ErrLeaseNotOwned)
	default:
		return false
	}
}

func strictCallerContext(ctx context.Context) error {
	switch ctx.Err() {
	case context.Canceled:
		return CanceledOutcome()
	case context.DeadlineExceeded:
		return DeadlineOutcome()
	default:
		return nil
	}
}

func contextReason(err error) Reason {
	if outcome, ok := err.(*strictOutcomeError); ok && outcome.kind == strictOutcomeCanceled { //nolint:errorlint // Only owned direct outcome errors carry cancellation priority.
		return ReasonCanceled
	}
	return ReasonDeadline
}

func strictDeadlineFailOpen(request Request, err error) bool {
	outcome, ok := err.(*strictOutcomeError) //nolint:errorlint // Only the direct owned deadline declaration can enable strict fail-open.
	return ok && outcome != nil && outcome.kind == strictOutcomeDeadline &&
		request.Policy.FailureMode() == FailOpen && request.Policy.Algorithm() != Concurrency
}

// StrictBatchDecision preserves input order and exact dispatched indexes.
type StrictBatchDecision struct {
	// Decisions has exactly one position per input request.
	Decisions []Decision
	// Attempted contains the ordered indexes dispatched to the backend.
	Attempted []int
	// Atomicity records the validated batch atomicity mode.
	Atomicity Atomicity
}

// Batch evaluates a validated per-item batch with structured failures.
func (service *StrictService) Batch(ctx context.Context, batch BatchRequest) (StrictBatchDecision, error) {
	if service == nil || service.backend == nil || service.name == "" {
		return StrictBatchDecision{}, fmt.Errorf("%w: strict service is nil or uninitialized", ErrInvalidPolicy)
	}
	if isNilInterface(ctx) {
		return StrictBatchDecision{}, fmt.Errorf("%w: context is required", ErrInvalidRequest)
	}
	if err := validateBatch(batch); err != nil {
		return StrictBatchDecision{}, err
	}
	result := StrictBatchDecision{Decisions: make([]Decision, len(batch.Requests)), Attempted: []int{}, Atomicity: AtomicityPerItem}
	items := make([]*BatchItemError, 0)
	for index, request := range batch.Requests {
		if contextErr := strictCallerContext(ctx); contextErr != nil {
			items = append(items, &BatchItemError{index: index, cause: contextErr})
			break
		}
		decision, err, dispatched := service.admit(ctx, request, true)
		if !dispatched {
			items = append(items, &BatchItemError{index: index, cause: err})
			break
		}
		result.Decisions[index] = decision
		result.Attempted = append(result.Attempted, index)
		if err != nil {
			items = append(items, &BatchItemError{index: index, cause: err})
			outcome, _ := err.(*strictOutcomeError) //nolint:errorlint // Batch receives only normalized strict-service errors.
			if outcome != nil && outcome.kind == strictOutcomeUnknown && strictCallerContext(ctx) != nil {
				break
			}
		}
	}
	if len(items) == 0 {
		return result, nil
	}
	return result, &StrictBatchError{items: items}
}

func validateBatch(batch BatchRequest) error {
	if len(batch.Requests) == 0 || len(batch.Requests) > MaxBatchSize {
		return fmt.Errorf("%w: batch size must be between 1 and %d", ErrInvalidRequest, MaxBatchSize)
	}
	if batch.Atomicity == AtomicityAllOrNothing {
		return fmt.Errorf("%w: backend-independent batches are per-item", ErrUnsupported)
	}
	if batch.Atomicity != AtomicityPerItem {
		return fmt.Errorf("%w: unknown batch atomicity", ErrInvalidRequest)
	}
	for _, request := range batch.Requests {
		if err := request.Validate(); err != nil {
			return err
		}
	}
	return nil
}

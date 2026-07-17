package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// MaxObservers bounds synchronous observation fan-out per decision.
const MaxObservers = 16

// Backend atomically evaluates admission requests within its documented scope.
type Backend interface {
	// Name returns a stable implementation identifier for decisions and leases.
	Name() string
	// Admit atomically consumes capacity or returns ErrRejected.
	Admit(context.Context, Request) (Decision, error)
}

// Service validates requests, applies failure behavior, and emits observations.
type Service struct {
	backend   Backend
	observers []Observer
}

// NewService constructs an admission service using backend and observers.
func NewService(backend Backend, observers ...Observer) (*Service, error) {
	if backend == nil || backend.Name() == "" {
		return nil, fmt.Errorf("%w: backend is required", ErrUnavailable)
	}
	filtered := make([]Observer, 0, len(observers))
	for _, observer := range observers {
		if observer != nil {
			filtered = append(filtered, observer)
		}
	}
	if len(filtered) > MaxObservers {
		return nil, fmt.Errorf("%w: at most %d observers are allowed", ErrInvalidPolicy, MaxObservers)
	}
	return &Service{backend: backend, observers: filtered}, nil
}

// Admit evaluates one request without sleeping or retrying.
func (service *Service) Admit(ctx context.Context, request Request) (decision Decision, err error) {
	if err := request.Validate(); err != nil {
		return Decision{}, err
	}
	started := time.Now()
	defer func() {
		decision.Backend = service.backend.Name()
		decision.PolicyRevision = request.Policy.Revision()
		observation := Observation{
			PolicyID: request.Policy.ID(), SubjectKind: request.Key.SubjectKind(),
			Decision: decision, Err: err, Duration: time.Since(started),
		}
		for _, observer := range service.observers {
			safeObserve(observer, observation)
		}
	}()

	decision, err = service.backend.Admit(ctx, request)
	if err == nil || errors.Is(err, ErrRejected) {
		return decision, err
	}
	err = normalizeBackendError(ctx, err)
	if request.Policy.FailureMode() == FailOpen &&
		(errors.Is(err, ErrUnavailable) || errors.Is(err, ErrDeadline)) {
		return Decision{
			Allowed: true, Limit: request.Policy.Limit(),
			Remaining: request.Policy.Limit(), Reason: ReasonFailOpen,
		}, nil
	}
	return Decision{
		Allowed: false, Limit: request.Policy.Limit(),
		Reason: ReasonBackendUnavailable,
	}, err
}

func normalizeBackendError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return ErrDeadline
	}
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return ErrDeadline
	}
	if errors.Is(err, ErrOverflow) {
		return ErrOverflow
	}
	if errors.Is(err, ErrCorrupt) {
		return ErrCorrupt
	}
	return ErrUnavailable
}

func safeObserve(observer Observer, observation Observation) {
	defer func() {
		_ = recover()
	}()
	observer.Observe(observation)
}

package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// MaxLeaseIDBytes bounds caller-generated concurrency lease identifiers.
const MaxLeaseIDBytes = 128

// LeaseRequest requests weighted concurrency admission under a unique lease ID.
type LeaseRequest struct {
	// Request must use a Concurrency policy.
	Request Request
	// LeaseID is an idempotent, caller-generated bounded identifier.
	LeaseID string
}

// Validate checks concurrency policy, admission inputs, and lease identity.
func (request LeaseRequest) Validate() error {
	if err := request.Request.Validate(); err != nil {
		return err
	}
	if request.Request.Policy.Algorithm() != Concurrency ||
		request.LeaseID == "" || len(request.LeaseID) > MaxLeaseIDBytes {
		return fmt.Errorf("%w: concurrency policy and bounded lease ID are required", ErrInvalidRequest)
	}
	return nil
}

// Lease is proof of weighted concurrency ownership until ExpiresAt or release.
type Lease struct {
	// ID is the caller-generated lease identity.
	ID string
	// Key identifies the concurrency state.
	Key Key
	// PolicyID identifies the owning policy.
	PolicyID string
	// PolicyRevision identifies the semantics used when acquiring the lease.
	PolicyRevision string
	// Cost is the held concurrency weight.
	Cost uint64
	// ExpiresAt is the automatic release boundary.
	ExpiresAt time.Time
	// Backend identifies the implementation that created the lease.
	Backend string
}

// LeaseBackend provides atomic acquire and ownership-checked release.
type LeaseBackend interface {
	// Acquire creates or idempotently returns a weighted lease.
	Acquire(context.Context, LeaseRequest) (Lease, Decision, error)
	// Release relinquishes an owned lease without affecting unrelated capacity.
	Release(context.Context, Lease) error
}

// Acquire validates and atomically obtains a concurrency lease.
func (service *Service) Acquire(ctx context.Context, request LeaseRequest) (lease Lease, decision Decision, err error) {
	if err := request.Validate(); err != nil {
		return Lease{}, Decision{}, err
	}
	backend, ok := service.backend.(LeaseBackend)
	if !ok {
		return Lease{}, Decision{}, fmt.Errorf("%w: backend does not guarantee leases", ErrUnsupported)
	}
	started := time.Now()
	defer func() {
		decision.Backend = service.backend.Name()
		decision.PolicyRevision = request.Request.Policy.Revision()
		lease.Backend = service.backend.Name()
		observation := Observation{
			PolicyID:    request.Request.Policy.ID(),
			SubjectKind: request.Request.Key.SubjectKind(),
			Decision:    decision, Err: err, Duration: time.Since(started),
		}
		for _, observer := range service.observers {
			safeObserve(observer, observation)
		}
	}()
	lease, decision, err = backend.Acquire(ctx, request)
	if err != nil && !errorsIsStableLease(err) {
		err = normalizeBackendError(ctx, err)
	}
	return lease, decision, err
}

// Release relinquishes a lease through the backend that owns it.
func (service *Service) Release(ctx context.Context, lease Lease) error {
	if lease.ID == "" || lease.Key.String() == "" || lease.PolicyID == "" ||
		lease.Cost == 0 || lease.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: complete lease is required", ErrInvalidRequest)
	}
	if lease.Backend != "" && lease.Backend != service.backend.Name() {
		return ErrLeaseNotOwned
	}
	backend, ok := service.backend.(LeaseBackend)
	if !ok {
		return ErrUnsupported
	}
	err := backend.Release(ctx, lease)
	if err != nil && !errorsIsStableLease(err) {
		return normalizeBackendError(ctx, err)
	}
	return err
}

func errorsIsStableLease(err error) bool {
	return errors.Is(err, ErrRejected) || errors.Is(err, ErrLeaseNotFound) ||
		errors.Is(err, ErrLeaseNotOwned) || errors.Is(err, ErrUnavailable) ||
		errors.Is(err, ErrDeadline) || errors.Is(err, ErrCorrupt)
}

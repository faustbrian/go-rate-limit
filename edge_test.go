package ratelimit

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

type edgeBackend struct {
	name       string
	admitErr   error
	acquireErr error
	releaseErr error
}

func (backend *edgeBackend) Name() string { return backend.name }
func (backend *edgeBackend) Admit(context.Context, Request) (Decision, error) {
	return Decision{}, backend.admitErr
}
func (backend *edgeBackend) Acquire(context.Context, LeaseRequest) (Lease, Decision, error) {
	return Lease{}, Decision{}, backend.acquireErr
}
func (backend *edgeBackend) Release(context.Context, Lease) error {
	return backend.releaseErr
}

func TestPolicyValidationEdgesAndAccessors(t *testing.T) {
	t.Parallel()

	specs := []PolicySpec{
		{ID: "x", Revision: "v1", Algorithm: TokenBucket, Capacity: math.MaxInt64, Burst: 1, Period: time.Second},
		{ID: "x", Revision: "v1", Algorithm: Concurrency, Capacity: 1},
		{ID: "x", Revision: "v1", Algorithm: FixedWindow, Capacity: 1, Period: time.Second, FailureMode: FailureMode(9)},
		{ID: "x", Revision: "v1", Algorithm: Concurrency, Capacity: 1, Lease: time.Second, FailureMode: FailOpen},
		{ID: "x", Revision: "v1", Algorithm: FixedWindow, Capacity: 1, Period: time.Second, Consistency: "eventual"},
	}
	for _, spec := range specs {
		if _, err := NewPolicy(spec); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("NewPolicy(%+v) error = %v", spec, err)
		}
	}
	policy, err := NewPolicy(PolicySpec{
		ID: "x", Revision: "v1", Algorithm: Concurrency,
		Capacity: 2, Lease: time.Second, MaxCost: 1,
		Consistency: ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	if policy.MaxCost() != 1 || policy.Consistency() != ConsistencyStrong ||
		policy.LeaseDuration() != time.Second || policy.Period() != 0 {
		t.Fatalf("policy accessors = %+v", policy)
	}
}

func TestKeyAndBatchValidationEdges(t *testing.T) {
	t.Parallel()

	for _, spec := range []KeySpec{
		{Namespace: "UPPER", Version: "v1", Subject: Subject{Kind: "x", Value: "y"}},
		{Namespace: "x", Version: "v1", Subject: Subject{Kind: "x", Value: "unsafe\nvalue"}},
	} {
		if _, err := NewKey(spec); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("NewKey(%+v) error = %v", spec, err)
		}
	}
	service, err := NewService(&edgeBackend{name: "edge"})
	if err != nil {
		t.Fatal(err)
	}
	request := edgeRequest(t, FixedWindow)
	if _, err := service.Batch(context.Background(), BatchRequest{
		Requests: []Request{request}, Atomicity: Atomicity("unknown"),
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unknown Batch() error = %v", err)
	}
	if _, err := service.Batch(context.Background(), BatchRequest{
		Requests: []Request{{}}, Atomicity: AtomicityPerItem,
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid item Batch() error = %v", err)
	}
	failing, err := NewService(&edgeBackend{name: "edge", admitErr: ErrRejected})
	if err != nil {
		t.Fatal(err)
	}
	result, err := failing.Batch(context.Background(), BatchRequest{
		Requests: []Request{request, request}, Atomicity: AtomicityPerItem,
	})
	if !errors.Is(err, ErrRejected) || len(result.Decisions) != 2 {
		t.Fatalf("failing Batch() = %+v, %v", result, err)
	}
	tooMany := make([]Request, MaxBatchSize+1)
	if _, err := service.Batch(context.Background(), BatchRequest{
		Requests: tooMany, Atomicity: AtomicityPerItem,
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("large Batch() error = %v", err)
	}
}

func TestLeaseServiceErrorAndValidationEdges(t *testing.T) {
	t.Parallel()

	request := LeaseRequest{Request: edgeRequest(t, Concurrency), LeaseID: "lease"}
	for _, invalid := range []LeaseRequest{
		{},
		{Request: edgeRequest(t, FixedWindow), LeaseID: "lease"},
		{Request: request.Request, LeaseID: string(make([]byte, MaxLeaseIDBytes+1))},
	} {
		if err := invalid.Validate(); err == nil {
			t.Fatalf("Validate(%+v) error = nil", invalid)
		}
	}
	backend := &edgeBackend{name: "edge", acquireErr: errors.New("boom")}
	service, err := NewService(backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.Acquire(context.Background(), LeaseRequest{}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid service Acquire() error = %v", err)
	}
	observed := false
	observingService, err := NewService(backend, ObserveFunc(func(Observation) { observed = true }))
	if err != nil {
		t.Fatal(err)
	}
	backend.acquireErr = ErrRejected
	_, _, _ = observingService.Acquire(context.Background(), request)
	if !observed {
		t.Fatal("lease observation was not emitted")
	}
	backend.acquireErr = errors.New("boom")
	if _, _, err := service.Acquire(context.Background(), request); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Acquire() error = %v", err)
	}
	backend.acquireErr = ErrRejected
	if _, _, err := service.Acquire(context.Background(), request); !errors.Is(err, ErrRejected) {
		t.Fatalf("stable Acquire() error = %v", err)
	}
	lease := Lease{
		ID: "lease", Key: request.Request.Key, PolicyID: request.Request.Policy.ID(),
		Cost: 1, ExpiresAt: request.Request.Now.Add(time.Second), Backend: "other",
	}
	if err := service.Release(context.Background(), lease); !errors.Is(err, ErrLeaseNotOwned) {
		t.Fatalf("foreign Release() error = %v", err)
	}
	lease.Backend = "edge"
	backend.releaseErr = errors.New("boom")
	if err := service.Release(context.Background(), lease); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Release() error = %v", err)
	}
	backend.releaseErr = ErrLeaseNotFound
	if err := service.Release(context.Background(), lease); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("stable Release() error = %v", err)
	}
	admissionOnly, err := NewService(&admissionBackend{name: "admission"})
	if err != nil {
		t.Fatal(err)
	}
	lease.Backend = "admission"
	if err := admissionOnly.Release(context.Background(), lease); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("unsupported Release() error = %v", err)
	}
	if err := service.Release(context.Background(), Lease{}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid Release() error = %v", err)
	}
}

type admissionBackend struct{ name string }

func (backend *admissionBackend) Name() string { return backend.name }
func (*admissionBackend) Admit(context.Context, Request) (Decision, error) {
	return Decision{}, nil
}

func TestBackendErrorNormalizationAndStableLeaseErrors(t *testing.T) {
	t.Parallel()

	deadline, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancel()
	if !errors.Is(normalizeBackendError(deadline, errors.New("late")), ErrDeadline) {
		t.Fatal("deadline was not normalized")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(normalizeBackendError(canceled, errors.New("canceled")), ErrDeadline) {
		t.Fatal("cancellation was not normalized")
	}
	for _, err := range []error{ErrUnavailable, ErrOverflow, ErrCorrupt} {
		if !errors.Is(normalizeBackendError(context.Background(), err), err) {
			t.Fatalf("stable error %v changed", err)
		}
	}
	for _, err := range []error{
		ErrRejected, ErrLeaseNotFound, ErrLeaseNotOwned,
		ErrUnavailable, ErrDeadline, ErrCorrupt,
	} {
		if !errorsIsStableLease(err) {
			t.Fatalf("error %v was not stable", err)
		}
	}
	if errorsIsStableLease(errors.New("unknown")) {
		t.Fatal("unknown lease error was stable")
	}
}

func edgeRequest(t *testing.T, algorithm Algorithm) Request {
	t.Helper()
	spec := PolicySpec{
		ID: "edge", Revision: "v1", Algorithm: algorithm,
		Capacity: 2, Period: time.Second, MaxCost: 2,
	}
	if algorithm == Concurrency {
		spec.Period, spec.Lease = 0, time.Second
	}
	policy, err := NewPolicy(spec)
	if err != nil {
		t.Fatal(err)
	}
	key, err := NewKey(KeySpec{
		Namespace: "test", Version: "v1",
		Subject: Subject{Kind: "case", Value: "edge"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return Request{Policy: policy, Key: key, Cost: 1, Now: time.Unix(100, 0)}
}

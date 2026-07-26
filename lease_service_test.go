package ratelimit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

type leaseBackend struct {
	backendFunc
	released ratelimit.Lease
}

func (backend *leaseBackend) Acquire(_ context.Context, request ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
	return ratelimit.Lease{
			ID: request.LeaseID, Key: request.Request.Key,
			PolicyID: request.Request.Policy.ID(), Cost: request.Request.Cost,
			ExpiresAt: request.Request.Now.Add(time.Second), Backend: backend.Name(),
		}, ratelimit.Decision{
			Allowed: true, Limit: request.Request.Policy.Limit(),
			Remaining: request.Request.Policy.Limit() - request.Request.Cost,
			Reason:    ratelimit.ReasonAllowed,
		}, nil
}

func (backend *leaseBackend) Release(_ context.Context, lease ratelimit.Lease) error {
	backend.released = lease
	return nil
}

func TestServiceDelegatesGuaranteedLeaseLifecycle(t *testing.T) {
	t.Parallel()

	backend := &leaseBackend{backendFunc: backendFunc{
		name: "lease-test",
		admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
			return ratelimit.Decision{}, ratelimit.ErrUnsupported
		},
	}}
	service, err := ratelimit.NewService(backend)
	if err != nil {
		t.Fatal(err)
	}
	request := concurrencyRequest(t)
	lease, decision, err := service.Acquire(context.Background(), ratelimit.LeaseRequest{
		Request: request, LeaseID: "job-1",
	})
	if err != nil || !decision.Allowed || lease.Backend != "lease-test" {
		t.Fatalf("Acquire() = %+v, %+v, %v", lease, decision, err)
	}
	if err := service.Release(context.Background(), lease); err != nil ||
		backend.released.ID != "job-1" {
		t.Fatalf("Release() = %v, released=%+v", err, backend.released)
	}
}

func TestServiceRejectsLeaseOnAdmissionOnlyBackend(t *testing.T) {
	t.Parallel()

	service, err := ratelimit.NewService(backendFunc{
		name: "admission-only",
		admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
			return ratelimit.Decision{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.Acquire(context.Background(), ratelimit.LeaseRequest{
		Request: concurrencyRequest(t), LeaseID: "job-1",
	}); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("Acquire() error = %v", err)
	}
}

func concurrencyRequest(t *testing.T) ratelimit.Request {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "workers", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 2, MaxCost: 2, Lease: time.Second,
		FailureMode: ratelimit.FailClosed,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "queue", Value: "jobs"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ratelimit.Request{Policy: policy, Key: key, Cost: 1, Now: time.Unix(100, 0)}
}

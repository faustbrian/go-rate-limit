package ratelimit

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestBatchSizeValidationIsIndependent(t *testing.T) {
	t.Parallel()

	service, err := NewService(&edgeBackend{name: "edge"})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	if _, err := service.Batch(context.Background(), BatchRequest{Atomicity: AtomicityPerItem}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Batch(empty) error = %v, want ErrInvalidRequest", err)
	}

	request := edgeRequest(t, FixedWindow)
	requests := make([]Request, MaxBatchSize+1)
	for index := range requests {
		requests[index] = request
	}
	if _, err := service.Batch(context.Background(), BatchRequest{
		Requests: requests, Atomicity: AtomicityPerItem,
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Batch(MaxBatchSize+1) error = %v, want ErrInvalidRequest", err)
	}
}

func TestKeyPartLengthValidationIsIndependent(t *testing.T) {
	t.Parallel()

	if validKeyPart("") {
		t.Fatal("validKeyPart(empty) = true")
	}
	if !validKeyPart(strings.Repeat("a", maxKeyPartBytes)) {
		t.Fatal("validKeyPart(exact maximum) = false")
	}
	if validKeyPart(strings.Repeat("a", maxKeyPartBytes+1)) {
		t.Fatal("validKeyPart(over maximum) = true")
	}
}

func TestPolicyArithmeticOverflowChecksAreIndependent(t *testing.T) {
	t.Parallel()

	base := PolicySpec{
		ID: "mutation", Revision: "v1", Algorithm: FixedWindow,
		Period: time.Microsecond,
	}

	carried := base
	carried.Capacity = math.MaxUint64
	carried.Burst = 1
	if _, err := NewPolicy(carried); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("NewPolicy(uint64 carry) error = %v, want ErrInvalidPolicy", err)
	}

	inexact := base
	inexact.Capacity = maxExactInteger
	inexact.Burst = 1
	if _, err := NewPolicy(inexact); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("NewPolicy(exact limit overflow) error = %v, want ErrInvalidPolicy", err)
	}
}

func TestRequestCompletenessFieldsAreIndependent(t *testing.T) {
	t.Parallel()

	request := edgeRequest(t, FixedWindow)
	if err := request.Validate(); err != nil {
		t.Fatalf("Validate(complete) error = %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*Request)
	}{
		{"policy", func(value *Request) { value.Policy = Policy{} }},
		{"key", func(value *Request) { value.Key = Key{} }},
		{"time", func(value *Request) { value.Now = time.Time{} }},
		{"zero-cost", func(value *Request) { value.Cost = 0 }},
		{"excess-cost", func(value *Request) { value.Cost = value.Policy.maxCost + 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			invalid := request
			test.mutate(&invalid)
			if err := invalid.Validate(); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("Validate() error = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

func TestLeaseCompletenessFieldsAreIndependent(t *testing.T) {
	t.Parallel()

	backend := &edgeBackend{name: "edge"}
	service, err := NewService(backend)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	request := edgeRequest(t, Concurrency)
	lease := Lease{
		ID: "lease", Key: request.Key, PolicyID: request.Policy.ID(), Cost: 1,
		ExpiresAt: request.Now.Add(time.Second), Backend: backend.Name(),
	}
	if err := service.Release(context.Background(), lease); err != nil {
		t.Fatalf("Release(complete) error = %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*Lease)
	}{
		{"id", func(value *Lease) { value.ID = "" }},
		{"key", func(value *Lease) { value.Key = Key{} }},
		{"policy", func(value *Lease) { value.PolicyID = "" }},
		{"cost", func(value *Lease) { value.Cost = 0 }},
		{"expiry", func(value *Lease) { value.ExpiresAt = time.Time{} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			invalid := lease
			test.mutate(&invalid)
			if err := service.Release(context.Background(), invalid); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("Release() error = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

func TestServiceFiltersOnlyNilObservers(t *testing.T) {
	t.Parallel()

	observer := ObserveFunc(func(Observation) {})
	service, err := NewService(&edgeBackend{name: "edge"}, nil, observer)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	if len(service.observers) != 1 || service.observers[0] == nil {
		t.Fatalf("filtered observers = %#v, want one non-nil observer", service.observers)
	}
}

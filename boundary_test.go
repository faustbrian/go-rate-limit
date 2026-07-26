package ratelimit_test

import (
	"context"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

const maxExactInteger = 9_007_199_254_740_991

func TestExactConstructionBoundaries(t *testing.T) {
	t.Parallel()

	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: strings.Repeat("a", 64), Revision: strings.Repeat("r", 64),
		Algorithm: ratelimit.TokenBucket, Capacity: maxExactInteger,
		Period: time.Microsecond, MaxCost: maxExactInteger,
	})
	if err != nil || policy.Limit() != maxExactInteger {
		t.Fatalf("exact arithmetic policy = %+v, %v", policy, err)
	}
	if _, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "azAZ09-_.:", Revision: "azAZ09-_.:",
		Algorithm: ratelimit.FixedWindow, Capacity: maxExactInteger - 1,
		Burst: 1, Period: time.Microsecond, MaxCost: maxExactInteger,
	}); err != nil {
		t.Fatalf("boundary identifier policy error = %v", err)
	}
	if _, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "overflow", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: maxExactInteger - 1, Burst: 2, Period: time.Microsecond,
	}); err == nil {
		t.Fatal("capacity plus burst above exact boundary was accepted")
	}
	concurrency, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "concurrency", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: ratelimit.MaxConcurrencyLeases, MaxCost: ratelimit.MaxConcurrencyLeases,
		Lease: time.Microsecond,
	})
	if err != nil {
		t.Fatalf("maximum concurrency policy error = %v", err)
	}

	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: strings.Repeat("a", 48), Version: strings.Repeat("9", 48),
		Subject: ratelimit.Subject{
			Kind: "az09-_", Value: strings.Repeat("s", ratelimit.MaxSubjectBytes),
		},
		Hash: true,
	})
	if err != nil {
		t.Fatalf("maximum key error = %v", err)
	}

	request := ratelimit.Request{
		Policy: concurrency, Key: key, Cost: ratelimit.MaxConcurrencyLeases,
		Now: time.UnixMicro(maxExactInteger - 1),
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("maximum request time error = %v", err)
	}
	request.Now = time.UnixMicro(-maxExactInteger)
	if err := request.Validate(); err != nil {
		t.Fatalf("minimum request time error = %v", err)
	}
	if err := (ratelimit.LeaseRequest{
		Request: request, LeaseID: strings.Repeat("l", ratelimit.MaxLeaseIDBytes),
	}).Validate(); err != nil {
		t.Fatalf("maximum lease ID error = %v", err)
	}
}

func TestExactFanoutAndBatchBoundaries(t *testing.T) {
	t.Parallel()

	observers := make([]ratelimit.Observer, ratelimit.MaxObservers)
	for index := range observers {
		observers[index] = ratelimit.ObserveFunc(func(ratelimit.Observation) {})
	}
	service, err := ratelimit.NewService(backendFunc{
		name: "boundary",
		admit: func(_ context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
			return ratelimit.Decision{
				Allowed: true, Limit: request.Policy.Limit(),
				Remaining: request.Policy.Limit() - request.Cost,
				Reason:    ratelimit.ReasonAllowed,
			}, nil
		},
	}, observers...)
	if err != nil {
		t.Fatalf("maximum observers error = %v", err)
	}
	request := validRequest(t, ratelimit.FailClosed)
	requests := make([]ratelimit.Request, ratelimit.MaxBatchSize)
	for index := range requests {
		requests[index] = request
	}
	result, err := service.Batch(context.Background(), ratelimit.BatchRequest{
		Requests: requests, Atomicity: ratelimit.AtomicityPerItem,
	})
	if err != nil || len(result.Decisions) != ratelimit.MaxBatchSize {
		t.Fatalf("maximum batch = %d, %v", len(result.Decisions), err)
	}
}

func TestIdentifierCharacterBoundaries(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"`", "{", "/", ":", "."} {
		if _, err := ratelimit.NewKey(ratelimit.KeySpec{
			Namespace: value, Version: "v1",
			Subject: ratelimit.Subject{Kind: "case", Value: "value"},
		}); err == nil {
			t.Fatalf("NewKey(%q) error = nil", value)
		}
	}
	for _, value := range []string{"`", "{", "/", "[", "@"} {
		if _, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
			ID: value, Revision: "v1", Algorithm: ratelimit.FixedWindow,
			Capacity: 1, Period: time.Second,
		}); err == nil {
			t.Fatalf("NewPolicy(%q) error = nil", value)
		}
	}
}

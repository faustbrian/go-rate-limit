package ratelimit_test

import (
	"context"
	"errors"
	"testing"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

func TestBatchDocumentsPerItemAtomicity(t *testing.T) {
	t.Parallel()

	calls := 0
	service, err := ratelimit.NewService(backendFunc{
		name: "batch-test",
		admit: func(_ context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
			calls++
			return ratelimit.Decision{
				Allowed: true, Limit: request.Policy.Limit(),
				Remaining: request.Policy.Limit() - request.Cost,
				Reason:    ratelimit.ReasonAllowed,
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	requests := []ratelimit.Request{
		validRequest(t, ratelimit.FailClosed),
		validRequest(t, ratelimit.FailClosed),
	}
	result, err := service.Batch(context.Background(), ratelimit.BatchRequest{
		Requests: requests, Atomicity: ratelimit.AtomicityPerItem,
	})
	if err != nil || calls != 2 || result.Atomicity != ratelimit.AtomicityPerItem ||
		len(result.Decisions) != 2 || !result.Decisions[1].Allowed {
		t.Fatalf("Batch() = %+v, calls=%d, err=%v", result, calls, err)
	}
}

func TestBatchRejectsUnboundedAndUnsupportedAtomicRequests(t *testing.T) {
	t.Parallel()

	service, err := ratelimit.NewService(backendFunc{
		name: "batch-test",
		admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
			return ratelimit.Decision{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Batch(context.Background(), ratelimit.BatchRequest{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("empty Batch() error = %v", err)
	}
	if _, err := service.Batch(context.Background(), ratelimit.BatchRequest{
		Requests:  []ratelimit.Request{validRequest(t, ratelimit.FailClosed)},
		Atomicity: ratelimit.AtomicityAllOrNothing,
	}); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("atomic Batch() error = %v", err)
	}
}

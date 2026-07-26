package ratelimitqueue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"github.com/faustbrian/golib/pkg/rate-limit/ratelimitqueue"
)

type backend struct {
	reject bool
}

func (backend *backend) Name() string { return "queue-test" }
func (backend *backend) Admit(_ context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	if backend.reject {
		return ratelimit.Decision{
			Allowed: false, Limit: request.Policy.Limit(), Remaining: 0,
			RetryAfter: 2 * time.Second, Reason: ratelimit.ReasonLimited,
		}, ratelimit.ErrRejected
	}
	return ratelimit.Decision{
		Allowed: true, Limit: request.Policy.Limit(),
		Remaining: request.Policy.Limit() - request.Cost,
		Reason:    ratelimit.ReasonAllowed,
	}, nil
}

func TestMiddlewareAdmitsWithoutOwningAcknowledgement(t *testing.T) {
	t.Parallel()

	service, err := ratelimit.NewService(&backend{})
	if err != nil {
		t.Fatal(err)
	}
	middleware, err := ratelimitqueue.New(ratelimitqueue.Options{
		Service: service, Policy: queuePolicy(t),
		Subject: ratelimitqueue.ByQueueAndTenant(),
	})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	handler := middleware(ratelimitqueue.HandlerFunc(func(_ context.Context, message ratelimitqueue.Message) error {
		called = message.ID == "job-1"
		return nil
	}))
	if err := handler.Handle(context.Background(), ratelimitqueue.Message{
		ID: "job-1", Queue: "emails", Tenant: "tenant-1",
	}); err != nil || !called {
		t.Fatalf("Handle() called=%v, error=%v", called, err)
	}
}

func TestMiddlewareReturnsDeferredWithoutCallingHandlerOrSleeping(t *testing.T) {
	t.Parallel()

	service, err := ratelimit.NewService(&backend{reject: true})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	middleware, err := ratelimitqueue.New(ratelimitqueue.Options{
		Service: service, Policy: queuePolicy(t), Now: func() time.Time { return now },
		Subject: ratelimitqueue.ByQueueAndTenant(),
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := middleware(ratelimitqueue.HandlerFunc(func(context.Context, ratelimitqueue.Message) error {
		t.Fatal("handler called for rejected work")
		return nil
	}))
	started := time.Now()
	err = handler.Handle(context.Background(), ratelimitqueue.Message{
		ID: "job-1", Queue: "emails", Tenant: "tenant-1",
	})
	if time.Since(started) >= time.Second {
		t.Fatal("middleware slept before returning")
	}
	var deferred *ratelimitqueue.Deferred
	if !errors.As(err, &deferred) || !errors.Is(err, ratelimit.ErrRejected) ||
		deferred.RetryAfter != 2*time.Second {
		t.Fatalf("Handle() error = %#v", err)
	}
}

func queuePolicy(t *testing.T) ratelimit.Policy {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "queue-admission", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 10, Period: time.Minute, MaxCost: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

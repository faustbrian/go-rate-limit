package ratelimitqueue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	ratelimitqueue "github.com/faustbrian/go-rate-limit/adapters/queue"
)

type nilQueueHandler struct{}

type contextKey struct{}

func (*nilQueueHandler) Handle(context.Context, ratelimitqueue.Message) error {
	panic("must not be called")
}

func TestMiddlewareAndHandlerNilContracts(t *testing.T) {
	for _, middleware := range []*ratelimitqueue.Middleware{nil, {}} {
		if handler, err := middleware.Wrap(ratelimitqueue.HandlerFunc(func(context.Context, ratelimitqueue.Message) error { return nil })); handler != nil ||
			!errors.Is(err, ratelimit.ErrInvalidPolicy) || err.Error() != "invalid rate limit policy: middleware is nil or uninitialized" {
			t.Fatalf("nil/zero middleware = %v, %v", handler, err)
		}
	}
	var function ratelimitqueue.HandlerFunc
	if err := function.Handle(context.Background(), ratelimitqueue.Message{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil HandlerFunc error = %v", err)
	}
	if !errors.Is(((*ratelimitqueue.Deferred)(nil)).Unwrap(), ratelimit.ErrRejected) {
		t.Fatal("nil Deferred did not unwrap rejection")
	}
}

type backend struct{}

func (backend) Name() string                                                         { return "test" }
func (backend) Admit(context.Context, ratelimit.Request) (ratelimit.Decision, error) { panic("legacy") }
func (backend) AdmitStrict(_ context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	return ratelimit.Decision{Allowed: true, Limit: request.Policy.Limit(), Remaining: request.Policy.Limit() - request.Cost, Reset: request.Now.Add(time.Second), Reason: ratelimit.ReasonAllowed}, nil
}

func TestConstructionAndContextValidationPrecedence(t *testing.T) {
	if _, err := ratelimitqueue.New(ratelimitqueue.Options{}); err == nil || err.Error() != "invalid rate limit policy: strict service is required" {
		t.Fatalf("zero options error = %v", err)
	}
	service, _ := ratelimit.NewStrictService(backend{})
	if _, err := ratelimitqueue.New(ratelimitqueue.Options{Service: service}); err == nil || err.Error() != "invalid rate limit policy: policy is required" {
		t.Fatalf("zero policy error = %v", err)
	}
	policy, _ := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: "queue", Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 1, Period: time.Second, MaxCost: 1})
	if _, err := ratelimitqueue.New(ratelimitqueue.Options{Service: service, Policy: policy}); err == nil || err.Error() != "invalid rate limit policy: subject is required" {
		t.Fatalf("nil subject error = %v", err)
	}
	middleware, err := ratelimitqueue.New(ratelimitqueue.Options{Service: service, Policy: policy, Subject: ratelimitqueue.ByPrincipal(), Now: func() time.Time { return time.Unix(100, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	for name, next := range map[string]ratelimitqueue.Handler{"literal nil": nil, "typed nil": (*nilQueueHandler)(nil)} {
		if handler, wrapErr := middleware.Wrap(next); handler != nil || !errors.Is(wrapErr, ratelimit.ErrInvalidPolicy) || wrapErr.Error() != "invalid rate limit policy: handler is required" {
			t.Fatalf("%s handler = %v, %v", name, handler, wrapErr)
		}
	}
	wantErr := errors.New("downstream")
	key := contextKey{}
	ctx := context.WithValue(context.Background(), key, "value")
	message := ratelimitqueue.Message{ID: "id", Queue: "queue", Principal: "principal", Attempt: 2}
	called := false
	wrapped, err := middleware.Wrap(ratelimitqueue.HandlerFunc(func(receivedContext context.Context, receivedMessage ratelimitqueue.Message) error {
		called = true
		if receivedContext != ctx || receivedContext.Value(key) != "value" || receivedMessage != message {
			t.Fatal("context or message identity changed")
		}
		return wantErr
	}))
	if err != nil {
		t.Fatal(err)
	}
	if callErr := wrapped.Handle(ctx, message); callErr != wantErr || !called { //nolint:errorlint // Exact downstream error identity is part of the contract.
		t.Fatalf("successful transfer = %v, called=%v", callErr, called)
	}
	called = false
	function := ratelimitqueue.HandlerFunc(func(context.Context, ratelimitqueue.Message) error { called = true; return nil })
	//lint:ignore SA1012 A nil context is the contract under test.
	if err := function.Handle(nil, ratelimitqueue.Message{}); //nolint:staticcheck // A nil context is the contract under test.
	err == nil || err.Error() != "invalid rate limit request: context is required" || called {
		t.Fatalf("nil context = %v, called=%v", err, called)
	}
}

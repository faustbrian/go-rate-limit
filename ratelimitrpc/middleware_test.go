package ratelimitrpc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"github.com/faustbrian/golib/pkg/rate-limit/ratelimitrpc"
)

type backend struct {
	requests []ratelimit.Request
	rejectAt int
}

func (backend *backend) Name() string { return "rpc-test" }
func (backend *backend) Admit(_ context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	backend.requests = append(backend.requests, request)
	if len(backend.requests) == backend.rejectAt {
		return ratelimit.Decision{
			Allowed: false, Limit: request.Policy.Limit(), Remaining: 0,
			RetryAfter: time.Second, Reason: ratelimit.ReasonLimited,
		}, ratelimit.ErrRejected
	}
	return ratelimit.Decision{
		Allowed: true, Limit: request.Policy.Limit(),
		Remaining: request.Policy.Limit() - request.Cost,
		Reason:    ratelimit.ReasonAllowed,
	}, nil
}

func TestMiddlewareAppliesGlobalPrincipalAndMethodRules(t *testing.T) {
	t.Parallel()

	implementation := &backend{}
	service, err := ratelimit.NewService(implementation)
	if err != nil {
		t.Fatal(err)
	}
	middleware, err := ratelimitrpc.New(ratelimitrpc.Options{
		Service: service, Now: func() time.Time { return time.Unix(100, 0) },
		Rules: []ratelimitrpc.Rule{
			{Policy: rpcPolicy(t, "global"), Subject: ratelimitrpc.Global("service")},
			{Policy: rpcPolicy(t, "principal"), Subject: ratelimitrpc.Principal()},
			{Policy: rpcPolicy(t, "method"), Subject: ratelimitrpc.Method()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := middleware(ratelimitrpc.HandlerFunc(func(_ context.Context, call ratelimitrpc.Call) (ratelimitrpc.Response, error) {
		return ratelimitrpc.Response{ID: call.ID, Result: "ok"}, nil
	}))
	response, err := handler.Handle(context.Background(), ratelimitrpc.Call{
		ID: "1", Method: "orders.create", Principal: "user-42", Tenant: "tenant-7",
	})
	if err != nil || response.Result != "ok" || len(implementation.requests) != 3 {
		t.Fatalf("Handle() = %+v, %v, requests=%d", response, err, len(implementation.requests))
	}
	for _, request := range implementation.requests {
		if request.Key.String() == "" || !request.Now.Equal(time.Unix(100, 0)) {
			t.Fatalf("request = %+v", request)
		}
	}
}

func TestMiddlewareReturnsGenericJSONRPCRejection(t *testing.T) {
	t.Parallel()

	implementation := &backend{rejectAt: 1}
	service, err := ratelimit.NewService(implementation)
	if err != nil {
		t.Fatal(err)
	}
	middleware, err := ratelimitrpc.New(ratelimitrpc.Options{
		Service: service, Rules: []ratelimitrpc.Rule{
			{Policy: rpcPolicy(t, "tenant-secret"), Subject: ratelimitrpc.Tenant()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := middleware(ratelimitrpc.HandlerFunc(func(context.Context, ratelimitrpc.Call) (ratelimitrpc.Response, error) {
		t.Fatal("next handler called")
		return ratelimitrpc.Response{}, nil
	})).Handle(context.Background(), ratelimitrpc.Call{ID: 9, Method: "x", Tenant: "private-tenant"})
	if !errors.Is(err, ratelimit.ErrRejected) || response.Error == nil ||
		response.Error.Code != ratelimitrpc.CodeRateLimited ||
		response.Error.Message != "rate limit exceeded" ||
		response.Error.RetryAfter != time.Second {
		t.Fatalf("Handle() = %+v, %v", response, err)
	}
}

func rpcPolicy(t *testing.T, id string) ratelimit.Policy {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: id, Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 10, Period: time.Minute, MaxCost: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

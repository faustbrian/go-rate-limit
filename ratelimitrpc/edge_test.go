package ratelimitrpc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

type edgeBackend struct{ err error }

func (*edgeBackend) Name() string { return "edge" }
func (backend *edgeBackend) Admit(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
	return ratelimit.Decision{}, backend.err
}

func TestRPCMiddlewareConfigurationAndFailureEdges(t *testing.T) {
	t.Parallel()

	service := edgeService(t, nil)
	policy := edgePolicy(t)
	for _, options := range []Options{
		{},
		{Service: service, Rules: make([]Rule, MaxRules+1)},
		{Service: service, Rules: []Rule{{Policy: policy}}},
	} {
		if _, err := New(options); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
			t.Fatalf("New(%+v) error = %v", options, err)
		}
	}
	tests := []struct {
		name    string
		rule    Rule
		backend error
	}{
		{name: "subject", rule: Rule{Policy: policy, Subject: func(Call) (ratelimit.Subject, error) {
			return ratelimit.Subject{}, errors.New("subject")
		}}},
		{name: "key", rule: Rule{Policy: policy, Subject: func(Call) (ratelimit.Subject, error) {
			return ratelimit.Subject{Kind: "x", Value: strings.Repeat("x", ratelimit.MaxSubjectBytes+1)}, nil
		}}},
		{name: "cost", rule: Rule{
			Policy: policy, Subject: Global("x"),
			Cost: func(Call) (uint64, error) { return 0, errors.New("cost") },
		}},
		{name: "backend", rule: Rule{Policy: policy, Subject: Global("x")}, backend: ratelimit.ErrUnavailable},
	}
	for _, test := range tests {
		middleware, err := New(Options{
			Service: edgeService(t, test.backend), Rules: []Rule{test.rule},
			Now: func() time.Time { return time.Unix(100, 0) },
		})
		if err != nil {
			t.Fatal(err)
		}
		response, err := middleware(HandlerFunc(func(context.Context, Call) (Response, error) {
			t.Fatal("next called")
			return Response{}, nil
		})).Handle(context.Background(), Call{ID: "1"})
		if err == nil || response.Error == nil || response.Error.Code != CodeRateUnavailable {
			t.Fatalf("%s response/error = %+v, %v", test.name, response, err)
		}
	}
	want := errors.New("handler")
	middleware, err := New(Options{
		Service: service, Rules: []Rule{{Policy: policy, Subject: Global("x")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := middleware(HandlerFunc(func(context.Context, Call) (Response, error) {
		return Response{}, want
	})).Handle(context.Background(), Call{}); !errors.Is(err, want) {
		t.Fatalf("handler error = %v", err)
	}
}

func TestRPCSubjectsRejectMissingValues(t *testing.T) {
	t.Parallel()

	for _, function := range []SubjectFunc{Global(""), Principal(), Method(), Tenant()} {
		if _, err := function(Call{}); !errors.Is(err, ratelimit.ErrInvalidKey) {
			t.Fatalf("missing subject error = %v", err)
		}
	}
}

func edgeService(t *testing.T, backendErr error) *ratelimit.Service {
	t.Helper()
	service, err := ratelimit.NewService(&edgeBackend{err: backendErr})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func edgePolicy(t *testing.T) ratelimit.Policy {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "edge", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 1, Period: time.Second, MaxCost: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

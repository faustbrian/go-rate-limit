package ratelimitqueue

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

func TestQueueMiddlewareEdges(t *testing.T) {
	t.Parallel()

	if (&Deferred{}).Error() == "" {
		t.Fatal("Deferred.Error() is empty")
	}
	for _, options := range []Options{{}, {Service: edgeService(t)}} {
		if _, err := New(options); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
			t.Fatalf("New(%+v) error = %v", options, err)
		}
	}
	policy := edgePolicy(t)
	tests := []struct {
		name    string
		subject SubjectFunc
		cost    func(Message) (uint64, error)
		backend *edgeBackend
	}{
		{name: "subject", subject: func(Message) (ratelimit.Subject, error) {
			return ratelimit.Subject{}, errors.New("subject")
		}},
		{name: "key", subject: func(Message) (ratelimit.Subject, error) {
			return ratelimit.Subject{Kind: "x", Value: strings.Repeat("x", ratelimit.MaxSubjectBytes+1)}, nil
		}},
		{name: "cost", subject: ByQueueAndTenant(), cost: func(Message) (uint64, error) {
			return 0, errors.New("cost")
		}},
		{name: "backend", subject: ByQueueAndTenant(), backend: &edgeBackend{err: ratelimit.ErrUnavailable}},
	}
	for _, test := range tests {
		service := edgeService(t)
		if test.backend != nil {
			var err error
			service, err = ratelimit.NewService(test.backend)
			if err != nil {
				t.Fatal(err)
			}
		}
		middleware, err := New(Options{
			Service: service, Policy: policy, Subject: test.subject,
			Cost: test.cost, Now: func() time.Time { return time.Unix(100, 0) },
		})
		if err != nil {
			t.Fatal(err)
		}
		err = middleware(HandlerFunc(func(context.Context, Message) error {
			t.Fatal("next called")
			return nil
		})).Handle(context.Background(), Message{Queue: "q", Tenant: "t"})
		if err == nil {
			t.Fatalf("%s error = nil", test.name)
		}
	}
	want := errors.New("handler")
	middleware, err := New(Options{
		Service: edgeService(t), Policy: policy, Subject: ByQueueAndTenant(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := middleware(HandlerFunc(func(context.Context, Message) error {
		return want
	})).Handle(context.Background(), Message{Queue: "q", Tenant: "t"}); !errors.Is(err, want) {
		t.Fatalf("handler error = %v", err)
	}
}

func TestQueueSubjects(t *testing.T) {
	t.Parallel()

	for _, function := range []SubjectFunc{ByQueueAndTenant(), ByPrincipal()} {
		if _, err := function(Message{}); !errors.Is(err, ratelimit.ErrInvalidKey) {
			t.Fatalf("empty subject error = %v", err)
		}
	}
	subject, err := ByPrincipal()(Message{Principal: "user-1"})
	if err != nil || subject.Kind != "principal" || subject.Value != "user-1" {
		t.Fatalf("ByPrincipal() = %+v, %v", subject, err)
	}
}

func edgeService(t *testing.T) *ratelimit.Service {
	t.Helper()
	service, err := ratelimit.NewService(&edgeBackend{})
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

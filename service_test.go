package ratelimit_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

type backendFunc struct {
	name  string
	admit func(context.Context, ratelimit.Request) (ratelimit.Decision, error)
}

func (b backendFunc) Name() string { return b.name }

func (b backendFunc) Admit(ctx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	return b.admit(ctx, request)
}

func TestServiceMakesBackendFailureBehaviorExplicit(t *testing.T) {
	t.Parallel()

	request := validRequest(t, ratelimit.FailOpen)
	observed := make(chan ratelimit.Observation, 1)
	service, err := ratelimit.NewService(backendFunc{
		name: "broken",
		admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
			return ratelimit.Decision{}, ratelimit.ErrUnavailable
		},
	}, ratelimit.ObserveFunc(func(observation ratelimit.Observation) {
		observed <- observation
	}))
	if err != nil {
		t.Fatal(err)
	}

	decision, err := service.Admit(context.Background(), request)
	if err != nil {
		t.Fatalf("Admit() error = %v", err)
	}
	if !decision.Allowed || decision.Reason != ratelimit.ReasonFailOpen ||
		decision.Backend != "broken" || decision.PolicyRevision != "v1" {
		t.Fatalf("decision = %+v", decision)
	}
	if observation := <-observed; observation.Decision.Reason != ratelimit.ReasonFailOpen {
		t.Fatalf("observation = %+v", observation)
	}

	request = validRequest(t, ratelimit.FailClosed)
	decision, err = service.Admit(context.Background(), request)
	if !errors.Is(err, ratelimit.ErrUnavailable) || decision.Allowed ||
		decision.Reason != ratelimit.ReasonBackendUnavailable {
		t.Fatalf("closed decision/error = %+v / %v", decision, err)
	}
}

func TestFailOpenCannotBypassIntegrityFailures(t *testing.T) {
	t.Parallel()

	for _, integrityErr := range []error{ratelimit.ErrCorrupt, ratelimit.ErrOverflow} {
		service, err := ratelimit.NewService(backendFunc{
			name: "broken",
			admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
				return ratelimit.Decision{}, integrityErr
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		decision, err := service.Admit(context.Background(), validRequest(t, ratelimit.FailOpen))
		if !errors.Is(err, integrityErr) || decision.Allowed ||
			decision.Reason != ratelimit.ReasonBackendUnavailable {
			t.Fatalf("Admit(%v) = %+v, %v", integrityErr, decision, err)
		}
	}
}

func TestServiceRedactsBackendFailureDetails(t *testing.T) {
	t.Parallel()

	service, err := ratelimit.NewService(backendFunc{
		name: "broken",
		admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
			return ratelimit.Decision{}, errors.New("password=secret raw-key=principal")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Admit(context.Background(), validRequest(t, ratelimit.FailClosed))
	if !errors.Is(err, ratelimit.ErrUnavailable) ||
		strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "principal") {
		t.Fatalf("Admit() error leaked backend details: %q", err)
	}
}

func TestServicePreservesRejectionDecision(t *testing.T) {
	t.Parallel()

	request := validRequest(t, ratelimit.FailClosed)
	reset := request.Now.Add(time.Second)
	service, err := ratelimit.NewService(backendFunc{
		name: "reference",
		admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
			return ratelimit.Decision{
				Allowed: false, Limit: 2, Remaining: 0, Reset: reset,
				RetryAfter: time.Second, Reason: ratelimit.ReasonLimited,
			}, ratelimit.ErrRejected
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	decision, err := service.Admit(context.Background(), request)
	if !errors.Is(err, ratelimit.ErrRejected) {
		t.Fatalf("Admit() error = %v", err)
	}
	if decision.Backend != "reference" || decision.PolicyRevision != "v1" ||
		!decision.Reset.Equal(reset) || decision.RetryAfter != time.Second {
		t.Fatalf("decision = %+v", decision)
	}
}

func TestServiceRejectsInvalidBackendAndRequest(t *testing.T) {
	t.Parallel()

	if _, err := ratelimit.NewService(nil); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("NewService(nil) error = %v", err)
	}
	observers := make([]ratelimit.Observer, 17)
	for index := range observers {
		observers[index] = ratelimit.ObserveFunc(func(ratelimit.Observation) {})
	}
	if _, err := ratelimit.NewService(backendFunc{name: "ok"}, observers...); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("NewService(too many observers) error = %v", err)
	}
	service, err := ratelimit.NewService(backendFunc{name: "ok", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
		return ratelimit.Decision{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Admit(context.Background(), ratelimit.Request{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("Admit(invalid) error = %v", err)
	}
}

func validRequest(t *testing.T, mode ratelimit.FailureMode) ratelimit.Request {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "test", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 2, Period: time.Second, FailureMode: mode,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "principal", Value: "42"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ratelimit.Request{Policy: policy, Key: key, Cost: 1, Now: time.Unix(100, 0)}
}

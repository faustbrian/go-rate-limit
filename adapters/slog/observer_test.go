package ratelimitslog

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

type callbackHandler struct {
	handle func(context.Context, slog.Record) error
}

type successBackend struct{}

func (successBackend) Name() string { return "test" }
func (successBackend) Admit(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
	panic("legacy")
}
func (successBackend) AdmitStrict(_ context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	return ratelimit.Decision{Allowed: true, Limit: request.Policy.Limit(), Remaining: request.Policy.Limit() - request.Cost, Reset: request.Now.Add(time.Second), Reason: ratelimit.ReasonAllowed}, nil
}

func (handler *callbackHandler) Enabled(context.Context, slog.Level) bool { return true }
func (handler *callbackHandler) Handle(ctx context.Context, record slog.Record) error {
	return handler.handle(ctx, record)
}
func (handler *callbackHandler) WithAttrs([]slog.Attr) slog.Handler { return handler }
func (handler *callbackHandler) WithGroup(string) slog.Handler      { return handler }

func TestObserverContract(t *testing.T) {
	if _, err := New(Options{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("New() error = %v", err)
	}
	var absent *Observer
	absent.Observe(ratelimit.Observation{})
	var output bytes.Buffer
	observer, err := New(Options{Logger: slog.New(slog.NewJSONHandler(&output, nil))})
	if err != nil {
		t.Fatal(err)
	}
	observer.Observe(ratelimit.Observation{PolicyID: "login", SubjectKind: "principal", Decision: ratelimit.Decision{Backend: "valkey", Reason: ratelimit.ReasonLimited, PolicyRevision: "v2"}, Err: ratelimit.ErrRejected, Duration: 2 * time.Millisecond})
	for _, fragment := range []string{`"msg":"rate limit decision"`, `"policy_id":"login"`, `"subject_kind":"principal"`, `"backend":"valkey"`} {
		if !strings.Contains(output.String(), fragment) {
			t.Fatalf("log missing %q: %s", fragment, output.String())
		}
	}
	tests := []struct {
		err  error
		want string
	}{{nil, "none"}, {ratelimit.ErrOutcomeUnknown, "outcome_unknown"}, {ratelimit.ErrCanceled, "canceled"}, {ratelimit.ErrDeadline, "deadline"}, {ratelimit.ErrRejected, "rejected"}, {ratelimit.ErrUnavailable, "unavailable"}, {ratelimit.ErrOverflow, "overflow"}, {ratelimit.ErrCorrupt, "corrupt"}, {ratelimit.ErrUnsupported, "unsupported"}, {ratelimit.ErrInvalidPolicy, "invalid"}, {ratelimit.ErrInvalidKey, "invalid"}, {ratelimit.ErrInvalidRequest, "invalid"}, {errors.New("other"), "internal"}}
	for _, test := range tests {
		if got := errorKind(test.err); got != test.want {
			t.Fatalf("errorKind(%v) = %q", test.err, got)
		}
	}
}

func TestObserverSinkLifecycle(t *testing.T) {
	observation := ratelimit.Observation{PolicyID: "policy", Decision: ratelimit.Decision{Reason: ratelimit.ReasonAllowed}}
	t.Run("blocking and caller synchronous", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		handler := &callbackHandler{handle: func(context.Context, slog.Record) error { close(entered); <-release; return nil }}
		observer, _ := New(Options{Logger: slog.New(handler)})
		done := make(chan struct{})
		go func() { observer.Observe(observation); close(done) }()
		<-entered
		select {
		case <-done:
			t.Fatal("sink did not block observer caller")
		default:
		}
		close(release)
		<-done
	})
	t.Run("panic propagates", func(t *testing.T) {
		handler := &callbackHandler{handle: func(context.Context, slog.Record) error { panic("sink panic") }}
		observer, _ := New(Options{Logger: slog.New(handler)})
		defer func() {
			if recover() != "sink panic" {
				t.Fatal("sink panic did not propagate")
			}
		}()
		observer.Observe(observation)
	})
	t.Run("service isolates sink panic", func(t *testing.T) {
		handler := &callbackHandler{handle: func(context.Context, slog.Record) error { panic("sink panic") }}
		observer, _ := New(Options{Logger: slog.New(handler)})
		var later atomic.Int64
		service, err := ratelimit.NewStrictService(successBackend{}, observer, ratelimit.ObserveFunc(func(ratelimit.Observation) { later.Add(1) }))
		if err != nil {
			t.Fatal(err)
		}
		policy, _ := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: "policy", Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 1, Period: time.Second, MaxCost: 1})
		key, _ := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "test", Version: "v1", Subject: ratelimit.Subject{Kind: "principal", Value: "p"}, Hash: true})
		if _, err := service.Admit(context.Background(), ratelimit.Request{Policy: policy, Key: key, Cost: 1, Now: time.Unix(100, 0)}); err != nil || later.Load() != 1 {
			t.Fatalf("service isolation=%v later=%d", err, later.Load())
		}
	})
	t.Run("reentry", func(t *testing.T) {
		var calls atomic.Int64
		var observer *Observer
		handler := &callbackHandler{handle: func(context.Context, slog.Record) error {
			if calls.Add(1) == 1 {
				observer.Observe(observation) //nolint:contextcheck // Observer re-entry has no context-bearing API.
			}
			return nil
		}}
		observer, _ = New(Options{Logger: slog.New(handler)})
		observer.Observe(observation)
		if calls.Load() != 2 {
			t.Fatalf("reentrant calls=%d", calls.Load())
		}
	})
	t.Run("concurrent", func(t *testing.T) {
		var calls atomic.Int64
		handler := &callbackHandler{handle: func(context.Context, slog.Record) error { calls.Add(1); return nil }}
		observer, _ := New(Options{Logger: slog.New(handler)})
		var group sync.WaitGroup
		for range 16 {
			group.Add(1)
			go func() { defer group.Done(); observer.Observe(observation) }()
		}
		group.Wait()
		if calls.Load() != 16 {
			t.Fatalf("concurrent calls=%d", calls.Load())
		}
	})
}

package ratelimitqueue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

type strictBackend struct {
	decision ratelimit.Decision
	err      error
	request  ratelimit.Request
	raw      bool
	mu       sync.Mutex
}

type hostileBackendError struct{}

func (hostileBackendError) Error() string { panic("hostile Error invoked") }
func (hostileBackendError) Is(error) bool { panic("hostile Is invoked") }
func (hostileBackendError) As(any) bool   { panic("hostile As invoked") }

func (*strictBackend) Name() string { return "test" }
func (*strictBackend) Admit(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
	panic("legacy")
}
func (backend *strictBackend) AdmitStrict(_ context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	backend.mu.Lock()
	backend.request = request
	backend.mu.Unlock()
	decision := backend.decision
	if decision == (ratelimit.Decision{}) && backend.err == nil && !backend.raw {
		decision = ratelimit.Decision{Allowed: true, Limit: request.Policy.Limit(), Remaining: request.Policy.Limit() - request.Cost, Reset: request.Now.Add(time.Second), Reason: ratelimit.ReasonAllowed}
	}
	return decision, backend.err
}

type panicErrorOnly struct{}

func (panicErrorOnly) Error() string { panic("hostile Error invoked") }

type panicIsOnly struct{}

func (panicIsOnly) Error() string { return "hostile-is" }
func (panicIsOnly) Is(error) bool { panic("hostile Is invoked") }

type panicAsOnly struct{}

func (panicAsOnly) Error() string { return "hostile-as" }
func (panicAsOnly) As(any) bool   { panic("hostile As invoked") }

type nilHandler struct{}

type nilQueueContext struct{}

func (*nilQueueContext) Deadline() (time.Time, bool) { panic("called") }
func (*nilQueueContext) Done() <-chan struct{}       { panic("called") }
func (*nilQueueContext) Err() error                  { panic("called") }
func (*nilQueueContext) Value(any) any               { panic("called") }

func (*nilHandler) Handle(context.Context, Message) error { panic("called") }
func queuePolicy(t *testing.T) ratelimit.Policy {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: "queue", Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 2, Period: time.Second, MaxCost: 2})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
func queueService(t *testing.T, backend *strictBackend) *ratelimit.StrictService {
	t.Helper()
	service, err := ratelimit.NewStrictService(backend)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestQueueMiddlewareOutcomes(t *testing.T) {
	policy := queuePolicy(t)
	service := queueService(t, &strictBackend{})
	subject := ByPrincipal()
	subjectErr := errors.New("subject")
	costErr := errors.New("cost")
	if _, err := New(Options{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("empty=%v", err)
	}
	if _, err := New(Options{Service: service}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("policy=%v", err)
	}
	if _, err := New(Options{Service: service, Policy: policy}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("subject=%v", err)
	}
	tests := []struct {
		name    string
		backend *strictBackend
		subject SubjectFunc
		cost    func(Message) (uint64, error)
		want    error
		called  bool
	}{
		{name: "success", backend: &strictBackend{}, subject: subject, called: true},
		{name: "subject", backend: &strictBackend{}, subject: func(Message) (ratelimit.Subject, error) { return ratelimit.Subject{}, subjectErr }, want: subjectErr},
		{name: "invalid key", backend: &strictBackend{}, subject: func(Message) (ratelimit.Subject, error) { return ratelimit.Subject{}, nil }, want: ratelimit.ErrInvalidKey},
		{name: "cost", backend: &strictBackend{}, subject: subject, cost: func(Message) (uint64, error) { return 0, costErr }, want: costErr},
		{name: "rejected", backend: &strictBackend{decision: ratelimit.Decision{Allowed: false, Limit: 2, Remaining: 0, Reset: time.Unix(101, 0), RetryAfter: time.Second, Reason: ratelimit.ReasonLimited}, err: ratelimit.ErrRejected}, subject: subject, want: ratelimit.ErrRejected},
		{name: "unavailable", backend: &strictBackend{err: ratelimit.ErrUnavailable}, subject: subject, want: ratelimit.ErrUnavailable},
		{name: "huge hostile", backend: &strictBackend{err: errors.New(strings.Repeat("hostile-marker", 1_048_576/len("hostile-marker")+1)[:1_048_576])}, subject: subject, want: ratelimit.ErrOutcomeUnknown},
		{name: "wrapped hostile", backend: &strictBackend{err: fmt.Errorf("hostile-marker: %w", ratelimit.ErrUnavailable)}, subject: subject, want: ratelimit.ErrOutcomeUnknown},
		{name: "joined hostile", backend: &strictBackend{err: errors.Join(ratelimit.ErrUnavailable, ratelimit.ErrCorrupt)}, subject: subject, want: ratelimit.ErrOutcomeUnknown},
		{name: "hostile", backend: &strictBackend{err: hostileBackendError{}}, subject: subject, want: ratelimit.ErrOutcomeUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			middleware, err := New(Options{Service: queueService(t, test.backend), Policy: policy, Subject: test.subject, Cost: test.cost, Now: func() time.Time { return time.Unix(100, 0) }})
			if err != nil {
				t.Fatal(err)
			}
			handler, err := middleware.Wrap(HandlerFunc(func(context.Context, Message) error { called = true; return nil }))
			if err != nil {
				t.Fatal(err)
			}
			callErr := handler.Handle(context.Background(), Message{Principal: "p"})
			if (test.want == nil) != (callErr == nil) || test.want != nil && !errors.Is(callErr, test.want) || called != test.called {
				t.Fatalf("error=%v called=%v", callErr, called)
			}
			if test.name == "rejected" {
				var deferred *Deferred
				if !errors.As(callErr, &deferred) || deferred.RetryAfter != time.Second {
					t.Fatalf("deferred=%+v", deferred)
				}
			}
			if errors.Is(callErr, ratelimit.ErrOutcomeUnknown) &&
				(callErr.Error() != "rate limit outcome unknown" || strings.Contains(callErr.Error(), "hostile-marker")) {
				t.Fatalf("unknown queue error was not bounded: %q", callErr.Error())
			}
		})
	}
	middleware, _ := New(Options{Service: service, Policy: policy, Subject: subject})
	if handler, err := middleware.Wrap(nil); handler != nil || !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil handler=%v,%v", handler, err)
	}
	var absent *nilHandler
	if handler, err := middleware.Wrap(absent); handler != nil || !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("typed nil=%v,%v", handler, err)
	}
	subjectCalled := false
	costCalled := false
	nowCalled := false
	var err error
	middleware, err = New(Options{
		Service: service, Policy: policy,
		Subject: func(Message) (ratelimit.Subject, error) {
			subjectCalled = true
			return ratelimit.Subject{Kind: "principal", Value: "p"}, nil
		},
		Cost: func(Message) (uint64, error) {
			costCalled = true
			return 1, nil
		},
		Now: func() time.Time {
			nowCalled = true
			return time.Unix(100, 0)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	handler, err := middleware.Wrap(HandlerFunc(func(context.Context, Message) error { called = true; return nil }))
	if err != nil {
		t.Fatal(err)
	}
	for _, ctx := range []context.Context{nil, (*nilQueueContext)(nil)} {
		if err := handler.Handle(ctx, Message{Principal: "p"}); err == nil || err.Error() != "invalid rate limit request: context is required" ||
			called || subjectCalled || costCalled || nowCalled {
			t.Fatalf("nil context=%v downstream=%v subject=%v cost=%v now=%v", err, called, subjectCalled, costCalled, nowCalled)
		}
	}
	wrapped := handler.(HandlerFunc)
	if err := wrapped((*nilQueueContext)(nil), Message{Principal: "p"}); err == nil || err.Error() != "invalid rate limit request: context is required" ||
		called || subjectCalled || costCalled || nowCalled {
		t.Fatalf("direct nil context=%v downstream=%v subject=%v cost=%v now=%v", err, called, subjectCalled, costCalled, nowCalled)
	}
	if nilInterface(1) {
		t.Fatal("integer reported nil")
	}
}

func TestQueueMiddlewareRejectsEveryUnsafeAndContradictoryAdmission(t *testing.T) {
	policy := queuePolicy(t)
	reset := time.Unix(101, 0)
	allowed := ratelimit.Decision{Allowed: true, Limit: 2, Remaining: 1, Reset: reset, Reason: ratelimit.ReasonAllowed}
	rejected := ratelimit.Decision{Limit: 2, Remaining: 0, Reset: reset, RetryAfter: time.Second, Reason: ratelimit.ReasonLimited}
	backendError := func(category error) error {
		value, err := ratelimit.NewBackendError(category)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	var nilBackendError *ratelimit.BackendError
	tests := []struct {
		name     string
		decision ratelimit.Decision
		err      error
	}{
		{name: "raw canceled", err: context.Canceled},
		{name: "raw deadline", err: context.DeadlineExceeded},
		{name: "nil backend error", err: nilBackendError},
		{name: "lease not found", err: ratelimit.ErrLeaseNotFound},
		{name: "lease not found wrapper", err: backendError(ratelimit.ErrLeaseNotFound)},
		{name: "lease not owned", err: ratelimit.ErrLeaseNotOwned},
		{name: "lease not owned wrapper", err: backendError(ratelimit.ErrLeaseNotOwned)},
		{name: "panic Error", err: panicErrorOnly{}},
		{name: "panic Is", err: panicIsOnly{}},
		{name: "panic As", err: panicAsOnly{}},
		{name: "zero success"},
		{name: "denied without error", decision: rejected},
		{name: "allowed rejection", decision: allowed, err: ratelimit.ErrRejected},
		{name: "wrong rejection reason", decision: func() ratelimit.Decision { value := rejected; value.Reason = ratelimit.ReasonAllowed; return value }(), err: ratelimit.ErrRejected},
		{name: "wrong rejection limit", decision: func() ratelimit.Decision { value := rejected; value.Limit = 3; return value }(), err: ratelimit.ErrRejected},
		{name: "wrong rejection remaining", decision: func() ratelimit.Decision { value := rejected; value.Remaining = 1; return value }(), err: ratelimit.ErrRejected},
		{name: "missing rejection reset", decision: func() ratelimit.Decision { value := rejected; value.Reset = time.Time{}; return value }(), err: ratelimit.ErrRejected},
		{name: "negative rejection retry", decision: func() ratelimit.Decision { value := rejected; value.RetryAfter = -time.Second; return value }(), err: ratelimit.ErrRejected},
		{name: "nonzero terminal", decision: allowed, err: ratelimit.ErrUnavailable},
	}
	for _, category := range []error{ratelimit.ErrRejected, ratelimit.ErrUnavailable, ratelimit.ErrOverflow, ratelimit.ErrCorrupt, ratelimit.ErrUnsupported, ratelimit.ErrLeaseNotFound, ratelimit.ErrLeaseNotOwned} {
		tests = append(tests, struct {
			name     string
			decision ratelimit.Decision
			err      error
		}{name: "wrapped " + category.Error(), err: fmt.Errorf("outer: %w", category)})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			middleware, err := New(Options{Service: queueService(t, &strictBackend{decision: test.decision, err: test.err, raw: true}), Policy: policy, Subject: ByPrincipal(), Now: func() time.Time { return time.Unix(100, 0) }})
			if err != nil {
				t.Fatal(err)
			}
			handler, err := middleware.Wrap(HandlerFunc(func(context.Context, Message) error { called = true; return nil }))
			if err != nil {
				t.Fatal(err)
			}
			callErr := handler.Handle(context.Background(), Message{Principal: "p"})
			if !errors.Is(callErr, ratelimit.ErrOutcomeUnknown) || called || callErr.Error() != "rate limit outcome unknown" {
				t.Fatalf("error=%v called=%v", callErr, called)
			}
		})
	}
}

type queueContextKey struct{}

func TestQueueMiddlewareCallbackLifecycle(t *testing.T) {
	policy := queuePolicy(t)
	service := queueService(t, &strictBackend{})
	message := Message{ID: "id", Queue: "queue", Tenant: "tenant", Principal: "principal", Attempt: 2}
	ctx := context.WithValue(context.Background(), queueContextKey{}, "value")
	var order []string
	var middleware *Middleware
	middleware, _ = New(Options{Service: service, Policy: policy,
		Subject: func(received Message) (ratelimit.Subject, error) {
			if received != message {
				t.Fatal("subject message identity changed")
			}
			order = append(order, "subject")
			if _, err := middleware.Wrap(HandlerFunc(func(context.Context, Message) error { return nil })); err != nil {
				t.Fatal(err)
			}
			return ratelimit.Subject{Kind: "principal", Value: received.Principal}, nil
		},
		Cost: func(received Message) (uint64, error) {
			if received != message {
				t.Fatal("cost message identity changed")
			}
			order = append(order, "cost")
			return 1, nil
		},
		Now: func() time.Time { order = append(order, "now"); return time.Unix(100, 0) },
	})
	handler, _ := middleware.Wrap(HandlerFunc(func(received context.Context, got Message) error {
		if received != ctx || got != message {
			t.Fatal("downstream arguments changed")
		}
		order = append(order, "handler")
		return nil
	}))
	if err := handler.Handle(ctx, message); err != nil {
		t.Fatal(err)
	}
	if strings.Join(order, ",") != "subject,cost,now,handler" {
		t.Fatalf("order=%v", order)
	}

	blocked := make(chan struct{})
	entered := make(chan struct{})
	blocking, _ := New(Options{Service: service, Policy: policy, Subject: func(Message) (ratelimit.Subject, error) {
		close(entered)
		<-blocked
		return ratelimit.Subject{Kind: "principal", Value: "p"}, nil
	}})
	blockingHandler, _ := blocking.Wrap(HandlerFunc(func(context.Context, Message) error { return nil }))
	done := make(chan struct{})
	go func() { _ = blockingHandler.Handle(context.Background(), message); close(done) }()
	<-entered
	select {
	case <-done:
		t.Fatal("callback did not block caller")
	default:
	}
	close(blocked)
	<-done

	panicking, _ := New(Options{Service: service, Policy: policy, Subject: func(Message) (ratelimit.Subject, error) { panic("subject panic") }})
	panickingHandler, _ := panicking.Wrap(HandlerFunc(func(context.Context, Message) error { return nil }))
	func() {
		defer func() {
			if recover() != "subject panic" {
				t.Fatal("callback panic did not propagate")
			}
		}()
		_ = panickingHandler.Handle(context.Background(), message)
	}()
	panickingDownstream, _ := middleware.Wrap(HandlerFunc(func(context.Context, Message) error { panic("handler panic") }))
	func() {
		defer func() {
			if recover() != "handler panic" {
				t.Fatal("downstream panic did not propagate")
			}
		}()
		_ = panickingDownstream.Handle(context.Background(), message)
	}()

	var subjects atomic.Int64
	var handled atomic.Int64
	concurrent, _ := New(Options{Service: service, Policy: policy, Subject: func(Message) (ratelimit.Subject, error) {
		subjects.Add(1)
		return ratelimit.Subject{Kind: "principal", Value: "p"}, nil
	}})
	concurrentHandler, _ := concurrent.Wrap(HandlerFunc(func(context.Context, Message) error { handled.Add(1); return nil }))
	var group sync.WaitGroup
	for range 16 {
		group.Add(1)
		go func() { defer group.Done(); _ = concurrentHandler.Handle(context.Background(), message) }()
	}
	group.Wait()
	if subjects.Load() != 16 || handled.Load() != 16 {
		t.Fatalf("concurrent calls=%d/%d", subjects.Load(), handled.Load())
	}
}

func TestQueueMiddlewareUsesConfiguredClock(t *testing.T) {
	policy := queuePolicy(t)
	backend := &strictBackend{}
	now := time.Unix(300, 0)
	middleware, err := New(Options{
		Service: queueService(t, backend), Policy: policy, Subject: ByPrincipal(),
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := middleware.Wrap(HandlerFunc(func(context.Context, Message) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Handle(context.Background(), Message{Principal: "p"}); err != nil {
		t.Fatal(err)
	}
	if !backend.request.Now.Equal(now) {
		t.Fatalf("request time=%v", backend.request.Now)
	}
}

func TestQueueMiddlewarePreservesAuthorityNeutralClockSkew(t *testing.T) {
	policy := queuePolicy(t)
	callerNow := time.Unix(300, 0)
	backendReset := time.Unix(200, 0)
	for _, test := range []struct {
		name     string
		decision ratelimit.Decision
		err      error
		called   bool
	}{
		{
			name: "allowed",
			decision: ratelimit.Decision{
				Allowed: true, Limit: policy.Limit(), Remaining: policy.Limit() - 1,
				Reset: backendReset, Reason: ratelimit.ReasonAllowed,
			},
			called: true,
		},
		{
			name: "rejected",
			decision: ratelimit.Decision{
				Limit: policy.Limit(), Reset: backendReset,
				RetryAfter: time.Second, Reason: ratelimit.ReasonLimited,
			},
			err: ratelimit.ErrRejected,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			middleware, err := New(Options{
				Service: queueService(t, &strictBackend{decision: test.decision, err: test.err}),
				Policy:  policy, Subject: ByPrincipal(), Now: func() time.Time { return callerNow },
			})
			if err != nil {
				t.Fatal(err)
			}
			called := false
			handler, err := middleware.Wrap(HandlerFunc(func(context.Context, Message) error { called = true; return nil }))
			if err != nil {
				t.Fatal(err)
			}
			callErr := handler.Handle(context.Background(), Message{Principal: "p"})
			if !errors.Is(callErr, test.err) || called != test.called {
				t.Fatalf("error=%v called=%v", callErr, called)
			}
		})
	}
}

func TestQueueHelpersAndTotals(t *testing.T) {
	byTenant := ByQueueAndTenant()
	if _, err := byTenant(Message{}); !errors.Is(err, ratelimit.ErrInvalidKey) {
		t.Fatalf("queue=%v", err)
	}
	if _, err := byTenant(Message{Queue: "q"}); !errors.Is(err, ratelimit.ErrInvalidKey) {
		t.Fatalf("tenant=%v", err)
	}
	subject, err := byTenant(Message{Queue: "ab", Tenant: "c"})
	if err != nil || subject.Value != "2:abc" {
		t.Fatalf("subject=%+v,%v", subject, err)
	}
	if _, err := ByPrincipal()(Message{}); !errors.Is(err, ratelimit.ErrInvalidKey) {
		t.Fatalf("principal=%v", err)
	}
	var function HandlerFunc
	if err := function.Handle(context.Background(), Message{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("function=%v", err)
	}
	deferred := &Deferred{cause: ratelimit.ErrRejected}
	if deferred.Error() != "rate-limited queue admission deferred" || !errors.Is(deferred, ratelimit.ErrRejected) {
		t.Fatalf("deferred=%v", deferred)
	}
	if cause := (&Deferred{}).Unwrap(); cause != ratelimit.ErrRejected { //nolint:errorlint // Unwrap must return the exact stable sentinel.
		t.Fatalf("zero deferred cause=%v", cause)
	}
	customCause := errors.New("custom rejection")
	if cause := (&Deferred{cause: customCause}).Unwrap(); cause != customCause { //nolint:errorlint // Unwrap must preserve the exact caller-owned cause.
		t.Fatalf("custom deferred cause=%v", cause)
	}
	var absent *Deferred
	if absent.Error() != "rate-limited queue admission deferred" || absent.Unwrap() != ratelimit.ErrRejected { //nolint:errorlint // Nil unwrap must return the exact stable sentinel.
		t.Fatal("nil deferred")
	}
}

package ratelimit_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

type lifecycleStrictBackend struct {
	name      func() string
	operation func(string, context.Context, any)
}

func (backend *lifecycleStrictBackend) Name() string {
	if backend.name != nil {
		return backend.name()
	}
	return "lifecycle"
}

func (*lifecycleStrictBackend) Admit(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
	panic("legacy admission invoked")
}

func (backend *lifecycleStrictBackend) AdmitStrict(ctx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	if backend.operation != nil {
		backend.operation("admit", ctx, request)
	}
	return completeAllowedAdmission(ctx, request)
}

func (backend *lifecycleStrictBackend) AcquireStrict(ctx context.Context, request ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
	if backend.operation != nil {
		backend.operation("acquire", ctx, request)
	}
	reset := request.Request.Now.Add(request.Request.Policy.LeaseDuration())
	lease := ratelimit.Lease{
		ID: request.LeaseID, Key: request.Request.Key,
		PolicyID: request.Request.Policy.ID(), PolicyRevision: request.Request.Policy.Revision(),
		Cost: request.Request.Cost, ExpiresAt: reset,
	}
	decision := ratelimit.Decision{
		Allowed: true, Limit: request.Request.Policy.Limit(),
		Remaining: request.Request.Policy.Limit() - request.Request.Cost,
		Reset:     reset, Reason: ratelimit.ReasonAllowed,
	}

	return lease, decision, nil
}

func (backend *lifecycleStrictBackend) ReleaseStrict(ctx context.Context, lease ratelimit.Lease) error {
	if backend.operation != nil {
		backend.operation("release", ctx, lease)
	}
	return nil
}

func TestStrictBackendNameLifecycle(t *testing.T) {
	t.Run("blocks caller and invokes no operation", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		var operations atomic.Int64
		backend := &lifecycleStrictBackend{
			name:      func() string { close(entered); <-release; return "lifecycle" },
			operation: func(string, context.Context, any) { operations.Add(1) },
		}
		done := make(chan struct{})
		go func() { defer close(done); _, _ = ratelimit.NewStrictService(backend) }()
		awaitSignal(t, entered)
		assertNotSignaled(t, done)
		close(release)
		awaitSignal(t, done)
		if operations.Load() != 0 {
			t.Fatalf("constructor operation calls = %d", operations.Load())
		}
	})

	t.Run("overlapping constructors", func(t *testing.T) {
		entered := make(chan struct{}, 2)
		release := make(chan struct{})
		backend := &lifecycleStrictBackend{name: func() string { entered <- struct{}{}; <-release; return "lifecycle" }}
		done := make(chan struct{}, 2)
		for range 2 {
			go func() { _, _ = ratelimit.NewStrictService(backend); done <- struct{}{} }()
		}
		awaitSignal(t, entered)
		awaitSignal(t, entered)
		close(release)
		awaitSignal(t, done)
		awaitSignal(t, done)
	})

	t.Run("reentry", func(t *testing.T) {
		var entered atomic.Bool
		backend := &lifecycleStrictBackend{}
		backend.name = func() string {
			if entered.CompareAndSwap(false, true) {
				if _, err := ratelimit.NewStrictService(&lifecycleStrictBackend{}); err != nil {
					t.Fatalf("reentrant constructor: %v", err)
				}
			}
			return "lifecycle"
		}
		if _, err := ratelimit.NewStrictService(backend); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("panic propagates", func(t *testing.T) {
		panicValue := &struct{ owner string }{"caller"}
		backend := &lifecycleStrictBackend{name: func() string { panic(panicValue) }}
		assertPanicIdentity(t, panicValue, func() { _, _ = ratelimit.NewStrictService(backend) })
	})
}

func TestStrictBackendOperationLifecycle(t *testing.T) {
	for _, operation := range []string{"admit", "batch", "acquire", "release"} {
		t.Run(operation+" blocks caller", func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			backend := &lifecycleStrictBackend{operation: func(string, context.Context, any) { close(entered); <-release }}
			service, _ := ratelimit.NewStrictService(backend)
			done := make(chan struct{})
			go func() { defer close(done); invokeLifecycleOperation(t, operation, service, context.Background()) }()
			awaitSignal(t, entered)
			assertNotSignaled(t, done)
			close(release)
			awaitSignal(t, done)
		})

		t.Run(operation+" overlaps", func(t *testing.T) {
			entered := make(chan struct{}, 2)
			release := make(chan struct{})
			backend := &lifecycleStrictBackend{operation: func(string, context.Context, any) { entered <- struct{}{}; <-release }}
			service, _ := ratelimit.NewStrictService(backend)
			done := make(chan struct{}, 2)
			for range 2 {
				go func() { invokeLifecycleOperation(t, operation, service, context.Background()); done <- struct{}{} }()
			}
			awaitSignal(t, entered)
			awaitSignal(t, entered)
			close(release)
			awaitSignal(t, done)
			awaitSignal(t, done)
		})

		t.Run(operation+" reenters", func(t *testing.T) {
			var depth atomic.Int64
			backend := &lifecycleStrictBackend{}
			service, _ := ratelimit.NewStrictService(backend)
			backend.operation = func(_ string, ctx context.Context, _ any) {
				if depth.Add(1) == 1 {
					invokeLifecycleOperation(t, operation, service, ctx)
				}
				depth.Add(-1)
			}
			invokeLifecycleOperation(t, operation, service, context.Background())
		})

		t.Run(operation+" panic propagates", func(t *testing.T) {
			panicValue := &struct{ operation string }{operation}
			backend := &lifecycleStrictBackend{operation: func(string, context.Context, any) { panic(panicValue) }}
			service, _ := ratelimit.NewStrictService(backend)
			assertPanicIdentity(t, panicValue, func() { invokeLifecycleOperation(t, operation, service, context.Background()) })
		})

		t.Run(operation+" forwards fresh inputs", func(t *testing.T) {
			type contextKey struct{}
			var calls atomic.Int64
			wantOperation := operation
			if wantOperation == "batch" {
				wantOperation = "admit"
			}
			backend := &lifecycleStrictBackend{operation: func(gotOperation string, ctx context.Context, value any) {
				if gotOperation != wantOperation {
					t.Fatalf("operation = %q, want %q", gotOperation, wantOperation)
				}
				call := calls.Add(1)
				if ctx.Value(contextKey{}) != call {
					t.Fatalf("context value on call %d = %v", call, ctx.Value(contextKey{}))
				}
				switch input := value.(type) {
				case ratelimit.Request:
					if input.Cost != uint64(call) {
						t.Fatalf("request cost on call %d = %d", call, input.Cost)
					}
				case ratelimit.LeaseRequest:
					if input.Request.Cost != uint64(call) {
						t.Fatalf("lease request cost on call %d = %d", call, input.Request.Cost)
					}
				case ratelimit.Lease:
					if input.Cost != uint64(call) {
						t.Fatalf("lease cost on call %d = %d", call, input.Cost)
					}
				}
			}}
			service, _ := ratelimit.NewStrictService(backend)
			for call := int64(1); call <= 2; call++ {
				ctx := context.WithValue(context.Background(), contextKey{}, call)
				invokeLifecycleOperationWithCost(t, operation, service, ctx, uint64(call))
			}
			if calls.Load() != 2 {
				t.Fatalf("operation calls = %d", calls.Load())
			}
		})
	}
}

func invokeLifecycleOperation(t *testing.T, operation string, service *ratelimit.StrictService, ctx context.Context) {
	t.Helper()
	invokeLifecycleOperationWithCost(t, operation, service, ctx, 1)
}

func invokeLifecycleOperationWithCost(t *testing.T, operation string, service *ratelimit.StrictService, ctx context.Context, cost uint64) {
	t.Helper()
	switch operation {
	case "admit":
		request := validRequest(t, ratelimit.FailClosed)
		request.Cost = cost
		if _, err := service.Admit(ctx, request); err != nil {
			t.Fatalf("Admit() error = %v", err)
		}
	case "batch":
		request := validRequest(t, ratelimit.FailClosed)
		request.Cost = cost
		if _, err := service.Batch(ctx, ratelimit.BatchRequest{Requests: []ratelimit.Request{request}, Atomicity: ratelimit.AtomicityPerItem}); err != nil {
			t.Fatalf("Batch() error = %v", err)
		}
	case "acquire":
		request := ratelimit.LeaseRequest{Request: concurrencyRequest(t), LeaseID: "lease"}
		request.Request.Cost = cost
		if _, _, err := service.Acquire(ctx, request); err != nil {
			t.Fatalf("Acquire() error = %v", err)
		}
	case "release":
		request := concurrencyRequest(t)
		lease := ratelimit.Lease{
			ID: "lease", Key: request.Key, PolicyID: request.Policy.ID(),
			PolicyRevision: request.Policy.Revision(), Cost: cost,
			ExpiresAt: request.Now.Add(time.Second), Backend: "lifecycle",
		}
		if err := service.Release(ctx, lease); err != nil {
			t.Fatalf("Release() error = %v", err)
		}
	default:
		t.Fatalf("unknown operation %q", operation)
	}
}

func awaitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for callback")
	}
}

func assertNotSignaled(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(25 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-signal:
		t.Fatal("caller returned before callback completed")
	case <-timer.C:
	}
}

func assertPanicIdentity(t *testing.T, want any, call func()) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered != want {
			t.Fatalf("panic = %v, want identity %v", recovered, want)
		}
	}()
	call()
}

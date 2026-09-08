package ratelimittest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

type lockWaitContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (ctx *lockWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

func TestAdmitStrictCancellationInterruptsLockWait(t *testing.T) {
	reference := NewReference()
	request := referenceRequest(t, ratelimit.TokenBucket, 1)
	reference.mu.Lock()
	base, cancel := context.WithCancel(context.Background())
	ctx := &lockWaitContext{Context: base, waiting: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, callErr := reference.AdmitStrict(ctx, request)
		done <- callErr
	}()
	select {
	case <-ctx.waiting:
	case <-time.After(100 * time.Millisecond):
		reference.mu.Unlock()
		<-done
		t.Fatal("strict admission did not enter the lock wait")
	}
	cancel()
	select {
	case callErr := <-done:
		if !errors.Is(callErr, ratelimit.ErrCanceled) {
			t.Fatalf("expected canceled outcome, got %v", callErr)
		}
	case <-time.After(100 * time.Millisecond):
		reference.mu.Unlock()
		<-done
		t.Fatal("strict admission did not stop waiting for the lock")
	}
	reference.mu.Unlock()
}

func TestLeaseStrictCancellationInterruptsLockWait(t *testing.T) {
	reference := NewReference()
	lease, _, err := reference.AcquireStrict(context.Background(), referenceLeaseRequest(t, "owned"))
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range []func(context.Context) error{
		func(ctx context.Context) error {
			_, _, callErr := reference.AcquireStrict(ctx, referenceLeaseRequest(t, "other"))
			return callErr
		},
		func(ctx context.Context) error { return reference.ReleaseStrict(ctx, lease) },
	} {
		reference.mu.Lock()
		base, cancel := context.WithCancel(context.Background())
		ctx := &lockWaitContext{Context: base, waiting: make(chan struct{})}
		done := make(chan error, 1)
		go func() { done <- call(ctx) }()
		select {
		case <-ctx.waiting:
		case <-time.After(100 * time.Millisecond):
			reference.mu.Unlock()
			<-done
			t.Fatal("strict lease operation did not enter the lock wait")
		}
		cancel()
		select {
		case callErr := <-done:
			if !errors.Is(callErr, ratelimit.ErrCanceled) {
				t.Fatalf("expected canceled outcome, got %v", callErr)
			}
		case <-time.After(100 * time.Millisecond):
			reference.mu.Unlock()
			<-done
			t.Fatal("strict lease operation did not stop waiting for the lock")
		}
		reference.mu.Unlock()
	}
}

func referenceRequest(t *testing.T, algorithm ratelimit.Algorithm, cost uint64) ratelimit.Request {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "strict", Revision: "v1", Algorithm: algorithm,
		Capacity: 2, Period: time.Second, MaxCost: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "case", Value: "strict"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ratelimit.Request{Policy: policy, Key: key, Cost: cost, Now: time.Unix(100, 0)}
}

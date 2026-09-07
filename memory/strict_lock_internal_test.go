package memory

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
	store, err := New(Options{MaxKeys: 1, Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	request := internalRequest(t, ratelimit.TokenBucket, 1)
	store.shards[0].mu.Lock()
	base, cancel := context.WithCancel(context.Background())
	ctx := &lockWaitContext{Context: base, waiting: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, callErr := store.AdmitStrict(ctx, request)
		done <- callErr
	}()
	select {
	case <-ctx.waiting:
	case <-time.After(100 * time.Millisecond):
		store.shards[0].mu.Unlock()
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
		store.shards[0].mu.Unlock()
		<-done
		t.Fatal("strict admission did not stop waiting for the lock")
	}
	assertCanceledWaiterDidNotReleaseOwner(t, &store.shards[0].mu)
}

func assertCanceledWaiterDidNotReleaseOwner(t *testing.T, mutex *contextMutex) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := mutex.LockContext(ctx); err == nil {
		mutex.Unlock()
		t.Fatal("canceled waiter released a lock held by another owner")
	}
	mutex.Unlock()
}

package ratelimittest_test

import (
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/rate-limit/memory"
	"github.com/faustbrian/golib/pkg/rate-limit/ratelimittest"
)

func TestMemoryMatchesReferenceModels(t *testing.T) {
	t.Parallel()

	ratelimittest.RunBackendConformance(t, func(t testing.TB) ratelimittest.BackendFixture {
		t.Helper()
		store, err := memory.New(memory.Options{MaxKeys: 64, Shards: 4})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return ratelimittest.BackendFixture{Backend: store, Leases: store}
	})
}

func TestMemoryIsAtomicUnderContention(t *testing.T) {
	t.Parallel()

	ratelimittest.RunBackendAtomicity(t, func(t testing.TB) ratelimittest.BackendFixture {
		t.Helper()
		store, err := memory.New(memory.Options{MaxKeys: 64, Shards: 4})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return ratelimittest.BackendFixture{Backend: store, Leases: store}
	})
}

func TestClockIsDeterministicAndMonotonicByChoice(t *testing.T) {
	t.Parallel()

	clock := ratelimittest.NewClock(time.Unix(10, 0))
	if got := clock.Now(); !got.Equal(time.Unix(10, 0)) {
		t.Fatalf("Now() = %v", got)
	}
	clock.Advance(time.Second)
	if got := clock.Now(); !got.Equal(time.Unix(11, 0)) {
		t.Fatalf("advanced Now() = %v", got)
	}
	clock.Set(time.Unix(5, 0))
	if got := clock.Now(); !got.Equal(time.Unix(5, 0)) {
		t.Fatalf("rolled-back Now() = %v", got)
	}
}

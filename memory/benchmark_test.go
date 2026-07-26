package memory_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"github.com/faustbrian/golib/pkg/rate-limit/memory"
)

func BenchmarkHotKey(b *testing.B) {
	store, err := memory.New(memory.Options{MaxKeys: 1024, Shards: 16})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	request := benchmarkRequest(b, "hot", 1_000_000)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := store.Admit(context.Background(), request); err != nil &&
				!errors.Is(err, ratelimit.ErrRejected) {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkHighCardinality(b *testing.B) {
	store, err := memory.New(memory.Options{MaxKeys: 4096, Shards: 16})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	policy := benchmarkPolicy(b, 10)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		key, err := ratelimit.NewKey(ratelimit.KeySpec{
			Namespace: "bench", Version: "v1",
			Subject: ratelimit.Subject{Kind: "principal", Value: strconv.Itoa(index % 8192)},
			Hash:    true,
		})
		if err != nil {
			b.Fatal(err)
		}
		_, _ = store.Admit(context.Background(), ratelimit.Request{
			Policy: policy, Key: key, Cost: 1, Now: time.Unix(100, 0),
		})
	}
}

func benchmarkRequest(tb testing.TB, value string, capacity uint64) ratelimit.Request {
	tb.Helper()
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "bench", Version: "v1",
		Subject: ratelimit.Subject{Kind: "principal", Value: value}, Hash: true,
	})
	if err != nil {
		tb.Fatal(err)
	}
	return ratelimit.Request{
		Policy: benchmarkPolicy(tb, capacity), Key: key, Cost: 1,
		Now: time.Unix(100, 0),
	}
}

func benchmarkPolicy(tb testing.TB, capacity uint64) ratelimit.Policy {
	tb.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "benchmark", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: capacity, Period: time.Minute, MaxCost: capacity,
	})
	if err != nil {
		tb.Fatal(err)
	}
	return policy
}

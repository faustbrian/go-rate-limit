package ratelimit_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"github.com/faustbrian/golib/pkg/rate-limit/memory"
)

func BenchmarkBatchSizes(b *testing.B) {
	for _, size := range []int{1, 16, 64, 256} {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			store, err := memory.New(memory.Options{MaxKeys: 512, Shards: 16})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })
			service, err := ratelimit.NewService(store)
			if err != nil {
				b.Fatal(err)
			}
			policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
				ID: "batch-benchmark", Revision: "v1",
				Algorithm: ratelimit.FixedWindow, Capacity: 1_000_000,
				Period: time.Minute, MaxCost: 1,
			})
			if err != nil {
				b.Fatal(err)
			}
			key, err := ratelimit.NewKey(ratelimit.KeySpec{
				Namespace: "bench", Version: "v1",
				Subject: ratelimit.Subject{Kind: "case", Value: "batch"},
				Hash:    true,
			})
			if err != nil {
				b.Fatal(err)
			}
			request := ratelimit.Request{
				Policy: policy, Key: key, Cost: 1, Now: time.Unix(100, 0),
			}
			requests := make([]ratelimit.Request, size)
			for index := range requests {
				requests[index] = request
			}
			b.ReportAllocs()
			for b.Loop() {
				_, _ = service.Batch(context.Background(), ratelimit.BatchRequest{
					Requests: requests, Atomicity: ratelimit.AtomicityPerItem,
				})
			}
		})
	}
}

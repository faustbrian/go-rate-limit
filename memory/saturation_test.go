package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit/v2"
	"github.com/faustbrian/go-rate-limit/v2/memory"
)

// Protect emergency budgets against key churn, while retaining the default
// eviction profile and explicit caller-owned idle reclamation.
func TestRejectOnCapacityPreservesConsumptionUntilExplicitSweep(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "strict"}[strict], func(t *testing.T) {
			store, err := memory.New(memory.Options{MaxKeys: 1, Shards: 1, RejectOnCapacity: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			admitRequest := store.Admit
			if strict {
				admitRequest = store.AdmitStrict
			}
			now := time.Unix(1800000000, 0)
			first := requestFor(t, ratelimit.TokenBucket, "first", "v1", 1, 0, time.Minute, 1, now)
			if d, err := admitRequest(context.Background(), first); err != nil || !d.Allowed {
				t.Fatalf("first: %+v %v", d, err)
			}
			second := requestFor(t, ratelimit.TokenBucket, "second", "v1", 1, 0, time.Minute, 1, now)
			if d, err := admitRequest(context.Background(), second); !errors.Is(err, ratelimit.ErrUnavailable) || d.Allowed {
				t.Fatalf("saturation: %+v %v", d, err)
			}
			if d, err := admitRequest(context.Background(), first); !errors.Is(err, ratelimit.ErrRejected) || d.Allowed {
				t.Fatalf("retained consumption: %+v %v", d, err)
			}
			if n, err := store.Sweep(now.Add(59*time.Second), time.Minute); err != nil || n != 0 {
				t.Fatalf("early sweep: %d %v", n, err)
			}
			if n, err := store.Sweep(now.Add(time.Minute), time.Minute); err != nil || n != 1 {
				t.Fatalf("idle sweep: %d %v", n, err)
			}
			second.Now = now.Add(time.Minute)
			if d, err := admitRequest(context.Background(), second); err != nil || !d.Allowed {
				t.Fatalf("reclaimed: %+v %v", d, err)
			}
		})
	}
}

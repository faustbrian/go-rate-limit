package ratelimit_test

import (
	"context"
	"fmt"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"github.com/faustbrian/go-rate-limit/memory"
)

func ExampleNewStrictService() {
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID:          "api-requests",
		Revision:    "v1",
		Algorithm:   ratelimit.TokenBucket,
		Capacity:    2,
		Period:      time.Second,
		Consistency: ratelimit.ConsistencyProcessLocal,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "checkout",
		Version:   "v1",
		Subject:   ratelimit.Subject{Kind: "account", Value: "account-123"},
		Hash:      true,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	backend, err := memory.New(memory.Options{MaxKeys: 1000, Shards: 16})
	if err != nil {
		fmt.Println(err)
		return
	}
	service, err := ratelimit.NewStrictService(backend)
	if err != nil {
		fmt.Println(err)
		return
	}

	decision, err := service.Admit(context.Background(), ratelimit.Request{
		Policy: policy,
		Key:    key,
		Cost:   1,
		Now:    time.Unix(1_000, 0),
	})
	fmt.Println(decision.Allowed, decision.Remaining, err)

	// Output: true 1 <nil>
}

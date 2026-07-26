package valkey

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

type fakeExecutor struct {
	reply []string
	err   error
	keys  []string
	args  []string
}

func (executor *fakeExecutor) exec(_ context.Context, keys, args []string) ([]string, error) {
	executor.keys = append([]string(nil), keys...)
	executor.args = append([]string(nil), args...)
	return executor.reply, executor.err
}

func TestStoreDecodesAtomicDecisionAndUsesOpaqueHashTag(t *testing.T) {
	t.Parallel()

	executor := &fakeExecutor{reply: []string{"1", "7", "10", "11000000", "0", "allowed"}}
	store, err := newStore(executor, Options{Prefix: "rl", Timeout: time.Second, Clock: ClientClock})
	if err != nil {
		t.Fatal(err)
	}
	request := valkeyRequest(t)
	decision, err := store.Admit(context.Background(), request)
	if err != nil {
		t.Fatalf("Admit() error = %v", err)
	}
	if !decision.Allowed || decision.Remaining != 7 || decision.Limit != 10 ||
		!decision.Reset.Equal(time.Unix(11, 0)) || decision.Reason != ratelimit.ReasonAllowed {
		t.Fatalf("decision = %+v", decision)
	}
	if len(executor.keys) != 1 || !strings.HasPrefix(executor.keys[0], "rl:{") ||
		strings.Contains(executor.keys[0], request.Key.String()) ||
		!strings.HasSuffix(executor.keys[0], "}") {
		t.Fatalf("key = %q", executor.keys)
	}
	if executor.args[0] != "1" || executor.args[1] != "token_bucket" ||
		executor.args[2] != "login" || executor.args[3] != "v1" {
		t.Fatalf("args = %q", executor.args)
	}
}

func TestStoreClassifiesRejectionCorruptionAndOutage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		reply []string
		err   error
		want  error
	}{
		{name: "rejected", reply: []string{"0", "0", "10", "11000000", "1000000", "limited"}, want: ratelimit.ErrRejected},
		{name: "corrupt", reply: []string{"1", "broken"}, want: ratelimit.ErrCorrupt},
		{name: "outage", err: errors.New("dial failed password=secret"), want: ratelimit.ErrUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store, err := newStore(&fakeExecutor{reply: test.reply, err: test.err}, Options{Prefix: "rl", Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.Admit(context.Background(), valkeyRequest(t))
			if !errors.Is(err, test.want) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("Admit() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestStoreValidatesConfigurationAndOperations(t *testing.T) {
	t.Parallel()

	if _, err := newStore(nil, Options{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("newStore(nil) error = %v", err)
	}
	store, err := newStore(&fakeExecutor{}, Options{Prefix: "bad:{prefix}", Timeout: time.Second})
	if !errors.Is(err, ratelimit.ErrInvalidPolicy) || store != nil {
		t.Fatalf("unsafe prefix = %v, %v", store, err)
	}
	store, err = newStore(&fakeExecutor{}, Options{Prefix: strings.Repeat("p", 65), Timeout: time.Second})
	if !errors.Is(err, ratelimit.ErrInvalidPolicy) || store != nil {
		t.Fatalf("oversized prefix = %v, %v", store, err)
	}
	store, err = newStore(&fakeExecutor{}, Options{Prefix: "rl", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Admit(context.Background(), ratelimit.Request{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid request error = %v", err)
	}
	concurrency := valkeyRequest(t)
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "login", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 1, MaxCost: 1, Lease: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	concurrency.Policy = policy
	concurrency.Cost = 1
	if _, err := store.Admit(context.Background(), concurrency); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("concurrency Admit() error = %v", err)
	}
}

func TestDecodeLeaseReplyPreservesRetryAndExpiry(t *testing.T) {
	t.Parallel()

	request := valkeyRequest(t)
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "workers", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 2, MaxCost: 2, Lease: time.Second,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Policy, request.Cost = policy, 1
	leaseRequest := ratelimit.LeaseRequest{Request: request, LeaseID: "job-1"}
	lease, decision, err := decodeLeaseReply(
		[]string{"1", "1", "2", "11000000", "0", "allowed", "11000000"},
		leaseRequest,
	)
	if err != nil || !decision.Allowed || lease.ID != "job-1" ||
		!lease.ExpiresAt.Equal(time.Unix(11, 0)) {
		t.Fatalf("decodeLeaseReply() = %+v, %+v, %v", lease, decision, err)
	}
}

func valkeyRequest(t *testing.T) ratelimit.Request {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "login", Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Capacity: 10, Period: time.Second, MaxCost: 10,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "http", Version: "v1",
		Subject: ratelimit.Subject{Kind: "principal", Value: "sensitive"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ratelimit.Request{Policy: policy, Key: key, Cost: 3, Now: time.Unix(10, 0)}
}

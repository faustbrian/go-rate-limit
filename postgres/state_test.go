package postgres

import (
	"errors"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

func TestMutateStatePreservesWeightedTokenArithmetic(t *testing.T) {
	t.Parallel()

	request := postgresTokenRequest(t, time.Unix(10, 0), 3)
	state, decision, err := mutateState(nil, request)
	if err != nil || !decision.Allowed || decision.Remaining != 3 {
		t.Fatalf("first = %+v, %+v, %v", state, decision, err)
	}
	request.Now = request.Now.Add(250 * time.Millisecond)
	state, decision, err = mutateState(state, request)
	if err != nil || !decision.Allowed || decision.Remaining != 1 {
		t.Fatalf("second = %+v, %+v, %v", state, decision, err)
	}
	_, decision, err = mutateState(state, request)
	if !errors.Is(err, ratelimit.ErrRejected) ||
		decision.RetryAfter != 500*time.Millisecond {
		t.Fatalf("rejected = %+v, %v", decision, err)
	}
}

func TestDecodeStateRejectsForeignSchema(t *testing.T) {
	t.Parallel()

	if _, err := decodeState([]byte(`{"schema":2}`)); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("decodeState() error = %v", err)
	}
}

func TestMutateLeasePrunesExpiryAndIsIdempotent(t *testing.T) {
	t.Parallel()

	request := concurrencyLeaseRequest(t, time.Unix(20, 0), "job-1", 1)
	state, lease, decision, err := mutateLease(nil, request, "digest-1")
	if err != nil || !decision.Allowed || lease.ID != "job-1" ||
		decision.Remaining != 1 {
		t.Fatalf("first = %+v, %+v, %+v, %v", state, lease, decision, err)
	}
	_, same, decision, err := mutateLease(state, request, "digest-1")
	if err != nil || same.ExpiresAt != lease.ExpiresAt || decision.Remaining != 1 {
		t.Fatalf("idempotent = %+v, %+v, %v", same, decision, err)
	}
	request.LeaseID = "job-2"
	request.Request.Cost = 2
	if _, _, _, err := mutateLease(state, request, "digest-2"); !errors.Is(err, ratelimit.ErrRejected) {
		t.Fatalf("contended mutateLease() error = %v", err)
	}
	request.Request.Now = lease.ExpiresAt
	if _, _, decision, err := mutateLease(state, request, "digest-2"); err != nil ||
		!decision.Allowed {
		t.Fatalf("expired mutateLease() = %+v, %v", decision, err)
	}
}

func TestStateClockRollbackIsClamped(t *testing.T) {
	t.Parallel()

	request := postgresRequest(t)
	request.Now = time.Unix(120, 0)
	request.Cost = 5
	state, _, err := mutateState(nil, request)
	if err != nil {
		t.Fatal(err)
	}
	request.Now = time.Unix(60, 0)
	request.Cost = 1
	if _, _, err := mutateState(state, request); !errors.Is(err, ratelimit.ErrRejected) {
		t.Fatalf("rollback mutateState() error = %v", err)
	}
}

func postgresTokenRequest(t *testing.T, now time.Time, cost uint64) ratelimit.Request {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "token", Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Capacity: 4, Burst: 2, Period: time.Second, MaxCost: 6,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "principal", Value: "42"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ratelimit.Request{Policy: policy, Key: key, Cost: cost, Now: now}
}

func concurrencyLeaseRequest(t *testing.T, now time.Time, id string, cost uint64) ratelimit.LeaseRequest {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "workers", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 2, MaxCost: 2, Lease: time.Second,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "queue", Value: "jobs"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ratelimit.LeaseRequest{
		Request: ratelimit.Request{Policy: policy, Key: key, Cost: cost, Now: now},
		LeaseID: id,
	}
}

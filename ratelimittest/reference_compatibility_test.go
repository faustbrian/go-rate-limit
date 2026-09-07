package ratelimittest

import (
	"context"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

func TestReferenceLegacyRevisionTransitionPreservesReleasedTokenState(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0).UTC()
	reference := NewReference()
	first := legacyReferenceRequest(t, "legacy-revision", "v1", ratelimit.TokenBucket, 10, now)
	if decision, err := reference.Admit(context.Background(), first); err != nil || decision.Remaining != 9 {
		t.Fatalf("initial Admit() = %+v, %v", decision, err)
	}
	second := legacyReferenceRequest(t, "legacy-revision", "v2", ratelimit.TokenBucket, 2, now)
	if decision, err := reference.Admit(context.Background(), second); err != nil || decision.Remaining != 8 {
		t.Fatalf("revision Admit() = %+v, %v; want released remaining 8", decision, err)
	}
}

func TestReferenceLegacyAlgorithmTransitionPreservesReleasedStateInterpretation(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0).UTC()
	reference := NewReference()
	first := legacyReferenceRequest(t, "legacy-algorithm", "v1", ratelimit.TokenBucket, 2, now)
	if _, err := reference.Admit(context.Background(), first); err != nil {
		t.Fatalf("initial Admit() error = %v", err)
	}
	second := legacyReferenceRequest(t, "legacy-algorithm", "v2", ratelimit.FixedWindow, 2, now)
	if decision, err := reference.Admit(context.Background(), second); err != nil || !decision.Allowed || decision.Remaining != 1 {
		t.Fatalf("algorithm-transition Admit() = %+v, %v", decision, err)
	}
}

func legacyReferenceRequest(t *testing.T, id, revision string, algorithm ratelimit.Algorithm, capacity uint64, now time.Time) ratelimit.Request {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: id, Revision: revision, Algorithm: algorithm,
		Capacity: capacity, Period: time.Minute, MaxCost: capacity,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "case", Value: id}, Hash: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ratelimit.Request{Policy: policy, Key: key, Cost: 1, Now: now}
}

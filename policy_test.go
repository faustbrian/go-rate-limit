package ratelimit_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

func TestNewPolicyValidatesAndCopiesIdentity(t *testing.T) {
	t.Parallel()

	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID:          "login",
		Revision:    "v2",
		Algorithm:   ratelimit.TokenBucket,
		Capacity:    10,
		Burst:       2,
		Period:      time.Minute,
		MaxCost:     3,
		FailureMode: ratelimit.FailClosed,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}

	if policy.ID() != "login" || policy.Revision() != "v2" {
		t.Fatalf("identity = %q/%q", policy.ID(), policy.Revision())
	}
	if policy.Limit() != 12 || policy.Capacity() != 10 || policy.Burst() != 2 {
		t.Fatalf("limits = %d/%d/%d", policy.Limit(), policy.Capacity(), policy.Burst())
	}

	invalid := []ratelimit.PolicySpec{
		{},
		{ID: strings.Repeat("a", 65), Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 1, Period: time.Second},
		{ID: "login\nprincipal", Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 1, Period: time.Second},
		{ID: "login", Revision: strings.Repeat("r", 65), Algorithm: ratelimit.TokenBucket, Capacity: 1, Period: time.Second},
		{ID: "login", Revision: "secret revision", Algorithm: ratelimit.TokenBucket, Capacity: 1, Period: time.Second},
		{ID: "x", Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 1},
		{ID: "x", Revision: "v1", Algorithm: ratelimit.Algorithm("unknown"), Capacity: 1, Period: time.Second},
		{ID: "x", Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 1, Period: time.Second, MaxCost: 2},
		{ID: "x", Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 1, Period: time.Microsecond + 1},
		{ID: "x", Revision: "v1", Algorithm: ratelimit.Concurrency, Capacity: 1, Lease: time.Microsecond + 1},
		{ID: "x", Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 9_007_199_254_740_992, Period: time.Microsecond},
		{ID: "x", Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 1_000_000_000_000, Period: time.Second},
		{ID: "x", Revision: "v1", Algorithm: ratelimit.Concurrency, Capacity: 1025, Lease: time.Second},
	}
	for _, spec := range invalid {
		_, err := ratelimit.NewPolicy(spec)
		if !errors.Is(err, ratelimit.ErrInvalidPolicy) {
			t.Fatalf("NewPolicy(%+v) error = %v", spec, err)
		}
	}
}

func TestNewKeyBoundsAndHashesSensitiveSubjects(t *testing.T) {
	t.Parallel()

	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "http",
		Version:   "v1",
		Subject:   ratelimit.Subject{Kind: "principal", Value: "secret@example.test"},
		Hash:      true,
	})
	if err != nil {
		t.Fatalf("NewKey() error = %v", err)
	}
	if key.String() == "" || key.String() == "secret@example.test" {
		t.Fatalf("persisted key leaked subject: %q", key.String())
	}
	if key.SubjectKind() != "principal" {
		t.Fatalf("SubjectKind() = %q", key.SubjectKind())
	}

	_, err = ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "http",
		Version:   "v1",
		Subject:   ratelimit.Subject{Kind: "ip", Value: string(make([]byte, ratelimit.MaxSubjectBytes+1))},
	})
	if !errors.Is(err, ratelimit.ErrInvalidKey) {
		t.Fatalf("oversized key error = %v", err)
	}
}

func TestRequestValidationRejectsImplicitTimeAndCost(t *testing.T) {
	t.Parallel()

	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "x", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 2, Period: time.Second, MaxCost: 2,
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

	for _, request := range []ratelimit.Request{
		{Policy: policy, Key: key, Cost: 0, Now: time.Unix(1, 0)},
		{Policy: policy, Key: key, Cost: 3, Now: time.Unix(1, 0)},
		{Policy: policy, Key: key, Cost: 1},
		{Policy: policy, Key: key, Cost: 1, Now: time.UnixMicro(9_007_199_254_740_992)},
		{Policy: policy, Key: key, Cost: 1, Now: time.UnixMicro(9_007_199_254_740_991)},
		{Policy: policy, Key: key, Cost: 1, Now: time.UnixMicro(-9_007_199_254_740_992)},
	} {
		if err := request.Validate(); !errors.Is(err, ratelimit.ErrInvalidRequest) {
			t.Fatalf("Validate() error = %v", err)
		}
	}
}

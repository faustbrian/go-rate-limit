package ratelimitprincipal_test

import (
	"errors"
	"testing"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"github.com/faustbrian/go-rate-limit/ratelimitprincipal"
)

type principal struct{ subject string }

type sequencedPrincipal struct {
	values []string
	calls  int
}

func (principal *sequencedPrincipal) Subject() string {
	value := principal.values[principal.calls]
	principal.calls++
	return value
}

func (principal principal) Subject() string { return principal.subject }

func TestKeyAcceptsAuthenticationPrincipalContractWithoutDependency(t *testing.T) {
	t.Parallel()

	key, err := ratelimitprincipal.Key(principal{subject: "user-42"})
	if err != nil || key.SubjectKind() != "principal" ||
		key.String() == "" || key.String() == "user-42" {
		t.Fatalf("Key() = %q, %v", key.String(), err)
	}
	if _, err := ratelimitprincipal.Key(principal{}); !errors.Is(err, ratelimit.ErrInvalidKey) {
		t.Fatalf("anonymous Key() error = %v", err)
	}
	if _, err := ratelimitprincipal.Key(nil); !errors.Is(err, ratelimit.ErrInvalidKey) {
		t.Fatalf("nil Key() error = %v", err)
	}
}

func TestLegacyKeyRetainsReleasedSubjectCardinality(t *testing.T) {
	empty := &sequencedPrincipal{values: []string{""}}
	if _, err := ratelimitprincipal.Key(empty); !errors.Is(err, ratelimit.ErrInvalidKey) || empty.calls != 1 {
		t.Fatalf("empty Key() calls = %d, error = %v", empty.calls, err)
	}
	success := &sequencedPrincipal{values: []string{"first", "second"}}
	key, err := ratelimitprincipal.Key(success)
	want, _ := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "auth", Version: "v1",
		Subject: ratelimit.Subject{Kind: "principal", Value: "second"}, Hash: true,
	})
	if err != nil || success.calls != 2 || key != want {
		t.Fatalf("success Key() = %q, %v, calls=%d", key.String(), err, success.calls)
	}
}

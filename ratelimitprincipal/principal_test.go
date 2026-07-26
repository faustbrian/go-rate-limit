package ratelimitprincipal_test

import (
	"errors"
	"testing"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"github.com/faustbrian/golib/pkg/rate-limit/ratelimitprincipal"
)

type principal struct{ subject string }

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
}

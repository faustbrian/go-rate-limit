//nolint:staticcheck // Explicit types compile-check the complete public API signatures.
package ratelimitauthentication_test

import (
	"testing"

	ratelimit "github.com/faustbrian/go-rate-limit"
	ratelimitauthentication "github.com/faustbrian/go-rate-limit/adapters/authentication"
)

func TestPublicAPIExists(t *testing.T) {
	t.Parallel()

	var _ ratelimitauthentication.Principal
	var _ func(ratelimitauthentication.Principal) (ratelimit.Key, error) = ratelimitauthentication.Key
}

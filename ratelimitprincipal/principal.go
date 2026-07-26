package ratelimitprincipal

import (
	"fmt"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

// Principal is the narrow identity contract required for key derivation.
type Principal interface {
	// Subject returns the stable authenticated subject identifier.
	Subject() string
}

// Key derives a bounded, irreversibly hashed key from principal.
func Key(principal Principal) (ratelimit.Key, error) {
	if principal == nil || principal.Subject() == "" {
		return ratelimit.Key{}, fmt.Errorf("%w: authenticated principal is required", ratelimit.ErrInvalidKey)
	}
	return ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "auth", Version: "v1",
		Subject: ratelimit.Subject{Kind: "principal", Value: principal.Subject()},
		Hash:    true,
	})
}

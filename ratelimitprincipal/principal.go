package ratelimitprincipal

import (
	"fmt"
	"reflect"

	ratelimit "github.com/faustbrian/go-rate-limit/v2"
)

// Principal is the narrow identity contract required for key derivation.
type Principal interface {
	// Subject returns the stable authenticated subject identifier.
	Subject() string
}

// KeyStrict rejects nil/typed-nil principals and snapshots Subject once.
// Subject panics propagate; identity authenticity remains the caller's concern.
func KeyStrict(principal Principal) (ratelimit.Key, error) {
	if nilInterface(principal) {
		return ratelimit.Key{}, fmt.Errorf("%w: principal is required", ratelimit.ErrInvalidPolicy)
	}
	subject := principal.Subject()
	if subject == "" {
		return ratelimit.Key{}, fmt.Errorf("%w: authenticated principal is required", ratelimit.ErrInvalidKey)
	}
	return ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "auth", Version: "v1",
		Subject: ratelimit.Subject{Kind: "principal", Value: subject}, Hash: true,
	})
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	current := reflect.ValueOf(value)
	switch current.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return current.IsNil()
	default:
		return false
	}
}

// Key derives a bounded SHA-256 key, checking only a nil interface. It calls
// Subject once for validation and again for derivation; values can differ.
// Typed-nil principals and callback panics retain their compatibility behavior.
// KeyStrict snapshots once; neither helper authenticates the identity.
func Key(principal Principal) (ratelimit.Key, error) {
	if principal == nil {
		return ratelimit.Key{}, fmt.Errorf("%w: authenticated principal is required", ratelimit.ErrInvalidKey)
	}
	if principal.Subject() == "" {
		return ratelimit.Key{}, fmt.Errorf("%w: authenticated principal is required", ratelimit.ErrInvalidKey)
	}
	return ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "auth", Version: "v1",
		Subject: ratelimit.Subject{Kind: "principal", Value: principal.Subject()},
		Hash:    true,
	})
}

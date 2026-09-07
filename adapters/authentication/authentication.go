package ratelimitauthentication

import (
	"fmt"
	"reflect"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

// Principal is the narrow identity contract required for key derivation.
type Principal interface{ Subject() string }

// Key derives a bounded hashed key from one validated subject snapshot.
func Key(principal Principal) (ratelimit.Key, error) {
	if nilInterface(principal) {
		return ratelimit.Key{}, fmt.Errorf("%w: principal is required", ratelimit.ErrInvalidPolicy)
	}
	subject := principal.Subject()
	if subject == "" {
		return ratelimit.Key{}, fmt.Errorf("%w: authenticated principal is required", ratelimit.ErrInvalidKey)
	}
	return ratelimit.NewKey(ratelimit.KeySpec{Namespace: "auth", Version: "v1", Subject: ratelimit.Subject{Kind: "principal", Value: subject}, Hash: true})
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

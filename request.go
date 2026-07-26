package ratelimit

import (
	"fmt"
	"time"
)

// Request is a weighted admission attempt evaluated at an explicit time.
type Request struct {
	// Policy contains immutable admission semantics.
	Policy Policy
	// Key identifies the bounded subject state.
	Key Key
	// Cost is the positive number of units requested.
	Cost uint64
	// Now is the caller-supplied current time used by client-clock backends.
	Now time.Time
}

// Validate checks that all request inputs are present and bounded by Policy.
func (r Request) Validate() error {
	if r.Policy.id == "" || r.Key.value == "" || r.Now.IsZero() ||
		r.Cost == 0 || r.Cost > r.Policy.maxCost {
		return fmt.Errorf("%w: policy, key, time, and bounded cost are required", ErrInvalidRequest)
	}
	micros := r.Now.UnixMicro()
	if micros < -int64(maxExactInteger) {
		return fmt.Errorf("%w: time exceeds exact backend range", ErrInvalidRequest)
	}
	duration := r.Policy.period
	if r.Policy.algorithm == Concurrency {
		duration = r.Policy.lease
	}
	if micros > int64(maxExactInteger)-duration.Microseconds() {
		return fmt.Errorf("%w: reset exceeds exact backend range", ErrInvalidRequest)
	}
	return nil
}

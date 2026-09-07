package ratelimittest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

// AdmitStrict evaluates one request with context-aware lock acquisition.
func (reference *Reference) AdmitStrict(ctx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	if reference == nil || reference.states == nil {
		return ratelimit.Decision{}, fmt.Errorf("%w: backend is nil or uninitialized", ratelimit.ErrInvalidPolicy)
	}
	if nilInterface(ctx) {
		return ratelimit.Decision{}, fmt.Errorf("%w: context is required", ratelimit.ErrInvalidRequest)
	}
	if err := request.Validate(); err != nil {
		return ratelimit.Decision{}, err
	}
	if err := referenceStrictContext(ctx); err != nil {
		return ratelimit.Decision{}, err
	}
	if request.Policy.Algorithm() == ratelimit.Concurrency {
		return ratelimit.Decision{}, ratelimit.ErrUnsupported
	}
	request.Now = request.Now.Truncate(time.Microsecond).UTC()
	if err := reference.mu.LockContext(ctx); err != nil {
		return ratelimit.Decision{}, referenceStrictError(ctx, err)
	}
	defer reference.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ratelimit.Decision{}, referenceStrictError(ctx, err)
	}
	effective := request.Now
	key := request.Policy.ID() + "\x00" + request.Key.String()
	if current := reference.states[key]; current != nil {
		if (request.Policy.Algorithm() == ratelimit.FixedWindow || request.Policy.Algorithm() == ratelimit.SlidingWindow) &&
			current.period != 0 && current.period != request.Policy.Period() {
			return ratelimit.Decision{}, ratelimit.ErrCorrupt
		}
		if current.observed.After(effective) {
			effective = current.observed
		}
	}
	if err := validateStrictEffectiveRange(effective, request.Policy.Period()); err != nil {
		return ratelimit.Decision{}, err
	}
	decision, err := reference.admitLocked(request, true)
	if current := reference.states[key]; current != nil {
		effective = current.observed
	}
	return strictDecisionResult(decision, err, effective)
}

// AcquireStrict obtains one lease with context-aware lock acquisition.
func (reference *Reference) AcquireStrict(ctx context.Context, request ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
	if reference == nil || reference.states == nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, fmt.Errorf("%w: backend is nil or uninitialized", ratelimit.ErrInvalidPolicy)
	}
	if nilInterface(ctx) {
		return ratelimit.Lease{}, ratelimit.Decision{}, fmt.Errorf("%w: context is required", ratelimit.ErrInvalidRequest)
	}
	if err := request.Validate(); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	request.Request.Now = request.Request.Now.Truncate(time.Microsecond).UTC()
	if err := reference.mu.LockContext(ctx); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, referenceStrictError(ctx, err)
	}
	defer reference.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, referenceStrictError(ctx, err)
	}
	effective := request.Request.Now
	if current := reference.states[request.Request.Policy.ID()+"\x00"+request.Request.Key.String()]; current != nil && current.observed.After(effective) {
		effective = current.observed
	}
	if err := validateStrictEffectiveRange(effective, request.Request.Policy.LeaseDuration()); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	lease, decision, err := reference.acquireLocked(request, true)
	if current := reference.states[request.Request.Policy.ID()+"\x00"+request.Request.Key.String()]; current != nil {
		effective = current.observed
	}
	return strictLeaseResult(lease, decision, err, effective)
}

func strictDecisionResult(decision ratelimit.Decision, err error, effective time.Time) (ratelimit.Decision, error) {
	if timeErr := validateStrictDecisionTime(decision, effective); timeErr != nil {
		return ratelimit.Decision{}, timeErr
	}
	return decision, err
}

func strictLeaseResult(lease ratelimit.Lease, decision ratelimit.Decision, err error, effective time.Time) (ratelimit.Lease, ratelimit.Decision, error) {
	if timeErr := validateStrictLeaseTime(lease, decision, effective); timeErr != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, timeErr
	}
	return lease, decision, err
}

// ReleaseStrict releases one lease with context-aware lock acquisition.
func (reference *Reference) ReleaseStrict(ctx context.Context, lease ratelimit.Lease) error {
	if reference == nil || reference.states == nil {
		return fmt.Errorf("%w: backend is nil or uninitialized", ratelimit.ErrInvalidPolicy)
	}
	if nilInterface(ctx) {
		return fmt.Errorf("%w: context is required", ratelimit.ErrInvalidRequest)
	}
	if lease.ID == "" || lease.PolicyID == "" || lease.Key.String() == "" || lease.Cost == 0 || lease.ExpiresAt.IsZero() {
		return ratelimit.ErrInvalidRequest
	}
	if err := referenceStrictContext(ctx); err != nil {
		return err
	}
	if err := reference.mu.LockContext(ctx); err != nil {
		return referenceStrictError(ctx, err)
	}
	defer reference.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return referenceStrictError(ctx, err)
	}
	return reference.releaseLocked(lease)
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

func referenceStrictContext(ctx context.Context) error {
	if ctx.Err() == context.Canceled {
		return ratelimit.CanceledOutcome()
	}
	if ctx.Err() == context.DeadlineExceeded {
		return ratelimit.DeadlineOutcome()
	}
	return nil
}

func referenceStrictError(ctx context.Context, err error) error {
	if errors.Is(err, context.Canceled) || ctx.Err() == context.Canceled {
		return ratelimit.CanceledOutcome()
	}
	if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
		return ratelimit.DeadlineOutcome()
	}
	return err
}

func validateStrictDecisionTime(decision ratelimit.Decision, effective time.Time) error {
	if !decision.Reset.IsZero() && decision.Reset.Before(effective) {
		return ratelimit.ErrCorrupt
	}
	return nil
}

func validateStrictEffectiveRange(effective time.Time, duration time.Duration) error {
	const maximumExactMicros = int64(9_007_199_254_740_991)
	micros := effective.UnixMicro()
	durationMicros := duration.Microseconds()
	if micros < -maximumExactMicros || durationMicros <= 0 || micros > maximumExactMicros-durationMicros {
		return ratelimit.ErrOverflow
	}
	return nil
}

func validateStrictLeaseTime(lease ratelimit.Lease, decision ratelimit.Decision, effective time.Time) error {
	if err := validateStrictDecisionTime(decision, effective); err != nil {
		return err
	}
	if !lease.ExpiresAt.IsZero() && (!lease.ExpiresAt.After(effective) || !lease.ExpiresAt.Equal(decision.Reset)) {
		return ratelimit.ErrCorrupt
	}
	return nil
}

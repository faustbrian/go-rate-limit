package ratelimit

import "errors"

var (
	// ErrRejected indicates a valid admission request exceeded its limit.
	ErrRejected = errors.New("rate limit rejected")
	// ErrInvalidPolicy indicates policy construction failed validation.
	ErrInvalidPolicy = errors.New("invalid rate limit policy")
	// ErrInvalidKey indicates key derivation received unsafe or oversized input.
	ErrInvalidKey = errors.New("invalid rate limit key")
	// ErrInvalidRequest indicates an admission or lease request is malformed.
	ErrInvalidRequest = errors.New("invalid rate limit request")
	// ErrUnavailable indicates the selected backend cannot currently decide.
	ErrUnavailable = errors.New("rate limit backend unavailable")
	// ErrDeadline indicates cancellation or deadline expiry interrupted a decision.
	ErrDeadline = errors.New("rate limit deadline exceeded")
	// ErrOverflow indicates bounded integer arithmetic could not represent a result.
	ErrOverflow = errors.New("rate limit arithmetic overflow")
	// ErrCorrupt indicates persisted state violates backend invariants.
	ErrCorrupt = errors.New("rate limit state corrupt")
	// ErrUnsupported indicates the backend cannot guarantee an operation's semantics.
	ErrUnsupported = errors.New("rate limit operation unsupported")
	// ErrLeaseNotFound indicates a concurrency lease does not exist or expired.
	ErrLeaseNotFound = errors.New("rate limit lease not found")
	// ErrLeaseNotOwned indicates a lease belongs to different policy or backend state.
	ErrLeaseNotOwned = errors.New("rate limit lease not owned")
)

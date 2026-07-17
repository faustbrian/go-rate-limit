package ratelimit

import "time"

// Reason is a stable, non-sensitive classification of an admission decision.
type Reason string

const (
	// ReasonAllowed means capacity was consumed successfully.
	ReasonAllowed Reason = "allowed"
	// ReasonLimited means insufficient capacity was available.
	ReasonLimited Reason = "limited"
	// ReasonFailOpen means a backend failure was admitted by policy.
	ReasonFailOpen Reason = "fail_open"
	// ReasonBackendUnavailable means fail-closed policy rejected an outage.
	ReasonBackendUnavailable Reason = "backend_unavailable"
)

// Decision describes the complete, observable result of admission.
type Decision struct {
	// Allowed reports whether the operation may proceed.
	Allowed bool
	// Remaining is the immediately available whole-unit capacity.
	Remaining uint64
	// Limit is the configured Capacity plus Burst.
	Limit uint64
	// Reset is the earliest useful replenishment or lease expiry time.
	Reset time.Time
	// RetryAfter is the minimum suggested delay after rejection.
	RetryAfter time.Duration
	// Reason is a stable machine-readable classification.
	Reason Reason
	// Backend identifies the implementation that made the decision.
	Backend string
	// PolicyRevision identifies the admission semantics used.
	PolicyRevision string
}

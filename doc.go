// Package ratelimit defines transport-neutral admission policies, requests,
// decisions, observations, batching, and concurrency leases.
//
// The package performs no sleeping or retry orchestration. Backends implement
// atomic state changes, while Service applies the policy's explicit failure
// mode and emits observations. StrictService validates explicit backend outcome
// declarations and never fails open on an unknown mutation result. Service
// retains its released compatibility behavior and does not validate those
// declarations. Neither service owns backend shutdown or application retries.
//
// Policy consistency is a declaration, not backend selection or verification.
// Callers choose the authority and supply time, non-secret policy labels and
// keys. Leases account for capacity; they do not authenticate callers or fence
// application side effects after expiry.
package ratelimit

// Package ratelimit defines transport-neutral admission policies, requests,
// decisions, observations, batching, and concurrency leases.
//
// The package performs no sleeping or retry orchestration. Backends implement
// atomic state changes, while Service applies the policy's explicit failure
// mode and emits bounded observations.
package ratelimit

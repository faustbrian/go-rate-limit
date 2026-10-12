// Package ratelimittelemetry adapts bounded observations to OpenTelemetry
// metrics using caller-controlled policy and subject-kind labels.
//
// The legacy instrumentation scope and attributes remain distinct from the
// canonical adapter. NewStrict checks typed nil, but provider panics propagate.
// Nil/zero observers retain legacy panic behavior, and directly supplied labels
// are not validated. Export and provider shutdown remain caller-owned.
//
// Deprecated: use github.com/faustbrian/go-rate-limit/v2/adapters/otel. This package remains supported through the documented compatibility interval.
package ratelimittelemetry

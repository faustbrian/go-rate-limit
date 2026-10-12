// Package ratelimitlog adapts bounded observations to structured slog records.
//
// Calls use a borrowed logger and context.Background, with no asynchronous queue
// or shutdown ownership. Direct labels are unvalidated, and nil/zero observers
// retain legacy panic behavior; the canonical adapter makes those calls no-ops.
//
// Deprecated: use github.com/faustbrian/go-rate-limit/v2/adapters/slog. This package remains supported through the documented compatibility interval.
package ratelimitlog

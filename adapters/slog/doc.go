// Package ratelimitslog records bounded rate-limit observations with slog at
// github.com/faustbrian/go-rate-limit/v3/adapters/slog.
//
// The logger is borrowed and calls are synchronous with context.Background.
// Direct observation labels require caller validation; only error classification
// avoids raw error text. Nil/zero observers are no-ops. Service observer isolation
// contains panics, but direct sink calls propagate them and may block.
package ratelimitslog

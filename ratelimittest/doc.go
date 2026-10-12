// Package ratelimittest provides deterministic clocks, rational reference
// models, fixtures, and reusable cross-backend conformance tests.
//
// These helpers characterize selected in-process scenarios, not official
// protocol conformance, restart durability or performance. Factories supply fresh
// state and register their own cleanup. Calls use background contexts, so backend
// budgets and the enclosing test timeout must bound completion. Reference maps
// are unbounded test state; sliding-window logic shares the 16-segment design.
package ratelimittest

package ratelimittest

import (
	"sync"
	"time"
)

// Clock is a concurrency-safe deterministic test clock.
type Clock struct {
	mu  sync.RWMutex
	now time.Time
}

// NewClock constructs a Clock at now.
func NewClock(now time.Time) *Clock {
	return &Clock{now: now}
}

// Now returns the current test time.
func (clock *Clock) Now() time.Time {
	clock.mu.RLock()
	defer clock.mu.RUnlock()
	return clock.now
}

// Set replaces the current test time, including backward jumps.
func (clock *Clock) Set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

// Advance adds duration to the current test time.
func (clock *Clock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

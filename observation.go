package ratelimit

import "time"

// Observation contains bounded decision metadata for logging and telemetry.
type Observation struct {
	// PolicyID is the stable policy identity and must not contain credentials.
	PolicyID string
	// SubjectKind is the bounded key category, never the raw subject value.
	SubjectKind string
	// Decision is the completed admission result.
	Decision Decision
	// Err is the stable error returned to the caller, if any.
	Err error
	// Duration is local service processing time.
	Duration time.Duration
}

// Observer consumes bounded admission observations.
type Observer interface {
	Observe(Observation)
}

// ObserveFunc adapts a function to Observer.
type ObserveFunc func(Observation)

// Observe calls function with observation.
func (function ObserveFunc) Observe(observation Observation) {
	function(observation)
}

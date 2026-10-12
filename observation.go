package ratelimit

import "time"

// Observation contains admission metadata. Services omit the raw key/subject.
// Direct construction is not validated: callers must bound labels and avoid
// secrets, especially when passing observations directly to adapters.
type Observation struct {
	// PolicyID is the stable policy identity and must not contain credentials.
	PolicyID string
	// SubjectKind is the bounded key category, never the raw subject value.
	SubjectKind string
	// Decision is the completed admission result.
	Decision Decision
	// Err is the stable error returned to the caller, if any.
	Err error
	// Duration is local service processing time before observer fan-out. It
	// excludes downstream application work and observer blocking.
	Duration time.Duration
}

// Observer consumes bounded admission observations.
type Observer interface {
	// Observe consumes one value synchronously. Implementations own sink
	// concurrency and blocking; services isolate panics but impose no timeout.
	Observe(Observation)
}

// ObserveFunc adapts a function to Observer.
type ObserveFunc func(Observation)

// Observe calls function with observation; a nil function panics. Services
// isolate this panic, whereas direct calls propagate it.
func (function ObserveFunc) Observe(observation Observation) {
	function(observation)
}

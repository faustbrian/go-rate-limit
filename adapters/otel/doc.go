// Package ratelimitotel records bounded rate-limit metrics with OpenTelemetry at
// github.com/faustbrian/go-rate-limit/v2/adapters/otel.
//
// Instrument creation and recording are synchronous; no exporter, global
// provider or shutdown is owned. Direct observations can contain arbitrary labels
// and are not sanitized by these instruments. Only error classification omits
// raw error text. Nil/zero observers are no-ops; sink panics otherwise propagate.
package ratelimitotel

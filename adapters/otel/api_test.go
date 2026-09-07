//nolint:staticcheck // Explicit types compile-check the complete public API signatures.
package ratelimitotel_test

import (
	"testing"

	ratelimit "github.com/faustbrian/go-rate-limit"
	ratelimitotel "github.com/faustbrian/go-rate-limit/adapters/otel"
	"go.opentelemetry.io/otel/metric"
)

func TestPublicAPIExists(t *testing.T) {
	t.Parallel()

	var _ = ratelimitotel.Options{MeterProvider: metric.MeterProvider(nil)}
	var _ func(ratelimitotel.Options) (*ratelimitotel.Observer, error) = ratelimitotel.New
	var observer *ratelimitotel.Observer
	var _ func(ratelimit.Observation) = observer.Observe
	var _ ratelimit.Observer = observer
}

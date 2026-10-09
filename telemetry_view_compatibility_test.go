//nolint:staticcheck // Characterize both supported telemetry scopes.
//lint:file-ignore SA1019 Compatibility characterization includes the legacy adapter.
package ratelimit_test

import (
	"context"
	"math"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit/v2"
	ratelimitotel "github.com/faustbrian/go-rate-limit/v2/adapters/otel"
	legacytelemetry "github.com/faustbrian/go-rate-limit/v2/ratelimittelemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestOTelFilteredViewsPreserveAggregation(t *testing.T) {
	for _, adapter := range []struct {
		scope     string
		construct func(metric.MeterProvider) (ratelimit.Observer, error)
	}{
		{"github.com/faustbrian/go-rate-limit/v2/adapters/otel", func(provider metric.MeterProvider) (ratelimit.Observer, error) {
			return ratelimitotel.New(ratelimitotel.Options{MeterProvider: provider})
		}},
		{"github.com/faustbrian/go-rate-limit/v2/ratelimittelemetry", func(provider metric.MeterProvider) (ratelimit.Observer, error) {
			return legacytelemetry.New(legacytelemetry.Options{MeterProvider: provider})
		}},
	} {
		for _, view := range []struct {
			name string
			key  attribute.Key
			want map[string]telemetrySeries
		}{
			{"policy", "rate_limit.policy.id", map[string]telemetrySeries{"A": {2, .004}, "B": {1, .007}}},
			{"backend", "rate_limit.backend", map[string]telemetrySeries{"memory": {3, .011}}},
			{"all-dropped", "", map[string]telemetrySeries{"": {3, .011}}},
		} {
			t.Run(adapter.scope+"/"+view.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				reader := sdkmetric.NewManualReader()
				provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithView(
					sdkmetric.NewView(sdkmetric.Instrument{Name: "rate_limit.*"}, sdkmetric.Stream{
						AttributeFilter: func(value attribute.KeyValue) bool { return value.Key == view.key },
					}),
				))
				defer func() {
					if err := provider.Shutdown(ctx); err != nil {
						t.Error(err)
					}
				}()
				observer, err := adapter.construct(provider)
				if err != nil {
					t.Fatal(err)
				}
				for _, observation := range []struct {
					policy   string
					duration time.Duration
				}{
					{"A", time.Millisecond}, {"B", 7 * time.Millisecond}, {"A", 3 * time.Millisecond},
				} {
					observer.Observe(ratelimit.Observation{
						PolicyID: observation.policy, SubjectKind: "principal", Duration: observation.duration,
						Decision: ratelimit.Decision{Backend: "memory", Reason: ratelimit.ReasonAllowed, PolicyRevision: "v1"},
					})
				}
				var destination metricdata.ResourceMetrics
				for range 2 {
					if err := reader.Collect(ctx, &destination); err != nil {
						t.Fatal(err)
					}
					assertFilteredTelemetry(t, destination, adapter.scope, view.key, view.want)
				}
			})
		}
	}
}

func assertFilteredTelemetry(t *testing.T, result metricdata.ResourceMetrics, scope string, key attribute.Key, want map[string]telemetrySeries) {
	t.Helper()
	if len(result.ScopeMetrics) != 1 || result.ScopeMetrics[0].Scope.Name != scope || len(result.ScopeMetrics[0].Metrics) != 2 {
		t.Fatalf("filtered scopes/instruments = %+v", result.ScopeMetrics)
	}
	seenInstruments := make(map[string]bool)
	for _, instrument := range result.ScopeMetrics[0].Metrics {
		if seenInstruments[instrument.Name] {
			t.Fatalf("duplicate instrument %q", instrument.Name)
		}
		seenInstruments[instrument.Name] = true
		seen := make(map[string]bool)
		check := func(attributes attribute.Set, count uint64, sum float64, histogram bool) {
			t.Helper()
			identity := ""
			if key == "" {
				if attributes.Len() != 0 {
					t.Fatalf("all-dropped attributes = %v", attributes)
				}
			} else {
				value, present := attributes.Value(key)
				if !present || attributes.Len() != 1 {
					t.Fatalf("retained attributes = %v, want only %q", attributes, key)
				}
				identity = value.AsString()
			}
			expected, exists := want[identity]
			if !exists || seen[identity] || count != expected.count || histogram && (math.IsNaN(sum) || math.IsInf(sum, 0) || math.Abs(sum-expected.sum) > 1e-12) {
				t.Fatalf("%s series=%q count=%d sum=%g, want %+v", instrument.Name, identity, count, sum, expected)
			}
			seen[identity] = true
		}
		switch instrument.Name {
		case "rate_limit.decisions":
			data, ok := instrument.Data.(metricdata.Sum[int64])
			if !ok || !data.IsMonotonic || data.Temporality != metricdata.CumulativeTemporality {
				t.Fatalf("decision aggregation = %+v", instrument)
			}
			for _, point := range data.DataPoints {
				if point.Value < 0 {
					t.Fatalf("negative count: %d", point.Value)
				}
				check(point.Attributes, uint64(point.Value), 0, false)
			}
		case "rate_limit.decision.duration":
			data, ok := instrument.Data.(metricdata.Histogram[float64])
			if !ok || data.Temporality != metricdata.CumulativeTemporality {
				t.Fatalf("duration aggregation = %+v", instrument)
			}
			for _, point := range data.DataPoints {
				check(point.Attributes, point.Count, point.Sum, true)
			}
		default:
			t.Fatalf("unexpected instrument %q", instrument.Name)
		}
		if len(seen) != len(want) {
			t.Fatalf("%s series = %v, want %v", instrument.Name, seen, want)
		}
	}
}

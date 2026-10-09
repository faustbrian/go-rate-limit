//nolint:staticcheck // The supported legacy adapter has its own metric scope.
//lint:file-ignore SA1019 Compatibility characterization includes the legacy adapter.
package ratelimit_test

import (
	"context"
	"math"
	"sync"
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

func TestOTelCollectionPreservesIndependentCumulativeSeries(t *testing.T) {
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
		t.Run(adapter.scope, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			defer func() {
				if err := provider.Shutdown(ctx); err != nil {
					t.Error(err)
				}
			}()
			observer, err := adapter.construct(provider)
			if err != nil {
				t.Fatal(err)
			}
			observe := func(policy string, duration time.Duration) {
				observer.Observe(ratelimit.Observation{
					PolicyID: policy, SubjectKind: "principal", Duration: duration,
					Decision: ratelimit.Decision{Backend: "memory", Reason: ratelimit.ReasonAllowed, PolicyRevision: "v1"},
				})
			}
			var destination metricdata.ResourceMetrics
			collect := func(want map[string]telemetrySeries) {
				t.Helper()
				if err := reader.Collect(ctx, &destination); err != nil {
					t.Fatal(err)
				}
				assertCumulativeTelemetry(t, destination, adapter.scope, want)
			}
			observe("A", time.Millisecond)
			collect(map[string]telemetrySeries{"A": {1, 0.001}})
			observe("B", 7*time.Millisecond)
			observe("A", 3*time.Millisecond)
			want := map[string]telemetrySeries{"A": {2, 0.004}, "B": {1, 0.007}}
			collect(want)
			collect(want)

			const writers, perWriter = 4, 32
			var joined sync.WaitGroup
			start := make(chan struct{})
			for range writers {
				joined.Go(func() {
					<-start
					for range perWriter {
						observe("C", 2*time.Millisecond)
					}
				})
			}
			close(start)
			var collectionErr error
			for range 4 {
				if err := reader.Collect(ctx, &destination); err != nil {
					collectionErr = err
					break
				}
			}
			joined.Wait()
			if collectionErr != nil {
				t.Fatal(collectionErr)
			}
			want["C"] = telemetrySeries{writers * perWriter, 0.256}
			collect(want)
			collect(want)
		})
	}
}

type telemetrySeries struct {
	count uint64
	sum   float64
}

func assertCumulativeTelemetry(t *testing.T, result metricdata.ResourceMetrics, scope string, want map[string]telemetrySeries) {
	t.Helper()
	if len(result.ScopeMetrics) != 1 || result.ScopeMetrics[0].Scope.Name != scope || len(result.ScopeMetrics[0].Metrics) != 2 {
		t.Fatalf("collected scopes/instruments = %+v", result.ScopeMetrics)
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
			value, present := attributes.Value("rate_limit.policy.id")
			policy := value.AsString()
			expected, exists := want[policy]
			invalidSum := histogram && (math.IsNaN(sum) || math.IsInf(sum, 0) || math.Abs(sum-expected.sum) > 1e-12)
			if !present || !exists || seen[policy] || count != expected.count || invalidSum {
				t.Fatalf("%s policy=%q count=%d sum=%g, want %+v", instrument.Name, policy, count, sum, expected)
			}
			seen[policy] = true
			for key, expected := range map[attribute.Key]string{
				"rate_limit.subject.kind": "principal", "rate_limit.backend": "memory",
				"rate_limit.reason": "allowed", "rate_limit.policy.revision": "v1",
			} {
				value, present := attributes.Value(key)
				if !present || value.AsString() != expected {
					t.Fatalf("attribute %q = %v/%v, want %q", key, value, present, expected)
				}
			}
			attributeCount := 5
			if scope == "github.com/faustbrian/go-rate-limit/v2/adapters/otel" {
				attributeCount++
				value, present := attributes.Value("rate_limit.error.kind")
				if !present || value.AsString() != "none" {
					t.Fatalf("successor error kind = %v/%v", value, present)
				}
			}
			if attributes.Len() != attributeCount {
				t.Fatalf("unexpected metric attributes = %v", attributes)
			}
		}
		switch instrument.Name {
		case "rate_limit.decisions":
			data, ok := instrument.Data.(metricdata.Sum[int64])
			if !ok || !data.IsMonotonic || data.Temporality != metricdata.CumulativeTemporality || instrument.Unit != "{decision}" || instrument.Description != "Rate limit admission decisions" {
				t.Fatalf("decision instrument = %+v", instrument)
			}
			for _, point := range data.DataPoints {
				if point.Value < 0 {
					t.Fatalf("negative decision count: %d", point.Value)
				}
				check(point.Attributes, uint64(point.Value), 0, false)
			}
		case "rate_limit.decision.duration":
			data, ok := instrument.Data.(metricdata.Histogram[float64])
			if !ok || data.Temporality != metricdata.CumulativeTemporality || instrument.Unit != "s" || instrument.Description != "Rate limit decision latency" {
				t.Fatalf("duration instrument = %+v", instrument)
			}
			for _, point := range data.DataPoints {
				var buckets uint64
				for _, count := range point.BucketCounts {
					buckets += count
				}
				if buckets != point.Count {
					t.Fatalf("histogram buckets = %d, count = %d", buckets, point.Count)
				}
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

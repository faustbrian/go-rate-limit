package ratelimittelemetry_test

import (
	"context"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"github.com/faustbrian/golib/pkg/rate-limit/ratelimittelemetry"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestObserverRecordsBoundedDecisionMetrics(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	observer, err := ratelimittelemetry.New(ratelimittelemetry.Options{
		MeterProvider: provider,
	})
	if err != nil {
		t.Fatal(err)
	}
	observer.Observe(ratelimit.Observation{
		PolicyID: "login", SubjectKind: "principal",
		Decision: ratelimit.Decision{
			Allowed: false, Backend: "valkey", Reason: ratelimit.ReasonLimited,
			PolicyRevision: "v1",
		},
		Err: ratelimit.ErrRejected, Duration: 2 * time.Millisecond,
	})
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	if len(collected.ScopeMetrics) != 1 || len(collected.ScopeMetrics[0].Metrics) != 2 {
		t.Fatalf("metrics = %+v", collected.ScopeMetrics)
	}
	for _, metric := range collected.ScopeMetrics[0].Metrics {
		if metric.Name != "rate_limit.decisions" && metric.Name != "rate_limit.decision.duration" {
			t.Fatalf("unexpected metric %q", metric.Name)
		}
	}
}

package ratelimittelemetry

import (
	"context"
	"fmt"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const scopeName = "github.com/faustbrian/golib/pkg/rate-limit/ratelimittelemetry"

// Options configures OpenTelemetry metric instruments.
type Options struct {
	// MeterProvider owns the created counter and latency histogram.
	MeterProvider metric.MeterProvider
}

// Observer records bounded decision counts and latency.
type Observer struct {
	decisions metric.Int64Counter
	duration  metric.Float64Histogram
}

// New constructs metric instruments using MeterProvider.
func New(options Options) (*Observer, error) {
	if options.MeterProvider == nil {
		return nil, fmt.Errorf("%w: meter provider is required", ratelimit.ErrInvalidPolicy)
	}
	meter := options.MeterProvider.Meter(scopeName)
	decisions, err := meter.Int64Counter(
		"rate_limit.decisions",
		metric.WithDescription("Rate limit admission decisions"),
		metric.WithUnit("{decision}"),
	)
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram(
		"rate_limit.decision.duration",
		metric.WithDescription("Rate limit decision latency"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	return &Observer{decisions: decisions, duration: duration}, nil
}

// Observe records one bounded decision and its duration.
func (observer *Observer) Observe(observation ratelimit.Observation) {
	attributes := metric.WithAttributes(
		attribute.String("rate_limit.policy.id", observation.PolicyID),
		attribute.String("rate_limit.policy.revision", observation.Decision.PolicyRevision),
		attribute.String("rate_limit.subject.kind", observation.SubjectKind),
		attribute.String("rate_limit.backend", observation.Decision.Backend),
		attribute.String("rate_limit.reason", string(observation.Decision.Reason)),
	)
	ctx := context.Background()
	observer.decisions.Add(ctx, 1, attributes)
	observer.duration.Record(ctx, observation.Duration.Seconds(), attributes)
}

var _ ratelimit.Observer = (*Observer)(nil)

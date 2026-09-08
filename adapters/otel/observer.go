package ratelimitotel

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const scopeName = "github.com/faustbrian/go-rate-limit/adapters/otel"

// Options configures OpenTelemetry metric instruments.
type Options struct {
	// MeterProvider is used during New to create instruments and is not retained.
	MeterProvider metric.MeterProvider
}

// Observer records bounded decision counts and latency.
type Observer struct {
	decisions metric.Int64Counter
	duration  metric.Float64Histogram
}

// New constructs instruments after panic-safe provider validation.
func New(options Options) (*Observer, error) {
	if nilInterface(options.MeterProvider) {
		return nil, fmt.Errorf("%w: meter provider is required", ratelimit.ErrInvalidPolicy)
	}
	meter := options.MeterProvider.Meter(scopeName)
	decisions, err := meter.Int64Counter("rate_limit.decisions", metric.WithDescription("Rate limit admission decisions"), metric.WithUnit("{decision}"))
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram("rate_limit.decision.duration", metric.WithDescription("Rate limit decision latency"), metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	return &Observer{decisions: decisions, duration: duration}, nil
}

// Observe records one bounded decision and its duration.
func (observer *Observer) Observe(observation ratelimit.Observation) {
	if observer == nil || observer.decisions == nil || observer.duration == nil {
		return
	}
	attributes := metric.WithAttributes(attribute.String("rate_limit.policy.id", observation.PolicyID), attribute.String("rate_limit.policy.revision", observation.Decision.PolicyRevision), attribute.String("rate_limit.subject.kind", observation.SubjectKind), attribute.String("rate_limit.backend", observation.Decision.Backend), attribute.String("rate_limit.reason", string(observation.Decision.Reason)), attribute.String("rate_limit.error.kind", errorKind(observation.Err)))
	ctx := context.Background()
	observer.decisions.Add(ctx, 1, attributes)
	observer.duration.Record(ctx, observation.Duration.Seconds(), attributes)
}

func errorKind(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, ratelimit.ErrOutcomeUnknown):
		return "outcome_unknown"
	case errors.Is(err, ratelimit.ErrCanceled):
		return "canceled"
	case errors.Is(err, ratelimit.ErrDeadline):
		return "deadline"
	case errors.Is(err, ratelimit.ErrRejected):
		return "rejected"
	case errors.Is(err, ratelimit.ErrUnavailable):
		return "unavailable"
	case errors.Is(err, ratelimit.ErrOverflow):
		return "overflow"
	case errors.Is(err, ratelimit.ErrCorrupt):
		return "corrupt"
	case errors.Is(err, ratelimit.ErrUnsupported):
		return "unsupported"
	case errors.Is(err, ratelimit.ErrInvalidPolicy), errors.Is(err, ratelimit.ErrInvalidKey), errors.Is(err, ratelimit.ErrInvalidRequest):
		return "invalid"
	default:
		return "internal"
	}
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

var _ ratelimit.Observer = (*Observer)(nil)

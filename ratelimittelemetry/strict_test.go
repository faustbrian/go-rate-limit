package ratelimittelemetry_test

import (
	"errors"
	"testing"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"github.com/faustbrian/go-rate-limit/ratelimittelemetry"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

type nilProvider struct{ metric.MeterProvider }

func TestNewStrictRejectsTypedNil(t *testing.T) {
	var provider *nilProvider
	observer, err := ratelimittelemetry.NewStrict(ratelimittelemetry.Options{MeterProvider: provider})
	if observer != nil || !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("NewStrict() = %v, %v", observer, err)
	}
}

func TestNewStrictAcceptsValueProvider(t *testing.T) {
	observer, err := ratelimittelemetry.NewStrict(ratelimittelemetry.Options{MeterProvider: metricnoop.NewMeterProvider()})
	if err != nil || observer == nil {
		t.Fatalf("NewStrict() = %v, %v", observer, err)
	}
	if observer, err := ratelimittelemetry.NewStrict(ratelimittelemetry.Options{}); observer != nil || !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("empty = %v, %v", observer, err)
	}
}

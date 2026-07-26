package ratelimittelemetry

import (
	"errors"
	"testing"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

type errorProvider struct {
	metric.MeterProvider
	meter metric.Meter
}

func (provider errorProvider) Meter(string, ...metric.MeterOption) metric.Meter {
	return provider.meter
}

type errorMeter struct {
	metric.Meter
	counterErr   error
	histogramErr error
}

func (meter errorMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	if meter.counterErr != nil {
		return nil, meter.counterErr
	}
	return meter.Meter.Int64Counter("fallback")
}

func (meter errorMeter) Float64Histogram(string, ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	if meter.histogramErr != nil {
		return nil, meter.histogramErr
	}
	return meter.Meter.Float64Histogram("fallback")
}

func TestTelemetryConfigurationAndInstrumentErrors(t *testing.T) {
	t.Parallel()

	if _, err := New(Options{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("New(empty) error = %v", err)
	}
	base := metricnoop.NewMeterProvider()
	want := errors.New("instrument")
	provider := errorProvider{
		MeterProvider: base,
		meter:         errorMeter{Meter: base.Meter("test"), counterErr: want},
	}
	if _, err := New(Options{MeterProvider: provider}); !errors.Is(err, want) {
		t.Fatalf("counter New() error = %v", err)
	}
	provider.meter = errorMeter{Meter: base.Meter("test"), histogramErr: want}
	if _, err := New(Options{MeterProvider: provider}); !errors.Is(err, want) {
		t.Fatalf("histogram New() error = %v", err)
	}
}

package ratelimitotel

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type nilProvider struct{ metric.MeterProvider }
type errorProvider struct {
	metric.MeterProvider
	meter metric.Meter
}

func (provider errorProvider) Meter(string, ...metric.MeterOption) metric.Meter {
	return provider.meter
}

type errorMeter struct {
	metric.Meter
	counterErr, histogramErr error
}

type callbackProvider struct {
	metric.MeterProvider
	meter metric.Meter
}

func (provider callbackProvider) Meter(string, ...metric.MeterOption) metric.Meter {
	return provider.meter
}

type callbackMeter struct {
	metric.Meter
	counter   metric.Int64Counter
	histogram metric.Float64Histogram
}

func (meter callbackMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return meter.counter, nil
}
func (meter callbackMeter) Float64Histogram(string, ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	return meter.histogram, nil
}

type callbackCounter struct {
	metric.Int64Counter
	add func(context.Context, int64, ...metric.AddOption)
}

func (counter callbackCounter) Add(ctx context.Context, value int64, options ...metric.AddOption) {
	counter.add(ctx, value, options...)
}

type callbackHistogram struct {
	metric.Float64Histogram
	record func(context.Context, float64, ...metric.RecordOption)
}

type panicProvider struct{ metric.MeterProvider }

func (panicProvider) Meter(string, ...metric.MeterOption) metric.Meter { panic("provider panic") }

type panicMeter struct {
	metric.Meter
	stage string
}

func (meter panicMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	if meter.stage == "counter" {
		panic("counter constructor panic")
	}
	return callbackCounter{add: func(context.Context, int64, ...metric.AddOption) {}}, nil
}
func (meter panicMeter) Float64Histogram(string, ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	if meter.stage == "histogram" {
		panic("histogram constructor panic")
	}
	return callbackHistogram{record: func(context.Context, float64, ...metric.RecordOption) {}}, nil
}

type successBackend struct{}

func (successBackend) Name() string { return "test" }
func (successBackend) Admit(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
	panic("legacy")
}
func (successBackend) AdmitStrict(_ context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	return ratelimit.Decision{Allowed: true, Limit: request.Policy.Limit(), Remaining: request.Policy.Limit() - request.Cost, Reset: request.Now.Add(time.Second), Reason: ratelimit.ReasonAllowed}, nil
}

func (histogram callbackHistogram) Record(ctx context.Context, value float64, options ...metric.RecordOption) {
	histogram.record(ctx, value, options...)
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

func TestObserverContract(t *testing.T) {
	if _, err := New(Options{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("New() error = %v", err)
	}
	var typedNil *nilProvider
	if _, err := New(Options{MeterProvider: typedNil}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("typed nil error = %v", err)
	}
	base := metricnoop.NewMeterProvider()
	want := errors.New("instrument")
	provider := errorProvider{MeterProvider: base, meter: errorMeter{Meter: base.Meter("test"), counterErr: want}}
	if _, err := New(Options{MeterProvider: provider}); !errors.Is(err, want) {
		t.Fatalf("counter error = %v", err)
	}
	provider.meter = errorMeter{Meter: base.Meter("test"), histogramErr: want}
	if _, err := New(Options{MeterProvider: provider}); !errors.Is(err, want) {
		t.Fatalf("histogram error = %v", err)
	}
	var absent *Observer
	absent.Observe(ratelimit.Observation{})
	reader := sdkmetric.NewManualReader()
	sdk := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		if err := sdk.Shutdown(context.Background()); err != nil {
			t.Errorf("meter provider shutdown: %v", err)
		}
	})
	observer, err := New(Options{MeterProvider: sdk})
	if err != nil {
		t.Fatal(err)
	}
	observer.Observe(ratelimit.Observation{PolicyID: "login", SubjectKind: "principal", Decision: ratelimit.Decision{Backend: "valkey", Reason: ratelimit.ReasonBackendUnavailable, PolicyRevision: "v1"}, Err: ratelimit.ErrUnavailable, Duration: time.Millisecond})
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	if len(collected.ScopeMetrics) != 1 || collected.ScopeMetrics[0].Scope.Name != scopeName || len(collected.ScopeMetrics[0].Metrics) != 2 {
		t.Fatalf("metrics = %+v", collected.ScopeMetrics)
	}
	foundDecisionMetric := false
	for _, measured := range collected.ScopeMetrics[0].Metrics {
		if measured.Name != "rate_limit.decisions" {
			continue
		}
		foundDecisionMetric = true
		sum, ok := measured.Data.(metricdata.Sum[int64])
		if !ok || len(sum.DataPoints) != 1 {
			t.Fatalf("decision metric = %+v", measured.Data)
		}
		kind, ok := sum.DataPoints[0].Attributes.Value("rate_limit.error.kind")
		if !ok || kind.AsString() != "unavailable" {
			t.Fatalf("rate_limit.error.kind = %v, present=%v", kind, ok)
		}
	}
	if !foundDecisionMetric {
		t.Fatal("rate_limit.decisions metric is absent")
	}
	tests := []struct {
		err  error
		want string
	}{{nil, "none"}, {ratelimit.ErrOutcomeUnknown, "outcome_unknown"}, {ratelimit.ErrCanceled, "canceled"}, {ratelimit.ErrDeadline, "deadline"}, {ratelimit.ErrRejected, "rejected"}, {ratelimit.ErrUnavailable, "unavailable"}, {ratelimit.ErrOverflow, "overflow"}, {ratelimit.ErrCorrupt, "corrupt"}, {ratelimit.ErrUnsupported, "unsupported"}, {ratelimit.ErrInvalidPolicy, "invalid"}, {ratelimit.ErrInvalidKey, "invalid"}, {ratelimit.ErrInvalidRequest, "invalid"}, {errors.New("other"), "internal"}}
	for _, test := range tests {
		if got := errorKind(test.err); got != test.want {
			t.Fatalf("errorKind(%v)=%q", test.err, got)
		}
	}
}

func TestObserverInstrumentLifecycle(t *testing.T) {
	observation := ratelimit.Observation{PolicyID: "policy", Decision: ratelimit.Decision{Reason: ratelimit.ReasonAllowed}}
	newObserver := func(counter metric.Int64Counter, histogram metric.Float64Histogram) *Observer {
		provider := callbackProvider{MeterProvider: metricnoop.NewMeterProvider(), meter: callbackMeter{Meter: metricnoop.NewMeterProvider().Meter("fallback"), counter: counter, histogram: histogram}}
		observer, err := New(Options{MeterProvider: provider})
		if err != nil {
			t.Fatal(err)
		}
		return observer
	}
	noHistogram := callbackHistogram{record: func(context.Context, float64, ...metric.RecordOption) {}}
	t.Run("blocking and caller synchronous", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		var wrongContext atomic.Bool
		observer := newObserver(callbackCounter{add: func(ctx context.Context, _ int64, _ ...metric.AddOption) {
			if ctx.Done() != nil {
				wrongContext.Store(true)
			}
			close(entered)
			<-release
		}}, noHistogram)
		done := make(chan struct{})
		go func() { observer.Observe(observation); close(done) }()
		<-entered
		select {
		case <-done:
			t.Fatal("instrument did not block observer caller")
		default:
		}
		close(release)
		<-done
		if wrongContext.Load() {
			t.Fatal("observation context is not background")
		}
	})
	t.Run("panic propagates", func(t *testing.T) {
		observer := newObserver(callbackCounter{add: func(context.Context, int64, ...metric.AddOption) { panic("instrument panic") }}, noHistogram)
		defer func() {
			if recover() != "instrument panic" {
				t.Fatal("instrument panic did not propagate")
			}
		}()
		observer.Observe(observation)
	})
	t.Run("constructor panics propagate", func(t *testing.T) {
		for _, test := range []struct {
			name     string
			provider metric.MeterProvider
			want     string
		}{
			{name: "provider", provider: panicProvider{}, want: "provider panic"},
			{name: "counter", provider: callbackProvider{MeterProvider: metricnoop.NewMeterProvider(), meter: panicMeter{stage: "counter"}}, want: "counter constructor panic"},
			{name: "histogram", provider: callbackProvider{MeterProvider: metricnoop.NewMeterProvider(), meter: panicMeter{stage: "histogram"}}, want: "histogram constructor panic"},
		} {
			t.Run(test.name, func(t *testing.T) {
				defer func() {
					if recover() != test.want {
						t.Fatal("constructor panic did not propagate")
					}
				}()
				_, _ = New(Options{MeterProvider: test.provider})
			})
		}
	})
	t.Run("service isolates instrument panic", func(t *testing.T) {
		observer := newObserver(callbackCounter{add: func(context.Context, int64, ...metric.AddOption) { panic("instrument panic") }}, noHistogram)
		var later atomic.Int64
		service, err := ratelimit.NewStrictService(successBackend{}, observer, ratelimit.ObserveFunc(func(ratelimit.Observation) { later.Add(1) }))
		if err != nil {
			t.Fatal(err)
		}
		policy, _ := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: "policy", Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 1, Period: time.Second, MaxCost: 1})
		key, _ := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "test", Version: "v1", Subject: ratelimit.Subject{Kind: "principal", Value: "p"}, Hash: true})
		if _, err := service.Admit(context.Background(), ratelimit.Request{Policy: policy, Key: key, Cost: 1, Now: time.Unix(100, 0)}); err != nil || later.Load() != 1 {
			t.Fatalf("service isolation=%v later=%d", err, later.Load())
		}
	})
	t.Run("reentry", func(t *testing.T) {
		var calls atomic.Int64
		var observer *Observer
		counter := callbackCounter{add: func(context.Context, int64, ...metric.AddOption) {
			if calls.Add(1) == 1 {
				observer.Observe(observation) //nolint:contextcheck // Observer re-entry has no context-bearing API.
			}
		}}
		observer = newObserver(counter, noHistogram)
		observer.Observe(observation)
		if calls.Load() != 2 {
			t.Fatalf("reentrant calls=%d", calls.Load())
		}
	})
	t.Run("concurrent", func(t *testing.T) {
		var counts atomic.Int64
		var durations atomic.Int64
		observer := newObserver(callbackCounter{add: func(context.Context, int64, ...metric.AddOption) { counts.Add(1) }}, callbackHistogram{record: func(context.Context, float64, ...metric.RecordOption) { durations.Add(1) }})
		var group sync.WaitGroup
		for range 16 {
			group.Add(1)
			go func() { defer group.Done(); observer.Observe(observation) }()
		}
		group.Wait()
		if counts.Load() != 16 || durations.Load() != 16 {
			t.Fatalf("concurrent calls=%d/%d", counts.Load(), durations.Load())
		}
	})
}

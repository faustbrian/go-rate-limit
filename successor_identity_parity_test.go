//nolint:staticcheck // This parity test intentionally imports deprecated compatibility packages.
//lint:file-ignore SA1019 Compatibility parity requires deprecated packages.
package ratelimit_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http/httptest"
	"net/netip"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	ratelimitauthentication "github.com/faustbrian/go-rate-limit/adapters/authentication"
	ratelimithttp "github.com/faustbrian/go-rate-limit/adapters/http"
	ratelimitotel "github.com/faustbrian/go-rate-limit/adapters/otel"
	ratelimitqueue "github.com/faustbrian/go-rate-limit/adapters/queue"
	ratelimitslog "github.com/faustbrian/go-rate-limit/adapters/slog"
	legacyhttp "github.com/faustbrian/go-rate-limit/ratelimithttp"
	legacylog "github.com/faustbrian/go-rate-limit/ratelimitlog"
	legacyprincipal "github.com/faustbrian/go-rate-limit/ratelimitprincipal"
	legacyqueue "github.com/faustbrian/go-rate-limit/ratelimitqueue"
	legacytelemetry "github.com/faustbrian/go-rate-limit/ratelimittelemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	legacyHTTPPath         = "github.com/faustbrian/go-rate-limit/ratelimithttp"
	successorHTTPPath      = "github.com/faustbrian/go-rate-limit/adapters/http"
	legacyLogPath          = "github.com/faustbrian/go-rate-limit/ratelimitlog"
	successorLogPath       = "github.com/faustbrian/go-rate-limit/adapters/slog"
	legacyPrincipalPath    = "github.com/faustbrian/go-rate-limit/ratelimitprincipal"
	successorAuthPath      = "github.com/faustbrian/go-rate-limit/adapters/authentication"
	legacyQueuePath        = "github.com/faustbrian/go-rate-limit/ratelimitqueue"
	successorQueuePath     = "github.com/faustbrian/go-rate-limit/adapters/queue"
	legacyTelemetryPath    = "github.com/faustbrian/go-rate-limit/ratelimittelemetry"
	successorTelemetryPath = "github.com/faustbrian/go-rate-limit/adapters/otel"
)

func TestSuccessorNamedTypesOwnDistinctPackageIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                      string
		legacy, successor         reflect.Type
		legacyPath, successorPath string
	}{
		{"http ClientIPOptions", reflect.TypeOf(legacyhttp.ClientIPOptions{}), reflect.TypeOf(ratelimithttp.ClientIPOptions{}), legacyHTTPPath, successorHTTPPath},
		{"http ClientIPExtractor", reflect.TypeOf((*legacyhttp.ClientIPExtractor)(nil)).Elem(), reflect.TypeOf((*ratelimithttp.ClientIPExtractor)(nil)).Elem(), legacyHTTPPath, successorHTTPPath},
		{"http Options", reflect.TypeOf(legacyhttp.Options{}), reflect.TypeOf(ratelimithttp.Options{}), legacyHTTPPath, successorHTTPPath},
		{"http Middleware", reflect.TypeOf(legacyhttp.Middleware(nil)), reflect.TypeOf((*ratelimithttp.Middleware)(nil)).Elem(), legacyHTTPPath, successorHTTPPath},
		{"slog Options", reflect.TypeOf(legacylog.Options{}), reflect.TypeOf(ratelimitslog.Options{}), legacyLogPath, successorLogPath},
		{"slog Observer", reflect.TypeOf((*legacylog.Observer)(nil)).Elem(), reflect.TypeOf((*ratelimitslog.Observer)(nil)).Elem(), legacyLogPath, successorLogPath},
		{"authentication Principal", reflect.TypeOf((*legacyprincipal.Principal)(nil)).Elem(), reflect.TypeOf((*ratelimitauthentication.Principal)(nil)).Elem(), legacyPrincipalPath, successorAuthPath},
		{"queue Message", reflect.TypeOf(legacyqueue.Message{}), reflect.TypeOf(ratelimitqueue.Message{}), legacyQueuePath, successorQueuePath},
		{"queue Handler", reflect.TypeOf((*legacyqueue.Handler)(nil)).Elem(), reflect.TypeOf((*ratelimitqueue.Handler)(nil)).Elem(), legacyQueuePath, successorQueuePath},
		{"queue HandlerFunc", reflect.TypeOf(legacyqueue.HandlerFunc(nil)), reflect.TypeOf(ratelimitqueue.HandlerFunc(nil)), legacyQueuePath, successorQueuePath},
		{"queue SubjectFunc", reflect.TypeOf(legacyqueue.SubjectFunc(nil)), reflect.TypeOf(ratelimitqueue.SubjectFunc(nil)), legacyQueuePath, successorQueuePath},
		{"queue Options", reflect.TypeOf(legacyqueue.Options{}), reflect.TypeOf(ratelimitqueue.Options{}), legacyQueuePath, successorQueuePath},
		{"queue Middleware", reflect.TypeOf(legacyqueue.Middleware(nil)), reflect.TypeOf((*ratelimitqueue.Middleware)(nil)).Elem(), legacyQueuePath, successorQueuePath},
		{"queue Deferred", reflect.TypeOf((*legacyqueue.Deferred)(nil)).Elem(), reflect.TypeOf((*ratelimitqueue.Deferred)(nil)).Elem(), legacyQueuePath, successorQueuePath},
		{"otel Options", reflect.TypeOf(legacytelemetry.Options{}), reflect.TypeOf(ratelimitotel.Options{}), legacyTelemetryPath, successorTelemetryPath},
		{"otel Observer", reflect.TypeOf((*legacytelemetry.Observer)(nil)).Elem(), reflect.TypeOf((*ratelimitotel.Observer)(nil)).Elem(), legacyTelemetryPath, successorTelemetryPath},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.legacy.Name() != test.successor.Name() || test.legacy.PkgPath() != test.legacyPath ||
				test.successor.PkgPath() != test.successorPath || test.legacy == test.successor {
				t.Fatalf("legacy=%s.%s successor=%s.%s", test.legacy.PkgPath(), test.legacy.Name(), test.successor.PkgPath(), test.successor.Name())
			}
		})
	}
}

func TestHTTPAndQueueSuccessorsPreserveSharedDomainBehavior(t *testing.T) {
	t.Parallel()

	trusted := netip.MustParsePrefix("10.0.0.0/8")
	legacyExtractor, legacyErr := legacyhttp.NewClientIPExtractor(legacyhttp.ClientIPOptions{TrustedProxies: []netip.Prefix{trusted}})
	successorExtractor, successorErr := ratelimithttp.NewClientIPExtractor(ratelimithttp.ClientIPOptions{TrustedProxies: []netip.Prefix{trusted}})
	if legacyErr != nil || successorErr != nil {
		t.Fatalf("extractors = %v, %v", legacyErr, successorErr)
	}
	legacyRequest := httptest.NewRequestWithContext(context.Background(), "GET", "http://example.test", nil)
	legacyRequest.RemoteAddr = "10.0.0.1:80"
	legacyRequest.Header.Set("X-Forwarded-For", "198.51.100.1, 10.0.0.2")
	successorRequest := legacyRequest.Clone(legacyRequest.Context())
	legacyAddress, legacyErr := legacyExtractor.ClientIP(legacyRequest)
	successorAddress, successorErr := successorExtractor.ClientIP(successorRequest)
	if legacyErr != nil || successorErr != nil || legacyAddress != successorAddress {
		t.Fatalf("client IP parity = %s/%v, %s/%v", legacyAddress, legacyErr, successorAddress, successorErr)
	}

	legacyTenant, legacyErr := legacyqueue.ByQueueAndTenant()(legacyqueue.Message{Queue: "ab", Tenant: "c"})
	successorTenant, successorErr := ratelimitqueue.ByQueueAndTenant()(ratelimitqueue.Message{Queue: "ab", Tenant: "c"})
	if legacyErr != nil || successorErr != nil || legacyTenant != successorTenant {
		t.Fatalf("queue tenant parity = %+v/%v, %+v/%v", legacyTenant, legacyErr, successorTenant, successorErr)
	}
	legacyPrincipal, legacyErr := legacyqueue.ByPrincipal()(legacyqueue.Message{Principal: "principal"})
	successorPrincipal, successorErr := ratelimitqueue.ByPrincipal()(ratelimitqueue.Message{Principal: "principal"})
	if legacyErr != nil || successorErr != nil || legacyPrincipal != successorPrincipal {
		t.Fatalf("queue principal parity = %+v/%v, %+v/%v", legacyPrincipal, legacyErr, successorPrincipal, successorErr)
	}
	if (&legacyqueue.Deferred{}).Error() != (&ratelimitqueue.Deferred{}).Error() {
		t.Fatal("queue deferral messages diverged")
	}
}

type parityPrincipal struct {
	values []string
	calls  int
}

func (principal *parityPrincipal) Subject() string {
	value := principal.values[min(principal.calls, len(principal.values)-1)]
	principal.calls++
	return value
}

func TestAuthenticationSuccessorMatchesStrictLegacyAndCharacterizesLegacyCalls(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		value    string
		wantErr  error
		wantText string
	}{
		{name: "valid", value: "principal"},
		{name: "empty", wantErr: ratelimit.ErrInvalidKey, wantText: "invalid rate limit key: authenticated principal is required"},
		{name: "oversized", value: strings.Repeat("a", 257), wantErr: ratelimit.ErrInvalidKey, wantText: "invalid rate limit key: invalid or oversized component"},
	} {
		t.Run(test.name, func(t *testing.T) {
			legacyValue := &parityPrincipal{values: []string{test.value}}
			successorValue := &parityPrincipal{values: []string{test.value}}
			legacyKey, legacyErr := legacyprincipal.KeyStrict(legacyValue)
			successorKey, successorErr := ratelimitauthentication.Key(successorValue)
			if legacyKey != successorKey || !errors.Is(legacyErr, test.wantErr) || !errors.Is(successorErr, test.wantErr) ||
				legacyValue.calls != 1 || successorValue.calls != 1 ||
				(test.wantText != "" && (legacyErr.Error() != test.wantText || successorErr.Error() != test.wantText)) {
				t.Fatalf("legacy=%q/%v/%d successor=%q/%v/%d", legacyKey.String(), legacyErr, legacyValue.calls, successorKey.String(), successorErr, successorValue.calls)
			}
		})
	}
	var legacyNil *parityPrincipal
	var successorNil *parityPrincipal
	legacyKey, legacyErr := legacyprincipal.KeyStrict(legacyNil)
	successorKey, successorErr := ratelimitauthentication.Key(successorNil)
	if legacyKey != (ratelimit.Key{}) || successorKey != (ratelimit.Key{}) ||
		!errors.Is(legacyErr, ratelimit.ErrInvalidPolicy) || !errors.Is(successorErr, ratelimit.ErrInvalidPolicy) ||
		legacyErr.Error() != "invalid rate limit policy: principal is required" || successorErr.Error() != legacyErr.Error() {
		t.Fatalf("typed nil parity = %q/%v, %q/%v", legacyKey.String(), legacyErr, successorKey.String(), successorErr)
	}

	emptyLegacy := &parityPrincipal{values: []string{""}}
	if _, err := legacyprincipal.Key(emptyLegacy); !errors.Is(err, ratelimit.ErrInvalidKey) || emptyLegacy.calls != 1 {
		t.Fatalf("legacy empty = %v, calls=%d", err, emptyLegacy.calls)
	}
	changingLegacy := &parityPrincipal{values: []string{"first", "second"}}
	key, err := legacyprincipal.Key(changingLegacy)
	want, wantErr := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "auth", Version: "v1", Subject: ratelimit.Subject{Kind: "principal", Value: "second"}, Hash: true})
	if err != nil || wantErr != nil || key != want || changingLegacy.calls != 2 {
		t.Fatalf("legacy changing = %q/%v, want %q, calls=%d", key.String(), err, want.String(), changingLegacy.calls)
	}
}

type parityLogHandler struct{ record slog.Record }

func (*parityLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (handler *parityLogHandler) Handle(_ context.Context, record slog.Record) error {
	handler.record = record.Clone()
	return nil
}
func (handler *parityLogHandler) WithAttrs([]slog.Attr) slog.Handler { return handler }
func (handler *parityLogHandler) WithGroup(string) slog.Handler      { return handler }

func TestSlogSuccessorPreservesSharedObservationRecord(t *testing.T) {
	t.Parallel()

	legacyHandler, successorHandler := &parityLogHandler{}, &parityLogHandler{}
	legacyObserver, legacyErr := legacylog.New(legacylog.Options{Logger: slog.New(legacyHandler), Level: slog.LevelWarn})
	successorObserver, successorErr := ratelimitslog.New(ratelimitslog.Options{Logger: slog.New(successorHandler), Level: slog.LevelWarn})
	if legacyErr != nil || successorErr != nil {
		t.Fatalf("construct observers = %v, %v", legacyErr, successorErr)
	}
	observation := ratelimit.Observation{PolicyID: "policy", SubjectKind: "principal", Decision: ratelimit.Decision{Allowed: false, Backend: "backend", Reason: ratelimit.ReasonLimited, PolicyRevision: "v1"}, Err: ratelimit.ErrRejected, Duration: 2 * time.Millisecond}
	legacyObserver.Observe(observation)
	successorObserver.Observe(observation)
	if legacyHandler.record.Message != successorHandler.record.Message || legacyHandler.record.Level != successorHandler.record.Level ||
		!reflect.DeepEqual(logAttributes(legacyHandler.record), logAttributes(successorHandler.record)) {
		t.Fatalf("legacy=%+v successor=%+v", logAttributes(legacyHandler.record), logAttributes(successorHandler.record))
	}
}

func logAttributes(record slog.Record) map[string]any {
	attributes := make(map[string]any, record.NumAttrs())
	record.Attrs(func(attribute slog.Attr) bool {
		attributes[attribute.Key] = attribute.Value.Any()
		return true
	})
	return attributes
}

func TestOTelSuccessorPreservesMetricsAndOwnsExactScope(t *testing.T) {
	t.Parallel()

	observation := ratelimit.Observation{PolicyID: "policy", SubjectKind: "principal", Decision: ratelimit.Decision{Allowed: false, Backend: "backend", Reason: ratelimit.ReasonLimited, PolicyRevision: "v1"}, Err: ratelimit.ErrRejected, Duration: 2 * time.Millisecond}
	legacyScope, legacyMetrics := collectParityMetrics(t, observation, func(provider metric.MeterProvider) (ratelimit.Observer, error) {
		return legacytelemetry.NewStrict(legacytelemetry.Options{MeterProvider: provider})
	})
	successorScope, successorMetrics := collectParityMetrics(t, observation, func(provider metric.MeterProvider) (ratelimit.Observer, error) {
		return ratelimitotel.New(ratelimitotel.Options{MeterProvider: provider})
	})
	if legacyScope != legacyTelemetryPath || successorScope != successorTelemetryPath || legacyScope == successorScope ||
		!reflect.DeepEqual(legacyMetrics, successorMetrics) {
		t.Fatalf("scopes=%q/%q metrics=%v/%v", legacyScope, successorScope, legacyMetrics, successorMetrics)
	}
}

func collectParityMetrics(t *testing.T, observation ratelimit.Observation, construct func(metric.MeterProvider) (ratelimit.Observer, error)) (string, []string) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	observer, err := construct(provider)
	if err != nil {
		t.Fatal(err)
	}
	observer.Observe(observation)
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	if len(collected.ScopeMetrics) != 1 {
		t.Fatalf("scope metrics = %+v", collected.ScopeMetrics)
	}
	metrics := make([]string, 0, len(collected.ScopeMetrics[0].Metrics))
	for _, current := range collected.ScopeMetrics[0].Metrics {
		metrics = append(metrics, current.Name+"|"+current.Description+"|"+current.Unit)
		assertCommonMetricAttributes(t, current)
	}
	sort.Strings(metrics)
	return collected.ScopeMetrics[0].Scope.Name, metrics
}

func assertCommonMetricAttributes(t *testing.T, current metricdata.Metrics) {
	t.Helper()
	var attributes attribute.Set
	switch data := current.Data.(type) {
	case metricdata.Sum[int64]:
		if len(data.DataPoints) != 1 || data.DataPoints[0].Value != 1 {
			t.Fatalf("counter data = %+v", data.DataPoints)
		}
		attributes = data.DataPoints[0].Attributes
	case metricdata.Histogram[float64]:
		if len(data.DataPoints) != 1 || data.DataPoints[0].Count != 1 || data.DataPoints[0].Sum != 0.002 {
			t.Fatalf("histogram data = %+v", data.DataPoints)
		}
		attributes = data.DataPoints[0].Attributes
	default:
		t.Fatalf("unexpected metric data %T", current.Data)
	}
	for key, want := range map[attribute.Key]string{
		"rate_limit.policy.id": "policy", "rate_limit.policy.revision": "v1", "rate_limit.subject.kind": "principal",
		"rate_limit.backend": "backend", "rate_limit.reason": "limited",
	} {
		value, ok := attributes.Value(key)
		if !ok || value.AsString() != want {
			t.Fatalf("attribute %q = %v/%v", key, value, ok)
		}
	}
}

package valkey

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	valkeygo "github.com/valkey-io/valkey-go"
)

func TestStoreConfigurationBoundariesAreIndependent(t *testing.T) {
	t.Parallel()

	executor := &fakeExecutor{}
	valid := Options{Prefix: "rl", Timeout: time.Nanosecond, Clock: ServerClock}
	store, err := newStore(executor, valid)
	if err != nil || store.options != valid {
		t.Fatalf("newStore(minimum valid) = %+v, %v", store, err)
	}
	invalid := []Options{
		{Timeout: time.Second},
		{Prefix: strings.Repeat("p", MaxPrefixBytes+1), Timeout: time.Second},
		{Prefix: "rl", Timeout: 0},
		{Prefix: "rl", Timeout: -time.Nanosecond},
		{Prefix: "bad prefix", Timeout: time.Second},
		{Prefix: "rl", Timeout: time.Second, Clock: ClockPolicy(2)},
	}
	for index, options := range invalid {
		if _, err := newStore(executor, options); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
			t.Fatalf("newStore(invalid %d) error = %v", index, err)
		}
	}
	if _, err := newStore(executor, Options{Prefix: strings.Repeat("p", MaxPrefixBytes), Timeout: time.Second}); err != nil {
		t.Fatalf("newStore(maximum prefix) error = %v", err)
	}
}

func TestArgumentTTLAndDecisionDurationBoundaries(t *testing.T) {
	t.Parallel()

	store, err := newStore(&fakeExecutor{}, Options{Prefix: "rl", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	request := valkeyRequest(t)
	if got := store.args(request)[10]; got != "2000" {
		t.Fatalf("admission TTL = %q", got)
	}
	decision, err := decodeDecision([]string{"1", "1", "2", "1000000", "2", "allowed"})
	if err != nil || decision.RetryAfter != 2*time.Microsecond {
		t.Fatalf("decodeDecision(duration) = %+v, %v", decision, err)
	}
	if _, err := decodeDecision([]string{"1", "1", "2", "1000000", "-1", "allowed"}); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("decodeDecision(negative retry) error = %v", err)
	}
	if _, err := decodeDecision([]string{"-1", "0", "0", "0", "0", "overflow"}); !errors.Is(err, ratelimit.ErrOverflow) {
		t.Fatalf("decodeDecision(overflow) error = %v", err)
	}
	if _, err := decodeDecision([]string{"-1", "0", "0", "0", "0", "corrupt"}); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("decodeDecision(corrupt) error = %v", err)
	}
}

func TestLeaseArgumentsAndValidationBoundaries(t *testing.T) {
	t.Parallel()

	request := edgeLeaseRequest(t)
	executor := &fullExecutor{acquireReply: []string{"1", "1", "2", "1", "0", "allowed", "1"}}
	store, err := newStore(executor, Options{Prefix: "rl", Timeout: time.Second, Clock: ServerClock})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Acquire(context.Background(), request); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if executor.acquireArgs[7] != "2000" || executor.acquireArgs[8] != "1" {
		t.Fatalf("lease arguments = %q", executor.acquireArgs)
	}
	valid := ratelimit.Lease{
		ID: "lease", Key: request.Request.Key, PolicyID: request.Request.Policy.ID(),
		Cost: 1, ExpiresAt: request.Request.Now.Add(time.Second),
	}
	invalid := []ratelimit.Lease{
		func() ratelimit.Lease { value := valid; value.ID = ""; return value }(),
		func() ratelimit.Lease { value := valid; value.PolicyID = ""; return value }(),
		func() ratelimit.Lease { value := valid; value.Key = ratelimit.Key{}; return value }(),
		func() ratelimit.Lease { value := valid; value.Cost = 0; return value }(),
	}
	for index, lease := range invalid {
		if err := store.Release(context.Background(), lease); !errors.Is(err, ratelimit.ErrInvalidRequest) {
			t.Fatalf("Release(invalid %d) error = %v", index, err)
		}
	}
}

func TestNativeCheckBoundariesAndSequencing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		info       func(context.Context) (string, error)
		config     func(context.Context) (map[string]string, error)
		wantErr    bool
		wantConfig bool
	}{
		{name: "info error", info: func(context.Context) (string, error) { return "", errors.New("info") }, wantErr: true},
		{name: "version parse", info: func(context.Context) (string, error) { return "missing", nil }, wantErr: true},
		{name: "version eight", info: func(context.Context) (string, error) { return "valkey_version:8.0.0", nil }, wantErr: true},
		{name: "version nine", info: func(context.Context) (string, error) { return "valkey_version:9.0.0", nil }, wantConfig: true},
		{name: "config error", info: func(context.Context) (string, error) { return "valkey_version:9.0.0", nil }, config: func(context.Context) (map[string]string, error) { return nil, errors.New("config") }, wantErr: true, wantConfig: true},
		{name: "evicting", info: func(context.Context) (string, error) { return "valkey_version:9.0.0", nil }, config: func(context.Context) (map[string]string, error) {
			return map[string]string{"maxmemory-policy": "allkeys-lru"}, nil
		}, wantErr: true, wantConfig: true},
	}
	for _, test := range tests {
		configCalled := false
		config := test.config
		if config == nil {
			config = func(context.Context) (map[string]string, error) {
				configCalled = true
				return map[string]string{"maxmemory-policy": "noeviction"}, nil
			}
		} else {
			original := config
			config = func(ctx context.Context) (map[string]string, error) {
				configCalled = true
				return original(ctx)
			}
		}
		executor := &nativeExecutor{info: test.info, config: config}
		store, err := newStore(executor, Options{Prefix: "rl", Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		err = store.Check(context.Background())
		if (err != nil) != test.wantErr || configCalled != test.wantConfig {
			t.Fatalf("%s Check() error=%v config=%v", test.name, err, configCalled)
		}
	}
}

type fakeScriptResult struct {
	messages []valkeygo.ValkeyMessage
	err      error
}

type fakeScriptMessage struct {
	value string
	err   error
}

func (message fakeScriptMessage) ToString() (string, error) {
	return message.value, message.err
}

func (result fakeScriptResult) ToArray() ([]valkeygo.ValkeyMessage, error) {
	return result.messages, result.err
}

func TestNativeOpenAndScriptDecodingErrors(t *testing.T) {
	t.Parallel()

	checkExecutor := &nativeExecutor{
		info:   func(context.Context) (string, error) { return "", errors.New("check") },
		config: func(context.Context) (map[string]string, error) { return nil, nil },
	}
	store, err := newStore(checkExecutor, Options{Prefix: "rl", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if opened, err := openChecked(context.Background(), store, nil); opened != nil || err == nil {
		t.Fatalf("openChecked(check failure) = %+v, %v", opened, err)
	}
	if opened, err := openChecked(context.Background(), nil, nil); opened != nil ||
		!errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("openChecked(nil store) = %+v, %v", opened, err)
	}
	want := errors.New("array")
	if _, err := decodeScriptResult(fakeScriptResult{err: want}); !errors.Is(err, want) {
		t.Fatalf("decodeScriptResult(array error) = %v", err)
	}
	if _, err := decodeScriptMessages([]scriptMessage{fakeScriptMessage{err: want}}); !errors.Is(err, want) {
		t.Fatalf("decodeScriptMessages(message error) = %v", err)
	}
	if reply, err := decodeScriptMessages([]scriptMessage{fakeScriptMessage{value: "ok"}}); err != nil ||
		len(reply) != 1 || reply[0] != "ok" {
		t.Fatalf("decodeScriptMessages(success) = %q, %v", reply, err)
	}
}

package valkey

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	valkeygo "github.com/valkey-io/valkey-go"
)

type fullExecutor struct {
	fakeExecutor
	acquireReply []string
	releaseReply []string
	leaseErr     error
}

func (executor *fullExecutor) acquire(context.Context, []string, []string) ([]string, error) {
	return executor.acquireReply, executor.leaseErr
}

func (executor *fullExecutor) release(context.Context, []string, []string) ([]string, error) {
	return executor.releaseReply, executor.leaseErr
}

func TestStoreAndDecisionEdges(t *testing.T) {
	t.Parallel()

	executor := &fakeExecutor{err: context.DeadlineExceeded}
	store, err := newStore(executor, Options{Prefix: "edge", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if store.Name() != "valkey" {
		t.Fatalf("Name() = %q", store.Name())
	}
	if _, err := store.Admit(context.Background(), valkeyRequest(t)); !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("deadline Admit() error = %v", err)
	}
	serverStore, err := newStore(&fakeExecutor{}, Options{
		Prefix: "edge", Timeout: time.Second, Clock: ServerClock,
	})
	if err != nil {
		t.Fatal(err)
	}
	args := serverStore.args(valkeyRequest(t))
	if args[8] != "10000000" || args[9] != "1" {
		t.Fatalf("server clock args = %q", args)
	}
	short := valkeyRequest(t)
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "short", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 1, Period: time.Millisecond, MaxCost: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	short.Policy = policy
	if got := serverStore.args(short)[10]; got != "1000" {
		t.Fatalf("minimum TTL = %q", got)
	}

	badReplies := [][]string{
		{},
		{"-1", "0", "0", "0", "0", "overflow"},
		{"-1", "0", "0", "0", "0", "corrupt"},
		{"x", "0", "0", "0", "0", "allowed"},
		{"1", "x", "0", "0", "0", "allowed"},
		{"1", "0", "x", "0", "0", "allowed"},
		{"1", "0", "1", "x", "0", "allowed"},
		{"1", "0", "1", "0", "x", "allowed"},
		{"1", "0", "1", "0", "-1", "allowed"},
		{"1", "0", "1", "0", "0", "mystery"},
	}
	for _, reply := range badReplies {
		if _, err := decodeDecision(reply); err == nil {
			t.Fatalf("decodeDecision(%q) error = nil", reply)
		}
	}
}

func TestLeaseEdges(t *testing.T) {
	t.Parallel()

	request := edgeLeaseRequest(t)
	if _, _, err := (&Store{}).Acquire(context.Background(), ratelimit.LeaseRequest{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid Acquire() error = %v", err)
	}
	admissionOnly, err := newStore(&fakeExecutor{}, Options{Prefix: "edge", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := admissionOnly.Acquire(context.Background(), request); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("unsupported Acquire() error = %v", err)
	}
	executor := &fullExecutor{leaseErr: errors.New("down")}
	store, err := newStore(executor, Options{Prefix: "edge", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Acquire(context.Background(), request); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("outage Acquire() error = %v", err)
	}
	executor.leaseErr = context.DeadlineExceeded
	if _, _, err := store.Acquire(context.Background(), request); !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("deadline Acquire() error = %v", err)
	}
	executor.leaseErr = nil
	store.options.Clock = ServerClock
	shortPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "short-lease", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 1, MaxCost: 1, Lease: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Request.Policy = shortPolicy
	executor.acquireReply = []string{"1", "0", "1", "1", "0", "allowed", "1"}
	if _, _, err := store.Acquire(context.Background(), request); err != nil {
		t.Fatalf("server clock Acquire() error = %v", err)
	}
	executor.acquireReply = []string{"1", "1", "2", "1", "0", "allowed", "bad"}
	if _, _, err := store.Acquire(context.Background(), request); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("corrupt Acquire() error = %v", err)
	}
	if _, _, err := decodeLeaseReply(nil, request); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("short decodeLeaseReply() error = %v", err)
	}
	if err := store.Release(context.Background(), ratelimit.Lease{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid Release() error = %v", err)
	}
	lease := ratelimit.Lease{
		ID: "lease", Key: request.Request.Key, PolicyID: request.Request.Policy.ID(),
		Cost: 1, ExpiresAt: request.Request.Now.Add(time.Second),
	}
	if err := admissionOnly.Release(context.Background(), lease); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("unsupported Release() error = %v", err)
	}
	executor.leaseErr = errors.New("down")
	if err := store.Release(context.Background(), lease); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("outage Release() error = %v", err)
	}
	executor.leaseErr = nil
	for reply, want := range map[string]error{
		"ok": nil, "not_found": ratelimit.ErrLeaseNotFound,
		"not_owned": ratelimit.ErrLeaseNotOwned, "mystery": ratelimit.ErrCorrupt,
	} {
		executor.releaseReply = []string{reply}
		err := store.Release(context.Background(), lease)
		if !errors.Is(err, want) || (want == nil && err != nil) {
			t.Fatalf("Release(%q) error = %v", reply, err)
		}
	}
	executor.releaseReply = nil
	if err := store.Release(context.Background(), lease); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("short Release() error = %v", err)
	}
}

func TestNativeCheckAndScriptEdges(t *testing.T) {
	address := os.Getenv("VALKEY_ADDRESS")
	if address == "" {
		t.Skip("VALKEY_ADDRESS is required for native edge tests")
	}
	client, err := valkeygo.NewClient(valkeygo.ClientOption{InitAddress: []string{address}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	store, err := New(client, Options{Prefix: "edge", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.Do(context.Background(), client.B().ConfigSet().ParameterValue().
		ParameterValue("maxmemory-policy", "allkeys-lru").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if err := store.Check(context.Background()); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("evicting Check() error = %v", err)
	}
	if _, err := Open(context.Background(), client, Options{
		Prefix: "edge", Timeout: time.Second,
	}); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("evicting Open() error = %v", err)
	}
	if err := client.Do(context.Background(), client.B().ConfigSet().ParameterValue().
		ParameterValue("maxmemory-policy", "noeviction").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	fakeStore, err := newStore(&fakeExecutor{}, Options{Prefix: "edge", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := fakeStore.Check(context.Background()); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("fake Check() error = %v", err)
	}
	for _, native := range []*nativeExecutor{
		{
			info:   func(context.Context) (string, error) { return "", errors.New("info") },
			config: func(context.Context) (map[string]string, error) { return nil, nil },
		},
		{
			info:   func(context.Context) (string, error) { return "valkey_version:8.0.0", nil },
			config: func(context.Context) (map[string]string, error) { return nil, nil },
		},
		{
			info:   func(context.Context) (string, error) { return "redis_version:9.0.0", nil },
			config: func(context.Context) (map[string]string, error) { return nil, nil },
		},
		{
			info:   func(context.Context) (string, error) { return "valkey_version:9.0.0", nil },
			config: func(context.Context) (map[string]string, error) { return nil, errors.New("config") },
		},
	} {
		checkStore, err := newStore(native, Options{Prefix: "edge", Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if err := checkStore.Check(context.Background()); !errors.Is(err, ratelimit.ErrUnavailable) {
			t.Fatalf("injected Check() error = %v", err)
		}
	}
	errorScript := valkeygo.NewLuaScript("return redis.error_reply('failure')")
	if _, err := executeScript(context.Background(), client, errorScript, []string{"edge"}, nil); err == nil {
		t.Fatal("script execution error = nil")
	}
	nestedScript := valkeygo.NewLuaScript("return {{'nested'}}")
	if _, err := executeScript(context.Background(), client, nestedScript, []string{"edge"}, nil); err == nil {
		t.Fatal("script conversion error = nil")
	}
	client.Close()
	if err := store.Check(context.Background()); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("closed Check() error = %v", err)
	}
	if _, err := Open(context.Background(), nil, Options{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("Open(nil) error = %v", err)
	}
}

func edgeLeaseRequest(t *testing.T) ratelimit.LeaseRequest {
	t.Helper()
	request := valkeyRequest(t)
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "lease", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 2, MaxCost: 2, Lease: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Policy, request.Cost = policy, 1
	return ratelimit.LeaseRequest{Request: request, LeaseID: "lease"}
}

package valkey_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"github.com/faustbrian/golib/pkg/rate-limit/ratelimittest"
	"github.com/faustbrian/golib/pkg/rate-limit/valkey"
	valkeygo "github.com/valkey-io/valkey-go"
)

func TestValkey9AdmissionLeaseAndNOSCRIPTRecovery(t *testing.T) {
	address := os.Getenv("VALKEY_ADDRESS")
	if address == "" {
		t.Skip("VALKEY_ADDRESS is required for live Valkey tests")
	}
	client, err := valkeygo.NewClient(valkeygo.ClientOption{InitAddress: []string{address}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	prefix := "rate-limit-integration-" + runID
	store, err := valkey.Open(context.Background(), client, valkey.Options{
		Prefix: prefix, Timeout: time.Second,
		Clock: valkey.ClientClock,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := integrationRequest(t, ratelimit.FixedWindow, "fixed", 2, time.Second)
	if decision, err := store.Admit(context.Background(), request); err != nil || !decision.Allowed {
		t.Fatalf("first Admit() = %+v, %v", decision, err)
	}
	if err := client.Do(context.Background(), client.B().ScriptFlush().Build()).Error(); err != nil {
		t.Fatalf("SCRIPT FLUSH error = %v", err)
	}
	request.Cost = 2
	if decision, err := store.Admit(context.Background(), request); !errors.Is(err, ratelimit.ErrRejected) ||
		decision.Remaining != 1 {
		t.Fatalf("post-NOSCRIPT Admit() = %+v, %v", decision, err)
	}
	reconnectName := "rate-limit-reconnect-" + runID
	reconnectClient, err := valkeygo.NewClient(valkeygo.ClientOption{
		InitAddress: []string{address}, ClientName: reconnectName,
		PipelineMultiplex: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reconnectClient.Close)
	reconnectStore, err := valkey.New(reconnectClient, valkey.Options{
		Prefix: "rate-limit-reconnect-" + runID, Timeout: time.Second,
		Clock: valkey.ClientClock,
	})
	if err != nil {
		t.Fatal(err)
	}
	reconnect := integrationRequest(t, ratelimit.FixedWindow, "reconnect", 2, time.Second)
	if decision, err := reconnectStore.Admit(context.Background(), reconnect); err != nil ||
		!decision.Allowed || decision.Remaining != 1 {
		t.Fatalf("pre-reconnect Admit() = %+v, %v", decision, err)
	}
	if err := client.Do(context.Background(), client.B().ClientKill().
		TypeNormal().SkipmeYes().Name(reconnectName).Build()).Error(); err != nil {
		t.Fatalf("CLIENT KILL error = %v", err)
	}
	if decision, err := reconnectStore.Admit(context.Background(), reconnect); !errors.Is(err, ratelimit.ErrUnavailable) || decision.Allowed {
		t.Fatalf("disconnected Admit() = %+v, %v", decision, err)
	}
	waitForValkeyReconnect(t, reconnectClient)
	if decision, err := reconnectStore.Admit(context.Background(), reconnect); err != nil ||
		!decision.Allowed || decision.Remaining != 0 {
		t.Fatalf("reconnected Admit() = %+v, %v", decision, err)
	}

	leaseRequest := integrationLeaseRequest(t)
	lease, decision, err := store.Acquire(context.Background(), leaseRequest)
	if err != nil || !decision.Allowed {
		t.Fatalf("Acquire() = %+v, %+v, %v", lease, decision, err)
	}
	if err := store.Release(context.Background(), lease); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	corruptRequest := integrationLeaseRequest(t)
	digest := sha256.Sum256([]byte(
		corruptRequest.Request.Policy.ID() + "\x00" + corruptRequest.Request.Key.String(),
	))
	storageKey := prefix + ":{" + hex.EncodeToString(digest[:]) + "}"
	fill := `
redis.call('HSET', KEYS[1], 'schema', '1', 'policy_id', ARGV[1],
    'algorithm', 'concurrency', 'revision', 'v1', 'last', '100000000')
for index = 1, 1030 do redis.call('HSET', KEYS[1], 'x' .. index, '1') end
return redis.call('HLEN', KEYS[1])`
	if err := client.Do(context.Background(), client.B().Eval().Script(fill).
		Numkeys(1).Key(storageKey).Arg(corruptRequest.Request.Policy.ID()).Build()).Error(); err != nil {
		t.Fatalf("corrupt fixture error = %v", err)
	}
	if _, _, err := store.Acquire(context.Background(), corruptRequest); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("overbudget state Acquire() error = %v", err)
	}
	serverStore, err := valkey.New(client, valkey.Options{
		Prefix: "rate-limit-server-clock-" + runID, Timeout: time.Second,
		Clock: valkey.ServerClock,
	})
	if err != nil {
		t.Fatal(err)
	}
	serverRequest := integrationRequest(t, ratelimit.FixedWindow, "server", 2, time.Second)
	serverRequest.Now = time.Unix(0, 0)
	if decision, err := serverStore.Admit(context.Background(), serverRequest); err != nil ||
		!decision.Allowed || decision.Reset.Before(time.Now().UTC()) {
		t.Fatalf("server-clock Admit() = %+v, %v", decision, err)
	}
	serverLeaseRequest := integrationLeaseRequest(t)
	serverLeaseRequest.Request.Now = time.Unix(0, 0)
	serverLease, decision, err := serverStore.Acquire(context.Background(), serverLeaseRequest)
	if err != nil || !decision.Allowed || serverLease.ExpiresAt.Before(time.Now().UTC()) {
		t.Fatalf("server-clock Acquire() = %+v, %+v, %v", serverLease, decision, err)
	}
	if err := serverStore.Release(context.Background(), serverLease); err != nil {
		t.Fatalf("server-clock Release() error = %v", err)
	}
	ratelimittest.RunBackendConformance(t, func(t testing.TB) ratelimittest.BackendFixture {
		t.Helper()
		conformance, err := valkey.New(client, valkey.Options{
			Prefix: "rate-limit-conformance-" + runID, Timeout: time.Second,
			Clock: valkey.ClientClock,
		})
		if err != nil {
			t.Fatal(err)
		}
		return ratelimittest.BackendFixture{Backend: conformance, Leases: conformance}
	})
	ratelimittest.RunBackendAtomicity(t, func(t testing.TB) ratelimittest.BackendFixture {
		t.Helper()
		conformance, err := valkey.New(client, valkey.Options{
			Prefix: "rate-limit-atomicity-" + runID, Timeout: 5 * time.Second,
			Clock: valkey.ClientClock,
		})
		if err != nil {
			t.Fatal(err)
		}
		return ratelimittest.BackendFixture{Backend: conformance, Leases: conformance}
	})
}

func waitForValkeyReconnect(t *testing.T, client valkeygo.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := client.Do(ctx, client.B().Ping().Build()).Error(); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Valkey reconnect timeout: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func integrationRequest(t *testing.T, algorithm ratelimit.Algorithm, id string, capacity uint64, period time.Duration) ratelimit.Request {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: id + "-" + t.Name(), Revision: "v1", Algorithm: algorithm,
		Capacity: capacity, Period: period, MaxCost: capacity,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "case", Value: t.Name()}, Hash: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ratelimit.Request{Policy: policy, Key: key, Cost: 1, Now: time.Unix(100, 0)}
}

func integrationLeaseRequest(t *testing.T) ratelimit.LeaseRequest {
	t.Helper()
	request := integrationRequest(t, ratelimit.FixedWindow, "unused", 2, time.Second)
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "lease-" + t.Name(), Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 2, MaxCost: 2, Lease: time.Second,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Policy = policy
	return ratelimit.LeaseRequest{Request: request, LeaseID: "job-1"}
}

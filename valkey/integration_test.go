package valkey_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"github.com/faustbrian/go-rate-limit/ratelimittest"
	"github.com/faustbrian/go-rate-limit/valkey"
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
	if err := client.Do(context.Background(), client.B().ConfigSet().ParameterValue().
		ParameterValue("maxmemory-policy", "noeviction").Build()).Error(); err != nil {
		t.Fatalf("configure disposable Valkey: %v", err)
	}
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

func TestValkey9StrictRevisionCarryAndMalformedState(t *testing.T) {
	address := os.Getenv("VALKEY_ADDRESS")
	if address == "" {
		t.Skip("VALKEY_ADDRESS is required for live Valkey tests")
	}
	client, err := valkeygo.NewClient(valkeygo.ClientOption{InitAddress: []string{address}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	prefix := "rate-limit-strict-state-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	strictOptions := valkey.Options{Prefix: prefix, Timeout: time.Second, Clock: valkey.ClientClock}
	store, err := valkey.OpenStrict(context.Background(), client, strictOptions)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if failed, openErr := valkey.OpenStrict(canceled, client, strictOptions); failed != nil || !errors.Is(openErr, ratelimit.ErrCanceled) {
		t.Fatalf("canceled OpenStrict() = %+v, %v", failed, openErr)
	}
	if pong, pingErr := client.Do(context.Background(), client.B().Ping().Build()).ToString(); pingErr != nil || pong != "PONG" {
		t.Fatalf("borrowed client after strict open = %q, %v", pong, pingErr)
	}
	serverRangeStore, err := valkey.NewStrict(client, valkey.Options{
		Prefix: prefix + "-server-range", Timeout: time.Second, Clock: valkey.ServerClock,
	})
	if err != nil {
		t.Fatal(err)
	}
	longDuration := time.Duration(8_000_000_000) * time.Second
	longPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "server-range-admit", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 1, MaxCost: 1, Period: longDuration, Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	rangeKey, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "case", Value: "server-range"}, Hash: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	longRequest := ratelimit.Request{Policy: longPolicy, Key: rangeKey, Cost: 1, Now: time.Unix(0, 0)}
	if decision, admitErr := serverRangeStore.AdmitStrict(context.Background(), longRequest); decision != (ratelimit.Decision{}) ||
		!errors.Is(admitErr, ratelimit.ErrOverflow) {
		t.Fatalf("server-range AdmitStrict() = %+v, %v", decision, admitErr)
	}
	longLeasePolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "server-range-lease", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 1, MaxCost: 1, Lease: longDuration, Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	longLeaseRequest := ratelimit.LeaseRequest{
		Request: ratelimit.Request{Policy: longLeasePolicy, Key: rangeKey, Cost: 1, Now: time.Unix(0, 0)},
		LeaseID: "server-range",
	}
	if lease, decision, acquireErr := serverRangeStore.AcquireStrict(context.Background(), longLeaseRequest); lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(acquireErr, ratelimit.ErrOverflow) {
		t.Fatalf("server-range AcquireStrict() = %+v, %+v, %v", lease, decision, acquireErr)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "case", Value: prefix}, Hash: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := func(id, revision string, algorithm ratelimit.Algorithm, capacity uint64) ratelimit.Request {
		policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
			ID: id + "-strict-revision", Revision: revision, Algorithm: algorithm,
			Capacity: capacity, Period: time.Second, MaxCost: capacity,
			Consistency: ratelimit.ConsistencyStrong,
		})
		if err != nil {
			t.Fatal(err)
		}
		return ratelimit.Request{Policy: policy, Key: key, Cost: 1, Now: time.Unix(100, 0)}
	}

	decrease := request("decrease", "v1", ratelimit.TokenBucket, 10)
	if decision, err := store.AdmitStrict(context.Background(), decrease); err != nil || decision.Remaining != 9 {
		t.Fatalf("initial decrease state = %+v, %v", decision, err)
	}
	decrease.Policy = request("decrease", "v2", ratelimit.TokenBucket, 2).Policy
	if decision, err := store.AdmitStrict(context.Background(), decrease); err != nil || !decision.Allowed || decision.Remaining != 1 {
		t.Fatalf("decreased revision = %+v, %v", decision, err)
	}

	increase := request("increase", "v1", ratelimit.TokenBucket, 2)
	if _, err := store.AdmitStrict(context.Background(), increase); err != nil {
		t.Fatal(err)
	}
	increase.Policy = request("increase", "v2", ratelimit.TokenBucket, 10).Policy
	if decision, err := store.AdmitStrict(context.Background(), increase); err != nil || !decision.Allowed || decision.Remaining != 0 {
		t.Fatalf("increased revision = %+v, %v", decision, err)
	}

	boundaryPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "fixed-boundary", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 1, MaxCost: 1, Period: 9_007_199_254_740_990 * time.Microsecond,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	boundaryRequest := ratelimit.Request{
		Policy: boundaryPolicy, Key: key, Cost: 1,
		Now: time.UnixMicro(-9_007_199_254_740_991),
	}
	if decision, err := store.AdmitStrict(context.Background(), boundaryRequest); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrOverflow) {
		t.Fatalf("fixed boundary = %+v, %v", decision, err)
	}
	if count, err := client.Do(context.Background(), client.B().Exists().Key(valkeyIntegrationStateKey(prefix, boundaryRequest)).Build()).AsInt64(); err != nil || count != 0 {
		t.Fatalf("fixed boundary state count = %d, %v", count, err)
	}

	const maxExact = uint64(9_007_199_254_740_991)
	ceilingPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "ceiling", Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Capacity: maxExact - 1, MaxCost: maxExact - 1, Period: time.Microsecond,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	ceilingRequest := ratelimit.Request{Policy: ceilingPolicy, Key: key, Cost: maxExact - 2, Now: time.UnixMicro(100)}
	if decision, err := store.AdmitStrict(context.Background(), ceilingRequest); err != nil || decision.Reset != ceilingRequest.Now.Add(time.Microsecond) || decision.Remaining != 1 {
		t.Fatalf("ceiling allowed = %+v, %v", decision, err)
	}
	ceilingRequest.Cost = maxExact - 1
	if decision, err := store.AdmitStrict(context.Background(), ceilingRequest); !errors.Is(err, ratelimit.ErrRejected) || decision.Reset != ceilingRequest.Now.Add(time.Microsecond) || decision.RetryAfter != time.Microsecond {
		t.Fatalf("ceiling rejected = %+v, %v", decision, err)
	}
	legacyCeilingPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "legacy-ceiling", Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Capacity: maxExact - 1, MaxCost: maxExact - 1, Period: time.Microsecond,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	legacyCeiling := ratelimit.Request{Policy: legacyCeilingPolicy, Key: key, Cost: maxExact - 2, Now: time.UnixMicro(100)}
	if decision, err := store.Admit(context.Background(), legacyCeiling); err != nil || decision.Reset != legacyCeiling.Now.Add(2*time.Microsecond) || decision.Remaining != 1 {
		t.Fatalf("legacy ceiling = %+v, %v", decision, err)
	}

	revisionPolicy := func(revision string, period time.Duration) ratelimit.Policy {
		policy, policyErr := ratelimit.NewPolicy(ratelimit.PolicySpec{
			ID: "period-revision", Revision: revision, Algorithm: ratelimit.TokenBucket,
			Capacity: 1, MaxCost: 1, Period: period, Consistency: ratelimit.ConsistencyStrong,
		})
		if policyErr != nil {
			t.Fatal(policyErr)
		}
		return policy
	}
	revisionRequest := ratelimit.Request{Policy: revisionPolicy("v1", 10*time.Microsecond), Key: key, Cost: 1, Now: time.UnixMicro(200)}
	if _, err := store.AdmitStrict(context.Background(), revisionRequest); err != nil {
		t.Fatal(err)
	}
	revisionRequest.Now = time.UnixMicro(205)
	if _, err := store.AdmitStrict(context.Background(), revisionRequest); !errors.Is(err, ratelimit.ErrRejected) {
		t.Fatalf("persist fractional remainder = %v", err)
	}
	revisionRequest.Policy = revisionPolicy("v2", time.Microsecond)
	if decision, err := store.AdmitStrict(context.Background(), revisionRequest); !errors.Is(err, ratelimit.ErrRejected) || errors.Is(err, ratelimit.ErrCorrupt) ||
		decision != (ratelimit.Decision{Limit: 1, Reset: time.UnixMicro(206), RetryAfter: time.Microsecond, Reason: ratelimit.ReasonLimited}) {
		t.Fatalf("period revision carry = %+v, %v", decision, err)
	}
	revisionState, err := client.Do(context.Background(), client.B().Hgetall().Key(valkeyIntegrationStateKey(prefix, revisionRequest)).Build()).AsStrMap()
	if err != nil || revisionState["revision"] != "v2" || revisionState["tokens"] != "0" || revisionState["remainder"] != "0" || revisionState["last"] != "205" {
		t.Fatalf("period revision state = %v, %v", revisionState, err)
	}
	for _, algorithm := range []ratelimit.Algorithm{ratelimit.FixedWindow, ratelimit.SlidingWindow} {
		id := "window-decrease-" + strings.ReplaceAll(string(algorithm), "_", "-")
		current := request(id, "v1", algorithm, 5)
		current.Cost = 4
		if _, err := store.AdmitStrict(context.Background(), current); err != nil {
			t.Fatal(err)
		}
		current.Policy = request(id, "v2", algorithm, 2).Policy
		current.Cost = 1
		for attempt := 0; attempt < 2; attempt++ {
			decision, err := store.AdmitStrict(context.Background(), current)
			if !errors.Is(err, ratelimit.ErrRejected) || decision.Remaining != 0 || decision.Reason != ratelimit.ReasonLimited {
				t.Fatalf("%s decreased revision attempt %d = %+v, %v", algorithm, attempt, decision, err)
			}
		}
	}

	for _, test := range []struct {
		name      string
		algorithm ratelimit.Algorithm
		fields    func(ratelimit.Request) map[string]string
	}{
		{name: "tokens above limit", algorithm: ratelimit.TokenBucket, fields: func(ratelimit.Request) map[string]string { return map[string]string{"tokens": "10"} }},
		{name: "rounded fractional tokens", algorithm: ratelimit.TokenBucket, fields: func(ratelimit.Request) map[string]string { return map[string]string{"tokens": "0.99999999999999999"} }},
		{name: "scientific tokens", algorithm: ratelimit.TokenBucket, fields: func(ratelimit.Request) map[string]string { return map[string]string{"tokens": "1e0"} }},
		{name: "negative tokens", algorithm: ratelimit.TokenBucket, fields: func(ratelimit.Request) map[string]string { return map[string]string{"tokens": "-1"} }},
		{name: "remainder outside period", algorithm: ratelimit.TokenBucket, fields: func(ratelimit.Request) map[string]string { return map[string]string{"remainder": "1000000"} }},
		{name: "token clock outside exact range", algorithm: ratelimit.TokenBucket, fields: func(ratelimit.Request) map[string]string { return map[string]string{"last": "-9007199254740992"} }},
		{name: "negative fixed usage", algorithm: ratelimit.FixedWindow, fields: func(ratelimit.Request) map[string]string { return map[string]string{"used": "-1"} }},
		{name: "rounded fractional fixed usage", algorithm: ratelimit.FixedWindow, fields: func(ratelimit.Request) map[string]string { return map[string]string{"used": "0.99999999999999999"} }},
		{name: "fixed window outside exact range", algorithm: ratelimit.FixedWindow, fields: func(ratelimit.Request) map[string]string { return map[string]string{"window": "-9007199254740992"} }},
		{name: "negative sliding segment", algorithm: ratelimit.SlidingWindow, fields: func(request ratelimit.Request) map[string]string {
			width := (request.Policy.Period().Microseconds() + 15) / 16
			index := request.Now.UnixMicro() / width
			return map[string]string{"b" + strconv.FormatInt(index%16, 10): strconv.FormatInt(index, 10) + ":-1"}
		}},
		{name: "sliding index outside exact range", algorithm: ratelimit.SlidingWindow, fields: func(request ratelimit.Request) map[string]string {
			width := (request.Policy.Period().Microseconds() + 15) / 16
			index := request.Now.UnixMicro() / width
			return map[string]string{"b" + strconv.FormatInt(index%16, 10): "-9007199254740992:1"}
		}},
		{name: "scientific sliding value", algorithm: ratelimit.SlidingWindow, fields: func(request ratelimit.Request) map[string]string {
			width := (request.Policy.Period().Microseconds() + 15) / 16
			index := request.Now.UnixMicro() / width
			return map[string]string{"b" + strconv.FormatInt(index%16, 10): strconv.FormatInt(index, 10) + ":1e0"}
		}},
		{name: "extra sliding segment", algorithm: ratelimit.SlidingWindow, fields: func(request ratelimit.Request) map[string]string {
			width := (request.Policy.Period().Microseconds() + 15) / 16
			index := request.Now.UnixMicro() / width
			return map[string]string{"b16": strconv.FormatInt(index, 10) + ":1"}
		}},
		{name: "future sliding segment", algorithm: ratelimit.SlidingWindow, fields: func(request ratelimit.Request) map[string]string {
			width := (request.Policy.Period().Microseconds() + 15) / 16
			index := request.Now.UnixMicro()/width + 1
			return map[string]string{"b" + strconv.FormatInt(index%16, 10): strconv.FormatInt(index, 10) + ":1"}
		}},
		{name: "wrong slot duplicate sliding index", algorithm: ratelimit.SlidingWindow, fields: func(request ratelimit.Request) map[string]string {
			width := (request.Policy.Period().Microseconds() + 15) / 16
			index := request.Now.UnixMicro() / width
			return map[string]string{"b" + strconv.FormatInt((index+1)%16, 10): strconv.FormatInt(index, 10) + ":1"}
		}},
		{name: "expired corrupt aggregate", algorithm: ratelimit.SlidingWindow, fields: func(request ratelimit.Request) map[string]string {
			width := (request.Policy.Period().Microseconds() + 15) / 16
			index := request.Now.UnixMicro() / width
			oldest := request.Now.Add(-request.Policy.Period()).UnixMicro() / width
			return map[string]string{
				"b" + strconv.FormatInt(index%16, 10):      strconv.FormatInt(index, 10) + ":2",
				"b" + strconv.FormatInt((oldest+1)%16, 10): strconv.FormatInt(oldest+1, 10) + ":1",
				"b" + strconv.FormatInt((oldest-1)%16, 10): strconv.FormatInt(oldest-1, 10) + ":1",
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			current := request("malformed-"+strings.ReplaceAll(test.name, " ", "-"), "v1", test.algorithm, 2)
			if _, err := store.AdmitStrict(context.Background(), current); err != nil {
				t.Fatal(err)
			}
			key := valkeyIntegrationStateKey(prefix, current)
			command := client.B().Hset().Key(key).FieldValue()
			for field, value := range test.fields(current) {
				command = command.FieldValue(field, value)
			}
			if err := client.Do(context.Background(), command.Build()).Error(); err != nil {
				t.Fatal(err)
			}
			before, err := client.Do(context.Background(), client.B().Hgetall().Key(key).Build()).AsStrMap()
			if err != nil {
				t.Fatal(err)
			}
			decision, err := store.AdmitStrict(context.Background(), current)
			if decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
				t.Fatalf("AdmitStrict() = %+v, %v", decision, err)
			}
			after, err := client.Do(context.Background(), client.B().Hgetall().Key(key).Build()).AsStrMap()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("malformed state mutated: before=%v after=%v", before, after)
			}
		})
	}

	leasePolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "malformed-lease-strict-revision", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 2, MaxCost: 2, Lease: time.Second, Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseRequest := ratelimit.LeaseRequest{Request: ratelimit.Request{Policy: leasePolicy, Key: key, Cost: 1, Now: time.Unix(100, 0)}, LeaseID: "good"}
	goodLease, _, err := store.AcquireStrict(context.Background(), leaseRequest)
	if err != nil {
		t.Fatal(err)
	}
	for name, invalidField := range map[string]string{
		"short":     "l:x",
		"uppercase": "l:" + strings.Repeat("A", sha256.Size*2),
	} {
		t.Run("lease-identity-"+name, func(t *testing.T) {
			identityPolicy, policyErr := ratelimit.NewPolicy(ratelimit.PolicySpec{
				ID: "lease-identity-" + name, Revision: "v1", Algorithm: ratelimit.Concurrency,
				Capacity: 2, MaxCost: 2, Lease: time.Second, Consistency: ratelimit.ConsistencyStrong,
			})
			if policyErr != nil {
				t.Fatal(policyErr)
			}
			identityRequest := ratelimit.LeaseRequest{
				Request: ratelimit.Request{Policy: identityPolicy, Key: key, Cost: 1, Now: time.Unix(100, 0)},
				LeaseID: "owned",
			}
			identityLease, _, acquireErr := store.AcquireStrict(context.Background(), identityRequest)
			if acquireErr != nil {
				t.Fatal(acquireErr)
			}
			identityKey := valkeyIntegrationStateKey(prefix, identityRequest.Request)
			if setErr := client.Do(context.Background(), client.B().Hset().Key(identityKey).FieldValue().FieldValue(invalidField, "1:200000000").Build()).Error(); setErr != nil {
				t.Fatal(setErr)
			}
			before, getErr := client.Do(context.Background(), client.B().Hgetall().Key(identityKey).Build()).AsStrMap()
			if getErr != nil {
				t.Fatal(getErr)
			}
			identityRequest.LeaseID = "other"
			if lease, decision, acquireErr := store.AcquireStrict(context.Background(), identityRequest); lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(acquireErr, ratelimit.ErrCorrupt) {
				t.Fatalf("malformed lease identity acquire = %+v, %+v, %v", lease, decision, acquireErr)
			}
			if releaseErr := store.ReleaseStrict(context.Background(), identityLease); !errors.Is(releaseErr, ratelimit.ErrCorrupt) {
				t.Fatalf("malformed lease identity release = %v", releaseErr)
			}
			after, getErr := client.Do(context.Background(), client.B().Hgetall().Key(identityKey).Build()).AsStrMap()
			if getErr != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("malformed lease identity mutated: before=%v after=%v error=%v", before, after, getErr)
			}
		})
	}
	for _, test := range []struct {
		name  string
		field string
		value string
	}{
		{name: "schema", field: "schema", value: "2"},
		{name: "policy", field: "policy_id", value: "different"},
		{name: "algorithm", field: "algorithm", value: "token_bucket"},
	} {
		t.Run("release-identity-"+test.name, func(t *testing.T) {
			policy, policyErr := ratelimit.NewPolicy(ratelimit.PolicySpec{
				ID: "release-identity-" + test.name, Revision: "v1", Algorithm: ratelimit.Concurrency,
				Capacity: 2, MaxCost: 2, Lease: time.Second, Consistency: ratelimit.ConsistencyStrong,
			})
			if policyErr != nil {
				t.Fatal(policyErr)
			}
			current := ratelimit.LeaseRequest{
				Request: ratelimit.Request{Policy: policy, Key: key, Cost: 1, Now: time.Unix(100, 0)},
				LeaseID: "target",
			}
			lease, _, acquireErr := store.AcquireStrict(context.Background(), current)
			if acquireErr != nil {
				t.Fatal(acquireErr)
			}
			stateKey := valkeyIntegrationStateKey(prefix, current.Request)
			if setErr := client.Do(context.Background(), client.B().Hset().Key(stateKey).FieldValue().
				FieldValue(test.field, test.value).Build()).Error(); setErr != nil {
				t.Fatal(setErr)
			}
			beforeIdentity, getErr := client.Do(context.Background(), client.B().Hgetall().Key(stateKey).Build()).AsStrMap()
			if getErr != nil {
				t.Fatal(getErr)
			}
			if releaseErr := store.ReleaseStrict(context.Background(), lease); !errors.Is(releaseErr, ratelimit.ErrCorrupt) {
				t.Fatalf("ReleaseStrict(%s mismatch) error = %v", test.field, releaseErr)
			}
			afterIdentity, getErr := client.Do(context.Background(), client.B().Hgetall().Key(stateKey).Build()).AsStrMap()
			if getErr != nil {
				t.Fatal(getErr)
			}
			if !reflect.DeepEqual(afterIdentity, beforeIdentity) {
				t.Fatalf("strict release mutated identity-corrupt state: before=%v after=%v", beforeIdentity, afterIdentity)
			}
			if releaseErr := store.Release(context.Background(), lease); !errors.Is(releaseErr, ratelimit.ErrLeaseNotFound) {
				t.Fatalf("legacy Release(%s mismatch) error = %v", test.field, releaseErr)
			}
		})
	}
	leaseKey := valkeyIntegrationStateKey(prefix, leaseRequest.Request)
	if err := client.Do(context.Background(), client.B().Hset().Key(leaseKey).FieldValue().FieldValue("l:malformed", "0.99999999999999999:100500000").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	before, err := client.Do(context.Background(), client.B().Hgetall().Key(leaseKey).Build()).AsStrMap()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseStrict(context.Background(), goodLease); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("ReleaseStrict(malformed sibling) error = %v", err)
	}
	after, err := client.Do(context.Background(), client.B().Hgetall().Key(leaseKey).Build()).AsStrMap()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("strict release mutated malformed state: before=%v after=%v", before, after)
	}
	leasePolicy, err = ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: leasePolicy.ID(), Revision: "v2", Algorithm: ratelimit.Concurrency,
		Capacity: 2, MaxCost: 2, Lease: time.Second, Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseRequest.Request.Policy = leasePolicy
	leaseRequest.LeaseID = "next"
	lease, decision, err := store.AcquireStrict(context.Background(), leaseRequest)
	if lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("malformed lease state = %+v, %+v, %v", lease, decision, err)
	}
	after, err = client.Do(context.Background(), client.B().Hgetall().Key(leaseKey).Build()).AsStrMap()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("malformed lease state mutated: before=%v after=%v", before, after)
	}
	if err := client.Do(context.Background(), client.B().Hdel().Key(leaseKey).Field("l:malformed").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if err := client.Do(context.Background(), client.B().Hset().Key(leaseKey).FieldValue().FieldValue("unexpected", "1").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	before, err = client.Do(context.Background(), client.B().Hgetall().Key(leaseKey).Build()).AsStrMap()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseStrict(context.Background(), goodLease); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("ReleaseStrict(unknown sibling) error = %v", err)
	}
	after, err = client.Do(context.Background(), client.B().Hgetall().Key(leaseKey).Build()).AsStrMap()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("strict release mutated unknown-field state: before=%v after=%v", before, after)
	}
	lease, decision, err = store.AcquireStrict(context.Background(), leaseRequest)
	if lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("unknown lease field state = %+v, %+v, %v", lease, decision, err)
	}
	after, err = client.Do(context.Background(), client.B().Hgetall().Key(leaseKey).Build()).AsStrMap()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("unknown lease field state mutated: before=%v after=%v", before, after)
	}
}

func TestValkeyStrictRejectsOversizedEncodedStateBeforeMutation(t *testing.T) {
	address := os.Getenv("VALKEY_ADDRESS")
	if address == "" {
		t.Skip("VALKEY_ADDRESS is required for live Valkey tests")
	}
	client, err := valkeygo.NewClient(valkeygo.ClientOption{InitAddress: []string{address}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	prefix := "rate-limit-oversized-state-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	store, err := valkey.New(client, valkey.Options{Prefix: prefix, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	oversized := strings.Repeat("r", 128*1024+1)

	admit := integrationRequestAt(t, ratelimit.TokenBucket, "oversized-admit", "v1", 2, time.Unix(100, 0))
	if _, err := store.AdmitStrict(context.Background(), admit); err != nil {
		t.Fatal(err)
	}
	admitKey := valkeyIntegrationStateKey(prefix, admit)
	if err := client.Do(context.Background(), client.B().Hset().Key(admitKey).FieldValue().FieldValue("revision", oversized).Build()).Error(); err != nil {
		t.Fatal(err)
	}
	beforeAdmit, err := client.Do(context.Background(), client.B().Hgetall().Key(admitKey).Build()).AsStrMap()
	if err != nil {
		t.Fatal(err)
	}
	if decision, err := store.AdmitStrict(context.Background(), admit); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("AdmitStrict(oversized) = %+v, %v", decision, err)
	}
	afterAdmit, _ := client.Do(context.Background(), client.B().Hgetall().Key(admitKey).Build()).AsStrMap()
	if !reflect.DeepEqual(afterAdmit, beforeAdmit) {
		t.Fatal("oversized admission state mutated")
	}

	leasePolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "oversized-lease", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 2, MaxCost: 2, Lease: time.Second, Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseRequest := ratelimit.LeaseRequest{
		Request: ratelimit.Request{Policy: leasePolicy, Key: admit.Key, Cost: 1, Now: time.Unix(100, 0)},
		LeaseID: "lease",
	}
	lease, _, err := store.AcquireStrict(context.Background(), leaseRequest)
	if err != nil {
		t.Fatal(err)
	}
	leaseKey := valkeyIntegrationStateKey(prefix, leaseRequest.Request)
	if err := client.Do(context.Background(), client.B().Hset().Key(leaseKey).FieldValue().FieldValue("revision", oversized).Build()).Error(); err != nil {
		t.Fatal(err)
	}
	beforeLease, err := client.Do(context.Background(), client.B().Hgetall().Key(leaseKey).Build()).AsStrMap()
	if err != nil {
		t.Fatal(err)
	}
	leaseRequest.LeaseID = "next"
	if next, decision, err := store.AcquireStrict(context.Background(), leaseRequest); next != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("AcquireStrict(oversized) = %+v, %+v, %v", next, decision, err)
	}
	if err := store.ReleaseStrict(context.Background(), lease); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("ReleaseStrict(oversized) = %v", err)
	}
	afterLease, _ := client.Do(context.Background(), client.B().Hgetall().Key(leaseKey).Build()).AsStrMap()
	if !reflect.DeepEqual(afterLease, beforeLease) {
		t.Fatal("oversized lease state mutated")
	}
}

func TestValkeyStrictSupportsMaximumLeaseCapacity(t *testing.T) {
	address := os.Getenv("VALKEY_ADDRESS")
	if address == "" {
		t.Skip("VALKEY_ADDRESS is required for live Valkey tests")
	}
	client, err := valkeygo.NewClient(valkeygo.ClientOption{InitAddress: []string{address}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	store, err := valkey.New(client, valkey.Options{
		Prefix:  "rate-limit-max-capacity-" + strconv.FormatInt(time.Now().UnixNano(), 36),
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := integrationLeaseRequest(t)
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "maximum-lease-capacity", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 1024, MaxCost: 1024, Lease: time.Hour,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Request.Policy = policy
	leases := make([]ratelimit.Lease, 0, 1024)
	for index := range 1024 {
		request.LeaseID = "lease-" + strconv.Itoa(index)
		lease, decision, acquireErr := store.AcquireStrict(context.Background(), request)
		if acquireErr != nil || !decision.Allowed || decision.Remaining != uint64(1023-index) {
			t.Fatalf("AcquireStrict(%d) = %+v, %+v, %v", index, lease, decision, acquireErr)
		}
		leases = append(leases, lease)
	}
	request.LeaseID = "over-capacity"
	if lease, decision, acquireErr := store.AcquireStrict(context.Background(), request); lease != (ratelimit.Lease{}) || decision.Allowed || !errors.Is(acquireErr, ratelimit.ErrRejected) {
		t.Fatalf("AcquireStrict(over capacity) = %+v, %+v, %v", lease, decision, acquireErr)
	}
	for index, lease := range leases {
		if releaseErr := store.ReleaseStrict(context.Background(), lease); releaseErr != nil {
			t.Fatalf("ReleaseStrict(%d) = %v", index, releaseErr)
		}
	}
}

func TestValkeyLegacyRevisionTransitionPreservesReleasedState(t *testing.T) {
	address := os.Getenv("VALKEY_ADDRESS")
	if address == "" {
		t.Skip("VALKEY_ADDRESS is required for live Valkey tests")
	}
	client, err := valkeygo.NewClient(valkeygo.ClientOption{InitAddress: []string{address}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	prefix := "rate-limit-legacy-revision-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	store, err := valkey.New(client, valkey.Options{Prefix: prefix, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0).UTC()
	request := func(revision string, capacity uint64) ratelimit.Request {
		return integrationRequestAt(t, ratelimit.TokenBucket, "legacy-revision", revision, capacity, now)
	}
	first := request("v1", 10)
	if decision, err := store.Admit(context.Background(), first); err != nil || decision.Remaining != 9 {
		t.Fatalf("initial Admit() = %+v, %v", decision, err)
	}
	if decision, err := store.Admit(context.Background(), request("v2", 2)); err != nil || decision.Remaining != 8 {
		t.Fatalf("revision Admit() = %+v, %v; want released remaining 8", decision, err)
	}
	digest := sha256.Sum256([]byte(first.Policy.ID() + "\x00" + first.Key.String()))
	storageKey := prefix + ":{" + hex.EncodeToString(digest[:]) + "}"
	if err := client.Do(context.Background(), client.B().Hset().Key(storageKey).FieldValue().FieldValue("legacy-extra", "1").Build()).Error(); err != nil {
		t.Fatalf("add released tolerated field: %v", err)
	}
	if decision, err := store.Admit(context.Background(), request("v3", 2)); err != nil || decision.Remaining != 7 {
		t.Fatalf("tolerated-state Admit() = %+v, %v; want released remaining 7", decision, err)
	}
}

func TestValkey9StrictRejectsClampedRangeAndAggregateCorruption(t *testing.T) {
	address := os.Getenv("VALKEY_ADDRESS")
	if address == "" {
		t.Skip("VALKEY_ADDRESS is required for live Valkey tests")
	}
	client, err := valkeygo.NewClient(valkeygo.ClientOption{InitAddress: []string{address}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	prefix := "rate-limit-strict-boundaries-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	store, err := valkey.NewStrict(client, valkey.Options{Prefix: prefix, Timeout: time.Second, Clock: valkey.ClientClock})
	if err != nil {
		t.Fatal(err)
	}
	base := integrationRequestAt(t, ratelimit.TokenBucket, "clamped-admit", "v1", 2, time.UnixMicro(9_007_199_254_740_990))
	shortPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: base.Policy.ID(), Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Capacity: 2, MaxCost: 2, Period: time.Microsecond, Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	base.Policy = shortPolicy
	if _, err := store.AdmitStrict(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	longPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: base.Policy.ID(), Revision: "v2", Algorithm: ratelimit.TokenBucket,
		Capacity: 2, MaxCost: 2, Period: 2 * time.Microsecond, Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	base.Policy = longPolicy
	base.Now = time.UnixMicro(9_007_199_254_740_989)
	admitBefore, err := client.Do(context.Background(), client.B().Hgetall().Key(valkeyIntegrationStateKey(prefix, base)).Build()).AsStrMap()
	if err != nil {
		t.Fatal(err)
	}
	if decision, err := store.AdmitStrict(context.Background(), base); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrOverflow) {
		t.Fatalf("clamped admit = %+v, %v", decision, err)
	}
	admitAfter, err := client.Do(context.Background(), client.B().Hgetall().Key(valkeyIntegrationStateKey(prefix, base)).Build()).AsStrMap()
	if err != nil || !reflect.DeepEqual(admitAfter, admitBefore) {
		t.Fatalf("clamped admit mutated state: before=%v after=%v error=%v", admitBefore, admitAfter, err)
	}
	shortLeasePolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "clamped-lease", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 2, MaxCost: 2, Lease: time.Microsecond, Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseRequest := ratelimit.LeaseRequest{
		Request: ratelimit.Request{
			Policy: shortLeasePolicy, Key: base.Key, Cost: 1,
			Now: time.UnixMicro(9_007_199_254_740_990),
		},
		LeaseID: "first",
	}
	if _, _, err := store.AcquireStrict(context.Background(), leaseRequest); err != nil {
		t.Fatal(err)
	}
	longLeasePolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: shortLeasePolicy.ID(), Revision: "v2", Algorithm: ratelimit.Concurrency,
		Capacity: 2, MaxCost: 2, Lease: 2 * time.Microsecond, Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseRequest.Request.Policy = longLeasePolicy
	leaseRequest.Request.Now = time.UnixMicro(9_007_199_254_740_989)
	leaseRequest.LeaseID = "second"
	leaseStateKey := valkeyIntegrationStateKey(prefix, leaseRequest.Request)
	leaseBefore, err := client.Do(context.Background(), client.B().Hgetall().Key(leaseStateKey).Build()).AsStrMap()
	if err != nil {
		t.Fatal(err)
	}
	if lease, decision, err := store.AcquireStrict(context.Background(), leaseRequest); lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrOverflow) {
		t.Fatalf("clamped acquire = %+v, %+v, %v", lease, decision, err)
	}
	leaseAfter, err := client.Do(context.Background(), client.B().Hgetall().Key(leaseStateKey).Build()).AsStrMap()
	if err != nil || !reflect.DeepEqual(leaseAfter, leaseBefore) {
		t.Fatalf("clamped acquire mutated state: before=%v after=%v error=%v", leaseBefore, leaseAfter, err)
	}

	fixed := integrationRequestAt(t, ratelimit.FixedWindow, "fixed-corrupt", "v1", 2, time.Unix(100, 0))
	if _, err := store.AdmitStrict(context.Background(), fixed); err != nil {
		t.Fatal(err)
	}
	if err := client.Do(context.Background(), client.B().Hset().Key(valkeyIntegrationStateKey(prefix, fixed)).FieldValue().FieldValue("used", "3").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if decision, err := store.AdmitStrict(context.Background(), fixed); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("fixed over-limit state = %+v, %v", decision, err)
	}
	rollbackFixed := integrationRequestAt(t, ratelimit.FixedWindow, "fixed-rollback", "v1", 2, time.UnixMicro(100_500_000))
	rollbackFixed.Policy, err = ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: rollbackFixed.Policy.ID(), Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 2, MaxCost: 2, Period: time.Second, Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AdmitStrict(context.Background(), rollbackFixed); err != nil {
		t.Fatal(err)
	}
	rollbackFixed.Now = time.UnixMicro(99_500_000)
	if decision, err := store.AdmitStrict(context.Background(), rollbackFixed); err != nil || !decision.Allowed || decision.Remaining != 0 || !decision.Reset.Equal(time.Unix(101, 0)) {
		t.Fatalf("fixed rollback clamp = %+v, %v", decision, err)
	}

	sliding := integrationRequestAt(t, ratelimit.SlidingWindow, "sliding-corrupt", "v1", 2, time.Unix(100, 0))
	if _, err := store.AdmitStrict(context.Background(), sliding); err != nil {
		t.Fatal(err)
	}
	slidingKey := valkeyIntegrationStateKey(prefix, sliding)
	if err := client.Do(context.Background(), client.B().Hset().Key(slidingKey).FieldValue().
		FieldValue("b10", "26:9007199254740991").FieldValue("b11", "11:1").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if decision, err := store.AdmitStrict(context.Background(), sliding); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("aggregate sliding overflow = %+v, %v", decision, err)
	}
	slidingOverLimit := integrationRequestAt(t, ratelimit.SlidingWindow, "sliding-over-limit", "v1", 2, time.Unix(100, 0))
	if _, err := store.AdmitStrict(context.Background(), slidingOverLimit); err != nil {
		t.Fatal(err)
	}
	slidingOverLimitKey := valkeyIntegrationStateKey(prefix, slidingOverLimit)
	if err := client.Do(context.Background(), client.B().Hset().Key(slidingOverLimitKey).FieldValue().
		FieldValue("b10", "26:2").FieldValue("b11", "11:1").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if decision, err := store.AdmitStrict(context.Background(), slidingOverLimit); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("same-revision sliding over limit = %+v, %v", decision, err)
	}
	carriedSliding := integrationRequestAt(t, ratelimit.SlidingWindow, "sliding-carry-boundary", "v1", 5, time.Unix(100, 0))
	if _, err := store.AdmitStrict(context.Background(), carriedSliding); err != nil {
		t.Fatal(err)
	}
	carriedSlidingKey := valkeyIntegrationStateKey(prefix, carriedSliding)
	if err := client.Do(context.Background(), client.B().Hset().Key(carriedSlidingKey).FieldValue().
		FieldValue("b10", "26:2").FieldValue("b11", "11:2").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	carriedSliding.Policy, err = ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: carriedSliding.Policy.ID(), Revision: "v2", Algorithm: ratelimit.SlidingWindow,
		Capacity: 2, MaxCost: 2, Period: time.Minute, Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision, err := store.AdmitStrict(context.Background(), carriedSliding); !errors.Is(err, ratelimit.ErrRejected) || decision.Remaining != 0 {
		t.Fatalf("sliding revision carry = %+v, %v", decision, err)
	}
	carriedState, err := client.Do(context.Background(), client.B().Hgetall().Key(carriedSlidingKey).Build()).AsStrMap()
	if err != nil || carriedState["revision"] != "v2" || carriedState["carried"] != "1" || carriedState["b10"] != "26:2" || carriedState["b11"] != "11:2" {
		t.Fatalf("sliding revision state = %v, %v", carriedState, err)
	}
	if decision, err := store.AdmitStrict(context.Background(), carriedSliding); !errors.Is(err, ratelimit.ErrRejected) || decision.Remaining != 0 {
		t.Fatalf("sliding carried state = %+v, %v", decision, err)
	}
}

func TestValkey9StrictWindowRevisionIntegrity(t *testing.T) {
	address := os.Getenv("VALKEY_ADDRESS")
	if address == "" {
		t.Skip("VALKEY_ADDRESS is required for live Valkey tests")
	}
	client, err := valkeygo.NewClient(valkeygo.ClientOption{InitAddress: []string{address}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	prefix := "rate-limit-strict-window-integrity-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	store, err := valkey.NewStrict(client, valkey.Options{Prefix: prefix, Timeout: time.Second, Clock: valkey.ClientClock})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "case", Value: prefix}, Hash: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := func(id, revision string, algorithm ratelimit.Algorithm, capacity uint64, period time.Duration) ratelimit.Policy {
		t.Helper()
		result, policyErr := ratelimit.NewPolicy(ratelimit.PolicySpec{
			ID: id, Revision: revision, Algorithm: algorithm,
			Capacity: capacity, MaxCost: capacity, Period: period,
			Consistency: ratelimit.ConsistencyStrong,
		})
		if policyErr != nil {
			t.Fatal(policyErr)
		}
		return result
	}

	for _, algorithm := range []ratelimit.Algorithm{ratelimit.FixedWindow, ratelimit.SlidingWindow} {
		t.Run(string(algorithm), func(t *testing.T) {
			t.Run("period change", func(t *testing.T) {
				request := ratelimit.Request{
					Policy: policy("period-change-"+string(algorithm), "v1", algorithm, 1, 10*time.Second),
					Key:    key, Cost: 1, Now: time.Unix(15, 0),
				}
				if decision, admitErr := store.AdmitStrict(context.Background(), request); admitErr != nil || !decision.Allowed {
					t.Fatalf("initial admit = %+v, %v", decision, admitErr)
				}
				stateKey := valkeyIntegrationStateKey(prefix, request)
				before, getErr := client.Do(context.Background(), client.B().Hgetall().Key(stateKey).Build()).AsStrMap()
				if getErr != nil {
					t.Fatal(getErr)
				}
				request.Policy = policy("period-change-"+string(algorithm), "v2", algorithm, 1, time.Minute)
				if decision, admitErr := store.AdmitStrict(context.Background(), request); decision != (ratelimit.Decision{}) || !errors.Is(admitErr, ratelimit.ErrCorrupt) {
					t.Fatalf("period change = %+v, %v", decision, admitErr)
				}
				after, getErr := client.Do(context.Background(), client.B().Hgetall().Key(stateKey).Build()).AsStrMap()
				if getErr != nil || !reflect.DeepEqual(after, before) {
					t.Fatalf("period change mutated state: before=%v after=%v, %v", before, after, getErr)
				}
			})

			t.Run("carry history", func(t *testing.T) {
				request := ratelimit.Request{
					Policy: policy("carry-history-"+string(algorithm), "v1", algorithm, 5, time.Minute),
					Key:    key, Cost: 4, Now: time.Unix(100, 0),
				}
				if decision, admitErr := store.AdmitStrict(context.Background(), request); admitErr != nil || !decision.Allowed {
					t.Fatalf("initial carry = %+v, %v", decision, admitErr)
				}
				request.Policy = policy("carry-history-"+string(algorithm), "v2", algorithm, 2, time.Minute)
				request.Cost = 1
				if decision, admitErr := store.AdmitStrict(context.Background(), request); !errors.Is(admitErr, ratelimit.ErrRejected) || decision.Remaining != 0 {
					t.Fatalf("decreased carry = %+v, %v", decision, admitErr)
				}
				request.Policy = policy("carry-history-"+string(algorithm), "v3", algorithm, 5, time.Minute)
				request.Cost = 3
				if decision, admitErr := store.AdmitStrict(context.Background(), request); !errors.Is(admitErr, ratelimit.ErrRejected) || decision.Remaining != 1 {
					t.Fatalf("increased carry = %+v, %v", decision, admitErr)
				}
			})
		})
	}

	zero := ratelimit.Request{
		Policy: policy("zero-reset", "v1", ratelimit.SlidingWindow, 2, time.Minute),
		Key:    key, Cost: 1, Now: time.Unix(100, 0),
	}
	if _, err := store.AdmitStrict(context.Background(), zero); err != nil {
		t.Fatal(err)
	}
	zeroKey := valkeyIntegrationStateKey(prefix, zero)
	if err := client.Do(context.Background(), client.B().Hset().Key(zeroKey).FieldValue().
		FieldValue("b10", "26:2").FieldValue("b11", "11:0").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	wantReset := time.UnixMicro((26+1)*3_750_000 + 60_000_000)
	if decision, admitErr := store.AdmitStrict(context.Background(), zero); !errors.Is(admitErr, ratelimit.ErrRejected) ||
		decision.Remaining != 0 || !decision.Reset.Equal(wantReset) || decision.RetryAfter != wantReset.Sub(zero.Now) {
		t.Fatalf("zero-use reset = %+v, %v", decision, admitErr)
	}
}

func valkeyIntegrationStateKey(prefix string, request ratelimit.Request) string {
	digest := sha256.Sum256([]byte(request.Policy.ID() + "\x00" + request.Key.String()))
	return prefix + ":{" + hex.EncodeToString(digest[:]) + "}"
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

func integrationRequestAt(t *testing.T, algorithm ratelimit.Algorithm, id, revision string, capacity uint64, now time.Time) ratelimit.Request {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: id, Revision: revision, Algorithm: algorithm,
		Capacity: capacity, Period: time.Minute, MaxCost: capacity,
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
	return ratelimit.Request{Policy: policy, Key: key, Cost: 1, Now: now}
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

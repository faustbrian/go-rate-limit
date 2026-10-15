package postgres

import (
	"context"
	"errors"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Risk plan: real SQL proves indivisible debit, retained rejection, command
// conflicts, original receipts after refill/restart, overlapping reversed
// bucket order, and actual committed state after an injected lost COMMIT ack.
// Only the acknowledgement boundary is doubled, not storage or the algorithm.
func TestAtomicPostgresAdmission(t *testing.T) {
	dsn := os.Getenv("POSTGRES_URL")
	if dsn == "" {
		t.Skip("POSTGRES_URL is required")
	}
	// This fixture drops package tables. Reject every remote/socket fallback
	// before opening a connection; the caller must supply a disposable database.
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid disposable PostgreSQL configuration")
	}
	hosts := []string{config.ConnConfig.Host}
	for _, fallback := range config.ConnConfig.Fallbacks {
		hosts = append(hosts, fallback.Host)
	}
	for _, host := range hosts {
		if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			t.Fatal("atomic fixture requires loopback PostgreSQL with no remote or socket fallback")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, _ = pool.Exec(ctx, AtomicSchemaMigration().Down)
	_, _ = pool.Exec(ctx, SchemaMigration().Down)
	if _, err := pool.Exec(ctx, SchemaMigration().Up); err != nil {
		t.Fatal(err)
	}
	options := StrictOptions{Options: Options{Timeout: 3 * time.Second, LockTimeout: time.Second, Clock: ClientClock}, RollbackTimeout: time.Second}
	store, err := NewStrict(pool, options)
	if err != nil {
		t.Fatal(err)
	}
	old := atomicIntegrationRequest(t, "populated", 2)
	if out, err := store.AdmitStrict(ctx, old); err != nil || out.Remaining != 1 {
		t.Fatalf("old state = %+v, %v", out, err)
	}
	if _, err := pool.Exec(ctx, AtomicSchemaMigration().Up); err != nil {
		t.Fatal(err)
	}
	if out, err := store.AdmitStrict(ctx, old); err != nil || out.Remaining != 0 {
		t.Fatalf("migration preserved state = %+v, %v", out, err)
	}
	request := ratelimit.AtomicBatchRequest{CommandID: "first", Requests: []ratelimit.Request{atomicIntegrationRequest(t, "ip", 2), atomicIntegrationRequest(t, "subject", 1)}}
	winner, err := store.AdmitAtomicStrict(ctx, request)
	if err != nil || !winner.Allowed || winner.Decisions[0].Remaining != 1 || winner.Decisions[1].Remaining != 0 {
		t.Fatalf("winner = %+v, %v", winner, err)
	}
	request.CommandID = "reject"
	denied, err := store.AdmitAtomicStrict(ctx, request)
	if !errors.Is(err, ratelimit.ErrRejected) || denied.Allowed || denied.Decisions[0].Reason != ratelimit.ReasonAtomicRejected || denied.Decisions[0].Remaining != 1 {
		t.Fatalf("denied = %+v, %v", denied, err)
	}
	// Independently consume the remaining IP token: a failed batch did not debit it.
	if decision, err := store.AdmitStrict(ctx, request.Requests[0]); err != nil || decision.Remaining != 0 {
		t.Fatalf("untouched IP = %+v, %v", decision, err)
	}
	restarted, _ := NewStrict(pool, options)
	for i := range request.Requests {
		request.Requests[i].Now = request.Requests[i].Now.Add(10 * time.Hour)
	}
	replayed, err := restarted.AdmitAtomicStrict(ctx, request)
	if !errors.Is(err, ratelimit.ErrRejected) || !reflect.DeepEqual(replayed, denied) {
		t.Fatalf("retained rejection = %+v, %v", replayed, err)
	}
	request.CommandID = "first"
	replayed, err = restarted.AdmitAtomicStrict(ctx, request)
	if err != nil || !reflect.DeepEqual(replayed, winner) {
		t.Fatalf("original winner = %+v, %v", replayed, err)
	}
	request.Requests[0].Cost = 2
	if out, err := restarted.AdmitAtomicStrict(ctx, request); !errors.Is(err, ratelimit.ErrInvalidRequest) || out.Allowed {
		t.Fatalf("changed intent = %+v, %v", out, err)
	}

	// Commit actually reaches PostgreSQL; only its acknowledgement is lost.
	lost := ratelimit.AtomicBatchRequest{CommandID: "lost", Requests: []ratelimit.Request{atomicIntegrationRequest(t, "lost-ip", 2), atomicIntegrationRequest(t, "lost-subject", 2)}}
	native := store.executor.(*nativeExecutor)
	original := native.database
	native.database = atomicLostAckDatabase{nativeDatabase: original}
	if out, err := store.AdmitAtomicStrict(ctx, lost); out.Allowed || !errors.Is(err, ratelimit.ErrOutcomeUnknown) {
		t.Fatalf("lost ack = %+v, %v", out, err)
	}
	native.database = original
	for range 2 {
		if out, err := restarted.AdmitAtomicStrict(ctx, lost); err != nil || !out.Allowed || out.Decisions[0].Remaining != 1 {
			t.Fatalf("reconciled = %+v, %v", out, err)
		}
	}
	if out, err := restarted.AdmitStrict(ctx, lost.Requests[0]); err != nil || out.Remaining != 0 {
		t.Fatalf("one debit only = %+v, %v", out, err)
	}

	rawKey := ratelimit.AtomicBatchRequest{CommandID: "raw-key-intent", Requests: []ratelimit.Request{atomicIntegrationRequest(t, "raw-key-policy", 2)}}
	for i, value := range []byte{0xff, 0xfe} {
		key, err := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "atomic", Version: "v1", Subject: ratelimit.Subject{Kind: "test", Value: string([]byte{value})}})
		if err != nil {
			t.Fatal("valid raw key rejected")
		}
		rawKey.Requests[0].Key = key
		out, err := restarted.AdmitAtomicStrict(ctx, rawKey)
		if i == 0 && (err != nil || !out.Allowed) {
			t.Fatal("original raw-key command denied")
		}
		if i == 1 {
			if !errors.Is(err, ratelimit.ErrInvalidRequest) || out.Allowed {
				t.Fatal("changed raw-key intent reused original admission")
			}
			// Changed-command rejection must not debit the second bucket.
			if decision, err := restarted.AdmitStrict(ctx, rawKey.Requests[0]); err != nil || decision.Remaining != 1 {
				t.Fatal("changed raw-key intent debited another bucket")
			}
		}
	}

	a := atomicIntegrationRequest(t, "overlap-a", 1)
	b := atomicIntegrationRequest(t, "overlap-b", 1)
	type result struct {
		out ratelimit.AtomicBatchDecision
		err error
	}
	results := make(chan result, 2)
	for i, requests := range [][]ratelimit.Request{{a, b}, {b, a}} {
		go func(i int, requests []ratelimit.Request) {
			out, err := restarted.AdmitAtomicStrict(ctx, ratelimit.AtomicBatchRequest{CommandID: []string{"overlap1", "overlap2"}[i], Requests: requests})
			results <- result{out, err}
		}(i, requests)
	}
	allowed := 0
	for range 2 {
		r := <-results
		if r.err == nil && r.out.Allowed {
			allowed++
		} else if !errors.Is(r.err, ratelimit.ErrRejected) {
			t.Fatalf("overlap = %+v, %v", r.out, r.err)
		}
	}
	if allowed != 1 {
		t.Fatalf("overlap winners = %d", allowed)
	}
	// A journal insert failure happens after both speculative state writes;
	// rollback must leave both buckets entirely available and no command row.
	failed := ratelimit.AtomicBatchRequest{CommandID: "rollback", Requests: []ratelimit.Request{atomicIntegrationRequest(t, "rollback-a", 2), atomicIntegrationRequest(t, "rollback-b", 2)}}
	native.database = atomicInsertFailureDatabase{nativeDatabase: original}
	if out, err := store.AdmitAtomicStrict(ctx, failed); out.Allowed || !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("insert failure = %+v, %v", out, err)
	}
	native.database = original
	if out, err := restarted.AdmitAtomicStrict(ctx, failed); err != nil || out.Decisions[0].Remaining != 1 || out.Decisions[1].Remaining != 1 {
		t.Fatalf("both rolled back = %+v, %v", out, err)
	}
	// Server authority overrides stale client Now once, and receipts retain it.
	serverOptions := options
	serverOptions.Clock = ServerClock
	server, _ := NewStrict(pool, serverOptions)
	serverRequest := ratelimit.AtomicBatchRequest{CommandID: "server", Requests: []ratelimit.Request{atomicIntegrationRequest(t, "server-a", 2)}}
	before := time.Now().Add(-time.Second)
	serverResult, err := server.AdmitAtomicStrict(ctx, serverRequest)
	if err != nil || !serverResult.Allowed || serverResult.Decisions[0].Reset.Before(before) {
		t.Fatalf("server clock = %+v, %v", serverResult, err)
	}
	serverRequest.Requests[0].Now = serverRequest.Requests[0].Now.Add(24 * time.Hour)
	if out, err := server.AdmitAtomicStrict(ctx, serverRequest); err != nil || !reflect.DeepEqual(out, serverResult) {
		t.Fatalf("server replay = %+v, %v", out, err)
	}
	// The same command cannot silently change clock authority.
	if out, err := restarted.AdmitAtomicStrict(ctx, serverRequest); out.Allowed || !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("clock conflict = %+v, %v", out, err)
	}
	burstPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: "burst", Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 1, Burst: 2, MaxCost: 3, Period: time.Hour, Consistency: ratelimit.ConsistencyStrong})
	if err != nil {
		t.Fatal(err)
	}
	burst := ratelimit.AtomicBatchRequest{CommandID: "burst-drain", Requests: []ratelimit.Request{atomicIntegrationRequest(t, "unused", 1)}}
	burst.Requests[0].Policy, burst.Requests[0].Cost = burstPolicy, 3
	if out, err := restarted.AdmitAtomicStrict(ctx, burst); err != nil || !out.Allowed || out.Decisions[0].Remaining != 0 {
		t.Fatalf("burst drain = %+v, %v", out, err)
	}
	burst.CommandID = "burst-reject"
	burstDenied, err := restarted.AdmitAtomicStrict(ctx, burst)
	if !errors.Is(err, ratelimit.ErrRejected) || burstDenied.Decisions[0].RetryAfter != 3*time.Hour {
		t.Fatalf("burst rejection = %+v, %v", burstDenied, err)
	}
	burst.CommandID = "burst-lost-rejection"
	native.database = atomicLostAckDatabase{nativeDatabase: original}
	if out, err := store.AdmitAtomicStrict(ctx, burst); out.Allowed || !errors.Is(err, ratelimit.ErrOutcomeUnknown) {
		t.Fatalf("lost rejection acknowledgement = %+v, %v", out, err)
	}
	native.database = original
	if out, err := restarted.AdmitAtomicStrict(ctx, burst); !errors.Is(err, ratelimit.ErrRejected) || out.Decisions[0].RetryAfter != 3*time.Hour {
		t.Fatalf("burst rejected reconciliation = %+v, %v", out, err)
	}
	burst.CommandID = "burst-reject"
	burst.Requests[0].Now = burst.Requests[0].Now.Add(24 * time.Hour)
	if out, err := restarted.AdmitAtomicStrict(ctx, burst); !errors.Is(err, ratelimit.ErrRejected) || !reflect.DeepEqual(out, burstDenied) {
		t.Fatalf("burst retained rejection = %+v, %v", out, err)
	}
	burst.CommandID = "burst-new-two-hours"
	burst.Requests[0].Now = time.Unix(100, 0).Add(2 * time.Hour)
	if out, err := restarted.AdmitAtomicStrict(ctx, burst); !errors.Is(err, ratelimit.ErrRejected) || out.Decisions[0].Remaining != 2 || out.Decisions[0].RetryAfter != time.Hour {
		t.Fatalf("burst before full refill = %+v, %v", out, err)
	}
	legacy, _ := New(pool, options.Options)
	for _, writer := range []struct {
		name  string
		admit func(context.Context, ratelimit.Request) (ratelimit.Decision, error)
	}{{"strict", store.AdmitStrict}, {"legacy", legacy.Admit}} {
		p, err := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: "burst-" + writer.name, Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 1, Burst: 2, MaxCost: 3, Period: time.Hour, Consistency: ratelimit.ConsistencyStrong})
		if err != nil {
			t.Fatal(err)
		}
		r := burst.Requests[0]
		r.Policy = p
		r.Now = time.Unix(100, 0)
		if out, err := writer.admit(ctx, r); err != nil || out.Remaining != 0 {
			t.Fatalf("%s drain = %+v, %v", writer.name, out, err)
		}
		r.Now = r.Now.Add(2 * time.Hour)
		if out, err := restarted.AdmitAtomicStrict(ctx, ratelimit.AtomicBatchRequest{CommandID: "after-" + writer.name, Requests: []ratelimit.Request{r}}); !errors.Is(err, ratelimit.ErrRejected) || out.Decisions[0].Remaining != 2 || out.Decisions[0].RetryAfter != time.Hour {
			t.Fatalf("%s retention = %+v, %v", writer.name, out, err)
		}
	}
	rollbackPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: "rollback-clock", Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 1, Burst: 2, MaxCost: 3, Period: time.Hour, Consistency: ratelimit.ConsistencyStrong})
	if err != nil {
		t.Fatal(err)
	}
	backward := burst.Requests[0]
	backward.Policy = rollbackPolicy
	backward.Now = time.Unix(100, 0).Add(10 * time.Hour)
	if out, err := restarted.AdmitAtomicStrict(ctx, ratelimit.AtomicBatchRequest{CommandID: "rollback-clock-drain", Requests: []ratelimit.Request{backward}}); err != nil || !out.Allowed {
		t.Fatalf("future drain = %+v, %v", out, err)
	}
	backward.Now = time.Unix(100, 0)
	if out, err := legacy.Admit(ctx, backward); !errors.Is(err, ratelimit.ErrRejected) || out.Remaining != 0 {
		t.Fatalf("backward legacy observation = %+v, %v", out, err)
	}
	backward.Now = backward.Now.Add(10 * time.Hour)
	if out, err := restarted.AdmitAtomicStrict(ctx, ratelimit.AtomicBatchRequest{CommandID: "rollback-clock-after", Requests: []ratelimit.Request{backward}}); !errors.Is(err, ratelimit.ErrRejected) || out.Decisions[0].Remaining != 0 || out.Decisions[0].RetryAfter != 3*time.Hour {
		t.Fatalf("no refill after clock rollback = %+v, %v", out, err)
	}
}

func atomicIntegrationRequest(t testing.TB, id string, capacity uint64) ratelimit.Request {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: id, Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: capacity, Period: time.Hour, MaxCost: capacity, Consistency: ratelimit.ConsistencyStrong})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "atomic", Version: "v1", Subject: ratelimit.Subject{Kind: "test", Value: "owner"}, Hash: true})
	if err != nil {
		t.Fatal(err)
	}
	return ratelimit.Request{Policy: policy, Key: key, Cost: 1, Now: time.Unix(100, 0)}
}

type atomicLostAckDatabase struct{ nativeDatabase }

func (d atomicLostAckDatabase) begin(ctx context.Context) (nativeTransaction, error) {
	tx, err := d.nativeDatabase.begin(ctx)
	return atomicLostAckTransaction{nativeTransaction: tx}, err
}

type atomicLostAckTransaction struct{ nativeTransaction }

func (tx atomicLostAckTransaction) commit(ctx context.Context) error {
	if err := tx.nativeTransaction.commit(ctx); err != nil {
		return err
	}
	return errors.New("lost commit acknowledgement")
}

type atomicInsertFailureDatabase struct{ nativeDatabase }

func (d atomicInsertFailureDatabase) begin(ctx context.Context) (nativeTransaction, error) {
	tx, err := d.nativeDatabase.begin(ctx)
	return atomicInsertFailureTransaction{nativeTransaction: tx}, err
}

type atomicInsertFailureTransaction struct{ nativeTransaction }

func (tx atomicInsertFailureTransaction) exec(ctx context.Context, query string, args ...any) error {
	if strings.HasPrefix(query, "INSERT INTO rate_limit_atomic_commands") {
		return errors.New("journal failure")
	}
	return tx.nativeTransaction.exec(ctx, query, args...)
}

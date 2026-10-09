package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit/v2"
	"github.com/faustbrian/go-rate-limit/v2/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Keep typed extended-protocol parameters while selecting text timestamp results.
// Simple protocol is not interchangeable: it serializes the owned JSON bytes as bytea.
type textTimestampCodec struct{ pgtype.TimestamptzCodec }

func (textTimestampCodec) PreferredFormat() int16 { return pgx.TextFormatCode }

func TestPGXTimestampFormatsPreserveLeaseInstants(t *testing.T) {
	dsn := os.Getenv("POSTGRES_URL")
	if dsn == "" {
		t.Skip("POSTGRES_URL is required for live PostgreSQL tests")
	}
	for _, format := range []string{"binary", "text"} {
		t.Run(format, func(t *testing.T) {
			ctx := context.Background()
			config, err := pgxpool.ParseConfig(dsn)
			if err != nil {
				t.Fatal(err)
			}
			config.ConnConfig.RuntimeParams["timezone"] = "Asia/Kolkata"
			if format == "text" {
				config.AfterConnect = func(_ context.Context, conn *pgx.Conn) error {
					conn.TypeMap().RegisterType(&pgtype.Type{Name: "timestamptz", OID: pgtype.TimestamptzOID,
						Codec: &textTimestampCodec{pgtype.TimestamptzCodec{ScanLocation: time.FixedZone("client", -8*60*60)}}})
					return nil
				}
			}
			pool, err := pgxpool.NewWithConfig(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closePoolWithin(t, pool) })
			migration := postgres.SchemaMigration()
			if _, err := pool.Exec(ctx, migration.Up); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = pool.Exec(context.Background(), migration.Down) })
			store, err := postgres.OpenStrict(ctx, pool, postgres.StrictOptions{
				Options:         postgres.Options{Timeout: time.Second, LockTimeout: time.Second, Clock: postgres.ClientClock},
				RollbackTimeout: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: "pgx-lease-" + format,
				Revision: "v1", Algorithm: ratelimit.Concurrency, Capacity: 2, MaxCost: 2,
				Lease: time.Second, Consistency: ratelimit.ConsistencyStrong})
			if err != nil {
				t.Fatal(err)
			}
			key, err := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "test", Version: "v1",
				Subject: ratelimit.Subject{Kind: "case", Value: format}, Hash: true})
			if err != nil {
				t.Fatal(err)
			}
			request := ratelimit.LeaseRequest{Request: ratelimit.Request{Policy: policy, Key: key, Cost: 1}, LeaseID: "job-1"}
			request.Request.Now = time.UnixMicro(100_123456).In(time.FixedZone("caller", 5*60*60+30*60))
			lease, decision, err := store.AcquireStrict(ctx, request)
			want := request.Request.Now.Add(time.Second)
			if err != nil || !decision.Allowed || !lease.ExpiresAt.Equal(want) || !decision.Reset.Equal(want) {
				t.Fatalf("lease instant: lease=%+v decision=%+v error=%v", lease, decision, err)
			}
			var scanned time.Time
			var micros int64
			if err := pool.QueryRow(ctx, "SELECT expires_at, (extract(epoch FROM expires_at) * 1000000)::bigint FROM rate_limit_states").Scan(&scanned, &micros); err != nil {
				t.Fatal(err)
			}
			// Strict state retention is twice the lease duration, independently of lease expiry.
			wantRetention := request.Request.Now.Add(2 * time.Second)
			if !scanned.Equal(wantRetention) || micros != wantRetention.UnixMicro() {
				t.Fatalf("persisted instant: scanned=%v micros=%d want=%v", scanned, micros, wantRetention)
			}
			retried, retryDecision, err := store.AcquireStrict(ctx, request)
			if err != nil || !retryDecision.Allowed || !retried.ExpiresAt.Equal(lease.ExpiresAt) || retryDecision.Remaining != decision.Remaining {
				t.Fatalf("lease retry: lease=%+v decision=%+v error=%v", retried, retryDecision, err)
			}
			if err := store.ReleaseStrict(ctx, lease); err != nil {
				t.Fatal(err)
			}
			if err := pool.Ping(ctx); err != nil {
				t.Fatalf("borrowed pool unusable: %v", err)
			}
		})
	}
}

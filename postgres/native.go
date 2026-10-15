package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// MaxCleanupBatch bounds rows locked and returned by one cleanup statement.
	MaxCleanupBatch      = 10_000
	maxEncodedStateBytes = 128 * 1024
	setLockTimeoutSQL    = "SELECT set_config('lock_timeout', $1, true)"
	lockAndTimeSQL       = "SELECT pg_advisory_xact_lock($1), clock_timestamp()"
	selectStateSQL       = "SELECT state, expires_at FROM rate_limit_states " +
		"WHERE state_key = $1 FOR UPDATE"
	selectStateStrictSQL = "SELECT CASE WHEN octet_length(state::text) <= $2 THEN state END, expires_at, " +
		"octet_length(state::text) <= $2 FROM rate_limit_states WHERE state_key = $1 FOR UPDATE"
	deleteStateSQL = "DELETE FROM rate_limit_states WHERE state_key = $1"
	upsertStateSQL = "INSERT INTO rate_limit_states " +
		"(state_key, state, expires_at, updated_at) VALUES ($1, $2, $3, $4) " +
		"ON CONFLICT (state_key) DO UPDATE SET state = EXCLUDED.state, " +
		"expires_at = EXCLUDED.expires_at, updated_at = EXCLUDED.updated_at"
	cleanupSQL = "WITH doomed AS (" +
		"SELECT state_key FROM rate_limit_states WHERE expires_at <= clock_timestamp() " +
		"ORDER BY expires_at LIMIT $1 FOR UPDATE SKIP LOCKED), deleted AS (" +
		"DELETE FROM rate_limit_states AS states USING doomed " +
		"WHERE states.state_key = doomed.state_key RETURNING 1) " +
		"SELECT count(*) FROM deleted"
)

type nativeExecutor struct {
	database nativeDatabase
	options  Options
}

type nativeDatabase interface {
	begin(context.Context) (nativeTransaction, error)
	queryRow(context.Context, string, ...any) pgx.Row
}

type nativeTransaction interface {
	queryRow(context.Context, string, ...any) pgx.Row
	exec(context.Context, string, ...any) error
	commit(context.Context) error
	rollback(context.Context) error
}

type transactionPool interface {
	Begin(context.Context) (pgx.Tx, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type poolDatabase struct{ pool transactionPool }

func (database poolDatabase) begin(ctx context.Context) (nativeTransaction, error) {
	tx, err := database.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return poolTransaction{tx: tx}, nil
}

func (database poolDatabase) queryRow(ctx context.Context, query string, args ...any) pgx.Row {
	return database.pool.QueryRow(ctx, query, args...)
}

type poolTransaction struct{ tx pgx.Tx }

func (tx poolTransaction) queryRow(ctx context.Context, query string, args ...any) pgx.Row {
	return tx.tx.QueryRow(ctx, query, args...)
}

func (tx poolTransaction) exec(ctx context.Context, query string, args ...any) error {
	_, err := tx.tx.Exec(ctx, query, args...)
	return err
}

func (tx poolTransaction) commit(ctx context.Context) error   { return tx.tx.Commit(ctx) }
func (tx poolTransaction) rollback(ctx context.Context) error { return tx.tx.Rollback(ctx) }

// New constructs a Store without checking whether its migration is installed.
// The pool is borrowed and never closed; concurrent calls rely on pool safety.
func New(pool *pgxpool.Pool, options Options) (*Store, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: pgx pool is required", ratelimit.ErrInvalidPolicy)
	}
	if options.LockTimeout < 1 {
		options.LockTimeout = options.Timeout
	}
	return newStore(&nativeExecutor{database: poolDatabase{pool: pool}, options: options}, options)
}

// Open constructs a Store and verifies the package-owned table exists.
func Open(ctx context.Context, pool *pgxpool.Pool, options Options) (*Store, error) {
	return openChecked(ctx, func() (*Store, error) { return New(pool, options) })
}

func openChecked(ctx context.Context, construct func() (*Store, error)) (*Store, error) {
	store, err := construct()
	if err != nil {
		return nil, err
	}
	if err := store.Check(ctx); err != nil {
		return nil, err
	}
	return store, nil
}

// Check verifies that rate_limit_states resolves via to_regclass. It does not
// verify column/index definitions or migration contents. It uses the caller
// context directly, not Options.Timeout; migration application is caller-owned.
func (store *Store) Check(ctx context.Context) error {
	native, ok := store.executor.(*nativeExecutor)
	if !ok {
		return ratelimit.ErrUnsupported
	}
	var table *string
	if err := native.database.queryRow(ctx, "SELECT to_regclass('rate_limit_states')::text").Scan(&table); err != nil {
		return fmt.Errorf("%w: schema check", ratelimit.ErrUnavailable)
	}
	if table == nil {
		return fmt.Errorf("%w: rate_limit_states migration is missing", ratelimit.ErrUnavailable)
	}
	return nil
}

// Cleanup deletes at most batch expired states using SKIP LOCKED. It uses the
// caller context directly and owns no scheduling; removal permits fresh state
// on later admission. batch must be in 1..MaxCleanupBatch.
func (store *Store) Cleanup(ctx context.Context, batch int) (int64, error) {
	if batch < 1 {
		return 0, fmt.Errorf("%w: cleanup batch must be positive", ratelimit.ErrInvalidRequest)
	}
	if batch > MaxCleanupBatch {
		return 0, fmt.Errorf("%w: cleanup batch must be positive", ratelimit.ErrInvalidRequest)
	}
	native, ok := store.executor.(*nativeExecutor)
	if !ok {
		return 0, ratelimit.ErrUnsupported
	}
	var count int64
	if err := native.database.queryRow(ctx, cleanupSQL, batch).Scan(&count); err != nil {
		return 0, fmt.Errorf("%w: cleanup", ratelimit.ErrUnavailable)
	}
	return count, nil
}

func (executor *nativeExecutor) admit(ctx context.Context, key []byte, request ratelimit.Request) (decision ratelimit.Decision, resultErr error) {
	tx, serverNow, err := executor.beginLocked(ctx, key)
	if err != nil {
		return ratelimit.Decision{}, err
	}
	defer func() { _ = tx.rollback(context.WithoutCancel(ctx)) }()
	if executor.options.Clock == ServerClock {
		request.Now = serverNow.UTC()
	}
	current, err := loadState(ctx, tx, key, request.Now)
	if err != nil {
		return ratelimit.Decision{}, err
	}
	next, decision, resultErr := mutateStateLegacy(current, request)
	if resultErr != nil {
		if !errors.Is(resultErr, ratelimit.ErrRejected) {
			return ratelimit.Decision{}, resultErr
		}
	}
	encoded := encodeState(next)
	ttl := max(request.Policy.Period()*2, time.Second)
	effective := request.Now
	if request.Policy.Algorithm() == ratelimit.TokenBucket {
		ttl = postgresAdmissionTTL(request.Policy)
		// Mutation clamps backwards client observations in its own request
		// copy. Expiry must follow that persisted time, not the borrowed time.
		effective = time.UnixMicro(next.ObservedMicros).UTC()
	}
	if err := tx.exec(ctx, upsertStateSQL, key, encoded, effective.Add(ttl), effective); err != nil {
		return ratelimit.Decision{}, err
	}
	if err := tx.commit(ctx); err != nil {
		return ratelimit.Decision{}, err
	}
	return decision, resultErr
}

func loadState(ctx context.Context, tx nativeTransaction, key []byte, now time.Time) (*persistedState, error) {
	var encoded []byte
	var expiresAt time.Time
	err := tx.queryRow(ctx, selectStateSQL, key).Scan(&encoded, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if expiresAt.After(now) {
		return decodeStateLegacy(encoded)
	}
	return nil, tx.exec(ctx, deleteStateSQL, key)
}

func loadStateStrict(ctx context.Context, tx nativeTransaction, key []byte, now time.Time) (*persistedState, error) {
	var encoded []byte
	var expiresAt time.Time
	var withinLimit bool
	err := tx.queryRow(ctx, selectStateStrictSQL, key, maxEncodedStateBytes).Scan(&encoded, &expiresAt, &withinLimit)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !withinLimit {
		return nil, ratelimit.ErrCorrupt
	}
	if expiresAt.After(now) {
		return decodeState(encoded)
	}
	return nil, tx.exec(ctx, deleteStateSQL, key)
}

// advisoryKey uses a digest prefix only for serialization; collisions can
// cause extra contention, while the full digest still identifies persisted rows.
func advisoryKey(key []byte) int64 {
	return int64(binary.BigEndian.Uint64(key[:8])) // #nosec G115 -- advisory lock identity intentionally retains every SHA-256 prefix bit, including the sign bit.
}

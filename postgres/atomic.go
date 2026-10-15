package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strconv"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit/v2"
	"github.com/jackc/pgx/v5"
)

const maxAtomicReceiptBytes = 16 * 1024

// AtomicSchemaMigration is additive migration 2 for strict atomic commands.
// Install it after SchemaMigration. Original command digests and receipts are
// retained indefinitely: deleting them would permit a delayed retry to debit
// again. Retention/archival must preserve a terminal command tombstone; this
// package never expires or automatically cleans these command records.
func AtomicSchemaMigration() Migration {
	return Migration{Version: 2, Name: "create_rate_limit_atomic_commands", Up: `CREATE TABLE rate_limit_atomic_commands (
    command_digest bytea PRIMARY KEY CHECK (octet_length(command_digest) = 32),
    intent_digest bytea NOT NULL CHECK (octet_length(intent_digest) = 32),
    receipt bytea NOT NULL CHECK (octet_length(receipt) <= 16384)
);`, Down: "DROP TABLE rate_limit_atomic_commands;"}
}

// AdmitAtomicStrict admits every input bucket or none in one primary database
// transaction. It requires NewStrict/OpenStrict and AtomicSchemaMigration.
// Stable commands bind complete ordered policy/key/cost intent and clock profile,
// not retry Now. Matching retries return the original committed or rejected
// receipt; changed intent returns ErrInvalidRequest without another debit.
// Commands are deployment-wide, caller-authorized correlation, not credentials.
// ServerClock observes once after all locks; ClientClock is intended only for
// trusted callers. Neither command nor original keys are stored in plaintext.
// A lost commit acknowledgement grants no admission and returns UnknownOutcome;
// reconcile by resubmitting the same original command and intent, never a new ID.
// Pool ownership remains with the caller. No automatic retry or fail-open path.
func (store *Store) AdmitAtomicStrict(ctx context.Context, request ratelimit.AtomicBatchRequest) (ratelimit.AtomicBatchDecision, error) {
	if err := store.strictReady(ctx); err != nil {
		return ratelimit.AtomicBatchDecision{}, err
	}
	if err := request.Validate(); err != nil {
		return ratelimit.AtomicBatchDecision{}, err
	}
	if err := postgresStrictContext(ctx); err != nil {
		return ratelimit.AtomicBatchDecision{}, err
	}
	native, ok := store.executor.(*nativeExecutor)
	if !ok {
		return ratelimit.AtomicBatchDecision{}, ratelimit.ErrInvalidPolicy
	}
	request.Requests = append([]ratelimit.Request(nil), request.Requests...)
	callCtx, cancel := context.WithTimeout(ctx, store.options.Timeout)
	defer cancel()
	return native.admitAtomic(callCtx, request, store.rollbackTimeout)
}

type atomicIntentEntry struct {
	Policy ratelimit.PolicySpec
	Key    []byte
	Kind   string
	Cost   uint64
}

func atomicIntent(request ratelimit.AtomicBatchRequest, clock ClockPolicy) [32]byte {
	entries := make([]atomicIntentEntry, len(request.Requests))
	for i, r := range request.Requests {
		p := r.Policy
		entries[i] = atomicIntentEntry{Policy: ratelimit.PolicySpec{ID: p.ID(), Revision: p.Revision(), Algorithm: p.Algorithm(), Capacity: p.Capacity(), Period: p.Period(), Burst: p.Burst(), MaxCost: p.MaxCost(), FailureMode: p.FailureMode(), Consistency: p.Consistency(), Lease: p.LeaseDuration()}, Key: []byte(r.Key.String()), Kind: r.Key.SubjectKind(), Cost: r.Cost}
	}
	// These private bounded values contain no custom marshaler or unsupported
	// type. Length-delimited JSON avoids ambiguity between adjacent key fields.
	// Key is bytes, not a JSON string: permitted raw non-UTF-8 key bytes must
	// remain distinct rather than being replaced by the same Unicode rune.
	encoded, _ := json.Marshal(struct {
		Version int
		Clock   ClockPolicy
		Entries []atomicIntentEntry
	}{1, clock, entries})
	return sha256.Sum256(encoded)
}

func (executor *nativeExecutor) admitAtomic(ctx context.Context, request ratelimit.AtomicBatchRequest, rollbackTimeout time.Duration) (out ratelimit.AtomicBatchDecision, resultErr error) {
	command := sha256.Sum256([]byte(request.CommandID))
	intent := atomicIntent(request, executor.options.Clock)
	tx, err := executor.database.begin(ctx)
	if err != nil {
		return out, ratelimit.UnknownOutcome(ctx.Err())
	}
	deadline := time.Now().Add(rollbackTimeout)
	committed := false
	defer func() {
		if !committed && rollbackStrict(ctx, tx, deadline) != nil {
			out, resultErr = ratelimit.AtomicBatchDecision{}, ratelimit.UnknownOutcome(ctx.Err())
		}
	}()
	var ignored string
	lockMilliseconds := int64(executor.options.LockTimeout / time.Millisecond)
	if executor.options.LockTimeout%time.Millisecond != 0 {
		lockMilliseconds++
	}
	if err := tx.queryRow(ctx, setLockTimeoutSQL, strconv.FormatInt(lockMilliseconds, 10)+"ms").Scan(&ignored); err != nil {
		return out, postgresPrecommitCategory(ctx, err)
	}
	// PostgreSQL's two-int32 lock space is disjoint from the bigint bucket
	// space used by legacy/strict single admission. Command lock always precedes
	// sorted unique bucket locks; prefix collisions only serialize extra work.
	var lock any
	var commandLock [2]int32
	// The fixed eight-byte digest prefix exactly fills two signed words. Decode
	// preserves their bit patterns without an overflowing numeric conversion.
	_, _ = binary.Decode(command[:8], binary.BigEndian, commandLock[:])
	if err := tx.queryRow(ctx, "SELECT pg_advisory_xact_lock($1::integer,$2::integer)", commandLock[0], commandLock[1]).Scan(&lock); err != nil {
		return out, postgresPrecommitCategory(ctx, err)
	}
	var savedIntent, receipt []byte
	err = tx.queryRow(ctx, "SELECT intent_digest, CASE WHEN octet_length(receipt) <= $2 THEN receipt END FROM rate_limit_atomic_commands WHERE command_digest=$1", command[:], maxAtomicReceiptBytes).Scan(&savedIntent, &receipt)
	if err == nil {
		if !bytes.Equal(savedIntent, intent[:]) {
			return out, ratelimit.ErrInvalidRequest
		}
		out, resultErr = decodeAtomicReceipt(receipt, request)
		if resultErr != nil && !errors.Is(resultErr, ratelimit.ErrRejected) {
			return ratelimit.AtomicBatchDecision{}, resultErr
		}
		if err := tx.commit(ctx); err != nil {
			return ratelimit.AtomicBatchDecision{}, ratelimit.UnknownOutcome(ctx.Err())
		}
		committed = true
		return out, resultErr
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return out, postgresPrecommitCategory(ctx, err)
	}
	keys := make([][32]byte, len(request.Requests))
	locks := make([]int64, 0, len(keys))
	for i, r := range request.Requests {
		keys[i] = sha256.Sum256([]byte(r.Policy.ID() + "\x00" + r.Key.String()))
		locks = append(locks, advisoryKey(keys[i][:]))
	}
	sort.Slice(locks, func(i, j int) bool { return locks[i] < locks[j] })
	for i, key := range locks {
		if i > 0 && key == locks[i-1] {
			continue
		}
		if err := tx.queryRow(ctx, "SELECT pg_advisory_xact_lock($1)", key).Scan(&lock); err != nil {
			return out, postgresPrecommitCategory(ctx, err)
		}
	}
	now := time.UnixMicro(request.Requests[0].Now.UnixMicro()).UTC()
	if executor.options.Clock == ServerClock {
		if err := tx.queryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
			return out, postgresPrecommitCategory(ctx, err)
		}
		now = time.UnixMicro(now.UnixMicro()).UTC()
	}
	states := make([]*persistedState, len(keys))
	for i, key := range keys {
		current, err := loadStateStrict(ctx, tx, key[:], now)
		if err != nil {
			return out, postgresPrecommitCategory(ctx, err)
		}
		states[i] = current
	}
	// One effective observation preserves per-bucket monotonic clock custody.
	for _, state := range states {
		if state != nil && state.ObservedMicros > now.UnixMicro() {
			now = time.UnixMicro(state.ObservedMicros).UTC()
		}
	}
	out = ratelimit.AtomicBatchDecision{CommandID: request.CommandID, Allowed: true, Decisions: make([]ratelimit.Decision, len(keys))}
	for i, r := range request.Requests {
		r.Now = now
		next, decision, err := mutateState(states[i], r)
		decision, err = strictPostgresDecisionResult(ctx, next, decision, err)
		if err != nil && !errors.Is(err, ratelimit.ErrRejected) {
			return ratelimit.AtomicBatchDecision{}, err
		}
		states[i] = next
		decision.Backend, decision.PolicyRevision = "postgres", r.Policy.Revision()
		out.Decisions[i] = decision
		if err != nil {
			out.Allowed = false
		}
	}
	if !out.Allowed {
		for i, decision := range out.Decisions {
			if !decision.Allowed {
				continue
			}
			r := request.Requests[i]
			states[i].Tokens += r.Cost // Restore speculative debit; no state writes on rejection.
			decision.Allowed, decision.Reason = false, ratelimit.ReasonAtomicRejected
			decision.Remaining = states[i].Tokens
			decision.Reset = now.Add(tokenDuration(r.Policy.Limit()-states[i].Tokens, states[i].Remainder, r.Policy))
			out.Decisions[i] = decision
		}
		resultErr = ratelimit.ErrRejected
	} else {
		for i, key := range keys {
			effective := time.UnixMicro(states[i].ObservedMicros).UTC()
			if err := tx.exec(ctx, upsertStateSQL, key[:], encodeState(states[i]), effective.Add(postgresAdmissionTTL(request.Requests[i].Policy)), effective); err != nil {
				return ratelimit.AtomicBatchDecision{}, postgresPrecommitCategory(ctx, err)
			}
		}
	}
	// Exclude raw correlation from durable receipt storage; it is reconstructed
	// only for the caller presenting the same command digest and complete intent.
	saved := out
	saved.CommandID = ""
	encoded, err := json.Marshal(saved)
	if err != nil || len(encoded) > maxAtomicReceiptBytes {
		return ratelimit.AtomicBatchDecision{}, ratelimit.ErrCorrupt
	}
	if err := tx.exec(ctx, "INSERT INTO rate_limit_atomic_commands(command_digest,intent_digest,receipt) VALUES($1,$2,$3)", command[:], intent[:], encoded); err != nil {
		return ratelimit.AtomicBatchDecision{}, postgresPrecommitCategory(ctx, err)
	}
	if err := tx.commit(ctx); err != nil {
		return ratelimit.AtomicBatchDecision{}, ratelimit.UnknownOutcome(ctx.Err())
	}
	committed = true
	return out, resultErr
}

func decodeAtomicReceipt(encoded []byte, request ratelimit.AtomicBatchRequest) (ratelimit.AtomicBatchDecision, error) {
	var out ratelimit.AtomicBatchDecision
	if len(encoded) == 0 || len(encoded) > maxAtomicReceiptBytes {
		return out, ratelimit.ErrCorrupt
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&out) != nil || out.CommandID != "" || len(out.Decisions) != len(request.Requests) {
		return ratelimit.AtomicBatchDecision{}, ratelimit.ErrCorrupt
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return ratelimit.AtomicBatchDecision{}, ratelimit.ErrCorrupt
	}
	limited := false
	for i, d := range out.Decisions {
		r := request.Requests[i]
		if d.Limit != r.Policy.Limit() || d.Remaining > d.Limit || d.Backend != "postgres" || d.PolicyRevision != r.Policy.Revision() || d.Reset.IsZero() {
			return ratelimit.AtomicBatchDecision{}, ratelimit.ErrCorrupt
		}
		if out.Allowed {
			if !d.Allowed || d.Reason != ratelimit.ReasonAllowed || d.RetryAfter != 0 || d.Remaining > d.Limit-r.Cost {
				return ratelimit.AtomicBatchDecision{}, ratelimit.ErrCorrupt
			}
		} else {
			if d.Allowed {
				return ratelimit.AtomicBatchDecision{}, ratelimit.ErrCorrupt
			}
			switch d.Reason {
			case ratelimit.ReasonLimited:
				// Burst permits cost greater than base capacity, hence valid
				// replenishment may span multiple periods. Reuse the checked
				// algorithm's empty-bucket upper bound, not a one-period cap.
				if d.Remaining >= r.Cost || d.RetryAfter <= 0 || d.RetryAfter > tokenDuration(r.Cost, 0, r.Policy) {
					return ratelimit.AtomicBatchDecision{}, ratelimit.ErrCorrupt
				}
				limited = true
			case ratelimit.ReasonAtomicRejected:
				if d.Remaining < r.Cost || d.RetryAfter != 0 {
					return ratelimit.AtomicBatchDecision{}, ratelimit.ErrCorrupt
				}
			default:
				return ratelimit.AtomicBatchDecision{}, ratelimit.ErrCorrupt
			}
		}
	}
	out.CommandID = request.CommandID
	if !out.Allowed {
		if !limited {
			return ratelimit.AtomicBatchDecision{}, ratelimit.ErrCorrupt
		}
		return out, ratelimit.ErrRejected
	}
	return out, nil
}

var _ ratelimit.StrictAtomicBatchBackend = (*Store)(nil)

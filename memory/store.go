package memory

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

const (
	slidingSegments = 16
	// MaxConfiguredKeys bounds retained process-local state.
	MaxConfiguredKeys = 1_000_000
	// MaxConfiguredShards bounds eager lock and map allocation.
	MaxConfiguredShards = 1024
)

// Options configures bounded process-local storage.
type Options struct {
	// MaxKeys is the hard cardinality bound across all shards.
	MaxKeys int
	// Shards controls lock striping and must not exceed MaxKeys.
	Shards int
}

// Store is a bounded, concurrency-safe, process-local backend.
type Store struct {
	shards []shard
	closed atomic.Bool
}

type shard struct {
	mu      sync.Mutex
	maxKeys int
	states  map[string]*state
}

type state struct {
	revision    string
	algorithm   ratelimit.Algorithm
	lastSeen    time.Time
	tokens      uint64
	remainder   uint64
	lastRefill  time.Time
	windowStart int64
	used        uint64
	segments    [slidingSegments]segment
	leases      map[string]leaseState
}

type segment struct {
	index int64
	used  uint64
}

type leaseState struct {
	cost      uint64
	expiresAt time.Time
}

// New validates options and constructs an empty Store.
func New(options Options) (*Store, error) {
	if options.MaxKeys <= 0 || options.MaxKeys > MaxConfiguredKeys ||
		options.Shards <= 0 || options.Shards > MaxConfiguredShards ||
		options.Shards > options.MaxKeys {
		return nil, fmt.Errorf("%w: positive cardinality and shard bounds are required", ratelimit.ErrInvalidPolicy)
	}
	store := &Store{shards: make([]shard, options.Shards)}
	base := options.MaxKeys / options.Shards
	extra := options.MaxKeys % options.Shards
	for index := range store.shards {
		store.shards[index].maxKeys = base
		if index < extra {
			store.shards[index].maxKeys++
		}
		store.shards[index].states = make(map[string]*state)
	}
	return store, nil
}

// Name returns the stable backend identifier.
func (store *Store) Name() string { return "memory" }

// Admit atomically evaluates one non-concurrency request in process.
func (store *Store) Admit(ctx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	if store.closed.Load() {
		return ratelimit.Decision{}, ratelimit.ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return ratelimit.Decision{}, err
	}
	if err := request.Validate(); err != nil {
		return ratelimit.Decision{}, err
	}
	if request.Policy.Algorithm() == ratelimit.Concurrency {
		return ratelimit.Decision{}, ratelimit.ErrUnsupported
	}
	request.Now = time.UnixMicro(request.Now.UnixMicro()).UTC()
	key := stateKey(request)
	target := store.shardFor(key)
	target.mu.Lock()
	defer target.mu.Unlock()

	if existing, ok := target.states[key]; ok && existing.algorithm != request.Policy.Algorithm() {
		return ratelimit.Decision{}, ratelimit.ErrCorrupt
	}
	current, err := target.getOrCreate(key, request)
	if err != nil {
		return ratelimit.Decision{}, err
	}
	if request.Now.Before(current.lastSeen) {
		request.Now = current.lastSeen
	}
	current.lastSeen = request.Now
	switch request.Policy.Algorithm() { //nolint:exhaustive // validated policy; concurrency rejected above
	case ratelimit.TokenBucket:
		return admitToken(current, request)
	case ratelimit.FixedWindow:
		return admitFixed(current, request)
	}
	return admitSliding(current, request)
}

// Acquire atomically obtains a weighted process-local concurrency lease.
func (store *Store) Acquire(ctx context.Context, request ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
	if store.closed.Load() {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	if err := request.Validate(); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	request.Request.Now = time.UnixMicro(request.Request.Now.UnixMicro()).UTC()
	key := stateKey(request.Request)
	target := store.shardFor(key)
	target.mu.Lock()
	defer target.mu.Unlock()
	if existing, ok := target.states[key]; ok && existing.algorithm != request.Request.Policy.Algorithm() {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrCorrupt
	}
	current, err := target.getOrCreate(key, request.Request)
	if err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	if request.Request.Now.Before(current.lastSeen) {
		request.Request.Now = current.lastSeen
	}
	current.lastSeen = request.Request.Now
	if current.leases == nil {
		current.leases = make(map[string]leaseState)
	}
	var used uint64
	for id, existing := range current.leases {
		if !existing.expiresAt.After(request.Request.Now) {
			delete(current.leases, id)
			continue
		}
		used += existing.cost
	}
	limit := request.Request.Policy.Limit()
	remaining := limit - min(used, limit)
	if existing, ok := current.leases[request.LeaseID]; ok {
		if existing.cost != request.Request.Cost {
			return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrLeaseNotOwned
		}
		lease := makeLease(request, existing.expiresAt)
		return lease, ratelimit.Decision{
			Allowed: true, Limit: limit, Remaining: remaining,
			Reset: existing.expiresAt, Reason: ratelimit.ReasonAllowed,
		}, nil
	}
	if request.Request.Cost > remaining {
		reset := earliestLease(current.leases)
		return ratelimit.Lease{}, ratelimit.Decision{
			Allowed: false, Limit: limit, Remaining: remaining, Reset: reset,
			RetryAfter: nonnegative(reset.Sub(request.Request.Now)),
			Reason:     ratelimit.ReasonLimited,
		}, ratelimit.ErrRejected
	}
	expiresAt := request.Request.Now.Add(request.Request.Policy.LeaseDuration())
	current.leases[request.LeaseID] = leaseState{cost: request.Request.Cost, expiresAt: expiresAt}
	lease := makeLease(request, expiresAt)
	return lease, ratelimit.Decision{
		Allowed: true, Limit: limit,
		Remaining: remaining - request.Request.Cost,
		Reset:     expiresAt, Reason: ratelimit.ReasonAllowed,
	}, nil
}

// Release relinquishes a lease after verifying its ownership fields.
func (store *Store) Release(ctx context.Context, lease ratelimit.Lease) error {
	if store.closed.Load() {
		return ratelimit.ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	key := lease.PolicyID + "\x00" + lease.Key.String()
	target := store.shardFor(key)
	target.mu.Lock()
	defer target.mu.Unlock()
	current, ok := target.states[key]
	if !ok || current.leases == nil {
		return ratelimit.ErrLeaseNotFound
	}
	existing, ok := current.leases[lease.ID]
	if !ok {
		return ratelimit.ErrLeaseNotFound
	}
	if existing.cost != lease.Cost || !existing.expiresAt.Equal(lease.ExpiresAt) {
		return ratelimit.ErrLeaseNotOwned
	}
	delete(current.leases, lease.ID)
	return nil
}

// Len returns the current number of keys across all shards.
func (store *Store) Len() int {
	total := 0
	for index := range store.shards {
		store.shards[index].mu.Lock()
		total += len(store.shards[index].states)
		store.shards[index].mu.Unlock()
	}
	return total
}

// Sweep removes idle keys and expired leases, returning the keys removed.
func (store *Store) Sweep(now time.Time, idleFor time.Duration) (int, error) {
	if store.closed.Load() {
		return 0, ratelimit.ErrUnavailable
	}
	if now.IsZero() || idleFor <= 0 {
		return 0, ratelimit.ErrInvalidRequest
	}
	removed := 0
	for index := range store.shards {
		target := &store.shards[index]
		target.mu.Lock()
		for key, current := range target.states {
			activeLease := false
			for id, lease := range current.leases {
				if lease.expiresAt.After(now) {
					activeLease = true
				} else {
					delete(current.leases, id)
				}
			}
			if !activeLease && !current.lastSeen.Add(idleFor).After(now) {
				delete(target.states, key)
				removed++
			}
		}
		target.mu.Unlock()
	}
	return removed, nil
}

// Close makes future operations return ratelimit.ErrUnavailable.
func (store *Store) Close() error {
	store.closed.Store(true)
	return nil
}

func (store *Store) shardFor(key string) *shard {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key))
	return &store.shards[hash.Sum64()%uint64(len(store.shards))]
}

func (target *shard) getOrCreate(key string, request ratelimit.Request) (*state, error) {
	if current, ok := target.states[key]; ok {
		if current.revision != request.Policy.Revision() {
			carryRevision(current, request)
		}
		return current, nil
	}
	if len(target.states) == target.maxKeys {
		if !target.evictOldest(request.Now) {
			return nil, ratelimit.ErrUnavailable
		}
	}
	current := &state{
		revision: request.Policy.Revision(), algorithm: request.Policy.Algorithm(),
		lastSeen: request.Now, lastRefill: request.Now,
		tokens: request.Policy.Limit(),
	}
	target.states[key] = current
	return current, nil
}

func (target *shard) evictOldest(now time.Time) bool {
	keys := make([]string, 0, len(target.states))
	for key, current := range target.states {
		activeLease := false
		for _, lease := range current.leases {
			if lease.expiresAt.After(now) {
				activeLease = true
				break
			}
		}
		if activeLease {
			continue
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return false
	}
	sort.Slice(keys, func(left, right int) bool {
		leftState, rightState := target.states[keys[left]], target.states[keys[right]]
		if leftState.lastSeen.Equal(rightState.lastSeen) {
			return keys[left] < keys[right]
		}
		return leftState.lastSeen.Before(rightState.lastSeen)
	})
	delete(target.states, keys[0])
	return true
}

func carryRevision(current *state, request ratelimit.Request) {
	switch current.algorithm {
	case ratelimit.TokenBucket:
		current.tokens = min(current.tokens, request.Policy.Limit())
	case ratelimit.FixedWindow, ratelimit.SlidingWindow:
		// Consumption is retained, so a rollout cannot refill the limit.
	case ratelimit.Concurrency:
		// Active leases are retained until release or expiry.
	}
	current.revision = request.Policy.Revision()
	if request.Now.After(current.lastRefill) {
		current.lastRefill = request.Now
	}
	current.remainder = 0
}

func admitToken(current *state, request ratelimit.Request) (ratelimit.Decision, error) {
	refill(current, request)
	limit := request.Policy.Limit()
	if current.tokens < request.Cost {
		retry := refillDuration(request.Cost-current.tokens, current.remainder, request.Policy)
		reset := refillDuration(limit-current.tokens, current.remainder, request.Policy)
		return ratelimit.Decision{
			Allowed: false, Limit: limit, Remaining: current.tokens,
			Reset: request.Now.Add(reset), RetryAfter: retry,
			Reason: ratelimit.ReasonLimited,
		}, ratelimit.ErrRejected
	}
	current.tokens -= request.Cost
	reset := refillDuration(limit-current.tokens, current.remainder, request.Policy)
	return ratelimit.Decision{
		Allowed: true, Limit: limit, Remaining: current.tokens,
		Reset: request.Now.Add(reset), Reason: ratelimit.ReasonAllowed,
	}, nil
}

func refill(current *state, request ratelimit.Request) {
	if !request.Now.After(current.lastRefill) || current.tokens == request.Policy.Limit() {
		if request.Now.After(current.lastRefill) {
			current.lastRefill = request.Now
			current.remainder = 0
		}
		return
	}
	elapsed := uint64(request.Now.Sub(current.lastRefill).Microseconds())
	high, low := bits.Mul64(elapsed, request.Policy.Capacity())
	low, carry := bits.Add64(low, current.remainder, 0)
	high += carry
	period := uint64(request.Policy.Period().Microseconds())
	if high >= period {
		current.tokens = request.Policy.Limit()
		current.remainder = 0
		current.lastRefill = request.Now
		return
	}
	added, remainder := bits.Div64(high, low, period)
	if added >= request.Policy.Limit()-current.tokens {
		current.tokens = request.Policy.Limit()
		current.remainder = 0
	} else {
		current.tokens += added
		current.remainder = remainder
	}
	current.lastRefill = request.Now
}

func refillDuration(tokens, remainder uint64, policy ratelimit.Policy) time.Duration {
	if tokens == 0 {
		return 0
	}
	high, low := bits.Mul64(tokens, uint64(policy.Period().Microseconds()))
	low, borrow := bits.Sub64(low, remainder, 0)
	high, _ = bits.Sub64(high, 0, borrow)
	capacity := policy.Capacity()
	if high >= capacity {
		return time.Duration(math.MaxInt64)
	}
	quotient, rest := bits.Div64(high, low, capacity)
	if rest != 0 {
		quotient++
	}
	if quotient > uint64(math.MaxInt64/int64(time.Microsecond)) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(quotient) * time.Microsecond
}

func admitFixed(current *state, request ratelimit.Request) (ratelimit.Decision, error) {
	period := int64(request.Policy.Period())
	start := floorBoundary(request.Now.UnixNano(), period)
	if current.windowStart != start {
		current.windowStart = start
		current.used = 0
	}
	reset := time.Unix(0, start+period)
	return consumeWindow(current, request, reset)
}

func admitSliding(current *state, request ratelimit.Request) (ratelimit.Decision, error) {
	segmentSize := (int64(request.Policy.Period()) + slidingSegments - 1) / slidingSegments
	index := floorBoundary(request.Now.UnixNano(), segmentSize) / segmentSize
	oldest := floorBoundary(request.Now.Add(-request.Policy.Period()).UnixNano(), segmentSize) / segmentSize
	var used uint64
	var earliest int64 = math.MaxInt64
	for slot := range current.segments {
		if current.segments[slot].index <= oldest {
			current.segments[slot] = segment{}
			continue
		}
		used += current.segments[slot].used
		if current.segments[slot].used > 0 && current.segments[slot].index < earliest {
			earliest = current.segments[slot].index
		}
	}
	current.used = used
	slot := positiveMod(index, slidingSegments)
	if current.segments[slot].index != index {
		current.segments[slot] = segment{index: index}
	}
	reset := request.Now.Add(request.Policy.Period())
	if earliest != math.MaxInt64 {
		reset = time.Unix(0, (earliest+1)*segmentSize).Add(request.Policy.Period())
	}
	decision, err := consumeWindow(current, request, reset)
	if err == nil {
		current.segments[slot].used += request.Cost
	}
	return decision, err
}

func consumeWindow(current *state, request ratelimit.Request, reset time.Time) (ratelimit.Decision, error) {
	limit := request.Policy.Limit()
	remaining := limit - min(current.used, limit)
	if request.Cost > remaining {
		return ratelimit.Decision{
			Allowed: false, Limit: limit, Remaining: remaining, Reset: reset,
			RetryAfter: nonnegative(reset.Sub(request.Now)), Reason: ratelimit.ReasonLimited,
		}, ratelimit.ErrRejected
	}
	current.used += request.Cost
	return ratelimit.Decision{
		Allowed: true, Limit: limit, Remaining: limit - current.used,
		Reset: reset, Reason: ratelimit.ReasonAllowed,
	}, nil
}

func stateKey(request ratelimit.Request) string {
	return request.Policy.ID() + "\x00" + request.Key.String()
}

func makeLease(request ratelimit.LeaseRequest, expiresAt time.Time) ratelimit.Lease {
	return ratelimit.Lease{
		ID: request.LeaseID, Key: request.Request.Key,
		PolicyID:       request.Request.Policy.ID(),
		PolicyRevision: request.Request.Policy.Revision(),
		Cost:           request.Request.Cost, ExpiresAt: expiresAt, Backend: "memory",
	}
}

func earliestLease(leases map[string]leaseState) time.Time {
	var earliest time.Time
	for _, lease := range leases {
		if earliest.IsZero() || lease.expiresAt.Before(earliest) {
			earliest = lease.expiresAt
		}
	}
	return earliest
}

func floorBoundary(value, size int64) int64 {
	quotient := value / size
	if value < 0 && value%size != 0 {
		quotient--
	}
	return quotient * size
}

func positiveMod(value int64, modulus int) int {
	result := value % int64(modulus)
	if result < 0 {
		result += int64(modulus)
	}
	return int(result)
}

func nonnegative(duration time.Duration) time.Duration {
	if duration < 0 {
		return 0
	}
	return duration
}

var _ ratelimit.Backend = (*Store)(nil)

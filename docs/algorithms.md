# Algorithms and precision

## Token bucket

Capacity tokens refill per Period with optional Burst. Weighted costs consume
whole tokens. Every backend uses integer arithmetic with a carried remainder.
Periods and leases must use microsecond precision, and request timestamps are
canonicalized to microseconds before state transitions. Valkey Lua integers
remain exactly representable: policy construction rejects limits above
2^53-1 or a non-concurrency Limit multiplied by Period microseconds above that
boundary, and requests reject timestamps or reset times outside the same exact
range. Sub-microsecond drift cannot make backend decisions diverge. Floating
point is never accepted outside the exact integer range in production
admission arithmetic.

Reset is the time until the bucket is full. RetryAfter is the time until the
requested cost is available. Clock rollback is clamped to the latest observed
time per key.

## Fixed window

Boundaries are deterministic Unix-time multiples of Period, including times
before the Unix epoch. A rollback cannot reopen an older window.

## Sliding window counter

The bounded sliding counter uses exactly 16 segments per Period. State cost is
constant per key regardless of capacity or request count. The approximation
may retain a segment until its segment boundary; Reset and RetryAfter expose
that boundary. It is not an unbounded request log.

## Concurrency

Concurrency is a separate leased policy. Capacity is weighted by Cost. Expired
leases are pruned atomically, repeated LeaseID acquisition is idempotent, and
release is ownership checked. Fail-open is invalid because an untracked lease
cannot guarantee cancellation or capacity.

Capacity plus Burst is limited to 1,024 for concurrency policies. Since Cost
is a positive integer, this is also the maximum number of live lease records
for one policy/key and bounds cleanup scans and persisted state size.

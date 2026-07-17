# Laravel RateLimiter migration

Map each Laravel limiter name to a stable Policy.ID and explicit Revision.
Map perMinute/perHour limits to FixedWindow when exact boundaries are desired,
or TokenBucket when smoothing and burst are desired. Laravel decay seconds
become Period; Laravel attempts become weighted Cost.

Replace RateLimiter::attempt and tooManyAttempts with Service.Admit. Handle
ErrRejected separately from backend failures and emit Decision headers through
ratelimithttp. Do not sleep inside admission.

Replace job throttling middleware with ratelimitqueue. On Deferred, return the
queue's native release/nack instruction without acknowledging the job. Durable
attempt counts, retry policy, and dead-letter behavior remain with go-queue.

Laravel cache-wide behavior usually requires Valkey. Memory is process-local
and changes semantics under multiple Go replicas. PostgreSQL is appropriate
only when the limit must coordinate with a transaction and benchmarks support
the workload.

Outbound vendor throttles must migrate to go-http-client, not this package.
Authorization remains in go-authorization and billable quotas require a
durable usage ledger.

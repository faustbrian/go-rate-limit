# Queue admission

ratelimitqueue runs admission before the worker handler. ByQueueAndTenant and
ByPrincipal provide bounded hashed subjects. Custom cost functions support
weighted jobs.

Rejected work returns Deferred immediately. The middleware does not sleep,
acknowledge, delete, reschedule, increment attempts, or change durable retry
semantics. The owning queue adapter should translate RetryAfter into its
native defer/nack operation and acknowledge only after its normal handler
contract permits.

Rate limiting is not job uniqueness, scheduler overlap, idempotency, or a
general lock. Use the owning packages for those semantics.

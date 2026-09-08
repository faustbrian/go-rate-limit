# Queue admission

New callers use `github.com/faustbrian/go-rate-limit/adapters/queue`. Construct
middleware with a `*ratelimit.StrictService`, then validate the downstream
handler with the checked `Wrap` method:

```go
middleware, err := ratelimitqueue.New(ratelimitqueue.Options{
	Service: strictService,
	Policy:  policy,
	Subject: ratelimitqueue.ByQueueAndTenant(),
})
if err != nil {
	return err
}
handler, err := middleware.Wrap(next)
```

`ByQueueAndTenant` and `ByPrincipal` provide bounded hashed subjects. Custom
cost functions support weighted jobs. `Wrap` rejects nil and typed-nil handlers
without invoking them.

For each message, reached callbacks run synchronously and at most once in the
order `Subject`, `Cost`, then `Now`; the downstream handler runs only after a
known allowed decision. Middleware retains configured callbacks and the
handler, so separate calls may invoke them concurrently, blocking blocks the
caller, re-entry is permitted, and callback panics propagate. It retains no
message, context, callback result, decision, or error after the call. See
[callback concurrency and ownership](api.md#callback-concurrency-and-ownership)
for the shared adapter contract.

Rejected work returns Deferred immediately. The middleware does not sleep,
acknowledge, delete, reschedule, increment attempts, or change durable retry
semantics. The owning queue adapter should translate RetryAfter into its
native defer/nack operation and acknowledge only after its normal handler
contract permits.

Rate limiting is not job uniqueness, scheduler overlap, idempotency, or a
general lock. Use the owning packages for those semantics.

The legacy `github.com/faustbrian/go-rate-limit/ratelimitqueue` package remains
supported through the compatibility interval. Its wrapper shape cannot report
a nil or typed-nil downstream handler during wrapping; use the successor for
new integrations.

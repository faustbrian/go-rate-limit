# Concepts and API

Policy is an immutable value created from PolicySpec. Identity consists of ID
and Revision. Algorithm, Capacity, Burst, Period, MaxCost, FailureMode,
Consistency, and Lease are explicit. Invalid or ambiguous policies are
rejected before backend access.

ID and Revision are limited to 64 bytes and the ASCII letters, digits, dash,
underscore, dot, and colon. They are deliberately safe for persistence,
decisions, logs, and controlled metric attributes. They must never contain a
credential, principal, tenant, IP address, or other sensitive value.

Request contains Policy, a validated Key, weighted Cost, and explicit Now.
Cost and time never receive implicit defaults in the core.

Decision reports Allowed, Remaining, Limit, Reset, RetryAfter, Reason,
Backend, and PolicyRevision. Rejection returns the decision together with
ErrRejected. Operational failures use ErrUnavailable, ErrDeadline,
ErrOverflow, or ErrCorrupt. Callers should use errors.Is.

Service applies fail-open or fail-closed policy behavior and emits Observation.
Fail-open returns ReasonFailOpen and is never permitted for concurrency
leases. It applies only to unavailable and deadline errors; corruption and
overflow always fail closed. At most 16 observers may be registered. Observer
panics are contained and cannot alter admission.

NewStrictService accepts only StrictBackend implementations. Its methods
validate complete results, preserve only direct stable backend categories, and
fail closed with ErrOutcomeUnknown when dispatch may have occurred or a backend
returns an unsafe or contradictory result. ErrCanceled identifies cancellation
known to precede dispatch; ErrDeadline preserves deadline compatibility;
ErrOutcomeUnknown is never evidence that retry is safe.

Batch accepts at most 256 requests. AtomicityPerItem validates every item
before execution, then reports each committed decision. All-or-nothing is
explicitly unsupported at the backend-neutral service layer.

StrictService.Batch additionally returns attempted input indexes and a
StrictBatchError containing ordered BatchItemError values, so partial dispatch
can be reconciled without parsing error strings.

## Callback concurrency and ownership

`StrictService` retains its backend and observers. Successor HTTP, queue, slog,
and OpenTelemetry objects retain the callbacks, sinks, or instruments they use
after construction. The OpenTelemetry constructor invokes the supplied meter
provider only to create its instruments; it retains the returned instruments,
not the provider. Calls are synchronous: blocking blocks the caller, separate
calls may invoke retained collaborators concurrently, no package lock is held
while invoking them, and re-entry is supported when the collaborator itself is
safe. Constructors do not invoke operational callbacks; `NewStrictService`
calls backend `Name` once and retains that immutable value.

HTTP derivation runs `Key`, `Cost`, then `Now`; queue derivation runs `Subject`,
`Cost`, then `Now`. Each reached callback runs at most once. Downstream HTTP
and queue handlers run once only after known admission. Callback inputs and
outputs are borrowed for the call and are not retained. The package retains no
request, message, context, observation, callback result, backend error,
cancellation cause, or downstream result after return.

Callback panics propagate unchanged except that `StrictService` isolates each
observer panic and continues later observers. Direct slog and OpenTelemetry
observer calls propagate sink panics; when registered with `StrictService`,
the service boundary isolates them. Callers own collaborator lifetime and
concurrency safety.

LeaseRequest, LeaseBackend, Service.Acquire, and Service.Release are reserved
for concurrency policies. Lease IDs are bounded and acquisition is
idempotent. Release verifies ID, cost, expiry, policy, key, and backend.

Key is namespaced, versioned, typed, and length bounded. Hash=true persists an
irreversible SHA-256 derivation instead of the subject. Raw credentials and
tenant-sensitive values should always be hashed.

The authoritative exported declarations are available through:

    go doc -all github.com/faustbrian/go-rate-limit

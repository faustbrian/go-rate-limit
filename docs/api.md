# Concepts and API

Policy is an immutable value created from PolicySpec. Identity consists of ID
and Revision. Algorithm, Capacity, Burst, Period, MaxCost, FailureMode,
Consistency, and Lease are explicit. Invalid or ambiguous policies are
rejected before backend access.

Request contains Policy, a validated Key, weighted Cost, and explicit Now.
Cost and time never receive implicit defaults in the core.

Decision reports Allowed, Remaining, Limit, Reset, RetryAfter, Reason,
Backend, and PolicyRevision. Rejection returns the decision together with
ErrRejected. Operational failures use ErrUnavailable, ErrDeadline,
ErrOverflow, or ErrCorrupt. Callers should use errors.Is.

Service applies fail-open or fail-closed policy behavior and emits Observation.
Fail-open returns ReasonFailOpen and is never permitted for concurrency
leases. Observer panics are contained and cannot alter admission.

Batch accepts at most 256 requests. AtomicityPerItem validates every item
before execution, then reports each committed decision. All-or-nothing is
explicitly unsupported at the backend-neutral service layer.

LeaseRequest, LeaseBackend, Service.Acquire, and Service.Release are reserved
for concurrency policies. Lease IDs are bounded and acquisition is
idempotent. Release verifies ID, cost, expiry, policy, key, and backend.

Key is namespaced, versioned, typed, and length bounded. Hash=true persists an
irreversible SHA-256 derivation instead of the subject. Raw credentials and
tenant-sensitive values should always be hashed.

The authoritative exported declarations are available through:

    go doc -all github.com/faustbrian/go-rate-limit

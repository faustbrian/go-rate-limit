# Failure and outage behavior

FailClosed is the default and returns an unavailable decision when the backend
cannot decide. Use it for login, abuse prevention, scarce resources, and every
concurrency lease.

FailOpen returns an allowed decision with ReasonFailOpen. Use it only when
availability is more important than duplicate admission and downstream work
can tolerate overload. It applies only to ErrUnavailable and ErrDeadline. State
corruption and arithmetic overflow always fail closed because admitting on an
integrity failure would create an attacker-controlled bypass. Fail-open does
not create backend state and must be observable.

Timeouts include context cancellation and backend deadline expiry. The core
does not retry or sleep because retrying an unknown distributed result can
double-consume capacity. Transport owners decide whether an operation is safe
to repeat.

Concurrency acquisition is the exception only for the same LeaseID, key,
policy ID, and Cost: that exact retry is idempotent. Reusing the ID with a
different Cost is rejected as non-owned rather than consuming or rewriting
capacity.

Public errors preserve only stable classifications through errors.Is. Raw
database, Valkey, network, command, and persisted-state text is discarded at
the backend boundary because it may contain credentials or sensitive keys.

HTTP maps rejection to 429 and backend failure to 503. JSON-RPC uses -32029
for rejection and -32030 for unavailable/invalid admission inputs. Queue
middleware returns a typed Deferred error; the queue adapter retains retry and
acknowledgement ownership.

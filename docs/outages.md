# Failure and outage behavior

FailClosed is the default and returns an unavailable decision when the backend
cannot decide. Use it for login, abuse prevention, scarce resources, and every
concurrency lease.

FailOpen returns an allowed decision with ReasonFailOpen. Use it only when
availability is more important than duplicate admission and downstream work
can tolerate overload. It does not create backend state and must be observable.

Timeouts include context cancellation and backend deadline expiry. The core
does not retry or sleep because retrying an unknown distributed result can
double-consume capacity. Transport owners decide whether an operation is safe
to repeat.

HTTP maps rejection to 429 and backend failure to 503. JSON-RPC uses -32029
for rejection and -32030 for unavailable/invalid admission inputs. Queue
middleware returns a typed Deferred error; the queue adapter retains retry and
acknowledgement ownership.

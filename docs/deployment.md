# Deployment and policy revisions

State identity is Policy.ID plus the derived Key. Revision is stored inside
that state, not appended to the storage key. A rolling deployment therefore
does not create a fresh bucket and silently double capacity.

On revision change, existing consumption, token balance, window segments, and
active leases are carried forward conservatively. Token balance is capped by
the new limit and an increase does not mint replacement tokens. Window
consumption is retained, so only capacity above prior use becomes available.
Decreasing concurrency capacity below active leased cost reports zero
remaining and rejects new leases until enough ownership expires or is
released; unsigned underflow cannot reopen capacity.

Revision values are opaque identifiers, not an ordered generation. Drain old
policy revisions before activating a same-ID revision; mixed old/new writers
cannot determine which opaque revision is authoritative. Period or window-shape
changes require a new Policy.ID (or a complete drain until prior state expires)
because stored window indexes cannot be reinterpreted safely. Strict backends
record the established fixed/sliding period and return ErrCorrupt on a same-ID
period mismatch without mutating state. Reusing a LeaseID
with a different Cost returns ErrLeaseNotOwned and never rewrites the stored
lease proof.

Deploy algorithm changes under a new Policy.ID. Reusing an ID with a different
algorithm is corruption and fails closed. Deploy key-derivation changes under
a new key Version and plan the capacity transition explicitly because old and
new keys are independent.

For multi-region deployments, document the authority topology before rollout.
Do not direct decisions to asynchronous read replicas.

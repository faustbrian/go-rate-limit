# Deployment and policy revisions

State identity is Policy.ID plus the derived Key. Revision is stored inside
that state, not appended to the storage key. A rolling deployment therefore
does not create a fresh bucket and silently double capacity.

On revision change, existing consumption, token balance, window segments, and
active leases are carried forward conservatively. Token balance is capped by
the new limit. Increasing capacity makes only the genuine difference
available; decreasing capacity retains over-consumption until it expires or
refills.

Deploy algorithm changes under a new Policy.ID. Reusing an ID with a different
algorithm is corruption and fails closed. Deploy key-derivation changes under
a new key Version and plan the capacity transition explicitly because old and
new keys are independent.

For multi-region deployments, document the authority topology before rollout.
Do not direct decisions to asynchronous read replicas.

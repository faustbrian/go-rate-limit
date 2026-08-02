# Goal: Harden Rate Limit Resilience Boundary

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and
"OPTIONAL" in this document are to be interpreted as described in BCP 14
[RFC2119] [RFC8174] when, and only when, they appear in all capitals, as
shown here.

## Required Proof

- Prove deprecated concurrency migration preserves documented behavior without
  retaining duplicate implementations.
- Simulate replica count and mixed policy revisions against memory, Valkey, and
  PostgreSQL backends.
- Fault backend timeout, failover, partition, ambiguous response, script reload,
  transaction conflict, and clock disagreement under fail-open/fail-closed
  policies.
- Test retry/hedge composition, weighted costs, hot keys, hostile cardinality,
  cleanup, and HPA feedback scenarios.
- Prove every decision and fallback reports its actual local or distributed
  guarantee.

Meaningful exact 100% statement coverage, exactly 100% viable mutation kills,
race, fuzz, stress, leak, backend fault, benchmark, API compatibility, docs,
security, and supply-chain gates MUST pass. Final review MUST find no duplicate
concurrency owner, capacity inflation, fail-mode ambiguity, retry bypass, or
false cluster-wide claim.

[RFC2119]: https://www.rfc-editor.org/rfc/rfc2119
[RFC8174]: https://www.rfc-editor.org/rfc/rfc8174

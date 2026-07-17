# Security model

Threats include spoofed proxy headers, alternate-credential bypass, adversarial
key cardinality, hot keys, integer overflow, clock rollback, revision-based
capacity doubling, hash collisions, script injection, backend response
corruption, partitions, and identity disclosure.

Controls:

- bounded, typed, namespaced, versioned keys with optional SHA-256 hashing;
- strict trusted-proxy parsing with bounded hops and bytes;
- integer-only arithmetic, carried remainders, and overflow clamping;
- per-key clock rollback clamping;
- opaque Valkey hash tags and PostgreSQL bytea digests;
- arguments passed separately from Lua source, preventing script injection;
- schema and algorithm validation before persisted state is trusted;
- bounded cardinality, TTLs, 16 sliding segments, cleanup batches, and leases;
- controlled telemetry labels and classified logging without raw backend text;
- generic transport rejection responses.

SHA-256 hashing is irreversible key minimization, not encryption. Low-entropy
subjects may still be guessed; use a deployment-specific HMAC in a custom key
function when offline enumeration is a concern.

Report vulnerabilities according to [../SECURITY.md](../SECURITY.md).

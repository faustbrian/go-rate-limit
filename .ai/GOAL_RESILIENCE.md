# Goal: Rate Limit Resilience Boundary

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and
"OPTIONAL" in this document are to be interpreted as described in BCP 14
[RFC2119] [RFC8174] when, and only when, they appear in all capitals, as
shown here.

## Objective

Correct and harden the implemented `rate-limit` boundary now that dedicated
semaphore, bulkhead, concurrency-limit, and adaptive-throttle modules are
planned. This supplements the executed base goals; historical goal text remains
evidence and MUST NOT be rewritten.

## Boundary Correction

`rate-limit` MUST own admission over time, burst, weighted cost, and quota
windows. Generic concurrency/semaphore admission MUST move to `semaphore` or
`bulkhead`. Adaptive in-flight capacity belongs to `concurrency-limit` and
probabilistic overload shedding belongs to `adaptive-throttle`.

Provide a compatibility and migration plan for any implemented concurrency
policy. Do not delete public behavior without SemVer treatment. An adapter MAY
bridge old callers to the new owner during a documented deprecation period.

## Distributed And Kubernetes Requirements

- Memory limits are per process/pod and aggregate capacity changes with
  replicas.
- Cluster-wide limits require an atomic Valkey or PostgreSQL backend with
  explicit consistency and outage semantics.
- Policy revision/keying MUST prevent rolling deployments from silently
  doubling or fragmenting capacity unless documented.
- Scale-up, scale-down, stale pods, failover, clock policy, hot keys, and
  backend latency MUST be covered.
- Fail-open, fail-closed, and optional bounded local emergency fallback MUST be
  explicit per operation; fallback cannot claim the same global guarantee.
- HPA guidance MUST account for rejection reducing CPU and for distributed
  backend saturation.

## Composition

Document rate permit acquisition once per logical operation versus per physical
attempt. Retry and hedge MUST not bypass quotas or immediately retry a quota
rejection by default. Local concurrency and adaptive rejections MUST remain
distinct from rate rejection in errors, metrics, and outer-policy classifiers.

## Deliverables And Acceptance

- Publish the corrected ownership matrix, migration guide, Kubernetes sizing,
  backend outage matrix, rollout guidance, and composition examples.
- Add conformance tests proving equivalent rate semantics across memory,
  Valkey, and PostgreSQL within each backend's documented consistency model.
- Preserve meaningful exact 100% statement coverage, exactly 100% viable
  mutation kills, and all repository gates.
- No concurrency primitive remains mislabeled as a rate algorithm in the final
  public API without an explicit deprecated compatibility surface.

[RFC2119]: https://www.rfc-editor.org/rfc/rfc2119
[RFC8174]: https://www.rfc-editor.org/rfc/rfc8174

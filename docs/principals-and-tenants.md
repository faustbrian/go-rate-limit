# Principals and tenants

ratelimitprincipal depends only on:

    interface { Subject() string }

authentication Principal satisfies that contract, so authentication does
not import this package and no reverse dependency is introduced. Anonymous
principals are rejected by this adapter; choose an explicit IP or device
subject if anonymous traffic needs limiting.

Tenant identifiers are admission partition keys, not authorization decisions.
authorization remains responsible for permission checks. Never infer tenant
access from an allowed rate-limit decision.

Hash principal and tenant subjects before persistence or telemetry. Issuer or
credential source may be included in a custom, length-prefixed derivation when
the same subject string is not globally unique.

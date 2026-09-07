# Principals and tenants

New callers use
`github.com/faustbrian/go-rate-limit/adapters/authentication`. Its `Key`
helper depends only on:

    interface { Subject() string }

An authentication `Principal` satisfies that contract, so authentication does
not import this package and no reverse dependency is introduced. The helper
rejects nil and typed-nil principals, snapshots `Subject` exactly once, and
returns a bounded hashed rate-limit key. Anonymous principals are rejected;
choose an explicit IP or device subject if anonymous traffic needs limiting.

Tenant identifiers are admission partition keys, not authorization decisions.
authorization remains responsible for permission checks. Never infer tenant
access from an allowed rate-limit decision.

Hash principal and tenant subjects before persistence or telemetry. Issuer or
credential source may be included in a custom, length-prefixed derivation when
the same subject string is not globally unique.

The legacy `github.com/faustbrian/go-rate-limit/ratelimitprincipal` package
remains supported through the compatibility interval. Its `Key` helper retains
legacy typed-nil and repeated-successful-`Subject` behavior; `KeyStrict` is the
checked compatibility-path alternative. New integrations should use the
successor package.

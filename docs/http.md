# HTTP middleware

ratelimithttp.New wraps an http.Handler. Supply Service and Policy. Optional
Now, Cost, and Key functions support deterministic clocks, weighted routes,
principals, tenants, or application operations.

Allowed responses receive RateLimit-Limit, RateLimit-Remaining, and
RateLimit-Reset. Rejections also receive Retry-After rounded up to whole
seconds. Bodies are generic and do not disclose identity, key, policy ID, or
backend internals.

The default key hashes the strict client IP derived by ClientIPExtractor.
Configure TrustedProxies explicitly; see trusted-proxies.md. Authentication
middleware may instead supply ratelimitprincipal.Key through Options.Key.
Configuration accepts at most 64 trusted prefixes. Forwarded chains are
limited to 4,096 bytes and 32 hops.

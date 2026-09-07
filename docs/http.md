# HTTP middleware

New callers use `github.com/faustbrian/go-rate-limit/adapters/http`. Construct
the middleware with a `*ratelimit.StrictService`, then use its checked `Wrap`
method:

```go
middleware, err := ratelimithttp.New(ratelimithttp.Options{
	Service: strictService,
	Policy:  policy,
})
if err != nil {
	return err
}
handler, err := middleware.Wrap(next)
```

`Wrap` rejects nil and typed-nil handlers without invoking them. Optional
`Now`, `Cost`, and `Key` functions support deterministic clocks, weighted
routes, principals, tenants, or application operations.

Allowed responses receive RateLimit-Limit, RateLimit-Remaining, and
RateLimit-Reset. Rejections also receive Retry-After rounded up to whole
seconds. Bodies are generic and do not disclose identity, key, policy ID, or
backend internals.

The default key hashes the strict client IP derived by `ClientIPExtractor`.
Configure TrustedProxies explicitly; see trusted-proxies.md. To derive a key
from authentication state, adapt the request to the narrower principal API:

```go
Key: func(request *http.Request) (ratelimit.Key, error) {
	principal, err := applicationPrincipal(request.Context())
	if err != nil {
		return ratelimit.Key{}, err
	}
	return ratelimitauthentication.Key(principal)
},
```

`applicationPrincipal` is application-owned; `adapters/authentication.Key`
accepts a `Principal`, not an `*http.Request`.
Configuration accepts at most 64 trusted prefixes. The forwarded chain selected
by `http.Header.Get` is limited to 4,096 bytes and 32 hops; repeated field lines
retain the released first-value behavior.

The legacy `github.com/faustbrian/go-rate-limit/ratelimithttp` package remains
supported through the compatibility interval. Its constructor directly returns
an `http.Handler` wrapper and cannot report a nil or typed-nil handler during
wrapping; do not use that shape for new integrations.

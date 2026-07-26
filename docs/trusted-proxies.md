# Trusted proxies

The HTTP extractor trusts X-Forwarded-For only when RemoteAddr belongs to an
explicit TrustedProxies prefix. It validates every hop, allows at most 32 hops
and 4096 header bytes, walks from the trusted edge toward the client, and
selects the first untrusted address. Malformed chains fail closed with a bad
request.

When the peer is not trusted, forwarding headers are ignored. Do not configure
0.0.0.0/0 or ::/0. Configure only load balancer and ingress networks controlled
by the deployment. Normalize IPv4-mapped IPv6 addresses before prefix checks;
the built-in extractor does this automatically.

Alternate credential sources must converge on one key function. Otherwise a
caller could bypass a principal limit by switching between IP, API key, cookie,
or bearer extraction paths.

# JSON-RPC middleware

ratelimitrpc composes at most 16 ordered rules. Built-in subject functions
cover global service, authenticated principal, method, and tenant limits.
Custom SubjectFunc and Cost callbacks support operation-specific policy.

Every rule is admitted before the handler executes. Earlier per-rule decisions
are not rolled back if a later rule rejects. A rejection returns code -32029,
message "rate limit exceeded", and bounded RetryAfter. No protected subject or
policy internals appear in the error.

The package defines a narrow Handler interface so a jsonrpc adapter can map
its request type without creating a reverse dependency.

# FAQ

## Is an allowed decision authorization?

No. go-authorization owns permission decisions.

## Is Remaining a billable quota?

No. It is ephemeral admission state, not a durable usage ledger.

## Can core admission wait for capacity?

No. Core calls never sleep or retry. Callers decide when to retry.

## Is Redis supported through the Valkey adapter?

No. A native Redis adapter requires independent atomic and conformance proof.

## Can I cancel a token reservation?

No reservation is exposed for token/window algorithms. Only concurrency
policies provide guaranteed lease acquisition and release.

## Why does sliding reset align to segments?

The algorithm uses 16 bounded segments, not an unbounded log. Segment
approximation is the storage and cost bound.

## Does fail-open work for concurrency?

No. NewPolicy rejects it because an untracked lease cannot be released safely.

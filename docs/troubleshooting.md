# Troubleshooting

## Every request is rejected

Check Cost, MaxCost, Capacity+Burst, explicit Now, key derivation, clock mode,
and whether a previous policy revision retained consumption. Inspect the typed
error before mapping it to a transport status.

Public errors deliberately omit driver text. Use secured backend-native
diagnostics to investigate connectivity without copying connection strings,
commands, keys, or stored state into application logs.

## Limits appear multiplied

Memory is probably running in multiple replicas, regions may have independent
authorities, or key Version changed. Use Valkey/PostgreSQL for one authority
and keep Revision out of the storage key.

## Client IP is the load balancer

Add only the actual proxy network to TrustedProxies. A missing trusted prefix
correctly causes forwarding headers to be ignored.

## Valkey Open fails

Verify Valkey 9 or newer and maxmemory-policy=noeviction. Redis is intentionally
not accepted as native Valkey compatibility.

## PostgreSQL latency spikes

Inspect hot-key lock contention, pool wait time, lock_timeout, cleanup backlog,
and the expires_at index. PostgreSQL may be the wrong authority for this
throughput.

## Queue jobs disappear

The rate-limit middleware never acknowledges work. Verify the queue adapter
maps Deferred to release/nack and does not treat it as success.

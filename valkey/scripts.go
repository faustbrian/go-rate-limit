package valkey

const admitScript = `
local schema = ARGV[1]
local algorithm = ARGV[2]
local policy_id = ARGV[3]
local revision = ARGV[4]
local capacity = tonumber(ARGV[5])
local burst = tonumber(ARGV[6])
local period = tonumber(ARGV[7])
local cost = tonumber(ARGV[8])
local now = tonumber(ARGV[9])
local server_clock = ARGV[10] == '1'
local ttl = tonumber(ARGV[11])
local limit = capacity + burst
local max_exact = 9007199254740991
local function integer(value) return string.format('%.0f', value) end

if server_clock then
    local server = redis.call('TIME')
    now = server[1] * 1000000 + server[2]
end
if period <= 0 or capacity <= 0 or cost <= 0 or limit > max_exact then
    return {'-1', '0', '0', '0', '0', 'overflow'}
end
if redis.call('EXISTS', KEYS[1]) == 1 then
    if redis.call('HGET', KEYS[1], 'schema') ~= schema or
       redis.call('HGET', KEYS[1], 'policy_id') ~= policy_id or
       redis.call('HGET', KEYS[1], 'algorithm') ~= algorithm then
        return {'-1', '0', '0', '0', '0', 'corrupt'}
    end
else
    redis.call('HSET', KEYS[1],
        'schema', schema, 'policy_id', policy_id, 'algorithm', algorithm,
        'revision', revision, 'tokens', integer(limit), 'remainder', '0',
        'last', integer(now), 'window', '0', 'used', '0')
end

local observed = tonumber(redis.call('HGET', KEYS[1], 'last'))
if now < observed then now = observed end
redis.call('HSET', KEYS[1], 'revision', revision)
local allowed = 0
local remaining = 0
local reset = now + period
local retry = 0

if algorithm == 'token_bucket' then
    local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens'))
    local remainder = tonumber(redis.call('HGET', KEYS[1], 'remainder'))
    local last = tonumber(redis.call('HGET', KEYS[1], 'last'))
    local elapsed = math.max(0, now - last)
    if elapsed > 0 and tokens < limit then
        if elapsed > math.floor((max_exact - remainder) / capacity) then
            tokens = limit
            remainder = 0
        else
            local numerator = elapsed * capacity + remainder
            local added = math.floor(numerator / period)
            remainder = numerator % period
            tokens = math.min(limit, tokens + added)
            if tokens == limit then remainder = 0 end
        end
    end
    if tokens >= cost then
        tokens = tokens - cost
        allowed = 1
    else
        local need = (cost - tokens) * period - remainder
        retry = math.floor((need + capacity - 1) / capacity)
    end
    local full = (limit - tokens) * period - remainder
    reset = now + math.max(0, math.floor((full + capacity - 1) / capacity))
    remaining = tokens
    redis.call('HSET', KEYS[1], 'tokens', integer(tokens),
        'remainder', integer(remainder), 'last', integer(now))
elseif algorithm == 'fixed_window' then
    local window = math.floor(now / period) * period
    local stored = tonumber(redis.call('HGET', KEYS[1], 'window'))
    local used = tonumber(redis.call('HGET', KEYS[1], 'used'))
    if stored ~= window then used = 0 end
    remaining = math.max(0, limit - used)
    reset = window + period
    if cost <= remaining then
        used = used + cost
        remaining = limit - used
        allowed = 1
    else
        retry = math.max(0, reset - now)
    end
    redis.call('HSET', KEYS[1], 'window', integer(window), 'used', integer(used))
elseif algorithm == 'sliding_window' then
    local width = math.floor((period + 15) / 16)
    local current = math.floor(now / width)
    local oldest = math.floor((now - period) / width)
    local used = 0
    local earliest = nil
    for slot = 0, 15 do
        local field = 'b' .. tostring(slot)
        local encoded = redis.call('HGET', KEYS[1], field)
        if encoded then
            local separator = string.find(encoded, ':')
            local idx = tonumber(string.sub(encoded, 1, separator - 1))
            local value = tonumber(string.sub(encoded, separator + 1))
            if idx <= oldest then
                redis.call('HDEL', KEYS[1], field)
            else
                used = used + value
                if earliest == nil or idx < earliest then earliest = idx end
            end
        end
    end
    remaining = math.max(0, limit - used)
    if earliest then reset = (earliest + 1) * width + period end
    if cost <= remaining then
        local slot = current % 16
        local field = 'b' .. tostring(slot)
        local encoded = redis.call('HGET', KEYS[1], field)
        local value = 0
        if encoded and tonumber(string.match(encoded, '^[^:]+')) == current then
            value = tonumber(string.match(encoded, '[^:]+$'))
        end
        redis.call('HSET', KEYS[1], field, integer(current) .. ':' .. integer(value + cost))
        remaining = remaining - cost
        allowed = 1
    else
        retry = math.max(0, reset - now)
    end
else
    return {'-1', '0', '0', '0', '0', 'corrupt'}
end

redis.call('HSET', KEYS[1], 'last', integer(now))
redis.call('PEXPIRE', KEYS[1], ttl)
if allowed == 1 then
    return {'1', integer(remaining), integer(limit), integer(reset), '0', 'allowed'}
end
return {'0', integer(remaining), integer(limit), integer(reset), integer(retry), 'limited'}
`

const acquireLeaseScript = `
local schema = ARGV[1]
local policy_id = ARGV[2]
local revision = ARGV[3]
local limit = tonumber(ARGV[4])
local cost = tonumber(ARGV[5])
local now = tonumber(ARGV[6])
local lease_us = tonumber(ARGV[7])
local ttl_ms = tonumber(ARGV[8])
local server_clock = ARGV[9] == '1'
local lease_field = 'l:' .. ARGV[10]
local function integer(value) return string.format('%.0f', value) end
if server_clock then
    local server = redis.call('TIME')
    now = server[1] * 1000000 + server[2]
end
if redis.call('EXISTS', KEYS[1]) == 1 then
    if redis.call('HGET', KEYS[1], 'schema') ~= schema or
       redis.call('HGET', KEYS[1], 'policy_id') ~= policy_id or
       redis.call('HGET', KEYS[1], 'algorithm') ~= 'concurrency' then
        return {'-1', '0', '0', '0', '0', 'corrupt', '0'}
    end
else
    redis.call('HSET', KEYS[1], 'schema', schema, 'policy_id', policy_id,
        'algorithm', 'concurrency', 'revision', revision, 'last', integer(now))
end
local observed = tonumber(redis.call('HGET', KEYS[1], 'last'))
if now < observed then now = observed end
redis.call('HSET', KEYS[1], 'last', integer(now))
redis.call('HSET', KEYS[1], 'revision', revision)
if redis.call('HLEN', KEYS[1]) > 1029 then
    return {'-1', '0', '0', '0', '0', 'corrupt', '0'}
end
local fields = redis.call('HGETALL', KEYS[1])
local used = 0
local earliest = 0
for index = 1, #fields, 2 do
    if string.sub(fields[index], 1, 2) == 'l:' then
        local encoded = fields[index + 1]
        local separator = string.find(encoded, ':')
        local lease_cost = tonumber(string.sub(encoded, 1, separator - 1))
        local expires = tonumber(string.sub(encoded, separator + 1))
        if expires <= now then
            redis.call('HDEL', KEYS[1], fields[index])
        else
            if lease_cost <= 0 or lease_cost > 1024 - used then
                return {'-1', '0', '0', '0', '0', 'corrupt', '0'}
            end
            used = used + lease_cost
            if earliest == 0 or expires < earliest then earliest = expires end
        end
    end
end
local existing = redis.call('HGET', KEYS[1], lease_field)
if existing then
    local separator = string.find(existing, ':')
    local existing_cost = tonumber(string.sub(existing, 1, separator - 1))
    local expires = tonumber(string.sub(existing, separator + 1))
    if existing_cost ~= cost then
        return {'-1', '0', '0', '0', '0', 'not_owned', '0'}
    end
    redis.call('PEXPIRE', KEYS[1], ttl_ms)
    return {'1', integer(limit - math.min(used, limit)), integer(limit), integer(expires),
        '0', 'allowed', integer(expires)}
end
local remaining = math.max(0, limit - used)
if cost > remaining then
    return {'0', integer(remaining), integer(limit), integer(earliest),
        integer(math.max(0, earliest - now)), 'limited', '0'}
end
local expires = now + lease_us
redis.call('HSET', KEYS[1], lease_field, integer(cost) .. ':' .. integer(expires))
redis.call('PEXPIRE', KEYS[1], ttl_ms)
return {'1', integer(remaining - cost), integer(limit), integer(expires),
    '0', 'allowed', integer(expires)}
`

const releaseLeaseScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then return {'not_found'} end
if redis.call('HGET', KEYS[1], 'schema') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'policy_id') ~= ARGV[2] or
   redis.call('HGET', KEYS[1], 'algorithm') ~= 'concurrency' then
    return {'not_found'}
end
local field = 'l:' .. ARGV[3]
local existing = redis.call('HGET', KEYS[1], field)
if not existing then return {'not_found'} end
if existing ~= ARGV[4] .. ':' .. ARGV[5] then return {'not_owned'} end
redis.call('HDEL', KEYS[1], field)
if redis.call('HLEN', KEYS[1]) == 5 then redis.call('DEL', KEYS[1]) end
return {'ok'}
`

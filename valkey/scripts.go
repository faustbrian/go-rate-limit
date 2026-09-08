package valkey

const admitScript = `local strict = false
` + admitScriptBody
const strictAdmitScript = `local strict = true
` + admitScriptBody

const admitScriptBody = `
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
local max_state_bytes = 131072
local max_allocator_bytes = 1048576
local function integer(value) return string.format('%.0f', value) end
local function canonical_unsigned(value)
    return type(value) == 'string' and string.len(value) <= 16 and
        (value == '0' or string.match(value, '^[1-9][0-9]*$') ~= nil)
end
local function canonical_signed(value)
    if canonical_unsigned(value) then return true end
    if type(value) ~= 'string' or string.len(value) > 17 or string.sub(value, 1, 1) ~= '-' then return false end
    return string.match(string.sub(value, 2), '^[1-9][0-9]*$') ~= nil
end
local function ceiling(value, divisor)
    if strict then return math.floor((value - 1) / divisor) + 1 end
    return math.floor((value + divisor - 1) / divisor)
end

if server_clock then
    local server = redis.call('TIME')
    now = server[1] * 1000000 + server[2]
end
if period <= 0 or capacity <= 0 or cost <= 0 or limit > max_exact then
    return {'-1', '0', '0', '0', '0', 'overflow'}
end
if strict and (now < -max_exact or now > max_exact or period > max_exact or now > max_exact - period) then
    return {'-1', '0', '0', '0', '0', 'overflow'}
end
local strict_fixed_window = nil
if strict and algorithm == 'fixed_window' then
    strict_fixed_window = math.floor(now / period) * period
    if strict_fixed_window < -max_exact or strict_fixed_window > max_exact then
        return {'-1', '0', '0', '0', '0', 'overflow'}
    end
end
if redis.call('EXISTS', KEYS[1]) == 1 then
    if strict then
        local state_bytes = redis.call('MEMORY', 'USAGE', KEYS[1])
        if state_bytes == false or state_bytes == nil or state_bytes > max_allocator_bytes then
            return {'-1', '0', '0', '0', '0', 'corrupt'}
        end
    end
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
    if strict then redis.call('HSET', KEYS[1], 'period', integer(period), 'carried', '0') end
end

if strict then
    local allowed_fields = {
        schema = true, policy_id = true, algorithm = true, revision = true,
        tokens = true, remainder = true, last = true, window = true, used = true,
        period = true, carried = true
    }
    local max_fields = 11
    if algorithm == 'sliding_window' then
        for slot = 0, 15 do allowed_fields['b' .. tostring(slot)] = true end
        max_fields = 27
    end
    if redis.call('HLEN', KEYS[1]) > max_fields then
        return {'-1', '0', '0', '0', '0', 'corrupt'}
    end
    local state_bytes = 0
    for _, field in ipairs(redis.call('HKEYS', KEYS[1])) do
        local value_bytes = redis.call('HSTRLEN', KEYS[1], field)
        state_bytes = state_bytes + string.len(field) + value_bytes
        if string.len(field) > 66 or value_bytes > 128 or state_bytes > max_state_bytes or not allowed_fields[field] then
            return {'-1', '0', '0', '0', '0', 'corrupt'}
        end
    end
end

local stored_revision = redis.call('HGET', KEYS[1], 'revision')
local stored_period_raw = redis.call('HGET', KEYS[1], 'period')
local stored_period = tonumber(stored_period_raw)
local carried_raw = redis.call('HGET', KEYS[1], 'carried')
local carried = carried_raw == '1'
local window_algorithm = algorithm == 'fixed_window' or algorithm == 'sliding_window'
if strict and ((window_algorithm and stored_period_raw ~= false and
   (not canonical_unsigned(stored_period_raw) or stored_period == nil or stored_period ~= math.floor(stored_period) or stored_period ~= period)) or
   (carried_raw ~= false and carried_raw ~= '0' and carried_raw ~= '1')) then
    return {'-1', '0', '0', '0', '0', 'corrupt'}
end
local observed_raw = redis.call('HGET', KEYS[1], 'last')
local observed = tonumber(observed_raw)
if strict and (not canonical_signed(observed_raw) or observed == nil or observed ~= math.floor(observed) or observed < -max_exact or observed > max_exact) then
    return {'-1', '0', '0', '0', '0', 'corrupt'}
end
if now < observed then now = observed end
if strict and now > max_exact - period then
    return {'-1', '0', '0', '0', '0', 'overflow'}
end
if strict and algorithm == 'fixed_window' then
    strict_fixed_window = math.floor(now / period) * period
    if strict_fixed_window < -max_exact or strict_fixed_window > max_exact then
        return {'-1', '0', '0', '0', '0', 'overflow'}
    end
end
if not strict then redis.call('HSET', KEYS[1], 'revision', revision) end
local allowed = 0
local remaining = 0
local reset = now + period
local retry = 0

if algorithm == 'token_bucket' then
    local tokens_raw = redis.call('HGET', KEYS[1], 'tokens')
    local remainder_raw = redis.call('HGET', KEYS[1], 'remainder')
    local last_raw = redis.call('HGET', KEYS[1], 'last')
    local tokens = tonumber(tokens_raw)
    local remainder = tonumber(remainder_raw)
    local last = tonumber(last_raw)
    if strict and (not canonical_unsigned(tokens_raw) or not canonical_unsigned(remainder_raw) or not canonical_signed(last_raw) or
       tokens == nil or remainder == nil or last == nil or
       tokens ~= math.floor(tokens) or remainder ~= math.floor(remainder) or last ~= math.floor(last) or
       tokens < 0 or tokens > max_exact or remainder < 0 or remainder > max_exact or
       (stored_revision == revision and remainder >= period) or
       last < -max_exact or last > max_exact or
       (stored_revision == revision and tokens > limit)) then
        return {'-1', '0', '0', '0', '0', 'corrupt'}
    end
    if strict and stored_revision ~= revision then
        tokens = math.min(tokens, limit)
        remainder = 0
        last = now
    end
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
        retry = ceiling(need, capacity)
    end
    local full = (limit - tokens) * period - remainder
    reset = now + math.max(0, ceiling(full, capacity))
    remaining = tokens
    redis.call('HSET', KEYS[1], 'tokens', integer(tokens),
        'remainder', integer(remainder), 'last', integer(now))
elseif algorithm == 'fixed_window' then
    local window = strict_fixed_window or math.floor(now / period) * period
    local stored_raw = redis.call('HGET', KEYS[1], 'window')
    local used_raw = redis.call('HGET', KEYS[1], 'used')
    local stored = tonumber(stored_raw)
    local used = tonumber(used_raw)
    if strict and (not canonical_signed(stored_raw) or not canonical_unsigned(used_raw) or
       stored == nil or used == nil or stored ~= math.floor(stored) or used ~= math.floor(used) or
       stored < -max_exact or stored > max_exact or used < 0 or used > max_exact or
       (stored_revision == revision and used > limit and not carried)) then
        return {'-1', '0', '0', '0', '0', 'corrupt'}
    end
    if strict and stored_revision ~= revision then carried = used > limit end
    if stored ~= window then used = 0; carried = false end
    remaining = math.max(0, limit - used)
    reset = window + period
    if cost <= remaining then
        used = used + cost
        remaining = limit - used
        allowed = 1
    else
        retry = math.max(0, reset - now)
    end
    if used <= limit then carried = false end
    redis.call('HSET', KEYS[1], 'window', integer(window), 'used', integer(used))
elseif algorithm == 'sliding_window' then
    local width = math.floor((period + 15) / 16)
    local current = math.floor(now / width)
    local oldest = math.floor((now - period) / width)
    local used = 0
    local earliest = nil
    local expired = {}
    local seen = {}
    for slot = 0, 15 do
        local field = 'b' .. tostring(slot)
        local encoded = redis.call('HGET', KEYS[1], field)
        if encoded then
            local separator = string.find(encoded, ':')
            if strict and separator == nil then return {'-1', '0', '0', '0', '0', 'corrupt'} end
            local idx_raw = string.sub(encoded, 1, separator - 1)
            local value_raw = string.sub(encoded, separator + 1)
            local idx = tonumber(idx_raw)
            local value = tonumber(value_raw)
            if strict and (not canonical_signed(idx_raw) or not canonical_unsigned(value_raw) or
               idx == nil or value == nil or idx ~= math.floor(idx) or value ~= math.floor(value) or
               idx < -max_exact or idx > max_exact or value < 0 or value > max_exact) then
                return {'-1', '0', '0', '0', '0', 'corrupt'}
            end
            if strict and value > 0 and idx > oldest then
                if idx > current or idx % 16 ~= slot or seen[idx] then
                    return {'-1', '0', '0', '0', '0', 'corrupt'}
                end
                seen[idx] = true
            end
            if idx <= oldest then
                if strict then table.insert(expired, field) else redis.call('HDEL', KEYS[1], field) end
            else
                if strict and value > max_exact - used then return {'-1', '0', '0', '0', '0', 'corrupt'} end
                used = used + value
                if value > 0 and (earliest == nil or idx < earliest) then earliest = idx end
            end
        end
    end
    if strict and stored_revision == revision and used > limit and not carried then
        return {'-1', '0', '0', '0', '0', 'corrupt'}
    end
    if strict and stored_revision ~= revision then carried = used > limit end
    if strict then for _, field in ipairs(expired) do redis.call('HDEL', KEYS[1], field) end end
    if used <= limit then carried = false end
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

if strict then
    redis.call('HSET', KEYS[1], 'last', integer(now), 'revision', revision,
        'period', integer(period), 'carried', carried and '1' or '0')
else
    redis.call('HSET', KEYS[1], 'last', integer(now))
end
redis.call('PEXPIRE', KEYS[1], ttl)
if allowed == 1 then
    local result = {'1', integer(remaining), integer(limit), integer(reset), '0', 'allowed'}
    if strict then table.insert(result, integer(now)) end
    return result
end
local result = {'0', integer(remaining), integer(limit), integer(reset), integer(retry), 'limited'}
if strict then table.insert(result, integer(now)) end
return result
`

const acquireLeaseScript = `local strict = false
` + acquireLeaseScriptBody
const strictAcquireLeaseScript = `local strict = true
` + acquireLeaseScriptBody

const acquireLeaseScriptBody = `
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
local max_exact = 9007199254740991
local max_state_bytes = 131072
local max_allocator_bytes = 1048576
local function integer(value) return string.format('%.0f', value) end
local function canonical_unsigned(value)
    return type(value) == 'string' and string.len(value) <= 16 and
        (value == '0' or string.match(value, '^[1-9][0-9]*$') ~= nil)
end
local function canonical_signed(value)
    if canonical_unsigned(value) then return true end
    if type(value) ~= 'string' or string.len(value) > 17 or string.sub(value, 1, 1) ~= '-' then return false end
    return string.match(string.sub(value, 2), '^[1-9][0-9]*$') ~= nil
end
local function valid_lease_field(value)
    return type(value) == 'string' and string.len(value) == 66 and
        string.sub(value, 1, 2) == 'l:' and string.match(string.sub(value, 3), '^[0-9a-f]+$') ~= nil
end
if server_clock then
    local server = redis.call('TIME')
    now = server[1] * 1000000 + server[2]
end
if strict and (now < -max_exact or now > max_exact or lease_us <= 0 or lease_us > max_exact or now > max_exact - lease_us) then
    return {'-1', '0', '0', '0', '0', 'overflow', '0'}
end
if redis.call('EXISTS', KEYS[1]) == 1 then
	if strict then
		local state_bytes = redis.call('MEMORY', 'USAGE', KEYS[1])
		if state_bytes == false or state_bytes == nil or state_bytes > max_allocator_bytes then
            return {'-1', '0', '0', '0', '0', 'corrupt', '0'}
        end
    end
    if redis.call('HGET', KEYS[1], 'schema') ~= schema or
       redis.call('HGET', KEYS[1], 'policy_id') ~= policy_id or
       redis.call('HGET', KEYS[1], 'algorithm') ~= 'concurrency' then
        return {'-1', '0', '0', '0', '0', 'corrupt', '0'}
    end
else
    redis.call('HSET', KEYS[1], 'schema', schema, 'policy_id', policy_id,
        'algorithm', 'concurrency', 'revision', revision, 'last', integer(now))
end
if redis.call('HLEN', KEYS[1]) > 1029 then
    return {'-1', '0', '0', '0', '0', 'corrupt', '0'}
end
local fields = redis.call('HGETALL', KEYS[1])
if strict then
local allowed_fields = {schema = true, policy_id = true, algorithm = true, revision = true, last = true}
local state_bytes = 0
for index = 1, #fields, 2 do
    local field = fields[index]
    state_bytes = state_bytes + string.len(field) + string.len(fields[index + 1])
    if string.len(field) > 66 or string.len(fields[index + 1]) > 128 or
       state_bytes > max_state_bytes or
       (not allowed_fields[field] and not valid_lease_field(field)) then
        return {'-1', '0', '0', '0', '0', 'corrupt', '0'}
    end
end
end
local observed_raw = redis.call('HGET', KEYS[1], 'last')
local observed = tonumber(observed_raw)
if strict and (not canonical_signed(observed_raw) or observed == nil or observed ~= math.floor(observed) or observed < -max_exact or observed > max_exact) then
    return {'-1', '0', '0', '0', '0', 'corrupt', '0'}
end
if now < observed then now = observed end
if strict and now > max_exact - lease_us then
    return {'-1', '0', '0', '0', '0', 'overflow', '0'}
end
if not strict then
    redis.call('HSET', KEYS[1], 'last', integer(now))
    redis.call('HSET', KEYS[1], 'revision', revision)
end
local used = 0
local earliest = 0
local expired = {}
for index = 1, #fields, 2 do
    if string.sub(fields[index], 1, 2) == 'l:' then
        local encoded = fields[index + 1]
        local separator = string.find(encoded, ':')
        if strict and separator == nil then return {'-1', '0', '0', '0', '0', 'corrupt', '0'} end
        local lease_cost_raw = string.sub(encoded, 1, separator - 1)
        local expires_raw = string.sub(encoded, separator + 1)
        local lease_cost = tonumber(lease_cost_raw)
        local expires = tonumber(expires_raw)
        if strict and (not canonical_unsigned(lease_cost_raw) or not canonical_signed(expires_raw) or
           lease_cost == nil or expires == nil or lease_cost ~= math.floor(lease_cost) or expires ~= math.floor(expires) or
           lease_cost <= 0 or lease_cost > 1024 or expires < -max_exact or expires > max_exact) then
            return {'-1', '0', '0', '0', '0', 'corrupt', '0'}
        end
        if expires <= now then
            if strict then table.insert(expired, fields[index]) else redis.call('HDEL', KEYS[1], fields[index]) end
        else
            if lease_cost <= 0 or lease_cost > 1024 - used then
                return {'-1', '0', '0', '0', '0', 'corrupt', '0'}
            end
            used = used + lease_cost
            if earliest == 0 or expires < earliest then earliest = expires end
        end
    end
end
if strict then
    for _, field in ipairs(expired) do redis.call('HDEL', KEYS[1], field) end
    redis.call('HSET', KEYS[1], 'last', integer(now), 'revision', revision)
end
local existing = redis.call('HGET', KEYS[1], lease_field)
if existing then
    local separator = string.find(existing, ':')
    local existing_cost_raw = string.sub(existing, 1, separator - 1)
    local expires_raw = string.sub(existing, separator + 1)
    local existing_cost = tonumber(existing_cost_raw)
    local expires = tonumber(expires_raw)
    if strict and (not canonical_unsigned(existing_cost_raw) or not canonical_signed(expires_raw)) then
        return {'-1', '0', '0', '0', '0', 'corrupt', '0'}
    end
    if existing_cost ~= cost then
        return {'-1', '0', '0', '0', '0', 'not_owned', '0'}
    end
    redis.call('PEXPIRE', KEYS[1], ttl_ms)
	local result = {'1', integer(limit - math.min(used, limit)), integer(limit), integer(expires),
		'0', 'allowed', integer(expires)}
	if strict then table.insert(result, integer(now)) end
	return result
end
local remaining = math.max(0, limit - used)
if cost > remaining then
	local result = {'0', integer(remaining), integer(limit), integer(earliest),
		integer(math.max(0, earliest - now)), 'limited', '0'}
	if strict then table.insert(result, integer(now)) end
	return result
end
local expires = now + lease_us
redis.call('HSET', KEYS[1], lease_field, integer(cost) .. ':' .. integer(expires))
redis.call('PEXPIRE', KEYS[1], ttl_ms)
local result = {'1', integer(remaining - cost), integer(limit), integer(expires),
	'0', 'allowed', integer(expires)}
if strict then table.insert(result, integer(now)) end
return result
`

const releaseLeaseScript = `local strict = false
` + releaseLeaseScriptBody
const strictReleaseLeaseScript = `local strict = true
` + releaseLeaseScriptBody

const releaseLeaseScriptBody = `
if redis.call('EXISTS', KEYS[1]) == 0 then return {'not_found'} end
local max_state_bytes = 131072
local max_allocator_bytes = 1048576
if strict then
    local state_bytes = redis.call('MEMORY', 'USAGE', KEYS[1])
    if state_bytes == false or state_bytes == nil or state_bytes > max_allocator_bytes then return {'corrupt'} end
end
if redis.call('HGET', KEYS[1], 'schema') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'policy_id') ~= ARGV[2] or
   redis.call('HGET', KEYS[1], 'algorithm') ~= 'concurrency' then
	if strict then return {'corrupt'} end
    return {'not_found'}
end
if strict then
    if redis.call('HLEN', KEYS[1]) > 1029 then return {'corrupt'} end
    local max_exact = 9007199254740991
    local function canonical_unsigned(value)
        return type(value) == 'string' and string.len(value) <= 16 and
            (value == '0' or string.match(value, '^[1-9][0-9]*$') ~= nil)
    end
    local function canonical_signed(value)
        if canonical_unsigned(value) then return true end
        if type(value) ~= 'string' or string.len(value) > 17 or string.sub(value, 1, 1) ~= '-' then return false end
        return string.match(string.sub(value, 2), '^[1-9][0-9]*$') ~= nil
    end
    local function valid_lease_field(value)
        return type(value) == 'string' and string.len(value) == 66 and
            string.sub(value, 1, 2) == 'l:' and string.match(string.sub(value, 3), '^[0-9a-f]+$') ~= nil
    end
    local allowed_fields = {schema = true, policy_id = true, algorithm = true, revision = true, last = true}
    local fields = redis.call('HGETALL', KEYS[1])
    local observed_raw = redis.call('HGET', KEYS[1], 'last')
    local observed = tonumber(observed_raw)
    if not canonical_signed(observed_raw) or observed == nil or observed ~= math.floor(observed) or observed < -max_exact or observed > max_exact then
        return {'corrupt'}
    end
    local used = 0
    local state_bytes = 0
    for index = 1, #fields, 2 do
        local name = fields[index]
        state_bytes = state_bytes + string.len(name) + string.len(fields[index + 1])
        if string.len(name) > 66 or string.len(fields[index + 1]) > 128 or state_bytes > max_state_bytes then return {'corrupt'} end
        if not allowed_fields[name] then
            if not valid_lease_field(name) then return {'corrupt'} end
            local encoded = fields[index + 1]
            local separator = string.find(encoded, ':')
            if separator == nil then return {'corrupt'} end
            local lease_cost_raw = string.sub(encoded, 1, separator - 1)
            local expires_raw = string.sub(encoded, separator + 1)
            local lease_cost = tonumber(lease_cost_raw)
            local expires = tonumber(expires_raw)
            if not canonical_unsigned(lease_cost_raw) or not canonical_signed(expires_raw) or
               lease_cost == nil or expires == nil or lease_cost ~= math.floor(lease_cost) or expires ~= math.floor(expires) or
               lease_cost <= 0 or lease_cost > 1024 or expires < -max_exact or expires > max_exact or lease_cost > 1024 - used then
                return {'corrupt'}
            end
            used = used + lease_cost
        end
    end
end
local field = 'l:' .. ARGV[3]
local existing = redis.call('HGET', KEYS[1], field)
if not existing then return {'not_found'} end
if existing ~= ARGV[4] .. ':' .. ARGV[5] then return {'not_owned'} end
redis.call('HDEL', KEYS[1], field)
if redis.call('HLEN', KEYS[1]) == 5 then redis.call('DEL', KEYS[1]) end
return {'ok'}
`

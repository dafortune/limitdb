local tokens_per_ms        = tonumber(ARGV[1])
local bucket_size          = tonumber(ARGV[2])
local new_content          = tonumber(ARGV[2])
local tokens_to_take       = tonumber(ARGV[3])
local ttl                  = tonumber(ARGV[4])
local source               = ARGV[5]

local current_time = redis.call('TIME')
local current_timestamp_ms = current_time[1] * 1000 + current_time[2] / 1000

local current = redis.pcall('HMGET', KEYS[1], 'd', 'r', 'nca', 'a')

if current.err ~= nil then
    current = {}
end

if current[1] and tokens_per_ms then
    -- drip bucket
    local last_drip = current[1]
    local content = current[2]
    local delta_ms = math.max(current_timestamp_ms - last_drip, 0)
    local drip_amount = delta_ms * tokens_per_ms
    new_content = math.min(content + drip_amount, bucket_size)
elseif current[1] and tokens_per_ms == 0 then
    -- fixed bucket
    new_content = current[2]
end

local enough_tokens = new_content >= tokens_to_take
local current_conformant_attempts = current[3] or 0;
local non_conformant_attempts = 0;
local attempts = (current[4] or 0) + 1

if enough_tokens then
    new_content = math.min(new_content - tokens_to_take, bucket_size)
else
    -- HINCRBY is the natural redis command to think about for this case
    -- however this approach allows to use a single "HMSET" command instead
    -- HINCRBY and "HMSET" which makes the code a bit cleaner and since LUA scripts
    -- runs atomically it has the same guarantees as HINCRBY
    non_conformant_attempts = current_conformant_attempts + 1
end

-- https://redis.io/commands/EVAL#replicating-commands-instead-of-scripts


local source_increment_count = nil
local source_remove_key = nil
local sources_key = nil

if source then
    local MAX_SOURCES = 5

    sources_key = KEYS[1] .. ':srcs'

    local source_score = redis.call('ZSCORE', sources_key, source)
    local n_counters = redis.call('ZCARD', sources_key)

    if source_score ~= false or n_counters < MAX_SOURCES then
        source_increment_count = 1
    else
        -- Get head of the sorted list (minimum score); it is guaranteed to exist
        -- because otherwise n_counters < MAX_SOURCES
        local head_entry = redis.call('ZRANGE', sources_key, 0, 0, 'WITHSCORES')
        local head_key = head_entry[1]
        local head_score = head_entry[2]

        source_increment_count = head_score + 1
        source_remove_key = head_key
    end
end

redis.replicate_commands()

-- These operation are O(log(N)) but N is expected to be pretty short (currently harcoded to 5)
-- also we operate with the head of the sorted set for removal (double linked list)
if source_increment_count then
    redis.call('ZINCRBY', sources_key, source_increment_count, source)
end

if source_remove_key then
    redis.call('ZREM', sources_key, source_remove_key)
end

if sources_key then
    redis.call('EXPIRE', sources_key, ttl)
end

redis.call('HMSET', KEYS[1],
            'd', current_timestamp_ms,
            'r', new_content,
            'nca', non_conformant_attempts,
            'a', attempts)

redis.call('EXPIRE', KEYS[1], ttl)

return { new_content, enough_tokens, current_timestamp_ms, current_conformant_attempts }

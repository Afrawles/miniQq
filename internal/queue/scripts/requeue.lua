local due = redis.call("ZRANGEBYSCORE", KEYS[1], '-inf', ARGV[1])
local count = 0 

for i, id in ipairs(due) do
    redis.call("ZREM", KEYS[1], id) -- retry queue , or any other queue
    redis.call("LPUSH", KEYS[2], id) -- ready
    redis.call('HSET', 'job:'..id, 'status', ARGV[2])
    count = count + 1
end
return count

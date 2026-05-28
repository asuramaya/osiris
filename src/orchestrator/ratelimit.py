"""Per-origin token-bucket rate limiting (Redis).

Rate limits are origin-scoped, not global (DESIGN §16) — keyed by the *resolved*
target origin at dispatch, since one helper run may touch many origins. The
bucket refills continuously at `rps` up to `capacity` (burst); acquisition is
atomic via a Lua script so concurrent workers can't oversubscribe an origin.
"""

from __future__ import annotations

import time

import redis.asyncio as aioredis

# KEYS[1]=bucket  ARGV: rps, capacity, now_ms, requested
# Stored as a hash {tokens, ts}. Returns 1 if granted, 0 if not.
_TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local rps = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local want = tonumber(ARGV[4])
local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  ts = now
end
local elapsed = math.max(0, now - ts) / 1000.0
tokens = math.min(capacity, tokens + elapsed * rps)
local granted = 0
if tokens >= want then
  tokens = tokens - want
  granted = 1
end
redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, 60000)
return granted
"""


class RateLimiter:
    def __init__(self, redis: aioredis.Redis) -> None:
        self.redis = redis
        self._script = redis.register_script(_TOKEN_BUCKET_LUA)

    async def acquire(self, origin: str, *, rps: float, capacity: float | None = None) -> bool:
        cap = capacity if capacity is not None else max(1.0, rps)
        now_ms = int(time.time() * 1000)
        granted = await self._script(
            keys=[f"tb:{origin}"], args=[rps, cap, now_ms, 1]
        )
        return bool(int(granted))

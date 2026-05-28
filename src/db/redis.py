from __future__ import annotations

import redis.asyncio as aioredis


def create_redis(url: str) -> aioredis.Redis:
    """Async Redis client. decode_responses=True so token-bucket/budget counters
    come back as str (we parse them), matching the Lua scripts' string returns."""
    client: aioredis.Redis = aioredis.from_url(  # type: ignore[no-untyped-call]
        url, decode_responses=True
    )
    return client

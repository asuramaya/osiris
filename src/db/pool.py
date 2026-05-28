from __future__ import annotations

import json
from typing import Any

import asyncpg


async def _init_connection(conn: Any) -> None:
    """Register JSON/JSONB codecs so Python dicts pass to/from jsonb columns directly."""
    for typename in ("json", "jsonb"):
        await conn.set_type_codec(
            typename,
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
        )


async def create_pool(dsn: str, *, min_size: int = 1, max_size: int = 10) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(
        dsn=dsn, init=_init_connection, min_size=min_size, max_size=max_size
    )
    assert pool is not None
    return pool

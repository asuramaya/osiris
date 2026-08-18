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


async def create_pool(
    dsn: str, *, min_size: int = 1, max_size: int = 10, application_name: str | None = None,
) -> asyncpg.Pool:
    """`application_name` (task #180 piece 2 (c)): tags every connection this pool opens so
    `pg_stat_activity` can be grouped BY DAEMON, not read as one undifferentiated blob —
    `asyncpg` forwards it straight into `server_settings` per-connection, no DSN mangling
    needed. Optional and appended-only: every existing caller with no name to give keeps
    Postgres's own default (the client library name), unchanged."""
    server_settings = {"application_name": application_name} if application_name else None
    pool = await asyncpg.create_pool(
        dsn=dsn, init=_init_connection, min_size=min_size, max_size=max_size,
        server_settings=server_settings,
    )
    assert pool is not None
    return pool

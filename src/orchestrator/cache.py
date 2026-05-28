"""Response cache — reuse source fetches across expansions (DESIGN §6 cache).

`cached_fetch` is the single fetch path for the cascade and federation: it returns
a fresh cached response when one exists, otherwise calls the connector and stores
the result. This is what makes deep / repeated expansion affordable — a (helper,
object) pair is fetched from the network at most once per TTL, no matter how many
times the case is expanded or previewed.
"""

from __future__ import annotations

from typing import Any

import asyncpg

from src.connectors.registry import Connector
from src.parsers.base import InputObject


async def cached_fetch(
    pool: asyncpg.Pool,
    connector: Connector,
    helper_id: str,
    input_object: InputObject,
    *,
    cache_ttl: int,
) -> dict[str, Any]:
    if cache_ttl > 0:
        cached = await pool.fetchval(
            "SELECT response FROM helper_cache "
            "WHERE helper_id=$1 AND object_canonical=$2 "
            "  AND fetched_at > now() - make_interval(secs => $3)",
            helper_id,
            input_object.canonical,
            cache_ttl,
        )
        if cached is not None:
            return dict(cached)

    response = await connector(input_object)
    await pool.execute(
        "INSERT INTO helper_cache (helper_id, object_canonical, response, fetched_at) "
        "VALUES ($1,$2,$3, now()) "
        "ON CONFLICT (helper_id, object_canonical) "
        "DO UPDATE SET response=EXCLUDED.response, fetched_at=now()",
        helper_id,
        input_object.canonical,
        response,
    )
    return response

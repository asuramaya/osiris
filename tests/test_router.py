from __future__ import annotations

import uuid

import redis.asyncio as aioredis
from src.actions.core import Actions
from src.orchestrator.manifests import Manifest
from src.orchestrator.ratelimit import RateLimiter
from src.orchestrator.router import Route, route


def _manifest(**over: object) -> Manifest:
    base = {
        "id": "h",
        "name": "h",
        "consumes": {"type": "Domain"},
        "parser": "crtsh_subdomains",
        "origin": "origin.router",
        "tier": "open",
    }
    base.update(over)
    return Manifest.model_validate(base)


async def test_route_cached(actions: Actions, case_id: str, redis_client: aioredis.Redis) -> None:
    limiter = RateLimiter(redis_client)
    obj = await actions.create_or_find_object("Domain", "r1.test", "analyst:test", case_id)
    await actions.pool.execute(
        "INSERT INTO helper_runs (helper_id, object_id, case_id, status, tier, finished_at) "
        "VALUES ('h',$1,$2,'done','open', now())",
        obj,
        uuid.UUID(case_id),
    )
    assert await route(actions.pool, limiter, _manifest(cache_ttl=3600), obj) is Route.CACHED


async def test_route_server_then_defer_on_token_exhaustion(
    actions: Actions, case_id: str, redis_client: aioredis.Redis
) -> None:
    limiter = RateLimiter(redis_client)
    obj = await actions.create_or_find_object("Domain", "r2.test", "analyst:test", case_id)
    m = _manifest(rate={"per_origin_rps": 0.001, "per_origin_concurrent": 1})
    # first dispatch consumes the only token -> SERVER_WORKER; next -> DEFER
    assert await route(actions.pool, limiter, m, obj) is Route.SERVER_WORKER
    assert await route(actions.pool, limiter, m, obj) is Route.DEFER


async def test_route_gated_awaits_human(
    actions: Actions, case_id: str, redis_client: aioredis.Redis
) -> None:
    limiter = RateLimiter(redis_client)
    obj = await actions.create_or_find_object("Domain", "r3.test", "analyst:test", case_id)
    assert await route(actions.pool, limiter, _manifest(tier="gated"), obj) is Route.AWAITING_HUMAN

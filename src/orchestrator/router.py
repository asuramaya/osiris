"""The router — per-(helper, object) routing decision (DESIGN §7).

Phase 3 implements the tier=open path: cache -> token bucket -> server worker /
defer. Fragile/gated/manual tiers return placeholder routes until their phases
(SearXNG, browser bridge, leases) land. Routing is per (helper, object), not per
source globally, and rate budget is checked against the helper's resolved origin.
"""

from __future__ import annotations

import enum
import uuid

import asyncpg

from src.connectors.leases import LeaseStore, valid_for_server_egress
from src.orchestrator.manifests import Manifest
from src.orchestrator.ratelimit import RateLimiter


class Route(enum.Enum):
    CACHED = "cached"
    SERVER_WORKER = "server_worker"
    SERVER_WORKER_WITH_LEASE = "server_worker_with_lease"
    DEFER = "defer"
    AWAITING_HUMAN = "awaiting_human"


async def _is_cached(pool: asyncpg.Pool, helper_id: str, object_id: uuid.UUID, ttl: int) -> bool:
    return bool(
        await pool.fetchval(
            "SELECT 1 FROM helper_runs WHERE helper_id=$1 AND object_id=$2 "
            "AND status='done' AND finished_at > now() - make_interval(secs => $3) LIMIT 1",
            helper_id,
            object_id,
            ttl,
        )
    )


async def has_cached_run_for_case(
    pool: asyncpg.Pool, helper_id: str, object_id: uuid.UUID, case_id: uuid.UUID, ttl: int
) -> bool:
    """Like _is_cached but scoped to ONE case. The global _is_cached makes route()
    return CACHED whenever ANY case ran the helper; the cascade uses this to tell a
    genuine same-case cache hit (skip) from a cross-case one (re-materialize into
    this case so its results actually land here)."""
    return bool(
        await pool.fetchval(
            "SELECT 1 FROM helper_runs WHERE helper_id=$1 AND object_id=$2 AND case_id=$3 "
            "AND status='done' AND finished_at > now() - make_interval(secs => $4) LIMIT 1",
            helper_id,
            object_id,
            case_id,
            ttl,
        )
    )


async def route(
    pool: asyncpg.Pool,
    limiter: RateLimiter,
    manifest: Manifest,
    object_id: uuid.UUID,
    *,
    lease_store: LeaseStore | None = None,
    current_ip: str | None = None,
) -> Route:
    if await _is_cached(pool, manifest.id, object_id, manifest.cache_ttl):
        return Route.CACHED

    if manifest.tier == "open":
        ok = await limiter.acquire(
            manifest.origin,
            rps=manifest.rate.per_origin_rps,
            capacity=float(manifest.rate.per_origin_concurrent),
        )
        return Route.SERVER_WORKER if ok else Route.DEFER

    if manifest.tier == "gated":
        # A valid (IP, UA)-bound lease lets us skip the human and reuse the solved
        # session server-side — the single-box happy path (same egress IP).
        if lease_store is not None and current_ip is not None:
            lease = await lease_store.get(manifest.origin)
            if lease is not None and valid_for_server_egress(lease, current_ip):
                return Route.SERVER_WORKER_WITH_LEASE
        return Route.AWAITING_HUMAN

    if manifest.tier in ("manual", "suggest"):
        # suggest = osint4all-style augmentation link-out: surfaced in the tray
        # for the analyst to open/co-browse, never scraped autonomously (#6).
        return Route.AWAITING_HUMAN
    return Route.DEFER  # fragile: needs SearXNG/browser (later phase)

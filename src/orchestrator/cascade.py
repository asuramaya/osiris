"""The cascade engine — durable outbox relay + trigger fan-out + dispatch.

This is the autonomous loop (DESIGN §6). Actions write events to the `outbox`
in the same transaction as the data change (no lost cascades, unlike fire-and-
forget pub/sub). The relay claims outbox rows (FOR UPDATE SKIP LOCKED), fans out
to matching helpers, and dispatches each through the router + budget gates.
Emitted objects write fresh outbox rows, so a single run_cascade() loop drives
the whole transitive expansion until it drains or budgets stop it.

The connector (network fetch) is injected, so this is fully testable against
real Postgres + real Redis with deterministic responses.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

import asyncpg

from src.actions.core import Actions
from src.connectors.registry import Connector
from src.orchestrator.budgets import BudgetLedger
from src.orchestrator.cache import cached_fetch
from src.orchestrator.challenges import ChallengeDetected
from src.orchestrator.handoff import suspend
from src.orchestrator.manifests import Manifest
from src.orchestrator.ratelimit import RateLimiter
from src.orchestrator.router import Route, route
from src.orchestrator.runner import claim_run, execute_claimed, load_input_object
from src.orchestrator.triggers import matching_helpers
from src.parsers.base import InputObject


@dataclass
class CascadeContext:
    actions: Actions
    limiter: RateLimiter
    ledger: BudgetLedger
    manifests: dict[str, Manifest]
    connectors: dict[str, Connector]

    @property
    def pool(self) -> asyncpg.Pool:
        return self.actions.pool


async def _hop_of(pool: asyncpg.Pool, case_id: uuid.UUID, object_id: uuid.UUID) -> int:
    hop = await pool.fetchval(
        "SELECT hop_distance FROM case_objects WHERE case_id=$1 AND object_id=$2",
        case_id,
        object_id,
    )
    return int(hop) if hop is not None else 0


async def dispatch(
    ctx: CascadeContext, manifest: Manifest, object_id: uuid.UUID, case_id: uuid.UUID
) -> str:
    """Route + budget-gate + run one (helper, object). Returns an outcome tag."""
    input_hop = await _hop_of(ctx.pool, case_id, object_id)

    decision = await ctx.ledger.check(case_id, object_id, hop_distance=input_hop)
    if not decision.allowed:
        return f"blocked:{decision.reason}"

    decided = await route(ctx.pool, ctx.limiter, manifest, object_id)
    if decided is Route.CACHED:
        return "cached"
    if decided is Route.DEFER:
        return "defer"
    if decided is Route.AWAITING_HUMAN:
        # gated tier — never attempt server-side; park for the analyst's browser.
        input_object = await load_input_object(ctx.pool, object_id)
        handoff_id = await suspend(
            ctx.actions, ctx.ledger, manifest, object_id, case_id,
            url=_render_url(manifest, input_object), challenge_kind=None,
        )
        return "suspended" if handoff_id is not None else "blocked:max_human_handoffs"

    # SERVER_WORKER: needs a connector (the network fetch) to run.
    connector = ctx.connectors.get(manifest.id)
    if connector is None:
        return "no_connector"
    if not await ctx.ledger.reserve_rate_credit(case_id):
        return "blocked:rate_credits"
    run_id = await claim_run(ctx.actions, manifest.id, object_id, case_id, manifest.tier)
    if run_id is None:  # another worker is already on it
        await ctx.ledger.refund_rate_credit(case_id)
        return "skipped:active"

    input_object = await load_input_object(ctx.pool, object_id)
    try:
        response = await cached_fetch(
            ctx.pool, connector, manifest.id, input_object, cache_ttl=manifest.cache_ttl
        )
    except ChallengeDetected as cd:
        # bot-fight / login wall hit mid-fetch — suspend the in-flight run instead
        # of solving or evading. The rate credit is refunded; a handoff credit is spent.
        await ctx.ledger.refund_rate_credit(case_id)
        handoff_id = await suspend(
            ctx.actions, ctx.ledger, manifest, object_id, case_id,
            url=cd.url, challenge_kind=cd.challenge.kind.value, existing_run_id=run_id,
        )
        return "suspended" if handoff_id is not None else "blocked:max_human_handoffs"
    await execute_claimed(
        ctx.actions, manifest, response, input_object, case_id, run_id, input_hop=input_hop
    )
    return "ran"


def _render_url(manifest: Manifest, input_object: InputObject) -> str | None:
    if manifest.template is None or manifest.template.url is None:
        return None
    return (
        manifest.template.url
        .replace("{object.canonical}", input_object.canonical)
        .replace("{object.type}", input_object.type)
    )


async def fire_triggers(
    ctx: CascadeContext, event: str, object_id: uuid.UUID, case_id: uuid.UUID
) -> list[str]:
    obj = await ctx.pool.fetchrow("SELECT type FROM objects WHERE id=$1", object_id)
    if obj is None:
        return []
    prop_rows = await ctx.pool.fetch(
        "SELECT DISTINCT name FROM current_assertions WHERE object_id=$1", object_id
    )
    props = {r["name"] for r in prop_rows}
    helper_ids = await matching_helpers(ctx.pool, event, obj["type"], props, case_id=case_id)
    outcomes: list[str] = []
    for hid in helper_ids:
        manifest = ctx.manifests.get(hid)
        if manifest is None:
            continue  # trigger for a helper not loaded in this worker
        outcomes.append(await dispatch(ctx, manifest, object_id, case_id))
    return outcomes


async def drain_outbox(ctx: CascadeContext, *, limit: int = 100) -> int:
    """Claim and process a batch of unpublished outbox events. Returns count."""
    rows = await ctx.pool.fetch(
        "UPDATE outbox SET claimed_at=now() WHERE id IN ("
        "  SELECT id FROM outbox WHERE published_at IS NULL AND claimed_at IS NULL "
        "  ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED"
        ") RETURNING id, event_type, object_id, case_id",
        limit,
    )
    for row in rows:
        if row["object_id"] is not None and row["case_id"] is not None:
            await fire_triggers(ctx, row["event_type"], row["object_id"], row["case_id"])
        await ctx.pool.execute("UPDATE outbox SET published_at=now() WHERE id=$1", row["id"])
    return len(rows)


async def run_cascade(ctx: CascadeContext, *, max_iterations: int = 1000) -> int:
    """Drain the outbox until empty (or budgets stop producing new events).
    Returns the total number of events processed."""
    total = 0
    for _ in range(max_iterations):
        n = await drain_outbox(ctx)
        if n == 0:
            break
        total += n
    return total

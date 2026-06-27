"""The watch — the kernel as a tripwire, not just a lens.

Three source-agnostic primitives, each riding reliability the kernel already has
(idempotent emit, durable outbox, atomic cursors). No real collection lives here:
a `tick` takes an injected puller; a real connector lands in a later phase.

  * watermarks (`get_cursor`/`set_cursor`) — a generic "last cursor" store. A source
    tick pulls only the delta past its cursor. Re-pulls are already safe (find-or-
    create dedups); the watermark makes them cheap.
  * `tick` — a scheduled, source-agnostic pull: read cursor -> puller(cursor) -> feed
    each item through Actions -> advance cursor. The materialized objects write outbox
    events, which the evaluator below picks up. One clean seam between collection and
    the watch.
  * `evaluate_subscriptions` — drains the durable outbox PAST ITS OWN CLAIM FLAG
    (`evaluated_at`, independent of the cascade's `published_at`), matches each new
    mutation against saved `subscriptions`, and emits an `alerts` row to a dumb sink.

The match is prospective: a subscription created today fires on tomorrow's events,
not yesterday's (each outbox row is evaluated exactly once). That is the tripwire
semantic — "tell me when X happens", not "search what already happened".
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

logger = logging.getLogger("osiris.monitor")


# --- watermarks: a generic cursor store -------------------------------------

async def get_cursor(pool: asyncpg.Pool, key: str) -> str | None:
    """The last recorded cursor for `key`, or None if never set."""
    cursor: str | None = await pool.fetchval("SELECT cursor FROM watermarks WHERE key=$1", key)
    return cursor


async def set_cursor(pool: asyncpg.Pool, key: str, cursor: str) -> None:
    """Record `cursor` for `key` (upsert)."""
    await pool.execute(
        "INSERT INTO watermarks (key, cursor, updated_at) VALUES ($1,$2,now()) "
        "ON CONFLICT (key) DO UPDATE SET cursor=EXCLUDED.cursor, updated_at=now()",
        key,
        cursor,
    )


# --- tick: a source-agnostic scheduled pull ---------------------------------

@dataclass
class WatchItem:
    """One thing a puller found in a delta: an object plus its facts. The tick
    materializes it through Actions (idempotent), so a re-pull is a no-op."""

    type: str
    canonical: str
    properties: dict[str, Any] = field(default_factory=dict)
    evidence_class: EvidenceClass = EvidenceClass.AUTHORITATIVE_API
    observed_at: datetime | None = None


@dataclass
class PullResult:
    """What a puller returns: the items in this delta and the new cursor to persist
    AFTER they are committed (so a crash mid-tick re-pulls the same delta safely)."""

    items: list[WatchItem]
    cursor: str


# A puller is the only collection-specific part: (last cursor) -> a delta. Injected,
# so the watch is fully testable with a canned source and no network.
Puller = Callable[[str | None], Awaitable[PullResult]]


async def tick(actions: Actions, source_id: str, puller: Puller) -> int:
    """Pull the delta past `source:<source_id>`'s cursor, materialize each item
    through Actions, then advance the cursor. Returns the number of items applied.

    The cursor is advanced ONLY after the items commit — if the process dies
    mid-tick, the next tick re-pulls the same delta and find-or-create dedups it."""
    pool = actions.pool
    cursor = await get_cursor(pool, f"source:{source_id}")
    result = await puller(cursor)
    for item in result.items:
        observed = item.observed_at or datetime.now(UTC)
        object_id = await actions.create_or_find_object(item.type, item.canonical, source_id)
        for name, value in item.properties.items():
            await actions.assert_property(
                object_id, name, value, source_id, observed,
                confidence_for(item.evidence_class),
                evidence_class=item.evidence_class.value,
            )
    await set_cursor(pool, f"source:{source_id}", result.cursor)
    return len(result.items)


# --- subscriptions: saved match criteria ------------------------------------

@dataclass(frozen=True)
class GraphEvent:
    """One outbox mutation, enriched with the object's type/canonical for matching."""

    outbox_id: int
    event_type: str
    object_id: uuid.UUID | None
    case_id: uuid.UUID | None
    object_type: str | None
    canonical: str | None
    payload: dict[str, Any]
    value: Any | None  # the new property value (property_added only)
    # the object's current scalar properties — loaded for object_created so a beat can
    # match a whole object at once (e.g. zip AND price); empty for other events.
    props: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Subscription:
    id: uuid.UUID
    name: str
    criteria: dict[str, Any]
    webhook_url: str | None


def matches(criteria: dict[str, Any], event: GraphEvent) -> dict[str, Any] | None:
    """Does `event` satisfy `criteria`? Returns a small "why it matched" dict, or
    None. All present clauses must hold (AND); absent clauses don't constrain.

    Supported clauses (all source-agnostic):
      event_types       : list[str]  — any of these outbox event types
      object_type       : str        — the object's type
      canonical_prefix  : str        — object canonical starts with (e.g. 'cik:')
      canonical_contains: str        — substring, case-insensitive
      property_name     : str        — for property_added: the property's name
      value_contains    : str        — for property_added: substring of the new value
    """
    ets = criteria.get("event_types")
    if ets and event.event_type not in ets:
        return None
    ot = criteria.get("object_type")
    if ot and event.object_type != ot:
        return None
    cp = criteria.get("canonical_prefix")
    if cp and not (event.canonical or "").startswith(cp):
        return None
    cc = criteria.get("canonical_contains")
    if cc and cc.lower() not in (event.canonical or "").lower():
        return None
    pn = criteria.get("property_name")
    if pn and event.payload.get("name") != pn:
        return None
    vc = criteria.get("value_contains")
    if vc and vc.lower() not in str(event.value or "").lower():
        return None
    # object-level conjunction over the object's properties (a "beat"): every condition
    # must hold. Reads event.props (object_created); falls back to the event value when a
    # condition names the property that just changed.
    for cond in criteria.get("where", []) or []:
        prop = cond.get("property")
        actual = event.props.get(prop)
        if actual is None and event.payload.get("name") == prop:
            actual = event.value
        if not match_condition(actual, cond.get("op", "contains"), cond.get("value")):
            return None
    return {
        "event_type": event.event_type,
        "object_type": event.object_type,
        "canonical": event.canonical,
        "property": event.payload.get("name"),
    }


def match_condition(actual: Any, op: str, expected: Any) -> bool:
    """One beat condition. Ops: eq / contains (case-insensitive substring) / lt / gt
    (numeric). A missing value or an un-parseable number fails closed (no false match).
    Shared by the evaluator (matches) and the read-model feed (/matches)."""
    if actual is None:
        return False
    if op == "eq":
        return str(actual).strip().lower() == str(expected).strip().lower()
    if op == "contains":
        return str(expected).lower() in str(actual).lower()
    if op in ("lt", "gt"):
        try:
            a, b = float(actual), float(expected)
        except (TypeError, ValueError):
            return False
        return a < b if op == "lt" else a > b
    return False


async def create_subscription(
    pool: asyncpg.Pool, name: str, criteria: dict[str, Any], webhook_url: str | None = None
) -> uuid.UUID:
    """Save a watch. Returns its id."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "INSERT INTO subscriptions (name, criteria, webhook_url) VALUES ($1,$2,$3) RETURNING id",
        name,
        criteria,
        webhook_url,
    )


async def _active_subscriptions(pool: asyncpg.Pool) -> list[Subscription]:
    import json
    rows = await pool.fetch(
        "SELECT id, name, criteria, webhook_url FROM subscriptions WHERE active ORDER BY created_at"
    )
    out: list[Subscription] = []
    for r in rows:
        crit = r["criteria"]
        if isinstance(crit, str):  # asyncpg returns jsonb as text unless a codec is set
            crit = json.loads(crit)
        out.append(Subscription(r["id"], r["name"], crit, r["webhook_url"]))
    return out


# --- the dumb alert sink ----------------------------------------------------

# A sink delivers an alert OUTSIDE the durable `alerts` table (which is always
# written first). Default: POST to the subscription's webhook if it has one. NOT a
# CRM — a table row + an optional notification, nothing more. Injectable for tests.
Sink = Callable[["Alert"], Awaitable[bool]]


@dataclass(frozen=True)
class Alert:
    id: uuid.UUID
    subscription_id: uuid.UUID
    subscription_name: str
    object_id: uuid.UUID | None
    event_type: str
    matched: dict[str, Any]
    webhook_url: str | None


async def _post_webhook(alert: Alert) -> bool:
    """Best-effort webhook delivery. Returns True if it should be marked delivered."""
    if not alert.webhook_url:
        return False
    import httpx
    payload = {
        "alert_id": str(alert.id),
        "subscription": alert.subscription_name,
        "event_type": alert.event_type,
        "object_id": str(alert.object_id) if alert.object_id else None,
        "matched": alert.matched,
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(alert.webhook_url, json=payload)
        resp.raise_for_status()
    return True


async def _load_event(pool: asyncpg.Pool, row: asyncpg.Record) -> GraphEvent:
    object_type: str | None = None
    canonical: str | None = None
    value: Any | None = None
    if row["object_id"] is not None:
        obj = await pool.fetchrow(
            "SELECT type, canonical FROM objects WHERE id=$1", row["object_id"]
        )
        if obj is not None:
            object_type, canonical = obj["type"], obj["canonical"]
    payload = row["payload"]
    if isinstance(payload, str):
        import json
        payload = json.loads(payload)
    if row["event_type"] == "property_added" and row["object_id"] is not None:
        name = payload.get("name")
        if name:
            value = await pool.fetchval(
                "SELECT value #>> '{}' FROM current_assertions "
                "WHERE object_id=$1 AND name=$2 ORDER BY observed_at DESC LIMIT 1",
                row["object_id"],
                name,
            )
    props: dict[str, Any] = {}
    if row["event_type"] == "object_created" and row["object_id"] is not None:
        # by evaluation time the object's assertions are committed (the evaluator runs
        # after the tick), so a beat can match the whole object on its creation event.
        for r in await pool.fetch(
            "SELECT DISTINCT ON (name) name, value #>> '{}' AS v FROM current_assertions "
            "WHERE object_id=$1 ORDER BY name, observed_at DESC",
            row["object_id"],
        ):
            props[r["name"]] = r["v"]
    return GraphEvent(
        outbox_id=row["id"],
        event_type=row["event_type"],
        object_id=row["object_id"],
        case_id=row["case_id"],
        object_type=object_type,
        canonical=canonical,
        payload=payload,
        value=value,
        props=props,
    )


async def evaluate_subscriptions(
    pool: asyncpg.Pool, *, sink: Sink | None = None, limit: int = 500
) -> int:
    """Claim a batch of un-evaluated outbox rows, match each against active
    subscriptions, and emit an `alerts` row per match. Returns the number of alerts
    emitted. Idempotent: the claim flag (`evaluated_at`) makes each row fire at most
    once; the (subscription, outbox) unique key makes a re-run a no-op.

    The claim is gap-free (FOR UPDATE SKIP LOCKED on the unevaluated flag), and the
    evaluator never touches `published_at`, so it is fully decoupled from the cascade.
    """
    deliver: Sink = sink if sink is not None else _post_webhook
    subs = await _active_subscriptions(pool)

    rows = await pool.fetch(
        "UPDATE outbox SET evaluated_at=now() WHERE id IN ("
        "  SELECT id FROM outbox WHERE evaluated_at IS NULL "
        "  ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED"
        ") RETURNING id, event_type, object_id, case_id, payload",
        limit,
    )
    if not rows:
        return 0

    fired: list[Alert] = []
    for row in rows:
        if not subs:
            continue
        event = await _load_event(pool, row)
        for sub in subs:
            why = matches(sub.criteria, event)
            if why is None:
                continue
            alert_id = await pool.fetchval(
                "INSERT INTO alerts (subscription_id, outbox_id, object_id, event_type, matched) "
                "VALUES ($1,$2,$3,$4,$5) ON CONFLICT (subscription_id, outbox_id) DO NOTHING "
                "RETURNING id",
                sub.id,
                event.outbox_id,
                event.object_id,
                event.event_type,
                why,
            )
            if alert_id is None:  # already alerted (idempotent re-run)
                continue
            fired.append(
                Alert(alert_id, sub.id, sub.name, event.object_id, event.event_type,
                      why, sub.webhook_url)
            )

    # deliver to the side-channel sink AFTER the durable rows are committed. A sink
    # failure must never lose the alert (the table row stands) or abort the batch.
    for alert in fired:
        try:
            if await deliver(alert):
                await pool.execute(
                    "UPDATE alerts SET delivered_at=now() WHERE id=$1", alert.id
                )
        except Exception as exc:
            logger.warning("alert sink failed for %s: %r", alert.id, exc)
    return len(fired)

"""The watch — watermarks, source ticks, and the subscription evaluator.

Proves the tripwire: a saved subscription fires on a matching new graph mutation
and stays QUIET on noise, the cursor primitives are atomic, and a canned source
tick drives the whole chain (pull -> materialize -> outbox -> alert) with no network.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.monitor import (
    Alert,
    GraphEvent,
    PullResult,
    WatchItem,
    create_subscription,
    evaluate_subscriptions,
    get_cursor,
    matches,
    set_cursor,
    tick,
)

NOW = datetime(2026, 6, 26, tzinfo=UTC)


def _ev(**kw: object) -> GraphEvent:
    base: dict[str, object] = dict(
        outbox_id=1, event_type="object_created", object_id=uuid.uuid4(), case_id=None,
        object_type="Organization", canonical="cik:0001", payload={}, value=None,
    )
    base.update(kw)
    return GraphEvent(**base)  # type: ignore[arg-type]


# --- the matcher (pure) -----------------------------------------------------

def test_matches_empty_criteria_matches_anything() -> None:
    assert matches({}, _ev()) is not None


def test_matches_all_clauses_must_hold() -> None:
    crit = {"event_types": ["object_created"], "object_type": "Organization",
            "canonical_prefix": "cik:"}
    assert matches(crit, _ev()) is not None
    # wrong event type
    assert matches(crit, _ev(event_type="property_added")) is None
    # wrong object type
    assert matches(crit, _ev(object_type="Person")) is None
    # wrong canonical prefix
    assert matches(crit, _ev(canonical="lei:XYZ")) is None


def test_matches_value_contains_on_property_added() -> None:
    crit = {"event_types": ["property_added"], "property_name": "name",
            "value_contains": "neuralink"}
    ev = _ev(event_type="property_added", payload={"name": "name"}, value="Neuralink Corp.")
    assert matches(crit, ev) is not None
    # right property, wrong value
    assert matches(crit, _ev(event_type="property_added", payload={"name": "name"},
                             value="Acme Inc.")) is None
    # right value, wrong property
    assert matches(crit, _ev(event_type="property_added", payload={"name": "address"},
                             value="Neuralink HQ")) is None


def test_matches_canonical_contains_case_insensitive() -> None:
    assert matches({"canonical_contains": "0001"}, _ev(canonical="cik:0001")) is not None
    assert matches({"canonical_contains": "ZZZ"}, _ev(canonical="cik:0001")) is None


# --- watermarks -------------------------------------------------------------

async def test_watermark_roundtrip_and_upsert(actions: Actions) -> None:
    pool = actions.pool
    assert await get_cursor(pool, "source:x") is None
    await set_cursor(pool, "source:x", "100")
    assert await get_cursor(pool, "source:x") == "100"
    await set_cursor(pool, "source:x", "200")  # upsert, not duplicate
    assert await get_cursor(pool, "source:x") == "200"


# --- source tick ------------------------------------------------------------

async def test_tick_materializes_delta_and_advances_cursor(actions: Actions) -> None:
    calls: list[str | None] = []

    async def puller(cursor: str | None) -> PullResult:
        calls.append(cursor)
        if cursor is None:  # first pull: two items
            return PullResult(
                [WatchItem("Organization", "cik:1", {"name": "One"}),
                 WatchItem("Organization", "cik:2", {"name": "Two"})],
                cursor="2",
            )
        return PullResult([], cursor=cursor)  # delta past "2" is empty

    n = await tick(actions, "testsrc", puller)
    assert n == 2
    assert calls == [None]
    assert await get_cursor(actions.pool, "source:testsrc") == "2"

    # the objects exist with their facts, graded by the item's evidence class
    row = await actions.pool.fetchrow(
        "SELECT a.value #>> '{}' AS v, a.evidence_class AS ec FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id WHERE o.canonical='cik:1' AND a.name='name'"
    )
    assert row["v"] == "One"
    assert row["ec"] == "authoritative_api"

    # second tick gets the advanced cursor and finds an empty delta
    n2 = await tick(actions, "testsrc", puller)
    assert n2 == 0
    assert calls == [None, "2"]


# --- the subscription evaluator ---------------------------------------------

async def test_evaluator_fires_on_match_quiet_on_noise(actions: Actions) -> None:
    sub = await create_subscription(
        actions.pool, "new SEC companies",
        {"event_types": ["object_created"], "object_type": "Organization",
         "canonical_prefix": "cik:"},
    )

    # a matching object...
    await actions.create_or_find_object("Organization", "cik:777", "edgar")
    # ...and two non-matching ones (noise): wrong type, wrong canonical scheme
    await actions.create_or_find_object("Person", "sec-person:jane", "edgar")
    await actions.create_or_find_object("Organization", "lei:ABCDEF", "gleif")

    fired = await evaluate_subscriptions(actions.pool)
    assert fired == 1

    rows = await actions.pool.fetch("SELECT subscription_id, event_type, matched FROM alerts")
    assert len(rows) == 1
    assert rows[0]["subscription_id"] == sub
    assert rows[0]["matched"]["object_type"] == "Organization"

    # idempotent: nothing new to evaluate -> no re-fire
    assert await evaluate_subscriptions(actions.pool) == 0


async def test_evaluator_matches_property_value(actions: Actions) -> None:
    await create_subscription(
        actions.pool, "neuralink name watch",
        {"event_types": ["property_added"], "property_name": "name",
         "value_contains": "neuralink"},
    )
    org = await actions.create_or_find_object("Organization", "cik:888", "edgar")
    await actions.assert_property(org, "name", "Neuralink Corp.", "edgar", NOW, 0.85)
    other = await actions.create_or_find_object("Organization", "cik:889", "edgar")
    await actions.assert_property(other, "name", "Acme Inc.", "edgar", NOW, 0.85)

    fired = await evaluate_subscriptions(actions.pool)
    assert fired == 1
    matched = await actions.pool.fetchval(
        "SELECT object_id FROM alerts WHERE event_type='property_added'"
    )
    assert matched == org


async def test_evaluator_inactive_subscription_is_silent(actions: Actions) -> None:
    sub = await create_subscription(
        actions.pool, "off", {"object_type": "Organization"}
    )
    await actions.pool.execute("UPDATE subscriptions SET active=false WHERE id=$1", sub)
    await actions.create_or_find_object("Organization", "cik:1", "edgar")
    assert await evaluate_subscriptions(actions.pool) == 0


async def test_evaluator_delivers_to_sink_and_marks_delivered(actions: Actions) -> None:
    await create_subscription(
        actions.pool, "wh", {"object_type": "Organization"}, webhook_url="https://example/hook"
    )
    await actions.create_or_find_object("Organization", "cik:5", "edgar")

    seen: list[Alert] = []

    async def sink(alert: Alert) -> bool:
        seen.append(alert)
        return True  # "delivered"

    fired = await evaluate_subscriptions(actions.pool, sink=sink)
    assert fired == 1
    assert len(seen) == 1 and seen[0].webhook_url == "https://example/hook"
    delivered = await actions.pool.fetchval("SELECT delivered_at FROM alerts")
    assert delivered is not None


async def test_evaluator_sink_failure_keeps_the_alert(actions: Actions) -> None:
    """A side-channel sink failure must never lose the durable alert row."""
    await create_subscription(actions.pool, "boom", {"object_type": "Organization"})
    await actions.create_or_find_object("Organization", "cik:6", "edgar")

    async def broken_sink(alert: Alert) -> bool:
        raise RuntimeError("webhook down")

    fired = await evaluate_subscriptions(actions.pool, sink=broken_sink)
    assert fired == 1
    row = await actions.pool.fetchrow("SELECT delivered_at FROM alerts")
    assert row is not None and row["delivered_at"] is None  # recorded, not delivered


async def test_evaluator_decoupled_from_cascade_published_at(actions: Actions) -> None:
    """The evaluator claims via evaluated_at, never published_at — so draining the
    cascade and evaluating subscriptions are independent passes over one outbox."""
    await create_subscription(actions.pool, "any", {"object_type": "Organization"})
    await actions.create_or_find_object("Organization", "cik:9", "edgar")
    await evaluate_subscriptions(actions.pool)
    # evaluator set evaluated_at but left published_at for the cascade
    row = await actions.pool.fetchrow(
        "SELECT published_at, evaluated_at FROM outbox WHERE event_type='object_created'"
    )
    assert row["evaluated_at"] is not None
    assert row["published_at"] is None

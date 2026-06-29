"""The watch — watermarks, source ticks, and the WATCH evaluator.

Proves the tripwire: a saved watch (a kind='watch' composition with a `select` spec)
fires on a matching new graph mutation and stays QUIET on noise, the cursor primitives
are atomic, and a canned source tick drives the whole chain (pull -> materialize ->
outbox -> alert) with no network. A watch and a lens are ONE primitive now.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from src.actions.core import Actions
from src.orchestrator.compositions import save_watch
from src.orchestrator.monitor import (
    Alert,
    GraphEvent,
    PullResult,
    WatchItem,
    evaluate_watches,
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


# --- one primitive: a watch is also a runnable lens -------------------------

async def test_watch_is_a_runnable_lens(actions: Actions) -> None:
    """The P3 headline: a watch's select spec is ONE primitive. The same saved watch you
    run() on demand (the lens — current members) is what the evaluator matches new objects
    against (the tripwire). Here we prove the lens half: run it and get the members."""
    from src.orchestrator.compositions import run_composition
    await save_watch(actions.pool, "sec orgs", "Organization", [], canonical_prefix="cik:")
    await actions.create_or_find_object("Organization", "cik:1", "edgar")
    await actions.create_or_find_object("Organization", "cik:2", "edgar")
    await actions.create_or_find_object("Organization", "lei:X", "gleif")  # not in the set

    lens = await run_composition(actions.pool, "sec orgs")
    assert lens["kind"] == "objects" and lens["count"] == 2  # exactly the cik: orgs


# --- the watch evaluator ----------------------------------------------------

async def test_evaluator_fires_on_match_quiet_on_noise(actions: Actions) -> None:
    # a watch = select(Organization, canonical scheme cik:) — "new SEC companies"
    wid = await save_watch(actions.pool, "new SEC companies", "Organization", [],
                           canonical_prefix="cik:")

    # a matching object...
    await actions.create_or_find_object("Organization", "cik:777", "edgar")
    # ...and two non-matching ones (noise): wrong type, wrong canonical scheme
    await actions.create_or_find_object("Person", "sec-person:jane", "edgar")
    await actions.create_or_find_object("Organization", "lei:ABCDEF", "gleif")

    fired = await evaluate_watches(actions.pool)
    assert fired == 1

    rows = await actions.pool.fetch("SELECT composition_id, event_type, matched FROM alerts")
    assert len(rows) == 1
    assert rows[0]["composition_id"] == wid
    assert rows[0]["matched"]["object_type"] == "Organization"

    # idempotent: nothing new to evaluate -> no re-fire
    assert await evaluate_watches(actions.pool) == 0


async def test_evaluator_matches_property_value(actions: Actions) -> None:
    # set-entry semantic: a where-clause on a property fires on the NEW object once its
    # facts are committed (the evaluator runs after the tick, so object_created sees them)
    await save_watch(actions.pool, "neuralink name watch", "Organization",
                     [{"property": "name", "op": "contains", "value": "neuralink"}])
    org = await actions.create_or_find_object("Organization", "cik:888", "edgar")
    await actions.assert_property(org, "name", "Neuralink Corp.", "edgar", NOW, 0.85)
    other = await actions.create_or_find_object("Organization", "cik:889", "edgar")
    await actions.assert_property(other, "name", "Acme Inc.", "edgar", NOW, 0.85)

    fired = await evaluate_watches(actions.pool)
    assert fired == 1
    matched = await actions.pool.fetchval(
        "SELECT object_id FROM alerts WHERE event_type='object_created'"
    )
    assert matched == org


async def test_evaluator_inactive_watch_is_silent(actions: Actions) -> None:
    wid = await save_watch(actions.pool, "off", "Organization", [])
    await actions.pool.execute("UPDATE compositions SET active=false WHERE id=$1", wid)
    await actions.create_or_find_object("Organization", "cik:1", "edgar")
    assert await evaluate_watches(actions.pool) == 0


async def test_evaluator_delivers_to_sink_and_marks_delivered(actions: Actions) -> None:
    await save_watch(actions.pool, "wh", "Organization", [],
                     webhook_url="https://example/hook")
    await actions.create_or_find_object("Organization", "cik:5", "edgar")

    seen: list[Alert] = []

    async def sink(alert: Alert) -> bool:
        seen.append(alert)
        return True  # "delivered"

    fired = await evaluate_watches(actions.pool, sink=sink)
    assert fired == 1
    assert len(seen) == 1 and seen[0].webhook_url == "https://example/hook"
    delivered = await actions.pool.fetchval("SELECT delivered_at FROM alerts")
    assert delivered is not None


async def test_evaluator_sink_failure_keeps_the_alert(actions: Actions) -> None:
    """A side-channel sink failure must never lose the durable alert row."""
    await save_watch(actions.pool, "boom", "Organization", [])
    await actions.create_or_find_object("Organization", "cik:6", "edgar")

    async def broken_sink(alert: Alert) -> bool:
        raise RuntimeError("webhook down")

    fired = await evaluate_watches(actions.pool, sink=broken_sink)
    assert fired == 1
    row = await actions.pool.fetchrow("SELECT delivered_at FROM alerts")
    assert row is not None and row["delivered_at"] is None  # recorded, not delivered


async def test_alert_delivery_is_rate_capped(
    actions: Actions, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D1 — the 3am-false-alert guard: a burst writes every durable row, but DELIVERY is
    capped per watch per window. The rows are never lost; only the side-channel is throttled."""
    monkeypatch.setenv("OSIRIS_ALERT_MAX_PER_WINDOW", "2")
    monkeypatch.setenv("OSIRIS_ALERT_COOLDOWN_SECS", "0")  # isolate the rate cap

    await save_watch(actions.pool, "all orgs", "Organization", [])
    for i in range(4):  # a burst of 4 matching new objects
        await actions.create_or_find_object("Organization", f"cik:{i}", "edgar")

    delivered: list[Alert] = []

    async def sink(a: Alert) -> bool:
        delivered.append(a)
        return True

    fired = await evaluate_watches(actions.pool, sink=sink)
    assert fired == 4                 # 4 durable rows written
    assert len(delivered) == 2        # only 2 delivered (the cap) — no 3am flood
    n_rows = await actions.pool.fetchval("SELECT count(*) FROM alerts")
    n_delivered = await actions.pool.fetchval(
        "SELECT count(*) FROM alerts WHERE delivered_at IS NOT NULL")
    assert n_rows == 4 and n_delivered == 2  # every row kept; 2 suppressed, readable at /alerts


async def test_evaluator_decoupled_from_cascade_published_at(actions: Actions) -> None:
    """The evaluator claims via evaluated_at, never published_at — so draining the
    cascade and evaluating watches are independent passes over one outbox."""
    await save_watch(actions.pool, "any", "Organization", [])
    await actions.create_or_find_object("Organization", "cik:9", "edgar")
    await evaluate_watches(actions.pool)
    # evaluator set evaluated_at but left published_at for the cascade
    row = await actions.pool.fetchrow(
        "SELECT published_at, evaluated_at FROM outbox WHERE event_type='object_created'"
    )
    assert row["evaluated_at"] is not None
    assert row["published_at"] is None

"""Concurrent-writer stress — prove the kernel's theory when the FLEET writes at once.

The multi-agent design rests on a claim: the event-sourced, ON-CONFLICT, multi-source
kernel is safe under concurrent writers, so N agents sharing one graph never corrupt it.
These tests hammer the real Postgres with racing writers and assert the invariants hold —
no duplicate objects, no lost writes, every agent's provenance preserved.
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import record_decision

_N = 50


async def test_racing_creators_collapse_to_one_object(actions: Actions) -> None:
    """50 agents create-or-find the SAME canonical at once → exactly ONE object, one
    create event. The ON CONFLICT (type, canonical) DO NOTHING race-safety, proven."""
    ids = await asyncio.gather(*[
        actions.create_or_find_object("Organization", "cik:99999", f"racer-{i}")
        for i in range(_N)
    ])
    assert len(set(ids)) == 1  # no duplicate objects under the race
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Organization' AND canonical='cik:99999'") == 1
    # emit is idempotent: exactly one 'create' event despite 50 racers
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE object_id=$1 AND event_type='create'",
        ids[0]) == 1


async def test_racing_multisource_assertions_none_lost(actions: Actions) -> None:
    """30 distinct agents assert the SAME property on one object concurrently → every
    source's value coexists as the multi-source set; none clobbers another."""
    oid = await actions.create_or_find_object("Organization", "cik:multi", "seed")
    now = datetime.now(UTC)
    await asyncio.gather(*[
        actions.assert_property(oid, "status", f"value-{i}", f"agent:{i:02d}", now, 0.9)
        for i in range(30)
    ])
    distinct = await actions.pool.fetchval(
        "SELECT count(DISTINCT source_id) FROM current_assertions "
        "WHERE object_id=$1 AND name='status'", oid)
    assert distinct == 30  # all 30 agents' values live in the current set


async def test_racing_same_source_supersedes_cleanly(actions: Actions) -> None:
    """The other side: ONE source asserting the same property many times concurrently must
    not leave a tangled supersedes chain — the current set converges to a single live row."""
    oid = await actions.create_or_find_object("Organization", "cik:one-source", "seed")
    await asyncio.gather(*[
        actions.assert_property(oid, "label", f"rev-{i}", "agent:solo",
                                datetime(2026, 7, 3, 0, 0, i % 60, tzinfo=UTC), 0.9)
        for i in range(20)
    ])
    live = await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE object_id=$1 AND name='label' "
        "AND source_id='agent:solo'", oid)
    assert live == 1  # exactly one non-superseded value for that source, no fork


async def test_fleet_consensus_dedups_but_keeps_every_provenance(actions: Actions) -> None:
    """The headline fleet case: 40 agents record the SAME ruling with DISTINCT identities,
    concurrently. It dedups to ONE Decision (canonical), but keeps all 40 provenances —
    visible consensus, no corruption. This is why the shared graph is safe to open up."""
    summary = "the fleet shares one graph, and it holds under load"
    ids = await asyncio.gather(*[
        record_decision(actions, summary, source=f"agent:{i:02d}") for i in range(40)
    ])
    assert len(set(ids)) == 1  # one object despite 40 racing find-or-creates
    distinct = await actions.pool.fetchval(
        "SELECT count(DISTINCT source_id) FROM assertions WHERE object_id=$1 AND name='summary'",
        ids[0])
    assert distinct == 40  # every agent that agreed is recorded — consensus is legible

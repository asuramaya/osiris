"""WAVE 18 ITEM 1 (thread 6d01f21e, Thoth's reversal of his own 2026-08-01 "stand for
now"): `_mint_closed_by`'s two placeholder specimens ('session' module default,
'analyst:operator' REST attribution) now resolve to a real SystemSource / operator
Person object instead of minting a placeholder Agent — this backfill re-points every
closed_by edge still targeting one of the old placeholders and retires them once
edgeless.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import (
    _OPERATOR_PERSON_CANONICAL,
    _SYSTEM_SOURCE_CANONICAL,
    backfill_closed_by_real_sources,
    open_thread,
)


async def _mint_placeholder_closed_by(
    actions: Actions, *, placeholder_canonical: str, edge_source: str,
) -> tuple[object, object]:
    now = datetime.now(UTC)
    placeholder = await actions.create_or_find_object(
        "Agent", placeholder_canonical, placeholder_canonical)
    thread = await open_thread(actions, f"a thread closed under {edge_source!r}",
                               source=edge_source)
    await actions.create_link(thread, placeholder, "closed_by", edge_source, now, 0.6,
                              evidence_class="direct_observation")
    return thread, placeholder


async def test_dry_run_reports_operator_and_system_placeholder_edges_writing_nothing(
    actions: Actions,
) -> None:
    t_operator, _ = await _mint_placeholder_closed_by(
        actions, placeholder_canonical="analyst:operator", edge_source="analyst:operator")
    t_session, _ = await _mint_placeholder_closed_by(
        actions, placeholder_canonical="session", edge_source="session")
    out = await backfill_closed_by_real_sources(actions, actor="test", dry_run=True)
    assert out["scanned"] == 2
    verdicts = {(p["placeholder"], p["source"]): p["to_type"] for p in out["plan"]}
    assert verdicts[("analyst:operator", "analyst:operator")] == "Person"
    assert verdicts[("session", "session")] == "SystemSource"
    # nothing written
    for t in (t_operator, t_session):
        target_type = await actions.pool.fetchval(
            "SELECT o.type FROM links l JOIN objects o ON o.id=l.to_id "
            "WHERE l.from_id=$1 AND l.type='closed_by' "
            "AND (l.valid_until IS NULL OR l.valid_until > now())", t)
        assert target_type == "Agent"


async def test_live_run_repoints_operator_edge_to_the_real_person_and_retires_placeholder(
    actions: Actions,
) -> None:
    t, placeholder = await _mint_placeholder_closed_by(
        actions, placeholder_canonical="analyst:operator", edge_source="analyst:operator")
    old = await actions.pool.fetchrow(
        "SELECT source_id, confidence, evidence_class FROM links "
        "WHERE from_id=$1 AND to_id=$2 AND type='closed_by'", t, placeholder)
    out = await backfill_closed_by_real_sources(
        actions, actor="test", dry_run=False, because="wave 18 item 1 compensating fold")
    assert out["retired"] == ["analyst:operator"]
    row = await actions.pool.fetchrow(
        "SELECT o.type, o.canonical, l.source_id, l.confidence, l.evidence_class "
        "FROM links l JOIN objects o ON o.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='closed_by' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", t)
    assert row["type"] == "Person"
    assert row["canonical"] == _OPERATOR_PERSON_CANONICAL
    # attribution survives the fold — same source_id/confidence/evidence_class as the
    # ORIGINAL edge, never overwritten by the backfill's own actor
    assert row["source_id"] == old["source_id"]
    assert row["confidence"] == old["confidence"]
    assert row["evidence_class"] == old["evidence_class"]
    # the old edge is invalidated, never left dangling alongside the new one
    old_active = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='closed_by' "
        "AND (valid_until IS NULL OR valid_until > now())", t, placeholder)
    assert old_active is None
    status = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE id=$1", placeholder)
    assert status == "retired"


async def test_live_run_repoints_session_edge_to_the_real_system_source(
    actions: Actions,
) -> None:
    t, placeholder = await _mint_placeholder_closed_by(
        actions, placeholder_canonical="session", edge_source="session")
    out = await backfill_closed_by_real_sources(
        actions, actor="test", dry_run=False, because="wave 18 item 1 compensating fold")
    assert out["retired"] == ["session"]
    row = await actions.pool.fetchrow(
        "SELECT o.type, o.canonical FROM links l JOIN objects o ON o.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='closed_by' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", t)
    assert row["type"] == "SystemSource"
    assert row["canonical"] == _SYSTEM_SOURCE_CANONICAL


async def test_placeholder_is_never_retired_while_still_load_bearing(
    actions: Actions,
) -> None:
    """A placeholder carrying edges from TWO different threads keeps existing until
    every edge off it is moved, never retired mid-fold."""
    now = datetime.now(UTC)
    placeholder = await actions.create_or_find_object("Agent", "session", "session")
    t1 = await open_thread(actions, "closed under session, first", source="session")
    t2 = await open_thread(actions, "closed under session, second", source="session")
    await actions.create_link(t1, placeholder, "closed_by", "session", now, 0.6,
                              evidence_class="direct_observation")
    await actions.create_link(t2, placeholder, "closed_by", "session", now, 0.6,
                              evidence_class="direct_observation")
    out = await backfill_closed_by_real_sources(
        actions, actor="test", dry_run=False, because="wave 18 item 1 compensating fold")
    assert set(out["retired"]) == {"session"}
    status = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE id=$1", placeholder)
    assert status == "retired"


async def test_dry_run_false_without_because_refuses(actions: Actions) -> None:
    out = await backfill_closed_by_real_sources(actions, actor="test", dry_run=False)
    assert "error" in out


async def test_idempotent_second_run_finds_nothing_left(actions: Actions) -> None:
    await _mint_placeholder_closed_by(
        actions, placeholder_canonical="session", edge_source="session")
    await backfill_closed_by_real_sources(
        actions, actor="test", dry_run=False, because="first pass")
    out = await backfill_closed_by_real_sources(actions, actor="test", dry_run=True)
    assert out["scanned"] == 0
    assert out["plan"] == []

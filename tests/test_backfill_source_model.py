"""Backfill source_model on historical agent-authored Decisions/Threads (task #21).

The provenance dimension (which Claude wrote it) is recoverable from the authoring agent's
own registration. Offline, idempotent, DERIVED, through the Actions waist. Real Postgres.
"""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from src.actions.core import Actions
from src.ingest.backfill_source_model import backfill_source_model
from src.orchestrator.agents import AgentIdentity, register_agent
from src.orchestrator.capture import open_thread, record_decision
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for


async def _register(actions: Actions, sid: str, model: str | None) -> None:
    """Register a fleet agent as the real path does — Agent object carrying source_model."""
    ident = AgentIdentity(
        agent_id=f"agent:{sid}", session=sid, project="osiris", model=model,
        cwd="/home/x/code/osiris", model_method="job_dir" if model else None,
    )
    await register_agent(actions, ident, actor="asuramaya")


async def _source_model(actions: Actions, oid: object, source: str | None = None) -> object:
    """The source_model assertion on a record (optionally from a specific source)."""
    if source is None:
        return await actions.pool.fetchrow(
            "SELECT value #>> '{}' AS model, source_id, evidence_class, confidence "
            "FROM current_assertions WHERE object_id=$1 AND name='source_model'", oid)
    return await actions.pool.fetchrow(
        "SELECT value #>> '{}' AS model, source_id, evidence_class, confidence "
        "FROM current_assertions WHERE object_id=$1 AND name='source_model' AND source_id=$2",
        oid, source)


async def test_backfill_stamps_missing_source_model_as_derived(actions: Actions) -> None:
    """The target case: an agent captured a Decision + a Thread before model stamping existed,
    so neither carries source_model. The backfill reads the agent's registered model and stamps
    each — DERIVED, attributed to its own source, never UPDATEing a row."""
    await _register(actions, "sess1", "claude-fable-5")
    d = await record_decision(actions, "the composer is a page of compositions, never a coded "
                              "page", kind="ruling", source="agent:sess1")
    t = await open_thread(actions, "wire the reordered-token recall into orient",
                          source="agent:sess1")

    res = await backfill_source_model(actions)
    assert res == {"candidates": 2, "stamped": 2, "skipped_no_model": 0}

    for oid in (d, t):
        row = await _source_model(actions, oid)
        assert row["model"] == "claude-fable-5"                 # the agent's registered model
        assert row["source_id"] == "source-model-backfill"      # attributed to the backfill
        assert row["evidence_class"] == "derived"               # an inference, never the harness
        # PG stores confidence as float4 (real) — approximate, so compare with tolerance
        assert float(row["confidence"]) == pytest.approx(confidence_for(EvidenceClass.DERIVED))

    # idempotent: a re-run finds no candidates and stamps nothing (the dimension is now present)
    again = await backfill_source_model(actions)
    assert again == {"candidates": 0, "stamped": 0, "skipped_no_model": 0}


async def test_backfill_skips_records_that_already_carry_source_model(actions: Actions) -> None:
    """A record whose source_model was stamped at write time (a real, higher-grade observation)
    is not a candidate — the dimension is present, and a DERIVED backfill must not pile on."""
    await _register(actions, "sess2", "claude-fable-5")
    d = await record_decision(actions, "an already-stamped decision", source="agent:sess2")
    # the agent stamped its own model at write time (DIRECT_OBSERVATION, higher grade)
    await actions.assert_property(
        d, "source_model", "claude-opus-4-8", "agent:sess2", datetime.now(UTC),
        confidence_for(EvidenceClass.DIRECT_OBSERVATION),
        evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)

    res = await backfill_source_model(actions)
    assert res["stamped"] == 0 and res["candidates"] == 0

    assert await _source_model(actions, d, "source-model-backfill") is None  # never touched
    kept = await _source_model(actions, d)
    assert kept["model"] == "claude-opus-4-8" and kept["source_id"] == "agent:sess2"


async def test_backfill_only_touches_agent_authored_records(actions: Actions) -> None:
    """The scope guard: a lone-operator `session` decision is not agent-authored (no agent to
    infer from), and an agent whose registration carries no model can't be inferred — both are
    left unstamped, the latter counted as skipped_no_model."""
    # a lone-operator decision (source='session', not an agent)
    lone = await record_decision(actions, "a lone-operator ruling", source="session")
    # an agent that authored work but whose model was never resolved (registered with model=None)
    await _register(actions, "nomodel", None)
    unmodeled = await record_decision(actions, "authored by an unmodeled agent",
                                      source="agent:nomodel")

    res = await backfill_source_model(actions)
    assert res["stamped"] == 0
    assert res["candidates"] == 1 and res["skipped_no_model"] == 1  # only the agent record

    for oid in (lone, unmodeled):
        cnt = await actions.pool.fetchval(
            "SELECT count(*) FROM assertions WHERE object_id=$1 AND name='source_model'", oid)
        assert cnt == 0

"""PROVENANCE PIECE 3(c), THE GROUNDS-LAW MEASURE: MEASURE ONLY, no
enforcement. `grounds_law_measure` counts, over the
last N days of fact writes, how many would be refused by a hypothetical grounds law: a write
carrying NONE of: an observation act (backed_by_observation on the writer's own Agent),
grounds (grounded_by link), cites, refs (a link to a Reference object), or a read-set entry
(session_reads before the write), broken down by writer and by channel (source_id prefix,
the closest per-write signal to "door" that actually exists; see the thread's own scope note).
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.orchestrator.compositions import grounds_law_measure
from src.orchestrator.provenance import stamp_read
from src.parsers.base import EvidenceClass

NOW = datetime(2026, 9, 14, tzinfo=UTC)


async def test_a_bare_write_with_none_of_the_five_signals_is_refused(actions: Actions) -> None:
    o = await actions.create_or_find_object("SoftwareProject", "repo:glm-bare", "test")
    await actions.assert_property(o, "status", "green", "agent:bare-writer", NOW, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)

    out = await grounds_law_measure(actions.pool, days=30)

    assert out["by_writer"]["agent:bare-writer"] == {"total": 1, "refused": 1}
    assert out["by_channel"]["agent"]["refused"] >= 1


async def test_a_writer_with_backed_by_observation_is_not_refused(actions: Actions) -> None:
    a = await actions.create_or_find_object("Agent", "agent:looked", "fleet-observer")
    await actions.assert_property(a, "backed_by_observation", True, "fleet-observer", NOW,
                                  0.6, evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)
    o = await actions.create_or_find_object("SoftwareProject", "repo:glm-looked", "test")
    await actions.assert_property(o, "status", "green", "agent:looked", NOW, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)

    out = await grounds_law_measure(actions.pool, days=30)

    assert out["by_writer"]["agent:looked"] == {"total": 1, "refused": 0}


async def test_a_grounded_by_link_is_not_refused(actions: Actions) -> None:
    d = await actions.create_or_find_object("Decision", "decision:glm-grounded", "test")
    ref = await actions.create_or_find_object("Reference", "ref:glm-canon", "test")
    await actions.assert_property(d, "summary", "a ruling", "agent:grounder", NOW, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    await actions.create_link(d, ref, "grounded_by", "agent:grounder", NOW, 0.9,
                              evidence_class=EvidenceClass.SELF_DECLARED.value)

    out = await grounds_law_measure(actions.pool, days=30)

    assert out["by_writer"]["agent:grounder"] == {"total": 1, "refused": 0}


async def test_a_cites_link_is_not_refused(actions: Actions) -> None:
    t = await actions.create_or_find_object("Thread", "thread:glm-cites", "test")
    ref = await actions.create_or_find_object("Reference", "ref:glm-cited", "test")
    await actions.assert_property(t, "summary", "an open question", "agent:citer", NOW, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    await actions.create_link(t, ref, "cites", "agent:citer", NOW, 0.9,
                              evidence_class=EvidenceClass.SELF_DECLARED.value)

    out = await grounds_law_measure(actions.pool, days=30)

    assert out["by_writer"]["agent:citer"] == {"total": 1, "refused": 0}


async def test_a_prior_read_is_not_refused(actions: Actions) -> None:
    o = await actions.create_or_find_object("SoftwareProject", "repo:glm-read", "test")
    await stamp_read(actions.pool, agent_id="agent:reader", door="dossier", object_id=o,
                     read_at=datetime.now(UTC) - timedelta(minutes=5))
    await actions.assert_property(o, "status", "green", "agent:reader", NOW, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)

    out = await grounds_law_measure(actions.pool, days=30)

    assert out["by_writer"]["agent:reader"] == {"total": 1, "refused": 0}


async def test_a_read_after_the_write_does_not_count(actions: Actions) -> None:
    o = await actions.create_or_find_object("SoftwareProject", "repo:glm-read-late", "test")
    await actions.assert_property(o, "status", "green", "agent:late-reader", NOW, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    await stamp_read(actions.pool, agent_id="agent:late-reader", door="dossier", object_id=o,
                     read_at=datetime.now(UTC) + timedelta(minutes=5))

    out = await grounds_law_measure(actions.pool, days=30)

    assert out["by_writer"]["agent:late-reader"] == {"total": 1, "refused": 1}


async def test_by_writer_and_by_channel_breakdown(actions: Actions) -> None:
    o = await actions.create_or_find_object("SoftwareProject", "repo:glm-breakdown", "test")
    await actions.assert_property(o, "a", "1", "agent:bd1", NOW, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    await actions.assert_property(o, "b", "2", "miner:bd2", NOW, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)

    out = await grounds_law_measure(actions.pool, days=30)

    assert out["by_writer"]["agent:bd1"]["total"] == 1
    assert out["by_writer"]["miner:bd2"]["total"] == 1
    assert out["by_channel"]["agent"]["total"] >= 1
    assert out["by_channel"]["miner"]["total"] == 1
    assert out["total"] >= 2
    assert out["refused"] >= 2


async def test_writes_outside_the_window_are_excluded(actions: Actions) -> None:
    # created_at is stamped at INSERT time (append-only kernel, never rewritten after the
    # fact), so the window boundary is exercised via `days` rather than backdating a row:
    # days=-1 puts the cutoff a day in the FUTURE, which every real write necessarily
    # predates, the same population-scoping code path a genuinely-30-days-old write would
    # hit, without fighting the event-sourced kernel's own append-only invariant.
    o = await actions.create_or_find_object("SoftwareProject", "repo:glm-old", "test")
    await actions.assert_property(o, "status", "green", "agent:old-writer", NOW, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)

    out = await grounds_law_measure(actions.pool, days=-1)

    assert "agent:old-writer" not in out["by_writer"]


async def test_empty_window_returns_zeroed_shape(actions: Actions) -> None:
    # days=-1: no write (including the per-worker type-catalog seed) can satisfy a cutoff
    # a day in the future.
    out = await grounds_law_measure(actions.pool, days=-1)

    assert out == {"total": 0, "refused": 0, "by_writer": {}, "by_channel": {}, "days": -1}

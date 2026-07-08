"""search v2 — the FTS engine (rung 1, thread 0deaec4f).

The old search read only the `name` property; these tests prove the rooms are searchable
(summaries/rationales), that the ranking inherits the evidence ladder, that every hit
carries testimony, and that the misses log records what the graph failed to answer.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.compositions import run_spec
from src.parsers.base import EvidenceClass

NOW = datetime(2026, 7, 7, tzinfo=UTC)


async def _search(actions: Actions, q: str, caller: str | None = None) -> dict:
    spec = {"op": "function", "name": "search", "args": {"q": q, "caller": caller}}
    out = await run_spec(actions.pool, spec, None, name="search")
    return out["items"]  # the composition envelope wraps the function's return


async def _decision(actions: Actions, canonical: str, summary: str, source: str,
                    ec: str, conf: float = 0.9) -> None:
    d = await actions.create_or_find_object("Decision", canonical, source)
    await actions.assert_property(d, "summary", summary, source, NOW, conf, evidence_class=ec)


async def test_search_reads_the_rooms_not_the_door_plaques(actions: Actions) -> None:
    """A decision whose SUMMARY says 'credence clamp' must be findable by those words —
    the exact miss the old name-only search shipped with."""
    await _decision(actions, "decision:clamp", "the credence clamp ships tonight",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    out = await _search(actions, "credence clamp")
    assert len(out["hits"]) == 1
    h = out["hits"][0]
    assert h["canonical"] == "decision:clamp" and h["field"] == "summary"
    # testimony: source, grade, when, snippet — the hit says WHY it surfaced
    assert h["source"] == "agent:a" and h["grade"] == "self_declared"
    assert "clamp" in h["snippet"].lower() and h["when"].startswith("2026-07")


async def test_ranking_inherits_the_evidence_ladder(actions: Actions) -> None:
    """Equal textual relevance → the deliberate ruling outranks the mined echo."""
    await _decision(actions, "decision:ruled", "wake economics uses haiku triage",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    await _decision(actions, "decision:mined", "wake economics uses haiku triage",
                    "agent:b", EvidenceClass.DERIVED.value, conf=0.4)
    out = await _search(actions, "wake economics haiku")
    assert [h["canonical"] for h in out["hits"]] == ["decision:ruled", "decision:mined"]
    assert out["hits"][0]["rank"] > out["hits"][1]["rank"]


async def test_stemming_and_phrases_work(actions: Actions) -> None:
    await _decision(actions, "decision:stem", "successor identities are minted with lineage",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    assert len((await _search(actions, "minting successors"))["hits"]) == 1  # stems match
    assert len((await _search(actions, '"minted with lineage"'))["hits"]) == 1  # phrase
    assert (await _search(actions, '"lineage with minted"'))["hits"] == []  # order matters


async def test_the_misses_log_records_recall_failures(actions: Actions) -> None:
    """Every call logs; the zero-hit rate is the embeddings tripwire — measured, not vibed."""
    await _decision(actions, "decision:x", "the membrane holds",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    await _search(actions, "membrane", caller="agent:tester")
    await _search(actions, "quantum bagels", caller="agent:tester")
    rows = await actions.pool.fetch("SELECT query, caller, hits FROM search_log ORDER BY id")
    assert [(r["query"], r["hits"]) for r in rows] == [("membrane", 1), ("quantum bagels", 0)]
    assert all(r["caller"] == "agent:tester" for r in rows)
    # and the digest surfaces the telemetry
    from datetime import timedelta

    from src.orchestrator.digest import fleet_digest
    dg = await fleet_digest(actions, since=NOW - timedelta(days=1))
    assert dg["retrieval"]["queries"] == 2 and dg["retrieval"]["zero_hits"] == 1
    assert dg["retrieval"]["top_missed"][0]["query"] == "quantum bagels"


async def test_one_row_per_object_best_witness(actions: Actions) -> None:
    """Two sources co-asserting one decision's summary → ONE hit (the multi-source set must
    not double-list), carrying the better witness."""
    await _decision(actions, "decision:co", "adopt the desk fold",
                    "agent:weak", EvidenceClass.DERIVED.value, conf=0.4)
    await _decision(actions, "decision:co", "adopt the desk fold",
                    "agent:strong", EvidenceClass.SELF_DECLARED.value)
    out = await _search(actions, "desk fold")
    assert len(out["hits"]) == 1
    assert out["hits"][0]["source"] == "agent:strong"

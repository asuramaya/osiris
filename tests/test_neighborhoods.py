"""Rung 4 — neighborhood consolidation (ruling a0cfcca1).

The mechanical pass folds echoes (delegates to consolidate_memory — its own tests hold
the merge law); these tests hold the SUMMARY pass: one Reference per active repo
neighborhood, fingerprint-watermarked so an unchanged neighborhood never costs an LLM
call, refreshed when the neighborhood moves, metered in llm_usage.
"""
from __future__ import annotations

from src.actions.core import Actions
from src.ingest.providers import Usage
from src.orchestrator.capture import open_thread, record_decision
from src.orchestrator.neighborhoods import consolidate_pass, summarize_neighborhoods


class FakeLLM:
    def __init__(self) -> None:
        self.calls = 0

    async def complete(self, *, system: str, prompt: str, model: str,
                       max_tokens: int = 2048,
                       usage_out: list[Usage] | None = None) -> str:
        self.calls += 1
        assert "Project: demoproj" in prompt  # the neighborhood names itself
        if usage_out is not None:
            usage_out.append(Usage(model=model, input_tokens=100, output_tokens=50))
        return f"Digest v{self.calls}: the demoproj neighborhood is mid-arc."


async def _seed(actions: Actions) -> None:
    await open_thread(actions, "wire the composer renderer to the schema", repo="demoproj")
    await open_thread(actions, "the satellite dispatcher needs a vantage", repo="demoproj")
    await record_decision(actions, "the renderer dispatches on result shape",
                          kind="ruling", repo="demoproj")


async def _ref(actions: Actions) -> dict | None:
    row = await actions.pool.fetchrow(
        "SELECT o.id, "
        " (SELECT value #>> '{}' FROM current_assertions WHERE object_id=o.id "
        "   AND name='body' ORDER BY confidence DESC, observed_at DESC LIMIT 1) AS body, "
        " (SELECT value #>> '{}' FROM current_assertions WHERE object_id=o.id "
        "   AND name='watermark' "
        "   ORDER BY confidence DESC, observed_at DESC LIMIT 1) AS watermark "
        "FROM objects o WHERE o.canonical='ref:neighborhood-demoproj'")
    return dict(row) if row else None


async def test_summary_is_watermarked_incremental_and_metered(actions: Actions) -> None:
    await _seed(actions)
    llm = FakeLLM()
    out = await summarize_neighborhoods(actions, llm)
    assert out["summarized"] == 1 and llm.calls == 1
    ref = await _ref(actions)
    assert ref is not None and "Digest v1" in ref["body"] and ref["watermark"]
    # the cost stream saw the call
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM llm_usage WHERE purpose='neighborhood-summary'") == 1
    # unchanged neighborhood → fingerprint matches → free skip, no second call
    out2 = await summarize_neighborhoods(actions, llm)
    assert out2["summarized"] == 0 and out2["skipped"] >= 1 and llm.calls == 1
    # the neighborhood MOVES → the fingerprint moves → the digest refreshes in place
    await open_thread(actions, "the delivery sink needs dedup before alerts flow",
                      repo="demoproj")
    out3 = await summarize_neighborhoods(actions, llm)
    assert out3["summarized"] == 1 and llm.calls == 2
    ref2 = await _ref(actions)
    assert ref2 is not None and "Digest v2" in ref2["body"]
    assert ref2["id"] == ref["id"]  # refreshed, never twinned
    assert ref2["watermark"] != ref["watermark"]


async def test_thin_neighborhoods_are_not_summarized(actions: Actions) -> None:
    """Two members is a hallway, not a neighborhood (the >=3 floor): no LLM spend on
    repos with nothing to consolidate."""
    await open_thread(actions, "a single lonely thread", repo="tinyproj")
    await record_decision(actions, "a single lonely ruling", repo="tinyproj")
    llm = FakeLLM()
    out = await summarize_neighborhoods(actions, llm)
    assert out == {"candidates": 0, "summarized": 0, "skipped": 0} and llm.calls == 0


async def test_consolidate_pass_walks_both_memory_types(actions: Actions) -> None:
    """The mechanical motion delegates to consolidate_memory for Threads AND Decisions —
    the counters come back namespaced per type."""
    out = await consolidate_pass(actions)
    assert set(out) == {"threads_merged", "threads_for_review",
                        "decisions_merged", "decisions_for_review"}

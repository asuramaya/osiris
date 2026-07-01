"""Decision mining — the project's WHY, over its own self-tracking.

The least-recoverable memory: code shows WHAT, the DAG shows WHEN, only the commit rationale
holds WHY. This mines the backward-looking decisions into `Decision` objects (decided_in the
commit, graded DERIVED), so "why is it this way?" becomes a query. The harder links
(supersedes / grounded_by-the-canon) are a judgment left to the AI path.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.ingest.decisions import extract_decisions, mine_decisions
from src.ontology.schema import LINK_TYPES, OBJECT_TYPES
from src.orchestrator.compositions import (
    list_functions,
    run_composition,
    run_spec,
    seed_default_compositions,
)

NOW = datetime(2026, 6, 28, tzinfo=UTC)


def test_schema_declares_decision() -> None:
    assert "Decision" in OBJECT_TYPES
    for lt in ("decided_in", "supersedes", "grounded_by"):
        assert lt in LINK_TYPES


def test_extract_decisions_is_tight() -> None:
    """The markers catch author-intended decisions + their kind; generic prose does not."""
    body = (
        "feat(x): do the thing\n\n"
        "RESET — engine is the product, verticals are frontends.\n"
        "We chose to event-source merges (object_events is truth).\n"
        "Deliberately NOT a generic join — set algebra or a Function.\n"
        "This wires the renderer and adds a test."          # generic → not a decision
    )
    got = extract_decisions(body)
    kinds = {k for _, k in got}
    statements = " ".join(s for s, _ in got)
    assert "reset" in kinds and "choice" in kinds and "rejection" in kinds
    assert "generic join" in statements
    assert "wires the renderer" not in statements          # generic engineering prose excluded
    # a sentence ABOUT the decision-miner is META, not a decision
    assert extract_decisions("This commit builds the decision miner and its records.") == []


async def _commit(actions: Actions, canon: str, rationale: str, date: str) -> None:
    cm = await actions.create_or_find_object("Commit", canon, "git")
    await actions.assert_property(cm, "rationale", rationale, "git", NOW, 0.85)
    await actions.assert_property(cm, "authored_date", date, "git", NOW, 0.85)


async def test_mine_decisions_links_and_dedups(actions: Actions) -> None:
    await _commit(actions, "commit:a",
                  "RESET — the engine is the product.\nWe chose to keep the set (multi-source).",
                  "2026-06-24T00:00:00+00:00")
    await _commit(actions, "commit:b",
                  "Deliberately NOT auto-merging Person (ruling #3).", "2026-06-26T00:00:00+00:00")

    res = await mine_decisions(actions)
    assert res["decisions"] == 3 and res["commits_scanned"] == 2
    p = actions.pool
    # each Decision is decided_in its commit, graded DERIVED, carries a kind
    n = await p.fetchval("SELECT count(*) FROM links WHERE type='decided_in'")
    assert n == 3
    ec = await p.fetchval("SELECT evidence_class FROM links WHERE type='decided_in' LIMIT 1")
    assert ec == "derived"                                  # a mined decision is an inference
    kinds = {r["k"] for r in await p.fetch(
        "SELECT value #>> '{}' AS k FROM current_assertions WHERE name='kind'")}
    assert {"reset", "choice", "ruling"} <= kinds
    # idempotent — a re-run mints no new Decisions or edges (content-hash canonical)
    again = await mine_decisions(actions)
    assert again["decisions"] == 3
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='Decision'") == 3
    assert await p.fetchval("SELECT count(*) FROM links WHERE type='decided_in'") == 3


async def test_decision_log_is_a_sections_op_tree(actions: Actions) -> None:
    """The eviction proof: `decision-log` is no longer a SQL Function — it's a `sections` op-tree
    (select Decision → table with `of:"first"` show-original rollups plucking the decided_in
    commit's hash + date → order). Same WHY-log, newest first; the show-original rollup names the
    commit. A kind filter is a `where`, not a bespoke arg."""
    await _commit(actions, "commit:a", "We chose to event-source merges.",
                  "2026-06-20T00:00:00+00:00")
    await _commit(actions, "commit:b", "RESET — engine is the product.",
                  "2026-06-24T00:00:00+00:00")
    await mine_decisions(actions)

    await seed_default_compositions(actions.pool)
    assert "decisions" not in list_functions()                   # the hand-written Function is gone
    res = await run_composition(actions.pool, "decision-log")
    assert res["kind"] == "data"
    log = next(iter(res["items"].values()))
    assert len(log) == 2 and log[0]["when"][:10] == "2026-06-24"  # newest first (full ISO)
    assert log[0]["kind"] == "reset" and log[0]["in"] == "commit:b"   # show-original: the hash
    # a kind filter is now just a `where` on the select the user forks in — not a Function arg
    choices = await run_spec(actions.pool, {"op": "table",
        "from": {"op": "select", "object_type": "Decision",
                 "where": [{"property": "kind", "op": "eq", "value": "choice"}]},
        "columns": [{"name": "kind", "property": "kind"}]}, None)
    assert len(choices["items"]) == 1 and choices["items"][0]["kind"] == "choice"

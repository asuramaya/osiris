"""ANSWERS EDGE MINTED BY REAL RESOLVE (2026-09-14): the hygiene nudge's
"already answered by N decision(s)" used to be tiered by pg_trgm text similarity,
measured dishonest (93% of true answer edges score under 0.4).
Fixed in two parts, each covered here:

(1) `obligation_hygiene._quote_summary` reads the `answered_by` list alone, present or
    absent, never a text-scored maybe; the "possibly answered" tier is gone.
(2) `capture.thread_answering_decisions` (hygiene's own read, and recall's
    `bears_on_from`) now UNIONs live `answers` edges with live `resolved_by` edges whose
    target is a Decision, a thread closed via `resolve_thread(artifact=<decision>)` used
    to be invisible here, only `resolve_thread(...)`-then-`record_decision(resolves=)`
    ever counted. Read-side widening, not a second minting path: `resolved_by` already
    exists for every such closure, live and historical, so no backfill is needed and
    `thread_closure_status`'s own separate mutual-exclusivity assumption between the two
    edge types (0055) is untouched, verified below."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.capture import (
    open_thread,
    record_decision,
    resolve_thread,
    thread_answering_decisions,
)
from src.orchestrator.obligation_hygiene import _quote_summary

# --- (1) obligation_hygiene._quote_summary reads the list alone, no tier --------------

def test_quote_summary_says_already_answered_plainly_when_an_edge_exists() -> None:
    item = {"summary": "a stale obligation", "summary_age_days": 9, "contested": False,
            "answered_by": [{"id": "abc12345", "summary": "the actual fix"}]}
    out = _quote_summary(item)
    assert "ALREADY ANSWERED by 1 decision(s): abc12345" in out
    assert "possibly answered" not in out


def test_quote_summary_says_no_recorded_answer_when_empty() -> None:
    item = {"summary": "a stale obligation", "summary_age_days": 9, "contested": False,
            "answered_by": []}
    out = _quote_summary(item)
    assert "no recorded answer" in out
    assert "ANSWERED" not in out


# --- (2) thread_answering_decisions sees a resolve_thread(artifact=<decision>) closure -

async def test_thread_answering_decisions_sees_an_answers_edge(actions: Actions) -> None:
    t = await open_thread(actions, "settled by a resolves= decision")
    d = await record_decision(actions, "settles it", resolves=str(t))
    out = await thread_answering_decisions(actions.pool, [t])
    assert out[t] == [{"id": str(d)[:8], "summary": "settles it"}]


async def test_thread_answering_decisions_sees_a_resolved_by_closure_too(
    actions: Actions,
) -> None:
    """The gap this fix closes: closing via `resolve_thread(artifact=<decision>)` alone
    (no `record_decision(resolves=)` in the mix) used to be invisible to this read."""
    t = await open_thread(actions, "closed by naming a decision as the artifact")
    d = await record_decision(actions, "the decision that closed it", kind="decision")
    closed = await resolve_thread(actions, str(t), because="built", artifact=str(d)[:8])
    assert closed == t
    # resolve_thread mints ONLY resolved_by for this door, confirms the read, not the
    # write, is what changed.
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='answers'", d, t) is None
    out = await thread_answering_decisions(actions.pool, [t])
    assert out[t] == [{"id": str(d)[:8], "summary": "the decision that closed it"}]


async def test_thread_answering_decisions_never_double_counts_both_edge_types(
    actions: Actions,
) -> None:
    """A thread that carries BOTH an `answers` edge and a `resolved_by` edge to the SAME
    decision (e.g. resolved twice, once each way) is named once, not twice."""
    t = await open_thread(actions, "closed twice by the same decision")
    d = await record_decision(actions, "the one decision", resolves=str(t))
    await resolve_thread(actions, str(t), because="also named as artifact",
                         artifact=str(d)[:8])
    out = await thread_answering_decisions(actions.pool, [t])
    assert len(out[t]) == 1
    assert out[t][0]["id"] == str(d)[:8]


async def test_thread_answering_decisions_a_non_decision_artifact_names_nothing(
    actions: Actions,
) -> None:
    t = await open_thread(actions, "closed by a commit, not a decision")
    await resolve_thread(actions, str(t), artifact="src/widget/voice.py:42")
    out = await thread_answering_decisions(actions.pool, [t])
    assert t not in out


# --- thread_closure_status's own edge-type invariants stay untouched ------------------

async def test_closure_status_still_treats_answers_and_resolved_by_as_distinct_witnesses(
    actions: Actions,
) -> None:
    """Locks in that this fix touched ONLY the read side of `answers`/`resolved_by`, the
    closure view's own separate query (thread_closure.py) still sees two witnesses when
    two DIFFERENT decisions close a thread through the two different doors, unaffected."""
    from src.orchestrator.thread_closure import thread_closure_status

    t = await open_thread(actions, "double-closed for the closure-status check")
    d1 = await record_decision(actions, "settles it via resolves=", resolves=str(t))
    d2 = await record_decision(actions, "a second, unrelated ruling")
    await resolve_thread(actions, str(t), artifact=str(d2)[:8])

    rows = await thread_closure_status(actions.pool, thread_ids=[t])
    row = rows[0]
    assert row["strength"] == "strong"
    assert len(row["closure_edges"]) == 2
    closers = {e["closer_id"] for e in row["closure_edges"]}
    assert closers == {d1, d2}

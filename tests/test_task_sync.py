"""The harness tasklist reconciled against the graph, Phase 1 report-only (Thoth DM 2636,
decisions ab27af61/42f63782). No writes anywhere in this module or these tests — every
assertion here is about what gets REPORTED, never about a graph mutation."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import open_thread
from src.orchestrator.task_sync import (
    parse_thread_citations,
    reconcile,
    resolve_task_citations,
)

NOW = datetime(2026, 8, 1, tzinfo=UTC)


def _task(id_: str, description: str = "", status: str = "pending") -> dict:
    return {"id": id_, "subject": "x", "description": description, "status": status}


# ── parse_thread_citations (pure) ──────────────────────────────────────────────────────

def test_parse_finds_a_single_thread_citation() -> None:
    assert parse_thread_citations(
        "Graph thread 5da19aa6, ruling 10f4058b.") == ["5da19aa6"]


def test_parse_finds_a_comma_separated_list() -> None:
    got = parse_thread_citations(
        "Graph threads: a94935ad, f01b3fcc, 588148bb, 00f6a18d.")
    assert got == ["a94935ad", "f01b3fcc", "588148bb", "00f6a18d"]


def test_parse_ignores_a_mail_thread_number() -> None:
    # "thread 1843" here names a MAIL thread (a small decimal id), not a graph Thread —
    # the 8-hex-char shape requirement excludes it on its own, no special-casing needed.
    assert parse_thread_citations(
        "Thoth DM 1843 (thread 1843): root-cause the fork-inbox bug.") == []


def test_parse_ignores_unrelated_hex_looking_tokens_far_from_the_keyword() -> None:
    assert parse_thread_citations(
        "commit d61c78e landed the fix, no thread here at all") == []


def test_parse_returns_empty_for_no_description() -> None:
    assert parse_thread_citations("") == []
    assert parse_thread_citations(None) == []  # type: ignore[arg-type]


def test_parse_dedupes_and_preserves_first_seen_order() -> None:
    got = parse_thread_citations("thread 5da19aa6 ... later, thread 5da19aa6 again")
    assert got == ["5da19aa6"]


# ── resolve_task_citations ─────────────────────────────────────────────────────────────

async def test_uncited_task_has_no_citations(actions: Actions) -> None:
    row = await resolve_task_citations(actions.pool, _task("1", "no reference at all"))
    assert row == {"task_id": "1", "bucket": "uncited"}


async def test_a_clean_citation_binds(actions: Actions) -> None:
    tid = await open_thread(actions, "the thing this task is about", source="agent:me")
    row = await resolve_task_citations(
        actions.pool, _task("2", f"Graph thread {str(tid)[:8]}."))
    assert row["bucket"] == "bound"
    assert row["thread_ids"] == [str(tid)]


async def test_a_citation_to_nothing_is_unresolvable_named_by_string(
    actions: Actions,
) -> None:
    row = await resolve_task_citations(actions.pool, _task("3", "thread ffffffff done"))
    assert row["bucket"] == "cited_unresolvable"
    assert row["failed"] == [{"citation": "ffffffff", "why": "no Thread matches"}]


async def test_an_ambiguous_citation_is_unresolvable_never_a_guess(
    actions: Actions,
) -> None:
    shared = "deadbeef"
    id_a = uuid.UUID(f"{shared}-0000-4000-8000-000000000001")
    id_b = uuid.UUID(f"{shared}-0000-4000-8000-000000000002")
    for oid, summary in ((id_a, "collision A"), (id_b, "collision B")):
        await actions.pool.execute(
            "INSERT INTO objects (id, type, canonical, status) VALUES ($1, 'Thread', $2, "
            "'active')", oid, f"thread:manual-{oid}")
        await actions.assert_property(oid, "summary", summary, "test", NOW, 0.9,
                                      evidence_class="self_declared")
    row = await resolve_task_citations(actions.pool, _task("4", f"thread {shared} done"))
    assert row["bucket"] == "cited_unresolvable"
    assert row["failed"][0]["citation"] == shared
    assert "matches 2 Thread" in row["failed"][0]["why"]


async def test_a_mix_of_resolving_and_failing_citations_is_bound_partial(
    actions: Actions,
) -> None:
    tid = await open_thread(actions, "the real one", source="agent:me")
    row = await resolve_task_citations(
        actions.pool, _task("5", f"threads: {str(tid)[:8]}, ffffffff"))
    assert row["bucket"] == "bound_partial"
    assert row["thread_ids"] == [str(tid)]
    assert row["failed"] == [{"citation": "ffffffff", "why": "no Thread matches"}]


# ── reconcile (the full report) ────────────────────────────────────────────────────────

async def test_reconcile_reports_all_six_buckets_as_named_rows_not_bare_counts(
    actions: Actions,
) -> None:
    tid = await open_thread(actions, "cleanly bound work", kind="task", source="agent:me")
    tasks = [
        _task("1", f"Graph thread {str(tid)[:8]}.", status="pending"),
        _task("2", "no citation here at all", status="pending"),
        _task("3", "thread ffffffff nowhere", status="pending"),
    ]
    out = await reconcile(actions.pool, tasks)
    assert [r["task_id"] for r in out["bound"]] == ["1"]
    assert [r["task_id"] for r in out["uncited"]] == ["2"]
    assert [r["task_id"] for r in out["cited_unresolvable"]] == ["3"]
    assert out["counts"] == {
        "bound": 1, "bound_partial": 0, "cited_unresolvable": 1, "uncited": 1,
        "disagreement": 0, "thread_side_orphans": 0,
    }


async def test_reconcile_flags_a_disagreement_never_silently_resolving_it(
    actions: Actions,
) -> None:
    # task says done, thread's own property_status still says open — a real disagreement
    tid = await open_thread(actions, "task says done, thread says open", source="agent:me")
    tasks = [_task("1", f"thread {str(tid)[:8]}", status="completed")]
    out = await reconcile(actions.pool, tasks)
    assert out["counts"]["disagreement"] == 1
    d = out["disagreement"][0]
    assert d["task_id"] == "1" and d["thread_id"] == str(tid)
    assert d["task_status"] == "completed" and d["thread_property_status"] == "open"


async def test_reconcile_finds_thread_side_orphans_kind_task_with_no_binding(
    actions: Actions,
) -> None:
    orphan = await open_thread(actions, "kind=task but no harness task ever cited it",
                               kind="task", source="agent:me")
    out = await reconcile(actions.pool, [_task("1", "unrelated", status="pending")])
    assert out["counts"]["thread_side_orphans"] == 1
    assert out["thread_side_orphans"][0]["thread_id"] == str(orphan)


async def test_reconcile_sees_threads_with_no_in_repo_edge_the_trap_named_in_the_module(
    actions: Actions,
) -> None:
    """The in_repo trap (Thoth DM 2636, Khnum's and Seshat's independent findings): a
    repo-scoped enumeration would silently exclude this thread entirely. reconcile must
    still bind and report it, proving it walks enumerate_threads UNSCOPED."""
    tid = await open_thread(actions, "no repo link at all", source="agent:me")  # no repo=
    tasks = [_task("1", f"thread {str(tid)[:8]}", status="pending")]
    out = await reconcile(actions.pool, tasks)
    assert [r["task_id"] for r in out["bound"]] == ["1"]
    assert out["bound"][0]["thread_ids"] == [str(tid)]

"""The harness tasklist reconciled against the graph. Phase 1 (Thoth DM 2636, decisions
ab27af61/42f63782) is report-only — reconcile()/resolve_task_citations()/
parse_thread_citations() never write. The write half (Thoth DM 2722, authorizing the
three-tier design of DM 2687) is additive/visibility-only by construction: Tier 1 asserts
correlation facts, never a status; Tier 2 mints review threads, never resolves anything.
Tier 3 (no status sync, no auto-close, no invented bindings, no touching ~/.claude/tasks)
needs no test — it is the shape of what Tier 1/2 deliberately do not do."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import open_thread
from src.orchestrator.task_sync import (
    archive_eligible_targets,
    mint_tier2_threads,
    parse_thread_citations,
    reconcile,
    resolve_task_citations,
    tier1_targets,
    tier2_mints,
    write_tier1_correlations,
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


async def test_reconcile_disagreement_is_scoped_by_store_not_bare_task_id(
    actions: Actions,
) -> None:
    """The exact collision this module's own docstring names as its reason to exist: task
    id "1" in TWO different stores, each bound to its OWN thread. Store A's task genuinely
    agrees with its thread (both open); store B's genuinely disagrees (task says done,
    thread says open). Keying the status lookup by bare task_id (the bug found in the
    Phase-1 dry run, decision pending) would let store B's "completed" leak onto store A's
    row too, fabricating a disagreement that never happened."""
    thread_a = await open_thread(actions, "store A's thread, task genuinely pending",
                                  source="agent:me")
    thread_b = await open_thread(actions, "store B's thread, task genuinely completed",
                                  source="agent:me")
    tasks = [
        {"id": "1", "subject": "x", "description": f"thread {str(thread_a)[:8]}",
         "status": "pending", "_store": "storeA"},
        {"id": "1", "subject": "x", "description": f"thread {str(thread_b)[:8]}",
         "status": "completed", "_store": "storeB"},
    ]
    out = await reconcile(actions.pool, tasks)
    assert out["counts"]["disagreement"] == 1
    d = out["disagreement"][0]
    assert d["store"] == "storeB"
    assert d["thread_id"] == str(thread_b)
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


# ── THE WRITE HALF (Thoth DM 2722) ─────────────────────────────────────────────────────

async def _property_rows(actions: Actions, thread_id: uuid.UUID, name: str) -> list[dict]:
    rows = await actions.pool.fetch(
        "SELECT source_id, value FROM current_assertions "
        "WHERE object_id=$1 AND name=$2 ORDER BY source_id", thread_id, name,
    )
    return [{"source_id": r["source_id"], "value": r["value"]} for r in rows]


def test_tier1_targets_is_bound_plus_bound_partial_resolved_only() -> None:
    report = {
        "bound": [{"task_id": "1", "store": "s1", "thread_ids": ["aaa"]}],
        "bound_partial": [{"task_id": "2", "store": "s2", "thread_ids": ["bbb"],
                            "failed": [{"citation": "zzz", "why": "no Thread matches"}]}],
    }
    assert tier1_targets(report) == [
        {"task_id": "1", "store": "s1", "thread_id": "aaa"},
        {"task_id": "2", "store": "s2", "thread_id": "bbb"},
    ]


async def test_write_tier1_correlations_asserts_a_fact_never_a_status(
    actions: Actions,
) -> None:
    tid = await open_thread(actions, "cited cleanly", kind="task", source="agent:me")
    tasks = [_task("1", f"thread {str(tid)[:8]}", status="completed")]
    report = await reconcile(actions.pool, tasks)
    written = await write_tier1_correlations(actions, report, observed_at=NOW)
    assert written == [{"task_id": "1", "store": None, "thread_id": str(tid),
                         "source_id": "harness:1"}]
    rows = await _property_rows(actions, tid, "harness_task_citation")
    assert len(rows) == 1
    assert rows[0]["source_id"] == "harness:1"
    assert rows[0]["value"] == {"task_id": "1", "store": None}
    # never touches status — the thread's own status property is untouched
    status_rows = await _property_rows(actions, tid, "status")
    assert len(status_rows) == 1  # the single row open_thread itself wrote, nothing added


async def test_write_tier1_correlations_per_citing_task_source_lets_citations_coexist(
    actions: Actions,
) -> None:
    """The part of the design Thoth said he'd defend hardest: two different (task, store)
    pairs citing the SAME thread must both survive as distinct current rows, never one
    silently overwriting the other the way a shared source would (current_assertions
    resolves one winner per object+name+SOURCE)."""
    tid = await open_thread(actions, "cited by two different tasks", source="agent:me")
    tasks = [
        {"id": "1", "subject": "x", "description": f"thread {str(tid)[:8]}",
         "status": "pending", "_store": "storeA"},
        {"id": "9", "subject": "x", "description": f"thread {str(tid)[:8]}",
         "status": "pending", "_store": "storeB"},
    ]
    report = await reconcile(actions.pool, tasks)
    await write_tier1_correlations(actions, report, observed_at=NOW)
    rows = await _property_rows(actions, tid, "harness_task_citation")
    assert {r["source_id"] for r in rows} == {"harness:storeA:1", "harness:storeB:9"}


def test_tier2_mints_is_pure_and_deterministic() -> None:
    report = {
        "disagreement": [{"task_id": "1", "store": "s1",
                           "thread_id": "aaaaaaaa-0000-0000-0000-000000000000",
                           "task_status": "completed", "thread_property_status": "open"}],
        "thread_side_orphans": [{"thread_id": "bbbbbbbb-0000-0000-0000-000000000000"}],
    }
    first = tier2_mints(report)
    second = tier2_mints(report)
    assert first == second  # same input -> byte-identical summaries, every time
    assert len(first) == 2
    assert first[0]["kind"] == "obligation"
    assert "DISAGREEMENT" in first[0]["summary"] and "aaaaaaaa" in first[0]["summary"]
    assert "ORPHAN" in first[1]["summary"] and "bbbbbbbb" in first[1]["summary"]


def test_tier2_mints_groups_disagreement_by_thread_not_by_citation() -> None:
    """Thoth's ruling, DM 2744: the first live dry run measured 40 disagreement rows
    landing on only 28 distinct Threads (one Thread, up to 4 citing tasks). One mint per
    Thread, every citing task enumerated inside — not one mint per citation, which would
    make resolving a single real disagreement take as many closes as it has citations."""
    shared = "cccccccc-0000-0000-0000-000000000000"
    report = {
        "disagreement": [
            {"task_id": "11", "store": "storeA", "thread_id": shared,
             "task_status": "completed", "thread_property_status": "open"},
            {"task_id": "61", "store": "storeB", "thread_id": shared,
             "task_status": "completed", "thread_property_status": "open"},
            {"task_id": "9", "store": "storeC", "thread_id": "dddddddd-0-0-0-0",
             "task_status": "pending", "thread_property_status": "resolved"},
        ],
        "thread_side_orphans": [],
    }
    mints = tier2_mints(report)
    assert len(mints) == 2  # 3 citations, 2 distinct threads -> 2 mints, not 3
    shared_mint = next(m for m in mints if "cccccccc" in m["summary"])
    assert "task 11" in shared_mint["summary"] and "task 61" in shared_mint["summary"]
    assert "storeA" in shared_mint["summary"] and "storeB" in shared_mint["summary"]
    assert "2 citing task(s)" in shared_mint["summary"]
    assert len(shared_mint["rows"]) == 2


async def test_mint_tier2_threads_creates_review_threads_never_touching_status(
    actions: Actions,
) -> None:
    disputed = await open_thread(actions, "the disputed thread", source="agent:me")
    report = {
        "disagreement": [{"task_id": "1", "store": "s1", "thread_id": str(disputed),
                           "task_status": "completed", "thread_property_status": "open"}],
        "thread_side_orphans": [],
    }
    mints = tier2_mints(report)
    out = await mint_tier2_threads(actions, mints)
    assert len(out) == 1
    review_id = uuid.UUID(out[0]["thread_id"])
    assert review_id != disputed  # a NEW thread, not a rewrite of the disputed one
    kind_rows = await _property_rows(actions, review_id, "kind")
    assert kind_rows[0]["value"] == "obligation"
    # the disputed thread's own status is untouched by minting a review of it
    disputed_status = await _property_rows(actions, disputed, "status")
    assert disputed_status[0]["value"] == "open"


async def test_mint_tier2_threads_mints_one_thread_for_multiple_citations(
    actions: Actions,
) -> None:
    """End-to-end proof of the consolidation, through the real DB: a Thread disputed by
    TWO citing tasks mints exactly ONE review thread, not two."""
    disputed = await open_thread(actions, "disputed by two tasks", source="agent:me")
    report = {
        "disagreement": [
            {"task_id": "11", "store": "storeA", "thread_id": str(disputed),
             "task_status": "completed", "thread_property_status": "open"},
            {"task_id": "61", "store": "storeB", "thread_id": str(disputed),
             "task_status": "completed", "thread_property_status": "open"},
        ],
        "thread_side_orphans": [],
    }
    out = await mint_tier2_threads(actions, tier2_mints(report))
    assert len(out) == 1


async def test_mint_tier2_threads_is_idempotent_on_rerun(actions: Actions) -> None:
    orphan = await open_thread(actions, "an orphan", kind="task", source="agent:me")
    report = {"disagreement": [], "thread_side_orphans": [{"thread_id": str(orphan)}]}
    mints = tier2_mints(report)
    first = await mint_tier2_threads(actions, mints)
    second = await mint_tier2_threads(actions, mints)
    assert first[0]["thread_id"] == second[0]["thread_id"]  # collapses, never duplicates


async def test_mint_tier2_threads_twins_when_the_citing_set_changes_between_runs(
    actions: Actions,
) -> None:
    """THE REAL RISK Thoth's dispatch (msg 3266, item 4) asked to be established with
    evidence, not assumed: mint_tier2_threads is idempotent on BYTE-IDENTICAL summary text
    only (see test_mint_tier2_threads_is_idempotent_on_rerun, and open_thread's own
    create_or_find_object -> ON CONFLICT (type, canonical) DO NOTHING, a real DB unique
    constraint). But a disagreement summary encodes the FULL current citing-task set
    (tier2_mints's own rerun note). A second run where a new task starts citing the same
    disputed Thread changes the summary text and therefore mints a SECOND, DISTINCT Thread
    rather than updating the first — a real twin, not a hypothetical one. This is why
    firing this at every turn-end is not yet safe: the citing set legitimately changes
    turn to turn as the agent works."""
    disputed = await open_thread(actions, "disputed, citing set will grow", source="agent:me")
    report_v1 = {
        "disagreement": [{"task_id": "11", "store": "storeA", "thread_id": str(disputed),
                           "task_status": "completed", "thread_property_status": "open"}],
        "thread_side_orphans": [],
    }
    first = await mint_tier2_threads(actions, tier2_mints(report_v1))
    # a second task starts citing the same disputed thread before the next run
    report_v2 = {
        "disagreement": [
            {"task_id": "11", "store": "storeA", "thread_id": str(disputed),
             "task_status": "completed", "thread_property_status": "open"},
            {"task_id": "61", "store": "storeB", "thread_id": str(disputed),
             "task_status": "completed", "thread_property_status": "open"},
        ],
        "thread_side_orphans": [],
    }
    second = await mint_tier2_threads(actions, tier2_mints(report_v2))
    assert first[0]["thread_id"] != second[0]["thread_id"]  # TWINNED, not collapsed


# ── archive_eligible_targets (Thoth DM 3266's item 1, pure computation only) ──────────────

def test_archive_eligible_targets_wants_completed_task_and_full_agreement() -> None:
    report = {
        "bound": [{"task_id": "1", "store": "s1", "thread_ids": ["aaa"]}],
        "disagreement": [],
    }
    tasks = [{"id": "1", "subject": "x", "description": "thread aaa", "status": "completed",
              "_store": "s1"}]
    assert archive_eligible_targets(report, tasks) == [
        {"task_id": "1", "store": "s1", "thread_ids": ["aaa"]},
    ]


def test_archive_eligible_targets_refuses_a_completed_task_still_disagreeing() -> None:
    # the harness says done but the graph's own bound thread never resolved -- exactly the
    # disagreement bucket; neither side auto-wins, so this row must NOT be archive-eligible
    report = {
        "bound": [{"task_id": "1", "store": "s1", "thread_ids": ["aaa"]}],
        "disagreement": [{"task_id": "1", "store": "s1", "thread_id": "aaa",
                           "task_status": "completed", "thread_property_status": "open"}],
    }
    tasks = [{"id": "1", "subject": "x", "description": "thread aaa", "status": "completed",
              "_store": "s1"}]
    assert archive_eligible_targets(report, tasks) == []


def test_archive_eligible_targets_refuses_a_task_still_pending() -> None:
    # both sides agree it's still open -- agreement, but not the DONE agreement archiving
    # requires
    report = {"bound": [{"task_id": "1", "store": "s1", "thread_ids": ["aaa"]}],
              "disagreement": []}
    tasks = [{"id": "1", "subject": "x", "description": "thread aaa", "status": "pending",
              "_store": "s1"}]
    assert archive_eligible_targets(report, tasks) == []


def test_archive_eligible_targets_never_touches_bound_partial() -> None:
    # even a completed, fully-agreeing resolved half must not archive -- the task still
    # carries an unresolved citation this module refuses to guess about
    report = {
        "bound": [],
        "bound_partial": [{"task_id": "1", "store": "s1", "thread_ids": ["aaa"],
                            "failed": [{"citation": "zzz", "why": "no Thread matches"}]}],
        "disagreement": [],
    }
    tasks = [{"id": "1", "subject": "x", "description": "thread aaa, zzz",
              "status": "completed", "_store": "s1"}]
    assert archive_eligible_targets(report, tasks) == []


def test_archive_eligible_targets_requires_unanimous_agreement_across_citations() -> None:
    # one thread resolved, one still open -- a task the operator is still working, per the
    # module's own rationale; must not archive on the strength of only the resolved half
    report = {
        "bound": [{"task_id": "1", "store": "s1", "thread_ids": ["aaa", "bbb"]}],
        "disagreement": [{"task_id": "1", "store": "s1", "thread_id": "bbb",
                           "task_status": "completed", "thread_property_status": "open"}],
    }
    tasks = [{"id": "1", "subject": "x", "description": "threads aaa, bbb",
              "status": "completed", "_store": "s1"}]
    assert archive_eligible_targets(report, tasks) == []


def test_archive_eligible_targets_is_scoped_by_store_not_bare_task_id() -> None:
    # the same collision hazard reconcile() itself refuses: task id "1" repeats across two
    # stores, only one of which is genuinely done-and-agreeing
    report = {
        "bound": [
            {"task_id": "1", "store": "storeA", "thread_ids": ["aaa"]},
            {"task_id": "1", "store": "storeB", "thread_ids": ["bbb"]},
        ],
        "disagreement": [],
    }
    tasks = [
        {"id": "1", "subject": "x", "description": "thread aaa", "status": "completed",
         "_store": "storeA"},
        {"id": "1", "subject": "x", "description": "thread bbb", "status": "pending",
         "_store": "storeB"},
    ]
    assert archive_eligible_targets(report, tasks) == [
        {"task_id": "1", "store": "storeA", "thread_ids": ["aaa"]},
    ]

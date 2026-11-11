"""The churn collapse folds repeated abstention rows the way `assertion_fold` does: rows are
grouped by everything that makes them the same fact, each group keeps its first and latest row,
and the copies between go, hot and cold. Nothing that points at a deleted copy is left pointing
at nothing. Dry run by default."""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from src.actions.core import Actions
from src.orchestrator import abstention_churn
from src.orchestrator.abstention_churn import _fold_chunk, _plan, collapse_abstention_churn
from src.orchestrator.backfill import BACKFILL_TARGETS, run_backfill

_NAME = "derivation_abstained_in_repo"


def _value(kind: int) -> str:
    return json.dumps({"link_type": "in_repo", "candidate_count": 2,
                       "reason": f"verdict {kind}", "candidates": []})


async def _chain(
    actions: Actions, tag: str, cold: int, hot: int, kinds: list[int] | None = None,
    current: set[int] | None = None,
) -> tuple[Any, list[int]]:
    """An object with `cold` rows in the cold tier then `hot` in the hot tier, oldest first,
    each superseding the one before. `kinds` gives each row's verdict (default: all the same).
    The last hot row is current unless `current` names which positions are. Returns the object
    id and the row ids."""
    pool = actions.pool
    obj = await actions.create_or_find_object("Decision", tag, "test")
    base = datetime.now(UTC) - timedelta(days=30)
    ids: list[int] = []
    prev: int | None = None
    total = cold + hot
    live = current if current is not None else {total - 1}
    for i in range(total):
        table = "assertions_cold" if i < cold else "assertions_hot"
        rid = await pool.fetchval(
            f"INSERT INTO {table} (id, object_id, name, value, source_id, observed_at, "
            "created_at, confidence, supersedes, is_current) "
            "VALUES (nextval('assertions_id_seq'), $1, $2, $3::jsonb, 'test', $4, $4, 0.6, "
            "$5, $6) RETURNING id", obj, _NAME, _value(kinds[i] if kinds else 0),
            base + timedelta(minutes=i), prev, i in live and table == "assertions_hot")
        ids.append(rid)
        prev = rid
    return obj, ids


async def _left(actions: Actions, obj: object) -> list[int]:
    rows = await actions.pool.fetch(
        "SELECT id FROM (SELECT id, object_id, name FROM assertions_hot UNION ALL "
        "SELECT id, object_id, name FROM assertions_cold) t "
        "WHERE object_id=$1 AND name=$2 ORDER BY id", obj, _NAME)
    return [r["id"] for r in rows]


async def _dangling(actions: Actions, obj: object) -> int:
    """Rows of this object whose supersedes names a row that no longer exists."""
    return int(await actions.pool.fetchval(abstention_churn._DANGLING_SQL, obj, _NAME))


async def _pointer(actions: Actions, row_id: int) -> int | None:
    for table in ("assertions_hot", "assertions_cold"):
        found = await actions.pool.fetchrow(
            f"SELECT supersedes FROM {table} WHERE id=$1", row_id)
        if found is not None:
            return found["supersedes"]
    raise AssertionError(f"row {row_id} is gone")


async def test_target_is_registered() -> None:
    assert "abstention_churn_collapse" in BACKFILL_TARGETS


async def test_dry_run_counts_and_writes_nothing(actions: Actions) -> None:
    obj, ids = await _chain(actions, "churn-dry", cold=4, hot=3)
    out = await collapse_abstention_churn(actions.pool, actor="test")
    assert out["dry_run"] is True
    assert out["drop_rows"] == 5 and out["drop_cold"] == 3 and out["drop_hot"] == 2
    assert out["objects_with_drops"] == 1 and out["drop_bytes"] > 0
    assert out["skipped_current"] == 0 and out["distinct_values_lost"] == 0
    assert out["distinct_values"] == 1
    assert out["oldest"] <= out["newest"]
    assert out["by_object"] == [{"object_id": str(obj), "drop_rows": 5,
                                 "bytes": out["drop_bytes"]}]
    assert await _left(actions, obj) == ids


async def test_the_plan_is_one_row_per_object_not_one_per_stored_row(actions: Actions) -> None:
    await _chain(actions, "churn-shape-a", cold=5, hot=4)
    await _chain(actions, "churn-shape-b", cold=2, hot=2)
    plan = await _plan(actions.pool)
    assert len(plan) == 2 and sum(r["total_rows"] for r in plan) == 13


async def test_apply_needs_a_because(actions: Actions) -> None:
    obj, ids = await _chain(actions, "churn-nobecause", cold=2, hot=2)
    out = await collapse_abstention_churn(actions.pool, actor="test", dry_run=False)
    assert "error" in out
    assert await _left(actions, obj) == ids


async def test_apply_keeps_first_and_latest_and_leaves_no_dangling_pointer(
    actions: Actions,
) -> None:
    obj, ids = await _chain(actions, "churn-apply", cold=4, hot=3)
    dry = await collapse_abstention_churn(actions.pool, actor="test")
    out = await collapse_abstention_churn(
        actions.pool, actor="test", dry_run=False, because="test authorization",
        chunk_rows=2)
    assert out["finished"] is True and out["deleted"] == 5 == dry["drop_rows"]
    assert out["dangling_after"] == 0 and await _dangling(actions, obj) == 0
    assert out["delete_misses"] == 0 and out["repoint_misses"] == 0
    assert await _left(actions, obj) == [ids[0], ids[-1]]
    latest = await actions.pool.fetchrow(
        "SELECT supersedes, is_current FROM assertions_hot WHERE id=$1", ids[-1])
    assert latest["supersedes"] == ids[0] and latest["is_current"] is True
    again = await collapse_abstention_churn(
        actions.pool, actor="test", dry_run=False, because="test authorization")
    assert again["drop_rows"] == 0 and again["deleted"] == 0 and again["finished"] is True


async def test_distinct_verdicts_each_keep_their_first_and_latest_row(
    actions: Actions,
) -> None:
    kinds = [0, 1, 0, 1, 0, 1, 0]
    obj, ids = await _chain(actions, "churn-variants", cold=3, hot=4, kinds=kinds)
    dry = await collapse_abstention_churn(actions.pool, actor="test")
    assert dry["distinct_values"] == 2 and dry["distinct_values_lost"] == 0
    assert dry["drop_rows"] == 3
    out = await collapse_abstention_churn(
        actions.pool, actor="test", dry_run=False, because="test authorization")
    assert out["finished"] is True and out["deleted"] == 3
    assert await _left(actions, obj) == [ids[0], ids[1], ids[5], ids[6]]
    values = {r["value"] for r in await actions.pool.fetch(
        "SELECT value::text FROM (SELECT * FROM assertions_hot UNION ALL "
        "SELECT * FROM assertions_cold) t WHERE object_id=$1 AND name=$2", obj, _NAME)}
    assert len(values) == 2                       # both verdicts are still there
    assert await _dangling(actions, obj) == 0


async def test_a_resolved_marker_that_pointed_at_a_copy_is_repointed_not_left_dangling(
    actions: Actions,
) -> None:
    obj, ids = await _chain(actions, "churn-resolved", cold=3, hot=2, current=set())
    marker = await actions.pool.fetchval(
        "INSERT INTO assertions_hot (id, object_id, name, value, source_id, observed_at, "
        "confidence, supersedes, is_current) VALUES (nextval('assertions_id_seq'), $1, $2, "
        "'{\"resolved\": true}'::jsonb, 'test', now(), 0.6, $3, true) RETURNING id",
        obj, _NAME, ids[2])
    short, short_ids = await _chain(actions, "churn-short", cold=0, hot=2)
    out = await collapse_abstention_churn(
        actions.pool, actor="test", dry_run=False, because="test authorization")
    assert out["finished"] is True
    assert await _left(actions, obj) == [ids[0], ids[-1], marker]
    assert await _left(actions, short) == short_ids
    assert await _pointer(actions, marker) == ids[0]          # it pointed at a deleted copy
    assert await _dangling(actions, obj) == 0 and out["dangling_after"] == 0


async def test_a_current_row_between_the_ends_stays_and_the_dry_run_says_so(
    actions: Actions,
) -> None:
    obj, ids = await _chain(actions, "churn-current", cold=2, hot=5, current={4, 6})
    dry = await collapse_abstention_churn(actions.pool, actor="test")
    assert dry["skipped_current"] == 1 and dry["drop_rows"] == 4
    out = await collapse_abstention_churn(
        actions.pool, actor="test", dry_run=False, because="test authorization")
    assert out["deleted"] == dry["drop_rows"] == 4          # the plan is what the apply does
    assert out["finished"] is True
    assert await _left(actions, obj) == [ids[0], ids[4], ids[6]]
    assert await _pointer(actions, ids[4]) == ids[0]
    assert await _dangling(actions, obj) == 0


async def test_a_row_that_moved_tier_or_became_current_is_counted_not_passed_silently(
    actions: Actions,
) -> None:
    obj, ids = await _chain(actions, "churn-moved", cold=0, hot=5)
    cand = [{"id": ids[1], "tier": "assertions_cold", "first_id": ids[0]},    # wrong tier
            {"id": ids[2], "tier": "assertions_hot", "first_id": ids[0]}]
    await actions.pool.execute("UPDATE assertions_hot SET is_current=true WHERE id=$1", ids[2])
    chunk = await _fold_chunk(actions.pool, obj, cand)                      # type: ignore[arg-type]
    assert chunk["planned"] == 2 and chunk["deleted"] == []
    assert await _left(actions, obj) == ids


async def test_time_budget_stops_between_chunks_and_a_rerun_finishes(actions: Actions) -> None:
    obj, ids = await _chain(actions, "churn-budget", cold=6, hot=1)
    first = await collapse_abstention_churn(
        actions.pool, actor="test", dry_run=False, because="test authorization",
        chunk_rows=1, max_seconds=0.0)
    assert first["finished"] is False and first["deleted"] == 0
    done = await collapse_abstention_churn(
        actions.pool, actor="test", dry_run=False, because="test authorization")
    assert done["finished"] is True
    assert await _left(actions, obj) == [ids[0], ids[-1]]


async def test_an_apply_records_a_decision_with_the_counts_and_the_id_range(
    actions: Actions,
) -> None:
    _, ids = await _chain(actions, "churn-audit", cold=3, hot=3)
    out = await collapse_abstention_churn(
        actions.pool, actor="test-actor", dry_run=False, because="the operator approved")
    assert out["audit"]["recorded"] is True
    summary = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "WHERE a.object_id = $1::uuid AND a.name='summary'", out["audit"]["decision"])
    assert summary is not None
    assert f"deleted 4 repeated abstention rows (ids {ids[1]} to {ids[4]})" in summary
    assert "test-actor" in summary and "the operator approved" in summary


async def test_a_dry_run_records_nothing(actions: Actions) -> None:
    await _chain(actions, "churn-dry-audit", cold=2, hot=2)
    out = await collapse_abstention_churn(actions.pool, actor="test")
    assert "audit" not in out


async def test_run_backfill_dispatches_the_target(actions: Actions) -> None:
    out = await run_backfill(actions.pool, "abstention_churn_collapse", actor="test",
                             dry_run=True, because=None)
    assert out["dry_run"] is True and "drop_rows" in out

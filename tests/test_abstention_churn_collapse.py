"""The churn collapse keeps the first and latest unresolved abstention per object and drops
the copies between, hot and cold, leaving the supersede chain whole. Dry run by default."""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.orchestrator.abstention_churn import collapse_abstention_churn
from src.orchestrator.backfill import BACKFILL_TARGETS, run_backfill

_NAME = "derivation_abstained_in_repo"


async def _chain(actions: Actions, tag: str, cold: int, hot: int) -> tuple[object, list[int]]:
    """An object with `cold` superseded copies in the cold tier then `hot` in the hot tier
    (the last hot one current), oldest first. Returns the object id and the row ids."""
    pool = actions.pool
    obj = await actions.create_or_find_object("Decision", tag, "test")
    base = datetime.now(UTC) - timedelta(days=30)
    ids: list[int] = []
    prev: int | None = None
    total = cold + hot
    for i in range(total):
        table = "assertions_cold" if i < cold else "assertions_hot"
        current = i == total - 1
        value = json.dumps({"link_type": "in_repo", "candidate_count": 0,
                            "reason": f"wording {i}", "candidates": []})
        rid = await pool.fetchval(
            f"INSERT INTO {table} (id, object_id, name, value, source_id, observed_at, "
            "created_at, confidence, supersedes, is_current) "
            "VALUES (nextval('assertions_id_seq'), $1, $2, $3::jsonb, 'test', $4, $4, 0.6, "
            "$5, $6) "
            "RETURNING id", obj, _NAME, value, base + timedelta(minutes=i), prev,
            current and table == "assertions_hot")
        ids.append(rid)
        prev = rid
    return obj, ids


async def _left(actions: Actions, obj: object) -> list[int]:
    rows = await actions.pool.fetch(
        "SELECT id FROM (SELECT id, object_id, name FROM assertions_hot UNION ALL "
        "SELECT id, object_id, name FROM assertions_cold) t "
        "WHERE object_id=$1 AND name=$2 ORDER BY id", obj, _NAME)
    return [r["id"] for r in rows]


async def test_target_is_registered() -> None:
    assert "abstention_churn_collapse" in BACKFILL_TARGETS


async def test_dry_run_counts_and_writes_nothing(actions: Actions) -> None:
    obj, ids = await _chain(actions, "churn-dry", cold=4, hot=3)
    out = await collapse_abstention_churn(actions.pool, actor="test")
    assert out["dry_run"] is True
    assert out["drop_rows"] == 5 and out["drop_cold"] == 3 and out["drop_hot"] == 2
    assert out["objects_with_drops"] == 1 and out["drop_bytes"] > 0
    assert out["oldest"] <= out["newest"]
    assert out["by_object"] == [{"object_id": str(obj), "drop_rows": 5,
                                 "bytes": out["drop_bytes"]}]
    assert await _left(actions, obj) == ids


async def test_apply_needs_a_because(actions: Actions) -> None:
    obj, ids = await _chain(actions, "churn-nobecause", cold=2, hot=2)
    out = await collapse_abstention_churn(actions.pool, actor="test", dry_run=False)
    assert "error" in out
    assert await _left(actions, obj) == ids


async def test_apply_keeps_first_and_latest_and_the_chain_whole(actions: Actions) -> None:
    obj, ids = await _chain(actions, "churn-apply", cold=4, hot=3)
    out = await collapse_abstention_churn(
        actions.pool, actor="test", dry_run=False, because="test authorization",
        chunk_rows=2)
    assert out["finished"] is True and out["deleted"] == 5
    assert await _left(actions, obj) == [ids[0], ids[-1]]
    latest = await actions.pool.fetchrow(
        "SELECT supersedes, is_current FROM assertions_hot WHERE id=$1", ids[-1])
    assert latest["supersedes"] == ids[0] and latest["is_current"] is True
    again = await collapse_abstention_churn(
        actions.pool, actor="test", dry_run=False, because="test authorization")
    assert again["drop_rows"] == 0 and again["deleted"] == 0


async def test_resolved_marker_and_short_chains_are_untouched(actions: Actions) -> None:
    obj, ids = await _chain(actions, "churn-resolved", cold=3, hot=1)
    marker = await actions.pool.fetchval(
        "INSERT INTO assertions_hot (id, object_id, name, value, source_id, observed_at, "
        "confidence, supersedes, is_current) VALUES (nextval('assertions_id_seq'), $1, $2, "
        "'{\"resolved\": true}'::jsonb, 'test', now(), 0.6, $3, true) RETURNING id",
        obj, _NAME, ids[-1])
    await actions.pool.execute("UPDATE assertions_hot SET is_current=false WHERE id=$1", ids[-1])
    short, short_ids = await _chain(actions, "churn-short", cold=0, hot=2)
    await collapse_abstention_churn(
        actions.pool, actor="test", dry_run=False, because="test authorization")
    assert await _left(actions, obj) == [ids[0], ids[-1], marker]
    assert await _left(actions, short) == short_ids


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


async def test_run_backfill_dispatches_the_target(actions: Actions) -> None:
    out = await run_backfill(actions.pool, "abstention_churn_collapse", actor="test",
                             dry_run=True, because=None)
    assert out["dry_run"] is True and "drop_rows" in out

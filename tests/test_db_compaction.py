"""Nightly database compaction: rewrite the big tables that are mostly dead or free space,
one at a time inside the night window, never while the disk brake is paused, the disk could
not hold the rewrite, or a bulk drain is still emptying the table."""
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import db_compaction
from src.orchestrator.db_compaction import (
    GB,
    compact_tonight,
    compaction_status,
    drain_running,
    last_receipts,
    reclaimable_bytes,
    rewrite_table,
    set_drain,
    table_sizes,
    worth_rewriting,
)

SCRATCH = "compaction_scratch"


async def _bloated(actions: Actions, name: str = SCRATCH) -> None:
    """A table that was big and is now mostly free space: 20000 rows of ~1 KB, 95% deleted,
    vacuumed (so the space is free inside the file, not returned)."""
    await actions.pool.execute(f"DROP TABLE IF EXISTS {name}")
    await actions.pool.execute(
        f"CREATE TABLE {name} (id int PRIMARY KEY, pad text NOT NULL)")
    await actions.pool.execute(
        f"INSERT INTO {name} SELECT g, repeat('x', 1000) || g FROM generate_series(1, 20000) g")
    await actions.pool.execute(f"DELETE FROM {name} WHERE id % 20 <> 0")
    await actions.pool.execute(f"VACUUM {name}")


async def _roomy(pool: Any) -> dict[str, Any]:
    return {"paused": False, "reason": None, "free_gb": 900.0, "floor_gb": 100.0}


@pytest.fixture(autouse=True)
def _scratch_thresholds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(db_compaction, "MIN_BLOAT_BYTES", 1024 * 1024)


def test_a_table_is_worth_rewriting_only_above_both_thresholds() -> None:
    assert worth_rewriting(10 * GB, 4 * GB) is True
    assert worth_rewriting(10 * GB, 2 * GB) is False       # under 2 GB
    assert worth_rewriting(100 * GB, 20 * GB) is False     # under 30%
    assert worth_rewriting(0, 0) is False


async def test_a_rewrite_gives_the_space_back(actions: Actions) -> None:
    await _bloated(actions)
    before = (await table_sizes(actions.pool, SCRATCH))["total_bytes"]
    engine = await rewrite_table(actions.pool, SCRATCH, seconds_left=120)
    after = (await table_sizes(actions.pool, SCRATCH))["total_bytes"]
    assert engine == "VACUUM FULL" and after < before / 4
    # the rows that were kept are all there
    assert await actions.pool.fetchval(f"SELECT count(*) FROM {SCRATCH}") == 1000


async def test_the_dead_space_is_measured_with_pgstattuple_when_it_can_be(
    actions: Actions,
) -> None:
    await _bloated(actions)
    reclaim, method = await reclaimable_bytes(actions.pool, SCRATCH)
    if method != "pgstattuple_approx":
        pytest.skip("pgstattuple is not installable in this database")
    assert reclaim > 10 * 1024 * 1024   # most of ~25 MB is free space


async def test_without_pgstattuple_the_statistics_estimate_is_used_and_labelled(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _none(pool: Any, oid_sql: str, table: str) -> None:
        return None

    monkeypatch.setattr(db_compaction, "_approx", _none)
    _, method = await reclaimable_bytes(actions.pool, SCRATCH)
    assert method.startswith("statistics")


async def test_the_night_run_rewrites_a_bloated_table_and_records_the_sizes(
    actions: Actions,
) -> None:
    await _bloated(actions)
    out = await compact_tonight(
        actions.pool, window_secs=600, tables=(SCRATCH,), brake=_roomy)
    r = out[SCRATCH]
    assert r["action"] == "rewritten" and r["engine"] == "VACUUM FULL"
    assert r["after_bytes"] < r["before_bytes"]
    assert (await last_receipts(actions.pool))[SCRATCH]["action"] == "rewritten"


async def test_a_table_with_little_dead_space_is_left_alone(actions: Actions) -> None:
    await actions.pool.execute(f"DROP TABLE IF EXISTS {SCRATCH}")
    await actions.pool.execute(f"CREATE TABLE {SCRATCH} (id int PRIMARY KEY, pad text)")
    await actions.pool.execute(
        f"INSERT INTO {SCRATCH} SELECT g, repeat('x', 1000) FROM generate_series(1, 2000) g")
    out = await compact_tonight(actions.pool, window_secs=600, tables=(SCRATCH,), brake=_roomy)
    assert out[SCRATCH]["action"] == "skipped"
    assert out[SCRATCH]["reason"] == "not enough dead space"


async def test_a_running_drain_defers_the_table(actions: Actions) -> None:
    await _bloated(actions)
    await set_drain(actions.pool, (SCRATCH,), True)
    out = await compact_tonight(actions.pool, window_secs=600, tables=(SCRATCH,), brake=_roomy)
    assert out[SCRATCH]["reason"] == "a bulk drain is still running on it"
    await set_drain(actions.pool, (SCRATCH,), False)
    out = await compact_tonight(actions.pool, window_secs=600, tables=(SCRATCH,), brake=_roomy)
    assert out[SCRATCH]["action"] == "rewritten"


async def test_the_soul_reencode_counts_as_a_drain_on_soul_lines(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator import soul_recompress

    assert await drain_running(actions.pool, "soul_lines") is False
    monkeypatch.setattr(soul_recompress, "read_progress", lambda: {"state": "running"})
    assert await drain_running(actions.pool, "soul_lines") is True
    assert await drain_running(actions.pool, "audit_log") is False


async def test_a_paused_brake_defers_the_table(actions: Actions) -> None:
    await _bloated(actions)

    async def _paused(pool: Any) -> dict[str, Any]:
        return {"paused": True, "reason": "root disk free 10 GB is below the 100 GB floor",
                "free_gb": 10.0, "floor_gb": 100.0}

    out = await compact_tonight(actions.pool, window_secs=600, tables=(SCRATCH,), brake=_paused)
    assert out[SCRATCH]["action"] == "skipped"
    assert out[SCRATCH]["reason"].startswith("paused: disk/WAL budget")


async def test_a_rewrite_that_the_disk_could_not_hold_is_not_started(
    actions: Actions,
) -> None:
    await _bloated(actions)

    async def _tight(pool: Any) -> dict[str, Any]:
        return {"paused": False, "reason": None, "free_gb": 100.0, "floor_gb": 100.0}

    out = await compact_tonight(actions.pool, window_secs=600, tables=(SCRATCH,), brake=_tight)
    assert out[SCRATCH]["action"] == "skipped" and "floor" in out[SCRATCH]["reason"]


async def test_nothing_starts_once_the_window_is_nearly_over(actions: Actions) -> None:
    await _bloated(actions)
    out = await compact_tonight(actions.pool, window_secs=30, tables=(SCRATCH,), brake=_roomy)
    assert out[SCRATCH]["reason"] == "the window is over"


async def test_a_failed_rewrite_is_recorded_and_does_not_stop_the_run(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _bloated(actions)

    async def _boom(pool: Any, table: str, **kwargs: Any) -> str:
        raise RuntimeError("could not obtain lock on relation")

    monkeypatch.setattr(db_compaction, "rewrite_table", _boom)
    out = await compact_tonight(actions.pool, window_secs=600, tables=(SCRATCH,), brake=_roomy)
    assert out[SCRATCH]["action"] == "failed" and "lock" in out[SCRATCH]["reason"]


async def test_vacuum_full_is_cancelled_when_the_window_runs_out(actions: Actions) -> None:
    await _bloated(actions)
    with pytest.raises(Exception, match="statement timeout|canceling"):
        await rewrite_table(actions.pool, SCRATCH, seconds_left=0.001)
    # nothing was lost by the cancelled rewrite
    assert await actions.pool.fetchval(f"SELECT count(*) FROM {SCRATCH}") == 1000


async def test_the_status_shows_sizes_and_what_the_last_night_did(actions: Actions) -> None:
    await _bloated(actions)
    await compact_tonight(actions.pool, window_secs=600, tables=(SCRATCH,), brake=_roomy)
    status = await compaction_status(actions.pool)
    assert status["window"] == "03:00 to 05:00"
    assert set(status["tables"]) == set(db_compaction.TABLES)
    assert status["tables"]["audit_log"]["total_gb"] >= 0


async def test_backup_status_carries_the_compaction_section(
    actions: Actions, tmp_path: Any,
) -> None:
    from src.orchestrator.compositions import _fn_backup_status

    out = await _fn_backup_status(
        actions.pool, None, {"vault": str(tmp_path / "v"), "backups": str(tmp_path / "b")})
    assert "soul_lines" in out["compaction"]["tables"]


# --- the heartbeat and the schedule ---------------------------------------------------------

async def test_the_night_heartbeat_compacts_and_tells_the_desk(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.orchestrator.mailbox as mailbox
    from src.orchestrator import disk_brake
    from src.workers.arq_worker import db_compaction_heartbeat

    await _bloated(actions)
    monkeypatch.setattr(db_compaction, "TABLES", (SCRATCH,))
    monkeypatch.setattr(disk_brake, "disk_usage", lambda: (900 * GB, 2000 * GB))
    captured: dict[str, Any] = {}

    async def _send(pool: Any, **kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"sent": 1}

    monkeypatch.setattr(mailbox, "send_message", _send)
    freed = await db_compaction_heartbeat({"cascade": SimpleNamespace(actions=actions)})
    assert freed > 0
    assert SCRATCH in captured["body"] and "database compaction" in captured["body"]


async def test_the_night_heartbeat_pauses_with_the_disk_brake(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator import disk_brake
    from src.workers.arq_worker import db_compaction_heartbeat

    await _bloated(actions)
    monkeypatch.setattr(db_compaction, "TABLES", (SCRATCH,))
    monkeypatch.setattr(disk_brake, "disk_usage", lambda: (10 * GB, 2000 * GB))
    assert await db_compaction_heartbeat({"cascade": SimpleNamespace(actions=actions)}) == 0
    receipts = await last_receipts(actions.pool)
    assert receipts[SCRATCH]["reason"].startswith("paused: disk/WAL budget")


async def test_the_housekeeping_steps_flag_a_drain_that_is_still_going(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.orchestrator.mailbox as mailbox
    from src.orchestrator.monitor import get_cursor
    from src.workers.arq_worker import storage_housekeeping_heartbeat

    async def _send(pool: Any, **kwargs: Any) -> dict[str, Any]:
        return {"sent": 1}

    monkeypatch.setattr(mailbox, "send_message", _send)
    await storage_housekeeping_heartbeat({"cascade": SimpleNamespace(actions=actions)})
    for table in ("audit_log", "assertions_hot", "assertions_cold"):
        flag = json.loads(await get_cursor(actions.pool, db_compaction.DRAIN_KEY + table) or "{}")
        assert flag.get("running") is False, table   # the quiet test database has nothing left


def test_the_compaction_runs_at_three_in_the_morning_after_housekeeping_and_not_at_startup(
) -> None:
    from src.workers.arq_worker import WorkerSettings

    def _job(suffix: str) -> Any:
        found = [c for c in WorkerSettings.cron_jobs if getattr(c, "name", "").endswith(suffix)]
        assert len(found) == 1
        return found[0]

    compaction = _job("db_compaction_heartbeat")
    housekeeping = _job("storage_housekeeping_heartbeat")
    assert compaction.run_at_startup is False
    assert compaction.hour == {3} and compaction.minute == {0}
    assert housekeeping.hour == {2}   # its drains finish before the compaction window opens

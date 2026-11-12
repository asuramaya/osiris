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


def test_the_outbox_is_rewritten_at_a_smaller_floor_than_the_big_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The live outbox: 709 MB reclaimable of 1.08 GB. Under the 2 GB floor it was skipped
    every night; its own floor lets it through, as does the audit log's, and now the
    assertions tables'."""
    monkeypatch.setattr(db_compaction, "MIN_BLOAT_BYTES", 2 * GB)  # the real floor
    mb = 1024 ** 2
    assert worth_rewriting(1083 * mb, 709 * mb, "outbox") is True
    assert worth_rewriting(1083 * mb, 709 * mb, "audit_log") is True  # the same reduced floor
    assert worth_rewriting(1667 * mb, 1219 * mb, "audit_log") is True  # the live audit log
    assert worth_rewriting(1083 * mb, 200 * mb, "audit_log") is False
    # the assertions tables have the reduced floor too (they are mostly empty since the fold)
    assert worth_rewriting(1913 * mb, 1091 * mb, "assertions_hot") is True
    assert worth_rewriting(1764 * mb, 648 * mb, "assertions_cold") is True
    assert worth_rewriting(1682 * mb, 1311 * mb, "assertions_cold") is True  # the live cold table
    assert worth_rewriting(1825 * mb, 1172 * mb, "assertions_hot") is True  # the live hot table
    assert worth_rewriting(1825 * mb, 200 * mb, "assertions_hot") is False  # still needs bloat
    assert worth_rewriting(100 * GB, 20 * GB, "assertions_cold") is False  # still needs 30%
    assert worth_rewriting(1083 * mb, 709 * mb) is False
    assert worth_rewriting(1083 * mb, 200 * mb, "outbox") is False  # still needs real bloat
    assert worth_rewriting(100 * GB, 20 * GB, "outbox") is False  # still needs 30% bloat


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

    await _bloated(actions)  # it used to lean on a table an earlier test happened to leave
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
    assert "floor" in out[SCRATCH]["reason"] and "minimum" in out[SCRATCH]["reason"]


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


async def _hold_the_table(actions: Actions) -> Any:
    """A transaction that keeps the table busy (ACCESS SHARE blocks VACUUM FULL's exclusive
    lock), returned with a way to let it go."""
    conn = await actions.pool.acquire()
    tx = conn.transaction()
    await tx.start()
    await conn.execute(f"LOCK TABLE {SCRATCH} IN ACCESS SHARE MODE")

    async def release() -> None:
        await tx.rollback()
        await actions.pool.release(conn)

    return release


async def test_a_busy_table_is_skipped_with_a_plain_reason_after_short_tries(
    actions: Actions,
) -> None:
    """The lock is asked for with a short timeout and retried; a table that stays busy is
    skipped, not failed, and nothing is changed."""
    from src.orchestrator.db_compaction import LockNotAcquired

    await _bloated(actions)
    release = await _hold_the_table(actions)
    sleeps: list[float] = []

    async def _sleep(secs: float) -> None:
        sleeps.append(secs)

    try:
        with pytest.raises(LockNotAcquired) as caught:
            await rewrite_table(actions.pool, SCRATCH, seconds_left=600, lock_attempts=3,
                                lock_timeout_secs=0.2, sleep=_sleep)
    finally:
        await release()
    assert len(sleeps) == 2  # waited between the three tries, not after the last
    assert "within 0.2 s on 3 tries" in caught.value.reason
    assert await actions.pool.fetchval(f"SELECT count(*) FROM {SCRATCH}") == 1000


async def test_the_night_run_records_a_never_free_table_as_skipped(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator.db_compaction import LockNotAcquired

    await _bloated(actions)

    async def _busy(pool: Any, table: str, **kwargs: Any) -> str:
        raise LockNotAcquired(table, 4, 3.0, 120.0)

    monkeypatch.setattr(db_compaction, "rewrite_table", _busy)
    out = await compact_tonight(actions.pool, window_secs=600, tables=(SCRATCH,), brake=_roomy)
    assert out[SCRATCH]["action"] == "skipped"
    assert out[SCRATCH]["reason"].startswith("could not get the table lock within 3 s on 4 tries")


async def test_a_lock_freed_between_tries_lets_the_rewrite_through(actions: Actions) -> None:
    await _bloated(actions)
    release = await _hold_the_table(actions)

    async def _let_go_then_wait(secs: float) -> None:
        await release()  # the busy holder finishes while we wait for the next try

    engine = await rewrite_table(actions.pool, SCRATCH, seconds_left=600, lock_attempts=3,
                                 lock_timeout_secs=0.2, sleep=_let_go_then_wait)
    assert engine == "VACUUM FULL"
    assert await actions.pool.fetchval(f"SELECT count(*) FROM {SCRATCH}") == 1000


async def _flag(actions: Actions, table: str, *, running: bool, hours_ago: float) -> None:
    from datetime import UTC, datetime, timedelta

    from src.orchestrator.monitor import set_cursor

    at = (datetime.now(UTC) - timedelta(hours=hours_ago)).isoformat()
    await set_cursor(actions.pool, db_compaction.DRAIN_KEY + table,
                     json.dumps({"running": running, "at": at}))


async def test_a_fresh_drain_flag_defers_the_table_and_a_stale_one_does_not(
    actions: Actions,
) -> None:
    """The flag is rewritten nightly. One that is older than 36 hours was left by a step that
    stopped, and it must not skip the table forever."""
    from src.orchestrator.db_compaction import drain_running

    await _flag(actions, "assertions_cold", running=True, hours_ago=2)
    assert await drain_running(actions.pool, "assertions_cold") is True
    await _flag(actions, "assertions_cold", running=True, hours_ago=35)
    assert await drain_running(actions.pool, "assertions_cold") is True
    await _flag(actions, "assertions_cold", running=True, hours_ago=37)
    assert await drain_running(actions.pool, "assertions_cold") is False
    await _flag(actions, "assertions_cold", running=False, hours_ago=1)
    assert await drain_running(actions.pool, "assertions_cold") is False


async def test_a_flag_with_no_time_is_still_honoured_and_a_broken_one_is_not_a_drain(
    actions: Actions,
) -> None:
    from src.orchestrator.db_compaction import drain_running
    from src.orchestrator.monitor import set_cursor

    await set_cursor(actions.pool, db_compaction.DRAIN_KEY + "assertions_hot",
                     json.dumps({"running": True}))
    assert await drain_running(actions.pool, "assertions_hot") is True
    await set_cursor(actions.pool, db_compaction.DRAIN_KEY + "assertions_hot",
                     json.dumps({"running": True, "at": "not a time"}))
    assert await drain_running(actions.pool, "assertions_hot") is False


async def test_a_stale_housekeeping_flag_is_ignored_but_a_live_backfill_flag_still_holds(
    actions: Actions,
) -> None:
    """The two flags are independent: the housekeeping one expires after 36 hours, a
    bulk-delete backfill's own flag still defers the table while it is unfinished."""
    from src.orchestrator.db_compaction import drain_running, set_backfill_drain

    await _flag(actions, "assertions_cold", running=True, hours_ago=40)
    assert await drain_running(actions.pool, "assertions_cold") is False
    await set_backfill_drain(actions.pool, "some_backfill", ("assertions_cold",), True)
    assert await drain_running(actions.pool, "assertions_cold") is True
    await set_backfill_drain(actions.pool, "some_backfill", ("assertions_cold",), False)
    assert await drain_running(actions.pool, "assertions_cold") is False


def test_ordinary_nightly_tiering_does_not_hold_the_drain_flag() -> None:
    """Live: tonight's run would move 472 rows. That is not a bulk drain, and holding the flag
    made the night compaction skip assertions_hot and assertions_cold every night."""
    from src.workers.arq_worker import _TIERING_DRAIN_ROWS, hot_cold_drain_running

    quiet = dict(layout_finished=True, fold_finished=True, tiering_paused=False)
    assert hot_cold_drain_running(rows_moved=0, **quiet) is False
    assert hot_cold_drain_running(rows_moved=472, **quiet) is False
    assert hot_cold_drain_running(rows_moved=_TIERING_DRAIN_ROWS, **quiet) is False


def test_a_real_bulk_drain_still_holds_the_flag() -> None:
    from src.workers.arq_worker import _TIERING_DRAIN_ROWS, hot_cold_drain_running

    base = dict(layout_finished=True, fold_finished=True, tiering_paused=False, rows_moved=0)
    assert hot_cold_drain_running(**{**base, "rows_moved": _TIERING_DRAIN_ROWS + 1}) is True
    assert hot_cold_drain_running(**{**base, "layout_finished": False}) is True
    assert hot_cold_drain_running(**{**base, "fold_finished": False}) is True
    assert hot_cold_drain_running(**{**base, "tiering_paused": True}) is True


async def test_the_flag_is_written_once_per_pass_and_cleared_when_the_pass_finishes(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The old flag was left true by a previous pass and every move re-set it. A finished pass
    with only ordinary tiering clears a stale 'running' flag."""
    import src.orchestrator.mailbox as mailbox
    from src.orchestrator.monitor import get_cursor
    from src.workers.arq_worker import storage_housekeeping_heartbeat

    async def _send(pool: Any, **kwargs: Any) -> dict[str, Any]:
        return {"sent": 1}

    monkeypatch.setattr(mailbox, "send_message", _send)
    await db_compaction.set_drain(actions.pool, ("assertions_hot", "assertions_cold"), True)
    await storage_housekeeping_heartbeat({"cascade": SimpleNamespace(actions=actions)})
    for table in ("assertions_hot", "assertions_cold"):
        flag = json.loads(await get_cursor(actions.pool, db_compaction.DRAIN_KEY + table) or "{}")
        assert flag.get("running") is False, table


async def test_a_failed_step_leaves_the_flag_as_it_was(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure says nothing about whether the drain is done, so the flag is not rewritten."""
    import src.orchestrator.assertion_fold as assertion_fold
    import src.orchestrator.mailbox as mailbox
    from src.orchestrator.monitor import get_cursor
    from src.workers.arq_worker import storage_housekeeping_heartbeat

    async def _send(pool: Any, **kwargs: Any) -> dict[str, Any]:
        return {"sent": 1}

    async def _boom(*a: Any, **k: Any) -> Any:
        raise RuntimeError("fold failed")

    monkeypatch.setattr(mailbox, "send_message", _send)
    monkeypatch.setattr(assertion_fold, "apply_fold", _boom)
    await db_compaction.set_drain(actions.pool, ("assertions_hot", "assertions_cold"), True)
    await storage_housekeeping_heartbeat({"cascade": SimpleNamespace(actions=actions)})
    flag = json.loads(
        await get_cursor(actions.pool, db_compaction.DRAIN_KEY + "assertions_hot") or "{}")
    assert flag.get("running") is True


def test_a_skip_names_the_rule_and_the_numbers(monkeypatch: pytest.MonkeyPatch) -> None:
    """The live cold table: 1.3 GB reclaimable of 1.7 GB used to be skipped with "not enough
    dead space", which read as a contradiction."""
    from src.orchestrator.db_compaction import skip_reason

    monkeypatch.setattr(db_compaction, "MIN_BLOAT_BYTES", 2 * GB)  # the real floor
    mb = 1024 ** 2
    assert skip_reason(1682 * mb, 1311 * mb, "soul_lines") == (
        "1.3 GB reclaimable is at or below this table's 2.0 GB floor")
    assert skip_reason(100 * GB, 20 * GB, "outbox") == (
        "20% reclaimable is at or below the 30% minimum")
    assert "256 MB floor" in skip_reason(1000 * mb, 200 * mb, "audit_log")
    assert skip_reason(0, 0, "outbox").endswith("minimum")


async def test_the_status_says_what_a_size_means(actions: Actions) -> None:
    status = await compaction_status(actions.pool)
    assert "heap, indexes and toast" in status["sizes"]


async def test_the_moved_rows_line_reaches_the_desk_even_when_another_step_failed(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The "moved N rows" line used to sit inside the all-three-steps-succeeded block, so a
    failed fold dropped it from the summary although the rows were counted as changed."""
    import src.orchestrator.assertion_fold as assertion_fold
    import src.orchestrator.mailbox as mailbox
    import src.orchestrator.migration_0064 as migration_0064
    from src.workers.arq_worker import storage_housekeeping_heartbeat

    bodies: list[str] = []

    async def _send(pool: Any, **kwargs: Any) -> dict[str, Any]:
        bodies.append(kwargs["body"])
        return {"sent": 1}

    async def _boom(*a: Any, **k: Any) -> Any:
        raise RuntimeError("fold failed")

    async def _moved(*a: Any, **k: Any) -> dict[str, Any]:
        return {"rows_moved": 7, "paused": False}

    monkeypatch.setattr(mailbox, "send_message", _send)
    monkeypatch.setattr(assertion_fold, "apply_fold", _boom)
    monkeypatch.setattr(migration_0064, "apply_migration_0064", _moved)
    await storage_housekeeping_heartbeat({"cascade": SimpleNamespace(actions=actions)})
    assert any("moved 7 rows of old history to the cold table" in b for b in bodies), bodies


def test_an_archive_step_that_left_rows_holds_the_flag_however_few_it_moved() -> None:
    """A run that stopped on its budget moves few rows yet leaves a backlog; the row-count
    threshold alone would clear the flag and let the night rewrite a half-drained table."""
    from src.workers.arq_worker import hot_cold_drain_running

    quiet = dict(layout_finished=True, fold_finished=True, tiering_paused=False)
    assert hot_cold_drain_running(rows_moved=40, more_remain=True, **quiet) is True
    assert hot_cold_drain_running(rows_moved=40, more_remain=False, **quiet) is False

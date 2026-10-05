"""The disk and WAL brake: every bulk-rewrite job checks it before each slice, pauses (logs,
no error) when the root disk is short of room or too much WAL was written in the last hour,
resumes by itself, and the status views say so."""
from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import disk_brake
from src.orchestrator.assertion_fold import apply_fold
from src.orchestrator.disk_brake import GB, brake_state, free_floor_bytes, pause_reason
from src.orchestrator.migration_0064 import apply_migration_0064
from src.orchestrator.monitor import set_cursor
from src.orchestrator.retention import (
    assert_property_audit_retirement,
    retire_layout_history,
)

TB = 1024 * GB


def _disk(monkeypatch: pytest.MonkeyPatch, free_gb: float, total_gb: float = 1000.0) -> None:
    monkeypatch.setattr(disk_brake, "disk_usage",
                        lambda: (int(free_gb * GB), int(total_gb * GB)))


def _wal(monkeypatch: pytest.MonkeyPatch, position_gb: float) -> None:
    async def _position(pool: Any) -> int:
        return int(position_gb * GB)

    monkeypatch.setattr(disk_brake, "_wal_position", _position)


async def _samples(actions: Actions, rows: list[list[float]]) -> None:
    await set_cursor(actions.pool, disk_brake.SAMPLES_KEY, json.dumps(rows))


@pytest.fixture(autouse=True)
def _clear_log_throttle() -> None:
    disk_brake._last_logged.clear()


def test_the_free_space_floor_is_the_larger_of_100_gb_and_a_tenth_of_the_disk() -> None:
    assert free_floor_bytes(500 * GB) == 100 * GB
    assert free_floor_bytes(2000 * GB) == 200 * GB


async def test_plenty_of_room_and_a_quiet_log_means_run(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disk(monkeypatch, 500)
    _wal(monkeypatch, 100)
    now = time.time()
    await _samples(actions, [[now - 1800, 99 * GB]])
    state = await brake_state(actions.pool, now=now)
    assert state["paused"] is False and state["label"] == "running"
    assert await pause_reason(actions.pool, "x") is None


async def test_low_free_space_pauses_with_the_reason(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disk(monkeypatch, 40)
    state = await brake_state(actions.pool)
    assert state["paused"] is True and state["label"] == "paused: disk/WAL budget"
    assert "40 GB is below the 100 GB floor" in state["reason"]
    assert await pause_reason(actions.pool, "x") is not None


async def test_too_much_wal_in_the_last_hour_pauses(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disk(monkeypatch, 500)
    _wal(monkeypatch, 150)
    now = time.time()
    await _samples(actions, [[now - 1800, 130 * GB]])  # 20 GB in half an hour
    state = await brake_state(actions.pool, now=now)
    assert state["paused"] is True
    assert state["wal_gb_last_hour"] == 20.0 and "over the 8 GB budget" in state["reason"]


async def test_wal_older_than_an_hour_does_not_count_and_it_resumes_by_itself(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disk(monkeypatch, 500)
    _wal(monkeypatch, 150)
    now = time.time()
    # the burst happened two hours ago; the only sample inside the hour is recent
    await _samples(actions, [[now - 7200, 100 * GB], [now - 600, 149.5 * GB]])
    state = await brake_state(actions.pool, now=now)
    assert state["paused"] is False and state["wal_gb_last_hour"] == 0.5


async def test_the_budget_and_floor_can_be_tuned_by_environment(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disk(monkeypatch, 150)
    monkeypatch.setenv("OSIRIS_DISK_FREE_FLOOR_GB", "200")
    assert (await brake_state(actions.pool))["paused"] is True
    monkeypatch.setenv("OSIRIS_DISK_FREE_FLOOR_GB", "50")
    assert (await brake_state(actions.pool))["paused"] is False


async def test_a_sample_is_recorded_at_most_once_a_minute(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator.monitor import get_cursor

    _wal(monkeypatch, 10)
    now = time.time()
    await _samples(actions, [[now - 30, 9 * GB]])
    await disk_brake.wal_written_last_hour(actions.pool, now=now)
    assert len(json.loads(await get_cursor(actions.pool, disk_brake.SAMPLES_KEY) or "[]")) == 1
    await disk_brake.wal_written_last_hour(actions.pool, now=now + 120)
    assert len(json.loads(await get_cursor(actions.pool, disk_brake.SAMPLES_KEY) or "[]")) == 2


async def test_a_brake_that_cannot_read_the_disk_pauses_the_job(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom() -> tuple[int, int]:
        raise OSError("no such device")

    monkeypatch.setattr(disk_brake, "disk_usage", _boom)
    reason = await pause_reason(actions.pool, "x")
    assert reason is not None and "could not read its state" in reason


# --- the real heartbeats -------------------------------------------------------------------

def _ctx(actions: Actions) -> dict[str, Any]:
    return {"cascade": SimpleNamespace(actions=actions)}


async def test_the_soul_encrypt_heartbeat_pauses_and_resumes(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.ingest.soul_crypto as soul_crypto
    from src.orchestrator import soul_encrypt_progress
    from src.workers.arq_worker import soul_encrypt_heartbeat

    calls: list[int] = []

    async def _tick(pool: Any, fernet: Any) -> dict[str, Any]:
        calls.append(1)
        return {"rows_done": 5}

    monkeypatch.setattr(soul_crypto, "get_soul_fernet", lambda: object())
    monkeypatch.setattr(soul_encrypt_progress, "encrypt_tick", _tick)
    monkeypatch.setattr(soul_encrypt_progress, "read_progress", lambda: {"rows_done": 0})

    _disk(monkeypatch, 10)
    assert await soul_encrypt_heartbeat(_ctx(actions)) == 0 and calls == []
    _disk(monkeypatch, 500)
    assert await soul_encrypt_heartbeat(_ctx(actions)) == 5 and calls == [1]


async def test_the_soul_recompress_heartbeat_pauses_before_training_or_rewriting(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.ingest.soul_crypto as soul_crypto
    from src.orchestrator import soul_recompress
    from src.workers.arq_worker import soul_recompress_heartbeat

    calls: list[str] = []

    async def _train(pool: Any, fernet: Any) -> dict[str, Any]:
        calls.append("train")
        return {}

    async def _tick(pool: Any, fernet: Any) -> dict[str, Any]:
        calls.append("tick")
        return {"rows_done": 3}

    monkeypatch.setattr(soul_crypto, "get_soul_fernet", lambda: object())
    monkeypatch.setattr(soul_recompress, "disabled", lambda: False)
    monkeypatch.setattr(soul_recompress, "maybe_train_dictionary", _train)
    monkeypatch.setattr(soul_recompress, "recompress_tick", _tick)
    monkeypatch.setattr(soul_recompress, "read_progress", lambda: {"rows_done": 0})

    _disk(monkeypatch, 10)
    assert await soul_recompress_heartbeat(_ctx(actions)) == 0 and calls == []
    _disk(monkeypatch, 500)
    assert await soul_recompress_heartbeat(_ctx(actions)) == 3 and calls == ["train", "tick"]


async def test_the_soul_cold_tier_heartbeat_pauses(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.ingest.soul_store import SoulStore
    from src.workers.arq_worker import soul_cold_tier_heartbeat

    calls: list[int] = []

    async def _fold(self: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append(1)
        return {"folded": [], "errors": [], "candidates": 0}

    monkeypatch.setattr(SoulStore, "fold_cold_tier_batch", _fold)
    _disk(monkeypatch, 10)
    assert await soul_cold_tier_heartbeat(_ctx(actions)) == 0 and calls == []
    _disk(monkeypatch, 500)
    await soul_cold_tier_heartbeat(_ctx(actions))
    assert calls == [1]


async def test_the_retention_heartbeat_pauses_and_then_deletes(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.orchestrator.mailbox as mailbox
    from src.workers.arq_worker import retention_heartbeat

    async def _send(pool: Any, **kwargs: Any) -> dict[str, Any]:
        return {"sent": 1}

    monkeypatch.setattr(mailbox, "send_message", _send)
    marker = "osiris_test_brake_marker"
    await actions.pool.execute(
        "INSERT INTO audit_log (action, actor, payload, created_at) "
        "VALUES ($1, 'agent:test', '{}', $2)", marker, datetime.now(UTC) - timedelta(days=100))

    async def _count() -> int:
        return int(await actions.pool.fetchval(
            "SELECT count(*) FROM audit_log WHERE action=$1", marker))

    _disk(monkeypatch, 10)
    assert await retention_heartbeat(_ctx(actions)) == 0
    assert await _count() == 1
    _disk(monkeypatch, 500)
    assert await retention_heartbeat(_ctx(actions)) >= 1
    assert await _count() == 0


async def test_the_graph_layout_heartbeat_pauses(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.orchestrator.graph_layout as graph_layout
    from src.workers.arq_worker import graph_layout_heartbeat

    calls: list[int] = []

    async def _batch(a: Any, **kwargs: Any) -> int:
        calls.append(1)
        return 2

    monkeypatch.setattr(graph_layout, "layout_batch", _batch)
    _disk(monkeypatch, 10)
    assert await graph_layout_heartbeat(_ctx(actions)) == 0 and calls == []
    _disk(monkeypatch, 500)
    assert await graph_layout_heartbeat(_ctx(actions)) == 2 and calls == [1]


async def test_the_housekeeping_heartbeat_does_nothing_while_paused_and_works_after(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.orchestrator.mailbox as mailbox
    from src.workers.arq_worker import storage_housekeeping_heartbeat

    async def _send(pool: Any, **kwargs: Any) -> dict[str, Any]:
        return {"sent": 1}

    monkeypatch.setattr(mailbox, "send_message", _send)
    old = datetime.now(UTC) - timedelta(days=30)
    obj = await actions.create_or_find_object("Domain", "brake-hk.example", "analyst:test")
    ids = []
    prev = None
    for minute in range(3):
        prev = await actions.pool.fetchval(
            "INSERT INTO assertions (object_id, name, value, source_id, observed_at, "
            " confidence, supersedes, evidence_class, is_current, created_at) "
            "VALUES ($1,'label','\"A\"','src:b',$2,0.9,$3,'authoritative_api',$4,$2) "
            "RETURNING id", obj, old + timedelta(minutes=minute), prev, minute == 2)
        ids.append(prev)

    async def _remaining() -> int:
        return int(await actions.pool.fetchval(
            "SELECT count(*) FROM assertions WHERE id = ANY($1::bigint[])", ids))

    _wal(monkeypatch, 200)
    _disk(monkeypatch, 500)
    await _samples(actions, [[time.time() - 1800, 100 * GB]])  # 100 GB in half an hour
    assert await storage_housekeeping_heartbeat(_ctx(actions)) == 0
    assert await _remaining() == 3
    # the window rolls over: no recent burst, the job runs and folds the repeated rows
    await _samples(actions, [[time.time() - 1800, 199.9 * GB]])
    assert await storage_housekeeping_heartbeat(_ctx(actions)) > 0
    assert await _remaining() == 2


# --- the long steps stop between batches -------------------------------------------------

async def _stop() -> str | None:
    return "the brake is on"


async def test_the_fold_stops_before_its_first_window_when_told_to(
    actions: Actions,
) -> None:
    out = await apply_fold(actions.pool, pause_check=_stop)
    assert out["folded"] == 0 and out["finished"] is False and out["paused"] == "the brake is on"


async def test_the_audit_and_layout_retirements_stop_when_told_to(actions: Actions) -> None:
    audit = await assert_property_audit_retirement(
        actions.pool, execute=True, pause_check=_stop)
    layout = await retire_layout_history(actions.pool, execute=True, pause_check=_stop)
    assert audit["paused"] == layout["paused"] == "the brake is on"
    assert audit["finished"] is False and layout["finished"] is False


async def test_the_archiver_stops_when_told_to(actions: Actions) -> None:
    out = await apply_migration_0064(actions.pool, pause_check=_stop)
    assert out["paused"] == "the brake is on" and out["rows_moved"] == 0


# --- the status views ---------------------------------------------------------------------

async def test_backup_status_carries_the_brake_reading(
    actions: Actions, tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator.compositions import _fn_backup_status

    _disk(monkeypatch, 10)
    out = await _fn_backup_status(
        actions.pool, None, {"vault": str(tmp_path / "v"), "backups": str(tmp_path / "b")})
    assert out["storage_brake"]["paused"] is True
    assert out["storage_brake"]["label"] == "paused: disk/WAL budget"

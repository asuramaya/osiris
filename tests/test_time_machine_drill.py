"""The monthly Time Machine drill: what it picks, when it is due, what it skips, what it records."""
from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from src.orchestrator import time_machine as tm
from src.orchestrator import time_machine_drill as dr

NOW = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
HOME = {"present": True, "reason": None, "dev": "eth0"}
AWAY = {"present": False, "reason": "reachable only over tailscale0"}


def _dump(name: str, when: datetime, available: bool = True) -> dict[str, Any]:
    return {"id": "restic-" + name, "kind": "restic", "when": when.isoformat(),
            "available": available, "label": f"dump {when:%Y-%m-%d}", "size_bytes": 1}


def test_due_is_monthly_and_waits_a_day_after_a_failure() -> None:
    assert dr.due({}, now=NOW)
    assert not dr.due({"last_passed_at": (NOW - timedelta(days=10)).isoformat()}, now=NOW)
    assert dr.due({"last_passed_at": (NOW - timedelta(days=29)).isoformat()}, now=NOW)
    failed = {"last_passed_at": (NOW - timedelta(days=40)).isoformat(),
              "last_attempt_at": (NOW - timedelta(hours=3)).isoformat(), "last_error": "x"}
    assert not dr.due(failed, now=NOW)
    failed["last_attempt_at"] = (NOW - timedelta(hours=30)).isoformat()
    assert dr.due(failed, now=NOW)


def test_the_kinds_alternate_starting_with_a_dump() -> None:
    assert dr.next_kind({}) == "dump"
    assert dr.next_kind({"last_kind": "dump"}) == "pitr"
    assert dr.next_kind({"last_kind": "pitr"}) == "dump"


def test_it_picks_the_oldest_dump_older_than_thirty_days_else_the_oldest() -> None:
    items = [_dump("a", NOW - timedelta(days=5)), _dump("b", NOW - timedelta(days=45)),
             _dump("c", NOW - timedelta(days=60)), _dump("d", NOW - timedelta(days=31))]
    assert dr.pick_dump(items, now=NOW)["id"] == "restic-c"
    young = [_dump("a", NOW - timedelta(days=5)), _dump("b", NOW - timedelta(days=9))]
    assert dr.pick_dump(young, now=NOW)["id"] == "restic-b"
    assert dr.pick_dump([], now=NOW) is None


GIB = 2**30


def _win(frm: str, to: str, base: str, segments: int = 0) -> dict[str, Any]:
    return {"from": frm, "to": to, "base": f"osiris-basebackup-{base}.tar.gz",
            "segments": segments}


def test_the_time_is_early_in_the_window_and_capped_by_the_log_budget() -> None:
    quiet = _win("2026-10-19T00:00:00+00:00", "2026-11-01T00:00:00+00:00", "20261018-205720")
    at, est = dr.plan_window(quiet, 4 * GIB) or ("", 0)
    # 13 days: a quarter in is 3.25 days, capped at the 2 days
    assert at == "2026-10-21T00:00:00+00:00" and est == 4 * GIB
    busy = _win("2026-10-05T00:00:00+00:00", "2026-10-13T00:00:00+00:00", "x", segments=34760)
    got = dr.plan_window(busy, 4 * GIB)            # 34760 segments in 8 days = 180 an hour
    assert got is not None
    when = datetime.fromisoformat(got[0])
    assert timedelta(hours=1) <= when - datetime(2026, 10, 5, tzinfo=UTC) < timedelta(days=2)
    assert got[1] <= dr.MAX_DRILL_BYTES + 1
    assert dr.plan_window(busy, dr.MAX_DRILL_BYTES) is None          # the base alone fills it
    assert dr.plan_window(busy, 46 * GIB) is None                    # a 46 GB base never fits


def test_it_takes_the_oldest_window_that_fits_and_skips_the_big_ones() -> None:
    windows = [
        _win("2026-11-16T02:21:00+00:00", "2026-11-18T03:00:00+00:00", "20261115-201856", 407),
        _win("2026-10-05T15:10:54+00:00", "2026-10-13T14:45:10+00:00", "20261005-092933", 34760),
        _win("2026-10-19T02:01:38+00:00", "2026-11-01T23:15:07+00:00", "20261018-205720", 2281)]
    sizes = {"osiris-basebackup-20261005-092933.tar.gz": 46 * GIB,
             "osiris-basebackup-20261018-205720.tar.gz": int(3.8 * GIB),
             "osiris-basebackup-20261115-201856.tar.gz": int(3.0 * GIB)}
    asked: list[str] = []

    def size_of(base: str, _from: datetime) -> int | None:
        asked.append(base)
        return sizes.get(base)

    mid, at, est, skipped = dr.pick_pitr(windows, size_of) or ("", "", 0, [])
    assert mid == "pitr-osiris-basebackup-20261018-205720"          # not the 46 GB one
    assert asked[0].endswith("20261005-092933.tar.gz") and len(asked) == 2   # oldest first
    assert at.startswith("2026-10-2") and est <= dr.MAX_DRILL_BYTES
    # the skipped window is named with its size, so the operator can see why it is not drilled
    assert [s["base"] for s in skipped] == ["osiris-basebackup-20261005-092933.tar.gz"]
    assert skipped[0]["base_bytes"] == 46 * GIB and "over the 12 GiB budget" in skipped[0]["why"]
    assert dr.pick_pitr([], size_of) is None
    assert dr.pick_pitr(windows, lambda b, a: None) is None          # nothing could be sized
    assert dr.pick_pitr([{"from": "x", "to": "y"}], size_of) is None


def test_the_status_line_for_the_at_home_list() -> None:
    assert dr.status_line({}, now=NOW)["state"] == "never"
    ok = {"last_passed_at": (NOW - timedelta(days=3)).isoformat(), "last_kind": "pitr"}
    assert dr.status_line(ok, now=NOW)["state"] == "passed"
    old = {"last_passed_at": (NOW - timedelta(days=50)).isoformat()}
    assert dr.status_line(old, now=NOW)["state"] == "overdue"
    bad = {**ok, "last_error": "the copy from the NAS failed"}
    line = dr.status_line(bad, now=NOW)
    assert line["state"] == "failed" and "copy from the NAS failed" in line["text"]


_WORLD_WINDOW = {"from": "2026-10-05T15:10:54+00:00", "to": "2026-10-13T14:45:10+00:00",
                 "base": "osiris-basebackup-20261005-092933.tar.gz", "segments": 0}


@pytest.fixture
def world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """A cache of NAS dumps, a restore map, no vault, and receipts in the test's own place."""
    from src.orchestrator import time_machine_restic as restic

    restic._write_cache({"as_of": NOW.isoformat(), "repo": "/r", "snapshots": 3,
                         "target": {"name": "nas", "path_or_url": "/r"},
                         "dumps": [
                             {"name": "osiris-20261001-043000.dump", "snapshot": "s1",
                              "snapshot_time": "", "path": "/v/osiris-20261001-043000.dump",
                              "size": 1000}]})
    (tmp_path / "rp.json").write_text(json.dumps({"windows": [
        {"from": "2026-10-05T15:10:54+00:00", "to": "2026-10-13T14:45:10+00:00",
         "base": "osiris-basebackup-20261005-092933.tar.gz"}], "gaps": []}))
    monkeypatch.setenv("OSIRIS_RESTORE_POINTS_FILE", str(tmp_path / "rp.json"))
    monkeypatch.setattr("src.orchestrator.network_presence.lan_presence", lambda _t: HOME)
    return {}


class _Pool:
    """Stands in for the pool onto the opened throwaway."""

    def __init__(self, objects: int = 5, newest: datetime | None = None,
                 assertion: datetime | None = None) -> None:
        self.objects, self.newest, self.assertion = objects, newest, assertion

    async def fetchval(self, sql: str, *a: Any) -> Any:
        if "count(*) FROM objects" in sql:
            return self.objects
        if "max(created_at) FROM objects" in sql:
            return self.newest
        if "max(created_at) FROM assertions" in sql:
            return self.assertion
        raise AssertionError(sql)

    async def close(self) -> None:
        return None


def _fake_rec() -> tm.Throwaway:
    return tm.Throwaway(id="x", container="c", volume="v", port=1, moment={}, started_at=0.0,
                        expires_at=1.0, state="ready", reader_password="pw")


@pytest.mark.asyncio
async def test_not_due_does_nothing(world: dict[str, Any]) -> None:
    dr._write_receipt({"last_passed_at": (NOW - timedelta(days=3)).isoformat()})
    out = await dr.run_drill(None, now=NOW, say=lambda _t: None)
    assert out == {"skipped": "not due"}


@pytest.mark.asyncio
async def test_away_from_home_is_a_skip_with_a_note_and_no_failure(
        world: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.orchestrator.network_presence.lan_presence", lambda _t: AWAY)
    started: list[int] = []
    out = await dr.run_drill(None, now=NOW, say=lambda _t: None,
                             start=lambda m: started.append(1) or _fake_rec())
    assert "home network" in out["skipped"] and started == []
    r = dr.read_receipt()
    assert r["last_skipped"]["why"] == out["skipped"]
    assert not r.get("last_error") and "last_attempt_at" not in r     # not a failed attempt
    assert dr.due(r, now=NOW)                                         # asks again next time


@pytest.mark.asyncio
async def test_a_busy_lock_or_short_disk_is_a_skip_not_a_failure(
        world: dict[str, Any]) -> None:
    for sentence in ("A backup to the NAS is running right now. Try again in a few minutes.",
                     "There is not enough disk to open that time: it needs about 30 GiB free."):
        def start(_m: Any, s: str = sentence) -> tm.Throwaway:
            raise tm.TimeMachineError(s)

        out = await dr.run_drill(None, now=NOW, say=lambda _t: None, start=start)
        assert out["skipped"] == sentence
        assert not dr.read_receipt().get("last_error")


@pytest.mark.asyncio
async def test_an_open_throwaway_is_never_disturbed(world: dict[str, Any]) -> None:
    tm._write_record(_fake_rec())
    started: list[int] = []
    out = await dr.run_drill(None, now=NOW, say=lambda _t: None,
                             start=lambda m: started.append(1) or _fake_rec())
    assert out["skipped"] == "a past moment is already open" and started == []


@pytest.mark.asyncio
async def test_a_failed_open_is_a_failure_with_a_receipt_a_teardown_and_a_desk_line(
        world: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    torn: list[int] = []
    monkeypatch.setattr(tm, "teardown", lambda **_k: torn.append(1) or [])
    told: list[str] = []

    async def tell(_pool: Any, kind: str, moment: tm.Moment, error: str) -> None:
        told.append(error)

    monkeypatch.setattr(dr, "_tell_desk", tell)

    def start(_m: Any) -> tm.Throwaway:
        raise tm.TimeMachineError("The NAS no longer holds the base backup for that window.")

    out = await dr.run_drill(None, kind="pitr", force=True, now=NOW, say=lambda _t: None,
                             start=start, base_size=lambda b, a: 3 * GIB)
    assert out["ok"] is False and "base backup" in out["error"]
    assert torn == [1] and told == [out["error"]]
    r = dr.read_receipt()
    assert r["last_error"] == out["error"] and r["last_kind"] == "pitr"
    assert r["history"][0]["ok"] is False
    assert not dr.due(r, now=NOW + timedelta(hours=2))               # retried no sooner than a day


@pytest.mark.asyncio
async def test_a_good_pitr_drill_passes_and_records(
        world: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncpg

    monkeypatch.setattr(tm, "teardown", lambda **_k: [])
    at = dr.plan_window(_WORLD_WINDOW, 3 * GIB)[0]            # type: ignore[index]
    asked = datetime.fromisoformat(at)
    pool = _Pool(assertion=asked - timedelta(minutes=2))

    async def make(*_a: Any, **_k: Any) -> _Pool:
        return pool

    monkeypatch.setattr(asyncpg, "create_pool", make)
    seen: list[tm.Moment] = []

    def start(m: tm.Moment) -> tm.Throwaway:
        seen.append(m)
        return _fake_rec()

    out = await dr.run_drill(None, kind="pitr", now=NOW, say=lambda _t: None, start=start,
                             base_size=lambda b, a: 3 * GIB)
    assert out["ok"] is True and seen[0].at == at
    r = dr.read_receipt()
    assert r["last_passed_at"] == NOW.isoformat() and r["last_error"] is None
    assert r["last_kind"] == "pitr" and r["history"][0]["ok"] is True
    assert dr.next_kind(r) == "dump" and not dr.due(r, now=NOW + timedelta(days=3))


@pytest.mark.asyncio
async def test_a_pitr_that_holds_data_after_the_time_asked_for_fails(
        world: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncpg

    monkeypatch.setattr(tm, "teardown", lambda **_k: [])
    late = datetime.fromisoformat(dr.plan_window(_WORLD_WINDOW, 3 * GIB)[0])  # type: ignore[index]
    pool = _Pool(assertion=late + timedelta(hours=5))                    # five hours too late

    async def make(*_a: Any, **_k: Any) -> _Pool:
        return pool

    monkeypatch.setattr(asyncpg, "create_pool", make)

    async def tell(*_a: Any) -> None:
        return None

    monkeypatch.setattr(dr, "_tell_desk", tell)
    out = await dr.run_drill(None, kind="pitr", now=NOW, say=lambda _t: None,
                             start=lambda m: _fake_rec(), base_size=lambda b, a: 3 * GIB)
    assert out["ok"] is False and "later than the time asked for" in out["error"]


@pytest.mark.asyncio
async def test_scratch_left_behind_fails_an_otherwise_good_drill(
        world: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import asyncpg
    from src.orchestrator import time_machine_restic as restic

    monkeypatch.setattr(tm, "teardown", lambda **_k: [])
    (restic.scratch_root() / "osiris-tm-leftover").mkdir(parents=True)
    pool = _Pool(assertion=datetime.fromisoformat(
        dr.plan_window(_WORLD_WINDOW, 3 * GIB)[0]) - timedelta(minutes=2))  # type: ignore[index]

    async def make(*_a: Any, **_k: Any) -> _Pool:
        return pool

    monkeypatch.setattr(asyncpg, "create_pool", make)

    async def tell(*_a: Any) -> None:
        return None

    monkeypatch.setattr(dr, "_tell_desk", tell)
    out = await dr.run_drill(None, kind="pitr", now=NOW, say=lambda _t: None,
                             start=lambda m: _fake_rec(), base_size=lambda b, a: 3 * GIB)
    assert out["ok"] is False and "osiris-tm-leftover" in out["error"]


@pytest.mark.asyncio
async def test_the_dump_drill_opens_the_chosen_nas_dump_and_makes_a_plan(
        world: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
        actions: Any) -> None:
    import asyncpg
    from src.orchestrator import time_machine_browse as browse
    from src.orchestrator import time_machine_restore as restore

    monkeypatch.setattr(tm, "teardown", lambda **_k: [])
    pool = _Pool(newest=datetime(2026, 9, 30, 10, 0, tzinfo=UTC))

    async def make(*_a: Any, **_k: Any) -> _Pool:
        return pool

    monkeypatch.setattr(asyncpg, "create_pool", make)

    async def listed(_pool: Any, **_k: Any) -> dict[str, Any]:
        return {"rows": [{"id": "abcdef12-0000-0000-0000-000000000000"}]}

    async def planned(_m: Any, _a: Any, ref: str, **_k: Any) -> dict[str, Any]:
        planned_for.append(ref)
        return {"digest": "d1g3st"}

    planned_for: list[str] = []
    monkeypatch.setattr(browse, "list_objects", listed)
    monkeypatch.setattr(restore, "plan_restore", planned)
    seen: list[tm.Moment] = []

    def start(m: tm.Moment) -> tm.Throwaway:
        seen.append(m)
        return _fake_rec()

    out = await dr.run_drill(actions.pool, kind="dump", now=NOW, say=lambda _t: None,
                             start=start)
    assert out["ok"] is True and seen[0].kind == "restic"
    assert seen[0].id == "restic-osiris-20261001-043000"
    assert planned_for == ["abcdef12-0000-0000-0000-000000000000"]    # a plan, nothing applied


def test_the_unit_is_a_daily_ask_that_never_applies_anything() -> None:
    root = Path(__file__).resolve().parent.parent / "deploy"
    service = (root / "osiris-time-machine-drill.service").read_text()
    assert "time-machine drill" in service and "apply" not in service.lower().replace(
        "never applies", "")
    assert "Persistent=true" in (root / "osiris-time-machine-drill.timer").read_text()


@pytest.mark.asyncio
async def test_when_no_window_fits_the_budget_it_is_a_plain_skip_and_nothing_is_pulled(
        world: dict[str, Any]) -> None:
    started: list[int] = []
    out = await dr.run_drill(None, kind="pitr", now=NOW, say=lambda _t: None,
                             start=lambda m: started.append(1) or _fake_rec(),
                             base_size=lambda b, a: 46 * GIB)
    assert "small enough for a monthly check" in out["skipped"] and started == []
    assert not dr.read_receipt().get("last_error")


@pytest.mark.asyncio
async def test_the_receipt_says_which_windows_were_not_drilled_and_why(
        world: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncpg

    monkeypatch.setattr(tm, "teardown", lambda **_k: [])
    (Path(os.environ["OSIRIS_RESTORE_POINTS_FILE"])).write_text(json.dumps({"windows": [
        _win("2026-10-05T15:10:54+00:00", "2026-10-13T14:45:10+00:00", "20261005-092933", 34760),
        _win("2026-10-19T02:01:38+00:00", "2026-11-01T23:15:07+00:00", "20261018-205720", 2281)],
        "gaps": []}))
    sizes = {"osiris-basebackup-20261005-092933.tar.gz": 46 * GIB,
             "osiris-basebackup-20261018-205720.tar.gz": int(3.8 * GIB)}
    when = datetime(2026, 10, 21, 2, 1, 38, tzinfo=UTC)
    pool = _Pool(assertion=when - timedelta(minutes=1))

    async def make(*_a: Any, **_k: Any) -> _Pool:
        return pool

    monkeypatch.setattr(asyncpg, "create_pool", make)
    out = await dr.run_drill(None, kind="pitr", now=NOW, say=lambda _t: None,
                             start=lambda m: _fake_rec(), base_size=lambda b, a: sizes[b])
    assert out["ok"] is True
    r = dr.read_receipt()
    assert [w["base_bytes"] for w in r["windows_not_drilled"]] == [46 * GIB]
    assert r["history"][0]["windows_not_drilled"][0]["why"].startswith("over the 12 GiB")
    line = dr.status_line({**r, "last_passed_at": NOW.isoformat()}, now=NOW)
    assert "too big" in line["text"] and "Oct 05-Oct 13 (base 46 GB)" in line["text"]

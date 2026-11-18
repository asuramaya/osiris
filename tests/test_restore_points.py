"""What can be restored and from when: windows, gaps, days without a dump, and the console line."""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from src.actions.core import Actions
from src.api.app import create_app
from src.orchestrator import restore_points as rp

T0 = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)


def at(hours: float) -> str:
    return (T0 + timedelta(hours=hours)).isoformat()


def wal(n: int, hours: float, ext: str = ".zst") -> tuple[str, dict[str, Any]]:
    return f"00000001000001{n // 256:02X}{n % 256:08X}{ext}", {"kind": "wal", "mtime": at(hours)}


def files(*items: Any) -> dict[str, dict[str, Any]]:
    return dict(items)


def base(name: str, hours: float, verified: bool = True) -> list[tuple[str, dict[str, Any]]]:
    out = [(name, {"kind": "base", "mtime": at(hours)})]
    if verified:
        out.append((name + ".verified", {"kind": "verified", "mtime": at(hours)}))
    return out


def test_classify_picks_only_what_restoring_needs() -> None:
    home = "/home/u/osiris-vault"
    assert rp.classify(f"{home}/wal_archive/000000010000010B000000A6.zst") == (
        "wal", "000000010000010B000000A6.zst")
    assert rp.classify(f"{home}/wal_archive/000000010000010B000000A6.00000028.backup") is None
    assert rp.classify(f"{home}/basebackups/osiris-basebackup-20261101-165136.tar.gz")[0] == "base"
    assert rp.classify(f"{home}/basebackups/osiris-basebackup-20261101-165136.tar.gz.verified") == (
        "verified", "osiris-basebackup-20261101-165136.tar.gz.verified")
    assert rp.classify(f"{home}/osiris-20261101-165136.dump")[0] == "dump"
    assert rp.classify(f"{home}/osiris-repo.bundle") is None
    assert rp.classify(f"{home}/osiris-recovery/soul.key.recovery.json") is None


def test_an_unbroken_run_after_a_base_is_a_window_and_a_hole_is_a_gap() -> None:
    f = files(*base("osiris-basebackup-A.tar.gz", 1),
              *[wal(n, 2 + n * 0.1) for n in range(10, 20)],      # run 1: segments 10..19
              *[wal(n, 100 + n * 0.1) for n in range(30, 36)],    # run 2 after a hole
              *base("osiris-basebackup-B.tar.gz", 102.5))

    out = rp.compute(f)

    assert [w["base"] for w in out["windows"]] == ["osiris-basebackup-A.tar.gz",
                                                   "osiris-basebackup-B.tar.gz"]
    assert out["windows"][0]["segments"] == 10 and out["windows"][1]["segments"] == 6
    assert all(w["base_verified"] for w in out["windows"])  # the .verified markers are read
    assert len(out["gaps"]) == 1
    assert out["gaps"][0]["missing_segments"] == 10  # 20..29
    assert out["gaps"][0]["from"] == at(2 + 19 * 0.1).replace("+00:00", "+00:00")
    assert out["oldest_restorable"] == at(1)
    assert out["wal_segments"] == 16 and out["base_backups"] == 2


def test_a_run_with_no_base_backup_is_not_a_window_and_an_unverified_base_is_marked() -> None:
    lone = rp.compute(files(*[wal(n, n * 0.1) for n in range(5)]))
    unverified = rp.compute(files(*base("osiris-basebackup-U.tar.gz", 0, verified=False),
                                  *[wal(n, 1 + n * 0.1) for n in range(5)]))

    assert lone["windows"] == [] and lone["oldest_restorable"] is None
    assert unverified["windows"][0]["base_verified"] is False


def test_days_without_a_dump_are_listed_between_the_first_and_last_dump() -> None:
    f = files(("osiris-20261101-040000.dump", {"kind": "dump", "mtime": at(12)}),
              ("osiris-20261104-040000.dump", {"kind": "dump", "mtime": at(24 * 3 + 12)}))

    out = rp.compute(f)

    assert len(out["days_without_a_dump"]) == 2 and out["dumps"] == 2
    assert out["oldest_restorable"] == at(12)  # a dump is a restore point too


def test_collect_reads_snapshots_a_few_at_a_time_and_keeps_every_name_once() -> None:
    calls: list[list[str]] = []
    vault = "/home/u/osiris-vault"

    def run(args: list[str], *, timeout: float) -> Any:
        calls.append(args)
        ids = [args[i + 1] for i, a in enumerate(args) if a == "--snapshot"]
        matches = [{"type": "file", "mtime": at(5), "path": f"{vault}/wal_archive/"
                    f"0000000100000001{int(i[1:]):08X}.zst"} for i in ids]
        matches.append({"type": "file", "mtime": at(5), "path": f"{vault}/notes.txt"})
        matches.append({"type": "dir", "mtime": at(5), "path": f"{vault}/wal_archive"})
        return SimpleNamespace(returncode=0, stdout=json.dumps([{"matches": matches}]), stderr="")

    snaps = [{"id": f"s{i}"} for i in range(5)]

    got = rp.collect(run, snaps, chunk=2)

    assert [len([a for a in c if a == "--snapshot"]) for c in calls] == [2, 2, 1]
    assert len(got) == 5 and all(v["kind"] == "wal" for v in got.values())
    assert all(c[-1] == "*" and "--no-lock" in c for c in calls)


def test_the_console_line_reads_live_facts_and_the_saved_map(tmp_path: Path) -> None:
    now = T0 + timedelta(days=2)
    cache = {"as_of": at(24), "windows": [{"from": at(1), "to": at(30)}], "gaps": [],
             "days_without_a_dump": [], "oldest_restorable": at(1), "snapshots": 40,
             "repo_bytes": 900 * 10**9}
    offload = {"last_successful_offload": at(40), "last_backup_end": at(40.1),
               "last_attempt_at": at(40)}
    thinning = {"approved": {"at": "x"}, "last_plan": {"id": "abc", "at": "y", "remove": ["s1"],
                                                       "freed_bytes": 5, "total": 40}}

    out = rp.status(offload=offload, thinning=thinning, ceiling_gb=800, cache=cache, now=now)

    assert out["known"] and out["last_backup"] == at(40.1)
    assert out["next_expected"] == (T0 + timedelta(hours=40, minutes=15)).isoformat()
    assert out["oldest_restorable"] == at(1) and out["snapshots"] == 40
    assert out["over_ceiling"] is True and out["stale"] is False
    assert out["thinning"]["approved"] is True and out["thinning"]["plan"]["to_forget"] == 1
    assert out["overdue"] is False and out["overdue_since"] is None
    old = rp.status(offload=offload, thinning={}, cache=cache, now=T0 + timedelta(days=20))
    assert old["stale"] is True
    assert old["overdue"] is True  # 20 days on, the last backup was 18 days ago (limit 7)
    assert old["overdue_since"] == at(40.1 + 7 * 24).replace("+00:00", "+00:00")
    assert rp.status(offload=offload, thinning={}, cache=cache, now=T0 + timedelta(days=20),
                     stale_days=0)["overdue"] is False
    never = rp.status(offload={}, thinning={}, cache=None, now=now)
    assert never["known"] is False and "restore map has not been built" in never["text"]


def test_refresh_saves_the_map_and_reads_back(monkeypatch: pytest.MonkeyPatch,
                                              tmp_path: Path) -> None:
    monkeypatch.setenv(rp.CACHE_ENV, str(tmp_path / "p.json"))
    vault = "/home/u/osiris-vault"

    def run(args: list[str], *, timeout: float) -> Any:
        if args[0] == "snapshots":
            return SimpleNamespace(returncode=0, stderr="", stdout=json.dumps(
                [{"id": "a", "time": at(1)}, {"id": "b", "time": at(9)}]))
        matches = [{"type": "file", "mtime": at(1), "path":
                    f"{vault}/basebackups/osiris-basebackup-20261101-000000.tar.gz"},
                   {"type": "file", "mtime": at(2), "path": f"{vault}/wal_archive/"
                    "000000010000000100000001.zst"}]
        return SimpleNamespace(returncode=0, stdout=json.dumps([{"matches": matches}]), stderr="")

    out = rp.refresh({"name": "nas"}, run, repo_bytes=lambda t: 123, now=T0)

    assert out["snapshots"] == 2 and out["repo_bytes"] == 123 and len(out["windows"]) == 1
    assert out["oldest_snapshot"] == at(1) and out["newest_snapshot"] == at(9)
    assert rp.read_cache()["repo_bytes"] == 123  # type: ignore[index]
    (tmp_path / "p.json").write_text("{broken")
    assert rp.read_cache() is None


async def test_the_api_route_answers_from_the_saved_map_without_the_nas(
        actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from src.orchestrator.manifests import load_manifests

    monkeypatch.setenv(rp.CACHE_ENV, str(tmp_path / "p.json"))
    monkeypatch.setenv("OSIRIS_REPO_THINNING_FILE", str(tmp_path / "t.json"))
    (tmp_path / "p.json").write_text(json.dumps({
        "as_of": datetime.now(UTC).isoformat(), "windows": [], "gaps": [], "snapshots": 7,
        "days_without_a_dump": [], "oldest_restorable": None, "repo_bytes": 10}))
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(Path(__file__).resolve().parent.parent / "helpers")

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 1)),
                                 base_url="http://test") as client:
        resp = await client.get("/backup/restore-points")

    assert resp.status_code == 200 and resp.headers["cache-control"] == "no-store"
    assert resp.json()["known"] is True and resp.json()["snapshots"] == 7


async def test_the_thinning_routes_are_loopback_only_apply_a_reviewed_id_and_refuse_a_stale_one(
        actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from src.orchestrator import repo_thinning
    from src.orchestrator.manifests import load_manifests

    monkeypatch.setenv(repo_thinning.STATE_ENV, str(tmp_path / "t.json"))
    seen: list[str | None] = []

    async def fake_command(*, pool: Any, apply_id: str | None = None, say: Any = print,
                           **kw: Any) -> int:
        seen.append(apply_id)
        say("repo-thin: that plan is not the latest one or is more than a day old; run again.")
        return 1

    monkeypatch.setattr(repo_thinning, "command", fake_command)
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(Path(__file__).resolve().parent.parent / "helpers")

    def client(host: str) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=(host, 1)),
                                 base_url="http://test")

    async with client("192.168.1.50") as far:
        assert (await far.post("/backup/thinning/apply", json={"plan_id": "a" * 12})
                ).status_code == 403
        assert (await far.get("/backup/thinning")).status_code == 403
    async with client("127.0.0.1") as here:
        bad = await here.post("/backup/thinning/apply", json={"plan_id": "not-an-id"})
        unknown = await here.post("/backup/thinning/burn")
        stale = await here.post("/backup/thinning/apply", json={"plan_id": "abcdef012345"})
        plan = await here.post("/backup/thinning/plan")
        state = await here.get("/backup/thinning")

    assert bad.status_code == 400 and unknown.status_code == 404
    assert stale.headers["cache-control"] == "no-store"
    rows = [json.loads(x) for x in stale.text.splitlines()]
    assert rows[-1]["ok"] is False and "not the latest one" in rows[-1]["text"]
    assert seen == ["abcdef012345", None] and plan.status_code == 200
    assert state.status_code == 200 and state.json()["plan"] is None

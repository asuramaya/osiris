"""NAS dump moments, against a REAL throwaway restic repository on local disk (no NAS)."""
from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from src.orchestrator import time_machine as tm
from src.orchestrator import time_machine_restic as tr

from tests.test_time_machine import FakeDocker

pytestmark = pytest.mark.skipif(shutil.which("restic") is None, reason="restic is not installed")

A, B = "osiris-20261105-043002.dump", "osiris-20261106-043002.dump"
HOME = {"present": True, "reason": None, "dev": "eth0"}


def _restic(env: dict[str, str], *args: str) -> None:
    subprocess.run(["restic", *args], env=env, check=True, capture_output=True, timeout=120)


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    vault = tmp_path / "osiris-vault"
    vault.mkdir()
    url = str(tmp_path / "repo")
    env = {**os.environ, "RESTIC_REPOSITORY": url, "RESTIC_PASSWORD": "pw", "RESTIC_CACHE_DIR":
           str(tmp_path / "cache")}
    _restic(env, "init")
    (vault / A).write_bytes(b"first dump " * 100)
    _restic(env, "backup", str(vault))
    (vault / B).write_bytes(b"second dump " * 100)
    _restic(env, "backup", str(vault))
    monkeypatch.setenv(tr.CACHE_ENV, str(tmp_path / "tm_restic.json"))
    monkeypatch.setenv("OSIRIS_TM_SCRATCH", str(tmp_path / "scratch"))
    (tmp_path / "scratch").mkdir()
    local_vault = tmp_path / "local-vault"
    local_vault.mkdir()
    monkeypatch.setattr(tm, "VAULT_DIR", local_vault)
    target = {"name": "nas", "path_or_url": url}
    return {"env": env, "target": target, "vault": vault, "local": local_vault,
            "sizes": {A: (vault / A).stat().st_size, B: (vault / B).stat().st_size}}


def _deps(repo: dict[str, Any], *, presence: dict[str, Any] | None = None,
          lock: bool = True) -> tr.Deps:
    from src.orchestrator import restic_locks

    @contextlib.contextmanager
    def hold() -> Iterator[bool]:
        yield lock

    def run(args: list[str], *, timeout: float, text: bool = True) -> Any:
        return restic_locks.run_restic(args, repo["env"], timeout=timeout, text=text)

    return tr.Deps(run=run, presence=lambda _t: presence or HOME, hold_lock=hold)


def test_refresh_lists_each_dump_once_from_the_newest_snapshot(repo: dict[str, Any]) -> None:
    body = tr.refresh(repo["target"], _deps(repo))
    names = [d["name"] for d in body["dumps"]]
    assert names == [B, A] and body["snapshots"] == 2
    a = next(d for d in body["dumps"] if d["name"] == A)
    assert a["size"] == repo["sizes"][A]
    assert (tr.read_cache() or {})["dumps"] == body["dumps"]
    assert oct(tr.cache_path().stat().st_mode & 0o777) == "0o600"


def test_the_list_hides_dumps_the_vault_still_has_and_reports_availability(
        repo: dict[str, Any]) -> None:
    tr.refresh(repo["target"], _deps(repo))
    (repo["local"] / A).write_bytes(b"x")
    items = tr.restic_moments(repo["local"], presence=lambda _t: HOME)
    assert [i["id"] for i in items] == ["restic-" + B.removesuffix(".dump")]
    assert items[0]["available"] is True and items[0]["kind"] == "restic"
    away = tr.restic_moments(repo["local"], presence=lambda _t: {
        "present": False, "reason": "reachable only over tailscale0"})
    assert away[0]["available"] is False and "home network" in away[0]["reason"]
    assert "tailscale0" in away[0]["reason"]
    broken = tr.restic_moments(repo["local"], presence=lambda _t: 1 / 0)
    assert broken[0]["available"] is False


def test_no_saved_list_means_no_nas_moments(repo: dict[str, Any]) -> None:
    assert tr.restic_moments(repo["local"], presence=lambda _t: HOME) == []


def test_refresh_refuses_away_and_changes_nothing(repo: dict[str, Any]) -> None:
    with pytest.raises(tm.TimeMachineError, match="home network"):
        tr.refresh(repo["target"], _deps(repo, presence={"present": False, "reason": "vpn"}))
    assert tr.read_cache() is None


def test_pull_copies_exactly_that_dump_read_only(repo: dict[str, Any], tmp_path: Path) -> None:
    tr.refresh(repo["target"], _deps(repo))
    m = tr.find_restic_moment("restic-" + A.removesuffix(".dump"))
    assert m.kind == "restic" and m.snapshot and m.size_bytes == repo["sizes"][A]
    got = tr.pull(m, tmp_path / "scratch" / "osiris-tm-x", _deps(repo), repo["target"])
    assert got.read_bytes() == b"first dump " * 100
    assert [p.name for p in (tmp_path / "scratch" / "osiris-tm-x").rglob("*.dump")] == [A]
    # reads only: the repository holds no lock and still checks clean
    locks = subprocess.run(["restic", "list", "locks"], env=repo["env"], capture_output=True,
                           text=True, timeout=60)
    assert locks.stdout.strip() == ""


def test_pull_refuses_away_and_while_an_offload_holds_the_lock(
        repo: dict[str, Any], tmp_path: Path) -> None:
    tr.refresh(repo["target"], _deps(repo))
    m = tr.find_restic_moment("restic-" + A.removesuffix(".dump"))
    with pytest.raises(tm.TimeMachineError, match="only be opened at home"):
        tr.pull(m, tmp_path / "s1", _deps(repo, presence={"present": False}), repo["target"])
    with pytest.raises(tm.TimeMachineError, match="backup to the NAS is running"):
        tr.pull(m, tmp_path / "s2", _deps(repo, lock=False), repo["target"])
    assert not list((tmp_path / "s1").rglob("*.dump")) if (tmp_path / "s1").exists() else True


def test_a_copy_of_the_wrong_size_is_not_used(repo: dict[str, Any], tmp_path: Path) -> None:
    tr.refresh(repo["target"], _deps(repo))
    m = tr.find_restic_moment("restic-" + A.removesuffix(".dump"))
    bad = tm.Moment(**{**m.__dict__, "size_bytes": m.size_bytes + 1})
    with pytest.raises(tm.TimeMachineError, match="it was not used"):
        tr.pull(bad, tmp_path / "s", _deps(repo), repo["target"])


def test_a_failed_restic_is_a_sentence(repo: dict[str, Any], tmp_path: Path) -> None:
    tr.refresh(repo["target"], _deps(repo))
    m = tr.find_restic_moment("restic-" + A.removesuffix(".dump"))
    gone = tm.Moment(**{**m.__dict__, "snapshot": "deadbeef"})
    with pytest.raises(tm.TimeMachineError, match="Copying that backup from the NAS failed"):
        tr.pull(gone, tmp_path / "s", _deps(repo), repo["target"])
    with pytest.raises(tm.TimeMachineError, match="no such moment"):
        tr.find_restic_moment("restic-osiris-20990101-000000")


def test_the_pull_budget_grows_with_the_dump() -> None:
    assert tr.pull_budget_secs(0) == tr.PULL_BASE_SECS
    assert tr.pull_budget_secs(3 * 1024**3) > tr.pull_budget_secs(1024**3)


def _start(repo: dict[str, Any], tmp_path: Path, d: FakeDocker, **kw: Any) -> tm.Throwaway:
    tr.refresh(repo["target"], _deps(repo))
    m = tr.find_restic_moment("restic-" + B.removesuffix(".dump"))
    return tm.start(m, sh=d, record_path=tmp_path / "rec.json", sleep=lambda _s: None,
                    free=lambda _p: 10**12, **kw)


def test_start_pulls_restores_and_removes_the_scratch_copy(
        repo: dict[str, Any], tmp_path: Path) -> None:
    d = FakeDocker()
    seen: list[str] = []
    rec = _start(repo, tmp_path, d,
                 pull=lambda m, s: tr.pull(m, s, _deps(repo), repo["target"]),
                 on_phase=lambda r: seen.append(r.state))
    assert rec.state == "ready" and "pulling" in seen
    assert seen.index("pulling") < seen.index("restoring")
    run = next(c for c in d.calls if c[:3] == ["docker", "run", "-d"])
    mount = next(a for a in run if a.endswith("/tm/restore.dump:ro"))
    assert "/osiris-tm-" in mount and B in mount
    assert not list((tmp_path / "scratch").glob("osiris-tm-*"))       # removed after the restore
    assert tm.status(record_path=tmp_path / "rec.json")["state"] == "ready"   # type: ignore[index]


def test_a_failed_pull_leaves_nothing_and_says_why(
        repo: dict[str, Any], tmp_path: Path) -> None:
    d = FakeDocker()

    def away(m: tm.Moment, s: Path) -> Path:
        s.mkdir(parents=True, exist_ok=True)
        (s / "partial.dump").write_bytes(b"half")
        raise tm.TimeMachineError("Not on the home network. That backup is only on the NAS.")

    with pytest.raises(tm.TimeMachineError, match="home network"):
        _start(repo, tmp_path, d, pull=away)
    assert not d.containers and not d.volumes
    assert not list((tmp_path / "scratch").glob("osiris-tm-*"))
    assert tm.read_record(tmp_path / "rec.json") is None


def test_teardown_and_reap_remove_a_crashed_starts_scratch_copy(
        repo: dict[str, Any], tmp_path: Path) -> None:
    scratch = tmp_path / "scratch" / "osiris-tm-crashed"
    scratch.mkdir()
    (scratch / "x.dump").write_bytes(b"x")
    rec = tm.Throwaway(id="crashed", container="osiris-tm-crashed", volume="osiris-tm-vol-c",
                       port=1, moment={}, started_at=0.0, expires_at=1.0, state="restoring",
                       scratch=str(scratch))
    tm._write_record(rec, tmp_path / "rec.json")
    tm.reap(sh=FakeDocker(), record_path=tmp_path / "rec.json", now=lambda: 10.0)
    assert not scratch.exists()
    # only a directory this module made is ever removed
    other = tmp_path / "scratch" / "keep-me"
    other.mkdir()
    tr.remove_scratch(other)
    assert other.exists()


def test_the_public_status_never_shows_the_scratch_path(
        repo: dict[str, Any], tmp_path: Path) -> None:
    rec = tm.Throwaway(id="a", container="c", volume="v", port=1, moment={}, started_at=0.0,
                       expires_at=1.0, scratch="/var/tmp/osiris-tm-a")
    assert "scratch" not in rec.public() and rec.public()["state_text"]


@pytest.mark.asyncio
async def test_the_console_refuses_a_nas_dump_away_from_home(
        repo: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    from src.orchestrator import time_machine_console as tmc

    tr.refresh(repo["target"], _deps(repo))
    monkeypatch.setattr("src.orchestrator.network_presence.lan_presence",
                        lambda _t: {"present": False, "reason": "reachable only over tailscale0"})
    spawned: list[str] = []
    monkeypatch.setattr(tm, "spawn_run", lambda mid, **_k: spawned.append(mid) or 1)
    out = await tmc.start("restic-" + B.removesuffix(".dump"))
    assert "only be opened at home" in out["error"] and spawned == []
    listing = (await tmc.moments())["moments"]
    nas = [m for m in listing if m["kind"] == "restic"]
    assert nas and all(m["available"] is False for m in nas)


def test_refresh_map_is_the_weekly_runs_one_call(repo: dict[str, Any]) -> None:
    deps = _deps(repo)
    assert tr.refresh_map(repo["target"], deps.run, presence=lambda _t: HOME) is None
    assert [d["name"] for d in (tr.read_cache() or {})["dumps"]] == [B, A]
    seen: list[list[str]] = []

    def spy(args: list[str], *, timeout: float, text: bool = True) -> Any:
        seen.append(args)
        return deps.run(args, timeout=timeout, text=text)

    tr.refresh_map(repo["target"], spy, presence=lambda _t: HOME)
    assert seen and all("--no-lock" in a for a in seen)            # listings only, no lock taken
    assert {a[0] for a in seen} == {"snapshots", "find"}


def test_refresh_map_needs_the_home_proof_and_never_raises(repo: dict[str, Any]) -> None:
    deps = _deps(repo)
    called: list[int] = []

    def must_not_run(*_a: Any, **_k: Any) -> Any:
        called.append(1)

    away = tr.refresh_map(repo["target"], must_not_run, presence=lambda _t: {"present": False})
    assert away and "home network" in away and called == []
    assert tr.refresh_map(repo["target"], deps.run, presence=lambda _t: 1 / 0) is not None

    def broken(args: list[str], *, timeout: float, text: bool = True) -> Any:
        return subprocess.CompletedProcess(args, 1, "", "repository is locked")

    out = tr.refresh_map(repo["target"], broken, presence=lambda _t: HOME)
    assert out and "locked" in out

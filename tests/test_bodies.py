"""BodyProvider — the LOCAL tier (`7ff54707`: the default product tier, not a test double) and
the StubRaProvider seam that keeps Phase 3 (Ra/Xen) tested before its metal exists.

Never real systemd in a unit test: the ProcessRunner seam is a constructor param precisely so
these tests can fake the subprocess boundary. The one test that DOES touch real `systemd-run`
is marked skipif when it is not on PATH.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from src.orchestrator import bodies
from src.orchestrator.bodies import (
    LocalProvider,
    StubRaProvider,
    _build_receipt,
    _exit_cause,
    _read_cgroup_stats,
    _systemd_run_argv,
    _write_receipt,
)
from src.orchestrator.trigger import _spawn_in_body

_RECEIPT_KEYS = {
    "v", "handle", "provider", "kind", "core_seconds", "wall_seconds", "ram_envelope_bytes",
    "ram_peak_bytes", "ram_gib_seconds", "exit_cause", "started_at", "ended_at", "seat_anchor",
    "repo_ref", "budget_usd",
}

NOW = datetime(2026, 7, 15, 12, 0, 0, tzinfo=UTC)


# --- systemd-run command construction (pure — no subprocess at all) ---

def test_systemd_run_argv_carries_hard_and_soft_ceilings() -> None:
    argv = _systemd_run_argv("osiris-body-abc123", None, 2 * 2**30, ["claude", "-p", "hi"])
    assert argv[0] == "systemd-run"
    assert "--user" in argv and "--scope" in argv and "--collect" in argv
    assert "--unit=osiris-body-abc123" in argv
    assert "-p" in argv
    assert f"MemoryMax={2 * 2**30}" in argv          # the hard ceiling, in bytes, no unit suffix
    assert f"MemoryHigh={int(2 * 2**30 * 0.9)}" in argv  # 90% soft throttle, per the spec's number
    assert "AllowedCPUs=0" not in " ".join(argv)     # cores=None: no pinning at all
    assert argv[-4:] == ["--", "claude", "-p", "hi"]  # the payload trails the flags, unambiguous


def test_systemd_run_argv_bounds_io_so_a_body_cannot_starve_the_box() -> None:
    """Warp's lesson as architecture (1460e590): the envelope caps I/O, not just RAM+CPU. A
    body's scope gets IOAccounting (turns the io controller on) and a LOW IOWeight so it yields
    disk bandwidth under contention — it can never peg the SSD out from under foreground work."""
    argv = _systemd_run_argv("osiris-body-io", None, 10**9, ["true"])
    assert "IOAccounting=yes" in argv                 # the weight is inert without the controller
    assert "IOWeight=50" in argv                      # below systemd's default 100 → yields first
    # an override travels the whole way through (per-provider policy, LocalProvider(io_weight=))
    assert "IOWeight=10" in _systemd_run_argv("osiris-body-io", None, 10**9, ["true"], 10)


def test_systemd_run_argv_pins_cores_when_given() -> None:
    argv = _systemd_run_argv("osiris-body-x", 4, 10**9, ["true"])
    assert "-p" in argv and "AllowedCPUs=0-3" in argv   # first 4 logical CPUs, not a NUMA claim
    single = _systemd_run_argv("osiris-body-y", 1, 10**9, ["true"])
    assert "AllowedCPUs=0" in single                    # one core: a single index, not "0-0"


# --- receipt minting from a fake cgroup dir ---

def test_read_cgroup_stats_parses_real_cgroup_v2_files(tmp_path: Path) -> None:
    cg = tmp_path / "cgroup"
    cg.mkdir()
    (cg / "cpu.stat").write_text("usage_usec 2500000\nuser_usec 1000000\nsystem_usec 1500000\n")
    (cg / "memory.peak").write_text("104857600\n")
    (cg / "memory.events").write_text(
        "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n")
    core_seconds, ram_peak, oom_kill = _read_cgroup_stats(cg)
    assert core_seconds == pytest.approx(2.5)   # usage_usec / 1e6
    assert ram_peak == 104857600
    assert oom_kill == 0


def test_read_cgroup_stats_degrades_to_zero_when_files_are_missing(tmp_path: Path) -> None:
    """An already-collected scope (or a kernel with no memory.peak) must never crash the
    receipt — an honest zero beats a fabricated number, same law as ceiling.py's `blind`."""
    cg = tmp_path / "gone"  # never created
    core_seconds, ram_peak, oom_kill = _read_cgroup_stats(cg)
    assert (core_seconds, ram_peak, oom_kill) == (0.0, 0, 0)


# --- exit-cause mapping, incl. oom-kill ---

def test_exit_cause_maps_normal_exit_and_signal_and_oom() -> None:
    assert _exit_cause(0, oom_kill=0) == "exit:0"
    assert _exit_cause(1, oom_kill=0) == "exit:1"
    assert _exit_cause(-9, oom_kill=0) == "signal:9"          # asyncio's negative-code convention
    assert _exit_cause(-9, oom_kill=1) == "oom-kill"          # oom_kill wins over a SIGKILL exit
    assert _exit_cause(0, oom_kill=1) == "oom-kill"           # even over a clean-looking exit
    assert _exit_cause(None, oom_kill=0) == "exit:-1"         # honest fallback, never a guess


# --- fsync-before-rename ---

def test_write_receipt_fsyncs_before_the_atomic_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    real_fsync = os.fsync
    real_replace = os.replace

    def _tracked_fsync(fd: int) -> None:
        calls.append("fsync")
        real_fsync(fd)

    def _tracked_replace(src: Any, dst: Any) -> None:
        calls.append("replace")
        real_replace(src, dst)

    monkeypatch.setattr(bodies.os, "fsync", _tracked_fsync)
    monkeypatch.setattr(bodies.os, "replace", _tracked_replace)

    dest = tmp_path / "receipts" / "h1.json"
    _write_receipt(dest, {"v": 1, "handle": "h1"})

    assert calls == ["fsync", "replace"]           # ORDER matters: never a partial file visible
    assert dest.exists()
    assert json.loads(dest.read_text()) == {"v": 1, "handle": "h1"}
    assert not list((tmp_path / "receipts").glob("*.tmp"))  # no orphaned tmp file left behind


def test_write_receipt_cleans_up_tmp_file_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crash mid-write must never leave a truncated receipt at the REAL path — only an
    orphaned tmp file (or nothing), and this proves the tmp file is removed too."""
    def _boom(fd: int) -> None:
        raise OSError("disk gone")

    monkeypatch.setattr(bodies.os, "fsync", _boom)
    dest = tmp_path / "receipts" / "h2.json"
    with pytest.raises(OSError):
        _write_receipt(dest, {"v": 1, "handle": "h2"})
    assert not dest.exists()
    assert list((tmp_path / "receipts").glob("*")) == []  # tmp file cleaned up, not orphaned


def test_build_receipt_matches_the_v1_schema_exactly() -> None:
    r = _build_receipt(
        handle="h3", provider="local", kind="claude", ram_envelope_bytes=2 * 2**30,
        exit_cause="exit:0", started_at=NOW, ended_at=NOW, seat_anchor="/tmp/jobs/wake-demo",
        repo_ref="/repo/demo", budget_usd=1.5, core_seconds=3.0, ram_peak_bytes=1234)
    assert set(r) == _RECEIPT_KEYS   # exactly this shape — the meter parses it verbatim
    assert r["v"] == 1 and r["provider"] == "local"
    assert r["wall_seconds"] == 0.0                 # started == ended in this fixture
    assert r["ram_gib_seconds"] == 0.0              # wall_seconds is 0 → the product is 0


def test_ram_gib_seconds_bills_the_envelope_not_the_peak() -> None:
    """Parity with the Xen tier: you pay for what you RESERVED, not what you happened to use."""
    from datetime import timedelta
    ended = NOW + timedelta(seconds=10)
    r = _build_receipt(
        handle="h4", provider="local", kind="claude", ram_envelope_bytes=2 * 2**30,
        exit_cause="exit:0", started_at=NOW, ended_at=ended, seat_anchor="a", repo_ref="r",
        budget_usd=None, core_seconds=1.0, ram_peak_bytes=1)  # peak is tiny; envelope is not
    assert r["ram_gib_seconds"] == pytest.approx(2 * 10)   # 2 GiB * 10s, ignoring the 1-byte peak
    assert r["ram_peak_bytes"] == 1                        # peak is still recorded, just not billed


# --- StubRaProvider round-trip ---

async def test_stub_ra_provider_round_trips_a_canned_receipt(tmp_path: Path) -> None:
    provider = StubRaProvider(receipts_dir=tmp_path / "receipts")
    assert await provider.receipt("nonexistent") is None   # nothing minted yet → None, not KeyError

    handle = await provider.summon(
        "claude", 2, 4 * 2**30, "/repo/demo", "/tmp/jobs/wake-demo", 5.0,
        command=["claude", "-p", "hi"])
    assert handle.startswith("ra-")                         # visually distinct from the local tier

    await provider.dissolve(handle)
    receipt = await provider.receipt(handle)
    assert receipt is not None
    assert set(receipt) == _RECEIPT_KEYS
    assert receipt["provider"] == "ra-stub" and receipt["handle"] == handle
    assert receipt["kind"] == "claude" and receipt["repo_ref"] == "/repo/demo"
    assert receipt["budget_usd"] == 5.0
    assert (tmp_path / "receipts" / f"{handle}.json").exists()  # a REAL file, not just a dict

    await provider.dissolve(handle)  # idempotent: dissolving twice must not raise
    assert await provider.receipt(handle) == receipt


# --- LocalProvider summon/dissolve/receipt, systemd faked out entirely ---

class _FakeCompletedProc:
    """A stand-in for the tail of `asyncio.subprocess.Process` that LocalProvider actually
    touches (`.communicate()`), used for the ControlGroup probe and the `stop` call."""

    def __init__(self, out: bytes = b"") -> None:
        self._out = out
        self.returncode = 0

    async def communicate(self) -> tuple[bytes, bytes]:
        return self._out, b""

    async def wait(self) -> int:
        return 0


def _fake_runner(cgroup_dir: Path, payload_returncode: int = 0) -> Any:
    """Ignores the real `systemd-run`/`systemctl` binaries entirely: answers the ControlGroup
    probe with a canned path to a FIXTURE cgroup dir, and spawns a REAL trivial subprocess for
    the payload so `.wait()`/`.returncode` behave exactly like the genuine asyncio contract
    (a hand-rolled fake Process is easy to get subtly wrong; a real `/bin/true` is not)."""
    calls: list[list[str]] = []

    async def runner(
        argv: list[str], *, cwd: str | None = None, env: dict[str, str] | None = None,
        stdout: int | None = None, stderr: int | None = None,
    ) -> Any:
        calls.append(argv)
        if argv[:3] == ["systemctl", "--user", "show"]:
            return _FakeCompletedProc(str(cgroup_dir).encode())
        if argv[:3] == ["systemctl", "--user", "stop"]:
            return _FakeCompletedProc()
        # the systemd-run payload call itself — run something REAL and short-lived
        code = "0" if payload_returncode == 0 else str(payload_returncode)
        return await asyncio.create_subprocess_exec(
            "sh", "-c", f"exit {code}", stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL)

    runner.calls = calls  # type: ignore[attr-defined]
    return runner


async def test_local_provider_summon_dissolve_round_trip(tmp_path: Path) -> None:
    """The fake runner never writes a trampoline snapshot, so this round-trip exercises the
    FALLBACK meter lane: numbers read straight off the (fixture) cgroup dir."""
    cgroup = tmp_path / "cgroup"
    cgroup.mkdir()
    (cgroup / "cpu.stat").write_text("usage_usec 500000\n")
    (cgroup / "memory.peak").write_text("1048576\n")
    (cgroup / "memory.events").write_text("oom_kill 0\n")

    runner = _fake_runner(cgroup)
    provider = LocalProvider(
        runner=runner, cgroup_root=Path("/"), receipts_dir=tmp_path / "receipts")

    handle = await provider.summon(
        "claude", None, 2 * 2**30, "/repo/demo", "/tmp/jobs/wake-demo", None,
        command=["claude", "-p", "hi"], env={"CLAUDE_JOB_DIR": "/tmp/jobs/wake-demo"})

    # summon() sent exactly ONE systemd-run and ONE ControlGroup probe — no stop yet
    kinds = [c[0] for c in runner.calls]
    assert kinds == ["systemd-run", "systemctl"]
    # the payload rides INSIDE the metering trampoline: python3 -c <code> <statsfile> <payload>
    spawn = runner.calls[0]
    assert spawn[-3:] == ["claude", "-p", "hi"]           # the payload, verbatim, at the tail
    tramp = spawn[spawn.index("--") + 1:]
    assert tramp[:2] == ["python3", "-c"]
    assert tramp[3].endswith(f".stats/{handle}.json")     # the snapshot's landing path, by argv

    await provider.dissolve(handle)
    receipt = await provider.receipt(handle)
    assert receipt is not None
    assert set(receipt) == _RECEIPT_KEYS
    assert receipt["provider"] == "local" and receipt["handle"] == handle
    assert receipt["core_seconds"] == pytest.approx(0.5)
    assert receipt["ram_peak_bytes"] == 1048576
    assert receipt["exit_cause"] == "exit:0"
    assert receipt["ram_envelope_bytes"] == 2 * 2**30
    assert (tmp_path / "receipts" / f"{handle}.json").exists()

    await provider.dissolve(handle)  # already dissolved: idempotent no-op, not a crash


async def test_local_provider_maps_oom_kill_over_the_exit_code(tmp_path: Path) -> None:
    cgroup = tmp_path / "cgroup"
    cgroup.mkdir()
    (cgroup / "cpu.stat").write_text("usage_usec 100\n")
    (cgroup / "memory.peak").write_text("999\n")
    (cgroup / "memory.events").write_text("oom_kill 1\n")   # the kernel killed it for memory

    runner = _fake_runner(cgroup, payload_returncode=137)  # looks like an ordinary SIGKILL exit
    provider = LocalProvider(
        runner=runner, cgroup_root=Path("/"), receipts_dir=tmp_path / "receipts")
    handle = await provider.summon(
        "claude", None, 10**8, "/repo/demo", "anchor", None, command=["claude"])
    await provider.dissolve(handle)
    receipt = await provider.receipt(handle)
    assert receipt is not None
    assert receipt["exit_cause"] == "oom-kill"   # memory.events wins, never the raw exit status


async def test_local_provider_prefers_the_trampoline_snapshot(tmp_path: Path) -> None:
    """You cannot meter a corpse: by dissolve time a naturally-exited scope's cgroup and unit
    bookkeeping are BOTH gone (systemd 259, probed 2026-07-15), so the trampoline's inside-the-
    body snapshot is the PRIMARY reading — the cgroup dir is only the fallback. Here both exist
    with different numbers; the snapshot must win, and its wait_rc (the payload's true wait
    status) must beat the wrapper's flattened exit code."""
    cgroup = tmp_path / "cgroup"
    cgroup.mkdir()
    (cgroup / "cpu.stat").write_text("usage_usec 1\n")     # the WRONG numbers — must lose
    (cgroup / "memory.peak").write_text("1\n")
    (cgroup / "memory.events").write_text("oom_kill 0\n")

    runner = _fake_runner(cgroup, payload_returncode=143)  # the wrapper's flattened signal exit
    provider = LocalProvider(
        runner=runner, cgroup_root=Path("/"), receipts_dir=tmp_path / "receipts")
    handle = await provider.summon(
        "claude", None, 10**8, "/repo/demo", "anchor", None, command=["claude"])
    stats = tmp_path / "receipts" / ".stats" / f"{handle}.json"
    stats.write_text(json.dumps({
        "wait_rc": -15,                                    # the TRUE status: signalled
        "cpu.stat": "usage_usec 750000\n",
        "memory.peak": "2097152\n",
        "memory.events": "oom_kill 0\n",
    }))
    await provider.dissolve(handle)
    receipt = await provider.receipt(handle)
    assert receipt is not None
    assert receipt["core_seconds"] == pytest.approx(0.75)  # the snapshot's, not the fixture's
    assert receipt["ram_peak_bytes"] == 2097152
    assert receipt["exit_cause"] == "signal:15"            # wait_rc, not the wrapper's exit:143
    assert not stats.exists()                              # scratch cleaned; the receipt remains


def test_trampoline_snapshots_before_it_dies(tmp_path: Path) -> None:
    """The trampoline's contract, exercised for REAL but without systemd: wraps the payload,
    propagates its exit status, and fsync-renames a snapshot of its own cgroup as its last act.
    (Under a scope the cgroup is the body's own; here it is the test session's — the mechanics
    are identical.)"""
    import subprocess

    stats = tmp_path / "stats.json"
    done = subprocess.run(
        ["python3", "-c", bodies._TRAMPOLINE, str(stats), "sh", "-c", "exit 3"], timeout=30)
    assert done.returncode == 3                            # the payload's status, passed through
    snap = json.loads(stats.read_text())
    assert snap["wait_rc"] == 3
    assert "usage_usec" in snap["cpu.stat"]                # a real cgroup read, not a guess
    assert not stats.with_suffix(".json.tmp").exists()     # renamed, never left partial


def test_trampoline_keeps_the_true_signal_status(tmp_path: Path) -> None:
    """A signalled payload exits the wrapper as 128+N (an exit code cannot go negative), but
    the snapshot's wait_rc carries the REAL wait status — that is where signal:N comes from."""
    import subprocess

    stats = tmp_path / "stats.json"
    done = subprocess.run(
        ["python3", "-c", bodies._TRAMPOLINE, str(stats), "sh", "-c", "kill -TERM $$"],
        timeout=30)
    assert done.returncode == 143                          # 128 + SIGTERM, the flattened form
    assert json.loads(stats.read_text())["wait_rc"] == -15  # the true form, kept for the receipt


async def test_dissolve_stops_the_scope_not_a_phantom_service(tmp_path: Path) -> None:
    """THE SILENT NO-OP STOP (caught live, 2026-07-15): `systemctl stop osiris-body-X` defaults
    the suffix to `.service` — a unit that does not exist — and the "not loaded" complaint dies
    in a DEVNULL'd stderr. The payload loops on, dissolve hangs on wait() forever. The stop must
    name `.scope`, exactly as the ControlGroup probe already does."""
    cgroup = tmp_path / "cgroup"
    cgroup.mkdir()
    (cgroup / "cpu.stat").write_text("usage_usec 100\n")
    (cgroup / "memory.peak").write_text("1\n")
    (cgroup / "memory.events").write_text("oom_kill 0\n")

    calls: list[list[str]] = []
    payload: list[Any] = []

    async def runner(
        argv: list[str], *, cwd: str | None = None, env: dict[str, str] | None = None,
        stdout: int | None = None, stderr: int | None = None,
    ) -> Any:
        calls.append(argv)
        if argv[:3] == ["systemctl", "--user", "show"]:
            return _FakeCompletedProc(str(cgroup).encode())
        if argv[:3] == ["systemctl", "--user", "stop"]:
            payload[0].kill()  # what a CORRECTLY-ADDRESSED stop does; a .service stop would not
            return _FakeCompletedProc()
        proc = await asyncio.create_subprocess_exec(
            "sleep", "30", stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        payload.append(proc)
        return proc

    provider = LocalProvider(
        runner=runner, cgroup_root=Path("/"), receipts_dir=tmp_path / "receipts")
    handle = await provider.summon(
        "claude", None, 10**8, "/repo/demo", "anchor", None, command=["claude"])
    await asyncio.wait_for(provider.dissolve(handle), 10)   # a phantom stop would hang here
    stops = [c for c in calls if c[:3] == ["systemctl", "--user", "stop"]]
    assert stops == [["systemctl", "--user", "stop", f"osiris-body-{handle}.scope"]]
    receipt = await provider.receipt(handle)
    assert receipt is not None and receipt["exit_cause"] == "signal:9"  # the kill, recorded


async def test_local_provider_receipt_is_none_before_dissolve(tmp_path: Path) -> None:
    cgroup = tmp_path / "cgroup"
    cgroup.mkdir()
    runner = _fake_runner(cgroup)
    provider = LocalProvider(
        runner=runner, cgroup_root=Path("/"), receipts_dir=tmp_path / "receipts")
    handle = await provider.summon(
        "claude", None, 10**8, "/repo/demo", "anchor", None, command=["claude"])
    assert await provider.receipt(handle) is None   # nothing minted until dissolve reaps it


def test_default_receipts_dir_reads_the_module_global_at_construction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Constructor injection AND the module-global patch idiom both work — the same discipline
    tests/test_trigger.py relies on for `trigger.RECEIPTS`, never the real home directory."""
    monkeypatch.setattr(bodies, "RECEIPTS", tmp_path / "patched")
    provider = LocalProvider()
    assert provider._receipts_dir == tmp_path / "patched"  # the patched global, not ~/.osiris


# --- _spawn_in_body: env/anchor discipline, matching _spawn_claude's ---

class _FakeBodyProvider:
    """Captures exactly what _spawn_in_body hands to summon() — no process, no systemd."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def summon(
        self, kind: str, cores: int | None, ram_bytes: int, repo_ref: str, seat_anchor: str,
        budget_usd: float | None, *, command: list[str], env: dict[str, str] | None = None,
    ) -> str:
        self.calls.append({
            "kind": kind, "cores": cores, "ram_bytes": ram_bytes, "repo_ref": repo_ref,
            "seat_anchor": seat_anchor, "budget_usd": budget_usd, "command": command, "env": env,
        })
        return "handle-1"

    async def dissolve(self, handle: str) -> None:
        return None

    async def receipt(self, handle: str) -> dict[str, Any] | None:
        return None


async def test_spawn_in_body_injects_claude_job_dir_and_the_anchor() -> None:
    provider = _FakeBodyProvider()
    handle = await _spawn_in_body(
        "/repo/demo", "wake up", job_dir="/tmp/x/jobs/wake-7", provider=provider)
    assert handle == "handle-1"
    assert len(provider.calls) == 1
    call = provider.calls[0]
    assert call["repo_ref"] == "/repo/demo"
    assert call["seat_anchor"] == "/tmp/x/jobs/wake-7"    # the durable anchor, not the bare repo
    assert call["env"]["CLAUDE_JOB_DIR"] == "/tmp/x/jobs/wake-7"
    assert "PATH" in call["env"]                    # inherited the parent env, not a bare dict
    assert call["command"][:2] == ["claude", "-p"]
    assert call["command"][-1] == "wake up"                  # the prompt trails the flags


async def test_spawn_in_body_seats_the_anchor_on_resume_when_there_is_no_job_dir() -> None:
    provider = _FakeBodyProvider()
    await _spawn_in_body(
        "/repo/demo", "resumed", resume_session="abcd1234-full-session-id", provider=provider)
    call = provider.calls[0]
    assert call["seat_anchor"] == "abcd1234-full-session-id"  # never the bare repo path
    assert "CLAUDE_JOB_DIR" not in call["env"]                # no job_dir given: nothing injected
    assert "--resume" in call["command"] and "abcd1234-full-session-id" in call["command"]


async def test_spawn_in_body_refuses_an_anchorless_summon() -> None:
    """path=identity is the root bug (dd47c1da): with neither job_dir nor resume_session the
    only anchor left would be the bare repo path — two bodies in one repo sharing an accounting
    identity. Refused loudly, never defaulted."""
    provider = _FakeBodyProvider()
    with pytest.raises(ValueError, match="anchor"):
        await _spawn_in_body("/repo/demo", "no anchor at all", provider=provider)
    assert provider.calls == []  # refused BEFORE the summon, not after


async def test_spawn_in_body_forwards_model_and_allowed_tools() -> None:
    provider = _FakeBodyProvider()
    await _spawn_in_body(
        "/repo/demo", "wake up", job_dir="/tmp/x/jobs/wake-9", model="claude-haiku-4-5",
        allowed_tools="mcp__osiris", provider=provider, cores=2, ram_bytes=10**9, budget_usd=1.0)
    call = provider.calls[0]
    pairs = list(zip(call["command"], call["command"][1:], strict=False))
    assert ("--model", "claude-haiku-4-5") in pairs
    assert ("--allowedTools", "mcp__osiris") in pairs
    assert call["cores"] == 2 and call["ram_bytes"] == 10**9 and call["budget_usd"] == 1.0


async def test_spawn_in_body_defaults_to_a_local_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    """No provider given → LocalProvider (doctrine 2: the local tier is the default product,
    never a fallback nobody chose)."""
    from src.orchestrator import trigger

    captured: dict[str, Any] = {}

    class _Stub:
        async def summon(self, *a: Any, **kw: Any) -> str:
            captured["provider_type"] = "seen"
            return "h"

        async def dissolve(self, handle: str) -> None:
            return None

        async def receipt(self, handle: str) -> dict[str, Any] | None:
            return None

    monkeypatch.setattr(trigger, "LocalProvider", lambda: _Stub())
    handle = await _spawn_in_body("/repo/demo", "hi", job_dir="/tmp/x/jobs/wake-1")
    assert handle == "h" and captured.get("provider_type") == "seen"


# --- live integration tests, real systemd-run, skipped when unavailable ---

_SYSTEMD_AVAILABLE = (
    shutil.which("systemd-run") is not None and shutil.which("systemctl") is not None)


async def _loaded_body_units() -> str:
    proc = await asyncio.create_subprocess_exec(
        "systemctl", "--user", "list-units", "osiris-body-*", "--all", "--no-legend",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    out, _ = await proc.communicate()
    return out.decode()


@pytest.mark.skipif(not _SYSTEMD_AVAILABLE, reason="systemd-run/systemctl not on PATH")
async def test_real_body_natural_exit_still_meters_cpu(tmp_path: Path) -> None:
    """THE CORPSE-METERING PROOF (the review's hole): a body that exits NATURALLY, before
    dissolve is ever called, must still produce non-zero CPU accounting — by dissolve time its
    scope's cgroup and unit bookkeeping are both gone, so only the trampoline's inside-the-body
    snapshot can carry the numbers out. A receipt asserting shape but not substance would have
    passed with core_seconds == 0; this one cannot."""
    provider = LocalProvider(receipts_dir=tmp_path / "receipts")
    burn = "import time\nt = time.time()\nwhile time.time() - t < 0.3:\n    pass"
    handle = await provider.summon(
        "claude", None, 128 * 2**20, str(tmp_path), str(tmp_path), None,
        command=["python3", "-c", burn])
    await provider._live[handle].proc.wait()   # the body dies of natural causes, unobserved
    await provider.dissolve(handle)
    receipt = await provider.receipt(handle)
    assert receipt is not None
    assert set(receipt) == _RECEIPT_KEYS
    assert receipt["exit_cause"] == "exit:0"
    assert receipt["core_seconds"] > 0.1       # ~0.3s of real burn — NOT an honestly-zeroed corpse
    assert receipt["ram_peak_bytes"] > 0
    assert receipt["wall_seconds"] > 0.0
    assert f"osiris-body-{handle}" not in await _loaded_body_units()  # no unit left loaded


@pytest.mark.skipif(not _SYSTEMD_AVAILABLE, reason="systemd-run/systemctl not on PATH")
async def test_real_body_dissolved_mid_flight(tmp_path: Path) -> None:
    """The kill path: a long-running body stopped by dissolve() itself. The trampoline survives
    the scope's SIGTERM just long enough to snapshot, so the receipt carries the true signal
    and the CPU spent up to the stop."""
    provider = LocalProvider(receipts_dir=tmp_path / "receipts")
    handle = await provider.summon(
        "claude", None, 128 * 2**20, str(tmp_path), str(tmp_path), None,
        command=["python3", "-c", "import time\nwhile True:\n    time.sleep(0.05)"])
    await asyncio.sleep(0.5)                   # let it live a little before the stop
    await provider.dissolve(handle)
    receipt = await provider.receipt(handle)
    assert receipt is not None
    assert receipt["exit_cause"] == "signal:15"   # systemctl stop's SIGTERM, honestly recorded
    assert receipt["core_seconds"] >= 0.0
    assert receipt["wall_seconds"] >= 0.4
    assert f"osiris-body-{handle}" not in await _loaded_body_units()  # stop left nothing loaded

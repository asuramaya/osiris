"""BODIES — the substrate a mind runs on, admitted and governed (constitution #2, ruling
`f1803b4a`).

Doctrine 2 (`7ff54707`): PROVIDER-AGNOSTIC BODIES; PLAIN LINUX IS THE DEFAULT. Never assume Xen
or rotten-apple. One `BodyProvider` interface, two tiers, same shape: **local** (this module,
today — transient systemd user scopes, hard memory ceilings, cgroup v2 IS the meter) and
**Ra/Xen** (Phase 3, `15a41cf0` — PVH microVMs, harder wall, gated on the metal). No Osiris
feature may ever REQUIRE the hypervisor tier — `StubRaProvider` keeps that seam's tests green
before the metal exists.

Why this module exists at all (`37fe6a09`, the Warp OOM canon): a 14.6GB headless-Chrome child
died inside Warp's desktop cgroup and the OOM killer took the WHOLE fleet with it — every
session borrowed its body from a scope nobody sized for it. A `LocalProvider` body gets its own
transient scope with its own `MemoryMax`; one body dying starves nothing else.

THE METER RIDES INSIDE THE BODY. You cannot meter a corpse: on systemd 259 a transient scope's
unit bookkeeping (CPUUsageNSec, MemoryPeak, Result) and its cgroup directory are BOTH gone the
instant its last process exits — `--collect` or not, natural exit or our own `systemctl stop`
(probed 2026-07-15: `show` answers `[not set]` on every counter immediately after either).
Reading the accounting after the fact was never an option, so the body's last living act is to
read ITSELF: a stdlib trampoline wraps the payload, waits for it, snapshots the scope's own
cgroup v2 files from inside (it IS the scope's last process — the cgroup cannot be collected
under it), and fsync-renames the snapshot before dying. cgroup v2 is still the meter; the
reading just happens at the only instant the meter is readable.

RECEIPT-BEFORE-DISSOLVE (parity with the Xen tier's dom0-is-the-meter discipline, `15a41cf0`):
`dissolve()` does not return until the receipt is minted and fsync'd. A body that is gone before
its cost is recorded is a hand that spent and cannot be governed — the exact shape of the bug
this whole doctrine exists to close.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import tempfile
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

# Where a body's RECEIPT lands — the meter agent (Phase 0.2, `src/ingest/wake_cost.py`'s sibling
# for resource-seconds) parses this file's shape exactly; the schema is versioned ("v":1) so a
# later field never breaks an old reader silently. Named and placed like trigger.RECEIPTS on
# purpose (same discipline, a different currency: dollars there, core/ram-seconds here).
RECEIPTS = Path.home() / ".osiris" / "body-receipts"

# cgroup v2's unified mount — where a scope's cpu.stat / memory.peak / memory.events live once
# `systemctl --user show -p ControlGroup` gives us the unit's path relative to this root.
_CGROUP_ROOT = Path("/sys/fs/cgroup")

# MemoryHigh is the soft throttle systemd applies before MemoryMax's hard kill — 90% per the
# spec's own number. int(), not round(): this is a throttle knob, not an accounting figure.
_MEMORY_HIGH_FRACTION = 0.9

# A fresh scope needs a moment to register with the user manager before its ControlGroup is
# queryable — polled, not slept-once, so the common case (already registered) costs nothing.
_CGROUP_POLL_ATTEMPTS = 10
_CGROUP_POLL_INTERVAL = 0.05


class BodyProvider(Protocol):
    """summon(kind, cores, ram_bytes, repo_ref, seat_anchor, budget_usd) -> handle ·
    dissolve(handle) · receipt(handle) — doctrine 2's shape, exactly. `command`/`env` are this
    module's one addition to the doctrine's six-param shorthand: the six say what a body NEEDS
    (its shape and its accounting anchors), but summoning one has to say what it RUNS — the
    payload and its environment, the same two things `_spawn_claude` already carries.
    """

    async def summon(
        self, kind: str, cores: int | None, ram_bytes: int, repo_ref: str, seat_anchor: str,
        budget_usd: float | None, *, command: Sequence[str], env: dict[str, str] | None = None,
    ) -> str: ...

    async def dissolve(self, handle: str) -> None: ...

    async def receipt(self, handle: str) -> dict[str, Any] | None: ...


class ProcessRunner(Protocol):
    """The injectable seam (constructor param): real subprocess invocation vs a fake, so unit
    tests never need real systemd. Mirrors `asyncio.create_subprocess_exec`'s shape so the
    default IS that call, verbatim — nothing to translate at the boundary."""

    async def __call__(
        self, argv: list[str], *, cwd: str | None = None, env: dict[str, str] | None = None,
        stdout: int | None = None, stderr: int | None = None,
    ) -> asyncio.subprocess.Process: ...


async def _default_runner(
    argv: list[str], *, cwd: str | None = None, env: dict[str, str] | None = None,
    stdout: int | None = None, stderr: int | None = None,
) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        *argv, cwd=cwd, env=env, stdout=stdout, stderr=stderr)


def _cpu_list(cores: int) -> str:
    """AllowedCPUs wants a CPU SET ('0-3'), not a count. Pinned to the first N logical CPUs —
    good enough for a ceiling; NUMA-aware placement is Ra's job, not this tier's."""
    return "0" if cores <= 1 else f"0-{cores - 1}"


# The body's dying act (see the module docstring: you cannot meter a corpse). Stdlib-only,
# argv-passed (never string-formatted — a prompt in the payload must not be able to inject
# code). `wait_rc` keeps the TRUE wait status — negative = signalled — which the wrapper's own
# 8-bit exit code cannot carry. The SIGTERM handler makes a `systemctl stop` survivable for
# exactly as long as the snapshot takes: handlers (unlike SIG_IGN) reset to default across
# exec, so the payload still dies on the first TERM while the trampoline stays to take the
# reading. /proc/self/cgroup's `0::` line is the pure-cgroup-v2 shape — the only one this
# provider supports (same constraint as reading cpu.stat at all).
_TRAMPOLINE = "\n".join((
    "import json, os, signal, subprocess, sys",
    "stats, cmd = sys.argv[1], sys.argv[2:]",
    "signal.signal(signal.SIGTERM, lambda *a: None)",
    "rc = subprocess.Popen(cmd).wait()",
    "cg = '/sys/fs/cgroup' + open('/proc/self/cgroup').read().split('::', 1)[1].strip()",
    "d = {'wait_rc': rc}",
    "for name in ('cpu.stat', 'memory.peak', 'memory.events'):",
    "    try:",
    "        d[name] = open(cg + '/' + name).read()",
    "    except OSError:",
    "        d[name] = ''",
    "fh = open(stats + '.tmp', 'w')",
    "json.dump(d, fh)",
    "fh.flush()",
    "os.fsync(fh.fileno())",
    "fh.close()",
    "os.replace(stats + '.tmp', stats)",
    "sys.exit(rc if rc >= 0 else 128 - rc)",
))


def _systemd_run_argv(
    unit: str, cores: int | None, ram_bytes: int, command: Sequence[str],
) -> list[str]:
    """The exact invocation the spec names: a transient --user --scope, --collect (nothing
    reads the unit post-mortem — the meter rides inside the body — and without it a FAILED
    scope, e.g. an oom-killed one, would stay loaded forever: a unit leak per casualty), a
    hard MemoryMax and a 90%-of-it MemoryHigh soft throttle, optional core pinning, then the
    payload."""
    high = int(ram_bytes * _MEMORY_HIGH_FRACTION)
    argv = [
        "systemd-run", "--user", "--scope", f"--unit={unit}", "--collect",
        "-p", f"MemoryMax={ram_bytes}", "-p", f"MemoryHigh={high}",
    ]
    if cores:
        argv += ["-p", f"AllowedCPUs={_cpu_list(cores)}"]
    argv += ["--", *command]
    return argv


def _parse_cpu_seconds(text: str) -> float:
    for line in text.splitlines():
        if line.startswith("usage_usec "):
            with contextlib.suppress(ValueError, IndexError):
                return int(line.split()[1]) / 1e6
    return 0.0


def _parse_peak_bytes(text: str) -> int:
    with contextlib.suppress(ValueError):
        return int(text.strip())
    return 0


def _parse_oom_kills(text: str) -> int:
    for line in text.splitlines():
        if line.startswith("oom_kill "):
            with contextlib.suppress(ValueError, IndexError):
                return int(line.split()[1])
    return 0


def _read_cgroup_stats(cgroup: Path) -> tuple[float, int, int]:
    """(core_seconds, ram_peak_bytes, oom_kill_count) straight off a cgroup v2 dir — the
    FALLBACK lane, for when the trampoline's snapshot never landed (a stop escalated to
    SIGKILL, memory.oom.group taking the trampoline too). A missing or unreadable file
    degrades to zero rather than raising: an already-collected scope still gets an HONEST
    receipt — zeroed, not fabricated — never a crash that loses the whole record."""
    def read(name: str) -> str:
        try:
            return (cgroup / name).read_text()
        except OSError:
            return ""
    return (_parse_cpu_seconds(read("cpu.stat")), _parse_peak_bytes(read("memory.peak")),
            _parse_oom_kills(read("memory.events")))


def _load_stats(path: Path) -> tuple[float, int, int, int | None] | None:
    """The trampoline's snapshot → (core_seconds, ram_peak_bytes, oom_kills, wait_rc) — the
    PRIMARY meter reading. None when the snapshot never landed; the caller falls back to
    whatever the cgroup can still say."""
    try:
        d = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(d, dict):
        return None
    rc = d.get("wait_rc")
    return (
        _parse_cpu_seconds(str(d.get("cpu.stat") or "")),
        _parse_peak_bytes(str(d.get("memory.peak") or "")),
        _parse_oom_kills(str(d.get("memory.events") or "")),
        int(rc) if isinstance(rc, int) else None,
    )


def _exit_cause(returncode: int | None, oom_kill: int) -> str:
    """"exit:N" | "signal:N" | "oom-kill" — oom-kill wins regardless of the wait() status,
    because a kernel that killed a process for memory can also leave it looking like an
    ordinary SIGKILL exit; memory.events is the ONLY honest source for which one happened."""
    if oom_kill > 0:
        return "oom-kill"
    if returncode is None:
        return "exit:-1"  # should not occur after wait() resolves; an honest fallback, no guess
    if returncode < 0:
        return f"signal:{-returncode}"
    return f"exit:{returncode}"


def _build_receipt(
    *, handle: str, provider: str, kind: str, ram_envelope_bytes: int, exit_cause: str,
    started_at: datetime, ended_at: datetime, seat_anchor: str, repo_ref: str,
    budget_usd: float | None, core_seconds: float, ram_peak_bytes: int,
) -> dict[str, Any]:
    """RECEIPT v1 — the canonical shape the meter agent parses exactly; do not add or rename a
    field here without bumping "v". ram_gib_seconds bills the ENVELOPE, not the peak (parity
    with the Xen tier: you pay for what you reserved, not what you happened to use)."""
    wall_seconds = (ended_at - started_at).total_seconds()
    return {
        "v": 1,
        "handle": handle,
        "provider": provider,
        "kind": kind,
        "core_seconds": core_seconds,
        "wall_seconds": wall_seconds,
        "ram_envelope_bytes": ram_envelope_bytes,
        "ram_peak_bytes": ram_peak_bytes,
        "ram_gib_seconds": (ram_envelope_bytes / 2**30) * wall_seconds,
        "exit_cause": exit_cause,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "seat_anchor": seat_anchor,
        "repo_ref": repo_ref,
        "budget_usd": budget_usd,
    }


def _write_receipt(path: Path, receipt: dict[str, Any]) -> None:
    """tmp-write + fsync + rename — the same discipline `_spawn_claude`'s receipt-before-return
    is built on, moved from "open the destination early" to "never let a reader see a partial
    file": a crash mid-write leaves the tmp file orphaned, never a truncated receipt at the real
    path. `os.replace` is atomic on the same filesystem, which the tmp file is by construction
    (created in the destination's own parent)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(receipt, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_name)
        raise


def _load_receipt(path: Path) -> dict[str, Any] | None:
    try:
        text = path.read_text()
    except OSError:
        return None
    try:
        parsed: dict[str, Any] = json.loads(text)
    except ValueError:
        return None
    return parsed


class _Waitable(Protocol):
    """What `_LiveBody.dissolve()` actually touches on `proc`: `.returncode` and an awaitable
    `.wait()`. `asyncio.subprocess.Process` satisfies this structurally (the ordinary case, set
    by `summon()`); RE-ADOPTION (the manager daemon, Phase 1 §4.0 — `docs/SPEC-agent-manager.md`)
    supplies a systemctl-polling stand-in for a scope whose child handle belonged to a now-dead
    daemon process — never a second implementation of the metering logic itself, only of the one
    thing `proc` needs to answer when nobody here is that process's parent."""

    @property
    def returncode(self) -> int | None: ...

    async def wait(self) -> int: ...


@dataclass(frozen=True)
class _LiveBody:
    """What LocalProvider needs at dissolve time — resolved once at summon (never re-queried
    after exit, when the scope's bookkeeping is already gone). `cgroup` serves the FALLBACK
    read only; the primary numbers come from the trampoline's snapshot."""

    unit: str
    kind: str
    repo_ref: str
    seat_anchor: str
    budget_usd: float | None
    ram_envelope_bytes: int
    started_at: datetime
    proc: _Waitable
    cgroup: Path | None


class LocalProvider:
    """The default product tier (`7ff54707`) — NOT a test double. `summon` spawns the payload
    inside a transient `systemd-run --user --scope`, wrapped in the trampoline that snapshots
    the body's own cgroup v2 accounting as its dying act; `dissolve` stops it (if still
    running), reads that snapshot (falling back to the cgroup dir, then honest zeros), and
    mints the receipt BEFORE returning."""

    def __init__(
        self, *, runner: ProcessRunner = _default_runner,
        cgroup_root: Path = _CGROUP_ROOT, receipts_dir: Path | None = None,
    ) -> None:
        self._runner = runner
        self._cgroup_root = cgroup_root
        # Read lazily off the module global at construction time (not baked into a default
        # argument) so tests can monkeypatch `bodies.RECEIPTS` exactly as test_trigger.py
        # patches `trigger.RECEIPTS` — never write a real receipt into the operator's home.
        self._receipts_dir = receipts_dir if receipts_dir is not None else RECEIPTS
        self._live: dict[str, _LiveBody] = {}

    def _stats_path(self, handle: str) -> Path:
        # A hidden subdir beside the receipts: the meter agent iterates the receipts root for
        # *.json and must never mistake a raw snapshot for a minted receipt.
        return self._receipts_dir / ".stats" / f"{handle}.json"

    def adopt(
        self, handle: str, *, unit: str, kind: str, repo_ref: str, seat_anchor: str,
        budget_usd: float | None, ram_envelope_bytes: int, started_at: datetime,
        proc: _Waitable, cgroup: Path | None,
    ) -> None:
        """RE-ADOPTION (doctrine 3, `2ceb7ba0`: daemon-death is a normal event; state is
        reconstructed, never trusted from memory): a body summoned by a now-DEAD daemon still
        has a live systemd scope. Seeding it into `_live` makes `dissolve()` treat it exactly
        like a body this instance summoned itself — same stats-file lookup, same cgroup
        fallback, same receipt-before-dissolve mint. The caller (the manager daemon,
        `src/manager/daemon.py`) supplies `proc` because THIS instance never held systemd-run's
        real child handle; only that one seam differs."""
        self._live[handle] = _LiveBody(
            unit=unit, kind=kind, repo_ref=repo_ref, seat_anchor=seat_anchor,
            budget_usd=budget_usd, ram_envelope_bytes=ram_envelope_bytes,
            started_at=started_at, proc=proc, cgroup=cgroup)

    async def summon(
        self, kind: str, cores: int | None, ram_bytes: int, repo_ref: str, seat_anchor: str,
        budget_usd: float | None, *, command: Sequence[str], env: dict[str, str] | None = None,
    ) -> str:
        handle = uuid.uuid4().hex[:12]
        unit = f"osiris-body-{handle}"
        stats = self._stats_path(handle)
        await asyncio.to_thread(stats.parent.mkdir, parents=True, exist_ok=True)
        # The trampoline's ~10MB interpreter and ~30ms startup land INSIDE the envelope and on
        # the meter — honest: metering from within is the harness's own cost of being metered.
        wrapped = ["python3", "-c", _TRAMPOLINE, str(stats), *command]
        argv = _systemd_run_argv(unit, cores, ram_bytes, wrapped)
        # started_at is stamped AT the spawn — not after the cgroup-resolve poll below, which
        # would clip the body's first ~100ms off the wall clock the envelope is billed on.
        started = datetime.now(UTC)
        proc = await self._runner(
            argv, cwd=repo_ref, env=env,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        # Resolved NOW, while the scope is fresh — not at dissolve time, when a naturally-exited
        # unit is already unloaded and unqueryable. See _resolve_cgroup.
        cgroup = await self._resolve_cgroup(unit)
        self._live[handle] = _LiveBody(
            unit=unit, kind=kind, repo_ref=repo_ref, seat_anchor=seat_anchor,
            budget_usd=budget_usd, ram_envelope_bytes=ram_bytes,
            started_at=started, proc=proc, cgroup=cgroup)
        return handle

    async def _resolve_cgroup(self, unit: str) -> Path | None:
        """Poll `systemctl --user show` for the scope's ControlGroup. None if it never showed
        up (the receipt still mints — honestly zeroed, per _read_cgroup_stats — never blocked
        on a probe that will not resolve)."""
        for attempt in range(_CGROUP_POLL_ATTEMPTS):
            if attempt:
                await asyncio.sleep(_CGROUP_POLL_INTERVAL)
            proc = await self._runner(
                ["systemctl", "--user", "show", "-p", "ControlGroup", "--value", f"{unit}.scope"],
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            out, _ = await proc.communicate()
            rel = out.decode().strip()
            if rel:
                return self._cgroup_root / rel.lstrip("/")
        return None

    async def dissolve(self, handle: str) -> None:
        live = self._live.pop(handle, None)
        if live is None:  # unknown or already-dissolved handle: idempotent no-op
            return
        if live.proc.returncode is None:
            with contextlib.suppress(OSError):
                # `.scope`, ALWAYS: a bare unit name defaults to `.service`, and systemctl's
                # "not loaded" complaint lands in a DEVNULL'd stderr — a stop that silently
                # stopped nothing, leaving the payload to loop forever (caught 2026-07-15).
                stopper = await self._runner(
                    ["systemctl", "--user", "stop", f"{live.unit}.scope"],
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
                await stopper.wait()
        # THE SYNCHRONIZATION POINT: systemd-run (scope mode) outlives its child, and the
        # trampoline fsync-renames its snapshot before exiting — so once wait() resolves, the
        # stats file is complete or will never come. No polling, no race. A wedged trampoline
        # is bounded by the scope's own TimeoutStopSec→SIGKILL escalation, never by us.
        returncode = await live.proc.wait()
        ended = datetime.now(UTC)
        stats = await asyncio.to_thread(_load_stats, self._stats_path(handle))
        if stats is not None:
            core_seconds, ram_peak, oom_kill, wait_rc = stats
            # the trampoline's wait_rc is the PAYLOAD's true status (negative = signalled);
            # systemd-run's own exit code flattens a signal into 128+N and loses the fact
            rc = wait_rc if wait_rc is not None else returncode
        else:
            core_seconds, ram_peak, oom_kill = (
                await asyncio.to_thread(_read_cgroup_stats, live.cgroup)
                if live.cgroup is not None else (0.0, 0, 0))
            rc = returncode
        # RECEIPT-BEFORE-DISSOLVE: minted and fsync'd here, before this call returns — the caller
        # never observes a "dissolved" body with no receipt on disk.
        receipt = _build_receipt(
            handle=handle, provider="local", kind=live.kind,
            ram_envelope_bytes=live.ram_envelope_bytes,
            exit_cause=_exit_cause(rc, oom_kill),
            started_at=live.started_at, ended_at=ended, seat_anchor=live.seat_anchor,
            repo_ref=live.repo_ref, budget_usd=live.budget_usd,
            core_seconds=core_seconds, ram_peak_bytes=ram_peak)
        await asyncio.to_thread(
            _write_receipt, self._receipts_dir / f"{handle}.json", receipt)
        with contextlib.suppress(OSError):  # the receipt is the record; the snapshot is scratch
            await asyncio.to_thread(self._stats_path(handle).unlink)

    async def receipt(self, handle: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(_load_receipt, self._receipts_dir / f"{handle}.json")


class StubRaProvider:
    """Same interface, canned receipt, provider="ra-stub" — keeps the Phase 3 seam (`15a41cf0`,
    Ra/Xen microVMs) tested end to end before the metal exists. Never spawns anything real:
    `command`/`env` are accepted (interface parity) and ignored."""

    def __init__(self, *, receipts_dir: Path | None = None) -> None:
        self._receipts_dir = receipts_dir if receipts_dir is not None else RECEIPTS
        self._live: dict[str, tuple[str, int, str, str, float | None, datetime]] = {}

    async def summon(
        self, kind: str, cores: int | None, ram_bytes: int, repo_ref: str, seat_anchor: str,
        budget_usd: float | None, *, command: Sequence[str], env: dict[str, str] | None = None,
    ) -> str:
        handle = f"ra-{uuid.uuid4().hex[:12]}"
        self._live[handle] = (kind, ram_bytes, repo_ref, seat_anchor, budget_usd, datetime.now(UTC))
        return handle

    async def dissolve(self, handle: str) -> None:
        live = self._live.pop(handle, None)
        if live is None:
            return
        kind, ram_bytes, repo_ref, seat_anchor, budget_usd, started_at = live
        ended = datetime.now(UTC)
        wall_seconds = (ended - started_at).total_seconds()
        # Canned: a stub microVM is modeled as fully using its own dedicated core and never
        # ballooning below its envelope — plausible numbers, never real measurements.
        receipt = _build_receipt(
            handle=handle, provider="ra-stub", kind=kind, ram_envelope_bytes=ram_bytes,
            exit_cause="exit:0", started_at=started_at, ended_at=ended, seat_anchor=seat_anchor,
            repo_ref=repo_ref, budget_usd=budget_usd, core_seconds=wall_seconds,
            ram_peak_bytes=ram_bytes)
        await asyncio.to_thread(
            _write_receipt, self._receipts_dir / f"{handle}.json", receipt)

    async def receipt(self, handle: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(_load_receipt, self._receipts_dir / f"{handle}.json")

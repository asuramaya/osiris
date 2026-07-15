"""The process census — OS TRUTH beside the graph's beliefs (heinrich's ghost-seat filing,
thread 1fe6811c).

The durable mount registry (agent_mounts) is the graph's BELIEF about who is live: a row with
a fresh `last_seen`. Two ways that belief outlives reality:

  * a GHOST — closing or killing a tab never retires its seat, so the row keeps its stamp for
    up to `last_seen`'s 15-minute decay window even though nothing is listening any more (the
    fleet carrying 277 of these at filing time);
  * a PHANTOM MOUNT — a seat that registered identity (a real session id, a real cwd — the
    whisper seats it unconditionally) but never backed an actual session: no transcript ever
    materialized. No ping-window catches this one, because there was never a pulse to decay.

Both are invisible to a query that only ever asks the graph. This module asks the OS instead:
`pgrep -x claude` is the verified live door (heinrich's census probe) — Claude Code rewrites
argv to the bare literal `claude`, so no session id ever rides there, and the harness
appends-and-closes its transcript fd rather than holding it open, so both `argv`-grep and
`lsof`-on-a-held-fd are VERIFIED DEAD ENDS; do not resurrect them. `/proc/<pid>/cwd` is each
body's project, exactly as `resolve_identity` derives one from a cwd (`read_project_label`'s
`.osiris` walk, falling back to the folder's basename) — reused here, not reinvented, so a
census label always matches a mount's. `/proc/<pid>/exe` is the second witness: it resolves to
the packaged binary itself (`~/.local/share/claude/versions/<ver>`, verified live on this box —
the installer replaces the file in place, so the running exe's directory shape is stable across
version bumps even though the exact version string is not), which tells a real `claude` body
apart from an unrelated process that happens to share the truncated 15-char `comm` field
`pgrep -x` matches on.

Pure OS truth, no graph read here at all — `fleet()` is the one place that folds this against
the mount registry's belief to make the gap visible. Every OS read is behind an injectable seam
so tests drive it with fakes, never a real `/proc` or a real `pgrep`.
"""
from __future__ import annotations

import subprocess
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path

from src.orchestrator.agents import read_project_label

PgrepFn = Callable[[], list[int]]
ReadFn = Callable[[int], "str | None"]


def _pgrep_claude() -> list[int]:
    """PIDs of every live `claude` body on the box — heinrich's verified census door. Absent
    binary / any OS failure degrades to an empty census (never a raised error): this is a
    best-effort cross-check, not a dependency anything else's correctness relies on."""
    try:
        out = subprocess.run(
            ["pgrep", "-x", "claude"], capture_output=True, text=True, timeout=2, check=False)
    except (OSError, subprocess.SubprocessError):
        return []
    return [int(tok) for tok in out.stdout.split() if tok.isdigit()]


def _proc_cwd(pid: int) -> str | None:
    """A body's working directory — None if it vanished between the pgrep snapshot and this
    read (a race, not an error: the process is simply no longer there to count)."""
    try:
        return str(Path(f"/proc/{pid}/cwd").resolve())
    except OSError:
        return None


def _proc_exe(pid: int) -> str | None:
    """A body's executable — None on the same vanished-process race as `_proc_cwd`."""
    try:
        return str(Path(f"/proc/{pid}/exe").resolve())
    except OSError:
        return None


def _is_claude_body(exe: str | None) -> bool:
    """Confirms `exe` is really the packaged claude binary (`.../claude/versions/<ver>`) and
    not some other process that happens to share `claude`'s truncated 15-char `comm` field —
    the second witness `pgrep -x` alone cannot be. A structural check (parent dir `versions`
    under a `claude` dir), never a hardcoded version string: the installer replaces the binary
    in place on every bump, so a literal version pin would silently go blind at the next
    update. `exe=None` (the vanished-process race) is refused, never assumed innocent."""
    if not exe:
        return False
    p = Path(exe)
    return p.parent.name == "versions" and p.parent.parent.name == "claude"


def live_bodies(
    *,
    pgrep: PgrepFn = _pgrep_claude,
    read_cwd: ReadFn = _proc_cwd,
    read_exe: ReadFn = _proc_exe,
) -> dict[str, list[int]]:
    """{project: [pid, ...]} — real OS processes backing a project RIGHT NOW. Pure OS truth: no
    graph read, no notion of "mounted" or "live" from the registry's side at all.

    The project label reuses `resolve_identity`'s exact cwd→project fold (`read_project_label`'s
    `.osiris` walk, falling back to the cwd's basename) so a census label always lines up with
    whatever a mount row calls the same project — this module does not invent a second mapping.

    Injectable seam (`pgrep`/`read_cwd`/`read_exe`) so tests drive this with fakes; the module
    functions above are the real OS-facing default and are never exercised by a test directly.

    Best-effort at every layer, not just its own default `pgrep`: an INJECTED `pgrep` that
    raises degrades to an empty census exactly the same as a missing binary — a census is a
    cross-check, never a hard dependency the rest of the fleet's correctness needs."""
    try:
        pids = pgrep()
    except Exception:  # noqa: BLE001
        return {}
    out: dict[str, list[int]] = defaultdict(list)
    for pid in pids:
        if not _is_claude_body(read_exe(pid)):
            continue
        cwd = read_cwd(pid)
        if not cwd:
            continue
        project = read_project_label(cwd) or Path(cwd).name
        out[project].append(pid)
    return dict(out)

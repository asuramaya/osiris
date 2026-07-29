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

Both are invisible to a query that only ever asks the graph. This module asks the OS instead.
Claude Code rewrites argv to the bare literal `claude`, so no session id ever rides there, and
the harness appends-and-closes its transcript fd rather than holding it open, so both
`argv`-grep and `lsof`-on-a-held-fd are VERIFIED DEAD ENDS; do not resurrect them. `pgrep -x
claude` was the original door and it is a THIRD dead end (field-verified 2026-07-17): the
harness daemon's pty-hosted sessions run with comm `2.1.212` — the version string, not
`claude` — so a comm match misses every daemon-hosted body, and a sweep trusting it would
release the doors of living sessions. The honest net is WIDE-THEN-NARROW: `pgrep -u <uid>`
lists every process we own, and `/proc/<pid>/exe` — which resolves to the packaged binary
itself (`~/.local/share/claude/versions/<ver>`; the installer replaces the file in place, so
the directory shape is stable across version bumps even though the exact version string is
not) — is the one discriminator that never lies about what a process is. `/proc/<pid>/cwd` is
each body's project, exactly as `resolve_identity` derives one from a cwd
(`read_project_label`'s `.osiris` walk, falling back to the folder's basename) — reused here,
not reinvented, so a census label always matches a mount's.

BLIND IS NOT EMPTY: a census that could not run (pgrep missing, timed out, errored) returns
None from its pgrep seam, never []. `live_bodies` degrades a blind census to {} (it only ever
ADDS a cross-check), but `live_bodies_by_cwd` propagates the None — its callers hold a DELETE
verb, and "I could not look" must never be read as "nobody is home".

Pure OS truth, no graph read here at all — `fleet()` is the one place that folds this against
the mount registry's belief to make the gap visible. Every OS read is behind an injectable seam
so tests drive it with fakes, never a real `/proc` or a real `pgrep`.
"""
from __future__ import annotations

import os
import subprocess
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path

from src.orchestrator.agents import read_project_label
from src.orchestrator.offices import is_bare_office_root

PgrepFn = Callable[[], "list[int] | None"]
ReadFn = Callable[[int], "str | None"]


def _pgrep_candidates() -> list[int] | None:
    """Every PID this user owns — the wide net; `_is_claude_body`'s exe check is the narrow
    one (comm-matching `pgrep -x claude` misses daemon-hosted bodies whose comm is the
    version string — see the module doctrine). None means BLIND (pgrep itself failed: missing
    binary, timeout, an error exit) as opposed to an honest empty list — pgrep's exit 1
    ("no matches") stays an honest [], though for our own uid it cannot happen."""
    try:
        out = subprocess.run(
            ["pgrep", "-u", str(os.getuid())],
            capture_output=True, text=True, timeout=2, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode > 1:
        return None
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
    pgrep: PgrepFn = _pgrep_candidates,
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
    raises (or returns None — the blind census) degrades to an empty census exactly the same
    as a missing binary — a census here is a cross-check, never a hard dependency the rest of
    the fleet's correctness needs. A caller that would ACT on emptiness (the door sweep's
    delete verb) must use `live_bodies_by_cwd`, where blindness stays distinguishable."""
    try:
        pids = pgrep()
    except Exception:  # noqa: BLE001
        return {}
    if pids is None:
        return {}
    out: dict[str, list[int]] = defaultdict(list)
    for pid in pids:
        if not _is_claude_body(read_exe(pid)):
            continue
        cwd = read_cwd(pid)
        if not cwd:
            continue
        # THE BARE OFFICE ROOT (msg 1888): a body sitting at ~/.osiris/seats itself has no
        # `.osiris` pin and no single project of its own — the old unconditional basename
        # fallback minted the literal phantom "seats" here (Thoth's live specimen: fleet()
        # reporting "seats — 0 live · 3 sessions · 2 os bodies"). Skip it, same as
        # resolve_identity stays honestly unresolved from cwd rather than inventing one —
        # this pure OS census has no agent_id to resolve the real project through its seat
        # (that's seats.resolve_project's job), so dropping the pid beats mis-tallying it.
        if is_bare_office_root(cwd):
            continue
        project = read_project_label(cwd) or Path(cwd).name
        out[project].append(pid)
    return dict(out)


def live_bodies_by_cwd(
    *,
    pgrep: PgrepFn = _pgrep_candidates,
    read_cwd: ReadFn = _proc_cwd,
    read_exe: ReadFn = _proc_exe,
) -> dict[str, list[int]] | None:
    """{resolved cwd: [pid, ...]} — the door sweep's witness, cwd-grained where `live_bodies`
    is project-grained: an office and the repo it governs can share one project label, and a
    door may only be released on the word of the exact directory it opens into.

    None means the census was BLIND (pgrep itself failed) — a caller holding a delete verb
    must skip its tick entirely, never treat blindness as an empty box: releasing every fresh
    door because we could not look would bounce every living session to 'mount first'."""
    try:
        pids = pgrep()
    except Exception:  # noqa: BLE001
        return None
    if pids is None:
        return None
    out: dict[str, list[int]] = defaultdict(list)
    for pid in pids:
        if not _is_claude_body(read_exe(pid)):
            continue
        cwd = read_cwd(pid)
        if not cwd:
            continue
        out[str(Path(cwd).resolve())].append(pid)
    return dict(out)

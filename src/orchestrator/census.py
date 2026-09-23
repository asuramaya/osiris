"""Process census: OS-level truth alongside the graph's beliefs.

The durable mount registry (agent_mounts) records the graph's belief about who is live: a row
with a fresh `last_seen`. Two ways that belief can outlive reality:

  * a GHOST: closing or killing a session's terminal never retires its seat, so the row keeps
    its timestamp for up to `last_seen`'s 15-minute decay window even though nothing is
    listening any more (277 such rows were found in the fleet at the time this was
    investigated);
  * a PHANTOM MOUNT: a seat that registered identity (a real session id, a real cwd, registered
    unconditionally during mount) but never backed an actual session: no transcript ever
    materialized. No ping-window catches this case, because there was never a pulse to decay.

Both are invisible to a query that only ever asks the graph. This module asks the OS instead.
Claude Code rewrites argv to the bare literal `claude`, so no session id ever appears there,
and the harness appends and closes its transcript file descriptor rather than holding it open,
so both an `argv` grep and an `lsof` check on a held file descriptor are verified dead ends; do
not resurrect them. `pgrep -x claude` was tried first and is a third dead end (field-verified
2026-07-17): the harness daemon's pty-hosted sessions run with comm `2.1.212` (the version
string, not `claude`), so a comm match misses every daemon-hosted session, and a sweep trusting
it would release the resources of living sessions. The reliable approach is wide-then-narrow:
`pgrep -u <uid>` lists every process this user owns, and `/proc/<pid>/exe` (which resolves to
the packaged binary itself, `~/.local/share/claude/versions/<ver>`; the installer replaces the
file in place, so the directory shape is stable across version bumps even though the exact
version string is not) is the one discriminator that never lies about what a process is.
`/proc/<pid>/cwd` gives each session's project, using the same cwd-to-project resolution
`resolve_identity` uses (`read_project_label`'s `.osiris` walk, falling back to the directory's
basename), reused here rather than reinvented, so a census label always matches a mount's.

BLIND IS NOT EMPTY: a census that could not run (pgrep missing, timed out, errored) returns
None from its pgrep step, never []. `live_bodies` degrades a blind census to {} (it only ever
adds a cross-check), but `live_bodies_by_cwd` propagates the None: its callers can delete based
on this data, and "could not look" must never be read as "nobody is home".

Pure OS truth, no graph read here at all: `fleet()` is the one place that folds this against
the mount registry's belief to make the gap visible. Every OS read goes through an injectable
interface so tests drive it with fakes, never a real `/proc` or a real `pgrep`.
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
CmdlineFn = Callable[[int], bytes]


def _pgrep_candidates() -> list[int] | None:
    """Every PID this user owns: the wide part of the net; `_is_claude_body`'s exe check is
    the narrow part (comm-matching via `pgrep -x claude` misses daemon-hosted sessions whose
    comm is the version string; see the module-level explanation above). None means the census
    was blind (pgrep itself failed: missing binary, timeout, an error exit), as opposed to an
    honest empty list; pgrep's exit code 1 ("no matches") stays an honest [], though for our
    own uid that cannot happen."""
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
    """A process's working directory. None if it vanished between the pgrep snapshot and this
    read (a race, not an error: the process is simply no longer there to count)."""
    try:
        return str(Path(f"/proc/{pid}/cwd").resolve())
    except OSError:
        return None


def _proc_exe(pid: int) -> str | None:
    """A process's executable path. None on the same vanished-process race as `_proc_cwd`."""
    try:
        return str(Path(f"/proc/{pid}/exe").resolve())
    except OSError:
        return None


def _proc_environ(pid: int) -> bytes:
    """A process's raw NUL-separated environ, empty bytes on the same vanished-process race
    the sibling probes above absorb, or when the caller lacks permission to read another
    uid's /proc/<pid>/environ (root-owned or another user's session, never ours to read);
    never raises."""
    try:
        return Path(f"/proc/{pid}/environ").read_bytes()
    except OSError:
        return b""


def _proc_cmdline(pid: int) -> bytes:
    """The raw NUL-separated argv, the same one `osiris_hook.py`'s own `_is_bg_spare_process`
    already reads for its parent process (`b"bg-spare" in cmdline`); same check, any pid, so
    a server-side reader (the deploy gate) can ask the same question about a matched harness
    session instead of only a process's own view of itself. Empty bytes on the same
    vanished-process race the sibling probes above absorb; never raises."""
    try:
        return Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return b""


def _is_bg_spare(cmdline: bytes) -> bool:
    """Identifies a pre-warmed `claude bg-spare --bg-spare <sock>` process: a real,
    exe-verified `claude` process sitting at a real cwd (often a seat's own office root,
    the same "bare container" case `_is_bg_spare_process`'s own docstring names). Both
    `_is_claude_body` and a cwd match correctly fire on it, so without this check nothing
    downstream could tell it apart from an actual live agent session. One specimen: pid
    3750764, 14 hours old, cwd at a seat's directory, no turns ever run; `wake()` refused
    with `refused-occupied-foreign` and `launch()` refused with `already-live` against a
    stale generation because this spare process read as the occupant. Classify by cmdline
    (the same `b"bg-spare" in cmdline` check `osiris_hook.py`'s own `_is_bg_spare_process`
    already uses for a hook's own parent process), never by cwd or exe alone: a spare is
    never a genuine occupant of anything, and stays invisible to both `live_bodies` and
    `live_bodies_by_cwd`."""
    return b"bg-spare" in cmdline


def _is_claude_body(exe: str | None) -> bool:
    """Confirms `exe` is really the packaged claude binary (`.../claude/versions/<ver>`) and
    not some other process that happens to share `claude`'s truncated 15-character `comm`
    field, the second check that `pgrep -x` alone cannot provide. A structural check (parent
    directory `versions` under a `claude` directory), never a hardcoded version string: the
    installer replaces the binary in place on every version bump, so a literal version pin
    would silently go blind at the next update. `exe=None` (the vanished-process race) is
    refused, never assumed innocent."""
    if not exe:
        return False
    p = Path(exe)
    return p.parent.name == "versions" and p.parent.parent.name == "claude"


def live_bodies(
    *,
    pgrep: PgrepFn = _pgrep_candidates,
    read_cwd: ReadFn = _proc_cwd,
    read_exe: ReadFn = _proc_exe,
    read_cmdline: CmdlineFn = _proc_cmdline,
) -> dict[str, list[int]]:
    """{project: [pid, ...]}: real OS processes backing a project right now. Pure OS truth: no
    graph read, no notion of "mounted" or "live" from the registry's side at all.

    The project label reuses `resolve_identity`'s exact cwd-to-project resolution
    (`read_project_label`'s `.osiris` walk, falling back to the cwd's basename) so a census
    label always lines up with whatever a mount row calls the same project; this module does
    not invent a second mapping.

    A `claude bg-spare` pre-warmed process is never counted (see `_is_bg_spare`): a real,
    exe-verified claude process with no conversation of its own is not a live session of
    anything.

    Injectable interface (`pgrep`/`read_cwd`/`read_exe`/`read_cmdline`) so tests drive this
    with fakes; the module functions above are the real OS-facing default and are never
    exercised by a test directly.

    Best-effort at every layer, not just its own default `pgrep`: an injected `pgrep` that
    raises (or returns None, the blind census) degrades to an empty census exactly the same
    as a missing binary; a census here is a cross-check, never a hard dependency the rest of
    the fleet's correctness needs. A caller that would act on emptiness (a resource-cleanup
    sweep's delete step) must use `live_bodies_by_cwd`, where blindness stays
    distinguishable."""
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
        if _is_bg_spare(read_cmdline(pid)):
            continue
        cwd = read_cwd(pid)
        if not cwd:
            continue
        # The bare office root: a process sitting at ~/.osiris/seats itself has no `.osiris`
        # pin and no single project of its own. The old unconditional basename fallback
        # minted the literal phantom project "seats" here (one specimen: fleet() reporting
        # "seats: 0 live, 3 sessions, 2 os bodies"). Skip it, the same way resolve_identity
        # stays honestly unresolved from cwd rather than inventing one: this pure OS census
        # has no agent_id to resolve the real project through its seat (that's
        # seats.resolve_project's job), so dropping the pid beats mis-tallying it.
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
    read_cmdline: CmdlineFn = _proc_cmdline,
) -> dict[str, list[int]] | None:
    """{resolved cwd: [pid, ...]}: the cleanup sweep's witness, cwd-grained where
    `live_bodies` is project-grained: an office and the repo it governs can share one project
    label, and a resource may only be released on the word of the exact directory it opens
    into. This is also `_resume_occupancy_gate`'s own 'foreign' signal (trigger.py): a real
    claude process of unknown identity sitting in the exact directory a resume would land in.

    A `claude bg-spare` pre-warmed process is never counted (see `_is_bg_spare`): one
    specimen found a seat's own directory read as occupied by a 14-hour-old warm spare
    process (pid 3750764, cwd at the seat's directory, no turns ever run), so `wake()`
    refused with `refused-occupied-foreign` and `launch()` refused with `already-live`
    against the wrong generation. A spare is a real, exe-verified, cwd-matching process;
    cmdline (`b"bg-spare" in cmdline`) is the only signal that tells it apart from an actual
    occupant.

    None means the census was blind (pgrep itself failed): a caller holding a delete action
    must skip its cycle entirely, never treat blindness as an empty result: releasing every
    fresh resource because we could not look would bounce every living session back to
    'mount first'."""
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
        if _is_bg_spare(read_cmdline(pid)):
            continue
        cwd = read_cwd(pid)
        if not cwd:
            continue
        out[str(Path(cwd).resolve())].append(pid)
    return dict(out)


def job_dirs_for_pids(
    pids: list[int], *, read_environ: Callable[[int], bytes] = _proc_environ,
) -> dict[int, str]:
    """{pid: CLAUDE_JOB_DIR} for whichever of `pids` actually carry the env var: the one
    identity anchor that survives a working-directory move via `EnterWorktree` (fixed for an
    OS process's entire life, per `launch_seat`'s own docstring in trigger.py), where a
    resolved-cwd string match cannot see through it (a prior investigation traced a
    double-count in the ghost-gap check to exactly this gap). Pure OS truth, no graph read;
    `fleet()`'s own ghost_gap check is the one caller, correlating a false_live node's own
    `job_dir` against this before filing either side of what might really be one session,
    not two. A pid absent from the returned dict either vanished (the same race every
    sibling probe here absorbs) or never had the variable set (a non-osiris process the
    exe/cwd checks upstream already filtered by construction, or a permission-denied read);
    always omitted, never guessed as a false correlation."""
    out: dict[int, str] = {}
    for pid in pids:
        raw = read_environ(pid)
        if not raw:
            continue
        for entry in raw.split(b"\0"):
            if entry.startswith(b"CLAUDE_JOB_DIR="):
                value = entry[len(b"CLAUDE_JOB_DIR="):].decode("utf-8", errors="replace")
                if value:
                    out[pid] = value
                break
    return out

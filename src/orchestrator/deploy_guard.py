"""deploy_guard — the code-ahead-of-schema alarm (thread e6f5556f), plus the reboot-is-a-
deploy confession (thread 489a39d0). Both are BOOT-TIME checks, not periodic ones:
`osiris-preflight.timer` (the existing weekly audit) is the wrong home for either — no
systemd ordering ties it to a service's own boot, so bolting on there would leave the
identical class of gap open for up to 7 days (schema drift) or indefinitely (an unreviewed
reboot). This lives in each service's OWN startup.

LOUD ALARM, NEVER REFUSE-TO-SERVE (Thoth's ruling, DM 1339, after the operator drew the
identical lesson on the mount-guard the same day): `osiris-mcp` is a fleet-wide single point
of failure — one process, one shared pool, the whole fleet's only door in. A FALSE POSITIVE
from a buggy check refusing to serve would self-inflict a total outage on every boot, forever,
strictly worse than the silent drift this guard exists to catch. A loud alarm carries no such
asymmetry: a false positive costs one spurious alarm, a true positive achieves exactly the
goal (unmissable, never silent-until-someone-looks) without new blast radius. So: any error
IN the check itself degrades to UNKNOWN, never to a refusal — the same fail-open discipline
as every other net in this codebase (mark_swept, sweep_route, the miner's own health read).

THE REBOOT LEG (thread 489a39d0): on 2026-07-28 09:17 CDT the machine slept and woke, systemd
brought osiris-mcp/worker/console up on whatever HEAD happened to be checked out — three
commits HELD for executive review went live with no review, no gates, no smoke, no receipt.
`osiris deploy`'s own discipline (dirty-tree guard, migration gate, tool-delta narration)
only runs inside `osiris deploy`; a raw service restart or a reboot bypasses all of it. TWO
candidate fixes were named: pin services to a deployed ref (a checkout/worktree `osiris
deploy` advances), or a boot-time guard that confesses the gap. THIS LEG IS THE CONFESSION
ONLY — cheap, ships first, never blocks. The ref-pin is a deliberate, un-closed SEAM: nothing
here prevents an unreviewed boot from serving, it only makes sure nobody can miss that it
happened."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import subprocess
from pathlib import Path

import asyncpg
import httpx

_log = logging.getLogger("osiris.deploy_guard")

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# The watermark key `osiris deploy` writes on a successful restart (src/cli.py's own
# `_real_record_deploy`) and this module's reboot guard reads back — the generic cursor
# store (`watermarks`, get_cursor/set_cursor) pulse.py's own `devhead:<repo>` already uses,
# not a new table. Deliberately a DIFFERENT namespace than pulse's `devhead:` key: that one
# tracks the last HEAD the developer-persona heartbeat happened to observe (a read-only
# sensor), a wholly different fact than "the last HEAD that went through the deploy ritual".
_DEPLOY_CURSOR_KEY = "deployed:osiris"


def schema_drift(
    db_version: str | None, code_head: str | None, *, db_version_known: bool | None = None,
) -> str | None:
    """Pure comparison, no IO — isolated-testable. Either side unknown (an empty
    alembic_version table, a script directory that failed to load) means "don't know", never
    drift: only a genuine, confident mismatch between two known values is reported.

    DIRECTION-AWARE (decision 8d3f5e2d/ruling on capability-exists-but-unadopted, task #142
    follow-up): a bare mismatch used to read "code is running ahead of (or behind)" — the
    same third-state collapse 60bc15db names elsewhere, just here between two populations
    with opposite severities. `db_version_known` (whether `db_version` exists as a script in
    THIS TREE's own alembic chain, from `check_schema_drift`'s IO half) tells them apart:
    CODE_AHEAD_OF_DB (db_version_known is True or None/undeterminable — the ordinary,
    BENIGN transient between a merge and the next `alembic upgrade head`, already
    migrate-before-restart guarded on `osiris deploy`, decision cda0866cba0a) from
    DB_AHEAD_OF_TREE (db_version_known is False — this tree has never heard of that
    revision at all, meaning some OTHER branch ran a migration against this shared database
    before merging — the shape that blocked a whole deploy on 2026-08-13). Defaulting an
    undeterminable `db_version_known` to the CALM reading, never the alarm, matches this
    module's own fail-open law: a check that cannot tell direction must not manufacture the
    scarier one."""
    if not db_version or not code_head:
        return None
    if db_version == code_head:
        return None
    if db_version_known is False:
        return (f"DB_AHEAD_OF_TREE: DB is at revision {db_version!r}, which this tree's own "
               f"migrations do not recognize — another branch likely ran `alembic upgrade`/"
               f"`osiris migrate` against this shared database before merging (decision "
               f"8d3f5e2d). Do NOT run `alembic upgrade head`; find and merge the branch "
               f"that owns revision {db_version!r}, or the divergence persists.")
    return (f"CODE_AHEAD_OF_DB: code expects migration head {code_head!r}, DB is at "
           f"{db_version!r} — benign, the ordinary gap between a merge and the next "
           "`alembic upgrade head` (already migrate-before-restart guarded on `osiris "
           "deploy`, decision cda0866cba0a).")


async def check_schema_drift(pool: asyncpg.Pool) -> str | None:
    """The IO half. ANY failure here — DB unreachable, alembic_version missing, the script
    directory failing to load — degrades to None ("unknown"), never to a refusal."""
    try:
        from alembic.config import Config
        from alembic.script import ScriptDirectory
        from alembic.util.exc import CommandError

        cfg = Config(str(_REPO_ROOT / "alembic.ini"))
        sd = ScriptDirectory.from_config(cfg)
        code_head = sd.get_current_head()
        db_version = await pool.fetchval("SELECT version_num FROM alembic_version")
        db_version_known: bool | None = None
        if db_version:
            try:
                sd.get_revision(db_version)
                db_version_known = True
            except CommandError:
                db_version_known = False
        return schema_drift(db_version, code_head, db_version_known=db_version_known)
    except Exception as exc:  # noqa: BLE001 — a check that can't complete is UNKNOWN, not a refusal
        _log.warning("schema_drift check failed, treating as unknown: %r", exc)
        return None


async def alarm_schema_drift(pool: asyncpg.Pool, drift: str, *, service: str) -> None:
    """LOUD, never a refusal, and never something that can itself block a boot (callers wrap
    this in their own broad guard too — belt and suspenders on the one rule this whole module
    exists to keep). A durable Thread, idempotent on the drift's own text so a persistent gap
    across many restarts never mints a duplicate, plus a CRITICAL log line, plus an
    operator-desk brief with a generous dedup window (24h, not send_message's own 600s
    default) — a schema drift can easily outlive ten minutes across restarts, and re-briefing
    the desk every single boot would be exactly the kind of noise that makes a real alarm
    easy to tune out.

    THE THREAD SUMMARY DELIBERATELY OMITS `service` (thread 35c425f9, the boot-listener
    double-record bug): open_thread's idempotency is a hash of the summary TEXT
    (`_canon("thread", summary)`), so baking `{service}` into it made osiris-mcp and
    osiris-worker mint two separate Thread objects for the identical drift condition, one
    per listener, instead of converging on one. `service` still survives per-observation —
    the log line, the operator DM body, and `source=f"boot:{service}"` (a per-assertion
    witness, not part of the canonical identity) all still name it."""
    from src.actions.core import Actions
    from src.orchestrator.capture import open_thread
    from src.orchestrator.mailbox import send_message

    _log.critical("%s booted against a drifted schema: %s", service, drift)
    actions = Actions(pool)
    # `drift` is now direction-aware and self-describing (schema_drift's own
    # CODE_AHEAD_OF_DB/DB_AHEAD_OF_TREE labels, each carrying its own correct action) — this
    # wrapper no longer hard-codes "run alembic upgrade head", which was actively WRONG
    # advice for the DB_AHEAD_OF_TREE case (decision 8d3f5e2d).
    await open_thread(
        actions, f"SCHEMA DRIFT: {drift}",
        kind="obligation", arc="Fleet-Hygiene", severity="alarm", source=f"boot:{service}",
    )
    with contextlib.suppress(Exception):  # the desk being unreachable must not compound the alarm
        await send_message(
            pool, from_agent=f"system:{service}", from_project="osiris", to_project="operator",
            body=f"{service} booted with a drifted schema — {drift}",
            dedup_window_secs=86400,
        )


def _git_head(repo_root: Path) -> str | None:
    """The running code's own on-disk HEAD — same shape as pulse.py's own `_git_head`
    (that one takes a str path for a developer-persona sensor over arbitrary dev repos; this
    one is deploy_guard's own copy, scoped to a Path, so this module has no import-time
    dependency on `src.orchestrator.pulse`). None on any git failure, never raised —
    deploy_guard's fail-open law applies here too."""
    try:
        return subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def unreviewed_boot(running_head: str | None, last_deployed: str | None) -> str | None:
    """Pure comparison, no IO — same null-handling discipline as `schema_drift`: either side
    unknown means 'don't know', never a mismatch. A box that has never once run `osiris
    deploy` (no cursor yet) is not evidence of an unreviewed reboot — it's evidence this
    guard is new, or the box is fresh."""
    if not running_head or not last_deployed:
        return None
    if running_head == last_deployed:
        return None
    return (f"running HEAD {running_head!r} was never recorded by `osiris deploy` (last "
           f"recorded deploy: {last_deployed!r})")


async def check_unreviewed_boot(pool: asyncpg.Pool) -> str | None:
    """The IO half — same fail-open discipline as `check_schema_drift`: any failure (git
    missing, not a checkout, the watermark unreadable) degrades to None, never a refusal."""
    try:
        from src.orchestrator.monitor import get_cursor

        running = _git_head(_REPO_ROOT)
        last_deployed = await get_cursor(pool, _DEPLOY_CURSOR_KEY)
        return unreviewed_boot(running, last_deployed)
    except Exception as exc:  # noqa: BLE001 — a check that can't complete is UNKNOWN, not a refusal
        _log.warning("unreviewed_boot check failed, treating as unknown: %r", exc)
        return None


def diverged_since_last_deploy(
    running_head: str | None, last_deployed: str | None, *, is_ancestor: bool | None,
) -> str | None:
    """Pure comparison, no IO — same null-handling discipline as `schema_drift`/
    `unreviewed_boot`. DISTINCT from `unreviewed_boot` (which fires on ANY difference,
    including a normal fast-forward advance): this fires ONLY when `last_deployed` is no
    longer an ancestor of `running_head` — the branch was rewritten, reset, or force-moved
    sideways since the last deploy, never just advanced. `is_ancestor=None` means the
    ancestry check itself could not run (a git failure, an unknown sha, a fresh box with no
    prior deploy) — 'unknown', never a mismatch (thread 771366d1: this whole check exists
    because two agents each acting on a locally-correct read moved the SAME ref out from
    under each other tonight — a false alarm here would be exactly the noise that teaches a
    reader to stop looking)."""
    if not running_head or not last_deployed or running_head == last_deployed:
        return None
    if is_ancestor is None or is_ancestor:
        return None
    return (f"HISTORY DIVERGED SINCE THE LAST DEPLOY: {last_deployed!r} (what `osiris "
            f"deploy` last recorded) is no longer an ancestor of the current HEAD "
            f"{running_head!r} — this branch was rewritten, reset, or force-moved sideways, "
            f"not just advanced. If nobody meant to rewrite history, find out who else "
            f"touched this ref before trusting this deploy.")


def _is_ancestor(repo_root: Path, older: str, newer: str) -> bool | None:
    """True/False from `git merge-base --is-ancestor <older> <newer>` — exit 0 means
    `older` IS an ancestor of `newer` (a normal fast-forward), exit 1 means it is NOT (a
    rewrite/reset/force-move). Any OTHER outcome — git missing, not a repo, either sha
    unknown to this checkout (exit 128) — is None, 'unknown', same fail-open law as every
    other check in this module: a check that cannot complete must never be read as a
    confident mismatch."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "merge-base", "--is-ancestor", older, newer],
            capture_output=True, text=True, check=False,
        )
    except OSError:
        return None
    if proc.returncode == 0:
        return True
    if proc.returncode == 1:
        return False
    return None


async def check_diverged_since_last_deploy(
    pool: asyncpg.Pool, *, repo_root: Path | None = None,
) -> str | None:
    """The IO half — same fail-open discipline as `check_unreviewed_boot`: any failure
    (git missing, not a checkout, the watermark unreadable) degrades to None, never a
    refusal. Deliberately NOT wired into service boot (unlike its two siblings above) —
    the race this exists to catch happens around `osiris deploy` time, when a human/agent
    is about to trust whatever ref is currently checked out, not at a service restart.

    `repo_root` MUST be the caller's own already-resolved deploy target when one is
    running a deploy (live false positive, 2026-08-04, Thoth's own catch on the deploy
    immediately before the second history rewrite): this house runs FIVE worktrees
    permanently, each an independent checkout with its OWN copy of this module on disk.
    `_REPO_ROOT` (module-level, derived from THIS FILE's own `__file__`) resolves to
    whichever worktree Python happened to import this module from — not necessarily the
    repo the deploy is actually acting on. Confirmed live: a deploy from the main
    checkout warned that the watermark '09bbd7c' was no longer an ancestor of HEAD
    '6af5eb3', when '6af5eb3' was Seshat's OWN worktree branch tip, not composer's —
    `_REPO_ROOT` had resolved to her worktree. `cmd_deploy` already resolves the correct
    `root` before this call (via `_find_repo_root()`/its own `repo_root` param); it must
    pass it through, never let this function guess. Falls back to `_REPO_ROOT` ONLY for
    a caller with no better answer (there is currently none — cmd_deploy always has one),
    so the signature stays callable standalone without silently becoming a no-op."""
    try:
        from src.orchestrator.monitor import get_cursor

        root = repo_root if repo_root is not None else _REPO_ROOT
        running = _git_head(root)
        last_deployed = await get_cursor(pool, _DEPLOY_CURSOR_KEY)
        ancestor = None
        if running and last_deployed and running != last_deployed:
            ancestor = _is_ancestor(root, last_deployed, running)
        return diverged_since_last_deploy(running, last_deployed, is_ancestor=ancestor)
    except Exception as exc:  # noqa: BLE001 — a check that can't complete is UNKNOWN, not a refusal
        _log.warning("diverged_since_last_deploy check failed, treating as unknown: %r", exc)
        return None


async def alarm_unreviewed_boot(
    pool: asyncpg.Pool, drift: str, *, running_head: str, service: str,
    src_root: str | None = None,
) -> None:
    """LOUD, never a refusal, never blocking — the reboot-is-a-deploy confession (thread
    489a39d0). Same shape as `alarm_schema_drift`, including the same lesson already applied
    there: `service` stays OUT of the Thread summary (the canonical-identity text) so two
    services confessing the same unreviewed HEAD converge on one Thread, not two — it still
    survives per-observation via the log line, the operator DM, and `source`.

    `src_root` (task #180 piece 2, msg 5253): the interpreter's own resolved `src.__file__`
    parent, printed BESIDE `running_head` — the venv-drift specimen (decision 6fc0c082) means
    a boot's own git ref and the CODE it is actually running can disagree; naming both facts
    in the same alarm is what would have made that week-long drift visible on day one. Same
    OUT-OF-THE-THREAD-SUMMARY treatment as `service`: `venv_import_hygiene` is the intended
    home for a genuine drift alarm (it fires standalone, deploy-time, independent of whether
    this reboot guard also fires) — folding a venv path into THIS Thread's dedup identity
    would mint a fresh Thread per differing path even when the underlying unreviewed-boot
    fact hasn't changed. Optional and appended-only: existing callers with no `src_root` to
    hand keep working unchanged.

    DEDUPS ON `running_head` ALONE, not the full `drift` text (decision 8a830336): `drift`
    also embeds `last_deployed`, which changes on every SUBSEQUENT successful `osiris
    deploy` — so the SAME unreviewed commit confessing across several restarts, with a
    different watermark each time, used to mint a fresh Thread per restart instead of
    converging on one. `running_head` alone is the actual fact worth deduping on: 'this
    exact commit is still unreviewed', regardless of what the ledger said it was compared
    against at the moment of each confession. `drift`'s fuller context (both shas) still
    reaches the log line and the operator brief, same as `service` does."""
    from src.actions.core import Actions
    from src.orchestrator.capture import open_thread
    from src.orchestrator.mailbox import send_message

    src_note = f" (src resolves from {src_root})" if src_root else ""
    _log.critical("%s booted on an unreviewed ref: %s%s", service, drift, src_note)
    actions = Actions(pool)
    await open_thread(
        actions,
        f"UNREVIEWED BOOT: running HEAD {running_head!r} was never recorded by `osiris "
        "deploy`. A service came up on code that never went through `osiris deploy` — "
        "most likely a raw service restart or a machine reboot picking up the working "
        "tree as-is, bypassing the dirty-tree guard and migration gate. Nothing was "
        "blocked; review what's actually running before trusting it, then run `osiris "
        "deploy` so the ledger and reality agree again.",
        kind="obligation", arc="Fleet-Hygiene", severity="alarm", source=f"boot:{service}",
    )
    with contextlib.suppress(Exception):  # the desk being unreachable must not compound the alarm
        await send_message(
            pool, from_agent=f"system:{service}", from_project="osiris", to_project="operator",
            body=f"{service} booted on an unreviewed ref — {drift}{src_note}. Review before "
                 "trusting it, then `osiris deploy` to re-sync the ledger.",
            dedup_window_secs=86400,
        )


async def origin_visibility(repo_root: Path) -> str:
    """THE READ-SIDE ALARM (2026-08-15 incident, ruling 2fc98818): "NO SEAT PUSHES" was
    restated in every deploy report for over a day while origin sat PUBLIC with four
    branches and the operator's own email in six commit messages, because the rule lived
    only in prose and nobody ever ran `git ls-remote` — a policy-layer instance of 60bc15db
    (a check that cannot distinguish "nobody pushed" from "nobody looked", and reports the
    former). This measures origin's TRUE state, every deploy, so the report is an
    instrument again, not a restated assumption.

    NEVER REFUSES, NEVER BLOCKS (577988ed, same fail-open discipline as `schema_drift`
    above): any read failure — no origin configured, `ls-remote` erroring, the network
    down, GitHub's API unreachable — degrades to an honest 'unknown' segment in the
    returned line, never a silent omission and never a raised exception that could hold up
    a deploy. A false alarm here costs one confusing report line; a refusal would strand
    every deploy on a network blip, exactly the asymmetry this module's own docstring
    already names.

    TWO INDEPENDENT READS, deliberately not one: `git ls-remote --heads origin` names
    every reachable branch — the actual blast radius of whatever has already been pushed,
    not what any seat MEANT to push. GitHub's own unauthenticated repos API confesses
    `private` directly; `ls-remote` alone cannot answer "can a stranger see this" because
    it succeeds identically whether the repo is public or merely one we hold credentials
    for. READ-ONLY BY CONSTRUCTION: no fetch, no clone, no write — a bare listing of what
    the network already advertises."""
    url = (await asyncio.to_thread(
        subprocess.run, ["git", "config", "--get", "remote.origin.url"], cwd=repo_root,
        capture_output=True, text=True, timeout=5, check=False)).stdout.strip()
    if not url:
        return "origin: no remote configured"

    ls = await asyncio.to_thread(
        subprocess.run, ["git", "ls-remote", "--heads", "origin"], cwd=repo_root,
        capture_output=True, text=True, timeout=15, check=False)
    if ls.returncode != 0:
        return (f"origin: ls-remote failed ({ls.stderr.strip()[:200]!r}) — "
                "branches/visibility UNKNOWN, not assumed clean")
    branches = sorted(
        line.split("refs/heads/", 1)[1] for line in ls.stdout.splitlines()
        if "refs/heads/" in line)

    match = re.search(r"github\.com[:/]([^/]+)/([^/.]+?)(?:\.git)?/?$", url)
    visibility = "unknown (remote isn't a recognizable github.com owner/repo URL)"
    if match:
        owner, name = match.group(1), match.group(2)
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"https://api.github.com/repos/{owner}/{name}")
            if resp.status_code == 200:
                visibility = "PUBLIC" if resp.json().get("private") is False else "private"
            elif resp.status_code == 404:
                visibility = "private-or-nonexistent (404, unauthenticated read)"
            else:
                visibility = f"unknown (GitHub API returned {resp.status_code})"
        except Exception as exc:  # noqa: BLE001 — an unreachable check is UNKNOWN, never a silent PRIVATE
            visibility = f"unknown ({exc})"

    names = f" — {', '.join(branches)}" if branches else ""
    return f"origin: {visibility}, {len(branches)} branch(es) reachable{names}"


async def local_ref_hygiene(repo_root: Path) -> str:
    """THE SECOND SURFACE OF THE SAME INCIDENT (Thoth's own post-incident audit, msg 4671):
    `origin_visibility` reads what origin already carries; this reads what this LOCAL
    checkout would carry if someone ran `git push --mirror` — the exact mechanism that kept
    a pre-redaction object graph alive tonight. Five stray `refs/temp-main*` refs, left over
    from an earlier session, held 2,096 commits reachable — every CONTENT check on the real
    branches read clean; only the COMMIT COUNT disagreeing with what the intended branch set
    should carry gave it away. `--mirror` publishes every ref under `refs/`, not just
    `refs/heads/*` and `refs/tags/*` (git's own documented behavior) — so any OTHER
    namespace present locally (a stray `refs/temp-main*`, a leftover `refs/original/*` from
    a past filter-repo run, anything unexpected) is exactly this exposure surface.

    NEVER REFUSES (577988ed, same discipline as `origin_visibility`): any git failure
    degrades to an honest 'unknown' segment, never silently 'clean'.

    LOCAL-ONLY, NO NETWORK: everything here is `git for-each-ref`/`git rev-list` against
    this checkout's own refs — a read of what COULD be sent, not a read of origin itself
    (that's `origin_visibility`'s job). ORDINARY REMOTE-TRACKING REFS ARE NOT STRAY
    (`refs/remotes/*` — present in EVERY checkout that has ever fetched, and even though
    `--mirror` literally does republish them too, doing so only reflects origin's own
    already-public history back at itself, never undisclosed local-only content; flagging
    them would drown the real signal in noise present on every single checkout). "Stray"
    here means every ref OUTSIDE `refs/heads/*`, `refs/tags/*`, AND `refs/remotes/*` — the
    exact shape of tonight's specimen (`refs/temp-main*`, an ad-hoc local scratch namespace
    nobody currently uses for anything, not a remote-tracking ref). The commit-count
    comparison follows the same scope: "intended" is heads+tags+remotes (everything a
    normal checkout legitimately carries), so a non-zero "extra" means commits reachable
    ONLY through a genuinely stray ref — precisely the signature that caught tonight's
    incident, worth surfacing even when the stray ref's own name looks innocuous."""
    all_refs = await asyncio.to_thread(
        subprocess.run, ["git", "for-each-ref", "--format=%(refname)"], cwd=repo_root,
        capture_output=True, text=True, timeout=15, check=False)
    if all_refs.returncode != 0:
        return (f"ref hygiene: for-each-ref failed ({all_refs.stderr.strip()[:200]!r}) — "
                "UNKNOWN, not assumed clean")
    refs = [line for line in all_refs.stdout.splitlines() if line]
    _ordinary = ("refs/heads/", "refs/tags/", "refs/remotes/")
    stray = sorted(r for r in refs if not r.startswith(_ordinary))

    intended = await asyncio.to_thread(
        subprocess.run,
        ["git", "rev-list", "--count", "--branches", "--tags", "--remotes"],
        cwd=repo_root, capture_output=True, text=True, timeout=15, check=False)
    everything = await asyncio.to_thread(
        subprocess.run, ["git", "rev-list", "--count", "--all"], cwd=repo_root,
        capture_output=True, text=True, timeout=15, check=False)
    if intended.returncode != 0 or everything.returncode != 0:
        counts_note = "commit counts UNKNOWN (rev-list failed)"
    else:
        try:
            n_intended = int(intended.stdout.strip())
            n_all = int(everything.stdout.strip())
        except ValueError:
            counts_note = "commit counts UNKNOWN (unparseable rev-list output)"
        else:
            extra = n_all - n_intended
            counts_note = (
                f"{n_all} commits reachable from ALL local refs, {n_intended} from "
                f"heads+tags+remotes — {extra} extra reachable only via a stray ref"
                if extra else f"{n_all} commits reachable, all of it via heads+tags+remotes")

    stray_note = (f"{len(stray)} ref(s) outside refs/heads|tags|remotes: {', '.join(stray)}"
                  if stray else "no refs outside refs/heads, refs/tags, or refs/remotes")
    return f"ref hygiene: {stray_note}; {counts_note}"


_MERGE_SUBJECT_BRANCH = re.compile(r"^merge\s+([^\s:]+)")  # stop at whitespace OR a directly-
# attached colon ("merge foo: did the thing") — both shapes appear in this house's own log


def _resolve_imported_src_root() -> Path:
    """Sync helper (ASYNC240, this codebase's own ruff gate: filesystem resolution stays out
    of async function bodies) — `import src; Path(src.__file__).resolve().parent.parent` is
    the SAME resolution every daemon's own `_REPO_ROOT`-style derivation goes through (this
    module's own `_REPO_ROOT` included)."""
    import src as _src

    src_file = getattr(_src, "__file__", None)
    if not src_file:
        raise ValueError("`src` has no __file__ (namespace package?)")
    return Path(src_file).resolve().parent.parent


def _resolve_path(p: Path) -> Path:
    """Sync helper (ASYNC240) — `Path.resolve()` stays out of the async caller's own body."""
    return p.resolve()


async def venv_import_hygiene(repo_root: Path) -> str:
    """THE VENV-DRIFT SPECIMEN (Thoth's confirmation, decision 6fc0c082, 2026-08-18):
    `/home/asuramaya/code/osiris/.venv`'s editable-install pointer
    (`_editable_impl_osiris.pth`, a raw directory string Python's site-packages loader
    resolves `src.*` imports through) had pointed at Imhotep's own worktree instead of
    self-referencing this repo since 2026-08-11 17:26 — over a week. Every daemon sharing
    that venv (`ExecStart=.../.venv/bin/...`) was silently running whatever Imhotep's
    worktree happened to have checked out, not the tree `osiris deploy` believed it was
    deploying. A stranger's machine must never run a worktree's code under main's name.

    NEVER REFUSES (577988ed, same fail-open discipline as every check beside it): an
    import failure, a namespace package with no `__file__`, or any other surprise degrades
    to 'unknown', never a blocked deploy — this is read-only corroboration, not a gate."""
    try:
        resolved_src_root = await asyncio.to_thread(_resolve_imported_src_root)
    except Exception as exc:  # noqa: BLE001 — an import failure is UNKNOWN, never a refusal
        return (f"venv import: could not import `src` to check ({exc!r}) — unknown, "
                "not assumed clean")

    try:
        resolved_repo_root = await asyncio.to_thread(_resolve_path, repo_root)
    except OSError as exc:
        return f"venv import: could not resolve repo_root ({exc!r}) — unknown, not assumed clean"

    if resolved_src_root == resolved_repo_root:
        return (f"venv import: clean — `src` resolves inside the deploying tree "
                f"({resolved_src_root})")
    return (f"venv import: ⚠ `src` resolves to {resolved_src_root}, NOT the deploying tree "
            f"{resolved_repo_root} — the venv's editable-install pointer names a different "
            "checkout (the exact shape of the Imhotep-worktree specimen, decision 6fc0c082); "
            "every daemon sharing this venv is running THAT tree's code, not this one's")


async def merge_claim_hygiene(repo_root: Path) -> str:
    """THE UNVERIFIED CLAIM (Sekhmet's specimen, msg 5201, thread #175/#180): commit fd3a703's
    own subject line read "merge sekhmet-launch-resume-fix + ratchet ..." — but
    `sekhmet-launch-resume-fix`'s tip was never actually an ancestor of fd3a703; the merge
    claimed a branch it never contained. This house's merge commits follow one convention
    consistently (sampled 20 recent: every single one) — the subject starts `merge
    <branch-name>` — so the claim is MECHANICALLY CHECKABLE: `git merge-base --is-ancestor
    <branch> HEAD` is either true or it isn't, no prose-reading required.

    NEVER REFUSES (577988ed, same fail-open discipline as `origin_visibility`/
    `local_ref_hygiene` beside it): a git failure, HEAD not being a merge subject shape, or
    the named branch no longer existing (deleted after merging, the common case) all degrade
    to a quiet 'nothing to verify' or 'unknown', never a blocked deploy — this is read-only
    corroboration of a claim already made, not a gate on making one.

    ONLY THE FIRST NAMED BRANCH is checked: the convention names exactly one real branch per
    merge (a "five-branch wave" commit still only merges ONE branch as its actual second git
    parent — the rest of the subject is prose about a ratchet or a batch, not more branches),
    and Sekhmet's own specimen was a single mis-claim, not a list."""
    head = await asyncio.to_thread(
        subprocess.run, ["git", "rev-parse", "HEAD"], cwd=repo_root,
        capture_output=True, text=True, timeout=5, check=False)
    if head.returncode != 0:
        return "merge claim: HEAD unknown, nothing to verify"
    subject = (await asyncio.to_thread(
        subprocess.run, ["git", "log", "-1", "--format=%s"], cwd=repo_root,
        capture_output=True, text=True, timeout=5, check=False)).stdout.strip()
    match = _MERGE_SUBJECT_BRANCH.match(subject)
    if not match:
        return "merge claim: HEAD's subject doesn't name a branch, nothing to verify"
    branch = match.group(1)
    exists = await asyncio.to_thread(
        subprocess.run, ["git", "rev-parse", "--verify", "--quiet", f"refs/heads/{branch}"],
        cwd=repo_root, capture_output=True, text=True, timeout=5, check=False)
    if exists.returncode != 0:
        return (f"merge claim: HEAD names {branch!r} but that branch no longer exists locally "
                "— unverifiable, not assumed false")
    proof = await asyncio.to_thread(_is_ancestor, repo_root, branch, head.stdout.strip())
    if proof is None:
        return f"merge claim: {branch!r} named, ancestry check inconclusive — unknown, not assumed"
    if proof:
        return f"merge claim: {branch!r} verified — an actual ancestor of HEAD"
    return (f"merge claim: ⚠ HEAD's subject names {branch!r} but it is NOT an ancestor — "
            "the exact shape of fd3a703's own specimen (named, never merged)")

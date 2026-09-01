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
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import asyncpg
import httpx

if TYPE_CHECKING:
    from src.actions.core import Actions

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


def _merge_parents(repo_root: Path, sha: str) -> list[str] | None:
    """A commit's own parent shas, oldest-recorded-first (`git log -1 --format=%P`) — None
    only on a git failure (fail open, same law as `_is_ancestor`), never mistaken for
    'confirmed zero parents'. THE STRONGEST POSSIBLE PROOF a merge claim is genuine (Thoth
    XC, thread 9b6b5269): a commit's parent list is written into its own sha at creation —
    unlike a branch ref (which moves, gets reused, gets deleted) it cannot be rewritten
    after the fact without changing the sha itself. A subject claiming 'merge X' on a
    commit with FEWER than two parents structurally never merged anything — the fd3a703
    shape, confirmed by the commit's own DAG rather than by comparing it against a branch
    name that may since have moved on."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "log", "-1", "--format=%P", sha],
            capture_output=True, text=True, check=False,
        )
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    line = proc.stdout.strip()
    return line.split() if line else []


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
        # first real user of the hatch (Thoth msg 5858): this alarm fires with no ctx and
        # no mounted caller — a claim about the SERVICE's own deploy state, which has no
        # SoftwareProject to declare. Refusing an alarm because it can't name a repo would
        # make the write that fires when something has gone wrong the one write the gate
        # rejects; unlinked_because turns it into a DECLARED gap instead of a silent orphan.
        unlinked_because="service-scoped claim: a deploy-state alarm has no SoftwareProject",
    )
    with contextlib.suppress(Exception):  # the desk being unreachable must not compound the alarm
        await send_message(
            pool, from_agent=f"system:{service}", from_project="osiris", to_project="operator",
            body=f"{service} booted on an unreviewed ref — {drift}{src_note}. Review before "
                 "trusting it, then `osiris deploy` to re-sync the ledger.",
            dedup_window_secs=86400,
        )


async def alarm_withheld_deploy_record(
    pool: asyncpg.Pool, *, running_head: str, reason: str,
) -> None:
    """THE WITHHELD-RECORD CONFESSION (thread 3b34f6c5, #52's own law: "the ledger's
    default failure is not rot, it is unrecorded completion"). `cmd_deploy`'s halcyon
    gate (ruling 921eabcf, `_real_check_false_mint_live`) can CORRECTLY refuse to record
    a deploy — a live specimen (decision 9992cf39, third of its shape) proved the code
    was genuinely restarted, healthy, and serving while the ledger record was withheld
    on a false-mint-live anomaly in ANOTHER house's identity, correctly left unrepaired
    (ruling a2cf8405-adjacent: not this house's identity to rewrite). REFUSING TO RECORD
    WAS RIGHT. RECORDING NOTHING WAS NOT: the watermark `_DEPLOY_CURSOR_KEY` reads stale,
    so the next reader of it alone concludes `running_head` never shipped — exactly #52's
    disease, except here a MECHANISM produces it on purpose rather than a row nobody
    updated. This is the confession that makes the silence speak, same family as
    `alarm_unreviewed_boot` above and Khnum's own inert-hatch fix (`unlinked_because`
    recorded even when unenforced, commit on thread eea88e1c's own arc) — a refusal that
    leaves no trace is functionally identical to a bug nobody can see.

    LOUD, NEVER A SECOND GATE: this never blocks, never retries the record, and never
    touches `_DEPLOY_CURSOR_KEY` itself — writing the watermark here would silently
    launder the very refusal this function exists to make visible, the same trap a
    "confession that quietly fixes what it's confessing" would be. `reason` is the
    caller's own already-composed refusal text (the exact lines `cmd_deploy` printed) —
    this function does not re-derive or summarize it, matching `_real_check_false_mint_
    live`'s own contract that a query has no business writing prose and a deploy gate
    has no business abbreviating the prose it already wrote.

    IDEMPOTENT ON THE EXACT SUMMARY (`open_thread`'s own contract, no `repo` passed —
    same reason `alarm_unreviewed_boot` passes none: a deploy-ledger alarm has no
    SoftwareProject to declare): a repeated deploy attempt against the SAME
    `running_head` with the SAME unresolved refusal mints no second Thread; a NEW head
    or a changed reason (the anomaly was reconciled differently, or a different
    candidate now offends) is a genuinely new fact and gets its own."""
    from src.actions.core import Actions
    from src.orchestrator.capture import open_thread
    from src.orchestrator.mailbox import send_message

    _log.critical("deploy record withheld for HEAD %s: %s", running_head, reason)
    actions = Actions(pool)
    await open_thread(
        actions,
        f"DEPLOY RECORD WITHHELD: HEAD {running_head!r} deployed successfully (restarted, "
        "healthy, whisper-probed clean) but `osiris deploy` refused to record it in the "
        f"ledger. Reason: {reason} The ledger's last recorded deploy is now stale — a "
        "reader who trusts it alone will wrongly conclude this HEAD never shipped. It did. "
        "Reconcile the flagged anomaly, then either re-run `osiris deploy` (a no-op restart "
        "will still re-check and, once clean, record) or correct the watermark by hand so "
        "the ledger and reality agree again.",
        kind="obligation", arc="Fleet-Hygiene", severity="alarm", source="deploy:withheld",
        unlinked_because="service-scoped claim: a deploy-ledger alarm has no SoftwareProject",
    )
    with contextlib.suppress(Exception):  # the desk being unreachable must not compound the alarm
        await send_message(
            pool, from_agent="system:osiris-deploy", from_project="osiris", to_project="operator",
            body=f"osiris deploy withheld its ledger record for HEAD {running_head} even "
                 f"though the code deployed cleanly — {reason} The ledger is stale until "
                 "this is reconciled.",
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


_MERGE_SUBJECT_BRANCH = re.compile(
    r"^merge\s+([^\s:]+)(?:\s+\(([0-9a-f]{7,40})\))?", re.IGNORECASE)
# CASE-INSENSITIVE since 2026-09-01 (Thoth XC, thread 9b6b5269, decision b86e65ed): this
# house's OWN commit-subject convention drifted from "merge <branch>" to "Merge <branch>"
# sometime before that date — a fresh sample that day found the 9 MOST RECENT merges
# capital-M, the other 50 in the trailing window lowercase. Every capital-M merge since the
# drift went unverified by this check, silently, because the lowercase-only literal simply
# never matched — see merge_claim_hygiene's own docstring for why the no-match branch was
# ALSO wrong (it read the miss as "nothing to verify" rather than "could not parse").
#
# group 1 stops at whitespace OR a directly-attached colon ("merge foo: did the thing") —
# both shapes appear in this house's own log. group 2, OPTIONAL, is the parenthetical
# commit sha this house's own convention cites right after the branch name in the majority
# of recent merges ("merge <branch> (<sha>) — ..."): found live (dry-run against this
# repo's own history, obligation 8752024d's ranged walk) — `sekhmet-150-backlog` was
# reused across TWO separate merges weeks apart; checking the branch's CURRENT tip against
# the OLDER merge commit false-flags it as unverified the moment the branch moves on to a
# second round of work, even though the historical merge was completely genuine (the cited
# sha from that merge IS an ancestor). The cited sha, when present, is TIME-STABLE proof a
# branch's own current tip can never be — preferred over the branch-tip check below, which
# `_verify_one_merge_claim` now also strengthens directly for the case no sha was cited.


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


async def _verify_one_merge_claim(
    repo_root: Path, sha: str, branch: str, cited: str | None = None,
) -> str:
    """One commit's own claim, checked in isolation — shared by BOTH the ranged walk and
    the HEAD-only fallback below (unified 2026-09-01, Thoth XC: the two legs used to carry
    separate inline copies specifically so an earlier fix's wording stayed byte-for-byte
    unchanged; a second, SUBSTANTIVE change to the verification logic itself is exactly the
    case that copy was always going to have to be kept in sync by hand, so it stops being
    two copies here). Three outcomes, never a fourth: VERIFIED, UNVERIFIABLE (the named
    branch no longer exists locally, or a check couldn't complete), or FAILED.

    STRUCTURAL PROOF FIRST (thread 9b6b5269's own upgrade, decision b86e65ed): a commit's
    PARENT LIST cannot be rewritten after the fact without changing its own sha — unlike a
    branch ref, which moves, gets reused, or gets deleted. Fewer than two parents means the
    subject claims a merge that never structurally happened at all: FAILED outright, the
    fd3a703 shape confirmed by the DAG itself, needing no branch or cited sha to prove.

    `cited` (the subject's own parenthetical sha, when present) is checked next, preferred
    over the branch's current tip — the branch-reuse specimen the module's own comment
    names: a branch's tip is not time-stable, the cited sha is.

    THE BRANCH-TIP CHECK, TWO DIRECTIONS KEPT BOTH (Thoth's own ruling: "if (c) turns out
    to have a case the branch-tip check covers and it does not, name it and keep both — do
    not silently drop a check"). OLD direction — is the branch's CURRENT tip an ancestor of
    `sha`? — holds when the branch was never touched again after merging, but false-
    positives the moment it was reused for further work (seshat-b98eb0b-casualty-sweep,
    2026-09-01: a live branch simply kept moving forward after a completely genuine merge).
    NEW direction — is `sha`'s own SECOND PARENT (the immutable thing this merge actually
    incorporated) an ancestor of the branch's current tip? — holds whether or not the
    branch was reused afterward, but would miss the rarer case of a branch WOUND BACKWARD
    to an earlier point that is still validly an ancestor of the merge (the old direction's
    own strength). VERIFIED if EITHER direction confirms; FAILED only when BOTH definitively
    disagree — a genuine mismatch in every direction there is."""
    parents = await asyncio.to_thread(_merge_parents, repo_root, sha)
    if parents is not None and len(parents) < 2:
        return (f"{sha[:8]} ⚠ subject claims a merge of {branch!r} but this commit has "
                f"only {len(parents)} parent(s) — not a real merge, the fd3a703 shape "
                "confirmed structurally (needs no branch or cited sha to prove)")
    second_parent = parents[1] if parents else None

    if cited:
        proof = await asyncio.to_thread(_is_ancestor, repo_root, cited, sha)
        if proof is True:
            return f"{sha[:8]} {branch!r} verified (cites {cited[:8]}, an actual ancestor)"
        if proof is False:
            return (f"{sha[:8]} ⚠ names {branch!r}, cites {cited[:8]}, but it is NOT "
                    "an ancestor")
        # proof is None (the cited sha itself is unresolvable — a truncated/garbled
        # parenthetical, or history since rewritten): fall through to the branch-tip check
        # below rather than reporting 'inconclusive' on a sha that may simply be malformed.
    exists = await asyncio.to_thread(
        subprocess.run, ["git", "rev-parse", "--verify", "--quiet", f"refs/heads/{branch}"],
        cwd=repo_root, capture_output=True, text=True, timeout=5, check=False)
    if exists.returncode != 0:
        if second_parent is not None:
            return (f"{sha[:8]} names {branch!r} (branch no longer exists locally) — the "
                    f"merge itself is structurally real (2 parents, incorporated "
                    f"{second_parent[:8]}); the NAME can't be confirmed without the branch "
                    "or a cited sha, but this is not the fd3a703 shape either — "
                    "unverifiable, not assumed false")
        return (f"{sha[:8]} names {branch!r} but that branch no longer exists locally — "
                "unverifiable, not assumed false")
    old_proof = await asyncio.to_thread(_is_ancestor, repo_root, branch, sha)
    new_proof = (await asyncio.to_thread(_is_ancestor, repo_root, second_parent, branch)
                if second_parent is not None else None)
    if old_proof is True or new_proof is True:
        if old_proof is True:
            basis = "an ancestor of the merge"
        else:
            assert second_parent is not None  # new_proof is only ever True when it was set
            basis = f"a descendant of what this merge actually incorporated ({second_parent[:8]})"
        return f"{sha[:8]} {branch!r} verified — its current tip is {basis}"
    if old_proof is False and new_proof is False:
        assert second_parent is not None  # new_proof is only ever False when it was set
        return (f"{sha[:8]} ⚠ names {branch!r} but its current tip is neither an ancestor "
                f"of the merge nor a descendant of what it actually incorporated "
                f"({second_parent[:8]}) — a genuine mismatch")
    if old_proof is False and new_proof is None:
        return f"{sha[:8]} ⚠ names {branch!r} but it is NOT an ancestor"
    return f"{sha[:8]} names {branch!r} — ancestry check inconclusive, unknown, not assumed"


async def merge_claim_hygiene(repo_root: Path, *, since: str | None = None) -> str:
    """THE UNVERIFIED CLAIM (Sekhmet's specimen, msg 5201, thread #175/#180): commit fd3a703's
    own subject line read "merge sekhmet-launch-resume-fix + ratchet ..." — but
    `sekhmet-launch-resume-fix`'s tip was never actually an ancestor of fd3a703; the merge
    claimed a branch it never contained. This house's merge commits follow one convention —
    the subject starts `merge <branch-name>` (case varies, see below) — so the claim is
    MECHANICALLY CHECKABLE: `git merge-base --is-ancestor <branch> HEAD` is either true or
    it isn't, no prose-reading required.

    RESAMPLED 2026-09-01 (Thoth XC, thread 9b6b5269, decision b86e65ed — DATED, unlike the
    frozen sample this replaces, which read "sampled 20 recent: every single one" and
    stayed load-bearing for weeks after the convention it described had already drifted):
    59 of the 60 most recent merge commits begin `merge`/`Merge ` case-insensitively; the
    one exception is a raw git-generated "Merge commit '<sha>' into <branch>" message, not
    this house's own convention at all. THE CASE ITSELF DRIFTED — lowercase for the older
    majority, capital-M for the 9 most recent at sampling time — which is exactly why
    `_MERGE_SUBJECT_BRANCH` is case-insensitive now: every capital-M merge before this fix
    silently matched nothing, and the no-match branches below used to read that silence as
    "nothing to verify" rather than "could not parse" (retro-verified clean: 8 of those 9
    check out as genuine merges, 1 is the non-convention exception above — zero real
    mis-claims went through the blind window).

    THE LEDGER-WALK GAP (obligation 8752024d, found by 1c85ed3's own deploy, 2026-08-18):
    checking only HEAD's own subject blinds this check whenever the standard "merge, raise
    ratchet, deploy" sequence rides TWO real merges under one final ratchet commit — the
    ratchet commit's own subject never claims a branch at all ("HEAD's subject doesn't name
    a branch, nothing to verify"), so the merge commits underneath it, the ones actually
    carrying a claim, went unchecked. `since` (the PREVIOUSLY deployed ref, read by the
    caller BEFORE `_real_record_deploy` overwrites the cursor to the new HEAD — this
    function never reads the cursor itself, same non-guessing discipline
    `check_diverged_since_last_deploy`'s own docstring already established for
    `repo_root`) turns this from "check the tip" into "check every commit that arrived
    since the fleet last trusted a ref" — the actual blast radius of one `osiris deploy`.

    `since=None` (no prior deploy recorded, or the caller has no better answer) falls back
    to the ORIGINAL HEAD-only check, unchanged — first-deploy-on-a-fresh-cursor stays
    exactly as permissive as before this fix.

    NEVER REFUSES (577988ed, same fail-open discipline as `origin_visibility`/
    `local_ref_hygiene` beside it): a git failure, no commit in range claiming a merge, or
    a named branch no longer existing (deleted after merging, the common case) all degrade
    to a quiet 'nothing to verify' or 'unknown', never a blocked deploy — this is read-only
    corroboration of claims already made, not a gate on making one.

    ONLY THE FIRST NAMED BRANCH per commit is checked: the convention names exactly one
    real branch per merge (a "five-branch wave" commit still only merges ONE branch as its
    actual second git parent — the rest of the subject is prose about a ratchet or a
    batch, not more branches), and Sekhmet's own specimen was a single mis-claim, not a
    list."""
    head = await asyncio.to_thread(
        subprocess.run, ["git", "rev-parse", "HEAD"], cwd=repo_root,
        capture_output=True, text=True, timeout=5, check=False)
    if head.returncode != 0:
        return "merge claim: HEAD unknown, nothing to verify"
    head_sha = head.stdout.strip()

    if since:
        known = await asyncio.to_thread(
            subprocess.run, ["git", "cat-file", "-e", since], cwd=repo_root,
            capture_output=True, text=True, timeout=5, check=False)
        if known.returncode != 0:
            since = None  # a ref this checkout has never heard of — degrade to HEAD-only,
            # never guess a range against a sha that isn't here (a fresh clone, a rewritten
            # history, or simply the first deploy this checkout has ever recorded)

    if since and since != head_sha:
        log = await asyncio.to_thread(
            subprocess.run, ["git", "log", f"{since}..{head_sha}", "--format=%H%x1f%s"],
            cwd=repo_root, capture_output=True, text=True, timeout=15, check=False)
        if log.returncode != 0:
            return (f"merge claim: could not walk {since[:8]}..{head_sha[:8]} "
                    f"({log.stderr.strip()[:150]!r}) — unknown, not assumed clean")
        commits = [line.split("\x1f", 1) for line in log.stdout.splitlines() if line]
        claims = [(sha, m.group(1), m.group(2)) for sha, subj in commits
                 if (m := _MERGE_SUBJECT_BRANCH.match(subj))]
        if not claims:
            return (f"merge claim: {len(commits)} commit(s) since last deploy "
                    f"({since[:8]}..{head_sha[:8]}) — none matched the expected `merge "
                    "<branch>` subject shape (CONFESSION, not a clean bill: this could be "
                    "genuinely merge-free history, or a convention this parser can no "
                    "longer recognize — the exact gap the case-sensitivity drift left open "
                    "before 2026-09-01)")
        results = [await _verify_one_merge_claim(repo_root, sha, branch, cited)
                  for sha, branch, cited in claims]
        failed = [r for r in results if "⚠" in r]
        if failed:
            return (f"merge claim: {len(claims)} merge claim(s) since last deploy "
                    f"({since[:8]}..{head_sha[:8]}) — ⚠ {len(failed)} FAILED: "
                    + "; ".join(failed))
        return (f"merge claim: {len(claims)} merge claim(s) since last deploy "
                f"({since[:8]}..{head_sha[:8]}) — " + "; ".join(results))

    # No usable `since` (no prior deploy recorded, or the caller has no better answer):
    # the original HEAD-only check — now sharing `_verify_one_merge_claim` with the ranged
    # walk above (unified 2026-09-01) rather than carrying its own inline copy of the same
    # ancestry/structural logic.
    subject = (await asyncio.to_thread(
        subprocess.run, ["git", "log", "-1", "--format=%s"], cwd=repo_root,
        capture_output=True, text=True, timeout=5, check=False)).stdout.strip()
    match = _MERGE_SUBJECT_BRANCH.match(subject)
    if not match:
        return (f"merge claim: HEAD's subject ({subject[:80]!r}) did not match the expected "
                "`merge <branch>` shape (CONFESSION, not a clean bill: this could be a "
                "genuinely non-merge HEAD, or a convention this parser can no longer "
                "recognize) — nothing checked")
    branch, cited = match.group(1), match.group(2)
    result = await _verify_one_merge_claim(repo_root, head_sha, branch, cited)
    # every _verify_one_merge_claim return starts "{sha[:8]} " (8 hex chars, one space) —
    # HEAD's own report names it plainly instead, its own warning symbol (if any) intact.
    return "merge claim: HEAD " + result[9:]


# ═══ THE LANDING AUDITOR (Thoth's dispatch msg 5339, thread 5256/5313) ═══
#
# Three times a lane was declared "accepted into the merge batch" in prose (a DM, a Decision
# summary) and never actually reached main — fd3a703/sekhmet-launch-resume-fix, seshat-
# roster-review, seshat-migration-stress-harness — found, each time, only by a HAND SWEEP
# (`git branch --no-merged main`), never by anything that runs on its own. `merge_claim_
# hygiene` above already answers "does a REAL merge commit's own subject tell the truth";
# this answers the complementary question a prose claim can dodge entirely: "is the branch
# actually IN main, full stop" — and separately, "does anything in the GRAPH'S OWN TEXT
# (a Decision, a Task) claim a branch landed that git disagrees with."

_MERGE_CLAIM_IN_TEXT = re.compile(_MERGE_SUBJECT_BRANCH.pattern.removeprefix("^"))
# THE SAME PARSER, UNANCHORED (Thoth: "reuse Khnum's cited-sha parser — one parser, not
# two"): `_MERGE_SUBJECT_BRANCH` is anchored to a commit SUBJECT's own start; graph prose
# carries the identical "merge <branch>(<sha>)" shape anywhere mid-paragraph (a decision
# quoting a commit's own message), so this strips the `^` and lets `.finditer` find every
# occurrence rather than only a string-initial one. Same two capture groups, same meaning.


async def stale_unmerged_branches(
    repo_root: Path, *, claimed: set[str], min_age_hours: float = 48.0,
) -> list[dict[str, Any]]:
    """`git branch --no-merged main`, minus anything already named by an OPEN held-work
    Thread (`claimed` — the caller's own `capture.open_held_work()` branch set, this
    function never queries the graph itself) and anything younger than `min_age_hours` — a
    fresh branch mid-build is not yet a specimen of anything. THIS is the exact sweep that
    actually rediscovered all three of Thoth's named specimens (by hand); nothing above
    catches a branch that never even attempted a merge-shaped commit.

    NEVER REFUSES (577988ed): a git failure, an unparseable date, or a non-repo root all
    degrade to an empty list — a courtesy sweep, not a gate on anything."""
    out = await asyncio.to_thread(
        subprocess.run,
        ["git", "branch", "--no-merged", "main",
         "--format=%(refname:short)%09%(committerdate:iso-strict)"],
        cwd=repo_root, capture_output=True, text=True, timeout=10, check=False)
    if out.returncode != 0:
        return []
    now = datetime.now(UTC)
    stale: list[dict[str, Any]] = []
    for line in out.stdout.splitlines():
        if "\t" not in line:
            continue
        branch, _, iso = line.partition("\t")
        branch = branch.strip()
        if not branch or branch == "main" or branch in claimed:
            continue
        try:
            tip_at = datetime.fromisoformat(iso.strip())
        except ValueError:
            continue
        age_hours = (now - tip_at).total_seconds() / 3600.0
        if age_hours >= min_age_hours:
            stale.append({"branch": branch, "age_hours": round(age_hours, 1),
                         "committed_at": iso.strip()})
    return stale


async def _verify_graph_claim(
    repo_root: Path, main_sha: str, branch: str, cited: str | None,
) -> str | None:
    """One graph-text mention, checked against `main`'s current tip (not an enclosing
    commit — prose has none): None for anything unverifiable (branch gone, sha unresolvable,
    a spurious non-branch match like "merge batch") so a caller only ever sees a PROVEN
    mismatch, never noise from this regex's own false-positive surface. Cited sha preferred
    over the branch's own tip, same reasoning as `_verify_one_merge_claim`'s docstring."""
    if cited:
        proof = await asyncio.to_thread(_is_ancestor, repo_root, cited, main_sha)
        if proof is False:
            return f"names {branch!r}, cites {cited[:8]}, but it is NOT an ancestor of main"
        if proof is True:
            return None  # verified — nothing to flag
    exists = await asyncio.to_thread(
        subprocess.run, ["git", "rev-parse", "--verify", "--quiet", f"refs/heads/{branch}"],
        cwd=repo_root, capture_output=True, text=True, timeout=5, check=False)
    if exists.returncode != 0:
        return None  # not a real local branch (or long since deleted) — unverifiable, not false
    proof = await asyncio.to_thread(_is_ancestor, repo_root, branch, main_sha)
    if proof is not False:
        return None  # True (verified) or None (inconclusive) — neither is a proven mismatch
    return f"names {branch!r} but it is NOT an ancestor of main"


async def audit_graph_merge_claims(
    pool: asyncpg.Pool, repo_root: Path,
) -> list[dict[str, Any]]:
    """Every Decision/Thread whose own summary or rationale contains a "merge <branch>
    (<sha>)"-shaped mention, checked against `main`'s CURRENT tip. Returns only PROVEN
    mismatches (`_verify_graph_claim` swallows everything unverifiable) — each a dict with
    `canonical` (the graph object naming the claim) and `note`. NEVER REFUSES: a git or DB
    failure degrades to an empty list."""
    head = await asyncio.to_thread(
        subprocess.run, ["git", "rev-parse", "main"], cwd=repo_root,
        capture_output=True, text=True, timeout=5, check=False)
    if head.returncode != 0:
        return []
    main_sha = head.stdout.strip()
    try:
        rows = await pool.fetch(
            "SELECT o.canonical, a.value #>> '{}' AS text FROM current_assertions a "
            "JOIN objects o ON o.id=a.object_id "
            "WHERE o.type IN ('Decision','Thread') AND a.name IN ('summary','rationale') "
            "AND a.value #>> '{}' ILIKE '%merge %'")
    except Exception:  # noqa: BLE001 — a DB hiccup is unknown, never a crash
        return []
    findings: list[dict[str, Any]] = []
    seen: set[str] = set()
    for r in rows:
        text = r["text"] or ""
        for m in _MERGE_CLAIM_IN_TEXT.finditer(text):
            # PROSE, NOT A COMMIT SUBJECT: a trailing comma/period right after the branch
            # name is common in a sentence ("...merge foo, ready for the batch") and was
            # never a concern for the anchored, commit-subject-only original pattern.
            branch = m.group(1).rstrip(",.;:")
            cited = m.group(2)
            key = f"{r['canonical']}:{branch}:{cited or ''}"
            if key in seen:
                continue
            seen.add(key)
            note = await _verify_graph_claim(repo_root, main_sha, branch, cited)
            if note is not None:
                findings.append({"canonical": r["canonical"], "note": note})
    return findings


async def landing_audit(actions: Actions, repo_root: Path) -> dict[str, Any]:
    """THE LANDING AUDITOR, composed: `stale_unmerged_branches` (extends the held-work
    surface, `capture.open_held_work`, rather than a parallel one — its branch list is the
    'already claimed, not yet a specimen' exemption) plus `audit_graph_merge_claims`,
    minting one typed obligation (`owner='thoth'`, the coordinator) per genuine finding.
    `open_thread` is idempotent on the summary's own text (same primitive
    `alarm_schema_drift` uses beside it), so a repeated run never re-pages the same
    specimen twice — this is safe to call on every deploy, unaided.

    NEVER REFUSES: every sub-check already fails open to an empty result on its own; a
    thread-mint failure here is swallowed the same way `alarm_schema_drift`'s desk-brief
    is, rather than compounding one alarm into a second, louder failure."""
    from src.orchestrator.capture import open_held_work, open_thread

    held = await open_held_work(actions.pool)
    claimed = {h["branch"] for h in held if h.get("branch")}
    stale = await stale_unmerged_branches(repo_root, claimed=claimed)
    claims = await audit_graph_merge_claims(actions.pool, repo_root)
    minted: list[str] = []
    for s in stale:
        summary = (f"LANDING AUDIT: branch {s['branch']!r} has sat unmerged into main for "
                  f"~{s['age_hours']:.0f}h with no open held-work claim naming it")
        with contextlib.suppress(Exception):
            # NEVER pass branch= here: open_held_work() treats ANY open Thread naming a
            # `branch` as a legitimate claim on it — this obligation's own existence would
            # exempt the very branch it is flagging from ever being re-swept, a self-
            # referential blind spot found live by this function's own idempotency test.
            minted.append(str(await open_thread(
                actions, summary, kind="obligation", owner="thoth",
                source="landing-auditor")))
    for c in claims:
        summary = f"LANDING AUDIT: {c['canonical']} {c['note']}"
        with contextlib.suppress(Exception):
            minted.append(str(await open_thread(
                actions, summary, kind="obligation", owner="thoth",
                source="landing-auditor")))
    return {"stale_unmerged_branches": stale, "graph_claim_mismatches": claims,
            "obligations": minted}

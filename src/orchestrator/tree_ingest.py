"""Self-healing tree ingest (thread 5126, operator ruling df646654/fe8ec7ff — chronohorn's
own wave: "#41 chronohorn ingest has to be a SELF-HEALING infra thing agents can do by
themselves, persistent and tooled — not an osiris-applied bandaid"). The census this needs
already existed, tested, with zero callers (neighborhoods.discover_trees) — the missing piece
was never the read, it was a DOOR any seat can act through, and a heartbeat that notices
without acting unattended.

`ingest_project` is that door: any seat's own self-service verb for its OWN project (mirrors
reconcile_seat_identity's authority shape exactly — self-service derives its target from the
caller's own mount, the third-party sibling takes an explicit target plus a required reason),
combining `gitlog.ingest_repo` (idempotent, always writes when not dry_run) with
`closure.close_by_commits` (dry_run-native already) in one act, so landing a tree's history
and closing the threads it witnesses is one decision, not two half-remembered ones.

`uningested_trees_alarm_tick` is the heartbeat leg: it never ingests anything itself — it
tracks the owning Seat's own OBLIGATION THREAD for a blind tree, and leaves the act to them.
Cold by default (osiris_tree_ingest_alarm_enabled), same law as every other scheduled writer
in this house.

THE MAIL-TO-THREAD SHAPE FIX (thread 358ac1ae, operator complaint routed via Atlas msg 6404
— Thoth's own reframe: "a condition that is STILL TRUE is a thread, not mail"): this used to
`send_message(..., grade='ask')` on every cooldown-cleared tick, so a tree that stayed blind
for 14 days sent 14 byte-identical graded asks — grade inflation (mount()/orient() reported
"14 ask" when it meant one), fleet-wide unread-count degradation, and no self-clearing (the
first alarm sat unsettled even after the tree was ingested on day two). Now: the FIRST time
a tree is seen blind, `_open_or_annotate_persisting_alarm` (deploy_guard.py's own shared
mint-or-annotate primitive, reused rather than re-derived — the tree-ingest alarm shares its
exact shape with schema-drift/unreviewed-boot: a periodic, source-not-a-human caller re-
running on the SAME condition) mints the Thread AND fires the one-and-only graded 'ask' DM;
every later tick that still finds the SAME tree blind (past its own unchanged 24h cooldown,
task text: "DO NOT 'FIX' THE CADENCE — the 24h per-tree cooldown is BY DESIGN and working")
only ANNOTATES the existing thread — no mail, ever, on a re-assertion. The moment a
previously-alarmed tree stops being actionable (its commits land, by whatever hand), this
tick resolves that tree's own still-open thread itself — the self-clearing defect (3) this
thread named. Defect (1), no dedupe on condition, was always the symptom, not a defect of
its own — the thread's own idempotent identity IS the dedupe now."""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.config.settings import Settings, get_settings
from src.ingest.closure import close_by_commits
from src.ingest.gitlog import ingest_repo, read_commits
from src.orchestrator.capture import _thread_canon, resolve_thread
from src.orchestrator.deploy_guard import _open_or_annotate_persisting_alarm
from src.orchestrator.mailbox import send_message
from src.orchestrator.monitor import get_cursor, set_cursor
from src.orchestrator.neighborhoods import discover_trees

_ALARM_PREFIX = "tree-ingest-alarm"
_ALARM_COOLDOWN_SECS = 86400  # one alarm per tree per day — a heartbeat firing every 15
                               # minutes must not re-page the same seat 96 times a day


def _alarm_summary(tree: str) -> str:
    """The Thread's own canonical identity text — DELIBERATELY volatile-detail-free (no
    path, no watermark), same law `_open_or_annotate_persisting_alarm`'s own docstring
    states for schema-drift/unreviewed-boot: baking a detail that can change per-tick into
    the summary would mint a fresh Thread each time instead of converging on one."""
    return f"[tree-ingest-alarm] {tree} has zero commits ingested"


def _canonical(project: str) -> str:
    return project if project.startswith("repo:") else f"repo:{project}"


def _usable_dir(path: str | None) -> bool:
    """A plain sync wrapper so the async caller never calls a blocking Path method inline
    (ASYNC240, this codebase's own ruff gate) — mirrors neighborhoods._path_gone."""
    if not path:
        return False
    return Path(path).is_dir()


async def _existing_shas(pool: Any, project_id: Any) -> set[str]:
    rows = await pool.fetch(
        "SELECT replace(c.canonical, 'commit:', '') AS short FROM objects c "
        "JOIN links l ON l.from_id=c.id AND l.type='in_repo' AND l.to_id=$1 "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "WHERE c.type='Commit' AND c.status='active'", project_id)
    return {r["short"] for r in rows}


async def ingest_project(
    actions: Actions, *, project: str, dry_run: bool = True, actor: str,
) -> dict[str, Any]:
    """THE SELF-SERVICE VERB: land one tree's git history and close the threads it witnesses,
    one call. `project` is a bare repo name (or already-prefixed `repo:...`) — must resolve
    to an ACTIVE SoftwareProject with a usable `on_disk_path` (census_trees's own write, or
    a future explicit registration); refuses loudly otherwise, never guessing a path from a
    name.

    `dry_run=True` (the default, DELIBERATELY, matching close_by_commits' own default) writes
    NOTHING: `ingest_repo` itself has no dry-run mode (it is idempotent-safe to re-run, but
    idempotent-safe is not the same as read-only), so the preview instead diffs the on-disk
    git log against the commits already linked `in_repo` in the graph and reports how many
    are new, plus `close_by_commits(dry_run=True)`'s own preview of what closure over the
    ALREADY-graphed commits would do (a tree with zero commits ingested so far reports zero
    closure candidates too — closure only ever sees what has already landed; the closure
    that would follow the NEW commits about to be ingested is not itself previewable without
    ingesting them, and this receipt says so rather than implying otherwise).

    `dry_run=False` actually lands it: `ingest_repo` then `close_by_commits(dry_run=False)`,
    same repo, one receipt naming both."""
    canonical = _canonical(project)
    row = await actions.pool.fetchrow(
        "SELECT id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=objects.id "
        "   AND a.name='on_disk_path' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS on_disk_path "
        "FROM objects WHERE canonical=$1 AND type='SoftwareProject' AND status='active'",
        canonical)
    if row is None:
        return {"error": f"no active SoftwareProject {canonical!r}"}
    on_disk_path = row["on_disk_path"]
    if not _usable_dir(on_disk_path):
        return {"error": f"no usable on_disk_path for {canonical!r} — census_trees "
                          f"hasn't registered one, or it no longer exists on disk",
                "on_disk_path": on_disk_path}
    name = canonical.removeprefix("repo:")

    if dry_run:
        on_disk = read_commits(on_disk_path)
        already = await _existing_shas(actions.pool, row["id"])
        new_commits = [c for c in on_disk if c.sha[:12] not in already]
        closure_preview = await close_by_commits(actions, repo=name, dry_run=True)
        return {
            "project": canonical, "dry_run": True, "on_disk_path": on_disk_path,
            "commits_on_disk": len(on_disk), "commits_already_graphed": len(already),
            "commits_would_ingest": len(new_commits),
            "sample": [c.subject[:80] for c in new_commits[:5]],
            "closure_preview_note": "closure over commits ALREADY in the graph only — the "
                "closure the new commits above would trigger is not previewable without "
                "landing them first",
            "closure_preview": closure_preview,
        }

    ingested = await ingest_repo(actions, path=on_disk_path, source_id=f"ingest_project:{actor}")
    closure = await close_by_commits(actions, repo=name, dry_run=False)
    return {"project": canonical, "dry_run": False, "on_disk_path": on_disk_path,
            "ingest": ingested, "closure": closure}


async def ingest_project_third_party(
    actions: Actions, *, project: str, because: str, dry_run: bool = True, actor: str,
) -> dict[str, Any]:
    """THE THIRD-PARTY SIBLING of ingest_project — same authority shape as reconcile_seat_
    identity_third_party: NOT self-scoped (a coordinator landing a tree on behalf of a seat
    that hasn't gotten to it yet, exactly the case this exists for), `because` REQUIRED (a
    third-party act with no stated reason is the silent-overwrite class this house already
    ruled against), does NOT check caller authority beyond being mounted — callers are
    responsible for the authorization this docstring cannot enforce. OTHERWISE IDENTICAL to
    the self-service verb, same receipt shape."""
    because = (because or "").strip()
    if not because:
        return {"error": "a third-party ingest with no stated reason is exactly the silent "
                         "overwrite this house rules against — refusing"}
    result = await ingest_project(actions, project=project, dry_run=dry_run, actor=actor)
    result["because"] = because
    return result


async def _owning_seat(pool: Any, canonical: str) -> str | None:
    """The Seat that `governs` `canonical`, if exactly one does — the reverse of charter_of."""
    rows = await pool.fetch(
        "SELECT s.canonical FROM links l "
        "JOIN objects s ON s.id=l.from_id AND s.type='Seat' "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' AND p.canonical=$1 "
        "WHERE l.type='governs' AND (l.valid_until IS NULL OR l.valid_until > now())",
        canonical)
    seats = {r["canonical"] for r in rows}
    return next(iter(seats)) if len(seats) == 1 else None


async def uningested_trees_alarm_tick(
    actions: Actions, *, settings: Settings | None = None,
) -> dict[str, Any]:
    """THE HEARTBEAT LEG: gated by osiris_tree_ingest_alarm_enabled (the flag lives here,
    never in the cron wrapper, so a test can exercise this directly). Runs discover_trees
    fleet-wide; for every row that is genuinely actionable (commits==0 AND an on_disk_path
    IS registered — a tree the graph knows nothing to ingest FROM is not this alarm's
    business) with exactly one governing Seat, tracks that Seat's own obligation Thread for
    the blind tree UNLESS one already fired for this tree within the last 24h (per-tree
    watermark, same primitive close_by_commits' own cursor uses, and the SAME cooldown this
    function always held — thread 358ac1ae is explicit that the cadence itself was never
    the bug). Never ingests anything itself.

    THE SHAPE (thread 358ac1ae): the FIRST time a tree's own Thread is minted, one graded
    'ask' DM fires alongside it — a human/mind should be told a new duty exists. Every later
    tick past cooldown that still finds the SAME tree blind only ANNOTATES that Thread
    (`_open_or_annotate_persisting_alarm`) — no mail. A tree that stops being actionable
    (commits now land) resolves its own still-open Thread, if one exists — self-clearing,
    the defect this thread's own dispatch named as (3).

    Returns what it found and what it did, including trees it could NOT alarm (no governing
    seat, or more than one) and trees it self-resolved — reported, never silently dropped."""
    st = settings or get_settings()
    if not st.osiris_tree_ingest_alarm_enabled:
        return {"enabled": False, "alarmed": []}
    watched = [w.strip() for w in st.osiris_dev_repos.split(",") if w.strip()]
    rows = await discover_trees(actions.pool, watched=watched)
    actionable = [r for r in rows if r["commits"] == 0 and r["path"]]
    actionable_trees = {r["tree"] for r in actionable}
    alarmed: list[dict[str, Any]] = []
    unowned: list[str] = []
    cooling: list[str] = []
    resolved: list[str] = []
    for r in actionable:
        canonical = f"repo:{r['tree']}"
        cursor_key = f"{_ALARM_PREFIX}:{r['tree']}"
        last = await get_cursor(actions.pool, cursor_key)
        if last is not None:
            age = (datetime.now(UTC) - datetime.fromisoformat(last)).total_seconds()
            if age < _ALARM_COOLDOWN_SECS:
                cooling.append(r["tree"])
                continue
        seat = await _owning_seat(actions.pool, canonical)
        if seat is None:
            unowned.append(r["tree"])
            continue
        summary = _alarm_summary(r["tree"])
        first_notice = not await actions.pool.fetchval(
            "SELECT 1 FROM objects WHERE canonical=$1 AND type='Thread'",
            _thread_canon(summary, None))
        tid = await _open_or_annotate_persisting_alarm(
            actions, summary, kind="obligation", owner=seat, arc="Fleet-Hygiene",
            severity="alarm", source="cron:tree_ingest_alarm")
        if first_notice:
            body = (f"[tree-ingest-alarm] {r['tree']} is on disk at {r['path']} with zero "
                    f"commits ingested ({r['reason']}). Self-heal it: mount there and call "
                    f"ingest_project(dry_run=True) for a receipt, then dry_run=False when it "
                    f"looks right — no sign-off needed, this is your own tree. Tracked from "
                    f"here as an open obligation thread; further re-checks annotate it, "
                    f"they don't re-mail you.")
            await send_message(actions.pool, from_agent="tree-ingest-alarm", from_project=None,
                               to_agent=seat, body=body, grade="ask")
        await set_cursor(actions.pool, cursor_key, datetime.now(UTC).isoformat())
        alarmed.append({"tree": r["tree"], "seat": seat, "thread": str(tid),
                        "first_notice": first_notice})

    # SELF-CLEARING (defect 3, thread 358ac1ae): a tree that no longer reads zero-commits
    # (whatever hand ingested it) resolves its own still-open alarm Thread here, rather than
    # leaving the first notice unsettled forever once the underlying condition has cleared.
    for r in rows:
        if r["tree"] in actionable_trees:
            continue
        canon = _thread_canon(_alarm_summary(r["tree"]), None)
        status = await actions.pool.fetchval(
            "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
            "ON a.object_id=o.id WHERE o.canonical=$1 AND o.type='Thread' AND a.name='status' "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", canon)
        if status == "open":
            await resolve_thread(
                actions, canon,
                because=f"{r['tree']} now has commits ingested — the zero-ingest "
                        "condition that opened this alarm has cleared",
                source="cron:tree_ingest_alarm")
            resolved.append(r["tree"])

    return {"enabled": True, "checked": len(rows), "actionable": len(actionable),
            "alarmed": alarmed, "unowned": unowned, "cooling": cooling, "resolved": resolved}

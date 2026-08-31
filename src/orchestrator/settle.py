"""THE OFFLOAD RITUAL'S BOXES, promoted (ruling c5b184cd, the /settle primitive; queue item
4 / #49 piece 3 originally built these inside scripts/osiris_stophook.py). Two callers now
need the SAME check — the Stop hook (deciding whether to refuse a quiet stop) and the new
/settle MCP tool (surfacing what's still unwritten, then confirming after the agent's own
dump) — and "two copies drifting" is precisely the bug class this house keeps finding
(manager_of_seat's own promotion out of a hook-local copy is the direct precedent). Kept
deliberately thin: this module answers "what's checked and what's still missing", never
"should a stop be refused" — that policy (occupancy %, once-then-allow) stays the hook's own,
since it is about WHEN to enforce, not WHAT to check.

`conn_or_pool` accepts either an asyncpg.Pool (the MCP server) or a raw asyncpg.Connection
(the hook script, which cannot hold a pool across its ~1s budget) — both implement the same
fetch/fetchval surface, so one implementation genuinely serves both callers.

`closure_edge_coverage` (Phase 1b, decision cb38d922) joins `filed_under_check` as a
second REPORT-ONLY confirm-time check: not a box the Stop hook shares, only the /settle
MCP tool's own confirm step — same reasoning as `uncommitted_git_work` for why it isn't
shared (a query against the live graph has no place racing the hook's ~1s budget). It reads
edge coverage through `thread_closure_status` (Phase 2, Seshat, decision cb38d922) rather
than re-deriving which edge types count as a closure trace — the exact "two copies
drifting" bug class this module's own opening paragraph names: Phase 2's `strength` tiers
are a live UNION ALL away from widening (Khnum's Phase 1a `closed_by` fallback is not
wired into the view yet), and a hand-rolled duplicate here would silently fall behind the
moment that lands.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Protocol

import asyncpg


class _Fetchable(Protocol):
    async def fetchval(self, query: str, *args: Any) -> Any: ...
    async def fetch(self, query: str, *args: Any) -> Any: ...


async def settle_boxes(
    conn_or_pool: _Fetchable, *, agent_id: str, mounted_at: datetime, cwd: str | None,
    seat_id: str | None = None,
) -> dict[str, bool | None]:
    """The boxes this session's own life should have checked, best-effort per box — a box
    this query could not evaluate is None and never counts as missing (fail open per-box,
    the same law the whole ritual runs on).

    Decisions/threads are 'this session's' when their defining assertion's source_id is this
    EXACT agent_id (the identity that mounted this job_dir) at or after `mounted_at` — the
    session's own first-mount stamp, set once at INSERT, never touched by a re-attach.

    `seat_id` (Thoth's ruling 205668ec) feeds the CHARTERED box below — the caller already
    resolves `held_seat` for the standing-orders cwd fix (mcp_server.py/osiris_stophook.py
    both do), so this is a pass-through, not a second lookup."""
    boxes: dict[str, bool | None] = {}
    try:
        boxes["decisions recorded this session"] = bool(await conn_or_pool.fetchval(
            "SELECT 1 FROM assertions a JOIN objects o ON o.id = a.object_id "
            "WHERE o.type = 'Decision' AND a.name = 'summary' AND a.source_id = $1 "
            "AND a.observed_at >= $2 LIMIT 1", agent_id, mounted_at))
    except Exception:  # noqa: BLE001 — one box's failure never dooms the others
        boxes["decisions recorded this session"] = None
    try:
        boxes["threads trued this session (opened or resolved)"] = bool(await conn_or_pool.fetchval(
            "SELECT 1 FROM assertions a JOIN objects o ON o.id = a.object_id "
            "WHERE o.type = 'Thread' AND a.name IN ('summary', 'status') "
            "AND a.source_id = $1 AND a.observed_at >= $2 LIMIT 1", agent_id, mounted_at))
    except Exception:  # noqa: BLE001
        boxes["threads trued this session (opened or resolved)"] = None
    boxes["standing orders touched this session"] = standing_orders_touched(cwd, mounted_at)
    # RULING 205668ec: two things were both called "charter" — the graph DECLARATION
    # (governs edges, charter_of/set_charter, the thing with authority) and the on-disk
    # FILE (prose, still literally charter.md, the Boot Compiler's own "standing orders"
    # output). The box above asks the file question, honestly named now. This one asks
    # the declaration question — SEPARATE and INDEPENDENT, never folded together again.
    boxes["seat is chartered (governs a repo)"] = await seat_chartered(conn_or_pool, seat_id)
    # an obligation opened this session — ONLY asked of a session whose own agent object was
    # itself born by a mint (minted_because stamped at birth, permanent on that exact
    # generation): a fresh heir owes its OWN heir at least one obligation left behind, not
    # just mail settled. No content-classifier — the primitive is 'opened an obligation',
    # not 'looks like a handoff'.
    #
    # THE KEY NAMES THE PREDICATE, NOT THE INTENTION (Thoth's ruling, DM 4099; renamed from
    # "a live succession/handoff note (this lineage was minted)"). This is the SAME ruling
    # 205668ec applies to the two boxes above — two things called one name — reached
    # independently on a different box in the same batch. The old name described what the
    # box is FOR while the query below tests something narrower and exact: a
    # Thread, of kind 'obligation', authored by THIS agent, since mounted_at. Four agents
    # across two lineages independently inferred a rule from the old name and encoded the
    # WRONG MECHANISM as standing practice in their office files — "satisfy it with a thread
    # carrying is_handoff:true" (practice 1637763e, confirmed 4x; decision c605fa74 is
    # another lineage's independent confirmation of the same wrong attribute). `is_handoff`
    # appears nowhere in this query. A thread opened with kind='handoff' AND is_handoff:true
    # does NOT clear this box; only kind='obligation' does. The code was never ambiguous —
    # nobody read it, because the receipt's own name answered the question first and wrongly.
    # Ruling 60bc15db, one layer above code: a name that describes an intention while the
    # predicate tests something else teaches every reader a false mechanism.
    #
    # Whether settle SHOULD ALSO check for a handoff marker is a SEPARATE question and gets
    # its own thread, never a widened query here (same ruling shape as charter/standing-
    # orders, 205668ec — two questions, two names).
    try:
        minted: bool | None = bool(await conn_or_pool.fetchval(
            "SELECT 1 FROM current_assertions a JOIN objects o ON o.id = a.object_id "
            "WHERE o.canonical = $1 AND a.name = 'minted_because' LIMIT 1", agent_id))
    except Exception:  # noqa: BLE001 — could not determine, NEVER the same as a
        # confirmed "not minted": the gate below must not silently drop the box the way a
        # bare `minted = False` would (60bc15db specimen 1 — the outer gate collapsing
        # "don't know" into "not applicable" one level above where the inner try already
        # gets this right).
        minted = None
    if minted:
        try:
            boxes["an obligation opened this session (this lineage was minted)"] = bool(
                await conn_or_pool.fetchval(
                    "SELECT 1 FROM assertions a JOIN objects o ON o.id = a.object_id "
                    "WHERE o.type = 'Thread' AND a.name = 'kind' "
                    "AND a.value #>> '{}' = 'obligation' AND a.source_id = $1 "
                    "AND a.observed_at >= $2 LIMIT 1", agent_id, mounted_at))
        except Exception:  # noqa: BLE001
            boxes["an obligation opened this session (this lineage was minted)"] = None
    elif minted is None:
        # fog-of-war on whether the box even applies is itself fog-of-war on the box —
        # surfaced via unevaluated_boxes, never silently omitted like a genuine non-mint.
        boxes["an obligation opened this session (this lineage was minted)"] = None
    return boxes


async def filed_under_check(
    conn_or_pool: _Fetchable, *, agent_id: str, mounted_at: datetime, project: str | None,
    seat_id: str | None = None,
) -> dict[str, Any] | None:
    """SETTLE VERIFIES WHAT YOU WROTE, NEVER WHO CAN READ IT (Thoth's Lane 4 finding,
    2026-07-31): John XVI settled complete:true — every box green — while his own writes
    filed under a DIFFERENT project than the one his successor's orient() scopes to. Every
    box above asks "did this session write?"; none of them ask "did it write WHERE its own
    successor will look?" This closes that gap: does the project this session is FILED
    UNDER (`project` — the mounted identity's own project, what a fresh heir inherits and
    what orient() scopes a SCOPED briefing to) agree with the project(s) this session's own
    Decision/Thread writes actually landed `in_repo`?

    REPORT-ONLY, NEVER A GATE (ruling 577988ed's closing lesson, reasserted by Thoth
    tonight: "a fleet-wide single-point-of-failure must never refuse-to-serve on a check
    that can false-positive — surface loudly instead"): this function has no notion of
    'missing' or 'complete' and is never folded into `missing_boxes`. A caller surfaces its
    `coherent` field informationally; it must never gate a settle, however wrong it looks —
    an agent at 95% context refused a settle by a check that itself might be wrong is a
    strictly worse outcome than the incoherence it would have caught.

    None (fails open) when `project` is unknown or this session wrote nothing this session
    (no signal either way — silence is not evidence of a mismatch). `writes_went_to` is
    every DISTINCT project canonical (bare name) a Decision or Thread this session authored
    is `in_repo` of; a write with NO `in_repo` link at all (an unfiled thread, Alfred V's
    own shape) is invisible to this specific check by construction — it names WHERE writes
    went, not whether every write went somewhere.

    THE CHARTER, CONSULTED BEFORE CALLING A DECLARED ARRANGEMENT INCOHERENT (thread
    992c0121, Soundwave XVI's own specimen, msg 6090/6091: a seat chartered for TWO repos
    — decepticons + chronohorn, decision b4ccbe03 — necessarily writes to both from a
    single experiment, and used to read `coherent:false` about its own normal operating
    mode every time). When `seat_id` is given and `went_to` spans more than one project,
    the seat's own ACTIVE `governs` set is checked FIRST — inlined here rather than
    imported from charter.py's own `charter_of` (typed against `asyncpg.Pool` alone; this
    module keeps serving both a Pool and a bare Connection, same reason `seat_chartered`
    below inlines the identical query shape). `went_to` fully contained in that charter
    reads `coherent: true`, `charter_repos` names the set it was checked against — a
    DECLARED arrangement, not drift. Exceeding the charter (or no charter at all) keeps
    the exact prior behavior: `coherent: false`.

    THE SUCCESSOR-BLINDNESS DISCLOSURE SURVIVES EITHER VERDICT, ON PURPOSE — Soundwave's
    own insistence, not a suppression request: a successor mounting under `filed_under`
    genuinely will not see writes filed under the OTHER project(s) in `went_to`, whether
    or not the charter declares the split intentional. `spans_multiple` is true whenever
    `len(went_to) > 1`, independent of `coherent` — this function's caller decides how to
    word the disclosure for each case, but must never gate it on `coherent` alone (that
    was the exact bug: a charter-aware `coherent:true` must not silently swallow a true,
    useful warning). Ruling 0222fd37 (unintended dual-project DRIFT, surfaced not
    resolved) is UNTOUCHED — only the CHARTERED case changes verdict; an uncharted spread
    still reads `coherent: false` exactly as before.

    NORMALIZES `project` THROUGH merged_into (decision 6b4d185e, thread aa6b52af — open
    since before tonight, naming this exact gap): `went_to` reads off LIVE `in_repo`
    edges, which `_move_project_estate` already re-points to a fold's survivor, but
    `project` (the mounted identity's own filed-under label) may still name a label that
    has since been FOLDED into another — comparing raw strings then false-fires
    'incoherent' forever after the fold, even though both sides mean the same project.
    Degrades to the raw label on any failure (577988ed — a diagnostic refinement must
    never be the reason this check goes blind; still report-only regardless).

    COMPOUNDING GAP FOUND WHILE VERIFYING THE ABOVE, FIXED IN THE SAME PASS (caught by
    actually running a fold against this function, not by reading it): the `in_repo` join
    below carried NO `valid_until` filter, so a folded project's INVALIDATED pre-fold edge
    still counted alongside its live re-pointed replacement — `went_to` reported BOTH the
    old and new label, which no amount of label normalization alone can reconcile
    (`_write_attribution`'s own healed-edge fix, a while back, is the exact same shape).
    Without this, the normalization fix above is only half-effective."""
    if not project:
        return None
    try:
        from src.orchestrator.project_identity import _normalize_project_label_through_merge
        project, _confession = await _normalize_project_label_through_merge(
            conn_or_pool, project)
    except Exception:  # noqa: BLE001 — see note above
        pass
    try:
        rows = await conn_or_pool.fetch(
            "SELECT DISTINCT p.canonical AS project FROM objects o "
            "JOIN links l ON l.from_id = o.id AND l.type = 'in_repo' "
            "AND (l.valid_until IS NULL OR l.valid_until > now()) "
            "JOIN objects p ON p.id = l.to_id AND p.type = 'SoftwareProject' "
            "WHERE o.type IN ('Decision', 'Thread') AND EXISTS ("
            "  SELECT 1 FROM assertions a WHERE a.object_id = o.id "
            "  AND a.source_id = $1 AND a.observed_at >= $2)",
            agent_id, mounted_at)
    except Exception:  # noqa: BLE001 — fail open, same law as every box above
        return None
    went_to = sorted({str(r["project"]).removeprefix("repo:") for r in rows})
    if not went_to:
        return None
    mismatched = [p for p in went_to if p != project]
    result: dict[str, Any] = {
        "filed_under": project, "writes_went_to": went_to, "coherent": not mismatched,
        "spans_multiple": len(went_to) > 1,
    }
    if mismatched and seat_id:
        try:
            charter_rows = await conn_or_pool.fetch(
                "SELECT p.canonical AS repo FROM links l "
                "JOIN objects s ON s.id=l.from_id AND s.type='Seat' AND s.canonical=$1 "
                "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
                "WHERE l.type='governs' AND (l.valid_until IS NULL OR l.valid_until > now())",
                seat_id)
        except Exception:  # noqa: BLE001 — fail open, same law as every box above
            charter_rows = []
        charter = sorted({str(r["repo"]).removeprefix("repo:") for r in charter_rows})
        if charter and set(went_to).issubset(charter):
            result["coherent"] = True
            result["charter_repos"] = charter
    return result


async def closure_edge_coverage(
    pool: asyncpg.Pool, *, agent_id: str, mounted_at: datetime,
) -> dict[str, Any] | None:
    """PHASE 1b (decision cb38d922, Thoth's measurement, 2026-08-01): "78% OF CLOSURES
    LEAVE NO TRAVERSABLE TRACE" — the closure verb (record_decision's resolves=,
    resolve_thread's artifact=) is used constantly (527 of 737 osiris Threads are
    resolved), but the EDGE it CAN mint (`answers`, `resolved_by`) is an optional kwarg
    most callers never pass, so the graph cannot answer "is this done?" by topology alone
    for four closures in five.

    Same discipline as `filed_under_check` right above: REPORT-ONLY, computed at CONFIRM
    time against the now-updated graph, NEVER folded into `missing_boxes` or `complete`
    (ruling 577988ed — a fleet-wide single point of failure must never refuse-to-serve on
    a check that can itself false-positive). "This session resolved N threads; M of them
    now carry a closure edge" — the feedback loop that lets a settling session SEE its own
    contribution to the graph's traversability, instead of the coverage decaying because
    nobody happens to be watching.

    `resolved_this_session` reads the graph's own WINNING status assertion
    (`current_assertions`), not raw event history — the same winning-status read the
    measurement itself used, and the whole reason topology beats a property here: an edge
    exists or it does not, while a status property can be asserted by more than one source
    and miscounted (cb38d922's own '1,858-vs-345' near-miss). `with_closure_edge` is READ,
    not re-derived: `thread_closure_status` (Phase 2, src/orchestrator/thread_closure.py)
    is the one place this graph decides which edge types count as a traversable closure —
    composing it here (instead of a second hand-rolled `links` query) means this count
    tracks whatever that module currently recognizes, automatically, the moment it widens
    (a `closed_by` UNION ALL arm is the named next step, not yet wired in).

    None (fails open) when this session resolved nothing — no signal either way, the same
    convention `filed_under_check` uses; a session that closed zero threads has nothing to
    report and is never punished for the silence. Also None if `thread_closure_status`
    itself fails (fresh migration, a test DB missing the view) — the same fail-open law
    every box above follows."""
    from src.orchestrator.thread_closure import thread_closure_status

    try:
        rows = await pool.fetch(
            "SELECT o.id FROM objects o JOIN current_assertions ca "
            "  ON ca.object_id = o.id AND ca.name = 'status' "
            "WHERE o.type = 'Thread' AND ca.value #>> '{}' = 'resolved' "
            "AND ca.source_id = $1 AND ca.observed_at >= $2",
            agent_id, mounted_at)
        if not rows:
            return None
        thread_ids = [r["id"] for r in rows]
        statuses = await thread_closure_status(pool, thread_ids=thread_ids)
    except Exception:  # noqa: BLE001 — fail open, same law as every box above
        return None
    edged = sum(1 for s in statuses if s["closed_by_topology"])
    return {"resolved_this_session": len(thread_ids), "with_closure_edge": edged}


def standing_orders_touched(cwd: str | None, mounted_at: datetime) -> bool | None:
    """None (can't evaluate, fails open) when this cwd has no standing-orders file at all —
    a session working in an ordinary repo, not an office, is never punished for a file that
    was never scaffolded here. Present: mtime at or after session start.

    Checks the ON-DISK FILE (ruling 205668ec: two things were both called "charter" — this
    is the prose one, the Boot Compiler's own "standing orders" output). Still literally
    named charter.md on disk — the RENAME is a held migration (35 offices, a boot-injection
    pointer, a compiler that writes it), not authorized here; only the box's own label and
    this function's name changed, matching what they actually check."""
    if not cwd:
        return None
    from pathlib import Path
    try:
        charter = Path(cwd) / "charter.md"
        if not charter.exists():
            return None
        return charter.stat().st_mtime >= mounted_at.timestamp()
    except OSError:
        return None


async def seat_chartered(conn_or_pool: _Fetchable, seat_id: str | None) -> bool | None:
    """The DECLARATION half of ruling 205668ec's split — does this seat currently govern
    any repo at all (a live `governs` edge), independent of whether its standing-orders
    file was touched this session. None (fails open) when there is no seat to ask — an
    unseated session has no charter to hold, same convention every box in this module
    follows. Not session-scoped like its siblings above (a charter is a standing fact, not
    something written THIS session) — reads the SAME governs-edge shape charter_of()
    (charter.py) does, inlined rather than imported so this module keeps serving both a
    Pool and a bare Connection (charter_of is typed against asyncpg.Pool alone)."""
    if not seat_id:
        return None
    try:
        return bool(await conn_or_pool.fetchval(
            "SELECT 1 FROM links l JOIN objects s ON s.id=l.from_id AND s.type='Seat' "
            "AND s.canonical=$1 JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
            "WHERE l.type='governs' AND (l.valid_until IS NULL OR l.valid_until > now()) "
            "LIMIT 1", seat_id))
    except Exception:  # noqa: BLE001 — fail open, same law as every box above
        return None


def missing_boxes(boxes: dict[str, bool | None]) -> list[str]:
    """The labels of any box explicitly False — never None (fog-of-war, could not be
    evaluated) or True (satisfied). Pure; the one line both the hook's verdict and
    /settle's own confirm step share instead of each re-deriving it."""
    return [label for label, ok in boxes.items() if ok is False]


def unevaluated_boxes(boxes: dict[str, bool | None]) -> list[str]:
    """The labels of any box that is None — COULD NOT BE EVALUATED, a DIFFERENT thing
    from missing (False) or satisfied (True), and deliberately never folded into either
    (Thoth DM 3076): a check that silently never runs and a check that genuinely ran and
    found nothing satisfied must never collapse onto the same signal —
    the exact SHAPE C defect (#117) that let standing_orders_touched's own #128 cwd bug hide
    behind `complete:true` for eleven days of a real, untouched charter.md. Fog-of-war
    stays non-blocking (a box that is structurally inapplicable — e.g. an unseated session
    with no standing-orders file to check — must not permanently red every settle it will ever
    call), but it must never again be invisible: a caller SEES this list, distinctly from
    `missing_boxes`, whether or not it changes what they decide to do about it."""
    return [label for label, ok in boxes.items() if ok is None]


async def uncommitted_git_work(repo_dir: str | None, *, timeout_s: float = 2.0) -> list[str] | None:
    """THE ONE BOX NOT IN THE GRAPH (operator, 2026-07-26, watching a live compaction: an
    agent asked "safe to compact?" had to run `git status` BY HAND before answering —
    mechanical, and a wasted expensive turn). /settle-only: unlike settle_boxes above, this
    is NOT shared with the Stop hook, which runs on a strict ~1s budget the hook's own
    docstring is explicit about — a subprocess against a large working tree has no place
    racing that clock for a question the hook never asks.

    `repo_dir` is deliberately NOT assumed to be the caller's mount cwd (Thoth's catch,
    2026-07-27, msg 1381): every seat-office agent mounts at its OFFICE
    (~/.osiris/seats/<handle>), which is never a git repo — the code it governs lives
    elsewhere (~/code/<project>). Passing the mount cwd there reads None for the entire
    seat-office fleet, including the exact case that started this — silently NOT solving
    the operator's complaint. The caller (settle(), in mcp_server.py) decides what
    `repo_dir` means; this function only ever checks the directory it's given.

    None (fails open) when repo_dir is empty, not inside a git worktree, git itself
    errors, or the check outruns `timeout_s` — the honest 'could not evaluate' case, never
    silently treated as clean. A [] means clean; a non-empty list is `git status
    --porcelain` lines verbatim (' M path', '?? path', ...) — the file, not just a
    boolean, since 'what' is the whole point of the box."""
    if not repo_dir:
        return None
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "-C", repo_dir, "status", "--porcelain",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
    except OSError:
        return None
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return None
    if proc.returncode != 0:
        return None
    return [line for line in out.decode("utf-8", errors="replace").splitlines() if line]

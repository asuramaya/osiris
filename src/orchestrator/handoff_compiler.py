"""THE HANDOFF COMPILER (Thoth's dispatch, DM 2338) — a succession briefing assembled FROM
THE GRAPH, not hand-written from a departing seat's memory.

NAMED handoff_compiler.py, not handoff.py (Thoth's own suggestion): `src/orchestrator/
handoff.py` already exists and is unrelated — a live human-in-the-loop analyst handoff
TRAY for suspended scrape runs (helper_runs/handoffs table state machine, DESIGN §9), a
completely different domain that happens to share the word. Verified before building,
never assumed; see the naming precedent this mirrors instead: `boot_compiler.py`.

THE PROBLEM THIS REPLACES: every prior handoff (the `is_handoff` Decision/Thread
convention, ruling c5b184cd) is prose a session assembled by hand at the last possible
moment — good when written, but a single point of failure. A session that dies mid-turn
(abrupt compaction, a crash) leaves nothing, even though the graph already holds every
fact a handoff needs: decisions recorded, threads opened/resolved, commits landed
(`decided_in`), corrections (`supersedes`/`superseded_by`), and who a thread's next move
belongs to (`owner`). This module reads those back and assembles them — #111's "docs
compile from the graph" idea (the Boot Compiler is its proof-of-concept), applied to the
highest-value instance: the thing a successor actually reads first.

COMPILE THE FACTS, LEAVE THE JUDGMENT TO PROSE (Thoth's own lean, argued and kept): a
compiled briefing gets the WHAT right but cannot supply the WHY-IT-MATTERS — which of nine
mistakes was the one pattern underneath them, which open thread is actually urgent. Rather
than guess at that with a classifier (Stage C's two false positives this same reign are
exactly why not), `compile_handoff` returns structured facts only; `render_handoff_briefing`
renders them with an explicit, empty JUDGMENT section for the departing seat to fill BY HAND
— the compiled section is the 90% win (nothing forgotten), the hand-written section is the
irreducible 10% (what it MEANS), and neither substitutes for the other.

RENDERS ON DEMAND, NEVER AUTOMATIC-ON-SEAM (Thoth's explicit guard): a compiled briefing
nobody reviewed is a confident summary of a graph that may itself be wrong — this reign
alone found the org chart, repo coverage, and at least one live agent's aliveness all
wrong in the graph at some point. Nothing in this module calls itself; a caller (a seat, or
the `handoff_briefing` MCP tool) asks for it, reads it, and decides what to do with it.

FIVE LENSES, EACH A DIRECT READ, NONE A GUESS:
  * shipped     — Decisions filed under `repo`, minted since `since`, each showing its
                   `decided_in` commit(s) if any and whether that commit is an ancestor of
                   the project's own deploy cursor (`deployed:<repo>`, the watermark
                   `osiris deploy` itself writes) — DEPLOYED, LANDED-NOT-DEPLOYED, or
                   UNKNOWN (no on_disk_path registered, or no cursor yet: never guessed).
  * open        — this repo's live Thread wall (`compositions.open_thread_wall`, the ONE
                   wall law every other lens already reads — not a second copy), each
                   carrying `owner`: whose move it is.
  * operator_gated — the subset of `open` owned by 'operator' — named explicitly so it can
                   never silently become the next seat's own task by omission.
  * corrections — Decisions minted since `since` that `supersedes` an earlier one, both
                   sides' summaries shown side by side. Thoth's own handoff (84412d98) was
                   half corrections; this is the part hand-written prose loses first.
  * unverified_heuristic — decisions/threads (already surfaced above) whose own text
                   self-flags as unconfirmed ("UNVERIFIED", "UNCONFIRMED", "FALSIFIABLE
                   PREDICTION", ...). NO STRUCTURED MARKER EXISTS FOR THIS YET (unlike
                   `is_handoff`) — this is the same class of stopgap
                   `nearest_handoff_ancestor`'s own ILIKE fallback already is, surfaced
                   AS a heuristic, never as fact. A real fix is a typed property on the
                   claiming Decision/Thread itself (out of scope here; named, not built).
"""
from __future__ import annotations

import asyncio
import uuid as _uuid
from datetime import datetime, timedelta
from typing import Any

import asyncpg

from src.orchestrator.agents import nearest_handoff_ancestor
from src.orchestrator.compositions import open_thread_wall
from src.orchestrator.monitor import get_cursor
from src.orchestrator.projects import _resolve_software_project

# Same class of stopgap as nearest_handoff_ancestor's own ILIKE fallback (agents.py) — no
# structured marker exists for "this claim is unverified" the way `is_handoff` exists for a
# handoff. Matched case-insensitively against a summary; the FIRST marker found is reported,
# not every one — this is a flag to go read the item, not a taxonomy.
_UNVERIFIED_MARKERS = (
    "UNVERIFIED", "UNCONFIRMED", "FALSIFIABLE PREDICTION",
    "MUST BE CHECKED, NOT ASSUMED", "MUST BE CHECKED NOT ASSUMED", "NOT YET VERIFIED",
)


async def since_last_handoff(
    pool: asyncpg.Pool, agent_id: str, *, max_hops: int = 5,
) -> tuple[datetime | None, str]:
    """(since, note) — the observed_at of the freshest is_handoff-marked Decision/Thread
    found by walking `agent_id`'s OWN chain first, then its succeeded_from ancestors
    (`nearest_handoff_ancestor`, agents.py — the same bounded walk orient()'s succession-
    note block and the boot whisper already share, never a second copy). Starting at
    `agent_id` ITSELF, not its predecessor, is deliberate and differs from orient()'s own
    call: orient() asks "what did my ancestor leave ME", so it starts one hop back;
    `since_last_handoff` asks "what is the most recent handoff on THIS lineage, including
    one I wrote myself earlier in this same reign" — the boundary for what counts as
    NEW — so it must check `agent_id` first.

    Calls `nearest_handoff_ancestor` with `respect_ack=False` — this asks "when did this
    reign end", a historical boundary fact that stays true whether or not anyone has since
    acknowledged reading the handoff (the ack-based retirement redesign, 2026-08-03). With
    the default `respect_ack=True`, an already-acked handoff would be invisible here and
    this function would silently walk past it to a more distant ancestor, mis-dating the
    boundary and re-listing already-summarized work — the exact double-count this
    function's own EXCLUSIVE-boundary logic below exists to prevent.

    None (compile the FULL history) when no handoff is found within `max_hops` — a fresh
    lineage, or one that predates the convention, is not an error; it is simply
    everything.

    The returned boundary is EXCLUSIVE of the handoff marker's own moment (one microsecond
    past its observed_at): the handoff Decision/Thread is the CLOSING act of the reign it
    summarizes, not the opening act of the next one — `compile_handoff`'s own `>=` filter
    would otherwise re-list a predecessor's own handoff as "shipped" in their successor's
    first briefing, which is exactly the kind of confusing double-count a boundary exists
    to prevent."""
    found = await nearest_handoff_ancestor(
        pool, agent_id, max_hops=max_hops, respect_ack=False)
    if found is None:
        return None, "no prior handoff found within the walked chain — compiling full history"
    ancestor_id, picks = found
    newest = max(picks, key=lambda p: p["observed_at"])
    since = newest["observed_at"] + timedelta(microseconds=1)
    return since, (
        f"since the last handoff on {ancestor_id} ({newest['observed_at'].isoformat()}): "
        f"{(newest['summary'] or '')[:120]}")


async def _deploy_verdict(
    repo_root: str | None, sha: str, deployed_sha: str | None,
) -> tuple[bool | None, str]:
    """(deployed, note) for one commit sha against the project's own deploy cursor.
    UNKNOWN (None), never guessed, when either side is missing or the check itself fails —
    same fail-open law deploy_guard.py's own comparisons run on (schema_drift,
    unreviewed_boot): an error IN the check is not evidence of anything about the commit."""
    if not repo_root:
        return None, "no on_disk_path registered for this project — deploy status unknown"
    if not deployed_sha:
        return None, "no deploy cursor recorded for this project — deploy status unknown"
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "-C", repo_root, "merge-base", "--is-ancestor", sha, deployed_sha,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        rc = await asyncio.wait_for(proc.wait(), timeout=2.0)
    except (OSError, TimeoutError):
        return None, "git ancestry check failed — deploy status unknown"
    if rc == 0:
        return True, f"deployed (ancestor of {deployed_sha[:12]})"
    if rc == 1:
        return False, f"landed, not yet deployed (cursor is at {deployed_sha[:12]})"
    return None, "git could not resolve one of the shas — deploy status unknown"


def _flag_unverified(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for it in items:
        text = (it.get("summary") or "").upper()
        marker = next((m for m in _UNVERIFIED_MARKERS if m in text), None)
        if marker:
            out.append({**it, "marker": marker})
    return out


async def compile_handoff(
    pool: asyncpg.Pool, *, repo: str, since: datetime | None = None,
) -> dict[str, Any]:
    """The data function — pure facts, no ranking relative to a caller's own identity (that
    is `render_handoff_briefing`'s and the MCP wrapper's job, which know who's asking; this
    stays agent-agnostic and directly testable). Returns {} if `repo` does not resolve to a
    live SoftwareProject."""
    proj = await _resolve_software_project(pool, repo)
    if proj is None:
        return {}
    proj_id, name = proj["id"], proj["canonical"].removeprefix("repo:")

    repo_root = await pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "WHERE a.object_id=$1 AND a.name='on_disk_path' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", proj_id)
    deployed_sha = await get_cursor(pool, f"deployed:{name}")

    shipped_rows = await pool.fetch(
        "WITH recorded AS ("
        "  SELECT a.object_id, min(a.observed_at) AS recorded_at FROM assertions a "
        "  WHERE a.name='summary' AND a.evidence_class='self_declared' GROUP BY a.object_id"
        ") "
        "SELECT d.id, "
        " (SELECT a.value #>>'{}' FROM current_assertions a WHERE a.object_id=d.id "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS summary, "
        " (SELECT a.value #>>'{}' FROM current_assertions a WHERE a.object_id=d.id "
        "   AND a.name='kind' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind, "
        " r.recorded_at, "
        " (SELECT array_agg(DISTINCT c.canonical) FROM links dl JOIN objects c ON c.id=dl.to_id "
        "   WHERE dl.from_id=d.id AND dl.type='decided_in') AS commit_canons "
        "FROM objects d "
        "JOIN links l ON l.from_id=d.id AND l.type='in_repo' AND l.to_id=$1 "
        "JOIN recorded r ON r.object_id=d.id "
        "WHERE d.type='Decision' AND d.status='active' "
        "  AND ($2::timestamptz IS NULL OR r.recorded_at >= $2) "
        "ORDER BY r.recorded_at ASC LIMIT 500", proj_id, since)

    shipped: list[dict[str, Any]] = []
    for row in shipped_rows:
        commits: list[dict[str, Any]] = []
        for canon in row["commit_canons"] or []:
            sha = canon.removeprefix("commit:")
            deployed, note = await _deploy_verdict(repo_root, sha, deployed_sha)
            commits.append({"sha": sha, "deployed": deployed, "note": note})
        shipped.append({
            "id": str(row["id"])[:8], "summary": row["summary"], "kind": row["kind"],
            "recorded_at": row["recorded_at"].isoformat() if row["recorded_at"] else None,
            "commits": commits,
        })

    correction_rows = await pool.fetch(
        "WITH recorded AS ("
        "  SELECT a.object_id, min(a.observed_at) AS recorded_at FROM assertions a "
        "  WHERE a.name='summary' AND a.evidence_class='self_declared' GROUP BY a.object_id"
        ") "
        "SELECT d.id AS new_id, "
        " (SELECT a.value #>>'{}' FROM current_assertions a WHERE a.object_id=d.id "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS new_summary, "
        " sup.value #>>'{}' AS old_id_text, r.recorded_at "
        "FROM objects d "
        "JOIN links l ON l.from_id=d.id AND l.type='in_repo' AND l.to_id=$1 "
        "JOIN recorded r ON r.object_id=d.id "
        "JOIN current_assertions sup ON sup.object_id=d.id AND sup.name='supersedes' "
        "WHERE d.type='Decision' AND d.status='active' "
        "  AND ($2::timestamptz IS NULL OR r.recorded_at >= $2) "
        "ORDER BY r.recorded_at ASC", proj_id, since)

    old_ids = [r["old_id_text"] for r in correction_rows if r["old_id_text"]]
    old_summaries: dict[str, str | None] = {}
    if old_ids:
        old_rows = await pool.fetch(
            "SELECT o.id, (SELECT a.value #>>'{}' FROM current_assertions a "
            "  WHERE a.object_id=o.id AND a.name='summary' "
            "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS summary "
            "FROM objects o WHERE o.id = ANY($1::uuid[])",
            [_uuid.UUID(i) for i in old_ids])
        old_summaries = {str(r["id"]): r["summary"] for r in old_rows}
    corrections = [
        {
            "new_id": str(r["new_id"])[:8], "new_summary": r["new_summary"],
            "old_id": r["old_id_text"][:8] if r["old_id_text"] else None,
            "old_summary": old_summaries.get(r["old_id_text"]),
            "recorded_at": r["recorded_at"].isoformat() if r["recorded_at"] else None,
        }
        for r in correction_rows
    ]

    wall, echoes = await open_thread_wall(pool, proj_id)
    # open_thread_wall's own rows OMIT "kind"/"owner"/"arc" entirely when undeclared (no
    # null-key noise, by that function's own design) — `.get()`, never `[...]`, or an
    # ordinary unclaimed/untyped thread KeyErrors this whole lens.
    open_items = [
        {
            "id": r["id"], "summary": r["summary"], "kind": r.get("kind"),
            "owner": r.get("owner"), "arc": r.get("arc"),
        }
        for r in sorted(wall, key=lambda r: (
            r.get("kind") != "obligation", (r.get("owner") or "").strip() != "operator"))
    ]
    operator_gated = [r for r in open_items if (r.get("owner") or "").strip() == "operator"]

    unverified = _flag_unverified(shipped) + [
        {**u, "kind": u.get("kind") or "thread"} for u in _flag_unverified(open_items)
    ]

    return {
        "repo": name,
        "since": since.isoformat() if since else None,
        "deploy_cursor": {"sha": deployed_sha, "on_disk_path": repo_root},
        "shipped": shipped,
        "open": open_items,
        "open_echo_count": len(echoes),
        "operator_gated": operator_gated,
        "corrections": corrections,
        "unverified_heuristic": unverified,
    }


def render_handoff_briefing(data: dict[str, Any]) -> str:
    """Markdown rendering of `compile_handoff`'s data — the COMPILED half only. Always ends
    with an explicit, empty JUDGMENT section: the departing seat's own prose belongs there,
    on top of the compiled facts, never instead of them (Thoth's lean, DM 2338) — this
    function never guesses at it, and never omits the section either, so it can't be
    forgotten by omission."""
    if not data:
        return "# Handoff briefing\n\nno such project — nothing compiled.\n"
    lines = [f"# Handoff briefing — {data['repo']}",
             f"since: {data['since'] or '(no prior handoff found — full history)'}", ""]

    lines.append("## Shipped")
    if not data["shipped"]:
        lines.append("(nothing recorded in this window)")
    for d in data["shipped"]:
        lines.append(f"- **{d['id']}** ({d['kind']}): {d['summary']}")
        for c in d["commits"]:
            mark = "deployed" if c["deployed"] is True else \
                   "NOT DEPLOYED" if c["deployed"] is False else "deploy status unknown"
            lines.append(f"  - commit `{c['sha']}` — {mark}")
        if not d["commits"]:
            lines.append("  - no commit cited")
    lines.append("")

    lines.append("## Corrections")
    if not data["corrections"]:
        lines.append("(none this window)")
    for c in data["corrections"]:
        lines.append(f"- **{c['new_id']}** supersedes **{c['old_id']}**")
        lines.append(f"  - was: {c['old_summary'] or '(summary unavailable)'}")
        lines.append(f"  - now: {c['new_summary']}")
    lines.append("")

    lines.append(f"## Open ({len(data['open'])} live, "
                 f"{data['open_echo_count']} echo(es) not shown)")
    for t in data["open"]:
        owner = t["owner"] or "unowned"
        lines.append(f"- **{t['id']}** [{t['kind'] or 'thread'}] (owner: {owner}): "
                     f"{t['summary']}")
    lines.append("")

    lines.append("## Operator-gated (blocked on the human's own word or hands)")
    if not data["operator_gated"]:
        lines.append("(none)")
    for t in data["operator_gated"]:
        lines.append(f"- **{t['id']}**: {t['summary']}")
    lines.append("")

    lines.append("## Unverified (heuristic — self-flagged text, not a structured marker; "
                 "verify before trusting)")
    if not data["unverified_heuristic"]:
        lines.append("(none matched)")
    for u in data["unverified_heuristic"]:
        lines.append(f"- **{u['id']}** [{u['marker']}]: {u['summary']}")
    lines.append("")

    lines.append("## Judgment (fill in by hand — the compiled section above is facts only)")
    lines.append("_what actually mattered this reign, and why — the departing seat's own "
                 "call, never compiled._")
    return "\n".join(lines)

"""THE SELF-SERVICE TRANSITION VERB (Thoth dispatch 6901, task #199's core-verbs lane,
the Jesus/Chad specimen): a seat moves its project binding from a fabricated
handle-project to the real repo project it actually works in, in one composed act.
Never a fourth mechanism — folds three already-shipped, independently-audited
primitives: `invalidate_works_in` (drop the stale edge), `correct_own_pin_value` (all
three pin copies — office, anchor, workspace — ruling b30e2b38), `set_charter`
(declare the real repos).

DELIBERATELY EXCLUDES `rebind_seat`, even though the operator's own parity-matrix scan
(decision fff496fe22b0) named it as part of a 5-call sequence: THE ANCHOR INVARIANT
(ruling 23771416, landed after that scan) pins `anchor_cwd` to
`<office_root>/<handle>` permanently, DERIVED, never a caller-supplied path — and
Jesus/Chad broke their own anchors by calling `rebind_seat` on themselves during
exactly this kind of transition (root-caused live, same ruling). A seat's WORK tree
moving is `bind_seat_tree`'s job, a separate concern this verb does not touch.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions


async def transition_seat_project(
    pool: asyncpg.Pool, agent_id: str, *,
    fabricated_project: str | None = None, real_project: str | None = None,
    because: str = "", repos: list[str] | None = None, dry_run: bool = True,
    office_root: Path | None = None, workspace_root: Path | None = None,
) -> dict[str, Any]:
    """PRECONDITION: the caller already carries TWO live works_in edges — the
    fabricated one and a real one — which means mounting at the real repo's cwd
    FIRST is a prerequisite this verb does not perform itself (same shape
    `invalidate_works_in` already requires). `fabricated_project` defaults to the
    seat's own handle (the exact specimen shape: a project fabricated FROM a handle
    shares its name). `real_project` disambiguates when more than one other live
    edge exists; omitted, it auto-picks the sole other edge and refuses rather than
    guesses when there is more than one.

    `dry_run=True` (default) returns the PLAN — which of invalidate/pin/charter
    actually differ from the target state — without writing anything. `dry_run=False`
    requires `because` and executes each planned step against the SAME functions
    their own MCP/CLI doors already wrap, in order: a step already matching the
    target state is skipped, not re-run as a no-op write. Every precondition is
    checked before the FIRST write (same ATOMIC-OR-REFUSED discipline
    `normalize_project_casing` uses) — a partial result past that point can only come
    from a genuine race, reported loudly under `error`, never silently swallowed."""
    from src.orchestrator.agents import invalidate_works_in
    from src.orchestrator.charter import charter_of, set_charter
    from src.orchestrator.offices import _default_office_root, correct_own_pin_value
    from src.orchestrator.projects import _peek_pin_value, _resolve_project_ref
    from src.orchestrator.seats import held_seat

    agent_id = (agent_id or "").strip()
    if not agent_id:
        return {"error": "agent_id is required"}
    bound = await held_seat(pool, agent_id)
    if bound is None:
        return {"error": f"{agent_id} holds no seat — a project transition is a seat's "
                         "own act, never performed on another's behalf"}
    handle = bound["handle"]

    fab_ref = (fabricated_project or handle).strip()
    fab_row, err = await _resolve_project_ref(pool, fab_ref, verb="transition_seat_project")
    if err:
        return err
    if fab_row is None:
        return {"error": f"no such SoftwareProject: {fab_ref!r} — nothing fabricated to "
                         "transition away from"}

    agent_row = await pool.fetchrow(
        "SELECT id, canonical FROM objects WHERE canonical=$1 AND type='Agent' "
        "AND status='active'", agent_id)
    if agent_row is None:
        return {"error": f"no such active Agent: {agent_id!r}"}
    live = await pool.fetch(
        "SELECT to_id, t.canonical AS project FROM links l JOIN objects t ON t.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='works_in' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", agent_row["id"])
    live_by_id = {r["to_id"]: r["project"] for r in live}
    if fab_row["id"] not in live_by_id:
        return {"error": f"{agent_row['canonical']} has no active works_in edge to "
                         f"{fab_row['canonical']} — nothing to transition"}
    others = {k: v for k, v in live_by_id.items() if k != fab_row["id"]}
    if not others:
        return {"error": f"{fab_row['canonical']} is your ONLY live works_in edge — "
                         "mount at the real repo's cwd first (this verb transitions an "
                         "already-dual binding, it does not create the first one)"}
    if real_project:
        real_row, err2 = await _resolve_project_ref(
            pool, real_project, verb="transition_seat_project")
        if err2:
            return err2
        if real_row is None or real_row["id"] not in others:
            return {"error": f"{real_project!r} is not among your other live works_in "
                             f"edges: {sorted(others.values())}"}
        real_id, real_canonical = real_row["id"], real_row["canonical"]
    elif len(others) == 1:
        ((real_id, real_canonical),) = others.items()
    else:
        return {"error": f"ambiguous — {len(others)} live works_in edges besides "
                         f"{fab_row['canonical']}: {sorted(others.values())}. Pass "
                         "real_project= explicitly; transition_seat_project never "
                         "guesses which one is real."}
    del real_id  # resolved only to prove membership; the canonical is what's used below

    real_name = real_canonical.removeprefix("repo:")
    root = office_root or _default_office_root()
    office = root / handle.lower()
    pin_peek = _peek_pin_value(str(office), "project")
    pin_needs_write = not pin_peek.get("ok") or pin_peek.get("value") != real_name

    wanted_repos = sorted({r.strip().removeprefix("repo:") for r in (repos or [real_name])
                           if r and r.strip()})
    current_charter = set(await charter_of(pool, bound["seat_id"]))
    charter_needs_write = set(wanted_repos) != current_charter

    plan: dict[str, Any] = {
        "invalidate_works_in": fab_row["canonical"],
        "correct_pin_value": (
            {"key": "project", "value": real_name} if pin_needs_write else None),
        "set_charter": wanted_repos if charter_needs_write else None,
    }
    out: dict[str, Any] = {
        "seat": bound["seat_id"], "fabricated_project": fab_row["canonical"],
        "real_project": real_canonical, "dry_run": dry_run, "plan": plan,
    }
    if dry_run:
        return out

    because = (because or "").strip()
    if not because:
        return {"error": "because is required to execute — dry_run=False without a "
                         "reason is refused before anything is touched"}

    steps: dict[str, Any] = {}
    steps["invalidate_works_in"] = await invalidate_works_in(
        Actions(pool), agent_id, fab_row["canonical"], because=because, actor=agent_id)
    if steps["invalidate_works_in"].get("error"):
        out["error"] = "invalidate_works_in refused — nothing else touched"
        out["steps"] = steps
        return out
    if pin_needs_write:
        steps["correct_pin_value"] = await correct_own_pin_value(
            pool, agent_id, "project", real_name, reason=because, office_root=office_root,
            workspace_root=workspace_root)
    if charter_needs_write:
        steps["set_charter"] = await set_charter(
            Actions(pool), bound["seat_id"], wanted_repos, actor=agent_id)
    out["steps"] = steps
    return out

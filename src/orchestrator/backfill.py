"""THE BACKFILL DISPATCH (thread c89a9873, wave 22, ruling 7be61879) — extracted from
mcp_server.py's own private `_backfill_impl` so the CLI and MCP surfaces call the SAME
orchestrator function, never one calling the other (the house's own CLI/MCP pair
convention — cmd_fleet_reconcile/fleet_reconcile.reconcile_execute is the template this
follows).

Seven structurally distinct repair verbs, no shared logic underneath, only a shared wire
shape (dry_run default True, idempotent). `BACKFILL_TARGETS` names all seven; `run_backfill`
dispatches on `target`. This module owns nothing about identity/mounting — `actor` arrives
already resolved, exactly like every other orchestrator-layer function in this codebase
(fleet_reconcile.reconcile_execute, charter.charter_for, ...). The MCP tool layer keeps its
own mount-gate (`_ident_for`) before calling in; the CLI layer takes `--actor` directly. No
behavior change from the pre-extraction `_backfill_impl` — a pure move."""

from __future__ import annotations

from typing import Any

import asyncpg

from src.actions.core import Actions

BACKFILL_TARGETS = frozenset({
    "bootstrap_orphan_references", "boot_alarm_commit_links", "task_sync_citation_links",
    "lineage_repo_links", "agent_project_links", "closed_by_real_sources",
    "operator_charter",
})

# CLI-ONLY FOR APPLY (thread c89a9873's own scope note, wave 22): operator_charter mints
# a `governs` link from person:operator to EVERY active SoftwareProject in one call — the
# live mechanism behind thread 1d5b9773's "authority by charter" design, fleet-wide blast
# radius, directly defines the operator's own scope of authority. The standard high-
# consequence confirm gate other UI writes get is not, on its own, enough ceremony for a
# write that redefines fleet-wide authority scope. The UI's Repairs panel may still show
# this target's DRY-RUN preview (read-only, harmless); its apply action must be absent —
# and `check_apply_authority` below refuses it structurally, so a caller bypassing the UI
# and posting straight to REST is refused too, never just permission-gated.
UI_APPLY_EXCLUDED_TARGETS = frozenset({"operator_charter"})


async def check_apply_authority(
    pool: asyncpg.Pool, target: str, *, actor: str, ruling: str | None = None,
    surface: str = "rest",
) -> str | None:
    """THE UI/REST DOOR'S OWN AUTHORITY GATE (thread c89a9873) — `operator_or_ruling`,
    the same shape `settings_service._authorized` already runs for a settings write:
    the caller must be a recognized operator actor (`seats._OPERATOR_ACTORS`), or cite a
    standing ruling naming this exact write via `verify_ruling`. Returns an error string,
    or None when authorized.

    `surface='ui'` (or any REST-originated call) additionally refuses `target ==
    'operator_charter'` OUTRIGHT — never merely gated behind confirm, structurally
    excluded regardless of the caller's own authority, per this house's own scope note.
    The CLI door does not call this function at all (it has its own, separate --because
    gate) — this check exists ONLY for the UI/REST surface's own write path."""
    if target in UI_APPLY_EXCLUDED_TARGETS:
        return (f"{target!r} cannot be applied from the UI/REST surface — its blast "
                "radius (fleet-wide operator authority) requires a deliberate terminal "
                f"act: `osiris backfill {target} --apply --because <reason>`")
    from src.orchestrator.seats import _OPERATOR_ACTORS

    if actor in _OPERATOR_ACTORS:
        return None
    if ruling:
        from src.orchestrator.capture import verify_ruling

        check = await verify_ruling(pool, ruling, write_name=f"backfill:{target}")
        return None if check["ok"] else check["error"]
    return (f"{actor!r} is not an operator actor — applying a backfill requires citing "
            "a standing ruling via `ruling=`, or the operator making this change "
            "directly")


async def run_backfill(
    pool: asyncpg.Pool, target: str, *, actor: str, dry_run: bool = True,
    because: str | None = None, only_bases: list[str] | None = None,
) -> dict[str, Any]:
    """Repair verb, dispatched over `target` — see `BACKFILL_TARGETS` for the full set.
    Dry run is the default for every target; `dry_run=False` requires `because` (except
    `agent_project_links`, which predates that convention — callers that want a stricter
    contract than this function's own must enforce it themselves, e.g. the CLI/UI doors
    built for wave 22 impose `because` unconditionally at their own layer). All seven
    idempotent."""
    if target == "bootstrap_orphan_references":
        from src.ingest.reference import (
            backfill_bootstrap_orphan_references as _f_orphan_refs,
        )
        return await _f_orphan_refs(
            Actions(pool), actor=actor, dry_run=dry_run, because=because)
    if target == "boot_alarm_commit_links":
        from src.orchestrator.capture import backfill_boot_alarm_commit_links as _f_boot_alarm
        return await _f_boot_alarm(
            Actions(pool), actor=actor, dry_run=dry_run, because=because)
    if target == "task_sync_citation_links":
        from src.orchestrator.task_sync import (
            backfill_task_sync_citation_links as _f_task_sync,
        )
        return await _f_task_sync(
            Actions(pool), actor=actor, dry_run=dry_run, because=because)
    if target == "lineage_repo_links":
        from src.orchestrator.capture import backfill_lineage_repo_links as _f_lineage
        return await _f_lineage(
            Actions(pool), actor=actor, dry_run=dry_run, because=because)
    if target == "agent_project_links":
        from src.orchestrator.agents import backfill_agent_project_links as _f_agent_links
        return await _f_agent_links(
            Actions(pool), actor=actor, dry_run=dry_run,
            only_bases=set(only_bases) if only_bases else None)
    if target == "closed_by_real_sources":
        from src.orchestrator.capture import (
            backfill_closed_by_real_sources as _f_closed_by_real_sources,
        )
        return await _f_closed_by_real_sources(
            Actions(pool), actor=actor, dry_run=dry_run, because=because)
    if target == "operator_charter":
        from src.orchestrator.capture import backfill_operator_charter as _f_operator_charter
        return await _f_operator_charter(
            Actions(pool), actor=actor, dry_run=dry_run, because=because)
    return {"error": f"unknown target {target!r}", "valid_targets": sorted(BACKFILL_TARGETS)}

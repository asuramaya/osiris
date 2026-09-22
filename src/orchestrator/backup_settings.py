"""backup_settings — thin door over the generalized settings registry (THE SETTINGS MENU
piece 3, thread 7eb26f68, Thoth's GO mail 10084/10094) folding the write half Wave 21
built (thread f04cce36 piece 3b) into `src/config/settings_registry.py`'s own SettingSpecs,
so this domain gets the SAME validation/authority/effect machinery every other knob does
instead of a parallel bespoke implementation.

STORAGE MOVED, THE DOOR DIDN'T: `get_backup_settings`/`write_backup_settings` keep their
exact outward shapes (dict in, dict out) — the MCP `backup_settings` tool, the
`/backup-settings` REST routes, `compositions.py`'s `backup_status` Function, and
`scripts/render_units.py` (WAVE 22 generalized this from `render_backup_timers.py`, its
own former name) all call these unchanged. Internally each field now
reads/writes through `settings_service.get_setting`/`write_setting` against the generic
`settings` table (`backup.vault_path`, one `backup.timer_schedule.<unit>` key PER timer —
not one shared blob, so a single schedule tweak never touches the other four — and
`backup.offbox_repositories`), never the old `backup_settings` singleton table. That table
is left in place, unused: dropping it is its own separate act this fold doesn't make, and
dev's own row was still at its seeded defaults (rev=0, every field empty/null) when this
landed, so nothing needed migrating across.

CLEARING AN OVERRIDE: a `path`/`schedule` value of `None` means "no override" (falls back
to the spec's own `None` default on read) — `settings_service._validate_value` accepts
`None` for exactly these two types for that reason: an operator needs to UN-set a real
infra path or schedule, not just overwrite it with another one, the same "clearing an
input and saving drops it" UX the original panel had.

THE BACKUP TOPOLOGY / INTERMITTENT TARGETS (Thoth mail 12812, operator ruling be21384a):
`offload_targets` REPLACES `offbox_repositories` as the canonical offload field —
`offbox_repositories` stays readable (its SettingSpec is unchanged, unused by any new
write path) so nothing already written goes dark. `get_backup_settings` returns BOTH:
`offload_targets` is the canonical field, and when it has never been explicitly written
(empty) but `offbox_repositories` DOES have rows, it's synthesized read-time from them
(kind='restic', expected_mountpoint=None — the only shape an old row could ever have
meant, since 'local' targets didn't exist before this) rather than requiring a real
write-forward migration the operator would have to remember to run. `offbox_repositories`
itself is also still returned, raw and unchanged, for one release."""
from __future__ import annotations

import asyncio
from typing import Any

import asyncpg

from src.config.settings_registry import BACKUP_TIMER_UNITS

__all__ = ["BACKUP_TIMER_UNITS", "get_backup_settings", "write_backup_settings"]

_VAULT_KEY = "backup.vault_path"
_OFFBOX_KEY = "backup.offbox_repositories"
_OFFLOAD_KEY = "backup.offload_targets"
_KNOWN_FIELDS = ("vault_path", "timer_schedules", "offbox_repositories", "offload_targets")


def _timer_key(unit: str) -> str:
    return f"backup.timer_schedule.{unit}"


def _synthesize_offload_targets_from_offbox(
    offbox_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Read-time-only migration (never writes anything): an old `offbox_repositories`
    row could only ever have meant a restic backend — 'local' targets are new. Falls
    back to the URL itself as `name` when the old row never carried one (the field
    existed in the SPEC's item_shape but no real writer populated it before now)."""
    out = []
    for i, r in enumerate(offbox_rows):
        if not isinstance(r, dict):
            continue
        out.append({
            "name": r.get("name") or r.get("url") or f"offbox-{i}",
            "kind": "restic",
            "path_or_url": r.get("url", ""),
            "expected_mountpoint": None,
            "schedule": r.get("schedule") or "",
            "enabled": bool(r.get("enabled", False)),
        })
    return out


async def _target_presence(target: dict[str, Any]) -> dict[str, Any] | None:
    """Per-target `present now` (Thoth mail 12812: `backup-settings get`/`backup-status`
    both show this). 'local' → the real live-mount check (`check_local_target_presence`,
    findmnt); 'restic' → None, deliberately — reachability is a network fact this door
    never checks (Thoth's own repeated instruction), so there is no honest yes/no to
    give here at all, only a shape verdict at write time."""
    if target.get("kind") != "local":
        return None
    mountpoint = target.get("expected_mountpoint")
    if not isinstance(mountpoint, str) or not mountpoint:
        return None
    from src.orchestrator.backup_validation import check_local_target_presence

    return await asyncio.to_thread(check_local_target_presence, mountpoint)


async def get_backup_settings(pool: asyncpg.Pool) -> dict[str, Any]:
    """The current settings — what the panel's write controls should show as their own
    starting values (the read half, `backup_status`, shows LIVE facts; this shows what
    the operator has actually SET, which may not have taken effect yet if deploy hasn't
    re-run)."""
    from src.orchestrator.settings_service import get_setting

    vault = await get_setting(pool, _VAULT_KEY)
    timer_schedules: dict[str, str] = {}
    for unit in BACKUP_TIMER_UNITS:
        val = (await get_setting(pool, _timer_key(unit)))["value"]
        if val:
            timer_schedules[unit] = val
    offbox_rows: list[dict[str, Any]] = (await get_setting(pool, _OFFBOX_KEY))["value"] or []
    offload_rows: list[dict[str, Any]] = (await get_setting(pool, _OFFLOAD_KEY))["value"] or []
    if not offload_rows and offbox_rows:
        offload_rows = _synthesize_offload_targets_from_offbox(offbox_rows)
    with_presence: list[dict[str, Any]] = []
    for t in offload_rows:
        with_presence.append(dict(t, presence=await _target_presence(t)))
    offload_rows = with_presence
    return {
        "vault_path": vault["value"],
        "timer_schedules": timer_schedules,
        "offload_targets": offload_rows,
        "offbox_repositories": offbox_rows,  # deprecated, one release — see module docstring
    }


async def write_backup_settings(
    pool: asyncpg.Pool, *, actor: str, because: str, ruling: str | None = None,
    **fields: Any,
) -> dict[str, Any]:
    """THE WRITE DOOR — unchanged outward shape, now a thin fan-out over
    `settings_service.write_setting` (one call per changed key), which carries the exact
    authority/because/validation contract this door used to implement by hand:
    `spec.authority='operator_or_ruling'`, `spec.write_name='backup_settings'`,
    `spec.requires_because=True` on every `backup.*` spec — the same 2-branch shape this
    door always had (the operator's own hand, or a ruling that names it), generalized
    rather than reinvented. `timer_schedules` stays a FULL-REPLACE field, same as
    before: any unit missing from the given dict is explicitly cleared (written as
    `None`), not left alone — "clearing an input and saving drops that override" holds
    at the per-unit level now too."""
    from src.orchestrator.settings_service import write_setting

    unknown = set(fields) - set(_KNOWN_FIELDS)
    if unknown:
        return {"error": f"unknown backup_settings field(s): {', '.join(sorted(unknown))}"}

    warnings: list[str] = []

    if "vault_path" in fields:
        vault_path = fields["vault_path"]
        # None clears the override — nothing to validate. Otherwise this mirrors
        # write_setting's own because -> authority -> value-shape order EXACTLY (calling
        # its own private `_authorized`, same spec, rather than a second authority
        # implementation) before running the deep filesystem checks below — an empty
        # `because` or an unauthorized actor must refuse with THAT reason, never a path
        # complaint about a write that was never going to happen anyway.
        if vault_path is not None:
            if not (because or "").strip():
                pass  # write_setting's own check below fires with its own message
            else:
                from src.config.settings_registry import spec_by_key
                from src.orchestrator.settings_service import _authorized

                vault_spec = spec_by_key(_VAULT_KEY)
                assert vault_spec is not None  # registered above, in this same module's spec
                auth_error = await _authorized(
                    pool, vault_spec, actor=actor, scope_id="", ruling=ruling)
                if auth_error is None:
                    from src.orchestrator.backup_validation import validate_vault_path

                    verdict = validate_vault_path(vault_path)
                    if not verdict["ok"]:
                        return {"error": f"vault_path {vault_path!r} refused: "
                                         f"{'; '.join(verdict['errors'])}"}
                    warnings.extend(verdict["warnings"])
        res = await write_setting(pool, _VAULT_KEY, fields["vault_path"],
                                  actor=actor, because=because, ruling=ruling)
        if res.get("error"):
            return res

    if "timer_schedules" in fields:
        sched = fields["timer_schedules"]
        if not isinstance(sched, dict):
            return {"error": "timer_schedules must be an object mapping unit name -> "
                             "OnCalendar="}
        unknown_units = set(sched) - set(BACKUP_TIMER_UNITS)
        if unknown_units:
            return {"error": f"unknown timer unit(s): {', '.join(sorted(unknown_units))} "
                             f"— must be one of {', '.join(BACKUP_TIMER_UNITS)}"}
        for unit in BACKUP_TIMER_UNITS:
            res = await write_setting(pool, _timer_key(unit), sched.get(unit),
                                      actor=actor, because=because, ruling=ruling)
            if res.get("error"):
                return res

    if "offbox_repositories" in fields:
        res = await write_setting(pool, _OFFBOX_KEY, fields["offbox_repositories"],
                                  actor=actor, because=because, ruling=ruling)
        if res.get("error"):
            return res

    if "offload_targets" in fields:
        from src.orchestrator.backup_validation import validate_restic_url

        targets = fields["offload_targets"]
        # Same because -> authority -> value-shape precedence as vault_path above — the
        # restic-grammar check only runs once both would-be-blocking checks already
        # passed, so a "no ruling"/"needs a because" refusal never hides behind a path
        # complaint about a write that was never going to happen anyway.
        run_shape_check = False
        if isinstance(targets, list) and (because or "").strip():
            from src.config.settings_registry import spec_by_key
            from src.orchestrator.settings_service import _authorized

            offload_spec = spec_by_key(_OFFLOAD_KEY)
            assert offload_spec is not None  # registered above, in this same module's spec
            auth_error = await _authorized(
                pool, offload_spec, actor=actor, scope_id="", ruling=ruling)
            run_shape_check = auth_error is None
        if run_shape_check:
            # SHAPE ONLY, no network call — the schema-level _validate_offload_targets
            # (settings_registry.py) already requires the field to exist; this adds the
            # restic-grammar check Thoth's own mail asked for at write time, same as the
            # vault_path validator above, never touching a 'local' kind (that one's own
            # presence is expected to fluctuate — checked read-only, never refused).
            for t in targets:
                if isinstance(t, dict) and t.get("kind") == "restic":
                    verdict = validate_restic_url(t.get("path_or_url", ""))
                    if not verdict["url_shape_ok"]:
                        return {"error": f"offload_targets[{t.get('name')!r}] refused: "
                                         f"{verdict['error']}"}
        res = await write_setting(pool, _OFFLOAD_KEY, fields["offload_targets"],
                                  actor=actor, because=because, ruling=ruling)
        if res.get("error"):
            return res

    result = await get_backup_settings(pool)
    result["because"] = because
    if warnings:
        result["warnings"] = warnings
    return result

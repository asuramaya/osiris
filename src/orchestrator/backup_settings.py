"""backup_settings — thin door over the generalized settings registry (THE SETTINGS MENU
piece 3, thread 7eb26f68, Thoth's GO mail 10084/10094) folding the write half Wave 21
built (thread f04cce36 piece 3b) into `src/config/settings_registry.py`'s own SettingSpecs,
so this domain gets the SAME validation/authority/effect machinery every other knob does
instead of a parallel bespoke implementation.

STORAGE MOVED, THE DOOR DIDN'T: `get_backup_settings`/`write_backup_settings` keep their
exact outward shapes (dict in, dict out) — the MCP `backup_settings` tool, the
`/backup-settings` REST routes, `compositions.py`'s `backup_status` Function, and
`scripts/render_backup_timers.py` all call these unchanged. Internally each field now
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
input and saving drops it" UX the original panel had."""
from __future__ import annotations

from typing import Any

import asyncpg

from src.config.settings_registry import BACKUP_TIMER_UNITS

__all__ = ["BACKUP_TIMER_UNITS", "get_backup_settings", "write_backup_settings"]

_VAULT_KEY = "backup.vault_path"
_OFFBOX_KEY = "backup.offbox_repositories"
_KNOWN_FIELDS = ("vault_path", "timer_schedules", "offbox_repositories")


def _timer_key(unit: str) -> str:
    return f"backup.timer_schedule.{unit}"


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
    offbox = await get_setting(pool, _OFFBOX_KEY)
    return {
        "vault_path": vault["value"],
        "timer_schedules": timer_schedules,
        "offbox_repositories": offbox["value"] or [],
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

    if "vault_path" in fields:
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

    result = await get_backup_settings(pool)
    result["because"] = because
    return result

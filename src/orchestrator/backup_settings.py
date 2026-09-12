"""backup_settings — the backup config panel's write half (Wave 21, operator's word
2026-09-11, thread f04cce36 piece 3).

The scope report (annotated on f04cce36) found this whole domain has no config layer at
all: every setting is a literal baked into a systemd unit file or a Python default-
argument constant. This module is the FIRST one — a SINGLETON settings row, same shape
`console.py`'s `console_state` already uses (id='default', an allow-listed field set, a
monotonic `rev`, `updated_by` recording who moved it last) — chosen over a new ontology
ObjectType (schema.py's ceremony — category/color/shape/description — is for real graph
entities with provenance; a settings blob is not one).

WRITE GATE (Thoth dispatch 9870/9895, "one MCP/CLI door pair behind operator authority
(ruled_by/verify_ruling for a worker)"): `write_backup_settings` copies `charter_for`'s
own authority shape (charter.py) — an operator actor writes freely; anyone else must cite
a standing ruling `verify_ruling` confirms actually names this write. Unlike `charter_for`
there is no seat-manager branch: a backup schedule is infra config, not a per-seat
authority relationship, so the only two doors are "the operator's own hand" and "a ruling
that says so" — never a third party's implied delegation."""
from __future__ import annotations

from typing import Any

import asyncpg

# THE FIVE BACKUP-LANE TIMERS (compositions.py's own `backup_status` Function reads the
# SAME list — imported from here, not redefined, so "which units exist" has one answer).
BACKUP_TIMER_UNITS: tuple[str, ...] = (
    "osiris-backup.timer",
    "osiris-base-backup.timer",
    "osiris-prune-manifest.timer",
    "osiris-prune-apply.timer",
    "osiris-preflight.timer",
)

# the only fields a writer may set — vault_path (the local disk/NAS-mounted primary
# target, ruling on piece-3 scope question (4): ONE path, off-box modeled separately),
# timer_schedules (an OnCalendar= override per unit, validated against
# BACKUP_TIMER_UNITS), offbox_repositories (a list of {url, schedule, enabled} —
# scope question (4)'s ruling: ships empty, the panel shows it held, since off-box
# itself stays unwired pending the operator's own ruling on cf134938).
_ALLOWED = ("vault_path", "timer_schedules", "offbox_repositories")
_EMPTY: dict[str, Any] = {
    "id": "default", "rev": 0, "updated_by": "operator", "vault_path": None,
    "timer_schedules": {}, "offbox_repositories": [],
}


def _row(row: asyncpg.Record | None) -> dict[str, Any]:
    if row is None:
        return dict(_EMPTY)
    d = dict(row)
    for k in ("timer_schedules", "offbox_repositories"):
        if isinstance(d.get(k), str):  # jsonb round-trips as text via some drivers
            import json

            d[k] = json.loads(d[k])
    if d.get("updated_at") is not None:
        d["updated_at"] = d["updated_at"].isoformat()
    return d


async def get_backup_settings(pool: asyncpg.Pool) -> dict[str, Any]:
    """The current settings — what the panel's write controls should show as their own
    starting values (the read half, `backup_status`, shows LIVE facts; this shows what
    the operator has actually SET, which may not have taken effect yet if deploy hasn't
    re-run)."""
    return _row(await pool.fetchrow("SELECT * FROM backup_settings WHERE id='default'"))


def _validate(fields: dict[str, Any]) -> str | None:
    """Returns an error string, or None if every given field is well-shaped. Syntax
    validation stops at "is this the right TYPE of value" — an actually malformed
    OnCalendar= expression is caught by systemd itself at daemon-reload, same as it
    always has been; re-implementing systemd's own calendar-spec grammar here would be
    a second, drifting copy of a grammar we don't own."""
    if "vault_path" in fields:
        vp = fields["vault_path"]
        if vp is not None and (not isinstance(vp, str) or not vp.startswith("/")):
            return "vault_path must be an absolute filesystem path (or null to unset)"
    if "timer_schedules" in fields:
        sched = fields["timer_schedules"]
        if not isinstance(sched, dict):
            return "timer_schedules must be an object mapping unit name -> OnCalendar="
        for unit, cal in sched.items():
            if unit not in BACKUP_TIMER_UNITS:
                return (f"unknown timer unit {unit!r} — must be one of "
                       f"{', '.join(BACKUP_TIMER_UNITS)}")
            if not isinstance(cal, str) or not cal.strip():
                return f"timer_schedules[{unit!r}] must be a non-empty OnCalendar= string"
    if "offbox_repositories" in fields:
        repos = fields["offbox_repositories"]
        if not isinstance(repos, list):
            return "offbox_repositories must be a list"
        for i, r in enumerate(repos):
            if not isinstance(r, dict) or not isinstance(r.get("url"), str) or not r["url"]:
                return f"offbox_repositories[{i}] needs a non-empty string 'url'"
            if not isinstance(r.get("enabled"), bool):
                return f"offbox_repositories[{i}] needs a boolean 'enabled'"
    return None


async def set_backup_settings(pool: asyncpg.Pool, *, by: str, **fields: Any) -> dict[str, Any]:
    """Partial-update the singleton (only the given fields), bump `rev`, stamp who wrote
    it — `console_state.set_console`'s own upsert shape, unchanged. Unknown fields are
    rejected; every given field is shape-validated by `_validate` before the write. This
    is the INNER primitive — no authority check here (that's `write_backup_settings`'s
    own job, same split `set_charter`/`charter_for` already keep: an unguarded inner
    write, a guarded outer door)."""
    import json

    err = _validate(fields)
    if err:
        return {"error": err}
    cols, vals, args = ["id", "updated_by", "rev"], ["'default'", "$1", "1"], [by]
    updates = ["updated_by=excluded.updated_by", "rev=backup_settings.rev+1",
              "updated_at=now()"]
    for k, v in fields.items():
        if k not in _ALLOWED:
            return {"error": f"unknown backup_settings field: {k!r}"}
        args.append(json.dumps(v) if k in ("timer_schedules", "offbox_repositories") else v)
        cols.append(k)
        vals.append(f"${len(args)}" + ("::jsonb" if k in
                                       ("timer_schedules", "offbox_repositories") else ""))
        updates.append(f"{k}=excluded.{k}")
    query = (f"INSERT INTO backup_settings ({', '.join(cols)}) VALUES ({', '.join(vals)}) "
             f"ON CONFLICT (id) DO UPDATE SET {', '.join(updates)} RETURNING *")
    return _row(await pool.fetchrow(query, *args))


async def write_backup_settings(
    pool: asyncpg.Pool, *, actor: str, because: str, ruling: str | None = None,
    **fields: Any,
) -> dict[str, Any]:
    """THE WRITE DOOR (Thoth dispatch 9870/9895) — `charter_for`'s own authority shape
    (charter.py), minus the seat-manager branch (there is no seat this setting belongs
    to): `actor` is either one of `seats._OPERATOR_ACTORS`'s sentinels, or `ruling`
    names a standing operator ruling `verify_ruling` confirms actually authorizes THIS
    write (write_name='backup_settings'). `because` is required — same testimony
    discipline `charter_for` runs: changing what backs up the whole graph up is a
    deliberate act, not a routine one."""
    from src.orchestrator.capture import verify_ruling
    from src.orchestrator.seats import _OPERATOR_ACTORS

    because = (because or "").strip()
    if not because:
        return {"error": "because is required — changing backup settings is testimony, "
                         "same discipline charter_for runs"}
    if actor not in _OPERATOR_ACTORS:
        if not ruling:
            return {"error": f"{actor!r} is not an operator actor — cite a standing "
                             "ruling (that names 'backup_settings') via `ruling=`, or "
                             "have the operator make this change directly"}
        check = await verify_ruling(pool, ruling, write_name="backup_settings")
        if not check["ok"]:
            return {"error": check["error"]}
    result = await set_backup_settings(pool, by=actor, **fields)
    if not result.get("error"):
        result["because"] = because
    return result

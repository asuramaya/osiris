"""THE SETTINGS REGISTRY's own SERVICE layer (THE SETTINGS MENU, ruling be1b2e47, thread
f4498ab304e4 piece 1, Thoth's GO mail 10040) — the VALUES half over `src/config/
settings_registry.py`'s own DECLARATIONS, generalizing `backup_settings.py`'s singleton-
table + charter_for-shaped door into the one `settings` table (migration 0067) every
future knob writes into without its own migration.

THE OVERLAY IS THE RISK (Thoth's own words, mail 10040) — `settings_with_overlay` is
OPT-IN PER FIELD: it only ever overrides a `Settings` attribute named by some
`SettingSpec.env_field`, never a blanket rewrite of `get_settings()` itself (every one of
its 250+ existing call sites is untouched and behaves exactly as before unless a caller
explicitly switches to this function). One DB read per short TTL (`_OVERLAY_TTL_SECS`),
fails open to the env/pydantic default on ANY error (a DB hiccup must never sink a cron),
and is never consulted on paths that run before a DB exists (migrations, soul-key init,
deploy's own preflight — those keep calling bare `get_settings()`, unchanged)."""
from __future__ import annotations

import json
import time
from typing import Any

import asyncpg

from src.config.settings import Settings
from src.config.settings_registry import SETTINGS, SettingSpec, spec_by_key

_OVERLAY_TTL_SECS = 30.0
_overlay_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _row_value(raw: Any) -> Any:
    """jsonb round-trips as text via this driver setup (backup_settings.py's own
    `_row` helper carries the identical cast) — never assume the wire type."""
    return json.loads(raw) if isinstance(raw, str) else raw


async def _overlay_map(pool: asyncpg.Pool) -> dict[str, Any]:
    """Every box-scope `settings` row, cached for `_OVERLAY_TTL_SECS` — ONE query no
    matter how many registered keys have `effect='immediate'`, not one query per key.
    Fails open (empty map, meaning "nothing overridden") on any read error."""
    now = time.monotonic()
    cached = _overlay_cache.get("box")
    if cached is not None and now - cached[0] < _OVERLAY_TTL_SECS:
        return cached[1]
    try:
        rows = await pool.fetch("SELECT key, value FROM settings WHERE scope='box' AND scope_id=''")
    except Exception:  # noqa: BLE001 — fails open, never blocks a caller on a DB hiccup
        return {}
    m = {r["key"]: _row_value(r["value"]) for r in rows}
    _overlay_cache["box"] = (now, m)
    return m


async def settings_with_overlay(pool: asyncpg.Pool) -> Settings:
    """`get_settings()`, with every REGISTERED `effect='immediate'` key's own current
    `settings` value substituted in when one has been written — env/pydantic default
    otherwise. Callers that want a registered daemon switch to be genuinely live (no
    restart) call this instead of bare `get_settings()`; every other call site is
    unaffected, by construction (opt-in per field).

    CALL-TIME IMPORT, DELIBERATE (this codebase's own established convention — see any
    arq_worker.py heartbeat): a module-level `from ... import get_settings` binds the
    name ONCE at import time, permanently immune to a test's own
    `monkeypatch.setattr(settings_mod, "get_settings", ...)` on the SOURCE module's
    attribute. Importing here, inside the function, re-reads the current attribute on
    every call instead."""
    from src.config.settings import get_settings

    base = get_settings()
    overlay = await _overlay_map(pool)
    if not overlay:
        return base
    overrides = {
        spec.env_field: overlay[spec.key]
        for spec in SETTINGS
        # secret_ref excluded on purpose (SECRETS ROTATE ACT, thread f4498ab304e4's own
        # follow-up): its stored `settings` row is a bare {"rotated": true} MARKER, never
        # the real value (rotate_secret writes the real value to spec.backing_file
        # instead) — substituting that marker onto a str-typed Settings field would
        # silently corrupt it, so this filter is load-bearing, not defensive dead code.
        if spec.effect == "immediate" and spec.env_field and spec.key in overlay
        and spec.type != "secret_ref"
    }
    return base.model_copy(update=overrides) if overrides else base


async def current_stored_value(pool: asyncpg.Pool, key: str) -> Any | None:
    """THE CURRENT `settings` TABLE VALUE for ONE registered key, regardless of its own
    `effect` classification (thread bc6a5d455da2, REBOOT SURVIVAL's fleet half) —
    deliberately NOT routed through `settings_with_overlay`'s own opt-in-per-field
    'immediate' filter, which this module's own docstring names as A DELIBERATE RISK
    BOUNDARY (Thoth mail 10040: "THE OVERLAY IS THE RISK"), not an oversight to widen.

    The live specimen this exists for: `wake.trigger.enabled` is registered `effect=
    'next_tick'` (settings_registry.py), so `settings_with_overlay` never substitutes
    it — correct for that filter's own stated purpose, but it leaves NO caller any way
    to read the operator's actual CURRENT stored toggle across a process boundary that
    lacks the worker's own environment drop-in (a bare CLI invocation run from an
    interactive shell, during an MCP outage — exactly the trigger-dark false-negative
    Thoth's mail 10225 diagnosed). This function is that narrow escape hatch: ONE named
    key, read straight from the stored overlay map, no `effect` filtering at all — never
    call it in a loop over many keys (use `settings_with_overlay` for that; this pays
    the same `_overlay_map` cache, just skips its own field-selection step for callers
    that already know exactly which one key they need). Returns None when nothing has
    been written for `key` (the caller's own job to fall back to the env/pydantic
    default), or when `key` is not a registered spec at all."""
    if spec_by_key(key) is None:
        return None
    overlay = await _overlay_map(pool)
    return overlay.get(key)


def _invalidate_overlay_cache() -> None:
    """Called by `write_setting` so a write is visible on the very next read, never
    stale for up to `_OVERLAY_TTL_SECS`; also the test-fixture reset — a module-level
    cache would otherwise leak a value written by one test into another sharing the
    same xdist worker within the TTL window."""
    _overlay_cache.clear()


def _current_value(spec: SettingSpec, stored: Any) -> Any:
    if spec.type == "secret_ref":
        return {"set": stored is not None}
    return stored if stored is not None else spec.default


async def _restart_unit_env_value(unit: str, env_field: str, spec_type: str) -> Any:
    """Best-effort LIVE value off the RUNNING unit's own environment (`systemctl --user
    show ... -p Environment`) — 'unavailable, not fabricated' the moment this isn't
    running on a box with that unit installed (CI, a dev worktree, systemd absent
    entirely), the same law compositions.py's own `_backup_timer_live_state` holds.
    Bounded subprocess timeout; any error at all degrades to None, never raises."""
    import asyncio

    try:
        proc = await asyncio.create_subprocess_exec(
            "systemctl", "--user", "show", f"{unit}.service", "-p", "Environment",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=5.0)
    except (OSError, TimeoutError):
        return None
    prefix = f"{env_field.upper()}="
    for line in out.decode(errors="replace").splitlines():
        if not line.startswith("Environment="):
            continue
        for token in line.removeprefix("Environment=").split():
            if not token.startswith(prefix):
                continue
            raw = token.removeprefix(prefix)
            if spec_type == "bool":
                return raw.strip().lower() in ("1", "true", "yes", "on")
            if spec_type == "int":
                try:
                    return int(raw)
                except ValueError:
                    return None
            if spec_type == "float":
                try:
                    return float(raw)
                except ValueError:
                    return None
            return raw
    return None


def _rendered_backup_timer_value(key: str) -> Any:
    """The actually-shipped `deploy/<unit>` file's own `OnCalendar=` line — generalizing
    compositions.py's own `_backup_timer_calendar` (same read, same file) for the
    registry's `backup.timer_schedule.<unit>` keys. None when the key names anything
    else (no rendered-file counterpart exists at all, e.g. `backup.vault_path` — never
    a guess) or the file doesn't carry that line."""
    prefix = "backup.timer_schedule."
    if not key.startswith(prefix):
        return None
    from src.orchestrator.compositions import _backup_timer_calendar

    return _backup_timer_calendar(key.removeprefix(prefix))


async def live_value(pool: asyncpg.Pool, spec: SettingSpec) -> Any:
    """THE RUNNING/SHIPPED value beside the STORED one (Thoth's mail 10111, thread
    c5ba8681) — null whenever a cheap read doesn't exist, never fabricated. See the
    scope note annotated on c5ba8681 for the full per-effect rationale:
    'immediate' reads the same cached overlay list/get already pays for; 'restart:<unit>'
    polls the running unit's own environment; 'next_deploy' reads the shipped file;
    anything else (a secret, or 'next_tick') has no cheap live source and stays null."""
    if spec.type == "secret_ref":
        return None
    if spec.effect == "immediate":
        if not spec.env_field:
            return None
        try:
            base = await settings_with_overlay(pool)
        except Exception:  # noqa: BLE001 — a live extra is a nice-to-have, never a crash
            return None
        return getattr(base, spec.env_field, None)
    if spec.effect.startswith("restart:"):
        if not spec.env_field:
            return None
        unit = spec.effect.split(":", 1)[1]
        return await _restart_unit_env_value(unit, spec.env_field, spec.type)
    if spec.effect == "next_deploy":
        return _rendered_backup_timer_value(spec.key)
    return None


async def list_settings(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Every declared SettingSpec's own metadata plus its current stored value (falling
    back to the spec's default when unset) — what a menu renders from, so a new knob
    added to SETTINGS appears with zero frontend change. Secrets never carry a real
    value here, only presence. `live` (null when not cheap to compute) is the
    running/shipped counterpart — see `live_value`'s own docstring."""
    try:
        rows = await pool.fetch("SELECT key, value FROM settings WHERE scope='box' AND scope_id=''")
        stored = {r["key"]: _row_value(r["value"]) for r in rows}
    except Exception:  # noqa: BLE001 — a list read degrades to defaults, never crashes
        stored = {}
    return [
        {
            "key": spec.key, "type": spec.type, "default": spec.default, "scope": spec.scope,
            "effect": spec.effect, "choices": list(spec.choices) if spec.choices else None,
            "item_shape": spec.item_shape, "authority": spec.authority,
            "requires_because": spec.requires_because, "consequence": spec.consequence,
            "value": _current_value(spec, stored.get(spec.key)),
            "live": await live_value(pool, spec),
        }
        for spec in SETTINGS
    ]


async def get_setting(pool: asyncpg.Pool, key: str) -> dict[str, Any]:
    """One key's own current value plus its `live` counterpart (null when not cheap —
    see `live_value`). `{"error": ...}` when the key is not registered — this door only
    ever answers for a declared knob, never an arbitrary string."""
    spec = spec_by_key(key)
    if spec is None:
        return {"error": f"unknown setting key: {key!r} — not in the registry"}
    row = await pool.fetchrow(
        "SELECT value FROM settings WHERE key=$1 AND scope='box' AND scope_id=''", key)
    stored = _row_value(row["value"]) if row is not None else None
    return {"key": key, "value": _current_value(spec, stored), "live": await live_value(pool, spec)}


def _validate_value(spec: SettingSpec, value: Any) -> dict[str, str] | None:
    """Generic, type-driven validation — one function for every knob's own shape,
    never a bespoke per-key check (backup_settings.py's own `_validate` is exactly the
    per-feature duplication this generalizes away). Returns a structured
    {field, message} error, or None when the value is well-shaped for its type."""
    t = spec.type
    if t == "bool" and not isinstance(value, bool):
        return {"field": spec.key, "message": "must be a boolean"}
    if t == "int" and not (isinstance(value, int) and not isinstance(value, bool)):
        return {"field": spec.key, "message": "must be an integer"}
    if t in ("float",) and not (isinstance(value, (int, float)) and not isinstance(value, bool)):
        return {"field": spec.key, "message": "must be a number"}
    if t == "str" and not isinstance(value, str):
        return {"field": spec.key, "message": "must be a string"}
    # 'path'/'schedule' both accept None meaning "no override, use the shipped default"
    # (THE SETTINGS MENU piece 3, thread 7eb26f68) — the only way to CLEAR a real infra
    # path or schedule back off, not just overwrite it with another one.
    if t == "path" and value is not None and not isinstance(value, str):
        return {"field": spec.key, "message": "must be a string, or null to unset"}
    if t == "path" and isinstance(value, str) and not value.startswith("/"):
        return {"field": spec.key, "message": "must be an absolute filesystem path"}
    if t == "schedule" and value is not None and not (isinstance(value, str) and value.strip()):
        return {"field": spec.key,
                "message": "must be a non-empty OnCalendar= expression, or null to unset"}
    if t == "enum":
        if not spec.choices:
            return {"field": spec.key, "message": "spec declares type='enum' with no choices"}
        if value not in spec.choices:
            return {"field": spec.key,
                    "message": f"must be one of {', '.join(spec.choices)}"}
    if t == "records":
        if not isinstance(value, list):
            return {"field": spec.key, "message": "must be a list"}
        shape = spec.item_shape or {}
        for i, item in enumerate(value):
            if not isinstance(item, dict):
                return {"field": f"{spec.key}[{i}]", "message": "each item must be an object"}
            for fname, ftype in shape.items():
                if fname not in item:
                    continue
                fval = item[fname]
                if ftype == "bool" and not isinstance(fval, bool):
                    return {"field": f"{spec.key}[{i}].{fname}", "message": "must be a boolean"}
                if ftype == "str" and not isinstance(fval, str):
                    return {"field": f"{spec.key}[{i}].{fname}", "message": "must be a string"}
    if t == "json" and not isinstance(value, (dict, list)):
        return {"field": spec.key, "message": "must be a JSON object or array"}
    if spec.validate is not None:
        err = spec.validate(value)
        if err:
            return {"field": spec.key, "message": err}
    return None


def _write_env_file_line(path: str, key: str, value: str) -> None:
    """Rewrite (or append) one KEY=value line in a flat EnvironmentFile= — the exact
    shape systemd's own EnvironmentFile= consumes (deploy/osiris.env.example), never a
    full re-serialization that could reorder or clobber unrelated lines/comments.
    Creates the file (and its parent directory) with 0600 perms if it doesn't exist yet
    — a secret, never world- or group-readable."""
    import os
    from pathlib import Path

    p = Path(path)
    lines = p.read_text().splitlines() if p.exists() else []
    prefix = f"{key}="
    new_line = f"{key}={value}"
    for i, line in enumerate(lines):
        if line.startswith(prefix):
            lines[i] = new_line
            break
    else:
        lines.append(new_line)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(lines) + "\n")
    os.chmod(p, 0o600)


async def _rotate_secret(
    pool: asyncpg.Pool, spec: SettingSpec, value: Any, *, actor: str, because: str,
    scope_id: str,
) -> dict[str, Any]:
    """THE SECRETS ROTATE ACT (thread f4498ab304e4's own follow-up, Thoth mail 10441):
    `write_setting`'s own secret_ref branch — a write on a secret_ref key IS a rotate,
    the same door, no second action to learn. The real value goes into `spec`'s own
    `backing_file` (0600, a KEY=value line, `env_field.upper()` as the key) — NEVER the
    `settings` table and NEVER echoed back in the receipt. Only a bare `{"rotated":
    true}` marker lands in the table (rev-bumped the same way every other write is),
    preserving `list_settings`/`get_setting`'s own pre-existing `{"set": stored is not
    None}` bookkeeping (`_current_value`) without the table ever holding the real
    value."""
    if not isinstance(value, str) or not value.strip():
        return {"error": "must be a non-empty string",
                "errors": [{"field": spec.key, "message": "must be a non-empty string"}]}
    if not spec.backing_file or not spec.env_field:
        return {"error": f"{spec.key!r} has no backing_file/env_field declared — "
                         "cannot rotate"}
    try:
        _write_env_file_line(spec.backing_file, spec.env_field.upper(), value)
    except OSError as exc:
        return {"error": f"could not write {spec.backing_file}: {exc}"}
    row = await pool.fetchrow(
        "INSERT INTO settings (key, scope, scope_id, value, updated_by, rev) "
        "VALUES ($1, $2, $3, 'true'::jsonb, $4, 1) "
        "ON CONFLICT (key, scope, scope_id) DO UPDATE SET "
        "  value='true'::jsonb, updated_by=excluded.updated_by, "
        "  rev=settings.rev+1, updated_at=now() "
        "RETURNING rev",
        spec.key, spec.scope, scope_id, actor)
    _invalidate_overlay_cache()
    result: dict[str, Any] = {
        "key": spec.key, "rotated": True, "rev": row["rev"], "because": because,
        "effect": spec.effect,
    }
    if spec.effect.startswith("restart:"):
        unit = spec.effect.split(":", 1)[1]
        result["note"] = f"takes effect on {unit}'s next restart, not automatically"
    elif spec.effect == "next_deploy":
        result["note"] = "takes effect on the next `osiris deploy`, not automatically"
    return result


async def _authorized(
    pool: asyncpg.Pool, spec: SettingSpec, *, actor: str, scope_id: str, ruling: str | None,
) -> str | None:
    """The authority ENUM's own three branches, each reusing an EXISTING check
    (`charter_for`'s own shape, charter.py) — never a new mechanism per knob. Returns an
    error string, or None when authorized."""
    from src.orchestrator.seats import _OPERATOR_ACTORS

    if actor in _OPERATOR_ACTORS:
        return None
    if spec.authority == "operator_or_manager" and spec.scope == "seat" and scope_id:
        from src.orchestrator.seats import held_seat, manager_of_seat

        caller_seat = (await held_seat(pool, actor) or {}).get("seat_id")
        if caller_seat and await manager_of_seat(pool, scope_id) == caller_seat:
            return None
    if spec.authority in ("operator_or_ruling", "operator_or_manager") and ruling:
        from src.orchestrator.capture import verify_ruling

        check = await verify_ruling(pool, ruling, write_name=spec.write_name or spec.key)
        return None if check["ok"] else check["error"]
    hint = ("a manager of the target seat, " if spec.authority == "operator_or_manager" else "")
    return (f"{actor!r} is not an operator actor — this setting requires {hint}"
            "citing a standing ruling via `ruling=`, or the operator making this "
            "change directly")


async def write_setting(
    pool: asyncpg.Pool, key: str, value: Any, *, actor: str, because: str = "",
    scope_id: str = "", ruling: str | None = None,
) -> dict[str, Any]:
    """THE WRITE DOOR — `backup_settings.write_backup_settings`'s own authority shape,
    generalized over the registry rather than one hardcoded field set. Returns
    `{"error": ..., "errors": [{"field","message"}, ...]}` on any refusal (structured,
    Seshat's fold 5, never one bare string for a menu to show per-field), or
    `{"key","value","rev"}` on success — EXCEPT a `type='secret_ref'` key, where a
    write IS a rotate (`_rotate_secret`, SECRETS ROTATE ACT, thread f4498ab304e4's own
    follow-up): `{"key","rotated":true,"rev",...}`, never `value` — the real secret
    goes into the spec's own backing file, never this table, never echoed back."""
    spec = spec_by_key(key)
    if spec is None:
        return {"error": f"unknown setting key: {key!r} — not in the registry"}
    because = (because or "").strip()
    if spec.requires_because and not because:
        return {"error": "because is required — changing this setting is testimony, "
                         "same discipline every operator-authority write in this house runs"}
    auth_error = await _authorized(pool, spec, actor=actor, scope_id=scope_id, ruling=ruling)
    if auth_error:
        return {"error": auth_error}
    if spec.type == "secret_ref":
        return await _rotate_secret(
            pool, spec, value, actor=actor, because=because, scope_id=scope_id)
    field_error = _validate_value(spec, value)
    if field_error:
        return {"error": field_error["message"], "errors": [field_error]}
    row = await pool.fetchrow(
        "INSERT INTO settings (key, scope, scope_id, value, updated_by, rev) "
        "VALUES ($1, $2, $3, $4::jsonb, $5, 1) "
        "ON CONFLICT (key, scope, scope_id) DO UPDATE SET "
        "  value=excluded.value, updated_by=excluded.updated_by, "
        "  rev=settings.rev+1, updated_at=now() "
        "RETURNING value, rev",
        key, spec.scope, scope_id, json.dumps(value), actor)
    _invalidate_overlay_cache()
    result: dict[str, Any] = {
        "key": key, "value": _row_value(row["value"]), "rev": row["rev"], "because": because,
        "effect": spec.effect,
    }
    if spec.effect.startswith("restart:"):
        unit = spec.effect.split(":", 1)[1]
        result["note"] = f"takes effect on {unit}'s next restart, not automatically"
    elif spec.effect == "next_deploy":
        result["note"] = "takes effect on the next `osiris deploy`, not automatically"
    return result

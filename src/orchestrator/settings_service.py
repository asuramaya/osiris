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
and is never consulted on paths that run before a DB exists (migrations, soul-key-init,
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
        if spec.effect == "immediate" and spec.env_field and spec.key in overlay
    }
    return base.model_copy(update=overrides) if overrides else base


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


async def list_settings(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Every declared SettingSpec's own metadata plus its current stored value (falling
    back to the spec's default when unset) — what a menu renders from, so a new knob
    added to SETTINGS appears with zero frontend change. Secrets never carry a real
    value here, only presence."""
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
        }
        for spec in SETTINGS
    ]


async def get_setting(pool: asyncpg.Pool, key: str) -> dict[str, Any]:
    """One key's own current value. `{"error": ...}` when the key is not registered —
    this door only ever answers for a declared knob, never an arbitrary string."""
    spec = spec_by_key(key)
    if spec is None:
        return {"error": f"unknown setting key: {key!r} — not in the registry"}
    row = await pool.fetchrow(
        "SELECT value FROM settings WHERE key=$1 AND scope='box' AND scope_id=''", key)
    stored = _row_value(row["value"]) if row is not None else None
    return {"key": key, "value": _current_value(spec, stored)}


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
    if t in ("str", "path") and not isinstance(value, str):
        return {"field": spec.key, "message": "must be a string"}
    if t == "path" and isinstance(value, str) and not value.startswith("/"):
        return {"field": spec.key, "message": "must be an absolute filesystem path"}
    if t == "schedule" and not (isinstance(value, str) and value.strip()):
        return {"field": spec.key, "message": "must be a non-empty OnCalendar= expression"}
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
    `{"key","value","rev"}` on success."""
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
        return {"error": "secret_ref settings are never written through this door — "
                         "rotating a secret is its own act, not yet built (thread "
                         "f4498ab304e4's own follow-up)"}
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

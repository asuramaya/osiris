"""THE SETTINGS REGISTRY (THE SETTINGS MENU, ruling be1b2e47, thread f4498ab304e4 piece 1,
Thoth's GO mail 10040) — the DECLARATIONS half of the substrate, ontology/schema.py's own
pattern (a tuple of dataclass instances, not a DB table) applied to configuration instead
of graph entities.

WHY DECLARATIONS LIVE IN PYTHON, NOT THE `settings` TABLE (migration 0067): that table
holds VALUES only — a row need not exist for a knob to have a default. Every menu, CLI, and
write door renders and validates against THIS tuple, never a hand-built form or bespoke
per-key code (`backup_settings.py`'s own `_ALLOWED`/`_validate` are exactly what a generic
registry replaces — one row's worth of declaration instead of a parallel implementation
per feature).

FIELD VOCABULARY (Seshat's fold 1, thread b095c53d, backup panel lessons): `type` names
what the MENU renders, not just a Python type — 'path'/'schedule'(OnCalendar=)/'enum'/
'records' (a list of structured records with a declared `item_shape`) alongside the plain
str/int/float/bool/json/secret_ref shapes, so a new field type needs exactly one new
renderer in piece 2, never a hand-built form per knob.

AUTHORITY (Seshat's fold 2): an ENUM over `charter_for`'s own branches (charter.py),
never a new mechanism — 'operator' (only an `_OPERATOR_ACTORS` sentinel may write),
'operator_or_manager' (a seat-scoped knob: the operator, or the target seat's own
manager), 'operator_or_ruling' (the operator, or any caller citing a standing ruling
`verify_ruling` confirms names this spec's own `write_name`) — `backup_settings.py`'s
own shape is exactly 'operator_or_ruling', generalized here rather than reinvented.

`requires_because`/`consequence` (Seshat's folds 3-4): backup_settings required
`because` on every save unconditionally, including a one-character schedule tweak — too
heavy for a low-stakes toggle. Each spec now opts in/out and names its own blast radius,
so the menu (piece 2) can show a confirm-on-save dialog only where `consequence='high'`.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

SettingType = Literal[
    "str", "int", "float", "bool", "enum", "json", "secret_ref", "path", "schedule", "records",
]
SettingScope = Literal["box", "project", "seat"]
Authority = Literal["operator", "operator_or_manager", "operator_or_ruling"]


@dataclass(frozen=True)
class SettingSpec:
    """One knob's own declaration. `key` is a dotted name (e.g. 'backup.vault_path'),
    never a bare settings.py attribute name — the registry's own namespace, distinct
    from (and, where `env_field` is set, mapped onto) an existing Settings field.

    `env_field`: when set, names the `Settings` attribute this key overlays — required
    for `effect='immediate'` (the overlay, `settings_service.settings_with_overlay`,
    only ever touches registered `env_field`s, opt-in per field, never a blanket
    override). A key with no `env_field` has no live-env counterpart at all (a genuinely
    new knob whose value ONLY ever lives in the `settings` table, e.g. `backup.*`).

    `write_name`: the string `verify_ruling` must find in a citing ruling's own text;
    defaults to `key` when unset — a spec need not repeat its own name.

    `item_shape`: for `type='records'` only — {field_name: SettingType} describing one
    record's own shape (e.g. backup's `offbox_repositories`: {'url': 'str',
    'schedule': 'schedule', 'enabled': 'bool'}); the generic `records` validator checks
    every item against this shape rather than a bespoke per-key loop."""

    key: str
    type: SettingType
    default: Any
    scope: SettingScope = "box"
    effect: str = "immediate"  # 'immediate' | 'next_tick' | 'restart:<unit>' | 'next_deploy'
    choices: tuple[str, ...] | None = None
    item_shape: dict[str, SettingType] | None = None
    validate: Callable[[Any], str | None] | None = None
    authority: Authority = "operator_or_ruling"
    requires_because: bool = True
    consequence: Literal["low", "high"] = "low"
    write_name: str | None = None
    env_field: str | None = None


_DAEMON_KILL_SWITCHES: tuple[SettingSpec, ...] = (
    SettingSpec("daemon.pit_watch.enabled", "bool", False, effect="immediate",
               consequence="low", requires_because=False,
               env_field="osiris_pit_watch_enabled"),
    SettingSpec("daemon.fleet_reconcile.enabled", "bool", False, effect="immediate",
               consequence="high", requires_because=True,
               env_field="osiris_fleet_reconcile_enabled"),
    SettingSpec("daemon.closure_miner.enabled", "bool", False, effect="immediate",
               consequence="high", requires_because=True,
               env_field="osiris_closure_miner_enabled"),
    SettingSpec("daemon.phantom_heal.enabled", "bool", False, effect="immediate",
               consequence="high", requires_because=True,
               env_field="osiris_phantom_heal_enabled"),
    SettingSpec("daemon.phantom_fold_reap.enabled", "bool", False, effect="immediate",
               consequence="high", requires_because=True,
               env_field="osiris_phantom_fold_reap_enabled"),
    SettingSpec("daemon.tree_ingest_alarm.enabled", "bool", False, effect="immediate",
               consequence="low", requires_because=False,
               env_field="osiris_tree_ingest_alarm_enabled"),
    SettingSpec("daemon.landing_audit.enabled", "bool", False, effect="immediate",
               consequence="low", requires_because=False,
               env_field="osiris_landing_audit_enabled"),
    SettingSpec("daemon.obligation_hygiene.enabled", "bool", True, effect="immediate",
               consequence="low", requires_because=False,
               env_field="osiris_obligation_hygiene_enabled"),
    SettingSpec("daemon.no_regrow.enabled", "bool", True, effect="immediate",
               consequence="low", requires_because=False,
               env_field="osiris_no_regrow_enabled"),
    SettingSpec("daemon.retention_heartbeat.enabled", "bool", True, effect="immediate",
               consequence="high", requires_because=True,
               env_field="osiris_retention_heartbeat_enabled"),
    SettingSpec("daemon.soul_cold_tier.enabled", "bool", True, effect="immediate",
               consequence="high", requires_because=True,
               env_field="osiris_soul_cold_tier_enabled"),
    SettingSpec("miner.abstention.enabled", "bool", True, effect="immediate",
               consequence="low", requires_because=False,
               env_field="osiris_abstention_miner_enabled"),
)

_MINER_BUDGETS: tuple[SettingSpec, ...] = (
    SettingSpec("miner.daily_budget_base", "int", 5, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_miner_daily_budget_base"),
    SettingSpec("miner.new_pair_starter_budget", "int", 1, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_miner_new_pair_starter_budget"),
    SettingSpec("miner.zero_acceptance_window_days", "int", 7, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_miner_zero_acceptance_window_days"),
)

SETTINGS: tuple[SettingSpec, ...] = _DAEMON_KILL_SWITCHES + _MINER_BUDGETS


_BY_KEY: dict[str, SettingSpec] = {s.key: s for s in SETTINGS}
assert len(_BY_KEY) == len(SETTINGS), "duplicate SettingSpec key in the registry"


def spec_by_key(key: str) -> SettingSpec | None:
    """The registry's own lookup — never a linear scan repeated at every call site."""
    return _BY_KEY.get(key)

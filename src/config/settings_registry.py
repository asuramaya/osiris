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

# THE FIVE BACKUP-LANE TIMERS (THE SETTINGS MENU piece 3, thread 7eb26f68, Thoth's GO mail
# 10084) — moved here from `src/orchestrator/backup_settings.py` (which re-exports this
# name so its own existing importers, compositions.py's `backup_status` and
# scripts/render_backup_timers.py, keep working unchanged) so the registry — the
# declarations layer — never has to import FROM the orchestrator layer to build its own
# SettingSpecs, only the reverse.
BACKUP_TIMER_UNITS: tuple[str, ...] = (
    "osiris-backup.timer",
    "osiris-base-backup.timer",
    "osiris-prune-manifest.timer",
    "osiris-prune-apply.timer",
    "osiris-preflight.timer",
)


def _validate_offbox_repositories(value: Any) -> str | None:
    """Restores the exact required-field check `backup_settings.py`'s own bespoke
    `_validate` used to run — the generic `records` validator (settings_service.py's
    `_validate_value`) only type-checks a field when it's PRESENT, never requires one,
    so a per-spec `validate` callable is where "url and enabled are mandatory" still
    lives, same division of labor the registry's own docstring describes."""
    if not isinstance(value, list):
        return None  # the generic type check already covers "must be a list"
    for i, r in enumerate(value):
        if not isinstance(r, dict):
            continue  # the generic check already covers "each item must be an object"
        if not isinstance(r.get("url"), str) or not r["url"]:
            return f"offbox_repositories[{i}] needs a non-empty string 'url'"
        if not isinstance(r.get("enabled"), bool):
            return f"offbox_repositories[{i}] needs a boolean 'enabled'"
    return None


# THE BACKUP CONFIG PANEL'S OWN FIELDS (THE SETTINGS MENU piece 3, thread 7eb26f68,
# Thoth's GO mail 10084/10094), folded in from `backup_settings.py`'s own singleton
# table (Wave 21, thread f04cce36 piece 3) — same authority shape that table's write
# door always had (`operator_or_ruling`, `write_name='backup_settings'`), generalized
# rather than reinvented. All three `effect='next_deploy'`: none of them take hold until
# `render_backup_timers.py` regenerates the shipped units on the next `osiris deploy`.
# `path`/`schedule` both accept `None` as a value (settings_service.py's
# `_validate_value`) meaning "no override" — how an operator clears one back to the
# shipped default, the same "clearing an input and saving drops it" UX the old panel had.
_BACKUP_SETTINGS: tuple[SettingSpec, ...] = (
    SettingSpec("backup.vault_path", "path", None, effect="next_deploy",
               authority="operator_or_ruling", requires_because=True,
               consequence="high", write_name="backup_settings"),
    *(
        SettingSpec(f"backup.timer_schedule.{unit}", "schedule", None,
                   effect="next_deploy", authority="operator_or_ruling",
                   requires_because=True, consequence="high",
                   write_name="backup_settings")
        for unit in BACKUP_TIMER_UNITS
    ),
    SettingSpec("backup.offbox_repositories", "records", [], effect="next_deploy",
               item_shape={"name": "str", "url": "str", "schedule": "schedule",
                          "enabled": "bool"},
               validate=_validate_offbox_repositories,
               authority="operator_or_ruling", requires_because=True,
               consequence="high", write_name="backup_settings"),
)

# THE WAKE/TRIGGER LADDER (Wave 22 piece 2, Census gap-list-1 #2, Thoth's dispatch mail
# 10111) — every env-only settings.py field src/orchestrator/trigger.py's own mail-wake
# machinery reads, ALL fresh per call (`st = settings or get_settings()`, the exact test
# seam trigger_mail_tick/dispatch_dm/wake_gate_preflight/wake_worker/launch_seat already
# use), never cached at process boot — so `effect='next_tick'` is honest here in the same
# sense it is for the miner budgets: a write is live on the very NEXT trigger_mail cron
# pass (arq_worker.py's own `trigger_mail` wrapper now threads
# `settings_with_overlay(actions.pool)` down through `trigger_mail_tick`'s own `st`,
# which is what every one of these fields is actually read from). Two knobs deliberately
# NOT included here: `osiris_lease_refuse` lives on a different process entirely (the
# manager daemon, manager/daemon.py's own `_lease_gate`) with no existing
# `settings_with_overlay` wiring anywhere in that module — a bigger, riskier first step
# than this pass's own scope, flagged as a follow-up rather than rushed. The
# `wake_worker`/`dispatch_dm` pass-through at trigger.py's own wake() tool path (line
# ~2721 passes the wake_worker's raw incoming `settings` param, not its resolved `st`)
# is a pre-existing inconsistency, harmless today (both resolve to the same bare read)
# but flagged, not touched, by this pass.
_WAKE_LADDER: tuple[SettingSpec, ...] = (
    SettingSpec("wake.trigger.enabled", "bool", False, effect="next_tick",
               consequence="high", requires_because=True,
               env_field="osiris_trigger_enabled"),
    SettingSpec("wake.trigger.projects", "str", "", effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_trigger_projects"),
    SettingSpec("wake.trigger.rate_cap", "int", 15, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_trigger_rate_cap"),
    SettingSpec("wake.trigger.window_secs", "int", 3600, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_trigger_window_secs"),
    SettingSpec("wake.trigger.grace_secs", "int", 300, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_trigger_grace_secs"),
    SettingSpec("wake.trigger.poke_only", "bool", False, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_trigger_poke_only"),
    SettingSpec("wake.mail_lease_secs", "int", 900, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_mail_lease_secs"),
    SettingSpec("wake.resume.ceiling_bytes", "int", 64_000_000, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_resume_ceiling_bytes"),
    SettingSpec("wake.resume.min_tail_bytes", "int", 200, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_resume_min_tail_bytes"),
    SettingSpec("wake.owner_live_secs", "int", 900, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_owner_live_secs"),
    SettingSpec("wake.model", "str", "", effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_wake_model"),
    SettingSpec("wake.poke_min_idle_secs", "int", 600, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_poke_min_idle_secs"),
    SettingSpec("wake.allowed_tools", "str", "mcp__osiris", effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_wake_allowed_tools"),
    SettingSpec("wake.hourly_budget", "int", 30, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_wake_hourly_budget"),
    SettingSpec("wake.message_attempts", "int", 3, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_wake_message_attempts"),
    SettingSpec("wake.dm_resume", "bool", True, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_dm_resume"),
    SettingSpec("wake.dm_active_secs", "int", 120, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_dm_active_secs"),
    SettingSpec("wake.seat_hourly_cap", "int", 6, effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_seat_wake_hourly_cap"),
    SettingSpec("wake.dm_resume_model", "str", "", effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_dm_resume_model"),
    SettingSpec("wake.launch_substrate", "str", "harness", effect="next_tick",
               consequence="low", requires_because=False,
               env_field="osiris_launch_substrate"),
    SettingSpec("wake.enabled", "bool", True, effect="next_tick",
               consequence="high", requires_because=True,
               env_field="osiris_wake_enabled"),
)

# DIAGNOSTICS (Wave 22 piece 2) — `osiris_memory_diag_enabled` gates the /diag/memory
# route (mcp_server.py); unlike the wake ladder above it has no "tick" at all (an HTTP
# route, not a cron), but IS read fresh on every call once wired to the overlay
# (mcp_server.py's diag_memory_route now calls settings_with_overlay), so 'immediate' —
# the same label the daemon kill-switches use — is the honest one, not 'next_tick'.
# `osiris_worker_boot_memtrace_enabled` is the opposite shape, confirmed by reading the
# ONE call site (arq_worker.py's own `startup()`): read exactly once at process boot and
# never again — the genuine 'restart:<unit>' case, the one this pass's own `live` feature
# (thread c5ba8681) needed a real registered example of to exercise that branch honestly.
_DIAGNOSTICS: tuple[SettingSpec, ...] = (
    SettingSpec("diag.memory_enabled", "bool", False, effect="immediate",
               consequence="low", requires_because=False,
               env_field="osiris_memory_diag_enabled"),
    SettingSpec("diag.worker_boot_memtrace.enabled", "bool", False,
               effect="restart:osiris-worker", consequence="low", requires_because=False,
               env_field="osiris_worker_boot_memtrace_enabled"),
)

SETTINGS: tuple[SettingSpec, ...] = (
    _DAEMON_KILL_SWITCHES + _MINER_BUDGETS + _BACKUP_SETTINGS + _WAKE_LADDER + _DIAGNOSTICS
)


_BY_KEY: dict[str, SettingSpec] = {s.key: s for s in SETTINGS}
assert len(_BY_KEY) == len(SETTINGS), "duplicate SettingSpec key in the registry"


def spec_by_key(key: str) -> SettingSpec | None:
    """The registry's own lookup — never a linear scan repeated at every call site."""
    return _BY_KEY.get(key)

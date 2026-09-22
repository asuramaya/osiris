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
from pathlib import Path
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
    every item against this shape rather than a bespoke per-key loop.

    `backing_file`: for `type='secret_ref'` only (SECRETS ROTATE ACT, thread
    f4498ab304e4's own follow-up, Thoth mail 10441) — the flat KEY=value file a rotate
    writes into (settings_service.py's `write_setting`, its own secret_ref branch),
    NEVER the `settings` table. `env_field` doubles here as the KEY= name within that
    file (uppercased) rather than a live `Settings` attribute to overlay — a secret's
    real value is never substituted into a live `Settings` object by
    `settings_with_overlay` (explicitly excluded there), only read by the consuming
    daemon at its own next boot, off the same file its systemd unit's own
    EnvironmentFile= already sources."""

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
    backing_file: str | None = None


# THE MANAGER OVERLAY'S OWN FIRST KNOB (SECRETS ROTATE ACT + MANAGER OVERLAY, thread
# f4498ab304e4's own follow-up, Thoth mail 10441): the WAKE LADDER's docstring above
# already named this exact gap — `osiris_lease_refuse` lives on manager/daemon.py's own
# `Manager._lease_gate`, a different process from the worker, with no existing
# `settings_with_overlay` wiring. That wiring now exists (`_lease_gate` calls
# `settings_with_overlay(self._pool)`, same pattern every arq_worker.py tick already
# uses) — 'immediate' is honest here for the same reason it is for the daemon kill
# switches above: read fresh on every lease attempt, never cached at process boot.
_MANAGER_OVERLAY: tuple[SettingSpec, ...] = (
    SettingSpec("daemon.lease_refuse.enabled", "bool", False, effect="immediate",
               consequence="high", requires_because=True,
               env_field="osiris_lease_refuse"),
)

# THE SECRETS ROTATE ACT'S OWN BACKING FILE (thread f4498ab304e4's follow-up, Thoth mail
# 10441): the SAME shared EnvironmentFile= every deployed unit already sources
# (deploy/*.service's own `EnvironmentFile=/etc/osiris/osiris.env`) — decision 0524d40e's
# proposed soul-key design independently reaches for this identical file (OSIRIS_SOUL_
# KEY_FILE) rather than a dedicated per-secret file, "no new deploy step" being the
# shared reasoning. Falls back to a dev-box `.env` at the repo root (this worktree has
# no /etc/osiris/osiris.env at all — confirmed absent, same gap cli.py's own bootstrap
# door names) so a rotate is exercisable here without writing into a system directory a
# dev box has no business touching.
SECRET_BACKING_FILE: str = (
    "/etc/osiris/osiris.env" if Path("/etc/osiris/osiris.env").exists()
    else str(Path(__file__).resolve().parents[2] / ".env")
)

# THE FIRST REAL secret_ref SPEC (SECRETS ROTATE ACT, thread f4498ab304e4's own
# follow-up, Thoth mail 10441): `secret_ref` has been a declared SettingType since THE
# SETTINGS MENU piece 1 with zero live specs exercising it — write_setting's own
# secret_ref branch was an unconditional refusal until this pass. `etherscan_api_key`
# (settings.py's own field, read fresh but from a STATIC os.environ snapshot by
# src/ingest/etherscan.py, never re-exported by a running process) is the one genuinely
# secret-shaped value already in this codebase with no registry entry at all —
# `effect='restart:osiris-worker'` because that snapshot only ever refreshes on the
# worker's own next boot, the exact "daemon that must restart" the write receipt names
# (settings_service.py's own existing `restart:<unit>` -> `result["note"]` convention,
# reused verbatim, not duplicated). `authority='operator'`: rotating a real external
# credential is not citable via a standing ruling the way a config knob is.
_SECRETS: tuple[SettingSpec, ...] = (
    # default="" (not None) matches Settings.etherscan_api_key's own field default —
    # test_settings_registry.py's test_every_default_matches_the_live_settings_default
    # checks every env_field-carrying spec against it, secrets included.
    SettingSpec("secrets.etherscan_api_key", "secret_ref", "",
               effect="restart:osiris-worker", authority="operator",
               consequence="high", requires_because=True,
               env_field="etherscan_api_key", backing_file=SECRET_BACKING_FILE),
)

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
# scripts/render_units.py, keep working unchanged) so the registry — the
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
    """DEPRECATED shape (see `_validate_offload_targets` below, THE BACKUP TOPOLOGY /
    INTERMITTENT TARGETS, Thoth mail 12812) — kept validating exactly as before so an
    old caller/UI that still writes this key keeps its existing contract; new writes
    belong on `backup.offload_targets` instead. Restores the exact required-field check
    `backup_settings.py`'s own bespoke `_validate` used to run — the generic `records`
    validator (settings_service.py's `_validate_value`) only type-checks a field when
    it's PRESENT, never requires one, so a per-spec `validate` callable is where "url
    and enabled are mandatory" still lives, same division of labor the registry's own
    docstring describes."""
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


def _validate_offload_targets(value: Any) -> str | None:
    """THE BACKUP TOPOLOGY / INTERMITTENT TARGETS (Thoth mail 12812, operator ruling
    be21384a): this box is a laptop — an 8 TB drive only when docked, a NAS only on
    Tailscale/LAN — so a target models WHERE it lives, not just a URL. `name` is the
    stable handle a runner/panel addresses it by (unique across the list); `kind` is
    'local' (a filesystem path under a mountpoint that comes and goes) or 'restic' (a
    restic repository URL, reachable or not regardless of anything mounted locally);
    `expected_mountpoint` is REQUIRED for 'local' (the presence check's own anchor,
    src/orchestrator/backup_validation.py) and must be absent/null for 'restic' (a
    restic URL names its own reachability, no local mountpoint to check); `schedule`
    is OnCalendar=-shaped, same convention as the five backup-lane timers;
    `path_or_url`/`enabled` are required on every kind. Shape only here — whether a
    'local' target's `expected_mountpoint` is ACTUALLY mounted right now, or a
    'restic' target's `path_or_url` has valid restic syntax, is `backup_validation.py`'s
    own job at write/read time, never this settings-shape gate's."""
    if not isinstance(value, list):
        return None  # the generic type check already covers "must be a list"
    names: set[str] = set()
    for i, t in enumerate(value):
        if not isinstance(t, dict):
            continue  # the generic check already covers "each item must be an object"
        name = t.get("name")
        if not isinstance(name, str) or not name:
            return f"offload_targets[{i}] needs a non-empty string 'name'"
        if name in names:
            return f"offload_targets[{i}] has duplicate name {name!r} — names must be unique"
        names.add(name)
        kind = t.get("kind")
        if kind not in ("local", "restic"):
            return f"offload_targets[{i}] ({name!r}) needs kind='local' or 'restic', got {kind!r}"
        if not isinstance(t.get("path_or_url"), str) or not t["path_or_url"]:
            return f"offload_targets[{i}] ({name!r}) needs a non-empty string 'path_or_url'"
        mountpoint = t.get("expected_mountpoint")
        if kind == "local":
            if not isinstance(mountpoint, str) or not mountpoint:
                return (f"offload_targets[{i}] ({name!r}) is kind='local' and needs a "
                        "non-empty string 'expected_mountpoint'")
        elif mountpoint is not None:
            return (f"offload_targets[{i}] ({name!r}) is kind='restic' — "
                    "'expected_mountpoint' must be absent or null, restic names its own "
                    "reachability")
        if not isinstance(t.get("schedule"), str) or not t["schedule"]:
            return f"offload_targets[{i}] ({name!r}) needs a non-empty string 'schedule'"
        if not isinstance(t.get("enabled"), bool):
            return f"offload_targets[{i}] ({name!r}) needs a boolean 'enabled'"
    return None


# THE BACKUP CONFIG PANEL'S OWN FIELDS (THE SETTINGS MENU piece 3, thread 7eb26f68,
# Thoth's GO mail 10084/10094), folded in from `backup_settings.py`'s own singleton
# table (Wave 21, thread f04cce36 piece 3) — same authority shape that table's write
# door always had (`operator_or_ruling`, `write_name='backup_settings'`), generalized
# rather than reinvented. All three `effect='next_deploy'`: none of them take hold until
# `scripts/render_units.py` regenerates the shipped units on the next `osiris deploy`.
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
    # REPLACES offbox_repositories (THE BACKUP TOPOLOGY / INTERMITTENT TARGETS, Thoth
    # mail 12812) — offbox_repositories itself is left in the registry, unused by any
    # new write path, so old rows stay readable (get_backup_settings synthesizes
    # offload_targets from them on read when this key has never been set); dropping
    # the old spec entirely is its own separate act this fold doesn't make.
    SettingSpec("backup.offload_targets", "records", [], effect="next_deploy",
               item_shape={"name": "str", "kind": "enum", "path_or_url": "str",
                          "expected_mountpoint": "str", "schedule": "schedule",
                          "enabled": "bool"},
               validate=_validate_offload_targets,
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
# which is what every one of these fields is actually read from). One knob deliberately
# NOT included here: `osiris_lease_refuse` lives on a different process entirely (the
# manager daemon, manager/daemon.py's own `_lease_gate`) — registered separately as
# `_MANAGER_OVERLAY` above instead (SECRETS ROTATE ACT + MANAGER OVERLAY, Thoth mail
# 10441: the "bigger, riskier first step" flagged here as a follow-up is now built —
# `_lease_gate` calls `settings_with_overlay(self._pool)` the same way every tick
# function below does). The
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

# WAVE 22 (ruling 7be61879, thread 40d6eef3, census gap-list-1 #1) — the daemon unit
# literals a stranger hits first: MemoryMax for the four persistent dev-box daemons,
# osiris-pulse's own --watch interval, the console's --host/--port, and the four pool
# sizes that already existed as real Settings fields (src/orchestrator/pool_health.py's
# own `_KNOWN_DAEMON_POOL_SETTINGS` names the exact unit each restarts under —
# `osiris_api_pool_size` restarts osiris-console, the daemon that actually serves
# `src.api.app` on the dev box, never a unit literally named "osiris-api").
#
# MemoryMax/watch-interval/host/port default to TODAY'S SHIPPED LITERAL rather than a
# None-means-unset sentinel: the generic int/str validators don't support null-clearing
# the way piece 3 added for path/schedule, and extending that further is out of scope
# here — an unwritten key therefore already renders byte-identical to the shipped file,
# "unset" falling out of matching defaults instead of a special case. The one exception:
# osiris-pulse carries no MemoryMax line at all today, so that spec alone defaults to ""
# (`scripts/render_units.py` treats empty as "no cap", omitting the line).
def _validate_memory_max(value: Any) -> str | None:
    import re as _re

    if value == "":
        return None
    if not isinstance(value, str) or not _re.fullmatch(r"infinity|\d+[KMGT]?", value):
        return "must be a systemd memory value like '3G'/'512M'/'infinity', or '' for no cap"
    return None


def _validate_port(value: Any) -> str | None:
    if not isinstance(value, int) or isinstance(value, bool) or not (1 <= value <= 65535):
        return "must be a valid TCP port (1-65535)"
    return None


def _validate_positive_int(value: Any) -> str | None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        return "must be a positive integer"
    return None


_DAEMON_UNIT_LITERALS: tuple[SettingSpec, ...] = (
    SettingSpec("daemon.osiris_mcp.memory_max", "str", "3G", effect="next_deploy",
               validate=_validate_memory_max, authority="operator_or_ruling",
               requires_because=True, consequence="high",
               write_name="daemon_unit_literals"),
    SettingSpec("daemon.osiris_worker.memory_max", "str", "3G", effect="next_deploy",
               validate=_validate_memory_max, authority="operator_or_ruling",
               requires_because=True, consequence="high",
               write_name="daemon_unit_literals"),
    SettingSpec("daemon.osiris_pulse.memory_max", "str", "", effect="next_deploy",
               validate=_validate_memory_max, authority="operator_or_ruling",
               requires_because=True, consequence="high",
               write_name="daemon_unit_literals"),
    SettingSpec("daemon.osiris_console.memory_max", "str", "512M", effect="next_deploy",
               validate=_validate_memory_max, authority="operator_or_ruling",
               requires_because=True, consequence="high",
               write_name="daemon_unit_literals"),
    SettingSpec("daemon.osiris_pulse.watch_interval_secs", "int", 600,
               effect="next_deploy", validate=_validate_positive_int,
               authority="operator_or_ruling", requires_because=True,
               consequence="high", write_name="daemon_unit_literals"),
    SettingSpec("daemon.osiris_console.host", "str", "127.0.0.1", effect="next_deploy",
               authority="operator_or_ruling", requires_because=True,
               consequence="high", write_name="daemon_unit_literals"),
    SettingSpec("daemon.osiris_console.port", "int", 8011, effect="next_deploy",
               validate=_validate_port, authority="operator_or_ruling",
               requires_because=True, consequence="high",
               write_name="daemon_unit_literals"),
    # THE CONSOLE GRACEFUL SHUTDOWN (thread 0be2f790's own deploy-reliability
    # follow-up, Thoth DM 10653): uvicorn's own drain window before it force-closes
    # in-flight connections on shutdown — an open SSE stream (a browser holding
    # /graph/stream/deltas or /console/stream across a restart) never disconnects on
    # its own, so this bounds how long a restart waits on it before systemd's own
    # TimeoutStopSec (90s default, unset on this unit) would otherwise SIGKILL the
    # whole process. 10s default matches the shipped unit's own literal.
    SettingSpec("daemon.osiris_console.graceful_shutdown_secs", "int", 10,
               effect="next_deploy", validate=_validate_positive_int,
               authority="operator_or_ruling", requires_because=True,
               consequence="high", write_name="daemon_unit_literals"),
    # osiris-pg-autotune.timer, the second of the two "hand-installed timer" census
    # targets — osiris-preflight.timer was ALREADY brought under deploy management by
    # piece 3 (one of BACKUP_TIMER_UNITS, its schedule already backup.timer_schedule.
    # osiris-preflight.timer); only this one is genuinely new. Kept OUT of
    # BACKUP_TIMER_UNITS (it isn't a backup-lane timer) and grouped under "daemon"
    # instead, an equally honest section for a Postgres-maintenance timer.
    SettingSpec("daemon.osiris_pg_autotune.schedule", "schedule", None,
               effect="next_deploy", authority="operator_or_ruling",
               requires_because=True, consequence="high",
               write_name="daemon_unit_literals"),
)

# THE FOUR POOL SIZES — real Settings fields already, keyed by their OWN settings.py
# attribute name (never a fabricated per-daemon alias) so a reader grepping settings.py
# finds the exact field a write governs. `env_field` documents which attribute
# render_units.py's own Environment= substitution writes into the unit file; it is inert
# for the LIVE overlay (`settings_with_overlay` only ever applies to `effect='immediate'`
# keys, and pool sizes are read once at daemon boot, hence `restart:<unit>`).
_POOL_SIZES: tuple[SettingSpec, ...] = (
    SettingSpec("daemon.osiris_mcp.pool_size", "int", 8, effect="restart:osiris-mcp",
               validate=_validate_positive_int, authority="operator_or_ruling",
               requires_because=True, consequence="high",
               write_name="daemon_unit_literals", env_field="osiris_mcp_pool_size"),
    SettingSpec("daemon.osiris_worker.pool_size", "int", 4, effect="restart:osiris-worker",
               validate=_validate_positive_int, authority="operator_or_ruling",
               requires_because=True, consequence="high",
               write_name="daemon_unit_literals", env_field="osiris_worker_pool_size"),
    # osiris_api_pool_size: pool_health.py's own map names its restart target
    # "osiris-console" (the dev-box daemon actually serving src.api.app), not "osiris-api".
    SettingSpec("daemon.osiris_api.pool_size", "int", 10, effect="restart:osiris-console",
               validate=_validate_positive_int, authority="operator_or_ruling",
               requires_because=True, consequence="high",
               write_name="daemon_unit_literals", env_field="osiris_api_pool_size"),
    # osiris-manager isn't one of the four MemoryMax daemons (a separate, reviewer-gated
    # unit outside deploy/user/'s own install pipeline) — registering its pool size does
    # not bring the unit itself under deploy management, a separate act this wave doesn't
    # take.
    SettingSpec("daemon.osiris_manager.pool_size", "int", 10,
               effect="restart:osiris-manager", validate=_validate_positive_int,
               authority="operator_or_ruling", requires_because=True,
               consequence="high", write_name="daemon_unit_literals",
               env_field="osiris_manager_pool_size"),
)

# THE TRANSCRIPTS ROOT (thread e332177f, 2026-09-14, "the oldest-first default made the
# sample blind" investigation's own root cause): osiris-mcp.service never set
# OSIRIS_TRANSCRIPTS at all, so `settings.osiris_transcripts` defaulted to "" inside the
# MCP process — silently inert for every transcript-reading path that runs there (the
# provenance backfill's own index, and the liveness transcript-mtime fallback whenever
# it's reached through an MCP door). Same shape as `_POOL_SIZES` above (`env_field`-
# driven, `restart:<unit>` — the value is read once at process boot, never live-
# overlaid) rather than a new pattern.
_INGEST_SETTINGS: tuple[SettingSpec, ...] = (
    SettingSpec("ingest.transcripts_root", "path", "", effect="restart:osiris-mcp",
               authority="operator_or_ruling", requires_because=True, consequence="low",
               write_name="daemon_unit_literals", env_field="osiris_transcripts"),
    # THE STALL'S OWN FIX, item 3 (thread 0be2f790, Thoth mail 10626, 2026-09-14
    # evening): a real transcript on this box ran 469MB, and reading one whole into
    # memory on osiris-mcp's own event loop thread starved the shared, whole-fleet
    # connection for 19 minutes before the operator restarted it by hand.
    # `effect='next_tick'`: read fresh each backfill call/job via `current_stored_value`
    # (the same `next_tick`-key escape hatch `wake.trigger.enabled` already established),
    # never baked into a live env var the way a `restart:<unit>` knob is — no `env_field`,
    # since nothing reads this off a live `Settings` object.
    # Raised 64MB -> 1GB (Thoth mail 10716, 2026-09-15): the cap existed for the MCP
    # loop-thread stall this ruling's own fix already ended — the scan is now off-loop,
    # streaming, and worker-side. Real transcripts on this box run 200-470MB; the old
    # 64MB cap silently folded almost every real writer into "no_transcript" (measured:
    # 30/30 sampled candidates were present-but-unresolved, not pruned — thread e332177f).
    SettingSpec("ingest.transcript_scan_max_bytes", "int", 1024 * 1024 * 1024,
               effect="next_tick", authority="operator_or_ruling", requires_because=False,
               consequence="low", write_name="daemon_unit_literals"),
)

# THE LAYOUT HEARTBEAT'S OWN KNOBS (Thoth mail 10609, product law -- every action has a
# door): batch_size genuinely reads live off the settings table (graph_layout.
# layout_batch's own current_stored_value lookup, NOT the effect='immediate'-only env
# overlay) -- a write here changes the very next tick, no restart. tick_seconds governs
# arq's own cron schedule, a static literal evaluated once at WorkerSettings class-
# definition time (same reason the pool sizes above are restart-effect), hence
# write_name="daemon_unit_literals" so a write re-renders the deployed unit's own
# environment exactly like a pool-size write does.
_LAYOUT_SETTINGS: tuple[SettingSpec, ...] = (
    SettingSpec("layout.batch_size", "int", 1000, effect="next_tick",
               validate=_validate_positive_int, consequence="low",
               requires_because=False, env_field="osiris_layout_batch_size"),
    SettingSpec("layout.tick_seconds", "int", 300, effect="restart:osiris-worker",
               validate=_validate_positive_int, authority="operator_or_ruling",
               requires_because=True, consequence="high",
               write_name="daemon_unit_literals", env_field="osiris_layout_tick_seconds"),
    # THE PHYSICS LAYOUT OOM (Thoth mail 11097, kernel-confirmed: anon-rss 26.3 GB,
    # process killed): run_physics_migrate's own declump pass built a full (n,n,2)
    # float64 pairwise array over the WHOLE active population -- 40 GB at n=50,087.
    # Fixed by a spatial-hash-grid declump (graph_layout._declump), but this setting
    # is the belt-and-suspenders guard: read fresh before any REMAINING quadratic-
    # memory step a future change might reintroduce, never cached at process boot
    # (a fresh `--physics` process every run, no daemon to restart) -- no env_field,
    # a genuinely new knob living only in the settings table.
    SettingSpec("layout.physics_max_bytes", "int", 2_000_000_000, effect="immediate",
               validate=_validate_positive_int, consequence="low",
               requires_because=False),
    # CONVERGE-OR-BUDGET DECLUMP (Thoth mail 11191, ruling 6befd2a5's own follow-up):
    # the physics migration's final global declump pass now iterates in small
    # chunks until the worst residual deficit clears 0.05*min_sep OR this wall-
    # clock budget is spent, whichever comes first -- read fresh each migration
    # run (no daemon to restart, same as layout.physics_max_bytes above).
    SettingSpec("layout.physics_declump_budget_secs", "int", 120, effect="immediate",
               validate=_validate_positive_int, consequence="low",
               requires_because=False),
)

SETTINGS: tuple[SettingSpec, ...] = (
    _MANAGER_OVERLAY + _SECRETS + _DAEMON_KILL_SWITCHES + _MINER_BUDGETS + _BACKUP_SETTINGS
    + _WAKE_LADDER + _DIAGNOSTICS + _DAEMON_UNIT_LITERALS + _POOL_SIZES + _INGEST_SETTINGS
    + _LAYOUT_SETTINGS
)


_BY_KEY: dict[str, SettingSpec] = {s.key: s for s in SETTINGS}
assert len(_BY_KEY) == len(SETTINGS), "duplicate SettingSpec key in the registry"


def spec_by_key(key: str) -> SettingSpec | None:
    """The registry's own lookup — never a linear scan repeated at every call site."""
    return _BY_KEY.get(key)

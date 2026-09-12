"""backup_settings — thin door over the settings registry (THE SETTINGS MENU piece 3,
thread 7eb26f68, Thoth's GO mail 10084/10094), folding Wave 21's own write half (thread
f04cce36 piece 3b) into `src/config/settings_registry.py`'s SettingSpecs.

`get_backup_settings`/`write_backup_settings` keep their exact outward dict shape; the
authority gate (`write_backup_settings` copies from `charter_for`: operator directly, or a
cited standing ruling that actually names this write) is now `settings_service.write_setting`'s
own generic authority check, exercised per key rather than reimplemented here — same
tests, same behavior, a different (generalized) mechanism underneath.
"""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.backup_settings import (
    BACKUP_TIMER_UNITS,
    get_backup_settings,
    write_backup_settings,
)


async def test_write_backup_settings_is_partial(actions: Actions) -> None:
    p = actions.pool
    s1 = await write_backup_settings(
        p, actor="operator", because="switching vaults", vault_path="/mnt/nas/osiris-vault")
    assert s1["vault_path"] == "/mnt/nas/osiris-vault"
    # a later partial write leaves vault_path untouched
    s2 = await write_backup_settings(
        p, actor="operator", because="tightening the dump schedule",
        timer_schedules={"osiris-backup.timer": "*-*-* 00,12:00:00"})
    assert s2["vault_path"] == "/mnt/nas/osiris-vault"
    assert s2["timer_schedules"] == {"osiris-backup.timer": "*-*-* 00,12:00:00"}
    assert await get_backup_settings(p) == {k: v for k, v in s2.items() if k != "because"}


async def test_write_backup_settings_timer_schedules_is_a_full_replace(actions: Actions) -> None:
    """Same UX the retired panel had — posting the field's FULL value each save, so a
    unit missing from the new dict is explicitly cleared, not left over from an earlier
    write (folded from one shared blob into 5 per-unit keys, piece 3's own design)."""
    p = actions.pool
    s1 = await write_backup_settings(
        p, actor="operator", because="two overrides",
        timer_schedules={"osiris-backup.timer": "*-*-* 06:00:00",
                        "osiris-preflight.timer": "Mon *-*-* 03:00:00"})
    assert s1["timer_schedules"] == {"osiris-backup.timer": "*-*-* 06:00:00",
                                     "osiris-preflight.timer": "Mon *-*-* 03:00:00"}
    s2 = await write_backup_settings(
        p, actor="operator", because="dropping the preflight override",
        timer_schedules={"osiris-backup.timer": "*-*-* 06:00:00"})
    assert s2["timer_schedules"] == {"osiris-backup.timer": "*-*-* 06:00:00"}


async def test_write_backup_settings_null_clears_the_vault_path(actions: Actions) -> None:
    p = actions.pool
    await write_backup_settings(p, actor="operator", because="set it",
                                vault_path="/mnt/nas/osiris-vault")
    cleared = await write_backup_settings(p, actor="operator", because="unset it",
                                          vault_path=None)
    assert cleared["vault_path"] is None


async def test_write_backup_settings_rejects_unknown_field(actions: Actions) -> None:
    res = await write_backup_settings(actions.pool, actor="operator", because="x", secret="oops")
    assert "error" in res and "unknown" in res["error"]


async def test_write_backup_settings_rejects_relative_vault_path(actions: Actions) -> None:
    res = await write_backup_settings(
        actions.pool, actor="operator", because="x", vault_path="relative/path")
    assert "error" in res and "absolute" in res["error"]


async def test_write_backup_settings_rejects_unknown_timer_unit(actions: Actions) -> None:
    res = await write_backup_settings(
        actions.pool, actor="operator", because="x",
        timer_schedules={"not-a-real.timer": "daily"})
    assert "error" in res and "unknown timer unit" in res["error"]


async def test_write_backup_settings_rejects_a_malformed_offbox_repository(
    actions: Actions,
) -> None:
    res = await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offbox_repositories=[{"url": "", "enabled": True}])
    assert "error" in res and "url" in res["error"]

    res2 = await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offbox_repositories=[{"url": "sftp://nas/repo", "enabled": "yes"}])
    assert "error" in res2
    assert any("enabled" in e["field"] for e in res2.get("errors", []))


async def test_write_backup_settings_accepts_a_well_formed_offbox_repository(
    actions: Actions,
) -> None:
    entry = {"name": "nas", "url": "sftp://nas/repo", "schedule": "daily", "enabled": False}
    res = await write_backup_settings(
        actions.pool, actor="operator", because="x", offbox_repositories=[entry])
    assert res["offbox_repositories"] == [entry]


async def test_get_backup_settings_starts_at_defaults(actions: Actions) -> None:
    base = await get_backup_settings(actions.pool)
    assert base == {"vault_path": None, "timer_schedules": {}, "offbox_repositories": []}


# --- the write door's own authority gate ------------------------------------

async def test_write_backup_settings_requires_because(actions: Actions) -> None:
    res = await write_backup_settings(
        actions.pool, actor="operator", because="", vault_path="/mnt/nas")
    assert "error" in res and "because" in res["error"]


async def test_write_backup_settings_the_operator_writes_freely(actions: Actions) -> None:
    res = await write_backup_settings(
        actions.pool, actor="operator", because="switching to the NAS mount",
        vault_path="/mnt/nas/osiris-vault")
    assert res["vault_path"] == "/mnt/nas/osiris-vault"
    assert res["because"] == "switching to the NAS mount"


async def test_write_backup_settings_a_worker_with_no_ruling_is_refused(
    actions: Actions,
) -> None:
    res = await write_backup_settings(
        actions.pool, actor="agent:some-worker", because="testing",
        vault_path="/mnt/nas")
    assert "error" in res and "ruling" in res["error"]


async def test_write_backup_settings_a_worker_citing_the_wrong_ruling_is_refused(
    actions: Actions,
) -> None:
    from src.orchestrator.capture import record_decision

    unrelated = await record_decision(
        actions, summary="some unrelated ruling about charter_for",
        kind="ruling", rationale="authorizes charter_for, not this")
    res = await write_backup_settings(
        actions.pool, actor="agent:some-worker", because="testing",
        ruling=str(unrelated), vault_path="/mnt/nas")
    assert "error" in res and "does not name" in res["error"]


async def test_write_backup_settings_a_worker_citing_a_matching_ruling_succeeds(
    actions: Actions,
) -> None:
    from src.orchestrator.capture import record_decision

    ruling = await record_decision(
        actions, summary="operator ruling authorizing backup_settings writes for wave 21",
        kind="ruling", rationale="backup_settings may be written by the on-call worker")
    res = await write_backup_settings(
        actions.pool, actor="agent:some-worker", because="applying the operator's ruling",
        ruling=str(ruling), vault_path="/mnt/nas/osiris-vault")
    assert res["vault_path"] == "/mnt/nas/osiris-vault"


def test_backup_timer_units_is_a_tuple_of_five() -> None:
    assert len(BACKUP_TIMER_UNITS) == 5
    assert "osiris-backup.timer" in BACKUP_TIMER_UNITS
    assert "osiris-preflight.timer" in BACKUP_TIMER_UNITS


def test_compositions_backup_status_timer_list_matches_backup_settings_exactly() -> None:
    """No drift — the read half (compositions.py's `_BACKUP_TIMER_UNITS`, with display
    labels) and the write door (this module's own `BACKUP_TIMER_UNITS`, bare names, used
    to validate writes) must agree on exactly which units exist."""
    from src.orchestrator.compositions import _BACKUP_TIMER_UNITS

    assert {u for u, _ in _BACKUP_TIMER_UNITS} == set(BACKUP_TIMER_UNITS)


def test_backup_settings_registers_every_field_in_the_registry() -> None:
    """Piece 3's own fold: vault_path, one schedule per timer, and offbox_repositories
    are ordinary SettingSpecs now, not a parallel implementation."""
    from src.config.settings_registry import SETTINGS, spec_by_key

    vault = spec_by_key("backup.vault_path")
    assert vault is not None and vault.type == "path" and vault.effect == "next_deploy"
    offbox = spec_by_key("backup.offbox_repositories")
    assert offbox is not None and offbox.type == "records"
    assert offbox.item_shape == {"name": "str", "url": "str", "schedule": "schedule",
                                 "enabled": "bool"}
    timer_keys = {s.key for s in SETTINGS if s.key.startswith("backup.timer_schedule.")}
    assert timer_keys == {f"backup.timer_schedule.{u}" for u in BACKUP_TIMER_UNITS}

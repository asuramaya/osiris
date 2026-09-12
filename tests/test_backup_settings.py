"""backup_settings — the config panel's write half (Wave 21, thread f04cce36 piece 3).

Same write semantics `test_console.py` already proves for `console_state` (partial
update, monotonic rev, who-moved-last) plus the authority gate `write_backup_settings`
copies from `charter_for` (operator directly, or a cited standing ruling that actually
names this write).
"""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.backup_settings import (
    BACKUP_TIMER_UNITS,
    get_backup_settings,
    set_backup_settings,
    write_backup_settings,
)


async def test_set_backup_settings_is_partial_and_bumps_rev(actions: Actions) -> None:
    p = actions.pool
    s0 = await get_backup_settings(p)
    s1 = await set_backup_settings(p, by="operator", vault_path="/mnt/nas/osiris-vault")
    assert s1["vault_path"] == "/mnt/nas/osiris-vault"
    assert s1["rev"] > s0["rev"]
    # a later partial write leaves vault_path untouched
    s2 = await set_backup_settings(
        p, by="operator", timer_schedules={"osiris-backup.timer": "*-*-* 00,12:00:00"})
    assert s2["vault_path"] == "/mnt/nas/osiris-vault"
    assert s2["timer_schedules"] == {"osiris-backup.timer": "*-*-* 00,12:00:00"}
    assert s2["rev"] > s1["rev"]
    assert await get_backup_settings(p) == s2


async def test_set_backup_settings_rejects_unknown_field(actions: Actions) -> None:
    res = await set_backup_settings(actions.pool, by="operator", secret="oops")
    assert "error" in res and "unknown" in res["error"]


async def test_set_backup_settings_rejects_relative_vault_path(actions: Actions) -> None:
    res = await set_backup_settings(actions.pool, by="operator", vault_path="relative/path")
    assert "error" in res and "absolute" in res["error"]


async def test_set_backup_settings_rejects_unknown_timer_unit(actions: Actions) -> None:
    res = await set_backup_settings(
        actions.pool, by="operator", timer_schedules={"not-a-real.timer": "daily"})
    assert "error" in res and "unknown timer unit" in res["error"]


async def test_set_backup_settings_rejects_a_malformed_offbox_repository(
    actions: Actions,
) -> None:
    res = await set_backup_settings(
        actions.pool, by="operator", offbox_repositories=[{"url": "", "enabled": True}])
    assert "error" in res and "url" in res["error"]

    res2 = await set_backup_settings(
        actions.pool, by="operator",
        offbox_repositories=[{"url": "sftp://nas/repo", "enabled": "yes"}])
    assert "error" in res2 and "enabled" in res2["error"]


async def test_set_backup_settings_accepts_a_well_formed_offbox_repository(
    actions: Actions,
) -> None:
    entry = {"url": "sftp://nas/repo", "schedule": "daily", "enabled": False}
    res = await set_backup_settings(actions.pool, by="operator", offbox_repositories=[entry])
    assert res["offbox_repositories"] == [entry]


async def test_backup_settings_survives_a_wiped_singleton(actions: Actions) -> None:
    p = actions.pool
    base = await get_backup_settings(p)
    assert base["rev"] == 0 and base["vault_path"] is None
    s = await set_backup_settings(p, by="operator", vault_path="/mnt/nas")
    assert s["vault_path"] == "/mnt/nas"


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

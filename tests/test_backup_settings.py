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

from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator.backup_settings import (
    BACKUP_TIMER_UNITS,
    get_backup_settings,
    write_backup_settings,
)


@pytest.fixture(autouse=True)
def _vault_path_always_present_mount(monkeypatch: Any) -> None:
    """This file exercises write_backup_settings' own mechanics (partial writes,
    authority, full-replace semantics) — the hot-path always-present-mount refusal is
    tested on its own in test_backup_validation.py, so every real tmp_path used here as
    a vault_path is treated as sitting under a declared mount, independent of whatever
    this box's own real fstab happens to say."""
    from src.orchestrator import backup_validation

    monkeypatch.setattr(backup_validation, "_is_always_present_mountpoint", lambda p: True)


async def test_write_backup_settings_is_partial(actions: Actions, tmp_path: Path) -> None:
    vault = tmp_path / "osiris-vault"
    vault.mkdir()
    p = actions.pool
    s1 = await write_backup_settings(
        p, actor="operator", because="switching vaults", vault_path=str(vault))
    assert s1["vault_path"] == str(vault)
    # a later partial write leaves vault_path untouched
    s2 = await write_backup_settings(
        p, actor="operator", because="tightening the dump schedule",
        timer_schedules={"osiris-backup.timer": "*-*-* 00,12:00:00"})
    assert s2["vault_path"] == str(vault)
    assert s2["timer_schedules"] == {"osiris-backup.timer": "*-*-* 00,12:00:00"}
    assert await get_backup_settings(p) == {
        k: v for k, v in s2.items() if k not in ("because", "warnings")}


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


async def test_write_backup_settings_null_clears_the_vault_path(
    actions: Actions, tmp_path: Path,
) -> None:
    p = actions.pool
    was_set = await write_backup_settings(p, actor="operator", because="set it",
                                          vault_path=str(tmp_path))
    assert was_set["vault_path"] == str(tmp_path)  # the set genuinely landed
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


# --- offload_targets: THE BACKUP TOPOLOGY / INTERMITTENT TARGETS, Thoth mail 12812 -----------

async def test_write_backup_settings_rejects_a_malformed_offload_target(
    actions: Actions,
) -> None:
    res = await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offload_targets=[{"kind": "local", "path_or_url": "/mnt/drive",
                          "expected_mountpoint": "/mnt/drive", "schedule": "daily",
                          "enabled": True}])
    assert "error" in res and "name" in res["error"]

    res2 = await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offload_targets=[{"name": "drive", "kind": "usb", "path_or_url": "/mnt/drive",
                          "expected_mountpoint": "/mnt/drive", "schedule": "daily",
                          "enabled": True}])
    assert "error" in res2 and "kind" in res2["error"]

    res3 = await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offload_targets=[{"name": "drive", "kind": "local", "path_or_url": "/mnt/drive",
                          "schedule": "daily", "enabled": True}])  # missing mountpoint
    assert "error" in res3

    res4 = await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offload_targets=[{"name": "nas", "kind": "restic", "path_or_url": "sftp://nas/repo",
                          "expected_mountpoint": "/mnt/nas",  # forbidden for restic
                          "schedule": "daily", "enabled": True}])
    assert "error" in res4


async def test_write_backup_settings_rejects_duplicate_offload_target_names(
    actions: Actions,
) -> None:
    dupe = {"name": "same", "kind": "restic", "path_or_url": "sftp://nas/a",
            "schedule": "daily", "enabled": True}
    res = await write_backup_settings(
        actions.pool, actor="operator", because="x", offload_targets=[dupe, dict(dupe)])
    assert "error" in res and "duplicate" in res["error"]


async def test_write_backup_settings_rejects_a_restic_target_with_shapeless_url(
    actions: Actions,
) -> None:
    """The write-time restic-grammar check (backup_validation.validate_restic_url,
    shape only, no network) on top of the schema-level field check above."""
    res = await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offload_targets=[{"name": "nas", "kind": "restic", "path_or_url": "justaword",
                          "schedule": "daily", "enabled": True}])
    assert "error" in res and "justaword" in res["error"]


async def test_write_backup_settings_accepts_a_well_formed_local_offload_target(
    actions: Actions, tmp_path: Path,
) -> None:
    target = {"name": "docked-drive", "kind": "local", "path_or_url": str(tmp_path),
             "expected_mountpoint": str(tmp_path), "schedule": "Sat 03:00:00",
             "enabled": True}
    res = await write_backup_settings(
        actions.pool, actor="operator", because="x", offload_targets=[target])
    assert len(res["offload_targets"]) == 1
    row = res["offload_targets"][0]
    assert {k: v for k, v in row.items() if k != "presence"} == target
    assert "presence" in row  # local kind always gets a live presence verdict


async def test_write_backup_settings_accepts_a_well_formed_restic_offload_target(
    actions: Actions,
) -> None:
    target = {"name": "nas", "kind": "restic", "path_or_url": "sftp://nas/repo",
             "schedule": "Sun 02:00:00", "enabled": False}
    res = await write_backup_settings(
        actions.pool, actor="operator", because="x", offload_targets=[target])
    row = res["offload_targets"][0]
    assert {k: v for k, v in row.items() if k != "presence"} == target
    assert row["presence"] is None  # never a network-derived verdict


async def test_get_backup_settings_synthesizes_offload_targets_from_legacy_offbox(
    actions: Actions,
) -> None:
    """A pre-existing offbox_repositories row, never touched by any new write, still
    surfaces through offload_targets on read — the read-time-only migration (never
    writes anything itself)."""
    legacy = {"name": "old-nas", "url": "sftp://old-nas/repo", "schedule": "daily",
             "enabled": True}
    await write_backup_settings(
        actions.pool, actor="operator", because="x", offbox_repositories=[legacy])
    out = await get_backup_settings(actions.pool)
    assert out["offbox_repositories"] == [legacy]
    assert out["offload_targets"] == [{
        "name": "old-nas", "kind": "restic", "path_or_url": "sftp://old-nas/repo",
        "expected_mountpoint": None, "schedule": "daily", "enabled": True,
        "presence": None,
    }]


async def test_write_backup_settings_offload_targets_wins_over_legacy_synthesis(
    actions: Actions,
) -> None:
    """Once offload_targets has genuinely been written, it wins outright — the legacy
    offbox_repositories synthesis only ever fires when offload_targets is EMPTY."""
    await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offbox_repositories=[{"name": "old", "url": "sftp://old/repo", "schedule": "daily",
                              "enabled": True}])
    await write_backup_settings(actions.pool, actor="operator", because="x",
                                offload_targets=[{"name": "new", "kind": "restic",
                                                  "path_or_url": "sftp://new/repo",
                                                  "schedule": "daily", "enabled": True}])
    out = await get_backup_settings(actions.pool)
    assert [t["name"] for t in out["offload_targets"]] == ["new"]


async def test_get_backup_settings_starts_at_defaults(actions: Actions) -> None:
    base = await get_backup_settings(actions.pool)
    assert base == {"vault_path": None, "timer_schedules": {}, "offload_targets": [],
                    "offbox_repositories": []}


# --- the write door's own authority gate ------------------------------------

async def test_write_backup_settings_requires_because(actions: Actions) -> None:
    res = await write_backup_settings(
        actions.pool, actor="operator", because="", vault_path="/mnt/nas")
    assert "error" in res and "because" in res["error"]


async def test_write_backup_settings_the_operator_writes_freely(
    actions: Actions, tmp_path: Path,
) -> None:
    res = await write_backup_settings(
        actions.pool, actor="operator", because="switching to the NAS mount",
        vault_path=str(tmp_path))
    assert res["vault_path"] == str(tmp_path)
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
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.capture import record_decision

    ruling = await record_decision(
        actions, summary="operator ruling authorizing backup_settings writes for wave 21",
        kind="ruling", rationale="backup_settings may be written by the on-call worker")
    res = await write_backup_settings(
        actions.pool, actor="agent:some-worker", because="applying the operator's ruling",
        ruling=str(ruling), vault_path=str(tmp_path))
    assert res["vault_path"] == str(tmp_path)


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

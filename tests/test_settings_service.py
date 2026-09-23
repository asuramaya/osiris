"""THE SETTINGS REGISTRY's own service layer (THE SETTINGS MENU, thread f4498ab304e4
piece 1): list/get/write over the `settings` table (migration 0067), the authority
gate `write_setting` copies from `backup_settings.write_backup_settings`/`charter_for`,
and the opt-in-per-field overlay `settings_with_overlay`."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator.settings_service import (
    _invalidate_overlay_cache,
    get_setting,
    list_settings,
    live_value,
    settings_with_overlay,
    write_setting,
)


async def test_list_settings_shows_every_registered_knob_with_its_default(
    actions: Actions,
) -> None:
    out = await list_settings(actions.pool)
    by_key = {s["key"]: s for s in out}
    assert "daemon.pit_watch.enabled" in by_key
    row = by_key["daemon.pit_watch.enabled"]
    assert row["value"] is False and row["type"] == "bool" and row["effect"] == "immediate"


async def test_get_setting_falls_back_to_the_default_when_unset(actions: Actions) -> None:
    out = await get_setting(actions.pool, "miner.daily_budget_base")
    assert out == {"key": "miner.daily_budget_base", "value": 5, "live": None}


async def test_get_setting_refuses_an_unregistered_key(actions: Actions) -> None:
    out = await get_setting(actions.pool, "not.a.real.key")
    assert "error" in out and "unknown" in out["error"]


async def test_write_setting_the_operator_writes_freely_and_bumps_rev(
    actions: Actions,
) -> None:
    out = await write_setting(
        actions.pool, "daemon.pit_watch.enabled", True, actor="operator")
    assert out["value"] is True and out["rev"] == 1
    again = await write_setting(
        actions.pool, "daemon.pit_watch.enabled", False, actor="operator")
    assert again["value"] is False and again["rev"] == 2
    assert await get_setting(actions.pool, "daemon.pit_watch.enabled") == {
        "key": "daemon.pit_watch.enabled", "value": False, "live": False}


async def test_write_setting_requires_because_only_when_the_spec_asks(
    actions: Actions,
) -> None:
    # pit_watch: requires_because=False, no `because` needed
    ok = await write_setting(actions.pool, "daemon.pit_watch.enabled", True, actor="operator")
    assert "error" not in ok
    # retention_heartbeat: requires_because=True, refused without one
    refused = await write_setting(
        actions.pool, "daemon.retention_heartbeat.enabled", False, actor="operator")
    assert "error" in refused and "because" in refused["error"]
    ok2 = await write_setting(
        actions.pool, "daemon.retention_heartbeat.enabled", False, actor="operator",
        because="pausing for the migration window")
    assert "error" not in ok2


async def test_write_setting_a_worker_with_no_ruling_is_refused(actions: Actions) -> None:
    out = await write_setting(
        actions.pool, "daemon.pit_watch.enabled", True, actor="agent:some-worker")
    assert "error" in out and "ruling" in out["error"]


async def test_write_setting_a_worker_citing_a_matching_ruling_succeeds(
    actions: Actions,
) -> None:
    from src.orchestrator.capture import record_decision

    ruling = await record_decision(
        actions, summary="operator ruling authorizing daemon.pit_watch.enabled writes",
        kind="ruling", rationale="daemon.pit_watch.enabled may be written by the on-call worker")
    out = await write_setting(
        actions.pool, "daemon.pit_watch.enabled", True, actor="agent:some-worker",
        ruling=str(ruling))
    assert out["value"] is True


async def test_write_setting_a_worker_citing_the_wrong_ruling_is_refused(
    actions: Actions,
) -> None:
    from src.orchestrator.capture import record_decision

    unrelated = await record_decision(
        actions, summary="some unrelated ruling about charter_for",
        kind="ruling", rationale="authorizes charter_for, not this")
    out = await write_setting(
        actions.pool, "daemon.pit_watch.enabled", True, actor="agent:some-worker",
        ruling=str(unrelated))
    assert "error" in out and "does not name" in out["error"]


async def test_write_setting_rejects_a_bad_type_with_a_structured_error(
    actions: Actions,
) -> None:
    out = await write_setting(
        actions.pool, "daemon.pit_watch.enabled", "not-a-bool", actor="operator")
    assert "error" in out
    assert out["errors"] == [{"field": "daemon.pit_watch.enabled", "message": "must be a boolean"}]


async def test_write_setting_refuses_an_unregistered_key(actions: Actions) -> None:
    out = await write_setting(actions.pool, "not.a.real.key", 1, actor="operator")
    assert "error" in out and "unknown" in out["error"]


async def test_write_setting_names_the_effect_and_a_note_for_non_immediate_knobs(
    actions: Actions,
) -> None:
    out = await write_setting(
        actions.pool, "miner.daily_budget_base", 10, actor="operator")
    assert out["effect"] == "next_tick"
    assert "note" not in out  # only restart:/next_deploy carry a note, per the design


# --- the overlay: opt-in per field, TTL-cached, fails open ----------------------------

async def test_overlay_leaves_settings_untouched_when_nothing_is_registered_immediate(
    actions: Actions,
) -> None:
    """A registered-but-untouched key reads its own env/default: the overlay is a
    no-op until something is actually written."""
    st = await settings_with_overlay(actions.pool)
    assert st.osiris_pit_watch_enabled is False  # settings.py's own default


async def test_overlay_reflects_a_write_immediately_after_invalidation(
    actions: Actions,
) -> None:
    await write_setting(actions.pool, "daemon.pit_watch.enabled", True, actor="operator")
    st = await settings_with_overlay(actions.pool)
    assert st.osiris_pit_watch_enabled is True


async def test_overlay_only_touches_registered_env_fields(actions: Actions) -> None:
    """Opt-in per field: writing a registered knob
    must never leak into an UNRELATED Settings attribute the overlay never touches."""
    await write_setting(actions.pool, "daemon.pit_watch.enabled", True, actor="operator")
    st = await settings_with_overlay(actions.pool)
    assert st.osiris_daily_usd == 10.0  # untouched, exactly settings.py's own default


async def test_overlay_fails_open_on_a_db_error(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _BrokenPool:
        async def fetch(self, *a: object, **k: object) -> list[object]:
            raise RuntimeError("db is down")

    st = await settings_with_overlay(_BrokenPool())  # type: ignore[arg-type]
    assert st.osiris_pit_watch_enabled is False  # the env/default, not a raised exception


def test_invalidate_overlay_cache_is_idempotent_on_an_empty_cache() -> None:
    _invalidate_overlay_cache()  # must never raise even with nothing cached
    _invalidate_overlay_cache()


# --- THE SECRETS ROTATE ACT: a write on a secret_ref key IS a rotate (thread ----------
# --- f4498ab304e4's own follow-up): the real value goes into the ---------------------
# --- spec's own backing_file, never the settings table, never echoed back -------------

# --- `live`: the running/shipped counterpart, null when not cheap (thread c5ba8681) ---

async def test_live_value_immediate_reads_the_overlay(actions: Actions) -> None:
    from src.config.settings_registry import spec_by_key

    spec = spec_by_key("daemon.pit_watch.enabled")
    assert spec is not None
    assert await live_value(actions.pool, spec) is False  # env default
    await write_setting(actions.pool, "daemon.pit_watch.enabled", True, actor="operator")
    assert await live_value(actions.pool, spec) is True


async def test_live_value_next_tick_is_null(actions: Actions) -> None:
    from src.config.settings_registry import spec_by_key

    spec = spec_by_key("miner.daily_budget_base")
    assert spec is not None
    assert await live_value(actions.pool, spec) is None


async def test_live_value_next_deploy_reads_the_shipped_timer_file(actions: Actions) -> None:
    from src.config.settings_registry import BACKUP_TIMER_UNITS, spec_by_key

    unit = BACKUP_TIMER_UNITS[0]
    spec = spec_by_key(f"backup.timer_schedule.{unit}")
    assert spec is not None
    live = await live_value(actions.pool, spec)
    assert live is None or isinstance(live, str)  # None on a checkout with no deploy/ unit file


async def test_live_value_next_deploy_is_null_with_no_rendered_file(actions: Actions) -> None:
    """`backup.vault_path` has no shipped-file counterpart at all: never a guess."""
    from src.config.settings_registry import spec_by_key

    spec = spec_by_key("backup.vault_path")
    assert spec is not None
    assert await live_value(actions.pool, spec) is None


async def test_live_value_secret_ref_is_always_null(actions: Actions) -> None:
    from dataclasses import replace

    from src.config import settings_registry

    fake = replace(settings_registry.SETTINGS[0], key="test.fake_secret", type="secret_ref")
    assert await live_value(actions.pool, fake) is None


async def test_live_value_restart_unit_fails_open_with_no_env_field(actions: Actions) -> None:
    from dataclasses import replace

    from src.config import settings_registry

    fake = replace(settings_registry.SETTINGS[0], key="test.fake_restart",
                   effect="restart:osiris-worker", env_field=None)
    assert await live_value(actions.pool, fake) is None


async def test_live_value_restart_unit_fails_open_when_systemd_is_unavailable(
    actions: Actions,
) -> None:
    """CI/dev-worktree law: no systemd, no unit installed: 'unavailable', not raised."""
    from dataclasses import replace

    from src.config import settings_registry

    fake = replace(settings_registry.SETTINGS[0], key="test.fake_restart",
                   effect="restart:osiris-nonexistent-unit-xyz",
                   env_field="osiris_pit_watch_enabled")
    assert await live_value(actions.pool, fake) is None


async def test_live_value_the_real_registered_restart_unit_spec_fails_open_in_ci(
    actions: Actions,
) -> None:
    """`diag.worker_boot_memtrace.enabled` is the one REAL registered restart:<unit>
    knob (this build's piece 2) - this test env has no osiris-worker unit installed, so it
    must degrade to null rather than raise, same law as the synthetic fakes above."""
    from src.config.settings_registry import spec_by_key

    spec = spec_by_key("diag.worker_boot_memtrace.enabled")
    assert spec is not None
    assert await live_value(actions.pool, spec) is None


async def test_list_settings_and_get_setting_both_carry_a_live_key(actions: Actions) -> None:
    out = await list_settings(actions.pool)
    assert all("live" in row for row in out)
    got = await get_setting(actions.pool, "daemon.pit_watch.enabled")
    assert "live" in got


def _fake_secret_spec(key: str = "test.fake_secret", **overrides: Any) -> Any:
    from dataclasses import replace

    from src.config import settings_registry

    base = replace(settings_registry.SETTINGS[0], key=key, type="secret_ref",
                   authority="operator", requires_because=False, backing_file=None,
                   env_field="test_fake_secret")
    return replace(base, **overrides) if overrides else base


async def test_write_setting_secret_ref_refuses_with_no_backing_file(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A secret_ref spec declared with no backing_file (a registry mistake: every real
    one must carry one) refuses loudly rather than silently doing nothing or crashing."""
    from src.orchestrator import settings_service as svc

    fake = _fake_secret_spec()
    monkeypatch.setattr(svc, "spec_by_key", lambda key: fake if key == fake.key else None)

    out = await svc.write_setting(actions.pool, fake.key, "value", actor="operator")
    assert "error" in out and "backing_file" in out["error"]


async def test_write_setting_secret_ref_rejects_an_empty_value(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    from src.orchestrator import settings_service as svc

    fake = _fake_secret_spec(backing_file=str(tmp_path / "secrets.env"))
    monkeypatch.setattr(svc, "spec_by_key", lambda key: fake if key == fake.key else None)

    out = await svc.write_setting(actions.pool, fake.key, "   ", actor="operator")
    assert "error" in out and "non-empty" in out["error"]
    assert not (tmp_path / "secrets.env").exists()


async def test_write_setting_secret_ref_rotates_into_the_file_never_the_table(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """The full round trip: the real value lands in the backing file (a KEY=value line,
    0600), a bare {"rotated": true} marker lands in the `settings` table (queried
    directly here, bypassing get_setting/list_settings, to prove the REAL value never
    reaches the table at all, not merely that it isn't returned), and the receipt
    never echoes `value`."""
    import stat

    from src.orchestrator import settings_service as svc

    backing = tmp_path / "secrets.env"
    fake = _fake_secret_spec(backing_file=str(backing))
    monkeypatch.setattr(svc, "spec_by_key", lambda key: fake if key == fake.key else None)

    out = await svc.write_setting(
        actions.pool, fake.key, "sk-super-secret-1", actor="operator", because="rotate it")
    assert out.get("rotated") is True
    assert "value" not in out
    assert out["key"] == fake.key

    assert backing.read_text().strip() == "TEST_FAKE_SECRET=sk-super-secret-1"
    assert stat.S_IMODE(backing.stat().st_mode) == 0o600

    row = await actions.pool.fetchrow(
        "SELECT value FROM settings WHERE key=$1 AND scope='box' AND scope_id=''", fake.key)
    assert row is not None
    stored = row["value"]
    import json as _json
    stored_py = _json.loads(stored) if isinstance(stored, str) else stored
    assert stored_py is True  # the marker, never the real value


async def test_write_setting_secret_ref_rotate_updates_not_appends(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """A second rotation replaces the existing KEY= line rather than appending a
    duplicate, the file stays exactly one line for this key."""
    from src.orchestrator import settings_service as svc

    backing = tmp_path / "secrets.env"
    fake = _fake_secret_spec(backing_file=str(backing))
    monkeypatch.setattr(svc, "spec_by_key", lambda key: fake if key == fake.key else None)

    await svc.write_setting(actions.pool, fake.key, "first-value", actor="operator")
    await svc.write_setting(actions.pool, fake.key, "second-value", actor="operator")

    lines = backing.read_text().splitlines()
    assert lines == ["TEST_FAKE_SECRET=second-value"]


async def test_write_setting_secret_ref_preserves_unrelated_lines(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """The rotate never re-serializes the whole file: an unrelated line already
    present (another daemon's own secret/config) survives untouched."""
    from src.orchestrator import settings_service as svc

    backing = tmp_path / "secrets.env"
    backing.write_text("DATABASE_URL=postgresql://x\nOTHER_KEY=unrelated\n")
    fake = _fake_secret_spec(backing_file=str(backing))
    monkeypatch.setattr(svc, "spec_by_key", lambda key: fake if key == fake.key else None)

    await svc.write_setting(actions.pool, fake.key, "new-secret", actor="operator")

    lines = backing.read_text().splitlines()
    assert "DATABASE_URL=postgresql://x" in lines
    assert "OTHER_KEY=unrelated" in lines
    assert "TEST_FAKE_SECRET=new-secret" in lines


async def test_write_setting_secret_ref_names_the_daemon_that_must_restart(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Same `effect.startswith("restart:")` -> `result["note"]` convention every other
    write receipt already uses, reused not duplicated, for a rotate."""
    from src.orchestrator import settings_service as svc

    fake = _fake_secret_spec(
        backing_file=str(tmp_path / "secrets.env"), effect="restart:osiris-worker")
    monkeypatch.setattr(svc, "spec_by_key", lambda key: fake if key == fake.key else None)

    out = await svc.write_setting(actions.pool, fake.key, "value", actor="operator")
    assert out["note"] == "takes effect on osiris-worker's next restart, not automatically"


async def test_the_real_registered_secret_spec_rotates_live(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """`secrets.etherscan_api_key` is the one real secret_ref spec registered today
    (SECRETS ROTATE ACT) proves the LIVE registry entry's own key/env_field/effect/
    authority, not just a synthetic fake, exercised against a scratch copy of its
    backing_file (never the box's real one: `backing_file` is the only field
    redirected here, everything else about the spec is exactly what ships)."""
    from dataclasses import replace

    from src.config import settings_registry
    from src.orchestrator import settings_service as svc

    real = settings_registry.spec_by_key("secrets.etherscan_api_key")
    assert real is not None and real.type == "secret_ref" and real.backing_file
    assert real.env_field == "etherscan_api_key"

    scratch = tmp_path / "secrets.env"
    redirected = replace(real, backing_file=str(scratch))
    monkeypatch.setattr(
        svc, "spec_by_key", lambda key: redirected if key == real.key else None)

    out = await svc.write_setting(
        actions.pool, real.key, "sk-live-round-trip", actor="operator",
        because="test the real registered spec")
    assert out.get("rotated") is True
    assert scratch.read_text().strip() == "ETHERSCAN_API_KEY=sk-live-round-trip"
    assert out["note"] == "takes effect on osiris-worker's next restart, not automatically"


async def test_settings_with_overlay_never_substitutes_a_secret_marker(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """The type-mismatch guard in `settings_with_overlay` itself: an `effect='immediate'`
    secret_ref (synthetic: every real one registered today is `restart:<unit>`, so this
    proves the filter holds even for a shape that isn't registered) never lands its
    boolean `{"rotated": true}` marker on a str-typed Settings attribute.

    Patches `svc.SETTINGS` directly (never `settings_registry.SETTINGS`): this
    module's own docstring on `settings_with_overlay` names exactly why a source-module
    patch would be silently ignored: the name was already bound at import time."""
    from src.orchestrator import settings_service as svc

    fake = _fake_secret_spec(
        key="test.fake_immediate_secret", effect="immediate",
        backing_file=str(tmp_path / "secrets.env"), env_field="etherscan_api_key")
    monkeypatch.setattr(svc, "spec_by_key", lambda key: fake if key == fake.key else None)
    monkeypatch.setattr(svc, "SETTINGS", (*svc.SETTINGS, fake))

    out = await svc.write_setting(actions.pool, fake.key, "value", actor="operator")
    assert out.get("rotated") is True

    result = await svc.settings_with_overlay(actions.pool)
    assert isinstance(result.etherscan_api_key, str)  # never True/the boolean marker

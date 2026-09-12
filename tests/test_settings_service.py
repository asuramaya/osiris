"""THE SETTINGS REGISTRY's own service layer (THE SETTINGS MENU, thread f4498ab304e4
piece 1) — list/get/write over the `settings` table (migration 0067), the authority
gate `write_setting` copies from `backup_settings.write_backup_settings`/`charter_for`,
and the opt-in-per-field overlay `settings_with_overlay`."""
from __future__ import annotations

import pytest
from src.actions.core import Actions
from src.orchestrator.settings_service import (
    _invalidate_overlay_cache,
    get_setting,
    list_settings,
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
    assert out == {"key": "miner.daily_budget_base", "value": 5}


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
        "key": "daemon.pit_watch.enabled", "value": False}


async def test_write_setting_requires_because_only_when_the_spec_asks(
    actions: Actions,
) -> None:
    # pit_watch: requires_because=False — no `because` needed
    ok = await write_setting(actions.pool, "daemon.pit_watch.enabled", True, actor="operator")
    assert "error" not in ok
    # retention_heartbeat: requires_because=True — refused without one
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
    """A registered-but-untouched key reads its own env/default — the overlay is a
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
    """Opt-in per field (Thoth's own words, mail 10040): writing a registered knob
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


# --- secrets are never written through this door --------------------------------------

async def test_write_setting_refuses_a_secret_ref_outright(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No secret_ref is registered yet (thread f4498ab304e4's own follow-up), so this
    exercises the refusal path directly against a synthetic spec rather than waiting
    for a real one to exist."""
    from dataclasses import replace

    from src.config import settings_registry
    from src.orchestrator import settings_service as svc

    fake = replace(settings_registry.SETTINGS[0], key="test.fake_secret", type="secret_ref")
    monkeypatch.setattr(svc, "spec_by_key", lambda key: fake if key == fake.key else None)

    out = await svc.write_setting(actions.pool, fake.key, "value", actor="operator")
    assert "error" in out and "rotat" in out["error"].lower()

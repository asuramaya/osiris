"""deploy_guard — the code-ahead-of-schema alarm (thread e6f5556f), plus the reboot-is-a-
deploy confession (thread 489a39d0). LOUD ALARM, never a refusal (Thoth's ruling, DM 1339):
osiris-mcp is a fleet-wide single point of failure, so a false positive from a buggy check
refusing to serve would self-inflict a total outage strictly worse than the silent drift
either guard exists to catch.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator.deploy_guard import (
    alarm_schema_drift,
    alarm_unreviewed_boot,
    check_schema_drift,
    check_unreviewed_boot,
    schema_drift,
    unreviewed_boot,
)


def test_matching_versions_are_not_drift() -> None:
    assert schema_drift("0036", "0036") is None


def test_a_genuine_mismatch_is_named_both_ways() -> None:
    out = schema_drift("0034", "0036")
    assert out is not None and "0034" in out and "0036" in out


def test_either_side_unknown_is_not_drift() -> None:
    """None means 'don't know' — an empty alembic_version table, a script directory that
    failed to load. Never treated as a confident mismatch."""
    assert schema_drift(None, "0036") is None
    assert schema_drift("0034", None) is None
    assert schema_drift(None, None) is None


async def test_check_schema_drift_is_clean_on_a_freshly_migrated_db(actions: Actions) -> None:
    """conftest's own pg_dsn fixture migrates the test container to head — so the running
    code and the test DB's alembic_version must already agree."""
    assert await check_schema_drift(actions.pool) is None


async def test_check_schema_drift_finds_a_real_mismatch(actions: Actions) -> None:
    real = await actions.pool.fetchval("SELECT version_num FROM alembic_version")
    await actions.pool.execute("UPDATE alembic_version SET version_num = '0001'")
    try:
        out = await check_schema_drift(actions.pool)
        assert out is not None and "0001" in out
    finally:  # alembic_version isn't in conftest's per-test _TABLES reset — restore by hand
        await actions.pool.execute(
            "UPDATE alembic_version SET version_num = $1", real)


async def test_check_schema_drift_fails_open_on_a_broken_script_directory(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ANY error in the check — a bad alembic.ini, a script directory that won't load —
    degrades to None (unknown), never to a refusal."""
    import src.orchestrator.deploy_guard as guard

    monkeypatch.setattr(guard, "_REPO_ROOT", guard._REPO_ROOT / "no-such-path")
    assert await check_schema_drift(actions.pool) is None


async def test_alarm_opens_one_durable_thread_and_briefs_the_desk(actions: Actions) -> None:
    await alarm_schema_drift(actions.pool, "code expects '0036', DB is at '0034'",
                             service="osiris-worker")
    thread = await actions.pool.fetchrow(
        "SELECT a.value #>> '{}' AS summary, a.source_id FROM current_assertions a "
        "JOIN objects o ON o.id = a.object_id "
        "WHERE o.type = 'Thread' AND a.name = 'summary' "
        "AND a.value #>> '{}' ILIKE '%SCHEMA DRIFT%'")
    # `service` is deliberately OUT of the summary (thread 35c425f9 — see alarm_schema_drift's
    # own docstring): it still survives per-observation via the assertion's own source.
    assert thread is not None and "osiris-worker" not in thread["summary"]
    assert thread["source_id"] == "boot:osiris-worker"
    brief = await actions.pool.fetchrow(
        "SELECT body FROM fleet_messages WHERE from_agent = 'system:osiris-worker' "
        "AND to_project = 'operator'")
    assert brief is not None and "0034" in brief["body"]


async def test_alarm_stamps_a_real_severity_property(actions: Actions) -> None:
    """Ruling c5b184cd, thread d56e7073/#44 (the live-desk composition's drift_alarms leg):
    a real, filterable property, not a summary a reader has to text-match."""
    await alarm_schema_drift(actions.pool, "code expects '0036', DB is at '0034'",
                             service="osiris-worker")
    severity = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "JOIN objects o ON o.id = a.object_id "
        "WHERE o.type = 'Thread' AND a.name = 'severity' "
        "AND EXISTS (SELECT 1 FROM current_assertions s WHERE s.object_id = o.id "
        "  AND s.name = 'summary' AND s.value #>> '{}' ILIKE '%SCHEMA DRIFT%')")
    assert severity == "alarm"


async def test_alarm_is_idempotent_on_the_same_drift_text(actions: Actions) -> None:
    """A persistent drift across many restarts must not mint a duplicate Thread every boot —
    open_thread's own summary-hash idempotency is the mechanism, reused here, not rebuilt."""
    for _ in range(3):
        await alarm_schema_drift(actions.pool, "code expects '0036', DB is at '0034'",
                                 service="osiris-worker")
    count = await actions.pool.fetchval(
        "SELECT count(*) FROM objects o "
        "JOIN current_assertions a ON a.object_id = o.id "
        "WHERE o.type = 'Thread' AND a.name = 'summary' "
        "AND a.value #>> '{}' ILIKE '%SCHEMA DRIFT%0036%'")
    assert count == 1


async def test_two_services_reporting_the_same_drift_converge_on_one_thread(
    actions: Actions,
) -> None:
    """The regression this fixes (thread 35c425f9, the boot-listener double-record bug):
    osiris-mcp and osiris-worker each independently detect the identical alembic gap at
    their own boot. Before the fix, baking {service} into the Thread summary forked this
    into two separate objects; now both converge on one."""
    await alarm_schema_drift(actions.pool, "code expects '0036', DB is at '0034'",
                             service="osiris-mcp")
    await alarm_schema_drift(actions.pool, "code expects '0036', DB is at '0034'",
                             service="osiris-worker")
    # count(DISTINCT o.id), not count(*): the two services' testimony legitimately coexists
    # as TWO current_assertions rows on the SAME object (assert_property's own multi-source
    # corroboration) — a plain count(*) would double-count one object as two.
    count = await actions.pool.fetchval(
        "SELECT count(DISTINCT o.id) FROM objects o "
        "JOIN current_assertions a ON a.object_id = o.id "
        "WHERE o.type = 'Thread' AND a.name = 'summary' "
        "AND a.value #>> '{}' ILIKE '%SCHEMA DRIFT%0036%'")
    assert count == 1
    sources = {r["source_id"] for r in await actions.pool.fetch(
        "SELECT a.source_id FROM current_assertions a "
        "JOIN objects o ON o.id = a.object_id "
        "WHERE o.type = 'Thread' AND a.name = 'status'")}
    # both listeners' testimony survives as distinct sources on the SAME object — nothing
    # is lost by converging, only the duplicate Thread is gone
    assert sources == {"boot:osiris-mcp", "boot:osiris-worker"}


async def test_alarm_uses_a_generous_desk_dedup_window(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A schema drift can easily outlive send_message's own 600s default across restarts —
    this must pass a longer window so the desk isn't re-briefed on every single boot."""
    import src.orchestrator.mailbox as mailbox

    captured: dict[str, Any] = {}

    async def _fake_send(pool: Any, **kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"sent": 1}

    monkeypatch.setattr(mailbox, "send_message", _fake_send)
    await alarm_schema_drift(actions.pool, "drift", service="osiris-mcp")
    assert captured["dedup_window_secs"] == 86400


async def test_alarm_survives_the_desk_being_unreachable(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The durable Thread must land even if the mailbox write fails — the alarm's most
    important half (a graph fact the fleet can see) must not depend on the desk succeeding."""
    import src.orchestrator.mailbox as mailbox

    async def _boom(*a: Any, **k: Any) -> None:
        raise RuntimeError("mailbox down")

    monkeypatch.setattr(mailbox, "send_message", _boom)
    await alarm_schema_drift(actions.pool, "drift-during-outage", service="osiris-worker")
    thread = await actions.pool.fetchval(
        "SELECT count(*) FROM objects o JOIN current_assertions a ON a.object_id = o.id "
        "WHERE o.type = 'Thread' AND a.name = 'summary' "
        "AND a.value #>> '{}' ILIKE '%drift-during-outage%'")
    assert thread == 1


# --- wiring: both services actually call the guard at their own boot -----------------------

async def test_mcp_boot_check_is_silent_on_a_matched_schema(actions: Actions) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        await srv._boot_check()  # must not raise, and mints nothing on a clean match
    finally:
        srv._pool = saved_pool
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type = 'Thread'") == 0


async def test_mcp_boot_check_alarms_on_a_real_mismatch(actions: Actions) -> None:
    from src import mcp_server as srv

    real = await actions.pool.fetchval("SELECT version_num FROM alembic_version")
    await actions.pool.execute("UPDATE alembic_version SET version_num = '0001'")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        await srv._boot_check()
    finally:
        srv._pool = saved_pool
        # alembic_version isn't in conftest's per-test _TABLES reset — restore by hand
        await actions.pool.execute("UPDATE alembic_version SET version_num = $1", real)
    thread = await actions.pool.fetchval(
        "SELECT count(*) FROM objects o JOIN current_assertions a ON a.object_id = o.id "
        "WHERE o.type = 'Thread' AND a.name = 'summary' "
        "AND a.value #>> '{}' ILIKE '%SCHEMA DRIFT%0001%'")
    assert thread == 1


def test_worker_startup_imports_the_guard() -> None:
    """A light presence check, same spirit as the cron-registration tests elsewhere in this
    suite — proves the wiring exists without needing a full CascadeContext/redis boot."""
    import inspect

    from src.workers import arq_worker

    src_text = inspect.getsource(arq_worker.startup)
    assert "check_schema_drift" in src_text and "alarm_schema_drift" in src_text


# --- the reboot-is-a-deploy confession (thread 489a39d0) -----------------------------------

def test_matching_heads_are_not_an_unreviewed_boot() -> None:
    assert unreviewed_boot("abc123", "abc123") is None


def test_a_genuine_head_mismatch_is_named_both_ways() -> None:
    out = unreviewed_boot("abc123", "def456")
    assert out is not None and "abc123" in out and "def456" in out


def test_either_side_unknown_is_not_an_unreviewed_boot() -> None:
    """A fresh box that never once ran `osiris deploy` (no cursor yet) is not evidence of an
    unreviewed reboot — same 'don't know' discipline as schema_drift's own null handling."""
    assert unreviewed_boot(None, "def456") is None
    assert unreviewed_boot("abc123", None) is None
    assert unreviewed_boot(None, None) is None


async def test_check_unreviewed_boot_is_clean_when_the_ledger_matches_running_head(
    actions: Actions,
) -> None:
    import src.orchestrator.deploy_guard as guard
    from src.orchestrator.monitor import set_cursor

    running = guard._git_head(guard._REPO_ROOT)
    assert running is not None  # this test runs inside a real git checkout
    await set_cursor(actions.pool, guard._DEPLOY_CURSOR_KEY, running)
    assert await check_unreviewed_boot(actions.pool) is None


async def test_check_unreviewed_boot_is_clean_on_a_box_with_no_deploy_history(
    actions: Actions,
) -> None:
    """No watermark ever written (conftest truncates `watermarks` per test) — unknown, never
    a false alarm on a fresh box or a freshly-added guard."""
    assert await check_unreviewed_boot(actions.pool) is None


async def test_check_unreviewed_boot_finds_a_real_mismatch(actions: Actions) -> None:
    import src.orchestrator.deploy_guard as guard
    from src.orchestrator.monitor import set_cursor

    await set_cursor(actions.pool, guard._DEPLOY_CURSOR_KEY, "0" * 40)
    out = await check_unreviewed_boot(actions.pool)
    assert out is not None and "0" * 40 in out


async def test_check_unreviewed_boot_fails_open_when_git_cannot_run(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.orchestrator.deploy_guard as guard
    from src.orchestrator.monitor import set_cursor

    await set_cursor(actions.pool, guard._DEPLOY_CURSOR_KEY, "0" * 40)
    monkeypatch.setattr(guard, "_REPO_ROOT", guard._REPO_ROOT / "no-such-path")
    assert await check_unreviewed_boot(actions.pool) is None


async def test_reboot_alarm_opens_one_durable_thread_and_briefs_the_desk(
    actions: Actions,
) -> None:
    await alarm_unreviewed_boot(
        actions.pool, "running HEAD 'aaa' was never recorded by `osiris deploy`",
        service="osiris-worker")
    thread = await actions.pool.fetchrow(
        "SELECT a.value #>> '{}' AS summary, a.source_id FROM current_assertions a "
        "JOIN objects o ON o.id = a.object_id "
        "WHERE o.type = 'Thread' AND a.name = 'summary' "
        "AND a.value #>> '{}' ILIKE '%UNREVIEWED BOOT%'")
    assert thread is not None and "osiris-worker" not in thread["summary"]
    assert thread["source_id"] == "boot:osiris-worker"
    brief = await actions.pool.fetchrow(
        "SELECT body FROM fleet_messages WHERE from_agent = 'system:osiris-worker' "
        "AND to_project = 'operator'")
    assert brief is not None and "aaa" in brief["body"]


async def test_reboot_alarm_is_idempotent_on_the_same_drift_text(actions: Actions) -> None:
    for _ in range(3):
        await alarm_unreviewed_boot(actions.pool, "running HEAD 'aaa' was never recorded",
                                    service="osiris-worker")
    count = await actions.pool.fetchval(
        "SELECT count(*) FROM objects o JOIN current_assertions a ON a.object_id = o.id "
        "WHERE o.type = 'Thread' AND a.name = 'summary' "
        "AND a.value #>> '{}' ILIKE '%UNREVIEWED BOOT%aaa%'")
    assert count == 1


async def test_two_services_confessing_the_same_unreviewed_head_converge_on_one_thread(
    actions: Actions,
) -> None:
    """Same lesson as thread 35c425f9, applied from the start: osiris-mcp and osiris-worker
    both booting on the identical unrecorded HEAD must not fork into two Threads."""
    await alarm_unreviewed_boot(actions.pool, "running HEAD 'aaa' was never recorded",
                                service="osiris-mcp")
    await alarm_unreviewed_boot(actions.pool, "running HEAD 'aaa' was never recorded",
                                service="osiris-worker")
    # count(DISTINCT o.id) — see the schema-drift version of this test for why a plain
    # count(*) would double-count one object's two-source testimony as two objects.
    count = await actions.pool.fetchval(
        "SELECT count(DISTINCT o.id) FROM objects o JOIN current_assertions a "
        "ON a.object_id = o.id "
        "WHERE o.type = 'Thread' AND a.name = 'summary' "
        "AND a.value #>> '{}' ILIKE '%UNREVIEWED BOOT%aaa%'")
    assert count == 1


async def test_reboot_alarm_survives_the_desk_being_unreachable(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.orchestrator.mailbox as mailbox

    async def _boom(*a: Any, **k: Any) -> None:
        raise RuntimeError("mailbox down")

    monkeypatch.setattr(mailbox, "send_message", _boom)
    await alarm_unreviewed_boot(actions.pool, "drift-during-outage", service="osiris-worker")
    thread = await actions.pool.fetchval(
        "SELECT count(*) FROM objects o JOIN current_assertions a ON a.object_id = o.id "
        "WHERE o.type = 'Thread' AND a.name = 'summary' "
        "AND a.value #>> '{}' ILIKE '%drift-during-outage%'")
    assert thread == 1


# --- wiring: both services actually call the reboot guard at their own boot too ------------

async def test_mcp_boot_check_alarms_on_an_unrecorded_head(actions: Actions) -> None:
    import src.orchestrator.deploy_guard as guard
    from src import mcp_server as srv
    from src.orchestrator.monitor import set_cursor

    await set_cursor(actions.pool, guard._DEPLOY_CURSOR_KEY, "0" * 40)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        await srv._boot_check()
    finally:
        srv._pool = saved_pool
    thread = await actions.pool.fetchval(
        "SELECT count(*) FROM objects o JOIN current_assertions a ON a.object_id = o.id "
        "WHERE o.type = 'Thread' AND a.name = 'summary' "
        "AND a.value #>> '{}' ILIKE '%UNREVIEWED BOOT%'")
    assert thread == 1


def test_worker_startup_imports_the_reboot_guard_too() -> None:
    import inspect

    from src.workers import arq_worker

    src_text = inspect.getsource(arq_worker.startup)
    assert "check_unreviewed_boot" in src_text and "alarm_unreviewed_boot" in src_text


# --- the ledger's write side (src/cli.py's own _real_record_deploy) ------------------------

async def test_real_record_deploy_writes_the_watermark(actions: Actions) -> None:
    import src.orchestrator.deploy_guard as guard
    from src.cli import _real_record_deploy
    from src.orchestrator.monitor import get_cursor

    head = await _real_record_deploy(actions.pool, Path.cwd())
    assert head is not None
    assert await get_cursor(actions.pool, guard._DEPLOY_CURSOR_KEY) == head


async def test_real_record_deploy_is_a_noop_off_a_non_git_root(actions: Actions) -> None:
    import src.orchestrator.deploy_guard as guard
    from src.cli import _real_record_deploy
    from src.orchestrator.monitor import get_cursor

    head = await _real_record_deploy(actions.pool, Path("/no/such/git/repo"))
    assert head is None
    assert await get_cursor(actions.pool, guard._DEPLOY_CURSOR_KEY) is None

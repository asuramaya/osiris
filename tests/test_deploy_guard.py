"""deploy_guard — the code-ahead-of-schema alarm (thread e6f5556f), plus the reboot-is-a-
deploy confession (thread 489a39d0). LOUD ALARM, never a refusal (Thoth's ruling, DM 1339):
osiris-mcp is a fleet-wide single point of failure, so a false positive from a buggy check
refusing to serve would self-inflict a total outage strictly worse than the silent drift
either guard exists to catch.
"""
from __future__ import annotations

import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator.deploy_guard import (
    _REPO_ROOT,
    _is_ancestor,
    alarm_schema_drift,
    alarm_unreviewed_boot,
    audit_graph_merge_claims,
    check_diverged_since_last_deploy,
    check_schema_drift,
    check_unreviewed_boot,
    diverged_since_last_deploy,
    landing_audit,
    local_ref_hygiene,
    merge_claim_hygiene,
    origin_visibility,
    schema_drift,
    stale_unmerged_branches,
    unreviewed_boot,
    venv_import_hygiene,
)
from src.parsers.base import EvidenceClass


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


def test_a_known_prior_revision_reads_as_the_benign_code_ahead_shape() -> None:
    """Decision 8d3f5e2d: db_version_known=True (or the default None) is the ORDINARY,
    benign transient — the DB just hasn't run a migration the tree already has."""
    out = schema_drift("0034", "0036", db_version_known=True)
    assert out is not None and out.startswith("CODE_AHEAD_OF_DB")
    out_default = schema_drift("0034", "0036")
    assert out_default is not None and out_default.startswith("CODE_AHEAD_OF_DB")


def test_an_unrecognized_revision_reads_as_the_blocking_db_ahead_shape() -> None:
    """db_version_known=False is the ONE population that actually blocks a deploy — the
    tree has never heard of this revision at all, meaning another branch's migration ran
    against shared DATABASE_URL before merging (decision 8d3f5e2d). Direction must be named,
    not just the bare mismatch — a caller reading only 'ahead of (or behind)' cannot tell
    these two populations apart, which is the exact defect this fix removes."""
    out = schema_drift("0099_unmerged", "0045", db_version_known=False)
    assert out is not None
    assert out.startswith("DB_AHEAD_OF_TREE")
    assert "8d3f5e2d" in out
    assert "0099_unmerged" in out
    assert "Do NOT run" in out


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
        # '0001' is a REAL, known revision in this tree's own chain -- benign, the DB is
        # simply behind, not carrying a revision this tree has never heard of.
        assert out.startswith("CODE_AHEAD_OF_DB")
    finally:  # alembic_version isn't in conftest's per-test _TABLES reset — restore by hand
        await actions.pool.execute(
            "UPDATE alembic_version SET version_num = $1", real)


async def test_check_schema_drift_names_a_revision_this_tree_never_heard_of(
    actions: Actions,
) -> None:
    """Decision 8d3f5e2d's live shape, reproduced: the DB carries a revision no script in
    this tree's own alembic/versions/ defines at all — the class that blocked a deploy on
    2026-08-13, caught by luck at deploy time. This check now names it directly."""
    real = await actions.pool.fetchval("SELECT version_num FROM alembic_version")
    await actions.pool.execute(
        "UPDATE alembic_version SET version_num = '0099_unmerged_branch_revision'")
    try:
        out = await check_schema_drift(actions.pool)
        assert out is not None
        assert out.startswith("DB_AHEAD_OF_TREE")
        assert "0099_unmerged_branch_revision" in out
    finally:
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
        running_head="aaa", service="osiris-worker")
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


async def test_reboot_alarm_declares_its_own_repo_gap_honestly(actions: Actions) -> None:
    """Thoth msg 5858 — the hatch's first real user: this alarm has no ctx and no mounted
    caller, so it can never satisfy a repo requirement the way a real agent write can. It
    must DECLARE that gap via unlinked_because rather than land as a silent orphan, and the
    declaration must survive even while required_link_kinds ships empty (decision b792c039
    — the gate stays dark; this is an honest confession, not enforcement)."""
    await alarm_unreviewed_boot(
        actions.pool, "running HEAD 'aaa' was never recorded by `osiris deploy`",
        running_head="aaa", service="osiris-worker")
    reason = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o "
        "ON o.id = a.object_id WHERE o.type = 'Thread' AND a.name = 'unlinked_because' "
        "AND a.source_id = 'boot:osiris-worker'")
    assert reason == "service-scoped claim: a deploy-state alarm has no SoftwareProject"


async def test_reboot_alarm_names_src_root_in_the_brief_not_the_thread(
    actions: Actions,
) -> None:
    """Task #180 piece 2 (msg 5253): `src_root` rides beside `running_head` in the operator
    brief — the fact that would have surfaced the week-long Imhotep-worktree drift on day
    one — but stays OUT of the Thread's own dedup identity, same discipline as `service`."""
    await alarm_unreviewed_boot(
        actions.pool, "running HEAD 'aaa' was never recorded by `osiris deploy`",
        running_head="aaa", service="osiris-worker",
        src_root="/home/asuramaya/code/osiris/.claude/worktrees/imhotep")
    thread = await actions.pool.fetchrow(
        "SELECT a.value #>> '{}' AS summary FROM current_assertions a "
        "JOIN objects o ON o.id = a.object_id "
        "WHERE o.type = 'Thread' AND a.name = 'summary' "
        "AND a.value #>> '{}' ILIKE '%UNREVIEWED BOOT%aaa%'")
    assert thread is not None and "imhotep" not in thread["summary"]
    brief = await actions.pool.fetchrow(
        "SELECT body FROM fleet_messages WHERE from_agent = 'system:osiris-worker' "
        "AND to_project = 'operator'")
    assert brief is not None and "imhotep" in brief["body"]


async def test_reboot_alarm_omitted_src_root_still_works(actions: Actions) -> None:
    """Existing callers with no `src_root` to hand (the pre-#180-piece-2 shape) keep working
    unchanged — the parameter is optional and appended-only."""
    await alarm_unreviewed_boot(actions.pool, "running HEAD 'aaa' was never recorded",
                                running_head="aaa", service="osiris-worker")
    brief = await actions.pool.fetchrow(
        "SELECT body FROM fleet_messages WHERE from_agent = 'system:osiris-worker' "
        "AND to_project = 'operator'")
    assert brief is not None


async def test_reboot_alarm_is_idempotent_on_the_same_drift_text(actions: Actions) -> None:
    for _ in range(3):
        await alarm_unreviewed_boot(actions.pool, "running HEAD 'aaa' was never recorded",
                                    running_head="aaa", service="osiris-worker")
    count = await actions.pool.fetchval(
        "SELECT count(*) FROM objects o JOIN current_assertions a ON a.object_id = o.id "
        "WHERE o.type = 'Thread' AND a.name = 'summary' "
        "AND a.value #>> '{}' ILIKE '%UNREVIEWED BOOT%aaa%'")
    assert count == 1


async def test_reboot_alarm_dedups_on_running_head_alone_across_different_watermarks(
    actions: Actions,
) -> None:
    """Decision 8a830336 — the 76-thread specimen: the SAME unreviewed commit confessing
    against a DIFFERENT `last_deployed` watermark each restart (a normal thing to happen
    across several real deploys) must still converge on ONE Thread, not one per distinct
    drift text. Only `running_head` is the canonical identity now."""
    await alarm_unreviewed_boot(
        actions.pool, "running HEAD 'aaa' was never recorded (last recorded deploy: 'old1')",
        running_head="aaa", service="osiris-worker")
    await alarm_unreviewed_boot(
        actions.pool, "running HEAD 'aaa' was never recorded (last recorded deploy: 'old2')",
        running_head="aaa", service="osiris-worker")
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
                                running_head="aaa", service="osiris-mcp")
    await alarm_unreviewed_boot(actions.pool, "running HEAD 'aaa' was never recorded",
                                running_head="aaa", service="osiris-worker")
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
    await alarm_unreviewed_boot(actions.pool, "drift-during-outage", running_head="bbb",
                                service="osiris-worker")
    thread = await actions.pool.fetchval(
        "SELECT count(*) FROM objects o JOIN current_assertions a ON a.object_id = o.id "
        "WHERE o.type = 'Thread' AND a.name = 'summary' "
        "AND a.value #>> '{}' ILIKE '%UNREVIEWED BOOT%bbb%'")
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


# --- THE ONE REAL WORKER GATE (decision 8a830336): osiris_worker_role, mirroring
# osiris_mcp_transport's own non-inferred shape — an ad hoc/local `arq` invocation left it
# unset and confessed truthfully but uselessly against the shared graph, 76 times fleet-wide,
# because arq_worker.startup() had no equivalent to mcp_server.main()'s own transport gate. ---

async def test_worker_startup_gate_mints_nothing_without_the_role_var(
    actions: Actions, redis_url: str, pg_dsn: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.orchestrator.deploy_guard as guard
    from src.orchestrator.monitor import set_cursor
    from src.workers.arq_worker import shutdown, startup

    await set_cursor(actions.pool, guard._DEPLOY_CURSOR_KEY, "0" * 40)  # guaranteed mismatch
    monkeypatch.setenv("DATABASE_URL", pg_dsn)
    monkeypatch.setenv("REDIS_URL", redis_url)
    monkeypatch.delenv("OSIRIS_WORKER_ROLE", raising=False)
    ctx: dict[str, Any] = {}
    await startup(ctx)
    try:
        count = await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type = 'Thread'")
        assert count == 0
    finally:
        await shutdown(ctx)


async def test_worker_startup_gate_runs_and_dedups_with_the_role_var_set(
    actions: Actions, redis_url: str, pg_dsn: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The var present is what the real osiris-worker.service unit sets — the guard must
    actually run (mints one Thread), same claim `test_mcp_boot_check_alarms_on_an_unrecorded_
    head` already carries for the sibling service."""
    import src.orchestrator.deploy_guard as guard
    from src.orchestrator.monitor import set_cursor
    from src.workers.arq_worker import shutdown, startup

    await set_cursor(actions.pool, guard._DEPLOY_CURSOR_KEY, "0" * 40)
    monkeypatch.setenv("DATABASE_URL", pg_dsn)
    monkeypatch.setenv("REDIS_URL", redis_url)
    monkeypatch.setenv("OSIRIS_WORKER_ROLE", "primary")
    ctx: dict[str, Any] = {}
    await startup(ctx)
    try:
        thread = await actions.pool.fetchval(
            "SELECT count(*) FROM objects o JOIN current_assertions a ON a.object_id = o.id "
            "WHERE o.type = 'Thread' AND a.name = 'summary' "
            "AND a.value #>> '{}' ILIKE '%UNREVIEWED BOOT%'")
        assert thread == 1
    finally:
        await shutdown(ctx)


# --- the ref-race detector (thread 771366d1: two agents moved main/composer out from under
# each other tonight; nothing noticed until a by-hand branch survey). DISTINCT from
# unreviewed_boot: that one fires on ANY head difference (including a normal fast-forward
# between deploys), which would be useless noise here — every deploy after the first one has
# a different running head than the last. This fires ONLY when the last-deployed head fell
# OUT of the branch's own ancestry, the specific shape of a rewrite/reset/force-move. ---------


def test_matching_heads_are_not_a_divergence() -> None:
    assert diverged_since_last_deploy("abc", "abc", is_ancestor=None) is None


def test_a_normal_fast_forward_is_not_a_divergence() -> None:
    """The common case: every deploy after the first advances past the last one. is_ancestor
    True means the old head is still in this branch's own past — ordinary progress."""
    assert diverged_since_last_deploy("new", "old", is_ancestor=True) is None


def test_a_genuine_rewrite_is_named_both_ways() -> None:
    out = diverged_since_last_deploy("new", "old", is_ancestor=False)
    assert out is not None and "old" in out and "new" in out


def test_either_side_unknown_is_not_a_divergence() -> None:
    assert diverged_since_last_deploy(None, "old", is_ancestor=False) is None
    assert diverged_since_last_deploy("new", None, is_ancestor=False) is None


def test_unresolvable_ancestry_is_not_a_divergence() -> None:
    """is_ancestor=None means the check itself could not run (bad sha, git failure) — 'don't
    know', same fail-open law as every other comparison in this module. A false alarm here
    is exactly the noise that teaches a reader to stop looking at a real one."""
    assert diverged_since_last_deploy("new", "old", is_ancestor=None) is None


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def _head(repo: Path) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


def _current_branch(repo: Path) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), "branch", "--show-current"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


def _commit(repo: Path, msg: str) -> str:
    _git(repo, "commit", "--allow-empty", "-q", "-m", msg)
    return _head(repo)


@pytest.fixture
def small_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@test")
    _git(repo, "config", "user.name", "test")
    return repo


def test_is_ancestor_true_for_a_real_fast_forward(small_repo: Path) -> None:
    a = _commit(small_repo, "a")
    b = _commit(small_repo, "b")
    assert _is_ancestor(small_repo, a, b) is True


def test_is_ancestor_false_for_a_real_rewrite(small_repo: Path) -> None:
    """Same shape as tonight's incident: `a` was once HEAD, then the branch got reset to a
    sibling commit that does not descend from it."""
    a = _commit(small_repo, "a")
    _git(small_repo, "checkout", "-q", "--orphan", "unrelated")
    c = _commit(small_repo, "c")
    assert _is_ancestor(small_repo, a, c) is False


def test_is_ancestor_none_for_an_unresolvable_sha(small_repo: Path) -> None:
    b = _commit(small_repo, "b")
    assert _is_ancestor(small_repo, "0" * 40, b) is None


def test_is_ancestor_none_off_a_non_git_root(tmp_path: Path) -> None:
    assert _is_ancestor(tmp_path, "a", "b") is None


async def test_check_diverged_is_clean_when_the_ledger_matches_running_head(
    actions: Actions,
) -> None:
    import src.orchestrator.deploy_guard as guard
    from src.orchestrator.monitor import set_cursor

    running = guard._git_head(guard._REPO_ROOT)
    assert running is not None
    await set_cursor(actions.pool, guard._DEPLOY_CURSOR_KEY, running)
    assert await check_diverged_since_last_deploy(actions.pool) is None


async def test_check_diverged_is_clean_on_a_box_with_no_deploy_history(
    actions: Actions,
) -> None:
    assert await check_diverged_since_last_deploy(actions.pool) is None


async def test_check_diverged_fails_open_when_git_cannot_run(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.orchestrator.deploy_guard as guard
    from src.orchestrator.monitor import set_cursor

    await set_cursor(actions.pool, guard._DEPLOY_CURSOR_KEY, "0" * 40)
    monkeypatch.setattr(guard, "_REPO_ROOT", guard._REPO_ROOT / "no-such-path")
    assert await check_diverged_since_last_deploy(actions.pool) is None


async def test_check_diverged_finds_a_real_rewrite(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, small_repo: Path,
) -> None:
    """End-to-end through the IO wrapper against a real throwaway repo, not just the pure
    function — the exact shape of tonight's incident: `a` was recorded as deployed, then the
    branch got rewritten onto an unrelated history and never re-deployed before something
    else tried to trust it."""
    import src.orchestrator.deploy_guard as guard
    from src.orchestrator.monitor import set_cursor

    a = _commit(small_repo, "a")
    _git(small_repo, "checkout", "-q", "--orphan", "unrelated")
    c = _commit(small_repo, "c")
    monkeypatch.setattr(guard, "_REPO_ROOT", small_repo)
    await set_cursor(actions.pool, guard._DEPLOY_CURSOR_KEY, a)
    out = await check_diverged_since_last_deploy(actions.pool)
    assert out is not None and a in out and c in out


async def test_check_diverged_is_clean_on_a_real_fast_forward(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, small_repo: Path,
) -> None:
    """The common case must stay silent end-to-end too, not just in the pure function."""
    import src.orchestrator.deploy_guard as guard
    from src.orchestrator.monitor import set_cursor

    a = _commit(small_repo, "a")
    _commit(small_repo, "b")
    monkeypatch.setattr(guard, "_REPO_ROOT", small_repo)
    await set_cursor(actions.pool, guard._DEPLOY_CURSOR_KEY, a)
    assert await check_diverged_since_last_deploy(actions.pool) is None


# --- wiring: osiris deploy actually calls the ref-race detector (src/cli.py) ---------------
# wait_for_health/wait_for_smoke default to REAL bounded pollers (120s/30s ceilings, real
# network round-trips against the live console/MCP) — these tests are exercising the
# divergence-guard wiring, not that wait, so a restart-succeeds path injects fast fakes.

async def _fake_wait_for_health() -> tuple[bool, float]:
    return True, 0.0


async def _fake_wait_for_smoke() -> tuple[list[str], float]:
    return [], 0.0


async def test_cmd_deploy_warns_but_never_refuses_on_a_real_divergence(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, small_repo: Path,
) -> None:
    import io
    from contextlib import redirect_stdout

    import src.orchestrator.deploy_guard as guard
    from src.cli import cmd_deploy
    from src.orchestrator.monitor import set_cursor

    a = _commit(small_repo, "a")
    _git(small_repo, "checkout", "-q", "--orphan", "unrelated")
    c = _commit(small_repo, "c")
    # _REPO_ROOT deliberately points somewhere ELSE (a live false positive, 2026-08-04:
    # this house runs five worktrees, each its own copy of deploy_guard.py on disk, and
    # _REPO_ROOT resolves to whichever one Python happened to import from — not
    # necessarily the repo being deployed). The load-bearing fact this test pins is that
    # `cmd_deploy` no longer trusts that ambient guess: it passes ITS OWN resolved
    # `repo_root` (small_repo, below) through explicitly, so the divergence is still
    # found even though _REPO_ROOT names a repo with no history at all.
    monkeypatch.setattr(guard, "_REPO_ROOT", small_repo.parent / "not-the-deploy-target")
    await set_cursor(actions.pool, guard._DEPLOY_CURSOR_KEY, a)

    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_deploy(repo_root=small_repo, git_status=lambda root: [],
                               restart=_restart, pool=actions.pool,
                               wait_for_health=_fake_wait_for_health,
                               wait_for_smoke=_fake_wait_for_smoke)
    assert "WARNING: HISTORY DIVERGED SINCE THE LAST DEPLOY" in buf.getvalue()
    assert a in buf.getvalue() and c in buf.getvalue()
    # THE LOAD-BEARING ASSERTION (577988ed: never refuse on a check that can false-positive):
    # a real divergence PRINTS, it does not change the exit code — restart still ran and the
    # deploy completed on its own ordinary merits, exactly as if the warning were absent.
    assert out in (0, 1)


async def test_cmd_deploy_ignores_an_unrelated_repo_roots_ambient_head(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, small_repo: Path, tmp_path: Path,
) -> None:
    """THE EXACT LIVE FALSE POSITIVE, pinned directly (Thoth's catch, 2026-08-04, the
    deploy immediately before the second history rewrite): the module-level `_REPO_ROOT`
    names a DIFFERENT repo whose HEAD has ALSO diverged from the ledger — if `cmd_deploy`
    ever silently fell back to that ambient guess instead of its own resolved root, this
    would warn on a deploy that is, on its OWN actual repo, a clean fast-forward."""
    import io
    from contextlib import redirect_stdout

    import src.orchestrator.deploy_guard as guard
    from src.cli import cmd_deploy
    from src.orchestrator.monitor import set_cursor

    # small_repo (the ACTUAL deploy target): a clean fast-forward, a -> b.
    a = _commit(small_repo, "a")
    _commit(small_repo, "b")
    await set_cursor(actions.pool, guard._DEPLOY_CURSOR_KEY, a)

    # an UNRELATED repo that _REPO_ROOT happens to resolve to, itself genuinely
    # diverged — if the guard reads THIS by accident, it would warn wrongly.
    decoy = tmp_path / "decoy"
    decoy.mkdir()
    _git(decoy, "init", "-q")
    _git(decoy, "config", "user.email", "test@test")
    _git(decoy, "config", "user.name", "test")
    _commit(decoy, "x")
    _git(decoy, "checkout", "-q", "--orphan", "unrelated")
    _commit(decoy, "y")
    monkeypatch.setattr(guard, "_REPO_ROOT", decoy)

    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    buf = io.StringIO()
    with redirect_stdout(buf):
        await cmd_deploy(repo_root=small_repo, git_status=lambda root: [],
                         restart=_restart, pool=actions.pool,
                         wait_for_health=_fake_wait_for_health,
                         wait_for_smoke=_fake_wait_for_smoke)
    assert "HISTORY DIVERGED" not in buf.getvalue()


async def test_cmd_deploy_is_silent_on_a_normal_deploy(
    actions: Actions, tmp_path: Path,
) -> None:
    """No prior deploy recorded (conftest truncates `watermarks` per test) — must not print
    the divergence warning on an ordinary first-ever deploy."""
    import io
    from contextlib import redirect_stdout

    from src.cli import cmd_deploy

    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    buf = io.StringIO()
    with redirect_stdout(buf):
        await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                         pool=actions.pool, wait_for_health=_fake_wait_for_health,
                         wait_for_smoke=_fake_wait_for_smoke)
    assert "HISTORY DIVERGED" not in buf.getvalue()


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


# --- origin_visibility (the read-side alarm, 2026-08-15 incident, ruling 2fc98818) ---------

class _FakeResponse:
    def __init__(self, status_code: int, json_body: dict[str, Any] | None = None) -> None:
        self.status_code = status_code
        self._json = json_body or {}

    def json(self) -> dict[str, Any]:
        return self._json


class _FakeAsyncClient:
    def __init__(self, outcome: _FakeResponse | Exception) -> None:
        self._outcome = outcome

    async def __aenter__(self) -> _FakeAsyncClient:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def get(self, url: str) -> _FakeResponse:
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return self._outcome


def _stub_ls_remote_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    """No branches, no error — isolates the GitHub-visibility leg from a real network
    `ls-remote` against a URL that was never a real reachable remote."""
    import src.orchestrator.deploy_guard as guard

    real_run = subprocess.run

    def _fake_run(argv: list[str], **kwargs: Any) -> Any:
        if argv[:2] == ["git", "ls-remote"]:
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")
        return real_run(argv, **kwargs)

    monkeypatch.setattr(guard.subprocess, "run", _fake_run)


async def test_origin_visibility_reports_no_remote_when_none_configured(
    small_repo: Path,
) -> None:
    assert await origin_visibility(small_repo) == "origin: no remote configured"


async def test_origin_visibility_lists_real_branches_from_a_local_remote(
    small_repo: Path, tmp_path: Path,
) -> None:
    """A REAL `git ls-remote` against a real local repo, not a mock — the branch-listing leg
    needs no network to prove it reads the true ref set, same discipline as `_is_ancestor`'s
    own real-repo tests in this file."""
    upstream = tmp_path / "upstream"
    upstream.mkdir()
    _git(upstream, "init", "-q")
    _git(upstream, "config", "user.email", "test@test")
    _git(upstream, "config", "user.name", "test")
    _commit(upstream, "a")
    _git(upstream, "checkout", "-q", "-b", "feature")
    _commit(upstream, "b")
    _git(small_repo, "remote", "add", "origin", str(upstream))

    note = await origin_visibility(small_repo)
    assert "2 branch(es) reachable" in note
    assert "feature" in note
    assert "unrecognizable github.com owner/repo" in note or "unknown" in note


async def test_origin_visibility_fails_open_when_ls_remote_errors(small_repo: Path) -> None:
    _git(small_repo, "remote", "add", "origin", "/no/such/path/at/all")
    note = await origin_visibility(small_repo)
    assert "UNKNOWN" in note
    assert "branches/visibility" in note


async def test_origin_visibility_reports_public_from_the_github_api(
    small_repo: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.orchestrator.deploy_guard as guard

    _git(small_repo, "remote", "add", "origin", "https://github.com/testowner/testrepo.git")
    _stub_ls_remote_clean(monkeypatch)
    monkeypatch.setattr(
        guard, "httpx",
        type("_H", (), {"AsyncClient": lambda **_: _FakeAsyncClient(
            _FakeResponse(200, {"private": False}))}))

    assert await origin_visibility(small_repo) == "origin: PUBLIC, 0 branch(es) reachable"


async def test_origin_visibility_reads_private_from_a_404(
    small_repo: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.orchestrator.deploy_guard as guard

    _git(small_repo, "remote", "add", "origin", "git@github.com:testowner/testrepo.git")
    _stub_ls_remote_clean(monkeypatch)
    monkeypatch.setattr(
        guard, "httpx",
        type("_H", (), {"AsyncClient": lambda **_: _FakeAsyncClient(_FakeResponse(404))}))

    note = await origin_visibility(small_repo)
    assert "private-or-nonexistent" in note


async def test_origin_visibility_fails_open_when_the_github_api_is_unreachable(
    small_repo: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.orchestrator.deploy_guard as guard

    _git(small_repo, "remote", "add", "origin", "https://github.com/testowner/testrepo.git")
    _stub_ls_remote_clean(monkeypatch)
    monkeypatch.setattr(
        guard, "httpx",
        type("_H", (), {"AsyncClient": lambda **_: _FakeAsyncClient(
            ConnectionError("no network"))}))

    note = await origin_visibility(small_repo)
    assert note.startswith("origin: unknown (")


# --- local_ref_hygiene (the --mirror exposure read, same 2026-08-15 incident) ---------------

async def test_local_ref_hygiene_is_clean_on_an_ordinary_repo(small_repo: Path) -> None:
    _commit(small_repo, "a")
    note = await local_ref_hygiene(small_repo)
    assert "no refs outside refs/heads, refs/tags, or refs/remotes" in note
    assert "extra reachable" not in note


async def test_local_ref_hygiene_names_a_stray_ref(small_repo: Path) -> None:
    a = _commit(small_repo, "a")
    _git(small_repo, "update-ref", "refs/temp-main-old", a)
    note = await local_ref_hygiene(small_repo)
    assert "refs/temp-main-old" in note
    assert "1 ref(s) outside refs/heads|tags|remotes" in note


async def test_local_ref_hygiene_does_not_flag_ordinary_remote_tracking_refs(
    small_repo: Path,
) -> None:
    """Caught live while dogfooding against the real osiris checkout: `refs/remotes/*` is
    present in EVERY clone that has ever fetched — flagging it as stray would drown the
    real signal in noise on every single ordinary checkout, not just this repo's own."""
    a = _commit(small_repo, "a")
    _git(small_repo, "update-ref", "refs/remotes/origin/main", a)
    note = await local_ref_hygiene(small_repo)
    assert "no refs outside refs/heads, refs/tags, or refs/remotes" in note
    assert "extra reachable" not in note


async def test_local_ref_hygiene_catches_a_commit_count_mismatch(small_repo: Path) -> None:
    """The exact incident signature: a stray ref keeping OLD history alive that a mirror
    push would carry, invisible to any check of the real branches' own content."""
    _git(small_repo, "checkout", "-q", "--orphan", "stray-history")
    stray_commit = _commit(small_repo, "history nobody meant to keep publishing")
    _git(small_repo, "update-ref", "refs/temp-main-old", stray_commit)
    _git(small_repo, "checkout", "-q", "--orphan", "main")
    _commit(small_repo, "the real, intended history")
    _git(small_repo, "branch", "-D", "stray-history")

    note = await local_ref_hygiene(small_repo)
    assert "extra reachable only via a stray ref" in note
    assert "1 extra" in note


async def test_local_ref_hygiene_includes_tags_as_intended(small_repo: Path) -> None:
    a = _commit(small_repo, "a")
    _git(small_repo, "tag", "v1.0.0", a)
    note = await local_ref_hygiene(small_repo)
    assert "no refs outside refs/heads, refs/tags, or refs/remotes" in note
    assert "extra reachable" not in note


async def test_local_ref_hygiene_fails_open_on_a_non_git_root(tmp_path: Path) -> None:
    note = await local_ref_hygiene(tmp_path)
    assert "UNKNOWN" in note


# ── merge_claim_hygiene (Sekhmet's fd3a703 specimen, msg 5201, thread #175/#180) ────────────

async def test_merge_claim_hygiene_verifies_a_real_merge(small_repo: Path) -> None:
    _commit(small_repo, "root")
    _git(small_repo, "checkout", "-q", "-b", "feature-x")
    _commit(small_repo, "the actual work")
    _git(small_repo, "checkout", "-q", "-")
    _git(small_repo, "merge", "--no-ff", "-m", "merge feature-x: did the thing", "feature-x")
    note = await merge_claim_hygiene(small_repo)
    assert note == "merge claim: 'feature-x' verified — an actual ancestor of HEAD"


async def test_merge_claim_hygiene_catches_a_false_claim(small_repo: Path) -> None:
    """Sekhmet's own specimen, reproduced exactly: a branch named in the subject that was
    never actually merged — fd3a703 claimed sekhmet-launch-resume-fix and never contained
    it. A plain commit whose SUBJECT follows the convention but whose PARENT is not the
    named branch's tip is indistinguishable from that incident by prose alone."""
    _commit(small_repo, "root")
    _git(small_repo, "checkout", "-q", "-b", "feature-y")
    _commit(small_repo, "work nobody actually merged")
    _git(small_repo, "checkout", "-q", "-")
    _commit(small_repo, "merge feature-y: claims the merge, never happened")
    note = await merge_claim_hygiene(small_repo)
    assert note == ("merge claim: ⚠ HEAD's subject names 'feature-y' but it is NOT an "
                    "ancestor — the exact shape of fd3a703's own specimen (named, never merged)")


async def test_merge_claim_hygiene_nothing_to_verify_off_a_plain_commit(
    small_repo: Path,
) -> None:
    _commit(small_repo, "just ordinary work, no merge claim in the subject")
    note = await merge_claim_hygiene(small_repo)
    assert "nothing to verify" in note


async def test_merge_claim_hygiene_unverifiable_when_the_named_branch_is_gone(
    small_repo: Path,
) -> None:
    """The common, innocent case: a merged feature branch gets deleted afterward. Absence of
    the branch must never read as a false claim — it is simply unverifiable now."""
    _commit(small_repo, "root")
    _git(small_repo, "checkout", "-q", "-b", "feature-z")
    _commit(small_repo, "work")
    _git(small_repo, "checkout", "-q", "-")
    _git(small_repo, "merge", "--no-ff", "-m", "merge feature-z: cleaned up after", "feature-z")
    _git(small_repo, "branch", "-D", "feature-z")
    note = await merge_claim_hygiene(small_repo)
    assert "no longer exists locally" in note and "not assumed false" in note


async def test_merge_claim_hygiene_fails_open_on_a_non_git_root(tmp_path: Path) -> None:
    note = await merge_claim_hygiene(tmp_path)
    assert "nothing to verify" in note


# ── merge_claim_hygiene's `since` ranged walk (obligation 8752024d, 1c85ed3's own gap) ──────
#
# THE SPECIMEN THIS CLOSES: a "merge, raise ratchet, deploy" sequence carries TWO real
# merges under one final ratchet commit whose own subject never claims a branch — the
# old HEAD-only check reported "nothing to verify" while a bad merge rode underneath.

async def test_merge_claim_hygiene_catches_a_hidden_bad_merge_under_a_ratchet_commit(
    small_repo: Path,
) -> None:
    root = _commit(small_repo, "root")
    _git(small_repo, "checkout", "-q", "-b", "feature-good")
    _commit(small_repo, "real work")
    _git(small_repo, "checkout", "-q", "-")
    _git(small_repo, "merge", "--no-ff", "-m", "merge feature-good: did the thing",
        "feature-good")
    # the SPECIMEN: a merge commit claiming a branch that was never actually merged,
    # buried under a ratchet commit whose own subject names nothing.
    _git(small_repo, "checkout", "-q", "-b", "feature-bad")
    _commit(small_repo, "work nobody actually merged")
    _git(small_repo, "checkout", "-q", "-")
    _commit(small_repo, "merge feature-bad: claims the merge, never happened")
    _commit(small_repo, "ratchet: 42 tools unchanged")  # names no branch at all

    note = await merge_claim_hygiene(small_repo, since=root)
    assert "2 merge claim(s) since last deploy" in note
    assert "⚠ 1 FAILED" in note
    assert "'feature-bad'" in note and "NOT an ancestor" in note
    assert root[:8] in note


async def test_merge_claim_hygiene_ranged_walk_all_clean(small_repo: Path) -> None:
    root = _commit(small_repo, "root")
    for name in ("feature-a", "feature-b"):
        _git(small_repo, "checkout", "-q", "-b", name)
        _commit(small_repo, f"work on {name}")
        _git(small_repo, "checkout", "-q", "-")
        _git(small_repo, "merge", "--no-ff", "-m", f"merge {name}: landed", name)
    _commit(small_repo, "ratchet: 42 tools unchanged")

    note = await merge_claim_hygiene(small_repo, since=root)
    assert "2 merge claim(s) since last deploy" in note
    assert "FAILED" not in note
    assert "'feature-a' verified" in note and "'feature-b' verified" in note


async def test_merge_claim_hygiene_prefers_the_cited_sha_over_a_reused_branchs_moved_tip(
    small_repo: Path,
) -> None:
    """THE LIVE SPECIMEN (found by dry-running the ranged walk against this house's own
    real history, obligation 8752024d): 'sekhmet-150-backlog' was reused across two
    separate merges weeks apart — checking the branch's CURRENT tip against the OLDER
    merge commit false-flagged a completely genuine historical merge the moment the
    branch moved on to a second round of work. The cited sha (this house's own
    'merge <branch> (<sha>) — ...' convention) is time-stable; the branch tip is not."""
    root = _commit(small_repo, "root")
    _git(small_repo, "checkout", "-q", "-b", "reused-branch")
    first_round = _commit(small_repo, "first round of work")
    _git(small_repo, "checkout", "-q", "-")
    _git(small_repo, "merge", "--no-ff", "-m",
        f"merge reused-branch ({first_round[:8]}) — first landing", "reused-branch")
    # the branch is REUSED for a second, later round — its tip moves on, unrelated to the
    # first merge commit above.
    _git(small_repo, "checkout", "-q", "reused-branch")
    _commit(small_repo, "second round of work, unrelated to the first merge")
    _git(small_repo, "checkout", "-q", "-")
    _commit(small_repo, "ratchet: 42 tools unchanged")

    note = await merge_claim_hygiene(small_repo, since=root)
    assert "FAILED" not in note
    assert "'reused-branch' verified" in note and "cites" in note


async def test_merge_claim_hygiene_catches_a_false_cited_sha(small_repo: Path) -> None:
    """A cited sha that is ITSELF not an ancestor is still a real false claim — preferring
    the cited sha over the branch tip must not become a way to launder a bad merge."""
    root = _commit(small_repo, "root")
    base_branch = _current_branch(small_repo)
    _git(small_repo, "checkout", "-q", "-b", "feature-w")
    real_work = _commit(small_repo, "the actual work")
    _git(small_repo, "checkout", "-q", "-b", "unrelated-elsewhere", root)
    unrelated = _commit(small_repo, "never actually merged")
    _git(small_repo, "checkout", "-q", base_branch)
    _git(small_repo, "merge", "--no-ff", "-m",
        f"merge feature-w ({unrelated[:8]}) — claims a sha that was never merged",
        "feature-w")

    note = await merge_claim_hygiene(small_repo, since=root)
    assert "⚠ 1 FAILED" in note
    assert unrelated[:8] in note and "NOT an ancestor" in note
    assert real_work  # the genuinely-merged commit exists; the CLAIM still cites the wrong one


async def test_merge_claim_hygiene_ranged_walk_no_merges_in_range(small_repo: Path) -> None:
    root = _commit(small_repo, "root")
    _commit(small_repo, "ordinary work, no merge claim")
    _commit(small_repo, "more ordinary work")

    note = await merge_claim_hygiene(small_repo, since=root)
    assert "2 commit(s) since last deploy" in note
    assert "none claim a merge" in note
    assert "nothing to verify" in note


async def test_merge_claim_hygiene_unknown_since_degrades_to_head_only(
    small_repo: Path,
) -> None:
    """A `since` sha this checkout has never heard of (a fresh clone, a rewritten history,
    the first deploy this checkout has ever recorded) must never be guessed against —
    degrades to the ORIGINAL HEAD-only check, exactly as if `since` were never passed."""
    _commit(small_repo, "root")
    _git(small_repo, "checkout", "-q", "-b", "feature-x")
    _commit(small_repo, "the actual work")
    _git(small_repo, "checkout", "-q", "-")
    _git(small_repo, "merge", "--no-ff", "-m", "merge feature-x: did the thing", "feature-x")

    note = await merge_claim_hygiene(small_repo, since="0" * 40)
    assert note == "merge claim: 'feature-x' verified — an actual ancestor of HEAD"


async def test_merge_claim_hygiene_since_equals_head_falls_back_to_head_only(
    small_repo: Path,
) -> None:
    """No new commits since the last deploy — an empty range degrades to the same
    HEAD-only check `since=None` would run, never an empty-but-technically-a-range walk."""
    _commit(small_repo, "root")
    _git(small_repo, "checkout", "-q", "-b", "feature-y")
    _commit(small_repo, "work")
    _git(small_repo, "checkout", "-q", "-")
    _git(small_repo, "merge", "--no-ff", "-m", "merge feature-y: landed", "feature-y")

    note = await merge_claim_hygiene(small_repo, since=_head(small_repo))
    assert note == "merge claim: 'feature-y' verified — an actual ancestor of HEAD"


# ── venv_import_hygiene (task #180 piece 2, decision 6fc0c082's own specimen) ───────────────

async def test_venv_import_hygiene_clean_when_src_resolves_from_repo_root() -> None:
    """The real, running interpreter's own `src` package DOES resolve from this very repo
    root in a correctly-configured dev box/CI — the positive case needs no fixture at all."""
    note = await venv_import_hygiene(_REPO_ROOT)
    assert note.startswith("venv import: clean")


async def test_venv_import_hygiene_flags_a_mismatched_repo_root(tmp_path: Path) -> None:
    """The Imhotep-worktree specimen, reproduced structurally: `src` resolves from the real
    repo, but the caller claims a DIFFERENT tree is the one being deployed."""
    note = await venv_import_hygiene(tmp_path)
    assert "NOT the deploying tree" in note
    assert str(_REPO_ROOT) in note
    assert str(tmp_path.resolve()) in note


# ── THE LANDING AUDITOR (Thoth dispatch msg 5339, thread 5256/5313) ─────────────────────────
# `git init`'s own default branch is `master`, not this house's real `main` — every test
# below says so explicitly rather than assume it.

def _commit_dated(repo: Path, msg: str, when: str) -> str:
    subprocess.run(
        ["git", "-C", str(repo), "commit", "--allow-empty", "-q", "-m", msg],
        check=True, capture_output=True,
        env={**__import__("os").environ, "GIT_AUTHOR_DATE": when, "GIT_COMMITTER_DATE": when},
    )
    return _head(repo)


@pytest.fixture
def main_repo(small_repo: Path) -> Path:
    _commit(small_repo, "root")
    _git(small_repo, "branch", "-M", "main")
    return small_repo


async def test_stale_unmerged_branches_flags_an_old_unclaimed_branch(
    main_repo: Path,
) -> None:
    _git(main_repo, "checkout", "-q", "-b", "seshat-old-lane")
    _commit_dated(main_repo, "old work, never merged", "2020-01-01T00:00:00")
    _git(main_repo, "checkout", "-q", "main")
    out = await stale_unmerged_branches(main_repo, claimed=set())
    assert [s["branch"] for s in out] == ["seshat-old-lane"]
    assert out[0]["age_hours"] > 48


async def test_stale_unmerged_branches_exempts_a_held_work_claim(main_repo: Path) -> None:
    _git(main_repo, "checkout", "-q", "-b", "seshat-old-lane")
    _commit_dated(main_repo, "old work, claimed", "2020-01-01T00:00:00")
    _git(main_repo, "checkout", "-q", "main")
    out = await stale_unmerged_branches(main_repo, claimed={"seshat-old-lane"})
    assert out == []


async def test_stale_unmerged_branches_exempts_a_fresh_branch(main_repo: Path) -> None:
    """A branch mid-build, committed moments ago, is not yet a specimen of anything."""
    _git(main_repo, "checkout", "-q", "-b", "seshat-fresh-lane")
    _commit(main_repo, "just started")
    _git(main_repo, "checkout", "-q", "main")
    out = await stale_unmerged_branches(main_repo, claimed=set())
    assert out == []


async def test_stale_unmerged_branches_empty_when_everything_merged(main_repo: Path) -> None:
    _git(main_repo, "checkout", "-q", "-b", "seshat-landed-lane")
    _commit_dated(main_repo, "old work, actually landed", "2020-01-01T00:00:00")
    _git(main_repo, "checkout", "-q", "main")
    _git(main_repo, "merge", "--no-ff", "-m", "merge seshat-landed-lane: real",
        "seshat-landed-lane")
    out = await stale_unmerged_branches(main_repo, claimed=set())
    assert out == []


async def test_stale_unmerged_branches_fails_open_on_a_non_git_root(tmp_path: Path) -> None:
    assert await stale_unmerged_branches(tmp_path, claimed=set()) == []


async def test_audit_graph_merge_claims_catches_a_decision_naming_an_unlanded_branch(
    actions: Actions, main_repo: Path,
) -> None:
    """decision 114b4052's own shape: prose says a branch is in, git disagrees — the
    branch is real and exists locally, but was never actually merged into main."""
    _git(main_repo, "checkout", "-q", "-b", "seshat-roster-review")
    _commit(main_repo, "the real work")
    _git(main_repo, "checkout", "-q", "main")
    obj = await actions.create_or_find_object("Decision", "decision:auditgraph01", "test")
    await actions.assert_property(
        obj, "summary",
        "accepted into merge seshat-roster-review, ready for the batch",
        "test", datetime.now(UTC), 0.9, evidence_class=EvidenceClass.SELF_DECLARED.value)
    out = await audit_graph_merge_claims(actions.pool, main_repo)
    assert len(out) == 1
    assert out[0]["canonical"] == "decision:auditgraph01"
    assert "seshat-roster-review" in out[0]["note"] and "NOT an ancestor" in out[0]["note"]


async def test_audit_graph_merge_claims_silent_when_actually_landed(
    actions: Actions, main_repo: Path,
) -> None:
    _git(main_repo, "checkout", "-q", "-b", "seshat-real-lane")
    _commit(main_repo, "the real work")
    _git(main_repo, "checkout", "-q", "main")
    _git(main_repo, "merge", "--no-ff", "-m", "merge seshat-real-lane: landed",
        "seshat-real-lane")
    obj = await actions.create_or_find_object("Decision", "decision:auditgraph02", "test")
    await actions.assert_property(
        obj, "summary", "merge seshat-real-lane: landed, all gates green",
        "test", datetime.now(UTC), 0.9, evidence_class=EvidenceClass.SELF_DECLARED.value)
    out = await audit_graph_merge_claims(actions.pool, main_repo)
    assert out == []


async def test_audit_graph_merge_claims_ignores_a_spurious_non_branch_match(
    actions: Actions, main_repo: Path,
) -> None:
    """"merge batch"/"merge conflict"-shaped prose parses as a candidate branch name that
    simply doesn't exist — unverifiable, never a false mismatch."""
    obj = await actions.create_or_find_object("Decision", "decision:auditgraph03", "test")
    await actions.assert_property(
        obj, "summary", "accepted into his merge batch, nothing landed yet though",
        "test", datetime.now(UTC), 0.9, evidence_class=EvidenceClass.SELF_DECLARED.value)
    out = await audit_graph_merge_claims(actions.pool, main_repo)
    assert out == []


async def test_landing_audit_mints_one_obligation_and_is_idempotent(
    actions: Actions, main_repo: Path,
) -> None:
    _git(main_repo, "checkout", "-q", "-b", "seshat-idempotent-lane")
    _commit_dated(main_repo, "old, unclaimed", "2020-01-01T00:00:00")
    _git(main_repo, "checkout", "-q", "main")

    first = await landing_audit(actions, main_repo)
    assert len(first["stale_unmerged_branches"]) == 1
    assert len(first["obligations"]) == 1

    second = await landing_audit(actions, main_repo)
    assert len(second["obligations"]) == 1
    assert second["obligations"] == first["obligations"]  # same Thread, not a duplicate


async def test_landing_audit_skips_a_branch_an_open_held_work_thread_already_claims(
    actions: Actions, main_repo: Path,
) -> None:
    from src.orchestrator.capture import open_thread

    _git(main_repo, "checkout", "-q", "-b", "seshat-claimed-lane")
    _commit_dated(main_repo, "old, but claimed", "2020-01-01T00:00:00")
    _git(main_repo, "checkout", "-q", "main")
    await open_thread(actions, "still building seshat-claimed-lane",
                      branch="seshat-claimed-lane")

    out = await landing_audit(actions, main_repo)
    assert out["stale_unmerged_branches"] == []


async def test_landing_audit_heartbeat_is_scheduled_as_a_cron_job() -> None:
    """The scheduled leg exists and is wired into the cron table — same proof shape as
    reap_leases'/reap_stuck_sweeps' own registration tests (Thoth DM 5544: a mechanism
    whose only trigger is `osiris deploy` succeeding is not adopted, it is hostage to
    whatever else can block a deploy)."""
    from src.workers.arq_worker import WorkerSettings

    crons = {c.coroutine.__name__ for c in WorkerSettings.cron_jobs}
    assert "landing_audit_heartbeat" in crons


async def test_landing_audit_heartbeat_noop_when_flag_off(
    actions: Actions, main_repo: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from src.workers.arq_worker import landing_audit_heartbeat

    monkeypatch.setenv("OSIRIS_LANDING_AUDIT_ENABLED", "0")
    _git(main_repo, "checkout", "-q", "-b", "seshat-heartbeat-off-lane")
    _commit_dated(main_repo, "stranded, flag off", "2020-01-01T00:00:00")
    _git(main_repo, "checkout", "-q", "main")

    monkeypatch.setattr(
        "src.orchestrator.deploy_guard._REPO_ROOT", main_repo, raising=False)
    ctx = {"cascade": SimpleNamespace(actions=actions)}
    out = await landing_audit_heartbeat(ctx)
    assert out == 0


async def test_landing_audit_heartbeat_mints_when_flag_on_replaying_a_stranded_branch(
    actions: Actions, main_repo: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The replay the dispatch asked for: a branch shaped exactly like tonight's
    seshat-hook-flip specimen — finished, committed, never merged — swept unprompted by
    the cron tick, with no `osiris deploy` involved at all."""
    from types import SimpleNamespace

    from src.workers.arq_worker import landing_audit_heartbeat

    monkeypatch.setenv("OSIRIS_LANDING_AUDIT_ENABLED", "1")
    _git(main_repo, "checkout", "-q", "-b", "seshat-hook-flip")
    _commit_dated(main_repo, "statusline port + onboard.py SessionEnd gap",
                  "2020-01-01T00:00:00")
    _git(main_repo, "checkout", "-q", "main")

    monkeypatch.setattr(
        "src.orchestrator.deploy_guard._REPO_ROOT", main_repo, raising=False)
    ctx = {"cascade": SimpleNamespace(actions=actions)}
    out = await landing_audit_heartbeat(ctx)
    assert out == 1

    second = await landing_audit_heartbeat(ctx)
    assert second == 1  # still one open obligation naming it, every tick

    minted = await actions.pool.fetch(
        "SELECT o.id FROM objects o "
        "JOIN current_assertions a ON a.object_id=o.id AND a.name='summary' "
        "WHERE o.type='Thread' AND a.value #>> '{}' ILIKE '%seshat-hook-flip%'")
    assert len(minted) == 1  # idempotent: one Thread, never a duplicate obligation

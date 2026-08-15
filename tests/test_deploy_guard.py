"""deploy_guard — the code-ahead-of-schema alarm (thread e6f5556f), plus the reboot-is-a-
deploy confession (thread 489a39d0). LOUD ALARM, never a refusal (Thoth's ruling, DM 1339):
osiris-mcp is a fleet-wide single point of failure, so a false positive from a buggy check
refusing to serve would self-inflict a total outage strictly worse than the silent drift
either guard exists to catch.
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator.deploy_guard import (
    _is_ancestor,
    alarm_schema_drift,
    alarm_unreviewed_boot,
    check_diverged_since_last_deploy,
    check_schema_drift,
    check_unreviewed_boot,
    diverged_since_last_deploy,
    origin_visibility,
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


def _commit(repo: Path, msg: str) -> str:
    _git(repo, "commit", "--allow-empty", "-q", "-m", msg)
    return subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


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

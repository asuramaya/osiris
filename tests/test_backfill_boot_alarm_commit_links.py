"""LANE 1 (thread 33962e00, decision 18464c67): the boot-startup watchdog's own
UNREVIEWED BOOT alarm Threads carry a sha in their own summary text — link it to the
Commit that sha already resolves to, via Lane 0's `derive_or_abstain`, never a guess.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator import capture
from src.orchestrator.deploy_guard import alarm_unreviewed_boot


async def _mint_commit(actions: Actions, sha: str) -> uuid.UUID:
    return await actions.create_or_find_object("Commit", f"commit:{sha[:12]}", "test")


async def _boot_alarm(pool, sha: str, service: str = "osiris-mcp") -> uuid.UUID:
    """`alarm_unreviewed_boot` returns None (fire-and-forget, same shape as its sibling
    `alarm_schema_drift`) — the id has to be looked up back out, same as test_deploy_
    guard.py's own tests do."""
    await alarm_unreviewed_boot(
        pool, f"running HEAD {sha!r} was never recorded by `osiris deploy`.",
        running_head=sha, service=service)
    return await pool.fetchval(
        "SELECT o.id FROM objects o JOIN current_assertions a ON a.object_id = o.id "
        "WHERE o.type = 'Thread' AND a.name = 'summary' "
        "AND a.value #>> '{}' ILIKE '%' || $1 || '%'", sha)


async def test_backfill_dry_run_writes_nothing(actions: Actions) -> None:
    sha = "abc1234def5678901234567890123456789012"
    commit = await _mint_commit(actions, sha)
    await _boot_alarm(actions.pool, sha)
    out = await capture.backfill_boot_alarm_commit_links(actions, actor="test", dry_run=True)
    assert out["to_mint"] == 1
    assert out["plan"][0]["to"] == str(commit)
    n = await actions.pool.fetchval("SELECT count(*) FROM links WHERE type='noted_in'")
    assert n == 0


async def test_backfill_mints_noted_in_when_the_commit_exists(actions: Actions) -> None:
    sha = "abc1234def5678901234567890123456789012"
    commit = await _mint_commit(actions, sha)
    thread = await _boot_alarm(actions.pool, sha)
    out = await capture.backfill_boot_alarm_commit_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_mint"] == 1
    linked = await actions.pool.fetchval(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='noted_in'", thread)
    assert linked == commit


async def test_backfill_is_idempotent(actions: Actions) -> None:
    sha = "abc1234def5678901234567890123456789012"
    await _mint_commit(actions, sha)
    await _boot_alarm(actions.pool, sha)
    first = await capture.backfill_boot_alarm_commit_links(
        actions, actor="test", dry_run=False, because="test authorization")
    second = await capture.backfill_boot_alarm_commit_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert first["to_mint"] == 1
    n = await actions.pool.fetchval("SELECT count(*) FROM links WHERE type='noted_in'")
    assert n == 1
    assert second["scanned"] == 0  # the link now exists, so the thread is no longer an orphan


async def test_backfill_abstains_when_no_commit_exists(actions: Actions) -> None:
    sha = "abc1234def5678901234567890123456789012"
    thread = await _boot_alarm(actions.pool, sha)
    out = await capture.backfill_boot_alarm_commit_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_abstain"] == 1
    n = await actions.pool.fetchval("SELECT count(*) FROM links WHERE from_id=$1", thread)
    assert n == 0
    reason = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_noted_in'", thread)
    assert reason is not None and reason["candidate_count"] == 0


async def test_backfill_abstains_on_an_ambiguous_sha_prefix(actions: Actions) -> None:
    """Two Commits sharing the same 12-char prefix — a real, if rare, collision — must
    abstain rather than guess which one the alarm meant. Real Commit canonicals are
    always exactly `commit:<12 hex>` (gitlog.py's own convention, find-or-create means
    two calls with the same 12 chars always collide onto one object) — this constructs
    the collision directly, at a longer canonical, to prove the LIKE-prefix match still
    abstains rather than guessing if that convention ever changes."""
    sha = "abc1234def5678901234567890123456789012"
    await actions.create_or_find_object("Commit", f"commit:{sha[:12]}", "test")
    await actions.create_or_find_object("Commit", f"commit:{sha[:12]}extra", "test")
    thread = await _boot_alarm(actions.pool, sha)
    out = await capture.backfill_boot_alarm_commit_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_abstain"] == 1
    n = await actions.pool.fetchval("SELECT count(*) FROM links WHERE from_id=$1", thread)
    assert n == 0


async def test_backfill_abstains_when_the_summary_has_no_sha(actions: Actions) -> None:
    thread = await _boot_alarm(actions.pool, "unknown")
    out = await capture.backfill_boot_alarm_commit_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_abstain"] == 1
    assert out["plan"][0]["reason"] == "no HEAD sha in summary"
    reason = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_noted_in'", thread)
    assert reason is not None
    assert reason["reason"] == "thread's summary contains no HEAD sha to resolve"


async def test_backfill_never_touches_a_thread_that_already_has_a_link(
    actions: Actions,
) -> None:
    """Scoped to zero-live-link orphans only — a boot alarm someone already linked (by
    hand or a prior lane) is out of this repair's population entirely."""
    sha = "abc1234def5678901234567890123456789012"
    commit = await _mint_commit(actions, sha)
    thread = await _boot_alarm(actions.pool, sha)
    await actions.create_link(thread, commit, "noted_in", "test", datetime.now(UTC),
                              0.6, evidence_class="direct_observation")
    out = await capture.backfill_boot_alarm_commit_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["scanned"] == 0


async def test_backfill_requires_a_because_to_execute(actions: Actions) -> None:
    sha = "abc1234def5678901234567890123456789012"
    await _mint_commit(actions, sha)
    await _boot_alarm(actions.pool, sha)
    out = await capture.backfill_boot_alarm_commit_links(actions, actor="test", dry_run=False)
    assert "error" in out
    n = await actions.pool.fetchval("SELECT count(*) FROM links WHERE type='noted_in'")
    assert n == 0

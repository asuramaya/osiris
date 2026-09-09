"""The vault-stays-tame-without-a-hand obligation, part 1 (thread 9fac4e0d): the ladder's
own weekly manifest-then-apply-if-clear gate. build_manifest_body is pure and tested
directly; mail_manifest/find_clear_manifest are proven against a real per-worker database
(the `actions` fixture's own catalog-seeded pg_dsn, matching thread 8542ee89's own lesson
about testing jsonb writes against a schema that actually has the Message type declared)."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
import scripts.osiris_prune_ladder as ladder
from scripts.osiris_prune_ladder import DumpFile, TranscriptChain, build_manifest_body
from scripts.osiris_transcript_cache_prune import SessionRow
from src.actions.core import Actions
from src.orchestrator.mailbox import dim_brief

NOW = datetime(2026, 9, 8, 12, 0, tzinfo=UTC)


def test_build_manifest_body_names_the_totals_and_the_dim_instruction() -> None:
    plans = {
        "backups/": {"keep": [DumpFile("keep1", NOW)],
                     "remove": [DumpFile("old1", NOW, size_bytes=1024**3)]},
        "vault": {"keep": [], "remove": []},
        "vault/basebackups": {"keep": [], "remove": []},
    }
    chain_plan: dict[str, list[TranscriptChain]] = {"keep": [], "remove": []}
    body = build_manifest_body(plans, chain_plan)
    assert "1 dump file(s)" in body
    assert "old1" in body
    assert "dim" in body.lower()
    assert "9fac4e0d" in body


def test_build_manifest_body_is_empty_safe() -> None:
    empty_plan: dict[str, list[DumpFile]] = {"keep": [], "remove": []}
    plans = {"backups/": empty_plan, "vault": empty_plan, "vault/basebackups": empty_plan}
    chain_plan: dict[str, list[TranscriptChain]] = {"keep": [], "remove": []}
    body = build_manifest_body(plans, chain_plan)
    assert "0 dump file(s)" in body
    assert "0 transcript chain(s)" in body


def test_build_manifest_body_names_legacy_tarballs_and_session_files() -> None:
    """Thoth mail 8441 items 1 and 2 — both new populations get their own named
    section and their own count in the manifest's own header, same as every
    population before them."""
    empty_plan: dict[str, list[DumpFile]] = {"keep": [], "remove": []}
    plans = {"backups/": empty_plan, "vault": empty_plan, "vault/basebackups": empty_plan}
    chain_plan: dict[str, list[TranscriptChain]] = {"keep": [], "remove": []}
    legacy_plan = {"keep": [], "remove": [DumpFile("stray.tar.gz.new", NOW, size_bytes=1024)]}
    session_plan = [SessionRow("sid-1", "/tmp/sid-1.jsonl", NOW, NOW)]

    body = build_manifest_body(plans, chain_plan, legacy_plan=legacy_plan,
                               session_plan=session_plan)

    assert "1 legacy transcript tarball(s)" in body
    assert "1 transcript cache file(s)" in body
    assert "stray.tar.gz.new" in body
    assert "/tmp/sid-1.jsonl" in body


@pytest.fixture(autouse=True)
def _use_test_dsn(pg_dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ladder, "DSN", pg_dsn)


async def test_find_clear_manifest_with_none_sent_refuses(actions: Actions) -> None:
    del actions  # triggers catalog seeding only
    mid, reason = await ladder.find_clear_manifest()
    assert mid is None
    assert "no manifest" in reason


async def test_mail_manifest_then_find_clear_manifest_refuses_while_too_young(
    actions: Actions,
) -> None:
    del actions
    empty_plan: dict[str, list[DumpFile]] = {"keep": [], "remove": []}
    plans = {"backups/": empty_plan, "vault": empty_plan, "vault/basebackups": empty_plan}
    chain_plan: dict[str, list[TranscriptChain]] = {"keep": [], "remove": []}

    sent_id = await ladder.mail_manifest(plans, chain_plan)
    assert sent_id > 0

    mid, reason = await ladder.find_clear_manifest()
    assert mid is None
    assert "not yet" in reason or "old" in reason


async def test_find_clear_manifest_is_clear_once_old_enough_and_undimmed(
    actions: Actions, pg_dsn: str,
) -> None:
    empty_plan: dict[str, list[DumpFile]] = {"keep": [], "remove": []}
    plans = {"backups/": empty_plan, "vault": empty_plan, "vault/basebackups": empty_plan}
    chain_plan: dict[str, list[TranscriptChain]] = {"keep": [], "remove": []}

    sent_id = await ladder.mail_manifest(plans, chain_plan)
    # backdate it past the min-age gate — a real weekly run would just wait a day
    await actions.pool.execute(
        "UPDATE fleet_messages SET created_at = now() - interval '25 hours' WHERE id=$1",
        sent_id)

    mid, reason = await ladder.find_clear_manifest()
    assert mid == sent_id
    assert "clear" in reason


async def test_find_clear_manifest_refuses_a_dimmed_manifest(
    actions: Actions,
) -> None:
    empty_plan: dict[str, list[DumpFile]] = {"keep": [], "remove": []}
    plans = {"backups/": empty_plan, "vault": empty_plan, "vault/basebackups": empty_plan}
    chain_plan: dict[str, list[TranscriptChain]] = {"keep": [], "remove": []}

    sent_id = await ladder.mail_manifest(plans, chain_plan)
    await actions.pool.execute(
        "UPDATE fleet_messages SET created_at = now() - interval '25 hours' WHERE id=$1",
        sent_id)
    await dim_brief(actions.pool, sent_id, because="conditions changed", by="test:agent")

    mid, reason = await ladder.find_clear_manifest()
    assert mid is None
    assert "dimmed" in reason


async def test_collect_session_prune_plan_delegates_to_the_cache_prune_script(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`_collect_session_prune_plan` (Thoth mail 8441 item 1) must reuse
    osiris_transcript_cache_prune's own `_collect_sessions`/`find_prunable_sessions`
    unchanged, never a re-derived query — patching `_collect_sessions` alone (no real
    DB) proves the wiring, since `find_prunable_sessions` itself is already covered
    directly in test_transcript_cache_prune.py."""
    import scripts.osiris_transcript_cache_prune as cache_prune

    dead = SessionRow("dead-sid", "/tmp/dead.jsonl",
                      datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC))
    alive = SessionRow("alive-sid", "/tmp/alive.jsonl", NOW, NOW)

    async def _fake_collect_sessions(harness: str = "claude-code") -> list[SessionRow]:
        return [dead, alive]

    monkeypatch.setattr(cache_prune, "_collect_sessions", _fake_collect_sessions)

    plan = await ladder._collect_session_prune_plan()
    assert [s.anchor_sid for s in plan] == ["dead-sid"]


async def test_find_clear_manifest_only_looks_at_the_newest_manifest(
    actions: Actions,
) -> None:
    """An OLD dimmed manifest must never block a fresh, clear one sent later — the
    query is scoped to the newest message this script ever sent, not "any"."""
    empty_plan: dict[str, list[DumpFile]] = {"keep": [], "remove": []}
    plans = {"backups/": empty_plan, "vault": empty_plan, "vault/basebackups": empty_plan}
    chain_plan: dict[str, list[TranscriptChain]] = {"keep": [], "remove": []}

    old_id = await ladder.mail_manifest(plans, chain_plan)
    await actions.pool.execute(
        "UPDATE fleet_messages SET created_at = now() - interval '10 days' WHERE id=$1",
        old_id)
    await dim_brief(actions.pool, old_id, because="stale week", by="test:agent")

    new_id = await ladder.mail_manifest(plans, chain_plan)
    await actions.pool.execute(
        "UPDATE fleet_messages SET created_at = now() - interval '25 hours' WHERE id=$1",
        new_id)

    mid, reason = await ladder.find_clear_manifest()
    assert mid == new_id
    assert "clear" in reason

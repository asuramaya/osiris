"""Resume across Claude accounts: the harness loads none of a transcript another account
owns, so resume detects it, says so, and launches fresh from a store-built briefing."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.orchestrator.resume_account import (
    account_mismatch,
    current_account_uuid,
    history_owner_sync,
    last_turns,
    store_account_mismatch,
    store_briefing,
)

OLD = "74d4ddbd-67aa-4830-aaee-388b128e19d5"
NEW = "2e54b530-8738-4bee-86c9-56a49e200cd2"


def line(kind: str, text: str, owner: str = OLD, **extra: Any) -> str:
    return json.dumps({"type": kind, "ownerAccountUuid": owner, **extra,
                       "message": {"content": text}}) + "\n"


def write_transcript(path: Path, old_lines: int = 5, new_lines: int = 1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(line("user" if i % 2 == 0 else "assistant", f"turn {i}")
                            for i in range(old_lines))
                    + "".join(line("assistant", "later", owner=NEW)
                              for _ in range(new_lines)))


def test_the_dominant_owner_is_counted_per_line(tmp_path: Path) -> None:
    t = tmp_path / "s.jsonl"
    write_transcript(t, old_lines=5, new_lines=2)
    assert history_owner_sync(t) == (OLD, {OLD: 5, NEW: 2})


def test_no_stamp_or_no_file_is_no_owner(tmp_path: Path) -> None:
    (tmp_path / "plain.jsonl").write_text('{"type":"user"}\n')
    assert history_owner_sync(tmp_path / "plain.jsonl") == (None, {})
    assert history_owner_sync(tmp_path / "gone.jsonl") == (None, {})


def test_the_current_account_is_read_from_the_claude_config(tmp_path: Path) -> None:
    (tmp_path / ".claude.json").write_text(json.dumps({"oauthAccount": {"accountUuid": NEW}}))
    assert current_account_uuid(tmp_path) == NEW
    assert current_account_uuid(tmp_path / "nowhere") is None


async def test_another_accounts_history_is_a_mismatch_with_a_plain_reason(
    tmp_path: Path,
) -> None:
    t = tmp_path / "s.jsonl"
    write_transcript(t)
    finding = await account_mismatch(t, current=NEW)
    assert finding is not None
    assert finding["history_owner"] == OLD and finding["current_account"] == NEW
    assert "history belongs to another Claude account; the harness will not load it" \
        in finding["detail"]


async def test_same_account_unknown_account_or_unstamped_history_is_no_mismatch(
    tmp_path: Path,
) -> None:
    t = tmp_path / "s.jsonl"
    write_transcript(t)
    assert await account_mismatch(t, current=OLD) is None
    assert await account_mismatch(tmp_path / "gone.jsonl", current=NEW) is None
    plain = tmp_path / "plain.jsonl"
    plain.write_text('{"type":"user"}\n')
    assert await account_mismatch(plain, current=NEW) is None


def test_last_turns_keeps_real_turns_only_oldest_first_and_bounded() -> None:
    raws = [line("user", "first").encode(),
            line("user", "hook noise", isMeta=True).encode(),
            json.dumps({"type": "system", "message": {"content": "x"}}).encode(),
            json.dumps({"type": "assistant", "message": {"content": [
                {"type": "tool_use", "name": "Bash"}]}}).encode(),
            line("assistant", "long " * 400).encode(),
            b"not json"]
    turns = last_turns(raws, 5)
    assert [r for r, _ in turns] == ["user", "assistant"]
    assert turns[0][1] == "first"
    assert "more characters" in turns[1][1] and len(turns[1][1]) < 600
    assert last_turns(raws, 1) == [turns[1]]


async def test_the_briefing_is_built_from_the_store_and_says_why(
    actions: Any, tmp_path: Path,
) -> None:
    from src.ingest.soul_store import SoulStore

    t = tmp_path / "abcd1234-0000-4000-8000-000000000000.jsonl"
    write_transcript(t, old_lines=30, new_lines=1)
    await SoulStore(actions.pool).ingest_path(str(t), "abcd1234")
    finding = await account_mismatch(t, current=NEW)
    assert finding is not None
    brief = await store_briefing(
        actions.pool, handle="Deckard", anchor_sid="abcd1234", finding=finding, turns=4)
    assert "Deckard, launched FRESH" in brief and "another Claude account" in brief
    assert "~/.osiris/seats/deckard/charter.md" in brief
    assert "LAST 4 TURNS" in brief and "[assistant] later" in brief
    assert "turn 28" in brief and "turn 3" not in brief   # bounded to the last turns
    assert len(brief) <= 7000


async def test_the_account_question_is_answered_from_the_store_alone(
    actions: Any, tmp_path: Path,
) -> None:
    """No file is needed: ingest, delete the transcript, and the store still says whose
    history it is (this is what lets resume decide before it writes anything)."""
    from src.ingest.soul_store import SoulStore

    t = tmp_path / "beef1234-0000-4000-8000-000000000000.jsonl"
    write_transcript(t, old_lines=8, new_lines=1)
    await SoulStore(actions.pool).ingest_path(str(t), "beef1234")
    t.unlink()
    finding = await store_account_mismatch(actions.pool, "beef1234", current=NEW)
    assert finding is not None
    assert finding["history_owner"] == OLD and finding["owner_lines"][OLD] == 8
    assert await store_account_mismatch(actions.pool, "beef1234", current=OLD) is None
    assert await store_account_mismatch(actions.pool, "deadbeef", current=NEW) is None

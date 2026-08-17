"""unresumed_heads (thread ef88e2bb's aftermath) — the false-mint-over-a-resumable-head
census. Read-only: proves it finds the specimen shape and stays silent on ordinary
successions (a genuine compaction resume, a lineage with nothing sitting unused)."""
from __future__ import annotations

import os
import time as _time
from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.config.settings import Settings
from src.orchestrator.seats import bind_holder, ensure_seat
from src.orchestrator.succession_repair import unresumed_heads

NOW = datetime(2026, 8, 17, tzinfo=UTC)
_SD = "self_declared"
_SID = "b5f04f84-0000-4000-8000-000000000000"


async def _agent(actions: Actions, canonical: str, *, generation: str, minted_because: str,
                 succeeded_from: str | None = None, wrote: bool = True,
                 session: str | None = None) -> str:
    a = await actions.create_or_find_object("Agent", canonical, "test")
    await actions.assert_property(a, "seat_generation", generation, "test", NOW, 0.9,
                                  evidence_class=_SD)
    await actions.assert_property(a, "minted_because", minted_because, "test", NOW, 0.9,
                                  evidence_class=_SD)
    if succeeded_from:
        await actions.assert_property(a, "succeeded_from", succeeded_from, "test", NOW, 0.9,
                                      evidence_class=_SD)
    if wrote:
        # wrote_anything needs an assertion SOURCED BY this agent's own canonical, on some
        # OTHER object — an act, not self-description (succession_chain's own docstring).
        other = await actions.create_or_find_object(
            "Thread", f"thread:did-something-{canonical}", canonical)
        await actions.assert_property(other, "summary", "real work", canonical, NOW, 0.9,
                                      evidence_class=_SD)
    if session:
        await actions.assert_property(a, "session", session, "test", NOW, 0.9,
                                      evidence_class=_SD)
    return canonical


def _settings(sense: Path) -> Settings:
    return Settings(osiris_sense_sessions=str(sense), osiris_resume_ceiling_bytes=8_000_000,
                    osiris_resume_min_tail_bytes=0)


def _resumable_transcript(sense: Path, agent_id: str, *, transcript_bytes: int = 16) -> None:
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{_SID}.jsonl"
    signed = ('{"type":"user","toolUseResult":'
              '"{\\"sent\\":1,\\"from\\":\\"' + agent_id + '\\"}"}\n').encode()
    t.write_bytes(signed + b"x" * transcript_bytes)
    old = _time.time() - 3600
    os.utime(t, (old, old))


async def _seated(actions: Actions, *, handle: str, holder: str, anchor_cwd: str) -> None:
    seat = await ensure_seat(actions, house="osiris", handle=handle, anchor_cwd=anchor_cwd,
                             source="test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id=holder)


async def test_finds_a_stranger_minted_over_a_still_resumable_predecessor(
    actions: Actions, tmp_path: Path,
) -> None:
    sense = tmp_path / "projects"
    await _agent(actions, "agent:ferry", generation="8", minted_because="compaction",
                wrote=True, session=_SID)
    _resumable_transcript(sense, "agent:ferry")
    await _agent(actions, "agent:ferry-ii", generation="9", minted_because="office-birth",
                succeeded_from="agent:ferry", wrote=True)
    await _seated(actions, handle="ferrytest", holder="agent:ferry-ii",
                 anchor_cwd="/tmp/ferrytest-office")

    out = await unresumed_heads(actions.pool, settings=_settings(sense))

    assert out["checked"] == 1
    assert len(out["found"]) == 1
    hit = out["found"][0]
    assert hit["stranger"] == "agent:ferry-ii"
    assert hit["unresumed_head"] == "agent:ferry"
    assert hit["resumable_session"] == _SID


async def test_silent_on_an_ordinary_office_birth_with_nothing_to_resume(
    actions: Actions, tmp_path: Path,
) -> None:
    """The common case (a seat's very first mint, no predecessor at all) must never
    false-positive."""
    sense = tmp_path / "projects"
    await _agent(actions, "agent:fresh", generation="1", minted_because="office-birth",
                wrote=True)
    await _seated(actions, handle="freshtest", holder="agent:fresh",
                 anchor_cwd="/tmp/freshtest-office")

    out = await unresumed_heads(actions.pool, settings=_settings(sense))

    assert out["checked"] == 1
    assert out["found"] == []


async def test_silent_when_the_head_was_a_genuine_resume_not_an_office_birth(
    actions: Actions, tmp_path: Path,
) -> None:
    """A generation minted via `compaction` (the normal in-band succession, or a real
    `--resume`) is never mistaken for the office-birth stranger class, even with a real
    resumable predecessor sitting there."""
    sense = tmp_path / "projects"
    await _agent(actions, "agent:calm", generation="1", minted_because="compaction",
                wrote=True, session=_SID)
    _resumable_transcript(sense, "agent:calm")
    await _agent(actions, "agent:calm-ii", generation="2", minted_because="compaction",
                succeeded_from="agent:calm", wrote=True)
    await _seated(actions, handle="calmtest", holder="agent:calm-ii",
                 anchor_cwd="/tmp/calmtest-office")

    out = await unresumed_heads(actions.pool, settings=_settings(sense))

    assert out["checked"] == 1
    assert out["found"] == []


async def test_silent_when_the_predecessor_transcript_is_past_the_resume_gate(
    actions: Actions, tmp_path: Path,
) -> None:
    """A predecessor session that genuinely closed at the compaction seam itself (the
    ordinary tiny-tail refusal every launch already applies) must not be reported as
    'still resumable' — same gate, same numbers, reused unchanged."""
    sense = tmp_path / "projects"
    await _agent(actions, "agent:tiny", generation="1", minted_because="compaction",
                wrote=True, session=_SID)
    _resumable_transcript(sense, "agent:tiny", transcript_bytes=1)
    await _agent(actions, "agent:tiny-ii", generation="2", minted_because="office-birth",
                succeeded_from="agent:tiny", wrote=True)
    await _seated(actions, handle="tinytest", holder="agent:tiny-ii",
                 anchor_cwd="/tmp/tinytest-office")

    settings = Settings(osiris_sense_sessions=str(sense), osiris_resume_ceiling_bytes=8_000_000,
                        osiris_resume_min_tail_bytes=1000)
    out = await unresumed_heads(actions.pool, settings=settings)

    assert out["checked"] == 1
    assert out["found"] == []

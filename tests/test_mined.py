"""consolidate_memory — collapse the miner's DERIVED near-duplicate memories.

The session-miner re-senses work a session already captured and mints a reworded copy;
distinct summaries hash to distinct objects, so the per-object grade read can't fold them.
These tests pin the invariant that keeps the collapse safe: a deliberate SELF_DECLARED
capture is NEVER the loser, so a wrong match can only ever refile a DERIVED echo (reversibly).
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.ingest.mined import consolidate_memory
from src.orchestrator.capture import open_thread
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_D_EC = EvidenceClass.DERIVED.value
_D_CONF = confidence_for(EvidenceClass.DERIVED)


async def _status(actions: Actions, oid: uuid.UUID) -> str:
    return await actions.pool.fetchval("SELECT status FROM objects WHERE id=$1", oid)  # type: ignore[no-any-return]


async def _derived_thread(
    actions: Actions, canon: str, summary: str, *, born: datetime | None = None
) -> uuid.UUID:
    """A DERIVED (session-miner) thread — the echo shape, distinct from a deliberate capture."""
    when = born or datetime.now(UTC)
    t = await actions.create_or_find_object("Thread", canon, "session-miner")
    await actions.assert_property(t, "summary", summary, "session-miner", when, _D_CONF,
                                  evidence_class=_D_EC)
    await actions.assert_property(t, "status", "open", "session-miner", when, _D_CONF,
                                  evidence_class=_D_EC)
    return t


async def test_derived_echo_folds_into_the_deliberate_capture(actions: Actions) -> None:
    a = await open_thread(  # SELF_DECLARED — the deliberate capture
        actions, "the composer renderer dispatches on result shape reading schema for styling")
    b = await _derived_thread(  # DERIVED — a reworded echo of the same thing
        actions, "thread:echo1",
        "composer renderer dispatches on result shape and reads schema styling variants")
    out = await consolidate_memory(actions, object_type="Thread", prefix="thread:")
    assert out["threads_merged"] == 1
    assert await _status(actions, b) == "merged"       # the echo folds in
    assert await _status(actions, a) == "active"       # the deliberate capture survives
    assert await actions.pool.fetchval("SELECT merged_into FROM objects WHERE id=$1", b) == a


async def test_deliberate_capture_is_never_the_loser_even_when_newer(actions: Actions) -> None:
    # the DERIVED echo is OLDER; direction must still hold — grade beats recency.
    b = await _derived_thread(
        actions, "thread:echo2",
        "the briefing composition renders open threads grouped by section title",
        born=datetime.now(UTC) - timedelta(days=5))
    a = await open_thread(
        actions, "briefing composition renders open threads grouped by their section title")
    out = await consolidate_memory(actions, object_type="Thread", prefix="thread:")
    assert out["threads_merged"] == 1
    assert await _status(actions, b) == "merged"       # DERIVED folds in despite being older
    assert await _status(actions, a) == "active"       # SELF_DECLARED wins despite being newer


async def test_two_deliberate_captures_are_left_for_review(actions: Actions) -> None:
    a = await open_thread(
        actions, "the generic renderer dispatches on result shape reading schema styling")
    b = await open_thread(
        actions, "generic renderer dispatches on result shape and reads schema styling")
    out = await consolidate_memory(actions, object_type="Thread", prefix="thread:")
    assert out["threads_merged"] == 0                  # genuine divergence — never auto-merged
    assert out["threads_for_review"] >= 1
    assert await _status(actions, a) == "active"
    assert await _status(actions, b) == "active"


async def test_two_derived_echoes_are_surfaced_not_merged(actions: Actions) -> None:
    # near-duplicate but NO deliberate anchor — bag-of-tokens can't safely pick a survivor
    # (it can't tell "do X to M5" from "do Y to M5"), so both persist and it goes to review.
    a = await _derived_thread(
        actions, "thread:d1", "the composer renderer dispatches on result shape reading schema")
    b = await _derived_thread(
        actions, "thread:d2", "composer renderer dispatches on result shape and reads schema now")
    out = await consolidate_memory(actions, object_type="Thread", prefix="thread:")
    assert out["threads_merged"] == 0
    assert out["threads_for_review"] >= 1
    assert await _status(actions, a) == "active"
    assert await _status(actions, b) == "active"


async def test_unrelated_threads_are_left_alone(actions: Actions) -> None:
    a = await open_thread(
        actions, "the composer renderer dispatches on result shape and schema styling")
    b = await _derived_thread(
        actions, "thread:unrel", "the satellite poller advances its cursor forward only")
    out = await consolidate_memory(actions, object_type="Thread", prefix="thread:")
    assert out["threads_merged"] == 0
    assert await _status(actions, a) == "active"
    assert await _status(actions, b) == "active"

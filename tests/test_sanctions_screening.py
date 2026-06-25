"""The crawl half meeting the ingested-base half: footprint screened vs watchlist."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.ingest.opensanctions import ingest_ftm
from src.ontology.resolution import find_sanctions_candidates, review_tray

NOW = datetime(2026, 6, 24, tzinfo=UTC)

_WATCHLIST = [
    {"id": "S1", "schema": "Person",
     "properties": {"name": ["Kim Jong Un"], "country": ["kp"], "topics": ["sanction"]}},
    {"id": "S2", "schema": "Person", "properties": {"name": ["Some Other Target"]}},
]


async def test_crawled_name_match_flags_watchlist_entity(actions: Actions) -> None:
    await ingest_ftm(actions, _WATCHLIST)

    # a crawled identity (from a non-opensanctions source) with a colliding name
    me = await actions.create_or_find_object("Person", "subject:case", "analyst")
    await actions.assert_property(me, "name", "Kim Jong Un", "gravatar", NOW, 0.8)
    # a crawled identity that should NOT match anything
    other = await actions.create_or_find_object("Person", "subject:other", "analyst")
    await actions.assert_property(other, "name", "Priya Kowalski", "github_deep", NOW, 0.8)

    queued = await find_sanctions_candidates(actions.pool)
    assert queued == 1  # only the Kim Jong Un collision

    s1 = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='S1'")
    tray = await review_tray(actions.pool)
    pair = next(t for t in tray if me in (t["a_id"], t["b_id"]))
    assert {pair["a_id"], pair["b_id"]} == {me, s1}
    assert pair["score"] == 0.5

    # idempotent: re-running queues nothing new
    assert await find_sanctions_candidates(actions.pool) == 0


async def test_short_names_do_not_collide(actions: Actions) -> None:
    await ingest_ftm(actions, [{"id": "S3", "schema": "Person", "properties": {"name": ["Li"]}}])
    obj = await actions.create_or_find_object("Person", "p:x", "analyst")
    await actions.assert_property(obj, "name", "Li", "github_deep", NOW, 0.8)
    assert await find_sanctions_candidates(actions.pool) == 0  # length guard

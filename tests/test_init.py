"""osiris init — a fresh migrated DB becomes a usable console (rooms + lenses + canon).

The defect: a clean DB seeds nothing, so the console lands on nothing and the README's
promised composition doesn't exist. init fixes that; running it twice must add nothing.
Real Postgres (the `actions` fixture truncates rooms/compositions), and pytest runs from the
repo root so the canon step ingests the REAL docs/reference/ — no fixtures, no mocks.
"""

from __future__ import annotations

import json

from src.actions.core import Actions
from src.init import ROOM_COMPOSITIONS, ROOM_CONFIG, init
from src.orchestrator.compositions import DEFAULT_COMPOSITIONS


def test_room_compositions_are_real_and_cover_the_defaults() -> None:
    """The room map invents NO compositions (every name exists in DEFAULT_COMPOSITIONS) and
    rooms EVERY default (no orphan visible only in All)."""
    assigned = [c for comps in ROOM_COMPOSITIONS.values() for c in comps]
    assert set(assigned) <= set(DEFAULT_COMPOSITIONS)          # no invented lens
    assert set(assigned) == set(DEFAULT_COMPOSITIONS)          # all defaults roomed
    assert len(assigned) == len(set(assigned))                 # no lens in two rooms
    assert set(ROOM_COMPOSITIONS) == set(ROOM_CONFIG)          # same rooms both maps


def test_comp_meta_covers_every_default_composition() -> None:
    """thread 36352764: roadmap/docs shipped with no `_COMP_META` entry and silently seeded
    with section=None — present in the DB, invisible in the sidebar, no error anywhere. Every
    DEFAULT_COMPOSITIONS name must have shelf metadata (a superset is fine — `_COMP_META` also
    covers agent-authored twins the seeder's second pass reaches by name, see
    compositions.seed_default_compositions)."""
    from src.orchestrator.compositions import _COMP_META

    assert set(DEFAULT_COMPOSITIONS) <= set(_COMP_META)


async def test_init_seeds_rooms_compositions_and_canon(actions: Actions) -> None:
    res = await init(actions)
    p = actions.pool

    # rooms exist with the exact config the UI reads (config.home / config.collect)
    rooms = {r["name"]: json.loads(r["config"]) if isinstance(r["config"], str) else r["config"]
             for r in await p.fetch("SELECT name, config FROM rooms")}
    assert rooms["engineer"] == {"home": "briefing"}
    assert rooms["analyst"] == {"home": "who-is-this", "collect": True}
    # engineer is created first → a fresh box lands on it (list_rooms orders by created_at)
    first = await p.fetchval("SELECT name FROM rooms ORDER BY created_at LIMIT 1")
    assert first == "engineer"

    # every default composition exists AND is roomed (none orphaned to the All scope)
    assert await p.fetchval("SELECT count(*) FROM compositions") == len(DEFAULT_COMPOSITIONS)
    assert await p.fetchval(
        "SELECT count(*) FROM compositions WHERE room_id IS NULL") == 0
    # every default landed on a real shelf section — thread 36352764: roadmap/docs silently
    # seeded with section=None (invisible in the sidebar) until caught live in production
    assert await p.fetchval(
        "SELECT count(*) FROM compositions WHERE section IS NULL") == 0
    # the specific attachments the console lands on
    for room, comps in ROOM_COMPOSITIONS.items():
        rid = await p.fetchval("SELECT id FROM rooms WHERE name=$1", room)
        got = {r["name"] for r in await p.fetch(
            "SELECT name FROM compositions WHERE room_id=$1", rid)}
        assert got == set(comps)

    # the README's promised lens now runs (was the "no composition" failure)
    assert await p.fetchval(
        "SELECT count(*) FROM compositions WHERE name='decision-log'") == 1

    # the design canon landed (real docs/reference/ + own docs)
    assert res["canon"]["vendor"] >= 5 and res["canon"]["own"] >= 2
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='Reference'") >= 7


async def test_init_is_idempotent(actions: Actions) -> None:
    """Running init twice adds nothing — rooms/compositions/canon counts stable."""
    await init(actions)
    p = actions.pool
    before = (
        await p.fetchval("SELECT count(*) FROM rooms"),
        await p.fetchval("SELECT count(*) FROM compositions"),
        await p.fetchval("SELECT count(*) FROM objects WHERE type='Reference'"),
        await p.fetchval("SELECT count(*) FROM links WHERE type IN ('cites','informs','mentions')"),
    )

    res2 = await init(actions)                                  # second run
    after = (
        await p.fetchval("SELECT count(*) FROM rooms"),
        await p.fetchval("SELECT count(*) FROM compositions"),
        await p.fetchval("SELECT count(*) FROM objects WHERE type='Reference'"),
        await p.fetchval("SELECT count(*) FROM links WHERE type IN ('cites','informs','mentions')"),
    )
    assert before == after                                     # nothing duplicated
    # the canon is a bootstrap seed — a second init skips it (guarded on Reference presence), so
    # reference.py's non-idempotent `cites` wiring never duplicates the COMPOSER→vendor edges
    assert res2["canon"] == {"skipped": "canon already present"}
    # rooms survive the second seed pass (COALESCE keeps room_id — no orphaning)
    assert await p.fetchval("SELECT count(*) FROM compositions WHERE room_id IS NULL") == 0

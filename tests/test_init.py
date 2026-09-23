"""osiris init: a fresh migrated DB becomes a usable console (lenses + canon).

The defect: a clean DB seeds nothing, so the console lands on nothing and the README's
promised composition doesn't exist. init fixes that; running it twice must add nothing.

ROOM RETIREMENT: rooms are retired, for good, a product shipped to strangers cannot
re-grow a concept that was retired on a prior init. Every default composition seeds
GLOBAL (room_id NULL); migration 0070's own backfill (room_id -> former_room_id on the 31
pre-existing scoped rows) is what stands as the permanent record on installs that predate
this: a fresh box never re-creates rooms at all.

Real Postgres (the `actions` fixture truncates rooms/compositions), and pytest runs from the
repo root so the canon step ingests the REAL docs/reference/: no fixtures, no mocks.
"""

from __future__ import annotations

from src.actions.core import Actions
from src.init import init
from src.orchestrator.compositions import _COMP_META, DEFAULT_COMPOSITIONS


def test_comp_meta_covers_every_default_composition() -> None:
    """roadmap/docs shipped with no `_COMP_META` entry and silently seeded with section=None:
    present in the DB, invisible in the sidebar, no error anywhere. Every DEFAULT_COMPOSITIONS
    name must have shelf metadata (a superset is fine, `_COMP_META` also covers agent-authored
    twins the seeder's second pass reaches by name, see
    compositions.seed_default_compositions)."""
    assert set(DEFAULT_COMPOSITIONS) <= set(_COMP_META)


async def test_init_seeds_compositions_global_and_canon(actions: Actions) -> None:
    res = await init(actions)
    p = actions.pool

    # rooms are retired, a fresh box creates none at all.
    assert await p.fetchval("SELECT count(*) FROM rooms") == 0

    # every default composition exists AND is seeded global (room_id NULL); rooms are
    # retired, so nothing here re-grows the concept on a fresh box.
    assert await p.fetchval("SELECT count(*) FROM compositions") == len(DEFAULT_COMPOSITIONS)
    assert await p.fetchval(
        "SELECT count(*) FROM compositions WHERE room_id IS NOT NULL") == 0
    assert await p.fetchval(
        "SELECT count(*) FROM compositions WHERE former_room_id IS NOT NULL") == 0
    # every default landed on a real shelf section: roadmap/docs silently
    # seeded with section=None (invisible in the sidebar) until caught live in production
    assert await p.fetchval(
        "SELECT count(*) FROM compositions WHERE section IS NULL") == 0

    # the README's promised lens now runs (was the "no composition" failure)
    assert await p.fetchval(
        "SELECT count(*) FROM compositions WHERE name='decision-log'") == 1

    # the design canon landed (real docs/reference/ + own docs)
    assert res["canon"]["vendor"] >= 5 and res["canon"]["own"] >= 2
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='Reference'") >= 7


async def test_init_compositions_only_skips_canon(actions: Actions) -> None:
    """`python -m src.init --compositions-only` routes to `canon=False`: a deploy step for
    syncing a new DEFAULT_COMPOSITIONS entry into a live DB without also paying for a canon
    re-ingest. Compositions still seed, global."""
    res = await init(actions, canon=False)
    assert res["canon"] is None
    p = actions.pool
    assert await p.fetchval("SELECT count(*) FROM compositions") == len(DEFAULT_COMPOSITIONS)
    assert await p.fetchval(
        "SELECT count(*) FROM compositions WHERE room_id IS NOT NULL") == 0
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='Reference'") == 0


async def test_init_is_idempotent(actions: Actions) -> None:
    """Running init twice adds nothing: compositions/canon counts stable."""
    await init(actions)
    p = actions.pool
    before = (
        await p.fetchval("SELECT count(*) FROM compositions"),
        await p.fetchval("SELECT count(*) FROM objects WHERE type='Reference'"),
        await p.fetchval("SELECT count(*) FROM links WHERE type IN ('cites','informs','mentions')"),
    )

    res2 = await init(actions)                                  # second run
    after = (
        await p.fetchval("SELECT count(*) FROM compositions"),
        await p.fetchval("SELECT count(*) FROM objects WHERE type='Reference'"),
        await p.fetchval("SELECT count(*) FROM links WHERE type IN ('cites','informs','mentions')"),
    )
    assert before == after                                     # nothing duplicated
    # the canon is a bootstrap seed, a second init skips it (guarded on Reference presence), so
    # reference.py's non-idempotent `cites` wiring never duplicates the COMPOSER→vendor edges
    assert res2["canon"] == {"skipped": "canon already present"}
    # compositions stay global on the second seed pass too, no room re-grown
    assert await p.fetchval("SELECT count(*) FROM compositions WHERE room_id IS NOT NULL") == 0

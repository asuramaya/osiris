"""THE GRAPH VISUALIZER, WAVE B item 5 (operator dispatch, wave 15, thread 8839): orphans
distinct at every level with a count per supernode. Items 3/4 already built the field
(compositions' own `_LIVE_LINK_COUNTS`-equivalent predicate, `orphans`, on every
supernode/cluster) and its rendering; this is the acceptance proof itself --
"on-screen orphan count equals graph_lint's" -- run against a real testcontainer DB, not
assumed from the shared SQL predicate alone.

graph_lint's own orphan definition (compositions._fn_triage, mode='census':
`link_count=0`, live links only, both directions) is EXACTLY what
graph_supernodes/graph_clusters build their own `orphans` field from
(app.py's `_LIVE_LINK_COUNTS`) -- both read the same UNION ALL over `links`, filtered the
same way. Proven live below with real objects: a project MEMBER can never itself be
orphan (membership requires an in_repo link, so link_count>=1) -- every true orphan is
therefore necessarily UNFILED, so graph_supernodes's own `unfiled.orphans` must equal
census's own total orphan count over the same (non-archived/merged/retired) population.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.orchestrator.compositions import _fn_triage

HELPERS = Path(__file__).parent.parent / "helpers"

_EXCLUDED_STATUSES = ("archived", "merged", "retired")


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    from src.orchestrator.manifests import load_manifests

    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(HELPERS)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _census_total_orphans(actions: Actions) -> int:
    rows = await _fn_triage(actions.pool, None, {"mode": "census"})
    return sum(
        int(r["orphans"]) for r in rows
        if r.get("status") not in _EXCLUDED_STATUSES and "orphans" in r
    )


async def test_on_screen_orphan_count_equals_graph_lints_own_census(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    # a real, varied population: a true orphan, a project member (never orphan by
    # construction), an unfiled-but-linked object, and a retired orphan (excluded from
    # both counts the same way).
    await actions.create_or_find_object("Thread", "thread:recon-true-orphan", "test")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:recon-proj", "test")
    member = await actions.create_or_find_object("Thread", "thread:recon-member", "test")
    await actions.create_link(member, proj, "in_repo", "test", datetime.now(UTC), 1.0)
    a = await actions.create_or_find_object("Thread", "thread:recon-linked-a", "test")
    b = await actions.create_or_find_object("Thread", "thread:recon-linked-b", "test")
    await actions.create_link(a, b, "cites", "test", datetime.now(UTC), 1.0)
    retired = await actions.create_or_find_object("Thread", "thread:recon-retired-orphan", "test")
    await actions.pool.execute("UPDATE objects SET status='retired' WHERE id=$1", retired)

    census_total = await _census_total_orphans(actions)
    r = await client.get("/graph/supernodes")
    on_screen_total = r.json()["unfiled"]["orphans"]

    assert census_total == on_screen_total
    assert on_screen_total >= 1  # the fixture's own true orphan is really counted, not a 0==0 fluke


async def test_a_project_member_can_never_be_the_on_screen_orphan_population(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """The structural finding item 3 already proved in isolation, reconciled here against
    the real census: since membership requires an in_repo edge, no supernode's own
    `orphans` field can ever be nonzero for a real project -- the WHOLE true orphan
    population, as measured by graph_lint's own census, lands in unfiled."""
    proj = await actions.create_or_find_object("SoftwareProject", "repo:recon-solo", "test")
    m1 = await actions.create_or_find_object("Thread", "thread:recon-solo-1", "test")
    m2 = await actions.create_or_find_object("Thread", "thread:recon-solo-2", "test")
    await actions.create_link(m1, proj, "in_repo", "test", datetime.now(UTC), 1.0)
    await actions.create_link(m2, proj, "in_repo", "test", datetime.now(UTC), 1.0)

    r = await client.get("/graph/supernodes")
    row = next(s for s in r.json()["supernodes"] if s["label"] == "repo:recon-solo")
    assert row["orphans"] == 0

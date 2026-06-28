"""Phase 3 — the broker PoC: a real source watcher driving the full watch loop.

Proves the loop on an easy, keyless source (new SEC Form D filings) with the network
injected: schedule (tick) -> delta (only filings newer than the cursor) -> ingest
(Organizations via Actions) -> outbox -> subscription evaluator -> a sourced alert,
AND that it stays quiet when there is no news / on filings that don't match the watch.
"""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.compositions import save_watch
from src.orchestrator.monitor import (
    evaluate_watches,
    get_cursor,
    tick,
)
from src.orchestrator.watchers import make_form_d_watcher

# canned EFTS hits (what efts_form_d_fetch would return), oldest..newest
_NEURALINK = {"cik": "0001708503", "issuer": "Neuralink Corp.",
              "file_date": "2026-06-20", "accession": "0001-26-001"}
_APPLE = {"cik": "0000320193", "issuer": "Apple Inc.",
          "file_date": "2026-06-21", "accession": "0002-26-002"}
_NEW = {"cik": "0001999999", "issuer": "Neuralink Devices LLC",
        "file_date": "2026-06-25", "accession": "0003-26-003"}


def _fetch_returning(hits: list[dict[str, str]]):
    async def fetch(query: str) -> list[dict[str, str]]:
        return hits
    return fetch


async def test_tick_ingests_filings_and_advances_cursor(actions: Actions) -> None:
    watcher = make_form_d_watcher("form-d", fetch=_fetch_returning([_NEURALINK, _APPLE]))
    n = await tick(actions, "form_d:test", watcher)
    assert n == 2
    # cursor is the newest file_date in the feed
    assert await get_cursor(actions.pool, "source:form_d:test") == "2026-06-21"
    # the filings landed as Organizations carrying their SEC facts
    row = await actions.pool.fetchrow(
        "SELECT a.value #>> '{}' AS v FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='cik:0001708503' AND a.name='name'"
    )
    assert row["v"] == "Neuralink Corp."


async def test_unchanged_feed_yields_no_news(actions: Actions) -> None:
    """A re-poll of a feed with nothing newer than the cursor ingests nothing —
    the watch is quiet when there is no news (no 3am false alert)."""
    feed = [_NEURALINK, _APPLE]
    watcher = make_form_d_watcher("form-d", fetch=_fetch_returning(feed))
    assert await tick(actions, "form_d:test", watcher) == 2
    assert await tick(actions, "form_d:test", watcher) == 0  # same feed, nothing fresh
    assert await get_cursor(actions.pool, "source:form_d:test") == "2026-06-21"


async def test_only_strictly_newer_filings_are_pulled(actions: Actions) -> None:
    watcher_a = make_form_d_watcher("form-d", fetch=_fetch_returning([_NEURALINK, _APPLE]))
    await tick(actions, "form_d:test", watcher_a)  # cursor -> 2026-06-21
    # a later poll sees a NEW filing (2026-06-25) alongside the two already-seen
    watcher_b = make_form_d_watcher(
        "form-d", fetch=_fetch_returning([_NEURALINK, _APPLE, _NEW])
    )
    n = await tick(actions, "form_d:test", watcher_b)
    assert n == 1  # only the strictly-newer filing
    assert await get_cursor(actions.pool, "source:form_d:test") == "2026-06-25"


async def test_full_loop_fires_on_match_quiet_on_noise(actions: Actions) -> None:
    """The broker proof end-to-end: a saved watch fires a sourced alert on the new
    filing it cares about, and stays silent on the one it doesn't."""
    await save_watch(
        actions.pool, "new Neuralink financings", "Organization",
        [{"property": "name", "op": "contains", "value": "neuralink"}],
    )
    watcher = make_form_d_watcher("form-d", fetch=_fetch_returning([_NEURALINK, _APPLE]))
    await tick(actions, "form_d:test", watcher)

    fired = await evaluate_watches(actions.pool)
    assert fired == 1  # Neuralink filing matched; Apple (noise) did not
    alerted = await actions.pool.fetchval(
        "SELECT o.canonical FROM alerts a JOIN objects o ON o.id=a.object_id LIMIT 1"
    )
    assert alerted == "cik:0001708503"

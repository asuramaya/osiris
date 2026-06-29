"""Harris County foreclosure watcher — the broker beat (ForeScan, done right).

Texas counties post a Notice of (Substitute) Trustee Sale 21 days before the
first-Tuesday auction. Harris County publishes them at the County Clerk's portal
(cclerk.hctx.net/applications/websearch/FRCL_R.aspx). The broker's edge is speed:
turn a freshly-filed notice into a sourced lead the day it posts.

This is the county last-mile the ForeScan grave is about — and the discipline holds:
the portal is an ASPX web-form, NOT a clean API, so the LIVE fetch is an HTML/postback
scrape (`live_fetch`, a documented integration point that a placeful satellite serves;
it is intentionally not a speculative mass-crawl). The PIPELINE, though, is fully real:
a notice → a graded `Property` node → the leads feed → an optional alert. To let the
front end be felt now, a clearly-labelled DEMO dataset stands in for the live scrape.

Each notice becomes a `Property` object keyed on its filing id, carrying the sale
facts as AUTHORITATIVE_API assertions (a county record is authoritative).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from src.orchestrator.monitor import Puller, PullResult, WatchItem
from src.parsers.base import EvidenceClass

_SOURCE = "harris_county_clerk"
_PORTAL = "https://www.cclerk.hctx.net/applications/websearch/FRCL_R.aspx"

# The fields a Texas Notice of Substitute Trustee Sale carries. Keys map 1:1 to the
# Property object's properties. `filed_date` (YYYY-MM-DD) is the watcher cursor.
_FIELDS = (
    "address", "zip", "owner", "lienholder", "trustee",
    "sale_date", "opening_bid", "filed_date", "notice_url",
)


def _watch_item(notice: dict[str, Any]) -> WatchItem:
    props: dict[str, Any] = {k: notice.get(k) for k in _FIELDS}
    props["county"] = "Harris"
    # the human-readable NAME of a property is its address — so the generic card
    # renderer titles the lead by address without knowing what a foreclosure is.
    if notice.get("address"):
        props["name"] = notice["address"]
    if notice.get("demo"):
        props["demo"] = "true"
    return WatchItem(
        type="Property",
        canonical=f"harris-notice:{notice['doc_id']}",
        properties=props,
        evidence_class=EvidenceClass.AUTHORITATIVE_API,
    )


def make_harris_foreclosure_watcher(
    *, fetch: Any,
) -> Puller:
    """A watcher over Harris County trustee-sale notices. `fetch(cursor)` returns the
    raw notice dicts (the live scrape, or the demo set); only notices filed strictly
    after the cursor are emitted, so a re-poll with no new filings is silent."""

    async def puller(cursor: str | None) -> PullResult:
        notices: list[dict[str, Any]] = await fetch(cursor)
        fresh = [
            n for n in notices
            if n.get("doc_id") and (cursor is None or (n.get("filed_date") or "") > cursor)
        ]
        items = [_watch_item(n) for n in fresh]
        newest = max((n.get("filed_date") or "" for n in notices), default="")
        return PullResult(items=items, cursor=max(newest, cursor or ""))

    return puller


# --- satellite collector: the vantage-bound last mile ------------------------

def make_harris_collector(*, fetch: Any) -> Callable[[Any], Awaitable[list[WatchItem]]]:
    """A satellite Collector over the county portal. It runs AT a vantage with portal
    access (the placeful last mile — NOT a placeless mass-scrape; the ForeScan grave is
    exactly the thing we don't re-enter). `job.target` carries the cursor (last filed_date);
    only strictly-newer notices become Property WatchItems, emitted into the CENTRAL graph
    by the satellite runner. `fetch` is injected (live_fetch in prod, demo_fetch in tests)."""

    async def collector(job: Any) -> list[WatchItem]:
        cursor = (getattr(job, "target", "") or None)
        notices: list[dict[str, Any]] = await fetch(cursor)
        fresh = [
            n for n in notices
            if n.get("doc_id") and (cursor is None or (n.get("filed_date") or "") > cursor)
        ]
        return [_watch_item(n) for n in fresh]

    return collector


# --- live fetch: the integration point (HTML/ASPX — satellite-shaped) --------

async def live_fetch(cursor: str | None) -> list[dict[str, Any]]:  # pragma: no cover
    """Scrape new notices from the County Clerk foreclosure portal. NOT IMPLEMENTED:
    FRCL_R.aspx is an ASPX postback form (search by Document ID / Sale Date / File
    Date), so this needs an HTML session + result parse — a placeful-satellite job,
    not a clean federation. Wire it here (or as a satellite Collector) when collecting
    live; the rest of the pipeline (parse → grade → lead → alert) is already real."""
    raise NotImplementedError(
        f"live Harris County scrape is the integration point ({_PORTAL}); "
        "use the demo dataset or a satellite collector for now"
    )


# The LIVE collector — registered as a satellite kind. THE WALL: `live_fetch` raises until a
# satellite runs it on a box with portal access (FRCL_R.aspx is an ASPX postback / antibot
# form). The seam is real and the whole pipeline past it (parse→grade→lead→alert) works; the
# placeful last mile is the operator's vantage, dispatched as a collection job.
harris_collector = make_harris_collector(fetch=live_fetch)


# --- DEMO dataset: synthetic notices so the front end can be felt -------------
# Clearly fictional owners; real Houston ZIPs; first-Tuesday 2026 sale dates. Every
# row is flagged demo=true and rendered with a DEMO badge — nothing implies a real
# person is in foreclosure.
SAMPLE_NOTICES: list[dict[str, Any]] = [
    {"doc_id": "DEMO-0001", "address": "18330 Olive Leaf Dr, Houston, TX",
     "zip": "77084", "owner": "DEMO — Rivera Family Trust",
     "lienholder": "Cornerstone Mortgage Co.", "trustee": "Buckley Bala Wilson Mann LLC",
     "sale_date": "2026-07-07", "opening_bid": "248000", "filed_date": "2026-06-16",
     "notice_url": _PORTAL, "demo": True},
    {"doc_id": "DEMO-0002", "address": "5102 Bellaire Blvd, Houston, TX",
     "zip": "77401", "owner": "DEMO — Okafor Holdings LLC",
     "lienholder": "Gulf Coast Educators FCU", "trustee": "Hughes Watters Askanase",
     "sale_date": "2026-07-07", "opening_bid": "612500", "filed_date": "2026-06-19",
     "notice_url": _PORTAL, "demo": True},
    {"doc_id": "DEMO-0003", "address": "2207 Engelmohr St, Houston, TX",
     "zip": "77054", "owner": "DEMO — J. Castillo",
     "lienholder": "PennyMac Loan Services", "trustee": "Codilis & Stawiarski",
     "sale_date": "2026-07-07", "opening_bid": "189900", "filed_date": "2026-06-20",
     "notice_url": _PORTAL, "demo": True},
    {"doc_id": "DEMO-0004", "address": "14411 Cypress North Houston Rd, Cypress, TX",
     "zip": "77429", "owner": "DEMO — Nguyen Investments",
     "lienholder": "Rocket Mortgage", "trustee": "Mackie Wolf Zientz & Mann",
     "sale_date": "2026-07-07", "opening_bid": "327000", "filed_date": "2026-06-22",
     "notice_url": _PORTAL, "demo": True},
    {"doc_id": "DEMO-0005", "address": "9015 Long Point Rd, Houston, TX",
     "zip": "77055", "owner": "DEMO — Patel Enterprises",
     "lienholder": "Wells Fargo Bank NA", "trustee": "Barrett Daffin Frappier",
     "sale_date": "2026-07-07", "opening_bid": "455000", "filed_date": "2026-06-23",
     "notice_url": _PORTAL, "demo": True},
    {"doc_id": "DEMO-0006", "address": "3320 Dixie Dr, Houston, TX",
     "zip": "77021", "owner": "DEMO — M. Thompson",
     "lienholder": "Freedom Mortgage Corp.", "trustee": "Marinosci Law Group",
     "sale_date": "2026-07-07", "opening_bid": "164500", "filed_date": "2026-06-24",
     "notice_url": _PORTAL, "demo": True},
    {"doc_id": "DEMO-0007", "address": "1207 Magnolia Bend Dr, Katy, TX",
     "zip": "77494", "owner": "DEMO — Sandoval Family Trust",
     "lienholder": "Cardinal Financial Co.", "trustee": "Power Default Services",
     "sale_date": "2026-07-07", "opening_bid": "289900", "filed_date": "2026-06-25",
     "notice_url": _PORTAL, "demo": True},
    {"doc_id": "DEMO-0008", "address": "7706 Antoine Dr, Houston, TX",
     "zip": "77088", "owner": "DEMO — Greenline Properties LLC",
     "lienholder": "Lakeview Loan Servicing", "trustee": "Robertson Anschutz Schneid",
     "sale_date": "2026-07-07", "opening_bid": "132750", "filed_date": "2026-06-26",
     "notice_url": _PORTAL, "demo": True},
]


async def demo_fetch(cursor: str | None) -> list[dict[str, Any]]:
    """A stand-in fetch returning the bundled synthetic notices (the live scrape's
    shape) so the leads feed populates without a live county session."""
    return SAMPLE_NOTICES

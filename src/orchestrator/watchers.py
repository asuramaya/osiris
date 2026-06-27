"""Source watchers — real pullers for the watch (cron Phase 3).

A watcher is a `monitor.Puller`: given the last cursor, it pulls only the delta of
NEW public records and returns them as `WatchItem`s plus the advanced cursor. The
generic `tick` machinery (monitor.py) materializes the items through Actions and
advances the cursor; the durable outbox + the subscription evaluator then turn a
matching new record into a sourced alert. This module supplies the *source-specific*
half — the fetch + the cursor semantics — for one easy, already-keyless source:
new SEC Form D filings.

The network fetch is injected (a `Fetch` callable) so the watcher is testable with a
canned delta and no network; `efts_form_d_fetch` is the live implementation.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from src.orchestrator.monitor import Puller, PullResult, WatchItem
from src.parsers.base import EvidenceClass

# EDGAR full-text search (keyless). SEC inverts antibot: it 403s browser UAs and
# wants a contact UA (matches src/ingest/edgar*.py).
_EFTS = "https://efts.sec.gov/LATEST/search-index"
_UA = {"User-Agent": "osiris-osint operator@example.com"}

# A fetch returns the raw filing hits for a query: each {cik, issuer, file_date,
# accession}. Injected so the watcher is hermetic; the live impl is below.
Fetch = Callable[[str], Awaitable[list[dict[str, str]]]]


async def efts_form_d_fetch(query: str) -> list[dict[str, str]]:
    """Live EFTS fetch: Form D filings mentioning `query`, with their file dates."""
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    async with httpx.AsyncClient(timeout=30.0, headers=_UA, follow_redirects=True) as client:
        r = await client.get(_EFTS, params={"q": f'"{query}"', "forms": "D"})
        r.raise_for_status()
        for hit in r.json().get("hits", {}).get("hits", []):
            src: dict[str, Any] = hit.get("_source", {})
            acc = (hit.get("_id") or "").split(":")[0]
            file_date = src.get("file_date", "")
            ciks = src.get("ciks", [])
            names = src.get("display_names", [])
            for cik, disp in zip(ciks, names, strict=False):
                key = (cik, acc)
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    {"cik": cik, "issuer": disp, "file_date": file_date, "accession": acc}
                )
    return out


def make_form_d_watcher(query: str, *, fetch: Fetch = efts_form_d_fetch) -> Puller:
    """A watcher for NEW Form D filings mentioning `query`. The cursor is the most
    recent `file_date` (YYYY-MM-DD) seen; a tick emits only filings strictly newer,
    so a re-poll of an unchanged feed yields nothing (quiet on no-news). Idempotency
    means a same-day re-filing that slips through still find-or-creates, not dupes."""

    async def puller(cursor: str | None) -> PullResult:
        hits = await fetch(query)
        fresh = [h for h in hits if cursor is None or (h.get("file_date") or "") > cursor]
        items = [
            WatchItem(
                type="Organization",
                canonical=f"cik:{h['cik']}",
                properties={
                    "name": h["issuer"],
                    "form_type": "D",
                    "filed_date": h.get("file_date") or None,
                },
                evidence_class=EvidenceClass.AUTHORITATIVE_API,
            )
            for h in fresh
            if h.get("cik")
        ]
        # advance to the newest date in the WHOLE feed (not just fresh) so an empty
        # delta still records "we've seen up to here" and never rewinds.
        newest = max((h.get("file_date") or "" for h in hits), default="")
        new_cursor = max(newest, cursor or "")
        return PullResult(items=items, cursor=new_cursor)

    return puller

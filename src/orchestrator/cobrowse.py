"""Co-browse: resolve a handoff by driving a real browser (DESIGN §9 mode 1).

The operator opens the parked URL in their genuine Chrome (over CDP); we scrape
the rendered DOM, capture the session cookies as a lease for later server-side
reuse, run the helper's scraper to shape the result, and post it back — which
finishes the run and lets downstream triggers cascade. This is the live path
that complements the analyst manually posting back from a browser extension.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.actions.core import Actions
from src.connectors.browser import BrowserResult, co_browse
from src.connectors.leases import LeaseStore, origin_of
from src.orchestrator.handoff import HandoffError, open_handoff, post_back
from src.orchestrator.manifests import Manifest

# A helper-specific function turning the rendered page into the dict its parser
# consumes (the analyst's extension does the equivalent client-side).
Scraper = Callable[[BrowserResult], dict[str, Any]]


async def resolve_handoff_via_browser(
    actions: Actions,
    lease_store: LeaseStore,
    manifest: Manifest,
    handoff_id: int,
    *,
    scraper: Scraper,
    bound_ip: str,
    issued_by: str,
    cdp_endpoint: str | None = None,
    lease_ttl_seconds: int = 900,
) -> dict[str, int]:
    row = await actions.pool.fetchrow(
        "SELECT url, origin FROM handoffs WHERE id=$1 AND resolved_at IS NULL", handoff_id
    )
    if row is None or row["url"] is None:
        raise HandoffError(f"handoff {handoff_id} not resolvable (missing or no url)")

    await open_handoff(actions, handoff_id)
    result = await co_browse(row["url"], cdp_endpoint=cdp_endpoint)

    # Capture the solved session as a lease for short-TTL server-side reuse,
    # keyed by the actual origin visited (what fetch_with_lease will look up).
    if result.cookies:
        await lease_store.capture(
            origin_of(result.url),
            result.cookies,
            result.ua,
            bound_ip=bound_ip,
            ttl_seconds=lease_ttl_seconds,
            issued_by=issued_by,
        )

    posted = scraper(result)
    return await post_back(actions, manifest, handoff_id, posted)


async def cobrowse_open(
    actions: Actions,
    lease_store: LeaseStore,
    handoff_id: int,
    *,
    bound_ip: str,
    issued_by: str,
    cdp_endpoint: str | None = None,
    lease_ttl_seconds: int = 900,
) -> dict[str, Any]:
    """Open a handoff's URL in a real browser, capture the session as a lease, and
    return a summary for the analyst to review (no auto-parse — they post back or
    promote). The lightweight path for arbitrary gated/suggest link-outs."""
    row = await actions.pool.fetchrow(
        "SELECT url FROM handoffs WHERE id=$1 AND resolved_at IS NULL", handoff_id
    )
    if row is None or row["url"] is None:
        raise HandoffError(f"handoff {handoff_id} not openable")
    await open_handoff(actions, handoff_id)
    result = await co_browse(row["url"], cdp_endpoint=cdp_endpoint)
    lease_captured = False
    if result.cookies:
        await lease_store.capture(
            origin_of(result.url), result.cookies, result.ua,
            bound_ip=bound_ip, ttl_seconds=lease_ttl_seconds, issued_by=issued_by,
        )
        lease_captured = True
    return {
        "title": result.title,
        "url": result.url,
        "lease_captured": lease_captured,
        "excerpt": result.html[:600],
    }

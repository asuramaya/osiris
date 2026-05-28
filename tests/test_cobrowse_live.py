"""Live co-browse: drives a REAL Chromium against a local site, captures the
session cookie as a lease, resolves the handoff, and proves server-side reuse.

Skips automatically if Chromium can't launch on this host."""

from __future__ import annotations

import asyncio
import re
import uuid
from pathlib import Path

import pytest
from cryptography.fernet import Fernet
from src.actions.core import Actions
from src.connectors.browser import BrowserResult
from src.connectors.leases import LeaseStore, fetch_with_lease, origin_of
from src.orchestrator.budgets import BudgetLedger
from src.orchestrator.cobrowse import resolve_handoff_via_browser
from src.orchestrator.handoff import suspend, tray
from src.orchestrator.manifests import load_manifests

KEY = Fernet.generate_key()
TELEGRAM = load_manifests(Path(__file__).parent.parent / "helpers")["telegram_channel_profile"]


async def _probe_chromium() -> bool:
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        await browser.close()
    return True


@pytest.fixture(scope="session")
def chromium_available() -> bool:
    try:
        return asyncio.run(_probe_chromium())
    except Exception:
        return False


def _scrape(result: BrowserResult) -> dict:
    """Helper-specific: turn the rendered DOM into the parser's input shape.
    (Playwright normalizes attribute quotes, so match either style.)"""
    subs = re.search(r"id=[\"']subs[\"']>(\d+)", result.html)
    desc = re.search(r"id=[\"']desc[\"']>([^<]+)", result.html)
    return {
        "title": result.title,
        "subscribers": int(subs.group(1)) if subs else None,
        "description": desc.group(1) if desc else None,
    }


async def test_live_cobrowse_captures_lease_and_resolves_handoff(
    actions: Actions,
    redis_client,
    local_site: str,
    chromium_available: bool,
    monkeypatch,
    tmp_path,
) -> None:
    if not chromium_available:
        pytest.skip("Chromium cannot launch on this host")
    monkeypatch.setenv("OSIRIS_ARTIFACT_DIR", str(tmp_path))

    case_id = await actions.pool.fetchval(
        "INSERT INTO cases (name, owner, budgets) VALUES ('c','analyst:test',$1) RETURNING id",
        {"max_human_handoffs": 5},
    )
    case_uuid = uuid.UUID(str(case_id))
    channel = await actions.create_or_find_object(
        "TelegramChannel", "dprk_news", "analyst:test", case_uuid
    )
    ledger = BudgetLedger(actions.pool, redis_client)
    store = LeaseStore(actions.pool, KEY)

    # park a handoff pointing at the (local) page the operator will co-browse
    handoff_id = await suspend(
        actions, ledger, TELEGRAM, channel, case_uuid, url=local_site, challenge_kind=None
    )
    assert handoff_id is not None

    counts = await resolve_handoff_via_browser(
        actions, store, TELEGRAM, handoff_id,
        scraper=_scrape, bound_ip="127.0.0.1", issued_by="analyst:test",
    )
    assert counts["properties"] >= 3

    # properties came from a REAL browser render (page <title> = "DPRK News")
    title = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='title'",
        channel,
    )
    assert title == "DPRK News"
    assert await tray(actions, case_id=case_uuid) == []  # handoff resolved

    # the session cookie set by the page was captured as a lease...
    lease = await store.get(origin_of(local_site))
    assert lease is not None
    assert any(c["value"] == "secret123" for c in lease.cookies)

    # ...and that lease now opens the protected endpoint server-side (no browser)
    res = await fetch_with_lease(f"{local_site}/protected", lease)
    assert "topsecret" in res["html"]

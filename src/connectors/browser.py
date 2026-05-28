"""Browser driver seam — Playwright-CDP against the analyst's real Chrome.

Design decision (reverses DESIGN §15.3): for the self-hosted single-operator box
we drive the analyst's *real* Chrome over CDP (`--remote-debugging-port`) rather
than shipping an MV3 extension. CDP attaches to the genuine profile/session, so
co-browse + cookie-lease capture work, and it's far less build friction solo.

NOTE: Playwright is NOT yet a project dependency (it pulls a ~150 MB browser).
This module lazy-imports it so the rest of the system — including the entire
handoff state machine, which is what makes Phase 4 valuable — runs and tests
without it. Wiring real CDP is a one-line `uv add playwright` + `playwright
install chromium` when the operator wants live co-browse.
"""

from __future__ import annotations

from typing import Any


async def fetch_via_cdp(url: str, *, cdp_endpoint: str = "http://127.0.0.1:9222") -> dict[str, Any]:
    """Attach to the operator's running Chrome over CDP, load `url` in their real
    session, and return the rendered DOM. Raises if Playwright isn't installed."""
    try:
        from playwright.async_api import async_playwright  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - exercised only with playwright absent
        raise RuntimeError(
            "Playwright not installed. Run `uv add playwright` and "
            "`uv run playwright install chromium` to enable live co-browse."
        ) from exc

    async with async_playwright() as p:  # pragma: no cover - needs a live browser
        browser = await p.chromium.connect_over_cdp(cdp_endpoint)
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = await context.new_page()
        await page.goto(url, wait_until="domcontentloaded")
        html = await page.content()
        title = await page.title()
        await page.close()
        return {"url": url, "title": title, "html": html}

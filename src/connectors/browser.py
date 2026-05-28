"""Browser driver — Playwright-CDP against the analyst's real Chrome (reverses
DESIGN §15.3). For the single-operator box we drive the genuine profile/session
over CDP (`--remote-debugging-port`) rather than shipping an MV3 extension, so
co-browse + cookie-lease capture use real cookies, with far less build friction.

`co_browse` connects to a running Chrome (cdp_endpoint) for live, session-bearing
co-browse, or launches its own headless Chromium when no endpoint is given (used
in tests and for unauthenticated fetches). Either way it returns the rendered DOM
plus the context cookies, which the lease subsystem can persist for server reuse.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class BrowserResult:
    url: str
    title: str
    html: str
    ua: str
    cookies: list[dict[str, Any]] = field(default_factory=list)


async def co_browse(
    url: str,
    *,
    cdp_endpoint: str | None = None,
    headless: bool = True,
    timeout_ms: int = 30000,
) -> BrowserResult:
    """Load `url` in a real browser and return its DOM + cookies.

    cdp_endpoint set  -> attach to the operator's running Chrome (real session).
    cdp_endpoint None -> launch a private headless Chromium (no session).
    """
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        owns_browser = cdp_endpoint is None
        if cdp_endpoint is not None:
            browser = await p.chromium.connect_over_cdp(cdp_endpoint)
            context = browser.contexts[0] if browser.contexts else await browser.new_context()
        else:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context()

        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            html = await page.content()
            title = await page.title()
            ua = str(await page.evaluate("() => navigator.userAgent"))
            cookies = [dict(c) for c in await context.cookies()]
        finally:
            await page.close()
            if owns_browser:  # never close the operator's real browser
                await browser.close()

    return BrowserResult(url=url, title=title, html=html, ua=ua, cookies=cookies)

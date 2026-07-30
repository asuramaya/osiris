"""The front door's own overflow bug (Seshat's sweep, Thoth DM 1992 fix 1): app.css:111-118's
`.queue-row-title` declared `flex:1 1 auto; overflow:hidden; text-overflow:ellipsis;
white-space:nowrap` — textbook, except flex items default to `min-width:auto` (sized to
content), which defeats the very shrink the ellipsis rule depends on. Every row on :8011's
front door overflowed the viewport with NO ellipsis, NO wrap, silently clipped mid-sentence —
worse than any composition bug because there was no truncation indicator at all.

Same harness class as test_ui_render.py: load the REAL app.css into a headless page (no
backend) with the exact queue-row markup catalog.html.j2 emits, and assert no horizontal
overflow at a narrow viewport — the width the bug hid at (it only showed up narrow; a wide
viewport had enough slack to mask it).
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

_CSS = (Path(__file__).resolve().parent.parent / "src" / "api" / "inbox" / "static" /
        "app.css").read_text()

# a title long enough that, unclamped, it would blow well past a narrow viewport
_LONG_TITLE = "A queue row title long enough to overflow any narrow viewport if min-width " \
    "does not shrink it — the exact shape a real desk/thread summary takes in production"

_HARNESS = (
    "<!doctype html><html><head><style>" + _CSS + "</style></head><body>"
    '<div class="queue-row" id="item-1">'
    '<span class="queue-row-glyph mono" title="question">?</span>'
    f'<span class="queue-row-title">{_LONG_TITLE}</span>'
    '<span class="queue-row-age mono">2s ago</span>'
    "</div></body></html>"
)


async def _probe() -> bool:
    from playwright.async_api import async_playwright
    async with async_playwright() as p:
        b = await p.chromium.launch(headless=True)
        await b.close()
    return True


@pytest.fixture(scope="session")
def chromium_available() -> bool:
    try:
        return asyncio.run(_probe())
    except Exception:
        return False


async def test_queue_row_title_never_overflows_a_narrow_viewport(
    chromium_available: bool,
) -> None:
    if not chromium_available:
        pytest.skip("Chromium can't launch on this host")
    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        # the bug hid at wide viewports (enough slack to absorb the un-shrunk title) and only
        # showed at narrow ones — this IS the regression, not an arbitrary choice.
        page = await browser.new_page(viewport={"width": 380, "height": 400})
        await page.set_content(_HARNESS)
        page_overflow = await page.evaluate(
            "document.documentElement.scrollWidth - document.documentElement.clientWidth")
        row_overflow = await page.locator(".queue-row").evaluate(
            "e => e.scrollWidth - e.clientWidth")
        await browser.close()
        assert page_overflow <= 1, f"front door overflows the viewport by {page_overflow}px"
        assert row_overflow <= 1, f"the row itself overflows its own box by {row_overflow}px"

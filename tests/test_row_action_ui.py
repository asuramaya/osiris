"""row_action's CLIENT half (thread e5d1eb6d, Thoth msg 1960 build 2) — the server side
(compositions._table / the `function` op, commit 89df464) has attached `_action:{action,args}`
to a row since #44, but osiris.js's table() had no case for it: it rendered as its own
JSON.stringify'd blob in a column, and live-desk's resolve buttons were broken in production
because of it. table() must treat `_action` as a CONTROL, never a column, and the click
delegate must round-trip a row's own args through /act faithfully — including surviving being
embedded in an HTML attribute, which is JSON's `"` characters' natural home for corruption.

Playwright + the real osiris.js/osiris.css (same harness as test_ui_render.py); /act is
intercepted via page.route — no backend, no DB, pure client behavior."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

_STATIC = Path(__file__).resolve().parent.parent / "src" / "ui" / "static"
_CSS = (_STATIC / "osiris.css").read_text()
_JS = (_STATIC / "osiris.js").read_text()

_HARNESS = (
    # a <base> so fetch('/act') has an origin to resolve against — about:blank (what
    # set_content() lands on with no prior navigation) has none, and a bare relative fetch
    # throws "Failed to parse URL" before page.route ever gets a chance to intercept it.
    "<!doctype html><html><head><base href=\"http://osiris.test/\"><style>" + _CSS +
    "</style></head><body><div id='panel'></div></body></html>"
)

ROWS_WITH_ACTION = [
    {"id": "abc12345", "summary": "first row", "kind": "obligation",
     "_action": {"action": "resolve_thread", "args": {"ref": "abc12345"}}},
    {"id": "def67890", "summary": "second row", "kind": "obligation",
     "_action": {"action": "resolve_thread", "args": {"ref": "def67890"}}},
]

_RENDER = """
async (result) => {
  const panel = document.getElementById('panel');
  panel.innerHTML = '';
  await window.Osiris.renderResult(result, {board: null, panel}, 'panel', ()=>{}, ()=>{});
}
"""


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


async def test_action_never_becomes_a_column(chromium_available: bool) -> None:
    if not chromium_available:
        pytest.skip("Chromium can't launch on this host")
    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.set_content(_HARNESS)
        await page.add_script_tag(content=_JS + "\nwindow.Osiris = Osiris;")
        result = {"kind": "rows", "spec": {"op": "table"}, "items": ROWS_WITH_ACTION}
        await page.evaluate(_RENDER, result)

        # no header/cell ever shows the control as data — it's a button or it's nothing
        assert await page.locator('th:has-text("_action")').count() == 0
        assert await page.locator('td:has-text("resolve_thread")').count() == 0
        assert await page.locator("button[data-action]").count() == 2
        await browser.close()


async def test_click_posts_to_act_round_trips_args_and_removes_the_row(
    chromium_available: bool,
) -> None:
    if not chromium_available:
        pytest.skip("Chromium can't launch on this host")
    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.set_content(_HARNESS)
        await page.add_script_tag(content=_JS + "\nwindow.Osiris = Osiris;")
        result = {"kind": "rows", "spec": {"op": "table"}, "items": ROWS_WITH_ACTION}
        await page.evaluate(_RENDER, result)

        seen: dict[str, Any] = {}

        async def _handle(route: Any) -> None:
            seen["body"] = json.loads(route.request.post_data or "{}")
            await route.fulfill(status=200, content_type="application/json",
                                body=json.dumps({"ok": True, "id": "x"}))

        await page.route("**/act", _handle)
        rows_before = await page.locator("tbody tr").count()
        await page.locator("button[data-action]").first.click()
        await page.wait_for_function(
            f"document.querySelectorAll('tbody tr').length < {rows_before}")

        # the attribute round-trip is the point: JSON.stringify({"ref":"abc12345"}) is built
        # almost entirely of '"' characters — a naive data-args="${...}" would have truncated
        # or corrupted the attribute at the first one, well before it ever reached fetch().
        assert seen["body"] == {"action": "resolve_thread", "args": {"ref": "abc12345"}}
        assert await page.locator("tbody tr").count() == rows_before - 1
        toast_text = await page.locator("#o-toast").inner_text()
        assert "resolve" in toast_text and "done" in toast_text
        await browser.close()


async def test_click_error_response_restores_the_button_and_keeps_the_row(
    chromium_available: bool,
) -> None:
    if not chromium_available:
        pytest.skip("Chromium can't launch on this host")
    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.set_content(_HARNESS)
        await page.add_script_tag(content=_JS + "\nwindow.Osiris = Osiris;")
        result = {"kind": "rows", "spec": {"op": "table"}, "items": ROWS_WITH_ACTION}
        await page.evaluate(_RENDER, result)

        async def _handle(route: Any) -> None:
            await route.fulfill(status=200, content_type="application/json",
                                body=json.dumps({"error": "no match"}))

        await page.route("**/act", _handle)
        btn = page.locator("button[data-action]").first
        await btn.click()
        await page.wait_for_function(
            "document.querySelector('button[data-action]') && "
            "!document.querySelector('button[data-action]').disabled")

        assert await page.locator("tbody tr").count() == 2  # a refused write removes nothing
        toast_text = await page.locator("#o-toast").inner_text()
        assert "no match" in toast_text
        await browser.close()

"""row_action's CLIENT half (thread e5d1eb6d, Thoth msg 1960 build 2) — the server side
(compositions._table / the `function` op, commit 89df464) has attached `_action:{action,args}`
to a row since #44, but osiris.js's table() had no case for it: it rendered as its own
JSON.stringify'd blob in a column, and live-desk's resolve buttons were broken in production
because of it. table() must treat `_action` as a CONTROL, never a column, and the click
delegate must round-trip a row's own args through /act faithfully — including surviving being
embedded in an HTML attribute, which is JSON's `"` characters' natural home for corruption.

Also covers the "run:" navigation form (task #90, Thoth msg 1976/2005): an action named
"run:<function>" must NOT reach /act at all — the click delegate dispatches an `osiris:run`
DOM event instead, which is this module's entire client-side contract (the page shell,
index.html, owns actually running the Function and switching the board; untested here, same
boundary the module itself respects).

And `_actions` (plural, task #91, Thoth msg 1976/2029): a row that affords MORE than one
verb (chrome's /desk: done/not mine/later on one debt) renders N buttons, each round-
tripping through /act exactly like the singular form — no new mechanism, N of the same one.
Verified here, with a hand-made spec, BEFORE any real composition is armed with it.

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


# --- "run:" dispatch (task #90, Thoth msg 1976/2005) — a row_action whose action starts with
# "run:<function>" is NAVIGATION, not a write: the click delegate must NOT POST it to /act, and
# must instead dispatch a document-level `osiris:run` CustomEvent carrying {name, args} — the
# page shell (index.html, untested here — see its own osiris:run listener) is what actually
# runs the Function and switches the board. This module has no access to that page state by
# design, so the event is the entire client-side contract this test can verify.

ROWS_WITH_RUN_ACTION = [
    {"box": "neo", "msgs": 3, "unsettled": 1,
     "_action": {"action": "run:mail_threads", "args": {"box": "neo"}}},
]


async def test_run_action_renders_a_generic_label_not_the_raw_action_string(
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
        result = {"kind": "rows", "spec": {"op": "table"}, "items": ROWS_WITH_RUN_ACTION}
        await page.evaluate(_RENDER, result)

        btn = page.locator("button[data-action]").first
        assert await btn.get_attribute("data-action") == "run:mail_threads"
        # generic prefix-strip, not the raw "run:mail_threads" string, and not a hardcoded
        # per-function label either — the module never learns what "mail_threads" means
        assert (await btn.inner_text()).strip() == "mail threads"
        await browser.close()


async def test_run_action_dispatches_an_event_instead_of_posting_to_act(
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
        result = {"kind": "rows", "spec": {"op": "table"}, "items": ROWS_WITH_RUN_ACTION}
        await page.evaluate(_RENDER, result)

        posted = {"hit": False}

        async def _handle(route: Any) -> None:
            posted["hit"] = True
            await route.fulfill(status=200, content_type="application/json", body="{}")

        await page.route("**/act", _handle)
        await page.evaluate(
            "window.__seen = null; "
            "document.addEventListener('osiris:run', (e) => { window.__seen = e.detail; });")

        rows_before = await page.locator("tbody tr").count()
        await page.locator("button[data-action]").first.click()
        await page.wait_for_function("window.__seen !== null")

        assert posted["hit"] is False  # never reached /act — it's navigation, not a write
        assert await page.locator("tbody tr").count() == rows_before  # the row is untouched
        seen = await page.evaluate("window.__seen")
        assert seen == {"name": "mail_threads", "args": {"box": "neo"}}
        await browser.close()


# --- `_actions` (plural, task #91, Thoth msg 1976/2029) — a row that affords MORE than one
# verb. Same click delegate, same POST /act per button, no new mechanism — verified with a
# hand-made spec BEFORE any real composition (chrome's own /desk motivating case) is armed.

ROWS_WITH_ACTIONS = [
    {"id": "t1", "summary": "a debt", "kind": "obligation",
     "_actions": [
         {"label": "done", "action": "resolve_thread",
          "args": {"ref": "t1", "because": "operator: done"}},
         {"label": "not mine", "action": "assign_thread",
          "args": {"ref": "t1", "owner": "neo", "because": "operator: not mine — neo owns this"}},
         {"label": "later", "action": "defer_thread",
          "args": {"ref": "t1", "days": 30, "because": "operator: not now"}},
     ]},
]


async def test_actions_plural_never_becomes_a_column(chromium_available: bool) -> None:
    if not chromium_available:
        pytest.skip("Chromium can't launch on this host")
    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.set_content(_HARNESS)
        await page.add_script_tag(content=_JS + "\nwindow.Osiris = Osiris;")
        result = {"kind": "rows", "spec": {"op": "table"}, "items": ROWS_WITH_ACTIONS}
        await page.evaluate(_RENDER, result)

        assert await page.locator('th:has-text("_actions")').count() == 0
        assert await page.locator('td:has-text("resolve_thread")').count() == 0
        # three buttons, one row — not one button, not three columns
        assert await page.locator("button[data-action]").count() == 3
        labels = await page.locator("button[data-action]").all_inner_texts()
        assert labels == ["done", "not mine", "later"]
        await browser.close()


async def test_actions_plural_each_button_round_trips_its_own_args(
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
        result = {"kind": "rows", "spec": {"op": "table"}, "items": ROWS_WITH_ACTIONS}
        await page.evaluate(_RENDER, result)

        seen: list[dict[str, Any]] = []

        async def _handle(route: Any) -> None:
            seen.append(json.loads(route.request.post_data or "{}"))
            await route.fulfill(status=200, content_type="application/json",
                                body=json.dumps({"ok": True}))

        await page.route("**/act", _handle)
        # click "not mine" (the middle button) — its own args, not the first button's
        await page.locator('button[data-action="assign_thread"]').click()
        await page.wait_for_function("document.querySelectorAll('tbody tr').length === 0")

        assert seen == [{"action": "assign_thread",
                         "args": {"ref": "t1", "owner": "neo",
                                  "because": "operator: not mine — neo owns this"}}]
        toast_text = await page.locator("#o-toast").inner_text()
        assert "not mine" in toast_text and "done" in toast_text
        await browser.close()


async def test_actions_plural_one_click_removes_the_whole_row_not_just_its_button(
    chromium_available: bool,
) -> None:
    """A resolved/assigned/deferred debt is gone — its OTHER two buttons must not survive as
    dead controls pointing at a thread that no longer holds the state they described."""
    if not chromium_available:
        pytest.skip("Chromium can't launch on this host")
    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.set_content(_HARNESS)
        await page.add_script_tag(content=_JS + "\nwindow.Osiris = Osiris;")
        result = {"kind": "rows", "spec": {"op": "table"}, "items": ROWS_WITH_ACTIONS}
        await page.evaluate(_RENDER, result)

        async def _handle(route: Any) -> None:
            await route.fulfill(status=200, content_type="application/json",
                                body=json.dumps({"ok": True}))

        await page.route("**/act", _handle)
        await page.locator('button[data-action="resolve_thread"]').click()
        await page.wait_for_function("document.querySelectorAll('tbody tr').length === 0")

        assert await page.locator("button[data-action]").count() == 0
        await browser.close()

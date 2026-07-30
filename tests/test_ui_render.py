"""Renderer stress — the generic renderer must survive ADVERSARIAL content without the CSS
failing. The bug this guards: the briefing "BY" column wrapped a commit hash one character
per line because a long-sentence cell in the same row stole all the table width.

Loads the REAL osiris.css + osiris.js into a headless page (no backend) and renders every
panel shape — objects table, timeline, aggregate rows, a Function's grouped data, an array
table, a value list — with hostile content (a 227-char no-space token next to a 60-word
sentence). Asserts, per render: (1) no horizontal overflow of the result panel, and (2) no
column crushed to a sliver (a real column is >= 80px, never the 10px that forces per-char
wrap). Self-skips if Chromium can't launch.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

_STATIC = Path(__file__).resolve().parent.parent / "src" / "ui" / "static"
_CSS = (_STATIC / "osiris.css").read_text()
_JS = (_STATIC / "osiris.js").read_text()

# the two hostile shapes: a long no-space token (a hash/url) and a long sentence (a summary)
TOKEN = "commit:" + "a" * 220        # 227 chars, no break opportunity
SENT = "NEXT: " + "renderer " * 60   # a long wrapping sentence

# (name, result, view) — every panel render path the generic renderer can take
def _objs(n: int) -> list[dict[str, Any]]:
    return [{"id": f"id{i}", "type": "Commit", "label": TOKEN,
             "props": {"rationale": SENT, "scope": "composer", "authored_date": "2026-06-29"}}
            for i in range(n)]


CASES: list[tuple[str, dict[str, Any], str]] = [
    ("objects-table", {"kind": "objects", "spec": {"op": "select"}, "items": _objs(3)}, "table"),
    ("objects-timeline",
     {"kind": "objects", "spec": {"op": "take", "from": {"op": "select"}}, "items": _objs(3)},
     "timeline"),
    ("aggregate-rows",
     {"kind": "rows", "spec": {"op": "aggregate"},
      "items": [{"group": {"scope": SENT}, "metric": 42}, {"group": {"scope": "x"}, "metric": 1}]},
     "panel"),
    ("function-data-grouped",
     {"kind": "data", "spec": {"op": "function"},
      "items": {"RESOLVED — self-healed by later commits":
                [{"thread": SENT, "by": TOKEN, "because": "renderer, generic"}]}},
     "panel"),
    ("function-data-array",
     {"kind": "data", "spec": {"op": "function"}, "items": [{"hash": TOKEN, "note": SENT, "n": 1}]},
     "panel"),
    ("values", {"kind": "values", "spec": {"op": "collect"}, "items": [TOKEN, SENT]}, "panel"),
]

_HARNESS = (
    "<!doctype html><html><head><style>" + _CSS + "\n"
    "#panel{width:760px;padding:18px 22px;box-sizing:border-box;overflow:auto;"
    "background:#0b0e13}</style></head><body><div id='panel'></div></body></html>"
)

_MEASURE = """
async ([result, view]) => {
  const panel = document.getElementById('panel');
  panel.innerHTML = '';
  await window.Osiris.renderResult(result, {board: null, panel}, view, ()=>{}, ()=>{});
  const cells = [...panel.querySelectorAll('td, li, .o-v, .tl-main')];
  const token = cells.find(c => c.textContent.startsWith('commit:aaa'));
  return {
    overflow: panel.scrollWidth - panel.clientWidth,
    tokenWidth: token ? token.offsetWidth : null,
    tokenHeight: token ? token.offsetHeight : null,
    rendered: panel.innerHTML.length,
  };
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


async def test_renderer_survives_adversarial_content(chromium_available: bool) -> None:
    if not chromium_available:
        pytest.skip("Chromium can't launch on this host")
    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1200, "height": 900})
        await page.set_content(_HARNESS)
        # the library binds a lexical `const Osiris`; expose it for page.evaluate
        await page.add_script_tag(content=_JS + "\nwindow.Osiris = Osiris;")
        failures = []
        for name, result, view in CASES:
            m = await page.evaluate(_MEASURE, [result, view])
            assert m["rendered"] > 0, f"{name}: rendered nothing"
            # (1) the result must never overflow its panel horizontally
            if m["overflow"] > 2:
                failures.append(f"{name}: horizontal overflow {m['overflow']}px")
            # (2) a long-token cell must get a real column, never crushed to a per-char sliver
            if m["tokenWidth"] is not None and m["tokenWidth"] < 80:
                failures.append(f"{name}: column crushed to {m['tokenWidth']}px (per-char wrap)")
            if m["tokenHeight"] is not None and m["tokenHeight"] > 500:
                failures.append(f"{name}: token cell {m['tokenHeight']}px tall (wrapped per char)")
        await browser.close()
        assert not failures, "renderer CSS failures:\n  " + "\n  ".join(failures)


# table()'s own cell clamp (Seshat's sweep, Thoth DM 1992 fix 2): `.r-table td .clamp` has
# existed in osiris.css since 2026-07-11 for objectsTable's cells, but table() never applied
# it — a MEDIUM string (short of the >160 "wall of text" bar) sailed through untouched, and a
# many-short-columns table starves a non-tight column thin enough that overflow-wrap:anywhere +
# word-break:break-word breaks it mid-word with nowhere else to go. Real specimen: fleet-live's
# DOORS/ANCESTORS columns, "1 door (session 82d04858 2s ago)" (33 chars), towering into 10+
# near-single-character lines next to five other short, tight columns.
MEDIUM_PROSE = "1 door (session 82d04858 2s ago)"     # the exact live shape, 33 chars
STARVED_ROWS = [
    # a column earns its place by VARYING (table()'s own law) — an identical value on every
    # row collapses to a header chip, never a cell, so each row's doors/ancestors must differ.
    {"id": f"id{i}", "type": "T", "kind": "obligation", "owner": "operator",
     "doors": f"{MEDIUM_PROSE[:-1]}{i})", "ancestors": f"{MEDIUM_PROSE[:-1]}{i})"}
    for i in range(3)
]


async def test_table_clamps_medium_prose_instead_of_breaking_mid_word(
    chromium_available: bool,
) -> None:
    if not chromium_available:
        pytest.skip("Chromium can't launch on this host")
    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        # narrow enough that six columns genuinely compete for width — the starvation the bug
        # depends on; a wide viewport had enough slack to hide it, same class as fix 1's bug.
        page = await browser.new_page(viewport={"width": 640, "height": 900})
        await page.set_content(_HARNESS)
        await page.add_script_tag(content=_JS + "\nwindow.Osiris = Osiris;")
        result = {"kind": "rows", "spec": {"op": "table"}, "items": STARVED_ROWS}
        info = await page.evaluate("""
            async (result) => {
              const panel = document.getElementById('panel');
              panel.innerHTML = '';
              await window.Osiris.renderResult(
                result, {board: null, panel}, 'panel', ()=>{}, ()=>{});
              const cell = [...panel.querySelectorAll('td')].find(
                td => td.textContent.startsWith('1 door'));
              return {
                hasClamp: !!cell && !!cell.querySelector('.clamp'),
                cellHeight: cell ? cell.offsetHeight : null,
              };
            }
        """, result)
        await browser.close()
        assert info["hasClamp"], "medium-prose cell never got the .clamp treatment"
        # a proper 2-line clamp is ~30-45px including cell padding at this font/line-height;
        # the pre-fix character-tower shape ran 10+ lines, well past 100px.
        assert info["cellHeight"] is not None and info["cellHeight"] < 60, (
            f"cell is {info['cellHeight']}px tall — looks like it's still wrapping mid-word")


# objects-mode's missing cap (Seshat's sweep, Thoth DM 1992 fix 3): the "open threads" lens
# resolves to "objects" mode (table or timeline sub-view, not table()/renderData's own SECTION_
# CAP=12 path) and used to dump every item verbatim — 305 live Threads, full-paragraph
# summaries, zero cap — while "data" mode next to it capped at 12 and SAID what it withheld.
# Both sub-views (objectsTable, timelineList) now reuse the same SECTION_CAP/_more idiom.
MANY_OBJECTS = [
    {"id": f"t{i}", "type": "Thread", "label": f"thread {i}",
     "props": {"summary": f"open thread number {i}",
               "created_at": f"2026-07-2{i % 10}T00:00:00"}}
    for i in range(40)
]


async def test_objects_table_mode_caps_and_says_what_it_withheld(chromium_available: bool) -> None:
    if not chromium_available:
        pytest.skip("Chromium can't launch on this host")
    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1200, "height": 900})
        await page.set_content(_HARNESS)
        await page.add_script_tag(content=_JS + "\nwindow.Osiris = Osiris;")
        rows = await page.evaluate("""
            async (result) => {
              const panel = document.getElementById('panel');
              panel.innerHTML = '';
              await window.Osiris.renderResult(
                result, {board: null, panel}, 'table', ()=>{}, ()=>{});
              return {
                bodyRows: panel.querySelectorAll('tbody tr').length,
                more: panel.querySelector('.r-more')?.textContent || null,
              };
            }
        """, {"kind": "objects", "spec": {"op": "select"}, "items": MANY_OBJECTS})
        await browser.close()
        assert rows["bodyRows"] == 12, f"expected the SECTION_CAP=12 shape, got {rows['bodyRows']}"
        assert rows["more"] and "28" in rows["more"], (
            f"no honest '+N more' note for the other 28: {rows['more']!r}")


async def test_objects_timeline_mode_caps_and_says_what_it_withheld(
    chromium_available: bool,
) -> None:
    if not chromium_available:
        pytest.skip("Chromium can't launch on this host")
    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1200, "height": 900})
        await page.set_content(_HARNESS)
        await page.add_script_tag(content=_JS + "\nwindow.Osiris = Osiris;")
        result = {"kind": "objects", "spec": {"op": "take", "from": {"op": "select"}},
                  "items": MANY_OBJECTS}
        info = await page.evaluate("""
            async (result) => {
              const panel = document.getElementById('panel');
              panel.innerHTML = '';
              await window.Osiris.renderResult(
                result, {board: null, panel}, 'timeline', ()=>{}, ()=>{});
              return {
                items: panel.querySelectorAll('.tl-item').length,
                more: panel.querySelector('.r-more')?.textContent || null,
              };
            }
        """, result)
        await browser.close()
        assert info["items"] == 12, f"expected the SECTION_CAP=12 shape, got {info['items']}"
        assert info["more"] and "28" in info["more"], (
            f"no honest '+N more' note for the other 28: {info['more']!r}")


# NESTED CELL VALUES (task #109's tail, Thoth DM 2145; compositions.py:2216's own documented
# gap): neither render_composition (chrome.py, left untouched — Thoth's own chrome.py route
# cuts are imminent, no reason to add fresh logic to a shrinking module) nor osiris.js's
# table() used to recurse into a CELL holding a list or dict — a raw JSON.stringify blob, or
# "[object Object]" repeated for a list of objects, worse than not showing it at all. table()'s
# _txt/_flatVal now flatten one into the same compact "key=value, key=value" prose
# _fleet_doors_summary/_fleet_ancestors_summary already hand-roll per-Function
# (compositions.py) — capped at 2 levels deep so a genuinely deep structure degrades to "{…}"
# rather than an unbounded blob. Each field below VARIES across rows (table()'s own law: a
# constant value collapses to a header chip, never a cell) so the fix is exercised as a real
# column, not a chip.
NESTED_ROWS = [
    {"id": "a", "bucket": "duplicate_suspect",
     "siblings": [{"canonical": "repo:coinbase-onchain-agent", "links": 3},
                  {"canonical": "repo:coinbase-onchain-web", "links": 5}],
     "stats": {"in": 12, "out": 3}, "tags": ["a", "b", "c"],
     "meta": {"outer": {"inner": {"deep": "v1"}}}},
    {"id": "b", "bucket": "hub", "siblings": [{"canonical": "repo:other", "links": 1}],
     "stats": {"in": 1, "out": 0}, "tags": ["x", "y"],
     "meta": {"outer": {"inner": {"deep": "v2"}}}},
    {"id": "c", "bucket": "normal", "siblings": [], "stats": {"in": 0, "out": 0}, "tags": [],
     "meta": {}},
]


async def test_table_flattens_nested_cell_values_instead_of_a_blob(
    chromium_available: bool,
) -> None:
    if not chromium_available:
        pytest.skip("Chromium can't launch on this host")
    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1200, "height": 900})
        await page.set_content(_HARNESS)
        await page.add_script_tag(content=_JS + "\nwindow.Osiris = Osiris;")
        result = {"kind": "rows", "spec": {"op": "table"}, "items": NESTED_ROWS}
        html = await page.evaluate("""
            async (result) => {
              const panel = document.getElementById('panel');
              panel.innerHTML = '';
              await window.Osiris.renderResult(
                result, {board: null, panel}, 'panel', ()=>{}, ()=>{});
              return panel.innerHTML;
            }
        """, result)
        await browser.close()
        assert "[object Object]" not in html, "a list of dicts joined as [object Object] mush"
        assert '{"in":12' not in html, "a nested dict fell straight to a JSON.stringify blob"
        # the nested list-of-dicts flattened into real, readable content — not dropped
        assert "coinbase-onchain-agent" in html
        assert "coinbase-onchain-web" in html
        # the nested plain dict flattened into the same "key=value" prose
        assert "in=12" in html and "out=3" in html
        # an already-working flat array of primitives is UNCHANGED — comma-joined
        assert "a, b, c" in html
        # a genuinely deep structure (meta.outer.inner.deep) is CAPPED, not unrolled forever
        assert "v1" not in html and "v2" not in html, (
            "depth cap didn't fire — a 3-level-deep leaf value leaked into the cell")
        assert "…" in html, "no collapse marker for the depth-capped structure"

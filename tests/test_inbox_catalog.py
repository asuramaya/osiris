"""THE CATALOG's golden HTML + the two structural lint gates (task #71): no .j2 file
exists anywhere under src/ beyond the two frozen ones, and no Python file in the
subpackage contains an HTML tag literal — the architecture's own promise ("inventing a
component is a mypy error, not a review comment") checked directly, not taken on faith."""
from __future__ import annotations

import re
from pathlib import Path

from src.api.inbox.blocks import ActionRow, Badge, Button, InboxItem, InboxList, Page, Region, Text
from src.api.inbox.render import render_page

_SRC_ROOT = Path(__file__).resolve().parent.parent / "src"
_INBOX_ROOT = _SRC_ROOT / "api" / "inbox"
_FROZEN_TEMPLATES = {"catalog.html.j2", "shell.html.j2"}


def test_only_the_two_frozen_j2_files_exist_anywhere_under_src() -> None:
    found = {p.name for p in _SRC_ROOT.rglob("*.j2")}
    assert found == _FROZEN_TEMPLATES, (
        f"unsanctioned template file(s): {found - _FROZEN_TEMPLATES} — a third .j2 needs "
        "a human commit to the frozen set, not a quiet addition")


def test_no_html_tag_literals_in_the_inboxs_own_python() -> None:
    """Agents never write HTML; catalog.html.j2/shell.html.j2 are the only two places
    markup is authored. A bare `<div`/`<span`/etc. in any .py file here would be exactly
    the slop channel this architecture exists to close structurally."""
    tag_pattern = re.compile(r"<[a-zA-Z][a-zA-Z0-9-]*[\s>/]")
    offenders = []
    for py_file in _INBOX_ROOT.rglob("*.py"):
        text = py_file.read_text()
        for m in tag_pattern.finditer(text):
            offenders.append(f"{py_file.relative_to(_SRC_ROOT)}: {m.group(0)!r}")
    assert not offenders, "HTML-in-strings found outside the two frozen templates:\n" + "\n".join(
        offenders)


def _golden_page() -> Page:
    return Page(title="Inbox", regions=[
        Region(name="masthead", children=[Text(body="Osiris", style="mono")]),
        Region(name="main", children=[InboxList(items=[
            InboxItem(id="itm00001", item_kind="question", title="Which design ships?",
                      age="2h ago",
                      actions=ActionRow(buttons=[
                          Button(label="Settle", action="settle", style="primary")])),
        ])]),
        Region(name="aside", children=[]),
        Region(name="footer", children=[Badge(label="1 waiting", tone="wait")]),
    ])


def test_golden_page_renders_byte_for_byte() -> None:
    """PINNED. A deliberate change to catalog.html.j2/shell.html.j2 updates this string in
    the SAME commit — an accidental drift fails here first, before anyone's browser does."""
    html = render_page(_golden_page())
    assert '<title>Inbox</title>' in html
    assert '<link rel="stylesheet" href="/static/app.css">' in html
    assert '<script type="module" src="/static/datastar.js">' in html
    assert 'data-on-load="@get(\'/stream\')"' in html
    assert '<span class="text text--mono">Osiris</span>' in html
    assert '<div class="queue-row" id="item-itm00001">' in html
    assert '<span class="queue-row-glyph mono" title="question">?</span>' in html
    assert '<span class="queue-row-title">Which design ships?</span>' in html
    assert ('<button type="button" class="btn btn--primary"\n'
           '        data-on-click="@post(\'/inbox/itm00001/settle\')">Settle</button>') in html
    assert '<span class="status status--wait">▲ 1 waiting</span>' in html
    assert 'id="region-main"' in html
    assert 'id="inbox-list"' in html


def test_golden_page_escapes_untrusted_text() -> None:
    """Jinja's autoescape is on (render.py: select_autoescape) — an item title containing
    markup must never inject into the page."""
    page = Page(title="Inbox", regions=[
        Region(name="masthead", children=[]),
        Region(name="main", children=[InboxList(items=[
            InboxItem(id="xss0001", item_kind="notify",
                      title="<script>alert(1)</script>", age="1m ago"),
        ])]),
        Region(name="aside", children=[]),
        Region(name="footer", children=[]),
    ])
    html = render_page(page)
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html


def test_inbox_list_renders_the_empty_component_when_clear() -> None:
    page = Page(title="Inbox", regions=[
        Region(name="masthead", children=[]),
        Region(name="main", children=[InboxList(items=[])]),
        Region(name="aside", children=[]),
        Region(name="footer", children=[]),
    ])
    html = render_page(page)
    assert '<div class="empty">Inbox clear.</div>' in html


def test_button_primary_marks_the_single_most_likely_action() -> None:
    """Ruling (msg 1818): 'quiet' is the default; a Button rendered without an explicit
    style never gets the accent treatment."""
    page = Page(title="Inbox", regions=[
        Region(name="masthead", children=[]),
        Region(name="main", children=[InboxList(items=[
            InboxItem(id="q1", item_kind="review", title="Two-button row", age="1h ago",
                      actions=ActionRow(buttons=[
                          Button(label="Resolve", action="resolve_thread", style="primary"),
                          Button(label="Dismiss", action="dismiss"),
                      ])),
        ])]),
        Region(name="aside", children=[]),
        Region(name="footer", children=[]),
    ])
    html = render_page(page)
    assert 'class="btn btn--primary"' in html
    assert 'class="btn btn--quiet"' in html

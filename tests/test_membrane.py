"""/membrane — the statusline's click-through. The renderer is pure; these are pure."""
from __future__ import annotations

from src.api.membrane import render_ambient_strip, render_membrane
from src.orchestrator.surface import Segment, Segments


def _segments(**overrides: Segment) -> Segments:
    base = {
        "live": Segment("live", show=True, value="3 live"),
        "owed": Segment("owed", show=True, value="owed 0"),
        "owed_here": Segment("owed_here", show=True, value="owe 0"),
        "briefs_total": Segment("briefs_total", show=True, value="briefs 2"),
        "briefs_mine": Segment("briefs_mine", show=False, value="briefs 0"),
        "wakes": Segment("wakes", show=True, value="wakes 4/h"),
        "mail": Segment("mail", show=True, value="mail 0"),
        "sensing": Segment("sensing", show=False, value="sensing"),
        "spend": Segment("spend", show=False, value=""),
    }
    base.update(overrides)
    return Segments(**base)


def _digest() -> dict:
    return {
        "since": "2026-07-07T09:00:00+00:00",
        "summary": {"agents": 2, "unresolved": 1, "swapped": 1, "activity": 1,
                    "laundering": 0, "conversations": 1, "operator_unread": 1},
        "roster": [
            {"agent": "agent:aaa", "project": "osiris", "model": "claude-fable-5",
             "resolved": True, "swapped": None},
            {"agent": "agent:bbb", "project": "sibling-one", "model": "claude-opus-4-8",
             "resolved": False, "swapped": "claude-fable-5 ↔ claude-opus-4-8"},
        ],
        "activity": [{"type": "Decision", "agent": "agent:aaa",
                      "summary": "shipped <thing>", "at": "2026-07-07T12:00:00+00:00"}],
        "danger": [], "laundering": [],
        "conversations": [
            {"thread": 7, "between": ["sibling-two", "sibling-one"], "msgs": 3, "unsettled": 1,
             "last_at": "2026-07-07T12:30:00+00:00",
             "last": {"from": "agent:bbb", "body": "digit-exact & <b>bold</b> claims"}},
        ],
        "operator_inbox": {"unread": 1, "latest": [
            {"from": "agent:aaa", "from_project": "osiris",
             "body": "<script>alert(1)</script> brief", "when": "2026-07-07T13:00:00+00:00"},
        ]},
    }


def test_the_windowed_summary_strip_is_pinned_before_the_ambient_strip_lands() -> None:
    """GOLDEN, ruling e9ef7373 stage 3: this page's existing windowed strip (desk-in-window,
    threads-in-window, fleet-in-window, wakes-in-window, activity, laundering, spend) answers
    a DIFFERENT question than the ambient glance and must NOT be replaced by it (Thoth, msg
    1094) — only a NEW strip is added beside it. Pinned byte-for-byte so that stays true."""
    page = render_membrane(_digest(), wakes=[
        {"to_project": "sibling-one", "from_agent": "agent:x", "message_id": 37,
         "woke_at": "2026-07-07 16:04:45+00"},
    ])
    i = page.index('<div class="strip">')
    j = page.index("</div>", i) + len("</div>")
    assert page[i:j] == (
        '<div class="strip"><span><a href="/desk"><b class="red">desk 1</b></a></span>'
        '<span><a href="/mail"><b>threads 1</b></a></span>'
        '<span><a href="/fleet"><b>fleet 2</b></a> '
        '<span class="dim">(1 unresolved · 1 swapped)</span></span>'
        '<span><a href="/fleet"><b>wakes 1</b></a></span>'
        '<span><a href="#activity"><b>activity 1</b></a></span>'
        '<span class="dim">laundering 0</span></div>'
    )


def test_ambient_strip_is_absent_when_no_segments_are_given() -> None:
    """Backward compatible: a caller that doesn't pass `ambient` gets exactly the page it
    always got — no 'right now' label, no second strip."""
    page = render_membrane(_digest(), wakes=[])
    assert "right now" not in page
    assert 'class="strip ambient"' not in page


def test_ambient_strip_adds_a_second_row_without_touching_the_first() -> None:
    seg = _segments(
        live=Segment("live", show=True, value="5 live"),
        owed=Segment("owed", show=True, severity="red", value="owed 3"),
        briefs_total=Segment("briefs_total", show=True, value="briefs 1"),
        wakes=Segment("wakes", show=True, value="wakes 2/h"),
    )
    page = render_membrane(_digest(), wakes=[], ambient=seg)
    assert '<div class="strip-label">right now</div>' in page
    assert '<div class="strip ambient">' in page  # the new row...
    assert '<div class="strip">' in page           # ...beside the untouched windowed one
    assert '<a href="/fleet"><b>5 live</b></a>' in page
    assert '<a href="/desk"><b class="red">owed 3</b></a>' in page
    assert '<a href="/desk"><b>briefs 1</b></a>' in page
    assert '<a href="#wakes"><b>wakes 2/h</b></a>' in page
    # the windowed strip's own numbers are still exactly what they were pre-ambient
    assert '<a href="/fleet"><b>fleet 2</b></a>' in page


def test_ambient_strip_hides_dark_until_it_matters_segments() -> None:
    dark = _segments()  # sensing/spend both show=False in the default fixture
    lit = render_ambient_strip(dark)
    assert "sensing" not in lit and "$" not in lit

    seg = _segments(
        sensing=Segment("sensing", show=True, severity="alarm",
                        value="not sensing: miner"),
        spend=Segment("spend", show=True, severity="amber", value="$6.50/$10",
                     note="1 unpriced"),
    )
    lit2 = render_ambient_strip(seg)
    assert '<a href="/fleet"><b class="red">not sensing: miner</b></a>' in lit2
    assert '<a href="#costs"><b class="amber">$6.50/$10</b></a>' in lit2
    assert '<span class="dim">1 unpriced</span>' in lit2


def test_membrane_renders_all_anchored_sections() -> None:
    page = render_membrane(_digest(), wakes=[
        {"to_project": "sibling-one", "from_agent": "agent:x", "message_id": 37,
         "woke_at": "2026-07-07 16:04:45+00"},
    ])
    for anchor in ('id="desk"', 'id="conversations"', 'id="fleet"', 'id="wakes"',
                   'id="activity"'):
        assert anchor in page
    assert "agent:bbb" in page and "unresolved" in page
    assert "sibling-one" in page and "msg 37" in page


def test_membrane_escapes_untrusted_bodies() -> None:
    # mail bodies and summaries are agent-authored text — they must never become markup
    page = render_membrane(_digest(), wakes=[])
    assert "<script>" not in page
    assert "&lt;script&gt;" in page
    assert "&lt;b&gt;bold&lt;/b&gt;" in page
    assert "shipped &lt;thing&gt;" in page


def test_membrane_renders_empty_states() -> None:
    dg = _digest()
    dg["conversations"] = []
    dg["operator_inbox"] = {"unread": 0, "latest": []}
    page = render_membrane(dg, wakes=[])
    assert "nothing waiting" in page and "no wakes yet" in page
    assert "no threads in window" in page

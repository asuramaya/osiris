"""/membrane — the statusline's click-through. The renderer is pure; these are pure."""
from __future__ import annotations

from src.api.membrane import render_membrane


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

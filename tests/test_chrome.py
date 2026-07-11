"""/desk /mail /fleet — the chrome opened (operator, 2026-07-11). Renderers are pure;
these feed them fixtures. The data functions get one live-graph test each via `actions`."""
from __future__ import annotations

from src.actions.core import Actions
from src.api.chrome import (
    page,
    render_desk,
    render_fleet,
    render_mail_box,
    render_mail_overview,
)


def _desk() -> dict:
    return {
        "needs_decision": [
            {"id": 289, "from": "agent:d", "from_project": "coldspot",
             "body": "🚨 CRITICAL: root escalation <script>x</script>",
             "when": "2026-07-11T19:22:00+00:00"}],
        "needs_hands": [
            {"id": 189, "from": "agent:x", "from_project": "osiris",
             "body": "hyper-home is dark", "when": "2026-07-10T22:47:00+00:00",
             "thread_folded": {"count": 1, "ids": [42], "note": "earlier"}}],
        "fyi": [
            {"id": 300, "from": "agent:t", "from_project": "osiris",
             "body": "supersedes shipped", "when": "2026-07-11T19:49:00+00:00",
             "same_story": {"count": 3, "also": [
                 {"id": 284, "from": "agent:e", "project": "Like-Us"},
                 {"id": 286, "from": "agent:n", "project": "neo"}], "note": "folded"}}],
        "dimmed": [
            {"id": 291, "from": "agent:a", "project": "tony",
             "headline": "model check", "moot": "fixed in bcbdeab", "by": "agent:fixer"}],
        "your_queue": {"threads": [
            {"id": "3ea7b203", "summary": "refill the gemini key", "kind": "obligation"}],
            "note": "owner=operator"},
        "note": "peek",
    }


def test_desk_renders_bands_folds_dims_and_queue() -> None:
    html = render_desk(_desk())
    assert "needs your decision" in html and "blocked on your hands" in html
    # untrusted bodies are escaped, cards carry stable ids for the poller's re-open
    assert "<script>x</script>" not in html and "&lt;script&gt;" in html
    assert 'id="m289"' in html and 'id="dim291"' in html
    assert "×3 same story" in html and "Like-Us (284)" in html
    assert "supersedes 1 earlier" in html and "(42)" in html
    assert "moot (agent:fixer): fixed in bcbdeab" in html
    assert "refill the gemini key" in html and "your queue" in html


def test_page_shell_carries_nav_poller_and_partial_contract() -> None:
    html = page("desk", "desk", "<p>INNER</p>")
    assert '<div id="c"><p>INNER</p></div>' in html
    assert "setInterval(tick,4000)" in html and "partial" in html
    assert 'details[open]' in html  # the poller re-opens what the operator was reading
    for tab in ("/desk", "/mail", "/fleet", "/membrane"):
        assert f'href="{tab}"' in html


def test_mail_overview_links_boxes_and_flags_unsettled() -> None:
    html = render_mail_overview([
        {"box": "osiris", "msgs": 12, "unsettled": 0, "last_at": "2026-07-11 19:49:56"},
        {"box": "decepticons", "msgs": 54, "unsettled": 3, "last_at": "2026-07-11 10:00:00"},
    ])
    assert '<a href="/mail?box=osiris">' in html
    assert "3 unsettled" in html and "settled" in html


def test_mail_box_renders_threads_with_messages_inside() -> None:
    html = render_mail_box("osiris", [
        {"thread": 228, "between": ["osiris", "rotten-apple"], "unsettled": 1,
         "last_at": "2026-07-11 19:54:12",
         "msgs": [
             {"id": 301, "from_agent": "agent:ra", "from_project": "rotten-apple",
              "to_agent": None, "body": "grievances <b>bold</b>",
              "created_at": "2026-07-11 19:54:12", "settled": False},
         ]}])
    assert 'id="t228"' in html and "osiris ↔ rotten-apple" in html
    assert "&lt;b&gt;bold&lt;/b&gt;" in html and "unsettled" in html


def test_fleet_renders_live_dots_and_wake_ledger() -> None:
    html = render_fleet({
        "mounts": [
            {"agent_id": "agent:ad1a1cb0-xx", "handle": "Thoth", "seat": "Thoth XX",
             "project": "osiris", "model": "claude-fable-5", "cwd": "/home/x/osiris",
             "last_seen": None, "age_secs": 30.0, "live": True},
            {"agent_id": "agent:old", "handle": None, "seat": None, "project": "neo",
             "model": "claude-haiku-4-5", "cwd": "/home/x/neo",
             "last_seen": None, "age_secs": 90000.0, "live": False},
        ],
        "wakes": [{"to_project": "tony", "from_agent": "agent:s", "message_id": 7,
                   "mode": "mint", "woke_at": "2026-07-11 19:09:45"}],
        "wakes_hour": 21, "wake_budget": 30,
    })
    assert "1 live · 2 mounted" in html and "wakes 21 / 30/h" in html
    assert "Thoth XX" in html and '<span class="live">●</span>' in html
    assert 'href="/mail?box=osiris"' in html    # a seat's project opens its mail
    assert "mint" in html and "msg 7" in html


async def test_desk_and_fleet_data_round_trip_the_live_graph(actions: Actions) -> None:
    """One end-to-end pass over the real graph shapes: read_desk feeds render_desk and
    fleet_data feeds render_fleet without shape errors on a live (empty-ish) instance."""
    from src.api.chrome import fleet_data, mail_overview, mail_threads
    from src.orchestrator.mailbox import read_desk, send_message

    p = actions.pool
    await send_message(p, from_agent="agent:a", from_project="osiris",
                       to_project="operator", body="🚨 CRITICAL: decide something")
    await send_message(p, from_agent="agent:a", from_project="osiris",
                       to_project="neo", body="lateral note")
    html = render_desk(await read_desk(p))
    assert "decide something" in html
    boxes = await mail_overview(p)
    assert {b["box"] for b in boxes} == {"operator", "neo"}
    html2 = render_mail_box("neo", await mail_threads(p, "neo"))
    assert "lateral note" in html2
    html3 = render_fleet(await fleet_data(p, wake_budget=30))
    assert "wake ledger" in html3

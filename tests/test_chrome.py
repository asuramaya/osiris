"""/desk /mail /fleet — the chrome opened (operator, 2026-07-11). Renderers are pure;
these feed them fixtures. The data functions get one live-graph test each via `actions`."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.api.chrome import (
    page,
    render_desk,
    render_desk_project,
    render_fleet,
    render_mail_box,
    render_mail_overview,
)
from src.orchestrator.capture import open_thread
from src.parsers.base import EvidenceClass

NOW = datetime.now(UTC)


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
            {"id": "3ea7b203", "summary": "refill the gemini key", "kind": "obligation",
             "project": "sibling-three"}],
            "note": "owner=operator"},
        "owed": 1,
        "letters": 1,
        "by_project": [
            {"project": "coldspot", "debts": [], "owed": 0, "critical": True,
             "oldest_secs": 18000.0,
             "asks": [{"id": 289, "from": "agent:d", "from_project": "coldspot",
                       "body": "🚨 CRITICAL: root escalation <script>x</script>",
                       "when": "2026-07-11T19:22:00+00:00"}]},
            {"project": "sibling-three", "owed": 1, "critical": False, "asks": [],
             "oldest_secs": 190000.0,
             "debts": [{"id": "3ea7b203", "summary": "refill the gemini key",
                        "kind": "obligation", "project": "sibling-three"}]}],
        "note": "peek",
    }


def test_desk_lands_on_a_roster_never_the_whole_backlog() -> None:
    """THE FLOOD CURE (operator, 2026-07-11: "the desk is better off as a per-project thing,
    like the mail. the overwhelming kill here is that i get flooded with my entire fleet worth
    of backlog on one tab"). The landing page is COUNTS ONLY — one line per project, linked.
    No brief bodies, no debt summaries: a page that shows everything shows nothing."""
    html = render_desk(_desk())
    assert "YOU OWE <b>1</b>" in html and "letters <b>1</b>" in html
    # a roster of links, critical first — and NOT the contents
    assert '<a href="/desk?p=coldspot">' in html and '<a href="/desk?p=sibling-three">' in html
    assert html.index("/desk?p=coldspot") < html.index("/desk?p=sibling-three")
    assert 'class="crit"' in html
    assert "refill the gemini key" not in html    # debt summaries stay behind the click
    assert "root escalation" not in html          # brief bodies stay behind the click
    assert "your queue" not in html               # the old duplicate scroll is gone
    # letters fold away (they owe nothing) but still clear in bulk, folded ids with the lead
    assert 'data-act="settle" data-ids="300,284,286"' in html
    assert "clear all 1" in html


def test_walking_into_one_project_opens_its_debts_and_the_four_doors() -> None:
    """?p=<project> is the sitting: that project's debts, each with its exits, and the briefs
    that asked — together, because that is the unit of work."""
    html = render_desk_project(_desk(), "sibling-three")
    assert '<a href="/desk">← all projects</a>' in html
    assert "refill the gemini key" in html
    assert 'data-verb="resolve" data-id="3ea7b203"' in html
    assert 'data-verb="assign" data-id="3ea7b203" data-owner="sibling-three"' in html
    assert 'data-verb="defer" data-id="3ea7b203" data-days="30"' in html
    # the coldspot ask does NOT bleed into sibling-three's sitting
    assert "root escalation" not in html
    # a project with an ask renders the brief + the human's own dismiss; bodies are escaped
    cold = render_desk_project(_desk(), "coldspot")
    assert 'id="m289"' in cold and 'data-act="settle" data-ids="289"' in cold
    assert "<script>x</script>" not in cold and "&lt;script&gt;" in cold
    # a cleared project says so instead of 500ing
    assert "cleared, or never was" in render_desk_project(_desk(), "ghost-repo")


def test_only_the_desk_arms_the_write_handler() -> None:
    """The console constitution, precisely: reads are free everywhere, and exactly one page
    may write — the operator's own desk (ruling 923c380f). A page that can write must also
    SAY it can; the read-only lenses must not ship the handler at all."""
    assert 'data-act' in page("desk", "desk", "<p>x</p>", actions=True)
    assert "your clicks write (signed operator)" in page("desk", "desk", "x", actions=True)
    for lens in ("mail", "fleet"):
        html = page(lens, lens, "<p>x</p>")
        assert "read-only" in html and "/desk/settle" not in html


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
        {"box": "sibling-two", "msgs": 54, "unsettled": 3, "last_at": "2026-07-11 10:00:00"},
    ])
    assert '<a href="/mail?box=osiris">' in html
    assert "3 unsettled" in html and "settled" in html


def test_mail_box_renders_threads_with_messages_inside() -> None:
    html = render_mail_box("osiris", [
        {"thread": 228, "between": ["osiris", "sibling-eight"], "unsettled": 1,
         "last_at": "2026-07-11 19:54:12",
         "msgs": [
             {"id": 301, "from_agent": "agent:ra", "from_project": "sibling-eight",
              "to_agent": None, "body": "grievances <b>bold</b>",
              "created_at": "2026-07-11 19:54:12", "settled": False},
         ]}])
    assert 'id="t228"' in html and "osiris ↔ sibling-eight" in html
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
    assert "1 live · 1 soul · 1 unreconciled" in html and "wakes 21 / 30/h" in html
    assert "Thoth XX" in html and '<span class="live">●</span>' in html
    assert 'href="/mail?box=osiris"' in html    # a seat's project opens its mail
    assert "mint" in html and "msg 7" in html


async def test_fleet_folds_one_soul_to_one_row(actions: Actions) -> None:
    """THE FOLD (operator, 2026-07-16: 'why is there 2 thoth XL agents... the agent hash
    should be a row'): an agent with many mount rows — its durable anchor plus a tab
    viewing it — renders ONCE; the realest row testifies for the card (a view's stale
    model label never wins over the session's own row), and ×N confesses the bodies."""
    from src.api.chrome import fleet_data, render_fleet
    from src.orchestrator.mounts import save_mount

    p = actions.pool
    # the real session's row: MCP-touched (sid:), model = the truth after a swap
    await save_mount(p, job_dir="/jobs/aaaa0001", agent_id="agent:cafe99aa",
                     project="osiris", cwd="/w/osiris", model="claude-opus-4-8",
                     session_key="sid:realconn")
    # the tab's window: marked view-of, carrying the stale model stamped at ITS birth
    await save_mount(p, job_dir="/jobs/bbbb0002", agent_id="agent:cafe99aa",
                     project="osiris", cwd="/w/osiris", model="claude-fable-5",
                     session_key="view-of:aaaa0001")
    data = await fleet_data(p)
    mine = [m for m in data["mounts"] if m["agent_id"] == "agent:cafe99aa"]
    assert len(mine) == 1                          # one soul, one row
    assert mine[0]["model"] == "claude-opus-4-8"   # the real row testifies
    assert mine[0]["sessions"] == 2 and mine[0]["live"] is True
    html = render_fleet(data)
    # the doors are EXPLAINED, never counted as population (operator, 2026-07-16):
    # the row unfolds to say what each way-in IS, in plain words
    assert "1 agent, 2 doors" in html
    assert "a window onto the same mind" in html   # the view door, explained
    assert "the SESSION itself" in html            # the real door, explained


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
    desk = await read_desk(p)
    html = render_desk(desk)
    # the LANDING page is the ROSTER: osiris appears with one ask — and the body does NOT.
    # That is the flood cure; contents live behind the click.
    assert '<a href="/desk?p=osiris">' in html and "decide something" not in html
    assert "decide something" in render_desk_project(desk, "osiris")
    boxes = await mail_overview(p)
    assert {b["box"] for b in boxes} == {"operator", "neo"}
    html2 = render_mail_box("neo", await mail_threads(p, "neo"))
    assert "lateral note" in html2
    html3 = render_fleet(await fleet_data(p, wake_budget=30))
    assert "wake ledger" in html3


async def test_a_miner_guess_is_never_debt(actions: Actions) -> None:
    """THE MINER MAY NOTICE, BUT MUST NEVER OBLIGE (operator, 2026-07-12: "the desk says this
    session owes 6, accurate or bug?" — bug; five of the six were the miner's inferences and
    two were provably false).

    A DERIVED thread owned by 'operator' is an LLM's guess that the human owes something.
    Nobody asked him. It stays on the desk — some guesses are real — but it must never enter
    `owed`, because a red number he cannot trust is one he learns to ignore, and that is how
    the desk reached a scary red 11 in the first place.
    """
    from src.orchestrator.mailbox import read_desk

    p = actions.pool
    asked = await open_thread(actions, "ship the release — needs your key", owner="operator",
                              repo="osiris")
    assert asked
    guess = await actions.create_or_find_object("Thread", "thread:mined-guess", "session-miner")
    await actions.assert_property(guess, "summary", "Check mid-flight 121G rsync",
                                  "session-miner", NOW, 0.4,
                                  evidence_class=EvidenceClass.DERIVED.value)
    await actions.assert_property(guess, "owner", "operator", "session-miner", NOW, 0.4,
                                  evidence_class=EvidenceClass.DERIVED.value)

    desk = await read_desk(p)
    assert desk["owed"] == 1                                   # the ASK counts
    assert [t["id"] for t in desk["your_queue"]["threads"]] == [str(asked)[:8]]
    guesses = desk["miner_guesses"]["threads"]                 # the GUESS is kept, not counted
    assert [t["summary"] for t in guesses] == ["Check mid-flight 121G rsync"]

    html = render_desk(desk)
    assert "the miner thinks you may owe" in html              # shown, folded, never red
    assert "not counted as debt" in html


async def test_fleet_folds_generations_under_the_living_head(actions: Actions) -> None:
    """THE SOUL IS THE LINEAGE (operator, 2026-07-16: 'metron ix, viii, vii all show up
    as separate seats, but the ancestors are superseded'): generations fold UNDER the
    freshest one — the head is the face, ancestors render as past lives inside the
    unfold, never as peer rows."""
    from datetime import UTC, datetime

    from src.api.chrome import fleet_data, render_fleet
    from src.orchestrator.mounts import save_mount

    p = actions.pool
    for gen, handle_gen in (("agent:3e7a0001", 1), ("agent:3e7a0001-ii", 2),
                            ("agent:3e7a0001-iii", 3)):
        o = await actions.create_or_find_object("Agent", gen, gen)
        await actions.assert_property(o, "handle", "Metra", gen, datetime.now(UTC), 0.9,
                                      evidence_class="self_declared")
        await actions.assert_property(o, "seat_generation", str(handle_gen), gen,
                                      datetime.now(UTC), 0.9,
                                      evidence_class="self_declared")
        await save_mount(p, job_dir=f"/jobs/{gen.removeprefix('agent:')}", agent_id=gen,
                         project="metrahouse", cwd="/w/metra", model=None,
                         session_key=None)
    # age the ancestors so -iii is unambiguously the head
    await p.execute("UPDATE agent_mounts SET last_seen = now() - interval '2 days' "
                    "WHERE agent_id IN ('agent:3e7a0001','agent:3e7a0001-ii')")

    data = await fleet_data(p)
    mine = [m for m in data["mounts"] if m["agent_id"].startswith("agent:3e7a0001")]
    assert len(mine) == 1                                   # one lineage, ONE row
    assert mine[0]["agent_id"] == "agent:3e7a0001-iii"      # the living head is the face
    assert len(mine[0]["ancestors"]) == 2                   # the past lives fold under it
    html = render_fleet(data)
    assert "2 past lives" in html
    assert "SUPERSEDED, an earlier life of this seat" in html

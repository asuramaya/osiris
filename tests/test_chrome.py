"""/desk /mail /fleet — the chrome opened (operator, 2026-07-11). Renderers are pure;
these feed them fixtures. The data functions get one live-graph test each via `actions`."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.api.chrome import (
    page,
    render_desk,
    render_desk_project,
    render_docs,
    render_fleet,
    render_mail_box,
    render_mail_overview,
    render_roadmap,
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
    """PROJECT → AGENT nesting: the room leads its block, the house's souls indent
    beneath it wearing their seat labels."""
    html = render_mail_overview([
        {"project": "osiris",
         "room": {"box": "osiris", "msgs": 12, "unsettled": 0,
                  "last_at": "2026-07-11 19:49:56"},
         "souls": [{"box": "@agent:ad1a1cb0-g40-iv", "soul": "Thoth XLI", "msgs": 3,
                    "unsettled": 1, "last_at": "2026-07-11 19:50:00"}],
         "last_at": "2026-07-11 19:50:00"},
        {"project": "sibling-two",
         "room": {"box": "sibling-two", "msgs": 54, "unsettled": 3,
                  "last_at": "2026-07-11 10:00:00"},
         "souls": [], "last_at": "2026-07-11 10:00:00"},
    ])
    assert '<a href="/mail?box=osiris">' in html
    assert "3 unsettled" in html and "settled" in html
    assert "@Thoth XLI" in html                      # the soul wears its name...
    assert 'box=%40agent%3Aad1a1cb0-g40-iv' in html or \
        'box=@agent:ad1a1cb0-g40-iv' in html         # ...and links to its lane


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
    # the doors are explained LEANLY (operator, 2026-07-16, third pass: '1 agent' is the
    # invariant, never said; the life is the roman in the name, never repeated)
    assert "— 2 doors" in html and "1 agent" not in html
    assert "tab → aaaa0001" in html                # the view door, one short line
    assert "session aaaa0001" in html              # the real door, one short line


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
    assert {g["project"] for g in boxes} == {"operator", "neo"}
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
    assert "life 3 of this seat" not in html                # the roman in the name says it
    assert "2 earlier lives · 2 in window:" in html         # the graph's depth, inside the unfold
    assert "Metra II — " in html                            # an ancestor: name and age, no sermon


async def test_fleet_folds_a_name_across_rebased_id_lineages(actions: Actions) -> None:
    """A restart can re-mint the ID BASE mid-lineage (Metron IX rode a new base while
    VIII and VII kept the old) — the NAME is the soul, so the fold spans bases when a
    handle exists: one row, the freshest generation as the face."""
    from datetime import UTC, datetime

    from src.api.chrome import fleet_data
    from src.orchestrator.mounts import save_mount

    p = actions.pool
    for agent, gen in (("agent:01d0ba5e", "7"), ("agent:4e60ba5e", "8")):
        o = await actions.create_or_find_object("Agent", agent, agent)
        await actions.assert_property(o, "handle", "Metrix", agent, datetime.now(UTC),
                                      0.9, evidence_class="self_declared")
        await actions.assert_property(o, "seat_generation", gen, agent,
                                      datetime.now(UTC), 0.9,
                                      evidence_class="self_declared")
        await save_mount(p, job_dir=f"/jobs/{agent.removeprefix('agent:')}",
                         agent_id=agent, project="metrixhouse", cwd="/w/mx", model=None,
                         session_key=None)
    await p.execute("UPDATE agent_mounts SET last_seen = now() - interval '3 days' "
                    "WHERE agent_id='agent:01d0ba5e'")

    data = await fleet_data(p)
    mine = [m for m in data["mounts"] if (m.get("handle") == "Metrix")]
    assert len(mine) == 1                              # one NAME, one row — across bases
    assert mine[0]["agent_id"] == "agent:4e60ba5e"     # the freshest life is the face
    assert len(mine[0]["ancestors"]) == 1              # the old base is a past life


async def test_fleet_counts_seated_minds_never_passing_strangers(actions: Actions) -> None:
    """SEATED ONLY (operator, 2026-07-17: 'chrome shows fleet 5 but there are only 3 agents
    up'): the whisper's own echo — agent id derived from the session id, no active object
    behind it (a bg-pty host, a spare) — is a stranger's door. Real, live, confessed beside
    the number as a visitor; never counted inside it. A sid-derived id that EARNED an object
    is seated (the visitor gate's other witness)."""
    from src.api.chrome import fleet_data, render_fleet
    from src.orchestrator.mounts import save_mount

    p = actions.pool
    # a seated mind: the bound base differs from the sid-derived base
    await save_mount(p, job_dir="/jobs/beef0001", agent_id="agent:0af3c001",
                     project="osiris", cwd="/w/osiris", model="claude-fable-5",
                     session_key="whisper:beef0001")
    # a passing stranger: id == sid base, no object behind it
    await save_mount(p, job_dir="/jobs/feed0002", agent_id="agent:feed0002",
                     project="atlas", cwd="/w/atlas", model=None,
                     session_key="whisper:feed0002")
    # the earned name: sid-derived base but a real active object — seated
    await actions.create_or_find_object("Agent", "agent:ca11ab1e", "agent:ca11ab1e")
    await save_mount(p, job_dir="/jobs/ca11ab1e", agent_id="agent:ca11ab1e",
                     project="kast", cwd="/w/kast", model=None,
                     session_key="whisper:ca11ab1e")
    data = await fleet_data(p)
    by_id = {m["agent_id"]: m for m in data["mounts"]}
    assert by_id["agent:0af3c001"]["seated"] is True
    assert by_id["agent:feed0002"]["seated"] is False
    assert by_id["agent:ca11ab1e"]["seated"] is True
    assert "2 live · 1 visitor" in render_fleet(data)


# --- /overhead (neo's eye, task #34) ---------------------------------------------------

def _overhead_data() -> dict:
    return {
        "totals": {
            "sessions": 2, "sessions_with_usage": 1, "channel_files": 3,
            "total_tokens": 1_600_000, "hidden_tokens": 400_000,
            "fresh_tokens": 200_000, "cache_read_tokens": 1_400_000,
            "cache_read_pct": 87.5, "hidden_pct": 25.0, "total_bytes": 4096,
            "calls": 120, "reminders": 42, "compactions": 3,
        },
        "top": [
            {"harness": "claude-code", "anchor_sid": "beef0001", "project": "widget",
             "channel_files": 3, "total_tokens": 1_600_000, "hidden_tokens": 400_000,
             "fresh_tokens": 200_000, "cache_read_tokens": 1_400_000,
             "cache_read_pct": 87.5, "hidden_pct": 25.0, "multiplier": 1.3,
             "basis": "tokens", "bytes": 2048, "calls": 120, "reminders": 42,
             "compactions": 3},
            {"harness": "claude-code", "anchor_sid": "cafe0002",
             "project": "<script>x</script>", "channel_files": 0, "total_tokens": 0,
             "hidden_tokens": 0, "fresh_tokens": 0, "cache_read_tokens": 0,
             "cache_read_pct": 0.0, "hidden_pct": 0.0, "multiplier": 1.0,
             "basis": "bytes", "bytes": 2048, "calls": 0, "reminders": 0,
             "compactions": 0},
        ],
    }


def test_render_overhead_totals_and_rows() -> None:
    from src.api.chrome import render_overhead
    html = render_overhead(_overhead_data())
    assert "1.6M tokens" in html
    assert "25.0% hidden" in html
    assert "42 reminders" in html
    assert "beef0001" in html
    assert "1.3×" in html
    # the usage-less session falls back to bytes, and says so
    assert "2KB" in html and "(bytes)" in html
    # untrusted project names are escaped, never rendered live
    assert "<script>" not in html


def test_render_overhead_empty_store() -> None:
    from src.api.chrome import render_overhead
    html = render_overhead({
        "totals": {"sessions": 0, "sessions_with_usage": 0, "channel_files": 0,
                   "total_tokens": 0, "hidden_tokens": 0, "fresh_tokens": 0,
                   "cache_read_tokens": 0, "cache_read_pct": 0.0, "hidden_pct": 0.0,
                   "total_bytes": 0, "calls": 0, "reminders": 0, "compactions": 0},
        "top": []})
    assert "eaten nothing yet" in html


def test_render_overhead_telemetry_band() -> None:
    from src.api.chrome import render_overhead
    html = render_overhead(_overhead_data(), telemetry={
        "events": 4242, "sessions": 39, "devices": 1, "event_kinds": 12,
        "oldest": "2026-07-02T23:00:00+00:00", "newest": "2026-07-16T10:58:00+00:00",
        "files": 39, "bytes": 2_400_000,
        "top_events": [{"event": "tengu_api_success", "n": 900},
                       {"event": "tengu_<b>x</b>", "n": 1}]})
    assert "retained telemetry" in html
    assert "4242 events" in html
    assert "2026-07-02" in html and "2026-07-16" in html
    assert "api_success ×900" in html          # the tengu_ prefix is noise, dropped
    assert "<b>x</b>" not in html              # event names are escaped
    # without a summary the band is absent, not zero-filled
    assert "retained telemetry" not in render_overhead(_overhead_data(), None)


# ── /roadmap ─────────────────────────────────────────────────────────────────────────────

def _roadmap_data(project: str = "seats") -> dict:
    return {
        "project": project,
        "arcs": [
            {"arc": "Security", "statuses": [
                {"status": "open", "owners": [
                    {"owner": "unowned", "threads": [
                        {"id": "aaaa1111", "summary": "an open duty",
                         "kind": "obligation"}]},
                    {"owner": "operator", "threads": [
                        {"id": "bbbb2222", "summary": "an operator blocker"}]},
                ]},
                {"status": "resolved", "owners": [
                    {"owner": "agent:builder", "threads": [
                        {"id": "cccc3333", "summary": "shipped work"}]},
                ]},
            ]},
            {"arc": "unsorted", "statuses": [
                {"status": "open", "owners": [
                    {"owner": "unowned", "threads": [
                        {"id": "dddd4444", "summary": "an untagged duty"}]},
                ]},
            ]},
        ],
        "note": "v2: arc→status→owner (thread 8df8e611)",
    }


def test_render_roadmap_bands_by_arc_then_status_then_owner() -> None:
    html = render_roadmap(_roadmap_data())
    assert "roadmap" in html and "seats" in html
    assert "4 threads" in html
    assert 'id="rm-arc-Security"' in html and 'id="rm-arc-unsorted"' in html
    assert html.index('id="rm-arc-Security"') < html.index('id="rm-arc-unsorted"')
    assert 'id="rm-open"' in html and 'id="rm-resolved"' in html
    assert html.index('id="rm-open"') < html.index('id="rm-resolved"')  # fixed status order
    assert "an open duty" in html and "operator" in html
    assert "shipped work" in html and "agent:builder" in html
    assert "an untagged duty" in html
    assert '<span class="pill">obligation</span>' in html
    assert "arc→status→owner" in html


def test_render_roadmap_every_real_arc_is_expanded_unsorted_is_not() -> None:
    html = render_roadmap(_roadmap_data())
    assert '<details class="proj" id="rm-arc-Security" open>' in html
    assert '<details class="proj" id="rm-arc-unsorted">' in html


def test_render_roadmap_open_band_is_expanded_others_are_not() -> None:
    html = render_roadmap(_roadmap_data())
    assert '<details class="band" id="rm-open" open>' in html
    assert '<details class="band" id="rm-resolved">' in html


def test_render_roadmap_refuses_honestly() -> None:
    html = render_roadmap({"error": "no such project: 'ghost'"})
    assert "no such project" in html


def test_render_roadmap_empty_project_says_so() -> None:
    html = render_roadmap({"project": "quietproj", "arcs": [], "note": "v2..."})
    assert "nothing tracked yet" in html and "quietproj" in html


def test_render_roadmap_is_generic_across_projects() -> None:
    """Same renderer, two different projects — nothing here hardcodes 'osiris' or 'seats'
    (the operator's 'every project needs it' — a primitive, not an osiris page)."""
    for project in ("seats", "some-other-project"):
        html = render_roadmap(_roadmap_data(project))
        assert project in html


# ── /docs ────────────────────────────────────────────────────────────────────────────────

def _docs_data() -> dict:
    return {
        "sections": [
            {"topic": "getting-started", "docs": [
                {"canonical": "ref:install", "name": "INSTALL", "vendor": "osiris"}]},
            {"topic": "reference", "docs": [
                {"canonical": "ref:palantir-object-sets", "name": "Object Sets",
                 "vendor": "palantir"}]},
        ],
    }


def test_render_docs_sections_by_topic() -> None:
    html = render_docs(_docs_data(), "seats")
    assert "docs" in html and "seats" in html
    assert "2 docs" in html
    assert 'id="doc-getting-started"' in html and 'id="doc-reference"' in html
    assert "INSTALL" in html and "osiris" in html
    assert "Object Sets" in html and "palantir" in html


def test_render_docs_empty_says_so() -> None:
    html = render_docs({"sections": []}, "seats")
    assert "nothing ingested yet" in html
    assert "python -m src.ingest.reference" in html


def test_render_docs_is_generic_across_projects() -> None:
    for project in ("seats", "some-other-project"):
        html = render_docs(_docs_data(), project)
        assert project in html


def test_page_shell_includes_the_new_nav_tabs() -> None:
    html = page("roadmap", "roadmap", "<p>x</p>")
    # "docs" routes at /canon, not /docs — FastAPI reserves /docs for its own Swagger UI
    for tab in ("/roadmap", "/canon"):
        assert f'href="{tab}"' in html

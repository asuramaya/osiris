"""Static-source guards for console.js routes that have no browser test coverage.

tagIt() posted to /objects/:id/tag instead of the real /objects/:id/tags route, was fixed once
on a branch (khnum-land-console, 1867a5c), then silently lost when a legitimate full rewrite
(9e2226b) superseded that branch without carrying the one-line fix forward — untested, so
nothing caught it live again for three days (thread 98344adb). This file exists so the same
route can't regress invisibly a second time.
"""
from __future__ import annotations

from pathlib import Path

_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()


def test_tag_it_posts_to_the_real_tags_route() -> None:
    assert "/objects/' + id + '/tags'" in _JS
    assert "/objects/' + id + '/tag'" not in _JS


# #92's tail (Thoth dispatch 9257 piece 1): the Mailbox surface used to fetch /pulse, which
# never carried a `messages` array (src/api/app.py's pulse_route returns
# {line, live, owed, briefs, wakes, spend} only) — renderMailbox() always rendered a false
# "no JSON mail-list route exists yet" wall. Ported onto the REAL mail read: the "mail" saved
# composition (MAIL_OVERVIEW, compositions.py — chrome.mail_overview's own fold-aware room/soul
# read), run through the same generic Osiris.renderResult() pipeline every other composition
# surface uses, with each row's "run:mail_threads" drill-in (test_row_action_ui.py's own
# documented gap: "the page shell, index.html, owns actually running the Function... untested
# here") finally caught by a scoped `osiris:run` listener.


def test_mailbox_no_longer_reads_the_messageless_pulse_route() -> None:
    assert "fetch('/pulse')" not in _JS.split("// ── Mailbox")[1].split("// ── ")[0]
    assert "no JSON mail-list route exists yet" not in _JS


def test_render_mailbox_runs_the_mail_composition() -> None:
    assert "async function renderMailbox() { await runMailboxComposition('mail', {}); }" in _JS


def test_mailbox_composition_runner_hits_the_saved_composition_route_with_no_args() -> None:
    assert "'/compositions/' + encodeURIComponent(name) + '/run'" in _JS


def test_mailbox_composition_runner_hits_run_spec_for_a_function_drill_in() -> None:
    assert "'/compositions/run-spec'" in _JS
    assert "op: 'function', name: name, args: args" in _JS


def test_mailbox_listens_for_the_osiris_run_navigation_event_scoped_to_its_own_surface() -> None:
    assert "document.addEventListener('osiris:run'" in _JS
    listener = _JS.split("document.addEventListener('osiris:run'", 1)[1][:300]
    assert "if (ACTIVE_SURFACE !== 'mailbox') return;" in listener
    assert "runMailboxComposition(e.detail.name, e.detail.args || {});" in listener


# THE COMPOSER SHELL (Thoth dispatch 9257 piece 2, thread 588148bb): "pick a room, run/author/
# fork compositions" — CMD-K's runTool() used to POST /compositions/{name}/run and dump the raw
# JSON into a <pre>, never reaching osiris.js's generic renderer (P4, commit 9c5e923) that piece
# 1 finally wired up for the mailbox. Every saved composition now renders through the same
# Osiris.renderResult() pipeline, room-scoped listing feeds the palette (PICK), a raw-spec save
# flow reaches /compositions (AUTHOR), and forking a running composition's own spec under a new
# name (FORK) is one more POST to the same route.


def test_run_tool_delegates_to_the_generic_composer_runner() -> None:
    assert "async function runTool(name) { await runComposition(name, {}, FOCUS); }" in _JS


def test_composer_runner_renders_through_osiris_render_result_not_a_raw_json_dump() -> None:
    body = _JS.split("async function runComposition(", 1)[1].split("\nfunction ", 1)[0]
    assert "Osiris.renderResult(res" in body
    assert "JSON.stringify(res, null, 2)" not in body


def test_composer_runner_hits_run_spec_for_a_function_drill_in() -> None:
    body = _JS.split("async function runComposition(", 1)[1].split("\nfunction ", 1)[0]
    assert "'/compositions/run-spec'" in body
    assert "op: 'function', name: name, args: args" in body


def test_shell_listens_for_osiris_run_scoped_away_from_the_mailbox_surface() -> None:
    needle = "document.addEventListener('osiris:run'"
    hits = [i for i in range(len(_JS)) if _JS.startswith(needle, i)]
    assert len(hits) == 2  # renderMailbox's own listener (piece 1) + this general one
    shell_listener = _JS[hits[1]:hits[1] + 700]
    assert "if (ACTIVE_SURFACE === 'mailbox') return;" in shell_listener
    # `bind_subject` (Thoth dispatch 9676/9690, 588148bb piece 4): a row_action whose target
    # is an op-tree, not a Function (browse), carries the row's own object as an `_action.
    # subject` — the listener must prefer it over the currently-focused node (FOCUS is
    # unrelated to which row was clicked), never fall back to FOCUS when a real subject rode
    # along on the event.
    assert ("runComposition(e.detail.name, e.detail.args || {}, "
            "e.detail.subject || FOCUS);") in shell_listener


def test_author_composition_previews_before_saving() -> None:
    body = _JS.split("async function authorComposition()", 1)[1].split("\nasync function ", 1)[0]
    # the preview call comes before the save call — a bad spec must never reach the DB
    preview_at = body.index("'/compositions/run-spec'")
    save_at = body.index("JSON.stringify({ name: name, spec: spec, room_id: ROOM || null })")
    assert preview_at < save_at


def test_fork_composition_saves_the_on_screen_spec_under_a_new_name() -> None:
    body = _JS.split("async function forkComposition()", 1)[1].split("\nasync function ", 1)[0]
    assert "LAST_COMPOSITION_RUN.spec" in body
    assert "room_id: ROOM || null" in body


def test_load_compositions_is_room_scoped_and_called_on_room_switch() -> None:
    assert "'/compositions' + (ROOM ? ('?room=' + encodeURIComponent(ROOM)) : '')" in _JS
    switch_room = _JS.split("async function switchRoom(", 1)[1].split("\nasync function ", 1)[0]
    assert "await loadCompositions();" in switch_room


def test_palette_has_an_author_composition_entry() -> None:
    assert "Author composition…" in _JS
    assert "run: () => authorComposition()" in _JS


# THE READ-ONLY PANE (Thoth dispatch 9378, lane B piece 2, thread 9d2aaf4d): pick a live seat
# (/pane/live), watch its transcript stream live (/pane/{agent_id}/stream, SSE) — no writes,
# no spawn.


def test_render_pane_loads_the_live_seat_picker() -> None:
    assert "await fetch('/pane/live')" in _JS


def test_open_pane_stream_opens_the_sse_route_for_the_picked_agent() -> None:
    assert "new EventSource('/pane/' + encodeURIComponent(agentId) + '/stream')" in _JS


def test_switching_away_from_pane_closes_the_open_sse_connection() -> None:
    assert "if (surface !== 'pane') closePaneStream();" in _JS


def test_pane_stream_error_events_close_the_connection_rather_than_looping_forever() -> None:
    body = _JS.split("PANE_SOURCE.onmessage = function(ev) {", 1)[1].split("};", 1)[0]
    assert "closePaneStream();" in body


# THE REPLY DOOR (Thoth dispatch 9378, lane B piece 3): a turn typed in the pane posts
# through /pane/{agent}/reply — a one-shot turn against the seat's own session, never a
# new spawn (piece 3's own dispatch line: "spawn only through launch's admission").


def test_send_pane_reply_posts_to_the_reply_route_for_the_open_agent() -> None:
    assert "'/pane/' + encodeURIComponent(PANE_AGENT) + '/reply'" in _JS


def test_send_pane_reply_never_calls_the_launch_or_spawn_doors() -> None:
    body = _JS.split("async function sendPaneReply()", 1)[1].split("\nasync function ", 1)[0]
    assert "runTool(" not in body and "runComposition(" not in body


def test_palette_search_includes_saved_compositions_in_both_search_paths() -> None:
    assert "SAVED_COMPOSITIONS.filter(c => c.name.toLowerCase().includes(ql))" in _JS
    assert "OMNI_ITEMS = toolHits.concat(compHits);" in _JS
    assert "OMNI_ITEMS = toolHits.concat(compHits, graphHits).slice(0, 16);" in _JS


# THE PROJECTS SWAP (Thoth dispatch 9542/9676/9690/9716, 588148bb): the hardcoded /projects
# fetch + hand-rolled projectRow()/openProjectInBrowse() replaced by the "projects" saved
# composition — proven complete across 4 pieces first (object_count, bucket badges, worktree
# nesting, click-through), plus the name-resolution parity gap the swap itself surfaced. The
# status toggle stays (never collapse the status dimension to one number, msg 5631); the
# table body renders through the same generic pipeline every other composition uses.


def test_projects_no_longer_fetches_the_hardcoded_route() -> None:
    assert "fetch('/projects')" not in _JS
    assert "'/compositions/projects/run'" in _JS


def test_projects_hand_rolled_row_renderer_is_gone() -> None:
    assert "function projectRow(" not in _JS
    assert "function openProjectInBrowse(" not in _JS


def test_projects_renders_through_the_generic_composer_pipeline() -> None:
    body = _JS.split("async function renderProjects()", 1)[1].split("\nfunction ", 1)[0]
    assert "Osiris.renderResult(filtered" in body
    assert "JSON.stringify(res, null, 2)" not in body


def test_projects_status_toggle_still_narrows_client_side_over_every_status() -> None:
    # the composition itself fetches every status in one call (status:"any", piece 1); the
    # toggle narrowing stays client-side, same shape as the pre-swap page.
    body = _JS.split("async function renderProjects()", 1)[1].split("\nfunction ", 1)[0]
    assert "PROJECTS_INDEX_STATUS === 'all' || r.status === PROJECTS_INDEX_STATUS" in body

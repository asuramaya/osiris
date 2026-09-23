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
_INDEX_HTML = (Path(__file__).parent.parent / "src" / "ui" / "static" / "index.html").read_text()


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
    save_at = body.index("JSON.stringify({ name: name, spec: spec })")
    assert preview_at < save_at


def test_fork_composition_saves_the_on_screen_spec_under_a_new_name() -> None:
    body = _JS.split("async function forkComposition()", 1)[1].split("\nasync function ", 1)[0]
    assert "LAST_COMPOSITION_RUN.spec" in body
    assert "room_id" not in body


def test_load_compositions_is_never_room_scoped() -> None:
    """?ROOM= RESIDUE RETIRED (thread 96f09d48, decision 31717ca7, Thoth DM 10792/12807):
    "end to end" turned out to mean the still-live ?room= READ filter too, not just the
    mint/list doors retired earlier this wave — switchRoom itself was already gone; now
    loadCompositions()'s own room-conditional query string, and the ROOM constant it read,
    are gone too. An unscoped fetch, unconditionally, same as the retired ternary's own
    always-falsy branch already produced."""
    assert "const ROOM" not in _JS
    assert "await fetch('/compositions').then(r => r.json());" in _JS
    assert "function switchRoom(" not in _JS
    assert "loadCompositions()" in _JS.split("Osiris.loadSchema().then", 1)[1][:300]


# ROOM RETIREMENT (thread 96f09d48, decision 31717ca7, Thoth DM 10792): the operator's own
# word — "scope really died and made itself obsolete... gotta remove that too." The
# workspace pill, its dropdown, and every function that only existed to drive them are
# gone; the header repo selector (#repo-pill) is now the one scoping lever.


def test_workspace_pill_functions_are_gone() -> None:
    for fn in ("toggleWorkspaceDropdown", "renderWorkspaceDropdown", "selectWorkspace",
               "updateWorkspaceScopeUI", "loadRooms", "newRoom"):
        assert f"function {fn}(" not in _JS, f"{fn} should have been removed"
    assert "let ROOMS" not in _JS and "ROOMS =" not in _JS


def test_workspace_pill_markup_is_gone_from_index_html() -> None:
    for needle in ("workspace-pill", "workspace-dropdown", "workspace-dd-items",
                   'id="room"'):
        assert needle not in _INDEX_HTML


def test_object_scope_params_no_longer_reads_a_room_subject() -> None:
    body = _JS.split("function objectScopeParams()", 1)[1].split("\n}", 1)[0]
    assert "ROOMS" not in body
    assert "room.config.subject" not in body


def test_watch_console_no_longer_syncs_room_id_but_keeps_focused_object_id() -> None:
    body = _JS.split("function watchConsole()", 1)[1]
    assert "s.room_id" not in body
    assert "s.focused_object_id" in body


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
    # TIP 1b review flaw #7 added a third source (a client-side agent-handle scan) to the
    # debounced graph search's own final assignment.
    assert "OMNI_ITEMS = toolHits.concat(compHits, graphHits, agentHits).slice(0, 16);" in _JS


# THE CONSOLE CHROME CLEANUP (thread 0be2f790's own operator-finding follow-up, Thoth DM
# 10731 piece 1): the left-nav "Projects" surface — redundant with the header's own repo
# selector, per the operator's own word — is retired: no nav item, no bespoke
# renderProjects()/status-toggle chrome. The "projects" saved composition itself is
# UNTOUCHED and stays reachable exactly like every other saved composition (the omnibox,
# or the CLI/MCP composition-run door) — only the surface wrapper around it is gone.
# Superseding test_projects_no_longer_fetches_the_hardcoded_route/
# test_projects_hand_rolled_row_renderer_is_gone/
# test_projects_renders_through_the_generic_composer_pipeline/
# test_projects_status_toggle_still_narrows_client_side_over_every_status, which all
# asserted on renderProjects()'s own body — meaningless once that function is gone.


def test_projects_nav_item_is_gone() -> None:
    assert 'data-surface="projects"' not in _INDEX_HTML
    assert "nav-projects" not in _INDEX_HTML
    assert ">Projects<" not in _INDEX_HTML


def test_projects_surface_chrome_is_gone_from_console_js() -> None:
    assert "function renderProjects(" not in _JS
    assert "PROJECTS_INDEX_DATA" not in _JS
    assert "setProjectsStatusFilter" not in _JS
    assert "if (surface === 'projects')" not in _JS


def test_projects_composition_still_runs_through_the_generic_omnibox_path() -> None:
    # the underlying data access is untouched — SAVED_COMPOSITIONS (loaded by
    # loadCompositions(), the omnibox's own source list) still resolves a "projects" hit
    # to the SAME generic runTool()/runComposition() pipeline every other saved
    # composition uses, never a route this cleanup would have orphaned.
    assert "'/compositions/' + encodeURIComponent(name) + '/run'" in _JS


def test_projects_array_and_its_loader_survive_for_the_repo_pill() -> None:
    # loadProjects()/PROJECTS stay — the repo-scope pill (renderRepoDropdown) depends on
    # them, even though the left-nav Projects SURFACE that also used to read from a
    # similarly-named endpoint is gone.
    assert "async function loadProjects()" in _JS
    assert "PROJECTS = await fetch('/objects?type=SoftwareProject')" in _JS


# THE BROWSE SWAP (Thoth dispatch 9838/9855, 588148bb): the entity set's own load used to
# GET /objects directly; it now runs an ephemeral {"op":"select","scope":{...}} op-tree
# through /compositions/run-spec — the SAME `scope` opt-in (and, since it's the same
# extraction, the same list_objects_scoped SQL) /objects itself calls. Type pills/search/
# sort stay client-side residue, same discipline the Projects swap used.


def test_browse_object_set_no_longer_fetches_objects_directly() -> None:
    assert "fetch(objectSetUrl(" not in _JS
    assert "function objectSetUrl(" not in _JS


def test_browse_select_runs_an_ephemeral_scope_spec_through_run_spec() -> None:
    body = _JS.split("async function runBrowseSelect(", 1)[1].split("\nfunction ", 1)[0]
    assert "'/compositions/run-spec'" in body
    assert "spec: spec" in body and "op: 'select', scope: browseScope(cursor)" in body


def test_browse_scope_carries_the_same_exclude_types_case_project_as_before() -> None:
    body = _JS.split("function browseScope(cursor) {", 1)[1].split("\n}", 1)[0]
    assert "scope.exclude_types = ['Agent']" in body
    assert "scope.case_id = s.case_id" in body
    assert "scope.project = s.project" in body


def test_browse_load_more_reuses_the_exact_same_helper_as_the_initial_load() -> None:
    body = _JS.split("async function loadMoreObjects()", 1)[1].split("\n}\n", 1)[0]
    assert "runBrowseSelect({ created_at: last.created_at, id: last.id })" in body


# THE TABLE FILTER QUERY SHAPE (thread 0be2f790's own operator-finding follow-up, Thoth DM
# 10711): the type-filter pill bar and the omnibox's status:/free-text portion now drive
# browseScope() server-side, instead of only re-filtering whatever page was already loaded —
# the fix for the operator paging through 26,351 of 30,290 rows to find 5 matches.


def test_browse_scope_carries_the_selected_types_and_parsed_search() -> None:
    body = _JS.split("function browseScope(cursor) {", 1)[1].split("\n}", 1)[0]
    assert "scope.types = Array.from(SELECTED_ENTITY_TYPES)" in body
    assert "scope.status = parsed.status" in body
    assert "scope.q = parsed.text" in body


def test_toggle_entity_type_refetches_the_server_scope_not_just_a_local_rerender() -> None:
    body = _JS.split("function toggleEntityType(t) {", 1)[1].split("\n}", 1)[0]
    assert "refetchFilteredObjectSet()" in body


def test_filter_entity_search_debounces_the_refetch() -> None:
    body = _JS.split("function filterEntitySearch(q) {", 1)[1].split("\n}", 1)[0]
    assert "setTimeout(refetchFilteredObjectSet, 250)" in body


def test_refetch_filtered_object_set_clears_and_reloads_the_scoped_set() -> None:
    body = _JS.split("async function refetchFilteredObjectSet()", 1)[1].split("\n}", 1)[0]
    assert "SET = []" in body
    assert "await loadObjectSet()" in body


def test_entity_toolbar_total_badge_reads_the_scoped_count_when_a_filter_is_active() -> None:
    body = _JS.split("function renderEntityToolbar()", 1)[1].split("\nfunction ", 1)[0]
    assert "filterActive ? SET.length" in body


def test_browse_bridges_composition_items_onto_the_shape_rendering_already_expects() -> None:
    body = _JS.split("async function runBrowseSelect(", 1)[1].split("\nfunction ", 1)[0]
    assert "name: it.display_label || it.label" in body
    assert "status: it.status" in body and "created_at: it.created_at" in body


# THE BACKUP CONFIG PANEL RETIREMENT (THE SETTINGS MENU piece 3, thread 7eb26f68, Thoth's
# GO mail 10084): the small dedicated view Wave 21 built (thread f04cce36 piece 3b) is
# gone — its 3 fields (vault_path, one schedule per timer, offbox_repositories) now render
# as ordinary registry entries in the generic Settings panel below, grouped under one
# "backup" section by the same key-prefix grouping every other group already uses.


def test_backup_panel_is_fully_retired_from_console_js() -> None:
    assert "renderBackupPanel" not in _JS
    assert "renderBackupPanelHtml" not in _JS
    assert "saveBackupVaultPath" not in _JS
    assert "saveBackupTimerSchedules" not in _JS
    assert "'Backup settings…'" not in _JS
    # the RETIRED panel's own dedicated fetch call is gone -- but the door itself
    # (GET /backup-settings) didn't move (backup_settings.py's own module docstring:
    # "storage moved, the door didn't"), and a later, genuinely different caller
    # legitimately reuses it: the Offload Targets panel (Thoth mail 12811/12814/12985,
    # tests/test_key_offload_panels.py). This blanket string ban would collide with
    # that legitimate reuse, so it narrows to the one retired call site's own shape.
    assert "async function renderBackupPanel(" not in _JS


# THE SETTINGS MENU (ruling be1b2e47, thread 7eb26f68 pieces 2+3): a generic view over
# settings(action='list')/GET /settings — one renderer per field type, never a hand-
# built form per knob. Reached via CMD-K.


def test_settings_panel_reads_the_settings_list_route() -> None:
    # THE SETTINGS PANE (Thoth mail 13350) moved the shared fetch into
    # renderSettingsInto(containerId); renderSettingsPanel is now a one-line
    # standalone-container wrapper around it (still reachable, no longer its own
    # palette row — see test_settings_panel_has_a_palette_entry_not_a_nav_tab below).
    body = _JS.split("async function renderSettingsInto(containerId)", 1)[1].split(
        "\nasync function ", 1)[0]
    assert "fetch('/settings')" in body


def test_settings_panel_has_a_palette_entry_not_a_nav_tab() -> None:
    # the three old standalone rows (Key…/Offload Targets…/Settings…) consolidated
    # into ONE "Settings" entry over THE SETTINGS PANE (Thoth mail 13350).
    assert "'Settings…'" not in _JS
    assert "label: 'Settings'," in _JS
    assert "run: () => renderSettingsPane()" in _JS
    assert 'data-surface="settings"' not in _JS


def test_settings_panel_covers_every_registry_field_type() -> None:
    body = _JS.split("function settingsFieldInput(item)", 1)[1].split(
        "\nfunction ", 1)[0]
    for t in ("secret_ref", "bool", "enum", "int", "float", "json", "records",
             "path", "schedule"):
        assert "'" + t + "'" in body or '"' + t + '"' in body


def test_settings_panel_path_and_schedule_round_trip_an_empty_box_to_null() -> None:
    """THE SETTINGS MENU piece 3 (thread 7eb26f68): clearing a path/schedule input and
    saving must send null — the only way to un-set a real infra path or timer override
    back to the shipped default, same UX the retired backup panel had."""
    body = _JS.split("function settingsFieldValue(item)", 1)[1].split(
        "\nasync function ", 1)[0]
    assert "item.type === 'path' || item.type === 'schedule'" in body
    assert "=== '' ? null :" in body


def test_settings_panel_secret_ref_never_gets_a_save_button() -> None:
    body = _JS.split("function renderSettingsPanelHtml(items)", 1)[1].split(
        "\nfunction ", 1)[0]
    assert "it.type === 'secret_ref'" in body


def test_settings_panel_secret_ref_gets_a_rotate_button_instead() -> None:
    """SECRETS ROTATE ACT (thread f4498ab304e4's own follow-up, Thoth mail 10441): the
    empty cell `secret_ref` used to render (a Save button would silently no-op — that
    door refused outright until this pass) is now a Rotate button wired to its own
    confirm-then-POST function, not saveSetting's own value-reading path."""
    body = _JS.split("function renderSettingsPanelHtml(items)", 1)[1].split(
        "\nfunction ", 1)[0]
    assert "rotateSecret(" in body
    assert ">Rotate<" in body


def test_rotate_secret_function_exists_and_never_reads_settingsfieldvalue() -> None:
    assert "async function rotateSecret(key)" in _JS
    body = _JS.split("async function rotateSecret(key)", 1)[1].split(
        "\nasync function ", 1)[0]
    assert "settingsFieldValue" not in body  # no <input> exists for a secret to read
    assert "fetch('/settings'" in body
    assert "confirm(" in body  # unconditional, never gated on item.consequence


def test_settings_panel_shows_a_live_value_only_when_the_backend_sends_one() -> None:
    """Thread c5ba8681 (Imhotep's own follow-up, not yet built): a future non-null
    `live` field just appears beside `value` — no UI change needed when it arrives."""
    body = _JS.split("function renderSettingsPanelHtml(items)", 1)[1].split(
        "\nfunction ", 1)[0]
    assert "it.live !== undefined && it.live !== null" in body


def test_settings_panel_confirms_before_a_high_consequence_save() -> None:
    body = _JS.split("async function saveSetting(key)", 1)[1].split(
        "\n// ── ", 1)[0]
    assert "item.consequence === 'high'" in body
    assert "confirm(" in body


def test_settings_panel_because_is_only_prompted_when_the_spec_requires_it() -> None:
    body = _JS.split("async function saveSetting(key)", 1)[1].split(
        "\n// ── ", 1)[0]
    assert "if (item.requires_because)" in body


def test_settings_panel_saves_post_to_the_settings_route() -> None:
    body = _JS.split("async function saveSetting(key)", 1)[1].split(
        "\n// ── ", 1)[0]
    assert "fetch('/settings'" in body


# ── Repairs panel (thread c89a9873, wave 22, ruling 7be61879) ──────────────────────────

def test_repairs_panel_has_a_palette_entry() -> None:
    assert "'Repairs…'" in _JS
    assert "run: () => renderRepairsPanel()" in _JS


def test_repairs_panel_lists_all_seven_targets() -> None:
    body = _JS.split("var REPAIRS_TARGETS", 1)[1].split(
        "\nfunction renderRepairsPanel", 1)[0]
    for target in (
        "bootstrap_orphan_references", "boot_alarm_commit_links",
        "task_sync_citation_links", "lineage_repo_links", "agent_project_links",
        "closed_by_real_sources", "operator_charter",
    ):
        assert "'" + target + "'" in body


def test_repairs_panel_operator_charter_has_no_apply_control() -> None:
    """The one target excluded from UI apply (thread c89a9873's own scope note) —
    structurally no button, not merely hidden behind a confirm."""
    body = _JS.split("function renderRepairsPanel()", 1)[1].split(
        "\nasync function dryRunRepair", 1)[0]
    assert "cliOnly" in body
    assert ">CLI-only<" in body


def test_repairs_panel_dry_run_posts_to_the_backfill_route() -> None:
    body = _JS.split("async function dryRunRepair(target)", 1)[1].split(
        "\nasync function applyRepair", 1)[0]
    assert "fetch('/backfill'" in body
    assert "dry_run: true" in body


def test_repairs_panel_apply_confirms_and_requires_because() -> None:
    body = _JS.split("async function applyRepair(target)", 1)[1].split(
        "\n// ── Projects", 1)[0]
    assert "confirm(" in body
    assert "prompt(" in body
    assert "dry_run: false" in body


def test_settings_panel_shows_structured_per_field_errors_inline() -> None:
    body = _JS.split("async function saveSetting(key)", 1)[1].split(
        "\n// ── ", 1)[0]
    assert "errEl.textContent = res.error" in body


# THE ATLAS REMOVAL (Thoth dispatch 9563, 588148bb): "atlas is terrible, it's like browse
# in graph mode but worse and uglier, I don't think it should exist at all" — the operator's
# own word. The sigma.js/graphology full-graph surface is gone; its useful backend
# (/graph/supernodes, /graph/clusters, the heartbeat layout) folds into browse's own graph
# mode as a separate piece — the removal is frontend-only, backend untouched here.


def test_atlas_surface_is_gone_from_console_js() -> None:
    assert "makeAtlas" not in _JS
    assert "ensureAtlas" not in _JS
    assert "atlasZoomOut" not in _JS
    assert "sigma-atlas" not in _JS


def test_atlas_nav_entry_and_vendor_scripts_are_gone_from_index_html() -> None:
    assert "nav-atlas" not in _INDEX_HTML
    assert "sigma-atlas" not in _INDEX_HTML
    assert "sigma.min.js" not in _INDEX_HTML
    assert "graphology.umd.min.js" not in _INDEX_HTML

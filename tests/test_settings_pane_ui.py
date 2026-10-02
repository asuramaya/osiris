"""THE SETTINGS PANE (Thoth mail 13350): ONE console pane replacing the three formerly-
separate palette panels (Key…, Offload Targets…, Settings…) with five embedded
sections -- KEY, BACKUP & OFFLOAD, REGISTRY, OPERATOR DESK, THE BOX. Every section reads
through an EXISTING REST door except four genuinely thin new ones (GET /restic-key/
status, GET /deploy-status, GET /operator/desk, POST /operator/desk/reply — API tests in
tests/test_settings_pane_api.py). Every section degrades independently.

Mirrors the repo's existing static-source-guard convention: string/substring proofs
against the served JS, no browser harness."""
from __future__ import annotations

from pathlib import Path

_CONSOLE_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()
_INDEX_HTML = (Path(__file__).parent.parent / "src" / "ui" / "static" / "index.html").read_text()


# --- reachable from CMD-K and the header, the three old panels consolidated -----------

def test_settings_is_the_single_admin_palette_entry_now() -> None:
    body = _CONSOLE_JS.split("const POWER_TOOLS = [", 1)[1][:3000]
    assert "label: 'Settings'," in body
    assert "run: () => renderSettingsPane()" in body
    # the three old standalone rows are gone -- consolidated, not duplicated
    assert "label: 'Key…'" not in body
    assert "label: 'Offload Targets…'" not in body
    assert "label: 'Settings…'" not in body


def test_header_carries_a_settings_link() -> None:
    assert 'onclick="renderSettingsPane()" title="Settings"' in _INDEX_HTML


def test_old_panel_renderers_still_exist_nothing_deleted() -> None:
    # each old standalone entry point survives, just no longer its own palette row
    assert "async function renderKeyPanel() {" in _CONSOLE_JS
    assert "async function renderOffloadPanel() {" in _CONSOLE_JS
    assert "async function renderSettingsPanel() {" in _CONSOLE_JS


# --- the shell: five sections, loaded in parallel, each degrading independently -------

def test_pane_shell_builds_all_five_sections() -> None:
    # PRODUCT VOICE (ruling 1e2ef5c3, thread ... layout item 3): Readiness ("The Box"
    # renamed) moved up to right after Backup & Offload, before Registry.
    body = _CONSOLE_JS.split("async function renderSettingsPane() {", 1)[1][:950]
    assert "settingsSectionShell('key', 'Key')" in body
    assert "settingsSectionShell('offload', 'Backup &amp; Offload')" in body
    assert "settingsSectionShell('box', 'Readiness')" in body
    assert "settingsSectionShell('registry', 'Registry')" in body
    assert "settingsSectionShell('desk', 'Operator Desk')" in body
    offload_at = body.index("settingsSectionShell('offload'")
    box_at = body.index("settingsSectionShell('box'")
    registry_at = body.index("settingsSectionShell('registry'")
    assert offload_at < box_at < registry_at


def test_pane_shell_loads_sections_independently_via_promise_all() -> None:
    body = _CONSOLE_JS.split("async function renderSettingsPane() {", 1)[1][:1150]
    assert "Promise.all([" in body
    for fn in ("renderSettingsSectionKey()", "renderSettingsSectionOffload()",
               "renderSettingsSectionRegistry()", "renderSettingsSectionDesk()",
               "renderSettingsSectionBox()"):
        assert fn in body


# --- section 1: KEY, embedded not re-implemented ---------------------------------------

def test_section_key_embeds_the_shared_key_renderer() -> None:
    body = _CONSOLE_JS.split("async function renderSettingsSectionKey() {", 1)[1][:150]
    assert "renderKeyInto('settings-sec-key')" in body


# --- section 2: BACKUP & OFFLOAD ------------------------------------------------------

def test_section_offload_embeds_the_editable_panel_and_reads_backup_status() -> None:
    # THE GUI PARITY tip (thread dd11ab34) split the backup-status fetch into its own
    # reusable renderBackupStatusSection() (the "Run offload now" button's own refresh
    # calls it too) -- the section's own body just kicks off both in parallel now.
    body = _CONSOLE_JS.split("async function renderSettingsSectionOffload() {", 1)[1][:900]
    assert "renderOffloadInto('settings-offload-targets')" in body
    assert "renderBackupStatusSection()" in body
    status_body = _CONSOLE_JS.split("async function renderBackupStatusSection() {", 1)[1][:700]
    assert "'/compositions/run-spec'" in status_body
    assert "name: 'backup_status'" in status_body
    # run_spec packages a Function's own output under `items`, never top-level -- a
    # bug this test would have caught: res.timers instead of res.items.timers.
    assert "res.items || {}" in status_body


def test_backup_status_flags_a_schedule_configured_but_not_taken_effect() -> None:
    body = _CONSOLE_JS.split("function renderBackupStatusHtml(status) {", 1)[1][:900]
    assert "t.configured_schedule && t.configured_schedule !== t.schedule" in body


def test_backup_status_shows_offload_target_presence_and_last_offload() -> None:
    body = _CONSOLE_JS.split("function renderBackupStatusHtml(status) {", 1)[1][:1500]
    assert "status.offbox && status.offbox.offload_targets" in body
    assert "t.last_successful_offload" in body
    assert "Connection is not checked automatically" in body


# --- section 3: REGISTRY --------------------------------------------------------------

def test_section_registry_embeds_the_shared_settings_renderer() -> None:
    body = _CONSOLE_JS.split("async function renderSettingsSectionRegistry() {", 1)[1][:150]
    assert "renderSettingsInto('settings-sec-registry')" in body


# --- section 4: OPERATOR DESK -----------------------------------------------------------

def test_section_desk_reads_both_the_desk_json_door_and_merge_candidates() -> None:
    body = _CONSOLE_JS.split("async function renderSettingsSectionDesk() {", 1)[1][:600]
    assert "fetch('/operator/desk')" in body
    assert "fetch('/merge-candidates')" in body
    assert "renderMergeCandidatesHtml(merges)" in body


def test_desk_card_settles_in_place_and_folds_earlier_ids_into_the_ack() -> None:
    body = _CONSOLE_JS.split("function deskCardHtml(c, band) {", 1)[1][:1200]
    assert "c.thread_folded.count" in body
    assert "data-folded=" in body
    assert "onclick=\"ackDeskCard(this)\"" in body


def test_decision_cards_get_a_reply_box_other_bands_do_not() -> None:
    body = _CONSOLE_JS.split("function deskCardHtml(c, band) {", 1)[1][:1200]
    assert "band === 'decision'" in body
    assert "replyDeskCard(" in body


def test_ack_desk_card_posts_the_lead_id_plus_every_folded_id() -> None:
    body = _CONSOLE_JS.split("async function ackDeskCard(btn) {", 1)[1][:400]
    assert "'/desk/settle'" in body
    assert "[id].concat(folded ? JSON.parse(folded) : [])" in body


def test_reply_desk_card_posts_to_the_operator_reply_door() -> None:
    body = _CONSOLE_JS.split("async function replyDeskCard(id) {", 1)[1][:350]
    assert "'/operator/desk/reply'" in body
    assert "id: id, body: body" in body


def test_merge_candidates_show_a_copy_line_never_auto_execute() -> None:
    # constitution #1: identity merges are review-gated, always -- this sub-band must
    # never wire a click directly to a merge write, only to REJECT and to a copy line
    # the operator pastes themselves.
    body = _CONSOLE_JS.split("function renderMergeCandidatesHtml(list) {", 1)[1][:1100]
    assert "Merging is a deliberate step you run yourself" in body
    assert "copyMergeLine(this)" in body
    assert "rejectMergeCandidate(this)" in body
    assert "'/merge-candidates/" not in body


def test_reject_merge_candidate_uses_the_existing_resolve_route() -> None:
    body = _CONSOLE_JS.split("async function rejectMergeCandidate(btn) {", 1)[1][:300]
    assert "'/merge-candidates/' + id + '/resolve'" in body
    assert "decision: 'rejected'" in body


# --- section 5: THE FIRST-RUN STEPPER ---------------------------------------------------
# The aggregation itself (whether a restic target's reachability is ever probed live, the
# ordering/flag logic) moved server-side into src.orchestrator.readiness -- see
# tests/test_readiness.py, e.g. test_restic_target_with_no_live_presence_is_still_done.
# This file only proves the console's own display/action wiring.

def test_stepper_reads_the_readiness_and_deploy_status_routes() -> None:
    body = _CONSOLE_JS.split("async function renderSettingsSectionBox() {", 1)[1][:600]
    assert "fetch('/readiness')" in body
    assert "fetch('/deploy-status')" in body
    assert "renderReadinessStepperHtml(readiness, deployStatus)" in body


def test_stepper_shows_deploy_snapshot_in_sync_state() -> None:
    body = _CONSOLE_JS.split(
        "function renderReadinessStepperHtml(readiness, deployStatus) {", 1)[1][:900]
    assert "deployStatus.in_sync" in body
    assert "deployStatus.running_sha.slice(0, 8)" in body


def test_stepper_highlights_exactly_the_current_step() -> None:
    body = _CONSOLE_JS.split("function readinessStepRow(s) {", 1)[1][:1200]
    assert "readiness-current" in body
    assert "s.current" in body


def test_stepper_only_hands_steps_have_actions_and_all_of_them_jump() -> None:
    body = _CONSOLE_JS.split("var READINESS_STEP_ACTIONS = {", 1)[1].split("};", 1)[0]
    for kind in ("enroll_recovery", "configure_offload", "verify_recovery"):
        assert kind + ":" in body, f"{kind} missing from READINESS_STEP_ACTIONS"
    assert body.count("kind: 'jump'") == 2
    # the two steps that need a terminal (sudo, a physical touch) each get a Copy command button
    assert "tpm_setup: { kind: 'copy'" in body
    assert "verify_recovery: { kind: 'copy'" in body and "osiris soul-key verify-recovery" in body
    assert "perform" not in body


def test_stepper_has_no_chore_buttons_or_one_shot_posts() -> None:
    # encryption, offload runs and restore tests run themselves.
    for gone in ("readinessEncryptExisting", "readinessRunOffload", "readinessRestoreDrill",
                 "Encrypt now", "Run offload now", "Test restore"):
        assert gone not in _CONSOLE_JS.split("READINESS, THE FIRST-RUN STEPPER", 1)[1][:9000], gone


def test_stepper_shows_progress_and_refreshes_while_work_is_in_flight() -> None:
    row = _CONSOLE_JS.split("function readinessStepRow(s) {", 1)[1][:1500]
    assert "p.done / p.total" in row
    assert "p.estimate ? 'about '" in row  # approximate until the first pass completes
    box = _CONSOLE_JS.split("async function renderSettingsSectionBox() {", 1)[1][:1800]
    assert "s.mode === 'auto'" in box
    assert "setTimeout(refreshReadinessIfShown" in box


# THE STALE-STEPPER CHROME-WALK FINDING: the Key/Backup & Offload panels' own action
# functions each re-render only their OWN section on success, so the stepper -- right
# there on the same pane, reporting the exact same underlying facts -- stayed visibly
# stale after clicking, say, "Set up the encryption key" until a manual reload.
# Confirmed live before the fix, in a real browser: initResticKey() succeeded, its own
# section updated, and the stepper's "Backup password set" row kept reading "Not set
# up yet." refreshReadinessIfShown() (a no-op when the stepper isn't the current view)
# now closes every one of these paths.

def test_refresh_readiness_if_shown_is_a_noop_outside_the_settings_pane() -> None:
    body = _CONSOLE_JS.split("function refreshReadinessIfShown() {", 1)[1][:200]
    assert "$('settings-sec-box')" in body
    assert "renderSettingsSectionBox()" in body


def test_every_key_and_offload_action_refreshes_the_stepper_on_success() -> None:
    for fn in ("initKey", "rotateKey", "finishKeyRotate", "restoreDrillKey",
               "enrollRecoveryBrowser", "recoverKeyBrowser", "initResticKey",
               "saveOffloadTargets", "runOffloadTick"):
        body = _CONSOLE_JS.split("async function " + fn + "(", 1)[1].split(
            "\nasync function ", 1)[0]
        assert "refreshReadinessIfShown();" in body, f"{fn} never refreshes the stepper"


def test_tpm_step_copies_its_command_and_carries_the_full_sequence_in_a_tooltip() -> None:
    row = _CONSOLE_JS.split("function readinessStepRow(s) {", 1)[1][:1500]
    assert "readinessCopy(this)" in row
    tip = _CONSOLE_JS.split("var READINESS_STEP_TIPS = {", 1)[1].split("};", 1)[0]
    assert "sudo usermod -aG tss $USER" in tip and "osiris soul-key reseal" in tip


def test_settings_pane_titles_itself_and_clears_the_browse_highlight() -> None:
    body = _CONSOLE_JS.split("async function renderSettingsPane() {", 1)[1][:600]
    assert "$('page-title').textContent = 'Settings'" in body
    assert "classList.remove('sel')" in body


def test_embedded_panels_drop_their_own_title_and_centring_wrapper() -> None:
    # the Settings pane's own section heading already names each panel: no second "REGISTRY"
    assert "function panelEmbedded(containerId)" in _CONSOLE_JS
    for panel, container in (("Registry", "SETTINGS_CONTAINER_ID"), ("Key", "KEY_CONTAINER_ID"),
                             ("Offload targets", "OFFLOAD_CONTAINER_ID")):
        assert f"panelTitleHtml({container}, '{panel}')" in _CONSOLE_JS, panel


def test_panel_tables_wrap_instead_of_clipping_and_browse_keeps_its_one_line_rows() -> None:
    # .ee-table clips every cell to one line (right for Browse's object list); the panels that
    # show prose and form fields carry .ee-form so a setting name or a reason is never cut off.
    assert _CONSOLE_JS.count('class="ee-table ee-form"') == 7
    assert '<table class="ee-table"><thead><tr><th style="width:105px' in _CONSOLE_JS
    css = (Path(__file__).parent.parent / "src" / "ui" / "static" / "osiris.css").read_text()
    form = css.split(".ee-table.ee-form td {", 1)[1][:200]
    assert "white-space: normal" in form and "overflow: visible" in form


def test_phone_widths_close_both_rails_and_float_them_over_the_stage() -> None:
    css = (Path(__file__).parent.parent / "src" / "ui" / "static" / "osiris.css").read_text()
    phone = css.split("PHONE WIDTHS", 1)[1][:1200]
    assert "grid-template-columns: 0 minmax(0, 1fr) 0" in phone
    assert "position: absolute" in phone
    setup = _CONSOLE_JS.split("function closeRailsOnPhone()", 1)[1][:300]
    assert "matchMedia('(max-width: 720px)')" in setup and "'lefthidden', 'righthidden'" in setup


def test_timestamps_are_shown_to_a_person_not_as_raw_iso() -> None:
    assert "function fmtWhen(iso)" in _CONSOLE_JS
    assert "esc(fmtWhen(status.as_of))" in _CONSOLE_JS
    assert "esc(fmtWhen(c.when))" in _CONSOLE_JS


# --- the advanced doors stay out of the setup path; no inline prose on the panels ----------

def test_replace_key_and_test_restore_live_in_one_collapsed_advanced_section() -> None:
    panel = _CONSOLE_JS.split("function renderKeyPanelHtml(s) {", 1)[1][:3200]
    adv = panel.split('<details class="adv-section">', 1)[1].split("</details>", 1)[0]
    assert "rotateKey()" in adv and "restoreDrillKey()" in adv and "key-rotate-confirm" in adv
    assert "<details class=\"adv-section\" open" not in _CONSOLE_JS  # closed by default
    # the one setup action (a recovery method) stays outside it
    assert "enrollRecoveryBrowser()" not in adv


def test_run_offload_now_lives_in_an_advanced_section() -> None:
    body = _CONSOLE_JS.split("async function renderSettingsSectionOffload() {", 1)[1][:1100]
    adv = body.split('<details class="adv-section">', 1)[1].split("</details>", 1)[0]
    assert "runOffloadTick()" in adv
    assert body.count("runOffloadTick()") == 1


def test_panel_descriptions_are_one_line_captions_with_the_detail_in_a_tooltip() -> None:
    for caption in ("The key that protects stored data.", "Every configuration setting.",
                    "Backfill repairs.",
                    "Copies for a drive or server that is only sometimes connected."):
        assert caption in _CONSOLE_JS, caption
    # the paragraphs they replaced
    for gone in ("Extra backup copies that are only sometimes connected",
                 "These are copies only. The main working data is never stored here.",
                 "Every configuration setting, in one place.",
                 "The eight backfill repair verbs. Dry run always writes nothing."):
        assert gone not in _CONSOLE_JS, gone


def test_recovery_checked_gets_the_same_copy_button_as_the_tpm_row() -> None:
    row = _CONSOLE_JS.split("function readinessStepRow(s) {", 1)[1][:1700]
    assert "act.command ||" in row and "readinessCopy(this)" in row


def test_phone_scrolls_wide_result_tables_and_clears_the_search_icon() -> None:
    css = (Path(__file__).parent.parent / "src" / "ui" / "static" / "osiris.css").read_text()
    phone = css.split("PHONE WIDTHS", 1)[1][:2200]
    assert ".r-table { display: block; max-width: 100%; overflow-x: auto; }" in phone
    assert ".search-compact-btn { margin-right: 8px; }" in phone
    assert ("body .r-table td, body .r-table th "
            "{ word-break: normal; overflow-wrap: break-word; }") in phone


def test_registry_tables_scroll_inside_their_own_box_when_wider_than_the_pane() -> None:
    body = _CONSOLE_JS.split("function renderSettingsPanelHtml(items) {", 1)[1][:2600]
    assert ('<div class="ee-form-scroll"><table class="ee-table ee-form">'
            '<thead><tr><th>Setting') in body

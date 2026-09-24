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
    body = _CONSOLE_JS.split("async function renderSettingsPane() {", 1)[1][:600]
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
    body = _CONSOLE_JS.split("async function renderSettingsPane() {", 1)[1][:800]
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


def test_stepper_every_action_kind_maps_to_a_jump_or_a_perform() -> None:
    body = _CONSOLE_JS.split("var READINESS_STEP_ACTIONS = {", 1)[1].split("};", 1)[0]
    for kind in ("init_key", "restart_services", "enroll_recovery", "encrypt_existing",
                 "init_restic", "configure_offload", "run_offload", "restore_drill"):
        assert kind + ":" in body, f"{kind} missing from READINESS_STEP_ACTIONS"
    assert body.count("kind: 'jump'") + body.count("kind: 'perform'") == 8


def test_stepper_one_shot_actions_post_to_the_real_routes() -> None:
    for fn, route in (
        ("readinessEncryptExisting", "/soul-key/encrypt-existing"),
        ("readinessRunOffload", "/offload-runner/tick"),
        ("readinessRestoreDrill", "/soul-key/restore-drill"),
    ):
        body = _CONSOLE_JS.split("async function " + fn + "(btn) {", 1)[1][:400]
        assert "fetch('" + route + "'" in body


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

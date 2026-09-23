"""THE KEY PANEL and THE OFFLOAD TARGETS PANEL, built off KEY CUSTODY REWRITTEN (gated
checks-only) and the deployed backup_settings offload_targets shape
(src/orchestrator/backup_settings.py's own `get_backup_settings`).

Piece 1 only: status card + init/rotate/restore-drill over the soul-key door, and
one row per offload target with live presence off GET /backup-settings. Browser-side
WebAuthn/PRF recovery enrollment (the key panel's own piece 2) is a separate later tip:
this panel never renders an enroll button, only a CLI pointer (no REST route, no MCP
tool, deliberately terminal-only). The /soul-key routes are not deployed yet
(checks-only gate): the panel degrades to a plain notice on a 404 rather than erroring,
since "not deployed yet" is the expected common case today.

Mirrors the repo's existing static-source-guard convention: string/substring proofs
against the served JS, no browser harness.
"""
from __future__ import annotations

from pathlib import Path

_CONSOLE_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()


# --- the key panel: status fetch degrades cleanly when the door isn't deployed --------

def test_key_panel_degrades_to_a_plain_notice_on_404_not_an_error() -> None:
    # THE SETTINGS PANE embeds this same fetch+render into more than
    # one container, so the shared implementation lives in renderKeyInto now, both the
    # standalone renderKeyPanel() and a pane's own embedded section call it.
    body = _CONSOLE_JS.split("async function renderKeyInto(containerId) {", 1)[1][:900]
    assert "res.status === 404" in body
    assert "Encryption key setup is not available in this deployment yet" in body
    assert "renderKeyInto(KEY_CONTAINER_ID)" in body  # "Check again" re-runs the same fetch
    assert "await renderKeyInto('result');" in _CONSOLE_JS.split(
        "async function renderKeyPanel() {", 1)[1][:300]


def test_key_panel_status_card_shows_backend_path_and_recovery_paths() -> None:
    body = _CONSOLE_JS.split("function renderKeyPanelHtml(s) {", 1)[1][:3000]
    assert "keyBackendLabel(s.backend)" in body
    assert "s.path" in body
    assert "keyAgeProse(s.created_age_seconds)" in body
    assert "s.recovery_paths_enrolled" in body


def test_key_panel_warns_when_recovery_has_at_most_one_path() -> None:
    body = _CONSOLE_JS.split("function renderKeyPanelHtml(s) {", 1)[1][:2000]
    assert "s.recovery_warning" in body
    # points at the CLI, never a browser button, per policy
    assert "osiris soul-key enroll-recovery" in body


def test_key_panel_flags_outstanding_legacy_plaintext_rows() -> None:
    body = _CONSOLE_JS.split("function renderKeyPanelHtml(s) {", 1)[1][:2000]
    assert "s.legacy_plaintext_rows != null && s.legacy_plaintext_rows > 0" in body


def test_key_panel_never_renders_a_browser_enrollment_button() -> None:
    # piece 2 (WebAuthn/PRF) is a separate, later, gated tip, this panel must not
    # invent an /soul-key/enroll-recovery or /soul-key/recover fetch call.
    assert "/soul-key/enroll-recovery" not in _CONSOLE_JS
    assert "/soul-key/recover'" not in _CONSOLE_JS
    assert "/soul-key/recover\"" not in _CONSOLE_JS


def test_init_key_posts_backend_only_no_invented_secret_reveal() -> None:
    # GUI PARITY added a confirm() gate before the fetch (restart:
    # true is consequential); window widened to clear it.
    body = _CONSOLE_JS.split("async function initKey() {", 1)[1][:1400]
    assert "'/soul-key/init'" in body
    assert "backend: backend" in body
    assert "renderKeyInto(KEY_CONTAINER_ID);" in body


def test_rotate_key_begins_without_finish_then_a_separate_finish_call() -> None:
    body = _CONSOLE_JS.split("async function rotateKey() {", 1)[1][:800]
    assert "'/soul-key/rotate'" in body
    assert "finish: false" in body
    finish_body = _CONSOLE_JS.split("async function finishKeyRotate() {", 1)[1][:700]
    assert "'/soul-key/rotate'" in finish_body
    assert "finish: true" in finish_body


def test_restore_drill_posts_and_reports_all_ok() -> None:
    body = _CONSOLE_JS.split("async function restoreDrillKey() {", 1)[1][:700]
    assert "'/soul-key/restore-drill'" in body
    assert "res.all_ok" in body


# --- the offload targets panel: rows read from the deployed backup-settings shape -----

def test_offload_panel_loads_rows_from_backup_settings() -> None:
    # THE SETTINGS PANE embeds this section too, the shared
    # fetch+render lives in renderOffloadInto(containerId) now.
    body = _CONSOLE_JS.split("async function renderOffloadInto(containerId) {", 1)[1][:700]
    assert "fetch('/backup-settings')" in body
    assert "settings.offload_targets" in body
    assert "await renderOffloadInto('result');" in _CONSOLE_JS.split(
        "async function renderOffloadPanel() {", 1)[1][:300]


def test_offload_presence_cell_never_probes_restic_reachability() -> None:
    # backup_validation.py's own repeated law: restic presence is always null, a shape
    # check only at write time, never a live network call from this read-only panel.
    body = _CONSOLE_JS.split("function offloadPresenceCell(t) {", 1)[1][:500]
    assert "t.kind !== 'local'" in body
    assert "Connection is not checked automatically" in body


def test_offload_presence_cell_shows_present_writable_and_free_space_for_local() -> None:
    body = _CONSOLE_JS.split("function offloadPresenceCell(t) {", 1)[1][:900]
    assert "p.present" in body
    assert "p.writable === false" in body
    assert "p.free_bytes" in body


def test_offload_row_disables_mountpoint_for_restic_kind() -> None:
    body = _CONSOLE_JS.split("function offloadRowHtml(t, i) {", 1)[1][:1500]
    assert "t.kind === 'local'" in body
    assert "disabled" in body


def test_offload_row_carries_a_remove_control() -> None:
    body = _CONSOLE_JS.split("function offloadRowHtml(t, i) {", 1)[1][:1500]
    assert "removeOffloadRow(" in body
    assert "Remove</button>" in body


def test_add_and_remove_offload_rows_sync_the_dom_first() -> None:
    assert "function addOffloadRow() {" in _CONSOLE_JS
    add_body = _CONSOLE_JS.split("function addOffloadRow() {", 1)[1][:300]
    assert "syncOffloadRowsFromDom();" in add_body
    remove_body = _CONSOLE_JS.split("function removeOffloadRow(i) {", 1)[1][:200]
    assert "syncOffloadRowsFromDom();" in remove_body


def test_save_offload_targets_is_a_single_full_array_replace_through_backup_settings() -> None:
    # no separate add/remove REST endpoint exists, one write door, the same one the
    # Settings panel's own backup section already uses.
    body = _CONSOLE_JS.split("async function saveOffloadTargets() {", 1)[1][:600]
    assert "fetch('/backup-settings'" in body
    assert "offload_targets: OFFLOAD_ROWS" in body
    assert "because: because" in body


def test_offload_panel_shows_vault_path_read_only_not_a_second_write_path() -> None:
    # vault_path's own editable field lives in Settings; this panel only displays it.
    body = _CONSOLE_JS.split("function renderOffloadPanelHtml() {", 1)[1][:1000]
    assert "OFFLOAD_VAULT" in body
    assert "vault_path: OFFLOAD_VAULT" not in body


# --- both panels reachable from CMD-K -------------------------------------------------

def test_both_panels_are_reachable_from_the_command_palette() -> None:
    # THE SETTINGS PANE consolidated the three formerly-separate
    # palette rows (Key…/Offload Targets…/Settings…) into ONE "Settings" entry, both
    # panels below are still reachable, now embedded as that pane's own sections
    # (see tests/test_settings_pane_ui.py for the full consolidation proof) rather
    # than each carrying its own standalone palette row.
    body = _CONSOLE_JS.split("const POWER_TOOLS = [", 1)[1][:2600]
    assert "label: 'Settings'," in body
    assert "run: () => renderSettingsPane()" in body
    assert "async function renderKeyPanel() {" in _CONSOLE_JS
    assert "async function renderOffloadPanel() {" in _CONSOLE_JS

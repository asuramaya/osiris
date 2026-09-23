"""GUI PARITY FOR THE KEY AND OFFLOAD LANES (thread dd11ab34, Thoth mail 13472) —
source-pin tests for the three frontend pieces: (1) a restic-credential status+Init
widget in the Backup & Offload section (over the new POST /restic-key/init), (2) a
"Run offload now" button over the new POST /offload-runner/tick, refreshing the
backup-status view with fresh receipts, (3) the Key panel's own Init now sends
restart: true and surfaces the door's own restart_hint.

Mirrors the repo's existing static-source-guard convention: string/substring proofs
against the served JS, no browser harness."""
from __future__ import annotations

from pathlib import Path

_CONSOLE_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()


# --- item 1: restic-key init, in Backup & Offload ---------------------------------------

def test_offload_section_loads_the_restic_credential_widget() -> None:
    body = _CONSOLE_JS.split(
        "async function renderSettingsSectionOffload() {", 1)[1][:900]
    assert "renderResticCredentialWidget()" in body
    assert "'settings-restic-credential'" in body


def test_restic_credential_widget_offers_init_only_when_absent() -> None:
    body = _CONSOLE_JS.split("function renderResticCredentialHtml(s) {", 1)[1][:900]
    assert "s.present ? ''" in body
    assert "initResticKey()" in body


def test_init_restic_key_reads_the_inline_backend_select_no_prompt() -> None:
    # PRODUCT VOICE (ruling 1e2ef5c3): an inline <select> next to the button, never a
    # browser prompt() dialog.
    body = _CONSOLE_JS.split("async function initResticKey() {", 1)[1][:600]
    assert "$('restic-init-backend')" in body
    assert "prompt(" not in body
    assert "'/restic-key/init'" in body
    assert "renderResticCredentialWidget();" in body


def test_restic_credential_never_shows_the_password_itself() -> None:
    # restic_key_status/init never return the password bytes server-side; the
    # widget must not invent a field to display one either.
    body = _CONSOLE_JS.split("function renderResticCredentialHtml(s) {", 1)[1][:400]
    assert "s.password" not in body


# --- item 2: on-demand offload tick ----------------------------------------------------

def test_offload_section_has_a_run_offload_now_button() -> None:
    body = _CONSOLE_JS.split(
        "async function renderSettingsSectionOffload() {", 1)[1][:700]
    assert "onclick=\"runOffloadTick()\">Run offload now</button>" in body


def test_run_offload_tick_posts_the_new_route_and_refreshes_receipts() -> None:
    body = _CONSOLE_JS.split("async function runOffloadTick() {", 1)[1][:900]
    assert "'/offload-runner/tick'" in body
    assert "renderBackupStatusSection()" in body


def test_run_offload_tick_reports_how_many_targets_succeeded() -> None:
    body = _CONSOLE_JS.split("async function runOffloadTick() {", 1)[1][:900]
    assert "(res.targets || []).filter(function(t) { return t.ok; }).length" in body


def test_backup_status_fetch_is_its_own_reusable_function() -> None:
    # both the section's own initial load and runOffloadTick's own refresh call the
    # SAME fetch+render, never a duplicated copy that could drift.
    assert "async function renderBackupStatusSection() {" in _CONSOLE_JS
    body = _CONSOLE_JS.split(
        "async function renderSettingsSectionOffload() {", 1)[1][:700]
    assert "renderBackupStatusSection()" in body


# --- item 3: soul-key init restarts the daemons ----------------------------------------

def test_init_key_sends_the_inline_restart_checkbox_state() -> None:
    # PRODUCT VOICE (ruling 1e2ef5c3): an inline checkbox (default checked, rendered
    # in renderKeyPanelHtml's own "not present" actions block) replaces the old
    # confirm() dialog -- initKey reads its state directly, never a browser prompt.
    body = _CONSOLE_JS.split("async function initKey() {", 1)[1][:1100]
    assert "$('key-init-restart')" in body
    assert "restart: restart" in body
    assert "confirm(" not in body


def test_init_key_surfaces_the_restart_hint() -> None:
    body = _CONSOLE_JS.split("async function initKey() {", 1)[1][:1400]
    assert "res.restart_hint" in body


def test_key_panel_offers_an_inline_restart_checkbox_when_not_present() -> None:
    body = _CONSOLE_JS.split("function renderKeyPanelHtml(s) {", 1)[1][:3000]
    assert "id=\"key-init-restart\" checked" in body
    assert "Restart the services now" in body

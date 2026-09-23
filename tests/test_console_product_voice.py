"""PRODUCT VOICE, THE CONSOLE (operator ruling 1e2ef5c3, Thoth mail 13501): "this is
public facing and it has to be treated as a product not a single owner project" --
every user-facing string in the console reads as a product written for a stranger.
BANNED: fleet agent/seat names, ruling/decision/thread id citations and wave/gate
numbers in blurbs, em dashes, and in-session coinages ("the box", "hot path", "the
ladder", "receipt" for a result, "door" for a REST/CLI endpoint). Provenance
citations stay in code comments and commit messages (explicitly out of this ruling's
scope) -- these tests only ever look at STRING CONTENT, never comments.

Scope: THE SETTINGS PANE (src/ui/static/console.js, the reviewed surface the ruling's
own specimen came from -- "over Khnum's soul-key door" in the Key blurb) plus a sweep
of the same three shared static files (console.js, osiris.js, space.js) for em dashes
and banned coinages outside comments. A full line-by-line rewrite of every string in
every surface (Repairs panel's own deep ontology vocabulary, the mailbox/composer
internals, etc.) is a larger undertaking than this tip covers -- reported as a scope
boundary, not claimed as done.

Also item 2 (product form, no browser dialogs): the Settings pane -- Key, Backup &
Offload, Registry sections -- never opens a window.prompt()/confirm() dialog; every
required input (a backend choice, a reason for a change, a destructive-action
confirmation) is an inline form control instead."""
from __future__ import annotations

import re
from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_CONSOLE_JS = (_STATIC / "console.js").read_text()
_OSIRIS_JS = (_STATIC / "osiris.js").read_text()
_SPACE_JS = (_STATIC / "space.js").read_text()

_AGENT_NAMES = ("Khnum", "Thoth", "Sekhmet", "Seshat", "Imhotep")
_CODE_COMMENT_PREFIXES = ("//", "*", "/*", "<!--")


def _non_comment_lines(text: str) -> list[str]:
    """Every line whose OWN code portion (before a trailing `//`, and never a line
    that opens as a comment) still has real content -- the same heuristic used to
    find the actual reported specimens before writing this file, verified by hand
    against the real diff rather than assumed correct."""
    out = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(_CODE_COMMENT_PREFIXES):
            continue
        code_part = line.split("//", 1)[0] if "//" in line else line
        if code_part.strip():
            out.append(code_part)
    return out


# --- the reported specimen itself: the Key blurb never names an agent again -----------

def test_key_blurb_never_names_an_agent_or_cites_a_door() -> None:
    body = _CONSOLE_JS.split("function renderKeyPanelHtml(s) {", 1)[1][:2000]
    for name in _AGENT_NAMES:
        assert name not in body
    assert "soul-key door" not in body


# --- no agent name, ruling citation, or wave/gate number in real console.js strings ---

def test_no_agent_name_in_console_js_string_content() -> None:
    for line in _non_comment_lines(_CONSOLE_JS):
        for name in _AGENT_NAMES:
            assert name not in line, f"agent name leaked into a string: {line.strip()[:120]!r}"


def test_no_ruling_or_wave_citation_in_console_js_string_content() -> None:
    pat = re.compile(r"\bruling [0-9a-f]{8}\b|\bwave \d+\b|\bgate w\d+\b", re.I)
    for line in _non_comment_lines(_CONSOLE_JS):
        assert not pat.search(line), f"a citation leaked into a string: {line.strip()[:120]!r}"


# --- no em dash outside a comment, in any of the three shared static files ------------

def test_no_em_dash_outside_comments_in_console_js() -> None:
    for line in _non_comment_lines(_CONSOLE_JS):
        assert "—" not in line, f"em dash in a string: {line.strip()[:120]!r}"


def test_no_em_dash_outside_comments_in_osiris_js() -> None:
    for line in _non_comment_lines(_OSIRIS_JS):
        assert "—" not in line, f"em dash in a string: {line.strip()[:120]!r}"


def test_no_em_dash_outside_comments_in_space_js() -> None:
    for line in _non_comment_lines(_SPACE_JS):
        assert "—" not in line, f"em dash in a string: {line.strip()[:120]!r}"


# --- no banned in-session coinage in console.js string content ------------------------

def test_no_banned_coinage_in_console_js_string_content() -> None:
    pat = re.compile(
        r"\bthe door\b|\bhot path\b|\bthe ladder\b|\breceipt\b|\ba body\b|\bthe body\b", re.I)
    for line in _non_comment_lines(_CONSOLE_JS):
        assert not pat.search(line), f"a coinage leaked into a string: {line.strip()[:120]!r}"


def test_the_box_was_renamed_readiness() -> None:
    assert "settingsSectionShell('box', 'Readiness')" in _CONSOLE_JS
    assert "settingsSectionShell('box', 'The Box')" not in _CONSOLE_JS


# --- item 2: the Settings pane never opens a browser dialog ---------------------------

def test_settings_pane_never_calls_prompt_or_confirm() -> None:
    body = _CONSOLE_JS.split("// ── The Key Panel", 1)[1].split("// ── Projects", 1)[0]
    assert not re.search(r"[^.]\b(prompt|confirm)\(", body), \
        "a window.prompt()/confirm() dialog remains in the Settings pane"


def test_key_init_uses_an_inline_backend_select_and_restart_checkbox() -> None:
    body = _CONSOLE_JS.split("function renderKeyPanelHtml(s) {", 1)[1][:2000]
    assert 'id="key-init-backend"' in body
    assert 'id="key-init-restart" checked' in body


def test_offload_save_uses_an_inline_reason_field_disabled_until_filled() -> None:
    body = _CONSOLE_JS.split("function renderOffloadPanelHtml() {", 1)[1][:1500]
    assert 'id="offload-reason"' in body
    assert "offload-save-btn" in body
    assert 'id="offload-save-btn" disabled' in body


def test_registry_high_consequence_save_uses_an_inline_confirm_checkbox() -> None:
    body = _CONSOLE_JS.split("function settingsActionCell(it)", 1)[1][:900]
    assert "needsConfirm" in body
    assert 'id="setting-confirm-' in body


def test_registry_secret_replace_uses_an_inline_password_field() -> None:
    body = _CONSOLE_JS.split("function settingsFieldInput(item) {", 1)[1][:700]
    assert "type=\"password\"" in body


# --- item 3: layout order -- Readiness moved up near the top --------------------------

def test_section_order_is_key_offload_readiness_registry_desk() -> None:
    body = _CONSOLE_JS.split("async function renderSettingsPane() {", 1)[1][:600]
    key_at = body.index("settingsSectionShell('key'")
    offload_at = body.index("settingsSectionShell('offload'")
    box_at = body.index("settingsSectionShell('box'")
    registry_at = body.index("settingsSectionShell('registry'")
    desk_at = body.index("settingsSectionShell('desk'")
    assert key_at < offload_at < box_at < registry_at < desk_at

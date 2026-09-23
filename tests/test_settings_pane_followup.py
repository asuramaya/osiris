"""THE SETTINGS PANE FOLLOW-UP (thread ea9aedba, Thoth mail 13404) — three small fixes
from Thoth's own live Chrome review of the deployed pane (decision abfc47ac):

  1. The Backup & Offload target-editor's own <input>/<select> elements had no explicit
     background — the theme's near-white text on the browser's plain white default made
     a bound, real value ("nas") read as an empty field. Same gap existed in the Registry
     section's own field inputs (not reported live, but the identical underlying cause —
     button/select/input/textarea's global rule sets `color`, never `background`), so the
     fix is scoped to every input/select an .ee-table renders, not just the offload ones.
  2. The recovery-path warning sentence dangled ("...in your terminal, or</div>") whenever
     no browser-enroll button followed it (recovery_paths_enrolled.length === 1, not 0) —
     the trailing ", or" had nothing after it.
  3. space.js: positionLabels' own `_placed`/`overlapsPlaced` were declared as a `const`/
     function right next to positionLabels itself, far below markDirty/forceRender — both
     of which can reach positionLabels (forceRender directly, markDirty via its
     requestAnimationFrame(renderIfDirty) chain). An early call hit the temporal dead zone
     ("Cannot access '_placed' before initialization"). Hoisted above every reachable path.

Mirrors the repo's existing static-source-guard convention: string/substring proofs
against the served JS/CSS, no browser harness."""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_CONSOLE_JS = (_STATIC / "console.js").read_text()
_SPACE_JS = (_STATIC / "space.js").read_text()
_OSIRIS_CSS = (_STATIC / "osiris.css").read_text()


# --- fix 1: ee-table inputs/selects get real, theme-consistent styling ---------------

def test_ee_table_inputs_and_selects_get_an_explicit_background() -> None:
    body = _OSIRIS_CSS.split('.ee-table input:not([type="checkbox"]), .ee-table select {',
                             1)[1][:200]
    assert "background: var(--panel2)" in body
    assert "color: var(--text)" in body


def test_ee_table_input_focus_state_matches_the_filter_class_convention() -> None:
    assert '.ee-table input:not([type="checkbox"]):focus, .ee-table select:focus {' in _OSIRIS_CSS
    body = _OSIRIS_CSS.split(
        '.ee-table input:not([type="checkbox"]):focus, .ee-table select:focus {', 1)[1][:100]
    assert "border-color: var(--border-focus)" in body


def test_checkboxes_are_excluded_never_get_a_background_swap() -> None:
    # a checkbox's own native appearance is the readable one; the rule must not touch it.
    assert '.ee-table input:not([type="checkbox"])' in _OSIRIS_CSS


# --- fix 2: the recovery-path warning sentence never dangles --------------------------

def test_recovery_warning_never_ends_with_a_dangling_or() -> None:
    body = _CONSOLE_JS.split("function renderKeyPanelHtml(s) {", 1)[1][:1200]
    assert "(enrollBtn ? ', or' + enrollBtn : '.')" in body
    # the old, dangling construction is gone
    assert "in your terminal, or' + enrollBtn +" not in body


def test_recovery_warning_ends_with_a_period_when_no_enroll_button_applies() -> None:
    # recovery_paths_enrolled.length === 1 (not 0): the warning still shows (soul_key_
    # status's own law: warn whenever <=1 enrolled) but enrollBtn is deliberately empty
    # (the server refuses a second enrollment) -- the sentence must still read cleanly.
    body = _CONSOLE_JS.split("function renderKeyPanelHtml(s) {", 1)[1][:1200]
    assert "in a terminal'" in body
    assert "enrollBtn ? ', or' + enrollBtn : '.')" in body


# --- fix 3: space.js hoisting closes the TDZ race ---------------------------------------

def test_placed_and_overlaps_placed_are_declared_before_mark_dirty() -> None:
    placed_at = _SPACE_JS.index("const _placed = []")
    mark_dirty_at = _SPACE_JS.index("function markDirty()")
    overlaps_at = _SPACE_JS.index("function overlapsPlaced(")
    assert placed_at < mark_dirty_at
    assert overlaps_at < mark_dirty_at


def test_placed_is_declared_before_every_path_that_can_reach_position_labels() -> None:
    placed_at = _SPACE_JS.index("const _placed = []")
    force_render_at = _SPACE_JS.index(
        "forceRender: () => { renderScene(); positionLabels(); }")
    window_space_at = _SPACE_JS.index("window.__space = api;")
    assert placed_at < force_render_at < window_space_at


def test_position_labels_own_body_is_unchanged_by_the_hoist() -> None:
    # the hoist only MOVED the declaration; positionLabels' own logic (and every other
    # test file's own split-on-"function positionLabels()" anchor) must still work.
    body = _SPACE_JS.split("function positionLabels() {", 1)[1][:200]
    assert "_placed.length = 0;" in body


# --- the same TDZ class hit labeledNodes too (live console finding, a real console
# exception on first render: "Cannot access 'labeledNodes' before initialization" at
# positionLabels, reached via renderIfDirty before pickLabels' own scope had run) --------

def test_labeled_nodes_and_friends_are_declared_before_mark_dirty() -> None:
    labeled_at = _SPACE_JS.index("let labeledNodes = []")
    mark_dirty_at = _SPACE_JS.index("function markDirty()")
    assert labeled_at < mark_dirty_at


def test_labeled_nodes_is_declared_before_every_path_that_can_reach_position_labels() -> None:
    labeled_at = _SPACE_JS.index("let labeledNodes = []")
    force_render_at = _SPACE_JS.index(
        "forceRender: () => { renderScene(); positionLabels(); }")
    window_space_at = _SPACE_JS.index("window.__space = api;")
    assert labeled_at < force_render_at < window_space_at


def test_pick_labels_own_body_is_unchanged_by_the_labeled_nodes_hoist() -> None:
    body = _SPACE_JS.split("function pickLabels() {", 1)[1][:2500]
    assert "labeledNodes = pool.concat(projectPool, communityPool)" in body
    assert ".slice(0, N_LABELS);" in body

"""THE LEGIBILITY PASS, TIP 1c (Thoth mail 10891, off w276's own deploy e432672). Thoth's
own live review of w276 accepted click=focus, hide, ego row, fit, and pick tolerance, but
found six new issues:

(1) the inspector stayed empty after a focus -- objectDetail() throws synchronously on a
malformed/error response body, and since the rail assignment only happens after a
successful call, a throw silently left the rail showing whatever it had BEFORE the click.
(2) Escape did not clear the focus -- it stepped back one breadcrumb instead (a leftover
from before the path-lens focus feature existed), never actually calling clearFocus().
(3) the omnibox's own client-side agent-handle fallback (tip 1b, review flaw #7) never
fired on the deployed page -- it gated on window.OsirisSpace, which is undefined until
initSpace's own promise resolves.
(4) the canvas controls still sat over the drawer on a no-cache load -- #main is a CSS grid
with no grid-template-rows, so its single implicit row sizes to its own content (a classic
grid gotcha) instead of the container's own height:100%, letting #cy grow past the real
viewport (measured: 1409px in a 1198px viewport).
(5) the header's own entity count read a stale global/filtered number during a focus
instead of the actual reachable set.
(6) a tiny reachable set (2-3 nodes) could fill the frame with 48px-capped discs -- the ego
layout's own spacing tracked whatever zoom the camera was already at (a scale-instability
feedback loop for small foci), and the fit's own floor was too low regardless.

Mirrors the existing static-source-guard convention; no browser test harness exists in this
repo. Live verification (before/after screenshots) is reported separately on thread
71c4ca0d once a Chrome connection is available.
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_SPACE_JS = (_STATIC / "space.js").read_text()
_CONSOLE_JS = (_STATIC / "console.js").read_text()
_OSIRIS_CSS = (_STATIC / "osiris.css").read_text()


# --- (1) the inspector always writes SOMETHING to the rail, success or failure -----------

def test_inspect_checks_the_response_and_never_silently_stays_stale() -> None:
    body = _SPACE_JS.split("async function inspect(id)", 1)[1][:1000]
    assert "if (!res.ok) throw new Error(" in body
    assert "} catch (err) {" in body
    body2 = _SPACE_JS.split("async function inspect(id)", 1)[1][:1400]
    assert 'rightRail.innerHTML = `<div class="insp-empty">Could not load' in body2
    assert "return;" in body2


def test_load_rels_failure_never_blanks_an_already_populated_rail() -> None:
    body = _SPACE_JS.split("async function inspect(id)", 1)[1][:2300]
    assert "await Osiris.loadRels(relsEl, id, (pickId) => focusObject(pickId), () => {}, obj);" \
        in body
    assert 'console.error("loadRels() failed for", id, err);' in body


# --- (2) Escape clears the focus outright -------------------------------------------------

def test_escape_clears_focus_stepbackbreadcrumb_is_gone() -> None:
    assert "function stepBackBreadcrumb()" not in _CONSOLE_JS
    body = _CONSOLE_JS.split("if (!hadDropdown && !hadPeek", 1)[1][:150]
    assert "window.OsirisSpace.clearFocus();" in body


# --- (3) the omnibox fallback awaits space readiness, not just a truthy global check ------

def test_omnibox_agent_fallback_awaits_space_readiness() -> None:
    # TIP 3b (Thoth mail 10953) replaced the `hits.length === 0`-gated fallback this test
    # used to assert with an unconditional client-side scan run inside a single Promise.all
    # alongside the server search -- see test_legibility_pass_tip3b.py for the full rewrite;
    # this still confirms the readiness promise is part of that combined wait.
    body = _CONSOLE_JS.split("OMNI_SEARCH_TIMER = setTimeout(async () => {", 1)[1][:1200]
    assert "window.OsirisSpace ? Promise.resolve(window.OsirisSpace) : " \
        "(window.__spaceReady || Promise.resolve(null))" in body


# --- (4) #main's grid row is pinned to 100%, breaking the content-driven growth ----------

def test_main_grid_row_is_pinned_to_full_height() -> None:
    body = _OSIRIS_CSS.split("#main {", 1)[1][:700]
    assert "grid-template-rows: 100%;" in body


def test_left_and_right_rails_get_min_height_zero_too() -> None:
    left_body = _OSIRIS_CSS.split("#left {\n  border-right:", 1)[1][:300]
    assert "min-height: 0;" in left_body
    right_body = _OSIRIS_CSS.split("#right {\n  border-left:", 1)[1][:200]
    assert "min-height: 0;" in right_body


# --- (5) the header entity count tracks the active focus lens ----------------------------

def test_header_count_follows_the_focus_lens() -> None:
    body = _CONSOLE_JS.split("function renderEntityToolbar()", 1)[1][:1800]
    assert "const focused = space && space.pathFocusId;" in body
    assert "const trueTotal = focused ? space.pathReachable.size" in body


def test_on_space_focus_repaints_the_toolbar_too() -> None:
    body = _CONSOLE_JS.split("function onSpaceFocus(id)", 1)[1][:250]
    assert "renderEntityToolbar();" in body


# --- (6) a stable ego-layout scale plus a real minimum fit view size ---------------------

def test_ego_layout_uses_the_stable_whole_graph_scale_not_the_transient_zoom() -> None:
    body = _SPACE_JS.split(
        "function applyEgoLayout(focusId, hopsUp, hopsDown, extraSeed)", 1)[1][:900]
    assert "const wpp = maxViewSize / wrap.clientHeight;" in body


def test_ego_fit_has_a_real_minimum_view_size() -> None:
    body = _SPACE_JS.split("function renderFocusEgoGroups(id, hopsUp, hopsDown)", 1)[1][:2000]
    assert "const EGO_FIT_MIN_VIEWSIZE = 400;" in body
    assert "viewSize = Math.max(EGO_FIT_MIN_VIEWSIZE, " \
        "Math.min(maxViewSize, span * 1.6 + 40));" in body

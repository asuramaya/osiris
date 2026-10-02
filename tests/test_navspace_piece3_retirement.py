"""NAVIGABLE SPACE, PIECE 3: RETIRE (once
THE READING LAYER, parts A/B/C landed). At parity with the space.js renderer, cytoscape and
its neighborhood-graph board are retired outright: the vendored cytoscape/fcose/cose-base/
layout-base files, osiris.js's own makeBoard (~400 lines -- the whole cytoscape board
implementation, the first board build), console.js's ensureBoard()/board var/
#cy-legacy mount, the Board kanban projection (renderBoardProjection/Lane/Card,
BOARD_GROUP_BY), the whole-graph LOD toggle (GRAPH_MODE, already gone since the INTEGRATION
piece), and #viewsw/the old view-tab switcher markup. This is a REMOVAL, not a rewrite --
one reversible commit, as planned -- so these tests prove absence, mirroring
the repo's existing static-source-guard convention.

test_graph_visualizer_wave_a.py (WAVE A's own readability proofs against makeBoard) is
retired outright alongside the code it tested -- its still-relevant pieces (breadcrumbs,
the edge-count badge, focus() never touching a board) move here rather than being lost.
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_VENDOR = _STATIC / "vendor"
_OSIRIS_JS = (_STATIC / "osiris.js").read_text()
_CONSOLE_JS = (_STATIC / "console.js").read_text()
_INDEX_HTML = (_STATIC / "index.html").read_text()
_OSIRIS_CSS = (_STATIC / "osiris.css").read_text()


# --- the vendored library itself is gone, not just unreferenced ---------------------------

def test_cytoscape_and_fcose_are_no_longer_vendored() -> None:
    for name in ("cytoscape.min.js", "cytoscape-fcose.js", "cose-base.js", "layout-base.js"):
        assert not (_VENDOR / name).exists(), f"{name} should have been deleted"


def test_index_html_no_longer_loads_the_cytoscape_scripts() -> None:
    for needle in ("vendor/cytoscape", "vendor/fcose", "vendor/cose-base", "vendor/layout-base"):
        assert needle not in _INDEX_HTML


# --- makeBoard itself -- the ~400-line cytoscape board implementation -- is deleted -------

def test_osiris_js_no_longer_defines_make_board() -> None:
    assert "function makeBoard(" not in _OSIRIS_JS
    assert "makeBoard" not in _OSIRIS_JS


def test_osiris_js_export_list_drops_make_board() -> None:
    body = _OSIRIS_JS.split("return { $, esc,", 1)[1][:200]
    assert "makeBoard" not in body


# --- console.js's own board plumbing (ensureBoard, #cy-legacy, Board kanban) is gone -------

def test_console_js_no_longer_mounts_a_legacy_board() -> None:
    for needle in ("ensureBoard", "var board", "cy-legacy", "showActionMenu", "closeActionMenu"):
        assert needle not in _CONSOLE_JS


def test_board_kanban_rendering_is_gone_not_just_unreachable() -> None:
    for needle in ("function renderBoardProjection", "function renderBoardLane",
                   "function renderBoardCard", "BOARD_GROUP_BY"):
        assert needle not in _CONSOLE_JS


def test_index_html_drops_the_legacy_mount_and_old_switcher() -> None:
    assert 'id="cy-legacy"' not in _INDEX_HTML
    assert 'id="viewsw"' not in _INDEX_HTML


def test_whole_graph_lod_toggle_is_gone() -> None:
    # already retired in the earlier INTEGRATION piece -- re-confirmed here as
    # part of piece 3's own explicit scope.
    for needle in ("GRAPH_MODE", "toggleGraphMode", "graph-mode-btn", "graph-zoomout-btn",
                   "graph-relayout-btn", "expandFocusOneHop", "collapseFocusOneHop"):
        assert needle not in _CONSOLE_JS
        assert needle not in _INDEX_HTML


def test_dead_viewswitcher_css_is_gone_too() -> None:
    for needle in (".viewsw-tabs", ".viewsw button", ".stage-viewsw-footer"):
        assert needle not in _OSIRIS_CSS


# --- still-alive concerns from the retired WAVE A file, carried forward -------------------

def test_breadcrumbs_are_still_wired_in_console_js() -> None:
    # stepBackBreadcrumb is gone (review flaw) -- Escape now
    # clears the focus outright instead of stepping back one crumb, per the amendment's own
    # ruling ("Escape clears, Back walks the stack"); the function had no other caller left.
    assert "function pushBreadcrumb(id, label)" in _CONSOLE_JS
    assert "function jumpToBreadcrumb(i)" in _CONSOLE_JS
    assert "function stepBackBreadcrumb()" not in _CONSOLE_JS


def test_escape_clears_the_focus_when_nothing_more_local_consumed_it() -> None:
    # TIP 1c review flaw #2: Escape used to step back a breadcrumb instead of actually
    # clearing anything -- a real state divergence from the amendment's own ruling.
    body = _CONSOLE_JS.split("if (!hadDropdown && !hadPeek", 1)[1][:120]
    assert "window.OsirisSpace.clearFocus();" in body


def test_breadcrumb_markup_still_exists() -> None:
    # the graph's OWN in-canvas search box (id="graph-search") is gone as of THE LEGIBILITY
    # PASS: superseded by the header omnibox, not
    # migrated. Breadcrumbs are untouched by that tip.
    assert 'id="graph-search"' not in _INDEX_HTML
    assert 'id="graph-breadcrumbs"' in _INDEX_HTML


def test_edge_count_badge_still_renders_on_the_table_row() -> None:
    # was "table row AND board card" before piece 3 -- the board card is gone, the table
    # row's own badge survives untouched.
    assert "function edgeCountBadge(id)" in _CONSOLE_JS
    assert _CONSOLE_JS.count("edgeCountBadge(o.id)") == 1


def test_focus_still_never_touches_a_board() -> None:
    body = _CONSOLE_JS.split("async function focus(id, fromBreadcrumb)", 1)[1].split(
        "\n}\n", 1)[0]
    assert "ensureBoard" not in body
    assert "space.focusObject(id)" in body

"""NAVIGABLE SPACE, INTEGRATION:
the three.js renderer (space.js) replaces the #cy cytoscape container in /ui/'s own browse
stage -- no separate page, graph and table coexist (the canvas fills the section, the table
sits in a collapsible drawer beneath it), Board is dropped from the switcher (its code stays
for piece 3's removal alongside the retired cytoscape mount). This supersedes, rather than
extends, test_graph_visualizer_fold.py's own fold/whole-graph-LOD proofs -- that toggle
lived inside the exact #cy content this integration replaces. Mirrors the existing
static-source-guard convention: no browser test harness exists in this repo, so these are
string-presence proofs against the served JS/HTML, not DOM assertions.
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_INDEX_HTML = (_STATIC / "index.html").read_text()
_CONSOLE_JS = (_STATIC / "console.js").read_text()
_SPACE_JS = (_STATIC / "space.js").read_text()


# --- the canvas replaces #cy's old cytoscape content ---------------------------------------

def test_cy_hosts_the_space_canvas_not_cytoscape_markup() -> None:
    cy_block = _INDEX_HTML.split('<div id="cy">', 1)[1].split("</div>\n      <div", 1)[0]
    assert 'id="canvas-wrap"' in cy_block
    assert 'id="labels"' in cy_block
    assert 'id="fit-btn"' in cy_block
    assert 'id="up-btn"' in cy_block
    assert "cytoscape" not in cy_block.lower()


def test_index_mounts_space_via_a_module_script() -> None:
    assert 'import { initSpace } from "/ui/space.js";' in _INDEX_HTML
    assert "window.__spaceReady" in _INDEX_HTML
    assert "window.OsirisSpace" in _INDEX_HTML


def test_the_legacy_cytoscape_board_is_gone_for_real_now() -> None:
    # piece 3: the hidden #cy-legacy mount
    # this integration piece deliberately kept ensureBoard()/Osiris.makeBoard on is now
    # actually deleted, not just unreachable from the UI -- see
    # tests/test_navspace_piece3_retirement.py for the fuller retirement proof.
    assert 'id="cy-legacy"' not in _INDEX_HTML
    assert "ensureBoard" not in _CONSOLE_JS
    assert "makeBoard" not in _CONSOLE_JS


# --- graph and table coexist: a collapsible drawer, never a hard Table/Graph switch --------

def test_table_lives_in_a_collapsible_drawer_beside_the_canvas() -> None:
    assert 'id="browse-drawer"' in _INDEX_HTML
    assert 'id="browse-table"' in _INDEX_HTML
    assert 'onclick="toggleTableDrawer()"' in _INDEX_HTML


def test_drawer_defaults_closed_so_the_canvas_fills_the_section() -> None:
    assert "let TABLE_DRAWER_OPEN = false;" in _CONSOLE_JS


def test_switching_into_browse_always_reveals_the_canvas() -> None:
    body = _CONSOLE_JS.split("async function switchSurface(surface)", 1)[1].split(
        "\n}\n", 1)[0]
    assert "showBoard();" in body


def test_board_kanban_switcher_is_gone_from_the_ui() -> None:
    assert 'setEntityView(\'board\')' not in _CONSOLE_JS
    assert 'onclick="setEntityView' not in _INDEX_HTML


# --- selection is shared between the canvas and the drawer's table -------------------------

def test_space_onfocus_hook_repaints_the_table_selection() -> None:
    assert "function onSpaceFocus(id)" in _CONSOLE_JS
    assert "window.onSpaceFocus = onSpaceFocus;" in _CONSOLE_JS
    assert "onFocus: (id) => { if (window.onSpaceFocus) window.onSpaceFocus(id); }" in _INDEX_HTML


def test_a_table_row_click_highlights_the_canvas_too() -> None:
    # THE READING LAYER, part B/C built select-vs-focus for a table row click; THE
    # A later legibility amendment retired that split outright -- a
    # row click always focuses now, still sharing selection with the canvas either way.
    body = _CONSOLE_JS.split("function inspectOnly(id)", 1)[1].split("\n}\n", 1)[0]
    assert "if (space) space.focusObject(id);" in body


def test_search_around_and_omni_picks_delegate_to_the_mounted_space_instance() -> None:
    body = _CONSOLE_JS.split("async function focus(id, fromBreadcrumb)", 1)[1].split(
        "\n}\n", 1)[0]
    assert "space.focusObject(id)" in body
    assert "window.OsirisSpace" in body


# --- the wire contract: GET /graph/stream, decoded client-side, no more tiling -------------

def test_space_decodes_the_graph_stream_binary_header() -> None:
    assert 'fetch("/graph/stream" + (fresh ? "?fresh=1" : ""))' in _SPACE_JS
    assert "getUint32(0, true)" in _SPACE_JS  # 4-byte LE header length, per graph_stream.py
    assert "/objects/viewport" not in _SPACE_JS  # the old tiled loader is fully retired


def test_space_consumes_the_deltas_sse_stream() -> None:
    # resumed from the snapshot's own watermark (the snapshot may come from the server's cache)
    assert '"/graph/stream/deltas" + (watermark != null ? `?since=${watermark}` : "")' in _SPACE_JS
    assert '"retired"' in _SPACE_JS and '"moved"' in _SPACE_JS

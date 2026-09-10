"""THE GRAPH VISUALIZER, WAVE B (operator dispatch, wave 15, thread 8839): the whole-graph
system -- server-side layout (item 1) and the viewport/LOD endpoints (items 2/3) are
covered directly against a real DB in tests/test_graph_layout.py and tests/test_api.py.
This file covers item 4 (the sigma.js/graphology full-view renderer, vendored beside
cytoscape) the same static-source-guard way test_console_js_routes.py already established
for this repo's JS -- no browser test harness exists here.
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_OSIRIS_JS = (_STATIC / "osiris.js").read_text()
_CONSOLE_JS = (_STATIC / "console.js").read_text()
_INDEX_HTML = (_STATIC / "index.html").read_text()


def test_sigma_and_graphology_are_vendored_locally() -> None:
    sigma = _STATIC / "vendor" / "sigma.min.js"
    graphology = _STATIC / "vendor" / "graphology.umd.min.js"
    assert sigma.exists() and sigma.stat().st_size > 1000
    assert graphology.exists() and graphology.stat().st_size > 1000


def test_vendor_scripts_load_before_the_atlas_can_be_built() -> None:
    g_idx = _INDEX_HTML.index("graphology.umd.min.js")
    s_idx = _INDEX_HTML.index("sigma.min.js")
    osiris_idx = _INDEX_HTML.index('src="/ui/osiris.js"')
    assert g_idx < osiris_idx and s_idx < osiris_idx


def test_make_atlas_is_exported_from_osiris_js() -> None:
    assert "function makeAtlas(container, onDrillDown)" in _OSIRIS_JS
    assert "makeAtlas," in _OSIRIS_JS  # present in the module's own public return


def test_atlas_never_computes_layout_client_side() -> None:
    """The whole point of wave B item 1's own heartbeat: the atlas reads x/y straight off
    the server response (graph_supernodes/graph_clusters/viewport), never calling a force
    layout of its own the way makeBoard's cytoscape/fcose does."""
    atlas_start = _OSIRIS_JS.index("function makeAtlas(")
    atlas_end = _OSIRIS_JS.index("// ---- THE GENERIC RENDERER")
    atlas_body = _OSIRIS_JS[atlas_start:atlas_end]
    assert "fcose" not in atlas_body
    assert ".layout(" not in atlas_body
    assert "x: s.x, y: s.y" in atlas_body  # supernode positions ride straight from the server


def test_atlas_three_levels_drill_down_and_zoom_out() -> None:
    assert "async function loadSupernodes()" in _OSIRIS_JS
    assert "async function loadClusters(projectId, projectLabel)" in _OSIRIS_JS
    assert "async function loadNodesNear(x, y, span)" in _OSIRIS_JS
    assert "zoomOut()" in _OSIRIS_JS


def test_atlas_orphan_counts_render_on_every_level() -> None:
    assert "function labelWithOrphans(base, orphans)" in _OSIRIS_JS
    assert _OSIRIS_JS.count("labelWithOrphans(") >= 3  # defined once, called for both levels


def test_atlas_reads_endpoints_from_waves_b_items_1_through_3() -> None:
    assert '"/graph/supernodes"' in _OSIRIS_JS
    assert "/graph/clusters?project=" in _OSIRIS_JS
    assert '"/objects/viewport?"' in _OSIRIS_JS


def test_console_js_wires_the_atlas_surface() -> None:
    assert "function ensureAtlas()" in _CONSOLE_JS
    assert "Osiris.makeAtlas($(\"sigma-atlas\")" in _CONSOLE_JS
    assert "surface === 'atlas'" in _CONSOLE_JS
    assert "function atlasZoomOut()" in _CONSOLE_JS


def test_atlas_markup_and_nav_item_exist() -> None:
    assert 'id="sigma-atlas"' in _INDEX_HTML
    assert 'data-surface="atlas"' in _INDEX_HTML
    assert 'onclick="atlasZoomOut()"' in _INDEX_HTML

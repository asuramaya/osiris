"""THE ATLAS FOLD (Thoth dispatch 9563, 588148bb): the retired Atlas's whole-graph LOD
navigation, rebuilt as a SECOND mode over the SAME cytoscape board (src/ui/static/osiris.js's
makeBoard) — never a second canvas, never a second library ("Cytoscape stays the renderer",
Thoth's own instruction). Mirrors test_graph_visualizer_wave_a.py's own static-source-guard
convention: no browser test harness exists in this repo for makeBoard, so these are
string-presence proofs against the served JS, not DOM assertions.
"""
from __future__ import annotations

from pathlib import Path

_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "osiris.js").read_text()
_CONSOLE_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()
_INDEX_HTML = (Path(__file__).parent.parent / "src" / "ui" / "static" / "index.html").read_text()


# --- positions come from the server, never a client-side layout ---------------------------

def _whole_graph_loaders_body() -> str:
    return _JS.split("async function loadSupernodes()", 1)[1].split(
        "\n    function exitWholeGraph", 1)[0]


def test_whole_graph_loaders_never_call_layout() -> None:
    body = _whole_graph_loaders_body()
    assert "layout(" not in body
    assert "position: { x:" in body  # every cy.add carries an explicit server position


def test_whole_graph_reuses_the_same_cy_instance_not_a_second_canvas() -> None:
    body = _whole_graph_loaders_body()
    assert "new cytoscape(" not in body
    assert "cy.add(" in body


# --- the three endpoints atlas built, fully reused -----------------------------------------

def test_load_supernodes_hits_the_supernodes_endpoint() -> None:
    assert 'fetch("/graph/supernodes")' in _JS


def test_load_clusters_hits_the_clusters_endpoint_with_the_project_label() -> None:
    assert 'fetch(`/graph/clusters?project=${encodeURIComponent(projectLabel)}`)' in _JS


def test_load_viewport_hits_the_bbox_endpoint() -> None:
    assert 'fetch("/objects/viewport?"' in _JS


def test_unfiled_supernode_carries_orphan_and_abstention_counts() -> None:
    assert "function unfiledLabel(u)" in _JS
    assert "u.orphans" in _JS and "u.abstained" in _JS
    assert 'raw: { ...g.unfiled, label: "unfiled", unfiled: true }' in _JS


# --- three-level drill: supernodes -> clusters -> real positioned nodes -------------------

def test_supernode_click_drills_into_clusters() -> None:
    body = _JS.split('cy.on("tap", "node"', 1)[1][:900]
    assert 'type === "supernode"' in body
    assert "loadClusters(e.target.id()" in body


def test_cluster_click_drills_into_the_viewport() -> None:
    body = _JS.split('cy.on("tap", "node"', 1)[1][:900]
    assert 'type === "cluster"' in body
    assert "loadViewportNear(pos.x, pos.y, 400)" in body


def test_a_real_positioned_node_click_behaves_like_neighborhood_mode() -> None:
    body = _JS.split('cy.on("tap", "node"', 1)[1][:900]
    assert "onFocus && onFocus(e.target.id(), false, type);" in body


def test_zoom_out_climbs_back_one_level_mirroring_the_retired_atlas() -> None:
    assert 'wgLevel === "nodes" && wgProject' in _JS
    assert "loadClusters(wgProject.id, wgProject.label)" in _JS


# --- sizing/labels: population, not cytoscape degree --------------------------------------

def test_supernode_and_cluster_size_by_population_not_degree() -> None:
    assert "const sizeForCount = (n) => Math.min(56, 10 + Math.sqrt(Math.max(n, 1)) * 3.2);" in _JS
    assert "node[type='supernode']" in _JS and "node[type='cluster']" in _JS


def test_labels_by_zoom_threshold_is_reused_not_reimplemented() -> None:
    # the base `node` style's own label function (ZOOM_LABEL_THRESHOLD) already applies to
    # every node kind — no second labeling/zoom mechanism for supernodes/clusters.
    style_block = _JS.split('style: [', 1)[1].split('],\n    });', 1)[0]
    assert style_block.count("cy.zoom() < ZOOM_LABEL_THRESHOLD") == 1


# --- the shell's own toggle (console.js) ---------------------------------------------------

def test_toggle_enters_whole_graph_via_load_supernodes() -> None:
    assert "(ensureBoard()).loadSupernodes();" in _CONSOLE_JS


def test_toggle_exits_whole_graph_and_repopulates_the_neighborhood_board() -> None:
    body = _CONSOLE_JS.split("function toggleGraphMode()", 1)[1].split("\nfunction ", 1)[0]
    assert "(ensureBoard()).exitWholeGraph();" in body
    assert "renderEntityExplorerStage();" in body


def test_whole_graph_mode_owns_the_canvas_until_explicitly_toggled_off() -> None:
    body = _CONSOLE_JS.split("function renderEntityExplorerStage()", 1)[1].split("\n\n", 1)[0]
    assert "if (GRAPH_MODE === 'whole') return;" in body


def test_leaving_browse_exits_whole_graph_mode() -> None:
    body = _CONSOLE_JS.split("async function switchSurface(surface)", 1)[1][:500]
    assert "GRAPH_MODE === 'whole'" in body
    assert "(ensureBoard()).exitWholeGraph();" in body


def test_relayout_expand_collapse_are_hidden_in_whole_graph_mode() -> None:
    body = _CONSOLE_JS.split("function setGraphModeUI()", 1)[1].split("\nfunction ", 1)[0]
    for btn_id in ("graph-relayout-btn", "graph-expand-btn", "graph-collapse-btn"):
        assert btn_id in body


def test_nav_and_controls_markup_exist() -> None:
    assert 'id="graph-mode-btn"' in _INDEX_HTML
    assert 'id="graph-zoomout-btn"' in _INDEX_HTML
    assert 'id="graph-level-badge"' in _INDEX_HTML
    assert 'onclick="toggleGraphMode()"' in _INDEX_HTML
    assert 'onclick="wholeGraphZoomOut()"' in _INDEX_HTML

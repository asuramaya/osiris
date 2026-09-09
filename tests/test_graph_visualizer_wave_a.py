"""THE GRAPH VISUALIZER, WAVE A (operator dispatch, wave 15, thread 8839): readability of
the cytoscape board (src/ui/static/osiris.js's makeBoard), one numbered item per section
below, mirroring test_console_js_routes.py's own static-source-guard convention -- no
browser test harness exists in this repo, so these are string-presence proofs against the
served JS, not DOM assertions.
"""
from __future__ import annotations

from pathlib import Path

_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "osiris.js").read_text()


# --- item 1: labels are titles, 40 chars, hidden below a zoom threshold -------------------

def test_labels_truncate_to_forty_characters() -> None:
    assert "LABEL_MAX = 40" in _JS
    assert "truncateLabel" in _JS


def test_labels_hide_below_a_zoom_threshold() -> None:
    assert "ZOOM_LABEL_THRESHOLD" in _JS
    assert 'cy.on("zoom"' in _JS and "cy.style().update()" in _JS


# --- item 2: edge labels off by default, shown on hover and on the selected node's edges --

def test_edge_labels_are_off_by_default() -> None:
    assert '"edge", style: {' in _JS
    assert 'label: "", "font-size": 9' in _JS


def test_edge_labels_show_on_hover_and_on_the_focused_nodes_edges() -> None:
    assert "edge.edge-hover, edge.edge-focus" in _JS
    assert 'cy.on("mouseover", "edge"' in _JS and 'cy.on("mouseout", "edge"' in _JS
    assert "connectedEdges().addClass(\"edge-focus\")" in _JS


# --- item 3: hub bundling -- >12 same-type edges off one node collapse into one bundle ----

def test_hub_bundle_threshold_is_twelve() -> None:
    assert "HUB_BUNDLE_THRESHOLD = 12" in _JS


def test_bundle_carries_its_own_count_and_type_as_the_label() -> None:
    assert "label: `${fresh.length} ${gr.type}`" in _JS


def test_a_bundle_node_expands_on_click_instead_of_focusing() -> None:
    assert 'e.target.data("type") === "bundle"' in _JS
    assert "expandBundle(e.target.id())" in _JS


def test_bundle_expansion_restores_the_withheld_nodes_and_edges() -> None:
    assert "const expandBundle = (bundleId) =>" in _JS
    assert "info.nodes.forEach" in _JS and "info.edges.forEach" in _JS


# --- item 4: node size by degree, colour by type, agents painted live/idle/dead -----------

def test_node_size_is_a_function_of_degree() -> None:
    assert "const nodeSize = (e) => Math.min(NODE_SIZE_MAX" in _JS
    assert "e.degree() * NODE_SIZE_PER_DEGREE" in _JS
    assert "width: (e) => nodeSize(e), height: (e) => nodeSize(e)" in _JS


def test_degree_based_style_refreshes_when_edges_change() -> None:
    assert 'cy.on("add remove", "edge", () => cy.style().update())' in _JS


def test_agent_nodes_paint_by_live_idle_dead_state() -> None:
    for state in ("live", "idle", "dead"):
        assert f"node[type='Agent'][agent_state='{state}']" in _JS
    assert "agent_state: n.agent_state" in _JS  # nodeData() passthrough


# --- item 5: sticky positions -- Re-layout is the only thing that moves a placed node -----

def test_positions_persist_across_reload() -> None:
    assert 'const POS_KEY = "osiris.board.positions"' in _JS
    assert "localStorage.setItem(POS_KEY" in _JS and "localStorage.getItem(POS_KEY" in _JS


def test_a_human_drag_saves_its_own_new_position() -> None:
    assert 'cy.on("dragfree", "node", savePositions)' in _JS


def test_new_nodes_land_near_an_already_placed_neighbor_not_at_random() -> None:
    assert "const settleNewNode = (n) =>" in _JS
    assert "connectedEdges().connectedNodes().filter((m) => m.id() !== n.id()" in _JS
    assert "PLACED.has(m.id()))" in _JS


def test_merge_graph_only_calls_layout_when_the_board_was_empty() -> None:
    assert "const wasEmpty = cy.nodes().length === 0" in _JS
    assert "if (wasEmpty) { layout(false); }" in _JS
    assert "newIds.forEach((id) => settleNewNode(cy.getElementById(id)))" in _JS


# --- item 6: search box focuses a node, expand/collapse one hop, breadcrumbs, Escape -----

def test_board_exposes_expand_and_collapse_one_hop() -> None:
    assert "async expandOneHop(id)" in _JS
    assert "collapseOneHop(id)" in _JS
    assert "n.degree() === 1" in _JS  # collapse only prunes leaves hanging off the center


def test_search_box_and_breadcrumbs_are_wired_in_console_js() -> None:
    console_js = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()
    assert "function graphSearchInput(q)" in console_js
    assert "function pickGraphSearch(i)" in console_js and "focus(item.id)" in console_js
    assert "function pushBreadcrumb(id, label)" in console_js
    assert "function jumpToBreadcrumb(i)" in console_js
    assert "function stepBackBreadcrumb()" in console_js
    assert "function expandFocusOneHop()" in console_js
    assert "function collapseFocusOneHop()" in console_js


def test_escape_steps_back_a_breadcrumb_when_nothing_more_local_consumed_it() -> None:
    console_js = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()
    assert "stepBackBreadcrumb()" in console_js
    assert "if (!hadDropdown && !hadPeek && ACTIVE_SURFACE === 'browse')" in console_js


def test_graph_search_and_breadcrumb_markup_exist() -> None:
    html = (Path(__file__).parent.parent / "src" / "ui" / "static" / "index.html").read_text()
    assert 'id="graph-search"' in html
    assert 'id="graph-breadcrumbs"' in html
    assert 'onclick="expandFocusOneHop()"' in html
    assert 'onclick="collapseFocusOneHop()"' in html


# --- item 7: Browse tiles carry an edge-count badge ---------------------------------------

def test_edge_counts_are_fetched_in_chunks_after_loading_the_object_set() -> None:
    console_js = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()
    assert "async function loadEdgeCounts(ids)" in console_js
    assert "loadEdgeCounts(SET.map(function(o){ return o.id; }))" in console_js
    assert "/objects/edge_counts?ids=" in console_js


def test_browse_tiles_render_the_edge_count_badge() -> None:
    console_js = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()
    assert "function edgeCountBadge(id)" in console_js
    assert console_js.count("edgeCountBadge(o.id)") >= 2  # table row AND board card


def test_focus_no_longer_forces_a_layout_on_every_click() -> None:
    console_js = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()
    assert "(ensureBoard()).layout((ensureBoard()).cy.nodes().length > 1)" not in console_js
    assert ("(ensureBoard()).mergeGraph(g); (ensureBoard()).focusNode(id); "
            "(ensureBoard()).fit();") in console_js

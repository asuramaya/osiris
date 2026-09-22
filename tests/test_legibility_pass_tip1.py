"""THE LEGIBILITY PASS, TIP 1 (ruling e1cb9e3b, Thoth DM 10708, thread 71c4ca0d). The
operator's own screenshots plus Thoth's own live measurement at fit (9 world units/px,
median nearest-neighbour 19 units = 2px, average node radius 8.4px = 75 units -- every node
covering ~60 neighbours; a focus on a degree-8 Decision reaching only itself and fitting the
camera to a point at 300x). Six pieces, all off d0d50d2: (a) world-unit node size with a
screen floor/cap, (b) a log-scale degree curve, (c) labels are real names not a wall of
garbage text, (d) focus hides (not dims) and is never empty, (e) one search (the header
omnibox) plus a shared type-visibility flag for header pills and the legend, (f) canvas
controls move off the table drawer's bottom edge. Mirrors the existing static-source-guard
convention -- no browser test harness exists in this repo; the live render (before/after at
fit and at one cluster) was verified via claude-in-chrome and reported on thread 71c4ca0d,
not re-proven here.

AMENDED (operator via Thoth mail 10726, ruling amending e1cb9e3b) before this tip even
shipped its first review: (1) a single CLICK on a node is the WHOLE gesture -- select,
inspector, hide, fit, one act; no double-click, no Enter, click on empty canvas clears;
(2) the hidden/fitted state renders within 100ms of the click from the client-side edge
index, the inspector fetch fills in after and never gates the visual; (3) the lens is
upstream by default until roots (no depth cap), downstream is a toggle off by default,
grounded_by/decided_in/answers added to the walk; (4) an EGO RELAYOUT while focused --
focused object at centre, ancestors ranked leftward by hop (roots farthest left), siblings
spread within their rank, spacing in screen pixels converted to world at the current zoom,
temporary, Clear restores the real stored positions.
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_SPACE_JS = (_STATIC / "space.js").read_text()
_CONSOLE_JS = (_STATIC / "console.js").read_text()
_INDEX_HTML = (_STATIC / "index.html").read_text()
_SPACE_HTML = (_STATIC / "space.html").read_text()
_OSIRIS_CSS = (_STATIC / "osiris.css").read_text()


# --- (a)+(b): world-unit radius, floor/cap in screen px, log-scale degree curve -----------

def test_node_size_is_a_constant_screen_px_degree_curve_not_world_units() -> None:
    # THE LAST RENDERER (operator ruling d7d55257, Thoth mail 11066) retired the world-unit
    # sizing scheme this test used to assert -- "points at a constant SCREEN size in px on a
    # steep degree curve... no world-unit sizing, no 48px cap." See
    # test_legibility_pass_tip5.py for the new nodeScreenPx curve's own tests.
    assert "function nodeRadiusWorld(nd)" not in _SPACE_JS
    assert "CATEGORY_BASE_WORLD" not in _SPACE_JS
    assert "function nodeScreenPx(nd)" in _SPACE_JS


# --- (c): labels are names, per-type formatting, hard truncation, a hover card ------------

def test_labels_resolve_real_names_off_the_wire_header_now() -> None:
    # TIP 1b (Thoth mail 10755): "swap the client label fallback for the header labels" --
    # Khnum's own `labels` array (graph_stream.py's _short_label, tip 2g, fixed live in
    # mail 10892/commit 0496a7d to resolve a real title for every type, Commit included)
    # is the source now, synchronous off nd.label, no per-node fetch for any type at all --
    # the earlier Commit-only client-side upgrade (review flaw #6) is retired outright now
    # that the gap it patched closed at the source.
    assert "function labelTextFor(nd)" in _SPACE_JS
    assert "label: snap.labels ? snap.labels[i] : undefined," in _SPACE_JS
    body = _SPACE_JS.split("function labelTextFor(nd)", 1)[1][:200]
    assert "{ return nd.label || fallbackLabel(nd); }" in body
    assert "async function fetchCommitSubject" not in _SPACE_JS
    assert "function truncateLabel" not in _SPACE_JS


def test_hover_card_exists_and_shows_label_plus_type_and_project() -> None:
    assert 'hoverEl.className = "hover-card";' in _SPACE_JS
    body = _SPACE_JS.split("function updateHoverCard(nd)", 1)[1].split("\n  }\n", 1)[0]
    assert "labelTextFor(nd)" in body
    assert "nd.type" in body and "nd.project" in body
    # never fires while dragging (would fight a pan) and is debounced, not per-mousemove
    mm_body = _SPACE_JS.split('addEventListener("mousemove"', 1)[1][:400]
    assert "if (dragging)" in mm_body
    assert "setTimeout(() => {" in mm_body


def test_hover_card_css_exists_in_both_pages() -> None:
    for html in (_INDEX_HTML, _SPACE_HTML):
        assert ".hover-card {" in html


# --- (d): focus hides (per-instance aVisible), never dims; never empty --------------------

def test_focus_uses_a_per_instance_visibility_flag_not_a_dim_scalar() -> None:
    assert 'geo.setAttribute("aVisible", visibleAttr);' in _SPACE_JS
    assert "attribute float aVisible;" in _SPACE_JS
    assert "* aVisible;" in _SPACE_JS


def test_a_focused_node_with_no_semantic_edges_still_lights_its_structural_neighbours() -> None:
    # THE DRILL (ruling d7d55257) inserted a container-focus dispatch and clearDrillState()
    # call at the top of focusObject, pushing this fallback further into the body. WAVE 27,
    # THE LENS PANEL: the literal "structural" check became isStructuralLike() so a genuine
    # "container"-class edge (now distinct from "structural") still widens this fallback the
    # same as before.
    body = _SPACE_JS.split("async function focusObject(id, opts)", 1)[1][:2900]
    assert "if (pathReachable.size <= 1) {" in body
    assert "if (!isStructuralLike(e.edgeClass)) continue;" in body
    assert "pathReachable.add(other);" in body


def test_base_edge_layer_hides_edges_touching_an_invisible_node() -> None:
    body = _SPACE_JS.split("function nodeVisible(nd)", 1)[1][:600]
    assert "hiddenNodeTypes.has(nd.type)" in body
    # CONSOLE CHROME CLEANUP piece 2 (decision 31717ca7): the repo selector's own
    # hidden-set must ALSO gate edge-geometry visibility here, the same as applyDim's
    # own per-instance flag — an edge touching a project-hidden node must not still draw.
    assert "hiddenProjects.has(nd.project)" in body
    assert "pathReachable.has(nd.id)" in body
    build_body = _SPACE_JS.split("function buildEdgeLines(nodes, edgeList)", 1)[1][:2300]
    assert "nodeVisible(byId.get(e.source))" in build_body
    assert "nodeVisible(byId.get(e.target))" in build_body


# --- (e): one search (header omnibox), type filters share the visibility flag -------------

def test_the_in_canvas_find_a_node_box_is_gone() -> None:
    for html in (_INDEX_HTML, _SPACE_HTML):
        assert 'id="graph-search"' not in html
        assert 'id="graph-search-dd"' not in html
    assert "searchInput" not in _SPACE_JS
    assert "searchDd" not in _SPACE_JS


def test_header_omnibox_graph_hits_always_focus() -> None:
    # SUPERSEDED by THE LEGIBILITY PASS TIP 1's own amendment (mail 10726): select-vs-focus
    # (click selects, Enter focuses) is retired -- a Graph hit focuses either way now.
    body = _CONSOLE_JS.split("const graphHits = hits.filter", 1)[1][:700]
    assert "run: () => { switchSurface('browse'); focus(h.id); }" in body
    assert "selectFromOmni" not in _CONSOLE_JS
    assert "runFocus" not in _CONSOLE_JS


def test_header_type_filters_and_the_legend_drive_the_same_visibility_flag() -> None:
    assert "function setHiddenTypes(types)" in _SPACE_JS
    assert "function syncSpaceTypeFilter()" in _CONSOLE_JS
    body = _CONSOLE_JS.split("function toggleEntityType(t)", 1)[1][:300]
    assert "syncSpaceTypeFilter();" in body
    legend_body = _SPACE_JS.split("[data-legend-node-type]", 1)[1][:400]
    assert "hiddenNodeTypes.delete(type)" in legend_body


def test_header_repo_selector_drives_the_same_visibility_flag_by_project() -> None:
    """CONSOLE CHROME CLEANUP piece 2 (decision 31717ca7, thread 0be2f790's own operator-
    finding follow-up): setHiddenProjects is the repo pill's own sibling to
    setHiddenTypes above — same per-instance aVisible flag, filtered by nd.project
    instead of nd.type."""
    assert "function setHiddenProjects(projects)" in _SPACE_JS
    assert "function syncSpaceProjectFilter()" in _CONSOLE_JS
    body = _CONSOLE_JS.split("function applyRepoFilter()", 1)[1][:400]
    assert "syncSpaceProjectFilter();" in body
    apply_dim_body = _SPACE_JS.split("function applyDim()", 1)[1][:800]
    assert "hiddenProjects.has(nd.project)" in apply_dim_body


def test_legend_gains_a_node_types_section() -> None:
    body = _SPACE_JS.split("function renderLegend(edgeList, nodeList)", 1)[1][:2200]
    assert "legend-node-type" in body
    assert "node types" in body


# --- (f): canvas controls move to the top strip, the drawer owns the bottom edge ----------

def test_canvas_controls_sit_at_the_top_not_colliding_with_the_drawer() -> None:
    body = _OSIRIS_CSS.split(".graph-canvas-controls {", 1)[1][:200]
    assert "top: 14px;" in body
    assert "bottom: 14px;" not in body


def test_status_line_and_legend_panel_moved_off_the_drawers_bottom_band() -> None:
    assert "#space-status-line { position: absolute; bottom: 44px;" in _INDEX_HTML
    assert ".legend-panel { position: absolute; top: 52px; right: 14px;" in _INDEX_HTML


# --- AMENDMENT item 1: a single click is the whole gesture --------------------------------

def test_click_on_empty_canvas_still_clears() -> None:
    click_body = _SPACE_JS.split(
        'renderer.domElement.addEventListener("click", (ev) => {', 1)[1][:300]
    assert "else clearFocus();" in click_body


# --- AMENDMENT item 2: the 100ms budget -- client-side, inspector fetch never gates it -----

def test_the_visual_work_is_synchronous_the_inspector_fetch_is_awaited_last() -> None:
    body = _SPACE_JS.split("async function focusObject(id, opts)", 1)[1]
    # every `await` inside focusObject's own body must be the final `await inspect(id);` --
    # no earlier await (a network call) can gate the synchronous select/hide/fit work above.
    fn_body = body.split("\n  async function inspect(id)", 1)[0]
    # THE DRILL (ruling d7d55257) inserted an early-exit container dispatch at the top -- a
    # SEPARATE branch (renderContainerDrill owns its own synchronous-then-one-await shape)
    # that returns before any of the ordinary ego-walk work below ever runs, excluded here.
    # WAVE 26, THE STORYLINE (mail 11534) added a second, sibling early-exit branch
    # (renderStoryline, same shape) for an Agent focus -- excluded for the identical reason.
    awaits = [ln.strip() for ln in fn_body.splitlines()
              if "await " in ln and "renderContainerDrill" not in ln
              and "renderStoryline" not in ln]
    assert awaits, "expected at least one await in focusObject"
    assert awaits[-1] == "await inspect(id);"
    assert len(awaits) == 1  # the ONLY await is the trailing inspector fetch


# --- AMENDMENT item 3: upstream until roots, downstream a toggle off by default -----------

def test_depth_is_unlimited_by_default_until_roots() -> None:
    assert "const FOCUS_DEPTH_DEFAULT = Infinity;" in _SPACE_JS


def test_downstream_is_a_toggle_off_by_default() -> None:
    assert "let includeDownstream = false;" in _SPACE_JS
    body = _SPACE_JS.split("async function focusObject(id, opts)", 1)[1][:1700]
    assert "includeDownstream ? bfsHops(inAdjPath, id, focusDepth) : new Map([[id, 0]])" in body
    btn_body = _SPACE_JS.split("if (downstreamBtn) {", 1)[1][:500]
    assert "includeDownstream = !includeDownstream;" in btn_body
    assert 'downstreamBtn.textContent = "Downstream: off";' in _SPACE_JS


# --- AMENDMENT item 4: ego relayout while focused, temporary, restored on clear -----------

def test_ego_relayout_exists_and_ranks_ancestors_leftward_roots_farthest() -> None:
    assert "function applyEgoLayout(focusId, hopsUp, hopsDown, extraSeed)" in _SPACE_JS
    body = _SPACE_JS.split(
        "function applyEgoLayout(focusId, hopsUp, hopsDown, extraSeed)", 1)[1][:3600]
    # ancestors (hopsUp) get a NEGATIVE signed rank -- more hops (closer to root) = further
    # negative = further left; downstream (hopsDown) gets a positive rank, mirrored right.
    assert "(byRank.get(-hop) || (byRank.set(-hop, []), byRank.get(-hop))).push(id);" in body
    assert "(byRank.get(hop) || (byRank.set(hop, []), byRank.get(hop))).push(id);" in body
    # THE SUCCESSION CHAIN COLUMN COMPRESSION (mail 11272 items 2/4) replaced the old flat
    # `const x = cx + signedHop * colW;` with a progressive per-side accumulation (colX) so
    # a pure succession run can use the tighter chain width instead of the full column
    # width every hop -- still ranks ancestors leftward/roots farthest, just not at a fixed
    # per-hop multiple any more.
    assert "const colX = new Map([[0, cx]]);" in body
    assert "const x = colX.get(signedHop);" in body


def test_ego_layout_spacing_is_screen_pixels_converted_to_world_at_a_stable_scale() -> None:
    # TIP 1c review flaw #6 (Thoth mail 10891): the CURRENT (pre-focus) worldPerPx made the
    # ego layout's own scale track whatever zoom the camera happened to already be at -- a
    # small reachable set following a tight prior focus could spiral the fit down to a
    # near-empty viewSize. maxViewSize (the whole graph's own stable fitted scale) fixes it.
    assert "const EGO_COL_SPACING_PX = 150;" in _SPACE_JS
    assert "const EGO_ROW_SPACING_PX = 34;" in _SPACE_JS
    body = _SPACE_JS.split(
        "function applyEgoLayout(focusId, hopsUp, hopsDown, extraSeed)", 1)[1][:900]
    assert "const wpp = maxViewSize / wrap.clientHeight;" in body
    assert "const colW = EGO_COL_SPACING_PX * wpp, rowH = EGO_ROW_SPACING_PX * wpp;" in body


def test_ego_layout_is_temporary_clear_restores_the_stored_positions() -> None:
    assert "function restoreEgoLayout()" in _SPACE_JS
    restore_body = _SPACE_JS.split("function restoreEgoLayout()", 1)[1].split("\n  }\n", 1)[0]
    assert "nd.x = pos.x; nd.y = pos.y;" in restore_body
    clear_body = _SPACE_JS.split("function clearFocus()", 1)[1][:400]
    assert "restoreEgoLayout();" in clear_body
    apply_body = _SPACE_JS.split(
        "function applyEgoLayout(focusId, hopsUp, hopsDown, extraSeed)", 1)[1][:200]
    assert "restoreEgoLayout();" in apply_body  # a fresh focus never layers onto a stale one


def test_ego_layout_moves_only_gpu_instances_for_the_moved_nodes_not_a_full_rebuild() -> None:
    # O(moved), never O(49k) -- the perf discipline this whole arc has held since mail 10581.
    assert "function syncMovedInstancePositions(movedIds)" in _SPACE_JS
    body = _SPACE_JS.split("function syncMovedInstancePositions(movedIds)", 1)[1][:700]
    assert "if (!movedIds.has(nd.id)) continue;" in body
    assert "mesh.setMatrixAt(i, dummy.matrix);" in body
    assert "pickMesh.setMatrixAt(i, dummy.matrix);" in body

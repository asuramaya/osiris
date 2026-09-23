"""THE READING LAYER, part A: EDGE CLASSES. The goal was to let a reader focus enough to
trace long paths back and upstream, with the inspector, table and graph working in harmony.
Structural edges (containment/membership -- in_repo, works_in, governs, ...) are real but
not what a reader is tracing and their degree dwarfs everything else (repo:osiris alone:
20,352); they are not drawn at rest. Semantic edges (the actual provenance trail --
possible_upstream, cites, derived_from, spawned_by, succeeded_from, supersedes, resolves,
...) draw always, with alpha falling by ON-SCREEN length rather than by zoom level. A
legend toggles both classes and individual types. Mirrors the existing static-source-guard
convention -- no browser test harness exists in this repo, so these are string-presence
proofs against the served JS/HTML; the live render (structural hidden/shown correctly on
toggle, no crash on a ~20k-edge rebuild, click-to-focus still works throughout) was
verified via claude-in-chrome and is reported on the thread, not re-proven here.
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_SPACE_JS = (_STATIC / "space.js").read_text()
_INDEX_HTML = (_STATIC / "index.html").read_text()
_SPACE_HTML = (_STATIC / "space.html").read_text()


# --- classification: a default, disclosed rather than parked on, overridden by the
# real per-type header field ---------------------------------------------------------------

def test_a_default_structural_type_list_exists_and_is_disclosed() -> None:
    assert "const STRUCTURAL_EDGE_TYPES = new Set([" in _SPACE_JS
    for t in ("in_repo", "works_in", "governs", "holds", "acts_for", "member_of"):
        assert f'"{t}"' in _SPACE_JS
    assert "function classOfEdgeType(type)" in _SPACE_JS


def test_classification_reads_the_wire_field_link_type_class_not_edge_classes() -> None:
    # THE WIRE EDGE CLASSES FIX: the browser used to read
    # `snap.edge_classes`, a field the wire never actually sends -- the real header field
    # is `link_type_class`, index-aligned to edge_types the same way. The old field is gone
    # outright as a live READ (an explanatory comment naming it, for the historical record,
    # is fine and expected -- checked as a real indexing expression, not a bare substring,
    # so the comment prose doesn't false-negative this).
    body = _SPACE_JS.split("async function fetchStreamSnapshot()", 1)[1].split(
        "\n  return { nodes, edges, edgeClassByType };", 1)[0]
    assert "snap.edge_classes[" not in body
    assert "snap.link_type_class[i]" in body
    assert "classOfEdgeType(type)" in body  # still the fallback for a type the header lacks


def test_header_container_class_no_longer_normalizes_to_structural() -> None:
    # THE LENS PANEL: "container" used to normalize to
    # "structural" here (every pre-existing check only ever distinguished "structural" from
    # everything else) -- now kept distinct so the legend can offer it as its own lens
    # toggle, alongside semantic/structural. Every call site that relied on the old
    # normalization goes through isStructuralLike() instead (see tests/test_lens_panel.py).
    body = _SPACE_JS.split("async function fetchStreamSnapshot()", 1)[1][:3200]
    assert 'if (cls === "container") cls = "structural";' not in body
    assert "function isStructuralLike(edgeClass)" in _SPACE_JS


def test_a_header_class_overrides_the_client_fallback_table() -> None:
    # live-verified regression: the browser marked authored_by "semantic"
    # (STRUCTURAL_EDGE_TYPES doesn't list it) while the header's own link_type_class says
    # authored_by is structural -- edgeClassByType must prefer the header's own value.
    body = _SPACE_JS.split("async function fetchStreamSnapshot()", 1)[1][:3400]
    assert "const cls = snap.link_type_class && snap.link_type_class[i];" in body
    assert "edgeClassByType[t] = cls || classOfEdgeType(t);" in body


def test_effective_edge_class_debug_hook_exists() -> None:
    body = _SPACE_JS.split("const api = {", 1)[1]
    assert "effectiveEdgeClass(type)" in body
    assert "get edgeClassByType()" in body


# --- the legend still opts a class/type OUT; nothing is hidden by default any more --------

def test_structural_class_is_no_longer_hidden_by_default() -> None:
    # the earlier "structural hidden at rest" rule is superseded outright by THE
    # DRAWING TIP's own ruling: nothing hidden, nothing drawn twice. Caps and hides were
    # the old answer to density; membership is a project fill and the two universal fans
    # are high-degree objects now, not a blanket structural-class hide. See
    # tests/test_drawing_tip.py for the full model.
    assert 'const hiddenEdgeClasses = new Set();' in _SPACE_JS
    assert 'const hiddenEdgeClasses = new Set(["structural"]);' not in _SPACE_JS


def test_edge_geometry_build_filters_by_hidden_classes_and_types() -> None:
    # TIP 4 (operator ruling "DENSITY NOT DISCS") reverted the parameter back
    # to edgeList -- no more zoom-tier edge budget. hiddenEdgeClasses/hiddenEdgeTypes start
    # empty now (THE DRAWING TIP) but the legend-toggle filter mechanism itself is unchanged.
    body = _SPACE_JS.split("function buildEdgeLines(nodes, edgeList)", 1)[1][:2300]
    assert "!hiddenEdgeClasses.has(e.edgeClass)" in body
    assert "!hiddenEdgeTypes.has(e.type)" in body


# --- semantic edges fade by ON-SCREEN length (a GPU shader, not a per-zoom CPU rewrite --
# the same discipline already established for node sizing) ---------------------------------

def test_edge_fade_is_a_shader_not_a_per_zoom_material_opacity_write() -> None:
    assert "function makeEdgeFadeMaterial()" in _SPACE_JS
    assert "attribute vec3 otherPosition;" in _SPACE_JS
    assert "float screenLen = distance(pxA, pxB);" in _SPACE_JS
    # the old viewSize-driven opacity scalar is gone, not just unused
    assert "function updateEdgeStyle()" not in _SPACE_JS
    assert "edgeLines.material.opacity" not in _SPACE_JS


def test_rescale_for_zoom_no_longer_touches_edge_style_at_all() -> None:
    body = _SPACE_JS.split("function rescaleForZoom()", 1)[1].split("\n  }\n", 1)[0]
    assert "updateEdgeStyle" not in body
    assert "edgeLines" not in body


def test_viewport_uniform_is_kept_current_on_resize() -> None:
    body = _SPACE_JS.split('window.addEventListener("resize"', 1)[1][:400]
    assert "edgeFadeUniforms.uViewportPx.value.set" in body


# --- the legend: lists real classes/types present, toggles rebuild the edge geometry ------

def test_legend_markup_exists_in_both_pages() -> None:
    for html in (_INDEX_HTML, _SPACE_HTML):
        assert 'id="legend-btn"' in html
        assert 'id="legend-panel"' in html


def test_legend_toggle_button_shows_and_hides_the_panel() -> None:
    body = _SPACE_JS.split("if (legendBtn && legendPanel)", 1)[1][:200]
    assert "legendPanel.hidden = !legendPanel.hidden;" in body


def test_legend_checkboxes_rebuild_edge_lines_on_change() -> None:
    # THE LEGIBILITY PASS, TIP 1(e) added a third checkbox group (node
    # types, alongside edge class/type) to the same legend panel: all three still rebuild.
    body = _SPACE_JS.split("function renderLegend(edgeList, nodeList)", 1)[1]
    # three legend checkbox groups (node type, class, type), setHiddenTypes' own call (the
    # header taxonomy pills' entry point, TIP 1(e)), setHiddenProjects' own call (the
    # header repo selector's entry point, CONSOLE CHROME CLEANUP piece 2),
    # plus TIP 1b's own review-flaw-#1 fix: focusObject and clearFocus each
    # rebuild the base layer too, so unreachable edges actually disappear on focus instead
    # of only nodes: seven call sites total. TIP 3 briefly added an eighth (refreshLOD,
    # rebuilding the edge layer under its own zoom-tier budget); TIP 4
    # (operator ruling "DENSITY NOT DISCS") retired that budget outright, back to
    # seven. THE DRILL added two more of its own: renderContainerDrill
    # (a container-scale focus rebuilds the base layer same as an ordinary focus) and
    # revealProjectStub (a stub reveal changes nodeVisible for the revealed ids, so the base
    # layer must rebuild too): nine call sites. THE STORYLINE added a
    # tenth: renderStoryline rebuilds the base layer same as an ordinary focus or the drill.
    # THE LENS PANEL added an eleventh: the new "high-degree objects"
    # lens checkbox rebuilds the base layer too (its own toggle changes which edges fold
    # into a badge vs. draw as a line). The same pass's hash-restore fix
    # added a twelfth: a hashchange listener reapplies a shared lens link's state and
    # must rebuild the edge layer the same way a legend checkbox does, since the restored
    # state can change which edge classes/types are hidden. This slice runs unbounded to
    # end-of-file (no closing boundary in the split above), so it catches every function
    # defined after renderLegend, not just renderLegend's own body: noted rather than
    # silently re-scoping an existing test's own slicing choice.
    assert body.count("buildEdgeLines(idToNode, edges);") == 12

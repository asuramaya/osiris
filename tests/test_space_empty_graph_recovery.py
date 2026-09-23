"""THE BLACK CANVAS ON A FRESH INSTALL: a newcomer's first Browse load
racing the layout worker's own startup batch got an empty /graph/stream snapshot and
stayed that way forever, with zero visible feedback. Two distinct bugs, confirmed live
against a genuinely empty scratch install (migrated + seeded, no worker ever run):

1. No on-screen indication the graph was empty at all -- the WebGL scene's background is
   the same dark navy whether it holds thousands of nodes or none, indistinguishable from
   the product being broken. Fixed with a plain-text overlay shown only while the node
   count is zero.
2. Once positioned, the graph never actually appeared without a manual reload.
   graph_layout._bulk_assert_positions writes graph_x/graph_y/graph_layout_v via a raw
   multi-row UPDATE+INSERT, deliberately bypassing actions.assert_property (the only
   thing that ever inserts an outbox row) for bulk-write efficiency -- so no outbox event,
   and therefore no /graph/stream/deltas message, is EVER emitted for a layout tick.
   Confirmed live: after the DB genuinely had every object positioned, the tab's own
   in-memory node/edge counts stayed at zero indefinitely with no delta ever arriving.
   Fixed with a bounded client-side poll of the full snapshot while empty, independent of
   the delta stream.

Mirrors the existing static-source-guard convention (see
test_navigable_space_integration.py's own docstring): no browser test harness exists in
this repo, so these are string-presence/ordering proofs against the served JS, not DOM
or WebGL assertions -- the live pixel-readback verification (0 non-black pixels before,
millions after) was done by hand against a real scratch install, not reproducible here.
"""
from __future__ import annotations

from pathlib import Path

_SPACE_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "space.js").read_text()


def _poll_block() -> str:
    return _SPACE_JS.split("if (nodes.length === 0) {", 1)[1].split(
        "\n  }\n\n  // ---- deltas", 1)[0]


def test_empty_overlay_element_exists_and_defaults_hidden() -> None:
    assert "const emptyOverlay = document.createElement" in _SPACE_JS
    body = _SPACE_JS.split("const emptyOverlay = document.createElement", 1)[1][:800]
    assert 'display:none' in body
    assert "wrap.appendChild(emptyOverlay)" in body
    assert "function updateEmptyOverlay(count)" in body


def test_empty_overlay_toggles_on_the_real_node_count_not_a_hardcoded_flag() -> None:
    body = _SPACE_JS.split("function updateEmptyOverlay(count)", 1)[1].split("\n", 1)[0]
    assert 'count ? "none" : "flex"' in body


def test_empty_overlay_updates_after_the_initial_load() -> None:
    # right after the first setStatus(...) call that reports the loaded node/edge counts
    marker = 'setStatus(`${nodes.length} objects, ${edges.length} edges`)'
    initial_load = _SPACE_JS.split(marker, 1)[1][:200]
    assert "updateEmptyOverlay(nodes.length)" in initial_load


def test_empty_overlay_updates_after_every_live_rebuild() -> None:
    # both the delta-driven rebuild and the empty-graph poll's own rebuild end the same way
    needle = "updateEmptyOverlay(nodes.length)"
    hits = [i for i in range(len(_SPACE_JS)) if _SPACE_JS.startswith(needle, i)]
    assert len(hits) >= 3  # initial load + poll fallback + delta-driven scheduleRebuild


# --- the empty-graph poll: the delta stream can never self-heal this case, see module
# docstring point 2 -------------------------------------------------------------------


def test_nodes_by_id_is_declared_before_the_empty_graph_poll_that_reassigns_it() -> None:
    # same TDZ bug class this file's own sibling fixes hit before: a variable referenced
    # inside a closure that could run before its own `let` line executes throws "Cannot
    # access before initialization". The poll's callback only ever fires asynchronously
    # (setInterval, earliest 3s later), well after this synchronous declaration runs, but
    # keeping the declaration textually first removes any doubt.
    decl_at = _SPACE_JS.index("let nodesById = new Map(nodes.map((nd) => [nd.id, nd]));")
    poll_at = _SPACE_JS.index("if (nodes.length === 0) {\n    let triesLeft")
    assert decl_at < poll_at


def test_empty_graph_poll_is_scoped_to_a_genuinely_empty_snapshot() -> None:
    assert "if (nodes.length === 0) {" in _SPACE_JS
    poll_block = _poll_block()
    assert "fetchStreamSnapshot()" in poll_block
    assert "setInterval" in poll_block


def test_empty_graph_poll_stops_once_nodes_arrive() -> None:
    poll_block = _poll_block()
    assert "if (!fresh.nodes.length) return;" in poll_block
    assert poll_block.count("clearInterval(pollTimer)") == 2  # success + exhausted-tries paths


def test_empty_graph_poll_is_bounded_not_infinite() -> None:
    poll_block = _poll_block()
    assert "triesLeft" in poll_block
    assert "if (--triesLeft <= 0) { clearInterval(pollTimer); return; }" in poll_block


def test_empty_graph_poll_rebuilds_the_whole_scene_on_success() -> None:
    poll_block = _poll_block()
    for needle in ("buildProjectFillModel(projectAggregates, edges)",
                   "buildCommunityModel(communityAggregates)",
                   "buildScene(nodes, edges)", "fitToNodes(nodes)"):
        assert needle in poll_block, f"{needle} missing from the empty-graph poll's rebuild"

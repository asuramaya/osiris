"""THE READING LAYER, part B: FOCUS = PATH LENS (ruling c5953bb1, Thoth DM 10596, thread
71c4ca0d), AMENDED by THE LEGIBILITY PASS TIP 1's own amendment (operator via Thoth mail
10726): a single CLICK is now the whole gesture -- select, inspector, hide, fit, one act.
The old double-click/Enter/select-vs-focus split is retired outright (see
test_legibility_pass_tip1.py for the amendment's own new tests: single-click, the
downstream toggle, ego relayout, the 100ms budget). This file keeps the still-valid parts:
the walk itself (over a curated provenance/evidence edge-type allowlist, PATH_EDGE_TYPES,
never structural containment) and the acceptance-test proof below.

Thoth's own stated acceptance bar for this part: "a test on a synthetic 5-hop chain where
focus at the tail lights exactly the chain and nothing else." buildPathAdjacency/walkPath
are pure, DOM-free, module-level functions in space.js specifically so this can be a REAL
executable proof of the algorithm rather than another string-presence guard -- run via a
Node subprocess importing the actual ES module, skipped (not failed) if Node isn't
available in this environment, since the project's own stack (CLAUDE.md) declares no Node
dependency. Everything else here stays the existing static-source-guard convention.
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_SPACE_JS = (_STATIC / "space.js").read_text()

_NODE = shutil.which("node")

_FIVE_HOP_CHAIN_SCRIPT = """
import { buildPathAdjacency, walkPath } from './space.js';

// a synthetic 5-hop chain (n0 -> n1 -> ... -> n5, six nodes) over a real path edge type,
// plus two decoys: an edge with both endpoints OFF the chain, and a STRUCTURAL edge
// touching a chain node (n2 -> z, in_repo) that must never widen the path.
const chain = [];
for (let i = 0; i < 5; i++) chain.push({ source: `n${i}`, target: `n${i + 1}`, type: 'cites' });
const decoys = [
  { source: 'x', target: 'y', type: 'cites' },
  { source: 'n2', target: 'z', type: 'in_repo' },
];
const edges = [...chain, ...decoys];
const { outAdj, inAdj } = buildPathAdjacency(edges);
const reachable = walkPath(outAdj, inAdj, 'n5', DEPTH_PLACEHOLDER); // focus at the TAIL
process.stdout.write(JSON.stringify([...reachable].sort()));
"""


def _run_node_es_module(script: str) -> list[str]:
    result = subprocess.run(
        [_NODE, "--input-type=module", "-e", script],
        cwd=_STATIC, capture_output=True, text=True, timeout=30, check=True)
    return json.loads(result.stdout)


@pytest.mark.skipif(_NODE is None, reason="node not available in this environment")
def test_focus_at_the_tail_of_a_five_hop_chain_lights_exactly_the_chain() -> None:
    script = _FIVE_HOP_CHAIN_SCRIPT.replace("DEPTH_PLACEHOLDER", "5")
    reachable = _run_node_es_module(script)
    assert reachable == ["n0", "n1", "n2", "n3", "n4", "n5"]


@pytest.mark.skipif(_NODE is None, reason="node not available in this environment")
def test_a_shallower_depth_does_not_reach_the_whole_chain() -> None:
    # the widen control exists precisely because the default depth (4) does NOT cover a
    # full 5-hop chain -- confirms the walk is genuinely depth-limited, not just capped by
    # running out of graph.
    script = _FIVE_HOP_CHAIN_SCRIPT.replace("DEPTH_PLACEHOLDER", "4")
    reachable = _run_node_es_module(script)
    assert reachable == ["n1", "n2", "n3", "n4", "n5"]
    assert "n0" not in reachable


# --- everything else: static-source-guard proofs, the existing convention -----------------

def test_a_single_click_is_the_whole_focus_gesture() -> None:
    # TIP 1 AMENDMENT (mail 10726): select-vs-focus is retired -- selectObject is gone, a
    # plain click focuses directly, and there is no separate double-click trigger left.
    assert "async function selectObject" not in _SPACE_JS
    assert "async function focusObject(id, opts)" in _SPACE_JS
    # window widened for TIP 3 (Thoth mail 10930): at the far/mid LOD tiers a click drills
    # into a glyph instead (no individual object to focus at that zoom) -- the near-tier
    # focusObject(hit.id) branch now sits after that.
    click_body = _SPACE_JS.split(
        'renderer.domElement.addEventListener("click", (ev) => {', 1)[1][:500]
    assert "focusObject(hit.id)" in click_body
    assert 'addEventListener("dblclick"' not in _SPACE_JS


def test_select_never_hides_the_graph_only_focus_does() -> None:
    # THE LEGIBILITY PASS, TIP 1(d) (ruling e1cb9e3b) replaced the old dim-to-near-invisible
    # with an outright per-instance HIDE (aVisible) -- "no dim" was Thoth's own instruction.
    body = _SPACE_JS.split("function applyDim()", 1)[1].split("\n  }\n", 1)[0]
    assert "const focused = !!pathFocusId;" in body
    assert "const focusHidden = focused && nd.id !== pathFocusId " \
        "&& !pathReachable.has(nd.id);" in body
    # CONSOLE CHROME CLEANUP piece 2 (decision 31717ca7): projectHidden (the repo
    # selector's own hidden-set check) joined the same condition — select/focus
    # semantics here are unchanged, only a third filter dimension was added alongside.
    assert "visibleAttr.setX(i, (typeHidden || projectHidden || focusHidden) ? 0 : 1);" in body


def test_enter_no_longer_promotes_a_selection_a_click_already_focused() -> None:
    # TIP 1 AMENDMENT (mail 10726): "no double-click or Enter" -- the old Enter-focuses-
    # selectedId listener is retired outright, not left as harmless redundancy.
    assert 'if (ev.key !== "Enter") return;' not in _SPACE_JS


def test_inspector_carries_a_focus_button() -> None:
    body = _SPACE_JS.split("async function inspect(id)", 1)[1][:1900]
    assert 'focusBtn.addEventListener("click", () => focusObject(id));' in body


def test_focus_walk_uses_the_curated_provenance_types_never_structural() -> None:
    # TIP 1 AMENDMENT (mail 10726) added grounded_by/decided_in/answers to the walk.
    for t in ("possible_upstream", "cites", "derived_from", "spawned_by",
              "succeeded_from", "supersedes", "resolves",
              "grounded_by", "decided_in", "answers"):
        assert f'"{t}"' in _SPACE_JS
    assert "export const PATH_EDGE_TYPES" in _SPACE_JS


def test_focus_hides_unreachable_outright_not_a_softer_dim() -> None:
    assert "FOCUS_DIM_FACTOR" not in _SPACE_JS  # retired outright, not just renamed
    assert "function setHiddenTypes(types)" in _SPACE_JS


def test_focus_is_never_empty_a_lone_reachable_node_widens_one_structural_hop() -> None:
    # Thoth's own live measurement (mail 10708): a degree-8 Decision with no PATH_EDGE_TYPES
    # links reached only itself and fit the camera to a point at 300x. WAVE 27, THE LENS
    # PANEL: the literal "structural" check became isStructuralLike() so a genuine
    # "container"-class edge (now distinct from "structural") still widens this fallback.
    body = _SPACE_JS.split("async function focusObject(id, opts)", 1)[1][:2900]
    assert "if (pathReachable.size <= 1) {" in body
    assert "if (!isStructuralLike(e.edgeClass)) continue;" in body


def test_reachable_path_edges_draw_with_a_directional_gradient() -> None:
    assert "PATH_EDGE_BRIGHT" in _SPACE_JS and "PATH_EDGE_DIM" in _SPACE_JS
    body = _SPACE_JS.split("function updatePathEdges()", 1)[1][:1400]
    assert "col.push(PATH_EDGE_BRIGHT" in body
    assert "PATH_EDGE_DIM.r" in body


def test_the_structural_edges_of_focus_carve_out_is_retired() -> None:
    # RETIRED by THE LAST RENDERER (operator ruling d7d55257, Thoth mail 11066): "an edge
    # draws only when both ends are visible, no structural-hop exception." updatePathEdges
    # used to draw every structural edge touching the focus regardless of whether the other
    # end was ever positioned or visible -- for a container-scale focus that meant thousands
    # of lines fanning to scattered positions, "a solid disc of edges." Only the real
    # PATH_EDGE_TYPES overlay (`onPath`, both ends reachable) remains.
    body = _SPACE_JS.split("function updatePathEdges()", 1)[1][:1400]
    assert "if (!onPath) continue;" in body
    assert "structuralOfFocus" not in _SPACE_JS


def test_camera_fits_to_the_reachable_set_not_a_fixed_view() -> None:
    body = _SPACE_JS.split("function renderFocusEgoGroups(id, hopsUp, hopsDown)", 1)[1][:2000]
    assert "for (const rid of pathReachable)" in body
    assert "Math.min(maxViewSize, span * 1.6 + 40)" in body


def test_widen_is_retired_depth_is_unlimited_by_default() -> None:
    # TIP 1 AMENDMENT (mail 10726): "upstream ... until roots" -- depth is Infinity by
    # default now, so a capped-depth Widen control no longer means anything. See
    # test_legibility_pass_tip1.py for its replacement, the downstream toggle.
    assert "const FOCUS_DEPTH_DEFAULT = Infinity;" in _SPACE_JS
    assert "widenBtn" not in _SPACE_JS


def test_focus_stack_supports_back_navigation() -> None:
    assert "function pushFocusStack(id)" in _SPACE_JS
    assert "function goBack()" in _SPACE_JS
    body = _SPACE_JS.split("function goBack()", 1)[1][:300]
    assert "focusStack.pop();" in body


def test_escape_clears_rather_than_stepping_back() -> None:
    # clearFocus (bound to both the Clear-focus button and, via console.js's own Escape
    # handler, Escape itself) resets the overlay outright -- Back is the separate,
    # deliberate stack-popping act.
    body = _SPACE_JS.split("function clearFocus()", 1)[1][:400]
    assert "pathFocusId = null;" in body
    assert "pathReachable = new Set();" in body


def test_back_and_downstream_buttons_exist_in_both_pages() -> None:
    # "downstream-btn" replaces the retired "widen-btn" (TIP 1 amendment, mail 10726).
    index_html = (_STATIC / "index.html").read_text()
    space_html = (_STATIC / "space.html").read_text()
    for html in (index_html, space_html):
        assert 'id="back-btn"' in html
        assert 'id="downstream-btn"' in html


def test_table_row_clicks_always_focus_now() -> None:
    # TIP 1 AMENDMENT (mail 10726): select-vs-focus is retired -- a table row click is the
    # same one gesture a canvas click is, always a real focus (see
    # test_legibility_pass_tip1.py's own fuller amendment tests).
    console_js = (_STATIC / "console.js").read_text()
    body = console_js.split("function inspectOnly(id)", 1)[1][:1300]
    assert "if (space) space.focusObject(id);" in body
    assert "selectObject" not in body

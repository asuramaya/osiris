"""THE LEGIBILITY PASS, TIP 1b.
The adversarial review of the deployed tip 1 found nine flaws, all folded into
this tip alongside the click-focus/tree-to-source/local-relayout amendment (already landed
in the prior commit, covered by test_legibility_pass_tip1.py's own amendment section):

(1) focus did not actually hide the unreachable graph (nodes hid, the base EDGE layer never
rebuilt so unreachable edges stayed drawn); (2) an unbounded hub focus reached 20,266 nodes
in 3.1s; (3) the GPU pick was exact-pixel with no tolerance, two real clicks on visible
small nodes missed; (4) the canvas controls were still bottom-anchored on a freshly deployed
build (a stale-static-asset cache, the same class of bug this agent hit live-verifying an
earlier tip); (5) labels ignored the type-filter's own hidden types; (6) Commit labels read
"Commit: commit:<sha>" instead of the subject line; (7) the omnibox found nothing for a
real agent handle that plainly exists; (8) the hover card was empty on a real
hover (same root cause as #3); (9) focusObject(null) left a degenerate "focused: 1
reachable" status. Mirrors the existing static-source-guard convention; the CSS-cache fix
(#4) gets a real HTTP-level proof since it's server-side.
"""
from __future__ import annotations

from pathlib import Path

import httpx
from src.api.app import create_app

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_SPACE_JS = (_STATIC / "space.js").read_text()
_CONSOLE_JS = (_STATIC / "console.js").read_text()


# --- flaw #4: a /ui asset must force revalidation, never a silent stale reuse -------------

async def test_ui_static_assets_force_revalidation() -> None:
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/ui/space.js")
        assert r.status_code == 200
        assert r.headers.get("cache-control") == "no-cache"


# --- flaw #1: focus rebuilds the BASE edge layer too, not just node visibility ------------

def test_focus_rebuilds_the_base_edge_layer_not_just_node_visibility() -> None:
    focus_body = _SPACE_JS.split("function renderFocusEgoGroups(id, hopsUp, hopsDown)", 1)[1][:2000]
    assert "buildEdgeLines(idToNode, edges); // review flaw #1" in focus_body
    clear_body = _SPACE_JS.split("function clearFocus()", 1)[1][:900]
    assert "buildEdgeLines(idToNode, edges);" in clear_body


# --- flaw #2: the walk is rank-capped, a hub can never light the whole graph --------------

def test_the_walk_is_rank_capped_a_hub_focus_cannot_light_the_whole_graph() -> None:
    assert "const MAX_EGO_NODES = 300;" in _SPACE_JS
    body = _SPACE_JS.split("function bfsHops(adj, startId, depth)", 1)[1][:500]
    assert "hops.size < MAX_EGO_NODES" in body
    assert "if (hops.size >= MAX_EGO_NODES) break;" in body
    # the SAME cap must also bound the "never empty" structural fallback below -- a real
    # hub (repo:osiris, structural degree 20k+) has few/no PATH_EDGE_TYPES links of its own,
    # so the primary walk alone reaches only itself and this fallback is what actually runs.
    fallback_body = _SPACE_JS.split("if (pathReachable.size <= 1) {", 1)[1][:400]
    assert "if (pathReachable.size >= MAX_EGO_NODES) break;" in fallback_body


# --- flaw #3 / #8: GPU pick has real tolerance, fixing both click-miss and empty hover ----

def test_gpu_pick_has_tolerance_not_exact_pixel_only() -> None:
    assert "const PICK_BOX = 33;" in _SPACE_JS
    body = _SPACE_JS.split("function pickAt(clientX, clientY)", 1)[1][:1400]
    assert "let bestId = 0, bestDist = Infinity;" in body
    assert "if (dist < bestDist) { bestDist = dist; bestId = id; }" in body


# --- flaw #5: labels respect the type filter -----------------------------------------------

def test_labels_respect_the_type_filter() -> None:
    # THE LAST RENDERER: pickLabels' own candidacy pool now reuses
    # nodeVisible -- the same viewport filter that already checks hiddenNodeTypes (and, for
    # free, hiddenProjects/focus-reachability too), rather than a separate type-only filter.
    body = _SPACE_JS.split("function pickLabels()", 1)[1][:600]
    assert "nodeVisible(nd) &&" in body
    nv_body = _SPACE_JS.split("function nodeVisible(nd)", 1)[1][:300]
    assert "hiddenNodeTypes.has(nd.type)" in nv_body
    # the pool filter alone is not enough -- nothing re-picks labels when a filter changes
    # unless setHiddenTypes/the legend's own checkbox handler also calls scheduleLabelPick.
    sht_body = _SPACE_JS.split("function setHiddenTypes(types)", 1)[1][:250]
    assert "scheduleLabelPick();" in sht_body
    legend_body = _SPACE_JS.split("[data-legend-node-type]", 1)[1][:400]
    assert "scheduleLabelPick();" in legend_body


# --- flaw #7: the omnibox finds an agent by handle off the already-loaded graph -----------

def test_omnibox_falls_back_to_a_client_side_agent_handle_scan() -> None:
    # TIP 3 review carry-over: a plain n.label read silently missed every
    # node whose label wasn't already resolved on this snapshot -- fixed to read it the same
    # fallback-safe way space.js's own pickLabels/labelTextFor do (nd.label, else
    # `${type} ${id.slice(0,8)}`).
    body = _CONSOLE_JS.split("const graphHits = hits.filter", 1)[1][:2000]
    assert "n.type === 'Agent'" in body
    assert "n.label || `${n.type} ${n.id.slice(0, 8)}`" in body
    assert "OMNI_ITEMS = toolHits.concat(compHits, graphHits, agentHits).slice(0, 16);" in body


def test_omnibox_fallback_awaits_space_readiness_not_just_a_truthy_global() -> None:
    # TIP 1c re-fix: "the fallback did not fire on the deployed page" --
    # it gated on window.OsirisSpace, which is undefined until initSpace's own promise
    # resolves; a search typed before that settles found nothing and stayed that way.
    # TIP 3b replaced the two-sequential-awaits shape this test used to
    # assert (a `hits.length === 0` gate, then a second await/token-check pair) with one
    # Promise.all over both the server search and the readiness promise together -- see
    # test_legibility_pass_tip3b.py for the full rewrite; this still confirms the readiness
    # promise itself is part of that combined wait, not dropped in the simplification.
    body = _CONSOLE_JS.split("OMNI_SEARCH_TIMER = setTimeout(async () => {", 1)[1][:1600]
    assert "window.OsirisSpace ? Promise.resolve(window.OsirisSpace) : " \
        "(window.__spaceReady || Promise.resolve(null))" in body
    assert "if (myToken !== OMNI_SEARCH_TOKEN) return; " \
        "// a newer keystroke already superseded this" in body


# --- flaw #9: a null/undefined focus id never produces a degenerate focused state ---------

def test_focus_object_guards_against_a_null_id() -> None:
    body = _SPACE_JS.split("async function focusObject(id, opts)", 1)[1][:500]
    assert "if (!id) { clearFocus(); return; }" in body

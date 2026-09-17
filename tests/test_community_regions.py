"""WAVE 26, PIECE 2: COMMUNITY REGIONS (Thoth mail 11592/11664, thread 3683a12a). At mid
zoom inside a district, each community is a labelled region refined from the district
fill -- never replacing it. Reuses Khnum's own `community_code`/`communities` wire fields
(the SAME Leiden partition his compact-arrangement layout is already built on) rather than
re-deriving anything. Same-community edges draw as lines; cross-community edges (within one
district only -- a community never spans two) aggregate into a per-(community,community,
type) ribbon that resolves the same per-ribbon-screen-distance way district ribbons do.
Nothing hidden below mid zoom either: a same-district cross-community edge just reads as an
ordinary same-district line until the reader is zoomed in enough to see the refinement.
Mirrors the repo's existing static-source-guard convention: string/substring proofs against
the served JS, no browser harness.
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_SPACE_JS = (_STATIC / "space.js").read_text()


# --- wire: community_code/communities parsed exactly like project_aggregates -------------

def test_community_code_is_parsed_onto_every_node_off_the_wire() -> None:
    assert "communityCode: snap.community_code ? snap.community_code[i] : 0," in _SPACE_JS


def test_community_aggregates_resolve_the_project_name_off_the_same_table_districts_use() -> None:
    body = _SPACE_JS.split("const districtAggregates = ", 1)[1][:900]
    assert "const communityAggregates = (snap.communities || []).map((c) => ({" in body
    assert "code: c.community, districtName: snap.projects[c.project]," in body


def test_fetch_stream_snapshot_returns_community_aggregates_alongside_district_ones() -> None:
    body = _SPACE_JS.split("async function fetchStreamSnapshot()", 1)[1][:6000]
    assert "return { nodes, edges, edgeClassByType, districtAggregates, communityAggregates };" \
        in body


def test_init_space_wires_the_community_model_after_the_district_one() -> None:
    body = _SPACE_JS.split("await fetchStreamSnapshot();", 1)[1][:300]
    assert "buildDistrictModel(districtAggregates, edges);" in body
    assert "buildCommunityModel(communityAggregates);" in body


# --- the model: nested inside the district model, not a peer of it -----------------------

def test_build_community_model_indexes_by_code_and_gates_labels_by_size() -> None:
    assert "const COMMUNITY_LABEL_MIN_COUNT = 20;" in _SPACE_JS
    body = _SPACE_JS.split("function buildCommunityModel(communityAggregates)", 1)[1][:700]
    assert "communityByCode = new Map(communities.map((c) => [c.code, c]));" in body
    assert "communityLabelCandidates = communities" in body
    assert ".filter((c) => c.count >= COMMUNITY_LABEL_MIN_COUNT)" in body
    assert "computeCommunityZoomViewSize();" in body


def test_community_zoom_view_size_uses_the_median_radius_defence() -> None:
    # same "one outlier district dominates the extent" defence THE DRAWING TIP's own spike
    # used for its first (later replaced) ribbon threshold -- reused here because whole-view
    # community VISIBILITY genuinely is a single yes/no gate, unlike ribbon resolution.
    body = _SPACE_JS.split("function computeCommunityZoomViewSize()", 1)[1][:400]
    assert "const radii = communities.map((c) => c.radius).sort((a, b) => a - b);" in body
    assert "communityZoomViewSize = radii[Math.floor(radii.length / 2)] * 3;" in body


# --- fills: a distinct nested layer, z between the district fill and edges ---------------

def test_community_fills_sit_visually_nested_inside_the_district_fill() -> None:
    body = _SPACE_JS.split("function buildCommunityFills()", 1)[1][:900]
    assert 'const cc = new THREE.Color("#3a2f5f");' in body  # distinct from the district's #2a3f5f
    assert "mesh.position.set(c.cx, c.cy, -0.45);" in body  # between district (-0.5), edges (-0.1)
    assert "communityMeshGroup.visible = communityRegionsVisible;" in body


# --- ribbons: same-district only, cross-community, per-ribbon screen-distance resolve ----

def test_community_ribbons_are_scoped_to_a_single_district_never_cross_district() -> None:
    body = _SPACE_JS.split("function computeCommunityRibbons()", 1)[1][:900]
    assert "if (!na || !nb || na.project !== nb.project) continue; " \
        "// community ribbons are SAME-district only" in body
    assert "if (!ca || !cb || ca === cb) continue; " \
        "// same-community: drawn individually, like same-district" in body


def test_community_ribbon_resolve_reuses_the_same_screen_px_threshold_as_districts() -> None:
    body = _SPACE_JS.split("function computeResolvedCommunityRibbonKeys()", 1)[1][:600]
    assert "if (!communityRegionsVisible) return resolved; " \
        "// hidden entirely below mid zoom" in body
    assert "Math.hypot(pa.x - pb.x, pa.y - pb.y) > RIBBON_RESOLVE_SCREEN_PX" in body


def test_community_ribbon_lines_never_draw_below_mid_zoom() -> None:
    body = _SPACE_JS.split("function buildCommunityRibbonLines()", 1)[1][:700]
    assert "const unresolved = communityRegionsVisible" in body
    assert "? communityRibbons.filter((r) => !communityRibbonsResolvedKeys.has(ribbonKey(r)))" \
        in body
    assert ": []; // never drawn at all below mid zoom -- the plain same-district line " \
        "covers it" in body


def test_sync_community_visibility_only_rebuilds_when_something_actually_changed() -> None:
    # WAVE 27, THE LENS PANEL: the zoom gate is now AND-ed with the lens's own hide -- a
    # checkbox toggle re-invokes this same function directly (see tests/test_lens_panel.py).
    body = _SPACE_JS.split("function syncCommunityVisibility()", 1)[1][:1100]
    assert "communityRegionsVisible = !communitiesHiddenByLens &&\n      " \
        "communityZoomViewSize > 0 && viewSize < communityZoomViewSize;" in body
    assert "if (wasVisible === communityRegionsVisible && !resolvedChanged) return;" in body
    assert "buildEdgeLines(idToNode, edges);" in body
    assert "buildCommunityRibbonLines();" in body
    assert "if (wasVisible !== communityRegionsVisible) scheduleLabelPick();" in body


def test_sync_community_visibility_is_wired_into_both_zoom_and_fit() -> None:
    for call_site in (
        "syncRibbonResolve();\n    syncStorylineAxis();\n    syncCommunityVisibility();\n"
        "    markDirty();",
        "syncRibbonResolve();\n    syncStorylineAxis();\n    syncCommunityVisibility();\n"
        "    const t2 = window.__spaceWheelTiming",
    ):
        assert call_site in _SPACE_JS


# --- buildEdgeLines: a same-district cross-community edge refines one level further -------

def test_build_edge_lines_refines_same_district_edges_by_community_when_visible() -> None:
    body = _SPACE_JS.split("function buildEdgeLines(nodes, edgeList)", 1)[1][:2700]
    assert "} else if (communityRegionsVisible && na && nb && na.project === nb.project &&" \
        in body
    assert "na.communityCode && nb.communityCode && na.communityCode !== nb.communityCode) {" \
        in body
    assert "if (!communityRibbonsResolvedKeys.has(`${ca}|${cb}|${e.type}`)) return false;" \
        in body


# --- accounting: a new bucket, live only when communities are actually visible -----------

def test_edge_accounting_adds_a_community_ribbon_bucket_gated_on_visibility() -> None:
    body = _SPACE_JS.split("function edgeAccounting()", 1)[1][:2200]
    assert "let fill = 0, landmark = 0, line = 0, ribbon = 0, communityRibbon = 0, other = 0;" \
        in body
    assert "if (communityRegionsVisible && na && nb && na.project === nb.project &&" in body
    assert "if (communityRibbonsResolvedKeys.has(`${ca}|${cb}|${e.type}`)) line++; " \
        "else communityRibbon++;" in body
    assert "accounted: fill + landmark + line + ribbon + communityRibbon + other };" in body


# --- labels: share the SAME N_LABELS pool and real-width declutter, gated on visibility ---

def test_community_labels_only_compete_for_a_slot_once_communities_are_visible() -> None:
    body = _SPACE_JS.split("function pickLabels()", 1)[1][:2500]
    assert "const communityPool = communityRegionsVisible ? " \
        "communityLabelCandidates.filter(inView) : [];" in body
    assert "labeledNodes = pool.concat(districtPool, communityPool)" in body


def test_pseudo_nodes_get_their_own_priority_tier_never_crowded_out_by_object_degree() -> None:
    # live-verification finding: a district's own count (thousands) always happened to
    # beat an ordinary node's degree by luck, so it never needed special priority -- a
    # community's own count (order 10s-100s) does not have that luck, and the plain
    # degree-only sort silently crowded EVERY community label out (0 ever won a slot
    # against ordinary high-degree nodes sharing the same view). Measured live: 0 visible
    # community labels at a real osiris mid-zoom before this fix, non-zero after.
    body = _SPACE_JS.split("function pickLabels()", 1)[1][:2500]
    assert "const tier = (nd) => (isLit(nd) ? 2 : " \
        "(nd.__isDistrict || nd.__isCommunity) ? 1 : 0);" in body


def test_community_pseudo_nodes_get_their_own_css_class_and_text() -> None:
    body = _SPACE_JS.split("function pickLabels()", 1)[1][:3400]
    assert 'div.className = nd.__isDistrict ? "lbl project-label"\n' \
        '        : nd.__isCommunity ? "lbl community-label" : "lbl";' in body
    assert "div.textContent = (nd.__isDistrict || nd.__isCommunity)" in body


def test_community_pseudo_nodes_declutter_like_district_ones_never_lit() -> None:
    body = _SPACE_JS.split("function positionLabels()", 1)[1][:1500]
    assert "if (nd.__isDistrict || nd.__isCommunity) {" in body


def test_community_label_css_exists_in_both_pages() -> None:
    for html in ("index.html", "space.html"):
        page = (_STATIC / html).read_text()
        assert ".lbl.community-label {" in page


# --- debug API: live-verification hooks -----------------------------------------------------

def test_debug_api_exposes_community_region_live_verification_hooks() -> None:
    body = _SPACE_JS.split("const api = {", 1)[1]
    assert "get communities()" in body
    assert "get communityRegionsVisible()" in body
    assert "get communityRibbons()" in body
    assert "get communityRibbonsResolvedKeys()" in body
    assert "get communityLabelCandidateCount()" in body
    assert "get communityZoomViewSize()" in body
    assert "get communityRegionsDrawn()" in body

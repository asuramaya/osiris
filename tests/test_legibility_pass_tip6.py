"""THE RENDERER, REOPENED FOR FOCUS AND PREVIEW (operator ruling, grounds 5b37d219, Thoth
mail 11272). THE LAST RENDERER (ruling d7d55257) froze the renderer after w301; this
ruling reopens it specifically for focus/preview, not a blanket reversal -- the base dim
layer's own "no bundling, straight lines only" rule (mail 11066) stays exactly as it was.

Measured defect that triggered the ruling: focus walked PATH_EDGE_TYPES only, so focusing
a real 402-degree agent (Sekhmet) reached 43 nodes over succeeded_from/succeeds_seat and
nothing else -- the operator saw a wall of same-named labels and never what the agent
actually did.

Live-verified via claude-in-chrome against the deployed graph (main fc9e1b47): focusing
that exact agent went from 43 (path-only) to 60 reachable (43 base + 17 direct one-hop
members), with one "spawned_by (in) 381" group node (paged on click) and one container
anchor pulled out ("analyst:operator"). Expanding the group added 50 more (110 total),
correctly laid out as a tight, ordered, de-overlapped chain instead of the wide horizontal
smear a naive physics seed produced on the first attempt (caught live, fixed before
shipping). The SAME chain-compression fix also closed the true root cause of "the camera
does not refit to something legible": the pre-existing ranked-column path layout itself
spread a real 43-generation succession chain across a 544,433-world-unit span using the
full column width every hop; compressed to an 85,028-unit span (6.4x) once succession-only
ranks use the tight chain spacing too. Ring/halo, pinned larger label, and inspector header
all confirmed visible together in the same screenshot; hover card confirmed showing
"Agent · repo:osiris · gen 43" for that same node. Bundled quadratic curves confirmed via
geometry inspection (56 vertices = 2 curved edges x 28 verts each, from
BUNDLE_CURVE_SEGMENTS=14 x 2) and a screenshot showing genuinely bowed (not straight) lines.

Mirrors the repo's existing static-source-guard convention: string/substring proofs
against the served JS, no browser harness.
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_SPACE_JS = (_STATIC / "space.js").read_text()


# --- item 1: one-hop-all-types neighbourhood, grouped, paged, container anchors -----------

def test_one_hop_by_type_direction_scans_all_edge_types_both_directions() -> None:
    body = _SPACE_JS.split("function oneHopByTypeDirection(id)", 1)[1][:900]
    assert 'direction = "out";' in body
    assert 'direction = "in";' in body
    # a container-class neighbour is pulled OUT of the type/direction grouping entirely --
    # one anchor each, never bucketed.
    assert "if (isContainerFocus(otherId)) { containerNeighbors.set(otherId, nd); " \
        "continue; }" in body


def test_small_buckets_place_directly_large_buckets_page_like_the_container_drill() -> None:
    body = _SPACE_JS.split("function buildEgoGroups(id, center)", 1)[1][:3900]
    assert "if (members.length <= DRILL_PAGE_SIZE && members.length <= budgetLeft) {" in body
    assert "const take = Math.min(ranked.length, DRILL_PAGE_SIZE * egoGroupPageCount, " \
        "Math.max(0, budgetLeft));" in body
    assert 'egoGroupEntries.push({ key: `more:${key}`, kind: "more"' in body


def test_focus_base_path_reachable_is_the_unchanged_provenance_walk() -> None:
    # the ORIGINAL PATH_EDGE_TYPES walk stays the base set; one-hop groups are additive.
    body = _SPACE_JS.split("async function focusObject(id, opts)", 1)[1][:4300]
    assert "focusBasePathReachable = new Set(pathReachable);" in body
    assert "renderFocusEgoGroups(id, hopsUp, hopsDown);" in body


def test_group_expand_click_never_wipes_its_own_just_set_state() -> None:
    # the SAME bug class caught in THE DRILL (mail 11241): a click handler sets
    # egoGroupExpandedKey/egoGroupPageCount then re-renders through the SAME shared path a
    # fresh focus uses -- renderFocusEgoGroups itself must never reset that state (only
    # focusObject, on an actual container change, does).
    body = _SPACE_JS.split("function focusObject(id, opts)", 1)[1][:3700]
    assert "if (egoGroupFocusId !== id) { egoGroupExpandedKey = null; " \
        "egoGroupPageCount = 1; }" in body
    render_body = _SPACE_JS.split("function renderFocusEgoGroups(id, hopsUp, hopsDown)", 1)[1][:400]
    assert "egoGroupExpandedKey = null" not in render_body
    assert "egoGroupPageCount = 1" not in render_body


def test_ego_group_debug_hooks_exist_for_live_verification() -> None:
    body = _SPACE_JS.split("const api = {", 1)[1]
    assert "oneHopByTypeDirection," in body
    assert "get egoGroupEntries()" in body
    assert "get egoContainerAnchorEntries()" in body
    assert "expandEgoGroup(key)" in body


# --- item 2: unmistakable focus -- ring/halo, pinned larger label, guaranteed refit -------

def test_focus_ring_is_a_real_overlay_sized_off_the_nodes_own_screen_radius() -> None:
    assert 'focusRingEl.className = "focus-ring";' in _SPACE_JS
    body = _SPACE_JS.split("function positionFocusRing()", 1)[1][:700]
    assert "const nd = pathFocusId ? idById.get(pathFocusId) : null;" in body
    assert "if (!nd) { focusRingEl.hidden = true; return; }" in body
    for html in ("index.html", "space.html"):
        page = (_STATIC / html).read_text()
        assert ".focus-ring {" in page


def test_focus_label_is_bigger_than_a_merely_lit_label() -> None:
    for html in ("index.html", "space.html"):
        page = (_STATIC / html).read_text()
        assert ".lbl.focus-label { font-size: 13px" in page
    body = _SPACE_JS.split("function positionLabels()", 1)[1][:3800]
    assert '(nd.id === pathFocusId ? " focus-label" : "")' in body
    assert "positionFocusRing();" in body


def test_every_focus_and_every_group_click_refits_the_camera() -> None:
    # live-verified regression: refitting only lived inside the original focusObject body;
    # a group/"more" click re-rendered through a path that never touched the camera. Now
    # the fit lives inside renderFocusEgoGroups, the ONE shared render path both use.
    body = _SPACE_JS.split("function renderFocusEgoGroups(id, hopsUp, hopsDown)", 1)[1][:1600]
    assert "for (const rid of pathReachable) {" in body
    assert "camera.position.x = (minX + maxX) / 2;" in body


def test_succession_only_ranks_use_the_tight_chain_width_not_the_full_column() -> None:
    # live-verified root cause of "the camera does not refit to something legible": a real
    # 43-generation succession chain in the ORIGINAL ranked-column path layout spanned
    # 544,433 world units at the full EGO_COL_SPACING_PX per hop; 85,028 (6.4x tighter)
    # once a pure single-file succession run uses CHAIN_SPACING_PX instead.
    body = _SPACE_JS.split(
        "function applyEgoLayout(focusId, hopsUp, hopsDown, extraSeed)", 1)[1][:4900]
    assert "const successionAdj = new Map();" in body
    assert "x += side * (isChainStep ? chainW : colW);" in body
    assert "const fixedIds = new Set([focusId, ...chainRankIds]);" in body


# --- item 3: hover card matches the label exactly, plus type/project/generation -----------

def test_hover_card_and_label_read_off_the_identical_identity_string() -> None:
    # labelTextFor(nd) is the SAME call pickLabels' own div.textContent uses -- label and
    # card were already structurally incapable of disagreeing before this tip; generation
    # is the new piece.
    body = _SPACE_JS.split("function updateHoverCard(nd)", 1)[1][:500]
    assert "${labelTextFor(nd)}" in body
    assert "const generation = computeGeneration(nd);" in body
    assert '" · chain depth " + generation' in body


def test_generation_is_hops_back_a_succeeded_from_chain_null_if_not_in_one() -> None:
    body = _SPACE_JS.split("function computeGeneration(nd)", 1)[1][:900]
    assert 'if (!succeededFromMembers.has(nd.id)) return null;' in body
    assert "return count + 1;" in body


# --- item 4: succession chains render as an ordered, de-overlapped chain ------------------

def test_succession_chain_gets_a_real_generation_order_not_a_radial_fan() -> None:
    body = _SPACE_JS.split("function orderSuccessionChain(focusId, members, edgeType)", 1)[1][:1400]
    assert "const dist = new Map([[focusId, 0]]);" in body
    assert ".sort((a, b) => dist.get(a.id) - dist.get(b.id));" in body


def test_chain_members_are_pinned_never_relaxed_back_into_a_smear() -> None:
    # live-verified: seeding a chain in order then letting the SAME O(n^2) repulsion that
    # spreads a wide rank run over it just as happily unfolds the chain back into the wide
    # smear this fix exists to prevent. pinned: true routes into applyEgoLayout's own
    # fixedIds set, which relaxPositions now accepts as a Set (not just one id).
    body = _SPACE_JS.split("function buildEgoGroups(id, center)", 1)[1][:3900]
    assert "pinned: true" in body
    relax_body = _SPACE_JS.split(
        "function relaxPositions(seed, springs, fixedId)", 1)[1][:1500]
    assert "if (id === fixedId || (fixedId instanceof Set && fixedId.has(id))) " \
        "continue;" in relax_body


def test_chain_spacing_is_tighter_than_the_ordinary_rank_column_width() -> None:
    assert "const CHAIN_SPACING_PX = 22;" in _SPACE_JS  # < EGO_COL_SPACING_PX (150)
    assert 'const SUCCESSION_EDGE_TYPES = new Set(["succeeded_from", ' \
        '"succeeds_seat"]);' in _SPACE_JS


# --- item 5: cross-cluster focus edges over a screen-px threshold bundle as curves --------

def test_only_the_focus_overlay_bundles_the_base_dim_layer_stays_straight() -> None:
    # THE LAST RENDERER's own "no bundling, straight lines only" rule (mail 11066) is
    # UNCHANGED for buildEdgeLines (the base dim layer) -- this reopening is scoped to
    # updatePathEdges (the focus overlay) only, per the new ruling's own grounds.
    base_body = _SPACE_JS.split("function buildEdgeLines(nodes, edgeList)", 1)[1][:2500]
    assert "BUNDLE_CURVE_SEGMENTS" not in base_body
    assert "quadratic" not in base_body.lower()


def test_cross_cluster_edges_over_the_screen_px_threshold_bundle_as_curves() -> None:
    body = _SPACE_JS.split("function updatePathEdges()", 1)[1][:3000]
    assert "const crossProject = a.project && b.project && a.project !== b.project;" in body
    assert "if (!crossProject || screenLen <= BUNDLE_SCREEN_PX_THRESHOLD) {" in body
    assert "const ctrlX = midX + nx * bow, ctrlY = midY + ny * bow;" in body
    assert body.count("BUNDLE_CURVE_SEGMENTS") >= 2  # the const, and the loop bound


def test_bundle_alpha_falls_with_length_never_hits_true_zero() -> None:
    body = _SPACE_JS.split("function updatePathEdges()", 1)[1][:3000]
    assert "const alpha = Math.max(BUNDLE_ALPHA_FLOOR, 1 / (1 + over));" in body
    assert "const BUNDLE_ALPHA_FLOOR = 0.15;" in _SPACE_JS


def test_bundle_threshold_is_measured_in_real_screen_pixels_not_world_units() -> None:
    # a "long" edge depends on the CURRENT zoom, not a fixed world distance -- screenLen
    # divides the world distance by the live worldPerPx(), same convention nodeScreenPx
    # and the wheel/render instrumentation already use.
    body = _SPACE_JS.split("function updatePathEdges()", 1)[1][:900]
    assert "const wpp = worldPerPx();" in body
    assert "const screenLen = worldLen / wpp;" in body


# --- w306 review BLOCKER (Thoth mail 11308): focus-set physics diverges to a non-finite --
# camera on a real high-degree Thread/Decision -------------------------------------------

def test_repulsion_has_a_real_minimum_separation_not_a_1_unit_floor() -> None:
    # live-verified root cause: buildEgoGroups' own angle-only fan at a constant radius put
    # genuinely different members at the exact same (x,y) once a bucket's own angular
    # spread wrapped past 2*PI (a real 154-member Message bucket, or repeated "more" clicks
    # growing `take` past ~79 at the old 0.08 rad step) -- dozens of exactly-coincident
    # pairs each computing a force under the old d2=max(d2,1) floor summed to an enormous
    # single-iteration displacement, compounding over 180 iterations into non-finite
    # territory. EGO_MIN_SEP2=400 bounds any ONE pair's force to EGO_REPULSION/400=8.
    assert "const EGO_MIN_SEP2 = 400;" in _SPACE_JS
    body = _SPACE_JS.split("function relaxPositions(seed, springs, fixedId)", 1)[1][:900]
    assert "if (d2 < EGO_MIN_SEP2) d2 = EGO_MIN_SEP2;" in body


def test_per_iteration_displacement_is_capped_regardless_of_pileup_size() -> None:
    # a hard ceiling independent of the min-separation fix above -- even a bounded PER-PAIR
    # force can still sum to something enormous if enough pairs pile onto one point in the
    # same iteration; this caps the TOTAL displacement a single node can take in one step.
    assert "const EGO_MAX_DISPLACEMENT = 400;" in _SPACE_JS
    body = _SPACE_JS.split("function relaxPositions(seed, springs, fixedId)", 1)[1][:1700]
    assert "if (mag > EGO_MAX_DISPLACEMENT) {" in body


def test_a_diverged_relax_falls_back_to_the_finite_pre_relax_seed() -> None:
    # the last-resort net: if positions STILL go non-finite despite the two guards above,
    # feed the camera fit the original (always-finite, pure arithmetic) seed instead of
    # NaN/Infinity -- what actually produced the black-canvas symptom live.
    body = _SPACE_JS.split("function relaxedOrSeed(seed, relaxed)", 1)[1][:400]
    assert "if (!Number.isFinite(p.x) || !Number.isFinite(p.y)) return seed;" in body
    for fn in ("function applyEgoLayout(focusId, hopsUp, hopsDown, extraSeed)",
               "async function renderContainerDrill(id, opts)"):
        call_body = _SPACE_JS.split(fn, 1)[1][:5700]
        assert "relaxedOrSeed(seed, relaxPositions(seed, springs," in call_body


def test_group_seeding_spirals_instead_of_a_constant_radius_circle() -> None:
    # structural defence alongside the physics-side fixes: a growing radius per index means
    # no two members can land at the exact same seed regardless of how large a bucket or a
    # repeatedly-"more"-clicked page gets.
    body = _SPACE_JS.split("function buildEgoGroups(id, center)", 1)[1][:4300]
    assert "const r2 = ringR * 1.3 + k * 2;" in body
    assert "const r2 = ringR + i * 2;" in body


# --- w306 review: chain labels still walled up, expansion left the table stale -----------

def test_chain_labels_declutter_past_generation_1_not_every_single_one() -> None:
    # live-verified: "lit labels always win their spot" flooded the view once a real
    # succession chain (up to 50+ pinned members, ALL lit since they're all in
    # pathReachable) tried to show every single one at once. A chain member keeps the
    # always-shown guarantee only at generation 1 or a multiple of 5. WAVE 26's own
    # storyline fix (test_storyline.py) narrowed the "always shown" bypass itself from
    # bare `lit` to `alwaysShown` (lit AND NOT storylineActive) -- the chain-generation
    # exception below is unchanged, still keyed off the same `lit`/`generation` values.
    body = _SPACE_JS.split("function positionLabels()", 1)[1][:3300]
    assert "const generation = lit ? computeGeneration(nd) : null;" in body
    assert "const chainDeclutters = generation != null && generation !== 1 " \
        "&& generation % 5 !== 0;" in body
    assert "if ((!alwaysShown || chainDeclutters) && overlapsPlaced(x, y, w))" in body


def test_group_expansion_notifies_the_table_not_just_the_initial_focus() -> None:
    # live-verified: onFocus (console.js's own onSpaceFocus -> renderEntityExplorerStage ->
    # hydrateFocusReachable) used to fire only from focusObject's own initial call; a
    # group/"more" click re-renders through renderFocusEgoGroups directly and never told
    # the table pathReachable had grown, so it stayed at the pre-expansion row count.
    # Live-verified after the fix: table rows == pathReachable.size (281 == 281) after a
    # real expansion, not stuck at the pre-expansion count.
    body = _SPACE_JS.split("function renderFocusEgoGroups(id, hopsUp, hopsDown)", 1)[1][:900]
    assert "if (onFocus) onFocus(id);" in body


# --- w312 review (mail 11359): agent focus wrongly drilled, label pool ignored focus,
# hover-card generation disagreed with the label's own, no-op group expansions ------------

def test_only_membership_container_types_take_the_drill() -> None:
    # a busy Agent seat's own structural degree can exceed MAX_EGO_NODES the same way a
    # real project's membership degree does now that spawned_by is structural (mail 11291)
    # -- the drill must never eat a non-membership focus regardless of degree.
    body = _SPACE_JS.split("function isContainerFocus(id)", 1)[1][:400]
    assert 'const CONTAINER_FOCUS_TYPES = new Set(["SoftwareProject", "Seat"]);' \
        in _SPACE_JS.split("function isContainerFocus(id)", 1)[0][-500:]
    assert "if (!nd || !CONTAINER_FOCUS_TYPES.has(nd.type)) return false;" in body


def test_label_pool_fills_lit_nodes_before_ranking_by_degree() -> None:
    # top-N-by-degree alone ignores the focus: a real ego focus is mostly low-natural-degree
    # nodes (Message, chain members), so the old sort filled the pool with unrelated
    # high-degree nodes elsewhere in the viewport instead of what was actually focused.
    body = _SPACE_JS.split("function pickLabels()", 1)[1][:2500]
    assert "const isLit = (nd) => nd.id === pathFocusId || pathReachable.has(nd.id) || " \
        "nd.id === selectedId;" in body
    # WAVE 26's own tier() wraps isLit -- lit still ranks strictly highest (tier 2), the
    # sort's own outcome for a lit node is unchanged; see test_community_regions.py for
    # the tier() rework itself.
    assert "tier(b) - tier(a) || (b.degree || 0) - (a.degree || 0)" in body


def test_hover_card_generation_is_named_honestly_not_claimed_as_the_labels_own() -> None:
    # computeGeneration is a succeeded_from hop count, not the seat's own generation
    # numeral the label string carries (Khnum's roman numeral) -- the two can genuinely
    # disagree, so the card names what it actually measures instead of "gen".
    body = _SPACE_JS.split("function updateHoverCard(nd)", 1)[1][:500]
    assert '" · chain depth " + generation' in body
    assert '" · gen "' not in _SPACE_JS


def test_group_offers_are_filtered_to_members_not_already_reachable() -> None:
    # oneHopByTypeDirection walks ALL edges touching id, including ones the original
    # PATH_EDGE_TYPES walk already reached -- a group entirely made of already-reachable
    # members is a dead click (expanding it never grows pathReachable) and must never be
    # offered at all.
    body = _SPACE_JS.split("function buildEgoGroups(id, center)", 1)[1][:1600]
    assert "const members = groups.get(key).filter((nd) => " \
        "!focusBasePathReachable.has(nd.id));" in body
    assert "if (members.length === 0) continue;" in body

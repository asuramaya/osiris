"""WAVE 27, THE LENS PANEL (Thoth mail 11754): the Legend gains a real lens -- edge classes
(semantic / structural / container, container now kept distinct instead of normalizing to
structural), communities and high-degree objects (the old "landmark" badges -- retired noun,
operator ruling 52a59652), each switchable on/off per class. "A reader's lens, never a
default hide": every new toggle defaults to shown/checked, same convention the existing
node-type/edge-class/edge-type checkboxes already use. State lives on the URL hash so a view
is shareable -- read once at load, written after every toggle. Mirrors the repo's existing
static-source-guard convention: string/substring proofs against the served JS, no browser
harness.
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_SPACE_JS = (_STATIC / "space.js").read_text()


# --- container: its own distinct edge class now, not folded into structural ---------------

def test_container_class_is_kept_distinct_not_normalized_to_structural() -> None:
    assert 'if (cls === "container") cls = "structural";' not in _SPACE_JS


def test_is_structural_like_treats_structural_and_container_alike() -> None:
    body = _SPACE_JS.split("function isStructuralLike(edgeClass)", 1)[1][:200]
    assert 'edgeClass === "structural" || edgeClass === "container";' in body


def test_container_focus_membership_walk_uses_is_structural_like() -> None:
    # THE MEMBERSHIP-CLASS FIX's own container-focus detection, and containerMembersByType,
    # and focusObject's own one-hop structural-widen fallback -- all three used to check
    # `e.edgeClass !== "structural"` directly; a genuine container-class edge (now distinct)
    # must still count as membership-shaped for all three, unchanged from before this tip.
    for anchor in (
        "function containerMembersByType(id)",
        "function isContainerFocus(id)",
    ):
        body = _SPACE_JS.split(anchor, 1)[1][:700]
        assert "if (!isStructuralLike(e.edgeClass)) continue;" in body
    focus_body = _SPACE_JS.split("async function focusObject(id, opts)", 1)[1][:2900]
    assert "if (!isStructuralLike(e.edgeClass)) continue;" in focus_body


# --- the legend: container gets its own class row, alongside semantic/structural ----------

def test_legend_lists_a_container_class_row_alongside_semantic_and_structural() -> None:
    body = _SPACE_JS.split("function renderLegend(edgeList, nodeList)", 1)[1][:2500]
    assert 'const byClass = { semantic: [], structural: [], container: [] };' in body
    assert 'classRow("container") + (byClass.container || []).map(typeRow).join("")' in body


# --- the two new lens rows: default shown, checkbox unchecked = hidden --------------------

def test_legend_gains_communities_and_high_degree_lens_rows() -> None:
    body = _SPACE_JS.split("function renderLegend(edgeList, nodeList)", 1)[1][:2900]
    assert 'lensRow("communities", "communities", !communitiesHiddenByLens)' in body
    assert 'lensRow("highDegree", "high-degree objects", !highDegreeBadgesHiddenByLens)' \
        in body


def test_lens_row_checkbox_unchecked_means_hidden_checked_means_shown() -> None:
    body = _SPACE_JS.split("const lensRow = (key, label, checked) =>", 1)[1][:300]
    assert '${checked ? "checked" : ""}' in body


def test_communities_lens_toggle_reinvokes_sync_community_visibility() -> None:
    body = _SPACE_JS.split('if (key === "communities") {', 1)[1][:150]
    assert "communitiesHiddenByLens = !el.checked;" in body
    assert "syncCommunityVisibility();" in body


def test_high_degree_lens_toggle_rebuilds_badges_and_edge_lines() -> None:
    body = _SPACE_JS.split('} else if (key === "highDegree") {', 1)[1][:200]
    assert "highDegreeBadgesHiddenByLens = !el.checked;" in body
    assert "buildLandmarkBadges();" in body
    assert "buildEdgeLines(idToNode, edges);" in body


def test_lens_toggle_writes_the_hash_directly_not_only_through_render_legend() -> None:
    # live-caught regression: syncCommunityVisibility() bails out before ever reaching
    # buildEdgeLines/renderLegend's own writeLensStateToHash call when this graph has zero
    # communities at all (communities.length === 0) -- the communities checkbox's own state
    # change never reached the hash. Fixed by writing the hash directly in the handler,
    # not solely relying on the indirect renderLegend path every other checkbox uses.
    body = _SPACE_JS.split("legendPanel.querySelectorAll(\"[data-legend-lens]\")", 1)[1][:1000]
    assert "writeLensStateToHash();" in body


# --- wiring: community visibility gate now ANDs in the lens ------------------------------

def test_community_regions_visible_is_gated_by_the_lens_too() -> None:
    body = _SPACE_JS.split("function syncCommunityVisibility()", 1)[1][:700]
    assert "communityRegionsVisible = !communitiesHiddenByLens &&\n      " \
        "communityZoomViewSize > 0 && viewSize < communityZoomViewSize;" in body


def test_landmark_badges_build_nothing_at_all_when_the_lens_hides_them() -> None:
    body = _SPACE_JS.split("function buildLandmarkBadges()", 1)[1][:700]
    assert "if (highDegreeBadgesHiddenByLens) return;" in body


# --- accounting stays exact under every lens state (the receipt's own acceptance line) ----

def test_hidden_high_degree_badge_edges_count_as_lines_not_landmarks() -> None:
    body = _SPACE_JS.split("function edgeAccounting()", 1)[1][:700]
    assert "if (highDegreeBadgesHiddenByLens) { line++; } else { landmark++; }" in body
    assert "accounted: fill + landmark + line + ribbon + communityRibbon + other };" in \
        _SPACE_JS.split("function edgeAccounting()", 1)[1][:2200]


def test_hidden_high_degree_badge_edges_draw_as_ordinary_lines() -> None:
    # never routed through district/community ribbon aggregation -- computeRibbons excludes
    # landmark-type edges unconditionally, badge shown or not, so falling through to that
    # swap here would misclassify (and potentially never-draw) them.
    body = _SPACE_JS.split("function buildEdgeLines(nodes, edgeList)", 1)[1][:1500]
    assert "if (lm && e.target === lm.id) {" in body
    assert "if (!highDegreeBadgesHiddenByLens) return false;" in body
    assert "return !hiddenEdgeClasses.has(e.edgeClass) && !hiddenEdgeTypes.has(e.type) &&\n" \
        "          nodeVisible(byId.get(e.source)) && nodeVisible(byId.get(e.target));" in body


# --- URL hash: read once at load, written after every toggle, round-trips -----------------

def test_lens_state_is_read_from_the_hash_before_the_first_build_scene() -> None:
    body = _SPACE_JS.split("await fetchStreamSnapshot();", 1)[1][:300]
    assert "applyLensStateFromHash();" in body
    assert body.index("applyLensStateFromHash();") < body.index("buildDistrictModel(")


def test_write_lens_state_serializes_every_toggle_with_sorted_arrays() -> None:
    body = _SPACE_JS.split("function writeLensStateToHash()", 1)[1][:700]
    assert "hiddenNodeTypes: [...hiddenNodeTypes].sort()," in body
    assert "hiddenEdgeClasses: [...hiddenEdgeClasses].sort()," in body
    assert "hiddenEdgeTypes: [...hiddenEdgeTypes].sort()," in body
    assert "hideCommunities: communitiesHiddenByLens," in body
    assert "hideHighDegree: highDegreeBadgesHiddenByLens," in body


def test_write_lens_state_uses_history_replace_state_not_a_navigation() -> None:
    # a lens toggle is not a back-button-worthy navigation event -- replaceState keeps the
    # hash current without growing browser history one entry per checkbox click.
    body = _SPACE_JS.split("function writeLensStateToHash()", 1)[1][:700]
    assert 'history.replaceState(null, "", `#${params.toString()}`);' in body


def test_apply_lens_state_restores_every_field_by_mutating_in_place() -> None:
    # mutates the existing Sets in place (clear + add) rather than reassigning them -- every
    # other function in this file reads hiddenEdgeClasses/hiddenEdgeTypes/hiddenNodeTypes by
    # closure variable, not a captured reference, so this is safe either way, but in-place
    # mutation needed no const-to-let change anywhere else in the file.
    body = _SPACE_JS.split("function applyLensStateFromHash()", 1)[1][:700]
    assert "hiddenNodeTypes.clear();" in body
    assert "hiddenEdgeClasses.clear();" in body
    assert "hiddenEdgeTypes.clear();" in body
    assert "communitiesHiddenByLens = !!state.hideCommunities;" in body
    assert "highDegreeBadgesHiddenByLens = !!state.hideHighDegree;" in body


def test_read_lens_state_never_throws_on_malformed_hash_json() -> None:
    body = _SPACE_JS.split("function readLensStateFromHash()", 1)[1][:300]
    assert "try {" in body
    assert "return JSON.parse(raw);" in body
    assert "} catch {" in body
    assert "return null;" in body


def test_lens_hash_round_trips_all_five_keys_symmetrically() -> None:
    # Thoth mail 11981: hiddenNodeTypes was written to the hash but never restored on load --
    # the write side and the read side had silently drifted apart. This locks the two
    # functions' own key lists together so that kind of drift fails a test instead of a
    # live tab: every key writeLensStateToHash serializes must be one applyLensStateFromHash
    # reads back, and vice versa.
    write_body = _SPACE_JS.split("function writeLensStateToHash()", 1)[1][:700]
    apply_body = _SPACE_JS.split("function applyLensStateFromHash()", 1)[1][:700]
    written_keys = {
        "hiddenNodeTypes: [...hiddenNodeTypes].sort(),": "state.hiddenNodeTypes",
        "hiddenEdgeClasses: [...hiddenEdgeClasses].sort(),": "state.hiddenEdgeClasses",
        "hiddenEdgeTypes: [...hiddenEdgeTypes].sort(),": "state.hiddenEdgeTypes",
        "hideCommunities: communitiesHiddenByLens,": "state.hideCommunities",
        "hideHighDegree: highDegreeBadgesHiddenByLens,": "state.hideHighDegree",
    }
    for written, restored in written_keys.items():
        assert written in write_body, f"written but never read back: {written}"
        assert restored in apply_body, f"written but never read back: {restored}"


def test_set_hidden_types_mutates_in_place_like_the_hash_restore_path_does() -> None:
    # THE LENS PANEL hash-restore bug (mail 11981): the header taxonomy pills drive the same
    # hiddenNodeTypes Set through setHiddenTypes, which used to reassign it wholesale
    # (`hiddenNodeTypes = new Set(...)`) instead of mutating in place -- the one place this
    # file broke its own applyLensStateFromHash/hiddenEdgeClasses/hiddenEdgeTypes convention.
    body = _SPACE_JS.split("function setHiddenTypes(types)", 1)[1][:400]
    assert "hiddenNodeTypes = new Set(types" not in body
    assert "hiddenNodeTypes.clear();" in body
    assert "for (const t of types || []) hiddenNodeTypes.add(t);" in body


def test_hashchange_reapplies_lens_state_and_rebuilds_every_dependent_view() -> None:
    # THE LENS PANEL hash-restore bug, root cause (Thoth mail 11981/12052): navigating to the
    # same page with a different #lens fragment is a same-document hash navigation -- no
    # reload, so applyLensStateFromHash's own once-at-load call (above) never re-runs, and the
    # new hash's state was silently ignored until the next manual toggle overwrote it with
    # stale in-memory state. A hashchange listener must reapply the hash and rebuild
    # everything a toggle already rebuilds: dimming, edge geometry (which also re-renders the
    # legend's own checkboxes), label picking, and both lens gates.
    body = _SPACE_JS.split('window.addEventListener("hashchange"', 1)[1][:400]
    assert "applyLensStateFromHash();" in body
    assert "applyDim();" in body
    assert "buildEdgeLines(idToNode, edges);" in body
    assert "scheduleLabelPick();" in body
    assert "syncCommunityVisibility();" in body
    assert "buildLandmarkBadges();" in body


def test_lens_state_is_a_single_named_hash_param_not_the_whole_hash() -> None:
    # shares the hash with any other future hash consumer -- URLSearchParams over the hash
    # string, one param, never a bare `location.hash = ...` overwrite.
    assert 'const LENS_HASH_PARAM = "lens";' in _SPACE_JS
    body = _SPACE_JS.split("function writeLensStateToHash()", 1)[1][:700]
    assert 'const params = new URLSearchParams(location.hash.replace(/^#/, ""));' in body
    assert "params.set(LENS_HASH_PARAM, JSON.stringify(state));" in body


# --- debug API: live-verification hooks ----------------------------------------------------

def test_debug_api_exposes_lens_panel_live_verification_hooks() -> None:
    body = _SPACE_JS.split("const api = {", 1)[1]
    assert "isStructuralLike," in body
    assert "get communitiesHiddenByLens()" in body
    assert "get highDegreeBadgesHiddenByLens()" in body
    assert "get landmarkBadgeEntryCount()" in body
    assert "get lensHashParam()" in body
    assert "readLensStateFromHash, applyLensStateFromHash," in body

"""THE DRAWING TIP (Thoth mail 11408, operator ruling 4a51cab1/1178e7d9, thread 325ef660):
"nothing hidden, nothing drawn twice" -- membership (in_repo/works_in/holds/member_of, plus
Sekhmet's new owned_by) is a fill (a project fill, never a spoke), the two universal
fans (acts_for -> the operator Person, authored_by -> the git identity) are HIGH-DEGREE
OBJECTS with a count, every other edge draws at rest: same-project as a line, cross-project
aggregated into a per-(projectA,projectB,type) ribbon that resolves to individual lines once
that SPECIFIC ribbon's own two project centroids are far enough apart on screen (not one
global viewSize scalar -- the flaw the spike's own single median-radius threshold had).
Project labels earn a slot in the SAME N_LABELS pool/declutter pass object labels already
use, gated by project size; a project below that gate stays an unlabeled fill (the "other"
wash). Builds on two numbers-first spikes (Seshat 57992143, Khnum 5f6c4db3) that decided the
build per the operator's own ruling. Mirrors the repo's existing static-source-guard
convention: string/substring proofs against the served JS, no browser harness.

WAVE 28, THE TAXONOMY SWEEP (ruling 70c001ec): district -> project, landmark/hub ->
high-degree object, renamed here in step with space.js itself.
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_SPACE_JS = (_STATIC / "space.js").read_text()


# --- item 1: membership is a project fill, never a line -------------------------------------

def test_project_fill_types_are_the_five_membership_link_types() -> None:
    assert 'const PROJECT_FILL_TYPES = new Set(["in_repo", "works_in", "holds", ' \
        '"member_of", "owned_by"]);' in _SPACE_JS


def test_build_edge_lines_never_draws_a_project_fill_type_as_a_line() -> None:
    body = _SPACE_JS.split("function buildEdgeLines(nodes, edgeList)", 1)[1][:1200]
    assert "if (PROJECT_FILL_TYPES.has(e.type)) return false;" in body


def test_project_fills_draw_every_project_as_a_region_behind_everything() -> None:
    body = _SPACE_JS.split("function buildProjectFillMeshes()", 1)[1][:900]
    assert "mesh.position.set(d.cx, d.cy, -0.5);" in body  # behind edges and nodes


# --- item 2: cross-project edges are ribbons, resolving PER RIBBON on-screen ----------------

def test_ribbons_aggregate_by_project_pair_and_type_excluding_fills_and_high_degree() -> None:
    body = _SPACE_JS.split("function computeRibbons()", 1)[1][:900]
    assert "if (PROJECT_FILL_TYPES.has(e.type)) continue;" in body
    assert "if (lm && e.target === lm.id) continue;" in body
    assert 'na.project === nb.project) continue; // same-project: drawn individually' in body


def test_ribbon_resolve_is_per_ribbon_screen_distance_not_one_global_scalar() -> None:
    # the spike's own flaw (Thoth mail 11408 item 2): a single median-radius viewSize
    # threshold flipped every ribbon at once. Each ribbon's own two project centroids are
    # now projected to real screen pixels and compared against a per-ribbon distance.
    assert "const RIBBON_RESOLVE_SCREEN_PX = 900;" in _SPACE_JS
    body = _SPACE_JS.split("function computeResolvedRibbonKeys()", 1)[1][:500]
    assert "const pa = projectFillScreenPx(da), pb = projectFillScreenPx(db);" in body
    assert "if (Math.hypot(pa.x - pb.x, pa.y - pb.y) > RIBBON_RESOLVE_SCREEN_PX) " \
        "resolved.add(ribbonKey(r));" in body


def test_sync_ribbon_resolve_only_rebuilds_when_the_resolved_set_actually_changed() -> None:
    body = _SPACE_JS.split("function syncRibbonResolve()", 1)[1][:500]
    assert "if (ribbonKeySetsEqual(resolved, ribbonsResolvedKeys)) return;" in body
    assert "buildEdgeLines(idToNode, edges);" in body
    assert "buildRibbonLines();" in body


def test_a_resolved_ribbon_is_dropped_from_the_ribbon_mesh_never_drawn_twice() -> None:
    body = _SPACE_JS.split("function buildRibbonLines()", 1)[1][:700]
    assert "const unresolved = ribbons.filter((r) => !ribbonsResolvedKeys.has(ribbonKey(r)));" \
        in body


def test_build_edge_lines_draws_a_cross_project_edge_only_once_its_own_ribbon_resolved() -> None:
    body = _SPACE_JS.split("function buildEdgeLines(nodes, edgeList)", 1)[1][:1900]
    assert "if (!ribbonsResolvedKeys.has(`${a}|${b}|${e.type}`)) return false;" in body


# --- item 3: the two universal fans are high-degree objects with a count, never spokes ------

def test_high_degree_edge_types_are_acts_for_and_authored_by_found_data_driven() -> None:
    assert 'const HIGH_DEGREE_EDGE_TYPES = ["acts_for", "authored_by"];' in _SPACE_JS
    body = _SPACE_JS.split("function findHighDegreeTarget(type)", 1)[1][:500]
    assert "let bestId = null, bestN = 0;" in body
    assert "if (n > bestN) { bestN = n; bestId = id; }" in body


def test_a_high_degree_targets_incoming_edge_is_excluded_from_lines_folded_into_its_badge() -> None:
    # WAVE 27, THE LENS PANEL: the flat `return false` became conditional on the lens --
    # folded into the badge (high-degree bucket, no line) when shown, an ordinary line when
    # the reader has toggled the badge off (see tests/test_lens_panel.py for the toggle
    # itself).
    edge_body = _SPACE_JS.split("function buildEdgeLines(nodes, edgeList)", 1)[1][:1500]
    assert "const lm = highDegreeTargets[e.type];" in edge_body
    assert "if (lm && e.target === lm.id) {" in edge_body
    assert "if (!highDegreeBadgesHiddenByLens) return false;" in edge_body
    badge_body = _SPACE_JS.split("function buildHighDegreeBadges()", 1)[1][:900]
    assert "div.textContent = `${nd ? labelTextFor(nd) : lm.id} — ${lm.count} ${t}`;" \
        in badge_body


# --- item 4: project labels earn a slot in the SAME shared budget as object labels ----------

def test_project_label_candidates_gate_on_a_minimum_size() -> None:
    assert "const PROJECT_LABEL_MIN_COUNT = 8;" in _SPACE_JS
    body = _SPACE_JS.split("function buildProjectLabelCandidates()", 1)[1][:500]
    assert "projectLabelCandidates = projectFills" in body
    assert ".filter((d) => d.count >= PROJECT_LABEL_MIN_COUNT)" in body


def test_pick_labels_merges_project_candidates_into_the_same_n_labels_pool() -> None:
    body = _SPACE_JS.split("function pickLabels()", 1)[1][:2500]
    assert "const projectPool = projectLabelCandidates.filter(inView);" in body
    assert "labeledNodes = pool.concat(projectPool, communityPool)" in body
    assert ".slice(0, N_LABELS);" in body


def test_a_project_label_declutters_like_an_ordinary_label_and_keeps_its_own_class() -> None:
    body = _SPACE_JS.split("function positionLabels()", 1)[1][:1500]
    assert "if (nd.__isProjectFill || nd.__isCommunity) {" in body
    assert "if (overlapsPlaced(x, y, w)) { div.hidden = true; continue; }" in body


def test_declutter_uses_each_labels_own_real_rendered_width_not_a_fixed_box() -> None:
    # live-verification finding (mail 11471's own "overlap pairs" acceptance line): a long
    # label (a full Decision title can render 200px+ wide) was always boxed at the same
    # fixed LABEL_W=90 for overlap purposes regardless of its own real width -- a genuine
    # visual overlap the old fixed-box declutter had no way to catch. Caught live (1 overlap
    # pair measured against the real DOM), fixed, reverified (0 overlap pairs after).
    assert "const labelWidths = new Map();" in _SPACE_JS
    pick_body = _SPACE_JS.split("function pickLabels()", 1)[1][:4200]
    assert "labelWidths.set(nd, div.offsetWidth || LABEL_W);" in pick_body
    assert "labelWidths.delete(nd);" in pick_body
    pos_body = _SPACE_JS.split("function positionLabels()", 1)[1][:1000]
    assert "const w = labelWidths.get(nd) || LABEL_W;" in pos_body


def test_a_project_below_the_label_gate_stays_an_unlabeled_fill() -> None:
    # buildProjectFillMeshes draws every project's own region regardless of label eligibility
    # -- an unlabeled small project is the "other" wash, not a separate code path.
    body = _SPACE_JS.split("function buildProjectFillMeshes()", 1)[1][:900]
    assert "for (const d of projectFills) {" in body
    assert "PROJECT_LABEL_MIN_COUNT" not in body


# --- item 5: the new structural types flow through focus like any other; supersedes stays
# part of the path walk, its class read off the header (already true since the wire-class
# tip, mail 11291) ---------------------------------------------------------------------------

def test_one_hop_grouping_never_filters_by_edge_type_or_class() -> None:
    # recorded_by/owned_by/admitted_by/vendor_of need no special-casing here --
    # oneHopByTypeDirection already walks every edge touching the focus id regardless of
    # type/class, pulling out only container-SCALE neighbours (isContainerFocus), never by
    # type. Confirms that stays true under this tip.
    body = _SPACE_JS.split("function oneHopByTypeDirection(id)", 1)[1][:900]
    assert "for (const e of edges) {" in body
    assert "if (isContainerFocus(otherId))" in body
    assert "e.edgeClass" not in body
    assert "e.type ===" not in body


def test_supersedes_stays_in_the_path_walk() -> None:
    assert '"succeeded_from", "supersedes", "resolves"' in _SPACE_JS


# --- accounting exact (the receipt's own acceptance line) -----------------------------------

def test_edge_accounting_classifies_every_edge_into_exactly_one_bucket() -> None:
    body = _SPACE_JS.split("function edgeAccounting()", 1)[1][:2200]
    assert "if (PROJECT_FILL_TYPES.has(e.type)) { fill++; continue; }" in body
    assert "if (highDegreeBadgesHiddenByLens) { line++; } else { highDegree++; }" in body
    assert "if (ribbonsResolvedKeys.has(`${a}|${b}|${e.type}`)) line++; else ribbon++;" in body
    assert "accounted: fill + highDegree + line + ribbon + communityRibbon + other };" in body


def test_project_label_candidates_state_is_declared_before_its_first_write() -> None:
    # live-verified TDZ crash (the same bug class THE LAST RENDERER's own commit message
    # already named once, projectObjectByName): buildProjectFillModel calls
    # buildProjectLabelCandidates() during initSpace's synchronous setup, well before a
    # `let projectLabelCandidates` declared down near pickLabels would have executed yet.
    # The `let` lives up in THE PROJECT FILL MODEL block instead, ahead of every call site.
    decl_idx = _SPACE_JS.index("let projectLabelCandidates = [];")
    call_idx = _SPACE_JS.index("buildProjectFillModel(projectAggregates, edges);")
    assert decl_idx < call_idx


def test_the_hidden_by_default_structural_rule_is_retired() -> None:
    # ruling c5953bb1's own "structural hidden at rest" is the OLD answer to density this
    # tip replaces outright (operator ruling 4a51cab1/1178e7d9, "caps and hides are escape
    # hatches") -- hiddenEdgeClasses starts empty, not seeded with "structural".
    assert 'const hiddenEdgeClasses = new Set();' in _SPACE_JS
    assert 'const hiddenEdgeClasses = new Set(["structural"]);' not in _SPACE_JS


def test_debug_api_exposes_the_drawing_tip_live_verification_hooks() -> None:
    body = _SPACE_JS.split("const api = {", 1)[1]
    assert "get projectFills()" in body
    assert "get highDegreeTargets()" in body
    assert "get ribbons()" in body
    assert "get ribbonsResolvedKeys()" in body
    assert "get projectLabelCandidateCount()" in body
    assert "edgeAccounting," in body

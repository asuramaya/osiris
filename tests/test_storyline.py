"""WAVE 26, PIECE 1: THE STORYLINE (Thoth mail 11534, operator's word "keep cooking,
everyone gets a lane", ruling 1178e7d9's fourth principle -- lineages are time; held
thread 3683a12a). Focusing an Agent lays its own succession chain (succeeded_from/
succeeds_seat, both directions) out on a real horizontal TIME axis instead of the ordinary
ranked-column ego tree: x from each body's own `createdAt` (graph_stream.py's new wire
field, item 11), one row for the chain, a sub-agent (spawned_by a chain member but not
itself IN the chain) hangs as a short branch off its own parent at its own spawn time, and
a Decision/Thread recorded_by a chain member or sub-agent sits as a tick directly on that
body's own row. Nothing hidden -- every chain member, sub-agent and tick is positioned,
none paged/capped by count (MAX_STORYLINE_NODES is a crash-guard, never a display budget).
Mirrors the repo's existing static-source-guard convention: string/substring proofs
against the served JS, no browser harness.
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_SPACE_JS = (_STATIC / "space.js").read_text()


# --- wire: createdAt is read off the new header array, index-aligned like x/y -------------

def test_created_at_is_parsed_onto_every_node_off_the_wire() -> None:
    assert "createdAt: snap.created_at ? snap.created_at[i] : 0," in _SPACE_JS


# --- detection: an Agent focus routes to the storyline, never the ordinary ego tree -------

def test_agent_focus_is_detected_by_node_type() -> None:
    body = _SPACE_JS.split("function isAgentFocus(id)", 1)[1][:200]
    assert 'nd.type === "Agent"' in body


def test_focus_object_dispatches_to_the_storyline_before_the_ordinary_ego_walk() -> None:
    body = _SPACE_JS.split("async function focusObject(id, opts)", 1)[1][:1200]
    assert "if (isContainerFocus(id)) { await renderContainerDrill(id, opts); return; }" in body
    assert "if (isAgentFocus(id)) { await renderStoryline(id, opts); return; }" in body


# --- the chain walk: BOTH succession edge types, both directions, from the focus ----------

def test_storyline_chain_walks_succession_adjacency_both_directions() -> None:
    body = _SPACE_JS.split("function buildSuccessionAdjacency()", 1)[1][:600]
    assert "if (!SUCCESSION_EDGE_TYPES.has(e.type)) continue;" in body
    assert "adj.get(e.source)" in body and "adj.get(e.target)" in body

    chain_body = _SPACE_JS.split("function buildStorylineChain(focusId, adj)", 1)[1][:700]
    assert "const chain = new Set([focusId]);" in chain_body
    assert "chain.size < MAX_STORYLINE_NODES" in chain_body


def test_storyline_node_cap_is_a_crash_guard_not_a_display_budget() -> None:
    # "nothing hidden": generous enough that no realistic chain/branch/tick population hits
    # it -- a safety net against a pathological/cyclic graph, disclosed as such in-line.
    assert "const MAX_STORYLINE_NODES = 4000;" in _SPACE_JS


# --- branches and ticks: a sub-agent is spawned_by a chain member but not itself in the
# chain; a tick is recorded_by a chain member OR a sub-agent -----------------------------

def test_sub_agents_are_spawned_by_a_chain_member_but_excluded_from_the_chain_itself() -> None:
    body = _SPACE_JS.split(
        "function buildStorylineBranchesAndTicks(chainIds)", 1)[1][:900]
    assert 'e.type !== "spawned_by" || !chainIds.has(e.target) || chainIds.has(e.source)' \
        in body


def test_ticks_are_recorded_by_a_chain_member_or_a_sub_agent() -> None:
    body = _SPACE_JS.split(
        "function buildStorylineBranchesAndTicks(chainIds)", 1)[1][:900]
    assert "const bodyIds = new Set([...chainIds, ...subAgentOf.keys()]);" in body
    assert 'e.type !== "recorded_by" || !bodyIds.has(e.target)' in body


# --- layout: x = time, y = 0 for the chain row, a branch/tick offset for everything else --

def test_chain_members_sit_on_the_time_axis_at_y_zero() -> None:
    body = _SPACE_JS.split("async function renderStoryline(id, opts)", 1)[1][:2600]
    assert "const timeToX = (t) => ((t - minT) / timeSpan) * axisWidth;" in body
    body2 = _SPACE_JS.split("async function renderStoryline(id, opts)", 1)[1][:3200]
    assert "nd.x = timeToX(nd.createdAt || minT);\n      nd.y = 0;" in body2


def test_sub_agents_branch_off_their_own_parent_at_their_own_spawn_time() -> None:
    body = _SPACE_JS.split("async function renderStoryline(id, opts)", 1)[1][:3800]
    assert "for (const [subId, parentId] of subAgentOf) {" in body
    assert "nd.y = dir * offsetPx * wpp;" in body


# --- WAVE 27, THE SPAWN ROW (Thoth mail 11754): a burst-spawned parent's siblings never ---
# --- collapse onto a shared (dir, tier) -- the tier is unbounded, not capped mod a constant.

def test_spawn_row_tier_is_never_capped_and_never_collides() -> None:
    # the old `% STORYLINE_SUBROW_TIERS` wrap put siblings 10 apart in spawn order back on
    # the exact same (dir, tier) -- with a real burst spawn (near-identical createdAt too),
    # the exact same (x, y). sqrt(tier) is injective on non-negative integers: no two
    # siblings of one parent ever collide, at any burst size.
    body = _SPACE_JS.split("async function renderStoryline(id, opts)", 1)[1][:3800]
    assert "const tier = Math.floor(n / 2); // sqrt(tier) is injective on tier -- " \
        "never repeats" in body
    assert "STORYLINE_SUBROW_TIERS" not in _SPACE_JS


def test_spawn_row_offset_grows_with_sqrt_not_linearly() -> None:
    # live-verification finding: a linear tier * STEP_PX offset fixes the collision but a
    # real fleet burst (measured live: one parent, 1188 siblings) exploded the vertical
    # extent to +-12,000 world units -- "0 label overlaps" only because nothing was legible
    # any more. sqrt(tier) keeps every position distinct while growing sub-linearly, so a
    # burst 100x bigger only needs ~10x the height.
    body = _SPACE_JS.split("async function renderStoryline(id, opts)", 1)[1][:3800]
    assert "const offsetPx = STORYLINE_ROW_OFFSET_PX + Math.sqrt(tier) * " \
        "STORYLINE_SUBROW_STEP_PX;" in body
    assert "nd.y = dir * offsetPx * wpp;" in body


def test_spawn_row_tracks_the_deepest_offset_actually_used_this_render() -> None:
    body = _SPACE_JS.split("async function renderStoryline(id, opts)", 1)[1][:3800]
    assert "storylineMaxSubrowOffsetPx = STORYLINE_ROW_OFFSET_PX;" in body
    assert "if (offsetPx > storylineMaxSubrowOffsetPx) " \
        "storylineMaxSubrowOffsetPx = offsetPx;" in body


def test_spawn_row_axis_clearance_follows_the_actual_max_offset_not_a_fixed_constant() -> None:
    body = _SPACE_JS.split("function buildStorylineAxis(fromT, toT)", 1)[1][:900]
    assert "const axisY = -((storylineMaxSubrowOffsetPx + 40) * wpp);" in body


def test_clear_storyline_state_resets_the_spawn_row_offset_tracker() -> None:
    body = _SPACE_JS.split("function clearStorylineState()", 1)[1][:800]
    assert "storylineMaxSubrowOffsetPx = 0;" in body


def test_debug_api_exposes_the_spawn_row_offset_hook() -> None:
    body = _SPACE_JS.split("const api = {", 1)[1]
    assert "get storylineMaxSubrowOffsetPx()" in body


def test_ticks_sit_on_their_own_body_row_not_a_separate_tier() -> None:
    # "as ticks on its segment" -- a tick rides the same y as the body it's recorded_by,
    # not its own offset row (which would misread as a second kind of branch).
    body = _SPACE_JS.split("async function renderStoryline(id, opts)", 1)[1][:4000]
    assert "for (const [tickId, bodyId] of ticksOf) {" in body
    assert "nd.y = body.y;" in body


def test_the_most_recent_body_naturally_sits_at_the_right_edge() -> None:
    # x is monotonic in createdAt (timeToX), so the newest chain member -- "the current
    # body" -- always has the largest x with no special-casing needed.
    body = _SPACE_JS.split("async function renderStoryline(id, opts)", 1)[1][:2600]
    assert "const timeToX = (t) => ((t - minT) / timeSpan) * axisWidth;" in body


# --- edges: a dedicated straight-line overlay, not updatePathEdges' own bundling logic ----

def test_storyline_lines_cover_both_succession_types_and_the_branch_connector() -> None:
    assert 'const STORYLINE_LINE_TYPES = new Set(["succeeded_from", "succeeds_seat", ' \
        '"spawned_by"]);' in _SPACE_JS
    body = _SPACE_JS.split("function buildStorylineLines()", 1)[1][:700]
    assert "if (!STORYLINE_LINE_TYPES.has(e.type)) continue;" in body
    assert "if (!pathReachable.has(e.source) || !pathReachable.has(e.target)) continue;" \
        in body


# --- the axis: date labels, regenerated over the CURRENT visible window on every zoom -----

def test_axis_labels_are_generated_across_a_time_range_not_a_fixed_set() -> None:
    body = _SPACE_JS.split("function buildStorylineAxis(fromT, toT)", 1)[1][:900]
    assert "const t = fromT + frac * (toT - fromT);" in body
    assert 'div.textContent = new Date(t * 1000).toISOString().slice(0, 10);' in body


def test_zoom_scrubs_the_axis_to_the_current_visible_time_window() -> None:
    body = _SPACE_JS.split("function syncStorylineAxis()", 1)[1][:700]
    assert "if (!storylineActive) return;" in body
    assert "buildStorylineAxis(fromT, toT);" in body
    for call_site in ('updateFrustum();\n    rescaleForZoom();\n    syncRibbonResolve();\n'
                       '    syncStorylineAxis();\n    syncCommunityVisibility();\n    markDirty();',
                       'rescaleForZoom();\n    syncRibbonResolve();\n    syncStorylineAxis();\n'
                       '    syncCommunityVisibility();'):
        assert call_site in _SPACE_JS


def test_axis_labels_never_join_the_shared_lbl_declutter_pool() -> None:
    # deliberately a SEPARATE div class ("lod-glyph-label storyline-axis-label", the same
    # convention high-degree badges/the old project labels used) -- kept off pickLabels' own
    # N_LABELS/overlapsPlaced machinery, and given a real vertical margin below the deepest
    # sub-agent tier so it never collides with an object label floating above its own node.
    body = _SPACE_JS.split("function buildStorylineAxis(fromT, toT)", 1)[1][:900]
    assert 'div.className = "lod-glyph-label storyline-axis-label";' in body
    assert "const axisY = -((storylineMaxSubrowOffsetPx + 40) * wpp);" in body


def test_storyline_axis_label_css_exists_in_both_pages() -> None:
    for html in ("index.html", "space.html"):
        page = (_STATIC / html).read_text()
        assert ".storyline-axis-label {" in page


# --- camera finiteness (the exact class of bug mail 11308 caught in the ego layout) --------

def test_camera_fit_only_moves_on_a_finite_bbox() -> None:
    body = _SPACE_JS.split("async function renderStoryline(id, opts)", 1)[1][:5200]
    assert "if (Number.isFinite(minX)) {" in body


# --- cleanup: leaving a storyline restores real positions and disposes its own divs -------

def test_clear_focus_and_switching_away_both_dispose_storyline_state() -> None:
    clear_body = _SPACE_JS.split("function clearFocus()", 1)[1][:600]
    assert "clearStorylineState();" in clear_body
    focus_body = _SPACE_JS.split("async function focusObject(id, opts)", 1)[1][:1200]
    assert "if (storylineActive) clearStorylineState();" in focus_body


def test_clear_storyline_state_disposes_lines_and_axis_and_resets_membership() -> None:
    body = _SPACE_JS.split("function clearStorylineState()", 1)[1][:800]
    assert "disposeStorylineAxis();" in body
    assert "storylineActive = false;" in body
    assert "storylineChainIds = new Set();" in body


def test_storyline_nodes_never_get_the_lit_always_wins_declutter_bypass() -> None:
    # live-verification finding (mail 11534's own "label overlaps 0" acceptance line):
    # pathReachable IS the whole storyline population during a storyline (chain +
    # sub-agents + ticks, often thousands), so every storyline node reads "lit" -- the
    # ordinary ego-focus "lit always wins its spot" guarantee, unchanged, would disable
    # declutter almost entirely (measured live: 780 overlap pairs). A storyline node
    # never gets that bypass; it declutters like an ordinary label.
    body = _SPACE_JS.split("function positionLabels()", 1)[1][:4000]
    assert "const alwaysShown = lit && !storylineActive;" in body
    assert "if ((!alwaysShown || chainDeclutters) && overlapsPlaced(x, y, w))" in body


# --- declutter: object labels (chain/sub-agent/tick nodes) share the real-width rule -------

def test_storyline_nodes_use_the_same_shared_label_pool_no_special_casing() -> None:
    # chain/sub-agent/tick members are real graph nodes (not project-fill-style pseudo-nodes)
    # repositioned in place -- pickLabels' own nodeVisible+viewport pool, labelWidths' real-
    # width declutter, and overlapsPlaced already apply with zero new code needed there.
    assert "function pickLabels()" in _SPACE_JS
    assert "const labelWidths = new Map();" in _SPACE_JS


# --- debug API: live-verification hooks -----------------------------------------------------

def test_debug_api_exposes_storyline_live_verification_hooks() -> None:
    body = _SPACE_JS.split("const api = {", 1)[1]
    assert "isAgentFocus," in body
    assert "get storylineActive()" in body
    assert "get storylineChainLength()" in body
    assert "get storylineSubAgentCount()" in body
    assert "get storylineTickCount()" in body
    assert "get storylineTimeSpanSeconds()" in body
    assert "get storylineAxisLabelCount()" in body

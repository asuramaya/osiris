"""Compositions — the composer's primitive. The key claim: an opinionated read-model
(`discrepancy`) is just a composition of neutral ops, so opinion leaves the engine and
becomes a saved, forkable spec the user owns. Also proves the ops and persistence.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator.compositions import (
    DEFAULT_COMPOSITIONS,
    _eval,
    _fn_desk_overview,
    _fn_desk_project,
    create_room,
    list_compositions,
    run_composition,
    save_composition,
    seed_default_compositions,
)
from src.orchestrator.discrepancy import footprint_discrepancy

NOW = datetime(2026, 6, 27, tzinfo=UTC)


async def _scenario(actions: Actions):
    """A US-disclosed company running a trial at a UAE site (2 hops) + a US site."""
    co = await actions.create_or_find_object("Organization", "cik:1", "edgar")
    await actions.assert_property(co, "name", "Acme Neuro", "edgar", NOW, 0.85)
    await actions.assert_property(co, "incorporation_state", "CA", "edgar", NOW, 0.85)
    src = "clinicaltrials"
    trial = await actions.create_or_find_object("ClinicalTrial", "nct:1", src)
    await actions.create_link(co, trial, "sponsors", src, NOW, 0.85)
    uae = await actions.create_or_find_object("Organization", "ctgov-org:ccad", src)
    await actions.assert_property(uae, "location", "Abu Dhabi, United Arab Emirates",
                                  src, NOW, 0.85)
    await actions.create_link(trial, uae, "site", src, NOW, 0.85)
    us = await actions.create_or_find_object("Organization", "ctgov-org:miami", src)
    await actions.assert_property(us, "location", "Miami, Florida, United States", src, NOW, 0.85)
    await actions.create_link(trial, us, "site", src, NOW, 0.85)
    return co


# --- #20: orient's scoped briefing IS a composition + recency ordering -------

async def test_order_by_recency_sorts_by_object_birth(actions: Actions) -> None:
    a = await actions.create_or_find_object("Thread", "thread:rec-a", "session")
    b = await actions.create_or_find_object("Thread", "thread:rec-b", "session")
    # force a distinct birth order (a is older) so the assertion can't flake on equal timestamps
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '1 hour' WHERE id=$1", a)
    spec = {"op": "order", "by": "recency", "dir": "desc",
            "from": {"op": "select", "object_type": "Thread", "canonical_prefix": "thread:rec-"}}
    assert (await _eval(actions.pool, spec, None)).objects == [b, a]      # newest first
    spec["dir"] = "asc"
    assert (await _eval(actions.pool, spec, None)).objects == [a, b]      # oldest first


async def test_project_briefing_composition_scopes_to_its_subject(actions: Actions) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", "repo:demo", "session")
    await actions.assert_property(proj, "name", "demo", "session", NOW, 0.9)
    t = await actions.create_or_find_object("Thread", "thread:demo-1", "session")
    await actions.assert_property(t, "summary", "the demo open thread", "session", NOW, 0.9)
    await actions.assert_property(t, "status", "open", "session", NOW, 0.9)
    await actions.create_link(t, proj, "in_repo", "session", NOW, 0.9)
    d = await actions.create_or_find_object("Decision", "decision:demo-1", "session")
    await actions.assert_property(d, "summary", "the demo ruling", "session", NOW, 0.9)
    await actions.assert_property(d, "kind", "ruling", "session", NOW, 0.9)
    await actions.create_link(d, proj, "in_repo", "session", NOW, 0.9)
    foreign = await actions.create_or_find_object("Thread", "thread:foreign", "session")
    await actions.assert_property(foreign, "summary", "a foreign thread", "session", NOW, 0.9)
    await actions.assert_property(foreign, "status", "open", "session", NOW, 0.9)

    await seed_default_compositions(actions.pool)
    items = (await run_composition(actions.pool, "project-briefing", proj))["items"]
    assert set(items) == {"open_threads", "recent_decisions", "tensions", "blind_spots"}
    threads = [r["summary"] for r in items["open_threads"]]
    assert "the demo open thread" in threads
    assert "a foreign thread" not in threads       # scoped OUT of the project's neighbourhood
    assert "the demo ruling" in [r["summary"] for r in items["recent_decisions"]]


async def test_table_op_id_property_returns_the_rows_own_short_id(actions: Actions) -> None:
    """task #60 (thread b81b0fac): a truncated summary must stay addressable — `id` is a
    row's own identity, never an assertion, so `_table` special-cases it rather than
    looking it up in `_props`."""
    d = await actions.create_or_find_object("Decision", "decision:idtest-1", "session")
    await actions.assert_property(d, "summary", "an id-bearing ruling", "session", NOW, 0.9)
    spec = {"op": "table", "columns": [{"property": "id"}, {"property": "summary"}],
            "from": {"op": "select", "object_type": "Decision",
                     "canonical_prefix": "decision:idtest-"}}
    rows = (await _eval(actions.pool, spec, None)).rows
    assert rows == [{"id": str(d)[:8], "summary": "an id-bearing ruling"}]


# --- the headline: discrepancy IS a composition -----------------------------

async def test_discrepancy_is_just_a_composition(actions: Actions) -> None:
    co = await _scenario(actions)
    await seed_default_compositions(actions.pool)

    res = await run_composition(actions.pool, "operational-vs-disclosed-geography", co)
    # the composition surfaces the foreign operational country — the SAME signal the
    # hardcoded read-model produced, now a forkable spec instead of engine code.
    assert res["kind"] == "values"
    assert res["items"] == ["United Arab Emirates"]

    # equivalence with the (now-vestigial) engine read-model
    legacy = await footprint_discrepancy(actions.pool, co)
    assert set(res["items"]) == {x["country"] for x in legacy["discrepancies"]}


# --- the neutral ops --------------------------------------------------------

async def test_select_op(actions: Actions) -> None:
    await _scenario(actions)
    spec = {"op": "select", "object_type": "ClinicalTrial"}
    res = await run_composition(
        actions.pool, await _save(actions, "trials", spec)
    )
    assert res["kind"] == "objects" and res["count"] == 1


async def test_traverse_then_collect(actions: Actions) -> None:
    co = await _scenario(actions)
    spec = {"op": "collect", "transform": "country", "properties": ["location"],
            "from": {"op": "traverse", "from": {"op": "subject"}, "hops": 2}}
    res = await run_composition(actions.pool, await _save(actions, "geo", spec), co)
    assert set(res["items"]) == {"United States", "United Arab Emirates"}


# --- P1 ops: union / intersect / aggregate / order / take -------------------

async def test_intersect_neighbourhood_with_type(actions: Actions) -> None:
    """'Organizations within 2 hops of the subject' = intersect(neighbourhood, orgs).
    The trial (a ClinicalTrial) and the subject itself fall out — set algebra, no join."""
    co = await _scenario(actions)
    spec = {"op": "intersect", "sets": [
        {"op": "traverse", "from": {"op": "subject"}, "hops": 2},
        {"op": "select", "object_type": "Organization"}]}
    res = await run_composition(actions.pool, await _save(actions, "orgs-near", spec), co)
    assert res["kind"] == "objects" and res["count"] == 2  # uae + us


async def test_union_dedups(actions: Actions) -> None:
    await _scenario(actions)
    spec = {"op": "union", "sets": [
        {"op": "select", "object_type": "Organization"},
        {"op": "select", "object_type": "ClinicalTrial"}]}
    res = await run_composition(actions.pool, await _save(actions, "everything", spec))
    assert res["count"] == 4  # 3 orgs + 1 trial, no double-count


async def _filings(actions: Actions) -> None:
    for cik, sector, amt in [("10", "ai", "100"), ("11", "ai", "300"), ("12", "bio", "50")]:
        o = await actions.create_or_find_object("Organization", f"cik:{cik}", "edgar")
        await actions.assert_property(o, "sector", sector, "edgar", NOW, 0.85)
        await actions.assert_property(o, "amount", amt, "edgar", NOW, 0.85)


async def test_aggregate_order_take(actions: Actions) -> None:
    """count per sector → order desc → take top 1: 'ai' wins with 2 (Palantir groupBy)."""
    await _filings(actions)
    spec = {"op": "take", "n": 1, "from": {
        "op": "order", "dir": "desc", "from": {
            "op": "aggregate", "group_by": ["sector"], "metric": {"type": "count"},
            "from": {"op": "select", "object_type": "Organization"}}}}
    res = await run_composition(actions.pool, await _save(actions, "top-sector", spec))
    assert res["kind"] == "rows" and res["count"] == 1
    assert res["items"][0]["group"]["sector"] == "ai"
    assert res["items"][0]["metric"] == 2


async def test_aggregate_sum_over_field(actions: Actions) -> None:
    await _filings(actions)
    spec = {"op": "aggregate", "group_by": ["sector"],
            "metric": {"type": "sum", "field": "amount"},
            "from": {"op": "select", "object_type": "Organization"}}
    res = await run_composition(actions.pool, await _save(actions, "sum-amt", spec))
    by = {r["group"]["sector"]: r["metric"] for r in res["items"]}
    assert by["ai"] == 400.0 and by["bio"] == 50.0


async def test_aggregate_dimension_cap(actions: Actions) -> None:
    """Palantir's ≤3-dimension cap is enforced — a 4-dim aggregate is rejected."""
    await _filings(actions)
    spec = {"op": "aggregate", "group_by": ["a", "b", "c", "d"], "metric": {"type": "count"},
            "from": {"op": "select", "object_type": "Organization"}}
    with pytest.raises(ValueError, match="dimension"):
        await run_composition(actions.pool, await _save(actions, "too-wide", spec))


# --- `group` (ruling c5b184cd, thread d56e7073/#44): the dynamic-titled sibling of `sections`,
# and the middle `aggregate` never had — one section PER DISTINCT VALUE, keeping members. ------

async def test_group_is_a_dynamic_sections_keeping_members(actions: Actions) -> None:
    """One partition per distinct sector, each holding its own real objects (not a metric) —
    exactly what aggregate discards and sections can't produce (no static titles here)."""
    await _filings(actions)
    spec = {"op": "group", "by": "sector", "from": {"op": "select", "object_type": "Organization"},
            "body": {"op": "table", "from": {"op": "these"}, "columns": [{"property": "amount"}]}}
    res = await run_composition(actions.pool, await _save(actions, "by-sector", spec))
    assert res["kind"] == "data"
    assert {r["amount"] for r in res["items"]["ai"]} == {"100", "300"}
    assert {r["amount"] for r in res["items"]["bio"]} == {"50"}


async def test_group_untagged_objects_bucket_as_none(actions: Actions) -> None:
    """A missing property value groups under the SAME literal osiris.js's own aggregate
    renderer already uses for a missing group dimension — one convention, not two."""
    org = await actions.create_or_find_object("Organization", "cik:99", "edgar")
    await actions.assert_property(org, "amount", "10", "edgar", NOW, 0.85)  # no `sector`
    spec = {"op": "group", "by": "sector", "from": {"op": "select", "object_type": "Organization"},
            "body": {"op": "table", "from": {"op": "these"}, "columns": [{"property": "amount"}]}}
    res = await run_composition(actions.pool, await _save(actions, "untagged", spec))
    assert list(res["items"].keys()) == ["(none)"]


async def test_group_nests_via_its_own_body(actions: Actions) -> None:
    """arc->status->owner IS just group-in-group's-body, nothing more — proved with sector
    then a second dimension, each {"op":"these"} resolving to the RIGHT enclosing partition."""
    for cik, sector, amt in [("20", "ai", "1"), ("21", "ai", "2"), ("22", "bio", "3")]:
        o = await actions.create_or_find_object("Organization", f"cik:{cik}", "edgar")
        await actions.assert_property(o, "sector", sector, "edgar", NOW, 0.85)
        await actions.assert_property(o, "band", "hi" if amt != "3" else "lo", "edgar", NOW, 0.85)
    spec = {"op": "group", "by": "sector", "from": {"op": "select", "object_type": "Organization"},
            "body": {"op": "group", "by": "band", "from": {"op": "these"},
                     "body": {"op": "table", "from": {"op": "these"},
                              "columns": [{"property": "id"}]}}}
    res = await run_composition(actions.pool, await _save(actions, "nested", spec))
    assert len(res["items"]["ai"]["hi"]) == 2
    assert len(res["items"]["bio"]["lo"]) == 1
    assert "ai" not in res["items"].get("bio", {})  # sibling partitions never leak into each other


async def test_group_depth_is_capped(actions: Actions) -> None:
    """The same closed-set discipline as aggregate's own ≤3-dimension cap (MAX_AGGREGATE_DIMS)
    — group nesting must not become an unbounded recursion an author can accidentally write."""
    from src.orchestrator.compositions import MAX_GROUP_DEPTH

    def _nested(n: int) -> dict:
        leaf = {"op": "table", "from": {"op": "these"}, "columns": [{"property": "sector"}]}
        node = leaf
        for _ in range(n):
            node = {"op": "group", "by": "sector", "from": {"op": "these"}, "body": node}
        return node

    await _filings(actions)
    spec = _nested(MAX_GROUP_DEPTH)  # MAX_GROUP_DEPTH nested groups, each depth 0..N-1: fine
    spec["from"] = {"op": "select", "object_type": "Organization"}  # the outermost anchors for real
    await run_composition(actions.pool, await _save(actions, "at-cap", spec))  # must not raise

    too_deep = _nested(MAX_GROUP_DEPTH + 1)
    too_deep["from"] = {"op": "select", "object_type": "Organization"}
    with pytest.raises(ValueError, match="nesting"):
        await run_composition(actions.pool, await _save(actions, "over-cap", too_deep))


async def test_these_outside_a_group_is_empty_not_an_error(actions: Actions) -> None:
    """A fragment tested standalone (the inline composer, W4) gets an empty set, never a
    crash — same 'a guess never poisons a real answer' spirit as the rest of this dispatcher."""
    res = await run_composition(actions.pool, await _save(actions, "bare-these", {"op": "these"}))
    assert res["kind"] == "objects" and res["count"] == 0


async def test_group_requires_by_and_body(actions: Actions) -> None:
    await _filings(actions)
    base = {"from": {"op": "select", "object_type": "Organization"}}
    with pytest.raises(ValueError, match="'by'"):
        await run_composition(actions.pool, await _save(
            actions, "no-by", {"op": "group", "body": {"op": "these"}, **base}))
    with pytest.raises(ValueError, match="'body'"):
        await run_composition(actions.pool, await _save(
            actions, "no-body", {"op": "group", "by": "sector", **base}))


# --- Function output re-entering the op-tree (task #60, follow-on to ruling c5b184cd): a
# Function's own output is no longer a dead-end leaf — group/order/take can consume a
# `{"op":"function"}` node whose data is a flat list of dicts. Proven against `desk_decisions`,
# a real registered Function, not a test-only stand-in. ---------------------------------------

async def _desk_decision(actions: Actions, from_agent: str, body: str) -> None:
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent=from_agent, from_project="osiris",
                        to_project="operator", body=body, desk_kind="decision")


async def test_function_output_is_reclassified_as_rows_when_list_of_dicts(
    actions: Actions,
) -> None:
    """The shape-based promotion in the `function` op handler: a list-of-dicts Function
    output becomes kind="rows" (order/take-able), not kind="data" (a leaf)."""
    await _desk_decision(actions, "agent:x", "a real call")
    res = await run_composition(actions.pool, await _save(
        actions, "fn-rows", {"op": "function", "name": "desk_decisions"}))
    assert res["kind"] == "rows"


async def test_dict_shaped_function_output_stays_data_not_rows(actions: Actions) -> None:
    """The reclassification is shape-based, not a blanket promotion — a Function returning a
    dict (sections-like output, e.g. `wall`) stays kind="data"; nothing there is list-shaped
    to group/order/take over."""
    res = await run_composition(actions.pool, await _save(
        actions, "fn-data", {"op": "function", "name": "wall", "args": {"me": ["operator"]}}))
    assert res["kind"] == "data"


async def test_group_consumes_a_function_node_yielding_a_list(actions: Actions) -> None:
    """The proof case: a Function's output can be the `from` of a `group` — partitioned in
    Python from the already-materialized rows, no re-query of the (nonexistent) objects."""
    await _desk_decision(actions, "agent:x", "call A")
    await _desk_decision(actions, "agent:y", "call B")
    spec = {"op": "group", "by": "from", "from": {"op": "function", "name": "desk_decisions"},
            "body": {"op": "these"}}
    res = await run_composition(actions.pool, await _save(actions, "fn-group", spec))
    assert res["kind"] == "data"
    assert {d["summary"] for d in res["items"]["agent:x"]} == {"call A"}
    assert {d["summary"] for d in res["items"]["agent:y"]} == {"call B"}


# --- group's `sequence` (rung 3, ruling d42c543b, Thoth msg 1937) — a caller-given key
# ORDER over `group`'s dynamic titles, distinct from `order` (which sorts by a derived
# property value, never a caller-literal sequence). The three absent-case tests are the
# ones Thoth named specifically ("those three are where a reordering bug hides").

async def test_group_sequence_orders_titles_and_appends_unlisted_ones_alphabetically(
    actions: Actions,
) -> None:
    await _filings(actions)
    o = await actions.create_or_find_object("Organization", "cik:30", "edgar")
    await actions.assert_property(o, "sector", "zz-unlisted", "edgar", NOW, 0.85)
    await actions.assert_property(o, "amount", "9", "edgar", NOW, 0.85)
    spec = {"op": "group", "by": "sector", "sequence": ["bio", "ai"],
            "from": {"op": "select", "object_type": "Organization"},
            "body": {"op": "table", "from": {"op": "these"}, "columns": [{"property": "amount"}]}}
    res = await run_composition(actions.pool, await _save(actions, "seq-basic", spec))
    # listed titles first, IN THE GIVEN ORDER — reversed from insertion/alphabetical
    assert list(res["items"].keys()) == ["bio", "ai", "zz-unlisted"]


async def test_group_sequence_key_with_no_matching_data_is_a_silent_skip(
    actions: Actions,
) -> None:
    """A sequence naming a title that doesn't exist in this run's data must not appear, and
    must not error — same as an ordinary empty group today (msg 1937's first absent case)."""
    await _filings(actions)  # only "ai" and "bio" sectors exist
    spec = {"op": "group", "by": "sector", "sequence": ["nonexistent", "bio", "ai"],
            "from": {"op": "select", "object_type": "Organization"},
            "body": {"op": "table", "from": {"op": "these"}, "columns": [{"property": "amount"}]}}
    res = await run_composition(actions.pool, await _save(actions, "seq-absent-key", spec))
    assert list(res["items"].keys()) == ["bio", "ai"]  # "nonexistent" never appears, no error


async def test_group_key_absent_from_sequence_appends_visibly_not_dropped(
    actions: Actions,
) -> None:
    """A data value the sequence never anticipated (a typo, a new category) must still
    render — appended after the sequenced titles, never silently lost (msg 1937's second
    absent case, the no-silent-caps law applied to reordering)."""
    await _filings(actions)  # "ai", "bio"
    o = await actions.create_or_find_object("Organization", "cik:31", "edgar")
    await actions.assert_property(o, "sector", "biotech-typo", "edgar", NOW, 0.85)
    await actions.assert_property(o, "amount", "7", "edgar", NOW, 0.85)
    spec = {"op": "group", "by": "sector", "sequence": ["ai"],  # "bio"/"biotech-typo" unlisted
            "from": {"op": "select", "object_type": "Organization"},
            "body": {"op": "table", "from": {"op": "these"}, "columns": [{"property": "amount"}]}}
    res = await run_composition(actions.pool, await _save(actions, "seq-unlisted-key", spec))
    # "ai" first (sequenced); the two unlisted titles appended, alphabetically
    assert list(res["items"].keys()) == ["ai", "bio", "biotech-typo"]


async def test_group_sequence_is_independent_per_nesting_level(actions: Actions) -> None:
    """A nested group's own `sequence` (or lack of one) never leaks to its parent's, and
    vice versa (msg 1937's third absent case) — each `group` node reads its own `node`."""
    for cik, sector, band in [("40", "ai", "hi"), ("41", "ai", "lo"),
                              ("42", "bio", "hi"), ("43", "bio", "lo")]:
        o = await actions.create_or_find_object("Organization", f"cik:{cik}", "edgar")
        await actions.assert_property(o, "sector", sector, "edgar", NOW, 0.85)
        await actions.assert_property(o, "band", band, "edgar", NOW, 0.85)
    spec = {"op": "group", "by": "sector", "sequence": ["bio", "ai"],
            "from": {"op": "select", "object_type": "Organization"},
            "body": {"op": "group", "by": "band", "sequence": ["lo", "hi"], "from": {"op": "these"},
                     "body": {"op": "table", "from": {"op": "these"},
                              "columns": [{"property": "band"}]}}}
    res = await run_composition(actions.pool, await _save(actions, "seq-nested", spec))
    assert list(res["items"].keys()) == ["bio", "ai"]                 # outer's own sequence
    assert list(res["items"]["bio"].keys()) == ["lo", "hi"]           # inner's own, independent
    assert list(res["items"]["ai"].keys()) == ["lo", "hi"]


async def test_docs_composition_renders_sections_in_the_fixed_topic_order(
    actions: Actions,
) -> None:
    """The real payoff: DOCS's own `sequence` (folded in from the former route-level
    DOCS_SECTION_ORDER re-sort) produces getting-started/concepts/reference/deployment/
    history in that order straight from the composition — no post-step needed anywhere."""
    from src.orchestrator.compositions import DOCS

    for topic in ("history", "getting-started", "reference"):  # deliberately out of order
        ref = await actions.create_or_find_object("Reference", f"ref:{topic}", "docs")
        await actions.assert_property(ref, "topic", topic, "docs", NOW, 0.85)
        await actions.assert_property(ref, "name", topic, "docs", NOW, 0.85)
    await save_composition(actions.pool, "docs", DOCS)
    res = await run_composition(actions.pool, "docs")
    assert list(res["items"].keys()) == ["getting-started", "reference", "history"]


async def test_order_and_take_already_worked_over_a_function_node(actions: Actions) -> None:
    """order/take needed NO code changes for this — they already branched on Result.kind.
    Pins that a function-sourced "rows" result orders/takes exactly like `table`'s does."""
    await _desk_decision(actions, "agent:x", "zzz-last")
    await _desk_decision(actions, "agent:x", "aaa-first")
    spec = {"op": "take", "n": 1, "from": {
        "op": "order", "by": "summary", "from": {"op": "function", "name": "desk_decisions"}}}
    res = await run_composition(actions.pool, await _save(actions, "fn-order-take", spec))
    assert len(res["items"]) == 1
    assert res["items"][0]["summary"] == "aaa-first"


async def test_nested_ops_work_inside_a_function_sourced_partition(actions: Actions) -> None:
    """The real payoff of widening `_THESE` to hold the partition's raw Result, not a bare
    UUID list: a Function-sourced partition's own body can itself order/take further — not
    just render its rows flat via a bare `{"op":"these"}`."""
    await _desk_decision(actions, "agent:x", "zzz-x-last")
    await _desk_decision(actions, "agent:x", "aaa-x-first")
    await _desk_decision(actions, "agent:y", "only-y")
    spec = {"op": "group", "by": "from", "from": {"op": "function", "name": "desk_decisions"},
            "body": {"op": "take", "n": 1, "from": {
                "op": "order", "by": "summary", "from": {"op": "these"}}}}
    res = await run_composition(actions.pool, await _save(actions, "fn-nested", spec))
    assert len(res["items"]["agent:x"]) == 1
    assert res["items"]["agent:x"][0]["summary"] == "aaa-x-first"
    assert res["items"]["agent:y"][0]["summary"] == "only-y"


# --- projection/pagination (ruling ad19a779, task #64): a caller who knows they want 3 rows
# of 2 fields must never have to receive 53 full rows and pay the trim after. Proven against
# the SAME nested shape (group-by-arc-then-owner) that produced the real 61K-char roadmap
# blob this ruling names. -----------------------------------------------------------------

async def _arc_owner_threads(actions: Actions) -> None:
    for cik, arc, owner, amt in [
        ("30", "ai", "agent:x", "1"), ("31", "ai", "agent:x", "2"),
        ("32", "ai", "agent:y", "3"), ("33", "bio", "agent:x", "4"),
    ]:
        o = await actions.create_or_find_object("Organization", f"cik:{cik}", "edgar")
        await actions.assert_property(o, "sector", arc, "edgar", NOW, 0.85)
        await actions.assert_property(o, "owner_field", owner, "edgar", NOW, 0.85)
        await actions.assert_property(o, "amount", amt, "edgar", NOW, 0.85)


async def test_no_bound_params_is_a_byte_identical_no_op(actions: Actions) -> None:
    await _filings(actions)
    spec = {"op": "select", "object_type": "Organization"}
    name = await _save(actions, "no-bound", spec)
    plain = await run_composition(actions.pool, name)
    bounded_but_unset = await run_composition(actions.pool, name, fields=None, take=None,
                                              depth=None)
    assert plain == bounded_but_unset
    assert "_projected" not in plain


async def test_take_caps_a_flat_list_and_reports_the_real_total(actions: Actions) -> None:
    await _filings(actions)
    spec = {"op": "select", "object_type": "Organization"}
    res = await run_composition(actions.pool, await _save(actions, "take-flat", spec), take=1)
    assert len(res["items"]) == 1
    assert res["_projected"]["dropped"]["(root)"] == {"shown": 1, "of": 3}


async def test_fields_keeps_only_the_named_columns_per_row(actions: Actions) -> None:
    await _filings(actions)
    spec = {"op": "table", "from": {"op": "select", "object_type": "Organization"},
            "columns": [{"property": "sector"}, {"property": "amount"}]}
    res = await run_composition(actions.pool, await _save(actions, "fields-flat", spec),
                                fields=["sector"])
    assert res["items"]
    assert all(set(row) == {"sector"} for row in res["items"])


async def test_depth_collapses_below_the_requested_level_to_an_honest_count(
    actions: Actions,
) -> None:
    await _arc_owner_threads(actions)
    spec = {"op": "group", "by": "sector",
            "from": {"op": "select", "object_type": "Organization"},
            "body": {"op": "group", "by": "owner_field", "from": {"op": "these"},
                     "body": {"op": "table", "from": {"op": "these"},
                              "columns": [{"property": "amount"}]}}}
    name = await _save(actions, "depth-nested", spec)

    full = await run_composition(actions.pool, name)
    assert full["items"]["ai"]["agent:x"] == [{"amount": "1"}, {"amount": "2"}]

    depth1 = await run_composition(actions.pool, name, depth=1)
    assert depth1["items"]["ai"] == {"_count": 3}   # ai: agent:x(2) + agent:y(1)
    assert depth1["items"]["bio"] == {"_count": 1}
    assert depth1["_projected"]["dropped"]["ai"] == {"shown": 0, "of": 3}

    # depth=2 walks BOTH dict levels this shape has (sector, owner_field) — nothing left to
    # collapse, so the leaf lists come through intact; `depth` bounds dict STRUCTURE only,
    # `take` bounds LIST length (composed together in the roadmap-shaped test below).
    depth2 = await run_composition(actions.pool, name, depth=2)
    assert depth2["items"]["ai"]["agent:x"] == [{"amount": "1"}, {"amount": "2"}]
    assert "_projected" not in depth2

    depth2_take1 = await run_composition(actions.pool, name, depth=2, take=1)
    assert depth2_take1["items"]["ai"]["agent:x"] == [{"amount": "1"}]


async def test_fields_take_and_depth_compose_on_the_real_roadmap_shape(
    actions: Actions,
) -> None:
    """The actual proof case named in the ruling: arc->owner->threads, asked for narrow AND
    small in one call — not the full nested tree, not a flat post-processed dump."""
    from src.orchestrator.capture import open_thread
    from src.orchestrator.compositions import ROADMAP

    proj = await actions.create_or_find_object("SoftwareProject", "repo:pgtest", "test")
    await actions.assert_property(proj, "name", "pgtest", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    for i in range(5):
        await open_thread(actions, f"duty {i}", repo="pgtest", arc="Fleet-Hygiene",
                          owner="agent:x", source="agent:me")
    await save_composition(actions.pool, "roadmap", ROADMAP)

    res = await run_composition(actions.pool, "roadmap", proj,
                                fields=["id", "summary"], take=2, depth=3)
    open_arc = res["items"]["open"]["Fleet-Hygiene"]["agent:x"]
    assert len(open_arc) == 2
    assert all(set(row) == {"id", "summary"} for row in open_arc)
    assert res["_projected"]["dropped"]["open.Fleet-Hygiene.agent:x"] == {"shown": 2, "of": 5}


async def test_the_mcp_run_composition_tool_wires_bound_params_through(
    actions: Actions,
) -> None:
    """srv._pool swap (mirrors test_describe.py's own pattern) — proves the ACTUAL tool
    passes fields/take/depth to the core function, not just that the core function works."""
    from src import mcp_server as srv

    await _filings(actions)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.run_composition("all-orgs-live", take=1)
    finally:
        srv._pool = saved_pool
    # a nonexistent composition name still round-trips cleanly through the wiring
    assert out == {"error": "no composition 'all-orgs-live'"}
    await save_composition(actions.pool, "orgs-mcp", {"op": "select",
                                                       "object_type": "Organization"})
    srv._pool = actions.pool
    try:
        out = await srv.run_composition("orgs-mcp", take=1)
    finally:
        srv._pool = saved_pool
    assert len(out["items"]) == 1
    assert out["_projected"]["dropped"]["(root)"]["of"] == 3


# --- persistence / forkability ----------------------------------------------

async def test_save_run_fork_roundtrip(actions: Actions) -> None:
    spec = {"op": "select", "object_type": "Organization"}
    await save_composition(actions.pool, "all-orgs", spec)
    # fork = save the same spec under a new name; both coexist
    await save_composition(actions.pool, "my-orgs", spec)
    names = {c["name"] for c in await list_compositions(actions.pool)}
    assert {"all-orgs", "my-orgs"} <= names
    # update-by-name (not a dup)
    await save_composition(actions.pool, "all-orgs", {"op": "select", "object_type": "Person"})
    rows = [c for c in await list_compositions(actions.pool) if c["name"] == "all-orgs"]
    assert len(rows) == 1 and rows[0]["spec"]["object_type"] == "Person"


async def test_default_compositions_seeded(actions: Actions) -> None:
    n = await seed_default_compositions(actions.pool)
    assert n == len(DEFAULT_COMPOSITIONS)
    names = {c["name"] for c in await list_compositions(actions.pool)}
    assert "operational-vs-disclosed-geography" in names


async def test_seeding_gives_only_mail_fleet_strip_and_fleet_live_a_refresh_secs(
    actions: Actions,
) -> None:
    """ruling cf9286b2: "mail and the fleet strip want seconds; docs, design-canon and the
    decision log want never" — absent/None is the default for every OTHER composition, not
    just the named ones. Thoth extended the ruling to "fleet-live" (msg 1977): a fleet
    roster that must be manually re-run is a fleet roster that lies by default. A future
    addition to DEFAULT_COMPOSITIONS that forgets to stay out of _COMP_REFRESH_SECS would
    silently start auto-polling; this pins the set."""
    await seed_default_compositions(actions.pool)
    by_name = {c["name"]: c["refresh_secs"] for c in await list_compositions(actions.pool)}
    assert by_name["mail"] == 8
    assert by_name["fleet-strip"] == 8
    assert by_name["fleet-live"] == 8
    assert by_name["docs"] is None
    assert by_name["design-canon"] is None
    assert by_name["decision-log"] is None
    assert sum(1 for v in by_name.values() if v is not None) == 3


async def test_save_composition_round_trips_refresh_secs(actions: Actions) -> None:
    await save_composition(actions.pool, "wm-comp", {"op": "select"}, refresh_secs=15)
    rows = [c for c in await list_compositions(actions.pool) if c["name"] == "wm-comp"]
    assert rows[0]["refresh_secs"] == 15


async def test_save_composition_omitting_refresh_secs_keeps_the_prior_value(
    actions: Actions,
) -> None:
    """Same COALESCE-keeps-prior contract as description/section (msg 1938's own note): a
    re-save that doesn't mention refresh_secs must never silently clear it."""
    await save_composition(actions.pool, "wm-comp2", {"op": "select"}, refresh_secs=20)
    await save_composition(actions.pool, "wm-comp2", {"op": "select", "object_type": "X"})
    rows = [c for c in await list_compositions(actions.pool) if c["name"] == "wm-comp2"]
    assert rows[0]["refresh_secs"] == 20
    assert rows[0]["spec"]["object_type"] == "X"  # the actual edit still landed


async def test_save_composition_defaults_refresh_secs_to_none(actions: Actions) -> None:
    """Manual only, the default (ruling cf9286b2) — a plain save/fork never inherits a tick
    it wasn't given."""
    await save_composition(actions.pool, "wm-comp3", {"op": "select"})
    rows = [c for c in await list_compositions(actions.pool) if c["name"] == "wm-comp3"]
    assert rows[0]["refresh_secs"] is None


# --- MUST BE SECTIONED (task #94): the invariant the orphan class needed --------------------

async def test_save_composition_defaults_missing_section_to_more_on_create(
    actions: Actions,
) -> None:
    """Neither the MCP save_composition tool nor the HTTP /compositions route ever pass
    section — a fresh create through either must never land with section=NULL (the exact
    room=NULL + section=NULL shape that rendered nowhere, task #94's own finding)."""
    await save_composition(actions.pool, "wm-nosec", {"op": "select"})
    rows = [c for c in await list_compositions(actions.pool) if c["name"] == "wm-nosec"]
    assert rows[0]["section"] == "_more"  # the client's own existing fallback label, reused


async def test_save_composition_explicit_section_wins_on_create(actions: Actions) -> None:
    await save_composition(actions.pool, "wm-sec", {"op": "select"}, section="engine")
    rows = [c for c in await list_compositions(actions.pool) if c["name"] == "wm-sec"]
    assert rows[0]["section"] == "engine"


async def test_save_composition_omitting_section_keeps_the_prior_value_on_resave(
    actions: Actions,
) -> None:
    """The default-to-_more guard must only fire on a genuine CREATE — a re-save that omits
    section keeps the COALESCE-keeps-prior contract the docstring already promises for it."""
    await save_composition(actions.pool, "wm-resec", {"op": "select"}, section="fleet")
    await save_composition(actions.pool, "wm-resec", {"op": "select", "object_type": "X"})
    rows = [c for c in await list_compositions(actions.pool) if c["name"] == "wm-resec"]
    assert rows[0]["section"] == "fleet"          # not overwritten to _more
    assert rows[0]["spec"]["object_type"] == "X"  # the actual edit still landed


# --- room_id GETS THE SAME TREATMENT (ruling 89e67c49): the identical invisibility defect,
# the identical Postgres NOT-NULL-on-INSERT-tuple trap, the identical fix shape — with one
# deliberate difference (no DB-level NOT NULL; see save_composition's own docstring for why
# ON DELETE SET NULL makes that unsafe here) that these tests exercise directly. -------------

async def test_save_composition_defaults_missing_room_to_the_engineer_room_on_create(
    actions: Actions,
) -> None:
    room_id = await create_room(actions.pool, "engineer")
    await save_composition(actions.pool, "wm-noroom", {"op": "select"})
    rows = [c for c in await list_compositions(actions.pool) if c["name"] == "wm-noroom"]
    assert rows[0]["room_id"] == str(room_id)


async def test_save_composition_leaves_room_unassigned_when_no_engineer_room_exists(
    actions: Actions,
) -> None:
    """The fallback degrades gracefully rather than fabricating a room that would fail its
    own foreign key — every test DB starts with zero rooms, so this is also the default
    coverage for that shape, not a contrived edge case."""
    await save_composition(actions.pool, "wm-noroomatall", {"op": "select"})
    rows = [c for c in await list_compositions(actions.pool) if c["name"] == "wm-noroomatall"]
    assert rows[0]["room_id"] is None


async def test_save_composition_explicit_room_wins_on_create(actions: Actions) -> None:
    await create_room(actions.pool, "engineer")
    other = await create_room(actions.pool, "analyst")
    await save_composition(actions.pool, "wm-room", {"op": "select"}, room_id=other)
    rows = [c for c in await list_compositions(actions.pool) if c["name"] == "wm-room"]
    assert rows[0]["room_id"] == str(other)  # never silently redirected to the fallback


async def test_save_composition_omitting_room_keeps_the_prior_value_on_resave(
    actions: Actions,
) -> None:
    await create_room(actions.pool, "engineer")
    mine = await create_room(actions.pool, "mine")
    await save_composition(actions.pool, "wm-reroom", {"op": "select"}, room_id=mine)
    await save_composition(actions.pool, "wm-reroom", {"op": "select", "object_type": "X"})
    rows = [c for c in await list_compositions(actions.pool) if c["name"] == "wm-reroom"]
    assert rows[0]["room_id"] == str(mine)        # not redirected to 'engineer'
    assert rows[0]["spec"]["object_type"] == "X"  # the actual edit still landed


async def _save(actions: Actions, name: str, spec: dict) -> str:
    await save_composition(actions.pool, name, spec)
    return name


async def test_object_items_resolves_props_by_grade_not_recency(actions: Actions) -> None:
    """Resolver-unify regression: object_items (the composer's object-list / Table renderer)
    resolved each property by RECENCY ONLY — a fresh DERIVED re-assertion buried an older
    SELF_DECLARED (the stuck-open-threads bug class). It now routes through winning_props (grade,
    THEN recency) like _props, so the cross-source winner ordering is one definition, not five."""
    from src.orchestrator.compositions import object_items
    from src.parsers.base import EvidenceClass
    from src.parsers.evidence import confidence_for

    older, newer = datetime(2026, 6, 26, tzinfo=UTC), datetime(2026, 6, 28, tzinfo=UTC)
    sd, dv = EvidenceClass.SELF_DECLARED, EvidenceClass.DERIVED
    o = await actions.create_or_find_object("Thread", "thread:grade-bulk", "session")
    # an OLDER SELF_DECLARED status=resolved, then a NEWER DERIVED status=open (a miner re-open)
    await actions.assert_property(o, "status", "resolved", "session", older,
                                  confidence_for(sd), evidence_class=sd.value)
    await actions.assert_property(o, "status", "open", "session-miner", newer,
                                  confidence_for(dv), evidence_class=dv.value)
    items = await object_items(actions.pool, [o])
    status = next(it["props"]["status"] for it in items if it["id"] == str(o))
    assert status == "resolved"   # GRADE wins over the fresher DERIVED re-open (was "open")


# --- the migrated ROADMAP (ruling c5b184cd, thread d56e7073/#44): the proof case for the
# Function/op line — `open` stays a Function (echo-filter, a real domain gap), `resolved`/
# `retracted` are pure `group`-by-arc-then-owner over live data. ---------------------------

async def test_roadmap_composition_open_section_is_echo_filtered_and_arc_grouped(
    actions: Actions,
) -> None:
    from src.orchestrator.capture import open_thread
    from src.orchestrator.compositions import ROADMAP

    proj = await actions.create_or_find_object("SoftwareProject", "repo:rmcomp", "test")
    await actions.assert_property(proj, "name", "rmcomp", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await open_thread(actions, "a real duty", repo="rmcomp", arc="Fleet-Hygiene",
                      owner="agent:x", source="agent:me")
    # a miner echo: derived-only, never touched by a self_declared assertion
    echo = await actions.create_or_find_object("Thread", "thread:echo-rm", "session-miner")
    for n, v in (("summary", "a guessed duty nobody touched"), ("status", "open"),
                 ("kind", "obligation")):
        await actions.assert_property(echo, n, v, "session-miner", NOW, 0.4,
                                      evidence_class="derived")
    await actions.create_link(echo, proj, "in_repo", "session-miner", NOW, 0.4,
                              evidence_class="derived")

    await save_composition(actions.pool, "roadmap", ROADMAP)
    res = await run_composition(actions.pool, "roadmap", proj)
    open_data = res["items"]["open"]
    assert "Fleet-Hygiene" in open_data
    assert any(t["summary"] == "a real duty" for t in open_data["Fleet-Hygiene"]["agent:x"])
    # the echo never appears anywhere in the open section — the filter is real, not cosmetic
    assert "a guessed duty nobody touched" not in str(open_data)


async def test_roadmap_open_section_names_its_own_dropped_tail(actions: Actions) -> None:
    """Thoth's own finding, live (2026-07-27): rank_open_threads' cap (ORIENT_OPEN_THREADS)
    was silently dropping everything past 25 — a "no silent caps" violation. The Function
    must stay list-shaped for `group` to consume it (task #60), so the honest count rides
    IN the list as one distinctly-tagged trailing row rather than a sidecar dict key
    (`_fn_wall`'s own pattern, unavailable here) — forming its own visible bucket, never
    mixed into a real arc/owner's own threads."""
    from src.orchestrator.capture import open_thread
    from src.orchestrator.compositions import ORIENT_OPEN_THREADS, ROADMAP

    proj = await actions.create_or_find_object("SoftwareProject", "repo:rmmore", "test")
    await actions.assert_property(proj, "name", "rmmore", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    total = ORIENT_OPEN_THREADS + 2
    for i in range(total):
        await open_thread(actions, f"duty {i}", repo="rmmore", arc="Fleet-Hygiene",
                          owner="agent:x", source="agent:me")

    await save_composition(actions.pool, "roadmap", ROADMAP)
    res = await run_composition(actions.pool, "roadmap", proj)
    open_data = res["items"]["open"]
    real_shown = len(open_data["Fleet-Hygiene"]["agent:x"])
    assert real_shown == ORIENT_OPEN_THREADS
    more_row = open_data["(more)"]["(more)"][0]
    assert "2 more open threads not shown" in more_row["summary"]


async def test_roadmap_composition_resolved_is_pure_op_tree_group(actions: Actions) -> None:
    from src.orchestrator.capture import open_thread, resolve_thread
    from src.orchestrator.compositions import ROADMAP

    proj = await actions.create_or_find_object("SoftwareProject", "repo:rmcomp2", "test")
    await actions.assert_property(proj, "name", "rmcomp2", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    tid = await open_thread(actions, "shipped work", repo="rmcomp2", arc="Token-Cost",
                            owner="agent:builder", source="agent:me")
    await resolve_thread(actions, str(tid), because="done", source="agent:me")

    await save_composition(actions.pool, "roadmap", ROADMAP)
    res = await run_composition(actions.pool, "roadmap", proj)
    resolved = res["items"]["resolved"]
    assert list(resolved["Token-Cost"]["agent:builder"])[0]["summary"] == "shipped work"
    assert res["items"]["retracted"] == {}  # nothing retracted — an empty group, not missing


async def test_roadmap_composition_renders_via_the_generic_renderer(actions: Actions) -> None:
    """End to end: the composition's own output feeds render_composition with no adapter —
    the whole point of a shared {kind,items} contract between the op-tree and the renderer."""
    from src.api.chrome import render_composition
    from src.orchestrator.capture import open_thread
    from src.orchestrator.compositions import ROADMAP

    proj = await actions.create_or_find_object("SoftwareProject", "repo:rmcomp3", "test")
    await actions.assert_property(proj, "name", "rmcomp3", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await open_thread(actions, "a tracked duty", repo="rmcomp3", arc="Fleet-Hygiene",
                      owner="agent:x", source="agent:me")
    await save_composition(actions.pool, "roadmap", ROADMAP)
    res = await run_composition(actions.pool, "roadmap", proj)
    html = render_composition(res, title="roadmap")
    assert "a tracked duty" in html and "Fleet-Hygiene" in html


# --- the migrated DOCS (ruling c5b184cd, thread d56e7073/#44): the simpler proof case — no
# Function at all, one `group by=topic` level, subject-free. ---------------------------------

async def test_docs_composition_groups_by_topic_and_excludes_untopiced(
    actions: Actions,
) -> None:
    from src.orchestrator.compositions import DOCS

    doc = await actions.create_or_find_object("Reference", "ref:some-doc", "test")
    await actions.assert_property(doc, "name", "Some Doc", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(doc, "topic", "concepts", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    stray = await actions.create_or_find_object("Reference", "ref:no-topic", "test")
    await actions.assert_property(stray, "name", "Stray Doc", "test", NOW, 0.9,
                                  evidence_class="self_declared")  # deliberately no topic

    await save_composition(actions.pool, "docs", DOCS)
    res = await run_composition(actions.pool, "docs")  # no subject needed
    assert res["kind"] == "data"
    assert res["items"]["concepts"][0]["name"] == "Some Doc"
    assert "Stray Doc" not in str(res["items"])  # untopiced -> excluded, not a catch-all


# --- desk_decisions (ruling c5b184cd, thread d56e7073/#44): the live-desk composition's
# 'decisions-awaiting-a-call' leg — a Function, since fleet_messages isn't the object graph.

async def test_desk_decisions_function_finds_unresolved_decision_briefs(
    actions: Actions,
) -> None:
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent="agent:x", from_project="osiris",
                       to_project="operator", body="needs your call: which approach?",
                       desk_kind="decision")
    await send_message(actions.pool, from_agent="agent:x", from_project="osiris",
                       to_project="operator", body="fyi, all done here", desk_kind="fyi")

    spec = {"op": "function", "name": "desk_decisions"}
    res = await run_composition(actions.pool, await _save(actions, "decisions", spec))
    # "rows", not "data" (task #60, the function-output-re-entering-the-op-tree follow-on):
    # a list-of-dicts Function output is reclassified so group/order/take can reach it —
    # `items` itself is unchanged, `_package` passes rows/data through identically.
    assert res["kind"] == "rows"
    bodies = [d["summary"] for d in res["items"]]
    assert any("which approach" in b for b in bodies)
    assert not any("all done here" in b for b in bodies)  # fyi, not a decision — excluded


# --- LIVE_DESK end to end (ruling c5b184cd, thread d56e7073/#44): the wedge that ends the
# briefs rot — "what's actionable for the operator right now," built with existing ops +
# Functions, no `group` needed. Resolved/stale fall out by construction (status=open).

async def test_live_desk_composition_end_to_end(actions: Actions) -> None:
    from src.orchestrator.capture import open_thread, resolve_thread
    from src.orchestrator.compositions import LIVE_DESK
    from src.orchestrator.deploy_guard import alarm_schema_drift
    from src.orchestrator.mailbox import send_message

    # owed_to_you: open, owner=operator
    await open_thread(actions, "operator must pick a direction", owner="operator",
                      source="agent:me")
    # NOT owed_to_you: someone else's open thread
    await open_thread(actions, "not the operator's", owner="agent:builder", source="agent:me")
    # resolved/stale: must fall out by construction (still owner=operator, but closed)
    stale = await open_thread(actions, "an old operator debt, now closed", owner="operator",
                              source="agent:me")
    await resolve_thread(actions, str(stale), because="handled", source="agent:me")
    # decisions_awaiting_a_call
    await send_message(actions.pool, from_agent="agent:x", from_project="osiris",
                       to_project="operator", body="which design should we ship?",
                       desk_kind="decision")
    # drift_alarms
    await alarm_schema_drift(actions.pool, "code expects '0036', DB is at '0034'",
                             service="osiris-worker")

    await save_composition(actions.pool, "live-desk", LIVE_DESK)
    res = await run_composition(actions.pool, "live-desk")
    assert res["kind"] == "data"
    owed = str(res["items"]["owed_to_you"])
    assert "operator must pick a direction" in owed
    assert "not the operator's" not in owed
    assert "now closed" not in owed  # resolved -- fell out by construction, no extra logic
    assert "which design should we ship" in str(res["items"]["decisions_awaiting_a_call"])
    assert "SCHEMA DRIFT" in str(res["items"]["drift_alarms"])


# --- fleet_live_agents / fleet_pulse_line / FLEET_STRIP (task #71 slice two, gated msg
# 1894/1897) — the /ui migration pilot: a Composition + two Functions, zero UI code. Both
# are Functions because liveness/seatedness are derived at read time from agent_mounts,
# never stored graph properties on Agent (see _fn_fleet_live_agents's own docstring).

async def test_fleet_live_agents_lists_only_live_seated_agents_on_one_project(
    actions: Actions,
) -> None:
    from src.orchestrator.mounts import save_mount

    p = actions.pool
    await save_mount(p, job_dir="/jobs/aaaa0001", agent_id="agent:deadbeef",
                     project="osiris", cwd="/w/osiris", model="claude-sonnet-5",
                     session_key="sid:realconn")
    # a different project's live agent must never leak into the osiris strip
    await save_mount(p, job_dir="/jobs/bbbb0002", agent_id="agent:c0ffee01",
                     project="neo", cwd="/w/neo", model="claude-opus-5",
                     session_key="sid:otherconn")

    spec = {"op": "function", "name": "fleet_live_agents"}
    res = await run_composition(p, await _save(actions, "fleet-agents", spec))
    assert res["kind"] == "rows"          # task #60's own reclassification, proven above
    agents = [r["agent"] for r in res["items"]]
    assert any("deadbeef" in a for a in agents)
    assert not any("c0ffee01" in a for a in agents)


async def test_fleet_live_agents_degrades_honestly_on_a_pool_failure() -> None:
    """A broken pool must say so, never silently render an empty table (msg 1894 point 4,
    degrade-honestly — renderer-independent, the same law build_inbox already follows)."""
    from src.orchestrator.compositions import _fn_fleet_live_agents

    class _BrokenPool:
        def __getattr__(self, name: str) -> object:
            async def _raise(*args: object, **kwargs: object) -> None:
                raise ConnectionError("pool gone")
            return _raise

    rows = await _fn_fleet_live_agents(_BrokenPool(), None, {})  # type: ignore[arg-type]
    assert rows == [{"agent": "fleet data unavailable", "project": "-", "model": "-"}]


async def test_fleet_pulse_line_returns_the_same_string_orient_shows(actions: Actions) -> None:
    from src.orchestrator.compositions import _fn_fleet_pulse_line
    from src.orchestrator.mounts import fleet_pulse

    expected = await fleet_pulse(actions.pool)
    line = await _fn_fleet_pulse_line(actions.pool, None, {})
    assert line == expected


async def test_fleet_strip_composition_end_to_end(actions: Actions) -> None:
    from src.orchestrator.compositions import FLEET_STRIP
    from src.orchestrator.mounts import save_mount

    await save_mount(actions.pool, job_dir="/jobs/cccc0003", agent_id="agent:feedface",
                     project="osiris", cwd="/w/osiris", model="claude-sonnet-5",
                     session_key="sid:realconn2")

    await save_composition(actions.pool, "fleet-strip", FLEET_STRIP)
    res = await run_composition(actions.pool, "fleet-strip")
    assert res["kind"] == "data"           # a `sections` op always packages as data
    assert isinstance(res["items"]["pulse"], str)
    assert "live" in res["items"]["pulse"]
    assert "agent:feedface" in str(res["items"]["live_agents"])


# --- fleet_live / "fleet-live" (rung 2, ruling d42c543b, Thoth msg 1926/1936) — /fleet's
# full-fidelity port: additive, the route stays live beside it. UNLIKE fleet_live_agents
# (ranked, one project, live+seated only), this is the whole roster — every project, every
# soul the mount registry knows — with doors/ancestors FLATTENED to readable prose rather
# than a nested list a table cell can't render (see _fn_fleet_live's own docstring).

async def test_fleet_live_lists_the_full_cross_project_roster(actions: Actions) -> None:
    """The one property fleet_live_agents deliberately does NOT have: no project filter,
    no live/seated-only cut. A dead soul on a different project must still show up."""
    from src.orchestrator.mounts import save_mount

    p = actions.pool
    await save_mount(p, job_dir="/jobs/aaaa0001", agent_id="agent:deadbeef",
                     project="osiris", cwd="/w/osiris", model="claude-sonnet-5",
                     session_key="sid:realconn")
    await save_mount(p, job_dir="/jobs/bbbb0002", agent_id="agent:c0ffee01",
                     project="neo", cwd="/w/neo", model="claude-opus-5",
                     session_key="sid:otherconn")
    await p.execute("UPDATE agent_mounts SET last_seen = now() - interval '2 days' "
                    "WHERE agent_id='agent:c0ffee01'")  # stale — still must appear

    spec = {"op": "function", "name": "fleet_live"}
    res = await run_composition(p, await _save(actions, "fleet-live-t1", spec))
    assert res["kind"] == "data"
    projects = {r.get("project") for r in res["items"]["unreconciled"]}
    assert projects == {"osiris", "neo"}         # both projects, live AND stale, both present


async def test_fleet_live_degrades_honestly_on_a_pool_failure() -> None:
    """A broken pool must say so, never a silent empty roster (the same degrade-honestly
    law fleet_live_agents/mail_overview already follow)."""
    from src.orchestrator.compositions import _fn_fleet_live

    class _BrokenPool:
        def __getattr__(self, name: str) -> object:
            async def _raise(*args: object, **kwargs: object) -> None:
                raise ConnectionError("pool gone")
            return _raise

    out = await _fn_fleet_live(_BrokenPool(), None, {})  # type: ignore[arg-type]
    assert out == {"pulse": "fleet data unavailable"}


async def test_fleet_live_flattens_doors_to_readable_prose_not_a_repr(
    actions: Actions,
) -> None:
    from src.orchestrator.compositions import _fn_fleet_live
    from src.orchestrator.mounts import save_mount

    p = actions.pool
    o = await actions.create_or_find_object("Agent", "agent:cafe99aa", "agent:cafe99aa")
    await actions.assert_property(o, "handle", "Cafe", "agent:cafe99aa", NOW, 0.9,
                                  evidence_class="self_declared")
    await save_mount(p, job_dir="/jobs/aaaa0001", agent_id="agent:cafe99aa",
                     project="osiris", cwd="/w/osiris", model="claude-opus-4-8",
                     session_key="sid:realconn")
    await save_mount(p, job_dir="/jobs/bbbb0002", agent_id="agent:cafe99aa",
                     project="osiris", cwd="/w/osiris", model="claude-fable-5",
                     session_key="view-of:aaaa0001")

    out = await _fn_fleet_live(p, None, {})
    mine = next(r for r in out["roster"] if r["seat"] == "Cafe I")
    assert "{" not in mine["doors"] and "[" not in mine["doors"]   # no repr leaked through
    assert mine["doors"].startswith("2 doors (")
    assert "tab→aaaa0001" in mine["doors"] and "session aaaa0001" in mine["doors"]


async def test_fleet_live_flattens_ancestors_to_readable_prose_not_a_repr(
    actions: Actions,
) -> None:
    from src.orchestrator.compositions import _fn_fleet_live
    from src.orchestrator.mounts import save_mount

    p = actions.pool
    for gen, handle_gen in (("agent:3e7a0001", 1), ("agent:3e7a0001-ii", 2),
                            ("agent:3e7a0001-iii", 3)):
        o = await actions.create_or_find_object("Agent", gen, gen)
        await actions.assert_property(o, "handle", "Metra", gen, NOW, 0.9,
                                      evidence_class="self_declared")
        await actions.assert_property(o, "seat_generation", str(handle_gen), gen, NOW, 0.9,
                                      evidence_class="self_declared")
        await save_mount(p, job_dir=f"/jobs/{gen.removeprefix('agent:')}", agent_id=gen,
                         project="metrahouse", cwd="/w/metra", model=None, session_key=None)
    # ages must be UNAMBIGUOUS, not just "old": -i strictly older than -ii, so the
    # "freshest ancestor" pick isn't a coin flip between two rows aged in one UPDATE
    await p.execute("UPDATE agent_mounts SET last_seen = now() - interval '3 days' "
                    "WHERE agent_id='agent:3e7a0001'")
    await p.execute("UPDATE agent_mounts SET last_seen = now() - interval '2 days' "
                    "WHERE agent_id='agent:3e7a0001-ii'")

    out = await _fn_fleet_live(p, None, {})
    mine = next(r for r in out["roster"] if r["seat"] == "Metra III")
    assert "{" not in mine["ancestors"] and "[" not in mine["ancestors"]
    assert mine["ancestors"] == "2 earlier lives, most recent Metra II 2d ago"


async def test_fleet_live_pulse_and_wake_ledger(actions: Actions) -> None:
    from src.orchestrator.compositions import _fn_fleet_live
    from src.orchestrator.mounts import save_mount

    p = actions.pool
    await save_mount(p, job_dir="/jobs/aaaa0003", agent_id="agent:feedface",
                     project="osiris", cwd="/w/osiris", model="claude-sonnet-5",
                     session_key="sid:realconn3")
    await p.execute("INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
                    "VALUES ('osiris', 'agent:x', 9, 'mint')")

    out = await _fn_fleet_live(p, None, {})
    assert "live" in out["pulse"] and "wakes" in out["pulse"]
    assert any(w["project"] == "osiris" and w["by"] == "agent:x" for w in out["wake_ledger"])


async def test_fleet_live_composition_is_registered_and_runs_end_to_end(
    actions: Actions,
) -> None:
    from src.orchestrator.mounts import save_mount

    await save_mount(actions.pool, job_dir="/jobs/dddd0004", agent_id="agent:0ddba11",
                     project="osiris", cwd="/w/osiris", model="claude-sonnet-5",
                     session_key="sid:realconn4")
    await save_composition(actions.pool, "fleet-live", DEFAULT_COMPOSITIONS["fleet-live"])
    res = await run_composition(actions.pool, "fleet-live")
    assert res["kind"] == "data"
    assert "agent:0ddba11" in str(res["items"]["unreconciled"])


# --- mail_overview / mail_threads / MAIL_OVERVIEW (task #71 consolidation wave 2, ruling
# d42c543b, msg 1929) — /mail's overview half ported as a Function + Composition. Both
# Functions wrap chrome.py's own mail_overview/mail_threads verbatim, never re-deriving
# the soul-fold or the box-routing logic.

async def test_mail_overview_function_lists_boxes_with_traffic(actions: Actions) -> None:
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent="agent:a", from_project="osiris",
                       to_project="neo", body="a project lane message")

    spec = {"op": "function", "name": "mail_overview"}
    res = await run_composition(actions.pool, await _save(actions, "mail-ov", spec))
    assert res["kind"] == "rows"          # task #60's own reclassification
    boxes = [r["box"] for r in res["items"]]
    assert "neo" in boxes


async def test_mail_overview_degrades_honestly_on_a_pool_failure() -> None:
    """A broken pool must say so, never a silent empty table (the same degrade-honestly
    law fleet_live_agents/fleet_pulse_line already follow)."""
    from src.orchestrator.compositions import _fn_mail_overview

    class _BrokenPool:
        def __getattr__(self, name: str) -> object:
            async def _raise(*args: object, **kwargs: object) -> None:
                raise ConnectionError("pool gone")
            return _raise

    rows = await _fn_mail_overview(_BrokenPool(), None, {})  # type: ignore[arg-type]
    assert rows == [{"box": "mail data unavailable", "msgs": "-", "unsettled": "-"}]


async def test_mail_threads_function_lists_one_boxs_threads_via_args_box(
    actions: Actions,
) -> None:
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent="agent:a", from_project="osiris",
                       to_project="neo", body="hello neo")
    await send_message(actions.pool, from_agent="agent:a", from_project="osiris",
                       to_project="other", body="not this box")

    spec = {"op": "function", "name": "mail_threads", "args": {"box": "neo"}}
    res = await run_composition(actions.pool, await _save(actions, "mail-th", spec))
    assert res["kind"] == "rows"
    latest = [r["latest"] for r in res["items"]]
    assert any("hello neo" in v for v in latest)
    assert not any("not this box" in v for v in latest)


async def test_mail_threads_names_the_missing_box_honestly() -> None:
    from src.orchestrator.compositions import _fn_mail_threads

    rows = await _fn_mail_threads(None, None, {})  # type: ignore[arg-type]
    assert "no box given" in rows[0]["thread"]


async def test_mail_threads_degrades_honestly_on_a_pool_failure() -> None:
    from src.orchestrator.compositions import _fn_mail_threads

    class _BrokenPool:
        def __getattr__(self, name: str) -> object:
            async def _raise(*args: object, **kwargs: object) -> None:
                raise ConnectionError("pool gone")
            return _raise

    rows = await _fn_mail_threads(_BrokenPool(), None, {"box": "neo"})  # type: ignore[arg-type]
    assert rows == [{"thread": "mail data unavailable", "between": "-", "msgs": "-"}]


async def test_mail_composition_end_to_end(actions: Actions) -> None:
    from src.orchestrator.compositions import MAIL_OVERVIEW
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent="agent:a", from_project="osiris",
                       to_project="neo", body="a project lane message")

    await save_composition(actions.pool, "mail", MAIL_OVERVIEW)
    res = await run_composition(actions.pool, "mail")
    assert res["kind"] == "rows"
    assert "neo" in [r["box"] for r in res["items"]]


async def test_mail_composition_rows_carry_the_drill_in_run_action(actions: Actions) -> None:
    """task #90 (Thoth msg 1976/2005) — each box's own row runs mail_threads for THAT box via
    the "run:" navigation dispatch, not a fixed/shared action across every row."""
    from src.orchestrator.compositions import MAIL_OVERVIEW
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent="agent:a", from_project="osiris",
                       to_project="neo", body="a project lane message")

    await save_composition(actions.pool, "mail", MAIL_OVERVIEW)
    res = await run_composition(actions.pool, "mail")
    row = next(r for r in res["items"] if r["box"] == "neo")
    assert row["_action"] == {"action": "run:mail_threads", "args": {"box": "neo"}}


# --- overhead (task #91, ruling d42c543b, msg 1959) — /overhead ported as one Function, two
# data sources (TranscriptStore's harness-cost accounting, TelemetryStore's retained-events
# forensics) composed once in Python. Wraps overhead_fleet/summary verbatim.

async def test_overhead_function_shows_zero_totals_with_no_sessions_ingested(
    actions: Actions,
) -> None:
    """Nothing eaten yet is an honest empty totals dict, not an error — same 'absence, never
    a zero-row pretence' law TelemetryStore.summary's own docstring states."""
    from src.orchestrator.compositions import _fn_overhead

    data = await _fn_overhead(actions.pool, None, {})
    assert data["totals"]["sessions"] == 0
    assert data["top_sessions"] == []
    assert "nothing retained yet" in data["telemetry"]


async def test_overhead_degrades_honestly_on_a_pool_failure() -> None:
    from src.orchestrator.compositions import _fn_overhead

    class _BrokenPool:
        def __getattr__(self, name: str) -> object:
            async def _raise(*args: object, **kwargs: object) -> None:
                raise ConnectionError("pool gone")
            return _raise

    data = await _fn_overhead(_BrokenPool(), None, {})  # type: ignore[arg-type]
    assert data == {"totals": "overhead data unavailable"}


async def test_overhead_composition_end_to_end(actions: Actions) -> None:
    await save_composition(actions.pool, "overhead", {"op": "function", "name": "overhead"})
    res = await run_composition(actions.pool, "overhead")
    assert res["kind"] == "data"          # a dict, not a list — no rows reclassification
    assert res["items"]["totals"]["sessions"] == 0


# --- desk_overview / desk_project (task #91, ruling d42c543b, msg 1959) — /desk's READ side
# only. The four action verbs (done/not mine/later/settle) are a rung-3 gap, proposed to
# Thoth separately, not built here — see _fn_desk_overview's own docstring.

async def test_desk_overview_lists_projects_with_asks(actions: Actions) -> None:
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent="agent:a", from_project="neo",
                       to_project="operator", body="pick a signing strategy",
                       desk_kind="decision")

    data = await _fn_desk_overview(actions.pool, None, {})
    assert data["owed"] >= 0
    projects = {p["project"]: p for p in data["by_project"]}
    assert "neo" in projects
    assert projects["neo"]["asks"] == 1


async def test_desk_overview_degrades_honestly_on_a_pool_failure() -> None:
    class _BrokenPool:
        def __getattr__(self, name: str) -> object:
            async def _raise(*args: object, **kwargs: object) -> None:
                raise ConnectionError("pool gone")
            return _raise

    data = await _fn_desk_overview(_BrokenPool(), None, {})  # type: ignore[arg-type]
    assert data == {"owed": "desk data unavailable"}


async def test_desk_project_function_lists_one_projects_asks_via_args_project(
    actions: Actions,
) -> None:
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent="agent:a", from_project="neo",
                       to_project="operator", body="pick a signing strategy",
                       desk_kind="decision")
    await send_message(actions.pool, from_agent="agent:a", from_project="other",
                       to_project="operator", body="not this project", desk_kind="decision")

    rows = await _fn_desk_project(actions.pool, None, {"project": "neo"})
    debts = [r["debt"] for r in rows]
    assert any("pick a signing strategy" in d for d in debts)
    assert not any("not this project" in d for d in debts)


async def test_desk_project_names_the_missing_project_honestly() -> None:
    rows = await _fn_desk_project(None, None, {})  # type: ignore[arg-type]
    assert "no project given" in rows[0]["debt"]


async def test_desk_project_degrades_honestly_on_a_pool_failure() -> None:
    class _BrokenPool:
        def __getattr__(self, name: str) -> object:
            async def _raise(*args: object, **kwargs: object) -> None:
                raise ConnectionError("pool gone")
            return _raise

    rows = await _fn_desk_project(_BrokenPool(), None, {"project": "neo"})  # type: ignore[arg-type]
    assert rows == [{"debt": "desk data unavailable", "kind": "-"}]


async def test_desk_composition_end_to_end(actions: Actions) -> None:
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent="agent:a", from_project="neo",
                       to_project="operator", body="pick a signing strategy",
                       desk_kind="decision")

    await save_composition(actions.pool, "desk", {"op": "function", "name": "desk_overview"})
    res = await run_composition(actions.pool, "desk")
    assert res["kind"] == "data"
    assert "neo" in [p["project"] for p in res["items"]["by_project"]]


# --- row_action on a `function` node (msg 1952, gating msg 1950's proposal) — SERVER HALF
# ONLY. A Function's row is already its own facts, so args resolve via row.get(property)
# directly, no `_props`/`_col_value` indirection the way `_table`'s object-backed version
# needs. Not wired into any real composition's own saved spec yet — the client half
# (table() recognizing `_action` as a control, a "run:" dispatch) isn't built. UPDATE
# (37af8b7): the singular client now IS built and browser-verified — see the row_actions
# (plural) block below for what's still not.

async def test_function_row_action_resolves_args_from_the_rows_own_keys(
    actions: Actions,
) -> None:
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent="agent:a", from_project="osiris",
                       to_project="neo", body="a project lane message")

    spec = {"op": "function", "name": "mail_overview",
            "row_action": {"action": "run:mail_threads", "args": {"box": {"property": "box"}}}}
    res = await run_composition(actions.pool, await _save(actions, "mail-drill", spec))
    assert res["kind"] == "rows"
    row = next(r for r in res["items"] if r["box"] == "neo")
    assert row["_action"] == {"action": "run:mail_threads", "args": {"box": "neo"}}


async def test_function_row_action_is_absent_without_a_declared_row_action(
    actions: Actions,
) -> None:
    """No row_action on the node -> no `_action` key at all (never a default, never
    inferred) — the same opt-in discipline `table`'s own row_action already follows."""
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent="agent:a", from_project="osiris",
                       to_project="neo", body="a project lane message")

    spec = {"op": "function", "name": "mail_overview"}
    res = await run_composition(actions.pool, await _save(actions, "mail-no-drill", spec))
    assert all("_action" not in r for r in res["items"])


async def test_function_row_action_does_not_apply_to_dict_shaped_output(
    actions: Actions,
) -> None:
    """A dict-shaped Function (kind stays "data", task #60) has no rows to attach a
    per-row control to — row_action is silently a no-op there, never an error."""
    spec = {"op": "function", "name": "fleet_pulse_line",
            "row_action": {"action": "run:whatever", "args": {}}}
    res = await run_composition(actions.pool, await _save(actions, "pulse-drill", spec))
    assert res["kind"] == "data"
    assert isinstance(res["items"], str)


# --- row_actions (plural) on a `function` node (Thoth msg 1976, gating msg 1971's proposal)
# — SERVER GRAMMAR ONLY. A row that affords more than one verb (chrome.py's /desk: done/not
# mine/later, three DIFFERENT actions on one debt row) needs more than row_action's single
# {action, args}. `row_actions` is a list of {label, action, args}; each row gets
# `_actions: [...]`. Its own arg templates add `{"literal": v}` alongside `{"property": p}`
# (via `_row_action_arg`) — refusing loudly on either malformed shape rather than picking a
# silent winner. NOT wired into any saved composition's own spec — the client has no case
# for `_actions` (plural) yet, same "don't arm a control with no client" discipline
# row_action's own build (89df464) already followed for the singular form.

async def test_function_row_actions_produces_a_labeled_action_list_per_row(
    actions: Actions,
) -> None:
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent="agent:a", from_project="osiris",
                       to_project="neo", body="a project lane message")

    spec = {"op": "function", "name": "mail_overview",
            "row_actions": [
                {"label": "done", "action": "resolve_thread",
                 "args": {"ref": {"property": "box"}, "because": {"literal": "operator: done"}}},
                {"label": "later", "action": "defer_thread",
                 "args": {"ref": {"property": "box"}, "days": {"literal": 30}}},
            ]}
    res = await run_composition(actions.pool, await _save(actions, "mail-triage", spec))
    row = next(r for r in res["items"] if r["box"] == "neo")
    assert row["_actions"] == [
        {"label": "done", "action": "resolve_thread",
         "args": {"ref": "neo", "because": "operator: done"}},
        {"label": "later", "action": "defer_thread", "args": {"ref": "neo", "days": 30}},
    ]


async def test_function_row_actions_label_falls_back_to_the_action_name(
    actions: Actions,
) -> None:
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent="agent:a", from_project="osiris",
                       to_project="neo", body="a project lane message")

    spec = {"op": "function", "name": "mail_overview",
            "row_actions": [{"action": "resolve_thread", "args": {}}]}
    res = await run_composition(actions.pool, await _save(actions, "mail-nolabel", spec))
    row = next(r for r in res["items"] if r["box"] == "neo")
    assert row["_actions"][0]["label"] == "resolve_thread"


async def test_function_row_actions_is_absent_without_a_declared_row_actions(
    actions: Actions,
) -> None:
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent="agent:a", from_project="osiris",
                       to_project="neo", body="a project lane message")

    spec = {"op": "function", "name": "mail_overview"}
    res = await run_composition(actions.pool, await _save(actions, "mail-no-triage", spec))
    assert all("_actions" not in r for r in res["items"])


async def test_row_action_arg_resolves_a_literal_constant() -> None:
    from src.orchestrator.compositions import _row_action_arg

    assert _row_action_arg({"box": "neo"}, {"literal": "operator: done"}) == "operator: done"
    assert _row_action_arg({"box": "neo"}, {"literal": 30}) == 30


async def test_row_action_arg_resolves_a_property_lookup_including_list_values() -> None:
    from src.orchestrator.compositions import _row_action_arg

    row = {"box": "neo", "thread_folded_ids": [1, 2, 3]}
    assert _row_action_arg(row, {"property": "box"}) == "neo"
    # a list-valued property passes through unchanged — no separate bulk-arg primitive
    assert _row_action_arg(row, {"property": "thread_folded_ids"}) == [1, 2, 3]


async def test_row_action_arg_refuses_when_both_literal_and_property_are_given() -> None:
    from src.orchestrator.compositions import _row_action_arg

    with pytest.raises(ValueError, match="BOTH literal and property"):
        _row_action_arg({"box": "neo"}, {"literal": "x", "property": "box"})


async def test_row_action_arg_refuses_on_an_unknown_spec_key() -> None:
    from src.orchestrator.compositions import _row_action_arg

    with pytest.raises(ValueError, match="unknown key"):
        _row_action_arg({"box": "neo"}, {"value": "box"})


async def test_row_action_arg_refuses_on_an_empty_spec() -> None:
    from src.orchestrator.compositions import _row_action_arg

    with pytest.raises(ValueError, match="needs literal or property"):
        _row_action_arg({"box": "neo"}, {})


async def test_no_default_composition_arms_row_actions_yet() -> None:
    """The client has no case for `_actions` (plural) yet — arming it in a real, saved
    composition would render a raw JSON blob (msg 1976's own rule, applied to itself)."""

    def _has_row_actions(node: Any) -> bool:
        if isinstance(node, dict):
            if "row_actions" in node:
                return True
            return any(_has_row_actions(v) for v in node.values())
        if isinstance(node, list):
            return any(_has_row_actions(v) for v in node)
        return False

    armed = [name for name, spec in DEFAULT_COMPOSITIONS.items() if _has_row_actions(spec)]
    assert armed == []

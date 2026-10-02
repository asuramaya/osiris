"""The whole-graph typed-array snapshot and its outbox-backed delta poll."""
from __future__ import annotations

import asyncio
import re
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.orchestrator.graph_layout import layout_batch
from src.orchestrator.graph_stream import (
    _short_label,
    decode_snapshot,
    deltas_since,
    encode_snapshot,
    fetch_snapshot,
    outbox_watermark,
    resolve_deltas_start_cursor,
)

HELPERS = Path(__file__).parent.parent / "helpers"


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    from src.orchestrator.manifests import load_manifests

    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(HELPERS)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# --- pure encode/decode: the basic encode/decode round trip ---------------------------


def test_snapshot_round_trips_through_the_decoder_with_the_exact_count() -> None:
    data = encode_snapshot(
        object_ids=["a", "b", "c"], x=[1.0, 2.0, 3.0], y=[4.0, 5.0, 6.0],
        type_code=[0, 1, 0], project_code=[0, 0, 1], weight=[2.0, 0.0, 5.0],
        status_flag=[0, 2, 0], edge_src=[0, 1], edge_dst=[1, 2], edge_type_code=[0, 1],
        edge_weight=[0.5, 1.0],
        types=["Thread", "Decision"], projects=["repo:x", "repo:y"],
        edge_types=["cites", "in_repo"], link_type_class=["semantic", "structural"],
        labels=["Thread abc", "Decision def", "Thread ghi"],
        created_at=[100.0, 200.0, 300.0],
        project_aggregates=[{"project": 0, "count": 2, "cx": 1.5, "cy": 4.5, "radius": 1.0}],
        cluster_edges=[{"a": 0, "b": 1, "class": "semantic", "count": 1}],
        type_pair_edges=[{"a": {"project": 0, "type": 0}, "b": {"project": 0, "type": 1},
                           "class": "semantic", "count": 1}],
        community_code=[1, 1, 0],
        communities=[{"community": 1, "project": 1, "count": 2, "cx": 1.5, "cy": 4.5,
                      "radius": 0.5}],
    )
    out = decode_snapshot(data)
    assert out["count"] == 3
    assert out["edge_count"] == 2
    assert out["object_ids"] == ["a", "b", "c"]
    assert out["types"] == ["Thread", "Decision"]
    assert out["projects"] == ["repo:x", "repo:y"]
    assert out["edge_types"] == ["cites", "in_repo"]
    assert out["link_type_class"] == ["semantic", "structural"]
    assert out["labels"] == ["Thread abc", "Decision def", "Thread ghi"]
    assert out["project_aggregates"] == [
        {"project": 0, "count": 2, "cx": 1.5, "cy": 4.5, "radius": 1.0}]
    assert out["type_aggregates"] == []
    assert out["cluster_edges"] == [{"a": 0, "b": 1, "class": "semantic", "count": 1}]
    assert out["type_pair_edges"] == [
        {"a": {"project": 0, "type": 0}, "b": {"project": 0, "type": 1},
         "class": "semantic", "count": 1}]
    assert out["x"] == pytest.approx([1.0, 2.0, 3.0])
    assert out["y"] == pytest.approx([4.0, 5.0, 6.0])
    assert out["type_code"] == [0, 1, 0]
    assert out["project_code"] == [0, 0, 1]
    assert out["weight"] == pytest.approx([2.0, 0.0, 5.0])
    assert out["status_flag"] == [0, 2, 0]
    assert out["edge_src"] == [0, 1]
    assert out["edge_dst"] == [1, 2]
    assert out["edge_type_code"] == [0, 1]
    assert out["edge_weight"] == pytest.approx([0.5, 1.0])
    assert out["created_at"] == pytest.approx([100.0, 200.0, 300.0])
    assert out["community_code"] == [1, 1, 0]
    assert out["communities"] == [
        {"community": 1, "project": 1, "count": 2, "cx": 1.5, "cy": 4.5, "radius": 0.5}]


def test_snapshot_community_code_defaults_to_all_zeros() -> None:
    """A caller that never passes `community_code` (every pre-existing test, and
    any live population where no project exceeds `_COMMUNITY_MIN_MEMBERS`) must
    still get a valid, index-aligned all-zero array, never a missing key or a
    length mismatch. 0 is the sentinel for "no real community", same as
    `project_aggregates`' own empty-list default for "no aggregates computed"."""
    data = encode_snapshot(
        object_ids=["a", "b"], x=[0.0, 1.0], y=[0.0, 1.0], type_code=[0, 0],
        project_code=[0, 0], weight=[0.0, 0.0], status_flag=[0, 0],
        edge_src=[], edge_dst=[], edge_type_code=[], edge_weight=[],
        types=["Thread"], projects=["unfiled"], edge_types=[], link_type_class=[],
        labels=["a", "b"], created_at=[0.0, 0.0],
    )
    out = decode_snapshot(data)
    assert out["community_code"] == [0, 0]
    assert out["communities"] == []


def test_encode_snapshot_rejects_a_mismatched_community_code_length() -> None:
    with pytest.raises(ValueError, match="community_code has"):
        encode_snapshot(
            object_ids=["a", "b"], x=[1.0, 2.0], y=[1.0, 2.0], type_code=[0, 0],
            project_code=[0, 0], weight=[0.0, 0.0], status_flag=[0, 0],
            edge_src=[], edge_dst=[], edge_type_code=[], edge_weight=[],
            types=["Thread"], projects=["unfiled"], edge_types=[], link_type_class=[],
            labels=["a", "b"], created_at=[0.0, 0.0], community_code=[1],
        )


def test_snapshot_with_no_edges_still_round_trips() -> None:
    data = encode_snapshot(
        object_ids=["only"], x=[0.0], y=[0.0], type_code=[0], project_code=[0],
        weight=[0.0], status_flag=[0], edge_src=[], edge_dst=[], edge_type_code=[],
        edge_weight=[],
        types=["Thread"], projects=["unfiled"], edge_types=[], link_type_class=[],
        labels=["Thread only"], created_at=[0.0],
    )
    out = decode_snapshot(data)
    assert out["count"] == 1
    assert out["edge_count"] == 0
    assert out["edge_src"] == []
    assert out["edge_weight"] == []
    assert out["project_aggregates"] == []
    assert out["type_aggregates"] == []
    assert out["cluster_edges"] == []
    assert out["type_pair_edges"] == []


def test_encode_snapshot_rejects_a_mismatched_node_column_length() -> None:
    with pytest.raises(ValueError, match="x has"):
        encode_snapshot(
            object_ids=["a", "b"], x=[1.0], y=[1.0, 2.0], type_code=[0, 0],
            project_code=[0, 0], weight=[0.0, 0.0], status_flag=[0, 0],
            edge_src=[], edge_dst=[], edge_type_code=[], edge_weight=[], types=[],
            projects=[], edge_types=[], link_type_class=[], labels=["a", "b"],
            created_at=[0.0, 0.0],
        )


def test_encode_snapshot_rejects_a_mismatched_edge_column_length() -> None:
    with pytest.raises(ValueError, match="edge_dst has"):
        encode_snapshot(
            object_ids=["a"], x=[1.0], y=[1.0], type_code=[0], project_code=[0],
            weight=[0.0], status_flag=[0], edge_src=[0, 0], edge_dst=[0],
            edge_type_code=[0, 0], edge_weight=[0.0, 0.0], types=[], projects=[],
            edge_types=[], link_type_class=[], labels=["a"], created_at=[0.0],
        )


def test_encode_snapshot_rejects_a_mismatched_labels_length() -> None:
    with pytest.raises(ValueError, match="labels has"):
        encode_snapshot(
            object_ids=["a", "b"], x=[1.0, 2.0], y=[1.0, 2.0], type_code=[0, 0],
            project_code=[0, 0], weight=[0.0, 0.0], status_flag=[0, 0],
            edge_src=[], edge_dst=[], edge_type_code=[], edge_weight=[], types=[],
            projects=[], edge_types=[], link_type_class=[], labels=["only-one"],
            created_at=[0.0, 0.0],
        )


# --- _short_label: label formatting, roman numerals, truncation ------------------------


def test_short_label_agent_uses_agent_fallback_never_the_raw_handle() -> None:
    """A raw `handle` assertion used to win outright, bypassing the resolved
    identity format. Now `agent_fallback` (required for type Agent) always
    wins."""
    assert _short_label(
        "Agent", "agent:deadbeef-g1", "Vega", None, None,
        agent_fallback="Vega VII") == "Vega VII"


def test_short_label_agent_with_no_fallback_falls_through_to_title() -> None:
    """A degenerate case (agent_fallback somehow unresolved) still never reads
    the raw handle as a label of its own -- falls through to the generic
    title/canonical chain like any other type."""
    assert _short_label("Agent", "agent:deadbeef-g1", "Vega", None, "some title") == (
        "Agent some title")


def test_short_label_software_project_strips_the_repo_scheme() -> None:
    assert _short_label("SoftwareProject", "repo:osiris", None, None, None) == "osiris"


def test_short_label_person_uses_name() -> None:
    assert _short_label(
        "Person", "principal:xyz", None, "Ada Lovelace", None) == "Ada Lovelace"


def test_short_label_uses_title_when_present_never_canonical() -> None:
    """A Decision, Thread, or Message (or any type carrying a summary/title/
    subject/name assertion) must show that title, not the bare canonical id
    like "Decision decision:91da77...". A regression caught live in production."""
    assert _short_label(
        "Decision", "decision:91da776625f9", None, None,
        "switch to the new renderer") == "Decision switch to the new renderer"


def test_short_label_collapses_embedded_newlines_to_one_line() -> None:
    assert _short_label(
        "Thread", "thread:x", None, None, "line one\nline two") == "Thread line one line two"


def test_short_label_falls_back_to_type_plus_canonical_only_when_no_title(
) -> None:
    assert _short_label(
        "Thread", "thread:abc123", None, None, None) == "Thread thread:abc123"


def test_short_label_agent_without_a_handle_falls_back_to_title_then_canonical() -> None:
    assert _short_label("Agent", "agent:deadbeef-g1", None, None, None) == (
        "Agent agent:deadbeef-g1")
    assert _short_label("Agent", "agent:deadbeef-g1", None, None, "a real title") == (
        "Agent a real title")


def test_short_label_hard_truncates_at_40_chars_with_an_ellipsis() -> None:
    long_canonical = "thread:" + "x" * 60
    label = _short_label("Thread", long_canonical, None, None, None)
    assert len(label) == 40
    assert label.endswith("…")


def test_short_label_truncation_never_clips_the_sidechain_marker() -> None:
    """A hard slice at 40 chars used to be able to land inside (or just past)
    the trailing ' ⌊ sub' marker, producing unreadable noise like ' ⌊ su…' or
    dropping it outright. The marker must always survive whole, even if that
    means the label runs a few characters past the nominal ceiling."""
    long_fallback = "x" * 45 + " ⌊ sub"
    label = _short_label("Agent", "agent:deadbeef-g1", None, None, None,
                         agent_fallback=long_fallback)
    assert label.endswith(" ⌊ sub")
    assert "…" in label


# --- DB-backed: fetch_snapshot ---------------------------------------------------------


async def test_fetch_snapshot_includes_only_already_placed_objects(
    actions: Actions,
) -> None:
    a = await actions.create_or_find_object("Thread", "thread:gs-a", "test")
    b = await actions.create_or_find_object("Thread", "thread:gs-b", "test")
    await actions.create_link(a, b, "cites", "test", datetime.now(UTC), 1.0)
    await layout_batch(actions, limit=1000)

    data = await fetch_snapshot(actions.pool)
    out = decode_snapshot(data)
    assert str(a) in out["object_ids"]
    assert str(b) in out["object_ids"]
    assert out["count"] == len(out["object_ids"])
    assert out["edge_count"] >= 1
    assert "cites" in out["edge_types"]
    idx = out["edge_types"].index("cites")
    assert out["link_type_class"][idx] == "semantic"


async def test_fetch_snapshot_positions_match_graph_x_graph_y(actions: Actions) -> None:
    from src.orchestrator.graph_layout import positions_for

    oid = await actions.create_or_find_object("Thread", "thread:gs-pos", "test")
    await layout_batch(actions, limit=1000)
    expected = (await positions_for(actions, [oid]))[oid]

    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(oid))
    assert (out["x"][idx], out["y"][idx]) == pytest.approx(expected)


async def test_fetch_snapshot_excludes_unplaced_objects(actions: Actions) -> None:
    oid = await actions.create_or_find_object("Thread", "thread:gs-unplaced", "test")
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    assert str(oid) not in out["object_ids"]


async def test_fetch_snapshot_created_at_is_index_aligned_and_real(
    actions: Actions,
) -> None:
    # A real epoch timestamp per object, index-aligned to object_ids same as x/y:
    # a genuinely later-created object reads a later created_at, never a placeholder.
    before = datetime.now(UTC).timestamp()
    oid = await actions.create_or_find_object("Thread", "thread:gs-created-at", "test")
    await layout_batch(actions, limit=1000)
    after = datetime.now(UTC).timestamp()

    out = decode_snapshot(await fetch_snapshot(actions.pool))
    assert len(out["created_at"]) == out["count"]
    idx = out["object_ids"].index(str(oid))
    # Float32 on the wire, not float64: at today's epoch magnitude (~1.79e9) the ULP is
    # 128s, so a value can round up to ~64s past its real value -- tolerance reflects
    # that honestly rather than asserting a sub-second precision the wire doesn't carry.
    assert before - 100 <= out["created_at"][idx] <= after + 100


async def test_fetch_snapshot_community_code_is_index_aligned_and_zero_below_threshold(
    actions: Actions,
) -> None:
    """Exercises the live query wiring end to end, for the ordinary case: a
    project well under `_COMMUNITY_MIN_MEMBERS` (500) has no real Leiden
    community, so every member's own `community_code` reads 0 and `communities`
    stays empty, never a crash or a missing key (a hermetic 500+-member
    population isn't worth the setup cost here; the pure encode/decode tests
    above already cover a real non-zero code end to end)."""
    proj = await actions.create_or_find_object("SoftwareProject", "repo:gs-community", "test")
    a = await actions.create_or_find_object("Thread", "thread:gs-comm-a", "test")
    b = await actions.create_or_find_object("Thread", "thread:gs-comm-b", "test")
    now = datetime.now(UTC)
    await actions.create_link(a, proj, "in_repo", "test", now, 1.0)
    await actions.create_link(b, proj, "in_repo", "test", now, 1.0)
    await actions.create_link(a, b, "cites", "test", now, 1.0)
    await layout_batch(actions, limit=1000)

    out = decode_snapshot(await fetch_snapshot(actions.pool))
    assert len(out["community_code"]) == out["count"]
    ia, ib = out["object_ids"].index(str(a)), out["object_ids"].index(str(b))
    assert out["community_code"][ia] == 0
    assert out["community_code"][ib] == 0
    assert out["communities"] == []


async def test_fetch_snapshot_project_falls_back_to_the_project_assertion(
    actions: Actions,
) -> None:
    """A member with no in_repo link but a `project` assertion must still surface
    under that project's own canonical in the snapshot. The renderer's labels,
    counts and filter all key on this same `projects` array."""
    await actions.create_or_find_object("SoftwareProject", "repo:gs-union", "test")
    member = await actions.create_or_find_object("Thread", "thread:gs-union-member", "test")
    now = datetime.now(UTC)
    await actions.assert_property(member, "project", "gs-union", "test", now, 1.0)
    await layout_batch(actions, limit=1000)

    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(member))
    pcode = out["project_code"][idx]
    assert out["projects"][pcode] == "repo:gs-union"
    assert out["projects"][pcode] != "unfiled"


async def test_fetch_snapshot_project_follows_a_merge(actions: Actions) -> None:
    """A member's own in_repo link can name a SoftwareProject that has since
    been folded into a survivor. The snapshot must key the object under the
    survivor's own live canonical, never the now-merged dupe's: the same rule
    graph_physics' own membership union follows."""
    survivor = await actions.create_or_find_object(
        "SoftwareProject", "repo:gs-merge-survivor", "test")
    dupe = await actions.create_or_find_object(
        "SoftwareProject", "repo:gs-merge-dupe", "test")
    member = await actions.create_or_find_object("Thread", "thread:gs-merge-member", "test")
    now = datetime.now(UTC)
    await actions.create_link(member, dupe, "in_repo", "test", now, 1.0)
    await actions.merge_objects(survivor, dupe, "test merge", "test")
    await layout_batch(actions, limit=1000)

    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(member))
    pcode = out["project_code"][idx]
    assert out["projects"][pcode] == "repo:gs-merge-survivor"


async def test_fetch_snapshot_project_follows_a_merge_via_the_project_assertion(
    actions: Actions,
) -> None:
    """The same fold, reached through the `project`-assertion fallback instead of a
    live in_repo link."""
    survivor = await actions.create_or_find_object(
        "SoftwareProject", "repo:gs-merge-survivor-2", "test")
    dupe = await actions.create_or_find_object(
        "SoftwareProject", "repo:gs-merge-dupe-2", "test")
    member = await actions.create_or_find_object(
        "Thread", "thread:gs-merge-assertion-member", "test")
    now = datetime.now(UTC)
    await actions.assert_property(member, "project", "gs-merge-dupe-2", "test", now, 1.0)
    await actions.merge_objects(survivor, dupe, "test merge", "test")
    await layout_batch(actions, limit=1000)

    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(member))
    pcode = out["project_code"][idx]
    assert out["projects"][pcode] == "repo:gs-merge-survivor-2"


async def test_fetch_snapshot_watermark_matches_the_live_outbox_tip(
    actions: Actions,
) -> None:
    """The header's own watermark is what a client passes to
    /graph/stream/deltas?since= to pick up only what changed after this exact
    snapshot. It must agree with outbox_watermark's own live read."""
    await actions.create_or_find_object("Thread", "thread:gs-watermark", "test")
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    assert out["watermark"] == await outbox_watermark(actions.pool)


async def test_fetch_snapshot_labels_index_align_with_object_ids(actions: Actions) -> None:
    """Labels line up with object_ids at the same index, same as every other column."""
    oid = await actions.create_or_find_object("Thread", "thread:gs-label", "test")
    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(oid))
    assert out["labels"][idx] == "Thread thread:gs-label"


async def test_fetch_snapshot_labels_use_the_real_title_not_the_canonical(
    actions: Actions,
) -> None:
    """A Decision, a Thread, and a Commit each carrying a title-shaped assertion
    (summary/title/subject) must show it, never the bare canonical. The exact
    regression caught live in production."""
    now = datetime.now(UTC)
    decision = await actions.create_or_find_object(
        "Decision", "decision:gs-title-a", "test")
    await actions.assert_property(
        decision, "summary", "a real decision summary", "test", now, 0.9)
    thread_obj = await actions.create_or_find_object("Thread", "thread:gs-title-b", "test")
    await actions.assert_property(
        thread_obj, "summary", "a real thread summary", "test", now, 0.9)
    commit_obj = await actions.create_or_find_object("Commit", "commit:gs-title-c", "test")
    await actions.assert_property(
        commit_obj, "subject", "a real commit subject", "test", now, 0.9)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))

    for oid, expected in (
        (decision, "Decision a real decision summary"),
        (thread_obj, "Thread a real thread summary"),
        (commit_obj, "Commit a real commit subject"),
    ):
        idx = out["object_ids"].index(str(oid))
        assert out["labels"][idx] == expected


async def test_fetch_snapshot_nameless_agent_falls_back_to_seat_handle_and_generation(
    actions: Actions,
) -> None:
    """A handle-less Agent whose lineage holds an active Seat labels as
    "<seat handle> <generation>", never its own canonical id. Every agent label
    carries a numeral: generation 1 shows "I" too, not a bare handle."""
    now = datetime.now(UTC)
    seat = await actions.create_or_find_object("Seat", "seat:gs-nameless-a", "test")
    await actions.assert_property(seat, "handle", "Nefer", "test", now, 0.9)
    agent = await actions.create_or_find_object("Agent", "agent:gs-nameless-a-g1", "test")
    await actions.create_link(agent, seat, "holds", "test", now, 1.0)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    assert out["labels"][idx] == "Nefer I"


async def test_fetch_snapshot_nameless_seat_holder_generation_is_uppercase_roman(
    actions: Actions,
) -> None:
    """Generation numerals in a seat holder's label render uppercase Roman
    numerals uniformly (e.g. "Nova CVI", "Orion XXXVIII"); the seat branch's
    own generation display used to render lowercase."""
    now = datetime.now(UTC)
    seat = await actions.create_or_find_object("Seat", "seat:gs-nameless-roman", "test")
    await actions.assert_property(seat, "handle", "Nova", "test", now, 0.9)
    agent = await actions.create_or_find_object("Agent", "agent:gs-nameless-roman-vii", "test")
    await actions.create_link(agent, seat, "holds", "test", now, 1.0)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    assert out["labels"][idx] == "Nova VII"


async def test_fetch_snapshot_seat_generation_past_39_uses_the_display_roman(
    actions: Actions,
) -> None:
    """The seat branch called `agents._to_roman`, the canonical-id-suffix
    formatter: capped at 39, falling back to a raw "g<n>" escape hatch above
    that (built for a mintable id, hex-collision-free, never meant to reach a
    human-facing label). Any seat holder past generation 39 hit this.
    `agents._roman_display` (unbounded, already uppercase, the same function
    `seat_label` itself uses) is the right one: 107 renders as "CVII", never
    "G107"."""
    now = datetime.now(UTC)
    seat = await actions.create_or_find_object("Seat", "seat:gs-past-39", "test")
    await actions.assert_property(seat, "handle", "Nova", "test", now, 0.9)
    agent = await actions.create_or_find_object("Agent", "agent:gs-past-39-holder", "test")
    await actions.assert_property(agent, "seat_generation", "107", "test", now, 0.9)
    await actions.create_link(agent, seat, "holds", "test", now, 1.0)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    label = out["labels"][idx]
    assert label == "Nova CVII"
    assert not re.search(r"G\d+", label)


async def test_fetch_snapshot_seat_holder_label_folds_in_the_model_suffix(
    actions: Actions,
) -> None:
    """The seat branch used to never read `source_model` at all: "Nova VII",
    never "Nova VII · sonnet-5", even when the assertion existed. Folded on
    now, uniformly with the patronym and canonical-stem branches."""
    now = datetime.now(UTC)
    seat = await actions.create_or_find_object("Seat", "seat:gs-seat-model", "test")
    await actions.assert_property(seat, "handle", "Iris", "test", now, 0.9)
    agent = await actions.create_or_find_object("Agent", "agent:gs-seat-model-holder", "test")
    await actions.assert_property(agent, "seat_generation", "65", "test", now, 0.9)
    await actions.assert_property(agent, "source_model", "claude-haiku-4-5", "test", now, 0.9)
    await actions.create_link(agent, seat, "holds", "test", now, 1.0)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    assert out["labels"][idx] == "Iris LXV · haiku-4-5"


async def test_fetch_snapshot_seat_holder_label_capitalises_a_lowercase_handle(
    actions: Actions,
) -> None:
    """A seat's own `handle` assertion is stored in whatever casing it was first
    claimed with ('nova' next to 'Vega'). The seat branch must display-case an
    all-lowercase one so the header never shows 'nova CVII' beside 'Vega LXXII'."""
    now = datetime.now(UTC)
    seat = await actions.create_or_find_object("Seat", "seat:gs-seat-lowercase", "test")
    await actions.assert_property(seat, "handle", "nova", "test", now, 0.9)
    agent = await actions.create_or_find_object("Agent", "agent:gs-seat-lowercase-holder", "test")
    await actions.assert_property(agent, "seat_generation", "107", "test", now, 0.9)
    await actions.create_link(agent, seat, "holds", "test", now, 1.0)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    assert out["labels"][idx] == "Nova CVII"


async def test_fetch_snapshot_seat_holder_label_leaves_mixed_case_handle_untouched(
    actions: Actions,
) -> None:
    """The capitalisation fix must never touch an already-cased handle ('Vega') --
    only the all-lowercase shape."""
    now = datetime.now(UTC)
    seat = await actions.create_or_find_object("Seat", "seat:gs-seat-mixed-case", "test")
    await actions.assert_property(seat, "handle", "Vega", "test", now, 0.9)
    agent = await actions.create_or_find_object("Agent", "agent:gs-seat-mixed-case-holder", "test")
    await actions.assert_property(agent, "seat_generation", "72", "test", now, 0.9)
    await actions.create_link(agent, seat, "holds", "test", now, 1.0)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    assert out["labels"][idx] == "Vega LXXII"


async def test_fetch_snapshot_seat_succession_canonical_reads_the_real_generation(
    actions: Actions,
) -> None:
    """A seat-succession canonical (agent:seat-<id>-g<N>) stamps its own ordinal
    as a seat_generation assertion, not in the canonical string the way an
    ordinary lineage id is. `_generation(canonical)` used to silently read 1 for
    a low "-g3" suffix (it only recognizes "-g<N>" as a generation marker for
    N > 39, the numeric-overflow escape hatch), dropping the numeral and
    printing a bare "Orion" instead of "Orion III"."""
    now = datetime.now(UTC)
    seat = await actions.create_or_find_object("Seat", "seat:gs-succession-gen", "test")
    await actions.assert_property(seat, "handle", "Orion", "test", now, 0.9)
    agent = await actions.create_or_find_object(
        "Agent", "agent:seat-gs-succession-gen-g3", "test")
    await actions.assert_property(agent, "seat_generation", "3", "test", now, 0.9)
    await actions.create_link(agent, seat, "holds", "test", now, 1.0)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    assert out["labels"][idx] == "Orion III"


async def test_fetch_snapshot_nameless_agent_without_a_seat_falls_back_to_patronym_and_model(
    actions: Actions,
) -> None:
    """No handle, no held Seat: "<patronym> <ROMAN> · <model short>", never "?".
    A large fraction of live agents have exactly this shape, which the old
    "Agent · <model> in <project>" fallback broke on, since that scheme never
    read `patronym` at all."""
    now = datetime.now(UTC)
    agent = await actions.create_or_find_object("Agent", "agent:gs-nameless-b-vii", "test")
    await actions.assert_property(agent, "source_model", "claude-sonnet-5", "test", now, 0.9)
    await actions.assert_property(agent, "patronym", "Orion", "test", now, 0.9)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    assert out["labels"][idx] == "Orion VII · sonnet-5"


async def test_fetch_snapshot_nameless_agent_generation_one_shows_a_roman_numeral(
    actions: Actions,
) -> None:
    """Generation 1 used to omit the roman suffix, indistinguishable from the
    old handle-bypass. Show "I" like every other generation."""
    now = datetime.now(UTC)
    agent = await actions.create_or_find_object("Agent", "agent:gs-nameless-gen1", "test")
    await actions.assert_property(agent, "source_model", "claude-opus-5", "test", now, 0.9)
    await actions.assert_property(agent, "patronym", "Juno", "test", now, 0.9)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    assert out["labels"][idx] == "Juno I · opus-5"


async def test_fetch_snapshot_nameless_sidechain_agent_gets_the_sub_marker(
    actions: Actions,
) -> None:
    now = datetime.now(UTC)
    agent = await actions.create_or_find_object("Agent", "agent:gs-nameless-sub-iii", "test")
    await actions.assert_property(agent, "source_model", "claude-sonnet-5", "test", now, 0.9)
    await actions.assert_property(agent, "patronym", "Iris", "test", now, 0.9)
    await actions.assert_property(agent, "is_sidechain", True, "test", now, 0.9)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    assert out["labels"][idx] == "Iris III · sonnet-5 ⌊ sub"


async def test_fetch_snapshot_nameless_agent_without_a_patronym_falls_back_to_canonical(
    actions: Actions,
) -> None:
    """Never "?": a patronym-less, seat-less, handle-less Agent still resolves to a
    real, resolvable name -- its own canonical short id, not a raw "?"."""
    now = datetime.now(UTC)
    agent = await actions.create_or_find_object("Agent", "agent:gs-nameless-nopat", "test")
    await actions.assert_property(agent, "source_model", "claude-sonnet-5", "test", now, 0.9)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    # The model suffix is folded onto the canonical-stem fallback too, not
    # just the patronym/seat branches.
    assert out["labels"][idx] == "gs-nameless-nopat · sonnet-5"
    assert "?" not in out["labels"][idx]


async def test_fetch_snapshot_agent_with_only_a_bare_handle_still_gets_the_identity_format(
    actions: Actions,
) -> None:
    """A number of live agents have a raw `handle` assertion (their lineage's
    own bare stamp, e.g. "Nova") and no `patronym`. `_short_label`'s old
    handle-first branch used it directly as the label, no generation, no model,
    bypassing the identity resolver entirely. That raw handle must now only
    seed the patronym slot, still producing a real "<name> <ROMAN>"."""
    now = datetime.now(UTC)
    agent = await actions.create_or_find_object(
        "Agent", "agent:gs-bare-handle-vii", "test")
    await actions.assert_property(agent, "handle", "Nova", "test", now, 0.9)
    await actions.assert_property(agent, "source_model", "claude-sonnet-5", "test", now, 0.9)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    assert out["labels"][idx] == "Nova VII · sonnet-5"


async def test_fetch_snapshot_agent_name_assertion_is_never_read_for_the_stem(
    actions: Actions,
) -> None:
    """A prior fix chased known auto-generated `name` shapes one regex at a
    time ("<model> in <project>", "<agent_type> spawn") and missed a third
    ("<model> · <description>", lineage.py:247, e.g. "claude-opus-4-8 ·
    Reconcile memory batch"). The `name` assertion is now never read for an
    Agent's stem at all, regardless of shape: covers the open-ended set in one
    structural rule instead of chasing it one detector at a time."""
    now = datetime.now(UTC)
    agent = await actions.create_or_find_object("Agent", "agent:gs-name-never-read", "test")
    await actions.assert_property(
        agent, "name", "claude-opus-4-8 · Reconcile memory batches", "test", now, 0.9)
    await actions.assert_property(agent, "source_model", "claude-opus-4-8", "test", now, 0.9)
    await actions.assert_property(agent, "is_sidechain", True, "test", now, 0.9)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    label = out["labels"][idx]
    assert "Reconcile" not in label
    # no patronym and no handle either -- falls to the canonical stem, same as
    # any other totally nameless agent (test_fetch_snapshot_nameless_agent_
    # without_a_patronym_falls_back_to_canonical's own precedent), with the
    # model suffix and sub marker still folded on.
    assert label == "gs-name-never-read · opus-4-8 ⌊ sub"


async def test_fetch_snapshot_agent_handle_stripped_to_its_leading_alphabetic_run(
    actions: Actions,
) -> None:
    """A legacy/malformed `handle` assertion carrying trailing noise past the
    real name must not seed the stem verbatim: only its own leading alphabetic
    run, the "stripped to letters" rule from the structural label formula.
    That stripped stem is also display-cased now ('nova' -> 'Nova'): a raw
    handle's own storage casing must never read differently from a
    patronym-derived stem, which is always correct case already."""
    now = datetime.now(UTC)
    agent = await actions.create_or_find_object("Agent", "agent:gs-handle-noise", "test")
    await actions.assert_property(agent, "handle", "nova G58", "test", now, 0.9)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    label = out["labels"][idx]
    assert re.match(r"^Nova [IVXLCDM]+$", label), label


async def test_fetch_snapshot_compound_patronym_never_gets_a_second_numeral(
    actions: Actions,
) -> None:
    """`patronym_for` (lineage.py) mints a spawned child's own `patronym`
    assertion as the compound "<parent stem> <parent ROMAN>.<birth ordinal>"
    (e.g. "Nova I.1"). A prior fix appended this child's own freshly-computed
    generation roman on top of that already-complete compound, producing
    "Nova I.1 I". The compound's own embedded roman+ordinal is now read as the
    whole generation field; nothing is appended a second time."""
    now = datetime.now(UTC)
    agent = await actions.create_or_find_object("Agent", "agent:gs-compound-patronym", "test")
    await actions.assert_property(agent, "patronym", "Nova I.1", "test", now, 0.9)
    await actions.assert_property(
        agent, "source_model", "claude-haiku-4-5-20251001", "test", now, 0.9)
    await actions.assert_property(agent, "is_sidechain", True, "test", now, 0.9)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    assert out["labels"][idx] == "Nova I.1 · haiku-4-5-20251001 ⌊ sub"


async def test_fetch_snapshot_compound_patronym_capitalises_a_lowercase_stem(
    actions: Actions,
) -> None:
    """`patronym_for` stamps a spawned child's compound patronym with the
    parent's own stem verbatim, so a parent claimed under a lowercase seat
    handle ('crane') produces a compound like 'crane I.1' for every one of its
    children too: one hop removed from the seat branch's own casing bug, same
    fix applies."""
    now = datetime.now(UTC)
    agent = await actions.create_or_find_object(
        "Agent", "agent:gs-compound-patronym-lowercase", "test")
    await actions.assert_property(agent, "patronym", "crane I.1", "test", now, 0.9)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    idx = out["object_ids"].index(str(agent))
    assert out["labels"][idx] == "Crane I.1"


async def test_fetch_snapshot_edge_weight_is_raw_flat_for_now(actions: Actions) -> None:
    """edge_weight is raw, never a pre-normalised curve: flat 1.0 for every
    individual link today, since there is no per-edge-weight consumer yet
    (a consumer would log2-normalise client-side when it needs a curve, from
    a raw value this array provides)."""
    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Thread", "thread:gs-ew-a", "test")
    b = await actions.create_or_find_object("Thread", "thread:gs-ew-b", "test")
    await actions.create_link(a, b, "cites", "test", now, 1.0)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    assert out["edge_count"] >= 1
    assert all(w == pytest.approx(1.0) for w in out["edge_weight"])


async def test_fetch_snapshot_type_pair_edges_covers_same_project_cross_type(
    actions: Actions,
) -> None:
    """Unlike cluster_edges (cross-project only), type_pair_edges includes a
    same-project, different-type pair."""
    now = datetime.now(UTC)
    project = await actions.create_or_find_object(
        "SoftwareProject", "repo:gs-tpe", "test")
    person = await actions.create_or_find_object("Person", "principal:gs-tpe-person", "test")
    thread_obj = await actions.create_or_find_object("Thread", "thread:gs-tpe-thread", "test")
    for oid in (person, thread_obj):
        await actions.create_link(oid, project, "in_repo", "test", now, 1.0)
    await actions.create_link(person, thread_obj, "cites", "test", now, 1.0)

    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))

    pcode = out["project_code"][out["object_ids"].index(str(person))]
    tcode_person = out["type_code"][out["object_ids"].index(str(person))]
    tcode_thread = out["type_code"][out["object_ids"].index(str(thread_obj))]
    bucket_a = {"project": pcode, "type": tcode_person}
    bucket_b = {"project": pcode, "type": tcode_thread}
    found = [
        r for r in out["type_pair_edges"]
        if {tuple(sorted(r["a"].items())), tuple(sorted(r["b"].items()))} ==
           {tuple(sorted(bucket_a.items())), tuple(sorted(bucket_b.items()))}
    ]
    assert found and found[0]["count"] >= 1
    # cluster_edges must NOT carry this same-project pair (its own cross-project rule)
    assert not any(e["a"] == e["b"] == pcode for e in out["cluster_edges"])


async def test_fetch_snapshot_aggregates_carry_every_placed_object(
    actions: Actions,
) -> None:
    """Every project_aggregates entry's own count sums to the snapshot's total object
    count: nothing dropped, nothing double-counted."""
    await actions.create_or_find_object("Thread", "thread:gs-agg", "test")
    await layout_batch(actions, limit=1000)
    out = decode_snapshot(await fetch_snapshot(actions.pool))
    assert sum(a["count"] for a in out["project_aggregates"]) == out["count"]
    assert sum(a["count"] for a in out["type_aggregates"]) == out["count"]


async def test_fetch_snapshot_cluster_edges_only_cover_cross_project_pairs(
    actions: Actions,
) -> None:
    """A real cross-project semantic edge shows up as one cluster_edges record naming
    both projects' own codes and the edge's class."""
    now = datetime.now(UTC)
    proj_a = await actions.create_or_find_object("SoftwareProject", "repo:gs-cl-a", "test")
    proj_b = await actions.create_or_find_object("SoftwareProject", "repo:gs-cl-b", "test")
    a = await actions.create_or_find_object("Thread", "thread:gs-cl-a-member", "test")
    b = await actions.create_or_find_object("Thread", "thread:gs-cl-b-member", "test")
    await actions.create_link(a, proj_a, "in_repo", "test", now, 1.0)
    await actions.create_link(b, proj_b, "in_repo", "test", now, 1.0)
    await actions.create_link(a, b, "cites", "test", now, 1.0)  # semantic, cross-project

    while await layout_batch(actions, limit=1000) > 0:
        pass

    out = decode_snapshot(await fetch_snapshot(actions.pool))
    # a's/b's own MEMBERSHIP project code (via in_repo), not proj_a's/proj_b's own
    # code as objects (a SoftwareProject carries no in_repo link of its own, so it
    # sits in the "unfiled" bucket -- a different axis from which project it names)
    pa = out["project_code"][out["object_ids"].index(str(a))]
    pb = out["project_code"][out["object_ids"].index(str(b))]
    assert out["projects"][pa] == "repo:gs-cl-a"
    assert out["projects"][pb] == "repo:gs-cl-b"
    lo, hi = (pa, pb) if pa <= pb else (pb, pa)
    matches = [e for e in out["cluster_edges"] if e["a"] == lo and e["b"] == hi]
    assert any(e["class"] == "semantic" and e["count"] >= 1 for e in matches)


# --- DB-backed: deltas_since ------------------------------------------------------------


async def test_deltas_since_reports_a_newly_created_object(actions: Actions) -> None:
    deltas, cursor0 = await deltas_since(actions.pool, 0)
    oid = await actions.create_or_find_object("Thread", "thread:gs-delta", "test")

    deltas, cursor1 = await deltas_since(actions.pool, cursor0)
    assert cursor1 > cursor0
    assert any(d["id"] == str(oid) for d in deltas)


async def test_deltas_since_coalesces_to_the_latest_event_per_object_and_covers_the_rest(
    actions: Actions,
) -> None:
    """Two events for the same object within one page collapse into one delta
    (a live-state view only cares about the current value), and the cursor
    advances past both. A later call must not re-surface the coalesced-away
    first event."""
    oid = await actions.create_or_find_object("Thread", "thread:gs-coalesce", "test")
    _, cursor = await deltas_since(actions.pool, 0)
    await actions.assert_property(oid, "graph_x", 1.0, "test", datetime.now(UTC), 1.0)
    await actions.assert_property(oid, "graph_x", 2.0, "test", datetime.now(UTC), 1.0)

    deltas, cursor2 = await deltas_since(actions.pool, cursor)
    matches = [d for d in deltas if d["id"] == str(oid)]
    assert len(matches) == 1

    deltas3, cursor3 = await deltas_since(actions.pool, cursor2)
    assert deltas3 == []
    assert cursor3 == cursor2


async def test_deltas_since_is_empty_when_nothing_changed(actions: Actions) -> None:
    """Never assumes the hermetic test DB's own outbox backlog fits in one page:
    a real backlog beyond `limit` is expected to take several calls to drain,
    not one."""
    await actions.create_or_find_object("Thread", "thread:gs-quiet", "test")
    cursor = 0
    for _ in range(200):
        deltas, cursor = await deltas_since(actions.pool, cursor)
        if not deltas:
            break
    else:
        raise AssertionError("deltas_since never drained to quiescence")
    deltas2, cursor2 = await deltas_since(actions.pool, cursor)
    assert deltas2 == []
    assert cursor2 == cursor


# --- THE OUTBOX GAP: graph_layout._bulk_assert_positions used to write
# graph_x/graph_y/graph_layout_v straight against the assertions table, bypassing
# actions.assert_property -- the only thing that ever inserted an outbox row -- so a layout
# tick never showed up here, not even after the object was genuinely positioned. Confirmed
# live against a real scratch install before this fix: an open SSE connection sat
# indefinitely, no delta ever arriving, for an object the DB had already positioned.
# _LAYOUT_MOVED_EVENT closes it: one more outbox row per positioned object, per tick. -----


async def test_deltas_since_reports_a_layout_tick_moving_a_previously_unplaced_object(
    actions: Actions,
) -> None:
    oid = await actions.create_or_find_object("Thread", "thread:gs-layout-tick", "test")
    _, cursor = await deltas_since(actions.pool, 0)  # past the object_created event

    await layout_batch(actions, limit=1000)

    deltas, cursor2 = await deltas_since(actions.pool, cursor)
    assert cursor2 > cursor
    matches = [d for d in deltas if d["id"] == str(oid)]
    assert len(matches) == 1
    assert matches[0]["op"] == "moved"
    assert "x" in matches[0] and "y" in matches[0]


# A live-route test (opening /graph/stream/deltas via httpx.ASGITransport and reading its
# actual SSE body while the tick runs) was attempted, not skipped for lack of trying:
# httpx.ASGITransport never returns from entering the stream at all -- confirmed by
# isolating the bare `client.stream("GET", ...).__aenter__()` call with no other test logic
# around it, still hangs past a 5s wait_for with nothing else happening concurrently. This
# route's generator never terminates on its own (the same shape /cases/{id}/stream and
# /console/stream already use, one 1s-interval poll loop that only exits on client
# disconnect or app shutdown); no other test in this repo exercises any of those three
# routes' live SSE body through this test client either (grepped for it), which is the
# same limitation, not a coincidence. deltas_since is the entire route: the handler is a
# thin `while true: deltas_since; yield; sleep(1)` with no branching of its own, so the
# test above already proves everything the route could add on top.


# --- REST: GET /graph/stream (single response, safe to exercise via the test client) --


async def test_graph_stream_endpoint_returns_decodable_bytes(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    oid = await actions.create_or_find_object("Thread", "thread:gs-http", "test")
    await layout_batch(actions, limit=1000)

    r = await client.get("/graph/stream")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/octet-stream"
    out = decode_snapshot(r.content)
    assert str(oid) in out["object_ids"]


# --- resolve_deltas_start_cursor: pulled out for direct testing -----------------------

# A fresh connection never starts from cursor 0 any more.


async def test_resolve_cursor_takes_the_later_of_since_and_last_event_id(
    actions: Actions,
) -> None:
    """A browser's automatic reconnect re-requests the original url, which still carries the
    snapshot's own `since`, with a NEWER Last-Event-ID: the stream resumes where it got to,
    never from the snapshot again. A first connection has only `since`."""
    await actions.create_or_find_object("Thread", "thread:gs-cursor-since", "test")
    assert await resolve_deltas_start_cursor(
        actions.pool, since=42, last_event_id=None) == 42
    assert await resolve_deltas_start_cursor(
        actions.pool, since=42, last_event_id="999") == 999
    assert await resolve_deltas_start_cursor(
        actions.pool, since=1000, last_event_id="999") == 1000


async def test_resolve_cursor_falls_back_to_last_event_id(actions: Actions) -> None:
    cursor = await resolve_deltas_start_cursor(
        actions.pool, since=None, last_event_id="17")
    assert cursor == 17


async def test_resolve_cursor_defaults_to_the_live_watermark_never_zero(
    actions: Actions,
) -> None:
    """The exact regression this fix closes: no `since`, no Last-Event-ID must resolve
    to the CURRENT outbox tip, never cursor 0 (a full backlog replay)."""
    await actions.create_or_find_object("Thread", "thread:gs-cursor-default", "test")
    cursor = await resolve_deltas_start_cursor(
        actions.pool, since=None, last_event_id=None)
    assert cursor == await outbox_watermark(actions.pool)
    assert cursor > 0


async def test_resolve_cursor_falls_back_to_watermark_on_a_malformed_last_event_id(
    actions: Actions,
) -> None:
    await actions.create_or_find_object("Thread", "thread:gs-cursor-malformed", "test")
    cursor = await resolve_deltas_start_cursor(
        actions.pool, since=None, last_event_id="not-a-number")
    assert cursor == await outbox_watermark(actions.pool)


# --- THE SNAPSHOT CACHE: opening Browse must not wait for a rebuild ---------------------------


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _cache(builder: Any, clock: _Clock, **kw: float) -> Any:
    from src.orchestrator.graph_stream import SnapshotCache

    return SnapshotCache(build=builder, clock=clock, **kw)


async def test_the_first_request_builds_and_later_ones_are_served_at_once() -> None:
    clock = _Clock()
    built: list[int] = []

    async def _build(pool: Any) -> bytes:
        built.append(1)
        return b"snapshot-1"

    cache = _cache(_build, clock)
    first, age1 = await cache.get(object())
    clock.now += 5
    second, age2 = await cache.get(object())

    assert (first, second) == (b"snapshot-1", b"snapshot-1")
    assert age1 == 0.0 and age2 == 5.0
    assert built == [1]  # the second request never touched the builder


async def test_concurrent_cold_requests_share_one_build() -> None:
    import asyncio

    clock = _Clock()
    gate = asyncio.Event()
    built: list[int] = []

    async def _build(pool: Any) -> bytes:
        built.append(1)
        await gate.wait()
        return b"shared"

    cache = _cache(_build, clock)
    waiting = [asyncio.create_task(cache.get(object())) for _ in range(5)]
    await asyncio.sleep(0)
    gate.set()
    results = await asyncio.gather(*waiting)

    assert [r[0] for r in results] == [b"shared"] * 5
    assert built == [1]


async def test_an_old_snapshot_is_served_at_once_while_one_rebuild_runs() -> None:
    import asyncio

    clock = _Clock()
    gate = asyncio.Event()
    versions = iter([b"v1", b"v2"])

    async def _build(pool: Any) -> bytes:
        data = next(versions)
        if data == b"v2":
            await gate.wait()  # the rebuild is slow, as on the live graph
        return data

    cache = _cache(_build, clock, fresh_secs=60.0)
    await cache.get(object())
    clock.now += 120  # stale, but inside the bound

    data, age = await cache.get(object())  # must not wait for v2
    again, _ = await cache.get(object())

    assert data == b"v1" and age == 120.0 and again == b"v1"
    gate.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    fresh, fresh_age = await cache.get(object())
    assert fresh == b"v2" and fresh_age == 0.0
    assert cache.builds == 2  # the two stale requests shared ONE rebuild


async def test_a_failed_rebuild_keeps_serving_the_previous_snapshot() -> None:
    clock = _Clock()
    calls: list[int] = []

    async def _build(pool: Any) -> bytes:
        calls.append(1)
        if len(calls) > 1:
            raise RuntimeError("database went away")
        return b"v1"

    cache = _cache(_build, clock, fresh_secs=60.0)
    await cache.get(object())
    clock.now += 120

    data, _ = await cache.get(object())  # triggers the failing background rebuild
    import asyncio
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    data_after, _ = await cache.get(object())

    assert data == b"v1" and data_after == b"v1"


async def test_past_the_staleness_bound_the_request_waits_for_a_real_rebuild() -> None:
    clock = _Clock()
    versions = iter([b"v1", b"v2"])

    async def _build(pool: Any) -> bytes:
        return next(versions)

    cache = _cache(_build, clock, max_stale_secs=600.0)
    await cache.get(object())
    clock.now += 601  # too old to hand to a client: its delta tail would be huge

    data, age = await cache.get(object())

    assert data == b"v2" and age == 0.0


async def test_a_cold_failure_reaches_the_caller_and_the_next_request_retries() -> None:
    clock = _Clock()
    outcomes = iter([RuntimeError("down"), b"ok"])

    async def _build(pool: Any) -> bytes:
        out = next(outcomes)
        if isinstance(out, Exception):
            raise out
        return out

    cache = _cache(_build, clock)
    with pytest.raises(RuntimeError, match="down"):
        await cache.get(object())

    data, _ = await cache.get(object())
    assert data == b"ok"


async def test_the_refresher_warms_the_cache_and_stops_when_told() -> None:
    import asyncio

    clock = _Clock()
    built: list[int] = []

    async def _build(pool: Any) -> bytes:
        built.append(1)
        return b"warm"

    cache = _cache(_build, clock, refresh_secs=0.01)
    stop = asyncio.Event()
    task = asyncio.create_task(cache.run_refresher(object(), stop))
    await asyncio.sleep(0.1)
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    assert len(built) >= 2  # warmed, then refreshed on its own
    data, _ = await cache.get(object())
    assert data == b"warm"


async def test_a_failing_refresher_round_never_kills_the_loop() -> None:
    import asyncio

    clock = _Clock()
    attempts: list[int] = []

    async def _build(pool: Any) -> bytes:
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("first round fails")
        return b"recovered"

    cache = _cache(_build, clock, refresh_secs=0.01)
    stop = asyncio.Event()
    task = asyncio.create_task(cache.run_refresher(object(), stop))
    await asyncio.sleep(0.1)
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    assert len(attempts) >= 2
    assert (await cache.get(object()))[0] == b"recovered"


async def test_the_route_serves_the_cached_snapshot_and_reports_its_age(
    client: httpx.AsyncClient, actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The route answers from the cache: a second request does not rebuild, the body keeps the
    frozen wire format (it decodes), and the response says how old the snapshot is."""
    from src.orchestrator import graph_stream

    real = graph_stream.fetch_snapshot
    built: list[int] = []

    async def _counted(pool: Any) -> bytes:
        built.append(1)
        return await real(pool)

    # the app makes its cache on first use, after this patch, so it builds through the counter
    monkeypatch.setattr(graph_stream, "fetch_snapshot", _counted)

    first = await client.get("/graph/stream")
    second = await client.get("/graph/stream")

    assert first.status_code == second.status_code == 200
    assert len(built) == 1
    assert float(second.headers["x-graph-snapshot-age"]) >= 0.0
    assert decode_snapshot(second.content)["watermark"] is not None


async def test_a_fresh_request_waits_for_a_build_that_started_after_it() -> None:
    import asyncio

    clock = _Clock()
    gate = asyncio.Event()
    built: list[bytes] = []

    async def _build(pool: Any) -> bytes:
        data = f"v{len(built) + 1}".encode()
        built.append(data)
        if data == b"v2":
            await gate.wait()
        return data

    cache = _cache(_build, clock, fresh_secs=60.0)
    await cache.get(object())          # v1 is cached
    clock.now += 120
    stale, _ = await cache.get(object())  # serves v1 and starts the v2 rebuild
    assert stale == b"v1"
    clock.now += 1                      # a change happens now: v2 began BEFORE it
    waiting = asyncio.create_task(cache.get(object(), fresh=True))
    await asyncio.sleep(0)
    gate.set()
    data, age = await waiting

    assert data == b"v3" and age == 0.0  # not v2: that build started before the request
    assert built == [b"v1", b"v2", b"v3"]


async def test_a_fresh_request_with_nothing_running_just_builds_once() -> None:
    clock = _Clock()
    built: list[int] = []

    async def _build(pool: Any) -> bytes:
        built.append(1)
        return b"v"

    cache = _cache(_build, clock)
    await cache.get(object())
    clock.now += 1
    data, _ = await cache.get(object(), fresh=True)

    assert data == b"v" and len(built) == 2


async def test_the_route_honours_fresh_by_rebuilding(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator import graph_stream

    real = graph_stream.fetch_snapshot
    built: list[int] = []

    async def _counted(pool: Any) -> bytes:
        built.append(1)
        return await real(pool)

    monkeypatch.setattr(graph_stream, "fetch_snapshot", _counted)
    await client.get("/graph/stream")
    await client.get("/graph/stream")
    assert len(built) == 1
    await client.get("/graph/stream?fresh=1")
    assert len(built) == 2


def test_the_console_resumes_its_delta_stream_from_the_snapshots_own_watermark() -> None:
    """The snapshot comes from a cache, so connecting to the delta stream at "now" would skip
    whatever changed between the snapshot and the connection."""
    js = (Path(__file__).parent.parent / "src" / "ui" / "static" / "space.js").read_text()
    assert 'new EventSource(\n      "/graph/stream/deltas" + (watermark != null' in js
    assert "watermark: snap.watermark" in js
    # a resync and the empty-graph poll must not be answered from the cache
    assert js.count("fetchStreamSnapshot(true)") == 2


# --- a restart does not start with an empty cache ------------------------------------------


def _tiny_snapshot() -> bytes:
    return encode_snapshot(
        object_ids=["a"], x=[1.0], y=[2.0], type_code=[0], project_code=[0], weight=[1.0],
        status_flag=[0], edge_src=[], edge_dst=[], edge_type_code=[], edge_weight=[],
        types=["Thread"], projects=["repo:x"], edge_types=[], link_type_class=[],
        labels=["Thread a"], created_at=[1.0], watermark=7, community_code=[0], communities=[])


async def test_a_restart_serves_the_previous_snapshot_at_once_while_a_new_one_builds(
    tmp_path: Path,
) -> None:
    from src.orchestrator.graph_stream import SnapshotCache

    path = tmp_path / "snap.bin"
    first_build = _tiny_snapshot()

    async def _first(pool: Any) -> bytes:
        return first_build

    before = SnapshotCache(build=_first, persist_path=path)
    await before.get(None)  # type: ignore[arg-type]
    assert path.read_bytes() == first_build

    gate = asyncio.Event()

    async def _slow(pool: Any) -> bytes:
        await gate.wait()
        return first_build

    after = SnapshotCache(build=_slow, persist_path=path)  # a new process: empty memory
    data, age = await asyncio.wait_for(after.get(None), timeout=5)  # type: ignore[arg-type]
    assert data == first_build
    assert age < 60
    gate.set()


async def test_a_persisted_snapshot_that_is_too_old_truncated_or_foreign_is_not_served(
    tmp_path: Path,
) -> None:
    import os as _os

    from src.orchestrator.graph_stream import SnapshotCache

    good = _tiny_snapshot()
    path = tmp_path / "snap.bin"

    async def _build(pool: Any) -> bytes:
        return b"rebuilt"

    for label, content, mtime_ago in (
        ("too old", good, 3600.0), ("truncated", good[:-4], 0.0),
        ("garbage", b"not a snapshot", 0.0),
    ):
        path.write_bytes(content)
        stamp = time.time() - mtime_ago
        _os.utime(path, (stamp, stamp))
        cache = SnapshotCache(build=_build, persist_path=path)
        assert await cache.restore() is False, label


def test_the_apps_cache_persists_to_the_configured_file() -> None:
    from src.orchestrator.graph_stream import snapshot_file_path

    assert snapshot_file_path().name == "graph_snapshot.bin"

"""Browsing a past moment: fixed read-only statements over a real database."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from src.actions.core import Actions
from src.orchestrator import time_machine_browse as tb

_NOW = datetime.now(UTC)


async def _obj(actions: Actions, typ: str, canonical: str, **props: str) -> object:
    oid = await actions.create_or_find_object(typ, canonical, "test")
    for name, value in props.items():
        await actions.assert_property(oid, name, value, "test", _NOW, 0.8,
                                      evidence_class="direct_observation")
    return oid


@pytest.mark.asyncio
async def test_list_filters_by_type_and_pages(actions: Actions) -> None:
    for i in range(5):
        await _obj(actions, "Thread", f"tm-list-t{i}", summary=f"thread number {i}")
    await _obj(actions, "Decision", "tm-list-d0", summary="a decision")
    got = await tb.list_objects(actions.pool, type="Thread", limit=2, offset=1)
    assert got["total"] >= 5 and len(got["rows"]) == 2 and got["offset"] == 1
    assert all(r["type"] == "Thread" for r in got["rows"])
    assert all(r["title"] and len(r["short_id"]) == 8 for r in got["rows"])
    counts = {c["type"]: c["count"] for c in await tb.type_counts(actions.pool)}
    assert counts["Thread"] >= 5 and counts["Decision"] >= 1


@pytest.mark.asyncio
async def test_page_size_is_clamped(actions: Actions) -> None:
    got = await tb.list_objects(actions.pool, limit=10**6, offset=-5)
    assert got["limit"] == tb.PAGE_MAX and got["offset"] == 0


@pytest.mark.asyncio
async def test_title_prefers_summary_and_follows_the_current_value(actions: Actions) -> None:
    oid = await _obj(actions, "Thread", "tm-title", name="the name")
    await actions.assert_property(oid, "summary", "first summary", "test", _NOW, 0.8,  # type: ignore[arg-type]
                                  evidence_class="direct_observation")
    await actions.assert_property(oid, "summary", "second summary", "test",  # type: ignore[arg-type]
                                  _NOW + timedelta(seconds=5), 0.8,
                                  evidence_class="direct_observation")
    d = await tb.object_detail(actions.pool, "tm-title")
    assert d is not None
    row = (await tb.search_objects(actions.pool, "tm-title"))["rows"][0]
    assert row["title"] == "second summary"
    summaries = [a for a in d["assertions"] if a["name"] == "summary"]
    assert [a["current"] for a in summaries] == [False, True]      # the chain is kept
    assert summaries[1]["supersedes"] == summaries[0]["id"]


@pytest.mark.asyncio
async def test_search_is_literal_not_a_pattern(actions: Actions) -> None:
    await _obj(actions, "Thread", "tm-search-a", summary="100% sure about it")
    await _obj(actions, "Thread", "tm-search-b", summary="1000 sure about it")
    hit = await tb.search_objects(actions.pool, "100% sure")
    assert [r["canonical"] for r in hit["rows"]] == ["tm-search-a"]
    assert hit["rows"][0]["matched"] == "summary"
    under = await tb.search_objects(actions.pool, "tm-search_a")       # _ is not a wildcard
    assert under["rows"] == []
    assert (await tb.search_objects(actions.pool, "   "))["rows"] == []
    typed = await tb.search_objects(actions.pool, "sure about", type="Decision")
    assert typed["rows"] == []


@pytest.mark.asyncio
async def test_a_hostile_query_is_only_ever_data(actions: Actions) -> None:
    await _obj(actions, "Thread", "tm-safe", summary="fine")
    for bad in ("'; DROP TABLE objects; --", "\\", "%", "_"):
        await tb.search_objects(actions.pool, bad)
        await tb.object_detail(actions.pool, bad)
    assert (await tb.object_detail(actions.pool, "tm-safe")) is not None


@pytest.mark.asyncio
async def test_detail_by_uuid_prefix_and_canonical_with_links_both_ways(
        actions: Actions) -> None:
    a = await _obj(actions, "Thread", "tm-det-a", summary="alpha")
    b = await _obj(actions, "Decision", "tm-det-b", summary="beta")
    await actions.create_link(a, b, "answers", "test", _NOW, 0.8,  # type: ignore[arg-type]
                              evidence_class="direct_observation")
    by_canon = await tb.object_detail(actions.pool, "tm-det-a")
    by_uuid = await tb.object_detail(actions.pool, str(a))
    by_prefix = await tb.object_detail(actions.pool, str(a)[:8])
    assert by_canon and by_uuid and by_prefix
    assert by_canon["object"]["id"] == by_uuid["object"]["id"] == by_prefix["object"]["id"]
    assert [lk["other"]["canonical"] for lk in by_canon["links_out"]] == ["tm-det-b"]
    into_b = await tb.object_detail(actions.pool, "tm-det-b")
    assert into_b and [lk["other"]["canonical"] for lk in into_b["links_in"]] == ["tm-det-a"]
    assert into_b["links_in"][0]["active"] is True


@pytest.mark.asyncio
async def test_unknown_ref_and_ambiguous_prefix(actions: Actions) -> None:
    assert await tb.object_detail(actions.pool, "no-such-thing") is None
    assert await tb.object_detail(actions.pool, "") is None
    got = await tb.resolve_ref(actions.pool, "0" * 6)      # a prefix nothing starts with
    assert got == []


@pytest.mark.asyncio
async def test_a_long_value_is_capped(actions: Actions) -> None:
    await _obj(actions, "Decision", "tm-long", rationale="x" * (tb.VALUE_TEXT_MAX + 500))
    d = await tb.object_detail(actions.pool, "tm-long")
    assert d is not None
    v = next(a["value"] for a in d["assertions"] if a["name"] == "rationale")
    assert len(v) < tb.VALUE_TEXT_MAX + 80 and v.endswith("more characters]")


@pytest.mark.asyncio
async def test_transcript_sessions_search_and_unknown_session(actions: Actions) -> None:
    await actions.pool.execute(
        "INSERT INTO soul_sessions (harness, anchor_sid, source_path, last_line_idx) "
        "VALUES ('claude-code', 'tm-sess-1', '/x/tm-sess-1.jsonl', 7)")
    got = await tb.transcript_sessions(actions.pool, query="tm-sess")
    assert [r["anchor_sid"] for r in got["rows"]] == ["tm-sess-1"] and got["rows"][0]["lines"] == 7
    assert (await tb.transcript_sessions(actions.pool, query="tm_sess"))["rows"] == []
    assert await tb.transcript_lines(actions.pool, "tm-never-stored") is None

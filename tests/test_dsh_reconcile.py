"""dsh00001 identity reconciliation (Thoth DM 5442 leg 2, ruling eb642d37, build ruling
DM 5461) — a synthetic 3-stint DSH session (unstamped → model-a → model-b, writes
attributed only inside the model-a stint) exercises the whole shape: mint the lineage,
place `wrote_as` on exactly the generation whose stint owns the writes, leave the 185
(here: 3) original rows untouched, and idempotency on a second run.
"""
from __future__ import annotations

import json
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.orchestrator.dsh_reconcile import measure, reconcile

pytestmark = pytest.mark.skipif(shutil.which("zstd") is None, reason="zstd CLI not on PATH")


def _session_line(**kw: object) -> str:
    return json.dumps(kw)


def _write_dsh_fixture(root: Path, anchor_sid: str, lines: list[str]) -> None:
    """Lay down a real zstd-compressed session file at the actual on-disk shape
    (`<root>/<slug>/session-<anchor_sid>-.../session.jsonl.zstd`) — the SAME nested
    shape `_find_dsh_session_file` walks, so this test exercises the real path, not a
    monkeypatched shortcut."""
    session_dir = root / "test-slug" / f"session-{anchor_sid}-0000-0000-000000000000"
    session_dir.mkdir(parents=True)
    raw = "\n".join(lines).encode()
    subprocess.run(["zstd", "-q", "-o", str(session_dir / "session.jsonl.zstd")],
                   input=raw, check=True)


def _epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _turn(role: str, model: str | None, ts: datetime) -> list[str]:
    out = []
    if model is not None:
        out.append(_session_line(type="request/header",
                                  data={"header": {"config": {"model": model}}}))
    out.append(_session_line(type=f"{role}/message",
                             data={"role": role, "message": {"content": "hi"},
                                   "time": _epoch_ms(ts)}))
    return out


def _build_fixture_lines() -> list[str]:
    """3 stints: unstamped (1 turn), model-a (2 turns), model-b (1 turn)."""
    lines = [_session_line(type="session", id="fixture-session-id", cwd="/x")]
    # unstamped: no request/header before this turn
    lines += _turn("user", None, datetime(2026, 8, 21, 0, 0, 0, tzinfo=UTC))
    # model-a stint (2 turns)
    lines += _turn("assistant", "model-a", datetime(2026, 8, 21, 0, 1, 0, tzinfo=UTC))
    lines += _turn("assistant", "model-a", datetime(2026, 8, 21, 0, 2, 0, tzinfo=UTC))
    # model-b stint (1 turn)
    lines += _turn("assistant", "model-b", datetime(2026, 8, 21, 0, 3, 0, tzinfo=UTC))
    return lines


async def test_measure_buckets_writes_into_the_owning_stint(
    actions: Actions, tmp_path: Path,
) -> None:
    _write_dsh_fixture(tmp_path, "fixtureA", _build_fixture_lines())
    root_agent_id = "agent:fixtureA-root"
    now_in_model_a = datetime(2026, 8, 21, 0, 1, 30, tzinfo=UTC)
    obj = await actions.create_or_find_object("Thread", "thread:dshtest-fixtureA", "test")
    await actions.assert_property(obj, "note", "attributed write", root_agent_id,
                                  now_in_model_a, 0.9, evidence_class="direct_observation")

    out = await measure(actions.pool, anchor_sid="fixtureA", root_agent_id=root_agent_id,
                        session_root=tmp_path)
    assert out["total_writes"] == 1
    assert out["unattributed"] == []
    by_model = {s["model"]: s["writes"] for s in out["stints"]}
    assert by_model["unstamped"] == 0
    assert by_model["model-a"] == 1
    assert by_model["model-b"] == 0


async def test_measure_reports_unattributed_when_observed_at_is_outside_every_stint(
    actions: Actions, tmp_path: Path,
) -> None:
    _write_dsh_fixture(tmp_path, "fixtureB", _build_fixture_lines())
    root_agent_id = "agent:fixtureB-root"
    way_outside = datetime(2020, 1, 1, tzinfo=UTC)
    obj = await actions.create_or_find_object("Thread", "thread:dshtest-fixtureB", "test")
    await actions.assert_property(obj, "note", "stray write", root_agent_id, way_outside,
                                  0.9, evidence_class="direct_observation")

    out = await measure(actions.pool, anchor_sid="fixtureB", root_agent_id=root_agent_id,
                        session_root=tmp_path)
    assert len(out["unattributed"]) == 1
    assert sum(s["writes"] for s in out["stints"]) == 0


async def test_measure_errors_cleanly_when_no_session_file_exists(
    actions: Actions, tmp_path: Path,
) -> None:
    out = await measure(actions.pool, anchor_sid="nonexistent", root_agent_id="agent:x",
                        session_root=tmp_path)
    assert "error" in out


async def test_reconcile_dry_run_writes_nothing(actions: Actions, tmp_path: Path) -> None:
    _write_dsh_fixture(tmp_path, "fixtureC", _build_fixture_lines())
    root_agent_id = "agent:fixtureC-root"
    out = await reconcile(actions, anchor_sid="fixtureC", root_agent_id=root_agent_id,
                          dry_run=True, session_root=tmp_path)
    assert out["dry_run"] is True
    assert len(out["plan"]) == 3
    assert [p["generation"] for p in out["plan"]] == [
        root_agent_id, f"{root_agent_id}-ii", f"{root_agent_id}-iii"]
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE canonical LIKE $1", f"{root_agent_id}%")
    assert n == 0, "dry_run must never write"


async def test_reconcile_execute_requires_a_because(actions: Actions, tmp_path: Path) -> None:
    _write_dsh_fixture(tmp_path, "fixtureD", _build_fixture_lines())
    out = await reconcile(actions, anchor_sid="fixtureD", root_agent_id="agent:fixtureD-root",
                          dry_run=False, session_root=tmp_path)
    assert "error" in out


async def test_reconcile_mints_the_lineage_and_places_wrote_as_on_the_owning_generation(
    actions: Actions, tmp_path: Path,
) -> None:
    _write_dsh_fixture(tmp_path, "fixtureE", _build_fixture_lines())
    root_agent_id = "agent:fixtureE-root"
    now_in_model_a = datetime(2026, 8, 21, 0, 1, 30, tzinfo=UTC)
    obj = await actions.create_or_find_object("Thread", "thread:dshtest-fixtureE", "test")
    await actions.assert_property(obj, "note", "attributed write", root_agent_id,
                                  now_in_model_a, 0.9, evidence_class="direct_observation")

    out = await reconcile(actions, anchor_sid="fixtureE", root_agent_id=root_agent_id,
                          dry_run=False, because="test build", session_root=tmp_path)
    assert out["dry_run"] is False

    rows = await actions.pool.fetch(
        "SELECT canonical FROM objects WHERE canonical LIKE $1 AND type='Agent' "
        "ORDER BY canonical", f"{root_agent_id}%")
    assert {r["canonical"] for r in rows} == {
        root_agent_id, f"{root_agent_id}-ii", f"{root_agent_id}-iii"}

    # succession chain: -ii succeeded_from root, -iii succeeded_from -ii
    gen2 = await actions.pool.fetchval(
        "SELECT o.id FROM objects o WHERE o.canonical=$1", f"{root_agent_id}-ii")
    succ = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='succeeded_from'", gen2)
    assert succ == root_agent_id

    # wrote_as lands ONLY on gen2 (model-a), never on root or gen3 (model-b)
    for canon, expect in ((root_agent_id, False), (f"{root_agent_id}-ii", True),
                          (f"{root_agent_id}-iii", False)):
        oid = await actions.pool.fetchval(
            "SELECT id FROM objects WHERE canonical=$1", canon)
        wa = await actions.pool.fetchval(
            "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
            "AND name='wrote_as'", oid)
        assert (wa == root_agent_id) == expect, f"{canon}: wrote_as={wa!r}"

    # the original attributed row is UNTOUCHED — still stamped from the raw root string
    original = await actions.pool.fetchval(
        "SELECT source_id FROM assertions WHERE object_id=$1 AND name='note'", obj)
    assert original == root_agent_id


async def test_reconcile_is_idempotent(actions: Actions, tmp_path: Path) -> None:
    _write_dsh_fixture(tmp_path, "fixtureF", _build_fixture_lines())
    root_agent_id = "agent:fixtureF-root"
    first = await reconcile(actions, anchor_sid="fixtureF", root_agent_id=root_agent_id,
                            dry_run=False, because="test build", session_root=tmp_path)
    second = await reconcile(actions, anchor_sid="fixtureF", root_agent_id=root_agent_id,
                             dry_run=False, because="re-run", session_root=tmp_path)
    assert [p["action"] for p in second["plan"]] == ["already minted — skip"] * 3
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE canonical LIKE $1 AND type='Agent'",
        f"{root_agent_id}%")
    assert n == 3, "the second run must not mint duplicates"
    assert first["plan"][0]["generation"] == second["plan"][0]["generation"]


async def test_reconcile_never_touches_the_root_agent_id_itself_as_retired_or_folded(
    actions: Actions, tmp_path: Path,
) -> None:
    """Thoth's hard rule: dsh00001 is never retired, never folded into another
    lineage — this verb only ever ADDS its own lineage."""
    _write_dsh_fixture(tmp_path, "fixtureG", _build_fixture_lines())
    root_agent_id = "agent:fixtureG-root"
    await reconcile(actions, anchor_sid="fixtureG", root_agent_id=root_agent_id,
                    dry_run=False, because="test build", session_root=tmp_path)
    oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", root_agent_id)
    retired = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='retired'", oid)
    assert retired is None
    merged_into = await actions.pool.fetchval(
        "SELECT merged_into FROM objects WHERE id=$1", oid)
    assert merged_into is None

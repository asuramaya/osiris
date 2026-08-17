"""The soul store — verbatim ingest + hash chain (task #51 piece 1, ruling 62dc6397).

Proves the round trip against real Postgres (testcontainers): every line stored exactly
as written, the hash chain detects a gap or a tamper, incremental ingest resumes the
chain correctly, and re_materialize() reconstructs the source byte-for-byte — the
acceptance test this piece stands or falls on.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest_asyncio
from src.actions.core import Actions
from src.ingest.soul_store import SoulStore, _chain_hash


def _write_transcript(path: Path, lines: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    return path


def _synthetic_lines(n: int) -> list[str]:
    out = []
    for i in range(n):
        ts = datetime(2026, 8, 17, 12, 0, i, tzinfo=UTC).isoformat()
        role = "assistant" if i % 2 else "user"
        out.append(json.dumps({"type": role, "timestamp": ts, "message": {"content": f"line {i}"}}))
    return out


@pytest_asyncio.fixture
async def store(actions: Actions) -> SoulStore:
    return SoulStore(actions.pool)


# --- pure hash chain -----------------------------------------------------

def test_chain_hash_is_deterministic_and_order_sensitive() -> None:
    a = _chain_hash(None, "line0")
    b = _chain_hash(a, "line1")
    assert a != b
    assert _chain_hash(None, "line0") == a  # deterministic
    assert _chain_hash("line1", "line0") != b  # prev_hash matters, not just content


# --- ingest + read back ---------------------------------------------------

async def test_ingest_stores_every_line_verbatim(store: SoulStore, tmp_path: Path) -> None:
    lines = _synthetic_lines(5)
    p = _write_transcript(tmp_path / "t.jsonl", lines)
    n = await store.ingest_path(str(p), "deadbeef")
    assert n == 5
    rows = await store.pool.fetch(
        "SELECT line_idx, raw_line FROM soul_lines WHERE harness='claude-code' "
        "AND anchor_sid='deadbeef' ORDER BY line_idx")
    assert [r["raw_line"] for r in rows] == lines
    assert [r["line_idx"] for r in rows] == list(range(5))


async def test_ingest_is_idempotent(store: SoulStore, tmp_path: Path) -> None:
    lines = _synthetic_lines(3)
    p = _write_transcript(tmp_path / "t.jsonl", lines)
    first = await store.ingest_path(str(p), "cafef00d")
    second = await store.ingest_path(str(p), "cafef00d")
    assert first == 3
    assert second == 0
    rows = await store.pool.fetch(
        "SELECT count(*) AS n FROM soul_lines WHERE harness='claude-code' "
        "AND anchor_sid='cafef00d'")
    assert rows[0]["n"] == 3


async def test_ingest_resumes_the_chain_incrementally(store: SoulStore, tmp_path: Path) -> None:
    """Appending to the source and re-ingesting continues the chain from last_hash,
    never re-hashing already-stored lines."""
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(2))
    await store.ingest_path(str(p), "feedface")
    fuller = _synthetic_lines(2) + _synthetic_lines(4)[2:]
    p.write_text("\n".join(fuller) + "\n")
    added = await store.ingest_path(str(p), "feedface")
    assert added == 2
    assert await store.verify_chain("feedface") is True
    materialized = await store.re_materialize("feedface")
    assert materialized == "\n".join(fuller)


# --- integrity: verify_chain ----------------------------------------------

async def test_verify_chain_true_on_a_clean_ingest(store: SoulStore, tmp_path: Path) -> None:
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(6))
    await store.ingest_path(str(p), "0ff1ce00")
    assert await store.verify_chain("0ff1ce00") is True


async def test_verify_chain_false_on_a_tampered_line(store: SoulStore, tmp_path: Path) -> None:
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(4))
    await store.ingest_path(str(p), "b16b00b5")
    await store.pool.execute(
        "UPDATE soul_lines SET raw_line='TAMPERED' "
        "WHERE harness='claude-code' AND anchor_sid='b16b00b5' AND line_idx=1")
    assert await store.verify_chain("b16b00b5") is False


async def test_verify_chain_false_on_a_gap(store: SoulStore, tmp_path: Path) -> None:
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(4))
    await store.ingest_path(str(p), "5ca1ab1e")
    await store.pool.execute(
        "DELETE FROM soul_lines WHERE harness='claude-code' AND anchor_sid='5ca1ab1e' "
        "AND line_idx=2")
    assert await store.verify_chain("5ca1ab1e") is False


async def test_verify_chain_true_on_zero_rows(store: SoulStore) -> None:
    assert await store.verify_chain("never-ingested") is True


# --- re_materialize: the acceptance test -----------------------------------

async def test_re_materialize_byte_compares_against_the_source(
    store: SoulStore, tmp_path: Path,
) -> None:
    lines = _synthetic_lines(9)
    p = _write_transcript(tmp_path / "t.jsonl", lines)
    await store.ingest_path(str(p), "decafbad")
    materialized = await store.re_materialize("decafbad")
    assert materialized is not None
    # source carries a trailing newline the join form doesn't reproduce — the documented
    # "mod a trailing newline" equivalence
    assert p.read_text() == materialized + "\n"
    assert materialized == "\n".join(lines)


async def test_re_materialize_none_when_never_ingested(store: SoulStore) -> None:
    assert await store.re_materialize("ghost0000") is None


# --- ingest() via the harness adapter door ----------------------------------

async def test_ingest_via_discover_finds_and_eats_the_real_locator(
    store: SoulStore, tmp_path: Path,
) -> None:
    sid = "aabbccdd"
    projects = tmp_path / "projects"
    proj_dir = projects / "-home-x-code-widget"
    lines = _synthetic_lines(3)
    _write_transcript(proj_dir / f"{sid}-session-uuid.jsonl", lines)
    n = await store.ingest(
        cwd="/home/x/code/widget", job_dir=f"/home/x/.claude/jobs/{sid}", root=projects)
    assert n == 3
    materialized = await store.re_materialize(sid)
    assert materialized == "\n".join(lines)


async def test_ingest_via_discover_returns_zero_when_nothing_found(
    store: SoulStore, tmp_path: Path,
) -> None:
    n = await store.ingest(cwd="/nowhere", job_dir="/x/jobs/deadbeef", root=tmp_path)
    assert n == 0

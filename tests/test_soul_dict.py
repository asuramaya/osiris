"""The trained-dictionary form (ZS2) of a stored transcript line: the format, the registry of
loaded dictionaries, training, every reader, the re-encode from the older forms, key rotation and
the loud failure when a dictionary is missing. Real Postgres through the `actions` fixture."""
from __future__ import annotations

import json
import os
import random
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.fernet import Fernet, InvalidToken, MultiFernet
from src.actions.core import Actions
from src.ingest import soul_crypto, soul_dicts
from src.ingest.soul_crypto import (
    CODEC_PLAIN,
    CODEC_UNSET,
    CODEC_ZS1,
    CODEC_ZS2,
    LINE_ENVELOPE_ZS1,
    LINE_ENVELOPE_ZS2,
    DictionaryMissing,
    get_soul_fernet,
    open_line,
    pack_line,
    unpack_line,
)
from src.ingest.soul_store import SoulStore, rewrap_soul_lines_key
from src.orchestrator import soul_recompress as rc

_WORDS = ("alpha beta gamma delta epsilon zeta theta kappa lambda sigma omega tool result "
          "assistant message content text build error file path commit branch merge review").split()


def _line(i: int, size: int = 1200) -> str:
    rnd = random.Random(i)
    ts = datetime(2026, 9, 1, 12, 0, i % 60, tzinfo=UTC).isoformat()
    body = " ".join(rnd.choice(_WORDS) for _ in range(size // 6))
    return json.dumps({"type": "assistant", "timestamp": ts, "uuid": f"u-{i:08d}",
                       "parentUuid": f"u-{max(i - 1, 0):08d}", "sessionId": "s-1",
                       "message": {"role": "assistant", "model": "claude", "content": body}})


def _trained(n: int = 400) -> bytes:
    import zstandard

    samples = [_line(i).encode() for i in range(n)]
    return zstandard.train_dictionary(16384, samples).as_bytes()


@pytest.fixture
def dictionary() -> int:
    soul_crypto.register_dictionary(7, _trained(), active=True)
    return 7


class _Clock:
    def __init__(self, step: float = 0.001) -> None:
        self.now, self.step = 0.0, step

    def __call__(self) -> float:
        self.now += self.step
        return self.now


async def _no_sleep(_s: float) -> None:
    return None


# --- the format, no database --------------------------------------------------------------

def test_a_line_written_with_a_dictionary_is_smaller_than_plain_zstd(dictionary: int) -> None:
    raw = _line(5000).encode()
    zs2, codec = pack_line(raw)
    assert codec == CODEC_ZS2 and zs2.startswith(LINE_ENVELOPE_ZS2)
    soul_crypto.clear_dictionaries()
    zs1, codec1 = pack_line(raw)
    assert codec1 == CODEC_ZS1 and len(zs2) < len(zs1)


def test_all_three_forms_open_through_the_same_call(dictionary: int) -> None:
    f = Fernet(Fernet.generate_key())
    raw = _line(9).encode()
    short = b'{"a":1}'
    zs2, _ = pack_line(raw)
    old = f.encrypt(raw)                                    # plain line, sealed the old way
    zs1 = f.encrypt(LINE_ENVELOPE_ZS1 + __import__("zstandard").ZstdCompressor().compress(raw))
    new = f.encrypt(zs2)
    assert open_line(f, old) == open_line(f, zs1) == open_line(f, new) == raw
    assert pack_line(short) == (short, CODEC_PLAIN)


def test_an_unloaded_dictionary_fails_loudly_never_with_garbage(dictionary: int) -> None:
    packed, _ = pack_line(_line(3).encode())
    soul_crypto.clear_dictionaries()
    with pytest.raises(DictionaryMissing):
        unpack_line(packed)


def test_the_dictionary_id_is_part_of_the_envelope(dictionary: int) -> None:
    packed, _ = pack_line(_line(4).encode())
    assert int.from_bytes(packed[4:8], "big") == dictionary


def test_a_line_that_does_not_shrink_stays_plain_even_with_a_dictionary(dictionary: int) -> None:
    raw = os.urandom(3000)
    assert pack_line(raw) == (raw, CODEC_PLAIN)


# --- database ------------------------------------------------------------------------------

async def _ingest(
    actions: Actions, tmp_path: Path, anchor: str, n: int, start: int = 0,
) -> list[str]:
    lines = [_line(start + i) for i in range(n)]
    p = tmp_path / f"{anchor}.jsonl"
    p.write_text("\n".join(lines) + "\n")
    await SoulStore(actions.pool).ingest_path(str(p), anchor)
    return lines


async def _codecs(actions: Actions, anchor: str) -> list[int]:
    got = await actions.pool.fetch(
        "SELECT codec FROM soul_lines WHERE anchor_sid=$1 ORDER BY line_idx", anchor)
    return [int(r["codec"]) for r in got]


async def _make_legacy(actions: Actions, anchor: str) -> None:
    """Rewrite every row of `anchor` the way it was stored before compression: the plain line
    sealed, marker 0."""
    f = get_soul_fernet()
    for r in await actions.pool.fetch(
            "SELECT line_idx, raw_line FROM soul_lines WHERE anchor_sid=$1", anchor):
        await actions.pool.execute(
            "UPDATE soul_lines SET raw_line=$1, codec=0 WHERE anchor_sid=$2 AND line_idx=$3",
            f.encrypt(open_line(f, bytes(r["raw_line"]))), anchor, r["line_idx"])


async def _train(actions: Actions) -> dict[str, object]:
    return await soul_dicts.train_dictionary(
        actions.pool, get_soul_fernet(), sample_lines=300, dict_bytes=16384, min_lines=50)


async def test_a_young_store_with_too_few_lines_trains_nothing(
    actions: Actions, tmp_path: Path,
) -> None:
    await _ingest(actions, tmp_path, "dict0001", 10)
    out = await soul_dicts.train_dictionary(actions.pool, get_soul_fernet())
    assert out["trained"] is False and "need" in out["reason"]
    assert await actions.pool.fetchval("SELECT count(*) FROM soul_dicts") == 0


async def test_training_stores_the_dictionary_sealed_and_makes_it_active(
    actions: Actions, tmp_path: Path,
) -> None:
    await _ingest(actions, tmp_path, "dict0002", 200)
    out = await _train(actions)
    assert out["trained"] is True
    row = await actions.pool.fetchrow("SELECT sealed_dict, active, trained_rows FROM soul_dicts")
    assert bytes(row["sealed_dict"]).startswith(b"gAAAA")      # sealed, never raw
    assert row["active"] is True and row["trained_rows"] >= 50
    assert soul_crypto.active_dictionary_id() == out["id"]
    # the sealed bytes open to the dictionary itself
    assert get_soul_fernet().decrypt(bytes(row["sealed_dict"]))[:4] == b"7\xa40\xec"


async def test_new_rows_are_written_with_the_dictionary_and_read_back_identical(
    actions: Actions, tmp_path: Path,
) -> None:
    await _ingest(actions, tmp_path, "dict0003", 200)
    await _train(actions)
    lines = await _ingest(actions, tmp_path, "dict0004", 6, start=1000)
    assert await _codecs(actions, "dict0004") == [CODEC_ZS2] * 6
    store = SoulStore(actions.pool)
    assert await store.raw_lines("dict0004") == lines
    assert await store.verify_chain("dict0004") is True


async def test_a_fresh_process_loads_the_dictionary_by_itself_before_reading(
    actions: Actions, tmp_path: Path,
) -> None:
    await _ingest(actions, tmp_path, "dict0005", 200)
    await _train(actions)
    lines = await _ingest(actions, tmp_path, "dict0006", 4, start=2000)
    soul_crypto.clear_dictionaries()                   # a different process that never trained
    assert await SoulStore(actions.pool).raw_lines("dict0006") == lines
    assert soul_crypto.active_dictionary_id() is not None


async def test_rows_in_every_older_form_stay_readable_beside_the_new_one(
    actions: Actions, tmp_path: Path,
) -> None:
    zs1_lines = await _ingest(actions, tmp_path, "dict0007", 200)     # ZS1 (no dictionary yet)
    await _train(actions)
    zs2_lines = await _ingest(actions, tmp_path, "dict0008", 5, start=3000)
    f = get_soul_fernet()
    plain_line = _line(77)
    await _ingest(actions, tmp_path, "dict0009", 1, start=77)
    await actions.pool.execute(
        "UPDATE soul_lines SET raw_line=$1, codec=0 WHERE anchor_sid='dict0009'",
        f.encrypt(plain_line.encode()))
    store = SoulStore(actions.pool)
    assert await store.raw_lines("dict0007") == zs1_lines
    assert await store.raw_lines("dict0008") == zs2_lines
    assert await store.raw_lines("dict0009") == [plain_line]
    assert await _codecs(actions, "dict0007") == [CODEC_ZS1] * 200


async def test_the_reencode_moves_older_rows_to_the_dictionary_form_and_shrinks_them(
    actions: Actions, tmp_path: Path,
) -> None:
    lines = await _ingest(actions, tmp_path, "dict0010", 200)         # ZS1
    before = await actions.pool.fetchval(
        "SELECT sum(octet_length(raw_line)) FROM soul_lines WHERE anchor_sid='dict0010'")
    await _train(actions)
    record = await rc.recompress_tick(
        actions.pool, get_soul_fernet(), clock=_Clock(), sleep=_no_sleep)
    after = await actions.pool.fetchval(
        "SELECT sum(octet_length(raw_line)) FROM soul_lines WHERE anchor_sid='dict0010'")
    assert record["state"] == "complete"
    assert await _codecs(actions, "dict0010") == [CODEC_ZS2] * 200
    assert after < before * 0.8
    store = SoulStore(actions.pool)
    assert await store.raw_lines("dict0010") == lines
    assert await store.verify_chain("dict0010") is True


async def test_the_reencode_leaves_the_dictionary_form_alone_and_never_enlarges_a_row(
    actions: Actions, tmp_path: Path,
) -> None:
    await _ingest(actions, tmp_path, "dict0011", 200)
    await _train(actions)
    await _ingest(actions, tmp_path, "dict0012", 3, start=4000)       # already ZS2
    stored = [bytes(r["raw_line"]) for r in await actions.pool.fetch(
        "SELECT raw_line FROM soul_lines WHERE anchor_sid='dict0012' ORDER BY line_idx")]
    await actions.pool.execute("UPDATE soul_lines SET codec=1 WHERE anchor_sid='dict0012'")
    await rc.recompress_tick(actions.pool, get_soul_fernet(), clock=_Clock(), sleep=_no_sleep)
    assert [bytes(r["raw_line"]) for r in await actions.pool.fetch(
        "SELECT raw_line FROM soul_lines WHERE anchor_sid='dict0012' ORDER BY line_idx")] == stored
    assert await _codecs(actions, "dict0012") == [CODEC_ZS2] * 3       # only the marker moved


async def test_without_a_dictionary_the_reencode_still_stops_at_zs1(
    actions: Actions, tmp_path: Path,
) -> None:
    await _ingest(actions, tmp_path, "dict0013", 4)
    f = get_soul_fernet()
    await _make_legacy(actions, "dict0013")
    await rc.recompress_tick(actions.pool, f, clock=_Clock(), sleep=_no_sleep)
    assert await _codecs(actions, "dict0013") == [CODEC_ZS1] * 4


async def test_a_key_rotation_reseals_the_dictionary_and_it_still_loads(
    actions: Actions, tmp_path: Path,
) -> None:
    lines = await _ingest(actions, tmp_path, "dict0014", 200)
    await _train(actions)
    await _ingest(actions, tmp_path, "dict0015", 3, start=5000)
    old = get_soul_fernet()
    new = MultiFernet([Fernet(Fernet.generate_key())])
    dry = await rewrap_soul_lines_key(actions.pool, old_fernet=old, new_fernet=new, dry_run=True)
    assert dry["dict_rewrapped"] == 1 and dry["dict_broken_count"] == 0
    done = await rewrap_soul_lines_key(actions.pool, old_fernet=old, new_fernet=new, dry_run=False)
    assert done["dict_rewrapped"] == 1
    sealed = bytes(await actions.pool.fetchval("SELECT sealed_dict FROM soul_dicts"))
    new.decrypt(sealed)
    with pytest.raises(InvalidToken):
        old.decrypt(sealed)
    again = await rewrap_soul_lines_key(actions.pool, old_fernet=old, new_fernet=new, dry_run=True)
    assert again["dict_rewrapped"] == 0 and again["dict_already_on_new_key"] == 1
    rows = await actions.pool.fetch(
        "SELECT raw_line FROM soul_lines WHERE anchor_sid='dict0014' ORDER BY line_idx")
    soul_crypto.clear_dictionaries()
    soul_crypto.register_dictionary(
        1, new.decrypt(sealed), active=False)
    assert [open_line(new, bytes(r["raw_line"])).decode() for r in rows[:3]] == lines[:3]


async def test_a_missing_dictionary_row_fails_loudly_on_read(
    actions: Actions, tmp_path: Path,
) -> None:
    await _ingest(actions, tmp_path, "dict0016", 200)
    await _train(actions)
    await _ingest(actions, tmp_path, "dict0017", 3, start=6000)
    await actions.pool.execute("DELETE FROM soul_dicts")
    soul_crypto.clear_dictionaries()
    with pytest.raises(DictionaryMissing):
        await SoulStore(actions.pool).raw_lines("dict0017")


async def test_the_worker_trains_once_then_reencodes_straight_to_the_dictionary_form(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.workers.arq_worker import soul_recompress_heartbeat

    monkeypatch.setattr(soul_dicts, "MIN_SAMPLE_LINES", 50)
    monkeypatch.setattr(soul_dicts, "SAMPLE_LINES", 300)
    monkeypatch.setattr(soul_dicts, "DICT_BYTES", 16384)
    await _ingest(actions, tmp_path, "dict0018", 200)
    await _make_legacy(actions, "dict0018")
    ctx = {"cascade": SimpleNamespace(actions=actions)}
    assert await soul_recompress_heartbeat(ctx) == 200
    assert await _codecs(actions, "dict0018") == [CODEC_ZS2] * 200      # never went through ZS1
    assert await actions.pool.fetchval("SELECT count(*) FROM soul_dicts") == 1
    assert await soul_recompress_heartbeat(ctx) == 0
    assert await actions.pool.fetchval("SELECT count(*) FROM soul_dicts") == 1


async def test_training_is_not_retried_every_tick_and_can_be_switched_off(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _ingest(actions, tmp_path, "dict0019", 10)
    f = get_soul_fernet()
    first = await rc.maybe_train_dictionary(actions.pool, f)
    assert first["trained"] is False and "need" in first["reason"]
    again = await rc.maybe_train_dictionary(actions.pool, f)
    assert again == {"trained": False, "reason": "tried recently"}
    monkeypatch.setenv(soul_dicts.DISABLE_ENV, "1")
    assert (await rc.maybe_train_dictionary(actions.pool, f))["reason"] == "switched off"


def test_the_codec_values_are_stable() -> None:
    assert (CODEC_UNSET, CODEC_ZS1, CODEC_PLAIN, CODEC_ZS2) == (0, 1, 2, 3)

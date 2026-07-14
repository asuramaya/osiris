"""WHAT DID THE GHOST FARM COST? — 818 wakes, and not one of them in the ledger.

Spawning an entire Claude session is the most expensive thing Osiris can do, and it was the one
thing nobody could see. The miner cost $40.49 and I can prove it to the cent. The farm that minted
463 agents on projects the operator had not opened in days? UNKNOWN.

That is the same disease the miner had: a producer whose spend nobody counted, which therefore
could not be falsified, and which therefore rotted. A HAND YOU CANNOT COST IS A HAND YOU CANNOT
GOVERN.
"""

from __future__ import annotations

import json
from pathlib import Path

from src.actions.core import Actions
from src.ingest.wake_cost import meter_wakes, metered_key


def _session(root: Path, sid: str, first: str, *, turns: int = 2) -> Path:
    d = root / "-repo"
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{sid}.jsonl"
    lines = [json.dumps({"type": "user", "message": {"content": first}})]
    for _ in range(turns):
        lines.append(json.dumps({"type": "assistant", "message": {
            "model": "claude-haiku-4-5-20251001",
            "usage": {"input_tokens": 10, "output_tokens": 100,
                      "cache_read_input_tokens": 5000, "cache_creation_input_tokens": 300}}}))
    p.write_text("\n".join(lines) + "\n")
    return p


async def test_a_wake_s_REAL_spend_is_read_off_its_own_transcript(
    actions: Actions, tmp_path: Path,
) -> None:
    """The truth was on the disk the whole time. Claude Code stamps every assistant turn with its
    real tokens and the model that served it. Nobody ever read it."""
    _session(tmp_path, "wake0001", "You have unread Osiris mail. Call mount(...)", turns=3)

    rep = await meter_wakes(actions.pool, tmp_path)
    assert rep["metered"] == 1

    row = await actions.pool.fetchrow(
        "SELECT model, input_tokens, output_tokens, cache_read_tokens, cost_usd "
        "FROM llm_usage WHERE purpose='wake'")
    assert row["model"] == "claude-haiku-4-5-20251001"
    assert row["output_tokens"] == 300 and row["cache_read_tokens"] == 15000
    # THE PRICE IS NOT GUESSED. The transcript gives TOKENS; a price table invented here would be a
    # guess wearing the authority of a measurement, and it would be stale within the month. The
    # tokens are a FACT. An honest gap beats a confident invention — that is the law of this week,
    # and it applies to me too.
    assert row["cost_usd"] is None


async def test_an_ORDINARY_session_is_not_billed_as_a_wake(
    actions: Actions, tmp_path: Path,
) -> None:
    """The fingerprint is the FIRST TURN, never a mention. This project has DISCUSSED the wake
    prompt at length — billing those conversations as wakes would be the instrument miscounting
    itself, which is the loop-pathology class in its cheapest form."""
    _session(tmp_path, "real0001", "why does the wake prompt say 'You have unread Osiris mail'?")
    assert (await meter_wakes(actions.pool, tmp_path))["metered"] == 0
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM llm_usage WHERE purpose='wake'") == 0


async def test_a_wake_is_metered_ONCE_and_never_double_billed(
    actions: Actions, tmp_path: Path,
) -> None:
    """A meter that counts twice is worse than no meter — it manufactures a debt that was never
    owed, which is precisely what the miner did to the operator's wall."""
    p = _session(tmp_path, "wake0002", "You have unread Osiris mail. Call mount(...)")
    assert (await meter_wakes(actions.pool, tmp_path))["metered"] == 1
    assert (await meter_wakes(actions.pool, tmp_path))["metered"] == 0
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM llm_usage WHERE purpose='wake'") == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM watermarks WHERE key=$1", metered_key(p.stem)) == 1


def _receipt(root: Path, stem: str, *, cost: float | None = 0.2559) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    p = root / f"{stem}.json"
    env: dict = {"type": "result", "num_turns": 15,
                 "usage": {"input_tokens": 100, "output_tokens": 13343,
                           "cache_read_input_tokens": 650619,
                           "cache_creation_input_tokens": 62016},
                 "modelUsage": {"claude-haiku-4-5-20251001": {}}}
    if cost is not None:
        env["total_cost_usd"] = cost
    p.write_text(json.dumps(env))
    return p


async def test_a_RESUMED_wake_is_priced_off_its_receipt(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE FIELD RUN THAT FOUND THIS (pokex pile-drain, wake 819, 2026-07-14): a resume-mode
    wake appends to a transcript the once-ever watermark already walked, so the transcript pass
    is structurally blind to it — $0.2559 of real spend sat in a perfect receipt while three
    meter ticks walked past. The wake's unit of account is the EVENT; the file was the wrong
    key. The envelope carries this run's OWN dollars and token deltas, so nothing double-counts
    the transcript's earlier life."""
    from src.ingest.wake_cost import meter_receipts, receipt_key

    receipts = tmp_path / "receipts"
    _receipt(receipts, "f3520001")
    rep = await meter_receipts(actions.pool, receipts=receipts)
    assert rep["receipts_metered"] == 1
    row = await actions.pool.fetchrow(
        "SELECT model, output_tokens, cache_read_tokens, cost_usd FROM llm_usage "
        "WHERE purpose='wake'")
    assert row["model"] == "claude-haiku-4-5-20251001"
    assert row["output_tokens"] == 13343 and row["cache_read_tokens"] == 650619
    assert row["cost_usd"] is not None and abs(float(row["cost_usd"]) - 0.2559) < 1e-6
    # once, ever — the receipt is watermarked
    assert (await meter_receipts(actions.pool, receipts=receipts))["receipts_metered"] == 0
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM watermarks WHERE key=$1", receipt_key("f3520001")) == 1


async def test_an_EMPTY_or_unpriced_receipt_is_left_for_the_next_tick(
    actions: Actions, tmp_path: Path,
) -> None:
    """A 0-byte receipt is a session still running (or a spawn that died before its first
    write — wake-7.json): billing it would invent a number, watermarking it would forget it
    forever. It is left alone, un-watermarked, for the tick after its session exits."""
    from src.ingest.wake_cost import meter_receipts, receipt_key

    receipts = tmp_path / "receipts"
    receipts.mkdir(parents=True)
    (receipts / "wake-run1.json").write_text("")             # still running / died at birth
    _receipt(receipts, "unpriced1", cost=None)               # envelope without a price
    rep = await meter_receipts(actions.pool, receipts=receipts)
    assert rep["receipts_metered"] == 0
    assert await actions.pool.fetchval("SELECT count(*) FROM llm_usage") == 0
    for stem in ("wake-run1", "unpriced1"):
        assert await actions.pool.fetchval(
            "SELECT count(*) FROM watermarks WHERE key=$1", receipt_key(stem)) == 0


async def test_a_receipt_billed_by_the_TRANSCRIPT_pass_is_never_billed_twice(
    actions: Actions, tmp_path: Path, monkeypatch: object,
) -> None:
    """A MINTED wake is a fresh transcript: the transcript pass meters it WITH its receipt's
    price, and must plant the receipt watermark so the receipts pass cannot bill the same
    dollars again. One wake, one row, whichever pass sees it first."""
    import src.orchestrator.trigger as trigger_mod
    from src.ingest.wake_cost import meter_receipts

    receipts = tmp_path / "receipts"
    _receipt(receipts, "wake0003")
    monkeypatch.setattr(trigger_mod, "RECEIPTS", receipts)  # type: ignore[attr-defined]
    _session(tmp_path, "wake0003", "You have unread Osiris mail. Call mount(...)")
    rep = await meter_wakes(actions.pool, tmp_path)
    assert rep["metered"] == 1 and rep["priced"] == 1
    assert (await meter_receipts(actions.pool, receipts=receipts))["receipts_metered"] == 0
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM llm_usage WHERE purpose='wake'") == 1

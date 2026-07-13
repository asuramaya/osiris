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

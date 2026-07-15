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
import os
from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.ingest.wake_cost import meter_bodies, meter_wakes, metered_key


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


# ═══ THE OTHER DIMENSION — body_usage: resource-seconds beside the vendor's dollars ═══


def _body_receipt_file(
    root: Path, handle: str, *, provider: str = "local", exit_cause: str = "exited",
    **overrides: object,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    env: dict[str, object] = {
        "v": 1, "handle": handle, "provider": provider, "kind": "claude-body",
        "core_seconds": 12.5, "wall_seconds": 40.0,
        "ram_envelope_bytes": 2 * 1024**3, "ram_peak_bytes": 1024**3,
        "ram_gib_seconds": 3.2, "exit_cause": exit_cause,
        "started_at": "2026-07-14T10:00:00Z", "ended_at": "2026-07-14T10:00:40Z",
        "seat_anchor": "/home/asuramaya/code/osiris", "repo_ref": "osiris",
        "budget_usd": 0.50,
    }
    env.update(overrides)
    p = root / f"{handle}.json"
    p.write_text(json.dumps(env))
    return p


async def test_a_body_receipt_is_swept_into_body_usage(
    actions: Actions, tmp_path: Path,
) -> None:
    """The RECEIPT v1 envelope parses into a body_usage row carrying BOTH dimensions the
    ceiling now needs — resource-seconds and exit cause, beside the seat/project it ran for."""
    receipts = tmp_path / "body-receipts"
    _body_receipt_file(receipts, "body0001", provider="ra")
    rep = await meter_bodies(actions.pool, receipts=receipts)
    assert rep == {"metered": 1, "skipped": 0}

    row = await actions.pool.fetchrow(
        "SELECT provider, kind, project, seat_anchor, core_seconds, wall_seconds, "
        "ram_envelope_bytes, ram_peak_bytes, ram_gib_seconds, exit_cause "
        "FROM body_usage WHERE handle='body0001'")
    assert row["provider"] == "ra" and row["exit_cause"] == "exited"
    assert row["project"] == "osiris"      # repo_ref lands in `project` (fleet_messages' idiom)
    assert row["seat_anchor"] == "/home/asuramaya/code/osiris"
    assert row["core_seconds"] == 12.5 and row["ram_gib_seconds"] == 3.2
    assert row["ram_peak_bytes"] == 1024**3


async def test_a_body_receipt_is_event_dated_by_its_OWN_mtime(
    actions: Actions, tmp_path: Path,
) -> None:
    """A LEDGER MUST BE DATED BY THE EVENT, NEVER BY THE BOOKKEEPING — the same law the wake
    receipts prove above. A receipt swept long after the body dissolved must file under the
    day the BODY RAN, never the day the sweep happened to notice it."""
    receipts = tmp_path / "body-receipts"
    p = _body_receipt_file(receipts, "body0002")
    old = datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC).timestamp()
    os.utime(p, (old, old))

    assert (await meter_bodies(actions.pool, receipts=receipts))["metered"] == 1
    stamped = await actions.pool.fetchval(
        "SELECT receipt_mtime FROM body_usage WHERE handle='body0002'")
    assert stamped == datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC)


async def test_a_body_receipt_is_metered_ONCE_and_never_double_billed(
    actions: Actions, tmp_path: Path,
) -> None:
    """Idempotent on `handle`: a body is dissolved once, so sweeping its receipt twice — or
    two ticks racing — must cost nothing extra."""
    receipts = tmp_path / "body-receipts"
    _body_receipt_file(receipts, "body0003")
    assert (await meter_bodies(actions.pool, receipts=receipts))["metered"] == 1
    assert (await meter_bodies(actions.pool, receipts=receipts))["metered"] == 0
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM body_usage WHERE handle='body0003'") == 1


async def test_malformed_and_zero_byte_body_receipts_are_skipped_never_crash(
    actions: Actions, tmp_path: Path,
) -> None:
    """The sweep must survive a body still dissolving (0-byte file), unparseable JSON, a
    future version this parser doesn't speak, and a receipt missing a required field — skip
    and count each, exactly as `_envelope` does for wake receipts. Never crash the sweep."""
    receipts = tmp_path / "body-receipts"
    receipts.mkdir(parents=True)
    (receipts / "still-writing.json").write_text("")                  # 0-byte: still dissolving
    (receipts / "not-json.json").write_text("{not valid json")        # malformed
    _body_receipt_file(receipts, "wrongver", v=2)                     # not THIS parser's shape
    _body_receipt_file(receipts, "missing-field", core_seconds=None)  # required field absent

    rep = await meter_bodies(actions.pool, receipts=receipts)
    assert rep == {"metered": 0, "skipped": 4}
    # scoped to THESE handles, not a bare table count — body_usage isn't in conftest's
    # per-test TRUNCATE list (it's telemetry, like llm_usage's sibling), so a raw count here
    # would depend on whatever earlier tests in this file already swept successfully.
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM body_usage WHERE handle IN ('wrongver','missing-field')") == 0


async def test_the_digest_surfaces_resource_seconds_beside_dollars(
    actions: Actions, tmp_path: Path,
) -> None:
    """The membrane's spend surface gains the meter's OTHER dimension — core-seconds and
    RAM-gib-seconds grouped by exit cause, in the SAME report shape as `costs`, right beside
    it. Visibility only: no enforcement policy is invented here, the ceiling's dollar gate
    (orchestrator/ceiling.py) is untouched."""
    from src.orchestrator.digest import fleet_digest

    # a clean slate for THIS table only: body_usage carries no per-test TRUNCATE (see above),
    # and this assertion cares about exact totals, not just presence.
    await actions.pool.execute("DELETE FROM body_usage")
    receipts = tmp_path / "body-receipts"
    _body_receipt_file(receipts, "digest0001", exit_cause="exited")
    _body_receipt_file(receipts, "digest0002", exit_cause="oom_killed")
    assert (await meter_bodies(actions.pool, receipts=receipts))["metered"] == 2

    dg = await fleet_digest(actions, since=datetime(2020, 1, 1, tzinfo=UTC))
    assert dg["bodies"]["count"] == 2
    assert dg["bodies"]["core_seconds"] == 25.0        # 12.5 x 2
    assert round(dg["bodies"]["ram_gib_seconds"], 1) == 6.4
    assert {b["exit_cause"] for b in dg["bodies"]["by"]} == {"exited", "oom_killed"}
    assert "visibility only" in dg["bodies"]["coverage"]
    assert dg["summary"]["body_core_seconds"] == 25.0

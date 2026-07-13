"""WHAT DID THE GHOST FARM COST? — metering the one thing Osiris does that nobody could see.

818 agent wakes. ZERO of them in llm_usage.

The single most expensive act in the system — spawning an entire Claude session, in a repo, with
tools — was invisible in the ledger. I can tell the operator the miner cost $40.49 to the cent. I
CANNOT TELL HIM WHAT THE GHOST FARM COST, and that farm minted 463 agents on projects he had not
opened in days.

THAT IS THE SAME DISEASE I SPENT ALL WEEK KILLING IN THE MINER: a producer whose spend nobody
counted, and which therefore could not be falsified, and which therefore rotted. The wake trigger
is dark today for other reasons (it reads a liveness field that used to lie), but if it ever comes
back it must come back METERED. A hand you cannot cost is a hand you cannot govern.

AND THE TRUTH WAS ON THE DISK THE WHOLE TIME. A wake writes a transcript like any other session,
and Claude Code stamps every assistant turn with its real usage — input, output, and the cache
split — and the model that served it. Nobody ever read it.

This is an OBSERVATION: a parse of files we already have. No model, no inference, no money, and it
cannot be wrong. It reads each wake ONCE, ever.

WHAT IT DOES NOT DO IS GUESS THE PRICE. The transcript carries TOKENS, not dollars. We record the
tokens — which are a FACT — and leave cost_usd NULL rather than fabricating a number from a price
table that will be stale within the month. An honest gap beats a confident invention; that is the
whole law of this week, and it applies to me too.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import asyncpg

from src.ingest.providers import Usage
from src.ingest.sessions import _WAKE_FIRST_TURN
from src.ingest.usage import record_usage

_METERED = "wake-metered:"
BATCH = 50   # free work, but a first pass over a fleet's history should not hog the loop


def metered_key(sid: str) -> str:
    return f"{_METERED}{sid}"


def _tally(path: Path) -> tuple[str, Usage] | None:
    """Sum a WAKE transcript's real usage. None if this is not a wake at all.

    The fingerprint is the FIRST user turn — never a mention. A session that merely DISCUSSES the
    wake prompt (this project has, at length) is a real conversation, and billing it as a wake
    would be the instrument miscounting itself.
    """
    first_user_seen = False
    is_wake = False
    model = ""
    tot = {"input": 0, "output": 0, "cache_read": 0, "cache_creation": 0}
    try:
        with path.open(errors="replace") as fh:
            for line in fh:
                try:
                    d = json.loads(line)
                except ValueError:
                    continue
                if not first_user_seen and d.get("type") == "user" and not d.get("isSidechain"):
                    first_user_seen = True
                    content = (d.get("message") or {}).get("content")
                    if isinstance(content, list):
                        content = " ".join(c.get("text", "") for c in content
                                           if isinstance(c, dict))
                    is_wake = str(content or "").lstrip().startswith(_WAKE_FIRST_TURN)
                    if not is_wake:
                        return None
                if d.get("type") != "assistant":
                    continue
                m = d.get("message") or {}
                u = m.get("usage") or {}
                if not u:
                    continue
                model = str(m.get("model") or model)
                tot["input"] += int(u.get("input_tokens") or 0)
                tot["output"] += int(u.get("output_tokens") or 0)
                tot["cache_read"] += int(u.get("cache_read_input_tokens") or 0)
                tot["cache_creation"] += int(u.get("cache_creation_input_tokens") or 0)
    except OSError:
        return None
    if not is_wake or not model:
        return None
    # cost_usd stays None ON PURPOSE: the transcript gives TOKENS, and a price table invented here
    # would be a guess wearing the authority of a measurement. The tokens are the fact.
    return path.stem, Usage(
        model=model, input_tokens=tot["input"], output_tokens=tot["output"],
        cache_read_tokens=tot["cache_read"], cache_creation_tokens=tot["cache_creation"])


def _wake_files(root: Path) -> list[Path]:
    try:
        return [p for p in root.expanduser().rglob("*.jsonl")
                if not p.parent.name.endswith("-osiris-extract")]
    except OSError:
        return []


async def meter_wakes(pool: asyncpg.Pool, root: Path, *, limit: int = BATCH) -> dict[str, int]:
    """Read every un-metered wake transcript into llm_usage. Free, deterministic, once each."""
    files = await asyncio.to_thread(_wake_files, root)
    if not files:
        return {"metered": 0, "tokens": 0}
    done = {r["key"] for r in await pool.fetch(
        "SELECT key FROM watermarks WHERE key LIKE $1", f"{_METERED}%")}
    metered = tokens = 0
    for path in files:
        if metered_key(path.stem) in done:
            continue
        got = await asyncio.to_thread(_tally, path)
        # Mark EVERY file we looked at, wake or not — otherwise we re-parse the whole fleet's
        # history on every tick forever, which is free but stupid.
        await pool.execute(
            "INSERT INTO watermarks (key, cursor, updated_at) VALUES ($1,'1',now()) "
            "ON CONFLICT (key) DO UPDATE SET updated_at=now()", metered_key(path.stem))
        if got is None:
            continue
        _, usage = got
        await record_usage(pool, purpose="wake", usage=usage)
        metered += 1
        tokens += usage.input_tokens + usage.output_tokens
        if metered >= limit:
            break
    return {"metered": metered, "tokens": tokens}

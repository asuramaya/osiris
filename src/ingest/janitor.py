"""THE MINER CLEANS UP AFTER ITSELF — on the same pass it emits.

The operator, 2026-07-12: "the miner should not only shit out slop, it should also clean up and
check and balance itself on the same pass so we don't end up with a noisy garbage graph."

He is right, and the fault is architectural. THE MINER WAS WRITE-ONLY. It emitted and never once
retracted, so every bug in it deposited permanent sediment, and a memory that only accretes is not
a memory — it is a landfill. 81% of the graph became machine inference; 959 of 1059 open threads
were untouched guesses nobody had ever read.

WHAT THE JANITOR MAY TOUCH — and the boundaries are the whole design:

  1. ONLY ITS OWN OUTPUT. Evidence class DERIVED, minted by the mining path. The miner may retract
     what the miner wrote. It may never touch a mind's declaration, another source's objects, or
     anything a human signed. (Rule 5: a miner never touches another source's objects.)

  2. ONLY WHAT NO MIND HAS TOUCHED. The instant an agent triages, adopts, resolves, or so much as
     comments on a mined thread, IT IS THEIRS. A mind's attention is testimony, and testimony
     outranks the machine that produced the row. This guard is absolute and it is checked first.

  3. ONLY WHAT IS PROVABLY GARBAGE — never what is merely suspected. Two classes qualify, and both
     exist ONLY because of bugs, which is exactly why they are decidable:

       (a) MINED FROM OSIRIS'S OWN ALARM CLOCK. The origin session's first words are the wake
           prompt: Osiris spawned it, Osiris talked to it, and then Osiris filed the echo as
           something the fleet had LEARNED. That was never knowledge, it was feedback.

       (b) PLAGIARISED FROM A DILIGENT AUTHOR. The origin session is self-documenting — it records
           its own decisions deliberately. The ownership boundary was supposed to leave those
           alone ("backfill the SILENT, never second-guess the diligent") and it NEVER FIRED,
           because it compared a session's transcript-derived id against the seat it actually
           writes under. So the miner spent its life re-minting reworded copies of the very
           decisions its best authors had already written by hand.

     Notice what is NOT on this list: "it looks stale", "nobody has read it", "it seems
     duplicative". LEXICAL SIMILARITY MAY ASK, BUT MUST NEVER ASSERT (ruling e27f7c3). A janitor
     that throws away what it merely suspects is worse than the mess it was cleaning.

  4. NEVER DELETE. Invariant 3: heal with compensating events. A retraction is an ASSERTION —
     `retracted` + `retracted_because` — so the row stays readable, auditable, and reversible
     forever. The lens stops hauling it; the record never forgets it happened.

Run on every miner tick (bounded), and retroactively over the sediment already laid down.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.ingest.sessions import _WAKE_FIRST_TURN, _is_self_documenting
from src.parsers.base import EvidenceClass

_SOURCE = "session-janitor"
_EC = EvidenceClass.DIRECT_OBSERVATION  # a fact about the record, not a reading of a conversation
_CONF = 0.95

WAKE_SPAWN = "mined from a wake session Osiris spawned itself — its chatter was never knowledge"
PLAGIARISED = ("mined from a session that documents itself — the ownership boundary should have "
               "left it alone (rule 7: backfill the silent, never second-guess the diligent)")


def _first_user_turn(path: Path) -> str:
    try:
        with path.open(errors="replace") as fh:
            for _ in range(40):
                line = fh.readline()
                if not line:
                    return ""
                if '"user"' not in line:
                    continue
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue
                if entry.get("type") != "user" or entry.get("isSidechain"):
                    continue
                content = (entry.get("message") or {}).get("content")
                if isinstance(content, list):
                    content = " ".join(c.get("text", "") for c in content if isinstance(c, dict))
                return str(content or "")
    except OSError:
        pass
    return ""


def wake_origins(root: Path) -> set[str]:
    """Every session id whose transcript OPENS with the wake prompt — Osiris's own spawns.

    The fingerprint is the FIRST TURN, never a mention: a session that merely discusses the wake
    prompt (this one has, at length) is a real conversation and its work is real."""
    out: set[str] = set()
    for p in root.expanduser().rglob("*.jsonl"):
        if p.parent.name.endswith("-osiris-extract"):
            continue
        if _first_user_turn(p).lstrip().startswith(_WAKE_FIRST_TURN):
            out.add(p.stem)
    return out


async def janitor_pass(
    actions: Actions, *, root: Path, dry_run: bool = True, limit: int = 200,
) -> dict[str, Any]:
    """Retract the miner's provable garbage. Returns what it did (or would do).

    `limit` bounds a tick's work — the sediment took months to lay down and does not have to be
    cleared in one pass. `dry_run` reports without writing, and IT IS THE DEFAULT: a janitor that
    cannot be rehearsed is a shredder.
    """
    pool = actions.pool
    wakes = await _to_thread(root)
    short = {w[:8] for w in wakes}
    rows = await pool.fetch(
        "SELECT o.id, o.type, ca.source_id AS origin, ca.value #>> '{}' AS summary "
        "FROM objects o JOIN current_assertions ca ON ca.object_id=o.id AND ca.name='summary' "
        "WHERE o.type IN ('Thread','Decision') AND o.status='active' "
        # the miner's OWN output, and only that
        "  AND ca.evidence_class='derived' AND ca.source_id LIKE 'agent:%' "
        # GUARD ONE, absolute: a mind's attention is testimony. Never retract what it touched.
        "  AND NOT EXISTS (SELECT 1 FROM assertions sa WHERE sa.object_id=o.id "
        "                  AND sa.evidence_class='self_declared') "
        # ...and never re-retract
        "  AND NOT EXISTS (SELECT 1 FROM current_assertions r WHERE r.object_id=o.id "
        "                  AND r.name='retracted') "
        "ORDER BY o.created_at")
    verdicts: dict[str, bool] = {}
    hits: list[tuple[Any, str, str]] = []
    for r in rows:
        origin = str(r["origin"])
        sid = origin.removeprefix("agent:")
        if sid in wakes or sid[:8] in short:
            hits.append((r["id"], str(r["type"]), WAKE_SPAWN))
            continue
        if origin not in verdicts:
            verdicts[origin] = await _is_self_documenting(pool, origin)
        if verdicts[origin]:
            hits.append((r["id"], str(r["type"]), PLAGIARISED))
        if len(hits) >= limit:
            break

    report: dict[str, Any] = {
        "candidates": len(hits),
        "from_wake": sum(1 for _, _, w in hits if w == WAKE_SPAWN),
        "plagiarised": sum(1 for _, _, w in hits if w == PLAGIARISED),
        "dry_run": dry_run,
    }
    if dry_run or not hits:
        report["sample"] = [str(i)[:8] for i, _, _ in hits[:5]]
        return report

    now = datetime.now(UTC)
    for oid, otype, why in hits:
        # a compensating EVENT, never a delete: the row stays readable and this is reversible by
        # re-asserting retracted='' — the record never forgets that we swept it
        await actions.assert_property(oid, "retracted", True, _SOURCE, now, _CONF,
                                      evidence_class=_EC.value)
        await actions.assert_property(oid, "retracted_because", why, _SOURCE, now, _CONF,
                                      evidence_class=_EC.value)
        if otype == "Thread":  # drop it off every open-thread lens (they read `status`)
            await actions.assert_property(oid, "status", "retracted", _SOURCE, now, _CONF,
                                          evidence_class=_EC.value)
    report["retracted"] = len(hits)
    return report


async def _to_thread(root: Path) -> set[str]:
    import asyncio
    return await asyncio.to_thread(wake_origins, root)

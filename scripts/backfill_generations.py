#!/usr/bin/env python3
"""Backfill true generations from transcript history (ruling a882b334: backfill is NECESSARY).

The mind ruling made the roman numeral mean WHICH MIND — one contiguous run of one model on
one unbroken context — but every lineage alive today was numbered under the old tenure
semantics: live swaps and compactions never minted, so the numerals understate the dead. This
one-shot walks each live lineage's own transcript (the harness's authoritative record),
counts the seams the old rules ignored (main-loop model changes + compact boundaries, a
change AT a boundary merged into one death), and mints the deficit of heirs with the seam's
own timestamp and succession string, then moves the durable mount row to the true head.

Scope: lineages with a durable mount row or a claimed handle, whose base session id is a real
8-hex transcript anchor. Sub-agent (full-UUID) canonicals never mint — the wall stands.
Where a lineage already holds minted generations (a session-death heir), only the DEFICIT is
minted: existing numerals are assumed to cover the earliest seams, so the head count lands
true even when per-heir seam attribution is best-effort (each backfilled heir says so in its
minted_because). Idempotent: a second run finds no deficit.

Usage: uv run python scripts/backfill_generations.py [--apply]  (default: dry-run report)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg
from src.actions.core import Actions
from src.config.dev_env import refuse_silent_live_db
from src.db.pool import create_pool
from src.orchestrator.agents import _CONF, _EC, _generation, _lineage_head, mint_heir
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")
PROJECTS = Path.home() / ".claude" / "projects"


@dataclass
class Seam:
    """One death the old rules never recorded."""

    because: str            # 'compaction' | 'model-succession'
    succession: str | None  # '<old> → <new>' when the model changed across the seam
    at: datetime
    model_after: str        # the new mind's model (the source_model stamp for the heir)


def _ts(e: dict[str, Any]) -> datetime | None:
    raw = e.get("timestamp")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None


def read_seams(files: list[Path]) -> tuple[list[Seam], str | None]:
    """Replay a lineage's transcript(s) and return (ordered seams, final model). Main-loop
    assistant messages only (sidechains are sub-agents; non-claude models are synthetic
    fallbacks, not the seat); a model change landing at a compact boundary is ONE death."""
    seams: list[Seam] = []
    current: str | None = None
    pending_compact: datetime | None = None
    for f in files:
        with f.open() as fh:
            for line in fh:
                try:
                    e = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if e.get("subtype") == "compact_boundary" and current is not None:
                    # only a lived life can die: a boundary before any main message is the
                    # file opening mid-compaction of an out-of-scope past
                    pending_compact = _ts(e) or pending_compact
                    continue
                if e.get("type") != "assistant" or e.get("isSidechain"):
                    continue
                model = str((e.get("message") or {}).get("model") or "")
                if not model.startswith("claude-"):
                    continue
                when = _ts(e) or datetime.now(UTC)
                if current is None:
                    current = model
                elif pending_compact is not None:
                    seams.append(Seam(
                        "compaction",
                        f"{current} → {model}" if model != current else None,
                        pending_compact, model))
                    current = model
                elif model != current:
                    seams.append(Seam("model-succession", f"{current} → {model}", when, model))
                    current = model
                pending_compact = None
    return seams, current


async def lineages(pool: asyncpg.Pool) -> list[str]:
    """Lineage ROOTS worth backfilling: agents holding a mount row or a handle."""
    rows = await pool.fetch(
        "SELECT agent_id AS c FROM agent_mounts UNION "
        "SELECT o.canonical FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='Agent' AND a.name='handle'")
    return sorted({_generation(r["c"])[0] for r in rows})


async def head_of(actions: Actions, root: str) -> tuple[str, int]:
    head = await _lineage_head(actions, root)
    return head, _generation(head)[1]


async def run(apply: bool) -> None:
    # thread 86d562e0: this DSN's own fallback IS the live fleet graph, no isolated dev
    # instance exists on this box — refuse a silent one-off run against it.
    refusal = refuse_silent_live_db("backfill_generations")
    if refusal is not None:
        print(refusal, file=sys.stderr)
        raise SystemExit(1)
    pool = await create_pool(DSN, min_size=1, max_size=2)
    actions = Actions(pool)
    total_minted = planned = 0
    for root in await lineages(pool):
        sid = root.removeprefix("agent:")
        if len(sid) != 8 or not all(c in "0123456789abcdef" for c in sid):
            continue  # derived fallback ids (j…/s…) and full-UUID sub-agents never mint
        files = sorted(PROJECTS.glob(f"*/{sid}*.jsonl"), key=lambda p: p.stat().st_mtime)
        files = [f for f in files if "subagents" not in f.parts]
        if not files:
            print(f"{root}: no transcript on this box — skipped")
            continue
        seams, final_model = read_seams(files)
        head, cur_gen = await head_of(actions, root)
        true_gen = 1 + len(seams)
        deficit = true_gen - cur_gen
        print(f"{root}: {len(seams)} seams in transcript → true gen {true_gen}, "
              f"graph head {head} (gen {cur_gen}) → "
              + (f"mint {deficit}" if deficit > 0 else "already true"))
        if deficit > 0:
            planned += deficit
        if deficit <= 0 or not apply:
            continue
        # existing generations are assumed to cover the EARLIEST seams; mint the tail
        for seam in seams[len(seams) - deficit:]:
            ancestor_oid = await actions.create_or_find_object("Agent", head, head)
            heir, heir_oid = await mint_heir(
                actions, head, ancestor_oid,
                because=f"backfill:{seam.because}", succession=seam.succession, now=seam.at)
            do = EvidenceClass.DIRECT_OBSERVATION
            await actions.assert_property(heir_oid, "source_model", seam.model_after, heir,
                                          seam.at, confidence_for(do), evidence_class=do.value)
            await actions.assert_property(heir_oid, "session", sid, heir, seam.at, _CONF,
                                          evidence_class=_EC)
            print(f"  minted {heir} ({seam.because}"
                  + (f", {seam.succession}" if seam.succession else "")
                  + f", {seam.at:%Y-%m-%d %H:%M})")
            head = heir
            total_minted += 1
        await pool.execute(
            "UPDATE agent_mounts SET agent_id=$2, model=COALESCE($3, model) "
            "WHERE agent_id LIKE $1 || '%' OR agent_id=$2",
            root, head, final_model)
    if apply:
        print(f"minted {total_minted} heirs")
    else:
        print(f"dry run — would mint {planned} heirs (pass --apply to write)")
    await pool.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="write; default is a dry-run report")
    asyncio.run(run(p.parse_args().apply))

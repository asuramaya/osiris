#!/usr/bin/env python3
"""Backfill the `arc` taxonomy onto existing osiris open threads (thread 8df8e611 / roadmap
v2, Thoth's locked 7-value list, DM 1299 -> 1307 -> 1311). The taxonomy is a human judgment
call, already made -- Thoth's one-pass review (msg 1311) on the table I proposed (msg 1307,
queried live off all 51 real open osiris threads). This script only APPLIES the reviewed
mapping mechanically; it does not re-derive or second-guess any of it.

Three kinds of action, matching msg 1311 exactly:
  1. ARC_MAP -- tag `arc` on an existing, otherwise-untouched thread.
  2. RESOLVE_MAP -- don't tag; the thread's work is already delivered, so resolve it outright
     (currently just d6ed2f17, shipped by recall()).
  3. NEW_THREADS -- two findings Thoth's review asked opened, not tags on existing threads
     (the stale-handoff-letter closure pass; the cross-project-leakage flag).

Everything else from the proposed table (the cross-project leakage cluster itself, and the
recommend-leave-unsorted set) is deliberately left untouched -- printed for visibility, never
silently dropped.

Dry-run by default, writes nothing. Idempotent: re-running after a partial or full apply is
safe (assert_property on the same value, resolve_thread on an already-resolved thread, and
open_thread's own summary-hash idempotency all no-op cleanly).

Usage: .venv/bin/python scripts/backfill_thread_arc.py [--apply]
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

# repo root importable regardless of PYTHONPATH (thread 3e96c10e: these top-level `from
# src...` imports failed ModuleNotFoundError on the exact bare invocation this script's own
# docstring documents, since sys.path[0] is the script's own directory, never CWD).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.actions.core import Actions  # noqa: E402
from src.config.dev_env import refuse_silent_live_db  # noqa: E402
from src.db.pool import create_pool  # noqa: E402
from src.orchestrator.capture import (  # noqa: E402
    _CONF,
    _EC,
    ARCS,
    _find_thread,
    open_thread,
    resolve_thread,
)

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")
SOURCE = "agent:c38f8f3b-v"

# Reviewed mapping (Thoth, msg 1311, on the table proposed msg 1307) -- short id -> arc.
ARC_MAP = {
    # Identity-Succession
    "dda0072f": "Identity-Succession",
    "8f140535": "Identity-Succession",
    "8f005905": "Identity-Succession",
    "2bbb35bd": "Identity-Succession",
    "edfaa6e0": "Identity-Succession",
    "ce892917": "Identity-Succession",
    "9f566244": "Identity-Succession",
    "88753005": "Identity-Succession",
    "879c97b9": "Identity-Succession",
    # Compaction-Resilience
    "aeae9977": "Compaction-Resilience",
    "af911f47": "Compaction-Resilience",
    "00378259": "Compaction-Resilience",
    "b6a64207": "Compaction-Resilience",
    "1d3e4bdc": "Compaction-Resilience",
    "0ca15d72": "Compaction-Resilience",
    "09da2fb6": "Compaction-Resilience",
    "b30519ce": "Compaction-Resilience",
    "215e5405": "Compaction-Resilience",
    "5177057a": "Compaction-Resilience",  # Thoth's call: Khnum's finding A is the PreCompact
                                          # offload fallback, not generic hygiene
    # Model-Identity
    "0a4ec5e7": "Model-Identity",  # resolved this session; tagged per my own proposal,
                                    # unchallenged in Thoth's review
    "9a22d0a2": "Model-Identity",  # the hook-feasibility follow-on, opened this session
                                    # (DM 1310) -- live open_thread tool predates `arc`
    # Token-Cost
    "24c3cc74": "Token-Cost",
    "ccee2304": "Token-Cost",
    "f34c572c": "Token-Cost",  # Thoth's call: orient verbosity IS the token lever, not
                                # generic hygiene
    # Surfaces-Roadmap-Docs
    "d56e7073": "Surfaces-Roadmap-Docs",
    "588148bb": "Surfaces-Roadmap-Docs",
    "9d2aaf4d": "Surfaces-Roadmap-Docs",
    # Fleet-Hygiene
    "33950e11": "Fleet-Hygiene",
    "594bc79e": "Fleet-Hygiene",
    "5af93c89": "Fleet-Hygiene",
    "1aa2ff36": "Fleet-Hygiene",
    "7db914dd": "Fleet-Hygiene",
    "4951d818": "Fleet-Hygiene",
    "9dc3ce8b": "Fleet-Hygiene",
    "7c65472b": "Fleet-Hygiene",
    "539ae43b": "Fleet-Hygiene",
    "6fa9791d": "Fleet-Hygiene",
    "1dbd3d0c": "Fleet-Hygiene",
    "31cfe91b": "Fleet-Hygiene",
}

# Thoth's call (msg 1311): don't tag -- the work is already delivered, resolve outright.
RESOLVE_MAP = {
    "d6ed2f17": dict(
        because="delivered by recall(ref) -- read-one-by-id verb, commit 84fc6f6.",
        artifact="84fc6f6",
    ),
}

# Left untouched on purpose -- named here so a report reader sees the whole picture, never a
# silent gap. Documentation only; no script action.
SKIPPED_CROSS_PROJECT_LEAKAGE = [
    "c5a14e8a", "f9981fec", "c287e84f", "2e8a1554", "3417f13d", "88ab35ae", "1964ea32",
]
LEFT_UNSORTED = ["cf6003af", "2ce21921", "a9cad7f7", "0666ebc5", "a4954f99", "00f6a18d"]

# Two findings Thoth's review asked opened -- new threads, not arc-tags on existing ones.
NEW_THREADS = [
    dict(
        summary=(
            "CLOSURE PASS OWED (Thoth's word, DM 1311, arc-backfill review): the stale "
            "handoff-letter cluster (8f140535, 8f005905, 2bbb35bd, edfaa6e0) is "
            "succession-hygiene debt, not just a tagging exercise -- each is a prior "
            "generation's handoff letter, long since superseded by the seat's own actual "
            "history. Read each, confirm nothing in it is still a live open ask, and "
            "resolve the ones that are purely historical record."
        ),
        kind="obligation", owner=SOURCE, arc="Identity-Succession",
    ),
    dict(
        summary=(
            "7 open threads (c5a14e8a, f9981fec, c287e84f, 2e8a1554, 3417f13d, 88ab35ae, "
            "1964ea32 -- hyper-home/dom0/Harris-portal content) read as non-osiris "
            "fleet-infrastructure entirely; flagged during the arc-taxonomy backfill "
            "(msg 1307/1311) rather than force-tagged. Likely a miner cross-project "
            "filing bug (wrong project attribution), not a taxonomy gap -- verify project "
            "filing, re-file or reap if confirmed leaked from a different sub-project."
        ),
        kind="obligation", arc="Fleet-Hygiene",
    ),
]


async def run(apply: bool) -> None:
    # thread 86d562e0: this DSN's own fallback IS the live fleet graph, no isolated dev
    # instance exists on this box — refuse a silent one-off run against it.
    refusal = refuse_silent_live_db("backfill_thread_arc")
    if refusal is not None:
        print(refusal, file=sys.stderr)
        raise SystemExit(1)
    pool = await create_pool(DSN, min_size=1, max_size=2)
    actions = Actions(pool)
    observed = datetime.now(UTC)

    print("== arc tags ==")
    by_arc: dict[str, int] = {}
    missing: list[str] = []
    for short, arc in ARC_MAP.items():
        assert arc in ARCS, arc
        tid = await _find_thread(pool, short)
        if tid is None:
            missing.append(short)
            print(f"  {short}: NOT FOUND -- skipped")
            continue
        status = await pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
            "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", tid)
        by_arc[arc] = by_arc.get(arc, 0) + 1
        print(f"  {short} ({status}) -> {arc}")
        if apply:
            await actions.assert_property(tid, "arc", arc, SOURCE, observed, _CONF,
                                          evidence_class=_EC)

    print("\n== resolve instead of tag ==")
    for short, kw in RESOLVE_MAP.items():
        tid = await _find_thread(pool, short)
        if tid is None:
            print(f"  {short}: NOT FOUND -- skipped")
            continue
        print(f"  {short}: resolve ({kw['because']})")
        if apply:
            await resolve_thread(actions, short, source=SOURCE, **kw)

    print("\n== new threads ==")
    for spec in NEW_THREADS:
        print(f"  open: {spec['summary'][:88]}...")
        if apply:
            t = await open_thread(actions, source=SOURCE, **spec)
            print(f"    -> {t}")

    print("\n== left untouched (documentation only, no action) ==")
    print(f"  cross-project leakage cluster, not tagged: {SKIPPED_CROSS_PROJECT_LEAKAGE}")
    print(f"  left unsorted, agreed: {LEFT_UNSORTED}")

    print("\n== bucket counts ==")
    for arc in ARCS:
        print(f"  {arc}: {by_arc.get(arc, 0)}")
    if missing:
        print(f"  MISSING (not found, review before re-running): {missing}")

    if apply:
        print(f"\napplied: {len(ARC_MAP) - len(missing)} arc tags, {len(RESOLVE_MAP)} "
              f"resolve(s), {len(NEW_THREADS)} new thread(s)")
    else:
        print(f"\ndry run -- {len(ARC_MAP) - len(missing)} of {len(ARC_MAP)} would be "
              "tagged, pass --apply to write")
    await pool.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="write; default is a dry-run report")
    asyncio.run(run(p.parse_args().apply))

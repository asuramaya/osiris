#!/usr/bin/env python3
"""Lane 1 — THE BACKFILL (Thoth msg 6435, operator dispatch 2026-09-02).

`status` on a `Thread` is meant to be single-valued per object, but `assert_property`'s
same-source-only supersession lets a DIFFERENT source's later `status='resolved'` sit
alongside an unrelated earlier `status='open'` forever — both simultaneously `is_current`
(ruling 1335332e). Measured 2026-09-02: 1,309 active Threads carry a current `open` AND a
current `resolved` at once; 1,255 of those are the plain two-witness case, 52 carry a third
current status row and 2 carry a fourth (all still confined to {open, resolved} in-scope, see
below), 4 threads ALSO carry a third value outside {open, resolved} (`retracted`, session-
janitor's own miner-garbage lifecycle — a genuinely different, out-of-scope population; see
the module docstring's "OUT OF SCOPE" note below) and trigger the whole-run refusal.

THE TRAP TESTED (Thoth's own instruction): does any thread's current `open` postdate its
current `resolved` — a genuine reopen, where collapsing to `resolved` would destroy real
work? Measured: 0 of 1,309. Every specimen's newest current row is `resolved`. The code
below does not trust that as a constant — it re-derives per thread and EXCLUDES (never
collapses) any thread where `open` is newest, reporting the population size whether zero
or not.

OUT OF SCOPE, NOT TOUCHED: a much larger population (689 threads, 1,378 rows) carries a
current `status='retracted'` written by `janitor_pass`/`dispose.py`'s own candidate-
retirement lifecycle, cross-source against an earlier `open` — the SAME leak mechanism,
but a different population than the one this dispatch named (open+resolved only). Flagged
for its own lane, not built here — scope was explicit.

MECHANISM: `Actions.assert_singular_property` (src/actions/core.py, merged 41d2fc9,
ruling 1335332e) — cross-source collapse, takes an advisory lock, never deletes. This
script calls it exactly once per collapsed thread, re-asserting the WINNING row's own
existing (value, source_id, observed_at, confidence, evidence_class) — an idempotent
"this is what already stands" — which is precisely what makes every OTHER current status
row on that object non-current. No direct `UPDATE assertions` anywhere in this file.

REFUSAL, same posture as `_retire_handoff_backlog` (mcp_server.py): if ANY thread in the
open+resolved population carries a status shape this script did not expect (a third value
outside {open,resolved}, or a tie in the newest-row's `observed_at` across differing
values), the WHOLE RUN refuses — `{"ok": False, "reason": ..., "unexpected": [...]}`,
nothing touched, even the clean 1,305 specimens wait for a human decision on the others.

`dry_run=True` is the hard default (no flag flips it silently) — pass `--apply` to write,
and even then this script is not authorized to run tonight; it exists so the operator can
run it once the dry-run evidence below has been reviewed.

Usage: uv run python scripts/backfill_thread_status_collapse.py [--apply] [--limit N]
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from typing import Any

import asyncpg
from src.actions.core import Actions
from src.config.dev_env import refuse_silent_live_db
from src.db.pool import create_pool

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")

_EXPECTED = {"open", "resolved"}


async def candidates(pool: asyncpg.Pool) -> dict[str, list[asyncpg.Record]]:
    """Every active Thread with a current 'open' AND a current 'resolved' status row,
    mapped to ALL of its current status rows (not just those two values — a thread also
    carrying a third current value is included here so the refusal check below can see
    it; it is never silently dropped)."""
    rows = await pool.fetch(
        "SELECT a.object_id, o.canonical, a.id, a.value #>> '{}' AS v, a.source_id, "
        "  a.observed_at, a.confidence, a.evidence_class "
        "FROM current_assertions a JOIN objects o ON o.id = a.object_id "
        "WHERE a.name = 'status' AND o.type = 'Thread' AND o.status = 'active' "
        "AND a.object_id IN ("
        "  SELECT a2.object_id FROM current_assertions a2 "
        "  WHERE a2.name = 'status' AND a2.value #>> '{}' = 'open' "
        "  AND EXISTS (SELECT 1 FROM current_assertions a3 "
        "    WHERE a3.object_id = a2.object_id AND a3.name = 'status' "
        "    AND a3.value #>> '{}' = 'resolved')) "
        "ORDER BY o.canonical, a.observed_at"
    )
    by_thread: dict[str, list[asyncpg.Record]] = {}
    for r in rows:
        by_thread.setdefault(r["canonical"], []).append(r)
    return by_thread


def _classify(rows: list[asyncpg.Record]) -> str:
    """One of: 'collapse' (newest current row is uniquely 'resolved'), 'reopen' (newest is
    uniquely 'open' — exclude, never collapse), 'unexpected' (a value outside {open,
    resolved}, or the newest observed_at is tied across differing values)."""
    if any(r["v"] not in _EXPECTED for r in rows):
        return "unexpected"
    newest_at = max(r["observed_at"] for r in rows)
    newest_vals = {r["v"] for r in rows if r["observed_at"] == newest_at}
    if len(newest_vals) != 1:
        return "unexpected"
    return "collapse" if newest_vals == {"resolved"} else "reopen"


async def run(*, apply: bool, limit: int | None) -> dict[str, Any]:
    refusal = refuse_silent_live_db("backfill_thread_status_collapse")
    if refusal is not None:
        print(refusal, file=sys.stderr)
        raise SystemExit(1)
    pool = await create_pool(
        DSN, min_size=1, max_size=2,
        application_name="osiris-script:backfill-thread-status-collapse")
    actions = Actions(pool)

    by_thread = await candidates(pool)
    unexpected: list[dict[str, Any]] = []
    reopened: list[dict[str, Any]] = []
    to_collapse: list[tuple[str, list[asyncpg.Record]]] = []
    for canonical, rows in by_thread.items():
        shape = _classify(rows)
        evidence = {
            "thread": canonical,
            "rows": [
                {"value": r["v"], "source_id": r["source_id"],
                 "observed_at": r["observed_at"].isoformat()}
                for r in sorted(rows, key=lambda r: r["observed_at"])
            ],
        }
        if shape == "unexpected":
            unexpected.append(evidence)
        elif shape == "reopen":
            reopened.append(evidence)
        else:
            to_collapse.append((canonical, rows))

    print(f"threads with current open+resolved: {len(by_thread)}")
    print(f"  reopened (open newer than resolve — EXCLUDED, never touched): {len(reopened)}")
    print(f"  unexpected shape (third value or a tie):                      {len(unexpected)}")
    print(f"  clean collapse candidates:                                    {len(to_collapse)}")

    if unexpected:
        print("\nUNEXPECTED — refusing the whole run, nothing touched:")
        for e in unexpected:
            print(f"  {e['thread']}: {e['rows']}")
        await pool.close()
        return {"ok": False, "reason": "unexpected thread shape(s) found", "unexpected": unexpected}

    if reopened:
        print("\nREOPENED — excluded from this run, left exactly as-is:")
        for e in reopened:
            print(f"  {e['thread']}: {e['rows']}")

    if limit is not None:
        to_collapse = to_collapse[:limit]

    receipts: list[dict[str, Any]] = []
    print(f"\n{'APPLYING' if apply else 'DRY RUN — would apply'} to {len(to_collapse)} threads:")
    for canonical, rows in to_collapse:
        winner = max(rows, key=lambda r: r["observed_at"])
        losers = [r for r in rows if r["id"] != winner["id"]]
        receipt = {
            "thread": canonical,
            "winner": {"value": winner["v"], "source_id": winner["source_id"],
                       "observed_at": winner["observed_at"].isoformat()},
            "collapsed": [
                {"value": r["v"], "source_id": r["source_id"],
                 "observed_at": r["observed_at"].isoformat()}
                for r in losers
            ],
        }
        receipts.append(receipt)
        print(f"  {canonical}: keep resolved@{winner['observed_at']:%Y-%m-%d %H:%M} "
              f"({winner['source_id']}), collapse "
              + ", ".join(f"{r['v']}@{r['observed_at']:%Y-%m-%d %H:%M}({r['source_id']})"
                           for r in losers))
        if apply:
            await actions.assert_singular_property(
                winner["object_id"], "status", winner["v"], winner["source_id"],
                winner["observed_at"], float(winner["confidence"]),
                evidence_class=winner["evidence_class"],
            )

    print(f"\n{'applied' if apply else 'dry run — would apply'}: {len(receipts)} collapses")
    await pool.close()
    return {
        "ok": True, "apply": apply,
        "threads_scanned": len(by_thread), "reopened": len(reopened),
        "collapsed": len(receipts), "receipts": receipts,
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="write; default is a dry-run report")
    p.add_argument("--limit", type=int, default=None, help="cap threads touched this run")
    args = p.parse_args()
    asyncio.run(run(apply=args.apply, limit=args.limit))

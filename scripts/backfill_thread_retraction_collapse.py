#!/usr/bin/env python3
"""Wave 3 Lane A (Thoth msg 6503) — THE 557 RETRACTED SIBLINGS.

Same cross-source leak as Lane 1 (ruling 1335332e), a different value pair: 557 active
Threads carry a current `status='open'` AND a current `status='retracted'` (no `resolved`).
Lane 1 measured this population and deliberately did NOT rule on it (the resolve case does
not obviously transfer) — this lane closes that question.

THE UNMEASURED QUESTION, NOW MEASURED: 304 of the 557 carry a `self_declared` assertion
somewhere in their history — sounds like "a mind touched it," which would mirror Lane 1's
open+resolved case and argue for excluding them. It does NOT. Read closely (not assumed):
in ALL 304, the self_declared assertion IS the retraction itself — `retracted`,
`retracted_because`, `status='retracted'`, written by `dispose.py`'s own candidate-drop path
(`_EC = "self_declared"`, its own comment: "a disposition is a MIND'S WORD, never the
machine's"). Zero of the 304 carry a self_declared `status` assertion holding any value
OTHER than 'retracted' — there is no specimen where a mind's later touch disputes or
reopens the retraction. The other 253 are `session-janitor`'s own automated cleanup
(evidence_class='direct_observation'), covered by its own "no mind ever touched it"
precondition directly. Both classes agree: `retracted` is the deliberate final word, machine
or mind, always the newest current row (0 reopened, verified), row-shape uniformly 2 current
rows per thread (no third value, no multi-witness agreement noise).

RULING: unlike Lane 1's `resolved` case (where a MIND's later act legitimately overturns an
earlier machine witness), here the machine and the mind AGREE — there is no population where
collapsing to `retracted` would destroy real, disputing testimony. Collapse ALL 557 to
`retracted`. `open` is always the sole loser (100% `evidence_class='derived'`, mined noise).

REFUSAL: same posture as Lane 1 — any thread outside this exact shape (a third status value,
more than 2 current rows, a self_declared status assertion disputing 'retracted', or a tie)
refuses the WHOLE run rather than being silently dropped.

MECHANISM: `Actions.assert_singular_property` only, re-asserting the winning `retracted`
row's own existing fields — no direct `UPDATE`.

`dry_run=True` is the hard default. Per Thoth's explicit instruction (msg 6503): dry-run and
report only — this script is NOT authorized to `--apply` yet.

Usage: uv run python scripts/backfill_thread_retraction_collapse.py [--apply] [--limit N]
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


async def candidates(pool: asyncpg.Pool) -> dict[str, list[asyncpg.Record]]:
    """Every active Thread with a current 'open' AND a current 'retracted' status row and
    NO current 'resolved' — the disjoint sibling of Lane 1's own population."""
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
        "    AND a3.value #>> '{}' = 'retracted') "
        "  AND NOT EXISTS (SELECT 1 FROM current_assertions a4 "
        "    WHERE a4.object_id = a2.object_id AND a4.name = 'status' "
        "    AND a4.value #>> '{}' = 'resolved')) "
        "ORDER BY o.canonical, a.observed_at"
    )
    by_thread: dict[str, list[asyncpg.Record]] = {}
    for r in rows:
        by_thread.setdefault(r["canonical"], []).append(r)
    return by_thread


async def _disputed(pool: asyncpg.Pool, object_id: Any) -> bool:
    """True if a self_declared status assertion on this object holds any value other than
    'retracted' — the one shape that would mean a mind's touch disputes the retraction,
    never observed in the 557 but checked per-thread rather than assumed from the aggregate."""
    row = await pool.fetchval(
        "SELECT count(*) FROM assertions WHERE object_id=$1 AND name='status' "
        "AND evidence_class='self_declared' AND value #>> '{}' != 'retracted'",
        object_id,
    )
    return bool(row)


def _classify(rows: list[asyncpg.Record]) -> str:
    """'collapse' (exactly {open, retracted}, retracted uniquely newest), 'unexpected'
    otherwise (a third value, >2 rows, a tie, or open newest — none observed, all refused)."""
    values = {r["v"] for r in rows}
    if values != {"open", "retracted"} or len(rows) != 2:
        return "unexpected"
    newest_at = max(r["observed_at"] for r in rows)
    newest_vals = {r["v"] for r in rows if r["observed_at"] == newest_at}
    if newest_vals != {"retracted"}:
        return "unexpected"
    return "collapse"


async def run(*, apply: bool, limit: int | None) -> dict[str, Any]:
    refusal = refuse_silent_live_db("backfill_thread_retraction_collapse")
    if refusal is not None:
        print(refusal, file=sys.stderr)
        raise SystemExit(1)
    pool = await create_pool(
        DSN, min_size=1, max_size=2,
        application_name="osiris-script:backfill-thread-retraction-collapse")
    actions = Actions(pool)

    by_thread = await candidates(pool)
    unexpected: list[dict[str, Any]] = []
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
            continue
        if await _disputed(pool, rows[0]["object_id"]):
            evidence["reason"] = "self_declared status disputes 'retracted' — untested shape"
            unexpected.append(evidence)
            continue
        to_collapse.append((canonical, rows))

    print(f"threads with current open+retracted (no resolved): {len(by_thread)}")
    print(f"  unexpected shape (refuses the whole run):         {len(unexpected)}")
    print(f"  clean collapse candidates:                        {len(to_collapse)}")

    if unexpected:
        print("\nUNEXPECTED — refusing the whole run, nothing touched:")
        for e in unexpected:
            print(f"  {e['thread']}: {e['rows']}" + (f" ({e['reason']})" if "reason" in e else ""))
        await pool.close()
        return {"ok": False, "reason": "unexpected thread shape(s) found", "unexpected": unexpected}

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
        print(f"  {canonical}: keep {winner['v']}@{winner['observed_at']:%Y-%m-%d %H:%M} "
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
        "threads_scanned": len(by_thread), "collapsed": len(receipts), "receipts": receipts,
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="write; default is a dry-run report")
    p.add_argument("--limit", type=int, default=None, help="cap threads touched this run")
    args = p.parse_args()
    asyncio.run(run(apply=args.apply, limit=args.limit))

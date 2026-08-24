"""dsh00001 identity reconciliation (task #181's sibling, Thoth DM 5442 leg 2, ruling
eb642d37 + the build ruling on DM 5461) — a ONE-OFF, evidenced repair, not the general
mechanism (that's Khnum's provenance-redesign lane, decision c3059bea; this module is a
specimen of it, built to not contradict its shape, never a substitute for it).

THE INCIDENT: agent:dsh00001 reached the graph without ever calling mount() — no Agent
was minted, no lineage existed, and 185 writes landed under that raw, unregistered
source_id. eb642d37 proved (deterministic time-join, not a guess) that the underlying
DSH session actually ran as FIVE distinct minds in sequence, one per contiguous
non-overlapping model stint: every stint carries [start, end] and every write's own
observed_at falls inside exactly one, so each write maps to a model by arithmetic, never
proximity.

THE REPAIR, Thoth's ruling verbatim: mint the five generations retroactively (one per
stint, chained via `mint_heir`'s own model-succession shape — the SAME mechanism a live
mid-session model swap already uses, just backdated to each stint's real start via
`mint_heir`'s `now` parameter); leave the 185 original rows COMPLETELY UNTOUCHED (
constitution 3 — heal with compensating events, never mutate — and the sharper reason:
those rows are HISTORICALLY TRUE, `source_id='agent:dsh00001'` is exactly what a
door that skipped mount() stamped, and rewriting them would assert a correctly-
registered agent wrote them, which never happened, destroying the only evidence the
door was skipped at all); assert ONE new property, `wrote_as`, on whichever generation's
stint actually contains attributed writes, naming the raw actor-string it's retroactively
claiming.

`wrote_as`'s OWN validity window is never encoded as a second, parallel string inside its
value (that would be exactly the duplicate-notion-of-time class this house keeps paying
for) — it reuses the kernel's existing temporal machinery: `wrote_as` is asserted with
`observed_at` = the stint's own start (the SAME instant `mint_heir` stamps this
generation's `succeeded_from`/`minted_because`), and the window's END is already fully
recorded by the standard succession chain the mint itself writes (this generation's own
`succeeded_by`, stamped at the NEXT generation's mint time, at that generation's stint
start) — one notion, reused, not invented.

HARD LIMITS, all binding, all Thoth's own ruling: dsh00001 is NEVER retired (that
destroys 185 real writes); it is NEVER folded into a Thoth lineage (that falsely credits
one lineage with four/five models' work); `is_compaction`/`cost_usd` read 0/0.00 across
every turn in the session this repairs and THAT GAP STAYS NAMED, never papered over with
a guessed generation boundary — this module claims MODEL boundaries only, proven by a
deterministic join, never compaction boundaries.

Khnum's own re-verification (decision c3059bea) found a NON-MONOTONIC model return in a
DIFFERENT DSH session (d4471954: A → B → A → C) — this module's own target session
(d4db540c) is confirmed monotonic (Thoth's ruling, cross-checked here fresh), but the
stint walk below still groups by CONSECUTIVE runs, never by a model-name set, so it would
handle a non-monotonic session correctly too (each return to an earlier model becomes its
OWN new stint/generation, never silently merged with the first).
"""
from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.ingest.harness import SessionLocator, TurnRow
from src.ingest.harness.dsh import _DSH_SESSIONS, DshSessionAdapter
from src.orchestrator.agents import mint_heir
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_DO = EvidenceClass.DIRECT_OBSERVATION


def _find_dsh_session_file(anchor_sid: str, *, root: Path | None = None) -> Path | None:
    """A local, narrowly-scoped stand-in for `DshSessionAdapter.enumerate()`, which
    only looks ONE level under `~/.dsh/sessions/<slug>/` for a session file — the real
    on-disk shape nests `<slug>/session-<uuid>/session.jsonl.zstd`, and a slug commonly
    holds 2+ session subdirectories now (confirmed live: the osiris slug carries both
    this repair's own target, d4db540c, and a newer one). Filed as thread c8c6006e
    (Khnum's adapter-contract lane owns the real fix) — this function exists so THIS
    one-off repair doesn't depend on that bug being fixed first, and doesn't touch
    dsh.py's shared enumerate() path at all. A two-level directory walk, nothing more."""
    sessions_dir = root or _DSH_SESSIONS
    if not sessions_dir.is_dir():
        return None
    for slug_dir in sessions_dir.iterdir():
        if not slug_dir.is_dir():
            continue
        for session_dir in slug_dir.iterdir():
            if not session_dir.is_dir() or anchor_sid not in session_dir.name:
                continue
            for f in session_dir.iterdir():
                if f.is_file() and f.suffix == ".zstd":
                    return f
    return None


class Stint:
    __slots__ = ("model", "n_turns", "start", "end")

    def __init__(self, model: str | None, n_turns: int, start: datetime, end: datetime):
        self.model = model
        self.n_turns = n_turns
        self.start = start
        self.end = end

    def contains(self, at: datetime) -> bool:
        return self.start <= at <= self.end


def _stints_from_turns(turns: Iterator[TurnRow]) -> list[Stint]:
    """Group turns into CONSECUTIVE-by-model runs, bounded by (min, max) recorded_at —
    eb642d37's own protocol. Grouping by adjacency (never by collecting-all-turns-per-
    model-name first) is what makes a non-monotonic return (Khnum's d4471954 finding,
    decision c3059bea) produce a fresh stint rather than silently re-merging with an
    earlier run of the same model."""
    stints: list[Stint] = []
    cur_model: Any = object()  # sentinel: never equals a real model string or None
    cur_start: datetime | None = None
    cur_end: datetime | None = None
    cur_n = 0
    for t in turns:
        if t.model != cur_model:
            if cur_start is not None and cur_end is not None:
                stints.append(Stint(cur_model if cur_model is not None else None,
                                    cur_n, cur_start, cur_end))
            cur_model = t.model
            cur_start = t.recorded_at
            cur_n = 0
        if t.recorded_at is not None:
            cur_end = t.recorded_at
        cur_n += 1
    if cur_start is not None and cur_end is not None:
        stints.append(Stint(cur_model if cur_model is not None else None, cur_n,
                            cur_start, cur_end))
    return stints


def _stint_label(model: str | None) -> str:
    return model if model is not None else "unstamped"


async def measure(
    pool: asyncpg.Pool, *, anchor_sid: str, root_agent_id: str, session_root: Path | None = None,
) -> dict[str, Any]:
    """READ-ONLY: locate the session, re-derive its stints fresh (never trust a cached
    figure — the whole design is re-derive-per-write, decision c3059bea), and bucket
    every graph write currently attributed to `root_agent_id` (or a numbered
    generation of it — repeat runs are idempotent, see `plan`) into the stint that
    contains its observed_at. A write outside every stint is UNATTRIBUTED and named as
    such, never snapped to the nearest neighbour. `session_root` overrides
    `~/.dsh/sessions` — a test-only seam, never passed by a real caller."""
    path = _find_dsh_session_file(anchor_sid, root=session_root)
    if path is None:
        return {"error": f"no DSH session file found for anchor_sid={anchor_sid!r} "
                         "under ~/.dsh/sessions/ (two-level walk)"}
    ad = DshSessionAdapter()
    loc = SessionLocator(anchor_sid=anchor_sid, session_id=anchor_sid, harness=ad.name,
                         source_path=str(path), cwd=None, project=None)
    stints = _stints_from_turns(ad.read_turns(loc))
    if not stints:
        return {"error": f"session file at {path} produced zero turns"}

    rows = await pool.fetch(
        "SELECT source_id, observed_at AS at FROM assertions "
        " WHERE source_id LIKE $1 "
        "UNION ALL SELECT source_id, first_seen FROM links "
        " WHERE source_id LIKE $1", f"{root_agent_id}%")
    buckets: list[int] = [0] * len(stints)
    unattributed: list[str] = []
    for r in rows:
        at = r["at"]
        placed = False
        for i, s in enumerate(stints):
            if at is not None and s.contains(at):
                buckets[i] += 1
                placed = True
                break
        if not placed:
            unattributed.append(f"{r['source_id']}@{at.isoformat() if at else 'null'}")

    return {
        "session_file": str(path), "total_turns": sum(s.n_turns for s in stints),
        "total_writes": len(rows),
        "stints": [
            {"index": i, "model": _stint_label(s.model), "n_turns": s.n_turns,
             "start": s.start.isoformat(), "end": s.end.isoformat(), "writes": buckets[i]}
            for i, s in enumerate(stints)
        ],
        "unattributed": unattributed,
    }


async def reconcile(
    actions: Actions, *, anchor_sid: str, root_agent_id: str, dry_run: bool = True,
    because: str | None = None, session_root: Path | None = None,
) -> dict[str, Any]:
    """THE REPAIR VERB, dry-run-first (restore_attribution/recover_harness_exchanges'
    own shape). Mints one Agent generation per stint (gen1 = `root_agent_id` itself,
    the earliest stint — minted directly, no ancestor; gen2.. via `mint_heir`, backdated
    to each stint's own start, `because='model-succession'`) and asserts `wrote_as` on
    whichever generation's stint actually carries attributed writes (there may be more
    than one — a repeat incident, or a session whose writes span two model eras, gets a
    `wrote_as` on each such generation; a stint with zero writes gets no `wrote_as` at
    all, only the mint).

    IDEMPOTENT: re-running finds the already-minted generations (by canonical, via
    `next_generation`'s own numeral scheme) and mints nothing twice; `wrote_as` uses
    `assert_property`'s own within-source supersession, so a repeat assertion of the
    SAME value from the SAME source is a no-op in substance (a fresh row, same content).

    NEVER retires `root_agent_id`, NEVER folds it into another lineage — this verb only
    ever ADDS a lineage of its own. `dry_run=False` requires a non-blank `because`,
    same discipline every repair verb here holds."""
    if not dry_run and not (because or "").strip():
        return {"error": "dry_run=False requires a non-blank `because` — retroactively "
                         "minting an identity is a deliberate act on the record, never "
                         "silent"}
    measured = await measure(actions.pool, anchor_sid=anchor_sid, root_agent_id=root_agent_id,
                             session_root=session_root)
    if "error" in measured:
        return measured

    from src.orchestrator.agents import next_generation

    plan: list[dict[str, Any]] = []
    # `cur_canonical` is always the CANDIDATE canonical for the generation about to be
    # processed this iteration (root_agent_id at i=0, next_generation(...) after);
    # `ancestor_canonical`/`ancestor_oid` are the ALREADY-RESOLVED previous generation,
    # ready for mint_heir. Both pairs advance together at the bottom of the loop,
    # unconditionally — whether this iteration minted fresh or found an existing row.
    cur_canonical = root_agent_id
    ancestor_canonical: str | None = None
    ancestor_oid: Any = None
    for i, st in enumerate(measured["stints"]):
        existing = await actions.pool.fetchrow(
            "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", cur_canonical)
        will_mint = existing is None
        entry: dict[str, Any] = {
            "generation": cur_canonical, "model": st["model"], "start": st["start"],
            "end": st["end"], "writes": st["writes"],
            "action": ("mint (root)" if i == 0 else "mint (heir)") if will_mint
                     else "already minted — skip",
        }
        plan.append(entry)
        if dry_run:
            if st["writes"]:
                entry["would_assert_wrote_as"] = root_agent_id
            cur_canonical = next_generation(cur_canonical)
            continue

        now_i = datetime.fromisoformat(st["start"])
        if i == 0:
            cur_oid = existing["id"] if existing is not None else (
                await actions.create_or_find_object("Agent", cur_canonical, "imhotep"))
        elif will_mint:
            assert ancestor_canonical is not None and ancestor_oid is not None, (
                "unreachable at i>0: the prior iteration always sets both before this "
                "one runs"
            )
            prev_model = _stint_label(measured["stints"][i - 1]["model"])
            heir_canon, heir_oid = await mint_heir(
                actions, ancestor_canonical, ancestor_oid, because="model-succession",
                succession=f"{prev_model} → {st['model']}", now=now_i)
            assert heir_canon == cur_canonical, (
                f"mint_heir produced {heir_canon!r}, expected {cur_canonical!r} — the "
                "numeral scheme drifted out from under this loop's own bookkeeping")
            cur_oid = heir_oid
        else:
            cur_oid = existing["id"]

        if st["writes"]:
            await actions.assert_property(
                cur_oid, "wrote_as", root_agent_id, "imhotep", now_i,
                confidence_for(_DO), evidence_class=_DO.value)
            entry["wrote_as_asserted"] = True

        ancestor_canonical, ancestor_oid = cur_canonical, cur_oid
        cur_canonical = next_generation(cur_canonical)

    return {"dry_run": dry_run, "measured": measured, "plan": plan}

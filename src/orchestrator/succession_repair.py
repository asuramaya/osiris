"""THE FALSE-MINT-OVER-A-RESUMABLE-HEAD CENSUS (thread ef88e2bb's own aftermath, operator
ruling 7d6815bb: self-healing over manual bandaids). launch's own resume/refuse gate was
fixed at commit 954c591/cb08cf7 — `osiris launch <seat>` no longer mints a fresh mind when
its only failure is absent signed testimony. This module answers a DIFFERENT question:
did that class of stranger already land, and is a resumable head still sitting there
unused right now?

NOT A FOLD, DELIBERATELY NEVER WIRED INTO `find_agent_fold_candidates`'s merge tray: a
specimen here is not a duplicate label of one mind — it's a genuinely new, distinct
generation, correctly linked via `succeeded_from`, that simply should have been a
`--resume` instead of a fresh mint. `merge()` itself refuses an Agent same-lineage pair
outright ("succession's job, not a fold's") — trying to route this into resolve_fold_
candidate's `merged` decision would just hit that refusal. Resolving a finding here is a
judgment call each time (kill the live stranger and resume the older session? accept the
cost already paid and let the stranger stand?) — never mechanical, so this module stops at
detection: read-only, proposes nothing, writes nothing.

A RELATED, NARROWER GAP, NAMED BUT NOT CLOSED HERE: `office_claim`'s own `office-birth`
mint (mount.py/handshake.py) never consults `_lineage_resume_candidate` at all — a bare
`claude` run in a seat's office, outside `osiris launch` entirely, can still mint a
stranger over a resumable head with no gate in its way. Closing THAT is a separate,
larger-radius change (office_claim is the FIRST-EVER-mount path for every seat, not just
a launch-triggered one) and is out of this census's scope — it only reports what already
happened, from either door."""
from __future__ import annotations

from typing import Any

import asyncpg

from src.config.settings import Settings, get_settings


async def unresumed_heads(
    pool: asyncpg.Pool, *, settings: Settings | None = None,
) -> dict[str, Any]:
    """For every active, currently-held Seat: does its CURRENT head's own `minted_because`
    read `office-birth` (a fresh claim_name mint, never a resume), and does that
    generation's own immediate predecessor (via `succeeded_from`) still have a real,
    still-resumable session sitting unused? Reuses `_lineage_resume_candidate`, called
    with the PREDECESSOR as `holder` — exactly reconstructing the check `osiris
    launch`/`launch_seat` would have made at mint time, had anything asked.
    `materialize=False` (the wire-resume-to-store rewrite, ruling d161a156/d63b2ca6):
    this census only checks resumability, never emits a materialized transcript — this
    module's own docstring promise, "read-only, proposes nothing, writes nothing"."""
    from src.orchestrator.seats import seat_receipt
    from src.orchestrator.succession import succession_chain
    from src.orchestrator.trigger import _lineage_resume_candidate

    st = settings or get_settings()
    seats = await pool.fetch(
        "SELECT o.canonical AS seat_id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='anchor_cwd' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS anchor_cwd "
        "FROM objects o WHERE o.type='Seat' AND o.status='active' ORDER BY o.canonical")
    checked = 0
    found: list[dict[str, Any]] = []
    for row in seats:
        seat_id, anchor_cwd = row["seat_id"], row["anchor_cwd"]
        if not anchor_cwd:
            continue
        receipt = await seat_receipt(pool, seat_id)
        holder = (receipt or {}).get("holder")
        if not holder:
            continue
        checked += 1
        chain = await succession_chain(pool, holder)
        if len(chain) < 2 or chain[0]["minted_because"] != "office-birth":
            continue
        predecessor = chain[1]
        if not predecessor["wrote_anything"] or not predecessor["session"]:
            continue
        # materialize=False: this module's own docstring promises "read-only, proposes
        # nothing, writes nothing" — a census must never emit a materialized transcript.
        candidate = await _lineage_resume_candidate(
            pool, predecessor["agent_id"], st, repo=anchor_cwd, seat_id=seat_id,
            materialize=False)
        if isinstance(candidate, tuple):
            resume, log = candidate
            found.append({
                "seat": seat_id, "stranger": holder,
                "unresumed_head": predecessor["agent_id"],
                "resumable_session": resume[0],
                "detail": log[-1] if log else None,
            })
    return {"checked": checked, "found": found,
            "note": "READ-ONLY — a finding here is a judgment call, never a mechanical "
                    "fold; nothing here writes or resolves anything"}

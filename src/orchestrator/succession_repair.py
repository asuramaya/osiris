"""Census for the false-mint-over-a-resumable-head case: a self-healing detection pass
rather than a manual check. launch's own resume/refuse gate was fixed at commit
954c591/cb08cf7: `osiris launch <seat>` no longer mints a fresh agent when its only
failure is absent signed testimony. This module answers a different question: did that
class of problem already occur, and is a resumable head still sitting there unused right
now?

Not a fold, and deliberately never wired into `find_agent_fold_candidates`'s merge tray: a
specimen here is not a duplicate label of one agent, it's a genuinely new, distinct
generation, correctly linked via `succeeded_from`, that simply should have been a
`--resume` instead of a fresh mint. `merge()` itself refuses an Agent same-lineage pair
outright ("succession's job, not a fold's"), so routing this into resolve_fold_candidate's
`merged` decision would just hit that refusal. Resolving a finding here is a judgment call
each time (stop the live duplicate and resume the older session, or accept the cost
already paid and let the duplicate stand?), never mechanical, so this module stops at
detection: read-only, proposes nothing, writes nothing.

A related, narrower gap, named but not closed here: `office_claim`'s own `office-birth`
mint (mount.py/handshake.py) never consults `_lineage_resume_candidate` at all. A bare
`claude` run in a seat's working directory, outside `osiris launch` entirely, can still
mint a duplicate agent over a resumable head with no gate in its way. Closing that is a
separate, larger-radius change (office_claim is the first-ever-mount path for every seat,
not just a launch-triggered one) and is out of this census's scope; it only reports what
already happened, however it happened."""
from __future__ import annotations

from typing import Any

import asyncpg

from src.config.settings import Settings, get_settings


async def unresumed_heads(
    pool: asyncpg.Pool, *, settings: Settings | None = None,
) -> dict[str, Any]:
    """For every active, currently-held seat: does its current head's own `minted_because`
    read `office-birth` (a fresh claim_name mint, never a resume), and does that
    generation's own immediate predecessor (via `succeeded_from`) still have a real,
    still-resumable session sitting unused? Reuses `_lineage_resume_candidate`, called
    with the predecessor as `holder`, exactly reconstructing the check `osiris
    launch`/`launch_seat` would have made at mint time, had anything asked.
    `materialize=False` (the wire-resume-to-store rewrite): this census only checks
    resumability, never emits a materialized transcript, matching this module's own
    promise that it is read-only and proposes or writes nothing."""
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
        # materialize=False: this module's own docstring promises read-only, proposes
        # nothing, writes nothing. A census must never emit a materialized transcript.
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

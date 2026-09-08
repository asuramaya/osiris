"""GHOST HOUSE-STAMP RETIREMENT (thread a732e331 clause 3, wave 7 dispatch msg 8079/8090):
classification_laws_heartbeat's own third sibling sweep — SoftwareProject junk
(project_hygiene.py) and Thread classification (migration_0060.py) both got one; a Seat's
own pre-house-optional GHOST stamp is the third population left standing.

derive_house's own `_is_ghost_house` (seats.py, Khnum's bb1cdc2) already makes a ghost stamp
READ AS EMPTY at derivation time -- nothing here is load-bearing for correctness. This sweep
is the WRITE-TIME half clause 3 explicitly asked for: a receipt naming each retired seat, so
the graph itself stops carrying a pre-house-optional artifact forward forever rather than
merely working around it on every read.

POPULATION: an active, MANAGED Seat (has a real `manager_of_seat`, matching the ghost
clause's own "the ghost clause only ever protects the managed-seat anchor check, never a
head's own authoritative declaration") whose current `house` assertion is non-empty and
equals one of its own `charter_of` (governed project) names, case-insensitive -- the exact
`_is_ghost_house` predicate, reused verbatim rather than re-derived.

RETIRE, NEVER DELETE: `retire_assertion` (the sanctioned cross-source supersede door, thread
52911d2a) supersedes the ghost `house` row with an empty string -- falsy under every existing
`if house:` read in this codebase (derive_house, _own_house_stamp's callers), the same
"empty" derive_house already treats a ghost as, now durable instead of read-time-patched. A
head's own matching stamp (the ordinary, legitimate case) and a managed seat's real,
differing house are both left untouched. Idempotent: a seat whose current `house` is already
empty never matches the population query again."""
from __future__ import annotations

from typing import Any

import asyncpg

from src.actions.core import Actions

MIGRATION_SOURCE = "hygiene:ghost_house_sweep"
_BECAUSE = "ghost house-stamp retirement (thread a732e331 clause 3): pre-house-optional stamp"

_MANAGED_SEATS_WITH_HOUSE_SQL = """
    SELECT o.canonical AS seat, a.id AS assertion_id, a.value #>> '{}' AS house
    FROM objects o
    JOIN current_assertions a ON a.object_id=o.id AND a.name='house'
    WHERE o.type='Seat' AND o.status='active'
"""


async def plan_ghost_house_sweep(pool: asyncpg.Pool) -> dict[str, Any]:
    """DRY RUN -- never writes. `_is_ghost_house`/`manager_of_seat` reused verbatim from
    seats.py, never a second copy of the ghost predicate."""
    from src.orchestrator.seats import _is_ghost_house, manager_of_seat

    rows = await pool.fetch(_MANAGED_SEATS_WITH_HOUSE_SQL)
    to_retire: list[dict[str, Any]] = []
    for row in rows:
        house = row["house"]
        if not house:
            continue
        seat_id = row["seat"]
        if await manager_of_seat(pool, seat_id) is None:
            continue  # a head's own matching stamp is the ordinary, legitimate case
        if await _is_ghost_house(pool, seat_id, house):
            to_retire.append({
                "seat": seat_id, "assertion_id": row["assertion_id"], "house": house,
            })
    return {"to_retire": to_retire, "seats_scanned": len(rows)}


async def apply_ghost_house_sweep(
    actions: Actions, *, actor: str = MIGRATION_SOURCE,
) -> dict[str, Any]:
    """Applies `plan_ghost_house_sweep`'s plan through the sanctioned `retire_assertion`
    door -- never a hand-written supersede. A row that clears this sweep's own population
    query but still fails there is reported, not raised, so one stale row never sinks the
    whole sweep."""
    from src.orchestrator.retirement import retire_assertion

    plan = await plan_ghost_house_sweep(actions.pool)
    retired: list[str] = []
    refused: list[dict[str, str]] = []
    for entry in plan["to_retire"]:
        result = await retire_assertion(
            actions, ref=entry["seat"], name="house", superseded_id=entry["assertion_id"],
            value="", because=_BECAUSE, actor=actor)
        if "error" in result:
            refused.append({"seat": entry["seat"], "error": result["error"]})
        else:
            retired.append(entry["seat"])
    return {"retired": retired, "refused": refused, "seats_scanned": plan["seats_scanned"]}

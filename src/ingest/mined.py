"""Shared guards + reconciliation for the mined-memory miners (decisions / threads).

Mining prose is rough by nature, and the 2026-07 live audit showed exactly how it rots
trust: mid-sentence fragments (`anchor), app start gated on…`), commits that DOCUMENT the
miner's own markers surfacing as threads, and stale rows from older miner versions living
forever because a re-mine only ever ADDED. Three shared defenses live here:

* `well_bounded` — a capture that starts or ends mid-sentence betrays itself with
  unbalanced delimiters (a `)` with no opener, an odd number of quotes). Cheap, and it
  rejects the fragments a lowercase-only guard can't (`Leon" vs "Daniel Leon") render…`
  starts uppercase).
* `unquoted` — a marker inside quotes/backticks is someone *talking about* the marker,
  not using it. Markers must match the text with quoted spans removed.
* `reconcile_mined` — re-mining must HEAL, not just add: mined objects the fresh mine no
  longer produces are archived via the event-sourced `Actions.set_status` (reversible),
  and objects the miner itself archived are resurrected if the text comes back. A human
  archive is never overridden (the last archive event's actor must be the miner).
"""

from __future__ import annotations

import re
import uuid
from typing import Any

from src.actions.core import Actions

# spans whose content is MENTIONED, not asserted: "…", “…”, `…`. Single quotes are left
# alone (apostrophes in possessives would pair up and eat real text between them).
_QUOTED = re.compile(r'"[^"]*"|“[^”]*”|`[^`]*`')


def well_bounded(s: str) -> bool:
    """True when the fragment starts/ends at a plausible sentence boundary.

    A split artifact carries scars: a closing paren before any opener (``anchor), app
    start…``), unequal paren counts in either direction (``…phrase "THE WALL" (a real`` —
    cut off mid-parenthetical), or an odd number of double quotes (``Leon" vs "Daniel
    Leon")`` — the opener lives in the sentence the split ate. Deliberately strict: a
    bare list token ("2) the satellite") also reads as unbalanced and is rejected —
    tight stays trustworthy.
    """
    if not s or s[0] in ")]}":
        return False
    if s.count("(") != s.count(")") or s.count("[") != s.count("]"):
        return False
    if s.count('"') % 2 or s.count("`") % 2 or s.count("“") != s.count("”"):
        return False
    first_close = s.find(")")
    if first_close != -1 and s.find("(") > first_close:  # find('(') == -1 implies count mismatch
        return False
    return True


def unquoted(s: str) -> str:
    """The fragment with quoted/backticked spans removed — the text actually ASSERTED.

    A marker that only occurs inside quotes ("only the exact phrase \"THE WALL\"…") is
    documentation about the miner, and must not count as a marker hit.
    """
    return _QUOTED.sub(" ", s)


async def reconcile_mined(
    actions: Actions,
    *,
    object_type: str,
    prefix: str,
    produced: set[str],
    source_id: str,
    case_id: uuid.UUID | None = None,
) -> dict[str, int]:
    """Archive mined objects the fresh mine no longer produces; resurrect ones it does.

    Scope is strictly the miner's own output: the canonical prefix (``decision:`` /
    ``thread:``) AND at least one assertion from the miner's source. Archival is the
    event-sourced ``set_status`` (an `archive` object_event — reversible, auditable).
    Resurrection only fires when the LAST archive event was the miner's own actor, so a
    human's deliberate archive is never undone by a cron re-mine.
    """
    pool = actions.pool
    rows = await pool.fetch(
        "SELECT o.id, o.canonical, o.status FROM objects o "
        "WHERE o.type=$1 AND o.canonical LIKE $2 || '%' "
        "  AND o.status IN ('active','archived') "
        "  AND EXISTS (SELECT 1 FROM assertions a "
        "              WHERE a.object_id=o.id AND a.source_id=$3)",
        object_type,
        prefix,
        source_id,
    )
    archived = resurrected = 0
    for r in rows:
        oid: Any = r["id"]
        if r["canonical"] in produced:
            if r["status"] != "archived":
                continue
            last_archiver = await pool.fetchval(
                "SELECT actor FROM object_events "
                "WHERE object_id=$1 AND event_type='archive' ORDER BY id DESC LIMIT 1",
                oid,
            )
            if last_archiver == source_id:
                await actions.set_status(
                    oid, "active", "re-mined: the source text produces this again",
                    source_id, case_id,
                )
                resurrected += 1
        elif r["status"] == "active":
            await actions.set_status(
                oid, "archived",
                "stale mined object — a re-mine of the commit record no longer produces it",
                source_id, case_id,
            )
            archived += 1
    return {"archived": archived, "resurrected": resurrected}

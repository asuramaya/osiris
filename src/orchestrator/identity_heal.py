"""Seat-identity self-healing (fe8ec7ff mechanism 3, operator ruling df646654: SELF-HEALING
OVER MANUAL CLEANUP). #157's own diagnosis (decision 6e2ea596/7a46db36) found the disease:
assert_property's supersession is SAME-SOURCE only, by design — a peer's correction from a
DIFFERENT source never retires an older contradicting value, so a stale row sits beside the
winning one forever, both "current" by current_assertions' own definition, outvoted but never
invalidated. The repair used to be a human walking rows by hand and staging retire_assertion
calls that needed the operator's personal sign-off every time (decision 4fdd419e, four calls
still staged when this was built). The operator's own standard: design as though no agent can
ever escalate to Thoth or the operator for this class of problem.

SCOPED DELIBERATELY NARROW to two properties, both single-valued by nature — a Seat's `house`
and an Agent's `project` — NEVER generalised to every property (Khnum's n=4 qualifier, 6e2ea596:
newest-wins was empirically true for that population, not a law). #102's `agreement` marks stay
untouched for everything else; a genuinely multi-valued or corroborating property is never a
target here.

Reuses retire_assertion for every write — no second supersession path. `heal_contradicting_
property` is the one place a "contradiction" is even defined (>1 current row, DIFFERENT
values) — same-value multi-source rows (real corroboration, not a contradiction) are left
alone, exactly as #102's agreement marks are."""
from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.orchestrator.retirement import retire_assertion

# Deliberately just these two — see module docstring. Never read as a general allowlist to
# extend without a fresh ruling: house/project were named BY THE OPERATOR, not inferred.
SEAT_IDENTITY_PROPS = ("house", "project")

_STALE_SEAT_DETECTION_CAP = 25


async def detect_possibly_stale_seats(
    pool: asyncpg.Pool, old_name: str, *, cap: int = _STALE_SEAT_DETECTION_CAP,
) -> dict[str, Any]:
    """DETECTION-ONLY, never a write (Thoth dispatch 6484/6493, the dtfb specimen f5d5473b):
    fold_project and rename_project already carry every graph EDGE correctly (charter
    included) and agent_mounts.project, but neither ever re-asserts a Seat's OWN `house` or
    `anchor_cwd` under the new name — not because the self-service repair tools don't work
    (`reconcile_seat_identity`/`_third_party` in this module, `rebind_seat` for a path),
    but because nothing ever tells a seat it needs them (Practice a5938da2's shape:
    capability exists, unadopted). This names the hits AND the verb that fixes each, in the
    same line, so the nudge and the fix stay one hop apart — called from fold_project's and
    rename_project's own receipts, adding a key, changing nothing else.

    A HEURISTIC, NEVER A VERDICT: house is matched EXACTLY against `old_name`; anchor_cwd is
    matched by its PATH'S OWN BASENAME (Path(value).name), not a raw substring — a bare
    substring match on a common old name ("core", "seats") would flag half the fleet. Even
    basename matching still over-matches on a common word; `note` says so in every response
    rather than let a caller read this list as ground truth. Capped at `cap` hits per field
    (a name common enough to blow the cap degrades to `truncated: true`, not a flood).

    THE .osiris PIN FILE IS DELIBERATELY UNCHECKED: rename_project's own docstring already
    calls itself "a graph-only verb" — reading another seat's pin off disk from inside this
    call would be a filesystem read across someone else's tree, on the hot path of a live,
    frequently-used verb. `note` says the pin is unchecked; a confessed blind spot beats a
    slow or fragile one.

    NEVER RAISES: any failure (a bad connection, a malformed row) degrades to
    `{"checked": False, "error": ...}` rather than aborting the fold/rename that called it —
    the #107 lesson (settle() whole-batch-aborting on one bad repo) applied to an advisory
    field, not a batch. An empty `old_name` is the same shape, not an exception."""
    try:
        bare = (old_name or "").removeprefix("repo:").strip()
        if not bare:
            return {"checked": False, "reason": "empty old_name — nothing to match"}
        hits: list[dict[str, str]] = []
        house_rows = await pool.fetch(
            "SELECT s.canonical AS seat, a.value #>> '{}' AS value "
            "FROM current_assertions a JOIN objects s ON s.id = a.object_id "
            "WHERE s.type = 'Seat' AND a.name = 'house' AND a.value #>> '{}' = $1 "
            "ORDER BY s.canonical", bare)
        for r in house_rows:
            hits.append({"seat": r["seat"], "field": "house", "value": r["value"],
                        "fix": "reconcile_seat_identity (self) or "
                               "reconcile_seat_identity_third_party (another seat)"})
        anchor_rows = await pool.fetch(
            "SELECT s.canonical AS seat, a.value #>> '{}' AS value "
            "FROM current_assertions a JOIN objects s ON s.id = a.object_id "
            "WHERE s.type = 'Seat' AND a.name = 'anchor_cwd' ORDER BY s.canonical")
        for r in anchor_rows:
            value = r["value"] or ""
            if value and Path(value).name == bare:
                hits.append({"seat": r["seat"], "field": "anchor_cwd", "value": value,
                            "fix": "rebind_seat, once the correct path is confirmed"})
        return {
            "checked": True,
            "old_name": bare,
            "hits": hits[:cap],
            "truncated": len(hits) > cap,
            "note": "heuristic name/basename match against the old name, never a verdict "
                    "— a common old name over-matches; the .osiris pin file is NOT checked "
                    "(off-graph, unsafe to read from a hot path)",
        }
    except Exception as exc:
        return {"checked": False, "error": str(exc)}


async def heal_contradicting_property(
    actions: Actions, *, object_id: uuid.UUID, name: str, actor: str, reason: str | None = None,
) -> dict[str, Any]:
    """The one mechanism: read every CURRENT assertion of `name` on `object_id` (multi-
    source, since assert_property's own supersession never crosses sources), tie-break them
    the SAME way the read path already does (confidence DESC, observed_at DESC — the winner
    is never a new decision, only the existing rule made to actually stick), and retire every
    OTHER current row that names a DIFFERENT value. A row that already agrees with the winner
    (real multi-source corroboration) is left untouched — never retired for merely being a
    second source, only for being a WRONG one.

    Each retirement goes through retire_assertion unchanged — reversible (the loser's own
    assertion id is in the receipt), attributed to `actor`, `because` self-documenting so an
    audit never has to guess why a row went quiet. `reason`, when given (the third-party
    sibling's own mandatory `because`), rides into that same `because` text so a THIRD-PARTY
    correction's own stated justification is distinguishable in the audit trail from a plain
    self-heal's mechanical "newest-declared-wins" — never a second write, never a second
    field, the SAME retire_assertion call either way. Returns `healed: False` when 0 or 1
    current rows exist (nothing to reconcile) or every row already agrees (already healed)."""
    rows = await actions.pool.fetch(
        "SELECT id, value #>> '{}' AS value, source_id, observed_at "
        "FROM current_assertions WHERE object_id=$1 AND name=$2 "
        "ORDER BY confidence DESC, observed_at DESC", object_id, name)
    if len(rows) <= 1:
        return {"healed": False, "reason": "nothing to reconcile", "current": len(rows)}
    winner = rows[0]
    superseded: list[dict[str, Any]] = []
    for loser in rows[1:]:
        if loser["value"] == winner["value"]:
            continue  # corroboration, not a contradiction — never touched
        result = await retire_assertion(
            actions, ref=str(object_id), name=name, superseded_id=loser["id"],
            value=winner["value"], actor=actor,
            because=(
                f"self-heal (fe8ec7ff mechanism 3, ruling df646654): newest-declared-wins "
                f"on seat-identity property {name!r} — {winner['value']!r} "
                f"(source={winner['source_id']}, {winner['observed_at'].isoformat()}) "
                f"outvotes {loser['value']!r} (source={loser['source_id']}, "
                f"{loser['observed_at'].isoformat()})"
                + (f" — third-party correction: {reason}" if reason else
                   ", never sign-off-gated for this property class")
                + " — reversible via the retired assertion's own id"))
        if "error" in result:
            superseded.append({"id": loser["id"], "value": loser["value"],
                               "error": result["error"]})
        else:
            superseded.append({"id": loser["id"], "value": loser["value"],
                               "source": loser["source_id"]})
    if not superseded:
        return {"healed": False, "reason": "every current row already agrees",
                "current": len(rows), "value": winner["value"]}
    # RECEIPT HONESTY: a `superseded` entry can be an ERROR (retire_assertion refused —
    # e.g. "already superseded", the exact shape of a current_assertions/is_current
    # inconsistency this mechanism cannot itself repair) — `healed` must never read True
    # over a batch where every attempted write actually failed. A partial success (some
    # rows really retired, one refused) still reports healed=True; the per-row `error` key
    # is how a caller tells which rows actually moved.
    if all("error" in s for s in superseded):
        return {"healed": False, "reason": "every contradicting row refused retirement",
                "winner": winner["value"], "superseded": superseded}
    return {"healed": True, "winner": winner["value"], "superseded": superseded}


async def reconcile_seat_identity(
    actions: Actions, *, seat_id: str, agent_id: str | None, actor: str,
    reason: str | None = None,
) -> dict[str, Any]:
    """THE SELF-SERVICE VERB (fe8ec7ff mechanism 3b): any agent may run this for its OWN
    seat, no personal sign-off — this is what #157's four staged retire_assertion calls
    become, one call each, not four operator authorizations. Heals `house` on the Seat
    object and, when `agent_id` is given (the seat's current holder), `project` on that
    Agent object — the same two properties the operator named from two angles (#157/#161).

    Refuses LOUDLY on an unknown seat (never guesses); `agent_id=None` heals house alone
    (a caller reconciling a seat it does not currently hold an agent identity for, or a
    vacant seat with a stale house). `reason` is internal plumbing for the third-party
    sibling below (its own mandatory `because`) — the self-service caller never sets it."""
    seat_row = await actions.pool.fetchrow(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Seat' AND status='active'",
        seat_id)
    if seat_row is None:
        return {"error": f"no active seat matches {seat_id!r}"}
    healed: dict[str, Any] = {
        "house": await heal_contradicting_property(
            actions, object_id=seat_row["id"], name="house", actor=actor, reason=reason),
    }
    if agent_id is not None:
        agent_row = await actions.pool.fetchrow(
            "SELECT id FROM objects WHERE canonical=$1 AND type='Agent' AND status='active'",
            agent_id)
        if agent_row is not None:
            healed["project"] = await heal_contradicting_property(
                actions, object_id=agent_row["id"], name="project", actor=actor, reason=reason)
    return {"seat_id": seat_id, "agent_id": agent_id, "healed": healed}


async def reconcile_seat_identity_third_party(
    actions: Actions, *, seat_id: str, agent_id: str | None, because: str, actor: str,
) -> dict[str, Any]:
    """THE THIRD-PARTY SIBLING of reconcile_seat_identity — the gap named in decision
    f78b41c8: mechanism 3 shipped self-service-only, and #157's own population (four OTHER
    seats' stale rows) structurally cannot be reached by a verb that always resolves its
    target from the caller's own held seat. Mirrors resync_seat_house_third_party's own
    precedent exactly: NOT self-scoped — the target need not be the caller, on purpose (a
    coordinator correcting a seat that cannot correct itself, or simply hasn't taken its own
    next turn yet, is exactly the case this exists for) — and `because` is REQUIRED, same
    cause resync_seat_house_third_party refuses an empty reason for: a correction with no
    stated reason is the silent overwrite 719ed5b1 rules against, not a fix. Does NOT check
    caller authority beyond being mounted — same as correct_agent_house and resync_seat_
    house_third_party, callers are responsible for the authorization this docstring cannot
    enforce.

    OTHERWISE IDENTICAL to the self-service verb — same heal_contradicting_property
    mechanism, same two properties (house/project), same reversibility, same graph writes
    for the same row (the only difference is `because` riding into the retired rows' own
    audit trail, naming the third party's reason instead of the mechanical default)."""
    because = (because or "").strip()
    if not because:
        return {"error": "a correction with no reason is exactly the silent overwrite "
                         "719ed5b1 rules against — refusing"}
    return await reconcile_seat_identity(actions, seat_id=seat_id, agent_id=agent_id,
                                         actor=actor, reason=because)

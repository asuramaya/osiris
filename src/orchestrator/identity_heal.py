"""Seat-identity self-healing. Automated repair is preferred over manual cleanup. An earlier
diagnosis found the underlying problem: assertion supersession only applies within the same
source, by design. A peer's correction from a different source never retires an older
contradicting value, so a stale row sits beside the winning one forever, both "current" by
current_assertions' own definition, outvoted but never invalidated. The repair used to be a
human walking rows by hand and staging retire_assertion calls that needed manual sign-off
every time. The design goal here is that no agent should need to escalate for this class of
problem.

Scoped deliberately narrow to two properties, both single-valued by nature: a Seat's `house`
and an Agent's `project`. This is never generalised to every property; newest-wins was
empirically true for that population, not a general law. Corroboration marks used elsewhere
stay untouched for everything else; a genuinely multi-valued or corroborating property is
never a target here.

Reuses retire_assertion for every write, so there is no second supersession path.
`heal_contradicting_property` is the one place a "contradiction" is even defined (more than
one current row, with different values); same-value multi-source rows (real corroboration,
not a contradiction) are left alone, the same way corroboration marks are handled elsewhere."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.orchestrator.retirement import retire_assertion
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_ANCHOR_EC = EvidenceClass.SELF_DECLARED.value
_ANCHOR_CONF = confidence_for(EvidenceClass.SELF_DECLARED)

# Deliberately just these two, see module docstring. Never read as a general allowlist to
# extend without a fresh decision: house/project were named explicitly, not inferred.
SEAT_IDENTITY_PROPS = ("house", "project")

_STALE_SEAT_DETECTION_CAP = 25


async def detect_possibly_stale_seats(
    pool: asyncpg.Pool, old_name: str, *, cap: int = _STALE_SEAT_DETECTION_CAP,
) -> dict[str, Any]:
    """Detection-only, never a write. fold_project and rename_project already carry every
    graph edge correctly (charter included) and agent_mounts.project, but neither ever
    re-asserts a Seat's own `house` or `anchor_cwd` under the new name. This isn't because
    the self-service repair tools don't work (`reconcile_seat_identity`/`_third_party` in
    this module, `rebind_seat` for a path), but because nothing ever tells a seat it needs
    them: the capability exists but goes unadopted. This function names the hits and the
    action that fixes each, in the same line, so the nudge and the fix stay one step apart.
    It is called from fold_project's and rename_project's own results, adding a key and
    changing nothing else.

    A heuristic, never a verdict: house is matched exactly against `old_name`; anchor_cwd is
    matched by its path's own basename (Path(value).name), not a raw substring. A bare
    substring match on a common old name ("core", "seats") would flag a large fraction of
    the fleet. Even basename matching still over-matches on a common word; `note` says so in
    every response rather than let a caller read this list as ground truth. Capped at `cap`
    hits per field (a name common enough to blow the cap degrades to `truncated: true`, not
    a flood).

    The .osiris pin file is deliberately unchecked: rename_project is documented as a
    graph-only action, and reading another seat's pin off disk from inside this call would
    be a filesystem read across someone else's tree, on the primary path of a frequently
    used call. `note` says the pin is unchecked; a confessed blind spot beats a slow or
    fragile one.

    Never raises: any failure (a bad connection, a malformed row) degrades to
    `{"checked": False, "error": ...}` rather than aborting the fold/rename that called it,
    following the lesson that a whole-batch abort on one bad repo is worse than an advisory
    field failing gracefully. An empty `old_name` is handled the same way, not as an
    exception."""
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
    """The one mechanism: read every current assertion of `name` on `object_id` (multiple
    sources, since supersession never crosses sources on write), tie-break them the same way
    the read path already does (confidence descending, then observed_at descending: the
    winner is never a new decision, only the existing rule made to actually stick), and
    retire every other current row that names a different value. A row that already agrees
    with the winner (real multi-source corroboration) is left untouched, never retired for
    merely being a second source, only for being a wrong one.

    Each retirement goes through retire_assertion unchanged: reversible (the loser's own
    assertion id is in the result), attributed to `actor`, with `because` self-documenting so
    an audit never has to guess why a row went quiet. `reason`, when given (the third-party
    sibling's own mandatory `because`), rides into that same `because` text so a third-party
    correction's own stated justification is distinguishable in the audit trail from a plain
    self-heal's mechanical "newest-declared-wins", with no second write or second field, the
    same retire_assertion call either way. Returns `healed: False` when zero or one current
    rows exist (nothing to reconcile) or every row already agrees (already healed)."""
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
            continue  # corroboration, not a contradiction, never touched
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
    # Result honesty: a `superseded` entry can be an error (retire_assertion refused, for
    # example "already superseded", the exact shape of a current_assertions/is_current
    # inconsistency this mechanism cannot itself repair). `healed` must never read True over
    # a batch where every attempted write actually failed. A partial success (some rows
    # really retired, one refused) still reports healed=True; the per-row `error` key
    # is how a caller tells which rows actually moved.
    if all("error" in s for s in superseded):
        return {"healed": False, "reason": "every contradicting row refused retirement",
                "winner": winner["value"], "superseded": superseded}
    return {"healed": True, "winner": winner["value"], "superseded": superseded}


async def reconcile_seat_identity(
    actions: Actions, *, seat_id: str, agent_id: str | None, actor: str,
    reason: str | None = None,
) -> dict[str, Any]:
    """The self-service action: any agent may run this for its own seat, with no manual
    sign-off required. This replaces what used to be several staged retire_assertion calls
    each needing separate authorization, with one call each. Heals `house` on the Seat
    object and, when `agent_id` is given (the seat's current holder), `project` on that
    Agent object, the same two properties identified as needing this treatment.

    Refuses loudly on an unknown seat (never guesses); `agent_id=None` heals house alone
    (a caller reconciling a seat it does not currently hold an agent identity for, or a
    vacant seat with a stale house). `reason` is internal plumbing for the third-party
    sibling below (its own mandatory `because`); the self-service caller never sets it."""
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
    """The third-party sibling of reconcile_seat_identity: the initial self-service-only
    version left a gap, since a population of other seats' stale rows structurally cannot be
    reached by an action that always resolves its target from the caller's own held seat.
    Mirrors an existing precedent for third-party house correction exactly: not self-scoped,
    the target need not be the caller, on purpose (a coordinator correcting a seat that
    cannot correct itself, or simply hasn't taken its own next turn yet, is exactly the case
    this exists for), and `because` is required, for the same reason that precedent refuses
    an empty reason: a correction with no stated reason is a silent overwrite, not a fix.
    Does not check caller authority beyond being mounted, the same as other third-party
    correction actions; callers are responsible for the authorization this docstring cannot
    enforce.

    Otherwise identical to the self-service action: same heal_contradicting_property
    mechanism, same two properties (house/project), same reversibility, same graph writes
    for the same row. The only difference is `because` riding into the retired rows' own
    audit trail, naming the third party's reason instead of the mechanical default."""
    because = (because or "").strip()
    if not because:
        return {"error": "a correction with no reason is exactly the silent overwrite "
                         "719ed5b1 rules against — refusing"}
    return await reconcile_seat_identity(actions, seat_id=seat_id, agent_id=agent_id,
                                         actor=actor, reason=because)


def _office_dir_exists(target: str) -> bool:
    """A plain sync helper (ASYNC240: file I/O stays out of async function bodies, the same
    convention `trigger._tree_exists` follows). This healer re-asserts identity at an
    office that already exists; it never provisions one."""
    return Path(target).is_dir()


async def detect_anchor_invariant_violations(actions: Actions) -> dict[str, list[dict[str, Any]]]:
    """The anchor invariant's own detector: read-only, wired in here so a caller need not
    remember to run a standalone script separately. Reports, for every active Seat: (a) a
    current `anchor_cwd` outside the office root, (b) more than one current `anchor_cwd` row
    at all (the supersession-leak shape). Kept as separate axes on purpose, so the surface
    reports that these disagree rather than silently collapsing them. A seat can be
    outside-root with only one current row (a clean, if invariant-violating, deliberate
    anchor) or multi-row while still resolving inside the root (corrupted-but-lucky).
    `heal_seat_anchor`'s own target population is the seats hitting both axes at once,
    computed by the caller from this same result, never a third axis duplicated here."""
    from src.orchestrator.offices import _default_office_root

    root = str(_default_office_root())
    rows = await actions.pool.fetch(
        "SELECT o.canonical AS seat, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS handle, "
        " a.id, a.value #>> '{}' AS v, a.source_id, a.observed_at, a.confidence "
        "FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE a.name='anchor_cwd' AND o.type='Seat' AND o.status='active' "
        "ORDER BY o.canonical, a.observed_at"
    )
    by_seat: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by_seat.setdefault(r["seat"], []).append(dict(r))

    outside_root: list[dict[str, Any]] = []
    multi_current: list[dict[str, Any]] = []
    for seat, seat_rows in by_seat.items():
        handle = seat_rows[0]["handle"]
        if len(seat_rows) > 1:
            multi_current.append({
                "seat": seat, "handle": handle,
                "rows": [{"value": r["v"], "source_id": r["source_id"],
                          "observed_at": r["observed_at"].isoformat()} for r in seat_rows],
            })
        for r in seat_rows:
            v = r["v"]
            if v and not (v == root or v.startswith(root.rstrip("/") + "/")):
                outside_root.append({
                    "seat": seat, "handle": handle, "value": v,
                    "source_id": r["source_id"], "observed_at": r["observed_at"].isoformat(),
                })

    return {"outside_root": outside_root, "multi_current": multi_current}


# --- THE ANCHOR INVARIANT -------------------------------------------------------------
#
# `heal_contradicting_property`'s own tie-break (confidence descending, then observed_at
# descending, so the newest wins) is exactly wrong for `anchor_cwd`: the corrupting value is
# always the newer one (a self-invoked rebind at the moment a session's cwd moved), and the
# correct office value is always the older, mint-time one. So this is a separate mechanism,
# not an extension of SEAT_IDENTITY_PROPS: the winner here is never "whoever wrote last," it
# is the one value the invariant itself computes: `<office_root>/<handle>`. Same shape as
# reconcile_seat_identity otherwise: a self-service action, a third-party sibling, reversible
# writes only (assert_singular_property never deletes, a superseded row stays readable).


async def heal_seat_anchor(
    actions: Actions, *, seat_id: str, actor: str, because: str | None = None,
    office_root: Path | None = None, dry_run: bool = True,
) -> dict[str, Any]:
    """Assert the invariant anchor (`<office_root>/<handle>`) as the seat's sole current
    `anchor_cwd`, via `Actions.assert_singular_property`. One call collapses every stray
    current row regardless of source, whether there are zero (no office anchor at all, in
    which case this call writes one), one (the common corrupted case: the correct office
    value already current beside a rogue one), or more.

    Refuses rather than guesses: no handle on record (nothing to derive an office path
    from), or the computed office directory does not exist on disk (this healer asserts
    identity at an office that already exists; scaffolding one is `establish_office`'s job,
    never silently done here). Returns `healed: False, reason: "already correct"` when the
    sole current value already matches the invariant, never a no-op write.

    `dry_run=True` is the default: the result always includes `current_before` and
    `target`; only when `dry_run=False` does the write actually happen. `because`, when
    given (the third-party sibling's own mandatory reason), rides into the audit trail the
    same way `reconcile_seat_identity`'s `reason` does."""
    from src.orchestrator.offices import seat_office_target

    seat_row = await actions.pool.fetchrow(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Seat' AND status='active'",
        seat_id)
    if seat_row is None:
        return {"error": f"no active seat matches {seat_id!r}"}
    target = await seat_office_target(actions.pool, seat_id, office_root=office_root)
    if target is None:
        return {"error": f"{seat_id} has no handle on record — cannot derive an office path"}
    handle = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        seat_row["id"])
    if not _office_dir_exists(target):
        return {"error": f"{target!r} does not exist on disk — this healer re-asserts "
                         "identity at an office that already exists; establish_office "
                         "scaffolds one, this verb never does"}
    current = await actions.pool.fetch(
        "SELECT value #>> '{}' AS v, source_id, observed_at FROM current_assertions "
        "WHERE object_id=$1 AND name='anchor_cwd' ORDER BY observed_at", seat_row["id"])
    values = {r["v"] for r in current}
    receipt: dict[str, Any] = {
        "seat_id": seat_id, "handle": handle, "target": target,
        "current_before": [
            {"value": r["v"], "source_id": r["source_id"],
             "observed_at": r["observed_at"].isoformat()}
            for r in current],
    }
    if values == {target}:
        receipt["healed"] = False
        receipt["reason"] = "already correct"
        return receipt
    receipt["dry_run"] = dry_run
    if dry_run:
        return receipt
    now = datetime.now(UTC)
    reason_text = (
        "anchor invariant repair (ruling 23771416): asserting the office as the sole "
        f"current anchor_cwd, collapsing {sorted(values - {target})!r}"
        + (f" — {because}" if because else ""))
    await actions.assert_singular_property(
        seat_row["id"], "anchor_cwd", target, actor, now, _ANCHOR_CONF,
        evidence_class=_ANCHOR_EC, because=reason_text)
    receipt["healed"] = True
    return receipt


async def heal_seat_anchor_third_party(
    actions: Actions, *, seat_id: str, because: str, actor: str,
    office_root: Path | None = None, dry_run: bool = True,
) -> dict[str, Any]:
    """The third-party sibling of `heal_seat_anchor`: the same mandatory-`because` rule as
    `reconcile_seat_identity_third_party`. A correction with no stated reason is a silent
    overwrite, not a fix."""
    because = (because or "").strip()
    if not because:
        return {"error": "a correction with no reason is exactly the silent overwrite "
                         "719ed5b1 rules against — refusing"}
    return await heal_seat_anchor(actions, seat_id=seat_id, actor=actor, because=because,
                                  office_root=office_root, dry_run=dry_run)

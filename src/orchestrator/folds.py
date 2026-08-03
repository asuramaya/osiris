"""AGENT FOLDS — the reconciliation primitive (operator directive 2026-07-16, thread
b975851b: "there needs to be a primitive for merging and resolving the mess, append only
... the scattered nodes need to be reconciled into their live/current lane").

The kernel HAS an identity merge — `Actions.merge_objects`: an append-only 'merge' event,
the `status='merged'` + `merged_into` projection, a `same_as` link, resolve-on-read.
It was built for the entity commons (Person/Company dedup) and never allowed near Agents:
constitution #1's "never AUTO-merge" had culturally over-extended into "never merge".
A fold is the review-gated form the constitution actually prescribes.

`fold_agent` wraps the kernel merge with the AGENT ESTATE — the three routing surfaces a
mind owns that a Person does not: unread mail, durable mount rows, thread ownership. The
same shape as succession's estate transfer (`mint_heir`, agents.py), generalized from
"death" to "recognition": a fold says two labels were always one mind, so the living
label inherits, and nothing is deleted or rewritten — the dupe's words stay stamped with
the dupe's id, and provenance resolves at read time through `merged_into`.

REVIEW-GATED, ALWAYS: a fold executes only on the operator's word or an approved
merge_candidate, and `evidence` is mandatory — a fold without citations is an auto-merge
wearing a signature.
"""
from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.orchestrator.seats import _OPERATOR_ACTORS

_log = logging.getLogger("osiris.folds")

# THE SCHEDULED REAPER'S OWN NAME (fleet_reconcile.py's reconcile_scheduled_tick) — the
# ONE non-operator actor fold_agent trusts, because that tick is ALREADY gated separately
# by osiris_fleet_reconcile_enabled (a distinct, operator-flipped signature). Defined here
# (not in fleet_reconcile.py) so the authority check and the identity it recognizes live
# in the same file; fleet_reconcile.py imports this constant rather than repeating the
# literal, so the two can never drift apart.
_SANCTIONED_AUTO_FOLD_ACTOR = "cron:fleet_reconcile_heartbeat"


async def living_head(pool: asyncpg.Pool, agent_id: str) -> str:
    """The lineage's living head — where a folded estate LANDS. THE GRAPH DECIDES
    (auto-heal, operator 2026-07-17: 'the folds have to auto heal'): resolve the label
    through the merge chain, then walk succeeded_by to the last ACTIVE generation — a
    walk that crosses REBASED lineages wherever a succession was recorded (Ra XV →
    XVI), so a tray row citing a dead generation still lands its estate on the one who
    answers to the name today. The registry only breaks the tie for a label with no
    succession record (an anonymous base), and its answer may never REGRESS to an
    older generation than the graph's. Estate must never land on a dead generation:
    succession's own transfer (mint_heir) would just have to move it again."""
    from src.orchestrator.agents import _generation, lineage_head

    canon = await canonical_agent(pool, agent_id)
    head = await lineage_head(pool, canon)
    if head != canon:
        return head
    base = _generation(canon)[0]
    row = await pool.fetchval(
        "SELECT agent_id FROM agent_mounts WHERE agent_id=$1 OR agent_id LIKE $1 || '-%' "
        "ORDER BY last_seen DESC NULLS LAST LIMIT 1", base)
    if row and _generation(str(row))[1] > _generation(canon)[1]:
        return str(row)
    return canon


async def wakeable_identity(pool: asyncpg.Pool, agent_id: str) -> str | None:
    """WAKE's own question — 'which OS session can be resumed' — answered independently of
    `living_head`'s DELIVERY question ('whose mailbox owns this name'). A succession is a
    DECLARED act (mint_heir) and living_head trusts it unconditionally, exactly as delivery
    should (ruling 1db1ff41: declared beats derived) — but a successor that was declared and
    never actually mounted has no OS session behind it, and a wake path that walks
    living_head's own answer strands mail in a live body's own inbox (thread 28842543: the
    mailbox honored an explicit live id, the wake still resolved through a never-mounted
    lineage head, and reported "has never mounted" beside a receipt naming that SAME live
    body's fresh last_seen). The most recently mounted id anywhere in `agent_id`'s lineage —
    the same auto-heal query `living_head` itself falls back to for an unsucceeded base
    (below), generalized here to fire even when a succession WAS declared, because wake cares
    which body can answer, not which name is now correct. None only when nothing in the
    whole lineage has ever mounted."""
    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    row = await pool.fetchval(
        "SELECT agent_id FROM agent_mounts WHERE agent_id=$1 OR agent_id LIKE $1 || '-%' "
        "ORDER BY last_seen DESC NULLS LAST LIMIT 1", base)
    return str(row) if row else None


async def canonical_agent(pool: asyncpg.Pool, agent_id: str) -> str:
    """Resolve an agent LABEL through the merged_into chain to its living label — the
    read-time half of a fold. A never-folded (or unknown) label returns itself."""
    current = agent_id
    for _ in range(10):  # chain guard: folds of folds terminate fast or not at all
        nxt = await pool.fetchval(
            "SELECT w.canonical FROM objects o JOIN objects w ON w.id = o.merged_into "
            "WHERE o.canonical = $1 AND o.status = 'merged'", current)
        if nxt is None:
            return current
        current = str(nxt)
    return current


async def _reversible_moved_links(
    pool: asyncpg.Pool, *, dupe_id: uuid.UUID, into_id: uuid.UUID, link_type: str,
    from_dupe: bool,
) -> list[dict[str, Any]]:
    """MERGE/UNMERGE's shared estate-reversal probe (ruling 31c02dca, built for
    unfold_seat/unfold_project's parity with unfold_agent): every OTHER object whose live
    `link_type` edge now points at `into` but which ALSO carries a now-invalid edge of the
    same type that once pointed at `dupe` — the exact trail an event-sourced
    (`invalidate_link` + `create_link`) estate move leaves behind, safe to auto-reverse
    because nothing has touched it since. Generalizes `unfold_agent`'s own thread-ownership
    check (`assert_property`'s history) from PROPERTIES to LINKS: fold_seat's holders and
    managed_by edges, fold_project's estate links. `from_dupe=True` reads edges pointing
    INTO dupe/into (holds, in_repo, works_in, governs, informs — the other object is the
    link's `from_id`); `from_dupe=False` reads edges pointing OUT of dupe/into (the
    managed_by direction where dupe/into is itself the manager — the other object is the
    link's `to_id`). A raw-UPDATE-moved estate item (mail, agent_mounts) leaves no such
    trail and is never found here — the caller reports those as `estate_unreturnable`
    instead, exactly as `unfold_agent` already does for its own mail/mounts."""
    if from_dupe:
        rows = await pool.fetch(
            "SELECT DISTINCT f.id AS fid, f.canonical AS label "
            "FROM links l JOIN objects f ON f.id=l.from_id "
            "WHERE l.to_id=$1 AND l.type=$3 "
            "AND (l.valid_until IS NULL OR l.valid_until > now()) "
            "AND EXISTS (SELECT 1 FROM links l2 WHERE l2.from_id=f.id AND l2.to_id=$2 "
            "AND l2.type=$3)",
            into_id, dupe_id, link_type)
    else:
        rows = await pool.fetch(
            "SELECT DISTINCT t.id AS fid, t.canonical AS label "
            "FROM links l JOIN objects t ON t.id=l.to_id "
            "WHERE l.from_id=$1 AND l.type=$3 "
            "AND (l.valid_until IS NULL OR l.valid_until > now()) "
            "AND EXISTS (SELECT 1 FROM links l2 WHERE l2.to_id=t.id AND l2.from_id=$2 "
            "AND l2.type=$3)",
            into_id, dupe_id, link_type)
    return [dict(r) for r in rows]


async def _move_agent_estate(
    actions: Actions, dupe: str, into: str, actor: str,
) -> dict[str, Any]:
    """The agent estate-move itself, factored out of `fold_agent` so `reconcile_agent_fold`
    (#127) can run the EXACT same repair on an already-merged pair rather than a second
    implementation that could drift from what a normal fold already does. Resolves
    `into`'s CURRENT living head fresh — never trusts a stale `merged_into` pointer, the
    same live-resolution `fold_agent` itself always used — and moves whatever is STILL
    live and addressed to `dupe`: unread mail, mount rows, open-thread ownership. Every
    move here is individually idempotent (an item already moved no longer matches its own
    WHERE clause), so running this twice, or running it after `fold_agent`'s own inline
    call already did the same work, changes nothing on the second pass."""
    from datetime import UTC, datetime

    head = await living_head(actions.pool, into)
    tag = await actions.pool.execute(
        "UPDATE fleet_messages SET to_agent=$1 WHERE to_agent=$2 AND read_at IS NULL",
        head, dupe)
    mail_moved = int(tag.rsplit(" ", 1)[-1])
    tag = await actions.pool.execute(
        "UPDATE agent_mounts SET agent_id=$1 WHERE agent_id=$2", head, dupe)
    rows_moved = int(tag.rsplit(" ", 1)[-1])
    threads = await actions.pool.fetch(
        "SELECT t.id FROM objects t JOIN current_assertions a ON a.object_id=t.id "
        "WHERE t.type='Thread' AND t.status='active' AND a.name='owner' "
        "AND a.value #>> '{}' = $1", dupe)
    now = datetime.now(UTC)
    for t in threads:
        await actions.assert_property(t["id"], "owner", head, actor, now, 0.9,
                                      evidence_class="self_declared")
    return {"living_head": head, "mail_readdressed": mail_moved,
            "mount_rows_repointed": rows_moved, "threads_reowned": len(threads)}


async def fold_agent(
    actions: Actions, *, dupe: str, into: str, evidence: str, actor: str,
) -> dict[str, Any]:
    """Fold agent `dupe` into agent `into`: the kernel merge (event, projection, same_as
    link) plus the estate — unread mail re-addressed to `into`'s LIVING HEAD, mount rows
    re-pointed, owned threads re-owned (evented via assert_property, never UPDATEd).

    ENFORCED, not just documented (census a5e53ed8/3f97f9c7, 2026-08-02: this docstring
    claimed "operator's word or an approved merge_candidate" for weeks while any mounted
    caller could fold any two agents): `actor` must be one of `seats._OPERATOR_ACTORS`'s
    sentinels, or the scheduled reaper's own name (`_SANCTIONED_AUTO_FOLD_ACTOR` — that
    tick is already gated separately by `osiris_fleet_reconcile_enabled`, a distinct
    operator signature). `resolve_fold_candidate`'s `decision='merged'` branch calls this
    function unchanged, so ITS caller inherits the same gate through this one check —
    no separate copy to drift out of sync. Refuses LOUDLY, naming who was refused.

    Refuses LOUDLY (an error dict, nothing written) when: evidence is empty; the actor is
    not authorized (above); either label is unknown or not an Agent; dupe==into or same
    lineage (generations are SUCCESSION, not duplication — folding one would collapse a
    death boundary the mind ruling keeps); dupe actively holds a Seat (transfer the seat
    first — a deliberate act, never a side effect); dupe is already folded. `into` may be
    any generation — the estate finds the living head regardless."""
    from src.orchestrator.agents import _generation

    dupe, into = (dupe or "").strip(), (into or "").strip()
    if not (evidence or "").strip():
        return {"error": "a fold without evidence is an auto-merge wearing a signature — "
                         "cite the transcripts/census/timing that prove one mind"}
    if actor not in _OPERATOR_ACTORS and actor != _SANCTIONED_AUTO_FOLD_ACTOR:
        return {"error": f"{actor!r} is not authorized to fold agents — fold_agent runs "
                         "only on the operator's own word (mount as the operator) or via "
                         "resolve_fold's judgment of an approved merge_candidate, relaying "
                         "the operator's own actor through unchanged; a mind cannot "
                         "approve its own fold"}
    if not dupe or not into:
        return {"error": "fold_agent needs both labels: dupe and into"}
    if _generation(dupe)[0] == _generation(into)[0]:
        return {"error": f"{dupe} and {into} are the same lineage — generations are "
                         "succession, not duplication; a fold here would collapse a "
                         "death boundary (the mind ruling, a882b334)"}
    rows = await actions.pool.fetch(
        "SELECT id, canonical, status FROM objects WHERE canonical = ANY($1::text[]) "
        "AND type='Agent'", [dupe, into])
    by_label = {r["canonical"]: r for r in rows}
    if dupe not in by_label or into not in by_label:
        missing = [x for x in (dupe, into) if x not in by_label]
        return {"error": f"unknown agent(s): {', '.join(missing)} — a fold never invents "
                         "either side"}
    if by_label[dupe]["status"] == "merged":
        prior = await canonical_agent(actions.pool, dupe)
        return {"error": f"{dupe} is already folded (→ {prior}) — nothing to do"}
    if by_label[into]["status"] == "merged":
        prior = await canonical_agent(actions.pool, into)
        return {"error": f"{into} is itself folded (→ {prior}) — fold into the living "
                         "label instead"}
    held = await actions.pool.fetchval(
        "SELECT ht.canonical FROM links hl JOIN objects hf ON hf.id=hl.from_id "
        "JOIN objects ht ON ht.id=hl.to_id WHERE hf.canonical=$1 AND hl.type='holds' "
        "AND (hl.valid_until IS NULL OR hl.valid_until > now()) LIMIT 1", dupe)
    if held:
        return {"error": f"{dupe} actively holds {held} — a seat transfer is a deliberate "
                         "act, never a fold's side effect; release or transfer the seat "
                         "first"}
    # THE ESTATE MOVES FIRST (#59's own precondition fix, mirroring fold_project's
    # already-safe pattern): a crash between here and the kernel merge below leaves
    # dupe.status=='active', so a retry re-enters this same function and simply
    # continues — every estate move here is idempotent (a mail row already re-addressed
    # no longer matches `to_agent=dupe`; a mount row the same; a thread already re-owned
    # no longer matches `owner=dupe`). The OLD order (merge first, estate after) made a
    # crash mid-fold PERMANENT: the merge's own "already folded — nothing to do" guard
    # refused every retry, stranding the estate forever — exactly the #127 class of bug,
    # now closed here before the reaper (task #59) could pour bulk, unattended volume
    # through it. `living_head(into)` is independent of dupe's own merge status (into is
    # already guaranteed unmerged by the guard above), so computing it before the merge
    # changes nothing about what it resolves to.
    estate = await _move_agent_estate(actions, dupe, into, actor)
    # the kernel merge: event, projection, same_as, case union, audit — resolve-on-read
    await actions.merge_objects(by_label[into]["id"], by_label[dupe]["id"],
                                justification=evidence, actor=actor)
    # a standing proposal for this pair (either order) is answered by the act itself —
    # AFTER the merge, deliberately: this is not estate (nothing is stranded if a crash
    # lands between the merge and here, only a tray row stays open a beat longer, and
    # resolve_fold_candidate's own re-check would just find the pair already merged)
    await actions.pool.execute(
        "UPDATE merge_candidates SET resolved='merged', resolved_by=$3, resolved_at=now() "
        "WHERE resolved IS NULL AND (a_id, b_id) IN (($1,$2),($2,$1))",
        by_label[dupe]["id"], by_label[into]["id"], actor)
    _log.info("fold: %s → %s (head %s): mail %d, rows %d, threads %d",
              dupe, into, estate["living_head"], estate["mail_readdressed"],
              estate["mount_rows_repointed"], estate["threads_reowned"])
    return {
        "folded": dupe, "into": into, "living_head": estate["living_head"],
        "mail_readdressed": estate["mail_readdressed"],
        "mount_rows_repointed": estate["mount_rows_repointed"],
        "threads_reowned": estate["threads_reowned"], "evidence": evidence,
        "note": (f"{dupe} is folded into {into} — its words stay its own (provenance "
                 "resolves through merged_into at read); its unread mail, mount rows, "
                 f"and open threads now belong to {estate['living_head']}. Reversible by "
                 "compensating event; nothing was deleted."),
    }


async def reconcile_agent_fold(
    actions: Actions, *, dupe: str, into: str, actor: str,
) -> dict[str, Any]:
    """THE REPAIR PATH fold_agent never had (#127, Thoth's framing verbatim: "folds are
    idempotent-by-REFUSAL when they need to be idempotent-by-REPAIR"): re-points any live
    mail/mount/thread estate item still aimed at an ALREADY-merged dupe, using the SAME
    `_move_agent_estate` `fold_agent` itself calls — not a second implementation that
    could drift from what a normal fold already does.

    THE INVERSE PRECONDITION of fold_agent, on purpose, so the two verbs' refusal
    conditions never overlap: fold_agent REQUIRES status=='active' and refuses a merged
    dupe; reconcile REQUIRES dupe.status=='merged' AND dupe's own `merged_into` pointing
    at exactly `into` (refuses to redirect a dupe merged into some OTHER agent — never
    guesses which pair a caller means).

    NEVER re-performs the fold: no `merge_objects` call, no same-lineage/actively-seated
    checks (those decide whether a fold SHOULD happen; this object already IS folded, so
    the only question is whether its estate move finished). UNMERGE-THEN-REMERGE IS NOT A
    SUBSTITUTE for this verb: `unfold_agent`'s own `estate_unreturnable` path would report
    — and drop — exactly the mail/mount items a partial fold already broke.

    SAME ACTOR GATE AS fold_agent, ENFORCED (finding 962579a6: a repair verb touching a
    merged estate needs the SAME authority as making the merge, not less — repairing is
    the same act, continued, never a lesser one).

    Refuses LOUDLY on: unauthorized actor; blank dupe/into; dupe==into; dupe not
    resolving to an Agent; dupe.status != 'merged' (fold_agent's job, not this one's);
    dupe's own `merged_into` not equal to `into`'s id; into not resolving to an ACTIVE
    Agent."""
    dupe, into = (dupe or "").strip(), (into or "").strip()
    if actor not in _OPERATOR_ACTORS and actor != _SANCTIONED_AUTO_FOLD_ACTOR:
        return {"error": f"{actor!r} is not authorized to reconcile an agent fold — same "
                         "gate as fold_agent itself (962579a6): repairing a merge needs "
                         "the same authority as making one"}
    if not dupe or not into:
        return {"error": "reconcile_agent_fold needs both labels: dupe and into"}
    if dupe == into:
        return {"error": "dupe and into name the same agent — nothing to reconcile"}
    row = await actions.pool.fetchrow(
        "SELECT id, status, merged_into FROM objects WHERE canonical=$1 AND type='Agent'",
        dupe)
    if row is None:
        return {"error": f"no such agent: {dupe!r} — reconcile never invents a label"}
    if row["status"] != "merged":
        return {"error": f"{dupe} is {row['status']}, not merged — reconcile_agent_fold "
                         "only repairs an ALREADY-completed fold; use merge to fold it in "
                         "the first place"}
    into_row = await actions.pool.fetchrow(
        "SELECT id, status FROM objects WHERE canonical=$1 AND type='Agent'", into)
    if into_row is None:
        return {"error": f"no such agent: {into!r} — reconcile never invents a label"}
    if into_row["id"] != row["merged_into"]:
        actual = await actions.pool.fetchval(
            "SELECT canonical FROM objects WHERE id=$1", row["merged_into"])
        return {"error": f"{dupe} is merged into {actual}, not {into} — "
                         "reconcile_agent_fold never redirects to a different pair"}
    if into_row["status"] != "active":
        return {"error": f"{into} is {into_row['status']}, not active"}
    estate = await _move_agent_estate(actions, dupe, into, actor)
    return {"reconciled": dupe, "into": into, **estate}

async def unfold_agent(
    actions: Actions, *, dupe: str, because: str, actor: str, execute: bool = False,
) -> dict[str, Any]:
    """Reverse a wrongful fold — the promise `fold_agent`'s own docstring makes
    ("reversible by compensating event") and the primitive that never existed to keep it
    (THE UNFOLD, Ferryman's resurrection, operator's word 2026-07-28). DRY RUN IS THE
    DEFAULT (`execute=False`): returns the exact plan — the kernel unmerge, any chain-
    integrity fix, and the estate items that CAN'T cleanly return — without writing
    anything. `execute=True` performs it.

    Refuses LOUDLY (an error dict, nothing written) when: `because` is blank; `dupe` is
    unknown or not currently folded (status != 'merged' — nothing to unfold); the
    ORIGINAL fold's own justification cites the operator's word and `because` does not
    ALSO carry a fresh one — a fold the operator blessed by name is not quietly undone by
    a different hand's say-so; it takes the same authority to reverse it that it took to
    make it. (Heuristic, not NLP: "cites the operator" = the word 'operator' appears in
    the original justification text.)

    CHAIN INTEGRITY: if `dupe`'s own `succeeded_by` currently points at a label from a
    DIFFERENT lineage (a cross-base pointer — successors are always same-base, by
    `_generation`'s own definition), that pointer is the fold's other half — a stitch,
    not a real succession — and gets cleared (asserted empty, superseding; the old value
    stays on the record, never deleted) so `dupe` reads as its own lineage's tail again.
    A same-base successor is never touched — real succession is not this verb's business.

    ESTATE: `fold_agent`'s mail/mount transfer is a raw UPDATE, not an event — the
    original `to_agent`/`agent_id` is gone the moment it moves, so nothing can PROVE
    which pre-fold messages or mount rows were `dupe`'s versus already the living head's
    own. Reported as `estate_unreturnable` for a human to read and judge, never guessed
    back. Thread ownership, by contrast, moved via `assert_property` (event-sourced): any
    thread whose CURRENT owner is the fold's living head but whose SUPERSEDED owner
    assertion names `dupe` is cleanly reversible, and `execute=True` re-asserts it."""
    from datetime import UTC, datetime

    from src.orchestrator.agents import _generation

    dupe, because = (dupe or "").strip(), (because or "").strip()
    if not because:
        return {"error": "an unfold without a because is an un-audited reversal — cite "
                         "the evidence/ruling that proves the fold was wrong"}
    if not dupe:
        return {"error": "unfold_agent needs a dupe label"}
    row = await actions.pool.fetchrow(
        "SELECT id, status, merged_into FROM objects WHERE canonical=$1 AND type='Agent'",
        dupe)
    if row is None:
        return {"error": f"unknown agent: {dupe} — an unfold never invents a label"}
    if row["status"] != "merged":
        return {"error": f"{dupe} is not folded (status={row['status']}) — nothing to "
                         "unfold"}
    into_canon = await actions.pool.fetchval(
        "SELECT canonical FROM objects WHERE id=$1", row["merged_into"])
    ev = await actions.pool.fetchrow(
        "SELECT payload, actor, created_at FROM object_events "
        "WHERE event_type='merge' AND related_id=$1 ORDER BY created_at DESC LIMIT 1",
        row["id"])
    original_evidence = str((ev["payload"] or {}).get("justification", "")) if ev else ""
    if "operator" in original_evidence.lower() and "operator" not in because.lower():
        return {"error": f"{dupe}'s fold was justified by citing the operator's word "
                         f"({original_evidence!r}) — an unfold needs the operator's word "
                         "too; add it to `because` or get it first"}
    head = await living_head(actions.pool, str(into_canon))
    fold_time = ev["created_at"] if ev else datetime.now(UTC)

    # CHAIN INTEGRITY — a cross-base succeeded_by is the fold's other half
    cur_base = _generation(dupe)[0]
    succ = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='succeeded_by' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        row["id"])
    stitch = bool(succ) and _generation(str(succ))[0] != cur_base

    # ESTATE — mail/mounts moved by a raw UPDATE (no event) can't be proven back;
    # thread ownership moved via assert_property (event-sourced) can be
    unreturnable_mail = [dict(r) for r in await actions.pool.fetch(
        "SELECT id, from_agent, created_at, read_at, left(body,120) AS body "
        "FROM fleet_messages WHERE to_agent=$1 AND created_at < $2 ORDER BY created_at",
        head, fold_time)]
    unreturnable_mounts = [dict(r) for r in await actions.pool.fetch(
        "SELECT job_dir, project, cwd, mounted_at FROM agent_mounts "
        "WHERE agent_id=$1 AND mounted_at < $2 ORDER BY mounted_at", head, fold_time)]
    reversible_threads = [dict(r) for r in await actions.pool.fetch(
        "SELECT o.id, o.canonical, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS summary "
        "FROM objects o "
        "JOIN current_assertions cur ON cur.object_id=o.id AND cur.name='owner' "
        "  AND cur.value #>> '{}' = $1 "
        "WHERE o.type='Thread' AND EXISTS ("
        "  SELECT 1 FROM assertions old WHERE old.object_id=o.id AND old.name='owner' "
        "  AND old.value #>> '{}' = $2)", head, dupe)]

    plan: list[dict[str, Any]] = [
        {"op": "unmerge_objects", "target": dupe, "detail": f"status merged→active, "
         f"merged_into cleared (was {into_canon})"}]
    if stitch:
        plan.append({"op": "assert_property", "target": dupe, "detail":
                    f"succeeded_by {succ!r} → '' (cross-lineage stitch cleared, {dupe} "
                    "becomes its own lineage's tail again)"})
    for t in reversible_threads:
        plan.append({"op": "assert_property", "target": t["canonical"], "detail":
                    f"owner {head} → {dupe} (restoring the pre-fold assertion)"})

    report: dict[str, Any] = {
        "dupe": dupe, "was_merged_into": into_canon, "living_head": head,
        "fold_actor": ev["actor"] if ev else None, "fold_justification": original_evidence,
        "fold_at": fold_time.isoformat() if hasattr(fold_time, "isoformat") else fold_time,
        "plan": plan,
        "estate_unreturnable": {
            "mail": unreturnable_mail, "mounts": unreturnable_mounts,
            "note": ("pre-fold UPDATEs overwrote to_agent/agent_id in place — these "
                     "predate the fold and still sit on the living head, but nothing "
                     "proves they were EVER addressed to the dupe rather than already "
                     "the head's own; read them and judge by hand, never auto-moved")
                    if (unreturnable_mail or unreturnable_mounts) else
                    "none found — no pre-fold mail or mount rows sit unclaimed on the head",
        },
        "execute": execute,
    }
    if not execute:
        return report

    await actions.unmerge_objects(row["id"], because, actor)
    now = datetime.now(UTC)
    if stitch:
        await actions.assert_property(row["id"], "succeeded_by", "", actor, now, 0.95,
                                      evidence_class="direct_observation")
    for t in reversible_threads:
        await actions.assert_property(t["id"], "owner", dupe, actor, now, 0.9,
                                      evidence_class="self_declared")
    report.update({
        "unmerged": True, "chain_restored": stitch, "threads_reowned": len(reversible_threads),
        "note": (f"{dupe} is active again — provenance for the folded era stays on the "
                 f"record (the merge event and same_as link are witnesses, never erased). "
                 + (f"succeeded_by cleared; {dupe} is its own lineage's tail. "
                    if stitch else "")
                 + f"{len(reversible_threads)} thread(s) re-owned. "
                 + ("Unreturnable estate items are listed above for a human to judge by "
                    "hand." if (unreturnable_mail or unreturnable_mounts) else "")),
    })
    return report


async def find_agent_fold_candidates(
    pool: asyncpg.Pool, *, projects_root: Path | None = None,
    jobs_home: Path | None = None,
) -> dict[str, Any]:
    """THE ARCHAEOLOGIST (thread b975851b) — mine the registry and the disk for anonymous
    agents that EVIDENCE says were never distinct minds, and queue them as review-gated
    merge_candidates. PROPOSALS ONLY: nothing folds here; the operator judges the tray
    (constitution #1's spirit — flag, never guess; the phantom-twin lint's law at census
    scale). Two evidence classes this pass:

    VIEW-ALIAS (score .9): an anonymous agent whose whole presence is a mount row with NO
    transcript for its sid anywhere and no state.json in its jobs dir — a doorbell ring —
    co-resident at the same cwd with a session that HAS a body. Propose fold into that
    session's agent (the 10802085/bef1d349/d1848828 class, decoded live 2026-07-16).

    RESTART-MINT (score .75 single-seat / .55 multi-seat): an anonymous agent mounted at
    a cwd where a NAMED lineage anchors — the b86de6c1-beside-628ef839 class. The
    single-seat presumption (operator rule): in a one-seat project the anon is presumed
    the seat's child or fold; where several seats share the project it is nuanced and
    the low score says verify by hand.

    CHARTER-MATCH (same scores, thread 3430c32b): when no named lineage anchors at the
    anon's cwd — the office migrations moved every seat's mount row home to
    ~/.osiris/seats/<handle>, stranding anons in the seats' OLD project rooms — the
    room's seat is still a GRAPH fact: a named lineage holding a live works_in or
    governs link to repo:<project>. Mount rows move house; the charter does not. A room
    whose charter names NO seat proposes nothing (its anons are the visitor class —
    demotion candidates for the visitor gate, never folds) and is counted in `seatless`.

    Pairs already rejected or linked not_same_as are never re-proposed."""
    from src.ontology.resolution import _suppressed
    from src.orchestrator.agents import _generation

    root = projects_root or Path.home() / ".claude" / "projects"
    jobs = jobs_home or Path.home() / ".claude" / "jobs"
    sup = await _suppressed(pool)

    def _has_body(sid8: str) -> bool:
        if not sid8 or not root.is_dir():
            return False
        return any(slug.is_dir() and any(slug.glob(sid8 + "*.jsonl"))
                   for slug in root.iterdir())

    anons = await pool.fetch(
        "SELECT DISTINCT ON (o.canonical) o.id AS oid, o.canonical, m.cwd, m.job_dir, "
        "m.project "
        "FROM objects o JOIN agent_mounts m ON m.agent_id = o.canonical "
        "WHERE o.type='Agent' AND o.status='active' AND NOT EXISTS ("
        "  SELECT 1 FROM current_assertions h JOIN objects ho ON ho.id=h.object_id "
        "  WHERE h.name='handle' AND (ho.canonical=o.canonical "
        "        OR o.canonical LIKE ho.canonical||'-%' OR ho.canonical LIKE o.canonical||'-%')) "
        "ORDER BY o.canonical, m.last_seen DESC")
    proposed: dict[str, int] = {"view-alias": 0, "restart-mint": 0, "charter-match": 0}
    seatless: dict[str, int] = {}
    for r in anons:
        sid8 = Path(r["job_dir"]).name if r["job_dir"] else ""
        cwd, mine = r["cwd"], str(r["canonical"])
        target: str | None = None
        cls, score, signals = "", 0.0, []
        bodiless = bool(sid8) and not _has_body(sid8) and not (
            jobs / sid8 / "state.json").exists()
        if bodiless and cwd:
            others = await pool.fetch(
                "SELECT agent_id, job_dir FROM agent_mounts WHERE cwd=$1 AND agent_id<>$2 "
                "ORDER BY last_seen DESC LIMIT 8", cwd, mine)
            for o in others:
                if _generation(str(o["agent_id"]))[0] == _generation(mine)[0]:
                    continue
                osid = Path(o["job_dir"]).name if o["job_dir"] else ""
                if osid and _has_body(osid):
                    target, cls, score = str(o["agent_id"]), "view-alias", 0.9
                    signals = [f"no transcript for sid {sid8} anywhere under {root}",
                               f"jobs/{sid8} holds no state.json (not a daemon session)",
                               f"co-resident at {cwd} with {o['agent_id']}, whose sid "
                               f"{osid} has a body"]
                    break
        if target is None and cwd:
            named = await pool.fetchval(
                "SELECT m2.agent_id FROM agent_mounts m2 WHERE m2.cwd=$1 "
                "AND m2.agent_id<>$2 AND EXISTS ("
                "  SELECT 1 FROM current_assertions h JOIN objects ho ON ho.id=h.object_id "
                "  WHERE h.name='handle' AND (ho.canonical=m2.agent_id "
                "        OR m2.agent_id LIKE ho.canonical||'-%')) "
                "ORDER BY m2.last_seen DESC LIMIT 1", cwd, mine)
            if named and _generation(str(named))[0] != _generation(mine)[0]:
                # THE SINGLE-SEAT PRESUMPTION (operator rule, 2026-07-16): 'an anon agent
                # in phanspeed — if the project only has one seat, we have to assume it
                # was a child or a fold of the only agent there; for projects with 2 like
                # xxit or monsterhouse, it's more nuanced.'
                seats_here = await pool.fetchval(
                    "SELECT count(DISTINCT h.value #>> '{}') FROM agent_mounts m2 "
                    "JOIN objects ho2 ON (m2.agent_id = ho2.canonical "
                    "     OR m2.agent_id LIKE ho2.canonical||'-%') "
                    "JOIN current_assertions h ON h.object_id=ho2.id AND h.name='handle' "
                    "WHERE m2.project = $1", r["project"]) or 0
                target, cls = str(named), "restart-mint"
                if int(seats_here) <= 1:
                    score = 0.75
                    signals = [f"anonymous mount at {cwd} — {named} is the project's "
                               "ONLY seat, so this session is presumed its child or fold "
                               "(the single-seat rule)"]
                else:
                    score = 0.55
                    signals = [f"anonymous mount at {cwd}, the anchor of named lineage "
                               f"{named} — but {seats_here} seats share this project; "
                               "nuanced, verify by hand"]
        if target is None and r["project"]:
            # THE CHARTER MATCH (thread 3430c32b): nobody named anchors at this cwd —
            # but the ROOM may still have a seat on the graph's record. Mount rows are
            # mortal and move house (the office migrations); works_in/governs edges are
            # the durable evidence of whose room this is.
            # governs' from_type spans BOTH Agent (pre-1db1ff41-ruling-3) and Seat
            # (post-re-key, decision 1db1ff41) — a fixed join on fo.type='Agent' would
            # silently stop matching every governs link the moment the re-key lands, no
            # crash, degrading this whole reversal back to works_in-only (Imhotep, DM
            # 2415/2416, caught before it shipped). works_in stays Agent-only; governs is
            # read either way and a Seat-origin edge is resolved to its current holder
            # below, before it ever reaches the souls bookkeeping.
            from src.orchestrator.seats import seat_occupancy

            holders = await pool.fetch(
                "SELECT DISTINCT fo.canonical AS holder, fo.type AS holder_type, "
                "l.type AS via "
                "FROM links l "
                "JOIN objects fo ON fo.id=l.from_id AND fo.type IN ('Agent','Seat') "
                "JOIN objects ro ON ro.id=l.to_id "
                "WHERE ro.canonical = 'repo:' || $1 "
                "  AND l.type IN ('works_in','governs') "
                "  AND (l.valid_until IS NULL OR l.valid_until > now()) "
                "  AND EXISTS ("
                "    SELECT 1 FROM current_assertions h JOIN objects ho ON ho.id=h.object_id "
                "    WHERE h.name='handle' AND (ho.canonical=fo.canonical "
                "          OR fo.canonical LIKE ho.canonical||'-%'))", r["project"])
            souls: dict[str, set[str]] = {}
            for h in holders:
                if h["holder_type"] == "Seat":
                    occ = await seat_occupancy(pool, str(h["holder"]))
                    holder_agent = occ.get("holder")
                    if not holder_agent:  # a vacant seat's charter proposes nothing
                        continue
                else:
                    holder_agent = str(h["holder"])
                # key souls by the LIVING head's base — a rebased lineage (Ra:
                # c7ef52a9 -> 443cd9d4) is one soul, not two, and must not
                # deflate the single-seat score
                head_ = await living_head(pool, _generation(str(holder_agent))[0])
                base = _generation(head_)[0]
                if base != _generation(mine)[0]:
                    souls.setdefault(base, set()).add(str(h["via"]))
            if not souls:
                seatless[r["project"]] = seatless.get(r["project"], 0) + 1
                continue
            # DECLARED BEATS DERIVED (operator ruling 1db1ff41, verbatim: "declared, all
            # roads lead to explicit") — REVERSES this tie-break's prior rule, which ranked
            # the room's RESIDENT (works_in) above a supervising charter (governs) on the
            # reasoning that "an Alfred governs coldspot, but coldspot's anons presumptively
            # belong to the seat that lives there." That reasoning is SUPERSEDED, not
            # forgotten: decision 2655a0a8 found the opposite holds on the two hardest real
            # cases in the fleet — alfred's DECLARED charter correctly named mudra->vajra and
            # sutra->tantra when both seats' own derived/resident pins were wrong. A charter
            # is a deliberate, operator-reviewed declaration; residence is wherever a session
            # happened to mount. The room's DECLARED governor now outranks its resident.
            governors = sorted(b for b, via in souls.items() if "governs" in via)
            if len(souls) == 1:
                base = next(iter(souls))
            elif len(governors) == 1:
                base = governors[0]
            else:
                pick = governors or sorted(souls)
                recent = await pool.fetchval(
                    "SELECT agent_id FROM agent_mounts "
                    "WHERE agent_id LIKE ANY($1::text[]) "
                    "ORDER BY last_seen DESC NULLS LAST LIMIT 1",
                    [b for b in pick] + [b + "-%" for b in pick])
                base = _generation(str(recent))[0] if recent else pick[0]
            target = await living_head(pool, base)
            cls = "charter-match"
            if len(souls) == 1:
                score = 0.75
                signals = [f"anonymous mount in room '{r['project']}' at {cwd} — no "
                           f"named lineage anchors there (the seat's row moved home at "
                           f"the office migration), but the graph's charter answers: "
                           f"{target} ({'/'.join(sorted(souls[base]))} "
                           f"repo:{r['project']}) is the room's ONLY seat — the "
                           "single-seat rule"]
            else:
                roster = ", ".join(f"{b} ({'/'.join(sorted(v))})"
                                   for b, v in sorted(souls.items()))
                score = 0.55
                signals = [f"anonymous mount in room '{r['project']}' at {cwd} — no "
                           f"named lineage anchors there; the charter names "
                           f"{len(souls)} souls for this room [{roster}], declared "
                           f"governor presumed: {target} — nuanced, verify by hand"]
        if target is None:
            continue
        trow = await pool.fetchrow(
            "SELECT id, status FROM objects WHERE canonical=$1 AND type='Agent'", target)
        if trow is None or trow["status"] != "active":
            continue
        if frozenset((r["oid"], trow["id"])) in sup:
            continue
        # the table orders pairs by uuid (CHECK a_id < b_id) — the ROLES live in reasons
        lo, hi = sorted((r["oid"], trow["id"]))
        row = await pool.fetchrow(
            "INSERT INTO merge_candidates (a_id, b_id, score, reasons) "
            "VALUES ($1,$2,$3,$4) ON CONFLICT (a_id, b_id) DO NOTHING RETURNING id",
            lo, hi, score,
            {"kind": "agent-fold", "class": cls, "dupe": mine, "into": target,
             "signals": signals})
        if row is not None:
            proposed[cls] += 1
    pending = [dict(p) for p in await pool.fetch(
        "SELECT c.id, c.score, c.reasons->>'class' AS class, "
        "c.reasons->>'dupe' AS dupe, c.reasons->>'into' AS into_label, "
        "c.reasons->'signals' AS signals "
        "FROM merge_candidates c "
        "WHERE c.resolved IS NULL AND c.reasons->>'kind'='agent-fold' "
        "ORDER BY c.score DESC, c.id LIMIT 100")]
    # labels the census can no longer resolve (folded meanwhile) would confuse the tray —
    # they are stamped by fold_agent itself, so pending here is always actionable
    return {"examined": len(anons), "proposed": proposed, "pending": pending,
            "seatless": seatless,
            "note": "proposals only — judge each with resolve_fold_candidate (merged | "
                    "rejected); a rejection is remembered and never re-proposed; "
                    "`seatless` counts anons in rooms whose charter names NO seat — "
                    "visitor-gate demotion candidates, not folds"}


async def resolve_fold_candidate(
    actions: Actions, *, candidate_id: int, decision: str, actor: str,
) -> dict[str, Any]:
    """Judge one agent-fold proposal from the tray. 'merged' executes fold_agent — the
    ESTATE-carrying fold, never the bare kernel merge (an agent folded without its mail,
    rows, and threads is the orphan machine again) — and INHERITS fold_agent's own
    operator-actor gate unchanged (census a5e53ed8: this used to be the exact self-
    approval gap the constitution forbids — an agent proposing AND judging its own
    candidate — since fixed by fold_agent's own check, not a second copy here). 'rejected'
    mints not_same_as both ways and the pair is never re-proposed — OPEN to any mounted
    caller, deliberately: rejecting is a judgment that two things are NOT the same mind,
    never an identity mutation, so it carries none of 'merged's blast radius and needs
    none of its gate. Either way the candidate row is stamped with the judge's name. The
    entity tray's twin (resolution.resolve_candidate) stays for Person/Company — this one
    exists because agents have estates."""
    row = await actions.pool.fetchrow(
        "SELECT c.id, c.resolved, c.reasons FROM merge_candidates c WHERE c.id=$1",
        candidate_id)
    if row is None:
        return {"error": f"candidate {candidate_id} not found"}
    if row["resolved"] is not None:
        return {"error": f"candidate {candidate_id} already {row['resolved']}"}
    reasons = row["reasons"] or {}
    if reasons.get("kind") != "agent-fold":
        return {"error": f"candidate {candidate_id} is not an agent-fold proposal — "
                         "judge it in the entity tray (resolution.resolve_candidate)"}
    if decision == "merged":
        # the pair's ROLES live in reasons — the table's columns are uuid-ordered
        signals = "; ".join(reasons.get("signals") or []) or "approved from the tray"
        out = await fold_agent(actions, dupe=str(reasons.get("dupe") or ""),
                               into=str(reasons.get("into") or ""),
                               evidence=f"approved candidate {candidate_id}: {signals}",
                               actor=actor)
        if "error" in out:
            return out  # the row stays unresolved — a refused fold is not a judgment
        return {**out, "candidate": candidate_id, "resolved": "merged"}
    if decision == "rejected":
        from datetime import UTC, datetime
        a_oid = await actions.pool.fetchval(
            "SELECT a_id FROM merge_candidates WHERE id=$1", candidate_id)
        b_oid = await actions.pool.fetchval(
            "SELECT b_id FROM merge_candidates WHERE id=$1", candidate_id)
        now = datetime.now(UTC)
        await actions.create_link(a_oid, b_oid, "not_same_as", actor, now, 1.0)
        await actions.create_link(b_oid, a_oid, "not_same_as", actor, now, 1.0)
        await actions.pool.execute(
            "UPDATE merge_candidates SET resolved='rejected', resolved_by=$2, "
            "resolved_at=now() WHERE id=$1", candidate_id, actor)
        return {"candidate": candidate_id, "resolved": "rejected",
                "note": "pair linked not_same_as — never re-proposed"}
    return {"error": f"decision must be 'merged' or 'rejected', got {decision!r}"}

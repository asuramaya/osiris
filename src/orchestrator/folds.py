"""Agent folds: the reconciliation primitive for merging and resolving duplicate agent
records, append-only, so scattered nodes get reconciled into their live/current lane.

The kernel already has an identity merge, `Actions.merge_objects`: an append-only
'merge' event, the `status='merged'` + `merged_into` projection, a `same_as` link,
resolve-on-read. It was built for the entity commons (Person/Company dedup) and never
used for Agents: the founding rule against auto-merging had culturally over-extended
into "never merge at all." A fold is the review-gated form that rule actually allows.

`fold_agent` wraps the kernel merge with the agent's associated routing records: the
three surfaces an agent owns that a Person does not: unread mail, durable mount rows,
thread ownership. This is the same shape as succession's transfer of those records
(`mint_heir`, agents.py), generalized from "death" to "recognition": a fold says two
labels were always one agent, so the living label inherits, and nothing is deleted or
rewritten. The duplicate's words stay stamped with the duplicate's id, and provenance
resolves at read time through `merged_into`.

Review-gated, always: a fold executes only on explicit authorization or an approved
merge_candidate, and `evidence` is mandatory. A fold without citations is an auto-merge
wearing a signature.
"""
from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions

_log = logging.getLogger("osiris.folds")

# The name of the scheduled reconciliation tick (fleet_reconcile.py's
# reconcile_scheduled_tick): the one non-human actor fold_agent trusts, because that tick
# is already gated separately by osiris_fleet_reconcile_enabled, a distinct feature flag.
# Defined here, not in fleet_reconcile.py, so the authority check and the identity it
# recognizes live in the same file; fleet_reconcile.py imports this constant rather than
# repeating the literal, so the two can never drift apart.
_SANCTIONED_AUTO_FOLD_ACTOR = "cron:fleet_reconcile_heartbeat"


async def living_head(pool: asyncpg.Pool, agent_id: str) -> str:
    """The lineage's living head: where a folded agent's associated records land. The
    graph decides this via auto-heal behavior (folds must self-heal): resolve the label
    through the merge chain, then walk succeeded_by to the last active generation. This
    walk crosses rebased lineages wherever a succession was recorded, so a record citing
    a dead generation still lands its records on whoever answers to the name today. The
    registry only breaks the tie for a label with no succession record (an anonymous
    base), and its answer may never regress to an older generation than the graph's. The
    result must never land on a dead generation: succession's own transfer (mint_heir)
    would just have to move it again.

    The invariant is unconditional. A broken mid-chain link (the walk's own succeeded_by
    resolution genuinely winning on a stale retraction partway through, e.g. a
    timing-based cleanup at a later, non-tied timestamp than the real pointer it
    invalidated, distinct from lineage_head's own true-tie fix) used to escape this
    safety net entirely, because it only ever fired when `head == canon`, i.e. the walk
    advanced zero hops from the very base. A walk that advances many real hops and only
    then hits a broken link looked identical to a genuine, correctly-resolved head:
    `head != canon` short-circuited straight past the live-mount check below. Fixed by
    making the comparison unconditional: always compute the freshest live-mounted
    candidate in this family and take whichever of {the walk's own answer, that
    candidate} names the newer generation. Head resolution must never name a generation
    older than one with a live mount, regardless of how far the walk itself got before
    stopping."""
    from src.orchestrator.agents import _generation, lineage_head

    canon = await canonical_agent(pool, agent_id)
    head = await lineage_head(pool, canon)
    base = _generation(canon)[0]
    row = await pool.fetchval(
        "SELECT agent_id FROM agent_mounts WHERE agent_id=$1 OR agent_id LIKE $1 || '-%' "
        "ORDER BY last_seen DESC NULLS LAST LIMIT 1", base)
    if row and _generation(str(row))[1] > _generation(head)[1]:
        return str(row)
    return head


async def wakeable_identity(pool: asyncpg.Pool, agent_id: str) -> str | None:
    """Answers the wake question, "which OS session can be resumed," independently of
    `living_head`'s delivery question, "whose mailbox owns this name." A succession is a
    declared act (mint_heir) and living_head trusts it unconditionally, exactly as
    delivery should: declared beats derived. But a successor that was declared and never
    actually mounted has no OS session behind it, and a wake path that walks
    living_head's own answer strands mail in a live session's own inbox. (Live specimen:
    the mailbox honored an explicit live id, the wake still resolved through a
    never-mounted lineage head, and reported "has never mounted" beside a result naming
    that same live session's fresh last_seen.) Returns the most recently mounted id
    anywhere in `agent_id`'s lineage, the same auto-heal query `living_head` itself falls
    back to for an unsucceeded base (below), generalized here to fire even when a
    succession was declared, because wake cares which session can answer, not which name
    is now correct. None only when nothing in the whole lineage has ever mounted."""
    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    row = await pool.fetchval(
        "SELECT agent_id FROM agent_mounts WHERE agent_id=$1 OR agent_id LIKE $1 || '-%' "
        "ORDER BY last_seen DESC NULLS LAST LIMIT 1", base)
    return str(row) if row else None


async def canonical_agent(pool: asyncpg.Pool, agent_id: str) -> str:
    """Resolve an agent label through the merged_into chain to its living label: the
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
    """Shared reversal probe used by both merge and unmerge, built so unfold_seat and
    unfold_project can match unfold_agent's behavior. Finds every other object whose live
    `link_type` edge now points at `into` but which also carries a now-invalid edge of
    the same type that once pointed at `dupe`. That is the exact trail an event-sourced
    (`invalidate_link` + `create_link`) record move leaves behind, safe to auto-reverse
    because nothing has touched it since. Generalizes `unfold_agent`'s own thread-
    ownership check (`assert_property`'s history) from properties to links: fold_seat's
    holders and managed_by edges, fold_project's associated links. `from_dupe=True` reads
    edges pointing into dupe/into (holds, in_repo, works_in, governs, informs; the other
    object is the link's `from_id`); `from_dupe=False` reads edges pointing out of
    dupe/into (the managed_by direction where dupe/into is itself the manager; the other
    object is the link's `to_id`). A record moved by a raw UPDATE (mail, agent_mounts)
    leaves no such trail and is never found here; the caller reports those as
    `estate_unreturnable` instead, exactly as `unfold_agent` already does for its own
    mail/mounts."""
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
    """The move of an agent's associated records itself, factored out of `fold_agent` so
    `reconcile_agent_fold` can run the exact same repair on an already-merged pair rather
    than a second implementation that could drift from what a normal fold already does.
    Resolves `into`'s current living head fresh (never trusts a stale `merged_into`
    pointer, the same live-resolution `fold_agent` itself always used) and moves
    whatever is still live and addressed to `dupe`: unread mail, mount rows, open-thread
    ownership, and (a gap distinct from mint_heir's own: this record-move never covered
    works_in/governs at all, not even a stale pointer, a plain omission, closed here)
    works_in/governs edges, via `agents.move_agent_project_links`, the same shared mover
    `mint_heir` now uses for its own, different gap on ordinary succession, never a
    second implementation. Every move here is individually idempotent (an item already
    moved no longer matches its own WHERE clause), so running this twice, or running it
    after `fold_agent`'s own inline call already did the same work, changes nothing on
    the second pass.

    The dual read-marker gap: the mail UPDATE above re-addresses `fleet_messages.
    to_agent` from `dupe` to `head`, but `message_recipients` rows stay keyed to `dupe`'s
    id. Every reader's own "have I already read this" check (`NOT EXISTS ... WHERE
    r3.agent_id=<head's lineage>`) then finds nothing, and mail `dupe` genuinely already
    read reappears as deliverable to `head`. (Live specimen: a run of dupe's recipient
    rows read by an old generation, with fleet_messages.read_at left NULL.) `mint_heir`
    (agents.py) closed this exact gap for ordinary succession years ago (its own INSERT
    ... ON CONFLICT DO NOTHING copy); `fold_agent`'s record-move never got the same fix,
    because it is a second, independent implementation of "one agent inherits another's
    mail." Mirrored here, verbatim in shape: a non-destructive copy (dupe keeps its own
    rows; unfold_agent's own `estate_unreturnable` framing for the raw to_agent/agent_id
    UPDATEs is untouched by this, since nothing here is destroyed, only duplicated
    forward)."""
    from datetime import UTC, datetime

    from src.orchestrator.agents import move_agent_project_links

    head = await living_head(actions.pool, into)
    tag = await actions.pool.execute(
        "UPDATE fleet_messages SET to_agent=$1 WHERE to_agent=$2 AND read_at IS NULL",
        head, dupe)
    mail_moved = int(tag.rsplit(" ", 1)[-1])
    tag = await actions.pool.execute(
        "INSERT INTO message_recipients (message_id, agent_id, delivered_at, read_at, "
        "deliveries)"
        " SELECT message_id, $1, delivered_at, read_at, deliveries FROM message_recipients"
        " WHERE agent_id=$2 ON CONFLICT (message_id, agent_id) DO NOTHING", head, dupe)
    read_markers_copied = int(tag.rsplit(" ", 1)[-1])
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
    dupe_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", dupe)
    head_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", head)
    project_links_moved: dict[str, int] = {}
    if dupe_oid is not None and head_oid is not None and dupe_oid != head_oid:
        project_links_moved = await move_agent_project_links(
            actions, dupe_oid, head_oid, actor, now)
    return {"living_head": head, "mail_readdressed": mail_moved,
            "read_markers_copied": read_markers_copied,
            "mount_rows_repointed": rows_moved, "threads_reowned": len(threads),
            "project_links_moved": project_links_moved}


async def fold_agent(
    actions: Actions, *, dupe: str, into: str, evidence: str, actor: str,
) -> dict[str, Any]:
    """Fold agent `dupe` into agent `into`: the kernel merge (event, projection, same_as
    link) plus its associated records: unread mail re-addressed to `into`'s living head,
    mount rows re-pointed, owned threads re-owned (evented via assert_property, never
    UPDATEd).

    This authorization check is enforced, not just documented: earlier this docstring
    claimed authorization was required while any mounted caller could in fact fold any
    two agents. `actor` must resolve to a recognized operator identity
    (`charter.is_operator_actor`; global recognition, no project in scope for a
    fleet-wide identity merge; authority is granted by charter), or the scheduled
    reconciliation tick's own name (`_SANCTIONED_AUTO_FOLD_ACTOR`; that tick is already
    gated separately by `osiris_fleet_reconcile_enabled`, a distinct feature flag).
    `resolve_fold_candidate`'s `decision='merged'` branch calls this function unchanged,
    so its caller inherits the same gate through this one check, with no separate copy to
    drift out of sync. Refuses loudly, naming who was refused.

    Refuses loudly (an error dict, nothing written) when: evidence is empty; the actor is
    not authorized (above); either label is unknown or not an Agent; dupe==into or same
    lineage (generations are succession, not duplication; folding one would collapse a
    death boundary the succession rule keeps); dupe actively holds a Seat (transfer the
    seat first, a deliberate act, never a side effect); dupe is already folded. `into`
    may be any generation: the record move finds the living head regardless."""
    from src.orchestrator.agents import _generation
    from src.orchestrator.charter import is_operator_actor

    dupe, into = (dupe or "").strip(), (into or "").strip()
    if not (evidence or "").strip():
        return {"error": "a fold without evidence is an auto-merge wearing a signature: "
                         "cite the transcripts/census/timing that prove one agent"}
    if not await is_operator_actor(actions.pool, actor) and actor != _SANCTIONED_AUTO_FOLD_ACTOR:
        return {"error": f"{actor!r} is not authorized to fold agents: fold_agent runs "
                         "only on the operator's own word (mount as the operator) or via "
                         "resolve_fold's judgment of an approved merge_candidate, relaying "
                         "the operator's own actor through unchanged; an agent cannot "
                         "approve its own fold"}
    if not dupe or not into:
        return {"error": "fold_agent needs both labels: dupe and into"}
    if _generation(dupe)[0] == _generation(into)[0]:
        return {"error": f"{dupe} and {into} are the same lineage: generations are "
                         "succession, not duplication; a fold here would collapse a "
                         "death boundary that the succession rule exists to protect"}
    rows = await actions.pool.fetch(
        "SELECT id, canonical, status FROM objects WHERE canonical = ANY($1::text[]) "
        "AND type='Agent'", [dupe, into])
    by_label = {r["canonical"]: r for r in rows}
    if dupe not in by_label or into not in by_label:
        missing = [x for x in (dupe, into) if x not in by_label]
        return {"error": f"unknown agent(s): {', '.join(missing)}: a fold never invents "
                         "either side"}
    if by_label[dupe]["status"] == "merged":
        prior = await canonical_agent(actions.pool, dupe)
        return {"error": f"{dupe} is already folded (now {prior}): nothing to do"}
    if by_label[into]["status"] == "merged":
        prior = await canonical_agent(actions.pool, into)
        return {"error": f"{into} is itself folded (now {prior}): fold into the living "
                         "label instead"}
    held = await actions.pool.fetchval(
        "SELECT ht.canonical FROM links hl JOIN objects hf ON hf.id=hl.from_id "
        "JOIN objects ht ON ht.id=hl.to_id WHERE hf.canonical=$1 AND hl.type='holds' "
        "AND (hl.valid_until IS NULL OR hl.valid_until > now()) LIMIT 1", dupe)
    if held:
        return {"error": f"{dupe} actively holds {held}: a seat transfer is a deliberate "
                         "act, never a fold's side effect; release or transfer the seat "
                         "first"}
    # The record move happens first, mirroring fold_project's already-safe pattern: a
    # crash between here and the kernel merge below leaves dupe.status=='active', so a
    # retry re-enters this same function and simply continues. Every record move here is
    # idempotent (a mail row already re-addressed no longer matches `to_agent=dupe`; a
    # mount row the same; a thread already re-owned no longer matches `owner=dupe`). The
    # old order (merge first, records moved after) made a crash mid-fold permanent: the
    # merge's own "already folded, nothing to do" guard refused every retry, stranding
    # the records forever. That class of bug is now closed here before the scheduled
    # reconciliation tick could pour bulk, unattended volume through it. `living_head
    # (into)` is independent of dupe's own merge status (into is already guaranteed
    # unmerged by the guard above), so computing it before the merge changes nothing
    # about what it resolves to.
    estate = await _move_agent_estate(actions, dupe, into, actor)
    # the kernel merge: event, projection, same_as, case union, audit; resolve-on-read
    await actions.merge_objects(by_label[into]["id"], by_label[dupe]["id"],
                                justification=evidence, actor=actor)
    # A standing proposal for this pair (either order) is answered by the act itself,
    # after the merge, deliberately: this is not a record move (nothing is stranded if a
    # crash lands between the merge and here, only a tray row stays open a beat longer,
    # and resolve_fold_candidate's own re-check would just find the pair already merged)
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
        "read_markers_copied": estate["read_markers_copied"],
        "mount_rows_repointed": estate["mount_rows_repointed"],
        "threads_reowned": estate["threads_reowned"],
        "project_links_moved": estate["project_links_moved"], "evidence": evidence,
        "note": (f"{dupe} is folded into {into}: its words stay its own (provenance "
                 "resolves through merged_into at read); its unread mail, mount rows, "
                 "open threads, and works_in/governs edges now belong to "
                 f"{estate['living_head']}. Reversible by compensating event; nothing "
                 "was deleted."),
    }


async def reconcile_agent_fold(
    actions: Actions, *, dupe: str, into: str, actor: str,
) -> dict[str, Any]:
    """The repair path fold_agent never had: folds were idempotent by refusal when they
    actually needed to be idempotent by repair. Re-points any live mail/mount/thread
    record still aimed at an already-merged dupe, using the same `_move_agent_estate`
    `fold_agent` itself calls, not a second implementation that could drift from what a
    normal fold already does.

    This is the inverse precondition of fold_agent, on purpose, so the two functions'
    refusal conditions never overlap: fold_agent requires status=='active' and refuses a
    merged dupe; reconcile requires dupe.status=='merged' and dupe's own `merged_into`
    pointing at exactly `into` (refuses to redirect a dupe merged into some other agent,
    never guessing which pair a caller means).

    Never re-performs the fold: no `merge_objects` call, no same-lineage/actively-seated
    checks (those decide whether a fold should happen; this object already is folded, so
    the only question is whether its record move finished). Unmerge-then-remerge is not
    a substitute for this function: `unfold_agent`'s own `estate_unreturnable` path
    would report, and drop, exactly the mail/mount items a partial fold already broke.

    Enforces the same actor gate as fold_agent: a repair action touching an already-
    merged agent's records needs the same authority as making the merge, not less.
    Repairing is the same act, continued, never a lesser one.

    Refuses loudly on: unauthorized actor; blank dupe/into; dupe==into; dupe not
    resolving to an Agent; dupe.status != 'merged' (fold_agent's job, not this one's);
    dupe's own `merged_into` not equal to `into`'s id; into not resolving to an active
    Agent."""
    from src.orchestrator.charter import is_operator_actor

    dupe, into = (dupe or "").strip(), (into or "").strip()
    if not await is_operator_actor(actions.pool, actor) and actor != _SANCTIONED_AUTO_FOLD_ACTOR:
        return {"error": f"{actor!r} is not authorized to reconcile an agent fold: same "
                         "gate as fold_agent itself: repairing a merge needs "
                         "the same authority as making one"}
    if not dupe or not into:
        return {"error": "reconcile_agent_fold needs both labels: dupe and into"}
    if dupe == into:
        return {"error": "dupe and into name the same agent: nothing to reconcile"}
    row = await actions.pool.fetchrow(
        "SELECT id, status, merged_into FROM objects WHERE canonical=$1 AND type='Agent'",
        dupe)
    if row is None:
        return {"error": f"no such agent: {dupe!r}: reconcile never invents a label"}
    if row["status"] != "merged":
        return {"error": f"{dupe} is {row['status']}, not merged: reconcile_agent_fold "
                         "only repairs an ALREADY-completed fold; use merge to fold it in "
                         "the first place"}
    into_row = await actions.pool.fetchrow(
        "SELECT id, status FROM objects WHERE canonical=$1 AND type='Agent'", into)
    if into_row is None:
        return {"error": f"no such agent: {into!r}: reconcile never invents a label"}
    if into_row["id"] != row["merged_into"]:
        actual = await actions.pool.fetchval(
            "SELECT canonical FROM objects WHERE id=$1", row["merged_into"])
        return {"error": f"{dupe} is merged into {actual}, not {into}: "
                         "reconcile_agent_fold never redirects to a different pair"}
    if into_row["status"] != "active":
        return {"error": f"{into} is {into_row['status']}, not active"}
    estate = await _move_agent_estate(actions, dupe, into, actor)
    return {"reconciled": dupe, "into": into, **estate}


async def fold_justification_and_parity_check(
    pool: asyncpg.Pool, object_id: uuid.UUID, because: str, *, label: str,
) -> tuple[asyncpg.Record | None, str, dict[str, Any] | None]:
    """The authorization-parity heuristic, kept as one shared copy instead of being
    duplicated byte-for-byte across unfold_agent/unfold_project/unfold_seat: fetches the
    most recent 'merge' event for `object_id` and refuses an unfold whose own `because`
    doesn't also carry the operator's word when the original fold's own justification
    did. A fold the operator blessed by name is not quietly undone by a different hand's
    say-so; it takes the same authority to reverse it that it took to make it. (This is a
    heuristic, not NLP: "cites the operator" means the word 'operator' appears in the
    justification text.)

    Returns `(ev, original_evidence, error)`: `ev` is the raw event row (or None if no
    merge event is on record) and `original_evidence` its justification text, both
    returned unconditionally, not only on success, because every one of the three call
    sites reuses `ev["actor"]`/`ev["created_at"]`/`original_evidence` in its own report
    after this check. A second fetch was the alternative, and this avoids it. `error` is
    an `{"error": ...}` dict when parity fails, else `None` (proceed normally)."""
    ev = await pool.fetchrow(
        "SELECT payload, actor, created_at FROM object_events "
        "WHERE event_type='merge' AND related_id=$1 ORDER BY created_at DESC LIMIT 1",
        object_id)
    original_evidence = str((ev["payload"] or {}).get("justification", "")) if ev else ""
    if "operator" in original_evidence.lower() and "operator" not in because.lower():
        return ev, original_evidence, {
            "error": f"{label}'s fold was justified by citing the operator's word "
                     f"({original_evidence!r}): an unfold needs the operator's word "
                     "too; add it to `because` or get it first"}
    return ev, original_evidence, None


async def unfold_agent(
    actions: Actions, *, dupe: str, because: str, actor: str, execute: bool = False,
) -> dict[str, Any]:
    """Reverse a wrongful fold: the promise `fold_agent`'s own docstring makes
    ("reversible by compensating event"), implemented here after having been missing for
    some time. Dry run is the default (`execute=False`): returns the exact plan, the
    kernel unmerge, any chain-integrity fix, and the record items that can't cleanly
    return, without writing anything. `execute=True` performs it.

    Refuses loudly (an error dict, nothing written) when: `because` is blank; `dupe` is
    unknown or not currently folded (status != 'merged', nothing to unfold); the original
    fold's own justification cites the operator's word and `because` does not also carry
    a fresh one. A fold the operator blessed by name is not quietly undone by a different
    hand's say-so; it takes the same authority to reverse it that it took to make it.
    (Heuristic, not NLP: "cites the operator" means the word 'operator' appears in the
    original justification text.)

    Chain integrity: if `dupe`'s own `succeeded_by` currently points at a label from a
    different lineage (a cross-base pointer; successors are always same-base, by
    `_generation`'s own definition), that pointer is the fold's other half, a stand-in
    link rather than a real succession, and gets cleared (asserted empty, superseding;
    the old value stays on the record, never deleted) so `dupe` reads as its own
    lineage's tail again. A same-base successor is never touched: real succession is not
    this function's business.

    Associated records: `fold_agent`'s mail/mount transfer is a raw UPDATE, not an event.
    The original `to_agent`/`agent_id` is gone the moment it moves, so nothing can prove
    which pre-fold messages or mount rows were `dupe`'s versus already the living head's
    own. Reported as `estate_unreturnable` for a human to read and judge, never guessed
    back. Thread ownership, by contrast, moved via `assert_property` (event-sourced): any
    thread whose current owner is the fold's living head but whose superseded owner
    assertion names `dupe` is cleanly reversible, and `execute=True` re-asserts it."""
    from datetime import UTC, datetime

    from src.orchestrator.agents import _generation

    dupe, because = (dupe or "").strip(), (because or "").strip()
    if not because:
        return {"error": "an unfold without a because is an un-audited reversal: cite "
                         "the evidence/ruling that proves the fold was wrong"}
    if not dupe:
        return {"error": "unfold_agent needs a dupe label"}
    row = await actions.pool.fetchrow(
        "SELECT id, status, merged_into FROM objects WHERE canonical=$1 AND type='Agent'",
        dupe)
    if row is None:
        return {"error": f"unknown agent: {dupe}: an unfold never invents a label"}
    if row["status"] != "merged":
        return {"error": f"{dupe} is not folded (status={row['status']}): nothing to "
                         "unfold"}
    into_canon = await actions.pool.fetchval(
        "SELECT canonical FROM objects WHERE id=$1", row["merged_into"])
    ev, original_evidence, parity_error = await fold_justification_and_parity_check(
        actions.pool, row["id"], because, label=dupe)
    if parity_error is not None:
        return parity_error
    head = await living_head(actions.pool, str(into_canon))
    fold_time = ev["created_at"] if ev else datetime.now(UTC)

    # Chain integrity: a cross-base succeeded_by is the fold's other half
    cur_base = _generation(dupe)[0]
    succ = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='succeeded_by' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        row["id"])
    stitch = bool(succ) and _generation(str(succ))[0] != cur_base

    # Associated records: mail/mounts moved by a raw UPDATE (no event) can't be proven
    # back; thread ownership moved via assert_property (event-sourced) can be
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
            "note": ("pre-fold UPDATEs overwrote to_agent/agent_id in place: these "
                     "predate the fold and still sit on the living head, but nothing "
                     "proves they were EVER addressed to the dupe rather than already "
                     "the head's own; read them and judge by hand, never auto-moved")
                    if (unreturnable_mail or unreturnable_mounts) else
                    "none found: no pre-fold mail or mount rows sit unclaimed on the head",
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
        "note": (f"{dupe} is active again: provenance for the folded era stays on the "
                 f"record (the merge event and same_as link are witnesses, never erased). "
                 + (f"succeeded_by cleared; {dupe} is its own lineage's tail. "
                    if stitch else "")
                 + f"{len(reversible_threads)} thread(s) re-owned. "
                 + ("Unreturned items are listed above for a human to judge by "
                    "hand." if (unreturnable_mail or unreturnable_mounts) else "")),
    })
    return report


async def find_agent_fold_candidates(
    pool: asyncpg.Pool, *, projects_root: Path | None = None,
    jobs_home: Path | None = None,
) -> dict[str, Any]:
    """Mines the registry and the disk for anonymous agents that the evidence says were
    never distinct agents, and queues them as review-gated merge_candidates. Proposals
    only: nothing folds here; a human judges the tray (flag, never guess, applied at
    census scale, the same principle behind the duplicate-detection lint). Two evidence
    classes this pass:

    View-alias (score .9): an anonymous agent whose whole presence is a mount row with no
    transcript for its sid anywhere and no state.json in its jobs dir, a doorbell ring,
    co-resident at the same cwd with a session that has a running process. Propose fold
    into that session's agent.

    Restart-mint (score .75 single-seat / .55 multi-seat): an anonymous agent mounted at
    a cwd where a named lineage anchors. The single-seat presumption: in a one-seat
    project the anonymous agent is presumed the seat's child or fold; where several
    seats share the project it is nuanced and the low score says verify by hand.

    Charter-match (same scores): when no named lineage anchors at the anonymous agent's
    cwd (office migrations moved every seat's mount row home to
    ~/.osiris/seats/<handle>, stranding anonymous agents in the seats' old project
    rooms), the room's seat is still a graph fact: a named lineage holding a live
    works_in or governs link to repo:<project>. Mount rows move house; the charter does
    not. A room whose charter names no seat proposes nothing (its anonymous agents are
    the visitor class, demotion candidates for the visitor gate, never folds) and is
    counted in `seatless`.

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
                               f"{osid} has an active session"]
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
                # The single-seat presumption: for an anonymous agent mounted in a project
                # with only one seat, assume it was a child or a fold of the only agent
                # there; for a project with two or more seats it's more nuanced.
                seats_here = await pool.fetchval(
                    "SELECT count(DISTINCT h.value #>> '{}') FROM agent_mounts m2 "
                    "JOIN objects ho2 ON (m2.agent_id = ho2.canonical "
                    "     OR m2.agent_id LIKE ho2.canonical||'-%') "
                    "JOIN current_assertions h ON h.object_id=ho2.id AND h.name='handle' "
                    "WHERE m2.project = $1", r["project"]) or 0
                target, cls = str(named), "restart-mint"
                if int(seats_here) <= 1:
                    score = 0.75
                    signals = [f"anonymous mount at {cwd}: {named} is the project's "
                               "ONLY seat, so this session is presumed its child or fold "
                               "(the single-seat rule)"]
                else:
                    score = 0.55
                    signals = [f"anonymous mount at {cwd}, the anchor of named lineage "
                               f"{named}, but {seats_here} seats share this project; "
                               "nuanced, verify by hand"]
        if target is None and r["project"]:
            # The charter match: nobody named anchors at this cwd, but the room may
            # still have a seat on the graph's record. Mount rows are mortal and move
            # house (the office migrations); works_in/governs edges are the durable
            # evidence of whose room this is.
            # governs' from_type used to span both Agent (pre-rekey) and Seat (post-
            # rekey) so the rekey wouldn't silently stop matching every governs link the
            # moment it landed; that gap was caught before it shipped.
            # Seat-only now: a prior rekey moved charter authority onto the Seat
            # entirely, so a legacy Agent-origin governs edge is no longer a live
            # declaration of anything, it's pre-rekey history an agent never
            # re-confirmed post-migration. Matching it here let a batch of garbled,
            # bulk-seeded fragments ('repo:?', hyphen-split junk, never migrated) keep
            # outranking real residents in several rooms; charter_for/set_charter are
            # already blind to these edges by construction (they can't see, heal, or
            # interact with an Agent-origin row at all), so this tie-break was the one
            # place still reading them. works_in stays Agent-only (a room's live
            # resident, never rekeyed); governs is Seat-only (a room's declared
            # governor); a Seat-origin edge is resolved to its current holder below,
            # before it ever reaches the bookkeeping of distinct agents.
            from src.orchestrator.seats import seat_occupancy

            holders = await pool.fetch(
                "SELECT DISTINCT fo.canonical AS holder, fo.type AS holder_type, "
                "l.type AS via "
                "FROM links l "
                "JOIN objects fo ON fo.id=l.from_id "
                "  AND ((l.type='works_in' AND fo.type='Agent') "
                "    OR (l.type='governs' AND fo.type='Seat')) "
                "JOIN objects ro ON ro.id=l.to_id "
                "WHERE ro.canonical = 'repo:' || $1 "
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
                # key distinct agents by the living head's base: a rebased lineage is
                # one agent, not two, and must not deflate the single-seat score
                head_ = await living_head(pool, _generation(str(holder_agent))[0])
                base = _generation(head_)[0]
                if base != _generation(mine)[0]:
                    souls.setdefault(base, set()).add(str(h["via"]))
            if not souls:
                seatless[r["project"]] = seatless.get(r["project"], 0) + 1
                continue
            # Declared beats derived. This reverses this tie-break's prior rule, which
            # ranked the room's resident (works_in) above a supervising charter (governs)
            # on the reasoning that a managing seat governs a room, but the room's
            # anonymous agents presumptively belong to the seat that lives there. That
            # reasoning is superseded, not forgotten: a later review found the opposite
            # holds on the two hardest real cases in the fleet, where a managing seat's
            # declared charter correctly named the true owners when both seats' own
            # derived/resident pins were wrong. A charter is a deliberate, reviewed
            # declaration; residence is wherever a session happened to mount. The room's
            # declared governor now outranks its resident.
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
                signals = [f"anonymous mount in room '{r['project']}' at {cwd}: no "
                           f"named lineage anchors there (the seat's row moved home at "
                           f"the office migration), but the graph's charter answers: "
                           f"{target} ({'/'.join(sorted(souls[base]))} "
                           f"repo:{r['project']}) is the room's ONLY seat: the "
                           "single-seat rule"]
            else:
                roster = ", ".join(f"{b} ({'/'.join(sorted(v))})"
                                   for b, v in sorted(souls.items()))
                score = 0.55
                signals = [f"anonymous mount in room '{r['project']}' at {cwd}: no "
                           f"named lineage anchors there; the charter names "
                           f"{len(souls)} distinct agents for this room [{roster}], declared "
                           f"governor presumed: {target}, nuanced, verify by hand"]
        if target is None:
            continue
        trow = await pool.fetchrow(
            "SELECT id, status FROM objects WHERE canonical=$1 AND type='Agent'", target)
        if trow is None or trow["status"] != "active":
            continue
        if frozenset((r["oid"], trow["id"])) in sup:
            continue
        # the table orders pairs by uuid (CHECK a_id < b_id); the roles live in reasons
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
    # labels the census can no longer resolve (folded meanwhile) would confuse the tray;
    # they are stamped by fold_agent itself, so pending here is always actionable
    from src.orchestrator.succession_repair import unresumed_heads
    succession = await unresumed_heads(pool)
    return {"examined": len(anons), "proposed": proposed, "pending": pending,
            "seatless": seatless, "unresumed_heads": succession,
            "note": "proposals only: judge each with resolve_fold_candidate (merged | "
                    "rejected); a rejection is remembered and never re-proposed; "
                    "`seatless` counts anons in rooms whose charter names NO seat: "
                    "visitor-gate demotion candidates, not folds. `unresumed_heads` is a "
                    "SEPARATE, NON-FOLD class (see the module docstring for the full "
                    "reasoning), never resolved via resolve_fold_candidate, a "
                    "human judgment call every time."}


async def resolve_fold_candidate(
    actions: Actions, *, candidate_id: int, decision: str, actor: str,
) -> dict[str, Any]:
    """Judge one agent-fold proposal from the tray. 'merged' executes fold_agent, the
    fold that carries the agent's associated records along, never the bare kernel merge
    (an agent folded without its mail, rows, and threads is the orphan machine again),
    and inherits fold_agent's own operator-actor gate unchanged. (This used to have the
    exact self-approval gap the founding rules forbid, an agent proposing and judging its
    own candidate, since fixed by fold_agent's own check, not a second copy here.)
    'rejected' mints not_same_as both ways and the pair is never re-proposed, open to any
    mounted caller, deliberately: rejecting is a judgment that two things are not the
    same agent, never an identity mutation, so it carries none of 'merged's blast radius
    and needs none of its gate. Either way the candidate row is stamped with the judge's
    name. The entity tray's counterpart (resolution.resolve_candidate) stays for
    Person/Company; this one exists because agents carry these extra associated
    records."""
    row = await actions.pool.fetchrow(
        "SELECT c.id, c.resolved, c.reasons FROM merge_candidates c WHERE c.id=$1",
        candidate_id)
    if row is None:
        return {"error": f"candidate {candidate_id} not found"}
    if row["resolved"] is not None:
        return {"error": f"candidate {candidate_id} already {row['resolved']}"}
    reasons = row["reasons"] or {}
    if reasons.get("kind") != "agent-fold":
        return {"error": f"candidate {candidate_id} is not an agent-fold proposal: "
                         "judge it in the entity tray (resolution.resolve_candidate)"}
    if decision == "merged":
        # the pair's roles live in reasons; the table's columns are uuid-ordered
        signals = "; ".join(reasons.get("signals") or []) or "approved from the tray"
        out = await fold_agent(actions, dupe=str(reasons.get("dupe") or ""),
                               into=str(reasons.get("into") or ""),
                               evidence=f"approved candidate {candidate_id}: {signals}",
                               actor=actor)
        if "error" in out:
            return out  # the row stays unresolved: a refused fold is not a judgment
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
                "note": "pair linked not_same_as: never re-proposed"}
    return {"error": f"decision must be 'merged' or 'rejected', got {decision!r}"}

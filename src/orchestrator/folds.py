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
from typing import Any

import asyncpg

from src.actions.core import Actions

_log = logging.getLogger("osiris.folds")


async def living_head(pool: asyncpg.Pool, agent_id: str) -> str:
    """The lineage's freshest generation — where a folded estate LANDS. The mount
    registry decides (it is bumped by every act); an unregistered lineage's head is the
    given label itself. Estate must never land on a dead generation: succession's own
    transfer (mint_heir) would just have to move it again."""
    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    head = await pool.fetchval(
        "SELECT agent_id FROM agent_mounts WHERE agent_id=$1 OR agent_id LIKE $1 || '-%' "
        "ORDER BY last_seen DESC NULLS LAST LIMIT 1", base)
    return str(head) if head else agent_id


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


async def fold_agent(
    actions: Actions, *, dupe: str, into: str, evidence: str, actor: str,
) -> dict[str, Any]:
    """Fold agent `dupe` into agent `into`: the kernel merge (event, projection, same_as
    link) plus the estate — unread mail re-addressed to `into`'s LIVING HEAD, mount rows
    re-pointed, owned threads re-owned (evented via assert_property, never UPDATEd).

    Refuses LOUDLY (an error dict, nothing written) when: evidence is empty; either label
    is unknown or not an Agent; dupe==into or same lineage (generations are SUCCESSION,
    not duplication — folding one would collapse a death boundary the mind ruling keeps);
    dupe actively holds a Seat (transfer the seat first — a deliberate act, never a side
    effect); dupe is already folded. `into` may be any generation — the estate finds the
    living head regardless."""
    from src.orchestrator.agents import _generation

    dupe, into = (dupe or "").strip(), (into or "").strip()
    if not (evidence or "").strip():
        return {"error": "a fold without evidence is an auto-merge wearing a signature — "
                         "cite the transcripts/census/timing that prove one mind"}
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
    # the kernel merge: event, projection, same_as, case union, audit — resolve-on-read
    await actions.merge_objects(by_label[into]["id"], by_label[dupe]["id"],
                                justification=evidence, actor=actor)
    # THE ESTATE — the same three surfaces succession transfers, landing on the living head
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
    from datetime import UTC, datetime
    now = datetime.now(UTC)
    for t in threads:
        await actions.assert_property(t["id"], "owner", head, actor, now, 0.9,
                                      evidence_class="self_declared")
    _log.info("fold: %s → %s (head %s): mail %d, rows %d, threads %d",
              dupe, into, head, mail_moved, rows_moved, len(threads))
    return {
        "folded": dupe, "into": into, "living_head": head,
        "mail_readdressed": mail_moved, "mount_rows_repointed": rows_moved,
        "threads_reowned": len(threads), "evidence": evidence,
        "note": (f"{dupe} is folded into {into} — its words stay its own (provenance "
                 "resolves through merged_into at read); its unread mail, mount rows, "
                 f"and open threads now belong to {head}. Reversible by compensating "
                 "event; nothing was deleted."),
    }

"""Merge / unmerge: the single symmetric pair replacing fold_agent + fold_seat +
fold_project (three dupe/into/evidence merges) and unfold_agent (their one, Agent-only
reversal). The goal, per the governing decision, was merge/unmerge parity: not
collapsed to save tokens, but for findability and consistency.

Self-typing target: `dupe`'s own string form indicates its type: 'agent:' maps to
Agent, 'seat:' maps to Seat, anything else maps to SoftwareProject. SoftwareProject is
the one type whose canonical resolution already tolerates a bare id, short id, or bare
name, unchanged from fold_project's own `_resolve_project_ref`. Agent and Seat
canonicals in this graph always carry their prefix, so the "else" branch is exactly as
unambiguous as the two prefixed cases: it never has to guess across types, avoiding
the failure mode that undermined an earlier attempt to collapse the AMEND family of
functions. merge()/unmerge() dispatch on this one rule and delegate straight to each
type's own, unmodified fold_X/unfold_X implementation: nothing about any of the four
underlying functions changed in this rework; only the entry point did.

Type-specific refusals are unioned, none cut: every refusal any of
fold_agent/fold_seat/fold_project/unfold_agent already had is still reachable,
unchanged, through the new entry point. The one deliberately new refusal only this
rework could produce, dupe and into resolving to different types, was structurally
unreachable before (you could never call fold_agent with a Seat ref; it only ever
queried type='Agent'). See merge()'s own docstring.

The holder-liveness contradiction is preserved, not reconciled, by explicit design
choice: fold_agent refuses an actively-seated dupe, since a seat transfer is a
deliberate act, never a fold's side effect, while fold_seat's whole job is moving
active holders. Both stay true, unreconciled: they answer different questions for
different types. An Agent merge is never a seat-transfer back door; a Seat merge's
whole reason to exist is moving holders. merge() does not paper over this; it
dispatches to whichever behavior is correct for the type at hand.

The actor-gate asymmetry is also preserved and also not reconciled, named here for
the record (found by applying the standard authority checklist to this change, not by
a fresh audit): fold_agent is enforced as operator-or-sanctioned-reaper-only;
fold_seat and fold_project carry no actor gate at all, so any mounted caller may fold
a Seat or a SoftwareProject duplicate today. This rework does not touch that asymmetry
in either direction: it is a parity change, not a fresh authority review. merge() for
an Agent stays gated exactly as fold_agent always was; merge() for a Seat or Project
stays open exactly as fold_seat/fold_project always were. This is reported here, not
fixed: a candidate for a future authority pass, not this one.
"""
from __future__ import annotations

from typing import Any

from src.actions.core import Actions


def _merge_type(ref: str) -> str:
    """The self-typing rule merge()/unmerge() both dispatch on. See this module's own
    docstring for why the "else" branch is exactly as unambiguous as the two prefixed
    cases, given this graph's own canonical naming convention."""
    if ref.startswith("agent:"):
        return "Agent"
    if ref.startswith("seat:"):
        return "Seat"
    return "SoftwareProject"


async def merge(actions: Actions, *, dupe: str, into: str, evidence: str, actor: str,
                force: bool = False, because: str | None = None,
                ) -> dict[str, Any]:
    """Fold `dupe` into `into`, replacing fold_agent/fold_seat/fold_project as the one
    entry point for all three. Type is read off `dupe`'s and `into`'s own form (see this
    module's docstring); each type's fold runs completely unchanged, with every refusal it
    already had: thin evidence, dupe==into, unknown or already-folded labels, an
    actively-seated Agent dupe, a contradicting SoftwareProject pair, an unauthorized
    actor for an Agent merge. All of these are unioned, none cut.

    The one new refusal, never reachable before this rework because no single function
    ever spanned two types: `dupe` and `into` resolving to different types. This is named
    plainly rather than falling through to a type-specific "unknown X" message that would
    not say why.

    The liveness guard applies to SoftwareProject folds only; fold_agent/fold_seat are out
    of its scope. A caller folding their own project stays fully open; folding a project
    that a different lineage's live session currently has mounted refuses by default.
    `force=True` (requires a non-empty `because`) is the deliberate override. `force`/
    `because` are passed through to fold_project only and ignored for Agent/Seat merges."""
    from src.orchestrator.folds import fold_agent
    from src.orchestrator.projects import fold_project
    from src.orchestrator.seats import fold_seat

    dupe_s, into_s = (dupe or "").strip(), (into or "").strip()
    dupe_type, into_type = _merge_type(dupe_s), _merge_type(into_s)
    if dupe_type != into_type:
        return {"error": f"dupe {dupe_s!r} looks like a {dupe_type} and into {into_s!r} "
                         f"looks like a {into_type}; merge is same-type only. This "
                         "cross-type pairing was never reachable through any of the "
                         "three original fold functions"}
    if dupe_type == "Agent":
        return await fold_agent(actions, dupe=dupe_s, into=into_s, evidence=evidence,
                                actor=actor)
    if dupe_type == "Seat":
        return await fold_seat(actions, dupe=dupe_s, into=into_s, evidence=evidence,
                               actor=actor)
    return await fold_project(actions, dupe=dupe_s, into=into_s, evidence=evidence,
                              actor=actor, force=force, because=because)


async def unmerge(actions: Actions, *, dupe: str, because: str, actor: str,
                  execute: bool = False) -> dict[str, Any]:
    """Reverse a wrongful merge, replacing unfold_agent as the one entry point for all
    three types, closing the parity gap noted earlier: before this, only an Agent fold
    was ever reversible; a Seat or Project fold was permanent. Type is read off `dupe`'s
    own form, same rule as merge(). Dry run is the default (`execute=False`) for every
    type: review the plan, then call again with `execute=True`. Every refusal each
    type's own unfold already had (blank `because`, an unknown or never-folded dupe, an
    operator-blessed fold needing the operator's word again to reverse) is still
    reachable, unchanged, through this one entry point."""
    from src.orchestrator.folds import unfold_agent
    from src.orchestrator.projects import unfold_project
    from src.orchestrator.seats import unfold_seat

    dupe_s = (dupe or "").strip()
    t = _merge_type(dupe_s)
    if t == "Agent":
        return await unfold_agent(actions, dupe=dupe_s, because=because, actor=actor,
                                  execute=execute)
    if t == "Seat":
        return await unfold_seat(actions, dupe=dupe_s, because=because, actor=actor,
                                 execute=execute)
    return await unfold_project(actions, dupe=dupe_s, because=because, actor=actor,
                                execute=execute)


async def reconcile_merge(actions: Actions, *, dupe: str, into: str, actor: str,
                          ) -> dict[str, Any]:
    """The repair function this area of the system needed, which never existed for any
    type before this build. `reconcile_project_fold` shipped the SoftwareProject half of
    that need earlier, but was never given an MCP tool of its own; Agent and Seat had no
    repair path at all. Accepts an already-merged `dupe` and re-points whatever remaining
    state is still aimed at it, without re-performing the merge. This is an
    idempotent-by-repair operation, distinct from `merge`'s idempotent-by-refusal: a
    second `merge` call on an already-folded dupe correctly does nothing, while
    `reconcile_merge` is for state that the first fold left stranded.

    Calling `unmerge` then `merge` again is not a substitute: `unmerge`'s own
    `estate_unreturnable` path reports, and drops, exactly the links a partial fold
    already broke, since a raw update erases which pre-fold item was ever provably the
    dupe's own.

    Type is read off `dupe`'s own form, same rule as `merge`/`unmerge`. Refuses: `dupe`
    and `into` resolving to different types; `dupe` not merged (that's `merge`'s job, not
    this one's); `dupe`'s own `merged_into` pointing at a different `into` (it never
    redirects to a pair the caller didn't name); `into` not active. The Agent branch is
    actor-gated exactly like `fold_agent`/`merge`, since repairing a merge needs the same
    authority as making one; Seat and Project stay open, matching their own fold_X's
    current posture, left unreconciled on purpose, same as `merge` itself."""
    from src.orchestrator.folds import reconcile_agent_fold
    from src.orchestrator.projects import reconcile_project_fold
    from src.orchestrator.seats import reconcile_seat_fold

    dupe_s, into_s = (dupe or "").strip(), (into or "").strip()
    dupe_type, into_type = _merge_type(dupe_s), _merge_type(into_s)
    if dupe_type != into_type:
        return {"error": f"dupe {dupe_s!r} looks like a {dupe_type} and into {into_s!r} "
                         f"looks like a {into_type}; reconcile is same-type only"}
    if dupe_type == "Agent":
        return await reconcile_agent_fold(actions, dupe=dupe_s, into=into_s, actor=actor)
    if dupe_type == "Seat":
        return await reconcile_seat_fold(actions, dupe=dupe_s, into=into_s, actor=actor)
    return await reconcile_project_fold(actions, dupe=dupe_s, into=into_s, actor=actor)

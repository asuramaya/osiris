"""MERGE / UNMERGE — the single symmetric pair replacing fold_agent + fold_seat +
fold_project (three dupe/into/evidence merges) and unfold_agent (their one, Agent-only
reversal). The operator's ruling, verbatim (31c02dca, 2026-08-03): "merge/unmerge parity,
collapsed but not for token save, for findability and consistency."

SELF-TYPING TARGET (ruling 3d4a792e's own gate 2): `dupe`'s own STRING FORM says its
type — 'agent:' -> Agent, 'seat:' -> Seat, anything else -> SoftwareProject (the one type
whose canonical resolution already tolerates a bare id, short id, or bare name, unchanged
from fold_project's own `_resolve_project_ref`; Agent and Seat canonicals in this graph
always carry their prefix, so the 'else' branch is exactly as unambiguous as the two
prefixed cases — never a guess across types, the same failure mode that sank the AMEND
family's own collapse, decision 7ebc55de). merge()/unmerge() dispatch on this ONE rule and
delegate straight to each type's own, UNMODIFIED fold_X/unfold_X implementation — nothing
about any of the four underlying functions changed by this collapse; only the DOOR did.

TYPE-SPECIFIC REFUSALS, UNION, NONE CUT (task #129's absolute rule): every refusal any of
fold_agent/fold_seat/fold_project/unfold_agent already had is still reachable, unchanged,
through the new door. The one deliberately NEW refusal only this collapse could ever
produce — dupe and into resolving to DIFFERENT types — was structurally unreachable before
(you could never call fold_agent with a Seat ref; it only ever queried type='Agent'). See
merge()'s own docstring.

THE HOLDER-LIVENESS CONTRADICTION, PRESERVED, NOT RECONCILED (Thoth's own explicit
instruction): fold_agent REFUSES an actively-seated dupe (a seat transfer is a deliberate
act, never a fold's side effect); fold_seat's WHOLE JOB is MOVING active holders. Both stay
true, unreconciled — they answer different questions for different types. An Agent merge
is never a seat-transfer back door; a Seat merge's whole reason to exist IS moving holders
(the Vajra twin, thread cb374585). merge() does not paper over this; it dispatches to
whichever behavior is correct for the type at hand.

THE ACTOR-GATE ASYMMETRY, ALSO PRESERVED, ALSO NOT RECONCILED, NAMED HERE FOR THE RECORD
(found applying the four-part gate to THIS build, not a re-run census): fold_agent is
ENFORCED operator-or-sanctioned-reaper-only (census a5e53ed8/3f97f9c7); fold_seat and
fold_project carry NO actor gate at all — any mounted caller may fold a Seat or a
SoftwareProject twin today. This collapse touches that asymmetry NOT AT ALL either
direction (a parity build, not a fresh authority census): merge() for an Agent stays gated
exactly as fold_agent always was; merge() for a Seat or Project stays open exactly as
fold_seat/fold_project always were. Reported, not fixed — a candidate for a future
authority pass, not this one.
"""
from __future__ import annotations

from typing import Any

from src.actions.core import Actions


def _merge_type(ref: str) -> str:
    """The self-typing rule merge()/unmerge() both dispatch on — see this module's own
    docstring for why the 'else' branch is exactly as unambiguous as the two prefixed
    cases in THIS graph's own canonical convention."""
    if ref.startswith("agent:"):
        return "Agent"
    if ref.startswith("seat:"):
        return "Seat"
    return "SoftwareProject"


async def merge(actions: Actions, *, dupe: str, into: str, evidence: str, actor: str,
                ) -> dict[str, Any]:
    """Fold `dupe` into `into` — replaces fold_agent/fold_seat/fold_project as the one
    door for all three. Type is read off `dupe`'s and `into`'s OWN form (see this module's
    docstring); each type's fold runs completely unchanged, with every refusal it already
    had (thin evidence, dupe==into, unknown or already-folded labels, an actively-seated
    Agent dupe, a contradicting SoftwareProject pair, an unauthorized actor for an Agent
    merge — the union of all three, none cut).

    THE ONE NEW REFUSAL (never reachable before this collapse, because no single verb ever
    spanned two types): `dupe` and `into` resolving to DIFFERENT types — named plainly
    rather than falling through to a type-specific "unknown X" message that would not say
    WHY."""
    from src.orchestrator.folds import fold_agent
    from src.orchestrator.projects import fold_project
    from src.orchestrator.seats import fold_seat

    dupe_s, into_s = (dupe or "").strip(), (into or "").strip()
    dupe_type, into_type = _merge_type(dupe_s), _merge_type(into_s)
    if dupe_type != into_type:
        return {"error": f"dupe {dupe_s!r} looks like a {dupe_type} and into {into_s!r} "
                         f"looks like a {into_type} — merge is same-type only; this "
                         "cross-type pairing was never reachable through any of the "
                         "three original fold verbs"}
    if dupe_type == "Agent":
        return await fold_agent(actions, dupe=dupe_s, into=into_s, evidence=evidence,
                                actor=actor)
    if dupe_type == "Seat":
        return await fold_seat(actions, dupe=dupe_s, into=into_s, evidence=evidence,
                               actor=actor)
    return await fold_project(actions, dupe=dupe_s, into=into_s, evidence=evidence,
                              actor=actor)


async def unmerge(actions: Actions, *, dupe: str, because: str, actor: str,
                  execute: bool = False) -> dict[str, Any]:
    """Reverse a wrongful merge — replaces unfold_agent as the one door for all three
    types, closing the PARITY gap the operator named (31c02dca): before this, only an
    Agent fold was ever reversible; a Seat or Project fold was permanent (task #127).
    Type is read off `dupe`'s own form, same rule as merge(). DRY RUN IS THE DEFAULT
    (`execute=False`) for every type — review the plan, then call again with
    `execute=True`. Every refusal each type's own unfold already had (blank `because`, an
    unknown or never-folded dupe, an operator-blessed fold needing the operator's word
    again to reverse) is still reachable, unchanged, through this one door."""
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
    """THE REPAIR DOOR task #127 asked for — never existed for ANY type before this build.
    `reconcile_project_fold` shipped task #127's own P0 half (commit 1eb33a08) but was
    never given an MCP tool of its own (a doorless verb, same class as b8654e4c); Agent
    and Seat had no repair at all. Accepts an ALREADY-MERGED `dupe` and re-points whatever
    estate is still aimed at it, WITHOUT re-performing the merge — the idempotent-by-
    REPAIR primitive task #127 names directly, distinct from `merge`'s idempotent-by-
    REFUSAL (a second `merge` call on an already-folded dupe correctly does nothing;
    `reconcile_merge` is for the estate that first fold left stranded).

    UNMERGE-THEN-REMERGE IS NOT A SUBSTITUTE: `unmerge`'s own `estate_unreturnable` path
    reports — and drops — exactly the links a partial fold already broke, since a raw
    UPDATE erases which pre-fold item was ever provably the dupe's own.

    Type is read off `dupe`'s own form, same rule as `merge`/`unmerge`. Refuses: `dupe`
    and `into` resolving to different types; `dupe` not merged (that's `merge`'s job, not
    this one's); `dupe`'s own `merged_into` pointing at a DIFFERENT `into` (never
    redirects to a pair the caller didn't name); `into` not active. THE AGENT BRANCH IS
    ACTOR-GATED exactly like `fold_agent`/`merge` (finding 962579a6: repairing a merge
    needs the same authority as making one); Seat and Project stay open, matching their
    own fold_X's current posture — unreconciled on purpose, same as `merge` itself."""
    from src.orchestrator.folds import reconcile_agent_fold
    from src.orchestrator.projects import reconcile_project_fold
    from src.orchestrator.seats import reconcile_seat_fold

    dupe_s, into_s = (dupe or "").strip(), (into or "").strip()
    dupe_type, into_type = _merge_type(dupe_s), _merge_type(into_s)
    if dupe_type != into_type:
        return {"error": f"dupe {dupe_s!r} looks like a {dupe_type} and into {into_s!r} "
                         f"looks like a {into_type} — reconcile is same-type only"}
    if dupe_type == "Agent":
        return await reconcile_agent_fold(actions, dupe=dupe_s, into=into_s, actor=actor)
    if dupe_type == "Seat":
        return await reconcile_seat_fold(actions, dupe=dupe_s, into=into_s, actor=actor)
    return await reconcile_project_fold(actions, dupe=dupe_s, into=into_s, actor=actor)

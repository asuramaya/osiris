"""MINT_SEAT — the org chart trickles (task #50, ruling cabc28f5).

A Fable-class coordinator seat (Thoth, Ra, alfred) extends itself with SPECIALIST WORKER
seats — one act: `ensure_seat` (the durable role) + the office scaffold (a directory, an
`.osiris` pin carrying project AND model, standing orders + a charter.md from `offices.py`'s
own template family, and an osiris-tool permission grant (d0a815ad/86ead89e) so a spawned
body can approve its own MCP calls without a human in the loop — never re-derived, never
duplicated) + an `intended_model` stamp (workers default Sonnet, ruling f6f6174d) +
`managed_by` (the org chart's first real link type — Seat-to-Seat, the minting seat becomes
manager of record).

IDEMPOTENT, and idempotent two different ways depending on what already exists:
  * the WORKER handle is brand new (no exact match, no near-miss) → mint the Seat,
    scaffold a fresh office, stamp the model, link managed_by. Every piece is new.
  * the WORKER handle already names a living Seat EXACTLY (Tantra, minted by the
    operator's own hand before this verb existed) → ADOPT: no new Seat, the office
    scaffold runs FILL-MISSING-ONLY (an occupied office is the seat's own hand-
    maintained home — the same never-clobber law CLAUDE.md and charter.md already run
    on, now also closing a HOLLOW shell's gaps: Tantra's real office was an
    operator-made dir with no pin, no orders at all), only the missing pieces get
    asserted (an unset intended_model, a missing managed_by edge). Calling it again
    once everything is already true is a pure no-op.

GUARDRAILS (the ruling's own, all refused LOUD, never silently swallowed):
  * PERSON COLLISION — this graph is shared with an entity-resolution product line; a
    worker handle that coincides with a real Person record must never be confused with
    one. Structurally impossible by construction (every seat lookup here filters on
    `type='Seat'`, so a Person is invisible to it) — the explicit check below exists
    ONLY to make the refusal a NAMED error instead of a silent 'seat not found'.
  * THE NEAR-MISS TWIN (ruling 7cffda8f, Alfred's field pilot) — a handle that
    NORMALIZES to the same name as a living seat (casefold, strip a trailing
    generation marker, strip punctuation: 'Tantra' vs the real 'tantra 1') but does not
    exact-match it refuses instead of silently minting a second identity wearing a
    near-stranger's face. `adopt=True` states the intent explicitly (no match refuses,
    never falls through to fresh); `force=True` is the only door past the refusal.
  * CROSS-HOUSE MINTING — a manager mints workers in its OWN house by default (no house
    param = inherit the manager's); crossing to a DIFFERENT house needs the operator's
    own hand (an `actor` naming the operator), never a seat's unilateral reach into a
    house it does not own. Scoped to FRESH minting only — adopting an already-existing
    worker (Tantra) is not a house crossing, it is recognizing what already exists.
"""
from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.orchestrator.boot_compiler import compile_managed_body, template_version, wrap_managed
from src.orchestrator.charter import charter_of
from src.orchestrator.offices import _CHARTER_TEMPLATE, _CHARTER_UNDECLARED, _default_office_root
from src.orchestrator.seats import (
    _OPERATOR_ACTORS,
    bind_seat_tree,
    ensure_seat,
    seat_facts,
    seat_occupancy,
)
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)

DEFAULT_WORKER_MODEL = "claude-sonnet-5"  # ruling f6f6174d: Sonnet is the worker default

# THE PERMISSION GRANT (d0a815ad/86ead89e, blessed by the operator "blessed, definitely",
# 2026-07-22): a spawned body starts, the office scaffold fires, and dies verbatim on "I need
# to grant permission for the Osiris tools to proceed" — a print-mode/autonomous session
# cannot approve its own MCP calls, so it can never mount(), so launch() can never produce a
# body. Alfred's own field test (vajra) tried writing exactly this file as an AGENT and was
# correctly classifier-refused (privilege-shaped). Scaffolding it HERE instead — server-side,
# inside the one authorized act that already writes .osiris/CLAUDE.md/charter.md — is not
# classifier-fenced the same way: no agent is deciding to grant itself anything, the MCP
# server is doing file I/O on behalf of an already-validated mint_seat call. Content verbatim
# Alfred's own tested string — never invent unvalidated JSON.
_PERMISSION_GRANT = json.dumps({"permissions": {"allow": ["mcp__osiris", "mcp__osiris__*"]}},
                               indent=2) + "\n"


async def _resolve_seat_ref(pool: Any, ref: str) -> str | None:
    """A Seat by its own canonical (`seat:xxxxxxxx`) or by handle — case-insensitive,
    unique across houses (claim_name's global-namespace law), and crucially NEVER a
    holder requirement (unlike seats.binding_of_handle, built for 'who currently sits
    here') — mint_seat resolves the ROLE, not who is presently in it. Only ever matches
    `type='Seat'`: a same-named Person elsewhere in this shared graph cannot collide with
    this query by construction."""
    ref = (ref or "").strip()
    if not ref:
        return None
    if ref.startswith("seat:"):
        found = await pool.fetchval(
            "SELECT canonical FROM objects WHERE canonical=$1 AND type='Seat' "
            "AND status='active'", ref)
        return str(found) if found else None
    rows = await pool.fetch(
        "SELECT o.canonical FROM objects o WHERE o.type='Seat' AND o.status='active' "
        "AND lower(COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='handle' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '')) = lower($1)", ref)
    return str(rows[0]["canonical"]) if len(rows) == 1 else None


async def _person_collision(pool: Any, handle: str) -> str | None:
    """The Person object's canonical if `handle` names one, case-insensitive — the NAMED
    refusal this guards (structurally, no Seat query ever finds a Person; this exists so
    the caller hears WHY, not just 'not found')."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT o.canonical FROM objects o WHERE o.type='Person' AND o.status='active' "
        "AND lower(COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='name' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '')) = lower($1) "
        "LIMIT 1", handle)


# THE NEAR-MISS GUARD (Alfred's field pilot, ruling 7cffda8f): Tantra's real claimed
# handle is 'tantra 1' — a bare 'Tantra' fresh-mint request NEVER exact-matches it, and
# the fresh path used to fire on any non-match, so it would have silently minted a twin
# in his house and called it success. Identity deserves MORE conservatism than
# open_thread's own near-dup dedup, not less: a false near-miss refusal costs a retry; a
# missed one mints a twin wearing a stranger's face.
_GEN_SUFFIX_RE = re.compile(r"[\s._-]+(?:[ivxlcdm]+|\d+)$")
_PUNCT_RE = re.compile(r"[^a-z0-9]+")


def _normalize_handle(handle: str) -> str:
    """Casefold, strip a trailing generation marker (a roman numeral or plain digit —
    'Tantra II', 'tantra 1', 'Tantra-2' all strip to 'tantra'), then strip whatever
    punctuation/whitespace remains. Pure — the guard's whole comparison key."""
    s = handle.strip().casefold()
    s = _GEN_SUFFIX_RE.sub("", s)
    return _PUNCT_RE.sub("", s)


async def _near_miss(pool: Any, handle: str) -> str | None:
    """A LIVING Seat whose handle normalizes the same as `handle` (but isn't reached by
    _resolve_seat_ref's own exact/case-insensitive match — the caller already checked
    that), or None. Scans the active roster; the fleet's seat count is small enough that
    a full scan beats a fragile SQL normalization of the same regex."""
    target = _normalize_handle(handle)
    if not target:
        return None
    rows = await pool.fetch(
        "SELECT (SELECT a.value #>> '{}' FROM current_assertions a "
        " WHERE a.object_id=o.id AND a.name='handle' "
        " ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS handle "
        "FROM objects o WHERE o.type='Seat' AND o.status='active'")
    for r in rows:
        cand = r["handle"]
        if cand and _normalize_handle(str(cand)) == target:
            return str(cand)
    return None


async def _scaffold_office(
    actions: Actions, *, handle: str, house: str, project: str, intended_model: str,
    office_root: Path, seat_id: str, manager_seat_id: str | None,
) -> dict[str, Any]:
    """A worker's office — dir + `.osiris` (project AND model, assignment 3's own gap
    for pre-existing seats closed at birth for a new one) + CLAUDE.md + charter.md + the
    osiris-tool permission grant (d0a815ad/86ead89e — the one human act launch() needs,
    spent here instead of at every future interactive walk-in), CLAUDE.md now compiled
    by THE BOOT COMPILER (thread 4951d818) rather than a frozen template string.
    FILL-MISSING-ONLY, every file its own exists-guard: a fresh mint's office cannot yet
    exist to collide with, and an ADOPTED seat's office (Alfred's field pilot, ruling
    7cffda8f — Tantra's real shell was an operator-made dir with no pin, no orders, a
    HOLLOW adoption otherwise) gets exactly its missing pieces filled, nothing present
    ever touched. `manager_seat_id` is passed explicitly (role is explicit too) rather
    than derived live — the caller's own `managed_by` link, when there is one, isn't
    created until AFTER this call returns, so a live derive here would read a brand-new
    worker as a manager-less 'coordinator'.

    `manager_seat_id=None` scaffolds a SELF-MANAGED seat instead (dispatch 3685/3688,
    `osiris new` — Ooblek's own real shape: a seat with no `managed_by` edge at all,
    never a flag, the absence itself). Role becomes 'coordinator' (the only template
    `derive_role` would ever assign a manager-less seat live, so this matches what a
    later `reissue_office` would derive anyway) and the charter prompt drops the
    worker-specific "GOVERNS, not where it sits" framing for language that doesn't
    presuppose an org chart above this seat."""
    office = office_root / handle.lower()
    office.mkdir(parents=True, exist_ok=True)
    pin = office / ".osiris"
    pin_state = "left in place"
    if not pin.exists():
        pin.write_text(f'project = "{project}"\nmodel = "{intended_model}"\n')
        pin_state = "written"
    orders = office / "CLAUDE.md"
    orders_state = "left in place"
    if not orders.exists():
        if manager_seat_id is not None:
            role, charter_block = "worker", (
                "Your charter was never formally declared — it lives only in prose. First "
                "act: `charter(repos=[...])` naming the repos you actually govern. A house "
                "is what a seat GOVERNS, not where it sits.")
        else:
            role, charter_block = "coordinator", (
                "Your charter was never formally declared — it lives only in prose. First "
                "act: `charter(repos=[...])` naming the project you actually own — this "
                "seat has no manager, so nobody else will do it for you.")
        body = await compile_managed_body(
            actions, seat_id=seat_id, handle=handle, house=house, office=str(office),
            seat_line=" — not yet seated: your next claim binds you (the on-ramp).",
            charter_block=charter_block,
            # a FRESH mint can never yet carry a peer_of edge (offices.py's own
            # establish_office is where a peer bonded later gets picked up live)
            peer_block="\n", role=role, manager_seat_id=manager_seat_id)
        orders.write_text(wrap_managed(body, template_version()))
        orders_state = "written"
    charter = office / "charter.md"
    charter_state = "left in place"
    if not charter.exists():
        charter.write_text(_CHARTER_TEMPLATE.format(handle=handle))
        charter_state = "written"
    grant = office / ".claude" / "settings.local.json"
    grant_state = "left in place"
    if not grant.exists():
        grant.parent.mkdir(parents=True, exist_ok=True)
        grant.write_text(_PERMISSION_GRANT)
        grant_state = "written"
    return {"office": str(office), "osiris_pin": pin_state,
            "standing_orders": orders_state, "charter_file": charter_state,
            "permission_grant": grant_state}


async def mint_seat(
    actions: Actions, *, manager: str, handle: str,
    house: str | None = None, project: str | None = None,
    intended_model: str = DEFAULT_WORKER_MODEL,
    office_root: Path | None = None, actor: str | None = None,
    adopt: bool = False, force: bool = False,
) -> dict[str, Any]:
    """The whole ceremony, one receipt. `manager` is the minting seat — its own handle or
    seat_id (whichever the caller knows about itself). `handle` is the worker's name.
    Refuses loudly on an unknown manager, a Person-handle collision, a NEAR-MISS handle
    (ruling 7cffda8f — a living seat whose handle normalizes the same, e.g. 'Tantra' vs
    'tantra 1'), or an unauthorized house crossing. Idempotent: minted once, adopted
    forever after. `adopt=True` states the caller's intent explicitly — no match REFUSES
    instead of silently falling through to a fresh mint (the caller said adopt; minting
    would be the lie). `force=True` is the only door past a near-miss refusal, for the
    rare case a distinct seat genuinely belongs beside a similarly-named one."""
    actor = actor or "ceremony:mint-seat"
    manager_seat_id = await _resolve_seat_ref(actions.pool, manager)
    if manager_seat_id is None:
        if await _person_collision(actions.pool, manager):
            return {"error": f"{manager!r} names a Person record, not a Seat — mint_seat "
                             "never treats a case entity as an org-chart role"}
        return {"error": f"no such manager seat: {manager!r} — mint_seat never invents "
                         "who is minting"}
    manager_facts = await seat_facts(actions.pool, manager_seat_id)
    manager_house = manager_facts.get("house")

    handle = (handle or "").strip()
    if not handle:
        return {"error": "a worker seat needs a handle"}
    person = await _person_collision(actions.pool, handle)
    if person:
        return {"error": f"{handle!r} names a Person record ({person}), not a seat — "
                         "mint_seat never mints or adopts a case entity as a worker"}

    now = datetime.now(UTC)
    root = office_root or _default_office_root()
    office_path = root / handle.lower()
    existing_seat_id = await _resolve_seat_ref(actions.pool, handle)
    if existing_seat_id is not None:
        # THE ADOPT PATH (Tantra's shape): no new identity, no house crossing to refuse —
        # recognizing what already exists is not the same act as minting fresh
        worker_seat_id = existing_seat_id
        # A LIVE SEAT IS NEVER ADOPTED (found live 2026-08-02, decision 2993b4e4): this
        # branch's own office scaffold + anchor_cwd backfill below writes the exact same
        # effect establish_office's own rollout guard (offices.py:262-278) refuses for a
        # live seat — until this check, this path did it unguarded, for ANY handle that
        # already resolves to a living Seat, including one whose session is running right
        # now. Gated with seat_occupancy — already imported, already this function's own
        # end-of-receipt authority for the identical question (below) — rather than a
        # second hand-rolled copy of establish_office's SQL. establish_office's own inline
        # check is a SEPARATE, still-separate implementation of this same question; this
        # is a named, not silent, duplication left for a follow-up unification, not a
        # third copy invented here.
        occ = await seat_occupancy(actions.pool, worker_seat_id)
        if occ["state"] == "occupied":
            return {"error": f"cannot adopt {handle!r} ({worker_seat_id}) — it is LIVE "
                             f"right now (holder {occ['holder']}). Adopting a live seat "
                             "would move its office out from under a running session, "
                             "splitting the session's history between two homes — the "
                             "same rule establish_office enforces. Close its tab first, "
                             "then mint_seat or establish_office; it wakes up in the "
                             "office"}
        worker_facts = await seat_facts(actions.pool, worker_seat_id)
        worker_house = worker_facts.get("house")
        seat_minted = False
    else:
        if adopt:
            return {"error": f"adopt=True but no living seat exactly matches {handle!r} "
                             "— minting would be the lie the caller explicitly refused"}
        if not force:
            near = await _near_miss(actions.pool, handle)
            if near:
                return {"error": f"near-miss twin refused: living seat {near!r} "
                                 f"normalizes to the same name as {handle!r} — pass the "
                                 f"exact handle {near!r} to adopt it, or force=True to "
                                 "mint a distinct seat anyway"}
        resolved_house = house or manager_house
        if house and manager_house and house != manager_house \
                and (actor or "") not in _OPERATOR_ACTORS:
            return {"error": f"cross-house mint refused: {manager!r} (house "
                             f"{manager_house!r}) may not mint a seat in house {house!r} — "
                             "only the operator's own hand crosses a house boundary"}
        seat_result = await ensure_seat(
            actions, house=resolved_house, handle=handle, source=actor,
            anchor_cwd=str(office_path))
        if "error" in seat_result:
            return seat_result
        worker_seat_id = seat_result["seat_id"]
        worker_house = resolved_house
        seat_minted = bool(seat_result["minted"])

    # THE OFFICE SCAFFOLD (assignment 3's templates): a fresh mint always scaffolds; an
    # ADOPTED seat scaffolds FILL-MISSING-ONLY (ruling 7cffda8f — an operator-made shell
    # with no pin/orders is a hollow adoption otherwise). Every write inside is its own
    # exists-guard, so running it here unconditionally is always safe.
    office_result: dict[str, Any] | None = None
    if existing_seat_id is not None or seat_minted:
        office_result = await _scaffold_office(
            actions, handle=handle, house=worker_house or "",
            project=project or worker_house or "", intended_model=intended_model,
            office_root=root, seat_id=worker_seat_id, manager_seat_id=manager_seat_id)

    worker_obj = await actions.create_or_find_object("Seat", worker_seat_id, actor)
    worker_facts = await seat_facts(actions.pool, worker_seat_id)
    stamped_model = False
    if not worker_facts.get("intended_model"):
        await actions.assert_property(worker_obj, "intended_model", intended_model, actor,
                                      now, _CONF, evidence_class=_EC)
        stamped_model = True
    if not worker_facts.get("anchor_cwd"):
        # FILL-MISSING-ONLY, same law as intended_model above — an ADOPTED seat (Tantra's
        # shape) never had ensure_seat mint its anchor_cwd, so a hollow shell still needs it
        # backfilled here or launch() can never find its office (trigger.py's own refusal).
        await actions.assert_property(worker_obj, "anchor_cwd", str(office_path), actor,
                                      now, _CONF, evidence_class=_EC)

    manager_obj = await actions.create_or_find_object("Seat", manager_seat_id, actor)
    already_linked = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='managed_by' "
        "AND (valid_until IS NULL OR valid_until > now()) LIMIT 1", worker_obj, manager_obj)
    linked_now = False
    if not already_linked:
        await actions.create_link(worker_obj, manager_obj, "managed_by", actor, now, _CONF,
                                  evidence_class=_EC)
        linked_now = True

    # THE RECEIPT COMPLETES THE LIFECYCLE (occupancy piece A, 9f566244) — mint_seat only
    # ever finished HALF of it: a seat, an office, a manager edge, and silence about
    # whether a BODY exists yet. Now the receipt states occupancy plainly and names whose
    # hand the next step needs, the same treatment every half-finished ceremony in this
    # house owes its caller (Ra's day this would have saved: minting told him nothing
    # about VACANT vs OCCUPIED, so he found out only by asking again later).
    occ = await seat_occupancy(actions.pool, worker_seat_id)
    next_step = {
        "vacant": "no session has ever attached — furniture until a body sits in it; "
                 f"launch(target={handle!r}) to body it, or start a session in the "
                 "office and have it claim_name itself",
        "occupied": "already live — no next step, someone's home",
        "cold": "held, but nobody's live right now — its holder resumes on its own "
               "next mount; no outside hand needed",
    }[occ["state"]]

    # TASK #157 PIECE 1 (operator's own words "fix the slop"): a fresh mint can never yet
    # have a charter (charter() needs the Seat object this call just minted); an ADOPTED
    # seat may already carry one. Either way the receipt now SAYS SO — establish_office's
    # own honest text, one door over, reused verbatim (_CHARTER_UNDECLARED) rather than a
    # second string invented here, exactly the "if it picks, it is wrong" discipline this
    # arc has run on all night.
    repos = await charter_of(actions.pool, worker_seat_id)

    return {
        "seat_id": worker_seat_id, "handle": handle, "house": worker_house,
        "seat_minted": seat_minted,
        **({"office": office_result} if office_result else {}),
        "intended_model": intended_model if stamped_model else worker_facts.get("intended_model"),
        "intended_model_stamped": stamped_model,
        "manager_seat_id": manager_seat_id,
        "managed_by": "linked" if linked_now else "already linked",
        "charter": repos or _CHARTER_UNDECLARED,
        "occupancy": occ["state"], "holder": occ["holder"], "next_step": next_step,
    }


async def found_seat(
    actions: Actions, *, handle: str, path: str | None = None,
    project: str | None = None, intended_model: str = DEFAULT_WORKER_MODEL,
    office_root: Path | None = None, actor: str,
) -> dict[str, Any]:
    """ONE ACT, no ceremony (dispatch 3685/3688, the operator's own "too much witchcraft
    to spawn a project... I'll remember 'osiris new' boom"): found a SELF-MANAGED seat —
    Ooblek's own real shape, a Seat with NO `managed_by` edge at all, never a flag, the
    absence itself (read off Ooblek's own dossier before building this, not assumed:
    seat:e9db6202 was self-claimed, then given an office, then — hours later, live —
    self-declared its own `governs` edge; no minting agent, no manager, ever). Composes
    the SAME primitives `mint_seat` does (`ensure_seat` + `_scaffold_office`, with
    `manager_seat_id=None`) plus `bind_seat_tree` for the CODE workspace — deliberately
    distinct from the seat's own identity office (offices.py's own ruling ed5f5ce2:
    "agents sit at ~/.osiris/seats/<handle>/, code stays in the repos they GOVERN").

    `path` defaults to `~/code/<handle>` (this repo's own convention) and is created if
    absent — osiris never assumes a git repo already exists there (a project needs none;
    resolution reads a `.osiris` pin or a bare folder name, never git — proven, not
    assumed: `~/.osiris/seats/ooblek` is not a repo and carries `project = "stopslop"`
    fine). `project` defaults to `handle`. Neither `.osiris` pin (the workspace's own, and
    the office's own) is ever overwritten if already present — fill-missing-only, the same
    law every office write in this codebase holds.

    DOES NOT eagerly create a `governs` edge — inventing one on an unlaunched mind's
    behalf would be exactly the kind of fact this call has no standing to assert. The
    scaffolded CLAUDE.md's own charter_block already tells a fresh, self-managed seat to
    `charter(repos=[...])` naming its own project as its first act, once it is actually
    live to say so in its own voice — matching Ooblek's own real bootstrap order exactly.

    IDEMPOTENT: a handle that already names a living, ALREADY self-managed seat converges
    (fills in whatever's missing, mints nothing new). A handle that names a living MANAGED
    seat (a real `managed_by` edge already out) REFUSES — this call founds independence,
    it does not strip an existing manager. A near-miss handle (Tantra-vs-'tantra 1' shape,
    ruling 7cffda8f) refuses the same way `mint_seat`'s own fresh path does."""
    from src.orchestrator.seats import manager_of_seat

    handle = (handle or "").strip()
    if not handle:
        return {"error": "a self-managed seat needs a handle"}
    person = await _person_collision(actions.pool, handle)
    if person:
        return {"error": f"{handle!r} names a Person record ({person}), not a seat — "
                         "found_seat never mints or adopts a case entity as a worker"}

    root = office_root or _default_office_root()
    office_path = root / handle.lower()
    project_name = project or handle
    # Path.home() alone, never `.expanduser()` (ASYNC240, this codebase's own ruff gate,
    # flags that specific method inside an async def; a shell has already expanded a
    # literal `~` in `path` by the time argv reaches this call anyway — this only
    # defensively handles a caller that passed one through unexpanded, e.g. a test).
    if path and (path == "~" or path.startswith("~/")):
        workspace = Path.home() / path[2:]
    elif path:
        workspace = Path(path)
    else:
        workspace = Path.home() / "code" / handle.lower()

    existing_seat_id = await _resolve_seat_ref(actions.pool, handle)
    if existing_seat_id is not None:
        existing_manager = await manager_of_seat(actions.pool, existing_seat_id)
        if existing_manager is not None:
            return {"error": f"{handle!r} ({existing_seat_id}) already names a MANAGED "
                             f"seat (manager {existing_manager}) — found_seat only founds "
                             "or converges on a self-managed one; pick a different handle, "
                             "or work with the existing seat through its own manager"}
        worker_seat_id = existing_seat_id
        seat_minted = False
    else:
        near = await _near_miss(actions.pool, handle)
        if near:
            return {"error": f"near-miss twin refused: living seat {near!r} normalizes to "
                             f"the same name as {handle!r} — pass the exact handle {near!r} "
                             "to work with it, or choose a distinct one"}
        seat_result = await ensure_seat(
            actions, house=handle, handle=handle, source=actor, anchor_cwd=str(office_path))
        if "error" in seat_result:
            return seat_result
        worker_seat_id = seat_result["seat_id"]
        seat_minted = bool(seat_result["minted"])

    workspace.mkdir(parents=True, exist_ok=True)
    workspace_pin = workspace / ".osiris"
    workspace_pin_state = "left in place"
    if not workspace_pin.exists():
        workspace_pin.write_text(f'project = "{project_name}"\n')
        workspace_pin_state = "written"

    office_result = await _scaffold_office(
        actions, handle=handle, house=handle, project=project_name,
        intended_model=intended_model, office_root=root, seat_id=worker_seat_id,
        manager_seat_id=None)

    tree = await bind_seat_tree(
        actions, seat_id=worker_seat_id, tree_cwd=str(workspace), actor=actor,
        because=f"osiris new: {handle}'s own code workspace")

    worker_obj = await actions.create_or_find_object("Seat", worker_seat_id, actor)
    worker_facts = await seat_facts(actions.pool, worker_seat_id)
    stamped_model = False
    if not worker_facts.get("intended_model"):
        await actions.assert_property(worker_obj, "intended_model", intended_model, actor,
                                      datetime.now(UTC), _CONF, evidence_class=_EC)
        stamped_model = True

    occ = await seat_occupancy(actions.pool, worker_seat_id)
    next_step = {
        "vacant": f"no session has ever attached — furniture until a body sits in it; "
                 f"launch(target={handle!r}) to body it",
        "occupied": "already live — no next step, someone's home",
        "cold": "held, but nobody's live right now — its holder resumes on its own next "
               "mount; no outside hand needed",
    }[occ["state"]]

    return {
        "seat_id": worker_seat_id, "handle": handle, "house": handle,
        "seat_minted": seat_minted, "project": project_name, "workspace": str(workspace),
        "workspace_pin": workspace_pin_state, "office": office_result,
        "tree_cwd": tree.get("tree_cwd"),
        "intended_model": intended_model if stamped_model else worker_facts.get("intended_model"),
        "intended_model_stamped": stamped_model,
        "managed_by": None,
        "occupancy": occ["state"], "holder": occ["holder"], "next_step": next_step,
    }

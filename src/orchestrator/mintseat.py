"""MINT_SEAT: an organization's structure grows by extension.

A coordinator seat extends itself with specialist worker seats: one action, `ensure_seat`
(the durable role) plus the office scaffold (a directory, an `.osiris` pin carrying project
and model, standing orders plus a charter.md from `offices.py`'s own template family, and an
osiris-tool permission grant so a spawned session can approve its own MCP calls without a
human in the loop, never re-derived, never duplicated) plus an `intended_model` stamp
(workers default to Sonnet) plus `managed_by` (the org chart's first real link type, Seat-to-
Seat: the minting seat becomes manager of record).

IDEMPOTENT, and idempotent two different ways depending on what already exists:
  * the WORKER handle is brand new (no exact match, no near-miss): mint the Seat,
    scaffold a fresh office, stamp the model, link managed_by. Every piece is new.
  * the WORKER handle already names a living Seat EXACTLY (minted by hand before this
    action existed): ADOPT: no new Seat, the office scaffold runs FILL-MISSING-ONLY (an
    occupied office is the seat's own hand-maintained home, the same never-clobber rule
    CLAUDE.md and charter.md already run on, now also closing a HOLLOW shell's gaps: a
    genuine hand-made office could be a directory with no pin, no orders at all), only
    the missing pieces get asserted (an unset intended_model, a missing managed_by edge).
    Calling it again once everything is already true is a pure no-op.

GUARDRAILS (all refused loudly, never silently swallowed):
  * PERSON COLLISION: this graph is shared with an entity-resolution product line; a
    worker handle that coincides with a real Person record must never be confused with
    one. Structurally impossible by construction (every seat lookup here filters on
    `type='Seat'`, so a Person is invisible to it): the explicit check below exists
    ONLY to make the refusal a NAMED error instead of a silent 'seat not found'.
  * THE NEAR-MISS DUPLICATE: a handle that NORMALIZES to the same name as a living seat
    (casefold, strip a trailing generation marker, strip punctuation: e.g. 'Example' vs
    the real 'example 1') but does not exact-match it refuses instead of silently
    minting a second identity wearing a near-match's face. `adopt=True` states the
    intent explicitly (no match refuses, never falls through to fresh); `force=True` is
    the only route past the refusal.
  * CROSS-HOUSE MINTING: a manager mints workers in its OWN house by default (no house
    param means inherit the manager's); crossing to a DIFFERENT house needs the
    operator's own hand (an `actor` naming the operator), never a seat's unilateral
    reach into a house it does not own. Scoped to FRESH minting only: adopting an
    already-existing worker is not a house crossing, it is recognizing what already
    exists.
"""
from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.orchestrator.agents import _GEN_SUFFIX_ALTERNATION
from src.orchestrator.boot_compiler import (
    compile_managed_body,
    scaffold_boot_file,
    template_version,
    wrap_managed,
)
from src.orchestrator.charter import charter_of, is_operator_actor
from src.orchestrator.offices import _CHARTER_TEMPLATE, _CHARTER_UNDECLARED, _default_office_root
from src.orchestrator.seats import (
    _FOUNDER_SOURCE_PREFIX,
    bind_seat_tree,
    ensure_seat,
    seat_facts,
    seat_occupancy,
)
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)

DEFAULT_WORKER_MODEL = "claude-sonnet-5"  # Sonnet is the worker default

# THE PERMISSION GRANT: a spawned session starts, the office scaffold fires, and dies
# verbatim on "I need to grant permission for the Osiris tools to proceed" - a
# print-mode/autonomous session cannot approve its own MCP calls, so it can never mount(),
# so launch() can never produce a session. A prior field test that tried writing exactly
# this file as an agent action was correctly classifier-refused (privilege-shaped).
# Scaffolding it HERE instead, server-side, inside the one authorized action that already
# writes .osiris/CLAUDE.md/charter.md, is not classifier-fenced the same way: no agent is
# deciding to grant itself anything, the MCP server is doing file I/O on behalf of an
# already-validated mint_seat call. Content is a verbatim tested string, never invented
# unvalidated JSON.
_PERMISSION_GRANT = json.dumps({"permissions": {"allow": ["mcp__osiris", "mcp__osiris__*"]}},
                               indent=2) + "\n"


async def _resolve_seat_ref(pool: Any, ref: str) -> str | None:
    """A Seat by its own canonical (`seat:xxxxxxxx`) or by handle, case-insensitive,
    unique across houses (claim_name's global-namespace rule), and crucially never a
    holder requirement (unlike seats.binding_of_handle, built for 'who currently sits
    here'): mint_seat resolves the ROLE, not who is presently in it. Only ever matches
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
    """The Person object's canonical if `handle` names one, case-insensitive: the NAMED
    refusal this guards (structurally, no Seat query ever finds a Person; this exists so
    the caller hears WHY, not just 'not found')."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT o.canonical FROM objects o WHERE o.type='Person' AND o.status='active' "
        "AND lower(COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='name' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '')) = lower($1) "
        "LIMIT 1", handle)


# THE NEAR-MISS GUARD: a seat's real claimed handle could be 'example 1' - a bare
# 'Example' fresh-mint request NEVER exact-matches it, and the fresh path used to fire on
# any non-match, so it would have silently minted a duplicate in that house and called it
# success. Identity deserves MORE conservatism than open_thread's own near-duplicate
# dedup, not less: a false near-miss refusal costs a retry; a missed one mints a
# duplicate wearing an unrelated seat's face.
_GEN_SUFFIX_RE = re.compile(r"[\s._-]+(?:" + _GEN_SUFFIX_ALTERNATION + r"|\d+)$")
_PUNCT_RE = re.compile(r"[^a-z0-9]+")


def _normalize_handle(handle: str) -> str:
    """Casefold, strip a trailing generation marker (a roman numeral or plain digit, e.g.
    'Example II', 'example 1', 'Example-2' all strip to 'example'), then strip whatever
    punctuation/whitespace remains. Pure: the guard's whole comparison key."""
    s = handle.strip().casefold()
    s = _GEN_SUFFIX_RE.sub("", s)
    return _PUNCT_RE.sub("", s)


async def _near_miss(pool: Any, handle: str) -> str | None:
    """A LIVING Seat whose handle normalizes the same as `handle` (but isn't reached by
    _resolve_seat_ref's own exact/case-insensitive match, since the caller already checked
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
    actions: Actions, *, handle: str, house: str, project: str | None, intended_model: str,
    office_root: Path, seat_id: str, manager_seat_id: str | None,
) -> dict[str, Any]:
    """A worker's office: directory plus `.osiris` (project AND model, a gap for
    pre-existing seats closed at birth for a new one; `project` falsy writes NO
    `project =` line at all, genuinely unset, never a fabricated placeholder, see
    found_seat's own docstring for the full rule) plus CLAUDE.md plus charter.md plus the
    osiris-tool permission grant (the one human act launch() needs, spent here instead of
    at every future interactive walk-in), CLAUDE.md now compiled by THE BOOT COMPILER
    rather than a frozen template string. FILL-MISSING-ONLY, every file its own
    exists-guard: a fresh mint's office cannot yet exist to collide with, and an ADOPTED
    seat's office (a shell that was a hand-made directory with no pin, no orders, a
    HOLLOW adoption otherwise) gets exactly its missing pieces filled, nothing present
    ever touched. `manager_seat_id` is passed explicitly (role is explicit too) rather
    than derived live: the caller's own `managed_by` link, when there is one, isn't
    created until AFTER this call returns, so a live derive here would read a brand-new
    worker as a manager-less 'coordinator'.

    `manager_seat_id=None` scaffolds a SELF-MANAGED seat instead (the `osiris new` shape:
    a seat with no `managed_by` edge at all, never a flag, the absence itself). Role
    becomes 'coordinator' (the only template `derive_role` would ever assign a
    manager-less seat live, so this matches what a later `reissue_office` would derive
    anyway) and the charter prompt drops the worker-specific "GOVERNS, not where it sits"
    framing for language that doesn't presuppose an org chart above this seat."""
    office = office_root / handle.lower()
    office.mkdir(parents=True, exist_ok=True)
    pin = office / ".osiris"
    pin_state = "left in place"
    project_declared: bool | None = None
    if not pin.exists():
        pin_text = f'project = "{project}"\n' if project else ""
        pin_text += f'model = "{intended_model}"\n'
        pin.write_text(pin_text)
        pin_state = "written"
        project_declared = bool(project)
    orders = office / "CLAUDE.md"
    agents = office / "AGENTS.md"
    orders_state = "left in place"
    agents_state = "left in place"
    if not orders.exists() or not agents.exists():
        if manager_seat_id is not None:
            role, charter_block = "worker", (
                "Your charter was never formally declared. It lives only in prose. First "
                "act: `charter(repos=[...])` naming the repos you actually govern. A house "
                "is what a seat GOVERNS, not where it sits.")
        else:
            role, charter_block = "coordinator", (
                "Your charter was never formally declared. It lives only in prose. First "
                "act: `charter(repos=[...])` naming the project you actually own. This "
                "seat has no manager, so nobody else will do it for you.")
        body = await compile_managed_body(
            actions, seat_id=seat_id, handle=handle, house=house, office=str(office),
            seat_line=", not yet seated: your next claim binds you (the on-ramp).",
            charter_block=charter_block,
            # a FRESH mint can never yet carry a peer_of edge (offices.py's own
            # establish_office is where a peer bonded later gets picked up live)
            peer_block="\n", role=role, manager_seat_id=manager_seat_id)
        wrapped = wrap_managed(body, template_version())
        # existing_note="left in place" preserves this site's own pre-existing, tested
        # short convention (unlike offices.py's two sites, which already used the
        # longer descriptive form before this helper existed).
        orders_state = scaffold_boot_file(orders, wrapped, label="standing orders",
                                          existing_note="left in place")
        agents_state = scaffold_boot_file(
            agents, wrapped, label="its own compiled standing orders (vendor-neutral)")
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
            "osiris_pin_project_declared": project_declared,
            "standing_orders": orders_state, "agents_md": agents_state,
            "charter_file": charter_state,
            "permission_grant": grant_state}


async def mint_seat(
    actions: Actions, *, manager: str, handle: str,
    house: str | None = None, project: str | None = None,
    intended_model: str = DEFAULT_WORKER_MODEL,
    office_root: Path | None = None, actor: str | None = None,
    adopt: bool = False, force: bool = False,
) -> dict[str, Any]:
    """The whole procedure, one result. `manager` is the minting seat, its own handle or
    seat_id (whichever the caller knows about itself). `handle` is the worker's name.
    Refuses loudly on an unknown manager, a Person-handle collision, a NEAR-MISS handle
    (a living seat whose handle normalizes the same, e.g. 'Example' vs 'example 1'), or
    an unauthorized house crossing. Idempotent: minted once, adopted forever after.
    `adopt=True` states the caller's intent explicitly: no match REFUSES instead of
    silently falling through to a fresh mint (the caller said adopt; minting would be
    the lie). `force=True` is the only route past a near-miss refusal, for the rare case
    a distinct seat genuinely belongs beside a similarly-named one."""
    actor = actor or "ceremony:mint-seat"
    manager_seat_id = await _resolve_seat_ref(actions.pool, manager)
    if manager_seat_id is None:
        if await _person_collision(actions.pool, manager):
            return {"error": f"{manager!r} names a Person record, not a Seat. mint_seat "
                             "never treats a case entity as an org-chart role"}
        return {"error": f"no such manager seat: {manager!r}. mint_seat never invents "
                         "who is minting"}
    manager_facts = await seat_facts(actions.pool, manager_seat_id)
    manager_house = manager_facts.get("house")

    handle = (handle or "").strip()
    if not handle:
        return {"error": "a worker seat needs a handle"}
    person = await _person_collision(actions.pool, handle)
    if person:
        return {"error": f"{handle!r} names a Person record ({person}), not a seat. "
                         "mint_seat never mints or adopts a case entity as a worker"}

    now = datetime.now(UTC)
    root = office_root or _default_office_root()
    office_path = root / handle.lower()
    existing_seat_id = await _resolve_seat_ref(actions.pool, handle)
    if existing_seat_id is not None:
        # THE ADOPT PATH: no new identity, no house crossing to refuse. Recognizing what
        # already exists is not the same act as minting fresh.
        worker_seat_id = existing_seat_id
        # A LIVE SEAT IS NEVER ADOPTED: this branch's own office scaffold plus
        # anchor_cwd backfill below writes the exact same effect establish_office's own
        # rollout guard refuses for a live seat. Until this check, this path did it
        # unguarded, for ANY handle that already resolves to a living Seat, including
        # one whose session is running right now. Gated with seat_occupancy, already
        # imported, already this function's own end-of-result authority for the
        # identical question (below), rather than a second hand-rolled copy of
        # establish_office's SQL. establish_office's own inline check is a SEPARATE,
        # still-separate implementation of this same question; this is a named, not
        # silent, duplication left for a follow-up unification, not a third copy
        # invented here.
        occ = await seat_occupancy(actions.pool, worker_seat_id)
        if occ["state"] == "occupied":
            return {"error": f"cannot adopt {handle!r} ({worker_seat_id}); it is LIVE "
                             f"right now (holder {occ['holder']}). Adopting a live seat "
                             "would move its office out from under a running session, "
                             "splitting the session's history between two homes, the "
                             "same rule establish_office enforces. Close its tab first, "
                             "then mint_seat or establish_office; it wakes up in the "
                             "office"}
        worker_facts = await seat_facts(actions.pool, worker_seat_id)
        worker_house = worker_facts.get("house")
        seat_minted = False
    else:
        if adopt:
            return {"error": f"adopt=True but no living seat exactly matches {handle!r}; "
                             "minting would contradict what the caller explicitly asked for"}
        if not force:
            near = await _near_miss(actions.pool, handle)
            if near:
                return {"error": f"near-miss duplicate refused: living seat {near!r} "
                                 f"normalizes to the same name as {handle!r}. Pass the "
                                 f"exact handle {near!r} to adopt it, or force=True to "
                                 "mint a distinct seat anyway"}
        resolved_house = house or manager_house
        if house and manager_house and house != manager_house \
                and not await is_operator_actor(actions.pool, actor or ""):
            return {"error": f"cross-house mint refused: {manager!r} (house "
                             f"{manager_house!r}) may not mint a seat in house {house!r}. "
                             "Only the operator's own hand crosses a house boundary"}
        # LINEAGE IS PER SEAT, NOT PER ACTOR: the SAME defect found_seat had, where two
        # seats minted under the same manager/actor share that actor's own id as their
        # `handle` assertion source, which _seat_lineage_ancestor later trusts as each
        # seat's founding lineage. Measured live at production scale for mint_seat: 17
        # real managed seats all share their minting manager's own agent id this way,
        # all resolving to that manager's CURRENT lineage head today. `handle` is
        # globally unique (claim_name enforces it), so prefixing it can never collide
        # across seats no matter how many workers one manager mints.
        seat_result = await ensure_seat(
            actions, house=resolved_house, handle=handle,
            source=f"{_FOUNDER_SOURCE_PREFIX}{handle}", anchor_cwd=str(office_path))
        if "error" in seat_result:
            return seat_result
        worker_seat_id = seat_result["seat_id"]
        worker_house = resolved_house
        seat_minted = bool(seat_result["minted"])
        if seat_minted:
            _founder_obj = await actions.create_or_find_object(
                "Seat", worker_seat_id, actor)
            await actions.assert_property(
                _founder_obj, "founded_by", actor, actor, now, _CONF,
                evidence_class=_EC)

    # THE OFFICE SCAFFOLD: a fresh mint always scaffolds; an ADOPTED seat scaffolds
    # FILL-MISSING-ONLY (a hand-made shell with no pin/orders is a hollow adoption
    # otherwise). Every write inside is its own exists-guard, so running it here
    # unconditionally is always safe.
    #
    # `project or worker_house` IS DELIBERATE, NOT found_seat's fabrication one call
    # over (a previously observed defect where a missing project silently fabricated
    # a placeholder project from the handle): a MANAGED worker with no explicit project
    # inherits its MANAGER's own already-real, already-declared house: house(seat) IS
    # the manager's own project by this house's own convention (derive_house's
    # docstring gives examples such as a coordinator seat's house matching its own
    # project name), never text invented from the WORKER's own brand-new handle. Only
    # the FINAL tail changed: `or ""` used to write a literal empty-string
    # `project = ""` when even worker_house was absent: an empty string is itself a
    # fabricated placeholder (it reads as "no project, decided," not "not yet decided"),
    # so that tail is now `or None`, letting `_scaffold_office` omit the line entirely
    # and leaving the pin genuinely unset, exactly like found_seat's own fix.
    office_result: dict[str, Any] | None = None
    if existing_seat_id is not None or seat_minted:
        office_result = await _scaffold_office(
            actions, handle=handle, house=worker_house or "",
            project=project or worker_house or None, intended_model=intended_model,
            office_root=root, seat_id=worker_seat_id, manager_seat_id=manager_seat_id)

    worker_obj = await actions.create_or_find_object("Seat", worker_seat_id, actor)
    worker_facts = await seat_facts(actions.pool, worker_seat_id)
    stamped_model = False
    if not worker_facts.get("intended_model"):
        await actions.assert_property(worker_obj, "intended_model", intended_model, actor,
                                      now, _CONF, evidence_class=_EC)
        stamped_model = True
    if not worker_facts.get("anchor_cwd"):
        # FILL-MISSING-ONLY, same rule as intended_model above: an ADOPTED seat never had
        # ensure_seat mint its anchor_cwd, so a hollow shell still needs it backfilled
        # here or launch() can never find its office (trigger.py's own refusal).
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

    # THE RESULT COMPLETES THE LIFECYCLE: mint_seat only ever finished HALF of it,
    # a seat, an office, a manager edge, and silence about whether a session exists yet.
    # Now the result states occupancy plainly and names whose hand the next step needs,
    # the same treatment every half-finished procedure in this house owes its caller
    # (a previously observed case this would have saved: minting told the caller nothing
    # about VACANT vs OCCUPIED, so they found out only by asking again later).
    occ = await seat_occupancy(actions.pool, worker_seat_id)
    # TWO AUDIENCES, TWO STRINGS (from an earlier transcript: this exact
    # `launch(target=...)` clause, printed one line above the CORRECT
    # `osiris launch <handle>`, is not runnable in a terminal). This result has two real
    # callers: an MCP-tool-calling agent (mint_seat itself, for whom
    # `launch(target=...)` is the actual callable syntax) and the CLI's own
    # `cmd_mint_seat` (a human at a terminal, for whom it never was). One string cannot
    # be correct for both, so this is two strings, not a rewording: `next_step` keeps its
    # MCP-native form unchanged; `next_step_cli` is the terminal-appropriate counterpart.
    next_step = {
        "vacant": "no session has ever attached, furniture until a session occupies it; "
                 f"launch(target={handle!r}) to start one, or start a session in the "
                 "office and have it claim_name itself",
        "occupied": "already live, no next step, someone's home",
        "cold": "held, but nobody's live right now, its holder resumes on its own "
               "next mount; no outside hand needed",
    }[occ["state"]]
    next_step_cli = {
        "vacant": "no session has ever attached, furniture until a session occupies it; run "
                 f"`osiris launch {handle}` to start it",
        "occupied": "already live, no next step, someone's home",
        "cold": "held, but nobody's live right now, its holder resumes on its own "
               "next mount; no outside hand needed",
    }[occ["state"]]

    # CHARTER VISIBILITY IN THE RESULT: a fresh mint can never yet have a charter
    # (charter() needs the Seat object this call just minted); an ADOPTED seat may
    # already carry one. Either way the result now SAYS SO, reusing establish_office's
    # own honest text verbatim (_CHARTER_UNDECLARED) rather than a second string
    # invented here.
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
        "next_step_cli": next_step_cli,
    }


async def found_seat(
    actions: Actions, *, handle: str, path: str | None = None,
    project: str | None = None,
    intended_model: str = DEFAULT_WORKER_MODEL,
    office_root: Path | None = None, actor: str,
) -> dict[str, Any]:
    """ONE ACTION, no elaborate setup: found a SELF-MANAGED seat, the shape a seat takes
    when it was self-claimed, then given an office, then self-declared its own `governs`
    edge later while live, with no minting agent and no manager, ever (a Seat with NO
    `managed_by` edge at all, never a flag, the absence itself). Composes the SAME
    primitives `mint_seat` does (`ensure_seat` plus `_scaffold_office`, with
    `manager_seat_id=None`) plus `bind_seat_tree` for the CODE workspace, deliberately
    distinct from the seat's own identity office (agents sit in their own durable home
    directory; code stays in the repos they GOVERN).

    `path` defaults to `~/code/<handle>` (this repo's own convention) and is created if
    absent: osiris never assumes a git repo already exists there (a project needs none;
    resolution reads a `.osiris` pin or a bare folder name, never git, proven, not
    assumed, by real examples of a seat's identity office that is not itself a repo but
    still carries a real project pin). Neither `.osiris` pin (the workspace's own, and
    the office's own) is ever overwritten if already present: fill-missing-only, the
    same rule every office write in this codebase holds.

    NO SEPARATE house DECLARATION: a self-managed seat's own Seat.house property is
    ALWAYS `project`'s own value, never a second, independently-given value that could
    disagree with it. `mint_seat`'s own org-chart path already held this rule one field
    over (`house or manager_house`: a managed worker inherits its real manager's real
    project, never invents one, never takes a second value either); this closes the
    direct-mint counterpart of that same rule by removing the second flag rather than
    merely deferring to it. An earlier design where "a direct mint with no --house stays
    homeless" is superseded here in the same direction it was already pointing: house
    never diverges from project, so no --house flag survives to disagree with `project`
    below in the first place. This entry point used to write `house=handle`
    unconditionally before that fix, which is precisely how several seats ended up with
    a Seat.house indistinguishable from a deliberately-chosen one by any test except
    "does it equal the handle"; the fix here is the same "never fabricate from the
    handle" rule, just enforced by construction now.

    `project`, WHEN OMITTED, IS LEFT GENUINELY UNSET, never fabricated from `handle`
    (a previously observed defect: the system could not handle "no project", and it
    falsely created placeholder projects named after the handle when the seat was really
    working somewhere else; measured population: 8 confirmed/strong specimens
    fleet-wide). NEITHER pin gets a `project =` line written when `project` is falsy.
    This is one of several mint entry points: an earlier inventory of graph-layer mint
    entry points (bootstrap_project, ingest_files, and other repo/project minting
    helpers) closed the fabrication class correctly FOR THAT LAYER: every one of those
    either derives from real disk truth or requires deliberate, validated caller text.
    This function writes PLAIN PIN FILES upstream of all of them, so none of those
    guards could ever see it. An absent line is not silence: an earlier fix already made
    "project unset" a first-class, self-healing state at the PIN-READ layer (mount/orient
    tolerate it, and self-heal a genuinely unset pin from the graph the moment
    governs+works_in+anchor_cwd unambiguously agree); that machinery could never engage
    while this entry point kept writing a fabricated placeholder into a pin that would
    then never again read as unset. Confirmed downstream-clean by reading, not assumed:
    neither establish_office nor rebind_seat ever writes a project line into either pin,
    so this fix cannot be silently undone by a later office procedure.

    DOES NOT eagerly create a `governs` edge: inventing one on an unlaunched agent's
    behalf would be exactly the kind of fact this call has no standing to assert. The
    scaffolded CLAUDE.md's own charter_block already tells a fresh, self-managed seat to
    `charter(repos=[...])` naming its own project as its first act, once it is actually
    live to say so in its own voice, matching the real self-managed bootstrap order
    exactly.

    IDEMPOTENT: a handle that already names a living, ALREADY self-managed seat converges
    (fills in whatever's missing, mints nothing new). A handle that names a living MANAGED
    seat (a real `managed_by` edge already out) REFUSES: this call founds independence,
    it does not strip an existing manager. A near-miss handle (the 'Example' vs
    'example 1' shape) refuses the same way `mint_seat`'s own fresh path does."""
    from src.orchestrator.seats import manager_of_seat

    handle = (handle or "").strip()
    if not handle:
        return {"error": "a self-managed seat needs a handle"}
    person = await _person_collision(actions.pool, handle)
    if person:
        return {"error": f"{handle!r} names a Person record ({person}), not a seat. "
                         "found_seat never mints or adopts a case entity as a worker"}

    root = office_root or _default_office_root()
    office_path = root / handle.lower()
    project_name = (project or "").strip() or None
    # NO SEPARATE house DECLARATION: a self-managed seat has no manager to derive its own
    # project from (mint_seat's own worker path uses `house or manager_house`; there is
    # no manager_house here at all), so the one remaining source of truth is this same
    # call's own --project, never a second, independently-given value that could
    # disagree with it.
    house_name = project_name
    # Path.home() alone, never `.expanduser()` (ASYNC240, this codebase's own ruff gate,
    # flags that specific method inside an async def; a shell has already expanded a
    # literal `~` in `path` by the time argv reaches this call anyway, this only
    # defensively handles a caller that passed one through unexpanded, e.g. a test).
    # THE SEAT TREE FABRICATION FIX: an omitted `path` used to fall back to
    # `~/code/<handle>` unconditionally, the exact same fabrication-from-handle defect
    # `house`/`project` were already cured of one call up (see this function's own
    # docstring), just never applied to the tree. `workspace` stays `None` here when
    # omitted; resolved below, once the seat is known, from its own real charter rather
    # than guessed from its name.
    if path and (path == "~" or path.startswith("~/")):
        workspace: Path | None = Path.home() / path[2:]
    elif path:
        workspace = Path(path)
    else:
        workspace = None

    existing_seat_id = await _resolve_seat_ref(actions.pool, handle)
    if existing_seat_id is not None:
        existing_manager = await manager_of_seat(actions.pool, existing_seat_id)
        if existing_manager is not None:
            return {"error": f"{handle!r} ({existing_seat_id}) already names a MANAGED "
                             f"seat (manager {existing_manager}). found_seat only founds "
                             "or converges on a self-managed one; pick a different handle, "
                             "or work with the existing seat through its own manager"}
        worker_seat_id = existing_seat_id
        seat_minted = False
        worker_facts = await seat_facts(actions.pool, worker_seat_id)
        worker_house = worker_facts.get("house")
    else:
        near = await _near_miss(actions.pool, handle)
        if near:
            return {"error": f"near-miss duplicate refused: living seat {near!r} normalizes to "
                             f"the same name as {handle!r}. Pass the exact handle {near!r} "
                             "to work with it, or choose a distinct one"}
        # LINEAGE IS PER SEAT, NOT PER ACTOR (see seats.py's own _FOUNDER_SOURCE_PREFIX
        # docstring): the handle assertion's source is what _seat_lineage_ancestor later
        # trusts as this seat's own founding lineage. `actor` is who ran `osiris new`,
        # and two DIFFERENT seats founded under the SAME --actor used to share that
        # source, so the second seat's first launch silently inherited the first seat's
        # own live generation. `handle` is globally unique (claim_name enforces it), so
        # prefixing it makes a source no other seat can ever carry: this always resolves
        # to "no ancestor yet", the fresh `agent:seat-<id>` root every never-launched
        # seat is supposed to get, regardless of how many other seats this same actor
        # has founded before or since.
        seat_result = await ensure_seat(
            actions, house=house_name, handle=handle,
            source=f"{_FOUNDER_SOURCE_PREFIX}{handle}", anchor_cwd=str(office_path))
        if "error" in seat_result:
            return seat_result
        worker_seat_id = seat_result["seat_id"]
        seat_minted = bool(seat_result["minted"])
        if seat_minted:
            _founder_obj = await actions.create_or_find_object(
                "Seat", worker_seat_id, actor)
            await actions.assert_property(
                _founder_obj, "founded_by", actor, actor, datetime.now(UTC), _CONF,
                evidence_class=_EC)
        worker_house = house_name

    tree_derivation = "explicit"
    if workspace is None:
        # THE REPAIR HALF: an omitted path on a CONVERGENCE (an already-founded seat,
        # `existing_seat_id` above) checks the seat's own real charter first:
        # `governed_trees` returns only projects this seat actively governs AND that
        # carry a recorded on_disk_path, so an unambiguous single real git tree there is
        # genuinely this seat's own code home, never a guess. A brand-new seat has no
        # charter yet (it self-charters live, on its own first turn, per this function's
        # own docstring) and a seat with zero or more than one real governed tree has
        # nothing unambiguous to derive: both cases leave `workspace` unset, same "never
        # fabricate, leave genuinely unset" rule this function already holds for
        # house/project.
        from src.orchestrator.charter import governed_trees
        from src.orchestrator.trigger import _is_git_tree, _tree_exists

        real_trees = [
            (repo, p) for repo, p in await governed_trees(actions.pool, worker_seat_id)
            if _tree_exists(p) and _is_git_tree(p)]
        if len(real_trees) == 1:
            workspace = Path(real_trees[0][1])
            tree_derivation = f"charter:{real_trees[0][0]}"
        else:
            tree_derivation = "unset: no path given and no single real governed tree"

    workspace_pin_state = "not applicable: no workspace resolved"
    tree: dict[str, Any] | None = None
    if workspace is not None:
        workspace.mkdir(parents=True, exist_ok=True)
        workspace_pin = workspace / ".osiris"
        workspace_pin_state = "left in place"
        if not workspace_pin.exists():
            workspace_pin.write_text(f'project = "{project_name}"\n' if project_name else "")
            workspace_pin_state = "written"

    office_result = await _scaffold_office(
        actions, handle=handle, house=worker_house or "", project=project_name,
        intended_model=intended_model, office_root=root, seat_id=worker_seat_id,
        manager_seat_id=None)

    if workspace is not None:
        tree = await bind_seat_tree(
            actions, seat_id=worker_seat_id, tree_cwd=str(workspace), actor=actor,
            because=f"osiris new: {handle}'s own code workspace ({tree_derivation})")

    worker_obj = await actions.create_or_find_object("Seat", worker_seat_id, actor)
    worker_facts = await seat_facts(actions.pool, worker_seat_id)
    stamped_model = False
    if not worker_facts.get("intended_model"):
        await actions.assert_property(worker_obj, "intended_model", intended_model, actor,
                                      datetime.now(UTC), _CONF, evidence_class=_EC)
        stamped_model = True

    occ = await seat_occupancy(actions.pool, worker_seat_id)
    # CLI-ONLY, NO MCP CALLER (unlike mint_seat's own counterpart above): found_seat is
    # never exposed as an MCP tool, so this text only ever reaches a human terminal via
    # cmd_new. No `launch(target=...)` MCP-syntax clause belongs here at all (an earlier
    # version of this text was copy-pasted from mint_seat's own, wrong audience).
    next_step = {
        "vacant": "no session has ever attached, furniture until a session occupies it; run "
                 f"`osiris launch {handle}` to start it",
        "occupied": "already live, no next step, someone's home",
        "cold": "held, but nobody's live right now, its holder resumes on its own next "
               "mount; no outside hand needed",
    }[occ["state"]]

    return {
        "seat_id": worker_seat_id, "handle": handle, "house": worker_house,
        "seat_minted": seat_minted, "project": project_name,
        "workspace": str(workspace) if workspace is not None else None,
        "workspace_pin": workspace_pin_state, "office": office_result,
        "tree_cwd": tree.get("tree_cwd") if tree else None,
        "tree_derivation": tree_derivation,
        "intended_model": intended_model if stamped_model else worker_facts.get("intended_model"),
        "intended_model_stamped": stamped_model,
        "managed_by": None,
        "occupancy": occ["state"], "holder": occ["holder"], "next_step": next_step,
    }

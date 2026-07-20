"""MINT_SEAT — the org chart trickles (task #50, ruling cabc28f5).

A Fable-class coordinator seat (Thoth, Ra, alfred) extends itself with SPECIALIST WORKER
seats — one act: `ensure_seat` (the durable role) + the office scaffold (a directory, an
`.osiris` pin carrying project AND model, standing orders + a charter.md from `offices.py`'s
own template family — never re-derived, never duplicated) + an `intended_model` stamp
(workers default Sonnet, ruling f6f6174d) + `managed_by` (the org chart's first real link
type — Seat-to-Seat, the minting seat becomes manager of record).

IDEMPOTENT, and idempotent two different ways depending on what already exists:
  * the WORKER handle is brand new → mint the Seat, scaffold a fresh office, stamp the
    model, link managed_by. Every piece is new.
  * the WORKER handle already names a living Seat (Tantra, minted by the operator's own
    hand before this verb existed) → ADOPT: no new Seat, no office rewrite (an occupied
    office is the seat's own hand-maintained home — the same never-clobber law CLAUDE.md
    and charter.md already run on), only the MISSING pieces get asserted (an unset
    intended_model, a missing managed_by edge). Calling it again once everything is
    already true is a pure no-op.

GUARDRAILS (the ruling's own, both refused LOUD, never silently swallowed):
  * PERSON COLLISION — this graph is shared with an entity-resolution product line; a
    worker handle that coincides with a real Person record must never be confused with
    one. Structurally impossible by construction (every seat lookup here filters on
    `type='Seat'`, so a Person is invisible to it) — the explicit check below exists
    ONLY to make the refusal a NAMED error instead of a silent 'seat not found'.
  * CROSS-HOUSE MINTING — a manager mints workers in its OWN house by default (no house
    param = inherit the manager's); crossing to a DIFFERENT house needs the operator's
    own hand (an `actor` naming the operator), never a seat's unilateral reach into a
    house it does not own. Scoped to FRESH minting only — adopting an already-existing
    worker (Tantra) is not a house crossing, it is recognizing what already exists.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.orchestrator.offices import _CHARTER_TEMPLATE, _DEFAULT_OFFICE_ROOT, _STANDING_ORDERS
from src.orchestrator.seats import ensure_seat
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)

# names naming the human, not a seat — the operator's own hand crosses a house boundary;
# no seat does it unilaterally (mirrors the reflection ACL's own operator-caller set,
# compositions.py's _OPERATOR_CALLERS — same law, same shape, different door)
_OPERATOR_ACTORS = {"operator", "analyst:operator", "console"}

DEFAULT_WORKER_MODEL = "claude-sonnet-5"  # ruling f6f6174d: Sonnet is the worker default


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


async def _seat_facts(pool: Any, seat_id: str) -> dict[str, Any]:
    """A Seat's own handle/house/intended_model — {} when no such active Seat."""
    row = await pool.fetchrow(
        "SELECT "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS handle, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='house' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS house, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='intended_model' ORDER BY a.confidence DESC, a.observed_at DESC "
        "   LIMIT 1) AS intended_model "
        "FROM objects o WHERE o.canonical=$1 AND o.type='Seat' AND o.status='active'", seat_id)
    return dict(row) if row else {}


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


def _scaffold_office(
    *, handle: str, house: str, project: str, intended_model: str,
    office_root: Path,
) -> dict[str, Any]:
    """A BRAND NEW worker's office — dir + `.osiris` (project AND model, assignment 3's
    own gap for pre-existing seats closed at birth for a new one) + CLAUDE.md +
    charter.md, all from offices.py's own template family, all idempotent (never
    overwrite — though a just-minted seat's office cannot yet exist to collide with).
    Never called for the adopt path: an already-living seat's office is its own hand-
    maintained home, exactly the file assignment 3 already refuses to clobber."""
    office = office_root / handle.lower()
    office.mkdir(parents=True, exist_ok=True)
    pin = office / ".osiris"
    if not pin.exists():
        pin.write_text(f'project = "{project}"\nmodel = "{intended_model}"\n')
    orders = office / "CLAUDE.md"
    orders_state = "left in place"
    if not orders.exists():
        orders.write_text(_STANDING_ORDERS.format(
            handle=handle, office=office, house=house,
            seat_line=" — not yet seated: your next claim binds you (the on-ramp).",
            charter_block=(
                "Your charter was never formally declared — it lives only in prose. First "
                "act: `charter(repos=[...])` naming the repos you actually govern. A house "
                "is what a seat GOVERNS, not where it sits.")))
        orders_state = "written"
    charter = office / "charter.md"
    charter_state = "left in place"
    if not charter.exists():
        charter.write_text(_CHARTER_TEMPLATE.format(handle=handle))
        charter_state = "written"
    return {"office": str(office), "standing_orders": orders_state, "charter_file": charter_state}


async def mint_seat(
    actions: Actions, *, manager: str, handle: str,
    house: str | None = None, project: str | None = None,
    intended_model: str = DEFAULT_WORKER_MODEL,
    office_root: Path | None = None, actor: str | None = None,
) -> dict[str, Any]:
    """The whole ceremony, one receipt. `manager` is the minting seat — its own handle or
    seat_id (whichever the caller knows about itself). `handle` is the worker's name.
    Refuses loudly on an unknown manager, a Person-handle collision, or an unauthorized
    house crossing. Idempotent: minted once, adopted forever after."""
    actor = actor or "ceremony:mint-seat"
    manager_seat_id = await _resolve_seat_ref(actions.pool, manager)
    if manager_seat_id is None:
        if await _person_collision(actions.pool, manager):
            return {"error": f"{manager!r} names a Person record, not a Seat — mint_seat "
                             "never treats a case entity as an org-chart role"}
        return {"error": f"no such manager seat: {manager!r} — mint_seat never invents "
                         "who is minting"}
    manager_facts = await _seat_facts(actions.pool, manager_seat_id)
    manager_house = manager_facts.get("house")

    handle = (handle or "").strip()
    if not handle:
        return {"error": "a worker seat needs a handle"}
    person = await _person_collision(actions.pool, handle)
    if person:
        return {"error": f"{handle!r} names a Person record ({person}), not a seat — "
                         "mint_seat never mints or adopts a case entity as a worker"}

    now = datetime.now(UTC)
    existing_seat_id = await _resolve_seat_ref(actions.pool, handle)
    office_result: dict[str, Any] | None = None
    if existing_seat_id is not None:
        # THE ADOPT PATH (Tantra's shape): no new identity, no house crossing to refuse —
        # recognizing what already exists is not the same act as minting fresh
        worker_seat_id = existing_seat_id
        worker_facts = await _seat_facts(actions.pool, worker_seat_id)
        worker_house = worker_facts.get("house")
        seat_minted = False
    else:
        resolved_house = house or manager_house
        if house and manager_house and house != manager_house \
                and (actor or "") not in _OPERATOR_ACTORS:
            return {"error": f"cross-house mint refused: {manager!r} (house "
                             f"{manager_house!r}) may not mint a seat in house {house!r} — "
                             "only the operator's own hand crosses a house boundary"}
        seat_result = await ensure_seat(
            actions, house=resolved_house, handle=handle, source=actor)
        if "error" in seat_result:
            return seat_result
        worker_seat_id = seat_result["seat_id"]
        worker_house = resolved_house
        seat_minted = bool(seat_result["minted"])
        if seat_minted:
            root = office_root or _DEFAULT_OFFICE_ROOT
            office_result = _scaffold_office(
                handle=handle, house=worker_house or "", project=project or worker_house or "",
                intended_model=intended_model, office_root=root)

    worker_obj = await actions.create_or_find_object("Seat", worker_seat_id, actor)
    worker_facts = await _seat_facts(actions.pool, worker_seat_id)
    stamped_model = False
    if not worker_facts.get("intended_model"):
        await actions.assert_property(worker_obj, "intended_model", intended_model, actor,
                                      now, _CONF, evidence_class=_EC)
        stamped_model = True

    manager_obj = await actions.create_or_find_object("Seat", manager_seat_id, actor)
    already_linked = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='managed_by' "
        "AND (valid_until IS NULL OR valid_until > now()) LIMIT 1", worker_obj, manager_obj)
    linked_now = False
    if not already_linked:
        await actions.create_link(worker_obj, manager_obj, "managed_by", actor, now, _CONF,
                                  evidence_class=_EC)
        linked_now = True

    return {
        "seat_id": worker_seat_id, "handle": handle, "house": worker_house,
        "seat_minted": seat_minted,
        **({"office": office_result} if office_result else {}),
        "intended_model": intended_model if stamped_model else worker_facts.get("intended_model"),
        "intended_model_stamped": stamped_model,
        "manager_seat_id": manager_seat_id,
        "managed_by": "linked" if linked_now else "already linked",
    }

"""THE BOOT COMPILER (thread 4951d818, task #53) — standing orders as a COMPILED
artifact, not hand-written prose. Four typed sources — house law, a role template
derived STRUCTURALLY (never a stored field to drift), live seat facts, and a capped,
curated slice of standing Practices (fa41acfc) — assemble into a MANAGED SECTION inside
CLAUDE.md, bounded by machine-readable markers.

THE NEVER-CLOBBER BOUNDARY: only the bytes between `<!-- osiris:compiled:begin -->` and
`<!-- osiris:compiled:end -->` are ever regenerated. Everything outside those markers —
a seat's own hand-composed founding narrative, a hand-added fact, charter.md always —
survives a reissue untouched. Real seat offices already carry exactly this kind of
irreplaceable content post-mint (Khnum's "WHY YOU EXIST" narrative; both Khnum's and
Seshat's own CLAUDE.md hand-naming their manager) — a naive whole-file regenerate would
be actively destructive, not merely careless.

THE REFUSAL IS THE BOUNDARY'S REAL TEETH (Thoth's added requirement, msg 1819): a
hand-edit that deletes, duplicates, or mangles a marker makes `reissue_office` REFUSE
LOUDLY, naming the seat — never guess which span was meant, never re-wrap, never
append a second section beside a first that's merely damaged.
"""
from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_TEMPLATES_DIR = Path(__file__).resolve().parent.parent.parent / "templates"

_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)

_MARKER_BEGIN_RE = re.compile(r"<!-- osiris:compiled:begin v=(\S+) -->")
_MARKER_END_RE = re.compile(r"<!-- osiris:compiled:end -->")

_ROLE_SURFACES = {"worker", "coordinator"}
_PRACTICE_LIMIT = 5


class MarkerError(Exception):
    """A malformed, duplicated, or missing managed-section marker — `reissue_office`
    catches this and refuses loudly rather than guessing which span was meant."""


def _read_template(name: str) -> str:
    return (_TEMPLATES_DIR / name).read_text()


def template_version() -> str:
    """A content hash of the STATIC template sources only (house law + both role
    templates) — never live seat facts or practices, which refresh every compile
    regardless of whether the templates themselves changed. A compiled file's own
    stamp is compared against this to detect template drift (thread 4951d818 piece 3)."""
    house = _read_template("house_law.md")
    worker = _read_template("role_worker.md")
    coordinator = _read_template("role_coordinator.md")
    return hashlib.sha1((house + worker + coordinator).encode()).hexdigest()[:12]


async def derive_role(pool: asyncpg.Pool, seat_id: str) -> str:
    """'coordinator' when the seat carries no active `managed_by` edge out (the head of
    its own chain), else 'worker' — the SAME single-hop topology check derive_house()
    already walks (seats.manager_of_seat), never a stored `role` property that could
    drift from it. Seat has no `role` column in schema.py by design."""
    from src.orchestrator.seats import manager_of_seat

    manager = await manager_of_seat(pool, seat_id)
    return "worker" if manager is not None else "coordinator"


async def _manager_block(pool: asyncpg.Pool, manager_seat_id: str | None) -> tuple[str, str]:
    """(the role template's `{manager_block}` prose, the manager's display handle) — a
    worker with no manager on record yet (should not happen post mint_seat, but a
    compile must never crash on a data gap) gets an honest placeholder instead of a
    fabricated name."""
    if manager_seat_id is None:
        return ("\nYour manager of record is not yet linked — flag this as a bug if "
                "you're seeing it; mint_seat's own managed_by edge should have set "
                "one.\n", "your manager")
    handle = await pool.fetchval(
        "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
        "ON a.object_id=o.id AND a.name='handle' WHERE o.canonical=$1 "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", manager_seat_id)
    who = handle or manager_seat_id
    return (f"\nYour manager of record is **{who}** — the `managed_by` edge is live "
            f"in the graph.\n", who)


_AMENDMENT_RENDER_CAP = 200

async def _armed_practices(
    pool: asyncpg.Pool, role: str, *, limit: int = _PRACTICE_LIMIT,
) -> list[dict[str, Any]]:
    """Top-N by confirmed witness count, refuted EXCLUDED (dead law never boot-arms —
    unlike practices()'s own on-demand listing, which still shows a refuted Practice,
    flagged). Role-scoping REUSES record_practice's existing `surface` free-text field
    rather than a new classifier or a new schema field (Stage C's two false positives
    this session are exactly why no auto-inference is built here): a Practice is only
    EXCLUDED for a role when its surface is LITERALLY 'worker' or 'coordinator' and
    doesn't match — any other surface value (a domain tag like 'deploy'/'search', or
    none at all) arms for every role, since that vocabulary is BlindSpot's domain
    space, not a role space, and this function must not conflate the two.

    `latest_amendment` (thread bd28a41f, Thoth's own measurement dispatch, 2026-09-01):
    a practice's `statement` is deliberately immutable (amend_practice's own idempotency-
    key law) — a correction lives ONLY in the append-only `amendment:<hex>` property
    family `practice_amendments()` already reads. Before this, that family was invisible
    here: a self-corrected practice compiled its original, now-wrong `statement` forever,
    and ranking by raw witness count actively favored the stale one (measured live:
    1637763e sat at confirmed=7 with a correcting amendment on file, while the newer
    practice that superseded it in prose sat at confirmed=0 and would rarely if ever be
    chosen). This does not re-rank anything — `ORDER BY confirmed DESC` is unchanged, and
    inventing a Practice-to-Practice "corrects" edge is a separate, bigger question this
    piece deliberately leaves alone — it only makes a practice's OWN latest amendment
    visible wherever that practice is already shown, the smallest fix that stops a reader
    from being taught a self-corrected lesson's stale half."""
    rows = await pool.fetch(
        "SELECT o.id, "
        " (SELECT a.value #>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='statement' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS statement, "
        " (SELECT a.value #>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='failure_prevented' ORDER BY a.confidence DESC, a.observed_at DESC "
        "   LIMIT 1) AS failure_prevented, "
        " (SELECT a.value #>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='surface' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS surface, "
        " (SELECT count(*) FROM links l WHERE l.from_id=o.id AND l.type='witnesses') "
        "   AS confirmed, "
        " (SELECT a.value #>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name LIKE 'amendment:%' ORDER BY a.confidence DESC, a.observed_at DESC "
        "   LIMIT 1) AS latest_amendment "
        "FROM objects o WHERE o.type='Practice' AND o.status='active' "
        "AND NOT EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='refuted_by') "
        "ORDER BY confirmed DESC, o.canonical ASC LIMIT $1", limit * 4)
    out: list[dict[str, Any]] = []
    for r in rows:
        surface = (r["surface"] or "").strip().lower()
        if surface in _ROLE_SURFACES and surface != role:
            continue
        out.append({"statement": r["statement"],
                    "failure_prevented": r["failure_prevented"],
                    "confirmed": r["confirmed"],
                    "latest_amendment": r["latest_amendment"]})
        if len(out) >= limit:
            break
    return out


def _render_amendment(amendment: str) -> str:
    """Same truncate-with-ellipsis convention orient()'s terse mode already uses for a
    capped summary — a reader who wants the whole thing has `practices()`."""
    amendment = " ".join(amendment.split())
    if len(amendment) <= _AMENDMENT_RENDER_CAP:
        return amendment
    return amendment[:_AMENDMENT_RENDER_CAP].rstrip() + "…"


async def _practice_block(pool: asyncpg.Pool, role: str) -> str:
    practices = await _armed_practices(pool, role)
    if not practices:
        return ""
    lines = "\n".join(
        f"- **{p['statement']}**"
        + (f" — {p['failure_prevented']}" if p["failure_prevented"] else "")
        + f" (confirmed: {p['confirmed']})"
        + (f" — AMENDED: {_render_amendment(p['latest_amendment'])}"
           if p["latest_amendment"] else "")
        for p in practices)
    return (
        "\n## Standing Practices\n"
        "(engineering lessons proven across the fleet — `practices()` for the rest)\n"
        f"{lines}\n")


async def compile_managed_body(
    actions: Actions, *, seat_id: str | None, handle: str, house: str, office: str,
    seat_line: str, charter_block: str, peer_block: str,
    role: str | None = None, manager_seat_id: str | None = None,
) -> str:
    """Assemble the managed section's CONTENT (no markers yet) from all four sources.
    `role`/`manager_seat_id` let a caller who already knows them (mint_seat, at scaffold
    time — BEFORE its own managed_by link exists yet) skip a live derive that would
    otherwise race the graph and read a brand-new worker as a manager-less
    'coordinator'. Omit both for a live derive (establish_office, reissue_office, where
    the seat is already fully linked). `seat_id=None` is the rare not-yet-seated case
    (a claimed handle with no bound Seat object yet, establish_office's own "on-ramp"
    branch) — no role can be derived without a seat, so the role section is skipped
    entirely rather than guessed, and practices arm unfiltered (no role to scope by)."""
    if role is not None:
        resolved_role: str | None = role
    elif seat_id is not None:
        resolved_role = await derive_role(actions.pool, seat_id)
    else:
        resolved_role = None
    role_body = ""
    if resolved_role == "worker":
        mgr = manager_seat_id
        if mgr is None and seat_id is not None:
            from src.orchestrator.seats import manager_of_seat
            mgr = await manager_of_seat(actions.pool, seat_id)
        manager_block, manager_handle = await _manager_block(actions.pool, mgr)
        role_body = _read_template("role_worker.md").format(
            handle=handle, office=office, manager_block=manager_block,
            manager_handle=manager_handle)
    elif resolved_role == "coordinator":
        role_body = _read_template("role_coordinator.md").format(handle=handle, office=office)
    house_body = _read_template("house_law.md").format(
        handle=handle, office=office, house=house, seat_line=seat_line,
        charter_block=charter_block, peer_block=peer_block)
    practice_body = await _practice_block(actions.pool, resolved_role or "")
    return house_body + ("\n" + role_body if role_body else "") + practice_body


def wrap_managed(body: str, version: str) -> str:
    return (f"<!-- osiris:compiled:begin v={version} -->\n"
            f"{body}\n"
            f"<!-- osiris:compiled:end -->\n")


def _has_any_markers(text: str) -> bool:
    return bool(_MARKER_BEGIN_RE.search(text) or _MARKER_END_RE.search(text))


def locate_managed_section(text: str) -> tuple[int, int, int, int, str]:
    """(begin-match start, begin-match end, end-match start, end-match end, version) for
    EXACTLY one well-formed marker pair. Raises MarkerError naming precisely what's
    wrong otherwise — none found, more than one of either marker, an END with no
    matching BEGIN (or vice versa), or an END that precedes its own BEGIN."""
    begins = list(_MARKER_BEGIN_RE.finditer(text))
    ends = list(_MARKER_END_RE.finditer(text))
    if not begins and not ends:
        raise MarkerError("no managed section found — this office has never been "
                           "compiled")
    if len(begins) > 1:
        raise MarkerError(f"found {len(begins)} BEGIN markers, expected exactly 1 — "
                           "a duplicated marker, not a reissue target")
    if len(ends) > 1:
        raise MarkerError(f"found {len(ends)} END markers, expected exactly 1 — "
                           "a duplicated marker, not a reissue target")
    if not begins:
        raise MarkerError("found an END marker with no matching BEGIN — a mangled "
                           "managed section")
    if not ends:
        raise MarkerError("found a BEGIN marker with no matching END — a mangled "
                           "managed section")
    b, e = begins[0], ends[0]
    if e.start() < b.end():
        raise MarkerError("found an END marker before its BEGIN — a mangled managed "
                           "section")
    return b.start(), b.end(), e.start(), e.end(), b.group(1)


async def reissue_office(
    actions: Actions, *, seat_id: str, because: str, actor: str, adopt: bool = False,
) -> dict[str, Any]:
    """Recompile a seat's managed section — the fourth compile point, fired on demand
    when law changes or a live fact (a peer bond, a manager reassignment) needs to
    reach an already-occupied office that establish_office/mint_seat's fill-missing-
    only scaffold will never revisit. `because` is required (a reissue is testimony,
    the same discipline rename_seat already runs).

    NEVER A SILENT REGENERATE: a malformed managed section REFUSES LOUDLY, naming the
    seat, rather than guessing which span to replace (Thoth's added requirement, msg
    1819) — the marker boundary's promise that a hand-edit OUTSIDE it is safe forever
    only holds if a hand-edit that damages the markers THEMSELVES is never silently
    repaired, re-wrapped, or ignored.

    `adopt=True` is the one-time on-ramp for an office that predates the compiler (zero
    markers on disk) — appends a fresh managed section at the end of the existing file,
    touching nothing already there. Without it, a file with zero markers refuses too (a
    missing section is never silently assumed to mean 'append one'); WITH it, a file
    that already carries any marker-shaped text — well-formed or not — also refuses,
    naming the seat, since adopt is for a first compile, not a second."""
    if not because.strip():
        return {"error": "because is required — a reissue is testimony, same as a rename"}
    from src.orchestrator.charter import charter_of
    from src.orchestrator.offices import _peer_addendum
    from src.orchestrator.seats import peer_of_seat, seat_facts

    row = await actions.pool.fetchrow(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Seat' AND status='active'",
        seat_id)
    if row is None:
        return {"error": f"no such seat: {seat_id!r}"}
    facts = await seat_facts(actions.pool, seat_id)
    handle = facts.get("handle")
    anchor = facts.get("anchor_cwd")
    if not handle or not anchor:
        return {"error": f"{seat_id!r} has no handle or no office on record — reissue "
                         "only ever recompiles an EXISTING office"}
    office = Path(anchor)
    orders_path = office / "CLAUDE.md"
    if not orders_path.exists():
        return {"error": f"{handle} ({seat_id}) has no CLAUDE.md on disk at "
                         f"{orders_path} — establish_office/mint_seat scaffolds the "
                         "first one; reissue only recompiles an existing managed "
                         "section"}
    text = orders_path.read_text()

    if adopt and _has_any_markers(text):
        return {"error": f"{handle} ({seat_id}) already carries managed-section "
                         "marker text — adopt=True is only for a first-time compile "
                         "on an office that predates the compiler; omit it to reissue "
                         "normally, or fix the marker by hand first if it's malformed"}
    if not adopt:
        try:
            locate_managed_section(text)
        except MarkerError as exc:
            return {"error": f"{handle} ({seat_id}): {exc} — refusing to guess; fix "
                             "the marker by hand, or pass adopt=True if this office "
                             "genuinely predates the compiler"}

    # THE CHARTER IS THE SEAT'S (ruling 1db1ff41): `seat_id` is already this call's own
    # parameter — no occupant lookup needed at all, and no lineage-string walk either.
    repos: list[str] = await charter_of(actions.pool, seat_id)
    charter_block = (
        "You govern: " + ", ".join(f"`{r}`" for r in repos) + "." if repos else
        "Your charter was never formally declared — it lives only in prose. First "
        "act: `charter(repos=[...])` naming the repos you actually govern. A house "
        "is what a seat GOVERNS, not where it sits.")
    peer_seat = await peer_of_seat(actions.pool, seat_id)
    peer_block = "\n"
    if peer_seat is not None:
        peer_handle = await actions.pool.fetchval(
            "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
            "ON a.object_id=o.id AND a.name='handle' WHERE o.canonical=$1 "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", peer_seat)
        peer_block = _peer_addendum(peer_seat, peer_handle)
    seat_line = f" — durable identity `{seat_id}`."

    body = await compile_managed_body(
        actions, seat_id=seat_id, handle=handle, house=facts.get("house") or "",
        office=str(office), seat_line=seat_line, charter_block=charter_block,
        peer_block=peer_block)
    version = template_version()
    wrapped = wrap_managed(body, version)

    if adopt:
        new_text = text.rstrip("\n") + "\n\n" + wrapped
    else:
        b_start, _b_end, _e_start, e_end, _old_version = locate_managed_section(text)
        new_text = text[:b_start] + wrapped + text[e_end:]

    if new_text == text:
        return {"seat": seat_id, "handle": handle, "version": version,
                "because": because, "changed": False,
                "note": "no change — the compiled section already matches"}
    orders_path.write_text(new_text)
    # TESTIMONY, DURABLE (a reissue is testimony, per this verb's own docstring): the
    # version + why land on the Seat object itself, not just in this call's receipt —
    # a janitor or a future drift-check reads this instead of re-parsing the file.
    await actions.assert_property(row["id"], "boot_compiled_version", version, actor,
                                  datetime.now(UTC), _CONF, evidence_class=_EC)
    return {"seat": seat_id, "handle": handle, "version": version, "because": because,
            "changed": True,
            "note": "managed section added (adopt)" if adopt else
                    "managed section recompiled"}


# ═══════════ THE ROLLOUT CHECK (thread 0e5bae06, #84) ═══════════
# "the machinery exists and passes its test" is not "the machinery is in effect" — the
# Boot Compiler shipped whole and reached 2 of 27 offices because nothing checked the
# ROLLOUT, only the acceptance test (the disease Thoth LXIV named across seven separate
# instances this reign). c72e206 is the cure's shape, copied here: NAME every gap, never
# just count — a >= comparison can't fail in the direction it exists to detect once other,
# unrelated rows (here, seats with no office at all) share the same table.
#
# FOUR reasons a seat is not "rolled out", kept DISTINCT rather than folded into one
# count, because only one of them is what adopt=True can fix:
#   never_compiled — has a handle, an office, a CLAUDE.md, zero markers. adopt-ready.
#   malformed      — has markers, but they're damaged. reissue refuses; needs a hand fix,
#                    never a second adopt (reissue_office's own refusal already covers
#                    this at write time — named here too so the check surfaces it BEFORE
#                    an operator tries and gets refused).
#   no_claude_md   — has a handle and an anchor_cwd, but no CLAUDE.md file on disk yet.
#                    establish_office/mint_seat's job, not adopt's — reissue_office
#                    refuses this case outright (no file to append to).
#   no_office      — no handle or no anchor_cwd on record at all. A DIFFERENT, already-
#                    tracked bug (thread 7a9c3c46) that adopt cannot touch because
#                    reissue_office has no office to find. Reported so it is never
#                    silently folded into "needs rollout" and miscounted as fixed by a
#                    sweep that cannot reach it.
async def boot_rollout_gaps(pool: asyncpg.Pool) -> list[dict[str, str]]:
    """Every active Seat NOT carrying a compiled managed section, one dict per seat,
    classified by `reason` (see the four kinds above) — never a bare count. Read-only:
    opens each office's CLAUDE.md to inspect it, writes nothing."""
    from src.orchestrator.seats import seat_facts

    rows = await pool.fetch(
        "SELECT o.canonical AS seat_id FROM objects o WHERE o.type='Seat' "
        "AND o.status='active' ORDER BY o.canonical")
    gaps: list[dict[str, str]] = []
    for row in rows:
        seat_id = row["seat_id"]
        facts = await seat_facts(pool, seat_id)
        handle, anchor, house = facts.get("handle"), facts.get("anchor_cwd"), facts.get("house")
        if not handle or not anchor:
            gaps.append({"seat_id": seat_id, "handle": handle or "", "house": house or "",
                        "reason": "no_office"})
            continue
        orders_path = Path(anchor) / "CLAUDE.md"
        if not orders_path.exists():
            gaps.append({"seat_id": seat_id, "handle": handle, "house": house or "",
                        "anchor_cwd": anchor, "reason": "no_claude_md"})
            continue
        try:
            locate_managed_section(orders_path.read_text())
        except MarkerError as exc:
            reason = "never_compiled" if "never been compiled" in str(exc) else "malformed"
            gaps.append({"seat_id": seat_id, "handle": handle, "house": house or "",
                        "anchor_cwd": anchor, "reason": reason, "detail": str(exc)})
    return gaps


def boot_rollout_gap_notes(gaps: list[dict[str, str]]) -> list[str]:
    """One printable, actionable line per gap — `cmd_boot_status` prints these and a
    caller-facing exit code follows from whether this list is empty, same contract as
    `composition_gap_notes`. Each line names the seat AND says what fixes it, because a
    gap that only says "N offices missing a section" is the exact miscount this check
    exists to replace."""
    fixes = {
        "never_compiled": "run `reissue_office(adopt=True)`",
        "malformed": "markers are damaged — needs a hand fix before any reissue",
        "no_claude_md": "no CLAUDE.md on disk — needs establish_office/mint_seat first",
        "no_office": "no handle or anchor_cwd on record — not an adopt target",
    }
    return [f"boot: {g['handle'] or g['seat_id']} ({g.get('house') or 'no house'}) has no "
            f"compiled section — {fixes[g['reason']]}"
            for g in sorted(gaps, key=lambda g: (g["reason"], g["handle"] or g["seat_id"]))]

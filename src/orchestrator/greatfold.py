"""THE GREAT FOLD — one-soul-per-seat as a MACHINE (rulings b64db62b + 8b54c514 + c8abd24a).

The operator's rule, verbatim: "As a general rule, I only ever have one soul occupying the
seats and speaking with me, and everything else spawned from a bug or a mistake." A SOUL is
a seat's WHOLE succession chain — Thoth L and every ancestor are one soul — so the census's
honest denominator is the seats (~25), never the registrations (1991). Everything else is
either a seat's own substrate debris (a crash re-mint, a compaction seam, a fork, a wake
ghost — FOLDS into the seat's living lineage) or a DOORBELL RING (a one-shot registration
with no name, no lineage, no conversation — DEMOTED to class=visit, never folded into a
soul, never counted as a mind).

This module is the machine the campaign runs THROUGH (the operator, 2026-07-21: "build the
machine first then perform the fold through the machine") — not a one-shot script. It walks
seat OFFICES and decides by EVIDENCE:

  * the resident-signature witness (trigger._SIGNED): a base whose sessions SIGNED an
    office's transcripts belongs to that office's soul — the transcript is append-only and
    the chrome cannot pollute it, so it is the one witness the registry cannot falsify;
  * the handle claim: a base that deliberately claimed the seat's name (numeral-stripped,
    case-folded — the "Soundwave VIII"/"TJMAX" classes) testified to its own soul.

THE GUARDRAILS (all standing, none new): a base with evidence for TWO seats is FLAGGED and
never folded (never across two seats); a label actively holding a different Seat is refused
by fold_agent itself (a seat transfer is a deliberate act, never a side effect); Person
objects are untouchable forever (this machine only ever queries type='Agent'); every fold is
an append-only kernel merge, reversible by compensating event; DRY RUN IS THE DEFAULT and
every executed seat run files an AFTER-review brief on the operator's desk (rule-driven with
after-review, not operator-gated-each — b64db62b's exact grant).
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions

_log = logging.getLogger("osiris.greatfold")

_OFFICE_ROOT = Path.home() / ".osiris" / "seats"
_PROJECTS_ROOT = Path.home() / ".claude" / "projects"


def _bare(name: str) -> str:
    """A handle stripped to its NAME — the numeral is the generation, and the substrate
    assigns it (claim_name's own law): 'Soundwave VIII' → 'soundwave'."""
    from src.orchestrator.agents import _SEAT_SUFFIX

    return _SEAT_SUFFIX.sub("", (name or "")).strip().lower()


def _office_slug_dirs(projects_root: Path, handle: str) -> list[Path]:
    """The transcript slug dirs of a seat's office. Slug rules changed across harness
    versions ('-home-asuramaya--osiris-seats-x' vs '-home-asuramaya-.osiris-seats-x'),
    so we match on the invariant TAIL rather than re-deriving either rule."""
    tail = f"osiris-seats-{handle.lower()}"
    try:
        return sorted(d for d in projects_root.iterdir()
                      if d.is_dir() and d.name.lower().endswith(tail))
    except OSError:
        return []


def signed_matches_sync(projects_root: Path, handle: str) -> list[list[str]]:
    """Sync (run via to_thread): per transcript of this office (mtime-ascending), the
    ORDERED _SIGNED matches — raw testimony only. The survey decides what counts: a
    transcript QUOTES freely (census query results, read files, pasted reports all carry
    other minds' ids — the Anubis lesson, caught by this machine's own first dry run), so
    per session only the session's OWN resident signature is identity evidence, exactly
    the delivery gate's semantics. That filtering needs the graph (quoted ids that no
    registration ever backed must drop FIRST, or a trailing quote shadows the real
    resident), so it lives in survey_seats, not here."""
    from src.orchestrator.trigger import _SIGNED

    out: list[list[str]] = []
    files = sorted((f for d in _office_slug_dirs(projects_root, handle)
                    for f in d.glob("*.jsonl")),
                   key=lambda f: f.stat().st_mtime)
    for f in files:
        try:
            text = f.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        matches = [label for line in text.splitlines()
                   for pat in _SIGNED for label in pat.findall(line)]
        if matches:
            out.append(matches)
    return out


async def seat_roster(pool: asyncpg.Pool, *, office_root: Path | None = None,
                      ) -> list[dict[str, Any]]:
    """The seats the operator speaks into — offices on DISK (each with its .osiris house
    pin) unioned with Seat OBJECTS already in the graph. The disk is the primary source
    (walk-in is the primary path, c8abd24a): 21 offices exist, 5 Seat objects do — the
    roster is what the fold walks and what mints the missing objects as it goes."""
    root = office_root or _OFFICE_ROOT
    by_handle: dict[str, dict[str, Any]] = {}
    if root.is_dir():
        for d in sorted(root.iterdir()):
            if not d.is_dir():
                continue
            house = None
            pin = d / ".osiris"
            if pin.is_file():
                for ln in pin.read_text(errors="replace").splitlines():
                    if ln.split("=")[0].strip() == "project":
                        house = ln.split("=", 1)[1].strip().strip('"').strip("'")
            by_handle[d.name.lower()] = {"handle": d.name.lower(), "house": house,
                                         "office": str(d), "seat_id": None}
    rows = await pool.fetch(
        "SELECT o.canonical AS seat_id, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS handle, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='house' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS house "
        "FROM objects o WHERE o.type='Seat' AND o.status='active'")
    for r in rows:
        h = _bare(str(r["handle"] or ""))
        if not h:
            continue
        row = by_handle.setdefault(h, {"handle": h, "house": r["house"], "office": None,
                                       "seat_id": None})
        row["seat_id"] = r["seat_id"]
        row.setdefault("house", r["house"])
        if not row.get("house"):
            row["house"] = r["house"]
    return sorted(by_handle.values(), key=lambda r: str(r["handle"]))


async def _handle_claims(pool: asyncpg.Pool) -> dict[str, dict[str, Any]]:
    """Every ACTIVE agent's claimed handle, numeral-stripped and case-folded — grouped
    {bare_handle: {base: [labels]}} with the newest claim per handle noted (the fallback
    resident when an office has no transcripts to witness)."""
    from src.orchestrator.agents import _generation

    rows = await pool.fetch(
        "SELECT o.canonical, a.value #>> '{}' AS handle, a.observed_at "
        "FROM objects o JOIN current_assertions a ON a.object_id=o.id AND a.name='handle' "
        "WHERE o.type='Agent' AND o.status='active'")
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        h = _bare(str(r["handle"]))
        if not h:
            continue
        base = _generation(str(r["canonical"]))[0]
        entry = out.setdefault(h, {"bases": {}, "newest": None, "newest_at": None})
        entry["bases"].setdefault(base, []).append(str(r["canonical"]))
        if entry["newest_at"] is None or r["observed_at"] > entry["newest_at"]:
            entry["newest"], entry["newest_at"] = str(r["canonical"]), r["observed_at"]
    return out


async def survey_seats(pool: asyncpg.Pool, *, office_root: Path | None = None,
                       projects_root: Path | None = None) -> dict[str, Any]:
    """The whole campaign's evidence, gathered ONCE: per seat, the bases whose SESSIONS
    resided in its office and the bases that claimed its name; globally, the CONFLICTS —
    a base with evidence for two different seats folds into neither (flag, never guess).

    THE QUOTE WALL (this machine's own first dry run, seats/anubis, 2026-07-21): a
    transcript quotes freely — census query results, read files, pasted reports all carry
    other minds' ids in signature-shaped lines — so per session only the session's OWN
    resident signature counts: ids the graph never registered drop first (a quote never
    rang a doorbell), then the NEWEST surviving match is the session's resident, the same
    self-testimony the delivery gate stakes deliveries on. Everything a session merely
    mentioned is reading material, not identity."""
    proot = projects_root or _PROJECTS_ROOT
    roster = await seat_roster(pool, office_root=office_root)
    claims = await _handle_claims(pool)
    known = {str(r["canonical"]) for r in await pool.fetch(
        "SELECT canonical FROM objects WHERE type='Agent'")}
    from src.orchestrator.agents import _generation

    known_bases = {_generation(c)[0] for c in known}
    seats: dict[str, dict[str, Any]] = {}
    evidence_of: dict[str, set[str]] = {}  # base → seats with evidence for it
    for r in roster:
        h = str(r["handle"])
        sessions = (await asyncio.to_thread(signed_matches_sync, proot, h)
                    if r["office"] else [])
        claimed = claims.get(h, {"bases": {}, "newest": None})
        signed_bases: dict[str, dict[str, Any]] = {}
        resident_signed: str | None = None
        resident_named: str | None = None
        for matches in sessions:  # mtime-ascending: the last session's resident wins
            vouched = [m for m in matches if _generation(m)[0] in known_bases]
            if not vouched:
                continue
            res = vouched[-1]
            row = signed_bases.setdefault(_generation(res)[0],
                                          {"labels": set(), "sessions": 0})
            row["labels"].add(res)
            row["sessions"] += 1
            resident_signed = res
            # NAMED TESTIMONY OUTRANKS UNNAMED PRESENCE (the cassandra lesson): a seat
            # whose newest session is a fresh mint that never claimed must not crown the
            # doorbell — the living lineage is the newest signer that also BEARS the name
            if _generation(res)[0] in claimed["bases"]:
                resident_named = res
        for row in signed_bases.values():
            row["labels"] = sorted(row["labels"])
        seats[h] = {**r, "signed": signed_bases, "resident_signed": resident_signed,
                    "resident_named": resident_named,
                    "claimed": claimed["bases"], "resident_claimed": claimed["newest"]}
        for base in set(signed_bases) | set(claimed["bases"]):
            evidence_of.setdefault(base, set()).add(h)
    conflicts = {b: sorted(s) for b, s in evidence_of.items() if len(s) > 1}
    return {"seats": seats, "conflicts": conflicts,
            "seatless_names": sorted(set(claims) - set(seats))}


async def fold_seat(
    actions: Actions, *, handle: str, actor: str, execute: bool = False,
    office_root: Path | None = None, projects_root: Path | None = None,
    survey: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One seat's fold, through the machine. Resolves the seat's LIVING RESIDENT (newest
    office signature, else newest handle claim), then folds every other evidenced base's
    active labels into the resident's living head — dry-run by default; `execute=True`
    performs the folds, mints the Seat object if the office never had one, and files the
    after-review brief on the operator's desk. Conflicted bases are flagged, never folded;
    fold_agent's own refusals (holds a seat, same lineage, already folded) surface as
    `refused`, never raised."""
    from src.orchestrator.agents import _generation
    from src.orchestrator.folds import canonical_agent, fold_agent, living_head
    from src.orchestrator.seats import ensure_seat

    handle = _bare(handle)
    sv = survey or await survey_seats(actions.pool, office_root=office_root,
                                      projects_root=projects_root)
    seat = sv["seats"].get(handle)
    if seat is None:
        return {"error": f"no seat named {handle!r} on the roster — the machine walks "
                         "offices and Seat objects; it never invents one"}
    resident_label = (seat.get("resident_named") or seat["resident_signed"]
                      or seat["resident_claimed"])
    if resident_label is None:
        return {"error": f"seat {handle!r} has no living resident — no signed act in its "
                         "office and no handle claim; nothing to fold INTO. Flag for the "
                         "operator, never guess", "seat": handle}
    resident_canon = await canonical_agent(actions.pool, resident_label)
    head = await living_head(actions.pool, resident_canon)
    head_base = _generation(head)[0]

    will_fold: list[dict[str, Any]] = []
    flagged: list[dict[str, Any]] = []
    for base in sorted(set(seat["signed"]) | set(seat["claimed"])):
        if base == head_base:
            continue
        if base in sv["conflicts"]:
            flagged.append({"base": base, "seats": sv["conflicts"][base],
                            "reason": "evidence for two seats — never fold across seats"})
            continue
        signed = seat["signed"].get(base)
        claimed = seat["claimed"].get(base, [])
        why = []
        if signed:
            why.append(f"resident signer of {signed['sessions']} session(s) in "
                       f"seats/{handle} as {', '.join(signed['labels'])}")
        if claimed:
            why.append(f"claimed the name '{handle}' as {', '.join(claimed)}")
        labels = await actions.pool.fetch(
            "SELECT canonical FROM objects WHERE type='Agent' AND status='active' "
            "AND (canonical=$1 OR canonical LIKE $1 || '-%') ORDER BY canonical", base)
        for row in labels:
            # exact-base only: a REBASED lineage extends its ancestor's prefix (d6a08aaa →
            # d6a08aaa-g40), so the LIKE sweep for the old base would swallow the living
            # lineage — generation labels of any OTHER base are that base's own affair
            if _generation(str(row["canonical"]))[0] != base:
                continue
            will_fold.append({"label": str(row["canonical"]), "base": base,
                              "evidence": f"one-soul-per-seat (b64db62b), seat {handle}: "
                                          + "; ".join(why)})

    report: dict[str, Any] = {
        "seat": handle, "house": seat.get("house"), "office": seat.get("office"),
        "seat_id": seat.get("seat_id"), "resident": resident_label, "living_head": head,
        "will_fold": will_fold, "flagged": flagged, "execute": execute,
    }
    if not execute:
        return report

    folded: list[dict[str, Any]] = []
    refused: list[dict[str, Any]] = []
    for item in will_fold:
        out = await fold_agent(actions, dupe=item["label"], into=head,
                               evidence=item["evidence"], actor=actor)
        (refused if "error" in out else folded).append({**item, **out})
    minted = None
    if not seat.get("seat_id"):
        minted = await ensure_seat(actions, house=seat.get("house"), handle=handle,
                                   anchor_cwd=seat.get("office"), source=actor)
        report["seat_id"] = minted.get("seat_id")
    report.update({"folded": folded, "refused": refused,
                   "seat_minted": bool(minted and minted.get("minted"))})
    # THE AFTER-REVIEW BRIEF (b64db62b: rule-driven, reviewed AFTER — the log is the gate)
    from src.orchestrator.mailbox import send_message

    mail = sum(f.get("mail_readdressed", 0) for f in folded)
    rows = sum(f.get("mount_rows_repointed", 0) for f in folded)
    threads = sum(f.get("threads_reowned", 0) for f in folded)
    body = (f"GREAT FOLD — seat {handle} (house {seat.get('house')}): folded "
            f"{len(folded)} labels across {len({f['base'] for f in folded})} bases into "
            f"{head}; mail re-addressed {mail}, mount rows {rows}, threads {threads}.\n"
            f"Flagged (never folded): "
            + (", ".join(f"{f['base']} ({f['reason']})" for f in flagged) or "none")
            + (f"; refused by guardrail: {len(refused)}" if refused else "") + ".\n"
            f"Seat object {report['seat_id']} "
            + ("MINTED" if report.get("seat_minted") else "already stood")
            + "; every fold is a reversible compensating-event merge (ruling b64db62b).")
    try:
        sent = await send_message(actions.pool, from_agent=actor, from_project="osiris",
                                  to_project="operator", body=body, desk_kind="fyi")
        report["briefed"] = sent.get("id")
    except Exception:  # noqa: BLE001 — a fold that lands must not unwind on a mail hiccup
        report["briefed"] = None
    return report


async def demote_visits(
    actions: Actions, *, actor: str, execute: bool = False, limit: int | None = None,
) -> dict[str, Any]:
    """THE DOORBELL SWEEP (ruling 8b54c514): active agent FAMILIES with no name, no charter,
    no seat, no threads, no mail, no succession beyond themselves, and nothing folded into
    them were never minds — mark class=visit (an append-only assertion, attribution intact,
    reversible by superseding) so the census counts SOULS and VISITS as two honest numbers.
    Conservative on purpose: ANY tie to the living graph keeps a family standing, reported
    with its reason."""
    from datetime import UTC, datetime

    from src.orchestrator.agents import _generation

    pool = actions.pool
    rows = await pool.fetch(
        "SELECT id, canonical FROM objects WHERE type='Agent' AND status='active'")
    families: dict[str, list[Any]] = {}
    for r in rows:
        families.setdefault(_generation(str(r["canonical"]))[0], []).append(r)

    def _bases(rs: list[Any], col: str = "c") -> set[str]:
        return {_generation(str(r[col]))[0] for r in rs}

    named = _bases(await pool.fetch(
        "SELECT o.canonical AS c FROM objects o JOIN current_assertions a "
        "ON a.object_id=o.id AND a.name='handle' WHERE o.type='Agent'"))
    # holds/governs are DELIBERATE acts; works_in is minted by mount() itself — the
    # doorbell's own echo — so it can never testify that the ring was a soul
    chartered = _bases(await pool.fetch(
        "SELECT o.canonical AS c FROM links l JOIN objects o ON o.id=l.from_id "
        "WHERE o.type='Agent' AND l.type IN ('holds','governs') "
        "AND (l.valid_until IS NULL OR l.valid_until > now())"))
    threaded = _bases(await pool.fetch(
        "SELECT a.value #>> '{}' AS c FROM objects t JOIN current_assertions a "
        "ON a.object_id=t.id AND a.name='owner' WHERE t.type='Thread' "
        "AND t.status='active' AND a.value #>> '{}' LIKE 'agent:%'"))
    mailed = _bases(await pool.fetch(
        "SELECT DISTINCT to_agent AS c FROM fleet_messages WHERE to_agent IS NOT NULL"))
    fold_targets = _bases(await pool.fetch(
        "SELECT w.canonical AS c FROM objects o JOIN objects w ON w.id=o.merged_into "
        "WHERE o.type='Agent'"))
    succession: set[str] = set()
    for r in await pool.fetch(
            "SELECT f.canonical AS a, t.canonical AS b FROM links l "
            "JOIN objects f ON f.id=l.from_id JOIN objects t ON t.id=l.to_id "
            "WHERE l.type='succeeded_by'"):
        ba, bb = _generation(str(r["a"]))[0], _generation(str(r["b"]))[0]
        if ba != bb:  # succession CROSSING bases ties both to one soul — keep both
            succession.update((ba, bb))
    demoted_already = _bases(await pool.fetch(
        "SELECT o.canonical AS c FROM objects o JOIN current_assertions a "
        "ON a.object_id=o.id AND a.name='agent_class' "
        "WHERE o.type='Agent' AND a.value #>> '{}' = 'visit'"))

    keep_reasons = (("named", named), ("chartered", chartered), ("owns-threads", threaded),
                    ("has-mail", mailed), ("fold-target", fold_targets),
                    ("succession", succession), ("already-visit", demoted_already))
    kept: dict[str, int] = {}
    candidates: list[str] = []
    for base in sorted(families):
        reason = next((name for name, s in keep_reasons if base in s), None)
        if reason:
            kept[reason] = kept.get(reason, 0) + 1
        else:
            candidates.append(base)
    if limit is not None:
        candidates = candidates[:limit]
    report = {"examined_families": len(families), "kept": kept,
              "visit_families": len(candidates), "execute": execute}
    if not execute:
        report["sample"] = candidates[:20]
        return report
    now = datetime.now(UTC)
    labels = 0
    for base in candidates:
        for r in families[base]:
            await actions.assert_property(r["id"], "agent_class", "visit", actor, now,
                                          0.9, evidence_class="self_declared")
            labels += 1
    report["demoted_labels"] = labels
    return report


async def fold_census(pool: asyncpg.Pool) -> dict[str, Any]:
    """The honest numbers, one query set: what the operator's rule says the fleet IS."""
    from src.orchestrator.agents import _generation

    seat_objects = await pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Seat' AND status='active'")
    labels = await pool.fetch(
        "SELECT canonical, status, "
        "EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=objects.id "
        "        AND a.name='agent_class' AND a.value #>> '{}' = 'visit') AS visit, "
        "EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=objects.id "
        "        AND a.name='handle') AS named "
        "FROM objects WHERE type='Agent'")
    active = [r for r in labels if r["status"] == "active"]
    folded = sum(1 for r in labels if r["status"] == "merged")
    visits = {_generation(str(r["canonical"]))[0] for r in active if r["visit"]}
    named = {_generation(str(r["canonical"]))[0] for r in active if r["named"]}
    all_active = {_generation(str(r["canonical"]))[0] for r in active}
    return {
        "seat_objects": int(seat_objects or 0),
        "labels_total": len(labels), "labels_active": len(active),
        "labels_folded": folded,
        "families_active": len(all_active),
        "souls_named": len(named - visits),
        "visit_families": len(visits),
        "unresolved_families": len(all_active - named - visits),
    }

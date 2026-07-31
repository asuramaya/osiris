"""PROJECT IDENTITY — the read-only evidence resolver (#110, decision 1db1ff41, Thoth's
dispatch DM 2427/2435, built first and alone per his explicit order).

Before a rename or a fork can be DECLARED, someone has to see what the evidence actually
says, tier by tier, without any one tier being crowned first for every case. Thoth's own
proposed "the git remote is authoritative when it exists" rule correctly resolves xxit,
tony, RAMstein and ByeByte — and fails completely on ballgem, John's real working repo,
which has no origin remote at all (Sekhmet, decision e221128e). No single evidence source
ranks first across the population: remote is authoritative for xxit and blind for ballgem;
write-attribution is authoritative for ballgem and blind for xxit (100% of xxit's own work
is filed under the stale name, which is why it went undetected for weeks). This module
supplies the naming/marking machinery that trap demands — NEVER a fixed precedence pick —
and reuses Sekhmet's own five-tier catalog (4e2cfeb6/e221128e) rather than inventing new
ones:

  OPERATOR_CONFIRMED — a caller-SUPPLIED citation (a decision id / quote). This function
    never parses decision prose looking for a quote; that is a human's read, not a query,
    and pretending otherwise would be the same silent-pick failure this module exists to
    refuse. Reported back verbatim, `checked=True`, so a caller who already did that
    reading gets it folded into one report rather than a second lookup.

  DECLARED_CHARTER — a seat's own governs edges, checked from BOTH origins: the Seat
    object directly (ruling 1db1ff41's ruling 3, the re-key this whole primitive depends
    on) AND every Agent lineage that has ever held the seat (ruling 3 shipped the schema
    and code, b9b5ce9, but `migrate_charter_to_seat` is dry-run-only as of this build —
    every live governs edge today is still Agent-typed; the day it flips this tier keeps
    working unchanged, because it already checks both).

  SELF_AUTHORED — EXISTENCE only, never content: a seat's own CLAUDE.md/charter.md is
    reported by path/size/mtime. Reading its prose for a project claim is the same
    human-judgment problem as tier 1, not a query this function pretends to answer.

  PIN — the seat's own `.osiris` project= label, read at its office (anchor_cwd).

  REMOTE — `git remote get-url origin`, run against each CANDIDATE project's registered
    on_disk_path (census_trees's own stored fact, never a guessed root — the same
    discipline discover_trees already holds). A seat's own anchor_cwd is its OFFICE, not
    its code repo (mintseat.py/greatfold.py: anchor_cwd is always the office path), so
    remote is checked per-candidate against the graph's on_disk_path, not against the
    seat directly.

  WRITE_ATTRIBUTION (DERIVED, weakest, flagged explicitly wherever it's the only signal) —
    the majority in_repo target across every Thread/Decision this seat's lineage has ever
    filed, the ballgem tier by necessity.

Nothing here writes. correct_project_name / rename_project / fork_project (still to come)
are the verbs a caller invokes once a human has read this report and made the declared
call — this module's whole job ends at naming the evidence.
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import asyncpg


def _remote_basename(url: str | None) -> str | None:
    """The repo name a remote URL implies, for comparison against a bare project label —
    'git@github.com:x/handlingtheloop.git' and 'https://github.com/x/handlingtheloop' both
    read as 'handlingtheloop'. None in, None out."""
    if not url:
        return None
    tail = url.rstrip("/").rsplit("/", 1)[-1]
    if ":" in tail and "/" not in url.rstrip("/").rsplit("/", 1)[-1]:
        tail = tail.rsplit(":", 1)[-1]
    return tail.removesuffix(".git") or None


def _git_remote(path: str) -> tuple[bool, str | None]:
    """(is_a_git_repo, origin_url_or_None) at `path` — same subprocess.run/try-except
    shape as pulse.py's own _git_head/repo_name, never raising on a missing/broken repo."""
    try:
        top = subprocess.run(["git", "-C", path, "rev-parse", "--show-toplevel"],
                             capture_output=True, text=True, timeout=5, check=True)
    except (OSError, subprocess.SubprocessError, subprocess.CalledProcessError):
        return False, None
    if not top.stdout.strip():
        return False, None
    try:
        url = subprocess.run(["git", "-C", path, "remote", "get-url", "origin"],
                             capture_output=True, text=True, timeout=5, check=True)
        return True, (url.stdout.strip() or None)
    except (OSError, subprocess.SubprocessError, subprocess.CalledProcessError):
        return True, None  # a real repo, just no `origin` configured — ballgem's own shape


def _self_authored(office: str | None) -> dict[str, Any]:
    """Existence/path/size/mtime for a seat's own CLAUDE.md and charter.md at its office —
    never their content. What they SAY is a human's read (same reasoning as tier 1); what
    this can honestly report is that they exist and when they last changed."""
    out: dict[str, Any] = {}
    for fname in ("CLAUDE.md", "charter.md"):
        out[fname] = {"exists": False, "path": None, "size": None, "mtime": None}
        if not office:
            continue
        p = Path(office) / fname
        if p.is_file():
            st = p.stat()
            out[fname] = {"exists": True, "path": str(p), "size": st.st_size,
                          "mtime": st.st_mtime}
    return out


async def _seat_lineage_bases(pool: asyncpg.Pool, seat_oid: Any) -> list[str]:
    """Every Agent lineage BASE that has ever held this seat (any generation, healed or
    active, `holds` link history) — write-attribution and the still-live Agent-origin
    governs edges both need this: a seat's holder churns across successions, but its
    authored history, and (until migrate_charter_to_seat actually runs) its charter, stay
    keyed on whichever generation was live when each was asserted."""
    from src.orchestrator.agents import _generation

    rows = await pool.fetch(
        "SELECT DISTINCT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='holds'", seat_oid)
    return sorted({_generation(str(r["canonical"]))[0] for r in rows})


async def _declared_charter(pool: asyncpg.Pool, seat_id: str, seat_oid: Any,
                            bases: list[str]) -> list[str]:
    """governs targets, checked from BOTH origins (module docstring) — bare project
    labels, deduplicated."""
    rows = list(await pool.fetch(
        "SELECT DISTINCT ro.canonical FROM links l JOIN objects ro ON ro.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='governs' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", seat_oid))
    if bases:
        rows += await pool.fetch(
            "SELECT DISTINCT ro.canonical FROM links l "
            "JOIN objects fo ON fo.id=l.from_id AND fo.type='Agent' "
            "JOIN objects ro ON ro.id=l.to_id "
            "WHERE l.type='governs' AND (l.valid_until IS NULL OR l.valid_until > now()) "
            "AND EXISTS (SELECT 1 FROM unnest($1::text[]) b "
            "            WHERE fo.canonical=b OR fo.canonical LIKE b || '-%')", bases)
    return sorted({str(r["canonical"]).removeprefix("repo:") for r in rows})


async def _write_attribution(pool: asyncpg.Pool, bases: list[str]) -> dict[str, Any]:
    """The majority in_repo target across every Thread/Decision/Commit this lineage's
    write-attribution names — DERIVED, the weakest tier, John/ballgem's own evidence
    (145 of 163) when nothing else has signal at all."""
    if not bases:
        return {"total": 0, "top": None, "breakdown": {}}
    rows = await pool.fetch(
        "SELECT ro.canonical AS project, count(*) AS n FROM links l "
        "JOIN objects ro ON ro.id=l.to_id AND ro.type='SoftwareProject' "
        "WHERE l.type='in_repo' "
        "AND EXISTS (SELECT 1 FROM unnest($1::text[]) b "
        "            WHERE l.source_id=b OR l.source_id LIKE b || '-%') "
        "GROUP BY ro.canonical ORDER BY n DESC", bases)
    breakdown = {str(r["project"]).removeprefix("repo:"): int(r["n"]) for r in rows}
    total = sum(breakdown.values())
    top = next(iter(breakdown), None)
    return {"total": total, "top": top, "breakdown": breakdown}


async def project_identity_evidence(
    pool: asyncpg.Pool, *, seat_id: str, operator_citation: str | None = None,
) -> dict[str, Any]:
    """Gather whichever tiers have signal for `seat_id`'s project identity and report each
    one's answer plus per-candidate agreement/disagreement — NEVER picking a winner. See
    the module docstring for the five tiers and why no fixed precedence list can work
    across the population (xxit/ballgem are each other's counter-example).

    Every distinct project label surfaced by ANY tier becomes a CANDIDATE row, cross-
    checked against every other tier that can speak to it (declared_charter membership,
    pin match, write-attribution share, and — via the candidate's own registered
    on_disk_path, never a guessed root — a live git remote check). A seat with zero
    holders ever (bases == []) still runs: pin/remote/self-authored are cwd-based, not
    lineage-based, and John-shaped cases (a seat whose whole history predates the Seat
    object) must not go blind just because `bases` came back empty."""
    from src.orchestrator.projects import _resolve_software_project
    from src.orchestrator.seats import seat_facts

    row = await pool.fetchrow("SELECT id FROM objects WHERE canonical=$1 AND type='Seat'",
                              seat_id)
    if row is None:
        return {"error": f"no such seat: {seat_id!r}"}
    seat_oid = row["id"]
    facts = await seat_facts(pool, seat_id)
    office = facts.get("anchor_cwd")
    bases = await _seat_lineage_bases(pool, seat_oid)
    charter = await _declared_charter(pool, seat_id, seat_oid, bases)
    pin = None
    if office:
        from src.orchestrator.agents import read_project_label
        pin = read_project_label(office)
    self_authored = _self_authored(office)
    write_attr = await _write_attribution(pool, bases)

    candidate_names = sorted({
        *charter,
        *([pin] if pin else []),
        *(k for k in write_attr["breakdown"]),
    })
    candidates: dict[str, Any] = {}
    for name in candidate_names:
        entry: dict[str, Any] = {
            "declared_charter": name in charter,
            "pin_match": name == pin,
            "write_attribution": {
                "count": write_attr["breakdown"].get(name, 0), "total": write_attr["total"],
            },
            "on_disk_path": None, "is_git_repo": None, "remote_url": None,
            "remote_agrees": None,
        }
        proj_row = await _resolve_software_project(pool, name)
        if proj_row is not None:
            path = await pool.fetchval(
                "SELECT a.value #>> '{}' FROM current_assertions a "
                "WHERE a.object_id=$1 AND a.name='on_disk_path' "
                "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", proj_row["id"])
            entry["on_disk_path"] = path
            if path:
                is_repo, remote_url = _git_remote(path)
                entry["is_git_repo"] = is_repo
                entry["remote_url"] = remote_url
                if remote_url:
                    entry["remote_agrees"] = (_remote_basename(remote_url) == name)
        candidates[name] = entry

    # write_attribution only counts as SUPPORT for the majority target — a single stray
    # commit filed under the wrong project is not the same claim as 145/163 (ballgem)
    supported = {n for n, e in candidates.items()
                if e["declared_charter"] or e["pin_match"] or e["remote_agrees"]
                or n == write_attr["top"]}
    if len(supported) <= 1:
        agreement = "single-candidate" if supported else "no-signal"
    else:
        agreement = "disagree"

    return {
        "seat_id": seat_id, "handle": facts.get("handle"), "office": office,
        "lineage_bases": bases,
        "operator_confirmed": {"citation": operator_citation,
                               "checked": operator_citation is not None},
        "self_authored": self_authored,
        "candidates": candidates,
        "agreement": agreement,
    }

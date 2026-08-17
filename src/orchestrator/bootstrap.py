"""Bootstrap — onboard a project by migrating its markdown memory into the graph.

The generalization of what we did to osiris tonight: a project's CLAUDE.md build log,
DESIGN.md, and memory/ essays are flat, append-only, re-injected-into-every-context BLOAT.
This ingests them into the shared graph as retrieval-sized `Reference` nodes (dated history
entries, design sections, canon docs) — so the knowledge becomes a bounded QUERY
(`consult_canon`) instead of a dump carried in every window — and registers the project.

Deliberately NO HANDS on the project's files: it READS the mds and WRITES the graph; it does
NOT overwrite the project's CLAUDE.md. It returns a suggested boot-sector template, and the
project's own agent (which has hands in its own repo) reviews + writes it. The knowledge
migration is Osiris's; the file shrink is the agent's — same split we used on osiris.

Idempotent: canonical find-or-create + the kernel's byte-dup assertion skip absorb re-runs.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.ingest.reference import ingest_log, ingest_reference_doc
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_EC = EvidenceClass.SELF_DECLARED
_CONF = confidence_for(_EC)

# What the migration recognizes as a project's own MEMORY vs its public exports. Two shapes:
#   LOGS  — a dated build log (CLAUDE.md) / design doc (DESIGN.md), chunked PER-ENTRY;
#   DOCS  — working notes / essays under a memory dir, one Reference each.
# Repos differ in layout, so we look in the common PLACES, not one shape.
_LOGS = (("CLAUDE.md", "history"), ("DESIGN.md", "design"))
_LOG_DIRS = ("", "docs")  # a log may sit at the repo root or under docs/
_DOC_DIRS = ("docs", "memory", "paper")  # where a project's working notes / essays live
# exports / index stubs kept as files, never ingested as memory (md-kill ruling):
_EXPORTS = frozenset({"README.md", "ARCHITECTURE.md", "CONTRIBUTING.md",
                      "CODE_OF_CONDUCT.md", "MEMORY.md"})


def _plan(path: str, project: str | None) -> tuple[str, list[tuple[str, str, str]], list[str]]:
    """Sync (all path/file IO lives here, off the async body): resolve the project name and
    discover its memory mds — dated LOGS as (path, topic, display) + DOC essay paths.

    Layout-tolerant, not osiris-shaped: a log is found at the root OR under docs/; essays are
    the top-level *.md under docs/ | memory/ | paper/ plus a root PLAN.md. Exports
    (README/ARCHITECTURE/…) and anything already taken as a log are excluded. Deliberately not
    a blind recursive sweep — that would drag in .venv / node_modules / a public web/ tree."""
    root = Path(path).expanduser()
    proj = project or root.name
    log_specs: list[tuple[str, str, str]] = []
    seen: set[Path] = set()
    for fname, topic in _LOGS:
        for d in _LOG_DIRS:
            p = (root / d / fname) if d else (root / fname)
            if p.is_file():
                log_specs.append((str(p), topic, f"{d}/{fname}" if d else fname))
                seen.add(p.resolve())
                break  # first location wins per log type
    candidates: list[Path] = []
    for d in _DOC_DIRS:
        sub = root / d
        if sub.is_dir():
            candidates.extend(sorted(sub.glob("*.md")))
    if (root / "PLAN.md").is_file():
        candidates.append(root / "PLAN.md")
    essays: list[str] = []
    for p in candidates:
        rp = p.resolve()
        if p.name in _EXPORTS or rp in seen:  # skip exports + the log already taken
            continue
        seen.add(rp)
        essays.append(str(p))
    return proj, log_specs, essays


def _essay_display(essay: str) -> str:
    p = Path(essay)
    return f"{p.parent.name}/{p.name}" if p.parent.name in _DOC_DIRS else p.name


def _boot_template(project: str, entries: int) -> str:
    """A generic boot-sector CLAUDE.md the project's agent reviews + writes. Constant-size:
    identity check + mount ritual + a constitution the agent fills. The history it replaces
    now lives in the graph (`ref:<project>-history-*`)."""
    return f"""# CLAUDE.md — boot sector

{project} is backed by Osiris — the shared memory graph. This file is the key, not the
memory. {entries} history/design/essay entries are indexed in the graph
(`ref:{project}-*`, `consult_canon`) — the source docs stay on disk; this file just points
in. DO NOT append history here.

## Mount FIRST (before anything)
Osiris MCP is at http://127.0.0.1:8790/mcp (a `.mcp.json` points here). On connect:
`mount(cwd=<this dir>, job_dir=$CLAUDE_JOB_DIR)` → `orient()` → then write back AS YOU GO
(`record_decision` / `open_thread` / `resolve_thread`). The graph, not the window, is memory.

## Identity check
If your environment names a different model than expected, SAY SO in your first reply —
a rug-pull is confessed, never inherited blind.

## {project} constitution (fill in — the invariants that rarely change)
- (the rules an agent here must never break)

## Stack & run (fill in)
- (how to build/test this project)
"""


async def bootstrap_project(
    actions: Actions, path: str, *, project: str | None = None, source: str = "ref:osiris"
) -> dict[str, Any]:
    """Migrate `path`'s markdown memory into the graph + register the project. Returns what
    was ingested and a suggested boot-sector for the agent to review + write itself.

    `source` STAMPS EVERY WRITE THIS CALL MAKES (the SoftwareProject registration and every
    ingested log entry — `ingest_reference_doc`'s essays are unaffected, their provenance is
    the document's own claimed vendor, not the caller). Defaults to the synthetic
    "ref:osiris" for the bare-CLI case (`main()` below, no caller identity exists at all);
    the MCP tool wrapper (mcp_server.bootstrap) passes the real mounted caller instead.
    FIXED 2026-08-03 (Thoth's Tier 1 dispatch off the silent-authority census, decision
    497a066a): every write used to be hardcoded "ref:osiris" regardless of who actually
    called bootstrap — worse than anonymous, a bad injection read as deliberate system
    canon and could not be traced back to a caller even after the fact.

    `project`, when explicit, goes through task #107's choke point (capture.py's
    `_validate_repo_name`, reused verbatim rather than re-validated here — same law as
    lineage.py's register_spawn/register_swarm): a path-shaped or otherwise malformed
    caller-supplied name refuses BEFORE any object is touched. The `project or root.name`
    default (`_plan`'s own fallback) is a directory basename, not caller text, so it never
    needs this — the guard only ever fires on an EXPLICIT `project=` argument."""
    project, log_specs, essays = _plan(path, project)
    from src.orchestrator.capture import _validate_repo_name
    try:
        _validate_repo_name(project, project)
    except ValueError as exc:
        return {"error": str(exc)}
    now = datetime.now(UTC)

    proj = await actions.create_or_find_object("SoftwareProject", f"repo:{project}", source)
    await actions.assert_property(proj, "name", project, source, now, _CONF,
                                  evidence_class=_EC.value)

    ingested: list[dict[str, Any]] = []
    total = 0
    for fpath, topic, display in log_specs:
        r = await ingest_log(actions, fpath, topic=f"{project}-{topic}", source=source)
        total += r["entries"]
        ingested.append({"file": display, "entries": r["entries"], "as": "log"})
    for essay in essays:
        await ingest_reference_doc(actions, essay)
        total += 1
        ingested.append({"file": _essay_display(essay), "entries": 1, "as": "doc"})

    migrated = (
        f"Indexed into the graph (ref:{project}-*, query via consult_canon). Migration is "
        "ADDITIVE: this is a second, queryable index over your docs — it never replaces or "
        "removes them. The one file to shrink is CLAUDE.md (write the suggested boot sector) "
        "so its history stops loading into every context; your source docs STAY on disk, and "
        "Osiris never touches your files. Then mount() and orient()."
    )
    return {
        "project": project,
        "registered": str(proj),
        "ingested": ingested,
        "entries": total,
        "suggested_boot_sector": _boot_template(project, total),
        "note": migrated if total else
        "No CLAUDE.md/DESIGN.md/memory found — nothing to migrate. mount() and start clean.",
    }


def main() -> None:  # pragma: no cover - CLI
    import asyncio
    import sys

    from src.config.settings import get_settings
    from src.db.pool import create_pool

    path = sys.argv[1] if len(sys.argv) > 1 else "."

    async def run() -> None:
        pool = await create_pool(get_settings().database_url)
        try:
            res = await bootstrap_project(Actions(pool), path)
            print(f"project={res['project']} entries={res['entries']}")
            for i in res["ingested"]:
                print(f"  {i['file']:24} {i['entries']:>3} {i['as']}")
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":  # pragma: no cover
    main()

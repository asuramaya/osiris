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

# what the migration recognizes as a project's own MEMORY (not its public exports —
# README/ARCHITECTURE stay as human-facing docs; those are kept, per the md-kill ruling).
_LOGS = (("CLAUDE.md", "history"), ("DESIGN.md", "design"))


def _plan(path: str, project: str | None) -> tuple[str, list[tuple[str, str, str]], list[str]]:
    """Sync (all path/file IO lives here, off the async body): resolve the project name and
    discover its memory mds — the dated logs as (path, topic, display) and the memory/ essay
    paths. Conservative: only files we know are project memory, never a directory sweep."""
    root = Path(path).expanduser()
    proj = project or root.name
    log_specs = [(str(root / f), t, f) for f, t in _LOGS if (root / f).is_file()]
    essays: list[str] = []
    mem = root / "memory"
    if mem.is_dir():
        essays = [str(p) for p in sorted(mem.glob("*.md")) if p.name != "MEMORY.md"]
    return proj, log_specs, essays


def _essay_display(essay: str) -> str:
    return f"memory/{Path(essay).name}"


def _boot_template(project: str, entries: int) -> str:
    """A generic boot-sector CLAUDE.md the project's agent reviews + writes. Constant-size:
    identity check + mount ritual + a constitution the agent fills. The history it replaces
    now lives in the graph (`ref:<project>-history-*`)."""
    return f"""# CLAUDE.md — boot sector

{project} is backed by Osiris — the shared memory graph. This file is the key, not the
memory. {entries} history/design/essay entries were migrated to the graph
(`ref:{project}-*`, `consult_canon`); DO NOT append history here.

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
    actions: Actions, path: str, *, project: str | None = None
) -> dict[str, Any]:
    """Migrate `path`'s markdown memory into the graph + register the project. Returns what
    was ingested and a suggested boot-sector for the agent to review + write itself."""
    project, log_specs, essays = _plan(path, project)
    now = datetime.now(UTC)

    proj = await actions.create_or_find_object("SoftwareProject", f"repo:{project}", "ref:osiris")
    await actions.assert_property(proj, "name", project, "ref:osiris", now, _CONF,
                                  evidence_class=_EC.value)

    ingested: list[dict[str, Any]] = []
    total = 0
    for fpath, topic, display in log_specs:
        r = await ingest_log(actions, fpath, topic=f"{project}-{topic}")
        total += r["entries"]
        ingested.append({"file": display, "entries": r["entries"], "as": "log"})
    for essay in essays:
        await ingest_reference_doc(actions, essay)
        total += 1
        ingested.append({"file": _essay_display(essay), "entries": 1, "as": "doc"})

    migrated = (
        f"Knowledge migrated to the graph (ref:{project}-*). Review the suggested boot "
        "sector, write it as your CLAUDE.md, and archive the originals — Osiris does not "
        "touch your files. Then mount() and orient()."
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

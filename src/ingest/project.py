"""One-shot project ingest — the atomic "ingest this repo" flow.

Dogfood finding (2026-07-02): `gitlog`, `files`, and `decisions` were three separate CLI
steps, and it was easy to run one and forget the rest — osiris had 161 commits but 0 files
because only gitlog had run. A repo isn't really "ingested" until its history, its file
tree, AND its decisions are all in the graph. This composes the three idempotent passes into
one call so a repo goes in whole.

    python -m src.ingest.project /path/to/repo

Each pass is idempotent (find-or-create on sha / dev-email / canonical / content-hash), so
re-running is safe and only adds what's new.
"""

from __future__ import annotations

from typing import Any

from src.actions.core import Actions
from src.ingest.decisions import mine_decisions
from src.ingest.files import ingest_files
from src.ingest.gitlog import ingest_repo


async def ingest_project(actions: Actions, path: str = ".", *, mine: bool = True) -> dict[str, Any]:
    """Ingest a repo WHOLE: git history → file tree → mined decisions, in order. Idempotent.

    `mine=False` skips the decision pass — for a BATCH ingest, run mine_decisions ONCE at the
    end instead of per-repo (it scans every Commit globally each time, so it's wasteful in a
    loop). A single-repo ingest mines by default so the repo is complete on its own.
    """
    history = await ingest_repo(actions, path)
    files = await ingest_files(actions, path)
    out: dict[str, Any] = {"repo": history.get("repo"), "history": history, "files": files}
    if mine:
        out["decisions"] = await mine_decisions(actions)
    return out


def main() -> None:  # pragma: no cover - CLI
    import asyncio
    import sys

    from src.config.settings import get_settings
    from src.db.pool import create_pool

    path = sys.argv[1] if len(sys.argv) > 1 else "."

    async def run() -> None:
        pool = await create_pool(
            get_settings().database_url, application_name="osiris-script:ingest-project")
        try:
            print(await ingest_project(Actions(pool), path))
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":  # pragma: no cover
    main()

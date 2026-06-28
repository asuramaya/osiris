"""Git-history ingest — Osiris tracking its own genesis (and any repository).

Proof that the engine is a GENERAL substrate, not OSINT-only: a git history is just
another structured source. The same collector pattern every federator uses — a source →
graded objects/links through the Actions waist — maps a repo's commits, developers, and
the history DAG into the entity graph. So Osiris can model its own development, the first
commit onward. The git log is the authoritative record (facts land AUTHORITATIVE_API),
and the commit date is the observed-at clock (time-travel the graph by commit).

  SoftwareProject ──in_repo── Commit ──authored_by── Person(dev)
                              Commit ──follows──────► parent Commit

Run: `python -m src.ingest.gitlog [path] [limit]`.
"""

from __future__ import annotations

import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SOURCE = "git"
_EC = EvidenceClass.AUTHORITATIVE_API.value
_CONF = confidence_for(EvidenceClass.AUTHORITATIVE_API)
# unit-separator delimited fields, record-separated — robust against newlines in subjects
_FMT = "%H%x1f%an%x1f%ae%x1f%aI%x1f%P%x1f%s%x1e"


@dataclass
class Commit:
    sha: str
    author_name: str
    author_email: str
    date: str  # ISO 8601 with offset
    parents: list[str] = field(default_factory=list)
    subject: str = ""


def parse_git_log(raw: str) -> list[Commit]:
    """Pure: a delimited `git log` dump → commits (genesis first if --reverse)."""
    out: list[Commit] = []
    for record in raw.split("\x1e"):
        rec = record.strip("\n")
        if not rec.strip():
            continue
        f = rec.split("\x1f")
        if len(f) < 6:
            continue
        out.append(Commit(f[0], f[1], f[2], f[3],
                          f[4].split() if f[4].strip() else [], f[5]))
    return out


def _git(path: str, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", path, *args], capture_output=True, text=True, check=True
    ).stdout


def read_commits(path: str, *, limit: int | None = None) -> list[Commit]:
    """Read a repo's history, genesis first. `limit` takes the most-recent N (git -n)."""
    args = ["log", "--reverse", f"--pretty=format:{_FMT}"]
    if limit:
        args += ["-n", str(limit)]
    return parse_git_log(_git(path, *args))


def _dev_canonical(c: Commit) -> str:
    return f"dev:{(c.author_email or c.author_name).strip().lower()}"


async def ingest_repo(
    actions: Actions, path: str = ".", *, limit: int | None = None,
    source_id: str = _SOURCE, case_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    """Ingest a repository's history into the entity graph. Idempotent (find-or-create
    on the commit sha / dev email), so re-running just adds new commits."""
    name = Path(_git(path, "rev-parse", "--show-toplevel").strip()).name
    commits = read_commits(path, limit=limit)
    latest = (
        datetime.fromisoformat(commits[-1].date) if commits else datetime.now(UTC)
    )

    repo = await actions.create_or_find_object(
        "SoftwareProject", f"repo:{name}", source_id, case_id
    )
    await actions.assert_property(repo, "name", name, source_id, latest, _CONF,
                                  case_id=case_id, evidence_class=_EC)

    devs: set[str] = set()
    for c in commits:
        observed = datetime.fromisoformat(c.date)
        short = c.sha[:12]

        dev = await actions.create_or_find_object("Person", _dev_canonical(c), source_id, case_id)
        devs.add(_dev_canonical(c))
        await actions.assert_property(dev, "name", c.author_name, source_id, observed, _CONF,
                                      case_id=case_id, evidence_class=_EC)
        if c.author_email:
            await actions.assert_property(dev, "email", c.author_email, source_id, observed,
                                          _CONF, case_id=case_id, evidence_class=_EC)

        cm = await actions.create_or_find_object("Commit", f"commit:{short}", source_id, case_id)
        await actions.assert_property(cm, "subject", c.subject, source_id, observed, _CONF,
                                      case_id=case_id, evidence_class=_EC)
        await actions.assert_property(cm, "authored_date", c.date, source_id, observed, _CONF,
                                      case_id=case_id, evidence_class=_EC)
        if not c.parents:  # the first commit — the genesis
            await actions.assert_property(cm, "genesis", "true", source_id, observed, _CONF,
                                          case_id=case_id, evidence_class=_EC)

        await actions.create_link(cm, dev, "authored_by", source_id, observed, _CONF,
                                  case_id=case_id, evidence_class=_EC)
        await actions.create_link(cm, repo, "in_repo", source_id, observed, _CONF,
                                  case_id=case_id, evidence_class=_EC)
        for parent in c.parents:
            par = await actions.create_or_find_object(
                "Commit", f"commit:{parent[:12]}", source_id, case_id
            )
            await actions.create_link(cm, par, "follows", source_id, observed, _CONF,
                                      case_id=case_id, evidence_class=_EC)

    return {"repo": name, "commits": len(commits), "developers": len(devs)}


def main() -> None:  # pragma: no cover - CLI
    import asyncio
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else "."
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None

    async def run() -> None:
        pool = await create_pool(get_settings().database_url)
        try:
            print(await ingest_repo(Actions(pool), path, limit=limit))
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":  # pragma: no cover
    main()

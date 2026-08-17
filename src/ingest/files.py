"""File substrate — the current SHAPE of a repo, so analogous files compare across the family.

`gitlog` ingests the COMMITS (the history); this ingests the TREE (what files exist now). A
`File` node is METADATA ONLY — path, extension, and a normalized `role` (license / readme / ci /
manifest / …). The content stays in git, read on demand (the diff-viewer pattern), so the graph
never bloats with file bodies. The `role` is the blocking key for the cross-repo family audit:
compare every repo's `license`, every repo's `ci`, within a role — never all-files-all-pairs.
"""
from __future__ import annotations

import hashlib
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SOURCE = "git-tree"
_EC = EvidenceClass.AUTHORITATIVE_API.value      # the tracked file list is ground truth from git
_CONF = confidence_for(EvidenceClass.AUTHORITATIVE_API)

# normalized role -> the basenames (lowercased) that play it. The role makes two files across
# repos COMPARABLE — it is what the family audit blocks on. Tight + high-signal on purpose.
_ROLE_NAMES: dict[str, frozenset[str]] = {
    "readme": frozenset({"readme", "readme.md", "readme.rst", "readme.txt"}),
    "license": frozenset({"license", "license.md", "license.txt", "licence", "copying"}),
    "makefile": frozenset({"makefile", "gnumakefile", "justfile"}),
    "gitignore": frozenset({".gitignore"}),
    "editorconfig": frozenset({".editorconfig"}),
    "dockerfile": frozenset({"dockerfile"}),
    "contributing": frozenset({"contributing.md", "contributing"}),
    "changelog": frozenset({"changelog.md", "changelog", "changes.md"}),
    "manifest": frozenset({"cargo.toml", "pyproject.toml", "setup.py", "setup.cfg",
                           "package.json", "go.mod", "gemfile", "pom.xml"}),
}


def file_role(path: str) -> str:
    """The normalized role of a file (the cross-repo blocking key), or '' if it has none.
    A role is a comparable identity: every repo's `license` is the same ROLE, so they compare."""
    p = path.replace("\\", "/").lower()
    base = p.rsplit("/", 1)[-1]
    if p.startswith(".github/workflows/") or "/.github/workflows/" in p:
        return "ci"
    if base in (".gitlab-ci.yml", ".travis.yml", "config.yml") and ".circleci" in p:
        return "ci"
    if base in (".gitlab-ci.yml", ".travis.yml"):
        return "ci"
    for role, names in _ROLE_NAMES.items():
        if base in names:
            return role
    return ""


def _ext(path: str) -> str:
    base = path.rsplit("/", 1)[-1]
    return base.rsplit(".", 1)[-1].lower() if "." in base else ""


# License classification by signature phrase — ordered most-specific first (BSD's
# "redistribution and use" is generic, so the named licenses are matched before it).
_LICENSE_SIGNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("MIT", ("permission is hereby granted, free of charge",)),
    ("Apache-2.0", ("apache license", "version 2.0")),
    ("AGPL-3.0", ("gnu affero general public license",)),
    ("GPL-3.0", ("gnu general public license", "version 3")),
    ("GPL-2.0", ("gnu general public license", "version 2")),
    ("LGPL", ("gnu lesser general public license",)),
    ("MPL-2.0", ("mozilla public license", "version 2.0")),
    ("Unlicense", ("this is free and unencumbered software released into the public domain",)),
    ("ISC", ("isc license",)),
    ("BSD-3-Clause", ("redistribution and use", "neither the name")),
    ("BSD-2-Clause", ("redistribution and use",)),
)


def classify_license(text: str) -> str:
    """The SPDX-ish license family from a LICENSE file's text — so a family using MIT in one
    repo and Apache in another is a real, flaggable inconsistency (not just 'both have one')."""
    t = text.lower()
    for name, needles in _LICENSE_SIGNS:
        if all(n in t for n in needles):
            return name
    return "unknown"


def _read(top: str, rel: str) -> str | None:
    """Read a tracked text file from the working tree (content stays in git; we only derive a
    hash + a license type at ingest, never store the body). None on a binary/unreadable file."""
    try:
        return (Path(top) / rel).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def _git(path: str, *args: str) -> str:
    return subprocess.run(["git", "-C", path, *args], capture_output=True, text=True,
                          check=True).stdout


def list_tracked_files(path: str) -> list[str]:
    """The repo's tracked files at HEAD (git ls-files — ignores build artifacts/untracked)."""
    return [ln for ln in _git(path, "ls-files").splitlines() if ln.strip()]


async def ingest_files(
    actions: Actions, path: str = ".", *, source_id: str = _SOURCE,
    case_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    """Ingest a repo's tracked-file tree as `File` nodes (metadata only) linked `in_repo` the
    SoftwareProject, each carrying its `role`/`ext`. Idempotent: objects dedup on canonical,
    and the in_repo link is deduped (the gitlog lesson — re-ingest must not inflate edges).

    `name` is a git-derived directory basename (never caller text), but still runs through
    task #107's choke point (capture.py's `_validate_repo_name`) before minting — same law
    as every other SoftwareProject mint site, belt-and-suspenders against a genuinely
    degenerate basename (an empty toplevel, a stray punctuation-only directory) rather than
    trusting derivation alone. Returns `{"error": ...}` instead of raising: this runs from
    the pulse daemon's own unattended tick (pulse.py), where an uncaught exception would
    take down monitoring for every OTHER repo in the same pass, not just this one."""
    top = _git(path, "rev-parse", "--show-toplevel").strip()
    name = Path(top).name
    from src.orchestrator.capture import _validate_repo_name
    try:
        _validate_repo_name(name, name)
    except ValueError as exc:
        return {"error": str(exc)}
    now = datetime.now(UTC)
    repo = await actions.create_or_find_object("SoftwareProject", f"repo:{name}", source_id,
                                               case_id)
    existing = {(r["from_id"], r["to_id"]) for r in await actions.pool.fetch(
        "SELECT from_id, to_id FROM links WHERE type='in_repo'")}
    roles = 0
    files = list_tracked_files(path)
    for f in files:
        fo = await actions.create_or_find_object("File", f"file:{name}/{f}", source_id, case_id)
        # `name` = the full path (the distinctive human label); role/ext are the audit keys
        await actions.assert_property(fo, "name", f, source_id, now, _CONF, case_id=case_id,
                                      evidence_class=_EC)
        role = file_role(f)
        if role:
            await actions.assert_property(fo, "role", role, source_id, now, _CONF,
                                          case_id=case_id, evidence_class=_EC)
            roles += 1
            # content facts for the drift audit — a hash (identity drift) and, for a license,
            # its classified TYPE. The body is never stored; we keep only what we audit on.
            content = _read(top, f)
            if content is not None:
                h = hashlib.sha256(content.encode("utf-8", "replace")).hexdigest()[:16]
                await actions.assert_property(fo, "content_hash", h, source_id, now, _CONF,
                                              case_id=case_id, evidence_class=_EC)
                if role == "license":
                    await actions.assert_property(fo, "license_type", classify_license(content),
                                                  source_id, now, _CONF, case_id=case_id,
                                                  evidence_class=_EC)
        ext = _ext(f)
        if ext:
            await actions.assert_property(fo, "ext", ext, source_id, now, _CONF,
                                          case_id=case_id, evidence_class=_EC)
        if (fo, repo) not in existing:
            await actions.create_link(fo, repo, "in_repo", source_id, now, _CONF,
                                      case_id=case_id, evidence_class=_EC)
            existing.add((fo, repo))
    return {"repo": name, "files": len(files), "roled": roles}


def main() -> None:  # pragma: no cover - CLI
    import asyncio
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else "."

    async def run() -> None:
        pool = await create_pool(get_settings().database_url)
        try:
            print(await ingest_files(Actions(pool), path))
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":  # pragma: no cover
    main()

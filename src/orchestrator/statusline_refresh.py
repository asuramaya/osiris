"""Keep the status bar's refresh interval where osiris put it, in every settings file osiris
owns. The harness reruns the status line command every `refreshInterval` seconds in every live
tab, so a stale value written by an older onboard keeps costing CPU after the command itself got
cheaper. `osiris deploy` calls `ensure_refresh_interval`, which sets `statusLine.refreshInterval`
to `REFRESH_SECONDS` in each file below whose statusLine runs osiris_hook.py, changing nothing
else in the file (same indentation, same file mode, every other key untouched), idempotently.

A file is OWNED only when its `statusLine.command` runs `osiris_hook.py statusline`: another
tool's status line is never touched. A file that is not valid JSON is reported and left alone."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.orchestrator.mcp_approval import _home

REFRESH_SECONDS = 30


def candidate_paths(repo_root: Path | None = None) -> list[Path]:
    """The settings files osiris may own: the user's, the deployed repo's, and every seat
    office's. Worktrees carry the repo's committed copy and follow it through git."""
    from src.orchestrator.offices import _default_office_root

    paths = [_home() / ".claude" / "settings.json"]
    if repo_root is not None:
        paths.append(Path(repo_root) / ".claude" / "settings.json")
    office_root = _default_office_root()
    if office_root.is_dir():
        paths.extend(sorted(office_root.glob("*/.claude/settings.json")))
    seen: set[Path] = set()
    unique: list[Path] = []
    for path in paths:
        if path not in seen:
            seen.add(path)
            unique.append(path)
    return unique


def _indent_of(text: str) -> int:
    for line in text.splitlines():
        stripped = line.lstrip(" ")
        if stripped.startswith('"') and len(line) - len(stripped) > 0:
            return len(line) - len(stripped)
    return 2


def _owned(doc: dict[str, Any]) -> bool:
    entry = doc.get("statusLine")
    command = str(entry.get("command", "")) if isinstance(entry, dict) else ""
    return "osiris_hook.py" in command and "statusline" in command


def ensure_refresh_interval(
    paths: list[Path], *, interval: int = REFRESH_SECONDS, dry_run: bool = False,
) -> dict[str, Any]:
    """`{"changed": [...], "current": [...], "skipped": [{"path", "why"}]}`; never raises for
    a bad file. With `dry_run` it only reports what it would change."""
    changed: list[str] = []
    current: list[str] = []
    skipped: list[dict[str, str]] = []
    for path in paths:
        if not path.is_file():
            continue
        try:
            text = path.read_text()
            doc = json.loads(text) if text.strip() else {}
        except (OSError, ValueError) as exc:
            skipped.append({"path": str(path), "why": f"not readable JSON ({exc})"})
            continue
        if not isinstance(doc, dict) or not _owned(doc):
            continue  # no osiris status line here: not ours to touch
        if doc["statusLine"].get("refreshInterval") == interval:
            current.append(str(path))
            continue
        if not dry_run:
            doc["statusLine"]["refreshInterval"] = interval
            try:
                tmp = path.with_name(path.name + ".tmp")
                tmp.write_text(json.dumps(doc, indent=_indent_of(text), ensure_ascii=False)
                               + ("\n" if text.endswith("\n") else ""))
                tmp.chmod(path.stat().st_mode & 0o777)
                tmp.replace(path)
            except OSError as exc:
                skipped.append({"path": str(path), "why": f"could not write ({exc})"})
                continue
        changed.append(str(path))
    return {"changed": changed, "current": current, "skipped": skipped,
            "interval": interval, "dry_run": dry_run}


def refresh_line(repo_root: Path | None = None) -> str:
    """The one line `osiris deploy` prints."""
    result = ensure_refresh_interval(candidate_paths(repo_root))
    line = (f"statusline refresh: {len(result['changed'])} file(s) set to "
            f"{result['interval']}s, {len(result['current'])} already current")
    if result["skipped"]:
        line += "; skipped " + ", ".join(f"{s['path']} ({s['why']})" for s in result["skipped"])
    return line

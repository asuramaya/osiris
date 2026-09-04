"""Is ~/.claude/commands actually installed from this repo's own commands/*.md, and is it
CURRENT? — task #204's structural fix (Thoth ruling msg 6918, decision 012b36fb, superseding
a34a9850): commands/*.md is now the tracked SOURCE OF TRUTH; scripts/install_commands.sh
copies it to the machine. This is that install's READ-ONLY verification twin, wired into
`osiris deploy`'s report the same way gate_hook.hook_status/push_guard.hook_status already
are — a MISSING or STALE machine copy should be exactly as visible as a missing gate hook,
never something that quietly rots. NEVER raises; any read failure is its own honest status
string, same fail-open discipline as its two siblings.
"""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def commands_status(repo_root: Path = REPO_ROOT) -> str:
    source_dir = repo_root / "commands"
    env_dir = os.environ.get("CLAUDE_COMMANDS_DIR")
    target_dir = Path(env_dir) if env_dir else Path.home() / ".claude" / "commands"
    if not source_dir.is_dir():
        return "slash commands: SOURCE MISSING (commands/ not found in this tree)"
    try:
        sources = sorted(source_dir.glob("*.md"))
    except OSError as exc:
        return f"slash commands: could not list {source_dir} ({exc}) — UNKNOWN"
    if not sources:
        return "slash commands: source directory has no *.md files — nothing to install"
    if not target_dir.is_dir():
        return (f"slash commands: NOT INSTALLED — {target_dir} does not exist, run "
                "scripts/install_commands.sh")
    missing: list[str] = []
    stale: list[str] = []
    for src in sources:
        target = target_dir / src.name
        if not target.is_file():
            missing.append(src.name)
            continue
        try:
            if src.read_bytes() != target.read_bytes():
                stale.append(src.name)
        except OSError as exc:
            return (f"slash commands: could not compare {src.name} "
                    f"({exc}) — UNKNOWN")
    if not missing and not stale:
        return f"slash commands: {len(sources)} installed and current — {target_dir}"
    parts = []
    if missing:
        parts.append(f"missing: {', '.join(missing)}")
    if stale:
        parts.append(f"stale: {', '.join(stale)}")
    return ("slash commands: OUT OF SYNC (" + "; ".join(parts) + ") — re-run "
            "scripts/install_commands.sh")

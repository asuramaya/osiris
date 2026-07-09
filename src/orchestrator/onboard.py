"""onboard — one command to wire a repo (or a whole box) into the Osiris fleet.

Onboarding a repo used to be a hand-ritual: write its `.mcp.json` pointing at the shared MCP
server, paste a boot-sector stanza into its CLAUDE.md, remember the mount ritual. This turns the
LOCAL half into one command — `python -m src.orchestrator.onboard <repo> [--statusline]` — that:

  (a) creates-or-PATCHES `<repo>/.mcp.json`, merging the `osiris` server entry into any existing
      `mcpServers` without clobbering other servers (idempotent; refuses invalid JSON);
  (b) with --statusline, creates-or-patches `<repo>/.claude/settings.json` with the shared
      statusline command (absolute paths at the osiris install), same merge/idempotence rules;
  (c) prints a checklist — the boot-sector stanza to paste into the repo's CLAUDE.md and the note
      to run the `bootstrap` MCP tool if the repo still carries markdown memory to migrate.

LOCAL-ONLY by design — NO DB, NO graph writes. The graph half of onboarding (registering the
project, migrating its md memory) happens later, from inside the new agent's first mounted
session via the `bootstrap` MCP tool. This is Osiris keeping its hands to config files it is
explicitly asked to write, and nothing else.

The strongest onboarding is NO per-repo file at all: `--user-scope` PRINTS the `claude mcp add
--scope user …` one-liner (it never edits ~/.claude.json — the `claude` CLI owns that living
config) so every project on the box mounts osiris by default. The default checklist LEADS with
that option; the per-repo `.mcp.json` remains for repo-pinned setups and overrides.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# The shared, always-on MCP server every fleet agent connects to (deploy/osiris-mcp.service;
# host/port are src/config/settings.py osiris_mcp_{host,port}). This mirrors THIS repo's own
# .mcp.json — the exact shape a newly-onboarded repo needs.
OSIRIS_MCP_URL = "http://127.0.0.1:8790/mcp"
_JOB_HEADER = "X-Osiris-Job"
_JOB_VALUE = "${CLAUDE_JOB_DIR}"  # literal — expanded per-session by Claude Code, not by us

# The box-wide default: one `claude mcp add` registers osiris for EVERY project. The single
# quotes keep ${CLAUDE_JOB_DIR} literal so each session expands it, not the shell running add.
_USER_SCOPE_CMD = (
    f"claude mcp add --scope user --transport http osiris {OSIRIS_MCP_URL} "
    "--header 'X-Osiris-Job: ${CLAUDE_JOB_DIR}'"
)


class InvalidConfigError(Exception):
    """A target config file exists but can't be safely merged (invalid JSON / wrong shape) —
    onboarding REFUSES rather than clobber a file the user (or another tool) is authoring."""


@dataclass(frozen=True)
class Change:
    """The outcome of applying one config file, for the printed manifest + tests."""

    status: str  # created | patched | unchanged | would-create | would-patch | skipped
    path: Path

    @property
    def wrote(self) -> bool:
        return self.status in ("created", "patched")


def _osiris_home() -> Path:
    """This osiris checkout's root (src/orchestrator/onboard.py → repo root). The statusline
    command points at THIS install's venv + script — the shared script lives here, not in the
    onboarded repo (which has no osiris deps of its own)."""
    return Path(__file__).resolve().parents[2]


def _server_entry() -> dict[str, Any]:
    """The `osiris` mcpServers entry — identical in shape to this repo's .mcp.json."""
    return {"type": "http", "url": OSIRIS_MCP_URL, "headers": {_JOB_HEADER: _JOB_VALUE}}


def merge_mcp(existing: dict[str, Any] | None) -> tuple[dict[str, Any], bool]:
    """Merge the osiris server into an mcpServers map WITHOUT clobbering other servers.

    Returns (result, changed). Idempotent: an already-correct osiris entry → changed=False and
    every other server is preserved untouched. Refuses a malformed `mcpServers`."""
    doc: dict[str, Any] = dict(existing) if existing else {}
    raw = doc.get("mcpServers")
    if raw is not None and not isinstance(raw, dict):
        raise InvalidConfigError("mcpServers is present but is not a JSON object")
    servers: dict[str, Any] = dict(raw or {})
    entry = _server_entry()
    changed = servers.get("osiris") != entry
    servers["osiris"] = entry
    doc["mcpServers"] = servers
    return doc, changed


def _statusline_command(osiris_home: Path) -> str:
    py = osiris_home / ".venv" / "bin" / "python"
    script = osiris_home / "scripts" / "osiris_statusline.py"
    return f"{py} {script}"


def _merge_hook(
    doc: dict[str, Any], event: str, cmd: dict[str, Any], *, matcher: str | None = None
) -> bool:
    """Idempotently add ONE command hook under `event` (optionally in a `matcher` group);
    foreign hooks pass untouched. Returns True when the doc changed."""
    hooks = dict(doc.get("hooks") or {})
    groups: list[Any] = list(hooks.get(event) or [])
    present = any(
        h.get("command") == cmd["command"]
        for grp in groups if isinstance(grp, dict)
        for h in (grp.get("hooks") or []) if isinstance(h, dict)
    )
    if present:
        return False
    group: dict[str, Any] = {"hooks": [cmd]}
    if matcher is not None:  # PreToolUse targets a specific tool (mcp__osiris__mount)
        group["matcher"] = matcher
    groups.append(group)
    hooks[event] = groups
    doc["hooks"] = hooks
    return True


def merge_settings(
    existing: dict[str, Any] | None, osiris_home: Path, *, hook: bool = False,
    whisper: bool = False, anchor: bool = False,
) -> tuple[dict[str, Any], bool]:
    """Merge the statusLine command and the requested hooks into a settings.json WITHOUT
    dropping other keys (permissions, env, worktree, …): hook=Stop mail-drain, whisper=
    SessionStart auto-mount, anchor=PreToolUse durable-anchor force. Returns (result, changed)."""
    doc: dict[str, Any] = dict(existing) if existing else {}
    entry = {"type": "command", "command": _statusline_command(osiris_home), "padding": 0}
    changed = doc.get("statusLine") != entry
    doc["statusLine"] = entry
    py = osiris_home / ".venv" / "bin" / "python"
    if hook:
        # the Stop mail-drain (operator-installed): the visible tab settles its own mailbox
        # at turn-ends instead of a twin doing it invisibly.
        script = osiris_home / "scripts" / "osiris_stophook.py"
        changed |= _merge_hook(
            doc, "Stop", {"type": "command", "command": f"{py} {script}", "timeout": 10})
    if whisper:
        # the SessionStart whisper (operator's blessing 2026-07-08): every session wakes up
        # already mounted and remembering Osiris — the hive-mind onboarding. Deliberately
        # SYSTEM python: the whisper is stdlib-only and runs for projects with no venv.
        wscript = osiris_home / "scripts" / "osiris_whisper.py"
        changed |= _merge_hook(
            doc, "SessionStart",
            {"type": "command", "command": f"python3 {wscript}", "timeout": 10})
    if anchor:
        # the PreToolUse durable-anchor (blessing 2026-07-08): force the derived job_dir into
        # every mount() so identity survives a reconnect and co-located agents stay distinct,
        # with no reliance on the agent passing it. STDLIB-only, fail-open. Matches the mount
        # tool exactly. Runs before permission/classifier gates (never rejected).
        ascript = osiris_home / "scripts" / "osiris_mount_anchor.py"
        changed |= _merge_hook(
            doc, "PreToolUse",
            {"type": "command", "command": f"python3 {ascript}", "timeout": 5},
            matcher="mcp__osiris__mount")
    return doc, changed


def _load_json_object(path: Path) -> dict[str, Any] | None:
    """Read a JSON object from `path`, or None if it's absent/empty. Raises InvalidConfigError
    (never overwrites) if the file is present but not a JSON object."""
    if not path.exists():
        return None
    text = path.read_text()
    if not text.strip():
        return None  # an empty file is treated as absent — safe to create fresh
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise InvalidConfigError(
            f"{path} exists but is not valid JSON ({e}); refusing to overwrite — "
            "fix or remove it, then re-run"
        ) from e
    if not isinstance(data, dict):
        raise InvalidConfigError(f"{path} is valid JSON but not an object; refusing to overwrite")
    return data


def _apply(
    path: Path,
    merger: Callable[[dict[str, Any] | None], tuple[dict[str, Any], bool]],
    *,
    dry_run: bool,
) -> Change:
    """Read → merge → write one config file. Writes only when the merge changes something, so a
    second run is a no-op. `dry_run` computes the outcome but writes nothing."""
    existing = _load_json_object(path)
    merged, changed = merger(existing)
    verb = "create" if existing is None else "patch"
    if not changed:
        return Change("unchanged", path)
    if dry_run:
        return Change(f"would-{verb}", path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(merged, indent=2) + "\n")
    return Change("patched" if verb == "patch" else "created", path)


# --- the printed checklist ------------------------------------------------------------------

_BOOT_STANZA = """## Osiris — mount FIRST (before anything)
This repo is wired into Osiris, the fleet's shared memory graph. On your first action:
`mount(cwd=<this repo>, job_dir=$CLAUDE_JOB_DIR)` -> `orient()` -> `inbox()`. Then write back
AS YOU GO -- `record_decision` when a ruling lands, `open_thread`/`resolve_thread` (kind=
'obligation' for a duty an action mints) as work opens and closes; the graph, not the context
window, is the memory. Model check: if your environment names a model other than the one this
project intends, SAY SO in your first reply -- a rug-pull is confessed, never inherited blind."""

_BOOTSTRAP_NOTE = (
    "If this repo still carries markdown memory (a CLAUDE.md build log, DESIGN.md, docs/ or\n"
    "memory/ essays), run the `bootstrap` MCP tool from your first mounted session -- it indexes\n"
    "them into the graph as queryable Reference nodes and never edits your files."
)


def _checklist(root: Path, *, user_scope: bool) -> str:
    if user_scope:
        head = (
            "USER SCOPE -- run this ONE command to register osiris for EVERY project on this box\n"
            "(it edits ~/.claude.json via the claude CLI, which owns that config; we never do):\n\n"
            f"    {_USER_SCOPE_CMD}\n\n"
            "Every repo you open then mounts osiris by default -- no per-repo file needed."
        )
    else:
        head = (
            "RECOMMENDED -- user scope: register osiris for EVERY project on this box at once,\n"
            "so no per-repo file is needed (the .mcp.json written above stays as a repo-pinned\n"
            "override). Run:\n\n"
            f"    {_USER_SCOPE_CMD}"
        )
    return (
        f"{head}\n\n"
        f"--- Paste into {root.name}/CLAUDE.md (the boot sector): ---\n\n"
        f"{_BOOT_STANZA}\n\n"
        f"--- Then, from the first mounted session: ---\n\n"
        f"{_BOOTSTRAP_NOTE}"
    )


def onboard(
    repo: str | Path,
    *,
    statusline: bool = False,
    hook: bool = False,
    whisper: bool = False,
    anchor: bool = False,
    dry_run: bool = False,
    user_scope: bool = False,
    osiris_home: str | Path | None = None,
) -> dict[str, Any]:
    """Wire `repo` into the fleet, locally. Returns a structured result (the applied Changes +
    the checklist text). Writes files unless `dry_run`; with `user_scope` it prints the box-wide
    one-liner instead of writing `.mcp.json`. Raises InvalidConfigError on an unmergeable file."""
    root = Path(repo).expanduser().resolve()
    if not root.is_dir():
        raise InvalidConfigError(f"{root} is not a directory")
    home = Path(osiris_home).expanduser().resolve() if osiris_home else _osiris_home()

    changes: list[Change] = []
    if user_scope:
        changes.append(Change("skipped", root / ".mcp.json"))  # print the one-liner instead
    else:
        changes.append(_apply(root / ".mcp.json", merge_mcp, dry_run=dry_run))
    if statusline or hook or whisper or anchor:
        changes.append(
            _apply(
                root / ".claude" / "settings.json",
                lambda e: merge_settings(e, home, hook=hook, whisper=whisper, anchor=anchor),
                dry_run=dry_run,
            )
        )

    return {
        "root": root,
        "changes": changes,
        "checklist": _checklist(root, user_scope=user_scope),
        "dry_run": dry_run,
        "user_scope": user_scope,
    }


def _render(result: dict[str, Any]) -> str:
    lines = [f"onboard {result['root'].name}  ({result['root']})"]
    if result["dry_run"]:
        lines.append("  [dry-run — no files written]")
    for ch in result["changes"]:
        lines.append(f"  {ch.status:>13}  {ch.path}")
    lines.append("")
    lines.append(result["checklist"])
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:  # pragma: no cover - CLI glue
    parser = argparse.ArgumentParser(
        prog="python -m src.orchestrator.onboard",
        description="Wire a repo into the Osiris fleet (local config only — no DB, no graph).",
    )
    parser.add_argument("repo", help="path to the repo to onboard")
    parser.add_argument("--name", help="project name (defaults to the repo dir name)")
    parser.add_argument(
        "--statusline", action="store_true", help="also write .claude/settings.json statusline"
    )
    parser.add_argument(
        "--hook",
        action="store_true",
        help="also install the Stop mail-drain hook (the visible tab settles its own mailbox "
             "at turn-ends) — an OPERATOR consent switch: run this yourself, per repo",
    )
    parser.add_argument(
        "--whisper",
        action="store_true",
        help="also install the SessionStart whisper (every session wakes up already mounted "
             "and told about Osiris) — an OPERATOR consent switch (blessing 2026-07-08)",
    )
    parser.add_argument(
        "--anchor",
        action="store_true",
        help="also install the PreToolUse durable-anchor hook (forces the derived job_dir into "
             "every mount so identity survives reconnect + co-located agents stay distinct) — "
             "an OPERATOR consent switch (blessing 2026-07-08)",
    )
    parser.add_argument(
        "--user-scope",
        action="store_true",
        help="print the box-wide `claude mcp add --scope user` one-liner; write no .mcp.json",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="show what would change without writing"
    )
    args = parser.parse_args(argv)
    # --name is accepted for parity with the bootstrap tool (the graph half names the project);
    # locally it changes nothing written, so it is not threaded further.
    try:
        result = onboard(
            args.repo,
            statusline=args.statusline,
            hook=args.hook,
            whisper=args.whisper,
            anchor=args.anchor,
            dry_run=args.dry_run,
            user_scope=args.user_scope,
        )
    except InvalidConfigError as e:
        print(f"onboard: {e}", file=sys.stderr)
        return 2
    print(_render(result))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

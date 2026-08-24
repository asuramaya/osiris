"""Claude Code transcript adapter — JSONL on disk under ~/.claude/projects/.

This is the EXISTING read path (sessions.py's _model_of / locate_current_transcript /
model_of_transcript), extracted behind the adapter protocol. The code moves, the reads
don't change — the same JSONL parsing that has been authoritative since the source-model
provenance was introduced (ruling 17516660). Slice 2 (the JSONL-fallback removal, task
#29) landed: every identity caller reads through the store now, and this adapter is the
ONE place Claude Code transcripts are parsed for identity. sessions.py's functions remain
for the adapter itself and the non-identity readers (the seam-hand probe, rebind).
"""
from __future__ import annotations

import json
import re
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

from src.ingest.harness import SessionLocator, TurnRow
from src.ingest.sessions import (
    locate_current_transcript,
    operator_swapped,
)

# Re-exported from sessions.py — the per-line model extractor.
_SYNTHETIC = "<synthetic>"

# THE OVERHEAD EYE (neo's, task #34): a system-reminder is a complete tagged block the
# harness injected into a user message. Counted per turn at ingest so the store can answer
# "what does the harness itself cost you" without re-reading a byte.
_REMINDER_RE = re.compile(r"<system-reminder>.*?</system-reminder>", re.IGNORECASE | re.DOTALL)


def _text_chunks(content: Any) -> Iterator[str]:
    """Every string in a message content tree. The modern harness nests reminder text
    inside tool_result items' own content lists, so a top-level-only walk (the ancestor's)
    undercounts — recurse."""
    if isinstance(content, str):
        yield content
    elif isinstance(content, list):
        for item in content:
            if isinstance(item, dict):
                for key in ("text", "content"):
                    yield from _text_chunks(item.get(key))


def _reminders_of_line(d: dict[str, Any]) -> int:
    total = 0
    for chunk in _text_chunks((d.get("message") or {}).get("content")):
        total += len(_REMINDER_RE.findall(chunk))
    return total


def _model_of_line(d: dict[str, Any]) -> str | None:
    if d.get("type") != "assistant":
        return None
    m = (d.get("message") or {}).get("model")
    return m if isinstance(m, str) and m and m != _SYNTHETIC else None


def _usage_of_line(d: dict[str, Any]) -> dict[str, Any]:
    u = (d.get("message") or {}).get("usage")
    if not isinstance(u, dict):
        return {}
    return {
        "tokens_in": int(u.get("input_tokens") or 0) or None,
        "tokens_out": int(u.get("output_tokens") or 0) or None,
        "cache_read": int(u.get("cache_read_input_tokens") or 0) or None,
        "cache_write": int(u.get("cache_creation_input_tokens") or 0) or None,
    }


def _ts(line: str) -> datetime | None:
    try:
        d: dict[str, Any] = json.loads(line)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    ts = d.get("timestamp") if isinstance(d, dict) else None
    if not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def decode_claude_project_name(slug: str) -> str:
    """Decode a Claude Code project slug (~/.claude/projects/<slug>/) into canonical project name.

    Prevents naive rsplit('-', 1) from butchering hyphenated project names like:
    - '-home-asuramaya-code-rotten-apple' -> 'rotten-apple' (NOT 'apple')
    - '-home-asuramaya-code-like-us' -> 'like-us' (NOT 'us')
    - '-home-asuramaya-code-dealer-to-fb' -> 'dealer-to-fb' (NOT 'fb')
    - '-home-asuramaya-code-osiris--claude-worktrees-imhotep' -> 'osiris'
    """
    if not slug:
        return ""
    if "--claude-worktrees-" in slug:
        slug = slug.split("--claude-worktrees-")[0]
    m = re.match(r"^-(?:home|root)(?:-[^-\s]+)?-(?:code|REPOS|src|projects)-(.*)$", slug)
    if m:
        return m.group(1).lower()
    m_seat = re.match(r"^-(?:home|root)(?:-[^-\s]+)?-*\.?osiris-seats-(.*)$", slug)
    if m_seat:
        seat_handle = m_seat.group(1)
        pin_path = Path.home() / ".osiris" / "seats" / seat_handle / ".osiris"
        if pin_path.is_file():
            try:
                for line in pin_path.read_text("utf-8").splitlines():
                    if line.strip().startswith("project"):
                        val = line.split("=", 1)[1].strip().strip("\"'")
                        if val:
                            return val.lower()
            except Exception:
                pass
        return seat_handle.lower()
    clean = slug.lstrip("-")
    for prefix in ["home-asuramaya-Downloads-", "home-asuramaya-code-", "home-asuramaya-", "home-"]:
        if clean.startswith(prefix):
            return clean[len(prefix):].lower()
    return clean.lower()


class ClaudeJsonlAdapter:
    """Claude Code's ~/.claude/projects/*/<sid>.jsonl transcript."""

    name = "claude-code"

    def discover(
        self, *, cwd: str | None, job_dir: str | None, root: Path | None = None,
    ) -> SessionLocator | None:
        # anchored_only, MAIN transcript only — two scars, keep both: (1) a job_dir that
        # matches no transcript must yield NOTHING, never the box-wide-hottest neighbor
        # (the cry-wolf false swap); (2) never anchor on a hotter subagents/ transcript —
        # a BACKGROUND child runs while the parent keeps calling, and the parent's writes
        # got attributed to its hot child (provenance theft, thread 0344e536 / be580da).
        base = root or (Path.home() / ".claude" / "projects")
        path = locate_current_transcript(base, job_dir, anchored_only=True)
        if path is None:
            return None
        stem = path.stem  # "<sid8>-<rest>" or full UUID
        anchor = stem.split("-")[0]
        if len(anchor) < 8:
            return None
        proj = None
        if cwd:
            # the parent dir name is the cwd with slashes dashed; the basename is the project
            dashed = str(cwd).rstrip("/").replace("/", "-")
            if path.parent.name == dashed:
                proj = Path(cwd).name
        return SessionLocator(
            anchor_sid=anchor, session_id=stem, harness=self.name,
            source_path=str(path), cwd=cwd, project=proj,
        )

    def discover_at(self, path: Path) -> SessionLocator | None:
        """Build a locator DIRECTLY from a caller-KNOWN path — no job_dir/cwd search at
        all (thread 7304bfd8). The fix for a background-job fork whose job_dir-based
        search (discover(), above) can land on a stub/wrong file or find nothing: the
        caller (mount()'s/automount()'s own `transcript_path` param, hook-stamped) already
        has the real one. Same stem-parse convention as discover(); cwd/project are left
        unset (unknown from a bare path alone) — identity only needs the model reading,
        never these for the explicit-path lane."""
        if not path.is_file():
            return None
        stem = path.stem
        anchor = stem.split("-")[0]
        if len(anchor) < 8:
            return None
        return SessionLocator(
            anchor_sid=anchor, session_id=stem, harness=self.name,
            source_path=str(path), cwd=None, project=None,
        )

    def enumerate(self, *, root: Path | None = None) -> Iterator[SessionLocator]:
        """Every Claude Code transcript on disk — the miner's backfill sweep.

        Walks ~/.claude/projects/*/*.jsonl. Each parent dir is a project (the cwd with
        slashes dashed); each file is a session. Skips the osiris-extract sidechains
        (those are the miner's OWN extractions, not real sessions — ingesting them would
        double-count)."""
        base = (root or (Path.home() / ".claude" / "projects")).expanduser()
        if not base.is_dir():
            return
        for proj_dir in base.iterdir():
            if not proj_dir.is_dir():
                continue
            # Decode dir name into canonical project name without truncating hyphenated names
            project = decode_claude_project_name(proj_dir.name)
            for path in proj_dir.glob("*.jsonl"):
                # skip the miner's own extraction sidechains (wake_cost.py's rule)
                if path.parent.name.endswith("-osiris-extract"):
                    continue
                stem = path.stem
                anchor = stem.split("-")[0]
                if len(anchor) < 8:
                    continue
                yield SessionLocator(
                    anchor_sid=anchor, session_id=stem, harness=self.name,
                    source_path=str(path), cwd=None, project=project,
                )
                yield from self._channels_of(path, anchor, project)

    def _channels_of(
        self, primary: Path, parent_sid: str, project: str | None,
    ) -> Iterator[SessionLocator]:
        """The hidden channels beside a primary — <stem>/subagents/agent-*.jsonl, plus
        <stem>/subagents/workflows/wf_*/agent-*.jsonl (the Workflow tool's fan-outs, a
        channel shape the ancestor never knew).

        Each subagent writes its own transcript there (first line carries isSidechain;
        the .meta.json sidecar names the agentType). These are the sessions the operator
        never sees on screen — the overhead lens exists to price them. anchor_sid is the
        subagent's own hex id (unique fleet-wide); parent_sid ties it back to the primary
        it served."""
        sa_dir = primary.parent / primary.stem / "subagents"
        if not sa_dir.is_dir():
            return
        yield from self._channel_files(sa_dir, parent_sid, project, kind=None)
        wf_root = sa_dir / "workflows"
        if wf_root.is_dir():
            for wf_dir in sorted(wf_root.iterdir()):
                if wf_dir.is_dir() and not wf_dir.is_symlink():
                    yield from self._channel_files(
                        wf_dir, parent_sid, project, kind="workflow")

    def _channel_files(
        self, directory: Path, parent_sid: str, project: str | None, *, kind: str | None,
    ) -> Iterator[SessionLocator]:
        """One directory of channel transcripts. kind pins the channel name (workflow
        fan-outs); None classifies per file (compaction by name, sidechain otherwise)."""
        for path in sorted(directory.glob("*.jsonl")):
            if path.is_symlink():
                continue
            stem = path.stem
            anchor = stem.removeprefix("agent-") or stem
            channel = kind or ("compaction" if "compact" in stem else "sidechain")
            agent_type = None
            try:
                meta = json.loads(path.with_suffix(".meta.json").read_text("utf-8"))
                if isinstance(meta, dict) and isinstance(meta.get("agentType"), str):
                    agent_type = meta["agentType"]
            except (OSError, json.JSONDecodeError, UnicodeDecodeError):
                pass
            yield SessionLocator(
                anchor_sid=anchor, session_id=stem, harness=self.name,
                source_path=str(path), cwd=None, project=project,
                channel=channel, parent_sid=parent_sid, agent_type=agent_type,
            )

    def read_turns(
        self, locator: SessionLocator, *, since_idx: int = 0,
    ) -> Iterator[TurnRow]:
        path = Path(locator.source_path)
        try:
            text = path.read_text("utf-8", errors="replace")
        except OSError:
            return
        deliberate = operator_swapped(text.splitlines())
        idx = 0
        for ln in text.splitlines():
            try:
                d = json.loads(ln)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if not isinstance(d, dict):
                continue
            if idx < since_idx:
                idx += 1
                continue
            role = str(d.get("type") or "")
            if role not in ("assistant", "user"):
                idx += 1
                continue
            model = _model_of_line(d)
            usage = _usage_of_line(d) if role == "assistant" else {}
            summary = bool(d.get("isCompactSummary") or d.get("isMeta"))
            # reminders only on live user turns: a compact summary QUOTES the past, and
            # counting its quoted reminders again after every compaction would inflate
            # the very churn number the lens exists to measure honestly
            reminders = _reminders_of_line(d) if role == "user" and not summary else None
            yield TurnRow(
                turn_idx=idx, role=role, model=model,
                tokens_in=usage.get("tokens_in"),
                tokens_out=usage.get("tokens_out"),
                cache_read=usage.get("cache_read"),
                cache_write=usage.get("cache_write"),
                recorded_at=_ts(ln),
                is_summary=summary,
                swap_deliberate=deliberate if role == "assistant" else None,
                source_ref=f"line:{idx}",
                reminders=reminders,
                is_compaction=bool(d.get("isCompactSummary")),
            )
            idx += 1

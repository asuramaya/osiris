"""Claude Code transcript adapter — JSONL on disk under ~/.claude/projects/.

This is the EXISTING read path (sessions.py's _model_of / locate_current_transcript /
model_of_transcript), extracted behind the adapter protocol. The code moves, the reads
don't change — the same JSONL parsing that has been authoritative since the source-model
provenance was introduced (ruling 17516660). Slice 2 migrates the callers onto the store;
until then the old functions in sessions.py still serve the direct path and this adapter
serves the store.
"""
from __future__ import annotations

import json
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


class ClaudeJsonlAdapter:
    """Claude Code's ~/.claude/projects/*/<sid>.jsonl transcript."""

    name = "claude-code"

    def discover(
        self, *, cwd: str | None, job_dir: str | None, root: Path | None = None,
    ) -> SessionLocator | None:
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
            # the cwdencoded dir name un-dashes back to a path; the basename is the project
            cwd_dashed = proj_dir.name
            project = cwd_dashed.rsplit("-", 1)[-1] if "-" in cwd_dashed else cwd_dashed
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
            yield TurnRow(
                turn_idx=idx, role=role, model=model,
                tokens_in=usage.get("tokens_in"),
                tokens_out=usage.get("tokens_out"),
                cache_read=usage.get("cache_read"),
                cache_write=usage.get("cache_write"),
                recorded_at=_ts(ln),
                is_summary=bool(d.get("isCompactSummary") or d.get("isMeta")),
                swap_deliberate=deliberate if role == "assistant" else None,
                source_ref=f"line:{idx}",
            )
            idx += 1

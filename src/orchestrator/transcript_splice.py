"""Splice a seat's session, fragmented across multiple project slugs by a mid-session cwd
move, back into ONE file at its own office slug (#204: extracted from mcp_server.py's own
heal_seat_transcript tool body, unchanged, so the CLI door added alongside it wraps the SAME
function rather than a second copy of this logic)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import asyncpg


async def heal_seat_transcript(
    pool: asyncpg.Pool, handle: str, source_paths: list[str], *,
    dry_run: bool = True, because: str = "",
) -> dict[str, Any]:
    """`handle` names the seat whose office the result lands at. `source_paths` are the
    original fragments, IN CHAIN ORDER (oldest first) — the session uuid and 8-char
    anchor_sid derive from `source_paths[0]`'s own filename.

    `verify_jsonl_chain_boundary` runs on every consecutive pair before anything is
    touched, refusing a false "same session" or a genuinely separate session sharing only
    a directory.

    `dry_run=True` (default) reports clean/refused per pair and where the result would
    land — nothing written. `dry_run=False` requires `because` and performs the real
    splice + rematerialize. Never touches a Seat row, anchor_cwd, or any source transcript
    — the anchor-repoint half is heal_seat_anchor, a different door."""
    handle = (handle or "").strip()
    if not handle:
        return {"error": "a handle is required"}
    if not source_paths or len(source_paths) < 2:
        return {"error": "source_paths needs at least two fragments to splice — a single "
                         "file has nothing to join"}
    from src.ingest.soul_store import SoulStore, verify_jsonl_chain_boundary
    from src.orchestrator.offices import _default_office_root

    first_stem = Path(source_paths[0]).stem
    if len(first_stem) != 36 or first_stem.count("-") != 4:
        return {"error": f"source_paths[0]'s filename ({first_stem!r}) is not a session "
                         "uuid — cannot derive the session id to splice under"}
    full_sid = first_stem
    anchor_sid = full_sid.split("-")[0]
    dest = _default_office_root() / handle.lower() / f"{full_sid}.jsonl"

    preflight: list[dict[str, Any]] = []
    for a, b in zip(source_paths, source_paths[1:], strict=False):
        reason = verify_jsonl_chain_boundary(a, b)
        preflight.append({"a": a, "b": b, "clean": reason is None, "reason": reason})
    out: dict[str, Any] = {
        "handle": handle, "anchor_sid": anchor_sid, "dry_run": dry_run,
        "preflight": preflight, "office_dest": str(dest),
    }
    if any(not p["clean"] for p in preflight):
        out["error"] = "preflight refused — see `preflight` for which pair and why"
        return out
    if dry_run:
        return out

    because = because.strip()
    if not because:
        return {"error": "because is required to execute — an operator-gated act needs "
                         "a stated reason", **out}

    store = SoulStore(pool)
    try:
        spliced = await store.splice_sources(anchor_sid, source_paths)
    except ValueError as e:
        return {"error": str(e), **out}
    out["spliced_lines"] = spliced
    out["verify_chain"] = await store.verify_chain(anchor_sid)
    out["rematerialize"] = await store.rematerialize_to_disk(anchor_sid, dest=str(dest))
    return out

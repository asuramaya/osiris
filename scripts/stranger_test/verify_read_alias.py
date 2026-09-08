#!/usr/bin/env python3
"""Stranger-test proof A, tail end: after `osiris rename-project <old> <new> --apply`, prove
the READ ALIAS survives — both the old and the new project name still resolve, to the SAME
object id (the acceptance bar dispatch 2589353a itself states: "writes filed with repo='<name>'
land under the canonical and read back under both names"). Imports
`src.orchestrator.capture._resolve_repo(pool, name) -> uuid.UUID | None` directly (see its own
docstring, src/orchestrator/capture.py ~line 1099) rather than round-tripping through a full MCP
handshake — this is the SAME mechanism search/get_object_list/graph_search/threads/orient all
rely on (commit dbe98c7, already on main) to resolve either name to the one canonical object.

Usage: verify_read_alias.py <old_name> <new_name>
Exits 0 with "READ ALIAS OK: ..." on success; exits 1 with a named assertion failure otherwise
(wired as its own `step` in run.sh so a broken alias shows up as its own WALL, distinct from the
rename command itself succeeding or failing)."""
import asyncio
import os
import sys

import asyncpg
from src.orchestrator.capture import _resolve_repo


async def main() -> int:
    if len(sys.argv) != 3:
        print("usage: verify_read_alias.py <old_name> <new_name>", file=sys.stderr)
        return 2
    old_name, new_name = sys.argv[1], sys.argv[2]
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    try:
        old_id = await _resolve_repo(pool, old_name)
        new_id = await _resolve_repo(pool, new_name)
        assert old_id is not None, (
            f"old name {old_name!r} no longer resolves — the read alias is broken")
        assert new_id is not None, (
            f"new name {new_name!r} doesn't resolve — the rename itself failed")
        assert old_id == new_id, (
            f"old/new resolved to DIFFERENT objects ({old_id} vs {new_id}) — not an "
            "alias, a split")
        print(f"READ ALIAS OK: both {old_name!r} and {new_name!r} resolve to {old_id}")
        return 0
    finally:
        await pool.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

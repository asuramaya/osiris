"""describe(table) — the physical schema get_schema doesn't give you (thread 1aa2ff36, Wave 1).

get_schema returns the ONTOLOGY: the object/link types this app's code declares, with their
categories and canonical schemes — the vocabulary a mind authors a composition or reads a
result against. It says nothing about a table's actual Postgres columns, types, or indexes,
because it was never meant to; that is a different question, at a different layer, and the
graph never had a verb for it. The corpus's own numbers made the gap concrete: 80 hand-rolled
introspections and 45 failed queries in one report window, four of them within 83 seconds on a
single day — a mind reaching for the schema tool, finding it silent on physical shape, and
going straight to `information_schema` by hand instead.

`describe_table` answers the physical question directly, straight off Postgres's own catalogs
— never a guess, never the ontology's idea of the world standing in for the database's."""
from __future__ import annotations

from typing import Any

import asyncpg


async def describe_table(pool: asyncpg.Pool, table: str) -> dict[str, Any]:
    """A table's actual columns (name/type/nullable/default), in column order, plus its
    indexes (name/definition). `table` is matched exactly against the `public` schema — every
    table this graph owns lives there. Returns `exists: False` with empty columns/indexes,
    never a silently-empty shape indistinguishable from a real-but-empty table, when the name
    doesn't match anything real."""
    exists = await pool.fetchval(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name=$1)", table)
    if not exists:
        return {"table": table, "exists": False, "columns": [], "indexes": []}
    cols = await pool.fetch(
        "SELECT column_name, data_type, is_nullable, column_default "
        "FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=$1 ORDER BY ordinal_position", table)
    idx = await pool.fetch(
        "SELECT indexname, indexdef FROM pg_indexes "
        "WHERE schemaname='public' AND tablename=$1 ORDER BY indexname", table)
    return {
        "table": table,
        "exists": True,
        "columns": [
            {"name": c["column_name"], "type": c["data_type"],
             "nullable": c["is_nullable"] == "YES", "default": c["column_default"]}
            for c in cols
        ],
        "indexes": [{"name": i["indexname"], "definition": i["indexdef"]} for i in idx],
    }

"""describe(table) — the physical schema get_schema doesn't give you (thread 1aa2ff36)."""
from __future__ import annotations

from src.actions.core import Actions
from src.ontology.catalog import ensure_type
from src.orchestrator.describe import describe_table


async def test_describes_a_real_tables_columns_in_order(actions: Actions) -> None:
    out = await describe_table(actions.pool, "watermarks")
    assert out["table"] == "watermarks" and out["exists"] is True
    assert out["columns"] == [
        {"name": "key", "type": "text", "nullable": False, "default": None},
        {"name": "cursor", "type": "text", "nullable": False, "default": None},
        {"name": "updated_at", "type": "timestamp with time zone", "nullable": False,
         "default": "now()"},
    ]


async def test_describes_a_real_tables_indexes(actions: Actions) -> None:
    out = await describe_table(actions.pool, "watermarks")
    names = [i["name"] for i in out["indexes"]]
    assert "watermarks_pkey" in names
    pkey = next(i for i in out["indexes"] if i["name"] == "watermarks_pkey")
    assert "UNIQUE INDEX watermarks_pkey" in pkey["definition"]
    assert "(key)" in pkey["definition"]


async def test_a_table_that_does_not_exist_says_so_honestly(actions: Actions) -> None:
    """exists=False, not a silently-empty shape indistinguishable from a real, empty table —
    the same discipline Wave 0's receipts are held to."""
    out = await describe_table(actions.pool, "not_a_real_table_at_all")
    assert out == {"table": "not_a_real_table_at_all", "exists": False,
                   "columns": [], "indexes": []}


async def test_the_mcp_tool_wrapper_delegates_to_describe_table(actions: Actions) -> None:
    """The wiring check for the actual MCP verb (mirrors the srv._pool swap pattern Wave 0
    established in test_capture.py) — proves `describe` the tool, not just describe_table
    the function, resolves the pool and returns the same shape."""
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.describe("watermarks")
    finally:
        srv._pool = saved_pool
    assert out["exists"] is True
    assert {"name": "key", "type": "text", "nullable": False, "default": None} in out["columns"]


async def test_get_schema_reads_the_live_catalog_not_the_static_seed(actions: Actions) -> None:
    """Task #97 workstream 2: get_schema must read the graph-backed Type catalog, not
    schema.py's static seed manifest — a type minted through accretion (or ensure_type
    directly) shows up here the moment it exists, with no deploy/reseed in between."""
    from src import mcp_server as srv

    await ensure_type(actions, name="LiveMintedWidget", kind="object", actor="test",
                      description="minted mid-test, not in the static seed manifest",
                      category=["Software"])
    await ensure_type(actions, name="live_minted_rel", kind="link", actor="test",
                      description="a live-minted relationship", domain=["LiveMintedWidget"])

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.get_schema()
    finally:
        srv._pool = saved_pool

    assert {"object_types", "link_types", "categories"} <= out.keys()
    widget = next(t for t in out["object_types"] if t["name"] == "LiveMintedWidget")
    assert widget["description"] == "minted mid-test, not in the static seed manifest"
    assert widget["category"] == ["Software"]
    rel = next(lt for lt in out["link_types"] if lt["name"] == "live_minted_rel")
    assert rel["connects"] == "LiveMintedWidget -> *"
    assert "Software" in out["categories"]

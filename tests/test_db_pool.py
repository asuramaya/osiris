"""create_pool's application_name tagging (task #180 piece 2 (c)) — the per-daemon
pg_stat_activity grouping fleet()'s new pool_health surface depends on."""
from __future__ import annotations

from src.db.pool import create_pool


async def test_create_pool_tags_connections_with_application_name(pg_dsn: str) -> None:
    pool = await create_pool(pg_dsn, min_size=1, max_size=1, application_name="osiris-test-tag")
    try:
        name = await pool.fetchval("SELECT current_setting('application_name')")
        assert name == "osiris-test-tag"
    finally:
        await pool.close()


async def test_create_pool_without_application_name_keeps_the_default(pg_dsn: str) -> None:
    """No caller is forced to opt in — the existing untagged shape must survive unchanged."""
    pool = await create_pool(pg_dsn, min_size=1, max_size=1)
    try:
        name = await pool.fetchval("SELECT current_setting('application_name')")
        assert name != "osiris-test-tag"
    finally:
        await pool.close()

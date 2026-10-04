"""graph_layout: where the layout heartbeat's positions live (storage redesign S13)

Revision ID: 0074
Revises: 0073

The layout heartbeat used to store each object's position as three ordinary property
assertions (graph_x, graph_y, graph_layout_v), superseding the previous row on every
re-layout. That is a position, not a fact with a history: it left about 2.1M history rows
(710k each for graph_x and graph_y) that nothing reads. The position now lives in one row per
object here, updated in place; the three property names are retired from the assertions
table by a batched job (src/orchestrator/retention.py: retire_layout_history).

The upgrade copies today's positions across, so the graph is whole the moment the new code
starts. Idempotent on a live box: a retried upgrade neither fails nor overwrites newer rows.
"""
from __future__ import annotations

from alembic import op

revision = "0074"
down_revision = "0073"
branch_labels = None
depends_on = None

_SOURCE = "cron:graph_layout"


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS graph_layout (
            object_id   uuid PRIMARY KEY REFERENCES objects(id),
            x           double precision NOT NULL,
            y           double precision NOT NULL,
            layout_v    integer NOT NULL,
            updated_at  timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        f"""
        INSERT INTO graph_layout (object_id, x, y, layout_v)
        SELECT DISTINCT ON (gx.object_id) gx.object_id,
               (gx.value #>> '{{}}')::float8, (gy.value #>> '{{}}')::float8,
               COALESCE((gv.value #>> '{{}}')::int, 0)
        FROM current_assertions gx
        JOIN current_assertions gy ON gy.object_id = gx.object_id
             AND gy.name = 'graph_y' AND gy.source_id = '{_SOURCE}'
        LEFT JOIN current_assertions gv ON gv.object_id = gx.object_id
             AND gv.name = 'graph_layout_v' AND gv.source_id = '{_SOURCE}'
        WHERE gx.name = 'graph_x' AND gx.source_id = '{_SOURCE}'
        ORDER BY gx.object_id, gx.id DESC
        ON CONFLICT (object_id) DO NOTHING
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS graph_layout")

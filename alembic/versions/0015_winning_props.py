"""winning_props — the ONE cross-source winner-per-property definition (grade, then recency)

current_assertions gives the latest assertion per (object, name, SOURCE) — a multi-source set.
The cross-source WINNER (constitution #5: SELF_DECLARED > … > DERIVED, then recency) is resolved
AT READ, and the ordering was hand-repeated as `DISTINCT ON (object_id[,name]) ORDER BY
confidence DESC, observed_at DESC` across _props / orient / the composer rollups. Where a site
diverged to a recency-ONLY ordering (the bulk-props + rollup paths, and export.py's earlier
copy), a fresh DERIVED re-assertion silently buried an older SELF_DECLARED — the stuck-open-
threads bug.

This is the single definition: a set-returning function taking the object ids, so the filter
stays INSIDE it (a bare VIEW would force a full-graph DISTINCT ON then filter — O(all assertions)
per lookup). Every read site calls it, so the winner ordering can never drift again.
"""
from __future__ import annotations

from alembic import op

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE FUNCTION winning_props(oids uuid[])
        RETURNS TABLE (object_id uuid, name text, value jsonb)
        LANGUAGE sql STABLE AS $func$
            SELECT DISTINCT ON (ca.object_id, ca.name) ca.object_id, ca.name, ca.value
            FROM current_assertions ca
            WHERE ca.object_id = ANY(oids)
            ORDER BY ca.object_id, ca.name, ca.confidence DESC, ca.observed_at DESC
        $func$
        """
    )


def downgrade() -> None:
    op.execute("DROP FUNCTION winning_props(uuid[])")

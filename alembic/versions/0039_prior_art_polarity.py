"""prior_art polarity — search_log learns CONTRADICT vs RE-DERIVE, not just which kind

Revision ID: 0039
Revises: 0038
Create Date: 2026-07-28

PRACTICE v2 layer 1 (Thoth LXII's DM 1785, grounds c54e8176 + thread 54a5c842):
0038's prior_art_kind/prior_art_strong told you WHICH object type a strong hit landed on,
not whether the new decision agreed with it or reversed it — a Practice hit got the same
"re-derivation" flavor whether it restated or silently contradicted the standing Practice.
record_decision now classifies a Practice hit as 'contradict' (a lexical reversal
fingerprint found, or an explicit refutes= naming this same practice) or 'rederive'
(no such fingerprint) and logs it here. Nullable — only Practice hits ever populate it.
Same incremental-ALTER pattern as 0034/0038: one nullable column on the existing
search_log table, no new table.
"""
from __future__ import annotations

from alembic import op

revision = "0039"
down_revision = "0038"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE search_log ADD COLUMN prior_art_polarity text")


def downgrade() -> None:
    op.execute("ALTER TABLE search_log DROP COLUMN prior_art_polarity")

"""fleet_messages.grade — mail says what it wants from you.

MAIL HAS NO GRADE (thread f9449d8d): the project inbox mixes broadcast FYI with actionable
asks, and the READER pays to sort them — one seat read 7 leased messages in full to discover
that 5 wanted nothing but an ack. The operator's DESK already solved this (desk_kind bands);
the project inbox never got it. `grade` is the SENDER'S OWN triage of what the message wants:
'ask' (this needs a reply or an act from you) | 'fyi' (a notice; an ack settles it). NULL is
honest ignorance — ungraded mail renders exactly as before, never guessed into a band.

Revision ID: 0029
Revises: 0028
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0029"
down_revision = "0028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("fleet_messages", sa.Column("grade", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("fleet_messages", "grade")

"""soul_lines.codec: which format the sealed line holds (storage redesign, compress before
encrypt)

Revision ID: 0073
Revises: 0072

A soul line is Fernet-encrypted, and ciphertext does not compress, so the database, every
dump and every restic snapshot carried each transcript line at about 1.34x its raw size.
Lines are now compressed (zstd) BEFORE they are encrypted. `codec` records which form a
row holds, so the background re-encode can find the rows still in the old form without
decrypting two million of them:

    0  not yet considered: every row that exists today, and any line written while no key
       existed. This is the re-encode's work queue.
    1  the line is wrapped in the versioned `ZS1` envelope (soul_crypto.pack_line)
    2  considered and kept as written: too small to be worth a frame, or it did not shrink

The column is a work queue for the re-encode and a measurement handle; the READER never
trusts it (it detects the envelope from the decrypted bytes), so a stale value can never
make a row unreadable. Adding a column with a constant default is a catalog change in
Postgres 16, not a table rewrite. Idempotent for a retried deploy.
"""
from __future__ import annotations

from alembic import op

revision = "0073"
down_revision = "0072"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE soul_lines ADD COLUMN IF NOT EXISTS codec smallint NOT NULL DEFAULT 0")


def downgrade() -> None:
    op.execute("ALTER TABLE soul_lines DROP COLUMN IF EXISTS codec")

"""soul_lines.raw_line becomes bytea — the NUL-byte gap (thread 173cbf11, Thoth DM 5350).

Confirmed live during task #181's Ptah/Ra recovery: SoulStore.ingest_path's own
`executemany` raised `asyncpg.exceptions.CharacterNotInRepertoireError` ("invalid byte
sequence for encoding UTF8: 0x00") the instant one raw JSONL line carried a literal NUL —
Postgres `text`/`varchar` columns cannot hold 0x00 at all, a hard server limitation, not a
query bug. Both of Thoth's own two named transcripts carry thousands of them (Ptah 4780,
Ra 18591) — not a one-off.

soul_store.py's own stated promise is byte-exact verbatim retention (0050's docstring: "no
semantic filtering... re_materialize() is the acceptance test this piece stands or falls
on"). Silently stripping/escaping the NUL to keep `text` working would violate that
promise outright — the correct fix is the storage type itself: `bytea` holds ANY byte
sequence, NUL included, with no encoding round-trip to go wrong. The hash chain (soul_
store.py's `_chain_hash`) now computes directly over these raw bytes rather than a
`str.encode("utf-8")` re-derivation — one less place fidelity could silently drift.

Existing rows (if any — this table is young, task #51 shipped 2026-08-17) cast cleanly:
`raw_line::bytea` re-encodes the current `text` value through the server's own UTF8
encoding, which is exactly what `_chain_hash`'s own prior `.encode("utf-8")` step already
assumed every stored row could survive.

Revision ID: 0052
Revises: 0051
"""
from alembic import op

revision = "0052"
down_revision = "0051"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE soul_lines ALTER COLUMN raw_line TYPE bytea USING raw_line::bytea")


def downgrade() -> None:
    op.execute(
        "ALTER TABLE soul_lines ALTER COLUMN raw_line TYPE text "
        "USING convert_from(raw_line, 'UTF8')")

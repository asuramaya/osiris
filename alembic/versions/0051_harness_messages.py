"""harness_messages — recovered cross-session SendMessage exchanges (task #181, thread
5cd49217's own sibling, Thoth DM 5320). Ptah measured that during a routing defect he and
Ra sent 3 messages through Osiris and ~24 through the harness's own cross-session socket
(the SendMessage tool) — 90% of a day's reasoning existed only in two jsonl files, outside
every graph query, outside orient(), outside search(). The soul store (0050) already keeps
those files byte-exact; this table is the RECOVERY side — SendMessage tool_use blocks
lifted OUT of soul_lines into typed, attributed, pair+time-threaded rows, so that traffic
becomes queryable the same way osiris mail (fleet_messages, 0028) already is.

Deliberately its OWN table, not a graph Object type (schema.py's ontology): osiris's own
mail already lives outside that catalog (fleet_messages), and a recovered exchange is the
exact same KIND of thing — high-volume, structured, time-series traffic — never a
knowledge object with provenance-graded properties. Idempotent per (anchor_sid,
turn_index): re-running recovery over an already-recovered session inserts nothing new,
the same watermark discipline soul_lines' own line_idx PK already uses.

`from_agent`/`to_resolved` are nullable — resolution to a real Agent canonical is a BEST
EFFORT at recovery time (the raw `to` field is often a bare session-id fragment the
harness itself uses, not a osiris canonical); the raw fields are never lost even when
resolution fails.

Revision ID: 0051
Revises: 0050
"""
from alembic import op

revision = "0051"
down_revision = "0050"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE harness_messages (
            id            bigserial PRIMARY KEY,
            anchor_sid    text NOT NULL,
            turn_index    bigint NOT NULL,
            from_agent    text,
            to_raw        text NOT NULL,
            to_resolved   text,
            summary       text,
            message       text NOT NULL,
            observed_at   timestamptz,
            recovered_at  timestamptz NOT NULL DEFAULT now(),
            UNIQUE (anchor_sid, turn_index)
        )
    """)
    op.execute("CREATE INDEX harness_messages_from ON harness_messages (from_agent)")
    op.execute("CREATE INDEX harness_messages_observed ON harness_messages (observed_at)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS harness_messages")

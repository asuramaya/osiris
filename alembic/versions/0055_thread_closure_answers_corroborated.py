"""thread_closure_edges' answers arm requires SAME-SOURCE status='resolved' corroboration —
the mint_bears_on conflation fix (Thoth DM 6230/6234, decision 36cbec2f).

THE INVARIANT THIS VIEW WAS BUILT ON (0043's own docstring, now FALSE): "`answers`
(Decision -> Thread): record_decision(resolves=...) mints this in the same transaction as
the status='resolved' write — always present for that closure path." That was true the day
0043 landed, when record_decision(resolves=...) (capture.py, always pairs the `answers` link
with a same-transaction, same-source status='resolved' write) was the ONLY producer of an
`answers` edge. `mint_bears_on` (capture.py, built for thread 898840dc — itself one of the
nine specimens this migration fixes) shipped later and mints the IDENTICAL `answers` link
type for the opposite purpose, BY DESIGN: its own docstring says plainly "this function
touches only the `links` table, never `status`... that separation is deliberate, not an
oversight" (Thoth's no-auto-act ruling, DM 4701 — a citation must never silently act on the
row it cites). Nobody revisited this view when that second producer shipped, so
`thread_closure_status`/`closure_health` have been reading every bears_on citation as a
closure ever since.

MEASURED, LIVE, ON THE REAL POPULATION: nine of closure_health's ten repo=osiris `disagree`
rows tonight were bears_on citations, not stale properties and not spurious edges — proven
by querying the FULL `assertions` history (every source, not just the current-winning row)
for `status` on each: zero ever received a status='resolved' write, from any source. One of
the nine is Thoth's own hand (decision 6c4bae42, `bears_on=["67a86a55", "0ae050d8"]`, used
exactly as documented) landing on Imhotep's live measurement mid-flight — a coordinator
citing a thread in passing manufactured a false "already closed" reading on the very
instrument the operator is about to lean on for backlog triage. capture.py has exactly two
producers of an `answers` link, confirmed by grep across the whole tree, not just capture.py
(nothing else creates one): resolves= (capture.py:1543, paired with the status write) and
mint_bears_on (capture.py:3261, deliberately unpaired). No third producer exists to account
for.

THE FIX, same shape as 0045's own strength-CASE on the resolved_by arm (one more WHERE
condition, not a new arm, not new schema): an `answers` edge counts as closure evidence ONLY
when the SAME source_id that minted the link ALSO wrote status='resolved' on that thread, at
any point in its full history (not just the current-winning row — a later, unrelated source
touching `status` must not erase this corroboration). This needs NO backfill and NO new
`properties` marker on existing or future links (the alternative Thoth proposed, and
explicitly left to my judgment to overrule if it couldn't be made honest): resolves= has
ALWAYS written both facts in the same atomic transaction under the same source_id, so the
corroboration check is retroactively true for every genuine resolves-closure ever minted,
with zero write-side change and zero migration risk. A bears_on-sourced `answers` edge has
no such source-matched status write anywhere, by the verb's own design, so it fails the
EXISTS check and simply drops out of `thread_closure_edges` — it never was a closure edge
and now the view stops claiming otherwise.

WHERE THE NINE LAND NOW, not into a void: with no closure edge, `thread_closure_status`
reads `closed_by_topology=False` for them — the same honest "no closure edge found" state
every genuinely untouched thread already reads (never "confirmed open" per this module's own
law, see thread_closure.py). Concretely, since all nine carry `property_status='open'`,
`closure_health` now buckets them as `open_both` — which is not a demotion into invisibility,
it is the TRUE state: they are unambiguously, uncontradicted-ly open. This answers Thoth's
own question (DM 6234) more precisely than her proposed "surface as UNDETERMINED": there is
no ambiguity left to preserve once the false signal is gone, so `open_both` is the honest
landing, not a euphemism for "we're not sure."

0043'S OWN DOCSTRING IS NOW A STALE CLAIM DESCRIBING A WORLD THAT NO LONGER EXISTS (Thoth's
own naming, DM 6234, citing #48's two hidden doors as precedent for why this matters) — its
SQL is untouched here (migrations are a historical record, not a place to rewrite the past),
but a pointer to this migration has been added directly above its now-false sentence so a
future reader hits the correction in the same breath as the claim, not three migrations
later.

Revision ID: 0055
Revises: 0054
"""
from alembic import op

revision = "0055"
down_revision = "0054"
branch_labels = None
depends_on = None

_UPGRADE_SQL = """
    CREATE VIEW thread_closure_edges AS
        SELECT o.id AS thread_id, l.id AS link_id, l.type AS edge_type,
               (CASE WHEN l.source_id = 'closure-backfill' THEN 'weak' ELSE 'strong' END)
                   ::text AS strength,
               l.to_id AS closer_id,
               l.source_id, l.created_at
        FROM objects o
        JOIN links l ON l.from_id = o.id AND l.type = 'resolved_by'
        WHERE o.type = 'Thread' AND l.valid_until IS NULL
    UNION ALL
        SELECT o.id AS thread_id, l.id AS link_id, l.type AS edge_type,
               'strong'::text AS strength, l.from_id AS closer_id,
               l.source_id, l.created_at
        FROM objects o
        JOIN links l ON l.to_id = o.id AND l.type = 'answers'
        WHERE o.type = 'Thread' AND l.valid_until IS NULL
          -- the mint_bears_on conflation fix: only a same-source status='resolved' write
          -- proves this `answers` edge came from resolves= rather than bears_on=.
          AND EXISTS (
              SELECT 1 FROM assertions a
              WHERE a.object_id = o.id AND a.name = 'status'
                AND a.value #>> '{}' = 'resolved'
                AND a.source_id = l.source_id
          )
    UNION ALL
        SELECT o.id AS thread_id, l.id AS link_id, l.type AS edge_type,
               'weak'::text AS strength, l.to_id AS closer_id,
               l.source_id, l.created_at
        FROM objects o
        JOIN links l ON l.from_id = o.id AND l.type = 'closed_by'
        WHERE o.type = 'Thread' AND l.valid_until IS NULL
"""

_DOWNGRADE_SQL = """
    CREATE VIEW thread_closure_edges AS
        SELECT o.id AS thread_id, l.id AS link_id, l.type AS edge_type,
               (CASE WHEN l.source_id = 'closure-backfill' THEN 'weak' ELSE 'strong' END)
                   ::text AS strength,
               l.to_id AS closer_id,
               l.source_id, l.created_at
        FROM objects o
        JOIN links l ON l.from_id = o.id AND l.type = 'resolved_by'
        WHERE o.type = 'Thread' AND l.valid_until IS NULL
    UNION ALL
        SELECT o.id AS thread_id, l.id AS link_id, l.type AS edge_type,
               'strong'::text AS strength, l.from_id AS closer_id,
               l.source_id, l.created_at
        FROM objects o
        JOIN links l ON l.to_id = o.id AND l.type = 'answers'
        WHERE o.type = 'Thread' AND l.valid_until IS NULL
    UNION ALL
        SELECT o.id AS thread_id, l.id AS link_id, l.type AS edge_type,
               'weak'::text AS strength, l.to_id AS closer_id,
               l.source_id, l.created_at
        FROM objects o
        JOIN links l ON l.from_id = o.id AND l.type = 'closed_by'
        WHERE o.type = 'Thread' AND l.valid_until IS NULL
"""


def upgrade() -> None:
    op.execute("DROP VIEW thread_closure_edges")
    op.execute(_UPGRADE_SQL)


def downgrade() -> None:
    op.execute("DROP VIEW thread_closure_edges")
    op.execute(_DOWNGRADE_SQL)

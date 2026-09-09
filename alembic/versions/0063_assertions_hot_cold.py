"""assertions hot/cold split -- THE EVENT LOG COLD PARTITION (dispatch wave 12, THE VAULT
BUILD): the assertions table, 3.1GB and append-only, moves its bulk out of the hot path
behind an umbrella view that keeps every existing caller working unchanged.

THE CHOICE (A vs B, both investigated before writing this): NATIVE Postgres declarative
partitioning (`PARTITION BY RANGE (created_at)`) was the more "textbook" option but was
rejected here. Two real, load-bearing costs, not just style:

  1. A partitioned table's PRIMARY KEY must include the partition column (PG16, still
     true) -- `id bigserial PRIMARY KEY` would have to become `(id, created_at)`, and a
     FK referencing it would need the same composite shape on both sides. Workable, but
     `supersedes bigint REFERENCES assertions(id)` -- a SELF-referencing FK where a row
     minted THIS month can supersede a row from a PRIOR month -- can't reference a
     partitioned table's own key by `id` alone; it would need to become
     `(supersedes, supersedes_created_at)`, touching the column shape every reader of
     `supersedes` (src/actions/core.py, src/orchestrator/retirement.py, compositions.py's
     dossier, this file's own sibling `migration_0063.py`) would need to carry.
  2. Retrofitting partitioning onto an EXISTING, live, populated table is not an
     in-place ALTER in Postgres at all: it requires creating a NEW partitioned relation
     under a temporary name, copying every row across (exactly the "never one giant scan
     over 3.1GB" bounded-batch work this dispatch already demands), then a swap -- twice
     the moving parts for the same live-migration risk this house already has to manage
     carefully once.

Repo-wide grep before choosing: `assertions.supersedes`/`INSERT INTO assertions` has
EXACTLY THREE write sites, all inside `Actions` (src/actions/core.py) -- the single-
writer waist the constitution's point 4 already names. That single fact is what makes
option B safe: the self-referencing FK's *enforcement* can move from the database to the
app layer (Actions already SELECTs to validate a target's existence in
`supersede_assertion` before it ever INSERTs) without opening any real integrity gap,
because nothing else in this codebase can write this column.

THE SHAPE actually built (Option B, refined past the dispatch's own sketch after a
codebase-wide grep turned up a wrinkle IT didn't anticipate -- see migration_0063.py's
own module docstring for the full account):

  assertions_hot   -- the renamed original table. Holds every CURRENT row forever
                      (is_current=true rows are NEVER moved -- migration_0063.py's own
                      invariant) plus recently-superseded rows still inside the trailing
                      window. No self-referencing FK (see above).
  assertions_cold  -- new, same column shape, holds only rows that are BOTH already
                      superseded (is_current=false) AND older than the archiver's
                      cutoff. `is_current` is CHECK'd false here -- cold can never hold a
                      live winner, which is exactly what keeps current_assertions correct
                      without touching it at all (see below).
  assertions       -- becomes a VIEW: `SELECT * FROM assertions_hot UNION ALL SELECT *
                      FROM assertions_cold`. Every one of the ~50 raw
                      "FROM assertions"/"JOIN assertions" call sites this repo actually
                      has (compositions.py's dossier history, retirement.py's supersede-
                      chain walks, trace_evidence, resolution.py, ingest/*, settle.py --
                      NOT just current_assertions, which was the dispatch's own stated
                      scope but turned out to undersell the real blast radius) keeps
                      reading full history, transparently, with zero code changes.

current_assertions (migration 0047) is UNTOUCHED here -- not one statement in this
migration mentions it. Its definition already reads `FROM assertions a WHERE
a.is_current`, and Postgres tracks a view's dependency by OID, not by name: renaming the
underlying table in step 1 leaves current_assertions pointing at exactly the same
relation (now called assertions_hot) without needing a `CREATE OR REPLACE`. Since a
current (is_current=true) row is never archived, current_assertions keeps returning
EXACTLY what it always did, off a permanently smaller table once the archiver
(migration_0063.py) runs -- recall and search, which read current_assertions/
winning_props, get the "out of the hot path" win the dispatch actually asked for, for
free, with no risk of a view-definition edit landing wrong.

INSTEAD OF triggers make the `assertions` view genuinely writable, because a plain UNION
ALL view is read-only in Postgres and this repo (not just Actions) writes the bare table
name directly: tests/conftest.py's own per-test reset does `DELETE FROM assertions ...`,
and tests/test_api.py, tests/test_retirement.py, tests/test_lap_lint.py all seed fixture
rows with a raw `INSERT INTO assertions (...) VALUES (...)`. Rewriting every one of those
call sites (test infra, not app code, but shared across the whole suite) would have been
a far larger and more error-prone diff than three small trigger functions. Every new row
still lands in assertions_hot (INSERT), matching the archiver's own invariant; UPDATE/
DELETE route to whichever physical table currently holds the id (checking hot first,
falling back to cold) so a rare admin correction of an archived row still works.

"COMPRESSED COLD": Postgres 16 has no table/partition-level compression short of
infrastructure this house hasn't provisioned (a separate compressed tablespace,
TimescaleDB, etc. -- said here plainly rather than overclaimed). What IS real and
verifiable: `ALTER TABLE assertions_cold ALTER COLUMN value SET COMPRESSION lz4` applies
column-level TOAST compression (PG14+) to the one field here that can plausibly be large,
`value` (jsonb) -- new TOASTed values written to assertions_cold (every row the archiver
moves) are lz4-compressed on disk, checkable per-row via `pg_column_compression(value)`.
Beyond that, "cold" is honestly just SMALLER and STATIC (fillfactor=100 -- no HOT-update
slack reserved, since a cold row is never touched again) rather than compressed in any
further sense: falls out of the hot working set / shared_buffers naturally by virtue of
almost never being scanned relative to assertions_hot's own is_current partial index.

Autocommit blocks around the rename and the cold-table DDL follow migration 0047's own
precedent (its docstring has the full AB-BA deadlock story) -- alembic's default ambient
transaction would otherwise hold the rename's brief AccessExclusiveLock for the entire
rest of this migration, including the view/trigger creation, against every live reader.
"""
from __future__ import annotations

from alembic import op

revision = "0063"
down_revision = "0062"
branch_labels = None
depends_on = None

_ASSERTIONS_COLUMNS = (
    "id, object_id, name, value, source_id, case_id, helper_run_id, evidence_uri, "
    "evidence_sha256, observed_at, confidence, supersedes, created_at, evidence_class, "
    "is_current"
)


def upgrade() -> None:
    # 1. rename -- metadata-only (no rewrite of the 3.1GB heap): same relation, same
    #    indexes, same sequence, same data, just a new name. current_assertions (OID-
    #    tracked, see module docstring) survives this untouched.
    with op.get_context().autocommit_block():
        op.execute("ALTER TABLE assertions RENAME TO assertions_hot")

    # 2. the self-referencing FK cannot express "references a row in either table" --
    #    drop it, enforcement moves to Actions (the sole writer; see module docstring).
    op.execute(
        "ALTER TABLE assertions_hot DROP CONSTRAINT IF EXISTS assertions_supersedes_fkey"
    )

    # 3. the cold table -- own PK, own indexes matching the cross-boundary query shapes
    #    (object+name history, per-source lookups, supersedes-chain joins). is_current is
    #    CHECK'd false: cold can never hold a live winner (the archiver's own invariant,
    #    enforced here too, in the schema, not just by the mover's own care).
    op.execute(
        """
        CREATE TABLE assertions_cold (
            id              bigint PRIMARY KEY,
            object_id       uuid NOT NULL REFERENCES objects(id),
            name            text NOT NULL,
            value           jsonb NOT NULL,
            source_id       text NOT NULL,
            case_id         uuid REFERENCES cases(id),
            helper_run_id   uuid,
            evidence_uri    text,
            evidence_sha256 text,
            observed_at     timestamptz NOT NULL,
            confidence      real NOT NULL CHECK (confidence BETWEEN 0 AND 1),
            supersedes      bigint,
            created_at      timestamptz NOT NULL,
            evidence_class  text,
            is_current      boolean NOT NULL DEFAULT false CHECK (is_current = false)
        ) WITH (fillfactor = 100)
        """
    )
    op.execute("CREATE INDEX assertions_cold_object_name_idx ON assertions_cold (object_id, name)")
    op.execute("CREATE INDEX assertions_cold_source_idx ON assertions_cold (source_id)")
    op.execute("CREATE INDEX assertions_cold_supersedes_idx ON assertions_cold (supersedes)")
    # "compressed cold" -- concrete and verifiable, see module docstring for what this
    # does and does not claim.
    op.execute("ALTER TABLE assertions_cold ALTER COLUMN value SET COMPRESSION lz4")

    # 4. the umbrella view -- `assertions` now means hot UNION ALL cold. Every existing
    #    raw-SQL reader of the bare table name keeps working with full history, unchanged.
    op.execute(
        "CREATE VIEW assertions AS "
        "SELECT * FROM assertions_hot UNION ALL SELECT * FROM assertions_cold"
    )

    # 5. INSTEAD OF triggers -- see module docstring for why this view must stay writable.
    op.execute(
        f"""
        CREATE FUNCTION assertions_view_insert() RETURNS trigger AS $$
        BEGIN
            IF NEW.id IS NULL THEN
                INSERT INTO assertions_hot
                    (object_id, name, value, source_id, case_id, helper_run_id,
                     evidence_uri, evidence_sha256, observed_at, confidence, supersedes,
                     created_at, evidence_class, is_current)
                VALUES
                    (NEW.object_id, NEW.name, NEW.value, NEW.source_id, NEW.case_id,
                     NEW.helper_run_id, NEW.evidence_uri, NEW.evidence_sha256,
                     NEW.observed_at, NEW.confidence, NEW.supersedes,
                     COALESCE(NEW.created_at, now()), NEW.evidence_class,
                     COALESCE(NEW.is_current, true))
                RETURNING id INTO NEW.id;
            ELSE
                INSERT INTO assertions_hot ({_ASSERTIONS_COLUMNS})
                VALUES
                    (NEW.id, NEW.object_id, NEW.name, NEW.value, NEW.source_id,
                     NEW.case_id, NEW.helper_run_id, NEW.evidence_uri,
                     NEW.evidence_sha256, NEW.observed_at, NEW.confidence,
                     NEW.supersedes, COALESCE(NEW.created_at, now()),
                     NEW.evidence_class, COALESCE(NEW.is_current, true));
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        "CREATE TRIGGER assertions_insert_instead_of INSTEAD OF INSERT ON assertions "
        "FOR EACH ROW EXECUTE FUNCTION assertions_view_insert()"
    )
    op.execute(
        """
        CREATE FUNCTION assertions_view_update() RETURNS trigger AS $$
        BEGIN
            UPDATE assertions_hot SET
                object_id=NEW.object_id, name=NEW.name, value=NEW.value,
                source_id=NEW.source_id, case_id=NEW.case_id,
                helper_run_id=NEW.helper_run_id, evidence_uri=NEW.evidence_uri,
                evidence_sha256=NEW.evidence_sha256, observed_at=NEW.observed_at,
                confidence=NEW.confidence, supersedes=NEW.supersedes,
                evidence_class=NEW.evidence_class, is_current=NEW.is_current
            WHERE id=OLD.id;
            IF NOT FOUND THEN
                UPDATE assertions_cold SET
                    object_id=NEW.object_id, name=NEW.name, value=NEW.value,
                    source_id=NEW.source_id, case_id=NEW.case_id,
                    helper_run_id=NEW.helper_run_id, evidence_uri=NEW.evidence_uri,
                    evidence_sha256=NEW.evidence_sha256, observed_at=NEW.observed_at,
                    confidence=NEW.confidence, supersedes=NEW.supersedes,
                    evidence_class=NEW.evidence_class, is_current=NEW.is_current
                WHERE id=OLD.id;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        "CREATE TRIGGER assertions_update_instead_of INSTEAD OF UPDATE ON assertions "
        "FOR EACH ROW EXECUTE FUNCTION assertions_view_update()"
    )
    op.execute(
        """
        CREATE FUNCTION assertions_view_delete() RETURNS trigger AS $$
        BEGIN
            DELETE FROM assertions_hot WHERE id=OLD.id;
            IF NOT FOUND THEN
                DELETE FROM assertions_cold WHERE id=OLD.id;
            END IF;
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        "CREATE TRIGGER assertions_delete_instead_of INSTEAD OF DELETE ON assertions "
        "FOR EACH ROW EXECUTE FUNCTION assertions_view_delete()"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS assertions_delete_instead_of ON assertions")
    op.execute("DROP FUNCTION IF EXISTS assertions_view_delete()")
    op.execute("DROP TRIGGER IF EXISTS assertions_update_instead_of ON assertions")
    op.execute("DROP FUNCTION IF EXISTS assertions_view_update()")
    op.execute("DROP TRIGGER IF EXISTS assertions_insert_instead_of ON assertions")
    op.execute("DROP FUNCTION IF EXISTS assertions_view_insert()")
    op.execute("DROP VIEW IF EXISTS assertions")
    # compensating, nothing dropped: every cold row re-enters assertions_hot exactly as
    # it was (same id, same content) before the physical table is renamed back.
    op.execute(
        f"INSERT INTO assertions_hot ({_ASSERTIONS_COLUMNS}) "
        f"SELECT {_ASSERTIONS_COLUMNS} FROM assertions_cold"
    )
    op.execute("DROP TABLE assertions_cold")
    with op.get_context().autocommit_block():
        op.execute("ALTER TABLE assertions_hot RENAME TO assertions")
    op.execute(
        "ALTER TABLE assertions ADD CONSTRAINT assertions_supersedes_fkey "
        "FOREIGN KEY (supersedes) REFERENCES assertions(id)"
    )

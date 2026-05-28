"""initial schema — ontology, append-only ledger, event-sourced identity, outbox

Revision ID: 0001
Revises:
Create Date: 2026-05-27

Embeds the resolved design rulings:
  #1 event-sourced merges    -> object_events is the source of truth; objects.status/
                                merged_into are a projection rebuilt from it.
  #2 multi-source = keep set -> assertions carry a *backward* `supersedes` pointer
                                (rows immutable); current_assertions = the non-superseded
                                set; consumers pick across sources at point of use.
  #6 hop budget              -> case_objects carries hop_distance (not objects.case_ids[]).
  concurrency               -> outbox (durable cascade events) + helper_runs partial
                                unique claim index on active statuses (retry-safe).
  integrity / scoping        -> assertions/links carry case_id, helper_run_id, evidence_sha256.
"""
from __future__ import annotations

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

STATEMENTS = [
    'CREATE EXTENSION IF NOT EXISTS "pgcrypto"',  # gen_random_uuid()

    # --- cases -------------------------------------------------------------
    """
    CREATE TABLE cases (
        id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        name              text NOT NULL,
        owner             text NOT NULL,                 -- single-operator: constant; kept for provenance
        budgets           jsonb NOT NULL DEFAULT '{}',   -- rate/handoff/hop/helpers-per-object caps
        trigger_overrides jsonb NOT NULL DEFAULT '{}',   -- per-case enable/disable (#5)
        auth_basis        jsonb NOT NULL DEFAULT '{}',   -- legal-basis / target-authorization gate
        created_at        timestamptz NOT NULL DEFAULT now(),
        archived_at       timestamptz
    )
    """,

    # --- objects (status/merged_into are a PROJECTION of object_events) -----
    """
    CREATE TABLE objects (
        id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        type         text NOT NULL,
        canonical    text NOT NULL,                       -- normalized key; deterministic dedupe
        status       text NOT NULL DEFAULT 'active',      -- active|merged|archived|draft (projection)
        merged_into  uuid REFERENCES objects(id),         -- projection of merge events
        created_at   timestamptz NOT NULL DEFAULT now(),
        UNIQUE (type, canonical)                          -- auto-merge for deterministic types only
    )
    """,
    "CREATE INDEX objects_type_idx ON objects (type)",

    # --- case membership + hop distance (#6) -------------------------------
    """
    CREATE TABLE case_objects (
        case_id      uuid NOT NULL REFERENCES cases(id),
        object_id    uuid NOT NULL REFERENCES objects(id),
        hop_distance int NOT NULL DEFAULT 0,              -- graph distance from seed (cascade budget)
        added_by_run uuid,                                -- helper_runs.id that pulled it in (null=seed)
        added_at     timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (case_id, object_id)
    )
    """,
    "CREATE INDEX case_objects_object_idx ON case_objects (object_id)",

    # --- object_events: append-only identity lineage = SOURCE OF TRUTH (#1) -
    """
    CREATE TABLE object_events (
        id          bigserial PRIMARY KEY,
        event_type  text NOT NULL,                        -- create|merge|split|unmerge|archive
        object_id   uuid NOT NULL REFERENCES objects(id), -- subject (winner, for merge)
        related_id  uuid REFERENCES objects(id),          -- loser (merge) / source (split)
        payload     jsonb NOT NULL DEFAULT '{}',          -- justification, partition_spec, ...
        actor       text NOT NULL,
        case_id     uuid REFERENCES cases(id),
        created_at  timestamptz NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX object_events_object_idx ON object_events (object_id)",
    "CREATE INDEX object_events_created_idx ON object_events (created_at)",

    # --- assertions: append-only; backward `supersedes`; multi-source (#1/#2)
    """
    CREATE TABLE assertions (
        id              bigserial PRIMARY KEY,
        object_id       uuid NOT NULL REFERENCES objects(id),
        name            text NOT NULL,
        value           jsonb NOT NULL,
        source_id       text NOT NULL,                    -- helper id or 'analyst:<id>'
        case_id         uuid REFERENCES cases(id),        -- which case produced it
        helper_run_id   uuid,                             -- provenance link to helper_runs
        evidence_uri    text,                             -- artifact path (local object store)
        evidence_sha256 text,                             -- content-addressed integrity
        observed_at     timestamptz NOT NULL,             -- when the fact was true (authoritative clock)
        confidence      real NOT NULL CHECK (confidence BETWEEN 0 AND 1),
        supersedes      bigint REFERENCES assertions(id), -- backward: old rows are NEVER mutated
        created_at      timestamptz NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX assertions_object_name_idx ON assertions (object_id, name)",
    "CREATE INDEX assertions_source_idx ON assertions (source_id)",
    "CREATE INDEX assertions_supersedes_idx ON assertions (supersedes)",

    # current value = the set of non-superseded assertions; per (object,name,source) the
    # action layer supersedes within-source, so cross-source disagreement survives as a SET.
    """
    CREATE VIEW current_assertions AS
        SELECT a.*
        FROM assertions a
        WHERE NOT EXISTS (SELECT 1 FROM assertions s WHERE s.supersedes = a.id)
    """,

    # --- links: append-only; deactivate via not_same_as / valid_until ------
    """
    CREATE TABLE links (
        id              bigserial PRIMARY KEY,
        from_id         uuid NOT NULL REFERENCES objects(id),
        to_id           uuid NOT NULL REFERENCES objects(id),
        type            text NOT NULL,
        properties      jsonb NOT NULL DEFAULT '{}',
        source_id       text NOT NULL,
        case_id         uuid REFERENCES cases(id),
        helper_run_id   uuid,
        evidence_uri    text,
        evidence_sha256 text,
        first_seen      timestamptz,
        last_seen       timestamptz,
        valid_until     timestamptz,                      -- explicit deactivation (never delete)
        confidence      real NOT NULL CHECK (confidence BETWEEN 0 AND 1),
        created_at      timestamptz NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX links_from_idx ON links (from_id, type)",
    "CREATE INDEX links_to_idx ON links (to_id, type)",

    # --- helper_runs: §9 state machine + windowed dedupe + atomic claim -----
    """
    CREATE TABLE helper_runs (
        id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        helper_id     text NOT NULL,
        object_id     uuid NOT NULL REFERENCES objects(id),
        case_id       uuid NOT NULL REFERENCES cases(id),
        status        text NOT NULL,        -- queued|running|done|failed|awaiting_human
                                            -- |in_browser|result_posted_back|abandoned
        tier          text NOT NULL,        -- open|fragile|gated|manual
        window_bucket timestamptz,          -- null=non-windowed; bucket start for windowed (#4)
        started_at    timestamptz,
        finished_at   timestamptz,
        result        jsonb,
        error         text,
        created_at    timestamptz NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX helper_runs_status_idx ON helper_runs (status)",
    "CREATE INDEX helper_runs_case_object_idx ON helper_runs (case_id, object_id)",
    # one *active* run per (helper,object,case,window) — prevents duplicate dispatch but
    # still allows a fresh attempt once a prior run is done/failed/abandoned.
    """
    CREATE UNIQUE INDEX helper_runs_active_claim ON helper_runs
        (helper_id, object_id, case_id, COALESCE(window_bucket, 'epoch'::timestamptz))
        WHERE status IN ('queued','running','awaiting_human','in_browser','result_posted_back')
    """,

    # --- triggers: projection of manifests (#5); not hand-edited ------------
    """
    CREATE TABLE triggers (
        id          serial PRIMARY KEY,
        on_event    text NOT NULL,          -- object_created|property_added|link_created|object_merged
        match       jsonb NOT NULL,
        helper_id   text NOT NULL,
        conditions  jsonb NOT NULL DEFAULT '{}',
        enabled     boolean NOT NULL DEFAULT true
    )
    """,

    # --- audit_log: universal append-only action record --------------------
    """
    CREATE TABLE audit_log (
        id         bigserial PRIMARY KEY,
        action     text NOT NULL,
        actor      text NOT NULL,
        case_id    uuid REFERENCES cases(id),
        payload    jsonb NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX audit_log_created_idx ON audit_log (created_at)",

    # --- outbox: transactional outbox for durable cascade events -----------
    """
    CREATE TABLE outbox (
        id           bigserial PRIMARY KEY,
        event_type   text NOT NULL,         -- object_created|property_added|link_created|object_merged
        object_id    uuid REFERENCES objects(id),
        case_id      uuid REFERENCES cases(id),
        payload      jsonb NOT NULL DEFAULT '{}',
        created_at   timestamptz NOT NULL DEFAULT now(),
        published_at timestamptz,
        claimed_at   timestamptz
    )
    """,
    "CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL",

    # --- merge_candidates: ordered pair (a<b), fuzzy ER review queue --------
    """
    CREATE TABLE merge_candidates (
        id          bigserial PRIMARY KEY,
        a_id        uuid NOT NULL REFERENCES objects(id),
        b_id        uuid NOT NULL REFERENCES objects(id),
        score       real NOT NULL,
        reasons     jsonb NOT NULL,
        resolved    text,                   -- null|merged|rejected
        resolved_by text,
        resolved_at timestamptz,
        created_at  timestamptz NOT NULL DEFAULT now(),
        CHECK (a_id < b_id),
        UNIQUE (a_id, b_id)
    )
    """,

    # --- cookie_leases: encrypted blob (key via OS keyring, Phase 5) --------
    """
    CREATE TABLE cookie_leases (
        id          bigserial PRIMARY KEY,
        origin      text NOT NULL,
        cookie_blob jsonb NOT NULL,         -- encrypted at rest (app-level)
        ua          text NOT NULL,
        bound_ip    inet,
        expires_at  timestamptz NOT NULL,
        issued_by   text NOT NULL,
        created_at  timestamptz NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX cookie_leases_origin_idx ON cookie_leases (origin)",
]

DROP_ORDER = [
    "cookie_leases", "merge_candidates", "outbox", "audit_log", "triggers",
    "helper_runs", "links", "assertions", "object_events", "case_objects",
    "objects", "cases",
]


def upgrade() -> None:
    for stmt in STATEMENTS:
        op.execute(stmt)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS current_assertions")
    for table in DROP_ORDER:
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

-- osiris-pg tuning, ALTER SYSTEM form — for the EXISTING running container (this box's
-- own hand-run osiris-pg, or any other already-standing instance), no data-volume remount
-- needed. Values and their justification live in deploy/postgresql.conf (the reference
-- this script and docker-compose.full.yml's `command:` both draw from) — read that file
-- first if a number here looks surprising.
--
-- NOT RUN BY THIS BUILD. The operator applies it:
--   docker exec -i osiris-pg psql -U osiris -d osiris -f deploy/postgres_tuning.sql
--   docker restart osiris-pg   # shared_buffers and maintenance_work_mem need this;
--                               # the rest take effect on the next SIGHUP / next backend
--
-- Also runs the audit_log stats fix named in postgresql.conf's own evidence section
-- (never auto-analyzed despite 3.2M rows — the planner has been working off a stale
-- 245K-row estimate) and tightens autovacuum specifically on the two highest-churn
-- tables (outbox, audit_log) rather than touching the global scale factor, which the
-- rest of the database's much smaller tables have no need of.

ALTER SYSTEM SET shared_buffers = '4GB';
ALTER SYSTEM SET effective_cache_size = '12GB';
ALTER SYSTEM SET work_mem = '32MB';
ALTER SYSTEM SET maintenance_work_mem = '512MB';
ALTER SYSTEM SET random_page_cost = 1.1;

-- per-table autovacuum: outbox and audit_log are the two highest-churn tables in the
-- database (3.2M rows each, entirely INSERT-driven) — the stock 0.1 analyze / 0.2 vacuum
-- scale factors (10%/20% of the table's OWN row estimate before triggering) mean, on a
-- table this large, autovacuum waits for hundreds of thousands of new rows before it
-- fires even once. Tightened here rather than globally: every other table in this
-- database is small enough that the stock scale factors already fire promptly.
ALTER TABLE outbox SET (autovacuum_vacuum_scale_factor = 0.05,
                        autovacuum_analyze_scale_factor = 0.02);
ALTER TABLE audit_log SET (autovacuum_vacuum_scale_factor = 0.05,
                           autovacuum_analyze_scale_factor = 0.02);

-- ONE-TIME CATCH-UP: audit_log has never been auto-analyzed (measured 2026-08-17 —
-- pg_stat_user_tables reported 245,854 live rows against a real count of 3,219,472, a
-- ~13x undercount) — the per-table setting above prevents recurrence, this line fixes
-- today's already-stale statistics immediately rather than waiting for the next
-- qualifying write.
ANALYZE audit_log;

SELECT pg_reload_conf();

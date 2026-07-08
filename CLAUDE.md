# CLAUDE.md — boot sector

Osiris is the memory; this file is only the key to it. **Do not append history here.**
The old build log is frozen at `docs/HISTORY.md` and lives in the graph as dated
`ref:history-*` nodes; DESIGN.md lives as `ref:design-*`; the memory essays as canon.
A memory carried in full into every context is cargo — recall is a bounded query.

## What this is
A self-hosted, provenance-first entity-graph engine — **"Palantir × Notion, composed by
conversation."** Endgame: a prosthesis for Claude + a lens for asuramaya — a composition
shape-shifter. The engine is the product; personas are compositions, never coded pages.

## Identity check (first, before anything)
Sessions on this project run **Fable 5** by the operator's standing choice. Harness
degradations silently swap the model mid-session; if your environment names a different
model, SAY SO in your first reply. A rug-pull is confessed, never inherited blind.

## Mount (orient from the graph, not from files)
- Dev instance: Postgres `127.0.0.1:5601` / Redis `:6396` (`.claude/settings.local.json`
  exports DATABASE_URL). Daemons: `systemctl --user status osiris-pulse osiris-worker`.
- Orient: MCP `run_composition('briefing')` (socket in `.mcp.json`) — open threads,
  obligations, recent decisions. Design/ops questions: `consult_canon(q)` (canon + essays;
  try q="ops"). History: grep docs/HISTORY.md — `ref:history-*` is outside mounted canon scope.
- Write back AS YOU GO: `record_decision` / `open_thread` (kind='obligation' for duties
  an action mints) / `resolve_thread`. A session can die at any instant; anything not
  written back does not exist. The session-miner backfills what you forget (DERIVED),
  but deliberate capture (SELF_DECLARED) is the record.

## Constitution (invariants — changing one is a ruling to record)
1. Never auto-merge Person; identity merges are review-gated, always.
2. Osiris has NO HANDS: it senses and remembers; it never mutates a repo or the world.
   Claude is the intelligence (lens); the worker is the alarm clock (tripwire).
3. Event-sourced, append-only kernel: status/merges are projections of `object_events`;
   heal with compensating events, never DELETE.
4. Every write goes through the Actions waist; object/link types are declared in
   `src/ontology/schema.py` (strict in CI, warn in prod).
5. Evidence-graded ingest: SELF_DECLARED > AUTHORITATIVE_API > … > DERIVED; miners are
   backfill behind ownership boundaries (a miner never touches another source's objects).
6. The membrane: the loop may close, but never silently and never irreversibly.
7. Loop pathology is a named bug class — any process reading AND writing the graph at
   different levels gets an explicit ownership boundary at design time.
8. Keyless collection is a safety feature (open entity commons, not private persons).
9. Build publicly: merge to the remote (github.com/asuramaya/osiris) on MAJOR changes,
   at a clean milestone (tree clean, CI-ready, secrets/PII scanned) — announced, never
   mid-stack. (Standing operator ruling; supersedes the old absolute push gate.)
10. Do not grow this file; do not create new md dumps. Knowledge goes in the graph.

## Stack & gates
Python 3.12 (`uv`) · asyncpg/httpx/arq · FastAPI · Postgres 16 + Redis 7 · Alembic.
Gates before any commit: `uv run pytest` (testcontainers — real PG, never mocks) ·
`uv run ruff check src tests` · `uv run mypy src` (--strict). Console: `:8011`.

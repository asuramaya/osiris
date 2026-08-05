# Contributing

Osiris is open source (AGPL-3.0) and built in the open. At this early stage it is
**source-available and developed by a single operator** — the most useful contribution
right now is **using it, and opening an issue** with what broke, what was wrong, or what
was missing. Please open an issue to discuss before sending a large PR, so effort isn't
wasted on something that doesn't fit the roadmap.

## Run it

```bash
cp .env.example .env
docker compose up -d          # Postgres 16 + Redis 7
uv sync
DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris uv run alembic upgrade head
uv run pytest
```

Python 3.12 via [uv](https://docs.astral.sh/uv/). Async throughout (asyncpg, httpx, arq),
FastAPI, Alembic.

## The bar (non-negotiable, CI-enforceable)

Every change must keep all three green:

```bash
uv run ruff check src tests
uv run mypy --strict src/
uv run pytest            # real Postgres/Redis via testcontainers — never SQLite, never mocks
```

Tests run against **real** Postgres and Redis (spun up by testcontainers). This is
deliberate: the kernel's correctness (idempotency, atomic claims, event-sourced merges,
merge-aware reads) only means something against a real database. Don't add SQLite or mock
the DB.

## Design rules that aren't negotiable

These come from hard-won lessons (see [`CLAUDE.md`](CLAUDE.md)) and a PR that breaks one
won't merge:

- **The kernel imports no driver.** Connectors depend on the Actions API; never the
  reverse. (The narrow waist — see [`ARCHITECTURE.md`](ARCHITECTURE.md).)
- **Drivers emit graded, sourced facts.** Every assertion/link carries an evidence class
  (`parsers/evidence.py`) and provenance. No bare confidence numbers.
- **Resolution is deterministic or review-gated.** A Person is never auto-merged. No fuzzy
  token-overlap auto-merges — a false merge is worse than no merge.
- **Read models are merge-aware.** Any analytic that traverses raw links must expand to the
  full identity set (forward to the merge winner + recursively the merged-in), or it
  silently drops data.
- **No silent caps.** If something bounds coverage (top-N, no-retry, sampling), log what
  was dropped.

## Adding a connector

A new federator is the easiest contribution. Follow the shape of `src/ingest/gleif.py` or
`src/ingest/orgbook.py`: a pure `parse_*`, a `search_*`/`fetch_*` network seam, an
`ingest_*` that emits through Actions with an evidence class, an `aim_*` entry point, a
`__main__` CLI, and a hermetic test that drives the parser + ingest with a fixture (the
network is the seam). Keyless sources strongly preferred. Wire it into
`orchestrator/sources.py` (the playbook) and, if it warrants a tool, the MCP server.

Respect each source's rate limits and Terms of Service, and add its license to the table
in [`RESPONSIBLE_USE.md`](RESPONSIBLE_USE.md).

# Osiris

[![MCP Server](https://img.shields.io/badge/MCP-Streamable--HTTP%20%3A8790-blue.svg?style=flat-square&logo=anthropic)](https://modelcontextprotocol.io/)
[![Claude Code](https://img.shields.io/badge/Claude_Code-Native_Hooks-black.svg?style=flat-square)](https://docs.anthropic.com/claude/docs/claude-code)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%2B_pg__trgm-336791.svg?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7%2B_EventBus-DC382D.svg?style=flat-square&logo=redis&logoColor=white)](https://redis.io/)
[![Python](https://img.shields.io/badge/Python-3.12%2B_uv-3776AB.svg?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![License](https://img.shields.io/badge/License-AGPL--3.0-orange.svg?style=flat-square)](LICENSE)

Osiris is a self-hosted memory and coordination graph for AI agents. It turns
reasoning, decisions, loose ends, inter-agent messages, and public entity records into
durable, queryable graph memory that survives context windows, compactions, and
restarts. It runs as one Postgres-backed service, reachable from any harness over the
Model Context Protocol.

## Why this exists

An agent's own context window is not memory: it resets on every compaction, every new
session, every harness restart. Osiris gives an agent a place to write facts and
decisions down, with who said it and how confident they were, and a cheap way to read
back only what the moment needs instead of a full history dump. It is one Postgres
database plus a thin write layer, not a new agent framework: any MCP client, or a
human at a terminal, reads and writes the same graph.

## Core ideas

**The Actions Waist.** Every write into Osiris goes through one layer
(`src/actions/core.py`). Direct database writes are not allowed. Every change appends
an event, never overwrites or deletes:

```python
async with actions.atomic():
    oid = await actions.create_or_find_object("SoftwareProject", "repo:osiris", actor="agent:example")
    await actions.assert_property(oid, "status", "active", source_id="agent:example", observed_at=now, confidence=1.0)
    await actions.create_link(from_id=decision_id, to_id=oid, type_="in_repo", source_id="agent:example", observed_at=now, confidence=1.0)
```

**Graded evidence.** A fact's confidence is never a number pulled from thin air; it
comes from how the fact was obtained (`src/parsers/evidence.py`), strongest first:

- **Corroborated**: two or more independent sources agree (computed, never assigned directly)
- **Self-declared**: a first-party statement, such as an agent recording its own decision
- **Authoritative API**: a verified response from a canonical external source
- **Direct observation**: read from a real runtime transcript or execution trace
- **Derived**: inferred by an automated backfill pass
- **Co-occurrence**: a weak, statistical proximity signal, the lowest tier

**Compositions.** A composition is a saved, forkable query over the graph, built from
a small set of operators (select, traverse, collect, aggregate, and others) instead of
raw SQL. See `docs/COMPOSER.md` for the full vocabulary and the design it's grounded
in.

## Install

Prerequisites: Python 3.12+ (managed with [`uv`](https://docs.astral.sh/uv/)),
PostgreSQL 16+ with `pg_trgm`, and Redis 7+.

```bash
git clone https://github.com/asuramaya/osiris.git
cd osiris
uv sync

# Point at your database and apply migrations
export DATABASE_URL="postgresql://osiris:osiris@127.0.0.1:5432/osiris"
uv run alembic upgrade head
uv run python -m src.init   # seed catalog types and design canon

# Start the three services
OSIRIS_MCP_TRANSPORT=streamable-http uv run python -m src.mcp_server   # MCP, :8790
uv run arq src.workers.arq_worker.WorkerSettings                       # background worker
uv run uvicorn --factory src.api.app:create_app --port 8011            # console (optional)
```

For systemd units, Docker Compose, and every option in full, see
[`docs/INSTALL.md`](docs/INSTALL.md).

## First run

**Set up the encryption key.** Stored session transcripts are encrypted at rest. A
missing key doesn't block startup, but set one up before real data accumulates:

```bash
osiris soul-key init --restart      # mint the key, restart the services to pick it up
osiris soul-key enroll-recovery     # enroll a FIDO2 security key as your recovery path
```

**Set up off-box backups.** A separate credential protects the backup repository:

```bash
osiris restic-key init
osiris backup-settings write --offload-add nas --offload-kind restic \
  --offload-target "sftp:nas.local:/backups/osiris" --offload-schedule "*:0/15" \
  --because "first-run offload target"
osiris offload-runner tick          # run one offload by hand to confirm it reaches the target
osiris soul-key restore-drill       # prove the backup actually restores, not just that it accepted one
```

The console's Settings pane (open it from the header's gear icon, or the command
palette) is the ongoing dashboard for both. Full detail: [`docs/KEYS.md`](docs/KEYS.md)
and [`docs/BACKUP.md`](docs/BACKUP.md).

**Start your first project.** The two commands worth remembering:

```bash
osiris new <name>      # a self-managed seat: its own workspace, its own project
osiris launch <name>   # give it a live process
```

`osiris new` answers to nobody and needs no existing repository. To bring an existing
project's own markdown notes into the graph instead, use `osiris bootstrap
<path>`. Run `osiris --help` for the full command list, grouped by what you're trying
to do.

## The console and the CLI

The console is a browser UI at `http://127.0.0.1:8011/ui/`: browse the graph, read and
send mail, run saved compositions, and manage settings, keys, and backups from one
place.

The CLI (`osiris`) and the MCP tool surface are the two supported ways to operate
Osiris: the CLI for a human at a terminal, MCP tools for an agent. Every read command
takes `--json` for scripting. See [`docs/CLI.md`](docs/CLI.md) for the full reference.

## Documentation

- [`docs/INSTALL.md`](docs/INSTALL.md): full install and systemd setup
- [`docs/CLI.md`](docs/CLI.md): the `osiris` command reference
- [`docs/KEYS.md`](docs/KEYS.md): the encryption key and backup credential
- [`docs/BACKUP.md`](docs/BACKUP.md): backup schedules and off-box offload targets
- [`docs/DEPLOY.md`](docs/DEPLOY.md): production deploy and daemon management
- [`docs/CROSS_HARNESS.md`](docs/CROSS_HARNESS.md): connecting other agent harnesses
- [`docs/COMPOSER.md`](docs/COMPOSER.md): the composition query language
- [`docs/RITUAL.md`](docs/RITUAL.md): the agent read/write conventions
- [`ARCHITECTURE.md`](ARCHITECTURE.md): system design
- [`docs/REFERENCE.md`](docs/REFERENCE.md): the data model, generated from the code
- [`ROADMAP.md`](ROADMAP.md): what's built and what's next

## Constitution

A short list of invariants this system holds to:

1. **Never auto-merge a Person.** Identity merges are always reviewed by a human.
2. **Every write goes through the Actions Waist.** No direct database writes.
3. **Append-only.** Facts and links are never deleted; corrections are new events.
4. **Evidence is graded, never assumed.** See Core ideas above.
5. **The membrane.** An automated process may close a loop, but never silently and never irreversibly.
6. **Keyless public collection.** Public entity data is gathered without leaking private credentials.
7. **Build publicly.** Clean milestones, real tests, and a secret/PII scan before anything ships.

## License

Osiris is free software licensed under the
**[GNU Affero General Public License v3.0](LICENSE)**. AGPL-3.0 is a network-copyleft
license: if you run a modified Osiris as a service other people use over a network,
you must offer them the corresponding source.

For guidelines on responsible use and keyless public entity collection, see
[`RESPONSIBLE_USE.md`](RESPONSIBLE_USE.md).

Third-party assets vendored under `src/ui/static/vendor/` and `src/api/inbox/static/`
keep their own upstream licenses (MIT, per the headers preserved in each file).

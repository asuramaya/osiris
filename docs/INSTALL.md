<!-- topic: getting-started -->

# INSTALL — zero to a mounted fleet

The stranger's runbook: from `git clone` to a working console and a fleet of Claude
sessions sharing one memory graph. Every step is copy-paste; nothing assumes you are the
operator. The hosted topology, backups, and alerting live in [`DEPLOY.md`](DEPLOY.md) —
this page gets one box from nothing to mounted.

## 0. Prerequisites

- Linux, Python 3.12, [`uv`](https://docs.astral.sh/uv/), Docker (for Postgres/Redis;
  the test gates also use it via testcontainers)
- Claude Code, for the fleet half (the console works without it)

## 1. Clone + dependencies

```bash
git clone https://github.com/asuramaya/osiris && cd osiris
uv sync
```

## 2. Substrate — Postgres 16 + Redis 7

```bash
docker run -d --name osiris-pg -e POSTGRES_USER=osiris -e POSTGRES_PASSWORD=osiris \
  -e POSTGRES_DB=osiris -p 127.0.0.1:5432:5432 postgres:16
docker run -d --name osiris-redis -p 127.0.0.1:6379:6379 redis:7
export DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris
export REDIS_URL=redis://127.0.0.1:6379/0
```

Already have Postgres/Redis? Point the two URLs at them and skip the containers.

## 3. Schema + seed

```bash
uv run alembic upgrade head   # the event-sourced kernel, to head
uv run python -m src.init     # rooms + default compositions + the design canon (idempotent)
```

A fresh DB is an empty shell until `src.init` — without it the console lands on nothing.

## 4. The three surfaces (foreground first)

Run each in its own terminal to see them work; step 5 makes them daemons.

```bash
# the console + read API (the human lens; /membrane is the fleet's upward lane)
uv run uvicorn --factory src.api.app:create_app --host 127.0.0.1 --port 8011

# the worker (cascade drain + watch + reaper — the tripwire)
uv run arq src.workers.arq_worker.WorkerSettings

# the MCP server (the fleet floodgate: ONE always-on process, one shared pool)
OSIRIS_MCP_TRANSPORT=streamable-http uv run python -m src.mcp_server
```

Verify: `curl -s 127.0.0.1:8011/health` → `{"status":"ok"}`; console at
<http://127.0.0.1:8011>, the read-only lens at <http://127.0.0.1:8011/membrane>; the MCP
server listens at `http://127.0.0.1:8790/mcp` — `curl -s -o /dev/null -w '%{http_code}\n'
127.0.0.1:8790/mcp` should print `406`: a bare GET carries no `Accept: text/event-stream`
header, which the streamable-http transport correctly rejects, so 406 means the server is
up and enforcing the protocol, not that something is broken. `curl: (7) Connection refused`
is the actual failure signal.

## 5. Keep them alive (systemd user units)

`deploy/user/` ships all four unit files. On a single-operator box they install as **user**
units — running as you, against your own instance (the `/opt` system-unit form is in
[`DEPLOY.md`](DEPLOY.md)). `osiris deploy` (re)installs these over
`~/.config/systemd/user/` on every deploy after the first (thread e6fd3772 piece 3-infra); by
hand, one time:

```bash
mkdir -p ~/.config/systemd/user
cp deploy/user/*.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now osiris-console osiris-pulse osiris-worker osiris-mcp
loginctl enable-linger "$USER"   # survive logout / reboot
```

Adjust `WorkingDirectory`, the venv path, and the DB/Redis URLs in each unit to your box;
`osiris-pulse` senses the repos listed in its `OSIRIS_DEV_REPOS=` line.

## 6. Wire your repos into the fleet

Box-wide (recommended — every project you open mounts osiris by default):

```bash
claude mcp add --scope user --transport http osiris http://127.0.0.1:8790/mcp \
  --header 'X-Osiris-Job: ${CLAUDE_JOB_DIR}'
```

Per-repo (pinned config, checked into that repo):

```bash
uv run python -m src.orchestrator.onboard /path/to/repo --statusline
```

It creates-or-patches the repo's `.mcp.json` (merging around any servers already there;
idempotent; refuses invalid JSON) and, with `--statusline`, `.claude/settings.json`; it
prints the boot-sector stanza to paste into that repo's CLAUDE.md. `--dry-run` previews.

First Claude session inside the repo: `mount(cwd=<repo>, job_dir=$CLAUDE_JOB_DIR)` →
`orient()` → `inbox()` — and if the repo carries markdown memory (a CLAUDE.md build log,
DESIGN.md, essays), run the `bootstrap` MCP tool once: it indexes them into the graph as
queryable Reference nodes and never edits your files.

## 7. Developing osiris itself

```bash
uv run pytest                  # real Postgres via testcontainers — Docker required
uv run ruff check src tests
uv run mypy src                # strict
```

Backups (`deploy/backup.sh` / `restore.sh`), the full container stack, alert delivery, and
the worker dead-man's-switch: [`DEPLOY.md`](DEPLOY.md).

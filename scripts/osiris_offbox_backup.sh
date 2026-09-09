#!/usr/bin/env bash
# THE OFF-BOX PUSH (thread cf134938's own shape, item 1 — design pending the operator's
# backend ruling on that thread; this ships now, parameterized by the repository URL
# alone, so the eventual ruling is a one-line config change to a timer's own
# ExecStart, never a rebuild). Copies the vault (dumps, base backups, WAL archive) to
# a SECOND box via restic — content-defined chunking + dedup + client-side encryption
# before upload, the correct default the moment data leaves this physically-controlled
# box. `restic init` runs exactly once per repository, idempotently: `restic
# snapshots` against a not-yet-initialized repo fails, and only THEN do we init — an
# already-initialized repo is never touched here, and restic itself refuses a
# double-init anyway.
#
# THE URL IS BACKEND-AGNOSTIC BY DESIGN: TrueNAS via `sftp:user@host:/path`, an
# S3-compatible target via `s3:https://host/bucket`, or (as this script's own tests
# exercise) a plain local directory via `local:/path` or a bare path — restic's own
# repository-URL scheme decides, this script never branches on it.
#
# CREDENTIALS: RESTIC_PASSWORD or RESTIC_PASSWORD_FILE (restic's own env contract)
# must already be set in the calling environment — a systemd EnvironmentFile= outside
# this repo in production, never a script argument (visible via `ps`) or anything
# committed here.
set -euo pipefail
REPO_URL="${1:?usage: osiris_offbox_backup.sh <restic-repository-url>}"
VAULT="${OSIRIS_VAULT:-$HOME/osiris-vault}"
export RESTIC_REPOSITORY="$REPO_URL"

if ! restic snapshots >/dev/null 2>&1; then
  restic init
fi

# `--exclude` the transient `.tmp` files osiris_backup.sh's own WAL pull writes
# mid-transfer — real content only, never a half-written segment caught mid-copy.
restic backup "$VAULT" --exclude="*.tmp"

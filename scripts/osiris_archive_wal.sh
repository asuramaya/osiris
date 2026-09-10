#!/usr/bin/env bash
# THE archive_command SCRIPT (vault lane, operator ruling 39384a87/c53a5fc0, item 3: "WAL
# archiving into the vault plus a weekly base backup, with preflight's drill restoring to
# a point in time"). Runs INSIDE the osiris-pg container, invoked by Postgres itself as
# `archive_command` for every completed WAL segment (`%p` = the segment's own path, `%f`
# = its bare filename) — Postgres's own contract: this must be atomic and idempotent, and
# a non-zero exit tells Postgres to RETRY the same segment later rather than lose it.
#
# BIND-MOUNTED READ-ONLY FROM THIS REPO CHECKOUT (compose-drift fix, Thoth mail 9122
# item 2, wave 16) to /var/lib/postgresql/data/osiris_archive_wal.sh — deploy/up.sh,
# docker-compose.yml, and deploy/docker-compose.full.yml all mount this exact file at
# this exact path, so a container recreate always finds the CURRENT checked-in version,
# never a stale manually-`docker cp`'d copy nobody remembers making. (Earlier this was
# a one-off manual copy INTO the data volume with no deploy artifact reproducing it —
# the exact silent-drift risk this fix closes; a bind mount from the repo is idempotent
# across recreates by construction, unlike a copy-once-and-forget step.)
#
# WRITES INTO THE SAME VOLUME (/var/lib/postgresql/data/wal_archive/), NOT DIRECTLY INTO
# THE HOST VAULT: this container carries no bind mount for ~/osiris-vault today (checked:
# `docker inspect osiris-pg` shows exactly one mount, the pgdata volume) — adding one
# means recreating the container, the same restart-class action this script's own
# deployment avoids needing. osiris_backup.sh (running on the HOST, which already has
# `docker exec` access) is the OTHER half: it pulls newly-archived segments out into the
# vault and prunes the ones it already has, keeping this in-volume staging directory
# small rather than letting it grow forever.
set -euo pipefail
SRC="$1"   # %p — the WAL segment's own full path
NAME="$2"  # %f — its bare filename
DEST_DIR="/var/lib/postgresql/data/wal_archive"
DEST="$DEST_DIR/$NAME"

mkdir -p "$DEST_DIR"

# ATOMIC AND IDEMPOTENT (Postgres's own archive_command contract): if the destination
# already exists, a PRIOR attempt already archived this exact segment — WAL segment names
# are unique and immutable, so an existing file of the SAME size is success already
# achieved, never overwritten (overwriting a completed archive risks corrupting it mid-
# write against a concurrent reader on the host side). A same-name, different-size file
# is impossible under normal operation (WAL segments are fixed-size) and is refused
# loudly rather than silently trusted.
if [ -e "$DEST" ]; then
  if [ "$(stat -c%s "$DEST" 2>/dev/null)" = "$(stat -c%s "$SRC")" ]; then
    exit 0
  fi
  echo "osiris_archive_wal: $DEST exists with a DIFFERENT size than $SRC — refusing to overwrite" >&2
  exit 1
fi

cp "$SRC" "$DEST.tmp" && mv "$DEST.tmp" "$DEST"

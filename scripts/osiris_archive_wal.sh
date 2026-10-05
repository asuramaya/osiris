#!/usr/bin/env bash
# THE archive_command SCRIPT (vault lane): WAL archiving into the vault plus a weekly
# base backup, with preflight's drill restoring to a point in time. Runs INSIDE the
# osiris-pg container, invoked by Postgres itself as `archive_command` for every
# completed WAL segment (`%p` = the segment's own path, `%f` = its bare filename).
# Postgres's own contract: this must be atomic and idempotent, and a non-zero exit
# tells Postgres to RETRY the same segment later rather than lose it.
#
# BIND-MOUNTED READ-ONLY FROM THIS REPO CHECKOUT (a compose-drift fix) to
# /var/lib/postgresql/data/osiris_archive_wal.sh: deploy/up.sh, docker-compose.yml,
# and deploy/docker-compose.full.yml all mount this exact file at this exact path, so
# a container recreate always finds the CURRENT checked-in version, never a stale
# manually-`docker cp`'d copy nobody remembers making. (Earlier this was a one-off
# manual copy INTO the data volume with no deploy artifact reproducing it: the exact
# silent-drift risk this fix closes. A bind mount from the repo is idempotent across
# recreates by construction, unlike a copy-once-and-forget step.)
#
# WRITES INTO THE SAME VOLUME (/var/lib/postgresql/data/wal_archive/), NOT DIRECTLY INTO
# THE HOST VAULT: this container carries no bind mount for ~/osiris-vault today (checked:
# `docker inspect osiris-pg` shows exactly one mount, the pgdata volume). Adding one
# means recreating the container, the same restart-class action this script's own
# deployment avoids needing. osiris_backup.sh (running on the HOST, which already has
# `docker exec` access) is the OTHER half: it pulls newly-archived segments out into the
# vault and prunes the ones it already has, keeping this in-volume staging directory
# small rather than letting it grow forever.
set -euo pipefail
SRC="$1"   # %p, the WAL segment's own full path
NAME="$2"  # %f, its bare filename
# overridable only so the script can be exercised against a temporary directory
DEST_DIR="${OSIRIS_WAL_ARCHIVE_DIR:-/var/lib/postgresql/data/wal_archive}"
DEST="$DEST_DIR/$NAME"

mkdir -p "$DEST_DIR"

# COMPRESSED AT THE SOURCE: a WAL segment is always 16 MB on disk however little of it holds
# data, and archive_timeout closes a segment every few minutes even when the database is
# quiet, so a mostly-empty segment costs a full 16 MB in the container, the vault and every
# copy of it unless it is compressed. zstd takes such a segment to a few hundred kilobytes.
# Only the plain 24-hex-character segment files are compressed (as "<name>.zst"); timeline
# history, backup-label and partial files are tiny and recovery wants them verbatim. The
# compressed copy is verified by decompressing it against the source before it is moved
# into place, and if zstd is missing or fails for any reason the segment is archived raw
# exactly as before: archiving must never fail because compression is unavailable.
# Restore reads either form (see osiris_pitr_drill.py's restore_command).
COMPRESS=0
if [[ "$NAME" =~ ^[0-9A-F]{24}$ ]] && command -v zstd >/dev/null 2>&1; then
  COMPRESS=1
fi

# ATOMIC AND IDEMPOTENT (Postgres's own archive_command contract): if the destination
# already exists, a PRIOR attempt already archived this exact segment. WAL segment names
# are unique and immutable, so an existing identical copy is success already achieved,
# never overwritten (overwriting a completed archive risks corrupting it mid-write against
# a concurrent reader on the host side). A same-name, different copy is impossible under
# normal operation and is refused loudly rather than silently trusted. An existing copy
# may be raw (written before compression, or by the fallback) or compressed.
if [ -e "$DEST" ]; then
  if [ "$(stat -c%s "$DEST" 2>/dev/null)" = "$(stat -c%s "$SRC")" ]; then
    exit 0
  fi
  echo "osiris_archive_wal: $DEST exists with a DIFFERENT size than $SRC, refusing to overwrite" >&2
  exit 1
fi
if [ -e "$DEST.zst" ]; then
  if zstd -dcq "$DEST.zst" 2>/dev/null | cmp -s - "$SRC"; then
    exit 0
  fi
  echo "osiris_archive_wal: $DEST.zst exists but does not match $SRC, refusing to overwrite" >&2
  exit 1
fi

if [ "$COMPRESS" = 1 ]; then
  if zstd -q -T1 -3 -f -o "$DEST.zst.tmp" "$SRC" \
      && zstd -dcq "$DEST.zst.tmp" | cmp -s - "$SRC"; then
    mv "$DEST.zst.tmp" "$DEST.zst"
    exit 0
  fi
  rm -f "$DEST.zst.tmp"
  echo "osiris_archive_wal: compressing $NAME failed or did not verify, archiving it raw" >&2
fi

cp "$SRC" "$DEST.tmp" && mv "$DEST.tmp" "$DEST"

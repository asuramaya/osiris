<!-- topic: operations -->

# Backup, the vault, and offload targets

Osiris keeps its own durable record on two layers: the **vault** — a laptop-local hot path
that's always there — and **offload targets** — copies of that same data sent somewhere
else, which may or may not be reachable at any given moment (a USB drive that's only
plugged in sometimes, a NAS only visible on the home LAN). This page covers both, plus the
`osiris backup-status`/`osiris backup-settings` doors that read and write them.

## The vault (the laptop hot path)

Everything below writes into `$OSIRIS_VAULT` (default `~/osiris-vault`), on a timer, with no
operator action required once the units are installed (`osiris deploy` ships them all):

| What | Schedule | Unit |
|------|----------|------|
| Postgres dump (`pg_dump -Fc`, compressed custom format) | every 6h (`04,10,16,22:30`) | `osiris-backup.timer` |
| Weekly base backup (`pg_basebackup`, for point-in-time recovery) | Sunday 02:00 | `osiris-base-backup.timer` |
| Prune ladder — mail the dry-run plan | Saturday 03:00 | `osiris-prune-manifest.timer` |
| Prune ladder — actually delete, if the manifest is clear | Sunday 03:00 | `osiris-prune-apply.timer` |
| Weekly preflight (includes a full local restore drill) | Monday 05:00 | `osiris-preflight.timer` |

The base backup runs an hour before the prune-apply pass on Sundays, on purpose — the
prune ladder's own WAL-segment retention needs a fresh base backup to anchor against before
it decides what's safe to discard.

Each dump also refreshes a full `git bundle create --all` of the repo into the vault, and a
hardlinked (or copied) local cache of the last 4 dumps stays in `backups/` for fast access
without touching the vault. Completed WAL segments are pulled out of the database
container's own archive directory into `$OSIRIS_VAULT/wal_archive` once safely captured.

### The prune ladder (GFS thinning)

A classic grandfather-father-son schedule, applied identically to dumps (in both
`backups/` and the vault) and to `<vault>/basebackups/`:

- **Last 48h** — every dump survives whole, nothing thinned.
- **48h – 30 days** — thins to one per calendar day.
- **30 days – 1 year** — thins to one per calendar week.
- **Beyond 1 year** — thins to one per calendar month, forever.

Within any bucket, the newest survives. **Dry-run is always the default** — nothing is ever
deleted by a timer without a human's own word given in advance: the Saturday manifest timer
mails the plan to the operator's desk as a decision brief; the Sunday apply timer only
actually deletes if that brief is at least 20 hours old and was never dimmed. Dimming the
brief (before Sunday) is exactly how you stop that week's apply — an ordinary "no news is
consent" cron, made safe by requiring the "no news" to be a real elapsed window, not an
instant default. See [`CLI.md`](CLI.md#scripts_osiris_prune_ladderpy---manifest--apply-if-clear--apply--the-retention-ladder)
for running the same tool by hand.

## Offload targets

An offload target is a copy of the vault's data sent somewhere else — a second drive, a NAS
share, a cloud bucket via restic — tracked as one row in the `backup.offload_targets`
setting:

```json
{"name": "nas", "kind": "restic", "path_or_url": "sftp:nas.local:/backups/osiris",
 "expected_mountpoint": null, "schedule": "*:0/15", "enabled": true}
```

| Field | Meaning |
|-------|---------|
| `name` | a stable handle you address the target by everywhere else (unique) |
| `kind` | `local` (a mounted drive) or `restic` (a restic repository, over sftp/s3/rest/…) |
| `path_or_url` | a filesystem path (`local`) or a restic repository URL (`restic`) |
| `expected_mountpoint` | **required** for `local` (the mountpoint the presence check looks for); must be **absent** for `restic` (a URL names its own reachability, no local mount concept applies) |
| `schedule` | an `OnCalendar=`-shaped string, informational (the actual runner ticks on its own timer, see below — this field documents intent, doesn't itself schedule anything separately) |
| `enabled` | whether the runner should ever touch this target at all |

**Presence means something different per kind, on purpose.** A `local` target gets a real,
live check — `findmnt` against `expected_mountpoint` — because whether a USB drive is
plugged in right now is knowable without touching the network. A `restic` target's presence
is deliberately left unchecked at the settings-read layer: reachability over a network is a
fact this door never probes just by reading your configuration; you find out whether a
restic target is actually reachable by watching whether it's actually offloading (see
Receipts, below), not by a live ping baked into every settings read.

`osiris backup-settings write`'s own validator still checks a restic URL's **shape** at
write time (no network call, ever) — it has to start with a scheme restic actually
understands (`s3:`, `b2:`, `sftp:`, `rest:`, `rest:http:`, `rest:https:`, `swift:`,
`azure:`, `gs:`, `rclone:`) or be a plain local/relative path. A malformed URL is refused
before it's ever saved, well before the runner would try and fail against it.

`offload_targets` is the current, canonical shape — it replaces the older
`offbox_repositories` field (still readable for one release, never written to by new
tooling; a box with only `offbox_repositories` set gets `offload_targets` synthesized
automatically on read, as `kind='restic'` rows with `enabled=true`).

## The opportunistic offload runner

```bash
osiris offload-runner tick [--vault PATH] [--json]
```

Runs every **15 minutes** via `osiris-offload.timer` (also safe to run by hand any time).
Per enabled target:

- **`local`, not present right now** (drive unplugged) — skipped silently, no error, no
  receipt written. This is the ordinary, expected state for an intermittent target, not a
  failure.
- **`local`, present** or **`restic`** (always attempted — no presence gate) — runs a
  restic backup: `restic init` first if the repository looks uninitialized (a plain probe,
  `restic snapshots`, tells the two cases apart), then `restic backup <vault> --exclude=*.tmp`.

Every attempt — success or failure — writes a small JSON receipt keyed by target name to
`~/.local/state/osiris/offload_receipts.json`:

```json
// success
{"last_successful_offload": "2026-09-23T04:00:00Z", "last_attempt_at": "2026-09-23T04:00:00Z", "last_error": null}
// failure — a prior success is never overwritten by a later failure
{"last_attempt_at": "2026-09-23T04:15:00Z", "last_error": "restic backup failed: repository locked"}
```

A failed tick never clobbers an earlier `last_successful_offload` — the receipt merges
fields rather than replacing the record, so "last time this actually worked" always stays
visible even through a string of later failures. `osiris backup-status` folds these
receipts straight into its own `offbox` section (see below), so `last_successful_offload`
per target is always one command away.

The restic password (a *separate* credential from the soul key — see [`KEYS.md`](KEYS.md))
comes from `osiris restic-key init`. If it's missing entirely, the whole tick refuses with
one clear error rather than failing per-target N times:

```
osiris offload-runner tick: no restic repository password found — run `osiris restic-key
init` once, by hand, in your own terminal
```

## `osiris backup-status [--vault P] [--backups P] [--json]`

The single live-health read — everything above, in one call:

```json
{
  "as_of": "...",
  "timers": [ /* 5 entries: dumps, base backups, prune-manifest, prune-apply, preflight —
                each with its shipped schedule, any operator override, and live systemd state */ ],
  "vault": { "dumps": {"count": N, "newest_at": "...", "newest_size_bytes": N},
             "base_backups": {"count": N, "newest_at": "..."},
             "wal_segments_kept": N, "wal_segments_prunable": N },
  "disk": { "free_bytes": N, "free_gb": N, "headroom_ok": true },
  "ladder": { "hot_window_hours": 48, "daily_window_days": 30, "weekly_window_days": 365 },
  "prune_manifest": { "clear_to_apply": true, "reason": "..." },
  "pitr_drill": { "wired_to_a_timer": false,
                   "note": "scripts/osiris_pitr_drill.py — manual-only today, no receipt persisted" },
  "offbox": { "wired": true, "offload_targets": [ /* + presence + receipt fields, merged */ ] }
}
```

Each section degrades independently to `{"error": "..."}` on its own failure — a vault
that's unreachable doesn't blank the timer or ladder sections too. This is the same
`backup_status` composition Function the console's Settings pane (Backup & Offload section)
calls, called directly here so the CLI can override `--vault`/`--backups` for a test or a
non-standard layout — no MCP tool wraps that override, so this is a CLI-side call over the
Function rather than the generic `composition` door.

## `osiris backup-settings <get|write> ...`

`get` needs no authority — anyone can read the live configuration. `write` needs either
operator authority (a raw terminal call from the operator carries it directly) or a
standing `--ruling` naming `'backup_settings'`, and always needs `--because` (a bare write
with no reason is refused).

```bash
# change the vault location
osiris backup-settings write --vault-path /mnt/backup-vault --because "moved to the new NAS mount"

# add a local, intermittent target (the 8 TB drive)
osiris backup-settings write \
  --offload-add big-drive --offload-kind local --offload-target /mnt/big-drive/osiris \
  --offload-mountpoint /mnt/big-drive --offload-schedule "*:0/15" \
  --because "adding the 8 TB drive as an offload target"

# add a restic target reached over the LAN
osiris backup-settings write \
  --offload-add nas --offload-kind restic --offload-target "sftp:nas.local:/backups/osiris" \
  --offload-schedule "*:0/15" --because "NAS restic target"

# drop a target
osiris backup-settings write --offload-remove big-drive --because "retiring the old drive"

# reschedule just one timer, leaving the rest untouched
osiris backup-settings write --timer osiris-backup.timer="*-*-* 00,06,12,18:00:00" \
  --because "shifting the dump schedule"
```

`--offload-add` always needs `--offload-kind` and `--offload-target`, plus
`--offload-schedule` (the schema requires every target to carry a non-empty schedule); a
`local` target additionally needs `--offload-mountpoint`. `--offload-add`/`--offload-remove`
read the current `offload_targets`, change only the one named target, and write the whole
list back — every other target is untouched. `--timer UNIT=ONCALENDAR` (repeatable) merges
into the current `timer_schedules`, touching only the named unit; the raw
`--timer-schedules`/`--offbox-repositories` JSON flags remain for a full-replace scripted
write, and `--offbox-repositories` is explicitly deprecated in favor of the `--offload-*`
flags.

## The restic password

See [`KEYS.md`](KEYS.md#the-restic-password) for the full custody mechanism — `osiris
restic-key init`/`status`. It's a completely separate secret from the soul-store encryption
key: it protects the **restic repository's own encryption**, nothing about the vault on
disk.

## Restore drills

Two, checking different things:

- **Off-box restore drill** — `osiris soul-key restore-drill [--repo-url URL]` (yes, filed
  under the `soul-key` door — see [`KEYS.md`](KEYS.md)) actually proves a restic repository
  restores: `restic check`, then a real `restic restore latest` into a scratch directory,
  then confirms real files landed. A clean `restic check` alone is **not** treated as proof
  a backup restores — only real restored content is. Without `--repo-url`, drills every URL
  in `backup.offbox_repositories`.
- **Local (PITR) restore drill** — `scripts/osiris_pitr_drill.py`, run as part of the weekly
  `osiris-preflight.timer` (always with `--drill`), against the newest base backup in the
  vault. `osiris backup-status`'s `pitr_drill` section currently reports this as
  `wired_to_a_timer: false` — it does run weekly, but (unlike the offload runner) has no
  persisted receipt yet, so there's nothing for `backup-status` to read back and confirm by.

## The Settings pane (Backup & Offload section)

The console's Settings pane (`Ctrl+K` → *Settings*, or the header gear) has a **Backup &
Offload** section combining an editable offload-targets panel with a read-only view of
`osiris backup-status` — a timers table (flagging any schedule that's been changed in
settings but not yet redeployed) and a targets table (kind, presence, last successful
offload, last error). See [`REFERENCE.md`](REFERENCE.md#the-consoles-settings-pane) for the
full pane layout.

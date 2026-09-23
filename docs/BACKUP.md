<!-- topic: operations -->

# Backup, the vault, and offload targets

Osiris keeps its own durable record on two layers: the vault, a local copy on the machine
running Osiris that is always there, and offload targets, copies of that same data sent
somewhere else that may or may not be reachable at any given moment. An offload target might
be a USB drive that's only plugged in sometimes, or a network drive only visible on the home
network. This page covers both, plus the `osiris backup-status`/`osiris backup-settings`
commands that read and write them.

## The vault

Everything below writes into `$OSIRIS_VAULT` (default `~/osiris-vault`), on a schedule, with
no manual action required once the underlying services are installed (`osiris deploy` ships
them all):

| What | Schedule | Unit |
|------|----------|------|
| Postgres dump (`pg_dump -Fc`, compressed custom format) | every 6 hours (`04,10,16,22:30`) | `osiris-backup.timer` |
| Weekly base backup (`pg_basebackup`, for point-in-time recovery) | Sunday 02:00 | `osiris-base-backup.timer` |
| Prune schedule: mail the dry-run plan | Saturday 03:00 | `osiris-prune-manifest.timer` |
| Prune schedule: actually delete, if the plan is clear | Sunday 03:00 | `osiris-prune-apply.timer` |
| Weekly preflight check (includes a full local restore drill) | Monday 05:00 | `osiris-preflight.timer` |

The base backup runs an hour before the prune-apply pass on Sundays, on purpose. The prune
schedule's own retention of write-ahead log segments needs a fresh base backup to anchor
against before it decides what is safe to discard.

Each dump also refreshes a full copy of the repository's version history into the vault, and
a local cache of the last 4 dumps stays in `backups/` for fast access without touching the
vault. Completed write-ahead log segments are pulled out of the database container's own
archive directory into `$OSIRIS_VAULT/wal_archive` once safely captured.

### The prune schedule

A classic thinning schedule, applied identically to dumps (in both `backups/` and the
vault) and to `<vault>/basebackups/`:

- **Last 48 hours**: every dump survives whole, nothing thinned.
- **48 hours to 30 days**: thins to one per calendar day.
- **30 days to 1 year**: thins to one per calendar week.
- **Beyond 1 year**: thins to one per calendar month, forever.

Within any bucket, the newest survives. A dry run is always the default. Nothing is ever
deleted on a schedule without a human's own word given in advance. The Saturday planning
step mails the plan as a decision to review; the Sunday apply step only actually deletes if
that plan is at least 20 hours old and was never marked stale. Marking it stale before
Sunday is exactly how you stop that week's deletion pass, an ordinary "no news is consent"
pattern made safe by requiring the "no news" to be a real elapsed window, not an instant
default. See [`CLI.md`](CLI.md) for running the same tool by hand.

## Offload targets

An offload target is a copy of the vault's data sent somewhere else: a second drive, a
network share, a cloud bucket reached through restic. Each one is tracked as one row in the
`backup.offload_targets` setting:

```json
{"name": "nas", "kind": "restic", "path_or_url": "sftp:nas.local:/backups/osiris",
 "expected_mountpoint": null, "schedule": "*:0/15", "enabled": true}
```

| Field | Meaning |
|-------|---------|
| `name` | a stable label you address the target by everywhere else (must be unique) |
| `kind` | `local` (a mounted drive) or `restic` (a restic repository, reachable over sftp, s3, or a similar transport) |
| `path_or_url` | a filesystem path (`local`) or a restic repository address (`restic`) |
| `expected_mountpoint` | required for `local`: the mount point the presence check looks for. Must be absent for `restic`: an address names its own reachability, and there's no local mount point concept to check |
| `schedule` | an informational schedule string. The actual runner ticks on its own fixed schedule, described below; this field only documents intent |
| `enabled` | whether the runner should ever touch this target at all |

Presence means something different per kind, on purpose. A `local` target gets a real, live
check against `expected_mountpoint`, because whether a USB drive is plugged in right now is
knowable without touching the network. A `restic` target's presence is deliberately left
unchecked when settings are read: reachability over a network is not something this
component probes just by reading your configuration. You find out whether a restic target is
actually reachable by watching whether it is actually receiving backups (see the section on
results below), not through a live network check baked into every settings read.

`osiris backup-settings write` still checks a restic address's shape when you save it, with
no network call ever made. It has to start with a scheme restic actually understands (`s3:`,
`b2:`, `sftp:`, `rest:`, `rest:http:`, `rest:https:`, `swift:`, `azure:`, `gs:`, `rclone:`)
or be a plain local or relative path. A malformed address is refused before it's ever saved,
well before the runner would try and fail against it.

`offload_targets` is the current, canonical shape. It replaces an older field kept readable
for one more release but no longer written to by new code: a machine with only the old field
set gets `offload_targets` filled in automatically when read.

## The opportunistic offload runner

```bash
osiris offload-runner tick [--vault PATH] [--json]
```

Runs every 15 minutes on its own schedule (also safe to run by hand any time). For each
enabled target:

- A `local` target that is not present right now, for example a drive that's unplugged, is
  skipped silently. No error, no result recorded. This is the ordinary, expected state for
  an intermittent target, not a failure.
- A `local` target that is present, or any `restic` target (always attempted, with no
  presence check first), gets a real restic backup run. If the repository looks
  uninitialized, it is set up first, then the backup itself runs.

Every attempt, success or failure, writes a small result record keyed by target name to
`~/.local/state/osiris/offload_receipts.json`:

```json
// success
{"last_successful_offload": "2026-09-23T04:00:00Z", "last_attempt_at": "2026-09-23T04:00:00Z", "last_error": null}
// failure: a prior success is never overwritten by a later failure
{"last_attempt_at": "2026-09-23T04:15:00Z", "last_error": "restic backup failed: repository locked"}
```

A failed attempt never erases an earlier successful one. Each record merges new fields in
rather than replacing the whole thing, so the last time a target actually worked always stays
visible, even through a run of later failures. `osiris backup-status` reads these records
directly, so the last successful offload for any target is always one command away.

The restic password, a separate credential from the soul key described in
[`KEYS.md`](KEYS.md), comes from `osiris restic-key init`. If it's missing entirely, the
whole run refuses with one clear error rather than failing separately for every target:

```
osiris offload-runner tick: no restic repository password found, run `osiris restic-key
init` once, by hand, in your own terminal
```

## `osiris backup-status [--vault P] [--backups P] [--json]`

A single live health check covering everything above, in one call:

```json
{
  "as_of": "...",
  "timers": [ /* 5 entries: dumps, base backups, prune plan, prune apply, preflight,
                each with its shipped schedule, any manual override, and live service state */ ],
  "vault": { "dumps": {"count": N, "newest_at": "...", "newest_size_bytes": N},
             "base_backups": {"count": N, "newest_at": "..."},
             "wal_segments_kept": N, "wal_segments_prunable": N },
  "disk": { "free_bytes": N, "free_gb": N, "headroom_ok": true },
  "ladder": { "hot_window_hours": 48, "daily_window_days": 30, "weekly_window_days": 365 },
  "prune_manifest": { "clear_to_apply": true, "reason": "..." },
  "pitr_drill": { "wired_to_a_timer": false,
                   "note": "manual only today, no result persisted" },
  "offbox": { "wired": true, "offload_targets": [ /* with presence and result fields merged in */ ] }
}
```

Each section degrades independently to a plain error notice on its own failure. A vault
that's unreachable doesn't blank the timer or prune sections too. This is the same
information the console's Settings pane shows in its Backup and Offload section, called
directly here so the command line can override `--vault`/`--backups` for a test or a
non-standard setup.

## `osiris backup-settings <get|write> ...`

`get` needs no special permission: anyone can read the live configuration. `write` needs
either direct administrator permission or a standing rule naming `'backup_settings'`, and
always needs `--because` (a bare write with no stated reason is refused).

```bash
# change the vault location
osiris backup-settings write --vault-path /mnt/backup-vault --because "moved to the new NAS mount"

# add a local, intermittent target (a large external drive)
osiris backup-settings write \
  --offload-add big-drive --offload-kind local --offload-target /mnt/big-drive/osiris \
  --offload-mountpoint /mnt/big-drive --offload-schedule "*:0/15" \
  --because "adding a large drive as an offload target"

# add a restic target reached over the local network
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
`--offload-schedule` (every target must carry a non-empty schedule). A `local` target
additionally needs `--offload-mountpoint`. `--offload-add`/`--offload-remove` read the
current list of targets, change only the one named target, and write the whole list back:
every other target is left untouched. `--timer UNIT=ONCALENDAR` (repeatable) merges into the
current timer configuration, touching only the named unit. The raw
`--timer-schedules`/`--offbox-repositories` flags remain for a full-replace scripted write,
and `--offbox-repositories` is deprecated in favor of the `--offload-*` flags above.

## The restic password

See [`KEYS.md`](KEYS.md#the-restic-password) for the full custody mechanism behind `osiris
restic-key init`/`status`. It's a completely separate secret from the soul-store encryption
key. It protects the restic repository's own encryption, nothing about the vault on disk.

## Restore drills

Two checks exist, and they check different things:

- **Off-box restore drill**: `osiris soul-key restore-drill [--repo-url URL]` (yes, this
  lives under the `soul-key` command; see [`KEYS.md`](KEYS.md)) actually proves a restic
  repository restores. It runs a full integrity check, then a real restore into a scratch
  directory, then confirms real files landed. A clean integrity check alone is not treated
  as proof a backup restores: only real restored content is. Without `--repo-url`, it drills
  every configured off-box repository.
- **Local restore drill**: run as part of the weekly preflight check (always with the full
  drill enabled), against the newest base backup in the vault. `osiris backup-status`
  currently reports this drill as not yet tied to a persisted result: it does run weekly,
  but unlike the offload runner, there's nothing yet for `backup-status` to read back and
  confirm by.

## The Settings pane (Backup and Offload section)

The console's Settings pane, opened from the command palette or the header's gear icon, has
a Backup and Offload section combining an editable offload-targets panel with a read-only
view of `osiris backup-status`: a timers table flagging any schedule that's been changed in
settings but not yet redeployed, and a targets table showing kind, presence, last successful
offload, and last error. See [`REFERENCE.md`](REFERENCE.md#the-consoles-settings-pane) for
the full pane layout.

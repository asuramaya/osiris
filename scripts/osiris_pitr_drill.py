"""THE PITR DRILL (the backup-verification lane): a backup that has never been restored is
only a hope, not a proven backup, the same principle scripts/osiris_preflight.py's own
`drill()` already holds for plain pg_dumps, extended here to the base-backup-plus-WAL pair.
Restores the newest base backup into a scratch container, replays archived WAL up to a
chosen point in time via ordinary Postgres archive recovery (a `recovery.signal` file plus
`restore_command`, recovery_target_time, and recovery_target_action='promote' so the drill
container finishes recovery and becomes an ordinary queryable server instead of sitting
paused), and proves a row written AFTER the base backup completed is present in the
restored copy: the one thing a base backup alone, with no WAL replayed on top of it, could
never show.

WAL SOURCE, GATHERED READ-ONLY (`_gather_wal_segments`): archived segments live in two
possible places at drill time, already pulled into the vault by osiris_backup.sh's own WAL
section, or still staged inside the live container waiting for that timer's next run. This
copies (never deletes, never touches the live container's own staging) whatever is
available from both into one scratch directory the drill container's `restore_command`
reads from, a plain `cp`, the standard textbook shape, no docker-in-docker needed inside
the drill container itself.

Orchestration (`run_drill`) is proven by a real run against real data, not mocked, the same
discipline osiris_archive_wal.sh's own WAL-writing half was verified with, directly inside
the live container. The config-text builder (`postgresql_auto_conf_pitr`) is pure and
tested directly.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tarfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

CONTAINER = "osiris-pg"
DRILL_NAME = "osiris-pitr-drill"
VAULT_WAL_DIR = Path.home() / "osiris-vault" / "wal_archive"

# The longest the drill waits for archive recovery to finish replaying. Replay time grows
# with the age of the base backup (every archived segment since it is replayed), so this is
# generous but still a bound: a recovery that has not finished by then is a finding.
RECOVERY_WAIT_SECS = 4 * 3600
RECOVERY_POLL_SECS = 10

# The scratch postgres must never inherit production tuning: the base backup carries the
# live postgresql.auto.conf (autotune sets shared_buffers near a third of the box's RAM),
# and a recovery server started with that on top of a multi-gigabyte replay is what pushed
# a drill's file-backed memory past 30G. Command-line settings outrank the config file.
# max_connections is NOT shrunk: archive recovery refuses to start when it is lower than the
# primary's (72 here), and the other settings recovery pins to the primary's values
# (workers, WAL senders, locks, prepared transactions) already match the defaults.
SCRATCH_POSTGRES_ARGS = (
    "-c", "shared_buffers=512MB", "-c", "maintenance_work_mem=256MB",
    "-c", "max_connections=72", "-c", "max_wal_size=4GB", "-c", "autovacuum=off",
)
SCRATCH_MEMORY = "6g"
SOUL_SAMPLE_TIMEOUT_SECS = 120


def postgresql_auto_conf_pitr(
    restore_command: str, target_time: str | None, target_action: str = "promote",
) -> str:
    """The lines a restored PGDATA needs appended to its own postgresql.auto.conf to
    perform archive recovery. `target_time=None` (the default drill mode) sets NO
    recovery target at all: Postgres then just replays every WAL segment
    `restore_command` can still find and promotes the moment `restore_command` first
    fails to produce the next one, i.e. it recovers to the latest point the archive can
    actually prove, which is what confirming "a row written after the base backup is
    present" needs. A caller-supplied `target_time` asks for an EXACT point instead, and
    can legitimately fail if that point turns out to be past what's archived (a real
    finding, not a bug in the drill): Postgres refuses to promote past a target it
    cannot reach. `target_action='promote'` (not the default 'pause') either way, so
    the drill container finishes on its own and becomes an ordinary queryable server;
    a caller then just polls pg_isready, exactly like osiris_preflight.py's own
    plain-dump `drill()`."""
    conf = f"restore_command = '{restore_command}'\n"
    if target_time is not None:
        conf += f"recovery_target_time = '{target_time}'\n"
        conf += f"recovery_target_action = '{target_action}'\n"
    return conf


def _gather_wal_segments(container: str, vault_wal_dir: Path, scratch_wal_dir: Path) -> int:
    """Stage the WAL the drill container's `restore_command` can read. Segments already
    pulled into the vault are NOT copied: the vault directory is mounted read-only into
    the drill container and read in place, because copying a multi-week archive (tens of
    gigabytes) wrote it all a second time and charged the whole read and write to the
    caller's memory as file cache. Only what is still staged inside the live container,
    waiting for the backup timer's next pull, is copied here (a handful of segments).
    Read-only against both sources. Returns how many segments the drill can see in total,
    vault plus staged."""
    scratch_wal_dir.mkdir(parents=True, exist_ok=True)
    listing = subprocess.run(
        ["docker", "exec", container, "ls", "-1", "/var/lib/postgresql/data/wal_archive"],
        capture_output=True, text=True, timeout=30)
    for seg in listing.stdout.split():
        dest = scratch_wal_dir / seg
        if dest.exists() or (vault_wal_dir / seg).is_file():
            continue
        with open(dest, "wb") as fh:
            subprocess.run(
                ["docker", "exec", container, "cat",
                 f"/var/lib/postgresql/data/wal_archive/{seg}"],
                stdout=fh, timeout=60, check=True)
    in_vault = sum(1 for f in vault_wal_dir.iterdir() if f.is_file()) \
        if vault_wal_dir.is_dir() else 0
    return in_vault + len(list(scratch_wal_dir.iterdir()))


def _wait_for_recovery_end(drill_name: str) -> str | None:
    """Block until the drill server has finished archive recovery and promoted, or return
    a failure string. `pg_isready` is NOT that signal: a server in archive recovery
    accepts read-only connections as soon as it reaches a consistent state, long before
    the rest of the archive is replayed, so a check made the moment it answers reads a
    half-replayed copy and reports a marker written after the base backup as missing (the
    shape that failed two weekly runs in a row). Only `pg_is_in_recovery() = false` means
    every available segment has been replayed."""
    deadline = time.monotonic() + RECOVERY_WAIT_SECS
    last = ""
    while time.monotonic() < deadline:
        r = subprocess.run(
            ["docker", "exec", drill_name, "psql", "-U", "osiris", "-d", "postgres", "-tAc",
             "SELECT pg_is_in_recovery(), COALESCE(pg_last_wal_replay_lsn()::text, '')"],
            capture_output=True, text=True, timeout=30)
        out = r.stdout.strip()
        if r.returncode == 0 and out.startswith("f"):
            return None
        if out:
            last = out
        status = subprocess.run(["docker", "inspect", "-f", "{{.State.Status}}", drill_name],
                                capture_output=True, text=True, timeout=10).stdout.strip()
        if status and status != "running":
            logs = subprocess.run(["docker", "logs", "--tail", "40", drill_name],
                                  capture_output=True, text=True, timeout=10)
            return (f"drill container stopped during recovery ({status}):\n"
                    f"{logs.stdout}\n{logs.stderr}")
        time.sleep(RECOVERY_POLL_SECS)
    return (f"archive recovery had not finished after {RECOVERY_WAIT_SECS}s "
            f"(last status: {last or 'server not yet accepting connections'}): replay is "
            "stalled or the archive is too long to replay in the allowed time")


def _wait_for_archive(container: str, walfile: str, timeout: float = 180.0) -> bool:
    """True once the live archiver has archived `walfile` (segment names are fixed-width
    hex, so a plain string comparison orders them). `pg_switch_wal()` only asks for the
    segment to be closed; the archiver copies it on its own schedule, so a drill that
    gathers the archive right after switching can miss the very segment holding its
    marker."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        out = subprocess.run(
            ["docker", "exec", container, "psql", "-U", "osiris", "-d", "osiris", "-tAc",
             f"SELECT COALESCE(last_archived_wal >= '{walfile}', false) FROM pg_stat_archiver"],
            capture_output=True, text=True, timeout=30).stdout.strip()
        if out == "t":
            return True
        time.sleep(2)
    return False


def _soul_encryption_state() -> str:
    """This box's own record of soul-store encryption: `no_key` (no key was ever set up
    here), `pending`, `running`, `complete`, or `unknown` when the record cannot be read.
    Only `complete` makes "no sampled row is encrypted" a hard failure."""
    try:
        from src.ingest.soul_crypto import soul_key_status
        from src.orchestrator.soul_encrypt_progress import read_progress, shape_encryption

        return str(shape_encryption(
            read_progress(), key_present=bool(soul_key_status()["present"]))["state"])
    except Exception:  # noqa: BLE001, an unreadable record is unproven, never a crash
        return "unknown"


def _soul_round_trip_check(container: str) -> str | None:
    """PROVE DECRYPTION, NOT PRESENCE: the marker-object check above only proves the
    restored copy has ROWS. It says nothing about whether the CURRENT key on this box can
    actually open the soul store's own encrypted content, which is the one thing a restore
    drill exists to prove that a plain "the data restored" check cannot. Shared by this
    module's own `run_drill` and scripts/osiris_preflight.py's plain-dump `drill()`
    (imported from here, never duplicated: this module has no import FROM
    osiris_preflight, so this is the direction that avoids a cycle). Picks ONE real
    `soul_lines` row from the restored scratch container (`docker exec psql`, the SAME
    query shape every other check in either drill already makes, so no new port, no new
    connectivity into the scratch container needed) and decrypts it on THIS box with the
    currently-configured `get_soul_fernet()`. None (pass) when the restored copy has no
    soul_lines rows at all: a soul-store-empty snapshot (a fresh install, or a dump taken
    before the first transcript was ever ingested) is not a failure of THIS specific
    check, the same "not activated here is not a failure" principle this module's own
    `drill_pitr` caller already holds for a missing base backup."""
    from cryptography.fernet import InvalidToken
    from src.ingest.soul_crypto import get_soul_fernet, is_encrypted

    # SAMPLE SEVERAL, SKIP LEGACY PLAINTEXT: a single random row can land on a
    # not-yet-migrated row (encrypt_existing_soul_lines's own backward pass, run
    # separately); decrypting THAT would fail for a reason that has nothing to do with
    # whether the currently-configured key actually opens real ciphertext, a false
    # "round-trip FAILED" this check exists to never report. Widens the sample instead
    # of narrowing the proof: the first ENCRYPTED row found is the one this checks.
    # PAGE SAMPLING, NOT `ORDER BY random()`: sorting by random() reads every row's
    # `raw_line` (the table is millions of rows and ~10 GB), which blew a 30 s limit on a
    # freshly restored, cold-cache scratch server the first time a drill ever got this far.
    # TABLESAMPLE SYSTEM reads a few hundred random pages and stays spread across the whole
    # table (a plain LIMIT would only ever see the oldest rows, which are the legacy
    # plaintext ones). A table too small to yield any sampled page falls back to a plain
    # LIMIT, which on a small table reads all of it anyway.
    def _sample(sql: str) -> list[bytes]:
        out = subprocess.run(
            ["docker", "exec", container, "psql", "-U", "osiris", "-d", "osiris", "-tAc", sql],
            capture_output=True, text=True, timeout=SOUL_SAMPLE_TIMEOUT_SECS)
        return [bytes.fromhex(line) for line in out.stdout.splitlines() if line.strip()]

    # HEX, NOT BASE64: Postgres' encode(..., 'base64') breaks its output into 76-character
    # lines, so a row longer than 57 bytes spans several lines and decoding each line on its
    # own hands the decryptor a fragment, which can never decrypt: every real row read as a
    # "round-trip FAILED" even though the live data decrypts fine (600 of 600 sampled live
    # rows did). Hex is one line per row.
    raws = _sample("SELECT encode(raw_line, 'hex') FROM soul_lines "
                   "TABLESAMPLE SYSTEM (0.02) LIMIT 50") \
        or _sample("SELECT encode(raw_line, 'hex') FROM soul_lines LIMIT 50")
    encrypted = next((r for r in raws if is_encrypted(r)), None)
    if encrypted is None:
        if not raws:
            return None  # an empty table: nothing was ever stored, so nothing to prove
        # A sample with rows but none encrypted proved nothing, and a check that passes
        # having checked nothing hides the day the data stops being readable. Whether that
        # is a failure depends on whether encryption is supposed to be finished here.
        state = _soul_encryption_state()
        if state == "complete":
            return (f"soul-store round-trip proved NOTHING: sampled {len(raws)} row(s) from "
                    "the restored copy and none is encrypted, though this box's encryption "
                    "progress record says every row is encrypted. Either the restored copy "
                    "lost its encryption or the sample missed it; the key was not exercised")
        print(f"soul-store round-trip: UNPROVEN (encryption state: {state}): sampled "
              f"{len(raws)} row(s), none encrypted, so the key was not exercised. Not a "
              "failure on a box that has not finished encrypting or never set a key up")
        return None
    try:
        get_soul_fernet().decrypt(encrypted)
    except InvalidToken:
        return ("soul-store round-trip FAILED: a real row from the restored copy does "
                "not decrypt under the key currently configured here. This is a "
                "genuine decryption-proof failure, not just a presence check")
    return None


def run_drill(
    base_backup: Path, target_time: str | None, marker_canonical: str, *,
    container: str = CONTAINER, drill_name: str = DRILL_NAME,
    scratch: Path | None = None,
) -> str | None:
    """Returns a failure string, or None on success. Cleans up the drill container and
    its scratch dirs in every case (`finally`), same discipline as
    osiris_preflight.py's own `drill()`.

    NEVER THE LIVE CLUSTER, codified after the exact live incident that named this
    obligation: a manual pg_basebackup restore into a DIFFERENTLY-NAMED database
    ("osiris_drill") on the SAME live cluster still generated real WAL against
    production, backlogging the archiver. A drill's own point is to generate ZERO WAL
    against the thing being drilled. `drill_name` defaults to a name that is never
    `container`, but a caller COULD override it to collide; this refuses outright rather
    than trusting the default stays unbroken forever: `docker rm -f -v` on the live
    container name would be catastrophic, not just a WAL-backlog nuisance."""
    if drill_name == container:
        return (f"REFUSING: drill_name {drill_name!r} equals the source container "
                f"{container!r}. A drill must restore into its own separate scratch "
                "container, never the one being drilled")
    scratch = scratch or Path(f"/var/tmp/osiris-scratch/pitr-drill-{int(time.time())}")
    pgdata = scratch / "pgdata"
    wal_dir = scratch / "wal"
    try:
        pgdata.mkdir(parents=True, exist_ok=True)
        with tarfile.open(base_backup, "r:gz") as tf:
            tf.extractall(pgdata, filter="data")  # noqa: S202, our own trusted backup

        n_wal = _gather_wal_segments(container, VAULT_WAL_DIR, wal_dir)
        if n_wal == 0:
            return "no WAL segments available anywhere: cannot replay past the base backup"

        (pgdata / "recovery.signal").touch()
        conf = postgresql_auto_conf_pitr(
            f"cp {VAULT_WAL_DIR}/%f %p 2>/dev/null || cp {wal_dir}/%f %p", target_time)
        with open(pgdata / "postgresql.auto.conf", "a") as fh:
            fh.write("\n" + conf)

        subprocess.run(["docker", "rm", "-f", "-v", drill_name],
                       capture_output=True, timeout=30)
        subprocess.run(
            ["docker", "run", "-d", "--name", drill_name,
             "--memory", SCRATCH_MEMORY, "--memory-swap", SCRATCH_MEMORY,
             "-v", f"{pgdata}:/var/lib/postgresql/data",
             "-v", f"{VAULT_WAL_DIR}:{VAULT_WAL_DIR}:ro",
             "-v", f"{wal_dir}:{wal_dir}:ro",
             "-e", "POSTGRES_PASSWORD=osiris", "postgres:16", *SCRATCH_POSTGRES_ARGS],
            capture_output=True, timeout=60, check=True)

        waited = _wait_for_recovery_end(drill_name)
        if waited is not None:
            return waited

        out = subprocess.run(
            ["docker", "exec", drill_name, "psql", "-U", "osiris", "-d", "osiris", "-tc",
             f"SELECT count(*) FROM objects WHERE canonical = '{marker_canonical}'"],
            capture_output=True, text=True, timeout=30)
        n = int((out.stdout or "0").strip() or 0)
        if n < 1:
            return (f"restored copy is missing the post-base-backup marker "
                    f"({marker_canonical!r}) even after archive recovery finished "
                    f"replaying {n_wal} segment(s): the archive does not reach it")
        return _soul_round_trip_check(drill_name)
    except Exception as e:  # noqa: BLE001
        return f"PITR drill failed: {type(e).__name__}: {e}"
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", drill_name], capture_output=True, timeout=30)
        # `pgdata` was written by the postgres container as ITS OWN uid (999 inside the
        # container, an unmapped/colliding uid on the host). The host user that ran this
        # script cannot even read it, let alone rmtree it, and ignore_errors=True on that
        # call would silently leak the whole scratch tree every single drill (caught by
        # running this drill for real: three runs, three leaked multi-GB directories
        # before this fix). A throwaway root container CAN delete it (root bypasses host
        # DAC on a bind mount); reclaim it first, then rmtree the rest normally.
        subprocess.run(["docker", "run", "--rm", "-v", f"{scratch}:/scratch", "postgres:16",
                        "rm", "-rf", "/scratch/pgdata"], capture_output=True, timeout=60)
        shutil.rmtree(scratch, ignore_errors=True)


def pick_and_ensure_marker(container: str = CONTAINER) -> str | None:
    """The unattended-drill marker: the live DB's own most-recently-created object,
    read-only, no manually-authored record needed each run, since the system writes
    constantly, so there is always a fresh one. `pg_switch_wal()` is an administrative
    WAL-control call, not a graph mutation (the convention against raw SQL is about
    writes into the graph's own rows, never about calling Postgres's own control
    functions); it forces the segment holding that object's write to archive
    immediately rather than waiting for it to fill naturally, so the drill doesn't have
    to wait either. Returns None on an empty database (nothing to prove yet, not a
    failure)."""
    out = subprocess.run(
        ["docker", "exec", container, "psql", "-U", "osiris", "-d", "osiris", "-tAc",
         "SELECT canonical FROM objects ORDER BY created_at DESC LIMIT 1"],
        capture_output=True, text=True, timeout=30)
    canonical = out.stdout.strip()
    if not canonical:
        return None
    switched = subprocess.run(
        ["docker", "exec", container, "psql", "-U", "osiris", "-d", "osiris", "-tAc",
         "SELECT pg_walfile_name(pg_switch_wal() - 1)"],
        capture_output=True, text=True, timeout=30).stdout.strip()
    # the switch only CLOSES the segment holding the marker; wait for the archiver to copy
    # it, or the gather that follows can miss exactly that segment
    if switched and not _wait_for_archive(container, switched):
        print(f"osiris_pitr_drill: the live archiver had not archived {switched} within "
              "the wait, the drill may not see its own marker", file=sys.stderr)
    return canonical


def main(argv: list[str] | None = None) -> int:
    from scripts.osiris_prune_ladder import _scan

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vault", type=Path, default=Path.home() / "osiris-vault")
    parser.add_argument("--marker", default=None,
                        help="canonical of an object known to exist AFTER the base "
                             "backup completed: the drill's own pass condition. "
                             "Default: auto-pick the live DB's newest object "
                             "(pick_and_ensure_marker), so an unattended weekly run "
                             "needs no manually-authored marker.")
    parser.add_argument("--target-time", default=None,
                        help="an EXACT recovery_target_time to demand (Postgres does NOT "
                             "accept the bare word 'now' here, unlike most other "
                             "timestamp contexts). Default: no target at all, replay "
                             "every WAL segment the archive can actually produce and "
                             "promote there, the honest 'as-current-as-provable' drill.")
    args = parser.parse_args(argv)

    backups = _scan(args.vault / "basebackups")
    if not backups:
        print("osiris_pitr_drill: no base backups found, nothing to drill", file=sys.stderr)
        return 1
    newest = max(backups, key=lambda f: f.when)
    target_time = args.target_time
    marker = args.marker or pick_and_ensure_marker()
    if marker is None:
        print("osiris_pitr_drill: no objects in the live DB, nothing to prove",
              file=sys.stderr)
        return 1
    print(f"osiris_pitr_drill: restoring {newest.path} to target_time={target_time!r}, "
          f"marker={marker!r}")
    fail = run_drill(Path(newest.path), target_time, marker)
    if fail:
        print(f"PITR DRILL FAILED: {fail}", file=sys.stderr)
        return 1
    from scripts.osiris_prune_ladder import mark_base_backup_verified

    mark_base_backup_verified(Path(newest.path))
    print("PITR drill: PASS, the marker written after the base backup is present in "
          "the restored copy; the base backup is now recorded as verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())

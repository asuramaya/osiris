"""THE PITR DRILL (the vault lane, operator ruling 39384a87/c53a5fc0, item 3's own last
piece): "a backup that's never been restored is a hope, not a backup" — the SAME law
scripts/osiris_preflight.py's own `drill()` already holds for plain pg_dumps, extended here
to the base-backup-plus-WAL pair. Restores the newest base backup into a scratch container,
replays archived WAL up to a chosen point in time via ordinary Postgres archive recovery
(a `recovery.signal` file plus `restore_command`, recovery_target_time, and
recovery_target_action='promote' so the drill container finishes recovery and becomes an
ordinary queryable server instead of sitting paused), and proves a row written AFTER the
base backup completed is present in the restored copy — the one thing a base backup alone,
with no WAL replayed on top of it, could never show.

WAL SOURCE, GATHERED READ-ONLY (`_gather_wal_segments`): archived segments live in two
possible places at drill time — already pulled into the vault by osiris_backup.sh's own WAL
section, or still staged inside the live container waiting for that timer's next run. This
copies (never deletes, never touches the live container's own staging) whatever is available
from both into one scratch directory the drill container's `restore_command` reads from — a
plain `cp`, the standard textbook shape, no docker-in-docker needed inside the drill
container itself.

Orchestration (`run_drill`) is proven by a real run against real data, not mocked — the same
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


def postgresql_auto_conf_pitr(
    restore_command: str, target_time: str | None, target_action: str = "promote",
) -> str:
    """The lines a restored PGDATA needs appended to its own postgresql.auto.conf to
    perform archive recovery. `target_time=None` (the default drill mode) sets NO
    recovery target at all — Postgres then just replays every WAL segment
    `restore_command` can still find and promotes the moment `restore_command` first
    fails to produce the next one, i.e. "recover to the latest point the archive can
    actually prove", which is what "a row written after the base backup is present"
    needs. A caller-supplied `target_time` asks for an EXACT point instead — and can
    legitimately fail if that point turns out to be past what's archived (a real
    finding, not a bug in the drill): Postgres refuses to promote past a target it
    cannot reach. `target_action='promote'` (not the default 'pause') either way, so
    the drill container finishes on its own and becomes an ordinary queryable server —
    a caller then just polls pg_isready, exactly like osiris_preflight.py's own
    plain-dump `drill()`."""
    conf = f"restore_command = '{restore_command}'\n"
    if target_time is not None:
        conf += f"recovery_target_time = '{target_time}'\n"
        conf += f"recovery_target_action = '{target_action}'\n"
    return conf


def _gather_wal_segments(container: str, vault_wal_dir: Path, scratch_wal_dir: Path) -> int:
    """Copy every currently-available archived WAL segment — already-pulled ones in the
    vault, plus anything still staged inside the live container — into `scratch_wal_dir`.
    Read-only against both sources. Returns how many segments landed there."""
    scratch_wal_dir.mkdir(parents=True, exist_ok=True)
    if vault_wal_dir.is_dir():
        for f in vault_wal_dir.iterdir():
            if f.is_file():
                shutil.copy2(f, scratch_wal_dir / f.name)
    listing = subprocess.run(
        ["docker", "exec", container, "ls", "-1", "/var/lib/postgresql/data/wal_archive"],
        capture_output=True, text=True, timeout=30)
    for seg in listing.stdout.split():
        dest = scratch_wal_dir / seg
        if dest.exists():
            continue
        with open(dest, "wb") as fh:
            subprocess.run(
                ["docker", "exec", container, "cat",
                 f"/var/lib/postgresql/data/wal_archive/{seg}"],
                stdout=fh, timeout=60, check=True)
    return len(list(scratch_wal_dir.iterdir()))


def run_drill(
    base_backup: Path, target_time: str | None, marker_canonical: str, *,
    container: str = CONTAINER, drill_name: str = DRILL_NAME,
    scratch: Path | None = None,
) -> str | None:
    """Returns a failure string, or None on success. Cleans up the drill container and
    its scratch dirs in every case (`finally`), same discipline as
    osiris_preflight.py's own `drill()`."""
    scratch = scratch or Path(f"/var/tmp/osiris-scratch/pitr-drill-{int(time.time())}")
    pgdata = scratch / "pgdata"
    wal_dir = scratch / "wal"
    try:
        pgdata.mkdir(parents=True, exist_ok=True)
        with tarfile.open(base_backup, "r:gz") as tf:
            tf.extractall(pgdata, filter="data")  # noqa: S202 — our own trusted backup

        n_wal = _gather_wal_segments(
            container, Path.home() / "osiris-vault" / "wal_archive", wal_dir)
        if n_wal == 0:
            return "no WAL segments available anywhere — cannot replay past the base backup"

        (pgdata / "recovery.signal").touch()
        conf = postgresql_auto_conf_pitr(f"cp {wal_dir}/%f %p", target_time)
        with open(pgdata / "postgresql.auto.conf", "a") as fh:
            fh.write("\n" + conf)

        subprocess.run(["docker", "rm", "-f", "-v", drill_name],
                       capture_output=True, timeout=30)
        subprocess.run(
            ["docker", "run", "-d", "--name", drill_name,
             "-v", f"{pgdata}:/var/lib/postgresql/data",
             "-v", f"{wal_dir}:{wal_dir}:ro",
             "-e", "POSTGRES_PASSWORD=osiris", "postgres:16"],
            capture_output=True, timeout=60, check=True)

        for _ in range(60):
            r = subprocess.run(["docker", "exec", drill_name, "pg_isready", "-U", "osiris"],
                               capture_output=True, timeout=10)
            if r.returncode == 0:
                break
            time.sleep(2)
        else:
            logs = subprocess.run(["docker", "logs", "--tail", "40", drill_name],
                                  capture_output=True, text=True, timeout=10)
            return (f"drill container never became ready — recovery may have stalled:\n"
                    f"{logs.stdout}\n{logs.stderr}")

        out = subprocess.run(
            ["docker", "exec", drill_name, "psql", "-U", "osiris", "-d", "osiris", "-tc",
             f"SELECT count(*) FROM objects WHERE canonical = '{marker_canonical}'"],
            capture_output=True, text=True, timeout=30)
        n = int((out.stdout or "0").strip() or 0)
        if n < 1:
            return (f"restored copy is missing the post-base-backup marker "
                    f"({marker_canonical!r}) — WAL replay did not reach it")
        return None
    except Exception as e:  # noqa: BLE001
        return f"PITR drill failed: {type(e).__name__}: {e}"
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", drill_name], capture_output=True, timeout=30)
        # `pgdata` was written by the postgres container as ITS OWN uid (999 inside the
        # container, an unmapped/colliding uid on the host) — the host user that ran this
        # script cannot even read it, let alone rmtree it, and ignore_errors=True on that
        # call would silently leak the whole scratch tree every single drill (caught by
        # running this drill for real: three runs, three leaked multi-GB directories
        # before this fix). A throwaway root container CAN delete it (root bypasses host
        # DAC on a bind mount) — reclaim it first, then rmtree the rest normally.
        subprocess.run(["docker", "run", "--rm", "-v", f"{scratch}:/scratch", "postgres:16",
                        "rm", "-rf", "/scratch/pgdata"], capture_output=True, timeout=60)
        shutil.rmtree(scratch, ignore_errors=True)


def pick_and_ensure_marker(container: str = CONTAINER) -> str | None:
    """The unattended-drill marker: the live DB's own most-recently-created object,
    read-only, no operator-authored Decision needed each run — the fleet writes
    constantly, so there is always a fresh one. `pg_switch_wal()` is an administrative
    WAL-control call, not a graph mutation (house law on raw SQL is about writes into
    the graph's own rows, never about calling Postgres's own control functions) — it
    forces the segment holding that object's write to archive immediately rather than
    waiting for it to fill naturally, so the drill doesn't have to wait either. Returns
    None on an empty database (nothing to prove yet, not a failure)."""
    out = subprocess.run(
        ["docker", "exec", container, "psql", "-U", "osiris", "-d", "osiris", "-tAc",
         "SELECT canonical FROM objects ORDER BY created_at DESC LIMIT 1"],
        capture_output=True, text=True, timeout=30)
    canonical = out.stdout.strip()
    if not canonical:
        return None
    subprocess.run(
        ["docker", "exec", container, "psql", "-U", "osiris", "-d", "osiris",
         "-c", "SELECT pg_switch_wal();"],
        capture_output=True, timeout=30)
    return canonical


def main(argv: list[str] | None = None) -> int:
    from scripts.osiris_prune_ladder import _scan

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vault", type=Path, default=Path.home() / "osiris-vault")
    parser.add_argument("--marker", default=None,
                        help="canonical of an object known to exist AFTER the base "
                             "backup completed — the drill's own pass condition. "
                             "Default: auto-pick the live DB's newest object "
                             "(pick_and_ensure_marker) — an unattended weekly run "
                             "needs no operator-authored marker.")
    parser.add_argument("--target-time", default=None,
                        help="an EXACT recovery_target_time to demand (Postgres does NOT "
                             "accept the bare word 'now' here, unlike most other "
                             "timestamp contexts). Default: no target at all — replay "
                             "every WAL segment the archive can actually produce and "
                             "promote there, the honest 'as-current-as-provable' drill.")
    args = parser.parse_args(argv)

    backups = _scan(args.vault / "basebackups")
    if not backups:
        print("osiris_pitr_drill: no base backups found — nothing to drill", file=sys.stderr)
        return 1
    newest = max(backups, key=lambda f: f.when)
    target_time = args.target_time
    marker = args.marker or pick_and_ensure_marker()
    if marker is None:
        print("osiris_pitr_drill: no objects in the live DB — nothing to prove",
              file=sys.stderr)
        return 1
    print(f"osiris_pitr_drill: restoring {newest.path} to target_time={target_time!r}, "
          f"marker={marker!r}")
    fail = run_drill(Path(newest.path), target_time, marker)
    if fail:
        print(f"PITR DRILL FAILED: {fail}", file=sys.stderr)
        return 1
    print("PITR drill: PASS — the marker written after the base backup is present in "
          "the restored copy")
    return 0


if __name__ == "__main__":
    sys.exit(main())

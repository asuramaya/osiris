"""THE OFF-BOX RESTORE DRILL (thread cf134938's own shape, item 3 — design pending the
operator's backend ruling on that thread; ships now, parameterized by the repository
URL alone, so the eventual ruling is a one-line config change). "A backup that's never
been restored is a hope, not a backup" — the SAME law osiris_pitr_drill.py already
holds for the local vault, applied here to the OFF-BOX copy specifically: proving the
SECOND copy survives in isolation is the whole point of having one, so this restores
FROM the remote repository into its own scratch directory — never the live vault — and
checks the result actually contains real content, not just that restic exited 0.

`restic check` alone is not enough: a repository can pass integrity checking while
holding zero snapshots (nothing ever backed up, or every snapshot pruned), and empty
is not restorable — this drill fails that case explicitly rather than reporting a
`check`-clean repo as proof of anything.

CREDENTIALS: RESTIC_PASSWORD or RESTIC_PASSWORD_FILE (restic's own env contract) must
already be set in the calling environment, same as osiris_offbox_backup.sh — this
script never touches it.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def run_drill(repo_url: str, *, scratch: Path | None = None) -> str | None:
    """Returns a failure string, or None on success. Cleans up the scratch directory
    in every case (`finally`), same discipline as osiris_pitr_drill.py's own
    run_drill. `scratch` defaults to a fresh tempdir — never a caller-reused
    directory, so a prior drill's leftovers can never be mistaken for this run's own
    restored content."""
    scratch = scratch or Path(tempfile.mkdtemp(prefix="osiris-offbox-drill-"))
    env = {**os.environ, "RESTIC_REPOSITORY": repo_url}
    try:
        check = subprocess.run(["restic", "check"], env=env, capture_output=True,
                               text=True, timeout=600)
        if check.returncode != 0:
            return f"restic check failed:\n{check.stdout}\n{check.stderr}"

        restore = subprocess.run(
            ["restic", "restore", "latest", "--target", str(scratch)],
            env=env, capture_output=True, text=True, timeout=1800)
        if restore.returncode != 0:
            return f"restic restore failed:\n{restore.stdout}\n{restore.stderr}"

        restored_files = [p for p in scratch.rglob("*") if p.is_file()]
        if not restored_files:
            return ("restore produced zero files — the repository has no snapshots, "
                     "or the latest one is empty; a check-clean repository is NOT "
                     "proof of a restorable backup")
        return None
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"off-box restore drill failed: {type(exc).__name__}: {exc}"
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo_url", help="the restic repository URL to drill against "
                                          "(the SAME url osiris_offbox_backup.sh was "
                                          "given)")
    args = parser.parse_args(argv)

    fail = run_drill(args.repo_url)
    if fail:
        print(f"OFF-BOX RESTORE DRILL FAILED: {fail}", file=sys.stderr)
        return 1
    print("off-box restore drill: PASS — restic check clean, restore produced real "
          "content")
    return 0


if __name__ == "__main__":
    sys.exit(main())

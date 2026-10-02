"""THE OFF-BOX RESTORE DRILL, IN TWO SHAPES. `run_bounded_drill` is the scheduled one: it proves
the backup is recoverable inside a time budget (restic check of the repository structure plus a
read-data subset, the header of the newest database dump, and a restore of a small sample of
files, each verified against the repository's own content hashes), so it can run on its own
timer on a laptop without moving gigabytes. `run_drill` is the FULL restore of the latest
snapshot, kept as the explicit manual door (`osiris soul-key restore-drill --full`).

THE OFF-BOX RESTORE DRILL: ships now, parameterized by the repository URL alone, so a
future decision about the backend can be a one-line config change. A backup that has
never been restored is a hope, not a backup, the same principle osiris_pitr_drill.py
already holds for the local vault, applied here to the OFF-BOX copy specifically:
proving the SECOND copy survives in isolation is the whole point of having one, so
this restores FROM the remote repository into its own scratch directory, never the
live vault, and checks the result actually contains real content, not just that
restic exited 0.

`restic check` alone is not enough: a repository can pass integrity checking while
holding zero snapshots (nothing ever backed up, or every snapshot pruned), and empty
is not restorable. This drill fails that case explicitly rather than reporting a
`check`-clean repo as proof of anything.

CREDENTIALS: `restic_credential.get_restic_password()`, the same resolution ladder
`orchestrator.offload_runner._run_restic_backup` already uses, not the ambient
environment (an earlier version of this script trusted RESTIC_PASSWORD/
RESTIC_PASSWORD_FILE to already be set by the caller; the runner and this drill now
resolve the SAME credential the SAME way, so a drill run genuinely proves the runner's
own real password unlocks the repository, not a different one a human happened to have
exported). Set directly as the RESTIC_PASSWORD subprocess env var, never a temp file,
never a CLI argument (visible via `ps`), matching osiris_offbox_backup.sh's own
long-standing rule; never logged or included in any error message this script prints.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def run_drill(repo_url: str, *, scratch: Path | None = None) -> str | None:
    """Returns a failure string, or None on success. Cleans up the scratch directory
    in every case (`finally`), same discipline as osiris_pitr_drill.py's own
    run_drill. `scratch` defaults to a fresh tempdir, never a caller-reused
    directory, so a prior drill's leftovers can never be mistaken for this run's own
    restored content. Resolves the restic password itself (`ResticPasswordMissing`
    degrades this one call, never a subprocess.run with a half-populated env);
    the password never appears in the returned failure string."""
    from src.orchestrator.restic_credential import ResticPasswordMissing, get_restic_password

    try:
        password = get_restic_password()
    except ResticPasswordMissing as exc:
        return str(exc)

    scratch = scratch or Path(tempfile.mkdtemp(prefix="osiris-offbox-drill-"))
    env = {**os.environ, "RESTIC_REPOSITORY": repo_url, "RESTIC_PASSWORD": password.decode()}
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
            return ("restore produced zero files: the repository has no snapshots, "
                     "or the latest one is empty; a check-clean repository is NOT "
                     "proof of a restorable backup")
        return None
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"off-box restore drill failed: {type(exc).__name__}: {exc}"
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


# --- THE BOUNDED DRILL ------------------------------------------------------------------------

DRILL_BUDGET_SECONDS = 600       # the whole bounded drill, every step included
READ_DATA_SUBSET = "2%"          # share of the pack data `restic check` reads and re-hashes
SAMPLE_FILES = 3                 # small files restored and verified, beside the two named ones
SAMPLE_MAX_BYTES = 1 << 20       # only files this small are sampled (the drill must stay cheap)
LS_NODE_CAP = 2_000_000          # never read an unbounded snapshot listing
_DUMP_RE = re.compile(r"/osiris-\d{8}-\d{6}\.(dump|sql)$")
_RECOVERY_SUFFIX = "/osiris-recovery/soul.key.recovery.json"


class _Budget:
    """A wall-clock budget shared by every step, so no single slow restic call can run past
    the whole drill's limit."""

    def __init__(self, seconds: float) -> None:
        self.deadline = time.monotonic() + seconds
        self.seconds = seconds

    def left(self) -> float:
        return self.deadline - time.monotonic()

    def timeout(self, step: str) -> float:
        if self.left() <= 1:
            raise TimeoutError(f"the drill ran out of its {self.seconds:.0f}s time budget "
                               f"before {step}")
        return self.left()


def _restic(args: list[str], env: dict[str, str], budget: _Budget, step: str,
            ) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["restic", *args], env=env, capture_output=True, text=True,
                          timeout=budget.timeout(step), check=False)


def _glob_escape(path: str) -> str:
    return re.sub(r"([\\*?\[])", r"\\\1", path)


def _pick_samples(env: dict[str, str], budget: _Budget, rng: random.Random,
                  ) -> tuple[str | None, str | None, list[tuple[str, int]]]:
    """Reads the latest snapshot's file listing (streamed, capped) and returns (recovery file
    path or None, newest database dump path or None, small sample files [(path, size)]).
    Samples prefer transcript files and are drawn at random, so successive drills cover
    different files."""
    proc = subprocess.Popen(["restic", "ls", "latest", "--json", "--recursive"], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    recovery: str | None = None
    newest_dump: str | None = None
    transcripts: list[tuple[str, int]] = []
    others: list[tuple[str, int]] = []
    seen_t = seen_o = nodes = 0
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            budget.timeout("the file listing finished")
            nodes += 1
            if nodes > LS_NODE_CAP:
                break
            try:
                node: dict[str, Any] = json.loads(line)
            except ValueError:
                continue
            if node.get("type") != "file":
                continue
            path, size = str(node.get("path", "")), int(node.get("size") or 0)
            if path.endswith(_RECOVERY_SUFFIX):
                recovery = path
            elif _DUMP_RE.search(path):
                if newest_dump is None or path > newest_dump:
                    newest_dump = path
            elif 0 < size <= SAMPLE_MAX_BYTES:
                # reservoir sampling: a uniform sample of the small files without holding them all
                is_transcript = "transcript" in "/".join(path.split("/")[-3:])
                bucket, count = ((transcripts, seen_t) if is_transcript else (others, seen_o))
                if is_transcript:
                    seen_t += 1
                else:
                    seen_o += 1
                if len(bucket) < SAMPLE_FILES:
                    bucket.append((path, size))
                else:
                    j = rng.randrange(count + 1)
                    if j < SAMPLE_FILES:
                        bucket[j] = (path, size)
    finally:
        proc.kill()
        proc.wait()
    chosen = (transcripts + others)[:SAMPLE_FILES]
    return recovery, newest_dump, chosen


def _dump_header_failure(path: str, env: dict[str, str], budget: _Budget) -> str | None:
    """Streams the first bytes of the newest dump out of the repository and checks they look
    like a database dump, without restoring the whole file."""
    proc = subprocess.Popen(["restic", "dump", "latest", path], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert proc.stdout is not None
        head = proc.stdout.read(4096)
    finally:
        proc.kill()
        proc.wait()
    budget.timeout("the dump header check finished")
    if path.endswith(".dump"):
        return None if head.startswith(b"PGDMP") else (
            f"the newest dump {path} does not start like a database dump (got {head[:8]!r})")
    return None if b"PostgreSQL database dump" in head[:2048] else (
        f"the newest dump {path} does not start like a database dump")


def run_bounded_drill(
    repo_url: str, *, scratch: Path | None = None, budget_secs: float = DRILL_BUDGET_SECONDS,
    rng: random.Random | None = None,
) -> str | None:
    """Returns a failure string, or None on success, the same convention as `run_drill`.
    Steps, all inside one time budget: (1) `restic check --read-data-subset` (repository
    structure plus a re-hash of a share of the stored data); (2) the newest database dump's
    header is streamed out and checked; (3) the recovery file and a few small sample files
    are restored into a scratch directory with `--verify` (every restored file re-hashed
    against the repository) and checked for the recorded size; the recovery file must also
    parse and carry its wrapped key. A repository with no snapshots, or nothing to sample,
    fails: a clean check alone proves nothing. Cleans up its scratch in every case."""
    from src.orchestrator.restic_credential import ResticPasswordMissing, get_restic_password

    try:
        password = get_restic_password()
    except ResticPasswordMissing as exc:
        return str(exc)
    scratch = scratch or Path(tempfile.mkdtemp(prefix="osiris-offbox-drill-"))
    env = {**os.environ, "RESTIC_REPOSITORY": repo_url, "RESTIC_PASSWORD": password.decode()}
    budget = _Budget(budget_secs)
    rng = rng or random.Random()
    try:
        check = _restic(["check", f"--read-data-subset={READ_DATA_SUBSET}"], env, budget,
                        "the repository check finished")
        if check.returncode != 0:
            return f"restic check failed:\n{check.stdout}\n{check.stderr}"
        recovery, dump, samples = _pick_samples(env, budget, rng)
        wanted = ([recovery] if recovery else []) + [p for p, _ in samples]
        if not wanted and not dump:
            return ("the latest snapshot has no files to sample, or the repository has no "
                    "snapshots; a check-clean repository is NOT proof of a restorable backup")
        if dump:
            failure = _dump_header_failure(dump, env, budget)
            if failure:
                return failure
        if wanted:
            args = ["restore", "latest", "--target", str(scratch), "--verify"]
            for p in wanted:
                args += ["--include", _glob_escape(p)]
            restore = _restic(args, env, budget, "the sample restore finished")
            if restore.returncode != 0:
                return f"restic restore failed:\n{restore.stdout}\n{restore.stderr}"
            sizes = dict(samples)
            for p in wanted:
                restored = scratch / p.lstrip("/")
                if not restored.is_file():
                    return f"the sample {p} was not restored"
                if p in sizes and restored.stat().st_size != sizes[p]:
                    return f"the sample {p} restored with the wrong size"
            if recovery:
                try:
                    blob = json.loads((scratch / recovery.lstrip("/")).read_text())
                except (OSError, ValueError):
                    return "the restored recovery file is not valid JSON"
                if not blob.get("wrapped_key"):
                    return "the restored recovery file carries no wrapped key"
        return None
    except TimeoutError as exc:
        return str(exc)
    except subprocess.TimeoutExpired as exc:
        return (f"off-box restore drill timed out after {exc.timeout:.0f}s "
                f"({budget_secs:.0f}s budget)")
    except OSError as exc:
        return f"off-box restore drill failed: {type(exc).__name__}: {exc}"
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo_url", help="the restic repository URL to drill against "
                                          "(the SAME url osiris_offbox_backup.sh was "
                                          "given)")
    parser.add_argument("--full", action="store_true",
                        help="restore the WHOLE latest snapshot (can move gigabytes) "
                             "instead of the bounded check and sample restore")
    args = parser.parse_args(argv)

    fail = run_drill(args.repo_url) if args.full else run_bounded_drill(args.repo_url)
    if fail:
        print(f"OFF-BOX RESTORE DRILL FAILED: {fail}", file=sys.stderr)
        return 1
    print("off-box restore drill: PASS. " + (
        "restic check clean, restore produced real content" if args.full else
        "restic check clean, dump header read, sample files restored and verified"))
    return 0


if __name__ == "__main__":
    sys.exit(main())

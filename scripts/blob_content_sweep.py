"""THE FULL BLOB-CONTENT SWEEP (task #30/#182, Thoth DM 5574) — checked in this time.

The disease this closes: decision 55669bac (2026-08-19T00:46) ran this exact method BY
HAND — 1,402 commit objects, 4,125 blobs, every reachable/unreachable/reflog-only object
in the store — and found zero real PII. It was never committed. 29+ commits landed after
it, its clean result does not cover them, and the only way to re-check was to rebuild the
method from memory. That gap IS the disease this whole wave (Thoth DM 5544) was about:
finished, working, one-off scans that leave no re-runnable artifact.

METHOD, matching 55669bac's own description exactly: `git cat-file --batch-all-objects
--batch` walks EVERY object ever written into this repo's object database in ONE pass —
reachable from a branch, unreachable-but-not-yet-gc'd, and reflog-only alike (this is
STRONGER than 55669bac's three separate reachable/unreachable/reflog enumerations: the
object store has no such distinction, only `git gc --prune` does). For every `commit` and
`blob` object, content is decoded (`errors="replace"`, binary-safe — a blob that isn't
text just never matches) and scanned against `PATTERNS` below. Never a full git-object walk
substitute like push_guard.py's diff-only scan (its own docstring names the gap this closes:
"a secret introduced by a binary or a rename with no textual delta would be invisible to
this method") — this reads the actual stored bytes, not a diff of them.

READ-ONLY. No repair, no rewrite, no push. Per Thoth's explicit instruction: report the
inventory, then STOP — a HEAD-only leak is a commit, a history-wide leak is the operator's
own call (the last rewrite orphaned five seat branches), and it is not this script's place
to guess which.

SELF-INCIDENT (Thoth DM 5583, commit 17e7fa6): the first version of this file stored the
two known-real addresses as a literal regex alternation, and the test fixtures spelled
them out in plain text to prove the detector caught them. Both are exactly the specimen
#30 hunts for, and the literal regex was WORSE: a human
reading this file's source sees both addresses at a glance, while the sweep's own
plain-webmail-shape pattern never flagged it (a regex's own escape characters break a
literal-text match against the regex source). A detector that cannot detect itself has a
permanent blind spot
at its most sensitive point. FIX: `KNOWN_REAL_HASHES` below holds sha256 digests, never the
addresses themselves — the real strings now appear NOWHERE in this repository, including
this file's own history from this commit forward. `test_blob_content_sweep.py`'s own
self-scan test (`test_this_files_own_source_and_tests_carry_no_literal_leak`) scans BOTH
this file and itself under the same rules as every other object this sweep walks, so this
class of mistake cannot recur silently — see that test for what it specifically proves."""
from __future__ import annotations

import argparse
import hashlib
import re
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple, TypedDict

REPO_ROOT = Path(__file__).resolve().parent.parent

# Same law as push_guard.DEFAULT_PATTERNS (820faa9d): secret-SHAPED formats, plus a BROAD
# webmail-domain regex (55669bac's own "not scoped to known addresses" — catches an address
# nobody has named yet), plus the operator's local path (9ba0bda134e0's low-severity, still-
# named finding). The two KNOWN-REAL addresses are NOT here — see KNOWN_REAL_HASHES below;
# storing them as a literal pattern is the exact self-incident this module's docstring names.
PATTERNS: dict[str, str] = {
    "private-key-block": r"-----BEGIN (RSA |EC |OPENSSH |DSA |PGP )?PRIVATE KEY-----",
    "aws-access-key-id": r"\bAKIA[0-9A-Z]{16}\b",
    "github-token": r"\bgh[pousr]_[A-Za-z0-9]{36,}\b",
    "slack-token": r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b",
    "stripe-live-key": r"\bsk_live_[A-Za-z0-9]{20,}\b",
    "webmail-address": (
        r"(?i)\b[A-Za-z0-9._%+-]+@(gmail|yahoo|hotmail|outlook|aol|icloud|proton"
        r"mail)\.(com|net)\b"
    ),
    "operator-home-path": r"/home/asuramaya\b",
}

# sha256(lowercased address) for each of the two known-real leaked addresses (2fc98818/
# 9ba0bda134e0) — the digest, never the address, is what lives in this repository. A
# candidate email-shaped token (EMAIL_TOKEN_RE below) is lowercased and hashed; a hash hit
# is reported as "known-real-address" with the ORIGINAL matched text still redacted the
# same way every other finding is (never printed in full, even from a hash hit).
KNOWN_REAL_HASHES: frozenset[str] = frozenset({
    "a35c19a9ca7efbc9ab64eab953a855268f1e9c71da5d405c14186a6dfe6c1a38",
    "cde3a5a75c730175276c9c7ae74bfd349ff89da79f639042611f139dc56be6ab",
})

# Generic email-shape extraction for the hash check — deliberately broader than
# `webmail-address` above (any domain, not just known webmail providers): a hash comparison
# has no false-positive cost the way a printed literal would, so there is no reason to
# narrow the candidate set the way the informational webmail pattern does.
EMAIL_TOKEN_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")

# Test-fixture addresses this house already knows are synthetic (9ba0bda134e0) — excluded
# so a re-run doesn't re-flag the same 20+ already-judged non-findings every time. A hit
# on any OTHER webmail address still fires; only these exact, previously-cleared literals
# are skipped.
KNOWN_SYNTHETIC = {
    "dakota.jm@gmail.com", "priya.kowalski42@gmail.com", "john.doe@gmail.com",
}


class Finding(NamedTuple):
    pattern: str
    object_type: str
    object_sha: str
    redacted: str


class SweepReport(TypedDict):
    objects_total: int
    commits_scanned: int
    blobs_scanned: int
    findings: list[Finding]


def _redact(snippet: str) -> str:
    return (snippet[:4] + "…" + snippet[-4:]) if len(snippet) > 10 else "…"


def iter_all_objects(repo_root: Path) -> list[tuple[str, str, bytes]]:
    """Every object in the store — `(sha, type, content)` — via ONE `git cat-file
    --batch-all-objects --batch` pass. This walks the object database directly, not a ref
    walk: reachable, unreachable-but-unpruned, and reflog-only objects are all present,
    with no separate enumeration needed for any of the three. Raises on a git failure —
    a sweep that silently saw fewer objects than the repo actually holds is worse than one
    that refuses to report at all (unlike push_guard's fail-open law: THIS script never
    gates a push, it only informs a report, so there is no blast-radius argument for
    swallowing an error here)."""
    proc = subprocess.run(
        ["git", "cat-file", "--batch-all-objects", "--batch"],
        cwd=repo_root, capture_output=True, timeout=120, check=True)
    out = proc.stdout
    objects: list[tuple[str, str, bytes]] = []
    pos = 0
    n = len(out)
    while pos < n:
        eol = out.index(b"\n", pos)
        header = out[pos:eol].decode("ascii")
        sha, otype, size_s = header.split(" ")
        size = int(size_s)
        start = eol + 1
        content = out[start:start + size]
        objects.append((sha, otype, content))
        pos = start + size + 1  # +1 skips the trailing newline after the object's bytes
    return objects


def scan_objects(
    objects: list[tuple[str, str, bytes]], patterns: dict[str, str],
    known_hashes: frozenset[str] | None = None,
) -> list[Finding]:
    """Pure — no IO. Scans only `commit`/`blob` objects (trees/tags carry no free-text
    content of the kind these patterns target). Binary-safe: `errors=\"replace\"` means a
    non-text blob just never matches anything, never raises. `known_hashes` (defaults to
    the module's own `KNOWN_REAL_HASHES`) drives the hash-based check ALONGSIDE the regex
    `patterns` — every email-shaped token in the text is lowercased and hashed, checked
    against this set, independent of whether it also matches `webmail-address`."""
    compiled = {name: re.compile(p) for name, p in patterns.items()}
    hashes = KNOWN_REAL_HASHES if known_hashes is None else known_hashes
    findings: list[Finding] = []
    for sha, otype, content in objects:
        if otype not in ("commit", "blob"):
            continue
        text = content.decode("utf-8", errors="replace")
        for token_match in EMAIL_TOKEN_RE.finditer(text):
            token = token_match.group(0)
            if hashlib.sha256(token.lower().encode()).hexdigest() in hashes:
                findings.append(Finding("known-real-address", otype, sha, _redact(token)))
        for name, rx in compiled.items():
            for m in rx.finditer(text):
                matched = m.group(0)
                if matched.lower() in {s.lower() for s in KNOWN_SYNTHETIC}:
                    continue
                findings.append(Finding(name, otype, sha, _redact(matched)))
    return findings


def sweep(
    repo_root: Path = REPO_ROOT, patterns: dict[str, str] | None = None,
    known_hashes: frozenset[str] | None = None,
) -> SweepReport:
    """The full report: counts scanned + findings. `patterns`/`known_hashes` overridable
    for testing; production callers always get the module's own `PATTERNS`/
    `KNOWN_REAL_HASHES`."""
    objects = iter_all_objects(repo_root)
    commits = sum(1 for _, t, _ in objects if t == "commit")
    blobs = sum(1 for _, t, _ in objects if t == "blob")
    findings = scan_objects(
        objects, patterns if patterns is not None else PATTERNS, known_hashes)
    return {
        "objects_total": len(objects), "commits_scanned": commits,
        "blobs_scanned": blobs, "findings": findings,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=REPO_ROOT)
    args = parser.parse_args(argv)

    report = sweep(args.repo)
    print(f"blob_content_sweep: {report['objects_total']} objects "
          f"({report['commits_scanned']} commits, {report['blobs_scanned']} blobs)")
    if not report["findings"]:
        print("blob_content_sweep: CLEAN — zero matches across every pattern")
        return 0
    print(f"blob_content_sweep: {len(report['findings'])} finding(s):")
    for f in report["findings"]:
        print(f"  {f.pattern}: {f.object_type} {f.object_sha[:12]} — matched {f.redacted!r}")
    return 1


if __name__ == "__main__":
    sys.exit(main())

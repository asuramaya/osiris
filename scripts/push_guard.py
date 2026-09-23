"""The pre-push secret/PII guard, built after a 2026-08-15 incident where the operator
directed that this hook be built because keeping the dev operations secure is important.

Origin is a single remote shared by every worktree of this repo (verified per-worktree,
not assumed: all 11 real worktrees carry the identical origin URL). One push door,
reachable from everywhere, is why a single habitual push exposed four branches from
three lineage generations. This is that door's own chokepoint: `.git/hooks/pre-push`,
the one place that fires no matter which named path triggered the push (`git push`
typed directly, `gh pr create`'s own implicit push, an IDE button, a harness auto-sync).
All of them execute git's push protocol underneath, and a pre-push hook sees every one
of them.

Deliberately not `core.hooksPath`: a known global-wiring landmine stays untouched. Hooks
are not per-worktree by default, so a single file written into the shared
`.git/hooks/pre-push` (installed by `scripts/install_push_guard_hook.sh`, never
`core.hooksPath`) covers every one of this repo's worktrees at once without disturbing
that other wiring at all.

Fail-open, never fail-closed on infrastructure: every check here that cannot complete
(git itself erroring, an unreadable pattern file, a crash in this script) degrades to
allow, loudly, never a silent pass and never a block. The only thing that blocks is a
positive match: this script finding a secret-shaped string or an operator-supplied
pattern actually present in the range about to be uploaded. A gate whose failure mode
blocks all shipping is worse than the leak it exists to prevent; a gate that quietly
lets a positive match through is worse than the friction of a refusal.

The escape hatch is part of the build, because whoever hits it may be revisiting this
script long after writing it, with no memory of the details: `OSIRIS_PUSH_GUARD_SKIP=1
git push ...` bypasses every check with one loud line to stderr naming that it happened,
and the refusal message itself prints this exact line so nobody needs to have read this
docstring first.

Five surfaces, from a post-incident audit (each surface below is scoped to the commit
range actually about to be uploaded, resolved fresh per push, never "the branch", since
a branch can carry commits already on origin):
  1. `commit_range`: the exact commits this specific push would add, not the whole branch.
  2. Commit messages in that range (`git filter-repo --replace-text` skips these and
     reports success: the exact miss that let two prior scrub attempts leave real
     content behind).
  3. Diff content (added and removed lines) of each commit's own patch against its
     parent: a pragmatic approximation of "blob content in the range", not a full
     git-object walk; see `scan_range`'s own docstring for what this specifically
     cannot catch.
  4. (Not this file's job: a push-time gate only ever sees what this push sends, never a
     stray local ref sitting unpushed.) The "what would a --mirror push expose" hygiene
     question is `deploy_guard.origin_visibility`'s own extension, a read not a gate.
  5. `is_worktree_checkout`: ref hygiene, not content (added 2026-08-18, following a
     specific incident review). The four surfaces above only ever ask "does this push
     carry a secret"; they had nothing to say about "should this actor be pushing at
     all", and correctly allowed a worker's clean, secretless branch straight onto the
     public origin, because that policy (workers commit, they never push, CLAUDE.md's
     own law) was never encoded here. Measured before this was added: the hook was
     already correctly installed and firing from every worktree (verified live: a
     throwaway bare remote pushed to from inside a worktree printed `push_guard: clean`
     and the push went through). The gap was never coverage, it was scope. A linked
     worktree (never the main checkout) now refuses categorically, any content, same
     `OSIRIS_PUSH_GUARD_SKIP` escape hatch as every other check here, one hatch, not a
     second one to remember.

Patterns: a small, low-false-positive-risk default set (private key blocks, GitHub/Slack/
Stripe token shapes, see `DEFAULT_PATTERNS`) plus an optional local, untracked file
(`<git-common-dir>/push_guard_patterns.txt`, inside `.git/`, which is itself never
tracked, so a real value like a specific person's address can be protected here without
ever being committed into the very script meant to guard against exactly that), one
regex per line, blank/`#`-prefixed lines skipped. Deliberately no default email-address
pattern: this house's own commit trailers stamp a real (noreply) address on every commit
by convention, and a bare email regex would refuse every ordinary commit this repo makes.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

SKIP_ENV_VAR = "OSIRIS_PUSH_GUARD_SKIP"

# Secret-shaped, not content-shaped: each pattern here is a well-known credential format,
# never a generic string/entropy heuristic (already ruled out as too noisy for a blocking
# gate). Named per pattern so a refusal can say which one matched, not just that something
# did.
DEFAULT_PATTERNS: dict[str, str] = {
    "private-key-block": r"-----BEGIN (RSA |EC |OPENSSH |DSA |PGP )?PRIVATE KEY-----",
    "aws-access-key-id": r"\bAKIA[0-9A-Z]{16}\b",
    "github-token": r"\bgh[pousr]_[A-Za-z0-9]{36,}\b",
    "slack-token": r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b",
    "stripe-live-key": r"\bsk_live_[A-Za-z0-9]{20,}\b",
}


def _run(cmd: list[str], cwd: Path) -> tuple[bool, str]:
    try:
        proc = subprocess.run(
            cmd, cwd=cwd, capture_output=True, text=True, check=False, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, f"{cmd[0]} could not run: {exc}"
    ok = proc.returncode == 0
    return ok, ((proc.stdout or "") + (proc.stderr or "")).strip()


def git_common_dir(repo_root: Path = REPO_ROOT) -> Path | None:
    """The shared `.git` directory, resolved from whichever worktree is actually calling
    this (`--git-common-dir` resolves correctly from a worktree, verified live). The same
    directory `install_push_guard_hook.sh` writes the installed hook into, and where the
    optional local patterns file lives. None (never a raised exception) if this isn't a git
    checkout at all."""
    ok, out = _run(["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
                    repo_root)
    return Path(out) if ok and out else None


def is_worktree_checkout(repo_root: Path) -> bool | None:
    """True when `repo_root` is a linked worktree: a seat's own isolated checkout
    (`.claude/worktrees/<name>`, by this house's own EnterWorktree/`git worktree add`
    convention), never the main checkout the operator/manager actually pushes from. Uses
    git's own structural signal, not a path-string convention that would go blind the
    moment a worktree is created somewhere else (`git worktree add /tmp/whatever`, still
    caught): `--git-dir` resolves to `<common-dir>/worktrees/<name>` for a linked worktree
    and to the exact same path as `--git-common-dir` for the main checkout. Comparing the
    two is the canonical way git itself tells them apart.

    None (never raises) on a git failure: the caller's job to fail open on that, same as
    every other infra read in this module."""
    ok1, git_dir = _run(["git", "rev-parse", "--path-format=absolute", "--git-dir"], repo_root)
    ok2, common_dir = _run(
        ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"], repo_root)
    if not ok1 or not ok2 or not git_dir or not common_dir:
        return None
    return git_dir != common_dir


WORKTREE_PUSH_REFUSAL = (
    "push_guard: REFUSED — this push is running from a LINKED WORKTREE (git rev-parse "
    "--git-dir != --git-common-dir), never the main checkout. House rule: a seat commits "
    "its own work but NEVER pushes — that is the manager's/operator's own hand, from the "
    "main checkout. This is REF HYGIENE, not a content scan — it refuses regardless of "
    "what the push carries.\n\n"
    "TO OVERRIDE (a deliberate operator push from a worktree, judged safe): "
    f"{SKIP_ENV_VAR}=1 git push ... — this bypasses every check in this hook and prints a "
    "loud warning when it does, it does not push silently."
)


def custom_patterns(common_dir: Path | None) -> dict[str, str]:
    """`<git-common-dir>/push_guard_patterns.txt`, one regex per line, `#`/blank skipped.
    Never tracked (lives inside `.git/`), so a real value can be protected here without
    ever being committed into this script. A missing or unreadable file is an empty set,
    not an error: this is an optional, additive surface, never a required one."""
    if common_dir is None:
        return {}
    path = common_dir / "push_guard_patterns.txt"
    if not path.is_file():
        return {}
    out: dict[str, str] = {}
    try:
        for i, line in enumerate(path.read_text().splitlines(), 1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            out[f"local-pattern-{i}"] = stripped
    except OSError:
        return {}
    return out


_NULL_SHA = "0" * 40


def parse_stdin_refs(text: str) -> list[tuple[str, str, str, str]]:
    """Pure: `<local ref> <local sha1> <remote ref> <remote sha1>` per line, git's own
    pre-push stdin contract. A malformed line is skipped, never fatal (a hook that crashes
    on a git-internals quirk it hasn't seen yet must not thereby block every future push)."""
    out: list[tuple[str, str, str, str]] = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 4:
            out.append((parts[0], parts[1], parts[2], parts[3]))
    return out


def commit_range(repo_root: Path, local_sha: str, remote_sha: str) -> list[str] | None:
    """The exact commits this push would add, not the whole branch. A delete push
    (`local_sha` all-zero) has nothing to scan. An update to an already-known remote ref
    uses the exact `remote_sha..local_sha` range. A brand-new ref (`remote_sha` all-zero)
    has no known upstream tip to diff against, so this scopes to whatever is not already
    reachable from any locally-known remote-tracking ref (`--not --remotes`), a
    best-effort local approximation (this hook never fetches: a network round-trip on
    every push would add exactly the kind of friction this design avoids), falling back
    to just `local_sha` itself if that yields nothing (a fresh clone with no
    remote-tracking history at all). Returns None (distinct from an empty list) on a git
    failure: the caller's job to decide that's unknown, not "nothing to scan"."""
    if local_sha == _NULL_SHA:
        return []
    if remote_sha != _NULL_SHA:
        ok, out = _run(["git", "rev-list", f"{remote_sha}..{local_sha}"], repo_root)
    else:
        ok, out = _run(["git", "rev-list", local_sha, "--not", "--remotes"], repo_root)
    if not ok:
        return None
    shas = [line for line in out.splitlines() if line]
    return shas if shas else [local_sha]


def scan_range(repo_root: Path, shas: list[str], patterns: dict[str, str]) -> list[str]:
    """Findings, one line each, naming the pattern, the commit's short sha, and a
    redacted context snippet. Scans in one subprocess call for the whole range (not one
    per commit, the same performance discipline `gate_hook.py`'s own `--audit` mode
    uses): each commit's own message (full body) and its own patch against its parent,
    `--unified=0` so context lines never dilute the signal. Includes both added and
    removed diff lines deliberately: a line a commit removes was still part of a blob
    this push uploads if that blob isn't already on the remote (the exact shape of a
    prior scrub's own miss, where tip-scoped checks never see a file's own history,
    only its current state).

    Named, not hidden, what this cannot catch (same discipline as `gate_hook.py`'s own
    "what this cannot catch" section): this is a per-commit patch scan, not a full
    git-object walk, so a secret that was never added or removed by an ordinary
    line-level diff (binary content, a rename with no textual delta, a blob introduced
    by a mode-only change) is invisible to this specific check. A real blob-content
    sweep across the whole object range is a materially larger build than this pass's
    scope; `deploy_guard.origin_visibility`'s own hygiene extension is the closer
    analogue for that class."""
    if not shas or not patterns:
        return []
    compiled = {name: re.compile(pat) for name, pat in patterns.items()}
    ok, out = _run(
        ["git", "log", "--no-walk", "-p", "--no-color", "--unified=0",
         "--format=%x00COMMIT %H%x00%n%B", *shas, "--"],
        repo_root)
    if not ok:
        return []
    findings: list[str] = []
    current_sha = shas[0][:12] if shas else "?"
    for line in out.splitlines():
        if line.startswith("\x00COMMIT "):
            current_sha = line[len("\x00COMMIT "):][:12]
            continue
        if (line.startswith(("--- a/", "--- /dev/null", "+++ b/", "+++ /dev/null"))):
            continue  # a real diff file-header line, not content, narrowed on purpose: a
            # bare startswith("---") would also eat a private-key block's own "-----BEGIN"
            # line, which is exactly the content this scan exists to catch (caught live by
            # this file's own test suite before it ever shipped).
        for name, rx in compiled.items():
            m = rx.search(line)
            if m:
                snippet = m.group(0)
                redacted = (snippet[:4] + "…" + snippet[-4:]) if len(snippet) > 10 else "…"
                findings.append(f"{name}: commit {current_sha} — matched {redacted!r}")
    return findings


REFUSAL_HEADER = (
    "push_guard: REFUSED — a positive match for secret/PII-shaped content was found in "
    "commits this push would upload to a PUBLIC remote."
)


def format_refusal(findings: list[str]) -> str:
    """The refusal message is the product: whoever hits this, possibly with no memory of
    the details, must learn from this text alone what matched, where, and both ways out,
    without reading this script."""
    lines = [REFUSAL_HEADER, ""]
    lines.extend(f"  - {f}" for f in findings)
    lines.append("")
    lines.append("TO FIX: amend or rebase the offending commit(s) so the match is gone, "
                  "then push again.")
    lines.append(f"TO OVERRIDE (a false positive, or content you have judged safe to "
                 f"publish): {SKIP_ENV_VAR}=1 git push ... — this bypasses every check in "
                 "this hook and prints a loud warning when it does, it does not push silently.")
    return "\n".join(lines)


def run(repo_root: Path, stdin_text: str) -> int:
    """The pre-push entry point. Returns the process exit code git itself will honor: 0
    lets the push through (clean, or any infra failure, fail-open, per this module's own
    docstring), 1 refuses (a real positive match, or an explicit test of that path)."""
    if os.environ.get(SKIP_ENV_VAR):
        print(f"push_guard: SKIPPED — {SKIP_ENV_VAR} is set. Pushing WITHOUT a secret/PII "
              "scan.", file=sys.stderr)
        return 0

    if is_worktree_checkout(repo_root):
        print(WORKTREE_PUSH_REFUSAL, file=sys.stderr)
        return 1

    common_dir = git_common_dir(repo_root)
    patterns = {**DEFAULT_PATTERNS, **custom_patterns(common_dir)}

    refs = parse_stdin_refs(stdin_text)
    if not refs:
        print("push_guard: nothing on stdin to scan — allowing", file=sys.stderr)
        return 0

    all_findings: list[str] = []
    for _local_ref, local_sha, _remote_ref, remote_sha in refs:
        shas = commit_range(repo_root, local_sha, remote_sha)
        if shas is None:
            print("push_guard: could not resolve the commit range for a ref being pushed "
                  "(git itself failed) — ALLOWING rather than blocking on an infra error; "
                  "this push was NOT scanned.", file=sys.stderr)
            continue
        all_findings.extend(scan_range(repo_root, shas, patterns))

    if all_findings:
        print(format_refusal(all_findings), file=sys.stderr)
        return 1
    print(f"push_guard: clean — {len(patterns)} pattern(s) checked, no match", file=sys.stderr)
    return 0


def hook_status(repo_root: Path = REPO_ROOT) -> str:
    """Is the pre-push guard actually installed, and is it the current tracked version?
    The verification `install_push_guard_hook.sh` is meant to make redundant, wired into
    `osiris deploy`'s own report so a missing hook is exactly as visible as a failing one,
    since installers are the part of a system that tends to rot unnoticed. Never raises;
    any read failure is its own honest status string, same fail-open discipline as
    `origin_visibility`."""
    common_dir = git_common_dir(repo_root)
    if common_dir is None:
        return "push_guard hook: not a git checkout — nothing to verify"
    tracked = repo_root / ".githooks" / "pre-push"
    installed = common_dir / "hooks" / "pre-push"
    if not tracked.is_file():
        return "push_guard hook: SOURCE MISSING (.githooks/pre-push not found in this tree)"
    if not installed.is_file():
        return ("push_guard hook: NOT INSTALLED — run "
                "scripts/install_push_guard_hook.sh (pushes from any worktree are "
                "UNSCANNED until this is fixed)")
    try:
        current = tracked.read_bytes() == installed.read_bytes()
    except OSError as exc:
        return f"push_guard hook: could not compare installed vs. tracked ({exc}) — UNKNOWN"
    if current:
        return "push_guard hook: installed and current"
    return ("push_guard hook: STALE — installed copy differs from the tracked source, "
            "re-run scripts/install_push_guard_hook.sh")


def main(argv: list[str] | None = None) -> int:
    return run(REPO_ROOT, sys.stdin.read())


if __name__ == "__main__":
    raise SystemExit(main())

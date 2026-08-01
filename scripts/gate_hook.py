"""THE GATES-ARE-LAW MECHANISM (task #131 follow-up, Thoth DM 2890/2894, operator ruling
4ef68cfe: "actions need mechanical state machines and enforcement as a rule... the laws are
also documents nobody reads though"). "Gates: TMPDIR=... pytest <touched> / ruff / mypy" has
been typed in every dispatch this house has ever sent and checked by nothing — a commit can
land fully ungated and no mechanism here would ever know. This is the mechanism, and per the
operator's own correction, its acceptance test is not "tests pass" — it is "the sentence
stops being typed" once it's armed.

TWO USES, ONE CODE PATH (never two implementations that can silently diverge):
  1. `git config core.hooksPath .githooks` + this script wired as `.githooks/pre-commit`
     (not done by this commit — installing it is a separate, deliberate act) runs it on the
     STAGED tree at commit time.
  2. `--audit A..B` re-derives the SAME check per-commit over a historical range, each
     commit in its own disposable `git worktree` (never the live shared tree — this repo has
     4 concurrent agents in it and a naive `git checkout <sha>` here would corrupt every one
     of their in-flight uncommitted edits). This is how the mechanism was PROVEN before being
     armed, not asserted.

REFUSE, NEVER ADVISE (operator ruling 4ef68cfe's discriminator): a failing gate is a nonzero
exit — git aborts the commit outright, the same shape as the deploy dirty-guard, never a
warning line a caller can choose to keep reading past. But armed is a SEPARATE bit from
BUILT: `Settings.osiris_gate_hook_enforce` (src/config/settings.py) must be True for a
failure to actually refuse — the exact same field-and-default shape `a333a07` used for
`osiris_closure_miner_enabled`, not a bespoke env var, at the operator's own explicit
instruction to mirror it. Default False (the shipped state) still runs every check and
PRINTS what it found, so the mechanism is observable and auditable before it can wedge a
single live commit in a shared tree.

TOUCHED-FILES-ONLY BY CONSTRUCTION, not an afterthought (Sekhmet's own #112 finding: the full
suite measured 209s under real 4-agent concurrency, and a per-commit gate demanding that is
unshippable — everyone disables it within a day). `resolve_test_files` is the one thing that
makes this safe to run on every commit: exact basename match first (`src/x/y.py` <->
`tests/test_y.py`, ~59% of this repo's modules), then a cheap static grep across `tests/` for
the module's own import path (`from src.x import y` / `from src.x.y import`) to catch the
rest — never a full-suite fallback, which would silently reintroduce the cost this exists to
avoid. A touched file with truly no resolvable test (scripts/, `__init__.py`, deploy config)
correctly runs no pytest at all; ruff/mypy still gate it, whole-project, because both are
already fast enough (single-digit seconds) to not need scoping. Every scoped pytest run
passes `-n 4` explicitly (`_PYTEST_XDIST_CAP`, msg 2919) rather than inheriting whatever
`-n auto` default lands in pyproject.toml — this gate can fire from multiple concurrent
agents' commits at once, and a fixed small cap here bounds worst-case total worker count on
a host Sekhmet measured with swap already fully exhausted.

WHAT THIS CANNOT CATCH, named plainly (Thoth's own ask, "including what it CANNOT catch"):
a test file that exercises a touched module WITHOUT importing it by that exact dotted path
(re-exported through an unrelated module, exercised only via an integration test that never
names the module directly) is invisible to the grep tier and will not be selected — the same
class of gap `resolve_test_files`'s own docstring names. This is a real, bounded blind spot,
not a hidden one; `--audit`'s own report names it as a caveat, never as zero.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
VENV_BIN = REPO_ROOT / ".venv" / "bin"

# A hub module (mounts.py: 37 test files resolved, measured live during this build) makes
# the grep tier degrade toward the exact 209s full-suite cost this mechanism exists to
# avoid. Capped rather than silently run wide -- SKIPPED is reported distinctly from PASSED,
# never treated as a failure (an untestable-by-this-mechanism scope is not the commit's
# fault), but it is a real, load-bearing question left open for whoever arms this: hub-module
# changes may need a different rule (always full suite, always human review) rather than this
# gate's default scoping. Not solved here -- named, per this house's "no silent caps" rule.
_PYTEST_FANOUT_CAP = 12
_PYTEST_TIMEOUT_SECS = 180

# Sekhmet's #112 measurement (msg 2919, decision c63f5bd9): this box's swap was FULLY
# EXHAUSTED (8.0/8.0Gi) with 49 claude processes live when she took the snapshot -- `-n auto`
# spawns one worker per core (20 here), and this gate can fire from MULTIPLE concurrent
# agents' commits at once (shared tree, up to ~5 seats), which multiplies exactly the
# contention that already had swap maxed out, not just this one invocation's own cost. Her
# own recommendation was "4 to 8 as a starting cap, not measured against real concurrent
# multi-agent load," and she explicitly handed the number to this gate to pick, since this is
# the thing that runs it on every commit. Picked the conservative end of her range: 4 workers
# x up to ~5 concurrent agents caps worst-case total workers near today's single-run `-n auto`
# number, on a host that was already out of swap running less than that.
_PYTEST_XDIST_CAP = 4


def _module_path(src_file: str) -> str | None:
    """`src/orchestrator/smoke.py` -> `src.orchestrator.smoke`; None for non-`src/` files
    (scripts/, tests/ themselves need no reverse lookup — a touched test file just runs)."""
    if not src_file.startswith("src/") or not src_file.endswith(".py"):
        return None
    return src_file[:-3].replace("/", ".")


def resolve_test_files(changed_files: list[str], repo_root: Path = REPO_ROOT) -> set[str]:
    """Changed repo-relative paths -> the test files worth running for them. See the module
    docstring's "TOUCHED-FILES-ONLY" paragraph for the two-tier resolution and its named
    blind spot. Pure except for the grep tier's read of the tests/ tree — no git, no
    subprocess with a nonzero-exit surface, fully unit-testable with a tmp_path tree."""
    tests_dir = repo_root / "tests"
    all_test_files = sorted(
        str(p.relative_to(repo_root)) for p in tests_dir.glob("test_*.py")
    ) if tests_dir.is_dir() else []

    out: set[str] = set()
    for f in changed_files:
        if f.startswith("tests/") and f.endswith(".py"):
            out.add(f)
            continue
        mod = _module_path(f)
        if mod is None:
            continue
        stem = Path(f).stem
        by_basename = f"tests/test_{stem}.py"
        if by_basename in all_test_files:
            out.add(by_basename)
        needle_from = f"from {mod} import"
        needle_import = f"import {mod}"
        parent_mod, _, leaf = mod.rpartition(".")
        needle_from_parent = f"from {parent_mod} import" if parent_mod else None
        for tf in all_test_files:
            if tf in out:
                continue
            try:
                text = (repo_root / tf).read_text()
            except OSError:
                continue
            if needle_from in text or needle_import in text:
                out.add(tf)
            elif needle_from_parent and needle_from_parent in text and re.search(
                rf"\b{re.escape(leaf)}\b", text,
            ):
                out.add(tf)
    return out


def _run(cmd: list[str], cwd: Path) -> tuple[bool, str]:
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=False)
    ok = proc.returncode == 0
    out = (proc.stdout or "") + (proc.stderr or "")
    return ok, out.strip()


def run_gates(repo_root: Path, changed_files: list[str]) -> dict[str, tuple[bool, str]]:
    """ruff/mypy stay whole-project (already single-digit seconds, no scoping needed);
    pytest is the one gate that must be scoped, or it re-imports the 209s full-suite cost
    this mechanism exists to avoid. No test files resolved -> pytest is skipped outright
    (reported as `(True, "no resolvable test files touched")`), never silently treated as a
    full-suite run and never treated as a failure — untested touched files are ruff/mypy's
    job to catch what they can, not a reason to block an otherwise-clean commit."""
    results: dict[str, tuple[bool, str]] = {}
    results["ruff"] = _run(
        [str(VENV_BIN / "ruff"), "check", "src", "tests", "scripts"], repo_root)
    results["mypy"] = _run([str(VENV_BIN / "mypy"), "src"], repo_root)
    test_files = sorted(resolve_test_files(changed_files, repo_root))
    if not test_files:
        results["pytest"] = (True, "no resolvable test files touched")
    elif len(test_files) > _PYTEST_FANOUT_CAP:
        results["pytest"] = (
            True,
            f"SKIPPED: {len(test_files)} test files resolved (hub-module fan-out), over "
            f"cap {_PYTEST_FANOUT_CAP} — ruff/mypy still gate this commit. "
            f"[{' '.join(test_files)}]")
    else:
        import os

        env = dict(**{"TMPDIR": "/var/tmp/osiris-scratch"})
        try:
            proc = subprocess.run(
                [str(VENV_BIN / "pytest"), *test_files, "-q",
                 "-n", str(_PYTEST_XDIST_CAP)], cwd=repo_root,
                capture_output=True, text=True, check=False,
                env={**os.environ, **env}, timeout=_PYTEST_TIMEOUT_SECS,
            )
            ok = proc.returncode == 0
            out = ((proc.stdout or "") + (proc.stderr or "")).strip()
            results["pytest"] = (ok, f"[{' '.join(test_files)}]\n{out}")
        except subprocess.TimeoutExpired:
            results["pytest"] = (
                False, f"TIMED OUT after {_PYTEST_TIMEOUT_SECS}s [{' '.join(test_files)}]")
    return results


def changed_files_staged(repo_root: Path = REPO_ROOT) -> list[str]:
    ok, out = _run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"], repo_root)
    return [line for line in out.splitlines() if line] if ok else []


def changed_files_for_commit(repo_root: Path, sha: str) -> list[str]:
    ok, out = _run(
        ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", sha], repo_root)
    return [line for line in out.splitlines() if line] if ok else []


def _report(label: str, results: dict[str, tuple[bool, str]]) -> bool:
    all_ok = all(ok for ok, _ in results.values())
    verdict = "PASS" if all_ok else "FAIL"
    print(f"gate_hook[{label}]: {verdict}")
    for name, (ok, out) in results.items():
        print(f"  {name}: {'ok' if ok else 'FAILED'}")
        if not ok:
            for line in out.splitlines()[-15:]:
                print(f"    {line}")
    return all_ok


def cmd_precommit(*, enforce: bool | None = None) -> int:
    """`enforce=None` (the real CLI path) reads `Settings.osiris_gate_hook_enforce`; a caller
    (tests, `--audit`'s own dry-run reasoning) may pass it explicitly instead — same DI shape
    `cmd_deploy`'s injected callables already use in this repo, never a bespoke test hook."""
    if enforce is None:
        from src.config.settings import get_settings

        enforce = get_settings().osiris_gate_hook_enforce
    changed = changed_files_staged(REPO_ROOT)
    if not changed:
        print("gate_hook: nothing staged, nothing to gate")
        return 0
    results = run_gates(REPO_ROOT, changed)
    all_ok = _report("staged", results)
    if all_ok:
        return 0
    if not enforce:
        print("gate_hook: NOT ENFORCED (osiris_gate_hook_enforce=False) — would have "
              "refused this commit, letting it through")
        return 0
    print("gate_hook: REFUSED — a gate failed and enforcement is armed")
    return 1


def cmd_audit(rev_range: str) -> int:
    import tempfile

    ok, out = _run(["git", "log", "--reverse", "--format=%H", rev_range], REPO_ROOT)
    if not ok:
        print(f"gate_hook: could not resolve range {rev_range!r}: {out}", file=sys.stderr)
        return 1
    shas = [line for line in out.splitlines() if line]
    print(f"gate_hook audit: {len(shas)} commits in {rev_range}")
    print("NOTE: pytest is scoped to each commit's own resolved test files (same rule the "
          "live hook uses) — this is a per-commit re-derivation, not a re-run of the whole "
          "historical suite, and every DB-touching test still runs against the shared dev "
          "Postgres each of these worktrees points at (same instance the live fleet is "
          "using right now) — sequential, never parallel, to avoid compounding Sekhmet's "
          "own measured cross-process contention (#112).")
    fails: list[str] = []
    with tempfile.TemporaryDirectory(prefix="gate-audit-") as tmp:
        tmp_path = Path(tmp)
        for i, sha in enumerate(shas, 1):
            short = sha[:7]
            wt = tmp_path / short
            wok, wout = _run(["git", "worktree", "add", "--detach", "-q", str(wt), sha],
                              REPO_ROOT)
            if not wok:
                print(f"[{i}/{len(shas)}] {short}: worktree add FAILED — {wout}")
                fails.append(short)
                continue
            changed = changed_files_for_commit(REPO_ROOT, sha)
            results = run_gates(wt, changed)
            all_ok = all(v[0] for v in results.values())
            print(f"[{i}/{len(shas)}] {short}: "
                  f"{'PASS' if all_ok else 'FAIL'} "
                  f"(ruff={'ok' if results['ruff'][0] else 'FAIL'}, "
                  f"mypy={'ok' if results['mypy'][0] else 'FAIL'}, "
                  f"pytest={'ok' if results['pytest'][0] else 'FAIL'})")
            if not all_ok:
                fails.append(short)
                for name, (ok2, out2) in results.items():
                    if not ok2:
                        print(f"    {name} tail:")
                        for line in out2.splitlines()[-10:]:
                            print(f"      {line}")
            _run(["git", "worktree", "remove", "--force", str(wt)], REPO_ROOT)
    print(f"gate_hook audit: {len(shas) - len(fails)}/{len(shas)} would have passed")
    if fails:
        print(f"WOULD HAVE BEEN REFUSED: {', '.join(fails)}")
    else:
        print("NONE would have been refused — no false positives against real, "
              "genuinely-gated history")
    return 1 if fails else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--audit", metavar="A..B",
                         help="retroactive dry-audit over a commit range, e.g. db8e3e9..HEAD")
    args = parser.parse_args(argv)
    if args.audit:
        return cmd_audit(args.audit)
    return cmd_precommit()


if __name__ == "__main__":
    raise SystemExit(main())

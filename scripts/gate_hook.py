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
unshippable — everyone disables it within a day). `resolve_test_files` is the base
resolution: exact basename match first (`src/x/y.py` <-> `tests/test_y.py`, ~59% of this
repo's modules), then `_file_references_module` walks the AST of every other `tests/` file
for a REAL import of the module (`from mod import ...`, bare `import mod`, or the
package-level `from <parent> import <leaf>` form) to catch the rest — never a text/regex
match against the file body, and never a full-suite fallback. Was a substring-plus-
word-boundary regex until Thoth DM 2948's own audit caught it producing a false match (a
module name that happened to appear as a bare word inside an unrelated prose string, not an
import — the same defect class as #104's "stop" matching the filename osiris_stophook.py).
`classify_test_files` then splits that set into
DIRECT (exercises the touched module's own logic — always runs, no cap) and FIXTURE-ONLY
(imports the module purely to seed a row for some OTHER module's test — subject to
`_PYTEST_FANOUT_CAP`), self-calibrated per touched module by which imported names are a
strict majority of the candidate set (Khnum's own empirical mounts.py split, msg 2941: 27 of
34 resolved files import ONLY `save_mount` and never call it as the function under test; 7
import something else and do). A hub-module touch is no longer zero coverage — only the
files that were never testing that module's own logic get skipped past the cap. A touched
file with truly no resolvable test (scripts/, `__init__.py`, deploy config) correctly runs no
pytest at all; ruff/mypy still gate it, whole-project, because both are already fast enough
(single-digit seconds) to not need scoping. Every scoped pytest run passes `-n 4` explicitly
(`_PYTEST_XDIST_CAP`, msg 2919) rather than inheriting whatever `-n auto` default lands in
pyproject.toml — this gate can fire from multiple concurrent agents' commits at once, and a
fixed small cap here bounds worst-case total worker count on a host Sekhmet measured with
swap already fully exhausted.

WHAT THIS CANNOT CATCH, named plainly (Thoth's own ask, "including what it CANNOT catch"):
a test file that exercises a touched module WITHOUT importing it by that exact dotted path
(re-exported through an unrelated module, exercised only via an integration test that never
names the module directly) is invisible to both the grep tier and the DIRECT/FIXTURE-ONLY
split built on top of it — the same class of gap `resolve_test_files`'s own docstring names,
inherited rather than solved by `classify_test_files` (Khnum's own honest caveat, msg 2941:
"N direct" is a verified FLOOR, never a ceiling). This is a real, bounded blind spot, not a
hidden one; `--audit`'s own report names it as a caveat, never as zero.

THE STAGE-RACE GUARD (Thoth DM 3005/3012, thread 3005) — A TOCTOU GUARD, NOT A #117 CURE.
Decision 96463307 investigated whether #117's shape 3 ("a status flag outliving the state it
described") is mechanizable and found NO general path — this is not that. It is one narrow,
separately-motivated mechanism: Practice 81cab2f4/decision b1863e56 already documented, in
prose, that "a verify-then-commit check on a shared git index is a SNAPSHOT, not a
GUARANTEE — the index is shared mutable state, so a concurrent agent's own `git add` can land
between your `git diff --staged` check and your `git commit`." Reproduced live before this
was built (throwaway repo, a pre-commit hook that sleeps 5s): a SECOND process's `git add`
during the FIRST process's hook execution lands, silently, INSIDE the first commit — git does
NOT hold the index lock across a hook's own run. This means gate_hook's own gate execution
(up to `_PYTEST_TIMEOUT_SECS`) is itself a live, previously-unrecognized EXTENSION of exactly
this race window, not merely a place that could report on it.

`cmd_precommit` now hashes the staged diff (`_staged_diff_digest`, full `git diff --cached`
text, not just names — file-list alone would miss a same-file content race) BEFORE calling
`run_gates` and AGAIN immediately after. A mismatch means the tree the gates just checked is
not the tree about to be committed — reported as its own verdict word, RACE, distinct from
FAILED/TIMEOUT/SKIPPED/ok (never folded into "a gate failed," the exact vocabulary-collapse
shape #117 piece (a) exists to catch) — and refused under the SAME `osiris_gate_hook_enforce`
flag as every other gate here, never a separate arming bit.

NAMED HONESTLY, NOT OVERSOLD: this closes the window WHILE gates run (proven real, and
probably the LARGER window in practice — up to 180s vs. a human/agent's near-instant
diff-then-commit pair). It does NOT close the window BEFORE `git commit` is even invoked —
the exact timing of b1863e56's own original incident, where the race landed between the
agent's manual `git diff --staged` review and their separate `git commit` call, before this
hook's process exists at all. Nothing running only after `git commit` starts can see before
that point. Practice 81cab2f4's warning therefore still stands for that narrower residual
case; its post-commit repair recipe (git reset --soft HEAD^ + selective unstage) becomes
unnecessary only for races this guard actually catches, not for the pre-invocation gap.

#117's SECOND RULE, A NUDGE NOT A GATE (Khnum's investigation d0ab1b0b, proposal msg 2983,
Thoth's routing msg 2984/2985) — piece (a) of #117 (tests/test_receipt_vocabulary.py, commit
7ef3bc1) is the mechanical, automatable half: a behavioral test that a status vocabulary never
collapses "didn't happen" onto "happened cleanly" (the exact 0044671 defect it was built to
catch). Piece (b), DERIVATION TRACE, is explicitly NOT automatable the same way — Khnum's own
honest split: whether a receipt field is genuinely templated from a locally-computed variable
that was forced through the real branch/exception/return path, versus a string literal or a
raw `args.get()`/parameter echo standing in for a value that should have been resolved first,
is a judgment call, not a syntax check. So this prints a QUESTION, never a verdict, whenever a
STAGED diff adds or modifies a function with a receipt-shaped signature (`@mcp.tool()` or
`-> dict[str, Any]`) — `receipt_shaped_touches` below, wired only into `cmd_precommit`'s
report (a retroactive `--audit` over already-merged history has no live author to hand the
question to). MECHANICAL TRIGGER ONLY: it fires on the SHAPE, never inspects whether the body
actually has the problem — same honest split as (a)/(b) generally, and it can never affect
`ok`/the exit code, by construction (the printed block carries no truth value to fold into
`_status_word`).
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import re
import subprocess
import sys
from pathlib import Path

from scripts.push_guard import git_common_dir

REPO_ROOT = Path(__file__).resolve().parent.parent
# NOT `REPO_ROOT / ".venv" / "bin"` (thread 64c6b197): when this module's own copy is a
# worktree's (a different tree than the interpreter running it -- worktrees have no
# materialized .venv, .gitignore excludes it), REPO_ROOT is the worktree but the live
# interpreter is still whichever venv actually launched this process. `sys.executable` names
# that unconditionally, so VENV_BIN stays correct whether __file__ lives in the main
# checkout (the plain CLI/--audit path) or in a worktree (the pre-commit hook path, which
# already resolves the interpreter from the main checkout's venv before exec'ing this file).
# DELIBERATELY NOT `.resolve()`d (caught live by the worktree acceptance test, thread
# 64c6b197): this is a uv-managed venv, and `.venv/bin/python` is a symlink straight to the
# shared uv toolchain (`~/.local/share/uv/python/.../bin/python3.12`), not a copy -- fully
# resolving it walks straight past the venv boundary to a directory with no ruff/mypy/pytest
# at all. `sys.executable` is already the absolute `.venv/bin/python3` path as actually
# invoked (verified: CPython does not resolve it), which is exactly the sibling directory
# those tools live in.
VENV_BIN = Path(sys.executable).parent

# A hub module (mounts.py: 34-37 test files resolved, measured live -- the exact count shifts
# a little commit to commit) makes the grep tier degrade toward the exact 209s full-suite
# cost this mechanism exists to avoid. Applies ONLY to the FIXTURE-ONLY tier that
# `classify_test_files` splits out (Khnum's msg 2941 refinement) -- the DIRECT tier (files
# that actually exercise the touched module, 7 of mounts.py's 34) always runs regardless of
# this cap, so a hub-module touch no longer means zero coverage, only that files merely
# seeding a row for some OTHER module's test get skipped. SKIPPED is reported distinctly from
# PASSED, never treated as a failure.
_PYTEST_FANOUT_CAP = 12
_PYTEST_TIMEOUT_SECS = 180

# obligation a3c71bf5: the omitted-files summary is always the FIRST line of a SKIPPED
# detail (see run_gates above) -- printing a fixed char cap keeps one pathological hub-module
# fan-out from producing an unreadable wall of text, but the cap must never look like the
# whole list (Khnum's census_blind rule: "could not show" must never render as "nothing to
# show") -- see `_print_skip_detail` below.
_SKIP_SUMMARY_DISPLAY_CAP = 2000

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


def _file_references_module(test_file: Path, mod: str) -> bool:
    """True iff this test file has a REAL import of `mod` — `from mod import ...`, bare
    `import mod` (aliased or not), or the package-level form `from <parent of mod> import
    <leaf of mod>`. AST-based, never a text/regex match against the whole file body: a bare
    word inside a string literal or comment is not an import (Thoth DM 2948 — the
    test_mailbox.py false match: it imported an unrelated name from the SAME PACKAGE and
    separately contained the bare word "smoke" only in a prose string, and the old
    substring-plus-word-boundary heuristic conflated the two into a false signal — the same
    defect class as #104's "stop" matching the filename osiris_stophook.py). Walks the WHOLE
    file, not just top-level statements, matching `_module_imports`'s own reach."""
    try:
        tree = ast.parse(test_file.read_text())
    except (OSError, SyntaxError):
        return False
    parent_mod, _, leaf = mod.rpartition(".")
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == mod:
                return True
            if parent_mod and node.module == parent_mod and any(
                alias.name == leaf for alias in node.names
            ):
                return True
        elif isinstance(node, ast.Import) and any(
            alias.name == mod for alias in node.names
        ):
            return True
    return False


def resolve_test_files(changed_files: list[str], repo_root: Path = REPO_ROOT) -> set[str]:
    """Changed repo-relative paths -> the test files worth running for them. See the module
    docstring's "TOUCHED-FILES-ONLY" paragraph for the two-tier resolution and its named
    blind spot. Pure except for the AST tier's read of the tests/ tree — no git, no
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
        for tf in all_test_files:
            if tf in out:
                continue
            if _file_references_module(repo_root / tf, mod):
                out.add(tf)
    return out


def _module_imports(test_file: Path, mod: str) -> set[str]:
    """Every name this test file imports FROM `mod` specifically (`from mod import a, b as
    c` -> `{"a", "b"}`, the ORIGINAL name not the local alias) — walks the WHOLE file, not
    just top-level statements, since real examples import inside function bodies (`from src
    import mcp_server as srv` inside a test function, Khnum's own msg 2941 grep). Empty set
    if the file doesn't import from `mod` at all, or can't be parsed."""
    try:
        tree = ast.parse(test_file.read_text())
    except (OSError, SyntaxError):
        return set()
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == mod:
            names.update(alias.name for alias in node.names)
    return names


def classify_test_files(
    changed_files: list[str], repo_root: Path = REPO_ROOT,
) -> tuple[set[str], set[str]]:
    """(direct, fixture_only) — DIRECT files exercise a touched module's own logic and must
    always run regardless of the fan-out cap; FIXTURE-ONLY files import that module purely to
    seed test data for something else (Khnum's empirical finding on mounts.py, msg 2941:
    27 of 34 resolved files import ONLY `save_mount`, never call it as the function under
    test — 7 import something else and DO exercise real logic). SELF-CALIBRATING per touched
    module, no hardcoded per-module allow-list: a name imported by a STRICT MAJORITY of a
    module's own candidate files is treated as a common/fixture name; a file importing
    anything outside that common set is DIRECT. Reproduces Khnum's manual mounts.py split
    without a table anyone has to maintain by hand.

    SAME BLIND SPOT AS `resolve_test_files`, inherited rather than solved (Khnum's own honest
    caveat, msg 2941): a file that exercises the touched module only INDIRECTLY (calls a
    function that itself calls the touched module, never imports it by name) reads as
    fixture-only or invisible here exactly as it does there — "N direct" is a verified FLOOR,
    never a ceiling."""
    direct: set[str] = set()
    fixture_only: set[str] = set()
    for f in changed_files:
        if f.startswith("tests/") and f.endswith(".py"):
            direct.add(f)
            continue
        mod = _module_path(f)
        if mod is None:
            continue
        candidates = resolve_test_files([f], repo_root)
        if not candidates:
            continue
        per_file_names = {tf: _module_imports(repo_root / tf, mod) for tf in candidates}
        if len(per_file_names) <= 1:
            # "common" is meaningless with nothing to be common RELATIVE TO -- a lone
            # candidate trivially satisfies any majority threshold against itself, which
            # would wrongly label the only test touching this module as fixture-only.
            direct.update(per_file_names)
            continue
        counts: dict[str, int] = {}
        for names in per_file_names.values():
            for n in names:
                counts[n] = counts.get(n, 0) + 1
        n_files = len(per_file_names)
        common = {n for n, c in counts.items() if c * 2 > n_files}
        for tf, names in per_file_names.items():
            if names and names <= common:
                fixture_only.add(tf)
            else:
                direct.add(tf)
    fixture_only -= direct
    return direct, fixture_only


def _run(cmd: list[str], cwd: Path) -> tuple[bool, str]:
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=False)
    ok = proc.returncode == 0
    out = (proc.stdout or "") + (proc.stderr or "")
    return ok, out.strip()


_RATCHET_TEST_NODEID = (
    "tests/test_tool_contract_diet.py::test_tool_contract_stays_under_the_ceiling"
)


def _is_merge_context(repo_root: Path) -> bool:
    """True for the commit-about-to-land in TWO cases: (1) live pre-commit, mid `git merge`
    — `MERGE_HEAD` exists from the moment a merge starts conflict-free until the merge
    commit itself lands, the standard git signal for "this commit, once made, will have 2+
    parents"; (2) `--audit`'s retroactive replay, where the commit already exists at HEAD in
    its own disposable worktree — read its actual parent count instead. Scoped this narrow
    on purpose (thread 1a0f91bb): a NON-merge commit that happens to exceed the ratchet
    ceiling is a real, ordinary regression and must still refuse — this only recognizes the
    one named pattern where the growth landed via a merge whose own follow-up "ratchet:"
    commit (this house's established practice) has not landed yet."""
    ok, out = _run(["git", "rev-parse", "--verify", "-q", "MERGE_HEAD"], repo_root)
    if ok and out.strip():
        return True
    ok, out = _run(["git", "rev-list", "--parents", "-n", "1", "HEAD"], repo_root)
    if not ok or not out.strip():
        return False
    return len(out.split()) > 2  # HEAD's own sha + >=2 parent shas


def _pytest_sole_failure_is_ratchet_ceiling(out: str) -> bool:
    """True iff pytest's own `-q` "FAILED <nodeid> - ..." lines name EXACTLY the ratchet
    ceiling test and nothing else. Never a blanket exemption for merge commits generally —
    a merge that ALSO breaks something else (#133's own real specimens, a74ce7a/ff72377)
    must still refuse; only this one named, narrow failure shape is eligible."""
    failed = [line for line in out.splitlines() if line.startswith("FAILED ")]
    if len(failed) != 1:
        return False
    return failed[0].split(" ", 2)[1] == _RATCHET_TEST_NODEID


def _module_level_import_names(tree: ast.Module) -> set[str]:
    """Every name this module binds via a TOP-LEVEL `import`/`from ... import` — the
    population a function-local import can shadow. Never a nested import (those belong to
    their own scope, not the module's)."""
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            names.update(alias.asname or alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.update(alias.asname or alias.name for alias in node.names)
    return names


def _own_scope_local_imports(
    func: ast.FunctionDef | ast.AsyncFunctionDef,
) -> list[tuple[str, int]]:
    """(name, lineno) for every name bound by an `import`/`from ... import` DIRECTLY in
    this function's own scope — walks the whole body (through if/for/while/try/with) but
    does NOT descend into a nested def/class/lambda, which each own a separate scope with
    its own local imports. Python's scoping rule (the actual mechanism behind the bug this
    lint exists to catch): any assignment anywhere in a function's body, at any nesting
    depth of a non-scope-creating block, makes that name local to the WHOLE function —
    not merely from that line onward."""
    out: list[tuple[str, int]] = []

    def walk(node: ast.AST) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef,
                                   ast.ClassDef, ast.Lambda)):
                continue  # a separate scope — its own local imports are its own business
            if isinstance(child, ast.Import):
                out.extend(
                    (alias.asname or alias.name.split(".")[0], child.lineno)
                    for alias in child.names)
            elif isinstance(child, ast.ImportFrom):
                out.extend(
                    (alias.asname or alias.name, child.lineno) for alias in child.names)
            else:
                walk(child)

    walk(func)
    return out


def _reads_name_before(func: ast.AST, name: str, before_lineno: int) -> bool:
    """True iff `name` is READ (`ast.Name` in `Load` context) anywhere in `func` — INCLUDING
    nested def/lambda bodies, since those close over this scope by Python's own LEGB rule —
    at a line strictly before `before_lineno`. This is the execution-order discriminator
    (ruling 2f7e1588): a shadow that is never read before its own import line is harmless
    (the common case, 18 of 19 candidates in the fleet-wide audit that motivated this);
    a shadow that IS read first — usually from a nested function called earlier in the same
    body — is a live NameError/UnboundLocalError waiting for that code path to run, the
    exact shape mailbox.py's send_message shipped with (a nested `_stamp_threads` closure,
    called before a redundant local `from datetime import ...` a few lines later)."""
    for node in ast.walk(func):
        if (isinstance(node, ast.Name) and node.id == name
                and isinstance(node.ctx, ast.Load) and node.lineno < before_lineno):
            return True
    return False


def shadow_before_use_violations(repo_root: Path, changed_files: list[str]) -> dict[str, list[str]]:
    """{file: [\"funcname shadows NAME at line N, read earlier at line M\", ...]} for every
    STAGED/touched `.py` file where a function-local import shadows a module-level import
    of the SAME name, AND that name is read somewhere in the function (including a nested
    closure) before the local import's own line — the ONE narrow, execution-order-based
    shape ruling 2f7e1588 asks for, never the mere presence of a shadow (which the
    fleet-wide audit measured at 19 candidates, 18 harmless, this being the one real bug).
    TOUCHED-FILES-ONLY, same law as `resolve_test_files` — a cheap, pure AST walk, no
    subprocess, no full-project cost. A file that fails to parse is skipped, never crashes
    the gate over it (ruff already gates syntax)."""
    out: dict[str, list[str]] = {}
    for f in changed_files:
        if not f.endswith(".py"):
            continue
        path = repo_root / f
        if not path.is_file():
            continue
        try:
            tree = ast.parse(path.read_text())
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue
        module_names = _module_level_import_names(tree)
        if not module_names:
            continue
        findings: list[str] = []
        for func in ast.walk(tree):
            if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for name, lineno in _own_scope_local_imports(func):
                if name not in module_names:
                    continue
                if _reads_name_before(func, name, lineno):
                    findings.append(
                        f"{func.name} shadows {name!r} with a local import at line "
                        f"{lineno} — {name!r} is read earlier in the same function "
                        f"(module level already imports it; delete the local import)")
        if findings:
            out[f] = findings
    return out


def run_gates(repo_root: Path, changed_files: list[str]) -> dict[str, tuple[bool, str]]:
    """ruff/mypy stay whole-project (already single-digit seconds, no scoping needed);
    pytest is the one gate that must be scoped, or it re-imports the 209s full-suite cost
    this mechanism exists to avoid. `classify_test_files` splits the resolved set into DIRECT
    (always run, no cap) and FIXTURE-ONLY (subject to `_PYTEST_FANOUT_CAP`) — a hub-module
    touch no longer means zero coverage, only that the files merely seeding a row for some
    OTHER module's test get skipped past the cap.

    A GATE THAT CANNOT DISTINGUISH "PASSED" FROM "NOT RUN" IS A DOCUMENT THAT COMPILES
    (Thoth DM 2957, the sharpest finding of the night — found INSIDE this tool, not by it:
    the first two retroactive audits reported "17/18 pass" as a real measurement when the
    flat fan-out cap was silently skipping pytest entirely for hub-module-heavy commits and
    the skip path returned `ok=True`, indistinguishable from a genuine pass). Every message
    that names an omission (over-cap fixture-only files skipped, whether alongside a real run
    of the direct tier or with nothing running at all) starts with the literal word "SKIPPED"
    — `_status_word` renders that as its own word, never folded into "ok". `ok` itself STAYS
    True for a skip (the enforcement semantics are unchanged and correct: an omitted file is
    ruff/mypy's job, not a reason to refuse an otherwise-clean commit) — only the REPORTING
    is fixed, because a caller reading "ok" as "verified" was the actual defect, not the
    cap's own cost-saving choice to omit low-signal files. Neither tier resolving anything
    AND nothing omitted -> `(True, "no resolvable test files touched")`, the one case that
    is honestly a plain, unqualified "ok" — there was nothing to skip in the first place."""
    results: dict[str, tuple[bool, str]] = {}
    results["ruff"] = _run(
        [str(VENV_BIN / "ruff"), "check", "src", "tests", "scripts"], repo_root)
    results["mypy"] = _run([str(VENV_BIN / "mypy"), "src"], repo_root)
    shadow_hits = shadow_before_use_violations(repo_root, changed_files)
    if shadow_hits:
        lines = [f"{f}: {msg}" for f, msgs in sorted(shadow_hits.items()) for msg in msgs]
        results["shadow_lint"] = (False, "\n".join(lines))
    else:
        results["shadow_lint"] = (True, "")
    direct, fixture_only = classify_test_files(changed_files, repo_root)
    selected = set(direct)
    omitted = ""
    if len(fixture_only) > _PYTEST_FANOUT_CAP:
        omitted = (f"{len(fixture_only)} fixture-only files (hub-module fan-out, over cap "
                   f"{_PYTEST_FANOUT_CAP}): [{' '.join(sorted(fixture_only))}]")
    else:
        selected |= fixture_only
    if not selected:
        results["pytest"] = (
            True, f"SKIPPED — nothing ran; omitted {omitted}" if omitted
            else "no resolvable test files touched")
    else:
        import os

        test_files = sorted(selected)
        env = dict(**{"TMPDIR": "/var/tmp/osiris-scratch"})

        def _run_pytest() -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                [str(VENV_BIN / "pytest"), *test_files, "-q",
                 "-n", str(_PYTEST_XDIST_CAP)], cwd=repo_root,
                capture_output=True, text=True, check=False,
                env={**os.environ, **env}, timeout=_PYTEST_TIMEOUT_SECS,
            )

        # TOLERANCE, NOT BLINDNESS (f1f8ad62, ruling f61cad1b: the ambient-load limb LEANS
        # true -- #197's own root causes reproduce under ordinary host contention with no
        # concurrent gate run required -- so a single hang is not proof of a real problem;
        # witness 3's own specimen, commit 40dfcca, was CORRECT and did not reproduce on
        # retry). retried ONE extra attempt only, on TIMEOUT alone -- never on a genuine
        # assertion failure, which still refuses on the first attempt with no second chance.
        # The retry's own outcome is never silently folded into a plain "ok": passing only
        # on the second attempt gets its OWN status word (PASSED-ON-RETRY, `_status_word`
        # below), reported exactly like SKIPPED/RATCHET-DEBT -- a receipt that cannot tell
        # "passed first try" from "passed on retry" has stopped being a gate. Timing out
        # TWICE in a row is a stronger signal than once and still refuses, unconditionally.
        retried = False
        try:
            try:
                proc = _run_pytest()
            except subprocess.TimeoutExpired:
                retried = True
                proc = _run_pytest()
            ok = proc.returncode == 0
            out = ((proc.stdout or "") + (proc.stderr or "")).strip()
            if ok and retried:
                tail = f" (also omitted {omitted})" if omitted else ""
                results["pytest"] = (
                    True,
                    f"PASSED ON RETRY — timed out after {_PYTEST_TIMEOUT_SECS}s on the "
                    f"first attempt, then passed clean on an immediate second attempt "
                    f"[{' '.join(test_files)}]{tail}. NOT a plain pass — the first "
                    f"attempt's hang is real signal (f1f8ad62); a PATTERN of retries "
                    f"across commits, not just one, is what would prove the ambient-load "
                    f"limb rather than merely lean toward it.\n{out}")
            elif ok and omitted:
                # ran clean, but NOT the whole relevant set -- must not read as a plain "ok"
                results["pytest"] = (
                    True, f"SKIPPED (partial) — ran {len(test_files)} clean, omitted "
                          f"{omitted}\n{out}")
            elif (not ok and _pytest_sole_failure_is_ratchet_ceiling(out)
                    and _is_merge_context(repo_root)):
                # thread 1a0f91bb, decided: the gate TOLERATES the split rather than
                # demanding the ratchet move in the same commit as a merge (impossible to
                # author freely into a merge commit's own tree without amending it) -- never
                # refused, but never a plain "ok" either, so the debt cannot go unnoticed.
                results["pytest"] = (
                    True,
                    "RATCHET-DEBT — merge commit exceeds the tool-contract ceiling before "
                    "its own follow-up \"ratchet:\" commit lands (thread 1a0f91bb's named "
                    "pattern; this house's practice raises the ceiling as a deliberate "
                    "later commit, never atomically with the growth-causing merge). NOT "
                    "refused — but the VERY NEXT commit must raise TOOL_CONTRACT_CEILING_"
                    f"CHARS, or this stops being tolerated.\n{out}")
            else:
                tail = f" (also omitted {omitted})" if omitted else ""
                retry_note = " (unchanged after an immediate retry)" if retried else ""
                results["pytest"] = (
                    ok, f"[{' '.join(test_files)}]{tail}{retry_note}\n{out}")
        except subprocess.TimeoutExpired:
            # A DISTINCT WORD FROM "FAILED" (Thoth DM 2948, same discipline as smoke_chrome's
            # timeout-vs-refusal split): a hang past _PYTEST_TIMEOUT_SECS is not proven to be
            # the code's fault on its own -- but TWO timeouts in a row (the retry above
            # exhausted, this is the second) is a materially stronger signal than one, and
            # still refuses unconditionally. `_status_word` below renders this as TIMEOUT,
            # never FAILED.
            tail = f" (also omitted {omitted})" if omitted else ""
            results["pytest"] = (
                False,
                f"TIMED OUT TWICE — {_PYTEST_TIMEOUT_SECS}s on the first attempt AND an "
                f"immediate retry (tolerance exhausted, f1f8ad62) under real ambient fleet "
                f"load (not a proven code failure -- see the DB-contention negative "
                f"control) [{' '.join(test_files)}]{tail}")
    return results


def _status_word(ok: bool, detail: str) -> str:
    """"ok" / "SKIPPED" / "TIMEOUT" / "FAILED" / "RATCHET-DEBT" / "PASSED-ON-RETRY" — never
    collapses a not-run result into the same word as a genuine pass (Thoth DM 2957), never
    collapses a hang into the same word as a real assertion failure (Thoth DM 2948), never
    collapses a known, narrowly-scoped ratchet-lag pass-through into a plain "ok" either
    (thread 1a0f91bb), and never collapses a pass that only happened on a timeout retry into
    the same "ok" as a clean first attempt (f1f8ad62's tolerance remedy, ruling f61cad1b) —
    a receipt that cannot tell "passed first try" from "passed on retry" has stopped being a
    gate."""
    if not ok:
        return "TIMEOUT" if detail.startswith("TIMED OUT") else "FAILED"
    if detail.startswith("SKIPPED"):
        return "SKIPPED"
    if detail.startswith("RATCHET-DEBT"):
        return "RATCHET-DEBT"
    if detail.startswith("PASSED ON RETRY"):
        return "PASSED-ON-RETRY"
    return "ok"


def changed_files_staged(repo_root: Path = REPO_ROOT) -> list[str]:
    ok, out = _run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"], repo_root)
    return [line for line in out.splitlines() if line] if ok else []


def _staged_diff_digest(repo_root: Path = REPO_ROOT) -> str:
    """SHA-256 of the full staged patch text (`git diff --cached`, CONTENT not just names —
    Practice 81cab2f4's own point: a same-file content race would show identical file lists
    with different bodies). A failed `git diff` hashes the empty string, same as "nothing
    staged" — comparing two digests never needs a separate ok-flag branch."""
    ok, out = _run(["git", "diff", "--cached"], repo_root)
    return hashlib.sha256((out if ok else "").encode()).hexdigest()


def changed_files_for_commit(repo_root: Path, sha: str) -> list[str]:
    ok, out = _run(
        ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", sha], repo_root)
    return [line for line in out.splitlines() if line] if ok else []


_HUNK_HEADER = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")


def _added_line_ranges(repo_root: Path, file: str) -> list[tuple[int, int]]:
    """New-file line ranges this STAGED diff actually adds or modifies, read from `git diff
    --cached -U0`'s own hunk headers (`@@ -a,b +c,d @@` -> the `+c,d` side only) — never the
    whole file, so a function merely sitting near an edit is never wrongly flagged as
    touched. A hunk with `+c,0` (pure deletion at that point, nothing added on the new-file
    side) contributes no range."""
    ok, out = _run(["git", "diff", "--cached", "-U0", "--", file], repo_root)
    if not ok:
        return []
    ranges: list[tuple[int, int]] = []
    for line in out.splitlines():
        m = _HUNK_HEADER.match(line)
        if not m:
            continue
        start = int(m.group(1))
        count = int(m.group(2)) if m.group(2) is not None else 1
        if count == 0:
            continue
        ranges.append((start, start + count - 1))
    return ranges


def _is_receipt_shaped(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """#117's own mechanical trigger (decision 7ca152c1) — decorated `@mcp.tool()` (an MCP
    tool's return value IS the receipt every caller trusts) or annotated `-> dict[str, Any]`
    (a hand-assembled dict receipt, #117's own named specimen shape). Matches the SHAPE only,
    never inspects the body — whether it actually HAS a derivation problem is the judgment
    call the printed question hands to whoever reads it, not this function's to make."""
    for dec in node.decorator_list:
        if (isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute)
                and dec.func.attr == "tool"):
            return True
    ret = node.returns
    if (isinstance(ret, ast.Subscript) and isinstance(ret.value, ast.Name)
            and ret.value.id == "dict"):
        sl = ret.slice
        if isinstance(sl, ast.Tuple) and len(sl.elts) == 2:
            key, val = sl.elts
            if (isinstance(key, ast.Name) and key.id == "str"
                    and isinstance(val, ast.Name) and val.id == "Any"):
                return True
    return False


DERIVATION_TRACE_QUESTION = (
    "DERIVATION TRACE (#117 piece b, decision d0ab1b0b) — a reviewer question, never a gate; "
    "does not affect this commit's PASS/FAIL. For every field the function(s) below return: "
    "is it templated from a locally-computed variable that was actually forced through the "
    "real branch/exception/return path? Or could it be a string literal that isn't, or a raw "
    "args.get()/parameter echo standing in for a value that should have been resolved first?"
)


def receipt_shaped_touches(repo_root: Path, changed_files: list[str]) -> dict[str, list[str]]:
    """{file: [function names]} for every function this STAGED diff adds or modifies (not
    merely a touched FILE — `_added_line_ranges` scopes to the diff's own hunks) whose
    signature is receipt-shaped (`_is_receipt_shaped`). Print-only input to the nudge in
    `cmd_precommit`'s own report; never touches `ok`/the exit code — a mechanical trigger for
    a human/agent judgment call, the same honest split Thoth endorsed for #117 piece (b)
    generally (piece (a), the automatable half, already ships as its own behavioral test,
    tests/test_receipt_vocabulary.py)."""
    out: dict[str, list[str]] = {}
    for f in changed_files:
        if not f.endswith(".py"):
            continue
        path = repo_root / f
        if not path.is_file():
            continue
        try:
            tree = ast.parse(path.read_text())
        except (OSError, SyntaxError):
            continue
        ranges = _added_line_ranges(repo_root, f)
        if not ranges:
            continue
        names: list[str] = []
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not _is_receipt_shaped(node):
                continue
            # start at the FIRST DECORATOR's own line, not `node.lineno` (which is the `def`
            # line even for a decorated function, ast's own behavior since 3.8) -- a diff that
            # only ADDS `@mcp.tool()` above an already-existing function must still count as
            # touching it.
            start = node.decorator_list[0].lineno if node.decorator_list else node.lineno
            end = node.end_lineno if node.end_lineno is not None else node.lineno
            if any(a <= end and start <= b for a, b in ranges):
                names.append(node.name)
        if names:
            out[f] = names
    return out


def _print_skip_detail(out: str, indent: str = "    ") -> None:
    """obligation a3c71bf5, filed while verifying b9045c3: `run_gates` puts the omitted-files
    summary on the FIRST line of a SKIPPED detail, with the pytest run's own (potentially
    long) output following after a newline. The callers here used to print only
    `out.splitlines()[-15:]` -- the LAST N lines of the WHOLE blob -- so a pytest run longer
    than the cap silently scrolled the summary line out entirely: the headline still read
    "SKIPPED (partial)" (honest) but named no files (useless). #117's shape one layer down
    from where it was last caught (the audit's own "17/18 pass" headline, Thoth DM 2957).

    The summary line is therefore ALWAYS printed here, in full, never subject to the tail
    cap below (which still applies to whatever run output follows it, for diagnostic
    context). If the summary line ITSELF is too long to show in full, it is truncated with
    an explicit marker naming how much was cut -- Khnum's census_blind rule: "could not
    show" must never render as "nothing to show." The file COUNT (stated by `run_gates`
    before the bracketed name list) always survives the cap even when the names don't."""
    head, _, body = out.partition("\n")
    if len(head) > _SKIP_SUMMARY_DISPLAY_CAP:
        print(f"{indent}{head[:_SKIP_SUMMARY_DISPLAY_CAP]}")
        print(f"{indent}...TRUNCATED FOR DISPLAY ({len(head) - _SKIP_SUMMARY_DISPLAY_CAP} "
              "more chars elided here -- not silently dropped, just not shown in full)")
    else:
        print(f"{indent}{head}")
    if body.strip():
        for line in body.splitlines()[-15:]:
            print(f"{indent}{line}")


def _report(label: str, results: dict[str, tuple[bool, str]]) -> bool:
    all_ok = all(ok for ok, _ in results.values())
    statuses = {name: _status_word(ok, out) for name, (ok, out) in results.items()}
    if not all_ok:
        verdict = "FAIL"
    elif "SKIPPED" in statuses.values():
        verdict = "PASS (UNVERIFIED — see SKIPPED below, not the same as a real pass)"
    elif "RATCHET-DEBT" in statuses.values():
        verdict = "PASS (RATCHET DEBT — see below, the next commit must raise the ceiling)"
    elif "PASSED-ON-RETRY" in statuses.values():
        verdict = "PASS (PASSED ON RETRY — see below, timed out once under transient host " \
                  "contention then passed clean; f1f8ad62)"
    else:
        verdict = "PASS"
    print(f"gate_hook[{label}]: {verdict}")
    for name, (ok, out) in results.items():
        print(f"  {name}: {statuses[name]}")
        if statuses[name] in ("SKIPPED", "RATCHET-DEBT", "PASSED-ON-RETRY"):
            _print_skip_detail(out)
        elif not ok:
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
    stage_before = _staged_diff_digest(REPO_ROOT)
    results = run_gates(REPO_ROOT, changed)
    stage_after = _staged_diff_digest(REPO_ROOT)
    all_ok = _report("staged", results)
    touches = receipt_shaped_touches(REPO_ROOT, changed)
    if touches:
        print(DERIVATION_TRACE_QUESTION)
        for f, names in sorted(touches.items()):
            print(f"  {f}: {', '.join(sorted(names))}")
    if stage_before != stage_after:
        now = set(changed_files_staged(REPO_ROOT))
        moved = sorted(now ^ set(changed))
        detail = (f"file set changed: {', '.join(moved)}" if moved
                  else "same files, staged content changed")
        print(
            "gate_hook[staged]: RACE — the staged tree changed WHILE the gates above "
            "were running (a concurrent git add landed mid-run — Practice 81cab2f4's "
            "TOCTOU hazard, proven live: git does not hold the index lock across a "
            "pre-commit hook's own execution). The results above describe a tree that "
            f"no longer exists and are not evidence about what is about to be "
            f"committed. {detail}")
        if not enforce:
            print("gate_hook: NOT ENFORCED (osiris_gate_hook_enforce=False) — would "
                  "have refused this commit for a stage race, letting it through")
            return 0
        print("gate_hook: REFUSED — staged content raced with the gate run and "
              "enforcement is armed")
        return 1
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
    timeouts: list[str] = []
    skipped: list[str] = []
    ratchet_debt: list[str] = []
    retried: list[str] = []
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
            statuses = {name: _status_word(*v) for name, v in results.items()}
            # A skip is NEVER folded into "PASS" (Thoth DM 2957) -- it gets its own verdict
            # word even though `all_ok` (the enforcement-relevant bit) is still True.
            if not all_ok:
                verdict = "FAIL"
            elif "SKIPPED" in statuses.values():
                verdict = "UNVERIFIED"
            elif "RATCHET-DEBT" in statuses.values():
                verdict = "RATCHET-DEBT"
            elif "PASSED-ON-RETRY" in statuses.values():
                verdict = "PASSED-ON-RETRY"
            else:
                verdict = "PASS"
            detail = ", ".join(f"{name}={statuses[name]}" for name in sorted(statuses))
            print(f"[{i}/{len(shas)}] {short}: {verdict} ({detail})")
            if not all_ok:
                if statuses["pytest"] == "TIMEOUT" and all(
                    ok for name, (ok, _) in results.items() if name != "pytest"
                ):
                    timeouts.append(short)
                else:
                    fails.append(short)
            elif verdict == "UNVERIFIED":
                skipped.append(short)
            elif verdict == "RATCHET-DEBT":
                ratchet_debt.append(short)
            elif verdict == "PASSED-ON-RETRY":
                retried.append(short)
            for name, (ok2, out2) in results.items():
                if statuses[name] in ("SKIPPED", "RATCHET-DEBT", "PASSED-ON-RETRY"):
                    print(f"    {name} tail:")
                    _print_skip_detail(out2, indent="      ")
                elif not ok2:
                    print(f"    {name} tail:")
                    for line in out2.splitlines()[-10:]:
                        print(f"      {line}")
            _run(["git", "worktree", "remove", "--force", str(wt)], REPO_ROOT)
    settled = (len(shas) - len(fails) - len(timeouts) - len(skipped) - len(ratchet_debt)
               - len(retried))
    print(f"gate_hook audit: {settled}/{len(shas)} would have passed cleanly and VERIFIED")
    if skipped:
        print(f"UNVERIFIED (passed, but at least one gate was skipped — not proof of "
              f"correctness, only absence of a caught problem): {', '.join(skipped)}")
    if ratchet_debt:
        print(f"RATCHET DEBT (merge landed over the tool-contract ceiling before its own "
              f"follow-up ratchet-raise commit — thread 1a0f91bb, not refused but not a "
              f"plain pass): {', '.join(ratchet_debt)}")
    if retried:
        print(f"PASSED ONLY ON RETRY (timed out once under transient host contention then "
              f"passed clean — f1f8ad62, not refused but not a plain first-try pass "
              f"either; a PATTERN here across an audit range is the real signal): "
              f"{', '.join(retried)}")
    if fails:
        print(f"WOULD HAVE BEEN REFUSED (real gate failure): {', '.join(fails)}")
    if timeouts:
        print(f"TIMED OUT TWICE IN A ROW, NOT PROVEN A REAL FAILURE (ruff/mypy clean, "
              f"pytest hung on both the first attempt and an immediate retry — "
              f"see the DB-contention negative control): {', '.join(timeouts)}")
    if not fails and not timeouts:
        print("NONE would have been refused — no false positives against real, "
              "genuinely-gated history")
    return 1 if fails or timeouts else 0


def hook_status(repo_root: Path = REPO_ROOT) -> str:
    """Is the pre-commit gate actually installed, and is it the CURRENT tracked version? —
    #133's own install-verification twin of `push_guard.hook_status`, wired into `osiris
    deploy`'s report the same way so a MISSING gate hook is exactly as visible as a missing
    push guard. Deliberately NOT `git config core.hooksPath` (that stays #103's landmine,
    machine-wide and therefore wrong for this repo's own gate): `install_gate_hook.sh` copies
    the tracked `.githooks/pre-commit` shim directly into this repo's SHARED `.git/hooks/
    pre-commit`, repo-scoped by construction, the exact mechanism `install_push_guard_hook.sh`
    already proved for pre-push. NEVER raises; any read failure is its own honest status
    string, same fail-open discipline as `push_guard.hook_status`."""
    common_dir = git_common_dir(repo_root)
    if common_dir is None:
        return "gate_hook hook: not a git checkout — nothing to verify"
    tracked = repo_root / ".githooks" / "pre-commit"
    installed = common_dir / "hooks" / "pre-commit"
    if not tracked.is_file():
        return "gate_hook hook: SOURCE MISSING (.githooks/pre-commit not found in this tree)"
    if not installed.is_file():
        return ("gate_hook hook: NOT INSTALLED — run "
                "scripts/install_gate_hook.sh (commits from any worktree are UNGATED at the "
                "hook layer until this is fixed; osiris_gate_hook_enforce still governs "
                "whether an installed hook actually refuses)")
    try:
        current = tracked.read_bytes() == installed.read_bytes()
    except OSError as exc:
        return f"gate_hook hook: could not compare installed vs. tracked ({exc}) — UNKNOWN"
    if current:
        return "gate_hook hook: installed and current"
    return ("gate_hook hook: STALE — installed copy differs from the tracked source, "
            "re-run scripts/install_gate_hook.sh")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--audit", metavar="A..B",
                         help="retroactive dry-audit over a commit range, e.g. db8e3e9..HEAD")
    args = parser.parse_args(argv)
    try:
        if args.audit:
            return cmd_audit(args.audit)
        return cmd_precommit()
    except Exception as exc:  # noqa: BLE001 — the escape hatch (dispatch 5399, LEG 1): with
        # core.hooksPath wired fleet-wide, a bug INSIDE this diagnostic's own code must never
        # be the thing that blocks every seat's commit. A genuine gate failure (ruff/mypy/
        # pytest finding a real problem) is caught and returned as an ordinary nonzero exit
        # by cmd_precommit/cmd_audit themselves and never reaches this branch — only an
        # unhandled internal exception (a bug in gate_hook.py itself) does, and it fails
        # OPEN, loudly, rather than wedging the shared tree.
        print(f"gate_hook: INTERNAL ERROR ({type(exc).__name__}: {exc}) — failing OPEN, "
              f"never blocking a commit for a bug in the diagnostic itself", file=sys.stderr)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

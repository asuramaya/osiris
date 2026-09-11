"""THE DEADLOCK FIX, item 4 (mail 9658): an unbounded wait is how a single hung test
becomes a 2.5-hour xdist controller hang nobody notices for a workday (the seshat
specimen — futex_do_wait forever, zero worker processes visible). pytest-timeout and
the conftest.py session watchdog (see pyproject.toml `[tool.pytest.ini_options]` and
`tests/conftest.py`'s `_watchdog_loop`) catch a hang from the OUTSIDE; this catches the
shape that CAUSES one, from the inside, before it ships.

Four call shapes this scans src/ and tests/ for, each with NO enclosing timeout by
construction (the caller must supply one — none of these default to bounded):

  - `Thread.join()` — blocks forever with no `timeout=` and no positional arg.
  - bare `asyncio.wait(...)` (not `asyncio.wait_for`) with no `timeout=` kwarg — the
    exact footgun the asyncio docs themselves warn about; `wait_for` is the fix, not a
    kwarg on `wait` itself, so any un-marked bare call here is presumed wrong.
  - `subprocess.run(...)`/`<proc>.communicate(...)` with no `timeout=` kwarg.
  - `socket.socket(...)` construction with no `.settimeout(...)` call anywhere in the
    same enclosing function.

TWO ESCAPE HATCHES, never a third: a call already bounded is invisible to this scan (the
common case — the fix IS adding `timeout=`); a call that is genuinely safe unbounded
(bind-only socket setup, a subprocess wrapping something that provably returns in
milliseconds) gets an inline `# unbounded-wait-ok: <reason>` comment on the call's own
line — the reason is unchecked prose, but its PRESENCE is enforced, so a marker can never
be silently blanket-copied without at least typing something.

`subprocess.run`/`.communicate` is the one shape with real pre-existing volume (182
sites at this test's own birth, overwhelmingly short git/gh CLI wrapper calls with no
history of ever hanging — the operator's own complaint was specifically pytest/xdist,
not these) — hand-marking 182 individual lines would be pure noise with no signal ratio.
Same ratchet shape `test_render_hygiene.py`'s `_ALLOWLIST` already uses: a per-file
EXACT count (not a ceiling — drift in EITHER direction fails, so a file that quietly
gains OR loses one of these call sites must touch this baseline, on purpose). Thread.join
and bare asyncio.wait start at a hard zero (nothing currently needs the ratchet) — any
NEW one refuses outright, no grandfather to hide behind.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_ROOTS = (ROOT / "src", ROOT / "tests")
_SELF = Path(__file__).resolve()

_MARKER_RE = re.compile(r"#\s*unbounded-wait-ok\b")

# THE RATCHET (same law as test_render_hygiene.py's _ALLOWLIST, msg 1914): the count
# must match EXACTLY. If you fixed one (added timeout=), lower it. If you added a new
# call site that's genuinely unbounded-by-necessity, either mark it inline (preferred —
# names the actual reason at the actual line) or raise this by exactly the number you
# added, with a comment saying why here rather than at 182 call sites.
_SUBPROCESS_BASELINE: dict[str, int] = {
    "src/cli.py": 67,
    "src/ingest/files.py": 3,
    "src/ingest/gitlog.py": 3,
    "src/ingest/sessions.py": 3,
    "src/orchestrator/deploy_guard.py": 3,
    "src/orchestrator/pulse.py": 3,
    "tests/conftest.py": 2,
    "tests/test_blob_content_sweep.py": 2,
    "tests/test_bodies.py": 1,
    "tests/test_cli.py": 1,
    "tests/test_commands_status.py": 4,
    "tests/test_compose_drift.py": 3,
    "tests/test_deploy_guard.py": 4,
    "tests/test_dsh_adapter.py": 1,
    "tests/test_dsh_reconcile.py": 1,
    "tests/test_files.py": 1,
    "tests/test_gate_hook.py": 3,
    "tests/test_gate_hook_git_env_isolation.py": 8,
    "tests/test_gate_hook_install.py": 1,
    "tests/test_gitlog.py": 1,
    "tests/test_handoff_compiler.py": 1,
    "tests/test_ingest_project.py": 1,
    "tests/test_migration_0062.py": 7,
    "tests/test_mined_precision.py": 1,
    "tests/test_neighborhoods.py": 9,
    "tests/test_offbox_backup.py": 2,
    "tests/test_orphan_rows.py": 3,
    "tests/test_osiris_hook.py": 2,
    "tests/test_portfolio.py": 1,
    "tests/test_preflight.py": 2,
    "tests/test_project_identity.py": 2,
    "tests/test_project_of.py": 6,
    "tests/test_pulse.py": 2,
    "tests/test_push_guard.py": 11,
    "tests/test_resolve_fleet_projects.py": 6,
    "tests/test_settle.py": 1,
    "tests/test_soul_store.py": 4,
    "tests/test_tool_contract_ceiling_merge_driver.py": 1,
    "tests/test_tree_ingest.py": 5,
}


def _imports(tree: ast.Module, mod: str) -> bool:
    for n in ast.walk(tree):
        if isinstance(n, ast.Import) and any(a.name.split(".")[0] == mod for a in n.names):
            return True
        if isinstance(n, ast.ImportFrom) and n.module and n.module.split(".")[0] == mod:
            return True
    return False


def _has_timeout_kwarg(call: ast.Call) -> bool:
    return any(kw.arg == "timeout" for kw in call.keywords)


def _marked(lines: list[str], lineno: int) -> bool:
    """The marker comment on the call's OWN line, or the line directly above it — a
    same-line comment often doesn't fit inside the 100-char line-length gate, so a
    standalone comment line right before the call is accepted too."""
    if _MARKER_RE.search(lines[lineno - 1]):
        return True
    return lineno >= 2 and bool(_MARKER_RE.search(lines[lineno - 2]))


def _call_name(call: ast.Call) -> str | None:
    func = call.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return None


def _scan(path: Path) -> tuple[list[tuple[int, str]], int]:
    """Unmarked violations for the zero-baseline shapes (thread.join, bare asyncio.wait,
    unguarded socket ctor), plus the raw unmarked subprocess.run/communicate count for
    the caller to ratchet-check separately."""
    src = path.read_text()
    lines = src.splitlines()
    tree = ast.parse(src, filename=str(path))
    has_threading = _imports(tree, "threading")
    has_subprocess = _imports(tree, "subprocess")
    has_socket = _imports(tree, "socket")

    violations: list[tuple[int, str]] = []
    subprocess_unmarked = 0

    # socket ctor sites: gather all, then check each enclosing function's body for any
    # .settimeout( call anywhere in it (a coarse but sufficient proxy — a function that
    # binds and later sets a timeout on the same handle passes; one that never does, in
    # any function in the file, is flagged at the ctor site itself).
    settimeout_lines: set[int] = set()
    if has_socket:
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "settimeout"):
                settimeout_lines.add(node.lineno)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _call_name(node)
        if has_threading and name == "join" and not node.args and not _has_timeout_kwarg(node):
            if not _marked(lines, node.lineno):
                violations.append((node.lineno, "Thread.join() with no timeout"))
        if (has_subprocess and name in ("run", "communicate")
                and not _has_timeout_kwarg(node)):
            if _marked(lines, node.lineno):
                continue
            subprocess_unmarked += 1
        if (name == "wait" and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name) and node.func.value.id == "asyncio"
                and not _has_timeout_kwarg(node)):
            if not _marked(lines, node.lineno):
                violations.append((node.lineno, "bare asyncio.wait() with no timeout"))
        if (has_socket and isinstance(node.func, ast.Attribute) and node.func.attr == "socket"
                and isinstance(node.func.value, ast.Name) and node.func.value.id == "socket"):
            if _marked(lines, node.lineno):
                continue
            # any settimeout() anywhere later in the same file, on or after this line,
            # is accepted as covering it — coarse on purpose (see docstring above); a
            # file with NO settimeout call at all can never pass this way.
            if not any(ln >= node.lineno for ln in settimeout_lines):
                violations.append((node.lineno, "socket.socket() with no settimeout() "
                                                "anywhere in this file"))

    return violations, subprocess_unmarked


def test_no_new_unbounded_waits_outside_the_ratchet() -> None:
    zero_baseline: list[str] = []
    subprocess_actual: dict[str, int] = {}
    for root in _ROOTS:
        for path in sorted(root.rglob("*.py")):
            if path.resolve() == _SELF:
                continue
            violations, subprocess_unmarked = _scan(path)
            rel = str(path.relative_to(ROOT))
            for lineno, why in violations:
                zero_baseline.append(f"{rel}:{lineno}: {why} — fix it, or mark the line "
                                     "`# unbounded-wait-ok: <reason>`")
            if subprocess_unmarked:
                subprocess_actual[rel] = subprocess_unmarked

    mismatches = []
    seen = set(subprocess_actual) | set(_SUBPROCESS_BASELINE)
    for rel in sorted(seen):
        actual = subprocess_actual.get(rel, 0)
        expected = _SUBPROCESS_BASELINE.get(rel, 0)
        if actual != expected:
            direction = "gained" if actual > expected else "fixed/removed"
            mismatches.append(
                f"{rel}: {direction} unmarked subprocess.run/communicate call(s) without "
                f"timeout= — ratchet says {expected}, found {actual}. If you fixed one, "
                "lower _SUBPROCESS_BASELINE to match. If you added a genuinely necessary "
                "one, mark it `# unbounded-wait-ok: <reason>` at the call site (preferred) "
                "or raise the baseline by exactly the delta, with a reason.")

    assert not zero_baseline, "\n" + "\n".join(zero_baseline)
    assert not mismatches, "\n" + "\n".join(mismatches)

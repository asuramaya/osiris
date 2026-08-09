"""Task #160 (Thoth, 2026-08-09): mechanical AST + call-graph sweep for src/ functions that
are correct, tested, and never reached from a LIVE surface (MCP tool, HTTP route, CLI
command, or daemon/cron entry point). Read-only — no repairs, this only reports.

METHOD (named so the ruling in the report can be checked against it; this is the FINAL
design after five blind-spot fixes made against live false positives — see the report for
what each one caught):
  1. Parse every .py file under src/ and scripts/ with `ast`. Collect every FunctionDef/
     AsyncFunctionDef (module-level, class methods, and nested closures — e.g. FastAPI
     route handlers defined inside create_app()) as a DEF, keyed by its bare (unqualified)
     name, alongside its file, lineno, decorator names, docstring, and LOC.
  2. For every DEF's body, collect every Name/Attribute node in Load context (not just
     Call — a registry dict value like `_FUNCTIONS = {"triage": _fn_triage}` is a Name
     load, not a Call, and must count as a reference or every Function-op handler would
     wrongly read as dead). NAME-MATCH, not scope/type resolved: a call to `foo()` reaches
     EVERY def named `foo` anywhere in the tree. Named blind spot, traded for tractability.
  3. PASS-THROUGH NODES (module_level_assignments): any module- or class-body-level
     assignment (`_FUNCTIONS = {...}`, `class WorkerSettings: functions = [...]`) is a
     relay, not a dead end — reaching the assigned NAME must reach everything its RHS
     references. Closed transitively (assign_closure) so multi-hop registries resolve in
     one lookup.
  4. IMPORT-ALIAS EDGES (import_alias_edges): `from X import foo as _foo` (walked at any
     depth, not just module top level — most of these are function-local, deliberately
     avoiding self-shadowing an MCP tool wrapper of the same name) makes a call to `_foo()`
     invisible to bare-name matching unless the alias is folded back to `foo`. Modeled as
     another pass-through edge, same machinery as #3.
  5. ROOTS, level 0: any def decorated with @mcp.tool, @mcp.custom_route, @mcp.resource,
     @mcp.prompt, @app.<verb>, or @router.<verb> (APIRouter-based sub-apps). PLUS: every
     module-level (non-nested-in-a-def) statement in ROOT_FILES (the deploy/*.service
     ExecStart targets + cli.py, hand-listed — see the constant) is directly root-reachable,
     folded through #3/#4 too. PLUS: every scripts/*.py file is its OWN root file
     automatically (confirmed live: 23/23 carry their own `if __name__ == "__main__":`) —
     an operator running one by hand is exactly as live as a systemd unit.
  6. BFS from roots across the reference graph (+ pass-through relays), SRC-ONLY — tests/
     never contributes an edge, so a function reached only from tests/ does not read as
     reached, per Thoth's ruling.
  7. Any def NOT reached, excluding dunders (called implicitly by the runtime, never as a
     literal Name), is a candidate — labeled with whether it's referenced anywhere in
     tests/ (test_only_reference) and whether something in src/ OUTSIDE its own file names
     it despite that referrer itself never making it into `reached`
     (referenced_cross_file_in_src — dead code calling dead code, surfaced not filtered).
  8. Dynamic-dispatch sites this instrument CANNOT resolve — getattr() with a non-literal
     attribute, or `.get()`/subscript on a short list of known registry-shaped dicts with a
     non-literal key — are grepped separately and listed as NAMED BLIND SPOTS rather than
     silently trusted to already be covered by #3.
"""
from __future__ import annotations

import ast
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
SRC = ROOT / "src"
SCRIPTS = ROOT / "scripts"
TESTS = ROOT / "tests"

# files whose MODULE-LEVEL statements are themselves root-reachable (systemd ExecStart /
# python -m targets / the CLI entry point) — see deploy/*.service, cli.py's __main__ block
ROOT_FILES = {
    "src/mcp_server.py", "src/api/app.py", "src/manager/daemon.py",
    "src/orchestrator/pulse.py", "src/workers/arq_worker.py", "src/cli.py",
    "scripts/osiris_preflight.py",
}
ROOT_DECORATOR_MARKERS = (
    "mcp.tool", "mcp.custom_route", "mcp.resource", "mcp.prompt",
    "app.get", "app.post", "app.put", "app.patch", "app.delete",
    # APIRouter()-based sub-apps (src/api/inbox/app.py: `router = APIRouter()`, mounted
    # into create_app() via include_router() — a second, smaller live surface at :8011,
    # decorated @router.<verb>(...) rather than @app.<verb>(...))
    "router.get", "router.post", "router.put", "router.patch", "router.delete",
)

DUNDER_OK = True  # dunders are called implicitly by the runtime, never a literal Name


def rel(p: Path) -> str:
    return str(p.relative_to(ROOT))


class DefInfo:
    __slots__ = ("name", "qual", "file", "lineno", "decorators", "is_dunder",
                "is_nested_closure", "docstring", "loc", "node")

    def __init__(self, name: str, qual: str, file: str, lineno: int,
                decorators: list[str], is_nested_closure: bool, docstring: str, loc: int,
                node: ast.AST):
        self.name = name
        self.qual = qual
        self.file = file
        self.lineno = lineno
        self.decorators = decorators
        self.is_dunder = name.startswith("__") and name.endswith("__")
        self.is_nested_closure = is_nested_closure
        self.docstring = docstring
        self.loc = loc
        self.node = node


def decorator_names(dec_list: list[ast.expr]) -> list[str]:
    out: list[str] = []
    for d in dec_list:
        try:
            out.append(ast.unparse(d))
        except Exception:
            out.append("<?>")
    return out


def collect_defs(tree: ast.AST, file: str) -> list[DefInfo]:
    defs: list[DefInfo] = []

    def walk(node: ast.AST, class_stack: list[str], nested_depth: int) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                qual = ".".join([*class_stack, child.name])
                doc = ast.get_docstring(child) or ""
                loc = (child.end_lineno or child.lineno) - child.lineno + 1
                defs.append(DefInfo(
                    name=child.name, qual=f"{file}::{qual}", file=file,
                    lineno=child.lineno, decorators=decorator_names(child.decorator_list),
                    is_nested_closure=nested_depth > 0, docstring=doc, loc=loc, node=child))
                walk(child, class_stack, nested_depth + 1)
            elif isinstance(child, ast.ClassDef):
                walk(child, [*class_stack, child.name], nested_depth)
            else:
                walk(child, class_stack, nested_depth)

    walk(tree, [], 0)
    return defs


def collect_name_refs(node: ast.AST) -> set[str]:
    """Every Name/Attribute in Load context anywhere under `node` — deliberately broader
    than Call nodes so a registry dict VALUE (not just a call) counts as a reference."""
    refs: set[str] = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load):
            refs.add(n.id)
        elif isinstance(n, ast.Attribute) and isinstance(n.ctx, ast.Load):
            refs.add(n.attr)
    return refs


def module_level_refs(tree: ast.Module) -> set[str]:
    """Refs appearing in statements that are NOT inside any def — module top-level code
    (the `if __name__ == '__main__':` block, WorkerSettings' functions=[...]/cron_jobs=[...]
    list literals, mcp.run() at the bottom of mcp_server.py, etc.)."""
    refs: set[str] = set()

    def walk(n: ast.AST) -> None:
        for child in ast.iter_child_nodes(n):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue  # def bodies are handled per-DefInfo, not here
            if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Load):
                refs.add(child.id)
            elif isinstance(child, ast.Attribute) and isinstance(child.ctx, ast.Load):
                refs.add(child.attr)
            walk(child)

    walk(tree)
    return refs


def _direct_assignments(body: list[ast.stmt]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for stmt in body:
        targets: list[str] = []
        value = None
        if isinstance(stmt, ast.Assign):
            value = stmt.value
            for t in stmt.targets:
                if isinstance(t, ast.Name):
                    targets.append(t.id)
        elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
            value = stmt.value
            targets.append(stmt.target.id)
        if value is None or not targets:
            continue
        refs = collect_name_refs(value)
        for target_name in targets:
            out.setdefault(target_name, set()).update(refs)
    return out


def import_alias_edges(tree: ast.AST) -> dict[str, set[str]]:
    """THE ALIASED-IMPORT BLIND SPOT (the actual live specimen: mcp_server.py:3148,
    `from src.orchestrator.agents import claim_name as _claim`, precisely so the MCP tool
    wrapper — itself named `claim_name` — doesn't shadow the import). A deferred, aliased
    import is common throughout this codebase (avoids exactly that self-shadowing for
    every MCP tool wrapper that delegates to a same-named orchestrator function) — walked
    at ANY depth, not just module top level, since most of these imports are function-local.
    Without this, EVERY orchestrator function whose MCP wrapper aliases it this way would
    misreport as unreached: the wrapper's call site never spells the def's own bare name."""
    out: dict[str, set[str]] = defaultdict(set)
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom):
            for alias in n.names:
                if alias.asname and alias.asname != alias.name:
                    out[alias.asname].add(alias.name)
        elif isinstance(n, ast.Import):
            for alias in n.names:
                if alias.asname and alias.asname != alias.name:
                    out[alias.asname].add(alias.name.rsplit(".", 1)[-1])
    return out


def module_level_assignments(tree: ast.Module) -> dict[str, set[str]]:
    """PASS-THROUGH NODES (all files, not just ROOT_FILES): a module-level assignment like
    `_FUNCTIONS = {"triage": _fn_triage, ...}` is not a def, so a plain def-only graph drops
    it — every _fn_* handler would misreport as unreached the moment ITS caller (run_spec)
    only ever names the DICT, never the handler directly. Maps assigned top-level name ->
    every Name/Attribute Load reference inside its RHS, so the BFS can relay THROUGH the
    registry: reaching `_FUNCTIONS` (the name) must reach everything IN it.

    ALSO scans direct CLASS-BODY assignments (not nested inside methods) the same way —
    arq_worker.py's `class WorkerSettings: functions = [expand_case_job, ...]` / `cron_jobs
    = [cron(watched(drain_cascade, ...)), ...]` is the live specimen: a class attribute list
    is arq's own registry, structurally identical to a module dict, just one indent deeper."""
    out = _direct_assignments(tree.body)
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for name, refs in _direct_assignments(node.body).items():
                out.setdefault(name, set()).update(refs)
    return out


def find_dynamic_dispatch_sites(tree: ast.Module, file: str) -> list[dict[str, Any]]:
    """Places a flat name-match graph cannot see through: getattr() with a non-literal
    attribute name, and subscript/.get() on a short list of known dispatch registries with
    a non-literal (computed) key."""
    sites: list[dict[str, Any]] = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "getattr":
            if len(n.args) >= 2 and not isinstance(n.args[1], ast.Constant):
                sites.append({"file": file, "lineno": n.lineno, "kind": "getattr-dynamic",
                             "detail": ast.unparse(n)[:120]})
        if isinstance(n, ast.Subscript) and not isinstance(n.slice, ast.Constant):
            base = ast.unparse(n.value) if hasattr(ast, "unparse") else ""
            if any(k in base for k in ("_FUNCTIONS", "ACTION_VERBS", "_OPS", "REGISTRY")):
                sites.append({"file": file, "lineno": n.lineno, "kind": "registry-subscript",
                             "detail": ast.unparse(n)[:120]})
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "get":
            base = ast.unparse(n.func.value) if hasattr(ast, "unparse") else ""
            if any(k in base for k in ("_FUNCTIONS", "ACTION_VERBS", "_OPS", "REGISTRY")):
                if n.args and not isinstance(n.args[0], ast.Constant):
                    sites.append({"file": file, "lineno": n.lineno, "kind": "registry-get",
                                 "detail": ast.unparse(n)[:120]})
    return sites


def main() -> None:
    all_defs: list[DefInfo] = []
    defs_by_name: dict[str, list[DefInfo]] = defaultdict(list)
    src_edges: dict[str, set[str]] = {}   # def.qual -> referenced short names (SRC-only body)
    module_root_refs: dict[str, set[str]] = {}  # ROOT_FILES -> refs at module top level
    # module-level assignment pass-through nodes, ALL files: bare name -> refs in its RHS
    assign_edges: dict[str, set[str]] = defaultdict(set)
    dynamic_sites: list[dict[str, Any]] = []
    test_refs: set[str] = set()  # short names referenced anywhere under tests/
    parse_errors: list[str] = []

    py_files = sorted(SRC.rglob("*.py")) + sorted(SCRIPTS.glob("*.py"))
    py_files = [p for p in py_files if "__pycache__" not in p.parts]

    for path in py_files:
        text = path.read_text()
        try:
            tree = ast.parse(text, filename=str(path))
        except SyntaxError as e:
            parse_errors.append(f"{rel(path)}: {e}")
            continue
        r = rel(path)
        defs = collect_defs(tree, r)
        all_defs.extend(defs)
        for d in defs:
            defs_by_name[d.name].append(d)
            src_edges[d.qual] = collect_name_refs(d.node)
        local_assigns = module_level_assignments(tree)
        # EVERY scripts/*.py is its own live surface (task #103/#119-style: 23 of 23 carry
        # their own `if __name__ == "__main__":`, confirmed by grep) — an operator running
        # `python scripts/foo.py` by hand is exactly as live as a systemd ExecStart, so
        # scripts/ is root-reachable file-by-file rather than needing each named in
        # ROOT_FILES individually (that set stays for the src/ daemon/CLI entry points).
        is_root_file = r in ROOT_FILES or r.startswith("scripts/")
        if is_root_file:
            root_refs = module_level_refs(tree)
            # module_level_refs skips ClassDef bodies entirely (they're handled per-DefInfo/
            # per-assignment, not as bare module statements) — so a ROOT_FILE's own class
            # attributes (arq_worker.py's `class WorkerSettings: functions = [...]`) need
            # their RHS refs folded in here too, or the class's registry role is invisible.
            for refs in local_assigns.values():
                root_refs |= refs
            module_root_refs[r] = root_refs
        for name, refs in local_assigns.items():
            assign_edges[name] |= refs
        for name, refs in import_alias_edges(tree).items():
            assign_edges[name] |= refs
        dynamic_sites.extend(find_dynamic_dispatch_sites(tree, r))

    if TESTS.exists():
        for path in sorted(TESTS.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            try:
                tree = ast.parse(path.read_text(), filename=str(path))
            except SyntaxError as e:
                parse_errors.append(f"{rel(path)}: {e}")
                continue
            test_refs |= collect_name_refs(tree)

    # --- roots: level-0 reachable qual names ---
    reached: set[str] = set()
    frontier: set[str] = set()

    for d in all_defs:
        if any(m in dec for dec in d.decorators for m in ROOT_DECORATOR_MARKERS):
            frontier.add(d.qual)
    for refs in module_root_refs.values():
        for name in refs:
            for d in defs_by_name.get(name, []):
                frontier.add(d.qual)

    # ASSIGN CLOSURE: `_FUNCTIONS = {"triage": _fn_triage}` means reaching the bare name
    # `_FUNCTIONS` must also reach `_fn_triage` — precompute the full transitive closure of
    # assign_edges once (cycle-guarded) so the BFS below can treat "name X is reached" as
    # "X's whole closure is reached" in one lookup, instead of a fragile multi-pass relay.
    def close_name(name: str, seen: set[str] | None = None) -> set[str]:
        seen = seen or set()
        if name in seen:
            return set()
        seen = seen | {name}
        out = set(assign_edges.get(name, set()))
        for nxt in list(out):
            out |= close_name(nxt, seen)
        return out

    assign_closure: dict[str, set[str]] = {name: close_name(name) for name in assign_edges}

    def expand_names(names: set[str]) -> set[str]:
        out = set(names)
        for n in names:
            out |= assign_closure.get(n, set())
        return out

    # a NAME can be "reached" without being a def (e.g. `_FUNCTIONS`, a module-level dict) —
    # track reached BARE NAMES separately purely for the cross-file bookkeeping below
    reached_names: set[str] = set()
    while frontier:
        reached |= frontier
        next_frontier: set[str] = set()
        pending_names: set[str] = set()
        for qual in frontier:
            pending_names |= src_edges.get(qual, set())
        for refs in module_root_refs.values():
            pending_names |= refs
        pending_names = expand_names(pending_names)
        new_names = pending_names - reached_names
        reached_names |= new_names
        for name in new_names:
            for d in defs_by_name.get(name, []):
                if d.qual not in reached:
                    next_frontier.add(d.qual)
        frontier = next_frontier - reached

    # --- candidates: defs never reached, excluding dunders ---
    candidates = []
    for d in all_defs:
        if d.is_dunder:
            continue
        if d.qual in reached:
            continue
        candidates.append(d)

    def referenced_outside_own_file(d: DefInfo) -> bool:
        for qual, refs in src_edges.items():
            owner_file = qual.split("::", 1)[0]
            if owner_file == d.file:
                continue
            if d.name in refs:
                return True
        for r, refs in module_root_refs.items():
            if r == d.file:
                continue
            if d.name in refs:
                return True
        return False

    out_candidates = []
    for d in candidates:
        test_only = d.name in test_refs
        cross_file_src_ref = referenced_outside_own_file(d)
        out_candidates.append({
            "qual": d.qual, "name": d.name, "file": d.file, "lineno": d.lineno,
            "loc": d.loc, "decorators": d.decorators, "nested_closure": d.is_nested_closure,
            "doc_first_line": d.docstring.strip().split("\n")[0][:160] if d.docstring else "",
            "test_only_reference": test_only,
            "referenced_cross_file_in_src": cross_file_src_ref,  # should be False for a
            # true candidate; True means SOMETHING in src/ (outside its own file) names it,
            # but that referrer itself never made it into the reached set — i.e. dead code
            # calling dead code. Surfaced, not filtered, since it's part of the same finding.
        })

    result = {
        "total_defs": len(all_defs),
        "total_reached": len(reached),
        "total_candidates": len(out_candidates),
        "parse_errors": parse_errors,
        "dynamic_dispatch_sites": dynamic_sites,
        "candidates": out_candidates,
    }
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

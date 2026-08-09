"""Task (Thoth, msg 3985, 2026-08-09): mechanical AST sweep for "the callee's own signature
already supports batching or injection, and the caller doesn't use it" -- the shape behind
TWO independent finds tonight, by two people who did not know about each other:
  - compositions.py's `select` calls winning_props(ARRAY[$1]) once PER OBJECT (2947 times,
    twice per page load) though the function's own signature already takes an array.
  - cli.py's cmd_deploy() calls _wait_for_health()/_wait_for_smoke() with ZERO args though
    both already accept an injectable probe -- six of cmd_deploy's OTHER dependencies are
    threaded through exactly this way, these two are not.

Read-only, no repairs. Same four rules as the sibling instrument
(scripts/reachability_sweep.py, task #160):
  1. Rank by measured or ESTIMABLE cost, not raw count -- a helper called twice in a loop
     is noise next to one called thousands of times or making a real network/DB round trip.
  2. Name what this CANNOT see: dynamic dispatch (getattr, computed attribute access),
     comprehensions this doesn't specifically pattern-match, anything reached only by name
     through a registry/dict this script never resolves.
  3. A loop is not automatically a defect -- some batching would change semantics (e.g. an
     early-exit loop, or per-item error isolation the caller actually wants). Where this
     script cannot tell devectorization from a deliberate choice, it says so explicitly
     rather than asserting a verdict.
  4. Read-only. This reports; it fixes nothing.

Excludes #164 (compositions.py/winning_props) and #165 (cli.py/cmd_deploy)'s own sites --
Seshat and Khnum own those two specifically. This looks for the THIRD through Nth specimen.

TWO INDEPENDENT PATTERNS, one instrument, one report:

PATTERN A -- BATCHABLE SIGNATURE, CALLED ONCE PER LOOP ITERATION.
  A function whose signature declares a collection-shaped parameter (list[X]/Sequence[X]/
  tuple[X, ...]/Iterable[X]/set[X]/frozenset[X]/Collection[X], by annotation text), found
  called inside a for/async-for loop where the argument bound to THAT parameter is the
  loop's own target name (`for x in xs: f(x)`) or a single-element collection literal built
  from it (`f([x])`) -- i.e., a batch-shaped callee invoked once per item when it could take
  the whole batch. Comprehensions are walked too (their own implicit loop targets), each
  candidate marked separately since a comprehension collecting per-item results is a
  DIFFERENT shape from a bare statement loop and sometimes the deliberate one.

PATTERN B -- A CALLER THAT ALREADY THREADS OTHER DEPENDENCIES, BUT NOT THIS ONE.
  Function A's own signature already has at least one injectable parameter (a keyword-or-
  positional default that is a NAMED CALLABLE -- an ast.Name/ast.Attribute default, not a
  literal/None/constant) -- proving A already applies the "thread dependencies through my
  own signature" pattern somewhere. A calls function B directly; B ALSO has an injectable
  parameter of its own; the call to B forwards none of B's injectable names as a keyword.
  That is the inconsistency itself -- not ordinary DI (a first pass that flagged "nothing
  in src/ ever overrides this kwarg" caught nearly every test-injected function in the
  codebase and proved nothing, since production code correctly takes the real default and
  only tests override it) but a caller that demonstrably already knows the pattern, applying
  it to some dependencies and silently not to others in the SAME function -- exactly
  cmd_deploy's own shape, six deps threaded, two calls left bare.
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
TESTS = ROOT / "tests"

# task #164/#165's own sites -- explicitly excluded, not this sweep's to re-report
EXCLUDED_CALLEES = {"winning_props", "_wait_for_health", "_wait_for_smoke"}

_COLLECTION_ANNOTATIONS = ("list[", "List[", "Sequence[", "sequence[", "Iterable[",
                            "iterable[", "set[", "Set[", "frozenset[", "FrozenSet[",
                            "Collection[", "collection[", "tuple[", "Tuple[")


def rel(p: Path) -> str:
    return str(p.relative_to(ROOT))


class FuncSig:
    __slots__ = ("name", "file", "lineno", "batchable_params", "injectable_params",
                "body_io_hint", "node")

    def __init__(self, name: str, file: str, lineno: int, batchable_params: list[str],
                injectable_params: dict[str, str], body_io_hint: bool, node: ast.AST):
        self.name = name
        self.file = file
        self.lineno = lineno
        self.batchable_params = batchable_params      # param names shaped like a collection
        self.injectable_params = injectable_params     # param name -> unparsed default expr
        self.body_io_hint = body_io_hint                # await / pool / conn / httpx / subprocess
        self.node = node


def _annotation_text(ann: ast.expr | None) -> str:
    if ann is None:
        return ""
    try:
        return ast.unparse(ann)
    except Exception:
        return ""


def _is_collection_annotation(text: str) -> bool:
    return any(text.startswith(p) for p in _COLLECTION_ANNOTATIONS)


def _body_has_io_hint(node: ast.AST) -> bool:
    """A coarse, named-as-such proxy for 'this callee probably costs something real' --
    rule 1's estimable-cost signal, since an AST sweep cannot measure wall-clock. True if
    the body awaits anything, or references a connection/pool/http-client-shaped name."""
    for n in ast.walk(node):
        if isinstance(n, (ast.Await,)):
            return True
        if isinstance(n, ast.Name) and n.id in ("pool", "conn", "httpx", "asyncpg"):
            return True
        if isinstance(n, ast.Attribute) and n.attr in (
                "execute", "fetch", "fetchrow", "fetchval", "get", "post", "request"):
            return True
    return False


def collect_func_sigs(tree: ast.AST, file: str) -> list[FuncSig]:
    out: list[FuncSig] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        args = node.args
        all_params = [*args.posonlyargs, *args.args, *args.kwonlyargs]
        batchable = [a.arg for a in all_params if _is_collection_annotation(
            _annotation_text(a.annotation))]
        # defaults align to the TAIL of args.args/posonlyargs; kwonlyargs pair with
        # kw_defaults positionally (None entries mean "no default", i.e. required kwonly)
        injectable: dict[str, str] = {}
        pos_all = [*args.posonlyargs, *args.args]
        pos_defaults = args.defaults
        offset = len(pos_all) - len(pos_defaults)
        for i, d in enumerate(pos_defaults):
            if isinstance(d, (ast.Name, ast.Attribute)):
                injectable[pos_all[offset + i].arg] = _annotation_text(d)
        for a, kwd in zip(args.kwonlyargs, args.kw_defaults, strict=True):
            if kwd is not None and isinstance(kwd, (ast.Name, ast.Attribute)):
                injectable[a.arg] = _annotation_text(kwd)
        out.append(FuncSig(
            name=node.name, file=file, lineno=node.lineno, batchable_params=batchable,
            injectable_params=injectable, body_io_hint=_body_has_io_hint(node), node=node))
    return out


def _loop_target_names(target: ast.expr) -> set[str]:
    if isinstance(target, ast.Name):
        return {target.id}
    if isinstance(target, (ast.Tuple, ast.List)):
        names: set[str] = set()
        for elt in target.elts:
            names |= _loop_target_names(elt)
        return names
    return set()


def _arg_matches_loop_var(arg: ast.expr, loop_vars: set[str]) -> bool:
    """`f(x)` where x is the loop's own target, or `f([x])` / `f((x,))` -- a single-element
    collection literal built from it. Does not attempt to resolve aliases or attribute
    access (`f(x.id)`) -- a named blind spot, not silently claimed as covered."""
    if isinstance(arg, ast.Name):
        return arg.id in loop_vars
    if isinstance(arg, (ast.List, ast.Tuple)) and len(arg.elts) == 1:
        return _arg_matches_loop_var(arg.elts[0], loop_vars)
    return False


class LoopCallVisitor(ast.NodeVisitor):
    """Walks one file's tree carrying a STACK of enclosing loop target-name sets, so a call
    nested inside a for/comprehension can be checked against the loop variable actually in
    scope at that point -- not just "is there a loop somewhere in this file." """

    def __init__(self, file: str, batchable_names: set[str]):
        self.file = file
        self.batchable_names = batchable_names
        self.loop_stack: list[tuple[set[str], str]] = []  # (target names, loop kind label)
        self.hits: list[dict[str, Any]] = []

    def _visit_loop_body(self, targets: set[str], kind: str,
                         body_nodes: list[ast.stmt]) -> None:
        self.loop_stack.append((targets, kind))
        for n in body_nodes:
            self.visit(n)
        self.loop_stack.pop()

    def visit_For(self, node: ast.For) -> None:
        self._visit_loop_body(_loop_target_names(node.target), "for", node.body)
        for n in node.orelse:
            self.visit(n)

    visit_AsyncFor = visit_For  # type: ignore[assignment]

    def _comprehension_targets(self, generators: list[ast.comprehension]) -> set[str]:
        names: set[str] = set()
        for g in generators:
            names |= _loop_target_names(g.target)
        return names

    def visit_ListComp(self, node: ast.ListComp) -> None:
        self._visit_comp(node, node.elt)

    def visit_SetComp(self, node: ast.SetComp) -> None:
        self._visit_comp(node, node.elt)

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        self._visit_comp(node, node.elt)

    def visit_DictComp(self, node: ast.DictComp) -> None:
        self._visit_comp(node, node.key)
        self._visit_comp(node, node.value)

    def _visit_comp(self, node: Any, elt: ast.expr) -> None:
        targets = self._comprehension_targets(node.generators)
        self.loop_stack.append((targets, "comprehension"))
        self.visit(elt)
        self.loop_stack.pop()

    def visit_Call(self, node: ast.Call) -> None:
        fname = None
        if isinstance(node.func, ast.Name):
            fname = node.func.id
        elif isinstance(node.func, ast.Attribute):
            fname = node.func.attr
        if fname in self.batchable_names and self.loop_stack:
            loop_vars: set[str] = set()
            kinds: list[str] = []
            for targets, kind in self.loop_stack:
                loop_vars |= targets
                kinds.append(kind)
            for arg in [*node.args, *[kw.value for kw in node.keywords]]:
                if _arg_matches_loop_var(arg, loop_vars):
                    self.hits.append({
                        "callee": fname, "file": self.file, "lineno": node.lineno,
                        "loop_kinds": kinds,
                        "call_text": ast.unparse(node)[:160],
                    })
                    break
        self.generic_visit(node)


def find_pattern_a(all_sigs: list[FuncSig], py_files: list[Path]) -> list[dict[str, Any]]:
    batchable_names = {s.name for s in all_sigs if s.batchable_params
                       and s.name not in EXCLUDED_CALLEES}
    sig_by_name: dict[str, FuncSig] = {}
    for s in all_sigs:
        sig_by_name.setdefault(s.name, s)  # first def wins for the cost-hint lookup
    hits: list[dict[str, Any]] = []
    for path in py_files:
        text = path.read_text()
        try:
            tree = ast.parse(text, filename=str(path))
        except SyntaxError:
            continue
        v = LoopCallVisitor(rel(path), batchable_names)
        v.visit(tree)
        for h in v.hits:
            sig = sig_by_name.get(h["callee"])
            h["callee_batchable_params"] = sig.batchable_params if sig else []
            h["callee_defined_at"] = f"{sig.file}:{sig.lineno}" if sig else "?"
            h["estimable_cost"] = "io" if (sig and sig.body_io_hint) else "unknown"
            hits.append(h)
    return hits


def find_pattern_b(all_sigs: list[FuncSig]) -> list[dict[str, Any]]:
    """NARROW, CALLER-AWARE signature (rewritten after the first pass proved too broad —
    "does anything in src/ pass this kwarg" flags nearly every dependency-injected function
    in the codebase, since production code CORRECTLY takes the real default and only tests
    override it; that is ordinary DI, not the bug). The actual shape Thoth named: function A
    ALREADY threads OTHER dependencies through its own signature (own injectable_params is
    non-empty — A has the pattern established), and calls function B directly (B also has
    injectable_params) WITHOUT forwarding any of B's names as a keyword — the inconsistency
    of applying the pattern to some dependencies and not others in the SAME function, exactly
    cmd_deploy's own shape (six deps threaded, two calls left bare)."""
    sig_by_name: dict[str, FuncSig] = {}
    for s in all_sigs:
        sig_by_name.setdefault(s.name, s)

    di_aware = [s for s in all_sigs if s.injectable_params]
    out: list[dict[str, Any]] = []
    for a in di_aware:
        for n in ast.walk(a.node):
            if not isinstance(n, ast.Call):
                continue
            fname = n.func.id if isinstance(n.func, ast.Name) else (
                n.func.attr if isinstance(n.func, ast.Attribute) else None)
            if fname is None or fname == a.name or fname in EXCLUDED_CALLEES:
                continue
            b = sig_by_name.get(fname)
            if b is None or not b.injectable_params:
                continue
            passed_kw = {kw.arg for kw in n.keywords if kw.arg}
            if passed_kw & set(b.injectable_params):
                continue  # already forwarding it — not a candidate
            out.append({
                "caller": a.name, "caller_defined_at": f"{a.file}:{a.lineno}",
                "caller_own_injectable_params": sorted(a.injectable_params),
                "callee": fname, "callee_defined_at": f"{b.file}:{b.lineno}",
                "callee_injectable_params_not_forwarded": sorted(b.injectable_params),
                "call_lineno": n.lineno,
                "call_text": ast.unparse(n)[:160],
                "estimable_cost": "io" if b.body_io_hint else "unknown",
            })
    return out


def main() -> None:
    py_files = sorted(p for p in SRC.rglob("*.py") if "__pycache__" not in p.parts)
    all_sigs: list[FuncSig] = []
    parse_errors: list[str] = []
    for path in py_files:
        text = path.read_text()
        try:
            tree = ast.parse(text, filename=str(path))
        except SyntaxError as e:
            parse_errors.append(f"{rel(path)}: {e}")
            continue
        all_sigs.extend(collect_func_sigs(tree, rel(path)))

    pattern_a = find_pattern_a(all_sigs, py_files)
    pattern_b = find_pattern_b(all_sigs)

    # rank: I/O-hinted callees first (rule 1 -- estimable cost over raw count), then by
    # how many loop-call sites / call sites total were found for that callee
    a_by_callee: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for h in pattern_a:
        a_by_callee[h["callee"]].append(h)
    pattern_a_grouped = sorted(
        ({"callee": k, "estimable_cost": v[0]["estimable_cost"],
          "callee_defined_at": v[0]["callee_defined_at"], "occurrences": v}
         for k, v in a_by_callee.items()),
        key=lambda g: (g["estimable_cost"] != "io", -len(g["occurrences"])))
    pattern_b_sorted = sorted(
        pattern_b, key=lambda g: (g["estimable_cost"] != "io",))

    result = {
        "excluded_callees": sorted(EXCLUDED_CALLEES),
        "parse_errors": parse_errors,
        "pattern_a_batchable_called_in_loop": {
            "count_distinct_callees": len(pattern_a_grouped),
            "count_occurrences": len(pattern_a),
            "groups": pattern_a_grouped,
        },
        "pattern_b_injectable_default_never_overridden_in_src": {
            "count": len(pattern_b_sorted),
            "candidates": pattern_b_sorted,
        },
        "named_blind_spots": [
            "Pattern A only matches a call argument that is EXACTLY the loop's own target "
            "name, or a single-element list/tuple literal built from it -- an argument "
            "reached through attribute access (x.id), a renamed local, or built up across "
            "several statements before the call is invisible to this pass.",
            "Pattern A does not resolve which loop the call is 'really' inside when a call "
            "sits behind a helper function called from a loop (one level of indirection) -- "
            "only a call TEXTUALLY inside a for/comprehension body is caught.",
            "Both patterns do bare NAME matching (like the reachability sweep) -- a call to "
            "a method sharing a name with an unrelated batchable-looking function will "
            "collide; verify each candidate by reading the actual call site, not just the "
            "report line.",
            "Pattern B only recognizes a Name/Attribute default as 'injectable' -- a "
            "None-defaulted parameter with an `if x is None: x = real_thing()` fallback "
            "inside the body (a different, equally real injection idiom) is invisible to "
            "this heuristic and undercounts candidates, never overcounts them.",
            "estimable_cost='io' is a coarse proxy (await / pool / conn / httpx / .execute/"
            ".fetch* in the callee body), not a measurement -- 'unknown' does not mean "
            "cheap, it means this sweep could not tell, per rule 3.",
            "Dynamic dispatch (getattr, computed attribute names) is not scanned here at "
            "all -- the reachability sweep's own dynamic_dispatch_sites output is the "
            "closer instrument for that surface, not duplicated here.",
        ],
    }
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

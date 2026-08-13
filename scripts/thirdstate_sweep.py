"""Task #142 (Thoth, msg 4012, 2026-08-11): mechanical AST sweep for ruling 60bc15db's
own shape -- "A FUNCTION THAT CANNOT DISTINGUISH 'NO' FROM 'I DON'T KNOW', AND REPORTS
'NO'." Fifteen-plus specimens were collected ad hoc, one at a time, across three reigns,
by accident, while doing something else. This is the first systematic sweep for the
sixteenth through Nth.

Read-only, no repairs. Same four rules as both sibling instruments
(scripts/reachability_sweep.py, task #160; scripts/batch_inject_sweep.py, task from msg
3985), stated here in Thoth's own words because they are the ruling's own test, not house
style:
  1. RANK BY BLAST RADIUS, not count. A private helper with one caller is noise. A
     function every mount reads is the finding.
  2. NAME WHAT THE INSTRUMENT CANNOT SEE. This cannot mechanically detect "this None
     means two different things" in general -- it detects two narrow, precise SHAPES of
     that mistake. Everything outside those shapes is a blind spot, said so explicitly.
  3. A COLLAPSED STATE IS NOT AUTOMATICALLY A DEFECT. If both states take the SAME
     REPAIR, collapsing them is correct and clarifying (this is what let the climb-
     continuation fix and the earlier batch/inject sweep both close clean). This script
     reports SHAPES, not verdicts -- the same-repair test is applied by a human reader,
     per candidate, same as both priors.
  4. READ-ONLY. Report the list. Fix nothing. Some of these will be deliberate.

TWO PATTERNS, one instrument, one report -- scoped to src/ (mirroring batch_inject_
sweep's own scope decision; scripts/, shell, and CI/systemd code are named blind spots,
not covered here, since ruling 60bc15db's own protocol paragraph names shell-specific
shapes -- `test -z`, exit code through a pipe -- this instrument does not sweep for).

PATTERN A -- SENTINEL COLLISION. A function containing two or more `return` statements
that produce the IDENTICAL sentinel-shaped literal (None / False / "" / 0 / [] / {} /
()/ set()) from DIFFERENT causes, where at least one is guarded by something that could
not determine the answer (a broad `except:`/`except Exception:` handler, or an early-
return guard whose test asserts the SUBJECT DOES NOT EXIST -- `x is None`, `not x`,
`len(x) == 0`, `x == []`/`{}`/`""`) and at least one other represents an ordinary,
unguarded return elsewhere in the same function -- presumed a genuine, determined
answer. This is ruling 60bc15db's own falsification test, mechanized: force the
determination to fail and check whether the two returns are byte-identical to a caller.
It generalizes shapes 2 ("returning None/False/[] for both 'no' and 'I don't know'"),
3 ("a bare except or fail-closed default"), and 5 ("a check whose subject does not
exist, reported as a negative about that subject") from the dispatch into one shape,
since all three collapse to the same falsification test.

PATTERN B -- TRUTHY-OMIT KEY IN A RETURNED DICT (shape 1, generalizing specimen 14 --
Sekhmet's orient()-charter finding -- to every OTHER site with the same idiom). Two
sub-shapes of "a key is present in an output payload iff some condition is truthy, with
no distinct signal for absence":
  (b1) `if <test>: D[<const key>] = <expr>` with NO else/elif, D is the function's own
       bare return value (`return D`), AND -- the narrowing that makes this tractable,
       same lesson as batch_inject_sweep's own Pattern B rebuild -- the SAME function
       ALSO assigns at least one OTHER key into D UNCONDITIONALLY (a sibling `D[key2] =
       ...` outside every if/guard). A first, unnarrowed pass (any no-else conditional
       key assign, D returned) found 184 sites in one run -- almost all of them the
       ordinary, correct "optional field" idiom (a conditionally-included auth header,
       an optional report line). The sibling-unconditional-key requirement is an
       internal-consistency signal, not a shape-in-isolation one: it flags a function
       that ALREADY reports its other fields unconditionally, proving it knows how to
       report state plainly, but treats THIS ONE key differently -- exactly orient()'s
       own shape (every other payload key present every time; charter alone omitted).
  (b2) `**(<dict-or-expr> if <test> else {})` inside a Dict literal that is part of a
       `return` -- the unpack-ternary idiom itself (orient()'s exact shape). No further
       narrowing needed; the shape itself is precise enough to be reviewable as found.
This instrument CANNOT judge polarity (rule 2/3): omitting a key when falsy is correct
for some subjects (an absent seat correctly omitting seat_bearings) and wrong for
others (an absent charter declaration silently reading as "chartered, fine"). It
reports the SHAPE only; a human applies the ruling's own test per candidate: "TRUTHY-
MEANS-FINE IS A PROPERTY OF THE SUBJECT, NOT OF THE IDIOM."

EXCLUDED, per Thoth's explicit instruction (msg 4012) -- already fixed and deployed,
not to be re-touched or re-reported as new: _read_osiris_key (agents.py, commit
92487ef) and orient()'s own charter key (mcp_server.py, decision b1193cb7/a29fb6e95f65).
"""
from __future__ import annotations

import ast
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
SRC = ROOT / "src"

# already-fixed specimens (msg 4012) -- not this sweep's to re-report
EXCLUDED_FUNCS = {"_read_osiris_key"}
EXCLUDED_FILE_KEY_HINT = ("src/mcp_server.py", "charter")

ROOT_DECORATOR_MARKERS = (
    "mcp.tool", "mcp.custom_route", "mcp.resource", "mcp.prompt",
    "app.get", "app.post", "app.put", "app.patch", "app.delete",
    "router.get", "router.post", "router.put", "router.patch", "router.delete",
)


def rel(p: Path) -> str:
    return str(p.relative_to(ROOT))


def _decorator_names(dec_list: list[ast.expr]) -> list[str]:
    out = []
    for d in dec_list:
        try:
            out.append(ast.unparse(d))
        except Exception:
            out.append("<?>")
    return out


def _body_has_io_hint(node: ast.AST) -> bool:
    """Same coarse proxy as batch_inject_sweep's own -- an AST sweep cannot measure
    wall-clock, so this stands in for rule 1's estimable-cost signal."""
    for n in ast.walk(node):
        if isinstance(n, ast.Await):
            return True
        if isinstance(n, ast.Name) and n.id in ("pool", "conn", "httpx", "asyncpg"):
            return True
        if isinstance(n, ast.Attribute) and n.attr in (
                "execute", "fetch", "fetchrow", "fetchval", "get", "post", "request"):
            return True
    return False


def _is_root_surface(decorators: list[str]) -> bool:
    return any(m in dec for dec in decorators for m in ROOT_DECORATOR_MARKERS)


def _own_scope_nodes(fn_node: ast.AST) -> list[ast.AST]:
    """Every descendant of fn_node's OWN scope -- stops at a nested def/lambda/class, so
    a Return/Try/If belonging to a closure defined inside this function is never
    mistaken for this function's own control flow (same discipline as both sibling
    sweeps' own scope-respecting walks)."""
    out: list[ast.AST] = []

    def walk(n: ast.AST) -> None:
        for child in ast.iter_child_nodes(n):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda,
                                  ast.ClassDef)):
                continue
            out.append(child)
            walk(child)

    walk(fn_node)
    return out


def _sentinel_shape(expr: ast.expr | None) -> str | None:
    """Normalizes a return VALUE to a comparable sentinel key, or None if the value
    isn't one of the narrow literal shapes this pattern checks -- a computed expression
    (the ordinary, non-sentinel case) is deliberately NOT matched here."""
    if expr is None:
        return "None"  # bare `return`
    if isinstance(expr, ast.Constant):
        v = expr.value
        if v is None:
            return "None"
        if v is False:
            return "False"
        if isinstance(v, str) and v == "":
            return '""'
        if isinstance(v, int) and not isinstance(v, bool) and v == 0:
            return "0"
        return None
    if isinstance(expr, ast.List) and not expr.elts:
        return "[]"
    if isinstance(expr, ast.Dict) and not expr.keys:
        return "{}"
    if isinstance(expr, ast.Tuple) and not expr.elts:
        return "()"
    if (isinstance(expr, ast.Call) and isinstance(expr.func, ast.Name)
            and expr.func.id == "set" and not expr.args and not expr.keywords):
        return "set()"
    return None


def _is_none_const(node: ast.expr) -> bool:
    return isinstance(node, ast.Constant) and node.value is None


def _is_zero_or_empty_const(node: ast.expr) -> bool:
    if isinstance(node, ast.Constant):
        v = node.value
        if isinstance(v, str) and v == "":
            return True
        if isinstance(v, int) and not isinstance(v, bool) and v == 0:
            return True
        return False
    if isinstance(node, (ast.List, ast.Tuple)) and not node.elts:
        return True
    if isinstance(node, ast.Dict) and not node.keys:
        return True
    return False


def _is_existence_guard_test(test: ast.expr) -> bool:
    """A test that asserts THE SUBJECT DOES NOT EXIST -- `x is None`, `len(x) == 0`,
    `x == []`/`{}`/`""`, or the broad `not x` (a NAMED imprecision: `not enabled`-style
    booleans unrelated to existence match this too; see named_blind_spots)."""
    if isinstance(test, ast.Compare) and len(test.ops) == 1:
        op = test.ops[0]
        left, right = test.left, test.comparators[0]
        if isinstance(op, ast.Is) and (_is_none_const(right) or _is_none_const(left)):
            return True
        if isinstance(op, ast.Eq) and (_is_zero_or_empty_const(right)
                                       or _is_zero_or_empty_const(left)):
            return True
    if isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not):
        return True
    return False


def _broad_except(handler: ast.ExceptHandler) -> bool:
    if handler.type is None:
        return True
    names = []
    if isinstance(handler.type, ast.Name):
        names = [handler.type.id]
    elif isinstance(handler.type, ast.Tuple):
        names = [e.id for e in handler.type.elts if isinstance(e, ast.Name)]
    return any(n in ("Exception", "BaseException") for n in names)


def find_pattern_a(fn_node: ast.AST, fn_name: str) -> list[dict[str, Any]]:
    own = _own_scope_nodes(fn_node)
    all_returns = [n for n in own if isinstance(n, ast.Return)]
    try_nodes = [n for n in own if isinstance(n, ast.Try)]
    if_nodes = [n for n in own if isinstance(n, ast.If)]

    tags: dict[int, dict[str, Any]] = {}
    for t in try_nodes:
        for h in t.handlers:
            broad = _broad_except(h)
            detail = f"except {ast.unparse(h.type)}" if h.type else "except:"
            for n in _own_scope_nodes(h):
                if isinstance(n, ast.Return):
                    tags[id(n)] = {"kind": "except", "broad_except": broad,
                                   "detail": detail, "lineno": n.lineno}
    for i in if_nodes:
        if not i.body or not isinstance(i.body[0], ast.Return):
            continue
        if not _is_existence_guard_test(i.test):
            continue
        r = i.body[0]
        if id(r) not in tags:  # except-handler tagging wins on any overlap
            tags[id(r)] = {"kind": "existence-guard", "guard_text": ast.unparse(i.test),
                           "lineno": r.lineno}
    for r in all_returns:
        if id(r) not in tags:
            tags[id(r)] = {"kind": "other", "lineno": r.lineno}

    groups: dict[str, list[dict[str, Any]]] = {}
    for r in all_returns:
        shape = _sentinel_shape(r.value)
        if shape is None:
            continue
        groups.setdefault(shape, []).append(tags[id(r)])

    hits = []
    for shape, entries in groups.items():
        unknown = [e for e in entries if e["kind"] in ("except", "existence-guard")]
        known_negative = [e for e in entries if e["kind"] == "other"]
        if unknown and known_negative:
            hits.append({
                "function": fn_name, "sentinel": shape,
                "unknown_sites": unknown, "genuine_negative_sites": known_negative,
            })
    return hits


def _node_inside_any_if(node: ast.stmt, if_nodes: list[ast.If]) -> bool:
    end = node.end_lineno or node.lineno
    for i in if_nodes:
        i_end = i.end_lineno or i.lineno
        if i.lineno <= node.lineno and end <= i_end and node is not i:
            return True
    return False


def find_pattern_b(fn_node: ast.AST, fn_name: str, file: str) -> list[dict[str, Any]]:
    own = _own_scope_nodes(fn_node)
    all_returns = [n for n in own if isinstance(n, ast.Return)]
    if_nodes = [n for n in own if isinstance(n, ast.If)]
    all_assigns = [n for n in own if isinstance(n, ast.Assign)]
    hits: list[dict[str, Any]] = []

    def excluded(text: str) -> bool:
        return file == EXCLUDED_FILE_KEY_HINT[0] and EXCLUDED_FILE_KEY_HINT[1] in text.lower()

    # (b1) `if <test>: D[key] = expr`, no else, D returned bare, AND D has an
    # unconditional sibling key elsewhere in the function -- the internal-consistency
    # narrowing (see module docstring); an unnarrowed pass found 184 sites, nearly all
    # the ordinary optional-field idiom.
    for i in if_nodes:
        if i.orelse:
            continue
        for n in _own_scope_nodes(i):
            if not isinstance(n, ast.Assign):
                continue
            for tgt in n.targets:
                if not (isinstance(tgt, ast.Subscript) and isinstance(tgt.value, ast.Name)):
                    continue
                key_node = tgt.slice
                if not (isinstance(key_node, ast.Constant) and isinstance(key_node.value, str)):
                    continue
                d_name, key = tgt.value.id, key_node.value
                if excluded(key):
                    continue
                bare_return = next(
                    (r for r in all_returns if isinstance(r.value, ast.Name)
                     and r.value.id == d_name), None)
                if bare_return is None:
                    continue
                sibling_key = None
                for a in all_assigns:
                    if _node_inside_any_if(a, if_nodes):
                        continue
                    for a_tgt in a.targets:
                        if not (isinstance(a_tgt, ast.Subscript)
                               and isinstance(a_tgt.value, ast.Name)
                               and a_tgt.value.id == d_name):
                            continue
                        a_key = a_tgt.slice
                        if (isinstance(a_key, ast.Constant) and isinstance(a_key.value, str)
                                and a_key.value != key):
                            sibling_key = a_key.value
                            break
                    if sibling_key:
                        break
                if sibling_key is None:
                    continue
                hits.append({
                    "function": fn_name, "shape": "b1-conditional-key-assign",
                    "if_lineno": i.lineno, "dict_var": d_name, "key": key,
                    "guard_text": ast.unparse(i.test),
                    "unconditional_sibling_key": sibling_key,
                    "return_lineno": bare_return.lineno,
                })

    # (b2) `**(X if test else {})` inside a Dict literal in a return
    for r in all_returns:
        if r.value is None or not isinstance(r.value, ast.Dict):
            continue
        for dkey_node, val_node in zip(r.value.keys, r.value.values, strict=True):
            if dkey_node is not None or not isinstance(val_node, ast.IfExp):
                continue
            orelse = val_node.orelse
            is_empty_sentinel = (
                (isinstance(orelse, ast.Dict) and not orelse.keys)
                or (isinstance(orelse, ast.List) and not orelse.elts)
                or _is_none_const(orelse))
            if not is_empty_sentinel:
                continue
            body_text = ast.unparse(val_node.body)[:120]
            test_text = ast.unparse(val_node.test)
            if excluded(body_text) or excluded(test_text):
                continue
            hits.append({
                "function": fn_name, "shape": "b2-unpack-ternary",
                "return_lineno": r.lineno, "test": test_text,
                "included_when_truthy": body_text,
            })
    return hits


def main() -> None:
    py_files = sorted(p for p in SRC.rglob("*.py") if "__pycache__" not in p.parts)
    parse_errors: list[str] = []
    pattern_a_hits: list[dict[str, Any]] = []
    pattern_b_hits: list[dict[str, Any]] = []

    for path in py_files:
        text = path.read_text()
        try:
            tree = ast.parse(text, filename=str(path))
        except SyntaxError as e:
            parse_errors.append(f"{rel(path)}: {e}")
            continue
        file = rel(path)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name in EXCLUDED_FUNCS:
                continue
            decorators = _decorator_names(node.decorator_list)
            blast_hint = ("mcp-tool-or-route" if _is_root_surface(decorators)
                          else "io" if _body_has_io_hint(node) else "unknown")
            for h in find_pattern_a(node, node.name):
                h["file"] = file
                h["lineno"] = node.lineno
                h["blast_hint"] = blast_hint
                pattern_a_hits.append(h)
            for h in find_pattern_b(node, node.name, file):
                h["file"] = file
                h["lineno"] = node.lineno
                h["blast_hint"] = blast_hint
                pattern_b_hits.append(h)

    def rank_key(h: dict[str, Any]) -> tuple[bool, bool]:
        return (h["blast_hint"] != "mcp-tool-or-route", h["blast_hint"] != "io")

    pattern_a_hits.sort(key=rank_key)
    pattern_b_hits.sort(key=rank_key)

    result = {
        "excluded_funcs": sorted(EXCLUDED_FUNCS),
        "excluded_file_key_hint": list(EXCLUDED_FILE_KEY_HINT),
        "parse_errors": parse_errors,
        "pattern_a_sentinel_collision": {
            "count": len(pattern_a_hits),
            "hits": pattern_a_hits,
        },
        "pattern_b_truthy_omit_key": {
            "count": len(pattern_b_hits),
            "hits": pattern_b_hits,
        },
        "named_blind_spots": [
            "Pattern A only matches returns whose VALUE is one of the narrow literal "
            "sentinel shapes (None / False / \"\" / 0 / [] / {} / () / set()) -- a "
            "function that collapses no/unknown into a COMPUTED value (a shared status "
            "STRING like \"FAILED\", a shared enum member, a reused object instance) is "
            "invisible to this pass; it can only catch the literal-sentinel case, which "
            "is most but not all of the fifteen collected specimens.",
            "Pattern A's existence-guard detection includes bare `not x` as a match -- "
            "over-broad by design (named, not silently accepted): this also matches "
            "ordinary boolean guards unrelated to existence (`if not enabled: return "
            "False`), so every existence-guard hit needs a human read of the actual "
            "guard_text before trusting the classification.",
            "Pattern A does not follow control flow across an early re-raise, a "
            "`finally`, or a handler that logs and continues past the try/except instead "
            "of returning directly from inside it -- only a Return that is the literal "
            "terminal statement of the handler body is tagged.",
            "Pattern B (b1)'s sibling-unconditional-key narrowing only looks for "
            "`D[const_key] = ...` as a literal subscript-assignment statement -- a "
            "dict built with a single big literal (`D = {...}`) instead of sequential "
            "subscript assignment (orient()'s own shape, caught by b2 instead) will "
            "never show a sibling here even if the same inconsistency is present in "
            "spirit. b1 and b2 are deliberately non-overlapping, not redundant.",
            "Pattern B (b2) only matches the `**(X if test else {})` unpack-ternary "
            "shape literally -- a semantically identical omission built with `dict.pop`, "
            "`del d[key]`, or a multi-statement conditional assembly is invisible here.",
            "Both patterns operate PER-FUNCTION only -- a collapse spread across two "
            "cooperating functions (one determines, a caller two hops away discards the "
            "distinction) is out of scope; this is a single-function shape sweep, not a "
            "call-graph one.",
            "scripts/, shell (.sh), CI (.yml), and systemd unit files are NOT swept here "
            "-- ruling 60bc15db's own protocol paragraph names several shell-specific "
            "shapes (`test -z`, an exit code read through a pipe, `head`-truncated "
            "output handed over as complete) that this Python-AST instrument cannot see "
            "at all.",
            "estimable_cost/blast_hint is the same coarse proxy as the sibling sweeps "
            "(await/pool/conn/httpx/.execute in the body, or an @mcp.tool/@app.<verb> "
            "decorator) -- 'unknown' does not mean low blast radius, it means this sweep "
            "could not tell, per rule 2.",
        ],
    }
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

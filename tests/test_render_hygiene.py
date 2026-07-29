"""A hardcoded HTML page, made unrepresentable by CI (ruling d42c543b's enforcement half).

Task #44 built the composition-rendering mechanism properly (ruling c5b184cd added
`row_action`, the write leg) and moved three pages onto it — then closed COMPLETED while
four pages (/desk, /mail, /fleet, /overhead) stayed hand-written HTML in Python. Nobody
noticed for months, because nothing failed when the rule was broken. Restating the rule in
prose would recreate exactly the conditions under which it died; this test is the rule.

THE LAW: the only files permitted to emit markup are the frozen shell
(src/ui/static/index.html) and the frozen generic renderer (src/ui/static/osiris.js) — both
outside this scan's reach (a different directory, not Python) and outside its language (this
walks `ast`, so Jinja templates — the sanctioned home for src/api/inbox's own rendering,
render.py + templates/catalog.html.j2 — are structurally never touched either). Every other
`.py` file under src/api/ that hand-emits a tag fails, by name, unless explicitly exempted.

THE RATCHET (Thoth, msg 1914): an allowlisted file's violating-function COUNT must match
_ALLOWLIST *exactly* — not merely stay under a ceiling. A `<=` check lets the ceiling go
stale forever (port three pages down to 22 renderers and a lingering "25" would still pass,
silently under-reporting real progress). Exact equality forces whoever ports a page, or adds
one, to touch this file either way: fewer real renderers means lowering the number; a new one
means it wasn't supposed to exist. A ratchet beats a rule because it cannot be ignored, only
decreased.

DETECTION favors false positives over silent misses (a guard that fails to fire is the
disease this test cures): any string literal or f-string fragment containing an opening or
closing HTML tag, attributed to its nearest enclosing function (nested helpers get their own
entries; a function's OWN direct markup is counted separately from a nested helper's).
Docstrings are skipped deliberately — they are prose, never emitted, and this codebase's own
writing style leans on bare `<placeholder>` notation constantly (`agent:<id>`, `<ref>`, ...),
which would otherwise drown every real hit in noise.

THE FIRST CATCH, before this test even landed (Thoth, msg 1927): app.py's `roadmap_page`
hand-emitted `f'<p class="dim">no such project: {_e(p_)!r}</p>'` on its not-found path — a
real violation, found by this scanner and fixed in the same commit that introduces it, not a
false positive kept around for demonstration. Two builds converged on the same defect from
opposite ends within the hour: Seshat's unrelated /roadmap-retirement sweep (commit bb86bbe)
deleted the whole function, absorbing the fix. A guard that can point at the real thing it
caught survives a future reader's urge to soften it.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

SRC_API = Path(__file__).resolve().parent.parent / "src" / "api"

# Real HTML tag names only (not "any `<letter`") — this codebase's prose constantly uses bare
# `<placeholder>` notation ("agent:<id>", "<ref>", "seat:<uuid8>"...) which a looser pattern
# would flag on nearly every docstring and error message in the tree.
_TAGS = (
    "!doctype", "html", "head", "body", "meta", "title", "style", "script",
    "div", "span", "p", "a", "h1", "h2", "h3", "h4", "h5", "h6", "b", "i",
    "u", "strong", "em", "small", "pre", "code", "br", "hr", "nav",
    "table", "tr", "td", "th", "thead", "tbody",
    "ul", "ol", "li", "dl", "dt", "dd",
    "form", "input", "button", "textarea", "label", "select", "option",
    "details", "summary", "img", "svg", "path", "iframe", "template",
)
_TAG_RE = re.compile(r"</?(?:" + "|".join(_TAGS) + r")\b", re.IGNORECASE)

# {path relative to src/api/, as posix: allowed violating-function count}. SHORT and EXPLICIT
# on purpose (design note, msg 1914) — every entry says why it's here and what retires it.
#
# THESE NUMBERS MOVE DOWNWARD BY HAND, NEVER RECOMPUTED (Thoth, msg 1921): a ratchet that
# derives its own baseline from the tree is not a ratchet, it is a thermometer — it would
# report task #44's erosion and permit it, which is exactly what happened for months. If you
# are here because this test failed after you ADDED a renderer, the answer is a composition
# op or a Function (ruling d42c543b), not a bump to the number below. If you are here because
# you PORTED a page and the count dropped, lower the number to match — that direction is the
# ratchet working, not friction to route around.
_ALLOWLIST: dict[str, int] = {
    # chrome.py — /desk /mail /fleet /overhead, task #44's own unfinished half (thread
    # d56e7073). CURRENT-AND-FALLING, 26 render functions as of this ratchet's creation,
    # verified against the actual tree with the scanner below, TWICE (msg 1914's brief said
    # "~25" from memory, msg 1921's follow-up restated "25" — both corrected here rather than
    # trusted, the same discipline that caught this file's own live app.py false-positive).
    # Only 3 functions in this file are irreplaceable data (mail_overview, mail_threads,
    # fleet_data — async, they fetch, they don't render); the 26 render functions retire as
    # each page moves onto render_composition/the Block vocabulary (src/api/inbox/*) the way
    # /live-desk and /roadmap just did (commit bb86bbe). /fleet and /canon are the two named
    # exits still open (Thoth, msg 1938, after Seshat verified both live in /ui): /fleet's
    # composition is 721 unfolded Agent rows with no liveness signal — neither it nor
    # fleet-strip reproduces render_fleet's soul-folding by generation, the wake ledger, the
    # wake budget, the visitor-vs-seated split, or the cross-project view; /canon's route
    # carries a fixed topic order the raw docs composition lacks. render_fleet and
    # render_composition retire when those two ports land, not before. chrome.render_composition
    # is itself a SECOND generic composition renderer, duplicating osiris.js client-side —
    # counted here as debt, not architecture, and NOT to be extended (it is scheduled to die).
    "chrome.py": 26,
    # membrane.py — TERMINAL, not ongoing debt: retired (commit 75297cf, "THE INBOX cutover:
    # retire /membrane"), UNROUTED, slated for OUTRIGHT DELETION in wave 3 (Thoth, msg 1921) —
    # kept in the tree only because app.py still imports `_e` from it. Expected to reach 0 and
    # be removed from this dict entirely, not ported function-by-function like chrome.py's.
    "membrane.py": 5,
}


class _MarkupScanner(ast.NodeVisitor):
    """Attributes every markup-shaped string literal to its nearest enclosing function
    (module scope reads as "<module>") — nested helpers (chrome.py's `unsettled_cell`
    inside `render_mail_overview`) get their own entry, distinct from their parent's own
    direct markup."""

    def __init__(self) -> None:
        self.hits: dict[str, set[int]] = {}
        self.stack: list[str] = []

    def _current(self) -> str:
        return self.stack[-1] if self.stack else "<module>"

    def _record(self, node: ast.AST) -> None:
        self.hits.setdefault(self._current(), set()).add(node.lineno)  # type: ignore[attr-defined]

    def _visit_body_skipping_docstring(self, node: ast.Module | ast.ClassDef |
                                       ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        body = list(node.body)
        if (body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)):
            body = body[1:]  # the docstring — prose, never emitted; skip it
        for stmt in body:
            self.visit(stmt)

    def visit_Module(self, node: ast.Module) -> None:
        self._visit_body_skipping_docstring(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.stack.append(node.name)
        self._visit_body_skipping_docstring(node)
        self.stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.stack.append(node.name)
        self._visit_body_skipping_docstring(node)
        self.stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.stack.append(node.name)
        self._visit_body_skipping_docstring(node)
        self.stack.pop()

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str) and _TAG_RE.search(node.value):
            self._record(node)

    def visit_JoinedStr(self, node: ast.JoinedStr) -> None:
        # an f-string's own literal fragments (not the `{expr}` parts) — but still recurse
        # (generic_visit) so a nested f-string inside a comprehension/lambda inside this one
        # is walked too, attributed to whichever function actually encloses IT.
        for part in node.values:
            if (isinstance(part, ast.Constant) and isinstance(part.value, str)
                    and _TAG_RE.search(part.value)):
                self._record(node)
                break
        self.generic_visit(node)


def _markup_functions(path: Path) -> dict[str, set[int]]:
    tree = ast.parse(path.read_text(), filename=str(path))
    scanner = _MarkupScanner()
    scanner.visit(tree)
    return scanner.hits


def _markup_functions_from_source(src: str) -> dict[str, set[int]]:
    scanner = _MarkupScanner()
    scanner.visit(ast.parse(src))
    return scanner.hits


def test_no_hardcoded_markup_under_src_api_outside_the_allowlist() -> None:
    violations: list[str] = []
    for path in sorted(SRC_API.rglob("*.py")):
        rel = path.relative_to(SRC_API).as_posix()
        hits = _markup_functions(path)
        allowed = _ALLOWLIST.get(rel)
        if allowed is None:
            for fn, lines in sorted(hits.items()):
                where = "module level" if fn == "<module>" else f"function `{fn}`"
                violations.append(
                    f"src/api/{rel}: {where} (line {min(lines)}) hand-emits HTML markup — "
                    "express it as a composition op or a Function, not hardcoded chrome "
                    "(ruling d42c543b)")
            continue
        if len(hits) != allowed:
            direction = "UP" if len(hits) > allowed else "DOWN"
            names = ", ".join(sorted(hits)) or "(none)"
            violations.append(
                f"src/api/{rel}: the ratchet moved {direction} — _ALLOWLIST says {allowed} "
                f"hardcoded renderers, the file actually has {len(hits)} ({names}). If you "
                "added one, that's the violation (ruling d42c543b: express it as a "
                "composition op or a Function). If you REMOVED one, lower _ALLOWLIST to "
                "match — the ratchet only decreases.")
    assert not violations, "\n".join(violations)


def test_docstrings_are_prose_never_scanned() -> None:
    """The exact false-positive class this codebase would drown in otherwise: its own prose
    style leans on bare `<placeholder>` notation (agent:<id>, <ref>, seat:<uuid8>)."""
    hits = _markup_functions_from_source(
        '"""A docstring mentioning <details> and agent:<id> is prose, not markup."""\n'
        "def f() -> str:\n"
        '    """This one too: <span> as an example, never returned."""\n'
        '    return "plain text, no tags"\n'
    )
    assert hits == {}


def test_a_hardcoded_tag_inside_a_function_is_caught_and_named() -> None:
    hits = _markup_functions_from_source(
        "def render_thing() -> str:\n"
        "    return '<div class=\"x\">hi</div>'\n"
    )
    assert "render_thing" in hits


def test_an_fstring_tag_is_caught_including_nested_helpers() -> None:
    hits = _markup_functions_from_source(
        "def outer() -> str:\n"
        "    def inner(x: str) -> str:\n"
        "        return f'<span>{x}</span>'\n"
        "    return f'<div>{inner(\"a\")}</div>'\n"
    )
    assert "inner" in hits
    assert "outer" in hits


def test_a_bare_placeholder_angle_bracket_is_not_a_tag() -> None:
    """`<id>`/`<ref>` prose (this house's own error-message convention) must not trip the
    detector — the exact false positive found live in app.py's triage_threads before the
    tag list was narrowed from "any `<letter`" to real tag names."""
    hits = _markup_functions_from_source(
        "def f() -> dict[str, str]:\n"
        "    return {'error': \"owner needed — a project, 'agent:<id>', or 'operator'\"}\n"
    )
    assert hits == {}


def test_plain_python_with_no_strings_at_all_is_clean() -> None:
    assert _markup_functions_from_source("def f(x: int) -> int:\n    return x + 1\n") == {}

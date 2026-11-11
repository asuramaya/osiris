"""PLAIN WORDS: what a tool, command or page says back to a person is written in plain
words. Two things are kept out of every tool docstring and every piece of user-facing
prose in src/ (error text, notes, replies, rendered pages): the em dash, and a short list
of private jargon ("receipt", "mind", "dual-door"). Say what happened (a result, a
record), say who did it (an agent), and join clauses with a comma, a colon or a new
sentence.

Unlike tests/test_product_voice.py's growth-only counts, this one is zero tolerance: the
surfaces were brought to zero and stay there.

WHAT IS SCANNED: every @mcp.tool docstring in src/mcp_server.py, and every string
constant in src/ that has a space in it, except (a) the docstring of a plain function or
module (a code comment in prose form, not shown to anyone), and (b) the first argument of
a logging call. WHAT IS LEFT ALONE: identifiers and field names (a token with an
underscore, slash, dot, equals or hyphen in it, or anything in backticks), and the
character sets that parse older text that still carries an em dash.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "src"
BANNED = re.compile(r"\breceipts?\b|\bminds?\b|\bdual-door\b|\bdoor pattern\b", re.I)
EM_DASH = "—"
_LOG_NAMES = {"debug", "info", "warning", "warn", "error", "exception", "critical", "log"}
_IDENT = re.compile(r"`[^`]*`|\S*[/_.=\-]\S*")
_CLASS_WITH_DASH = re.compile(r"\[[^\]]*" + EM_DASH + r"[^\]]*\]|\(\?: " + EM_DASH)


def _is_tool(fn: ast.AST) -> bool:
    return isinstance(fn, ast.FunctionDef | ast.AsyncFunctionDef) and any(
        ast.unparse(d).startswith("mcp.tool") for d in fn.decorator_list)


def _scan(path: Path, root: Path = SRC.parent) -> list[str]:
    tree = ast.parse(path.read_text())
    skip: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.Module | ast.ClassDef):
            first = node.body[0] if node.body else None
            if (isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant)
                    and isinstance(first.value.value, str) and not _is_tool(node)):
                skip.add(id(first.value))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                and node.func.attr in _LOG_NAMES and node.args:
            for sub in ast.walk(node.args[0]):
                skip.add(id(sub))
    found: list[str] = []
    rel = path.relative_to(root)
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
            continue
        text = node.value
        if id(node) in skip or " " not in text:
            continue
        if EM_DASH in text and re.search(r"[A-Za-z]", _CLASS_WITH_DASH.sub("", text)) \
                and EM_DASH in _CLASS_WITH_DASH.sub("", text):
            found.append(f"{rel}:{node.lineno}: em dash in {text[:70]!r}")
        word = BANNED.search(_IDENT.sub(" ", text))
        if word:
            found.append(f"{rel}:{node.lineno}: '{word.group(0)}' in {text[:70]!r}")
    return found


def test_user_facing_prose_has_no_em_dash_and_no_private_jargon() -> None:
    files = sorted(SRC.rglob("*.py"))
    assert len(files) > 100, "the source tree was not found: an empty scan proves nothing"
    found = [hit for f in files for hit in _scan(f)]
    assert not found, (
        "user-facing prose must be plain: no em dash, and say 'agent' (not mind), "
        "'result' or 'record' (not receipt). Replace the following:\n  " + "\n  ".join(found))


def test_the_scan_really_catches_what_it_names(tmp_path: Path) -> None:
    """The guard is only worth having if it fires: feed it each bad shape."""
    bad = tmp_path / "bad.py"
    bad.write_text(
        'def f():\n'
        '    return {"error": "no such thing — try again"}\n'
        'def g():\n'
        '    return "the receipt was saved"\n'
        'def h():\n'
        '    return "a mind\'s own testimony"\n')
    good = tmp_path / "good.py"
    good.write_text(
        'def f():\n'
        '    """A mind — in a plain-function docstring, never shown."""\n'
        '    log.info("saved — receipt %s", 1)\n'
        '    return "see `receipts` and body-receipts/x.json, then split on [-—]"\n')
    assert len(_scan(bad, tmp_path)) == 3
    assert _scan(good, tmp_path) == []

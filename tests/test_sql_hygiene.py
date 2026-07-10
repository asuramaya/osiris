"""SQL hygiene tripwire — the onboarding-day outage class, made unrepresentable.

Every scalar subquery on current_assertions carries a ≤1-row assumption that holds
under ONE writer and breaks the day a second source describes the object: asyncpg
raises CardinalityViolation and whatever read it dies (the miner died for a DAY this
way, decision 3191e0df; the read surfaces rotted quietly, obligation 7667d3df).

The law (winning_props's exact ordering): every scalar subquery per (object, name)
takes ORDER BY confidence DESC, observed_at DESC LIMIT 1. This test walks every SQL
string literal in src/ and fails on any `(SELECT … FROM current_assertions …)` value
subquery missing it. Existence probes (SELECT 1) and aggregates are exempt — they are
cardinality-safe by construction.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "src"

_AGGREGATE = re.compile(
    r"\b(count|sum|min|max|avg|bool_or|bool_and|array_agg|string_agg|jsonb_agg|json_agg)\s*\(",
    re.IGNORECASE,
)
_EXISTENCE = re.compile(r"^\(\s*SELECT\s+1\b", re.IGNORECASE)
# the outage shape: a subquery whose select-list is a bare property `value` read —
# that and only that is the scalar ≤1-row assumption. Set-subqueries (IN (SELECT
# object_id, …)), derived tables, and window scans are cardinality-safe.
_VALUE_READ = re.compile(r"^\(\s*SELECT\s+(?:\w+\.)?value\b", re.IGNORECASE)


def _sql_literals(path: Path) -> list[str]:
    """Every string literal in the file that mentions current_assertions — implicit
    concatenation is already folded by the parser; f-string literal fragments are
    joined with a placeholder so a spliced-in identifier doesn't split the query."""
    tree = ast.parse(path.read_text(), filename=str(path))
    out: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            s = node.value
        elif isinstance(node, ast.JoinedStr):
            s = "".join(
                v.value if isinstance(v, ast.Constant) and isinstance(v.value, str) else " X "
                for v in node.values
            )
        else:
            continue
        if "current_assertions" in s:
            out.append(s)
    return out


def _subqueries(sql: str) -> list[str]:
    """Each balanced `(SELECT …)` region of the string (nested ones surface on their
    own pass, since the scan restarts at every opening match)."""
    subs: list[str] = []
    for m in re.finditer(r"\(\s*SELECT\b", sql, re.IGNORECASE):
        depth, i = 0, m.start()
        while i < len(sql):
            if sql[i] == "(":
                depth += 1
            elif sql[i] == ")":
                depth -= 1
                if depth == 0:
                    subs.append(sql[m.start(): i + 1])
                    break
            i += 1
    return subs


def _violates(sub: str) -> bool:
    if "current_assertions" not in sub:
        return False
    if _EXISTENCE.match(sub) or not _VALUE_READ.match(sub):
        return False
    head = sub.split("FROM", 1)[0]  # the select-list
    if _AGGREGATE.search(head):
        return False
    body = re.sub(r"\s+", " ", sub, flags=re.MULTILINE)
    return not re.search(
        r"ORDER\s+BY\s+(?:\w+\.)?confidence\s+DESC(?:\s+NULLS\s+LAST)?"
        r"\s*,\s*(?:\w+\.)?observed_at\s+DESC"
        r"\s+LIMIT\s+1", body, re.IGNORECASE)


def test_no_bare_scalar_subqueries_on_current_assertions() -> None:
    violations: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        for sql in _sql_literals(path):
            for sub in _subqueries(sql):
                if _violates(sub):
                    snippet = re.sub(r"\s+", " ", sub)[:120]
                    violations.append(f"{path.relative_to(SRC.parent)}: {snippet}")
    assert not violations, (
        "bare scalar subqueries on current_assertions (add ORDER BY confidence DESC, "
        "observed_at DESC LIMIT 1 — winning_props's ordering):\n" + "\n".join(violations))


def test_the_tripwire_itself_bites() -> None:
    """The checker recognizes the outage pattern and honors the two exemptions."""
    bad = "(SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='x' LIMIT 1)"
    good = ("(SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='x' "
            "ORDER BY confidence DESC, observed_at DESC LIMIT 1)")
    aliased = ("(SELECT a.value FROM current_assertions a WHERE a.object_id=o.id "
               "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)")
    nulls_last = ("(SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='x' "
                  "ORDER BY confidence DESC NULLS LAST, observed_at DESC LIMIT 1)")
    exists = "(SELECT 1 FROM current_assertions WHERE object_id=$1 AND name='x')"
    agg = "(SELECT count(*) FROM current_assertions WHERE object_id=$1)"
    assert _violates(bad)
    assert not _violates(good)
    assert not _violates(aliased)
    assert not _violates(nulls_last)
    assert not _violates(exists)
    assert not _violates(agg)

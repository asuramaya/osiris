"""Every reader that picks a property's current value breaks ties by write order
(src/db/current_order.py)."""
from __future__ import annotations

import ast
import re
from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.db.current_order import CURRENT_VALUE_ORDER
from src.orchestrator.mailbox import _dm_ineligibility
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

ROOT = Path(__file__).resolve().parent.parent
BARE = re.compile(
    r"((?:\w+\.)?)confidence\s+DESC(?:\s+NULLS\s+LAST)?\s*,\s*((?:\w+\.)?)observed_at\s+DESC"
    r"(?P<after>\s*,\s*(?:\w+\.)?id\s+DESC)?", re.I)


# Code that already ran, or runs once at boot or by hand, exactly as written: its reads are left
# as they were so what ran stays what ran.
FROZEN = ("src/orchestrator/graph_migrations.py", "src/orchestrator/migration_0062.py")
FROZEN_PREFIX = "scripts/backfill_"


def _sql_strings() -> list[tuple[str, int, str]]:
    found = []
    for sub in ("src", "scripts"):
        for path in sorted((ROOT / sub).rglob("*.py")):
            rel = str(path.relative_to(ROOT))
            if rel in FROZEN or rel.startswith(FROZEN_PREFIX):
                continue
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    found.append((str(path.relative_to(ROOT)), node.lineno, node.value))
    return found


# `observed_at DESC` alone over current_assertions: a list ordered for display (every row is
# returned, so ties only reorder the list) or a CTE already reduced to one row per object, not a
# pick of one current value. Each entry is a file and a snippet of the SQL it covers.
LIST_ORDERINGS = (
    ("src/orchestrator/capture.py", "NOT (value ? 'resolved')"),
    ("src/orchestrator/capture.py", "FROM latest s JOIN latest k"),
    ("src/orchestrator/capture.py", "a.name='lean' ORDER BY a.object_id"),
    ("src/orchestrator/seats.py", "a.name='house' AND a.value #>> '{}' != $2"),
    ("src/orchestrator/session_brief.py", "a.name='session' AND a.value"),
    ("src/orchestrator/handshake.py", "a.name='bridge_session_id'"),
)
OBSERVED_ONLY = re.compile(
    r"(?P<alias>(?:\w+\.)?)observed_at\s+DESC(?!\s*,\s*(?:\w+\.)?id\b)", re.I)


def test_the_definition_names_the_three_terms() -> None:
    assert CURRENT_VALUE_ORDER == "confidence DESC, observed_at DESC, id DESC"


def test_the_scan_sees_the_readers() -> None:
    sites = [m for _f, _n, s in _sql_strings() for m in BARE.finditer(s)]
    assert len(sites) > 400, "the scan lost the readers it exists to guard"


def test_no_reader_orders_by_confidence_and_observed_at_without_the_write_order_tiebreak() -> None:
    bare = []
    for path, lineno, text in _sql_strings():
        for m in BARE.finditer(text):
            if m.group("after") is None:
                near = re.sub(r"\s+", " ", text[max(0, m.start() - 60):m.end() + 20])
                bare.append(f"{path}:{lineno}: ...{near}")
            elif m.group(1) != m.group(2):
                bare.append(f"{path}:{lineno}: confidence and observed_at use different aliases")
    assert not bare, (
        "a current-value reader without `id DESC` returns tied rows in plan order; add the "
        "tiebreak (src/db/current_order.py):\n" + "\n".join(bare))


async def test_tied_current_rows_read_back_as_the_later_write(actions: Actions) -> None:
    """Two sources, one confidence, one observed_at: the one written last wins, every time."""
    oid = await actions.create_or_find_object("Agent", "agent:tie00001", "test")
    do = EvidenceClass.DIRECT_OBSERVATION
    at = datetime.now(UTC)
    for source, value in (("first-writer", True), ("second-writer", False)):
        await actions.assert_property(oid, "false_mint", value, source, at, confidence_for(do),
                                      evidence_class=do.value)
    for _ in range(20):
        assert await actions.pool.fetchval(
            f"SELECT value #>> '{{}}' FROM current_assertions WHERE object_id=$1 "
            f"AND name='false_mint' ORDER BY {CURRENT_VALUE_ORDER} LIMIT 1", oid) == "false"
    # and a real reader agrees: the later write ("not a false mint") is deliverable
    assert await _dm_ineligibility(actions.pool, "agent:tie00001") is None


def test_the_id_alias_agrees_with_the_other_two() -> None:
    wrong = []
    for path, lineno, text in _sql_strings():
        for m in BARE.finditer(text):
            tie = re.match(r"\s*,\s*((?:\w+\.)?)id\s+DESC", m.group("after") or "", re.I)
            if tie and tie.group(1) != m.group(1):
                wrong.append(f"{path}:{lineno}")
    assert not wrong, f"confidence, observed_at and id must use one alias: {wrong}"


def test_no_reader_picks_a_current_value_by_observed_at_alone() -> None:
    """The same tie, one term shorter: `ORDER BY observed_at DESC LIMIT 1` over current_assertions
    returns tied rows in plan order too."""
    bare = []
    for path, lineno, text in _sql_strings():
        if "current_assertions" not in text:
            continue
        for m in OBSERVED_ONLY.finditer(text):
            before = text[max(0, m.start() - 40):m.start()]
            if re.search(r"confidence\s+DESC\s*,\s*$", before, re.I):
                continue  # the three-term reader above (checked by its own test)
            flat = re.sub(r"\s+", " ", text)
            if any(path == f and snippet in flat for f, snippet in LIST_ORDERINGS):
                continue
            near = re.sub(r"\s+", " ", text[max(0, m.start() - 90):m.end() + 25])
            bare.append(f"{path}:{lineno}: ...{near}")
    assert not bare, "append `, <alias>.id DESC` (src/db/current_order.py):\n" + "\n".join(bare)

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
    r"((?:\w+\.)?)confidence\s+DESC\s*,\s*((?:\w+\.)?)observed_at\s+DESC"
    r"(?P<after>\s*,\s*(?:\w+\.)?id\s+DESC)?", re.I)


def _sql_strings() -> list[tuple[str, int, str]]:
    found = []
    for sub in ("src", "scripts"):
        for path in sorted((ROOT / sub).rglob("*.py")):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    found.append((str(path.relative_to(ROOT)), node.lineno, node.value))
    return found


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

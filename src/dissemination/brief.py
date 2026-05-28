"""Phase 10 — PDF case brief (DESIGN §12 dissemination).

Renders a case as a human-readable intelligence brief: summary, threat actors,
techniques (with ATT&CK ids), and indicators. STIX 2.1 bundle export already
ships (Phase 1) for machine consumers; this is the analyst-facing artifact.
generated_at is passed in so callers control the timestamp (and tests stay
deterministic).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from io import BytesIO
from typing import Any

import asyncpg
from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

_PROP = (
    "(SELECT value #>> '{{}}' FROM current_assertions a "
    " WHERE a.object_id = o.id AND a.name = '{name}' LIMIT 1)"
)


async def _rows(pool: asyncpg.Pool, case_id: uuid.UUID, type_: str, props: list[str]) -> list[Any]:
    cols = ", ".join(f"{_PROP.format(name=p)} AS {p}" for p in props)
    rows = await pool.fetch(
        f"SELECT o.canonical, {cols} FROM objects o "
        "JOIN case_objects co ON co.object_id = o.id "
        "WHERE co.case_id = $1 AND o.type = $2 AND o.status = 'active' "
        "ORDER BY o.canonical",
        case_id,
        type_,
    )
    return list(rows)


async def build_case_brief(
    pool: asyncpg.Pool, case_id: uuid.UUID, *, generated_at: datetime
) -> bytes:
    name = await pool.fetchval("SELECT name FROM cases WHERE id=$1", case_id) or "Unnamed case"
    by_type = {
        r["type"]: r["n"]
        for r in await pool.fetch(
            "SELECT o.type, count(*) AS n FROM objects o JOIN case_objects co ON co.object_id=o.id "
            "WHERE co.case_id=$1 GROUP BY o.type ORDER BY n DESC",
            case_id,
        )
    }
    actors = await _rows(pool, case_id, "IntrusionSet", ["name", "external_id"])
    techniques = await _rows(pool, case_id, "AttackPattern", ["name", "external_id"])
    iocs = await _rows(pool, case_id, "Indicator", ["pattern_value", "ioc_type"])

    styles = getSampleStyleSheet()
    story: list[Any] = [
        Paragraph(f"OSINT → TTP/TTAL Brief — {name}", styles["Title"]),
        Paragraph(f"Generated {generated_at:%Y-%m-%d %H:%M UTC}", styles["Normal"]),
        Spacer(1, 14),
    ]

    def section(title: str, rows: list[list[str]], header: list[str]) -> None:
        story.append(Paragraph(title, styles["Heading2"]))
        if not rows:
            story.append(Paragraph("<i>none</i>", styles["Normal"]))
            story.append(Spacer(1, 10))
            return
        table = Table([header, *rows], hAlign="LEFT", colWidths=[110, 360])
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#243049")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cccccc")),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        story.append(table)
        story.append(Spacer(1, 12))

    section(
        "Summary",
        [[t, str(n)] for t, n in by_type.items()],
        ["Entity type", "Count"],
    )
    section(
        "Threat actors",
        [[r["external_id"] or "—", r["name"] or r["canonical"]] for r in actors],
        ["ATT&CK", "Actor"],
    )
    section(
        "Techniques observed",
        [[r["external_id"] or "—", r["name"] or r["canonical"]] for r in techniques],
        ["ATT&CK", "Technique"],
    )
    section(
        "Indicators",
        [[r["ioc_type"] or "—", r["pattern_value"] or r["canonical"]] for r in iocs],
        ["Type", "Indicator"],
    )
    story.append(Paragraph(
        "A machine-readable STIX 2.1 bundle of this case is available via the export API.",
        styles["Italic"],
    ))

    buf = BytesIO()
    SimpleDocTemplate(buf, pagesize=LETTER, title=f"Osiris brief — {name}").build(story)
    return buf.getvalue()

"""docs(topic=None) — References organized as a flat, ONE-LEVEL, topic-grouped section tree
(thread 521ae613a6f4 / d56e7073, the composition-renderer readiness call).

No hierarchy link type exists between References — only `cites` ("this document cites/draws
from that reference"), a semantic mismatch for tree structure that this verb deliberately does
NOT bend into one (Thoth's call, msg 1227). v1 groups by the self-declared `topic` property
instead: `src/ingest/reference.py`'s `parse_doc` already reads an optional
`<!-- topic: ... -->` header, and every vendor doc under `docs/reference/*.md` has always
carried one. Our own docs (`docs/*.md` + `ARCHITECTURE.md`) now do too, added alongside this
build: getting-started / concepts / reference / deployment / history (headroom-modeled) —
`_SECTION_ORDER` below is that fixed presentation order; any OTHER topic value still renders,
just sorted in after it, never dropped.

A Reference with NO topic is excluded here, not swept into a catch-all "unsectioned" bucket —
deliberately, because `topic` is exactly the marker that distinguishes the docs canon (seeded
by `ingest_canon`, every entry topic-headered by convention) from the broader, fleet-wide
Reference corpus (papers and vendor docs any agent on any project ingests ad hoc via the
`ingest_reference` MCP tool, which has no `topic` parameter at all — showing those here would
turn a documentation screen into a junk drawer). A canon doc that ever ships without its
topic header simply will not appear — the fix is the header, not a fallback bucket here.

Purely a READ over whatever is already ingested: seeding is `python -m src.ingest.reference`
(`ingest_canon`), a separate, explicit act — same discipline as doors()/describe()/surface(),
which never write either."""
from __future__ import annotations

from typing import Any

import asyncpg

_SECTION_ORDER = ("getting-started", "concepts", "reference", "deployment", "history")


async def docs(pool: asyncpg.Pool, *, topic: str | None = None) -> dict[str, Any]:
    """Every ingested Reference that declares a `topic`, grouped into a flat section tree:
    `{sections: [{topic, docs: [{canonical, name, vendor}, ...]}, ...]}`, sections ordered
    getting-started/concepts/reference/deployment/history first, any other declared topic
    after, alphabetically. Pass `topic` to read one section directly:
    `{topic, docs: [...]}` (empty list, never an error, for an unknown or empty topic —
    an honest zero is not a refusal)."""
    rows = await pool.fetch(
        "SELECT o.canonical, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='name' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS name, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='topic' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS topic, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='vendor' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS vendor "
        "FROM objects o WHERE o.type='Reference' AND o.status='active'")
    sections: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        t = r["topic"]
        if not t:
            continue
        sections.setdefault(t, []).append(
            {"canonical": r["canonical"], "name": r["name"], "vendor": r["vendor"]})

    def _sorted(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(entries, key=lambda d: d["name"] or "")

    if topic is not None:
        return {"topic": topic, "docs": _sorted(sections.get(topic, []))}
    ordered = [t for t in _SECTION_ORDER if t in sections] + sorted(
        t for t in sections if t not in _SECTION_ORDER)
    return {"sections": [{"topic": t, "docs": _sorted(sections[t])} for t in ordered]}

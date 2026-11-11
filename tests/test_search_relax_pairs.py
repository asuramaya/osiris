"""The relaxed (any-term) search leg was rewritten to ask the full-text index which rows match
each word instead of parsing every candidate row's text (3.5 to 15 s down to 0.1 to 0.5 s on a
long query). The old statement is kept here, frozen, so the two can be run against the same
rows and shown to rank them the same way."""
from __future__ import annotations

from typing import Any

from src.actions.core import Actions
from src.orchestrator.capture import record_decision
from src.orchestrator.compositions import _FTS_FIELDS, _GRADE_W, _RELAX_SQL

_OLD_RELAX_SQL = (
    "WITH words AS (SELECT DISTINCT lower(w) AS w FROM unnest($1::text[]) AS w), "
    "n AS MATERIALIZED (SELECT count(*)::float + 1 AS total FROM current_assertions a "
    "  JOIN objects o ON o.id = a.object_id AND o.status = 'active' "
    "  WHERE a.name IN (" + _FTS_FIELDS + ")), "
    # df per word through the expression index (ix_assertions_fts), never a corpus scan
    "df AS MATERIALIZED (SELECT w.w, (SELECT count(*) FROM current_assertions a "
    "  JOIN objects o ON o.id = a.object_id AND o.status = 'active' "
    "  WHERE a.name IN (" + _FTS_FIELDS + ") "
    "    AND to_tsvector('english', a.value #>> '{}') @@ plainto_tsquery('english', w.w)) "
    "  + 1 AS d FROM words w), "
    # only rows that match ANY word are fetched, via the same index, and each builds its
    # tsvector once
    "hit AS MATERIALIZED ("
    "  SELECT o.id, o.type, o.canonical, a.name AS field, a.value #>> '{}' AS text, "
    "   a.source_id, a.evidence_class, a.observed_at, "
    "   to_tsvector('english', a.value #>> '{}') AS tv, " + _GRADE_W + " AS gw "
    "  FROM current_assertions a JOIN objects o ON o.id = a.object_id "
    "   AND o.status = 'active' "
    "  WHERE a.name IN (" + _FTS_FIELDS + ") "
    "    AND to_tsvector('english', a.value #>> '{}') @@ websearch_to_tsquery('english', $2)), "
    "cand AS ("
    "  SELECT DISTINCT ON (c.id) c.id, c.type, c.canonical, c.field, c.text, "
    "   c.source_id, c.evidence_class, c.observed_at, "
    "   ((SELECT sum(ln(n.total / df.d)) FROM df, n "
    "     WHERE c.tv @@ plainto_tsquery('english', df.w)) * c.gw * "
    "    (1.0 / (1.0 + EXTRACT(epoch FROM (now() - c.observed_at)) / 7776000.0)))::real "
    "     AS rank "
    "  FROM hit c ORDER BY c.id, rank DESC), "
    "top AS (SELECT * FROM cand ORDER BY rank DESC LIMIT $3) "
    "SELECT top.*, ts_headline('english', top.text, websearch_to_tsquery('english', $2), "
    "         'MaxWords=20, MinWords=8, MaxFragments=1') AS snippet "
    "FROM top ORDER BY top.rank DESC")



_TEXTS = [
    "outbox retention reaper deletes published rows older than thirty days",
    "outbox drain claims a batch and a restart strands the unpublished rest",
    "compaction floor for the audit log is lowered to two hundred fifty six megabytes",
    "the watchdog logs a stack dump through rich and stalls the event loop",
    "plain log handler replaces rich and a line costs microseconds",
    "semantic matrix rebuild moves to a worker thread off the loop",
    "decision dedup scan memoizes its comparison key",
    "retention heartbeat runs daily and posts a desk line with the count",
]


async def _run(actions: Actions, sql: str, q: str) -> list[Any]:
    words = [w for w in q.split() if w]
    return await actions.pool.fetch(sql, [w.lower() for w in words], " OR ".join(words), 15)


async def test_the_pairs_form_ranks_exactly_as_the_form_it_replaced(actions: Actions) -> None:
    for t in _TEXTS:
        await record_decision(actions, t, repo="relaxproj")
    for q in ("outbox retention restart unpublished rows",
              "stack dump rich event loop watchdog stall plain handler",
              "compaction audit log megabytes floor lowered retention"):
        old = await _run(actions, _OLD_RELAX_SQL, q)
        new = await _run(actions, _RELAX_SQL, q)
        assert old, q
        assert [str(r["id"]) for r in new] == [str(r["id"]) for r in old]
        assert [r["field"] for r in new] == [r["field"] for r in old]
        for a, b in zip(old, new, strict=True):
            assert abs(float(a["rank"]) - float(b["rank"])) < 1e-3
            assert a["snippet"] == b["snippet"]


async def test_the_pairs_form_never_returns_an_unranked_row(actions: Actions) -> None:
    for t in _TEXTS:
        await record_decision(actions, t, repo="relaxproj")
    rows = await _run(actions, _RELAX_SQL, "outbox (stack) 2026-11-11, rich: worker's loop")
    assert rows and all(r["rank"] is not None for r in rows)

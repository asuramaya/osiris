"""Reference ingest — the design canon (Palantir/Notion + own docs) as project memory.

The self-referential loop: the models that shape the front end live IN the graph as sourced,
gradeable objects, so the canon is queryable next to the commits and threads that implement it.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.ingest.reference import (
    ingest_canon,
    ingest_reference_doc,
    mine_mentions,
    parse_doc,
)
from src.ontology.schema import LINK_TYPES, OBJECT_TYPES

NOW = datetime(2026, 6, 28, tzinfo=UTC)


def test_parse_doc_pulls_header_title_body() -> None:
    d = parse_doc("<!-- source: http://x | vendor: palantir | topic: ops -->\n"
                  "# Object Sets\n\nThe operations.")
    assert d["vendor"] == "palantir" and d["source"] == "http://x" and d["topic"] == "ops"
    assert d["title"] == "Object Sets" and "The operations." in d["body"]


def test_parse_doc_tolerates_no_header() -> None:
    d = parse_doc("# Own Doc\n\nstuff")            # our own docs carry no header comment
    assert d["title"] == "Own Doc" and "vendor" not in d   # vendor defaults at ingest


def test_schema_declares_reference() -> None:
    assert "Reference" in OBJECT_TYPES
    assert "cites" in LINK_TYPES and "informs" in LINK_TYPES and "mentions" in LINK_TYPES


async def test_mine_mentions_joins_a_doc_to_the_entities_it_names(actions: Actions) -> None:
    """Layer 3, keyless: a doc links to the named entities that appear in its text — and only
    distinctive ones (a short/common name doesn't false-match)."""
    org = await actions.create_or_find_object("Organization", "cik:1", "edgar")
    await actions.assert_property(org, "name", "Neuralink Corp", "edgar", NOW, 0.85)
    short = await actions.create_or_find_object("Organization", "cik:2", "edgar")
    await actions.assert_property(short, "name", "AI", "edgar", NOW, 0.85)  # too short → ignored
    absent = await actions.create_or_find_object("Person", "p:1", "edgar")
    await actions.assert_property(absent, "name", "Elon Musk", "edgar", NOW, 0.85)
    doc = await actions.create_or_find_object("Reference", "ref:note", "ref:osiris")
    await actions.assert_property(
        doc, "body", "A note on Neuralink Corp and the AI sector.", "ref:osiris", NOW, 0.6)

    res = await mine_mentions(actions)
    assert res["mentions"] == 1                      # only the distinctive name matched
    p = actions.pool
    tgt = await p.fetchval(
        "SELECT to_id FROM links WHERE type='mentions' AND from_id=$1", doc)
    assert tgt == org                                # the doc mentions Neuralink Corp
    assert await p.fetchval(                          # not "AI" (too short), not the unnamed
        "SELECT count(*) FROM links WHERE type='mentions'") == 1
    ec = await p.fetchval("SELECT evidence_class FROM links WHERE type='mentions' LIMIT 1")
    assert ec == "co_occurrence"                      # a name match is a speculative inference
    # idempotent: a re-run creates 0 new (create_link is a plain append; we dedup)
    again = await mine_mentions(actions)
    assert again["mentions"] == 0
    assert await p.fetchval("SELECT count(*) FROM links WHERE type='mentions'") == 1


async def test_ingest_reference_doc_grades_and_dedups(actions: Actions, tmp_path: object) -> None:
    p = tmp_path / "palantir-thing.md"  # type: ignore[attr-defined]
    p.write_text("<!-- source: http://p | vendor: palantir | topic: t -->\n"
                 "# Object Sets\n\nThe ops.")
    r = await ingest_reference_doc(actions, str(p))
    assert r["canonical"] == "ref:palantir-thing" and r["vendor"] == "palantir"
    row = await actions.pool.fetchrow(
        "SELECT "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='name') AS name, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='vendor') AS vendor, "
        " (SELECT evidence_class FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='name') AS ec "
        "FROM objects o WHERE o.type='Reference'")
    assert row["name"] == "Object Sets" and row["vendor"] == "palantir"
    assert row["ec"] == "authoritative_api"        # a vendor doc is the published canon
    await ingest_reference_doc(actions, str(p))     # idempotent on the canonical
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Reference'") == 1


async def test_ingest_canon_wires_cites_edges(actions: Actions) -> None:
    """The real repo canon: docs/reference/* + own docs, and COMPOSER cites the vendor refs
    (the link COMPOSER.md actually declares — design memory that knows its own sources)."""
    res = await ingest_canon(actions)
    assert res["vendor"] >= 5 and res["own"] >= 2          # 5 vendor pages + own docs
    n = await actions.pool.fetchval("SELECT count(*) FROM links WHERE type='cites'")
    assert n == res["cites"] and n >= 5                    # COMPOSER → each vendor ref
    # the own docs are SELF_DECLARED, the vendor docs AUTHORITATIVE_API
    own_ec = await actions.pool.fetchval(
        "SELECT evidence_class FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='ref:composer' AND a.name='name'")
    assert own_ec == "self_declared"

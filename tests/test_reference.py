"""Reference ingest — the design canon (Palantir/Notion + own docs) as project memory.

The self-referential loop: the models that shape the front end live IN the graph as sourced,
gradeable objects, so the canon is queryable next to the commits and threads that implement it.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.ingest.reference import (
    _read,
    ingest_canon,
    ingest_reference_doc,
    mine_mentions,
    parse_doc,
    unwire_informs_fanout,
)
from src.ontology.schema import LINK_TYPES, OBJECT_TYPES


def test_ingest_read_redacts_credentials(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """The ingest read strips credential shapes (ruling f8f22e14) — a project's mds can carry
    printed key material just like a transcript, and must not enter the graph raw."""
    p = tmp_path / "NOTES.md"
    p.write_text(
        "# Notes\n\nExport ETHERSCAN_API_KEY=abcdef1234567890secretvalue then run.\n"
        "The token sk-ant-abcdef0123456789xyz must never land in the graph.\n")
    out = _read(str(p))
    assert "abcdef1234567890secretvalue" not in out
    assert "sk-ant-abcdef0123456789xyz" not in out
    assert "[REDACTED" in out
    assert "# Notes" in out          # ordinary prose survives

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


async def test_grounds_property_is_stored(actions: Actions, tmp_path: object) -> None:
    """A reference carries the precise module it grounds (the `grounds:` header) as a property —
    the field the retrieval Function searches to point 'how was X solved?' at the right canon."""
    p = tmp_path / "palantir-thing.md"  # type: ignore[attr-defined]
    p.write_text("<!-- source: http://p | vendor: palantir | topic: t | grounds: src/x.py -->\n"
                 "# Thing\n\nbody")
    r = await ingest_reference_doc(actions, str(p))
    assert r["grounds"] == "src/x.py"
    g = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.type='Reference' AND a.name='grounds'")
    assert g == "src/x.py"


async def test_ingest_canon_informs_the_repo(actions: Actions) -> None:
    """The self-referential loop: every reference attaches to the project it grounds via an
    `informs` edge, so 'what design canon grounds this repo?' is one hop from the repo node."""
    repo = await actions.create_or_find_object("SoftwareProject", "repo:osiris", "gitlog")
    res = await ingest_canon(actions)
    assert res["informs"] >= 7                            # 7 vendor + own docs, all inform it
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE type='informs' AND to_id=$1", repo)
    assert n == res["informs"]
    ec = await actions.pool.fetchval(
        "SELECT evidence_class FROM links WHERE type='informs' LIMIT 1")
    assert ec == "self_declared"                          # our own attribution, not vendor canon
    again = await ingest_canon(actions)                   # idempotent — no duplicate edges
    assert again["informs"] == 0


# --- _wire_informs must wire EXACTLY the project it grounds, never fan out to every
# active SoftwareProject fleet-wide (thread 5156 — the repo:? specimen's own root cause,
# decision ca091c4b: measured live, 1037 of 1054 informs edges were pure cross-join
# noise). Contract test: ingesting the canon wires exactly repo:osiris, nothing else. ---


async def test_ingest_canon_wires_only_the_named_project_not_every_active_one(
    actions: Actions,
) -> None:
    repo = await actions.create_or_find_object("SoftwareProject", "repo:osiris", "gitlog")
    unrelated1 = await actions.create_or_find_object("SoftwareProject", "repo:unrelated1",
                                                      "gitlog")
    unrelated2 = await actions.create_or_find_object("SoftwareProject", "repo:unrelated2",
                                                      "gitlog")
    res = await ingest_canon(actions)
    assert res["informs"] >= 7
    informed = {r["to_id"] for r in await actions.pool.fetch(
        "SELECT DISTINCT to_id FROM links WHERE type='informs' "
        "AND (valid_until IS NULL OR valid_until > now())")}
    assert informed == {repo}, "informs fanned out beyond the one project it actually grounds"
    assert unrelated1 not in informed
    assert unrelated2 not in informed


async def test_unwire_informs_fanout_dry_run_finds_exactly_the_noise(actions: Actions) -> None:
    """A fixture reproducing the OLD cross-join shape directly (bypassing the now-fixed
    _wire_informs): the repair verb must find exactly the noise edges, leave the one
    genuine edge alone, and never touch an informs edge asserted by a different source."""
    repo = await actions.create_or_find_object("SoftwareProject", "repo:osiris", "gitlog")
    unrelated1 = await actions.create_or_find_object("SoftwareProject", "repo:unrelated1",
                                                      "gitlog")
    unrelated2 = await actions.create_or_find_object("SoftwareProject", "repo:unrelated2",
                                                      "gitlog")
    unrelated3 = await actions.create_or_find_object("SoftwareProject", "repo:unrelated3",
                                                      "gitlog")
    ref = await actions.create_or_find_object("Reference", "ref:fixture", "test")
    now = datetime.now(UTC)
    # the genuine edge (must survive)
    await actions.create_link(ref, repo, "informs", "ref:osiris", now, 0.9,
                              evidence_class="self_declared")
    # the fan-out noise (must be found and, on execute, invalidated)
    await actions.create_link(ref, unrelated1, "informs", "ref:osiris", now, 0.9,
                              evidence_class="self_declared")
    await actions.create_link(ref, unrelated2, "informs", "ref:osiris", now, 0.9,
                              evidence_class="self_declared")
    # a genuinely third-party-asserted informs edge, on its OWN triple (the fan-out's own
    # dedup never let it double up with a noise row on the same (from,to) pair) — never
    # the fan-out's own signature, must never be touched
    await actions.create_link(ref, unrelated3, "informs", "agent:third-party", now, 0.9,
                              evidence_class="self_declared")

    out = await unwire_informs_fanout(actions, project="osiris", actor="agent:test",
                                      dry_run=True)
    assert out["dry_run"] is True
    assert out["edges_to_unwire"] == 2
    targets = {item["to_id"] for item in out["plan"]}
    assert targets == {str(unrelated1), str(unrelated2)}

    # dry run wrote nothing
    live = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE type='informs' AND from_id=$1 "
        "AND (valid_until IS NULL OR valid_until > now())", ref)
    assert live == 4

    out2 = await unwire_informs_fanout(actions, project="osiris", actor="agent:test",
                                       dry_run=False, because="thread 5156 repair")
    assert out2["unwired"] == 2

    remaining = {(r["to_id"]) for r in await actions.pool.fetch(
        "SELECT to_id FROM links WHERE type='informs' AND from_id=$1 "
        "AND (valid_until IS NULL OR valid_until > now())", ref)}
    assert remaining == {repo, unrelated3}, (
        "the genuine edge or the third-party edge was wrongly touched")

    # idempotent
    out3 = await unwire_informs_fanout(actions, project="osiris", actor="agent:test",
                                       dry_run=True)
    assert out3["edges_to_unwire"] == 0


async def test_unwire_informs_fanout_refuses_execute_without_because(actions: Actions) -> None:
    repo = await actions.create_or_find_object("SoftwareProject", "repo:osiris", "gitlog")
    unrelated = await actions.create_or_find_object("SoftwareProject", "repo:unrelated3",
                                                     "gitlog")
    ref = await actions.create_or_find_object("Reference", "ref:fixture2", "test")
    now = datetime.now(UTC)
    await actions.create_link(ref, repo, "informs", "ref:osiris", now, 0.9)
    await actions.create_link(ref, unrelated, "informs", "ref:osiris", now, 0.9)

    out = await unwire_informs_fanout(actions, project="osiris", actor="agent:test",
                                      dry_run=False, because="  ")
    assert "un-audited reversal" in out["error"]
    live = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE type='informs' AND to_id=$1 "
        "AND (valid_until IS NULL OR valid_until > now())", unrelated)
    assert live == 1, "the noise edge was removed despite the refusal"


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


async def test_ingest_canon_cites_is_idempotent(actions: Actions) -> None:
    """The `cites` wiring (COMPOSER → each vendor ref) must dedup like informs/mentions — a
    second ingest_canon adds no duplicate edges (regression: it went 7→14 before the guard)."""
    first = await ingest_canon(actions)
    n = await actions.pool.fetchval("SELECT count(*) FROM links WHERE type='cites'")
    assert n == first["cites"] and n >= 5                 # every new cite is counted once
    again = await ingest_canon(actions)                   # re-run creates no duplicate cites
    assert again["cites"] == 0
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE type='cites'") == n


# --- the md-kill (task #18): a build log ingests as PER-ENTRY nodes, never one dump -----

_LOG = """# CLAUDE.md — session-start notes

Read the graph first.

## Runtime context (locked)
Single machine, single operator. Services bind to 127.0.0.1.

## Build order
- **Phase 0 (DONE):** schema + six actions + audit/outbox + tests. """ + "kernel " * 70 + """
- **THE LIVENESS NIGHT — fresh-eyes audit (DONE, 2026-07-02):** proven is not alive; """ \
    + "fleet repair " * 40 + """
- **tiny note:** too small to be its own entry.
"""


def test_parse_log_chunks_sections_and_dated_entries() -> None:
    from src.ingest.reference import parse_log

    entries = parse_log(_LOG)
    titles = {e["title"] for e in entries}
    assert "(overview)" in titles                      # pre-## prose survives
    assert "Runtime context (locked)" in titles        # a ## section is a chunk
    assert "Phase 0 (DONE)" in titles                  # a big bullet is its own entry
    liveness = next(e for e in entries if "LIVENESS" in e["title"])
    assert liveness["date"] == "2026-07-02"            # dated from its own header
    assert "fleet repair" in liveness["body"]
    # the tiny bullet did NOT become a node — it stays with its section's remainder
    assert not any(e["title"] == "tiny note" for e in entries)
    build = next(e for e in entries if e["title"] == "Build order")
    assert "too small" in build["body"]


def test_parse_doc_strips_essay_frontmatter() -> None:
    doc = parse_doc("---\nname: osiris-strange-loop\ntype: project\n---\n\n# The frame\n\nbody.")
    assert doc["title"] == "The frame"
    assert "name: osiris" not in doc["body"]


async def test_ingest_log_is_idempotent_per_entry(actions: Actions, tmp_path) -> None:  # type: ignore[no-untyped-def]
    from src.ingest.reference import ingest_log

    p = tmp_path / "log.md"
    p.write_text(_LOG)
    r1 = await ingest_log(actions, str(p), topic="history")
    n1 = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Reference' AND canonical LIKE 'ref:history-%'")
    r2 = await ingest_log(actions, str(p), topic="history")
    n2 = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Reference' AND canonical LIKE 'ref:history-%'")
    assert r1["entries"] == r2["entries"] and n1 == n2  # re-ingest mints nothing new
    # bounded retrieval: the liveness entry is its OWN node with its date
    row = await actions.pool.fetchrow(
        "SELECT o.canonical, (SELECT value #>> '{}' FROM current_assertions a "
        " WHERE a.object_id=o.id AND a.name='date') AS d "
        "FROM objects o WHERE o.canonical LIKE 'ref:history-2026-07-02%'")
    assert row is not None and row["d"] == "2026-07-02"


def test_parse_log_splits_entries_whose_bold_header_wraps() -> None:
    """Live receipt: THE LIVENESS NIGHT's header wraps a line; without DOTALL it never
    split and got swallowed into the previous entry — retrieval returned the wrong node."""
    from src.ingest.reference import parse_log

    log = ("## Build order\n"
           "- **FIRST ENTRY (DONE, 2026-06-01):** " + "alpha " * 90 + "\n"
           "- **THE WRAPPED NIGHT — a header long enough that the operator's editor\n"
           "  wraps it (DONE, 2026-07-02):** " + "beta " * 90 + "\n")
    entries = parse_log(log)
    wrapped = next(e for e in entries if "WRAPPED NIGHT" in e["title"])
    assert wrapped["date"] == "2026-07-02"
    assert "beta" in wrapped["body"] and "alpha" not in wrapped["body"]
    first = next(e for e in entries if "FIRST ENTRY" in e["title"])
    assert "beta" not in first["body"]  # the wrapped entry no longer swallowed

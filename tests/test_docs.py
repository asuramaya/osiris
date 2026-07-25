"""docs(topic=None) — References as a flat, topic-grouped section tree (thread 521ae613a6f4)."""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.ingest.reference import ingest_reference_doc
from src.orchestrator.docs import docs

NOW = datetime(2026, 7, 25, tzinfo=UTC)


async def _ref(actions: Actions, canon: str, name: str, *, topic: str | None,
               vendor: str | None = None) -> None:
    r = await actions.create_or_find_object("Reference", canon, "test")
    await actions.assert_property(r, "name", name, "test", NOW, 0.9,
                                  evidence_class="self_declared")
    if topic:
        await actions.assert_property(r, "topic", topic, "test", NOW, 0.9,
                                      evidence_class="self_declared")
    if vendor:
        await actions.assert_property(r, "vendor", vendor, "test", NOW, 0.9,
                                      evidence_class="self_declared")


async def test_docs_groups_by_topic_in_the_fixed_order(actions: Actions) -> None:
    await _ref(actions, "ref:d1", "Deploy doc", topic="deployment")
    await _ref(actions, "ref:g1", "Install doc", topic="getting-started")
    await _ref(actions, "ref:c1", "Composer doc", topic="concepts")
    await _ref(actions, "ref:x1", "Some esoteric doc", topic="esoterica")

    out = await docs(actions.pool)

    topics = [s["topic"] for s in out["sections"]]
    # fixed order first (only the ones actually present), unlisted topics after, alphabetical
    assert topics == ["getting-started", "concepts", "deployment", "esoterica"]
    concepts = next(s for s in out["sections"] if s["topic"] == "concepts")
    assert [d["name"] for d in concepts["docs"]] == ["Composer doc"]


async def test_docs_excludes_references_with_no_declared_topic(actions: Actions) -> None:
    await _ref(actions, "ref:has-topic", "Has a topic", topic="reference")
    await _ref(actions, "ref:no-topic", "An ad hoc research paper", topic=None)

    out = await docs(actions.pool)

    names = {d["name"] for s in out["sections"] for d in s["docs"]}
    assert names == {"Has a topic"}


async def test_docs_filters_to_a_single_topic(actions: Actions) -> None:
    await _ref(actions, "ref:r1", "Ref one", topic="reference")
    await _ref(actions, "ref:r2", "Ref two", topic="reference")
    await _ref(actions, "ref:h1", "History one", topic="history")

    out = await docs(actions.pool, topic="reference")

    assert out == {"topic": "reference", "docs": [
        {"canonical": "ref:r1", "name": "Ref one", "vendor": None},
        {"canonical": "ref:r2", "name": "Ref two", "vendor": None},
    ]}


async def test_docs_unknown_topic_is_an_honest_empty_list_not_an_error(
    actions: Actions,
) -> None:
    out = await docs(actions.pool, topic="nonexistent")
    assert out == {"topic": "nonexistent", "docs": []}


async def test_docs_carries_vendor_for_grading_context(actions: Actions) -> None:
    await _ref(actions, "ref:v1", "Palantir aggregate", topic="concepts", vendor="palantir")
    out = await docs(actions.pool, topic="concepts")
    assert out["docs"][0]["vendor"] == "palantir"


async def test_docs_end_to_end_from_a_real_ingested_file(
    actions: Actions, tmp_path: Path,
) -> None:
    """The whole pipe: a markdown file with the real `<!-- topic: ... -->` header convention,
    ingested through the actual ingest path (not a hand-seeded assertion), shows up sectioned."""
    doc = tmp_path / "GETTING-STARTED.md"
    doc.write_text("<!-- topic: getting-started -->\n\n# Getting Started\n\nDo the thing.\n")
    await ingest_reference_doc(actions, str(doc))

    out = await docs(actions.pool, topic="getting-started")

    assert out["docs"] == [
        {"canonical": "ref:getting-started", "name": "Getting Started", "vendor": "osiris"}]

"""Phase 4 — the AI-extraction driver (universal parser).

Proves: a messy document becomes graded graph nodes with no bespoke parser, via an
INJECTED LLM (hermetic — canned JSON, no network/cost). The parse is tolerant of bad
shapes (an extractor must not crash a cron), and everything lands DERIVED so an AI
guess is a speculative leaf, not an authoritative fact.
"""
from __future__ import annotations

import uuid
from typing import Any

import pytest
from src.actions.core import Actions
from src.ingest.extract import (
    ExtractionResult,
    LLMClient,
    extract_document,
    parse_extraction,
)

_DOC_JSON = """
{"entities": [
   {"name": "Neuralink Corp.", "type": "Organization", "properties": {"role": "issuer"}},
   {"name": "Elon Musk", "type": "Person", "properties": {"title": "CEO"}}
 ],
 "relationships": [
   {"from": "Elon Musk", "to": "Neuralink Corp.", "type": "officer_of"}
 ]}
"""


class _FakeLLM:
    """Returns a fixed completion; records the model it was asked to use. `usage` (when
    given) is appended to `usage_out`, the same shape a real provider's own envelope
    reader populates it with — lets tests prove record_usage's own wiring."""

    def __init__(self, reply: str, usage: Any = None) -> None:
        self.reply = reply
        self.model_used: str | None = None
        self.usage = usage

    async def complete(
        self, *, system: str, prompt: str, model: str, max_tokens: int = 2048,
        usage_out: list[Any] | None = None,
    ) -> str:
        self.model_used = model
        if usage_out is not None and self.usage is not None:
            usage_out.append(self.usage)
        return self.reply


# --- the parser (pure) ------------------------------------------------------

def test_parse_extraction_reads_entities_and_relationships() -> None:
    r = parse_extraction(_DOC_JSON)
    assert {e.name for e in r.entities} == {"Neuralink Corp.", "Elon Musk"}
    assert {e.type for e in r.entities} == {"Organization", "Person"}
    assert len(r.relationships) == 1
    assert r.relationships[0].type == "officer_of"


def test_parse_extraction_tolerates_markdown_fences() -> None:
    fenced = "```json\n" + _DOC_JSON.strip() + "\n```"
    assert len(parse_extraction(fenced).entities) == 2


def test_parse_extraction_never_raises_on_garbage() -> None:
    assert parse_extraction("not json at all") == ExtractionResult()
    assert parse_extraction("[1,2,3]") == ExtractionResult()  # wrong top-level shape
    # a relationship to an entity that wasn't extracted is dropped, not invented
    one_sided = '{"entities":[{"name":"A","type":"Person"}],' \
                '"relationships":[{"from":"A","to":"Ghost","type":"x"}]}'
    assert parse_extraction(one_sided).relationships == []


def test_parse_extraction_reclassifies_a_bad_type_hint() -> None:
    # the model mislabels an obvious company as a Person -> the classifier corrects it
    bad = '{"entities":[{"name":"Acme Holdings LLC","type":"Person"}]}'
    assert parse_extraction(bad).entities[0].type == "Organization"


# --- the driver (emits through Actions, graded DERIVED) ---------------------

async def test_extract_document_emits_graded_nodes(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    llm: LLMClient = _FakeLLM(_DOC_JSON)
    counts = await extract_document(actions, "raw filing text...", llm, case_id=cid,
                                    model="claude-haiku-4-5-20251001")
    assert counts == {
        "entities": 2, "relationships": 1,
        "canonicals": ["extracted-org:neuralink-corp", "extracted-person:elon-musk"],
    }
    # everything the LLM read is graded DERIVED (a lead to verify, not a fact) — excludes
    # the session-persistent Type catalog (task #97), not this test's business
    classes = {
        r["evidence_class"]
        for r in await actions.pool.fetch(
            "SELECT a.evidence_class FROM current_assertions a "
            "JOIN objects o ON o.id = a.object_id WHERE o.type <> 'Type'")
    }
    assert classes == {"derived"}
    link_class = await actions.pool.fetchval("SELECT evidence_class FROM links")
    assert link_class == "derived"


async def test_extract_document_records_its_own_usage(actions: Actions, case_id: str) -> None:
    """Auto-ingest cost levers (thread ccee2304, mail 9873): before this, only
    session-extract called record_usage — usage_summary's own totals were the
    auto-ingest's cost, not all LLM spend, silently missing every document-extract call.
    A distinct purpose ('document-extract') keeps its own line in the telemetry."""
    from src.ingest.providers import Usage

    cid = uuid.UUID(case_id)
    usage = Usage(model="claude-haiku-4-5-20251001", input_tokens=500, output_tokens=120)
    llm: LLMClient = _FakeLLM(_DOC_JSON, usage=usage)
    await extract_document(actions, "raw filing text...", llm, case_id=cid,
                           model="claude-haiku-4-5-20251001")
    row = await actions.pool.fetchrow(
        "SELECT purpose, model, input_tokens, output_tokens FROM llm_usage")
    assert row is not None
    assert row["purpose"] == "document-extract"
    assert row["model"] == "claude-haiku-4-5-20251001"
    assert row["input_tokens"] == 500 and row["output_tokens"] == 120


async def test_extract_document_is_idempotent(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    for _ in range(2):  # re-extracting the same doc must not fork the graph
        await extract_document(actions, "doc", _FakeLLM(_DOC_JSON), case_id=cid)
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE canonical LIKE 'extracted-%'"
    )
    assert n == 2


async def test_extract_uses_configured_model(actions: Actions, case_id: str) -> None:
    fake = _FakeLLM('{"entities":[]}')
    await extract_document(actions, "doc", fake, case_id=uuid.UUID(case_id), model="some-model")
    assert fake.model_used == "some-model"


async def test_extract_requires_a_provider_when_none_injected(
    actions: Actions, case_id: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With no client injected and no key configured, extraction fails loudly — the
    deployment must wire the model (a key), not silently produce nothing."""
    monkeypatch.setattr("src.ingest.extract.llm_provider", lambda: None)
    with pytest.raises(RuntimeError, match="LLM provider"):
        await extract_document(actions, "doc", case_id=uuid.UUID(case_id))


async def test_unknown_relationship_demotes_to_controlled_vocab(
    actions: Actions, case_id: str
) -> None:
    """An LLM emits free-form relationship phrases; link types are a CONTROLLED
    vocabulary. An undeclared phrase becomes a generic `related_to` link with the raw
    phrase kept in `relation` — nuance survives as data, the catalog stays clean."""
    doc = ('{"entities":[{"name":"Acme","type":"Organization"},'
           '{"name":"Globex","type":"Organization"}],'
           '"relationships":[{"from":"Acme","to":"Globex","type":"acquired_by"}]}')
    await extract_document(actions, "d", _FakeLLM(doc), case_id=uuid.UUID(case_id))
    row = await actions.pool.fetchrow("SELECT type, properties FROM links LIMIT 1")
    assert row["type"] == "related_to"
    assert row["properties"]["relation"] == "acquired_by"

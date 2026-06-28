"""AI-extraction driver — the universal parser (cron Phase 4).

Most sources need a bespoke parser. This one doesn't: it hands a messy document to
an LLM and gets back graded entities and relationships, emitted through the SAME
Actions narrow waist as every other driver. It collapses the per-source parser tax —
any text (a filing, a press release, a court PDF's text) becomes graph nodes.

Two deliberate design choices:

  * **Graded DERIVED.** An LLM's reading of a document is an inference, not an
    authoritative fact — so everything it extracts is `DERIVED` (0.4). The evidence
    taxonomy already encodes AI uncertainty: a DERIVED node is a SPECULATIVE LEAF in
    the frontier, so an AI guess never spawns crawls until a second, independent
    source corroborates it. The model's confidence is a lead to verify, not a fact.
  * **The LLM is an injected seam.** `LLMClient` is a Protocol; tests use a fake that
    returns canned JSON (hermetic, no network, no cost). `AnthropicClient` is the live
    impl over httpx (no SDK dep). A document→entities task is flash-tier, so the model
    defaults to Haiku — Opus would be wasteful per-document at cron scale.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

import httpx

from src.actions.core import Actions
from src.config.settings import get_settings
from src.ontology.entity_type import classify_entity_type, clean_entity_name
from src.ontology.schema import is_known_link_type
from src.parsers.base import EvidenceClass

_SOURCE = "ai_extract"
_EC = EvidenceClass.DERIVED  # an LLM reading is inferred, never authoritative
_CONF = 0.4  # confidence_for(DERIVED)

_SYSTEM = (
    "You are an entity-extraction engine for an OSINT graph. Read the document and "
    "return STRICT JSON only — no prose, no markdown fences. Extract real-world "
    "entities (people and organizations) and the relationships stated between them. "
    "Do not invent facts not supported by the text. Schema:\n"
    '{"entities":[{"name":str,"type":"Person"|"Organization",'
    '"properties":{<field>:str}}],'
    '"relationships":[{"from":str,"to":str,"type":str}]}\n'
    "Use the exact entity `name` strings as the `from`/`to` of relationships."
)


class LLMClient(Protocol):
    """The injected model seam: a document-grounded completion returning JSON text."""

    async def complete(
        self, *, system: str, prompt: str, model: str, max_tokens: int = 2048
    ) -> str: ...


@dataclass
class AnthropicClient:
    """Live LLM over the Anthropic Messages API via httpx (no SDK dependency)."""

    api_key: str
    base_url: str = "https://api.anthropic.com"

    async def complete(
        self, *, system: str, prompt: str, model: str, max_tokens: int = 2048
    ) -> str:
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(
                f"{self.base_url}/v1/messages",
                headers={
                    "x-api-key": self.api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": model,
                    "max_tokens": max_tokens,
                    "system": system,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            r.raise_for_status()
            data = r.json()
            return "".join(
                b.get("text", "") for b in data.get("content", []) if b.get("type") == "text"
            )


@dataclass
class ExtractedEntity:
    name: str
    type: str
    properties: dict[str, str] = field(default_factory=dict)


@dataclass
class ExtractedRelationship:
    from_name: str
    to_name: str
    type: str


@dataclass
class ExtractionResult:
    entities: list[ExtractedEntity] = field(default_factory=list)
    relationships: list[ExtractedRelationship] = field(default_factory=list)


def _strip_fences(raw: str) -> str:
    """Tolerate a model that wraps JSON in ```json fences despite instructions."""
    s = raw.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*\n?", "", s)
        s = re.sub(r"\n?```$", "", s)
    return s.strip()


def _canonical(type_: str, name: str) -> str:
    """A stable, dedup-friendly canonical for an entity with no authoritative id.
    Same name => same node (idempotent re-extraction); a later same-name federated
    entity (cik:/lei:/Qxxx) becomes a review-gated cross-base candidate, not a dupe."""
    norm = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    prefix = "extracted-person" if type_ == "Person" else "extracted-org"
    return f"{prefix}:{norm}"


def parse_extraction(raw: str) -> ExtractionResult:
    """Pure: LLM JSON text -> a validated ExtractionResult. Tolerant of fences and
    of missing fields; classifies entity type conservatively (the model's hint is
    cross-checked against the Person/Org classifier). Never raises on a bad shape —
    returns what it can salvage, because an extractor must not crash a cron."""
    try:
        data = json.loads(_strip_fences(raw))
    except (json.JSONDecodeError, ValueError):
        return ExtractionResult()
    if not isinstance(data, dict):
        return ExtractionResult()

    entities: list[ExtractedEntity] = []
    by_name: dict[str, str] = {}  # name -> resolved type (for relationship typing)
    for e in data.get("entities", []) or []:
        if not isinstance(e, dict):
            continue
        name = clean_entity_name(str(e.get("name", "")).strip())
        if not name:
            continue
        hinted = e.get("type")
        # The classifier is conservative — it calls something an Organization only on a
        # strong signal (a legal-form token like LLC/Corp). That precision overrides a
        # bad model hint ("Acme Holdings LLC" labelled Person); otherwise trust a valid
        # hint, and fall back to the classifier (which defaults a plain name to Person).
        classified = classify_entity_type(name)
        if classified == "Organization":
            type_ = "Organization"
        elif hinted in ("Person", "Organization"):
            type_ = hinted
        else:
            type_ = classified
        props = {
            str(k): str(v)
            for k, v in (e.get("properties") or {}).items()
            if v is not None and str(v).strip()
        }
        entities.append(ExtractedEntity(name=name, type=type_, properties=props))
        by_name[name] = type_

    relationships: list[ExtractedRelationship] = []
    for rel in data.get("relationships", []) or []:
        if not isinstance(rel, dict):
            continue
        f = clean_entity_name(str(rel.get("from", "")).strip())
        t = clean_entity_name(str(rel.get("to", "")).strip())
        rtype = str(rel.get("type", "")).strip() or "related_to"
        if f and t and f in by_name and t in by_name:
            relationships.append(ExtractedRelationship(f, t, rtype))
    return ExtractionResult(entities=entities, relationships=relationships)


async def extract_document(
    actions: Actions,
    text: str,
    llm: LLMClient,
    *,
    case_id: uuid.UUID | None = None,
    model: str | None = None,
    source_id: str = _SOURCE,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    """Extract entities/relationships from `text` via `llm` and emit them DERIVED.
    Returns counts + the canonical of each entity. Idempotent (find-or-create)."""
    model = model or get_settings().osiris_extract_model
    raw = await llm.complete(system=_SYSTEM, prompt=text, model=model)
    result = parse_extraction(raw)
    observed = observed_at or datetime.now(UTC)

    ids: dict[str, uuid.UUID] = {}
    for ent in result.entities:
        canonical = _canonical(ent.type, ent.name)
        oid = await actions.create_or_find_object(ent.type, canonical, source_id, case_id)
        ids[ent.name] = oid
        await actions.assert_property(
            oid, "name", ent.name, source_id, observed, _CONF, case_id=case_id,
            evidence_class=_EC.value,
        )
        for k, v in ent.properties.items():
            await actions.assert_property(
                oid, k, v, source_id, observed, _CONF, case_id=case_id, evidence_class=_EC.value
            )

    n_links = 0
    for rel in result.relationships:
        f, t = ids.get(rel.from_name), ids.get(rel.to_name)
        if f is None or t is None:
            continue
        # Link types are a CONTROLLED vocabulary (ontology/schema.py). An LLM emits
        # free-form phrases ("officer_of", "acquired_by", …) — a known one passes
        # through; anything else is demoted to a generic `related_to` link with the
        # raw phrase kept in `relation`. Nuance survives as data; the catalog stays clean.
        if is_known_link_type(rel.type):
            link_type, props = rel.type, None
        else:
            link_type, props = "related_to", {"relation": rel.type}
        await actions.create_link(
            f, t, link_type, source_id, observed, _CONF, case_id=case_id,
            properties=props, evidence_class=_EC.value,
        )
        n_links += 1

    return {
        "entities": len(result.entities),
        "relationships": n_links,
        "canonicals": [_canonical(e.type, e.name) for e in result.entities],
    }

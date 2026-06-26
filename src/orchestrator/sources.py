"""Source & analysis registry — the investigation playbook, as data.

The one piece of judgment that lived in the operator's head (or the AI's): given an
object, WHICH sources are worth pulling and WHICH analyses apply. Encoding it here
turns "what do I do next?" into a lookup both the human front-end and an MCP client can
read — so neither has to *know* that a private company means SEC Form D. Every entry
maps to a real capability (an ingest function or a read-model), names what it yields,
and is keyless unless flagged.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class Capability:
    id: str
    label: str
    kind: Literal["collect", "analyze"]
    applies_to: tuple[str, ...]        # object types this is worth running on
    yields: str                        # what it surfaces, in one line
    tool: str                          # the MCP tool / function that runs it
    keyless: bool = True


# COLLECT — federate a base or crawl, materializing new nodes/links.
_COLLECT: tuple[Capability, ...] = (
    Capability(
        "wikidata", "Wikidata entity + network", "collect", ("Organization", "Person"),
        "founders/officers, official social accounts, relationship network", "aim_entity",
    ),
    Capability(
        "edgar_formd", "SEC Form D (private placements)", "collect", ("Organization",),
        "private financing rounds — officers, amounts, investor counts, feeder SPVs",
        "ingest_form_d",
    ),
    Capability(
        "edgar_expand", "Repeat-player portfolio (Form D)", "collect", ("Person", "Organization"),
        "every filing MENTIONING this operator → their whole co-investment book",
        "expand_operator",
    ),
    Capability(
        "clinicaltrials", "ClinicalTrials.gov", "collect", ("Organization",),
        "registered human trials — status, sites, investigators, posted results",
        "ingest_trials",
    ),
    Capability(
        "facility_cotenants", "Clinical-site co-tenants", "collect", ("Organization",),
        "the OTHER sponsors running trials at this facility", "expand_facility",
    ),
    Capability(
        "footprint", "Keyless web footprint", "collect",
        ("Username", "Email", "Account", "Person", "Domain", "URL"),
        "GitHub/social/web identifiers via the cascade", "expand_case",
    ),
    Capability(
        "litigation", "Court records (CourtListener)", "collect", ("Organization", "Person"),
        "lawsuits & enforcement — dockets, parties, judges; 'sued or charged?'",
        "ingest_litigation",
    ),
)

# ANALYZE — read-model lenses over what's already in the graph (no new collection).
_ANALYZE: tuple[Capability, ...] = (
    Capability(
        "dossier", "Entity dossier", "analyze", ("Organization", "Person"),
        "identity properties + the named relationship network", "entity_dossier",
    ),
    Capability(
        "discrepancy", "Footprint discrepancy", "analyze", ("Organization",),
        "operational geography that the disclosed home omits (shadow footprint)",
        "footprint_discrepancy",
    ),
    Capability(
        "coinvestment", "Co-investment ties", "analyze", ("Organization",),
        "other companies funded by SPVs that share an operator with this one",
        "coinvestment_ties",
    ),
    Capability(
        "subject_report", "Subject report (footprint)", "analyze",
        ("Person", "Account", "Username", "Email"),
        "who is this? — Verified / Corroborated / Speculative tiers", "subject_report",
    ),
    Capability(
        "sanctions_screen", "Watchlist screening", "analyze", ("Person", "Organization"),
        "name/identifier matches against the ingested sanctions/PEP base",
        "find_sanctions_candidates",
    ),
)

REGISTRY: tuple[Capability, ...] = _COLLECT + _ANALYZE


def suggest(object_type: str) -> list[Capability]:
    """Capabilities worth running on an object of this type — collect first, then
    analyze. This is the externalized 'what next?' the operator used to supply."""
    hits = [c for c in REGISTRY if object_type in c.applies_to]
    return sorted(hits, key=lambda c: (c.kind != "collect", c.id))


def as_dicts(caps: list[Capability]) -> list[dict[str, object]]:
    return [
        {
            "id": c.id, "label": c.label, "kind": c.kind, "yields": c.yields,
            "tool": c.tool, "keyless": c.keyless,
        }
        for c in caps
    ]

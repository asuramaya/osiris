"""The semantic layer — the declared Object-Type and Link-Type catalog.

Palantir's ontology composes from a SMALL, DECLARED primitive set: object types,
link types, properties (the "what"); action types + functions (the "how"). Osiris
already has the kinetic waist (`actions/core.py` — the only write path) and a function
registry (`orchestrator/sources.py` — the capability playbook). This module supplies
the missing semantic layer: the single source of truth for WHAT TYPES EXIST, so types
stop being invented inline by each parser and scattered across the UI, the parsers, and
the docs.

Everything reads this catalog — the UI takes its colours/shapes from it, the generic
Object view treats every type through one Interface (type · graded properties · links ·
provenance), and `validate_*` lets a caller check a type is known. Osiris's two
enrichments over Palantir — evidence-grading and event-sourcing — are kernel-wide
invariants (every property/link carries provenance; the graph is an append-only
ledger), so they live in `actions/core.py` + `parsers/evidence.py`, not per-type here.

This is a CATALOG, not a framework: a declarative registry existing code reads. A new
type is a new entry here (reviewed), never an inline string. Seeded from the types and
links actually in the graph.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("osiris.schema")


@dataclass(frozen=True)
class ObjectType:
    """One declared kind of entity. `schemes` are the canonical-id prefixes it uses
    (e.g. cik:, lei:); `color`/`shape` drive every surface's rendering."""

    name: str
    category: str
    color: str
    shape: str  # cytoscape node shape
    description: str
    schemes: tuple[str, ...] = ()


@dataclass(frozen=True)
class LinkType:
    """One declared relationship. `domain`/`range` are advisory (what tends to connect
    to what); empty means unconstrained."""

    name: str
    description: str
    domain: tuple[str, ...] = ()
    range: tuple[str, ...] = ()


# --- Object types (the "what") — grouped by category for the UI -------------
_OBJECT_TYPES: tuple[ObjectType, ...] = (
    # Entities — the core of the open-base graph
    ObjectType("Organization", "Entity", "#4493f8", "round-rectangle",
               "A company, fund, agency, or other organization.",
               ("cik:", "lei:", "bc-reg:", "Q", "sec-org:", "company:", "ctgov-org:")),
    ObjectType("Person", "Entity", "#e3b341", "ellipse",
               "An individual — officer, director, investigator, developer, identity hub "
               "(resolved probabilistically; never auto-merged).",
               ("sec-person:", "ctgov-person:", "Q", "subject:", "cluster:", "person:", "dev:")),
    # Assets
    ObjectType("CryptoAddress", "Asset", "#f0883e", "diamond",
               "An on-chain wallet/address (EVM, fused with OFAC designations).",
               ("eth:", "wallet:")),
    ObjectType("Property", "Asset", "#39c5cf", "round-rectangle",
               "A real-property parcel / foreclosure notice.", ("harris-notice:",)),
    # Records
    ObjectType("CourtCase", "Record", "#ca8aff", "rectangle",
               "A litigation docket / opinion (parties, court, judge).", ("courtlistener:",)),
    ObjectType("ClinicalTrial", "Record", "#56d4dd", "rectangle",
               "A registered human trial (status, sites, investigators).", ("nct:",)),
    ObjectType("ObservedData", "Record", "#8b949e", "rectangle",
               "Raw evidence — a scraped record / feed entry behind a claim.",
               ("ioc:", "threatfox:")),
    # Identity / footprint
    ObjectType("Account", "Identity", "#ffd33d", "round-rectangle",
               "A platform account (github:, twitter:, …) — a footprint fragment.",
               ("github:", "twitter:", "linkedin:", "instagram:", "youtube:", "facebook:",
                "soundcloud:", "replit:", "gitlab:", "pypi:")),
    ObjectType("Username", "Identity", "#ffab70", "ellipse",
               "A handle — connective tissue across platforms."),
    ObjectType("Email", "Identity", "#7ee787", "ellipse", "An email-address observable."),
    ObjectType("Phone", "Identity", "#85e89d", "ellipse",
               "A phone-number observable (enriched offline)."),
    # Web
    ObjectType("URL", "Web", "#6cb6ff", "round-rectangle",
               "A web page / search hit / archived snapshot.", ("http", "https")),
    ObjectType("Domain", "Web", "#39c5cf", "ellipse", "A DNS domain observable.",
               ("about.me",)),
    # Threat-intel (the second frontend)
    ObjectType("IntrusionSet", "ThreatIntel", "#f85149", "hexagon",
               "An actor cluster — tracked related intrusion activity."),
    ObjectType("ThreatActor", "ThreatIntel", "#da3633", "hexagon",
               "The human or group behind activity."),
    ObjectType("Campaign", "ThreatIntel", "#db61a2", "hexagon",
               "Time-bounded activity attributed to an actor."),
    ObjectType("Malware", "ThreatIntel", "#d29922", "round-rectangle",
               "Malicious software — a capability an actor uses."),
    ObjectType("Tool", "ThreatIntel", "#bc8cff", "round-rectangle",
               "Legitimate/utility software used in operations."),
    ObjectType("AttackPattern", "ThreatIntel", "#3fb950", "diamond",
               "A technique (MITRE ATT&CK Txxxx) — HOW something is done."),
    ObjectType("Indicator", "ThreatIntel", "#58a6ff", "triangle",
               "An IOC / detection signal — a hash, IP, or domain."),
    ObjectType("CourseOfAction", "ThreatIntel", "#79c0ff", "round-rectangle",
               "A mitigation / response to a technique (ATT&CK course-of-action)."),
    ObjectType("Tactic", "ThreatIntel", "#a5d6ff", "diamond",
               "An ATT&CK tactic — the adversary's goal a technique serves."),
    ObjectType("Identity", "ThreatIntel", "#ffa657", "ellipse",
               "A STIX identity — the named individual/org/sector behind activity."),
    # Software — a project tracking its own genesis (Osiris ingests its own git history;
    # proof that the engine is a general substrate, not OSINT-only).
    ObjectType("SoftwareProject", "Software", "#8ab4f8", "round-rectangle",
               "A software repository / project.", ("repo:",)),
    ObjectType("Commit", "Software", "#a371f7", "ellipse",
               "A version-control commit — an event in a project's history.", ("commit:",)),
    ObjectType("Thread", "Software", "#f0883e", "diamond",
               "An open thread / wall / next-step — project memory of what's unresolved.",
               ("thread:",)),
    ObjectType("Reference", "Software", "#7ee787", "round-rectangle",
               "A design/reference document — external canon (Palantir/Notion) or own docs, "
               "ingested as project memory.", ("ref:",)),
    ObjectType("Decision", "Software", "#d2a8ff", "octagon",
               "An architectural/design decision mined from the project's own commit "
               "rationale — the 'why', as queryable institutional memory.", ("decision:",)),
    ObjectType("File", "Software", "#6cb6ff", "round-rectangle",
               "A tracked file in a repository (metadata only — content stays in git, "
               "read on demand). Its `role` lets analogous files be compared across repos.",
               ("file:",)),
    ObjectType("Agent", "Software", "#f778ba", "hexagon",
               "A Claude instance operating over the graph — an analyst in the fleet. "
               "Carries its model (source-model provenance) and works in a project on "
               "behalf of a principal. 'A man and all his imaginary friends.'", ("agent:",)),
    ObjectType("Tension", "Software", "#e685b5", "vee",
               "A held POLARITY — two positions in productive tension, neither settled. Unlike a "
               "Decision (which settles) or a Thread (which closes), a tension is HELD: the "
               "current lean is recorded but never auto-resolved or consolidated away; the lean "
               "history is the dance across sessions.", ("tension:",)),
    ObjectType("Seat", "Software", "#ffd33d", "star",
               "A durable ROLE in a house — the fleet's addressable identity (the identity "
               "core, ruling 5cef856b). Minted ONCE as seat:<uuid8>, never re-keyed: handle, "
               "house, and anchor_cwd are mutable ASSERTIONS on it, because the whole bug "
               "class was keying identity on mutable facts (path, session). Minds (Agents) "
               "hold it in succession via `holds`; the seat outlives them all — it exists "
               "BEFORE its first session, which is what lets the daemon export it at birth.",
               ("seat:",)),
    # Observables
    ObjectType("IPv4", "Observable", "#2dd4bf", "ellipse", "An IPv4 address observable."),
    ObjectType("TelegramChannel", "Observable", "#56a3ff", "ellipse",
               "A Telegram channel observable."),
    ObjectType("FileHash", "Observable", "#f0883e", "triangle",
               "A file hash observable (md5/sha1/sha256)."),
    ObjectType("Phrase", "Observable", "#adbac7", "ellipse", "Free text — a search seed."),
)

# --- Link types (the relationships) — seeded from what's in the graph -------
_LINK_TYPES: tuple[LinkType, ...] = (
    LinkType("controlled_by", "Asset/wallet is controlled by a holder.",
             ("CryptoAddress",), ("Person", "Organization")),
    LinkType("owns", "Owns the target.", ("Person", "Organization")),
    LinkType("owned_by", "Is owned by the target."),
    LinkType("subsidiary_of", "Is a subsidiary of the target org.",
             ("Organization",), ("Organization",)),
    LinkType("ultimate_parent", "Ultimate parent org (GLEIF level-2).",
             ("Organization",), ("Organization",)),
    LinkType("founded_by", "Founded by the target person.", ("Organization",), ("Person",)),
    LinkType("officer", "Has the target as an officer.", ("Organization",), ("Person",)),
    LinkType("director", "Has the target as a director.", ("Organization",), ("Person",)),
    LinkType("ceo", "Has the target as CEO.", ("Organization",), ("Person",)),
    LinkType("chairperson", "Has the target as chairperson.", ("Organization",), ("Person",)),
    LinkType("directs", "Person directs the target org.", ("Person",), ("Organization",)),
    LinkType("promoter", "Promoter of the target."),
    LinkType("represents", "Legal/agent representation."),
    LinkType("associate_of", "Known associate.", ("Person",), ("Person",)),
    LinkType("member_of", "Membership in an organization.", ("Person",), ("Organization",)),
    LinkType("employs", "Employment relationship.", ("Organization",), ("Person",)),
    LinkType("not_same_as", "Negative ER memory — confirmed distinct (suppresses re-match)."),
    LinkType("family", "Familial relationship.", ("Person",), ("Person",)),
    LinkType("sponsors", "Sponsors the target (trial/event).",
             ("Organization",), ("ClinicalTrial",)),
    LinkType("investigator", "Investigator on the target trial.",
             ("ClinicalTrial",), ("Person",)),
    LinkType("site", "Trial site / facility.", ("ClinicalTrial",), ("Organization",)),
    LinkType("raises_for", "Feeder SPV raises capital for the core company.",
             ("Organization",), ("Organization",)),
    LinkType("transacted_with", "On-chain counterparty (aggregated flow).",
             ("CryptoAddress",), ("CryptoAddress",)),
    LinkType("litigation", "Party/mention in a court case.",
             ("Organization", "Person"), ("CourtCase",)),
    LinkType("appears_in", "Subject appears in the target record."),
    LinkType("has_account", "Identity hub has the target account.",
             ("Person",), ("Account",)),
    LinkType("has_email", "Has the target email.", ("Person",), ("Email",)),
    LinkType("has_url", "Has/owns the target URL.", (), ("URL",)),
    LinkType("has_domain", "Has/owns the target domain.", (), ("Domain",)),
    LinkType("has_subdomain", "Subdomain of.", ("Domain",), ("Domain",)),
    LinkType("is_profile", "Account is a profile of the subject.", ("Account",)),
    LinkType("declares", "Self-declared social/owned link (rel=me, profile).",),
    LinkType("committed_as", "Commit-authored as the target email.", (), ("Email",)),
    LinkType("derived_handle", "Handle derived from a local-part (speculative)."),
    LinkType("co_occurs", "Co-occurs near the subject in a snippet (speculative)."),
    LinkType("rel_me", "Self-declared identity link (rel=me / profile).",
             ("Account", "Person"), ("Account", "URL")),
    LinkType("spouse", "Spouse.", ("Person",), ("Person",)),
    LinkType("registered_with", "Registered with the target authority/registry."),
    LinkType("related_to", "Generic association — the specific kind (e.g. an AI-extracted "
             "relationship) is kept in the link's `relation` property."),
    LinkType("search_variant", "A search/handle variant."),
    LinkType("linked_to", "Generic association."),
    LinkType("has_observation", "Points at raw observed evidence.", (), ("ObservedData",)),
    LinkType("acts_for", "Agent acts on behalf of the principal (AUTHORITY).",
             ("Agent",), ("Person",)),
    LinkType("works_in", "Agent operates in the target project.",
             ("Agent",), ("SoftwareProject",)),
    LinkType("spawned_by", "Sub-agent was spawned by (delegated from) its direct parent agent "
             "— the fractal DELEGATION tree, distinct from acts_for (authority).",
             ("Agent",), ("Agent",)),
    LinkType("succeeded_from", "Minted heir → its ancestor (ruling be292762): a fresh context "
             "arriving across a succession seam or wearing a retired face is MINTED its own "
             "lineage-linked id (agent:<base>-ii…) instead of writing under the dead name — "
             "SUCCESSION, distinct from spawned_by (delegation).",
             ("Agent",), ("Agent",)),
    LinkType("succeeds_seat", "Holder → the mind that held this SEAT before it (operator's "
             "HOUSE/SEAT/HOLDER ruling, 2026-07-12; asked for by Ra V, a-sibling). DISTINCT "
             "from succeeded_from, which is ANCHOR ancestry — the same conversation minting an "
             "heir across a model swap. This is a different conversation taking up the same JOB "
             "in the same house, so a seat's history is WALKABLE from the record. Ra could not "
             "walk it, mistook his live CONTEMPORARY for his own ghost, and asked to be merged "
             "with a stranger; the missing edge is what made that reading possible.",
             ("Agent",), ("Agent",)),
    LinkType("governs", "THE CHARTER (Phase 1 §4.1, ruling dd47c1da): the repos a SEAT rules — "
             "'a house is what a seat governs, not where it sits'. Distinct from works_in (the "
             "durable mount's current home): governs is an explicit, self-declared charter that "
             "survives a folder move (alfred's charter is six repos; a move to a new cwd does "
             "not shrink it). Healed by a compensating event (`valid_until`) when a repo drops "
             "off the charter — never DELETE, so a seat's shrinking rule stays a fact the graph "
             "remembers.", ("Agent",), ("SoftwareProject",)),
    LinkType("holds", "THE BINDING (identity core, ruling 5cef856b): the mind currently "
             "holding a durable Seat. Minted at attach (the ceremony: a one-time token the "
             "spawner exported at birth), RE-LINKED to the heir at every mint so the binding "
             "follows the lineage head; the old link heals by `valid_until`, never DELETE — "
             "a seat's holder history stays walkable. Distinct from succeeds_seat (holder → "
             "prior holder, mind-to-mind): holds is mind → ROLE.",
             ("Agent",), ("Seat",)),
    LinkType("archived_snapshot", "A Wayback/archive snapshot of the target.", (), ("URL",)),
    LinkType("same_as", "Identity merge edge (loser → winner)."),
    LinkType("uses", "Actor/source employs this capability or technique."),
    LinkType("indicates", "Indicator points at the linked malware/technique.",
             ("Indicator",)),
    LinkType("based-on", "Indicator derived from raw observed evidence.", ("Indicator",)),
    LinkType("subtechnique-of", "A more specific technique under a broader one.",
             ("AttackPattern",), ("AttackPattern",)),
    LinkType("authored_by", "Commit authored by a developer.", ("Commit",), ("Person",)),
    LinkType("in_repo", "Commit or file belongs to a repository.",
             ("Commit", "File"), ("SoftwareProject",)),
    LinkType("follows", "Commit follows its parent (the history DAG).",
             ("Commit",), ("Commit",)),
    LinkType("noted_in", "A thread / wall surfaced in this commit's rationale.",
             ("Thread",), ("Commit",)),
    LinkType("resolved_by", "The later commit that addressed this thread (closes it).",
             ("Thread",), ("Commit",)),
    LinkType("cites", "This document cites / draws from that reference.",
             ("Reference",), ("Reference",)),
    LinkType("informs", "This reference grounds / informs that artifact.",
             ("Reference",), ("SoftwareProject", "Commit")),
    LinkType("mentions", "This document names that entity in its text (the doc joins the graph).",
             ("Reference", "Commit"), ("Organization", "Person")),
    LinkType("decided_in", "Decision was stated in this commit (the 'why', sourced).",
             ("Decision",), ("Commit",)),
    LinkType("supersedes", "This decision overrides/replaces an earlier one.",
             ("Decision",), ("Decision",)),
    LinkType("grounded_by", "Decision is grounded by this design reference (the canon).",
             ("Decision",), ("Reference",)),
    LinkType("answers", "This decision is the ANSWER to that thread — the ruling a question was "
                        "minted to get. Distinct from decided_in (where it was said) and "
                        "grounded_by (what it rests on): this names what it SETTLED. Minted by "
                        "record_decision(resolves=…), which closes the thread in the same act, "
                        "so a ruling that names its question never leaves the question lit.",
             ("Decision",), ("Thread",)),
)

OBJECT_TYPES: dict[str, ObjectType] = {t.name: t for t in _OBJECT_TYPES}
LINK_TYPES: dict[str, LinkType] = {lt.name: lt for lt in _LINK_TYPES}

_DEFAULT = ObjectType("Unknown", "Other", "#6e7681", "ellipse", "An ontology object.")


def object_type(name: str) -> ObjectType:
    """The declared type, or a neutral default (so the UI never breaks on a new type)."""
    return OBJECT_TYPES.get(name, _DEFAULT)


def is_known_object_type(name: str) -> bool:
    return name in OBJECT_TYPES


def is_known_link_type(name: str) -> bool:
    return name in LINK_TYPES


# --- enforcement: the kinetic waist validates against the semantic layer -----
# Default is WARN (a novel type logs but never crashes a user's run). Tests flip this
# to STRICT (an undeclared type raises) so the build fails — the discipline bites in CI
# without breaking production on an edge case.

class UnknownTypeError(Exception):
    """Raised in strict mode when an undeclared object/link type is emitted."""


_STRICT = False


def set_strict(strict: bool) -> None:
    global _STRICT
    _STRICT = strict


def _complain(kind: str, name: str) -> None:
    msg = f"undeclared {kind} {name!r} — add it to ontology/schema.py (the catalog)"
    if _STRICT:
        raise UnknownTypeError(msg)
    logger.warning(msg)


def check_object_type(name: str) -> None:
    if name not in OBJECT_TYPES:
        _complain("object type", name)


def check_link_type(name: str) -> None:
    if name not in LINK_TYPES:
        _complain("link type", name)


def categories() -> list[str]:
    """Object-type categories in declaration order (for grouped display)."""
    out: list[str] = []
    for t in _OBJECT_TYPES:
        if t.category not in out:
            out.append(t.category)
    return out


def _connects(lt: LinkType) -> str:
    dom = "/".join(lt.domain) if lt.domain else "*"
    rng = "/".join(lt.range) if lt.range else "*"
    return "—" if not lt.domain and not lt.range else f"{dom} → {rng}"


def render_reference() -> str:
    """Render the data-model section of docs/REFERENCE.md FROM this catalog, so the
    docs can never drift from the code. Regenerate with `python -m src.ontology.schema`;
    a test asserts REFERENCE.md matches this exactly."""
    lines = ["## Data model", "",
             "Generated from `src/ontology/schema.py` — the declared semantic layer "
             "(the single source of truth). Do not edit by hand; run "
             "`python -m src.ontology.schema`.", ""]
    for cat in categories():
        lines.append(f"### {cat} object types")
        lines.append("")
        lines.append("| Type | Canonical schemes | Description |")
        lines.append("|------|-------------------|-------------|")
        for t in _OBJECT_TYPES:
            if t.category != cat:
                continue
            schemes = " · ".join(f"`{s}`" for s in t.schemes) or "—"
            lines.append(f"| `{t.name}` | {schemes} | {t.description} |")
        lines.append("")
    lines.append("### Link types")
    lines.append("")
    lines.append("| Link | Connects | Meaning |")
    lines.append("|------|----------|---------|")
    for lt in _LINK_TYPES:
        lines.append(f"| `{lt.name}` | {_connects(lt)} | {lt.description} |")
    return "\n".join(lines) + "\n"


def catalog() -> dict[str, Any]:
    """JSON-serializable catalog for the API / UI — the semantic layer as data."""
    return {
        "object_types": [
            {
                "name": t.name, "category": t.category, "color": t.color,
                "shape": t.shape, "description": t.description, "schemes": list(t.schemes),
            }
            for t in _OBJECT_TYPES
        ],
        "link_types": [
            {
                "name": lt.name, "description": lt.description,
                "domain": list(lt.domain), "range": list(lt.range),
            }
            for lt in _LINK_TYPES
        ],
        "categories": categories(),
    }


if __name__ == "__main__":  # pragma: no cover - doc generator
    print(render_reference(), end="")

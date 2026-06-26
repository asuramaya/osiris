"""Sourced dossier report — the follow-the-money investigator's deliverable.

The engine's output stops at a graph; an investigator ships a *story with receipts*.
This assembles the graph around one entity into a litigation-defensible Markdown
dossier where EVERY claim carries its provenance inline (source · how-obtained · when).
That provenance — the append-only, evidence-graded kernel — is the point: the report
is built to survive a defamation threat. Sections: identity, network, financing
(Form D feeders), litigation, footprint discrepancy, co-investment, and a sources
appendix. Merge-aware (reads the whole identity cluster).
"""

from __future__ import annotations

import uuid
from typing import Any

import asyncpg

from src.orchestrator.coinvest import _identity_set, coinvestment_ties
from src.orchestrator.discrepancy import footprint_discrepancy

# compact provenance tags for the evidence classes
_TAG = {
    "self_declared": "self-declared", "authoritative_api": "authoritative",
    "direct_observation": "observed", "co_occurrence": "co-occurrence",
    "derived": "derived", "corroborated": "corroborated", None: "unclassified",
}


def _prov(source: str | None, ec: str | None, when: Any = None) -> str:
    bits = [b for b in (source, _TAG.get(ec, ec), str(when)[:10] if when else None) if b]
    return f"_({' · '.join(bits)})_"


def _sub(prop: str, ref: str) -> str:
    """A correlated subselect for a property value (keeps the inline SQL readable)."""
    return (
        f"(SELECT value #>> '{{}}' FROM current_assertions "
        f"WHERE object_id={ref} AND name='{prop}')"
    )


async def build_dossier_report(pool: asyncpg.Pool, object_id: uuid.UUID) -> str:
    """Assemble a provenance-annotated Markdown dossier for an entity."""
    cluster = await _identity_set(pool, object_id)
    header = await pool.fetchrow(
        "SELECT type, canonical, "
        "  (SELECT value #>> '{}' FROM current_assertions a "
        "   WHERE a.object_id = o.id AND a.name='name' ORDER BY confidence DESC LIMIT 1) AS name "
        "FROM objects o WHERE o.id = ANY($1::uuid[]) AND o.status='active' "
        "ORDER BY (SELECT count(*) FROM current_assertions x WHERE x.object_id=o.id) DESC LIMIT 1",
        cluster,
    )
    name = (header and header["name"]) or (header and header["canonical"]) or str(object_id)
    out: list[str] = [f"# Dossier: {name}", ""]
    if header:
        out.append(f"*{header['type']} · `{header['canonical']}`*")
        out.append("")
    sources: set[str] = set()

    # --- identity ---------------------------------------------------------
    props = await pool.fetch(
        "SELECT name, value #>> '{}' AS value, source_id, evidence_class, observed_at "
        "FROM current_assertions WHERE object_id = ANY($1::uuid[]) "
        "AND name NOT IN ('name','tag','parties','attorneys','firms') "
        "ORDER BY name, confidence DESC NULLS LAST",
        cluster,
    )
    if props:
        out += ["## Identity", ""]
        for r in props:
            sources.add(r["source_id"] or "")
            out.append(
                f"- **{r['name']}**: {r['value']}  "
                f"{_prov(r['source_id'], r['evidence_class'], r['observed_at'])}"
            )
        out.append("")

    # --- financing (Form D feeders raising for it) ------------------------
    feeders = await pool.fetch(
        f"SELECT {_sub('name', 's.id')} nm, {_sub('amount_raised', 's.id')} amt, "
        f"  {_sub('investors', 's.id')} inv "
        "FROM links l JOIN objects s ON s.id=l.from_id "
        "WHERE l.to_id = ANY($1::uuid[]) AND l.type='raises_for' "
        f"ORDER BY {_sub('amount_raised', 's.id')}::numeric DESC NULLS LAST LIMIT 25",
        cluster,
    )
    if feeders:
        sources.add("edgar")
        out += ["## Financing — Form D feeder funds", ""]
        seen_f: set[str] = set()
        for r in feeders:
            if not r["nm"] or r["nm"] in seen_f:
                continue
            seen_f.add(r["nm"])
            amt = f"${int(r['amt']):,}" if r["amt"] else "undisclosed"
            inv = f", {r['inv']} investors" if r["inv"] else ""
            out.append(f"- **{r['nm']}** — raised {amt}{inv}  _(edgar · authoritative)_")
        out.append("")

    # --- litigation -------------------------------------------------------
    cases = await pool.fetch(
        f"SELECT l.evidence_class ec, {_sub('name', 'c.id')} nm, {_sub('court', 'c.id')} court, "
        f"  {_sub('date_filed', 'c.id')} dt, {_sub('nature', 'c.id')} nature "
        "FROM links l JOIN objects c ON c.id=l.to_id AND c.type='CourtCase' "
        "WHERE l.from_id = ANY($1::uuid[]) AND l.type='litigation' "
        "ORDER BY (l.evidence_class='direct_observation') DESC, dt DESC NULLS LAST LIMIT 30",
        cluster,
    )
    if cases:
        sources.add("courtlistener")
        out += ["## Litigation", ""]
        seen_c: set[str] = set()
        for r in cases:
            if not r["nm"] or r["nm"] in seen_c:
                continue
            seen_c.add(r["nm"])
            role = "**named party**" if r["ec"] == "direct_observation" else "mentioned"
            meta = " · ".join(b for b in (r["court"], (r["dt"] or "")[:10], r["nature"]) if b)
            out.append(f"- ⚖ {r['nm']} — {meta}  ({role})")
        out.append("")

    # --- footprint discrepancy -------------------------------------------
    disc = await footprint_discrepancy(pool, object_id)
    if disc.get("discrepancies"):
        out += ["## Footprint discrepancy", ""]
        out.append(
            f"Discloses: **{', '.join(disc['home'])}**. "
            "Operates where it discloses no presence:"
        )
        for d in disc["discrepancies"]:
            who = ", ".join(sorted({x["name"] for x in d["reach"] if x["name"]})[:4])
            out.append(f"- ⚑ **{d['country']}** — {who}")
        out.append("")

    # --- co-investment ----------------------------------------------------
    ties = await coinvestment_ties(pool, object_id, limit=10)
    if ties:
        out += ["## Co-investment ties", ""]
        for t in ties:
            out.append(f"- **{t['company']}** — {t['shared_operators']} shared SPV operator(s)")
        out.append("")

    # --- sources appendix -------------------------------------------------
    if sources:
        out += ["## Sources", ""]
        out.append(", ".join(f"`{s}`" for s in sorted(s for s in sources if s)))
        out.append("")
    return "\n".join(out)

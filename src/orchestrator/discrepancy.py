"""Footprint-discrepancy analysis — where an entity OPERATES vs where it DISCLOSES.

The interesting OSINT signal is rarely a single fact; it's a contradiction. An entity
discloses a home (incorporation, HQ, registered address) but its *activities* — trial
sites, feeder funds, officers — touch jurisdictions its corporate footprint never
mentions. Those foreign reach-points are the shadow footprint: a US company running
brain-implant trials in Abu Dhabi, or raising retail money through a Jakarta fund,
discloses neither.

This resolves the entity's full cluster (the same company is fragmented across bases
until merged), reads its HOME countries from its own location facts, sweeps the
locations of everything within two hops, and flags the operational countries the home
set doesn't cover.
"""

from __future__ import annotations

import uuid
from typing import Any

import asyncpg

from src.ontology.resolution import normalize_org_name

# US state / territory postal codes -> the home country is the United States.
_US_STATES = frozenset(
    "AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT "
    "NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC PR".split()
)
# EDGAR stateOrCountry codes for foreign places (Form D officer addresses use these).
_EDGAR_FOREIGN = {
    "A0": "Canada", "A1": "Canada", "A2": "Canada", "A3": "Canada", "A4": "Canada",
    "A5": "Canada", "A6": "Canada", "A7": "Canada", "A8": "Canada", "A9": "Canada",
    "B0": "Canada", "B2": "Canada",
    "K8": "Indonesia", "F4": "China", "K3": "Hong Kong", "W8": "Singapore",
    "X5": "United Kingdom", "D8": "Germany", "Y6": "United Arab Emirates",
}
_COUNTRY_ALIASES = {
    "united states": "United States", "usa": "United States", "us": "United States",
    "u.s.": "United States", "uk": "United Kingdom", "u.k.": "United Kingdom",
    "united kingdom": "United Kingdom", "uae": "United Arab Emirates",
    "united arab emirates": "United Arab Emirates",
}
# location-bearing property names we read as an entity's OWN (disclosed home) facts.
_HOME_PROPS = ("incorporation_state", "address", "headquarters", "hq")


def country_of(location: str | None) -> str | None:
    """Best-effort country from a 'City, Region, Country' (or EDGAR-coded) string."""
    if not location:
        return None
    last = location.split(",")[-1].strip()
    if last in _US_STATES:
        return "United States"
    if last in _EDGAR_FOREIGN:
        return _EDGAR_FOREIGN[last]
    low = last.lower()
    if low in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[low]
    return last or None


async def _cluster(pool: asyncpg.Pool, object_id: uuid.UUID) -> list[uuid.UUID]:
    """The entity's full identity cluster: all active Organization objects whose name
    normalizes the same (the same company, still fragmented across bases pre-merge)."""
    name = await pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='name' LIMIT 1",
        object_id,
    )
    norm = normalize_org_name(name or "")
    if not norm:
        return [object_id]
    ids = {object_id}
    for r in await pool.fetch(
        "SELECT a.object_id FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id AND o.type='Organization' AND o.status='active' "
        "WHERE a.name='name'"
    ):
        if normalize_org_name(
            await pool.fetchval(
                "SELECT value #>> '{}' FROM current_assertions "
                "WHERE object_id=$1 AND name='name' LIMIT 1", r["object_id"]
            ) or ""
        ) == norm:
            ids.add(r["object_id"])
    return list(ids)


async def footprint_discrepancy(pool: asyncpg.Pool, object_id: uuid.UUID) -> dict[str, Any]:
    """Home countries vs operational reach. Returns the disclosed home set, the
    operational locations (each with its country, neighbor, and whether it's foreign),
    and the discrepancy: operational countries the home set never discloses."""
    cluster = await _cluster(pool, object_id)

    # HOME: the entity's own disclosed location facts
    home: set[str] = set()
    for r in await pool.fetch(
        "SELECT value #>> '{}' AS v FROM current_assertions "
        "WHERE object_id = ANY($1::uuid[]) AND name = ANY($2::text[])",
        cluster, list(_HOME_PROPS),
    ):
        c = country_of(r["v"])
        if c:
            home.add(c)
    home = home or {"United States"}  # default; CA incorporation etc. resolves here too

    # OPERATIONAL: locations of every node within two hops of the cluster
    rows = await pool.fetch(
        """
        WITH RECURSIVE nbr(id, depth) AS (
            SELECT unnest($1::uuid[]), 0
          UNION
            SELECT CASE WHEN l.from_id = n.id THEN l.to_id ELSE l.from_id END, n.depth + 1
            FROM nbr n JOIN links l ON (l.from_id = n.id OR l.to_id = n.id)
            WHERE n.depth < 2
        )
        SELECT DISTINCT o.id, o.type,
               (SELECT value #>> '{}' FROM current_assertions a
                WHERE a.object_id=o.id AND a.name='name' LIMIT 1) AS name,
               loc.value #>> '{}' AS location
        FROM nbr JOIN objects o ON o.id = nbr.id
        JOIN current_assertions loc ON loc.object_id = o.id AND loc.name = 'location'
        WHERE o.id <> ALL($1::uuid[])
        """,
        cluster,
    )

    operational: list[dict[str, Any]] = []
    by_country: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        c = country_of(r["location"])
        if not c:
            continue
        item = {
            "country": c, "location": r["location"], "type": r["type"], "name": r["name"],
            "foreign": c not in home,
        }
        operational.append(item)
        by_country.setdefault(c, []).append(item)

    discrepancies = [
        {"country": c, "reach": by_country[c]}
        for c in sorted(by_country)
        if c not in home
    ]
    return {
        "home": sorted(home),
        "operational_countries": sorted(by_country),
        "discrepancies": discrepancies,
    }

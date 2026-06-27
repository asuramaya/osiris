"""The semantic layer — the declared Object-Type / Link-Type catalog.

Guards the one anti-mess invariant: the catalog is the single source of truth and a
SUPERSET of what's actually in the graph. If a parser emits a type the catalog doesn't
declare, this fails — a new type must be a reviewed entry, not an inline string.
"""
from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.ontology.schema import (
    LINK_TYPES,
    OBJECT_TYPES,
    catalog,
    is_known_link_type,
    is_known_object_type,
    object_type,
)

# the types/links actually present in the demo graph (PG :5439) — the catalog must
# cover every one. Extend these lists when a new collector lands, in lockstep with schema.
_IN_USE_OBJECTS = {
    "Organization", "Person", "CryptoAddress", "URL", "CourtCase", "Account",
    "ObservedData", "ClinicalTrial", "Email", "Domain", "Phone", "Username",
    "IntrusionSet", "AttackPattern", "Malware", "Indicator", "Phrase",
    "TelegramChannel", "Tool", "IPv4",
}
_IN_USE_LINKS = {
    "site", "controlled_by", "appears_in", "litigation", "raises_for", "director",
    "officer", "owns", "archived_snapshot", "investigator", "family", "sponsors",
    "same_as", "has_account", "is_profile", "transacted_with", "has_observation",
    "has_url", "co_occurs", "linked_to", "founded_by", "has_email", "directs",
    "owned_by", "uses", "promoter", "derived_handle", "ceo", "indicates",
    "chairperson", "based-on", "search_variant", "has_domain", "represents",
    "committed_as", "has_subdomain", "subtechnique-of", "declares",
}


def test_catalog_covers_every_type_in_use() -> None:
    missing_obj = _IN_USE_OBJECTS - set(OBJECT_TYPES)
    assert not missing_obj, f"object types in the graph but not declared: {missing_obj}"
    missing_link = _IN_USE_LINKS - set(LINK_TYPES)
    assert not missing_link, f"link types in the graph but not declared: {missing_link}"


def test_every_object_type_is_fully_specified() -> None:
    for t in OBJECT_TYPES.values():
        assert t.color.startswith("#") and t.shape and t.category and t.description


def test_lookup_falls_back_safely() -> None:
    assert object_type("Organization").color == "#4493f8"
    # an unknown type returns a neutral default, never raises (the UI must not break)
    assert object_type("SomethingNew").name == "Unknown"
    assert is_known_object_type("Person")
    assert not is_known_object_type("Nope")
    assert is_known_link_type("controlled_by")


def test_catalog_is_serializable_with_expected_shape() -> None:
    c = catalog()
    assert {"object_types", "link_types", "categories"} <= c.keys()
    org = next(t for t in c["object_types"] if t["name"] == "Organization")
    assert org["color"] == "#4493f8" and "cik:" in org["schemes"]
    assert "Entity" in c["categories"]


# --- the API exposes it (the UI reads /schema) ------------------------------

@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_schema_endpoint_serves_the_catalog(client: httpx.AsyncClient) -> None:
    c = (await client.get("/schema")).json()
    names = {t["name"] for t in c["object_types"]}
    assert "Organization" in names and "Property" in names
    assert any(lt["name"] == "controlled_by" for lt in c["link_types"])

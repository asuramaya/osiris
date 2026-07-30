"""The semantic layer — the declared Object-Type / Link-Type catalog.

Guards the one anti-mess invariant: the catalog is the single source of truth and a
SUPERSET of what's actually in the graph. If a parser emits a type the catalog doesn't
declare, this fails — a new type must be a reviewed entry, not an inline string.
"""
from __future__ import annotations

import re
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.ontology import schema
from src.ontology.schema import (
    LINK_TYPES,
    OBJECT_TYPES,
    UnknownTypeError,
    catalog,
    check_link_type,
    check_object_type,
    is_known_link_type,
    is_known_object_type,
    object_type,
    render_reference,
)

_REFERENCE = Path(__file__).resolve().parent.parent / "docs" / "REFERENCE.md"

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


# --- enforcement: the kinetic waist validates against the catalog -----------

def test_guard_raises_in_strict_mode_passes_for_declared() -> None:
    schema.set_strict(True)  # (the autouse fixture also sets this)
    try:
        check_object_type("Organization")  # declared — fine
        check_link_type("controlled_by")
        with pytest.raises(UnknownTypeError):
            check_object_type("NotAThing")
        with pytest.raises(UnknownTypeError):
            check_link_type("not_a_real_link")
        # warn mode never raises (a novel type logs but doesn't crash a user's run)
        schema.set_strict(False)
        check_object_type("NotAThing")
    finally:
        schema.set_strict(True)


# --- the docs cannot drift: REFERENCE.md is generated from the catalog ------

@pytest.mark.skip(
    reason="retired by #111: the operator ruled docs need a COMPILER from the graph "
           "(boot_compiler.py's marker-wrapped-section pattern, a markdown render "
           "target alongside osiris.js's HTML one), not a hand-maintained artifact "
           "snapshotting a catalog. This byte-for-byte assertion's whole premise — a "
           "committed doc that must be regenerated by hand — is what #111 removes.")
def test_reference_data_model_is_generated_from_schema() -> None:
    """The data-model block in REFERENCE.md must equal schema.render_reference()
    exactly. If a type changes, regenerate: `python -m src.ontology.schema`."""
    txt = _REFERENCE.read_text()
    m = re.search(r"<!-- BEGIN generated:schema.*?-->\n(.*?)\n<!-- END generated:schema -->",
                  txt, re.S)
    assert m, "generated:schema marker block missing from REFERENCE.md"
    assert m.group(1) == render_reference().rstrip(), (
        "REFERENCE.md is stale — run `python -m src.ontology.schema` to regenerate"
    )

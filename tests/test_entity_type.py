"""Person vs Organization classification — the real Form D / SPV names it must get right."""
from __future__ import annotations

import pytest
from src.ontology.entity_type import (
    classify_entity_type,
    clean_entity_name,
    is_organization,
    is_plausible_person_name,
)

_ORGS = [
    "Brilliant Phoenix GP Inc.",
    "n/a Brilliant Phoenix GP Inc.",
    "LLC Sydecar",
    "MyAsia VC, LLC N/A",
    "Finally Fund Admin LLC",
    "Supermassive Capital LLC",
    "LLC MegaCap Capital",
    "AC VENTURES LLC SERIES NEURALINK 1",
    "NEURALINK A SERIES OF VUVP FUND LLC",
    "TWE Neuralink SPV MGR",
    "Brilliant Phoenix Mortgage Investment Corp.",
    "DataPower Capital Partners LLC",
    "Eagle VP Fund 2 Series Neuralink",
]
_PEOPLE = [
    "Chunhua Wu", "Ye Chen", "Alexander Mashinsky", "Daniel Leon", "S. Leon",
    "Sajid Rahman", "Brett Sagan", "Harumi Urata-Thompson", "Joseph Fernandes",
    "Melissa Garlough", "Adam Forsythe", "Ardit Dodaj", "Patrick Martin",
    "Elon Musk", "Jared Birchall", "Yaron Shalem", "John Smith Jr",
]


@pytest.mark.parametrize("name", _ORGS)
def test_organizations_detected(name: str) -> None:
    assert is_organization(name) is True
    assert classify_entity_type(name) == "Organization"


@pytest.mark.parametrize("name", _PEOPLE)
def test_people_not_misclassified(name: str) -> None:
    assert is_organization(name) is False
    assert classify_entity_type(name) == "Person"


def test_clean_strips_na_placeholder() -> None:
    assert clean_entity_name("n/a Brilliant Phoenix GP Inc.") == "Brilliant Phoenix GP Inc."
    assert clean_entity_name("MyAsia VC, LLC N/A") == "MyAsia VC, LLC"
    assert clean_entity_name("  Daniel  Leon ") == "Daniel Leon"


def test_generational_suffix_is_not_an_org_signal() -> None:
    assert is_organization("Charles Windsor III") is False
    assert is_organization("Fund 2") is True  # via 'fund', not the digit


def test_person_with_stray_digits_is_not_an_org() -> None:
    # a bare digit must NOT trigger org — people carry case/inmate numbers in filings
    assert is_organization("Desiree Lambert Inmate No. 13432-046") is False
    assert is_organization("John Smith 3rd") is False


def test_plausible_person_name_gate() -> None:
    assert is_plausible_person_name("Jaimie Henderson") is True
    assert is_plausible_person_name("Dr. Nader Pouratian") is True
    # contact strings / orgs / one-word / sentences are rejected
    assert is_plausible_person_name("Call 1-877-CTLILLY (1-877-285-4559)") is False
    assert is_plausible_person_name("Clinical Transparency (dept. 2834)") is False
    assert is_plausible_person_name("Brilliant Phoenix GP Inc.") is False
    assert is_plausible_person_name("Madonna") is False
    assert is_plausible_person_name("contact@trial.org") is False

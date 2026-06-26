"""The source/analysis registry — the playbook as data."""
from __future__ import annotations

from src.orchestrator.sources import REGISTRY, as_dicts, suggest


def test_suggest_private_company_includes_formd_and_trials() -> None:
    ids = {c.id for c in suggest("Organization")}
    # the judgment that used to live in the operator's head:
    assert {"edgar_formd", "clinicaltrials", "wikidata", "discrepancy", "coinvestment"} <= ids


def test_suggest_orders_collect_before_analyze() -> None:
    caps = suggest("Organization")
    kinds = [c.kind for c in caps]
    assert kinds == sorted(kinds, key=lambda k: k != "collect")  # all collect, then analyze


def test_suggest_person_is_footprint_and_screening() -> None:
    ids = {c.id for c in suggest("Person")}
    assert "footprint" in ids and "sanctions_screen" in ids
    assert "clinicaltrials" not in ids  # a person doesn't sponsor trials


def test_every_capability_names_a_tool() -> None:
    assert all(c.tool and c.label and c.yields for c in REGISTRY)
    d = as_dicts(suggest("Organization"))
    assert d and all("tool" in x and "yields" in x for x in d)

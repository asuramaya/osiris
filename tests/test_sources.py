"""The source/analysis registry — the playbook as data."""
from __future__ import annotations

import re

from src.orchestrator.compositions import DEFAULT_COMPOSITIONS
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


def test_repointed_analyses_name_a_real_composition() -> None:
    """The analyses evicted into compositions (discrepancy/coinvestment/subject_report/
    network_screen) must invoke a composition that actually exists — guards the playbook
    against drifting from DEFAULT_COMPOSITIONS after the surface was cut."""
    repointed = [c for c in REGISTRY if c.tool.startswith("run_composition(")]
    assert {c.id for c in repointed} == {
        "discrepancy", "coinvestment", "subject_report", "network_screen"}
    for c in repointed:
        m = re.search(r"run_composition\('([^']+)'", c.tool)
        assert m is not None, c.tool
        assert m.group(1) in DEFAULT_COMPOSITIONS, c.tool

"""PROVENANCE PIECE 3(b) UI: the browse object
inspector (osiris.js's own objectDetail/propRow) shows agreement/distinct_upstreams/
disputed beside each property's provenance line, and a "who else read this upstream"
click-through (console.js's bindUpstreamExpansions) that runs the upstream_readers
Function via the existing generic composition door, no bespoke route, static-source
guards matching this codebase's own convention for frontend logic (test_console_js_routes.py).
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).resolve().parent.parent / "src" / "ui" / "static"
_OSIRIS_JS = (_STATIC / "osiris.js").read_text()
_CONSOLE_JS = (_STATIC / "console.js").read_text()


def test_prop_row_calls_provenance_signals() -> None:
    body = _OSIRIS_JS.split("function propRow(p)", 1)[1].split("\n  function ", 1)[0]
    assert "provenanceSignals(p)" in body


def test_provenance_signals_covers_agreement_disputed_and_upstream_count() -> None:
    body = _OSIRIS_JS.split("function provenanceSignals(p)", 1)[1].split(
        "\n  function propRow", 1)[0]
    assert "contradicting" in body and "agreeing" in body
    assert "p.disputed" in body
    assert "p.distinct_upstreams" in body


def test_provenance_signals_upstream_link_carries_the_first_upstream_id() -> None:
    body = _OSIRIS_JS.split("function provenanceSignals(p)", 1)[1].split(
        "\n  function propRow", 1)[0]
    assert "p.upstream_ids[0]" in body
    assert "o-upstream-link" in body


def test_upstream_expansion_calls_the_generic_composition_door_not_a_bespoke_route() -> None:
    body = _CONSOLE_JS.split("function bindUpstreamExpansions(scope)", 1)[1].split(
        "\nfunction ", 1)[0]
    assert "fetch('/compositions/upstream-readers/run'" in body
    assert "subject: link.dataset.upstream" in body


def test_upstream_expansion_click_through_reuses_inspect_only(
) -> None:
    body = _CONSOLE_JS.split("function bindUpstreamExpansions(scope)", 1)[1].split(
        "\nfunction ", 1)[0]
    assert "inspectOnly(a.dataset.pick)" in body


def test_inspect_binds_the_upstream_expansions() -> None:
    body = _CONSOLE_JS.split("async function inspect(id)", 1)[1].split(
        "\nfunction ", 1)[0]
    assert "bindUpstreamExpansions(right)" in body

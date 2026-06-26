"""The sourced-dossier Markdown report — every claim carries its provenance."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.dissemination.dossier_report import build_dossier_report

NOW = datetime(2026, 6, 26, tzinfo=UTC)


async def test_report_renders_sections_with_provenance(actions: Actions) -> None:
    co = await actions.create_or_find_object("Organization", "cik:0001", "edgar")
    await actions.assert_property(co, "name", "Scam Co", "edgar", NOW, 0.85,
                                  evidence_class="authoritative_api")
    await actions.assert_property(co, "incorporation_state", "DE", "edgar", NOW, 0.85,
                                  evidence_class="authoritative_api")

    # a court case where it's a named party
    case = await actions.create_or_find_object("CourtCase", "courtlistener:1", "courtlistener")
    await actions.assert_property(case, "name", "SEC v. Scam Co", "courtlistener", NOW, 0.85,
                                  evidence_class="authoritative_api")
    await actions.assert_property(case, "court", "S.D.N.Y.", "courtlistener", NOW, 0.85)
    await actions.create_link(co, case, "litigation", "courtlistener", NOW, 0.6,
                              evidence_class="direct_observation")

    # a principal who is BOTH officer and director collapses to one line
    boss = await actions.create_or_find_object("Person", "sec-person:boss", "edgar")
    await actions.assert_property(boss, "name", "Jane Boss", "edgar", NOW, 0.85,
                                  evidence_class="authoritative_api")
    await actions.create_link(co, boss, "officer", "edgar", NOW, 0.85,
                              evidence_class="authoritative_api")
    await actions.create_link(co, boss, "director", "edgar", NOW, 0.85,
                              evidence_class="authoritative_api")

    md = await build_dossier_report(actions.pool, co)

    assert md.startswith("# Dossier: Scam Co")
    assert "## Identity" in md and "## Litigation" in md and "## Sources" in md
    # principals: one line per person, roles merged
    assert "## Principals" in md
    assert "**Jane Boss** — director, officer" in md
    # provenance is inline on every identity claim
    assert "incorporation_state" in md and "edgar · authoritative" in md
    # the litigation role is surfaced (named party vs mentioned)
    assert "SEC v. Scam Co" in md and "named party" in md
    # sources appendix lists the bases used
    assert "`courtlistener`" in md and "`edgar`" in md

"""The adversary's SCOPE (task #37) — "armed for one project" as pure matching semantics.

The licence (OSIRIS_SENSE_SESSIONS) says whether the adversary may read at all; the scope
(OSIRIS_SENSE_PROJECTS) says which projects the licence covers. Empty scope = everything —
the lever ships dark, arming it is the operator's hand.
"""
from __future__ import annotations

from src.ingest.scope import scope_match, sense_scopes


def test_sense_scopes_parses_comma_and_space_lists() -> None:
    assert sense_scopes("") == []
    assert sense_scopes("pokex, monsterhouse thoth") == ["pokex", "monsterhouse", "thoth"]
    assert sense_scopes("code/pokex") == ["code-pokex"]  # '/' normalizes to slug dashes
    assert sense_scopes(" -weird- ") == ["weird"]


def test_scope_match_is_suffix_over_project_slugs() -> None:
    assert scope_match("-home-x-code-pokex", ["pokex"])
    # the office slug: '/.osiris' doubles the dash — the suffix still lands
    assert scope_match("-home-asuramaya--osiris-seats-thoth", ["thoth"])
    assert scope_match("-home-x-code-pokex", ["code-pokex"])  # multi-segment entries work
    assert not scope_match("-home-x-code-pokexy", ["pokex"])  # dash boundary respected
    assert not scope_match("-home-x-code-mono", ["pokex", "thoth"])
    assert scope_match("anything-at-all", [])  # empty scope = everything (unarmed default)
    assert scope_match("pokex", ["pokex"])  # bare-equal edge

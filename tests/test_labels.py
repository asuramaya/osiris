"""src/ontology/labels.py — task #97 workstream 3 (ruling 52daab71). Pure functions,
no DB: resolve_label's three tiers (rule/chain/canonical) and disambiguate_labels'
collision handling."""
from __future__ import annotations

from src.ontology.labels import LABEL_CHAIN, disambiguate_labels, resolve_label


def test_label_chain_is_thoths_own_ordering() -> None:
    assert LABEL_CHAIN == ("name", "title", "summary", "statement", "surface", "handle")


def test_rule_tier_wins_when_declared_field_is_present() -> None:
    r = resolve_label("Tension", {"pole_a": "bounded recall", "pole_b": "complete memory"},
                      "tension:abc123")
    assert r.label == "bounded recall"
    assert r.subtitle == "complete memory"
    assert r.source == "rule"
    assert r.field == "pole_a"


def test_rule_tier_falls_through_to_chain_when_declared_field_is_null_on_this_row() -> None:
    # Agent's label_field is "handle" — an unclaimed agent has no handle yet, so this
    # row must fall to the chain's own "name" (the composite "claude in osiris" string).
    r = resolve_label("Agent", {"name": "claude in osiris"}, "agent:abc123")
    assert r.label == "claude in osiris"
    assert r.source == "chain"
    assert r.field == "name"


def test_rule_tier_falls_through_when_declared_field_is_blank_or_whitespace() -> None:
    r = resolve_label("Agent", {"handle": "   ", "name": "claude in osiris"}, "agent:abc123")
    assert r.label == "claude in osiris"
    assert r.source == "chain"


def test_chain_tier_picks_first_populated_property_in_order() -> None:
    # an undeclared type (no label_field/subtitle_field) walks the chain directly
    assert resolve_label("Unknown", {"title": "t", "summary": "s"}, "x:1").label == "t"
    assert resolve_label("Unknown", {"summary": "s", "statement": "st"}, "x:1").label == "s"
    assert resolve_label("Unknown", {"statement": "st", "surface": "sf"}, "x:1").label == "st"
    assert resolve_label("Unknown", {"surface": "sf", "handle": "h"}, "x:1").label == "sf"
    assert resolve_label("Unknown", {"handle": "h"}, "x:1").label == "h"


def test_practice_resolves_via_statement_not_canonical() -> None:
    # the exact reported bug: a Practice has none of name/title/summary — only
    # statement/failure_prevented/surface. Without "statement" in the chain this
    # falls straight to the raw hash.
    r = resolve_label("Practice", {"statement": "measure it yourself, don't trust an "
                                                "inherited number",
                                   "failure_prevented": "a wrong verb count shipped"},
                      "practice:b1eb7520e783")
    assert r.label == "measure it yourself, don't trust an inherited number"
    assert r.subtitle == "a wrong verb count shipped"
    assert r.source == "chain"
    assert r.field == "statement"


def test_blindspot_resolves_via_surface_not_canonical() -> None:
    r = resolve_label("BlindSpot", {"surface": "mcp-tool-list-refresh",
                                    "cannot_see": "the harness's own deferred-tool index "
                                                  "updates lazily"},
                      "blindspot:7d6fd3c05e03")
    assert r.label == "mcp-tool-list-refresh"
    assert r.subtitle == "the harness's own deferred-tool index updates lazily"
    assert r.source == "chain"
    assert r.field == "surface"


def test_canonical_tier_when_nothing_resolves() -> None:
    r = resolve_label("Unknown", {}, "x:deadbeef")
    assert r.label == "x:deadbeef"
    assert r.source == "canonical"
    assert r.field is None


def test_subtitle_is_none_when_not_declared_or_not_populated() -> None:
    assert resolve_label("Unknown", {"name": "n"}, "x:1").subtitle is None
    # BlindSpot declares subtitle_field=cannot_see; absent on this row -> None, not an error
    assert resolve_label("BlindSpot", {"surface": "s"}, "x:1").subtitle is None


def test_disambiguate_labels_passes_through_non_colliding() -> None:
    out = disambiguate_labels([("a", "alpha", "repo:alpha"), ("b", "beta", "repo:beta")])
    assert out == {"a": "alpha", "b": "beta"}


def test_disambiguate_labels_strips_common_prefix_on_collision() -> None:
    # the live bug, Thoth's own cited example: three project rows all truncating to
    # "/home/asuramaya/code/REPOS/c…" at a 28-char display width — the full labels
    # differ, but not until well past where a naive truncation already cut them off.
    items = [
        ("a", "/home/asuramaya/code/REPOS/coinbase-onchain", "repo:a"),
        ("b", "/home/asuramaya/code/REPOS/coinbase-agent", "repo:b"),
        ("c", "/home/asuramaya/code/REPOS/coinbase-web", "repo:c"),
    ]
    out = disambiguate_labels(items, width=28)
    assert out["a"] == "…onchain"
    assert out["b"] == "…agent"
    assert out["c"] == "…web"


def test_disambiguate_labels_falls_back_to_canonical_suffix_on_genuine_duplicate() -> None:
    # identical labels AND identical canonicals-minus-suffix would still tie after
    # stripping — never render two distinct objects as visually identical
    items = [("a", "duplicate", "obj:aaaaaaaa"), ("b", "duplicate", "obj:bbbbbbbb")]
    out = disambiguate_labels(items)
    assert out["a"] == "duplicate (aaaaaaaa)"
    assert out["b"] == "duplicate (bbbbbbbb)"


def test_disambiguate_labels_naive_truncation_would_have_collided() -> None:
    # proves the fix is load-bearing: a plain label[:width] truncation of these three
    # really does collapse to one indistinguishable string, which is the bug reported.
    labels = ["/home/asuramaya/code/REPOS/coinbase-onchain",
              "/home/asuramaya/code/REPOS/coinbase-agent",
              "/home/asuramaya/code/REPOS/coinbase-web"]
    naive = {lbl[:28] for lbl in labels}
    assert len(naive) == 1
    assert naive == {"/home/asuramaya/code/REPOS/c"}


def test_disambiguate_labels_truncates_long_solo_label_at_a_word_boundary() -> None:
    items = [("a", "MY APPROVAL ERROR, not the build's — a fleet-wide stat sitting above "
                   "a project-scoped roster", "practice:a")]
    out = disambiguate_labels(items, width=30)
    assert out["a"] == "MY APPROVAL ERROR, not the…"
    assert len(out["a"]) <= 31  # word-boundary cut stays close to the requested width


def test_disambiguate_labels_one_label_is_prefix_of_another() -> None:
    # narrow width forces the two to collide ("coinbase-v2"[:8] == "coinbase"[:8]) —
    # the shorter label IS the common prefix -> shown unchanged; the longer one shows
    # only what it adds beyond that shared prefix
    items = [("a", "coinbase", "repo:a"), ("b", "coinbase-v2", "repo:b")]
    out = disambiguate_labels(items, width=8)
    assert out["a"] == "coinbase"
    assert out["b"] == "…-v2"

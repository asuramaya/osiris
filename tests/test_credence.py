"""Credence — the upstream dual of the authority chain: a re-report can't out-credence its source.

The spawned_by tree is the independence oracle. These tests drive the pure resolver — the relay
clamp, the verification rebuttal, independent corroboration, the deepest-first no-leak invariant,
and the laundering flag — plus one end-to-end pass over real Postgres.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.orchestrator.credence import Claim, credence_props, resolve_credence
from src.parsers.base import EvidenceClass

NOW = datetime(2026, 7, 7, tzinfo=UTC)


def _c(oid: str, name: str, value: str, source: str, conf: float,
       when: datetime = NOW) -> Claim:
    return Claim(object_id=oid, name=name, value=value, source_id=source,
                 confidence=conf, observed_at=when)


def test_no_ancestry_reduces_to_grade_then_recency() -> None:
    # two UNRELATED agent sources (no spawned_by relation) → highest grade wins, nothing clamped:
    # with no ancestry, resolve_credence is exactly winning_props.
    claims = [
        _c("o1", "status", "x-hi", "agent:a", 0.9),
        _c("o1", "status", "x-lo", "agent:b", 0.6),
    ]
    (w,) = resolve_credence(claims, parent_of={}, looked={})
    assert w.value == "x-hi" and w.source_id == "agent:a"
    assert w.laundering == ()


def test_relay_ancestor_is_clamped_to_origin() -> None:
    # agent:b is a child of agent:a. a RELAYED at an inflated grade and never looked → a is capped
    # at b's origin grade, so the observed child wins over the louder ancestor.
    claims = [
        _c("o1", "status", "laundered", "agent:a", 0.9),   # ancestor relay, inflated grade
        _c("o1", "status", "observed", "agent:b", 0.6),    # origin: deepest, actually looked
    ]
    (w,) = resolve_credence(claims, parent_of={"agent:b": "agent:a"}, looked={})
    assert w.value == "observed" and w.source_id == "agent:b"
    assert w.confidence == 0.6
    assert "agent:a" in w.laundering


def test_verification_rebuttal_survives_the_clamp() -> None:
    # same shape, but the ancestor performed its OWN observation (backed_by_observation) → it
    # VERIFIED, and verification is corroboration, not relay: NOT clamped, it keeps its grade.
    claims = [
        _c("o1", "status", "verified", "agent:a", 0.9),
        _c("o1", "status", "observed", "agent:b", 0.6),
    ]
    (w,) = resolve_credence(
        claims, parent_of={"agent:b": "agent:a"}, looked={"agent:a": True})
    assert w.value == "verified" and w.source_id == "agent:a"
    assert w.confidence == 0.9
    assert w.laundering == ()


def test_independent_corroboration_is_not_clamped() -> None:
    # two SIBLINGS (both children of a common root, neither an ancestor of the other) → independent:
    # not a relay of one another, so no clamp and no laundering.
    claims = [
        _c("o1", "status", "x", "agent:a", 0.6, NOW),
        _c("o1", "status", "x", "agent:b", 0.6, NOW + timedelta(minutes=1)),
    ]
    parent_of = {"agent:a": "agent:root", "agent:b": "agent:root"}
    (w,) = resolve_credence(claims, parent_of, looked={})
    assert w.laundering == ()


def test_deepest_first_prevents_inflation_leak() -> None:
    # chain root A → B → C, C the deepest observer. B and A each relay at inflated grades and never
    # looked. Deepest-first, B is clamped to C's 0.6 BEFORE A reads it, so A also caps at 0.6 —
    # B's inflation can't leak upward. Were it to leak, A would win at 0.95; it must not.
    claims = [
        _c("o1", "s", "top", "agent:a", 0.95),
        _c("o1", "s", "mid", "agent:b", 0.9),
        _c("o1", "s", "observed", "agent:c", 0.6),
    ]
    parent_of = {"agent:c": "agent:b", "agent:b": "agent:a"}
    (w,) = resolve_credence(claims, parent_of, looked={})
    assert w.value == "observed" and w.source_id == "agent:c"
    assert w.confidence == 0.6
    assert set(w.laundering) == {"agent:a", "agent:b"}


def test_single_source_is_identity() -> None:
    claims = [_c("o1", "s", "v", "agent:a", 0.6)]
    (w,) = resolve_credence(claims, parent_of={"agent:a": "agent:x"}, looked={})
    assert w.value == "v" and w.laundering == ()


async def test_credence_props_clamps_a_relay_over_the_graph(actions: Actions) -> None:
    # end-to-end: agent A (parent) and B (child, spawned_by A) both assert a fact on a shared
    # object; A relays at SELF_DECLARED without looking → credence_props clamps it to the origin.
    o = await actions.create_or_find_object("SoftwareProject", "repo:cred-demo", "test")
    a = await actions.create_or_find_object("Agent", "agent:aaa", "fleet-observer")
    b = await actions.create_or_find_object("Agent", "agent:bbb", "fleet-observer")
    await actions.create_link(b, a, "spawned_by", "fleet-observer", NOW, 0.6,
                              evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)
    await actions.assert_property(a, "backed_by_observation", False, "fleet-observer", NOW,
                                  0.6, evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)
    await actions.assert_property(o, "status", "A-relayed", "agent:aaa", NOW, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    await actions.assert_property(o, "status", "B-observed", "agent:bbb", NOW, 0.6,
                                  evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)
    winners = {w.name: w for w in await credence_props(actions, [o])}
    assert winners["status"].value == "B-observed"      # the origin, not the louder relay
    assert "agent:aaa" in winners["status"].laundering  # the relay flagged (non-empty = clamp)

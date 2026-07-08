"""Credence — the upstream dual of the authority chain: a re-report can't out-credence its source.

The spawned_by tree is the independence oracle. These tests drive the pure resolver — the relay
clamp, the Tier-2 relay-vs-dispute split, the verification rebuttal, independent corroboration,
the deepest-first no-leak invariant, and the laundering flag — plus one end-to-end pass over real
Postgres.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from src.actions.core import Actions
from src.orchestrator.credence import (
    Claim,
    _same_claim,
    credence_props,
    resolve_credence,
)
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
    res = resolve_credence(claims, parent_of={}, looked={})
    (w,) = res.winners
    assert w.value == "x-hi" and w.source_id == "agent:a"
    assert w.laundering == ()
    assert res.disputes == []  # no ancestry → nothing to relay OR dispute


def test_relay_ancestor_is_clamped_to_origin() -> None:
    # agent:b is a child of agent:a. a RELAYED the SAME claim (reworded — case/punctuation) at an
    # inflated grade and never looked → a is capped at b's origin grade, so the observed child
    # wins over the louder ancestor. Tier-2 reads the paraphrase as a relay, not a dispute.
    claims = [
        _c("o1", "status", "Prod DB is DOWN.", "agent:a", 0.9),  # ancestor relay, inflated grade
        _c("o1", "status", "prod db is down", "agent:b", 0.6),   # origin: deepest, actually looked
    ]
    res = resolve_credence(claims, parent_of={"agent:b": "agent:a"}, looked={})
    (w,) = res.winners
    assert w.value == "prod db is down" and w.source_id == "agent:b"
    assert w.confidence == 0.6
    assert "agent:a" in w.laundering
    assert res.disputes == []  # a paraphrase relay is NOT a dispute


def test_verification_rebuttal_survives_the_clamp() -> None:
    # same shape, but the ancestor performed its OWN observation (backed_by_observation) → it
    # VERIFIED, and verification is corroboration, not relay: NOT clamped, it keeps its grade.
    # Even with a DIFFERENT value, a looker is neither clamped NOR disputed — rebuttal unchanged.
    claims = [
        _c("o1", "status", "verified", "agent:a", 0.9),
        _c("o1", "status", "observed", "agent:b", 0.6),
    ]
    res = resolve_credence(
        claims, parent_of={"agent:b": "agent:a"}, looked={"agent:a": True})
    (w,) = res.winners
    assert w.value == "verified" and w.source_id == "agent:a"
    assert w.confidence == 0.9
    assert w.laundering == ()
    assert res.disputes == []  # a verifier that looked is never promoted to a dispute


def test_independent_corroboration_is_not_clamped() -> None:
    # two SIBLINGS (both children of a common root, neither an ancestor of the other) → independent:
    # not a relay of one another, so no clamp and no laundering.
    claims = [
        _c("o1", "status", "x", "agent:a", 0.6, NOW),
        _c("o1", "status", "x", "agent:b", 0.6, NOW + timedelta(minutes=1)),
    ]
    parent_of = {"agent:a": "agent:root", "agent:b": "agent:root"}
    res = resolve_credence(claims, parent_of, looked={})
    (w,) = res.winners
    assert w.laundering == ()
    assert res.disputes == []  # neither is the other's ancestor → no dispute either


def test_deepest_first_prevents_inflation_leak() -> None:
    # chain root A → B → C, C the deepest observer. B and A each relay the SAME claim (reworded) at
    # inflated grades and never looked. Deepest-first, B is clamped to C's 0.6 BEFORE A reads it,
    # so A also caps at 0.6 — B's inflation can't leak upward. Were it to leak, A would win at 0.95.
    claims = [
        _c("o1", "s", "Service is degraded!", "agent:a", 0.95),
        _c("o1", "s", "service is degraded", "agent:b", 0.9),
        _c("o1", "s", "service is degraded", "agent:c", 0.6),
    ]
    parent_of = {"agent:c": "agent:b", "agent:b": "agent:a"}
    res = resolve_credence(claims, parent_of, looked={})
    (w,) = res.winners
    assert w.value == "service is degraded" and w.source_id == "agent:c"
    assert w.confidence == 0.6
    assert set(w.laundering) == {"agent:a", "agent:b"}
    assert res.disputes == []  # a reworded relay chain, not a disagreement


def test_single_source_is_identity() -> None:
    claims = [_c("o1", "s", "v", "agent:a", 0.6)]
    res = resolve_credence(claims, parent_of={"agent:a": "agent:x"}, looked={})
    (w,) = res.winners
    assert w.value == "v" and w.laundering == ()
    assert res.disputes == []


def test_paraphrase_relay_still_clamps_and_is_not_a_dispute() -> None:
    # the ancestor re-reports the SAME claim in different words (case, punctuation, an extra filler
    # word) → Tier-2 reads it as a relay: still clamped, still flagged, NOT surfaced as a dispute.
    claims = [
        _c("o1", "status", "The prod DB is DOWN.", "agent:a", 0.9),
        _c("o1", "status", "prod db is down", "agent:b", 0.6),
    ]
    res = resolve_credence(claims, parent_of={"agent:b": "agent:a"}, looked={})
    (w,) = res.winners
    assert w.confidence == 0.6 and w.source_id == "agent:b"  # clamped to the origin
    assert "agent:a" in w.laundering
    assert res.disputes == []


def test_genuine_disagreement_is_a_dispute_not_a_clamp() -> None:
    # agent:b (child) observed the build GREEN; agent:a (ancestor) asserts it RED. Materially
    # different claims → a is DISPUTING, not relaying. Tier-2: no clamp, no laundering flag (a
    # false accusation avoided), and the disagreement is surfaced as a dispute with both poles.
    claims = [
        _c("o1", "build", "the build is red", "agent:a", 0.9),
        _c("o1", "build", "the build is green", "agent:b", 0.6),
    ]
    res = resolve_credence(claims, parent_of={"agent:b": "agent:a"}, looked={})
    (w,) = res.winners
    assert w.laundering == ()                            # NOT accused of laundering
    assert w.confidence == 0.9 and w.source_id == "agent:a"  # not clamped — a keeps its grade
    (d,) = res.disputes
    assert d.object_id == "o1" and d.name == "build"
    assert {p[0] for p in d.positions} == {"agent:a", "agent:b"}       # both sources surfaced
    assert {p[1] for p in d.positions} == {"the build is red", "the build is green"}
    assert dict((p[0], p[2]) for p in d.positions) == {"agent:a": 0.9, "agent:b": 0.6}  # grades


def test_same_claim_normalizes_paraphrase_but_splits_real_disagreement() -> None:
    assert _same_claim("Prod is DOWN!", "prod is down")          # case + punctuation
    assert _same_claim("the prod db is down", "prod db is down")  # token containment
    assert _same_claim("deploy failed on the ci server",
                       "deploy failed on the ci runner")         # high overlap (Jaccard 5/7)
    assert not _same_claim("the build is red", "the build is green")  # one token differs → dispute
    assert not _same_claim("prod is up", "prod is down")             # antonym → dispute
    assert not _same_claim("", "anything")                           # empty is its own claim


async def test_credence_props_clamps_a_relay_over_the_graph(actions: Actions) -> None:
    # end-to-end: agent A (parent) and B (child, spawned_by A) assert the SAME claim (reworded) on
    # a shared object; A relays at SELF_DECLARED without looking → credence_props reads the
    # paraphrase as a relay and clamps it to the origin, flagging the launderer.
    o = await actions.create_or_find_object("SoftwareProject", "repo:cred-demo", "test")
    a = await actions.create_or_find_object("Agent", "agent:aaa", "fleet-observer")
    b = await actions.create_or_find_object("Agent", "agent:bbb", "fleet-observer")
    await actions.create_link(b, a, "spawned_by", "fleet-observer", NOW, 0.6,
                              evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)
    await actions.assert_property(a, "backed_by_observation", False, "fleet-observer", NOW,
                                  0.6, evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)
    await actions.assert_property(o, "status", "The pipeline is GREEN.", "agent:aaa", NOW, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    await actions.assert_property(o, "status", "pipeline is green", "agent:bbb", NOW, 0.6,
                                  evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)
    res = await credence_props(actions, [o])
    winners = {w.name: w for w in res.winners}
    assert winners["status"].source_id == "agent:bbb"   # the origin, not the louder relay
    # clamped to the origin grade (approx: confidence rides a PG `real` — float32 round-trip)
    assert winners["status"].confidence == pytest.approx(0.6, abs=1e-6)
    assert "agent:aaa" in winners["status"].laundering  # the relay flagged (non-empty = clamp)
    assert res.disputes == []                            # a paraphrase relay is not a dispute


async def test_credence_props_surfaces_a_genuine_dispute_over_the_graph(actions: Actions) -> None:
    # end-to-end dispute: A (parent, never looked) and B (child) assert MATERIALLY DIFFERENT values
    # on the same fact → credence_props surfaces a dispute (both poles) and clamps nothing.
    o = await actions.create_or_find_object("SoftwareProject", "repo:dispute-demo", "test")
    a = await actions.create_or_find_object("Agent", "agent:pa", "fleet-observer")
    b = await actions.create_or_find_object("Agent", "agent:pb", "fleet-observer")
    await actions.create_link(b, a, "spawned_by", "fleet-observer", NOW, 0.6,
                              evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)
    await actions.assert_property(a, "backed_by_observation", False, "fleet-observer", NOW,
                                  0.6, evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)
    await actions.assert_property(o, "verdict", "the migration is safe", "agent:pa", NOW, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    await actions.assert_property(o, "verdict", "the migration is unsafe", "agent:pb", NOW, 0.6,
                                  evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)
    res = await credence_props(actions, [o])
    winners = {w.name: w for w in res.winners}
    assert winners["verdict"].laundering == ()          # no false laundering accusation
    (d,) = res.disputes
    assert d.name == "verdict"
    assert {p[0] for p in d.positions} == {"agent:pa", "agent:pb"}
    assert {p[1] for p in d.positions} == {"the migration is safe", "the migration is unsafe"}

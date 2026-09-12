"""Miners as last resort (decision ac892cd9). Item 1: the Proposal object type + the
propose() write door, with the last-resort law wired against derive_or_abstain's own
abstention shape. Item 2: accept()/reject() and the read-only proposals_band(). Item
3: the daily budget per (miner, owner) pair, scaled by the trailing 30-day acceptance
rate, hard-stopped to zero on a 7-day window of rejections with no acceptances (with a
receipt Thread to the owner). Item 4: guarded_miner_tick, the failure-receipt-first
tick discipline (per-pair telemetry itself lives in test_digest.py, beside the rest of
fleet_digest's own streams). No miner wiring here — that's its own, later commit, per
Thoth's "one commit per item" instruction."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from src.actions.core import Actions
from src.config.settings import get_settings
from src.orchestrator import capture
from src.orchestrator.capture import _thread_canon
from src.orchestrator.proposals import (
    accept,
    guarded_miner_tick,
    proposals_band,
    propose,
    reject,
)


async def _mint_bare(actions: Actions, type_name: str) -> uuid.UUID:
    from src.ontology.catalog import ensure_type

    await ensure_type(actions, name=type_name, kind="object", actor="test")
    return await actions.create_or_find_object(
        type_name, f"{type_name.lower()}:{uuid.uuid4()}", "test")


_CANDIDATE = {"kind": "link", "from_id": "placeholder", "to_id": "placeholder",
              "link_type": "implements"}


async def test_propose_mints_when_a_live_abstention_exists(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [], "test")

    out = await propose(actions, from_id=orphan, link_type="implements",
                        candidate=_CANDIDATE, confidence=0.9, owner="operator",
                        miner="test-miner", actor="test-miner")
    assert "error" not in out
    assert out["owner"] == "operator"
    assert out["status"] == "proposed"
    # confidence CAPPED at the DERIVED tier (0.4) regardless of the 0.9 passed in — a
    # miner's own guess is never graded above what a mechanical sweep already earns.
    assert out["confidence"] == 0.4

    proposal_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", out["proposal"])
    assert proposal_id is not None
    evidence_pointer = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='evidence_pointer'", proposal_id)
    assert evidence_pointer == {"from_id": str(orphan), "link_type": "implements"}
    candidate = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='candidate'", proposal_id)
    assert candidate == _CANDIDATE
    expires_at = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='expires_at'", proposal_id)
    delta = datetime.fromisoformat(expires_at) - datetime.now(UTC)
    assert timedelta(days=13) < delta < timedelta(days=15)


async def test_propose_refuses_without_any_abstention_at_all(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    out = await propose(actions, from_id=orphan, link_type="implements",
                        candidate=_CANDIDATE, confidence=0.9, owner="operator",
                        miner="test-miner", actor="test-miner")
    assert "error" in out
    assert "last-resort" in out["error"]
    n = await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Proposal'")
    assert n == 0


async def test_propose_refuses_when_the_abstention_is_already_resolved(
    actions: Actions,
) -> None:
    """A successful mint SUPERSEDES the abstention with a resolved:true marker — Khnum
    (mail 8849) and Sekhmet (mail 8857) both independently: a resolved abstention is a
    settled question, never a genuine gap a miner should propose against."""
    orphan = await _mint_bare(actions, "GateWidget")
    target = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    # resolves it for real: exactly one candidate now, mints the link and supersedes
    # the prior abstention with resolved:true (capture.py's own retirement path).
    await capture.derive_or_abstain(actions, orphan, "implements", [target], "test")

    out = await propose(actions, from_id=orphan, link_type="implements",
                        candidate=_CANDIDATE, confidence=0.9, owner="operator",
                        miner="test-miner", actor="test-miner")
    assert "error" in out
    assert "last-resort" in out["error"]


async def test_propose_refuses_on_an_unresolvable_owner(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    out = await propose(actions, from_id=orphan, link_type="implements",
                        candidate=_CANDIDATE, confidence=0.9,
                        owner="nothing-names-this-seat", miner="test-miner",
                        actor="test-miner")
    assert "error" in out
    assert "owner" in out["error"]
    n = await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Proposal'")
    assert n == 0


async def test_propose_refuses_on_a_malformed_candidate_shape(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    out = await propose(actions, from_id=orphan, link_type="implements",
                        candidate={"kind": "nonsense"}, confidence=0.9,
                        owner="operator", miner="test-miner", actor="test-miner")
    assert "error" in out
    assert "candidate" in out["error"]


async def test_propose_resolves_a_real_seat_as_owner(actions: Actions) -> None:
    from src.orchestrator.seats import ensure_seat

    seat = await ensure_seat(actions, house="proptesthouse", handle="Proptestseat",
                             source="test")
    orphan = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    out = await propose(actions, from_id=orphan, link_type="implements",
                        candidate=_CANDIDATE, confidence=0.9, owner="Proptestseat",
                        miner="test-miner", actor="test-miner")
    assert "error" not in out
    assert out["owner"] == seat["seat_id"]


_LINK_CANDIDATE = {"kind": "link", "from_id": "placeholder", "to_id": "placeholder",
                   "link_type": "implements"}


async def test_accept_mints_the_link_candidate_verbatim(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    target = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    candidate = {"kind": "link", "from_id": str(orphan), "to_id": str(target),
                "link_type": "implements"}
    out = await propose(actions, from_id=orphan, link_type="implements",
                        candidate=candidate, confidence=0.9, owner="operator",
                        miner="test-miner", actor="test-miner")
    assert "error" not in out

    acc = await accept(actions, proposal=out["proposal"], actor="agent:accepter")
    assert "error" not in acc
    assert acc["status"] == "accepted"
    row = await actions.pool.fetchrow(
        "SELECT evidence_class, confidence, properties FROM links "
        "WHERE from_id=$1 AND to_id=$2 AND type='implements'", orphan, target)
    assert row is not None
    assert row["evidence_class"] == "self_declared"
    assert row["properties"]["accepted_from"] == out["proposal"]
    status = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id AND o.canonical=$1 "
        "WHERE a.name='status'", out["proposal"])
    assert status == "accepted"


async def test_accept_mints_the_object_candidate_verbatim(actions: Actions) -> None:
    from src.ontology.catalog import ensure_type

    await ensure_type(actions, name="GateWidget", kind="object", actor="test")
    orphan = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    new_canon = f"gatewidget:{uuid.uuid4()}"
    candidate = {"kind": "object", "type": "GateWidget", "canonical": new_canon,
                "properties": {"note": "minted from a proposal"}}
    out = await propose(actions, from_id=orphan, link_type="implements",
                        candidate=candidate, confidence=0.9, owner="operator",
                        miner="test-miner", actor="test-miner")
    assert "error" not in out

    acc = await accept(actions, proposal=out["proposal"], actor="agent:accepter")
    assert "error" not in acc
    new_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='GateWidget' AND canonical=$1", new_canon)
    assert new_id is not None
    note = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 AND a.name='note'",
        new_id)
    assert note == "minted from a proposal"
    accepted_from = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='accepted_from_proposal'", new_id)
    assert accepted_from == out["proposal"]


async def test_accept_refuses_a_non_proposed_proposal(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    out = await propose(actions, from_id=orphan, link_type="implements",
                        candidate=_LINK_CANDIDATE, confidence=0.9, owner="operator",
                        miner="test-miner", actor="test-miner")
    first = await reject(actions, proposal=out["proposal"], reason="test rejection",
                         actor="agent:rejecter")
    assert first["status"] == "rejected"

    second = await accept(actions, proposal=out["proposal"], actor="agent:accepter")
    assert "error" in second
    assert "rejected" in second["error"]


async def test_accept_refuses_an_expired_proposal(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    out = await propose(actions, from_id=orphan, link_type="implements",
                        candidate=_LINK_CANDIDATE, confidence=0.9, owner="operator",
                        miner="test-miner", actor="test-miner")
    proposal_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", out["proposal"])
    past = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    await actions.assert_property(proposal_id, "expires_at", past, "test-miner",
                                  datetime.now(UTC), 0.4, evidence_class="derived",
                                  actor="test-miner")

    acc = await accept(actions, proposal=out["proposal"], actor="agent:accepter")
    assert "error" in acc
    assert "expired" in acc["error"]


async def test_reject_retires_with_a_mandatory_reason(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    out = await propose(actions, from_id=orphan, link_type="implements",
                        candidate=_LINK_CANDIDATE, confidence=0.9, owner="operator",
                        miner="test-miner", actor="test-miner")
    rej = await reject(actions, proposal=out["proposal"], reason="wrong shortlist",
                       actor="agent:rejecter")
    assert rej["status"] == "rejected"
    proposal_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", out["proposal"])
    reason = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='reject_reason'", proposal_id)
    assert reason == "wrong shortlist"


async def test_reject_refuses_without_a_reason(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    out = await propose(actions, from_id=orphan, link_type="implements",
                        candidate=_LINK_CANDIDATE, confidence=0.9, owner="operator",
                        miner="test-miner", actor="test-miner")
    rej = await reject(actions, proposal=out["proposal"], reason="   ",
                       actor="agent:rejecter")
    assert "error" in rej


async def test_proposals_band_counts_and_groups_by_owner(actions: Actions) -> None:
    orphan1 = await _mint_bare(actions, "GateWidget")
    orphan2 = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan1, "implements", [], "test")
    await capture.derive_or_abstain(actions, orphan2, "implements", [], "test")
    out1 = await propose(actions, from_id=orphan1, link_type="implements",
                         candidate=_LINK_CANDIDATE, confidence=0.9, owner="operator",
                         miner="test-miner", actor="test-miner")
    # a DIFFERENT miner for the second one — the band groups by owner regardless of
    # miner, and this keeps the two calls out of the same (miner, owner) daily budget
    # (item 3), which this test predates and isn't about.
    out2 = await propose(actions, from_id=orphan2, link_type="implements",
                         candidate=_LINK_CANDIDATE, confidence=0.9, owner="operator",
                         miner="test-miner-2", actor="test-miner-2")

    band = await proposals_band(actions.pool)
    assert band["count"] >= 2
    assert "operator" in band["by_owner"]
    owner_proposals = {p["proposal"] for p in band["by_owner"]["operator"]}
    assert {out1["proposal"], out2["proposal"]}.issubset(owner_proposals) or len(
        band["by_owner"]["operator"]) == 3  # capped at 3 -- either fully present or capped


async def _seed_resolved(
    actions: Actions, miner: str, owner: str, status: str, observed_at: datetime,
) -> None:
    """A resolved Proposal's own history shape, minted directly (never through
    propose(), which would trip the very budget these fixtures set up to test) — only
    the three properties `_throttle_status`'s own queries read."""
    canonical = f"proposal:{uuid.uuid4()}"
    proposal_id = await actions.create_or_find_object("Proposal", canonical, miner)
    for name, value in (("miner", miner), ("owner", owner), ("status", status)):
        await actions.assert_property(proposal_id, name, value, miner, observed_at,
                                      0.4, evidence_class="derived", actor=miner)


async def test_propose_refuses_once_the_new_pair_starter_budget_is_spent_today(
    actions: Actions,
) -> None:
    orphan1 = await _mint_bare(actions, "GateWidget")
    orphan2 = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan1, "implements", [], "test")
    await capture.derive_or_abstain(actions, orphan2, "implements", [], "test")
    first = await propose(actions, from_id=orphan1, link_type="implements",
                          candidate=_LINK_CANDIDATE, confidence=0.9, owner="operator",
                          miner="fresh-miner", actor="fresh-miner")
    assert "error" not in first
    assert get_settings().osiris_miner_new_pair_starter_budget == 1  # this test's own assumption

    second = await propose(actions, from_id=orphan2, link_type="implements",
                           candidate=_LINK_CANDIDATE, confidence=0.9, owner="operator",
                           miner="fresh-miner", actor="fresh-miner")
    assert "error" in second
    assert "budget" in second["error"]


async def test_propose_scales_the_budget_up_with_a_good_trailing_acceptance_rate(
    actions: Actions,
) -> None:
    ten_days_ago = datetime.now(UTC) - timedelta(days=10)
    for _ in range(4):
        await _seed_resolved(actions, "good-miner", "operator", "accepted", ten_days_ago)
    await _seed_resolved(actions, "good-miner", "operator", "rejected", ten_days_ago)
    # rate = 4/5 = 0.8, budget = round(5 * 0.8) = 4 -- four proposals succeed today,
    # the fifth is refused.
    results = []
    for _ in range(5):
        orphan = await _mint_bare(actions, "GateWidget")
        await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
        results.append(await propose(
            actions, from_id=orphan, link_type="implements", candidate=_LINK_CANDIDATE,
            confidence=0.9, owner="operator", miner="good-miner", actor="good-miner"))
    assert sum(1 for r in results if "error" not in r) == 4
    assert "budget" in results[4]["error"]


async def test_propose_hard_stops_to_zero_on_a_7_day_rejection_only_window(
    actions: Actions,
) -> None:
    # an old good record that a naive 30-day rate alone would still trust...
    twenty_days_ago = datetime.now(UTC) - timedelta(days=20)
    for _ in range(4):
        await _seed_resolved(actions, "backsliding-miner", "operator", "accepted",
                             twenty_days_ago)
    # ...but the last 7 days are nothing but rejections -- the hard stop overrides.
    two_days_ago = datetime.now(UTC) - timedelta(days=2)
    for _ in range(3):
        await _seed_resolved(actions, "backsliding-miner", "operator", "rejected",
                             two_days_ago)

    orphan = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    out = await propose(actions, from_id=orphan, link_type="implements",
                        candidate=_LINK_CANDIDATE, confidence=0.9, owner="operator",
                        miner="backsliding-miner", actor="backsliding-miner")
    assert "error" in out
    assert "throttled" in out["error"]

    today = datetime.now(UTC).date().isoformat()
    summary = (f"Miner backsliding-miner throttled to zero proposals for operator: "
              f"3 rejection(s) in the trailing 7 days with zero acceptances "
              f"(as of {today})")
    thread_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='Thread' AND canonical=$1",
        _thread_canon(summary, None))
    assert thread_id is not None
    owner_value = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 AND a.name='owner'",
        thread_id)
    assert owner_value == "operator"

    # a second refused attempt the same day mints no second Thread -- open_thread's
    # own idempotency on the summary hash, which already embeds the day.
    orphan2 = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan2, "implements", [], "test")
    await propose(actions, from_id=orphan2, link_type="implements",
                  candidate=_LINK_CANDIDATE, confidence=0.9, owner="operator",
                  miner="backsliding-miner", actor="backsliding-miner")
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Thread' AND canonical=$1",
        _thread_canon(summary, None))
    assert n == 1


async def test_guarded_miner_tick_writes_a_receipt_before_the_exception_propagates(
    actions: Actions,
) -> None:
    async def _raising() -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await guarded_miner_tick(actions, "flaky-miner", _raising)

    thread_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='Thread' AND canonical=$1",
        _thread_canon("miner flaky-miner's tick raised RuntimeError", None))
    assert thread_id is not None
    severity = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='severity'", thread_id)
    assert severity == "alarm"


async def test_guarded_miner_tick_returns_the_result_when_nothing_raises(
    actions: Actions,
) -> None:
    out = await guarded_miner_tick(actions, "clean-miner", lambda: _return(42))
    assert out == 42


async def _return(value: int) -> int:
    return value


async def test_proposals_band_excludes_a_resolved_proposal(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    out = await propose(actions, from_id=orphan, link_type="implements",
                        candidate=_LINK_CANDIDATE, confidence=0.9, owner="operator",
                        miner="test-miner", actor="test-miner")
    await reject(actions, proposal=out["proposal"], reason="test", actor="agent:rejecter")

    band = await proposals_band(actions.pool)
    all_proposals = {p["proposal"] for rows in band["by_owner"].values() for p in rows}
    assert out["proposal"] not in all_proposals

"""Miners as last resort, item 1 (decision ac892cd9): the Proposal object type + the
propose() write door, with the last-resort law wired against derive_or_abstain's own
abstention shape. No accept/reject, no budget, no telemetry here — those are their own,
later commits, per Thoth's "one commit per item" instruction."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.orchestrator import capture
from src.orchestrator.proposals import propose


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

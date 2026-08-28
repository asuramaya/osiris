"""LANE 0 (thread aac54abb, operator ruling a0339e16): the single primitive every
orphan-healing lane calls. Binary, no dial — mints iff the caller's own lookup returned
exactly one candidate, else writes nothing and records why."""
from __future__ import annotations

import uuid

from src.actions.core import Actions
from src.ontology.catalog import ensure_type
from src.orchestrator import capture


async def _mint_bare(actions: Actions, type_name: str) -> uuid.UUID:
    await ensure_type(actions, name=type_name, kind="object", actor="test")
    return await actions.create_or_find_object(
        type_name, f"{type_name.lower()}:{uuid.uuid4()}", "test")


async def test_derive_or_abstain_mints_on_exactly_one_candidate(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    target = await _mint_bare(actions, "GateWidget")
    out = await capture.derive_or_abstain(
        actions, orphan, "implements", [target], "test")
    assert out == {"minted": True, "to": target, "abstained": False, "reason": None,
                   "candidate_count": 1}
    row = await actions.pool.fetchrow(
        "SELECT evidence_class, confidence, properties FROM links "
        "WHERE from_id=$1 AND to_id=$2 AND type='implements'", orphan, target)
    assert row["evidence_class"] == "direct_observation"
    assert round(row["confidence"], 2) == 0.6  # real column precision — DIRECT_OBSERVATION
    assert row["properties"]["origin"] == "derived"


async def test_derive_or_abstain_is_idempotent_on_a_repeat_call(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    target = await _mint_bare(actions, "GateWidget")
    first = await capture.derive_or_abstain(actions, orphan, "implements", [target], "test")
    second = await capture.derive_or_abstain(actions, orphan, "implements", [target], "test")
    assert first["minted"] is True
    assert second["minted"] is False  # already existed, not a re-mint
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='implements'",
        orphan, target)
    assert n == 1


async def test_derive_or_abstain_writes_nothing_on_zero_candidates(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    out = await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    assert out["minted"] is False
    assert out["abstained"] is True
    assert out["candidate_count"] == 0
    n = await actions.pool.fetchval("SELECT count(*) FROM links WHERE from_id=$1", orphan)
    assert n == 0
    reason = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_implements'", orphan)
    assert reason is not None and reason["candidate_count"] == 0


async def test_derive_or_abstain_writes_nothing_on_ambiguous_candidates(
    actions: Actions,
) -> None:
    """The exact mistake Thoth made twice before asking for this: a join that
    confirmed a story without checking whether it returned one row or many."""
    orphan = await _mint_bare(actions, "GateWidget")
    t1 = await _mint_bare(actions, "GateWidget")
    t2 = await _mint_bare(actions, "GateWidget")
    out = await capture.derive_or_abstain(actions, orphan, "implements", [t1, t2], "test")
    assert out["minted"] is False
    assert out["abstained"] is True
    assert out["candidate_count"] == 2
    assert set(out["candidates"]) == {t1, t2}
    n = await actions.pool.fetchval("SELECT count(*) FROM links WHERE from_id=$1", orphan)
    assert n == 0
    recorded = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_implements'", orphan)
    assert set(recorded["candidates"]) == {str(t1), str(t2)}


async def test_derive_or_abstain_uses_the_callers_own_reason_when_given(
    actions: Actions,
) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    t1 = await _mint_bare(actions, "GateWidget")
    t2 = await _mint_bare(actions, "GateWidget")
    out = await capture.derive_or_abstain(
        actions, orphan, "implements", [t1, t2], "test",
        why_if_ambiguous="session df4c827f holds 232 linked agents, not 1")
    assert out["reason"] == "session df4c827f holds 232 linked agents, not 1"


async def test_derive_or_abstain_namespaces_the_abstention_by_link_type(
    actions: Actions,
) -> None:
    """A second lane abstaining on the SAME object under a DIFFERENT link_type must
    never clobber the first lane's own record — assert_property's last-write-wins
    would otherwise silently lose it."""
    orphan = await _mint_bare(actions, "GateWidget")
    t1 = await _mint_bare(actions, "GateWidget")
    t2 = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [t1, t2], "test")
    await capture.derive_or_abstain(actions, orphan, "rediscovers", [], "test")
    implements_reason = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_implements'", orphan)
    rediscovers_reason = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_rediscovers'", orphan)
    assert implements_reason is not None and implements_reason["candidate_count"] == 2
    assert rediscovers_reason is not None and rediscovers_reason["candidate_count"] == 0


# --- abstained_derivations: the read surface (thread f1f14cb3 item 2, msg 5937) -----

async def test_abstained_derivations_resolves_from_id_and_every_candidate(
    actions: Actions,
) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    t1 = await _mint_bare(actions, "GateWidget")
    t2 = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [t1, t2], "test")
    out = await capture.abstained_derivations(actions.pool, "implements")
    assert out["count"] == 1
    row = out["sample"][0]
    assert row["from_id"] == str(orphan)[:8]
    assert row["from_type"] == "GateWidget"
    assert row["link_type"] == "implements"
    assert row["candidate_count"] == 2
    assert {c["id"] for c in row["candidates"]} == {str(t1)[:8], str(t2)[:8]}
    assert all(c["type"] == "GateWidget" for c in row["candidates"])


async def test_abstained_derivations_scopes_by_link_type_not_a_soup(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    await capture.derive_or_abstain(actions, orphan, "rediscovers", [], "test")
    implements_only = await capture.abstained_derivations(actions.pool, "implements")
    assert implements_only["count"] == 1
    assert implements_only["sample"][0]["link_type"] == "implements"

    pooled = await capture.abstained_derivations(actions.pool, None)
    assert pooled["count"] >= 2
    link_types = {r["link_type"] for r in pooled["sample"] if r["from_id"] == str(orphan)[:8]}
    assert link_types == {"implements", "rediscovers"}


async def test_abstained_derivations_count_is_never_capped_by_limit(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    out = await capture.abstained_derivations(actions.pool, "implements", limit=0)
    assert out["count"] == 1
    assert out["sample"] == []


# --- the retry door (thread e67fc338, Thoth's ruling): zero-candidate is retryable, ---
# --- ambiguous never is — structurally, not by a filter someone could widen -----------

async def test_retryable_abstentions_includes_zero_candidate_excludes_ambiguous(
    actions: Actions,
) -> None:
    zero = await _mint_bare(actions, "GateWidget")
    t1 = await _mint_bare(actions, "GateWidget")
    t2 = await _mint_bare(actions, "GateWidget")
    ambiguous = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, zero, "implements", [], "test")
    await capture.derive_or_abstain(actions, ambiguous, "implements", [t1, t2], "test")
    out = await capture.retryable_abstentions(actions.pool, "implements")
    ids = {r["from_id"] for r in out["sample"]}
    assert str(zero)[:8] in ids
    assert str(ambiguous)[:8] not in ids
    assert out["count"] == 1


async def test_retryable_abstentions_never_returns_an_ambiguous_row_even_unscoped(
    actions: Actions,
) -> None:
    """The structural guarantee: even pooling every link_type together (link_type=None),
    a 2+-candidate abstention can never appear — the SQL's own WHERE clause excludes it,
    not application code that a later edit could accidentally widen."""
    t1 = await _mint_bare(actions, "GateWidget")
    t2 = await _mint_bare(actions, "GateWidget")
    ambiguous = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, ambiguous, "confirms", [t1, t2], "test")
    out = await capture.retryable_abstentions(actions.pool, None)
    assert str(ambiguous)[:8] not in {r["from_id"] for r in out["sample"]}


async def test_derive_or_abstain_resolves_a_stale_abstention_on_a_successful_retry(
    actions: Actions,
) -> None:
    """A zero-candidate abstention that later resolves must stop appearing as still-
    abstained (or still-retryable) — otherwise the abstention record itself becomes the
    stale, never-revisited fact this whole lane exists to fix."""
    orphan = await _mint_bare(actions, "GateWidget")
    target = await _mint_bare(actions, "GateWidget")
    first = await capture.derive_or_abstain(actions, orphan, "implements", [], "test")
    assert first["abstained"] is True
    retryable = await capture.retryable_abstentions(actions.pool, "implements")
    assert str(orphan)[:8] in {r["from_id"] for r in retryable["sample"]}

    second = await capture.derive_or_abstain(
        actions, orphan, "implements", [target], "test", retried=True)
    assert second["minted"] is True

    still_abstained = await capture.abstained_derivations(actions.pool, "implements")
    assert str(orphan)[:8] not in {r["from_id"] for r in still_abstained["sample"]}
    now_retryable = await capture.retryable_abstentions(actions.pool, "implements")
    assert str(orphan)[:8] not in {r["from_id"] for r in now_retryable["sample"]}

    row = await actions.pool.fetchrow(
        "SELECT properties FROM links WHERE from_id=$1 AND to_id=$2 AND type='implements'",
        orphan, target)
    assert row["properties"]["retried"] is True

    resolved_value = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_implements'", orphan)
    assert resolved_value["resolved"] is True
    assert resolved_value["resolved_to"] == str(target)


async def test_derive_or_abstain_first_pass_success_writes_no_resolved_marker(
    actions: Actions,
) -> None:
    """The common case — a fresh cardinality-1 mint with no prior abstention on record —
    must not write a spurious 'resolved' property implying one existed."""
    orphan = await _mint_bare(actions, "GateWidget")
    target = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [target], "test")
    row = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_implements'", orphan)
    assert row is None

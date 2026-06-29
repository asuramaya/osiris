"""P2 of the composer — the opinionated read-models leave the engine as FUNCTIONS.

coinvest / subject_report / screen_network are not pure op-trees: their precision lives
in domain logic the closed ops can't express (merge-aware cluster resolution, a platform-
degree filter, multi-signal fuzzy matching). That is exactly Palantir's split — a small
closed op set PLUS a Function escape hatch. So the eviction keeps every drop of the
analytics: the logic is registered as a named Function, and a forkable composition merely
REFERENCES it ({"op":"function","name":...}). The proof each must pass: running the
composition is byte-equal to calling the bespoke read-model directly (opinion moved, did
not degrade).
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from src.actions.core import Actions
from src.ingest.opensanctions import ingest_ftm
from src.ontology.resolution import screen_network
from src.orchestrator.coinvest import coinvestment_ties
from src.orchestrator.compositions import (
    list_functions,
    run_composition,
    save_composition,
    seed_default_compositions,
)
from src.orchestrator.frontier import subject_report
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

NOW = datetime(2026, 6, 28, tzinfo=UTC)


# --- coinvest ---------------------------------------------------------------

async def _spv(actions: Actions, spv: str, operator: str, target: uuid.UUID) -> None:
    s = await actions.create_or_find_object("Organization", spv, "edgar")
    op = await actions.create_or_find_object("Person", operator, "edgar")
    await actions.assert_property(op, "name", operator.split(":")[-1], "edgar", NOW, 0.85)
    await actions.create_link(s, op, "officer", "edgar", NOW, 0.85)
    await actions.create_link(s, target, "raises_for", "edgar", NOW, 0.35)


async def test_coinvest_function_is_byte_equal(actions: Actions) -> None:
    neuralink = await actions.create_or_find_object("Organization", "company:n", "edgar")
    await actions.assert_property(neuralink, "name", "Neuralink", "edgar", NOW, 0.85)
    openai = await actions.create_or_find_object("Organization", "company:o", "edgar")
    await actions.assert_property(openai, "name", "OpenAI", "edgar", NOW, 0.85)
    await _spv(actions, "org:s1", "sec-person:Alpha Ventures", neuralink)
    await _spv(actions, "org:s2", "sec-person:Alpha Ventures", openai)
    await _spv(actions, "org:m1", "sec-person:Beta Capital", neuralink)
    await _spv(actions, "org:m2", "sec-person:Beta Capital", openai)

    await seed_default_compositions(actions.pool)
    direct = await coinvestment_ties(actions.pool, neuralink)
    via = await run_composition(actions.pool, "co-investment-ties", neuralink)
    assert via["kind"] == "data"
    assert via["items"] == direct           # the precise tie list, unchanged
    assert direct[0]["company"] == "OpenAI"  # and it's the REAL (non-degraded) analytic
    assert direct[0]["shared_operators"] == 2


# --- screen_network ---------------------------------------------------------

async def test_screen_function_is_byte_equal(actions: Actions) -> None:
    await ingest_ftm(actions, [{"id": "W1", "schema": "Person", "properties": {
        "name": ["Dmitry Dirtyhands"], "topics": ["sanction"]}}])
    company = await actions.create_or_find_object("Organization", "cik:42", "edgar")
    await actions.assert_property(company, "name", "Target Corp", "edgar", NOW, 0.85)
    spv = await actions.create_or_find_object("Organization", "cik:99", "edgar")
    await actions.assert_property(spv, "name", "Feeder SPV LP", "edgar", NOW, 0.85)
    await actions.create_link(spv, company, "raises_for", "edgar", NOW, 0.35)
    dirty = await actions.create_or_find_object("Person", "sec-person:dirty", "edgar")
    await actions.assert_property(dirty, "name", "Dmitry Dirtyhands", "edgar", NOW, 0.85)
    await actions.create_link(spv, dirty, "officer", "edgar", NOW, 0.85)

    await seed_default_compositions(actions.pool)
    direct = await screen_network(actions.pool, company)
    via = await run_composition(actions.pool, "screen-financing-network", company)
    assert via["items"] == direct
    assert direct[0]["member_name"] == "Dmitry Dirtyhands"  # the dirty operator survives


# --- subject_report (the subject is the CASE id here) -----------------------

async def test_subject_report_function_is_byte_equal(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    seed = await actions.create_or_find_object("Username", "asuramaya", "analyst:test", cid)
    acct = await actions.create_or_find_object("Account", "soundcloud:wrenaudio7", "helper", cid)
    await actions.pool.execute(
        "UPDATE case_objects SET hop_distance=1 WHERE case_id=$1 AND object_id=$2", cid, acct)
    await actions.create_link(seed, acct, "declares", "helper", NOW,
                              confidence_for(EvidenceClass.SELF_DECLARED), case_id=cid,
                              evidence_class=EvidenceClass.SELF_DECLARED.value)

    await seed_default_compositions(actions.pool)
    direct = await subject_report(actions.pool, cid)
    via = await run_composition(actions.pool, "who-is-this", cid)
    assert via["items"] == direct
    assert {f["canonical"] for f in direct["verified"]} >= {"asuramaya", "soundcloud:wrenaudio7"}


# --- the registry + guards --------------------------------------------------

async def test_function_registry_is_listable(actions: Actions) -> None:
    assert list_functions() == ["briefing", "coinvest", "screen_network", "subject_report"]


async def test_briefing_orients_without_a_subject(actions: Actions) -> None:
    """The human-side memory prosthesis: a subject-free Function that orients you on
    arrival — open threads (what's blocked) + recent work (what happened). A returning
    human and a fresh Claude are in the same zero-context state; this restores it."""
    cm = await actions.create_or_find_object("Commit", "commit:aa", "git")
    await actions.assert_property(cm, "summary", "ship the thing", "git", NOW, 0.85)
    await actions.assert_property(cm, "scope", "ui", "git", NOW, 0.85)
    await actions.assert_property(cm, "authored_date", "2026-06-29T00:00:00+00:00",
                                  "git", NOW, 0.85)
    th = await actions.create_or_find_object("Thread", "thread:1", "git-memory")
    await actions.assert_property(th, "summary", "THE WALL: needs portal access", "git-memory",
                                 NOW, 0.4)
    await actions.assert_property(th, "status", "open", "git-memory", NOW, 0.4)
    # a thread a later commit already closed — must self-heal OUT of the open list and INTO
    # the resolved section, carrying the provenance of why it was closed.
    done = await actions.create_or_find_object("Thread", "thread:2", "git-memory")
    await actions.assert_property(done, "summary", "NEXT: build the renderer", "git-memory",
                                  NOW, 0.4)
    await actions.assert_property(done, "status", "resolved", "git-memory", NOW, 0.4)
    await actions.assert_property(done, "resolved_in", "commit:zz", "git-memory", NOW, 0.4)
    await actions.assert_property(done, "resolved_because", "renderer, generic", "git-memory",
                                  NOW, 0.4)

    await save_composition(actions.pool, "briefing", {"op": "function", "name": "briefing"})
    # runs with NO subject (it briefs the project, not an entity)
    res = await run_composition(actions.pool, "briefing")
    assert res["kind"] == "data"
    threads = next(v for k, v in res["items"].items() if "Open threads" in k)
    recent = next(v for k, v in res["items"].items() if "Recent work" in k)
    healed = next(v for k, v in res["items"].items() if "Resolved" in k)
    assert any("THE WALL" in t["thread"] for t in threads)
    assert not any("renderer" in t["thread"] for t in threads)   # resolved → not still open
    assert any(r["change"] == "ship the thing" and r["scope"] == "ui" for r in recent)
    assert any(h["by"] == "commit:zz" and "renderer" in h["because"] for h in healed)


async def test_unknown_function_and_missing_subject_raise(actions: Actions) -> None:
    await save_composition(actions.pool, "bogus", {"op": "function", "name": "nope"})
    with pytest.raises(ValueError, match="unknown function"):
        await run_composition(actions.pool, "bogus", uuid.uuid4())
    await save_composition(actions.pool, "needs-subj", {"op": "function", "name": "coinvest"})
    with pytest.raises(ValueError, match="requires a subject"):
        await run_composition(actions.pool, "needs-subj", None)

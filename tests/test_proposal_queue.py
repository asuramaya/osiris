"""Judging the miner's proposals in one pass: the numbered list, the batch, the CLI, and the
desk route. Every decision goes through the real `proposal` accept/reject action (the same
audit trail), never a write path of its own."""
from __future__ import annotations

import io
import uuid
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime, timedelta
from typing import Any

from src.actions.core import Actions
from src.cli import cmd_proposals
from src.orchestrator import capture
from src.orchestrator import proposal_queue as pq
from src.orchestrator.proposals import propose


async def _mint(actions: Actions, type_name: str, canonical: str,
                name: str | None = None) -> uuid.UUID:
    from src.ontology.catalog import ensure_type

    await ensure_type(actions, name=type_name, kind="object", actor="test")
    oid = await actions.create_or_find_object(type_name, canonical, "test")
    if name:
        await actions.assert_property(oid, "name", name, "test", datetime.now(UTC), 0.9,
                                      evidence_class="self_declared")
    return oid


_MINERS = iter(range(10_000))


async def _propose(actions: Actions, title: str, project: str, *,
                   signal: str = "dominance") -> str:
    """One live proposal: an object, an abstention on its in_repo, a proposal to file it in
    `project`. Returns the proposal's ref."""
    thing = await _mint(actions, "Practice", f"practice:{uuid.uuid4()}", title)
    repo = await _mint(actions, "SoftwareProject", f"repo:{project}", project)
    await capture.derive_or_abstain(actions, thing, "in_repo", [], "test")
    out = await propose(
        actions, from_id=thing, link_type="in_repo",
        candidate={"kind": "link", "from_id": str(thing), "to_id": str(repo),
                   "link_type": "in_repo", "signal": signal},
        confidence=0.4, owner="operator", miner=f"abstention-{next(_MINERS)}",
        actor="miner:test")
    assert "error" not in out, out
    return str(out["proposal"])


async def _status(actions: Actions, ref: str) -> str:
    return str(await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='status'", ref))


async def _run(actions: Actions, *words: str, **kw: Any) -> tuple[int, str, str]:
    so, se = io.StringIO(), io.StringIO()
    with redirect_stdout(so), redirect_stderr(se):
        code = await cmd_proposals(list(words), pool=actions.pool, **kw)
    return code, so.getvalue(), se.getvalue()


async def _expire_in(actions: Actions, ref: str, days: float) -> None:
    """Move a proposal's expiry (the test needs an order and a lapse to look at)."""
    oid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", ref)
    await actions.pool.execute(
        "UPDATE assertions SET value = to_jsonb($2::text) WHERE object_id = $1 "
        "AND name = 'expires_at' AND is_current", oid,
        (datetime.now(UTC) + timedelta(days=days)).isoformat())


# --- the list -------------------------------------------------------------------------------

async def test_pending_is_ordered_soonest_to_lapse_first_under_stable_handles(
        actions: Actions) -> None:
    later = await _propose(actions, "Later one", "osiris")
    soon = await _propose(actions, "Soon one", "shellbiz")
    await _expire_in(actions, later, 12)
    await _expire_in(actions, soon, 2)
    items = await pq.pending(actions.pool)
    assert [(i["title"], i["target"]) for i in items] == [
        ("Soon one", "shellbiz"), ("Later one", "osiris")]
    assert items[0]["ref"] == soon and items[0]["days_left"] in (1, 2)
    assert items[0]["signal"] == "dominance" and "number" not in items[0]
    # a handle is the first hex digits of the proposal's own id, at least five
    assert soon.removeprefix("proposal:").replace("-", "").startswith(items[0]["handle"])
    assert len(items[0]["handle"]) >= 5


async def test_expired_and_judged_proposals_are_not_listed(actions: Actions) -> None:
    gone = await _propose(actions, "Past its date", "osiris")
    judged = await _propose(actions, "Already judged", "osiris")
    kept = await _propose(actions, "Still live", "osiris")
    await _expire_in(actions, gone, -1)
    (res,) = await pq.judge(actions.pool, "test:operator", "reject", [judged], reason="no")
    assert res["ok"]
    assert [i["ref"] for i in await pq.pending(actions.pool)] == [kept]


async def test_the_list_is_one_short_line_each(actions: Actions) -> None:
    await _propose(actions, "A practice about migrations " + "x" * 80, "osiris")
    await _propose(actions, "Second", "shellbiz")
    code, out, err = await _run(actions)
    assert code == 0 and err == ""
    lines = out.splitlines()
    assert lines[0].startswith("osiris proposals: 2 waiting; the first lapses in")
    assert len(lines) == 4  # header, two proposals, one line saying what to type
    items = await pq.pending(actions.pool)
    assert lines[1].startswith(f" {items[0]['handle']}  ") and "  ->  " in lines[1]
    assert "…" in lines[1] and max(len(ln) for ln in lines) <= 120  # titles are cut
    assert lines[-1] == (f"Judge them: osiris proposals accept {items[0]['handle']}  |  "
                         f"reject {items[1]['handle']}  |  accept all")
    assert "proposal:" not in out and "0.4" not in out  # no refs or grades by default


async def test_verbose_adds_the_detail(actions: Actions) -> None:
    ref = await _propose(actions, "Detail", "osiris")
    _c, out, _e = await _run(actions, verbose=True)
    assert ref in out and "Practice in_repo" in out and "abstention-" in out


async def test_an_empty_list_says_so(actions: Actions) -> None:
    code, out, _e = await _run(actions)
    assert code == 0 and out == "osiris proposals: nothing waiting.\n"


def test_handles_lengthen_only_when_two_would_collide() -> None:
    a = "proposal:8b75b4e2-b98e-42b5-9f7d-e8b2bd569638"
    b = "proposal:8b75b9ff-0000-4000-8000-000000000000"
    c = "proposal:1234abcd-0000-4000-8000-000000000000"
    assert pq._handles([a, b, c]) == ["8b75b4", "8b75b9", "1234a"]


# --- the renumbering accident ---------------------------------------------------------------

async def test_judging_one_never_moves_the_handle_of_another(actions: Actions) -> None:
    """The live accident: `accept 1`, then `reject 6` as a second command; the accept had
    renumbered the list, so 6 named a different proposal. A handle is the proposal's own id,
    so after any judgement every other proposal answers to the same handle as before."""
    refs = [await _propose(actions, f"P{i}", "osiris") for i in range(6)]
    for i, r in enumerate(refs):
        await _expire_in(actions, r, i + 2)
    before = {i["ref"]: i["handle"] for i in await pq.pending(actions.pool)}
    first, sixth = before[refs[0]], before[refs[5]]
    code, _o, _e = await _run(actions, "accept", first)
    assert code == 0 and await _status(actions, refs[0]) == "accepted"
    after = {i["ref"]: i["handle"] for i in await pq.pending(actions.pool)}
    assert refs[0] not in after
    assert {r: h for r, h in before.items() if r in after} == after  # nobody renamed
    # the second command, typed from the first listing, lands on the proposal it names
    code, out, _e = await _run(actions, "reject", sixth)
    assert code == 0 and "P5" in out
    assert [await _status(actions, r) for r in refs] == [
        "accepted", "proposed", "proposed", "proposed", "proposed", "rejected"]


async def test_a_bare_number_is_refused_and_nothing_is_acted_on(actions: Actions) -> None:
    ref = await _propose(actions, "Only", "osiris")
    for word in ("1", "6", "banana"):
        code, out, err = await _run(actions, "accept", word)
        assert code == 1 and out == ""
        assert "is not a proposal id" in err
    assert await _status(actions, ref) == "proposed"


async def test_an_unknown_or_ambiguous_handle_acts_on_nothing(actions: Actions) -> None:
    from src.ontology.catalog import ensure_type

    keep = await _propose(actions, "Keep", "osiris")
    await ensure_type(actions, name="Proposal", kind="object", actor="test")
    for tail in ("aaaa1111-0000-4000-8000-000000000001", "aaaa2222-0000-4000-8000-000000000002"):
        await actions.create_or_find_object("Proposal", f"proposal:{tail}", "test")
    code, out, err = await _run(actions, "accept", "ffff0")
    assert code == 1 and out == "" and "no proposal starts with ffff0" in err
    code, out, err = await _run(actions, "accept", "aaaa")
    assert code == 1 and out == "" and "matches 2 proposals" in err and "type more" in err
    assert await _status(actions, keep) == "proposed"


async def test_a_refusal_on_one_does_not_stop_the_rest(actions: Actions) -> None:
    good = await _propose(actions, "Fine", "osiris")
    results = await pq.judge(actions.pool, "test:operator", "accept",
                             ["proposal:" + str(uuid.uuid4()), good])
    assert [r["ok"] for r in results] == [False, True]
    assert "no such Proposal" in results[0]["error"]
    assert await _status(actions, good) == "accepted"
    again = await pq.judge(actions.pool, "test:operator", "accept", [good])
    assert again[0]["ok"] is False  # already judged: the real verb refuses


# --- the batch ------------------------------------------------------------------------------

async def test_accept_goes_through_the_real_verb_and_mints_the_link(actions: Actions) -> None:
    a = await _propose(actions, "First", "osiris")
    b = await _propose(actions, "Second", "shellbiz")
    await _expire_in(actions, a, 3)
    await _expire_in(actions, b, 9)
    handle = (await pq.pending(actions.pool))[0]["handle"]
    code, out, err = await _run(actions, "accept", handle)
    assert code == 0 and err == ""
    assert out.splitlines() == [f" accepted {handle[:5]}: First  ->  osiris",
                                "osiris proposals: 1 accepted."]
    assert await _status(actions, a) == "accepted" and await _status(actions, b) == "proposed"
    link = await actions.pool.fetchrow(
        "SELECT l.properties, l.evidence_class FROM links l WHERE l.type='in_repo' "
        "AND l.properties->>'accepted_from' = $1", a)
    assert link is not None and link["evidence_class"] == "self_declared"
    resolver = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='resolved_by'", a)
    assert resolver == "console"


async def test_reject_records_the_reason_the_miner_reads_back(actions: Actions) -> None:
    ref = await _propose(actions, "Wrong place", "osiris")
    handle = (await pq.pending(actions.pool))[0]["handle"]
    code, _out, _e = await _run(actions, "reject", handle)
    assert code == 0 and await _status(actions, ref) == "rejected"
    reason = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='reject_reason'", ref)
    assert reason == "not a match (judged in one pass)"
    ref2 = await _propose(actions, "Another", "osiris")
    h2 = (await pq.pending(actions.pool))[0]["handle"]
    await _run(actions, "reject", h2, reason="wrong project")
    reason2 = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='reject_reason'", ref2)
    assert reason2 == "wrong project"


async def test_accept_all_and_a_full_ref(actions: Actions) -> None:
    refs = [await _propose(actions, f"P{i}", "osiris") for i in range(3)]
    code, _o, _e = await _run(actions, "accept", refs[0])  # a full ref works too
    assert code == 0 and await _status(actions, refs[0]) == "accepted"
    code, out, _e = await _run(actions, "accept", "all")
    assert code == 0 and "2 accepted" in out
    assert {await _status(actions, r) for r in refs} == {"accepted"}


async def test_the_list_json_for_scripts(actions: Actions) -> None:
    import json
    await _propose(actions, "J", "osiris")
    code, out, _e = await _run(actions, as_json=True)
    assert code == 0
    (item,) = json.loads(out)
    assert item["title"] == "J" and item["ref"].startswith("proposal:") and item["handle"]


# --- reopen ---------------------------------------------------------------------------------

async def _reason(actions: Actions, ref: str, name: str) -> str | None:
    return await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name=$2", ref, name)


async def test_reopen_undoes_a_mistaken_rejection_and_keeps_the_history(
        actions: Actions) -> None:
    ref = await _propose(actions, "Rejected by mistake", "shellbiz")
    handle = (await pq.pending(actions.pool))[0]["handle"]
    await _run(actions, "reject", handle)
    assert await _status(actions, ref) == "rejected" and await pq.pending(actions.pool) == []
    code, out, err = await _run(actions, "reopen", handle, reason="rejected by mistake")
    assert code == 0 and err == ""
    assert out.splitlines() == [f" reopened {handle[:5]}: Rejected by mistake  ->  shellbiz",
                                "osiris proposals: 1 reopened."]
    assert await _status(actions, ref) == "proposed"
    assert [i["ref"] for i in await pq.pending(actions.pool)] == [ref]
    # the rejection's reason is marked withdrawn, not deleted; who and why are recorded
    assert (await _reason(actions, ref, "reject_reason")).startswith("withdrawn: ")
    assert await _reason(actions, ref, "reopened_by") == "console"
    assert await _reason(actions, ref, "reopen_reason") == "rejected by mistake"
    # the history is intact: the rejection and the reversal are both rows
    states = await actions.pool.fetch(
        "SELECT a.value #>> '{}' AS v FROM assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='status' ORDER BY a.id", ref)
    assert [r["v"] for r in states] == ["proposed", "rejected", "proposed"]


async def test_a_reopened_proposal_no_longer_counts_against_the_miner(
        actions: Actions) -> None:
    from src.orchestrator.proposals import _throttle_status

    ref = await _propose(actions, "Throttle", "osiris")
    miner = await _reason(actions, ref, "miner")
    assert miner
    handle = (await pq.pending(actions.pool))[0]["handle"]
    await _run(actions, "reject", handle)
    assert (await _throttle_status(actions.pool, miner, "operator"))["rejected_7d"] == 1
    await _run(actions, "reopen", handle, reason="mistake")
    assert (await _throttle_status(actions.pool, miner, "operator"))["rejected_7d"] == 0


async def test_reopen_refusals(actions: Actions) -> None:
    from src.orchestrator.proposals import reopen

    acc = await _propose(actions, "Accepted", "osiris")
    live = await _propose(actions, "Still proposed", "osiris")
    gone = await _propose(actions, "Will lapse", "osiris")
    await pq.judge(actions.pool, "test:operator", "accept", [acc])
    await pq.judge(actions.pool, "test:operator", "reject", [gone], reason="no")
    await _expire_in(actions, gone, -1)
    for ref, text in ((acc, "minted a link"), (live, "nothing to reopen"),
                      (gone, "expired")):
        (res,) = [await pq.reopen_one(actions.pool, "console", ref, "x")]
        assert res["ok"] is False and text in res["error"], (ref, res)
    code, out, err = await _run(actions, "reopen", "abcde")
    assert code == 1 and "reopen needs --reason" in err and out == ""
    # not the operator's hands, and no ruling named: refused; a real ruling authorizes it
    miss = await _propose(actions, "Needs authority", "osiris")
    await pq.judge(actions.pool, "test:operator", "reject", [miss], reason="no")
    res2 = await reopen(Actions(actions.pool), proposal=miss, reason="x", actor="agent:x")
    assert "only the operator" in res2["error"]
    from src.ontology.catalog import ensure_type
    await ensure_type(actions, name="Decision", kind="object", actor="test")
    await actions.create_or_find_object("Decision", "decision:abc123", "test")
    res3 = await reopen(Actions(actions.pool), proposal=miss, reason="x", actor="agent:x",
                        ruling="decision:abc123")
    assert res3["reopened"] is True and await _status(actions, miss) == "proposed"


def test_the_pure_helpers() -> None:
    assert pq._hex_of("proposal:8B75b4e2-b98e") == "8b75b4e2b98e"


def test_the_flags_and_the_command() -> None:
    from src import cli
    parser = cli._build_parser()
    a = parser.parse_args(["proposals"])
    assert a.words == [] and a.verbose is False and a.as_json is False
    a = parser.parse_args(["proposals", "reject", "8b75b", "--reason", "nope"])
    assert a.words == ["reject", "8b75b"] and a.reason == "nope" and a.ruling is None
    a = parser.parse_args(["proposals", "reopen", "8b75b", "--reason", "r", "--ruling", "d1"])
    assert a.words == ["reopen", "8b75b"] and a.ruling == "d1"

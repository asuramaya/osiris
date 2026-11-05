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

async def test_pending_is_numbered_soonest_to_lapse_first(actions: Actions) -> None:
    later = await _propose(actions, "Later one", "osiris")
    soon = await _propose(actions, "Soon one", "shellbiz")
    await _expire_in(actions, later, 12)
    await _expire_in(actions, soon, 2)
    items = await pq.pending(actions.pool)
    assert [(i["number"], i["title"], i["target"]) for i in items] == [
        (1, "Soon one", "shellbiz"), (2, "Later one", "osiris")]
    assert items[0]["ref"] == soon and items[0]["days_left"] in (1, 2)
    assert items[0]["signal"] == "dominance"


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
    assert lines[1].startswith(" 1  ") and "  ->  " in lines[1] and "(dominance)" in lines[1]
    assert "…" in lines[1] and max(len(ln) for ln in lines) <= 120  # titles are cut
    assert lines[-1] == "Judge them: osiris proposals accept 1 3  |  reject 2  |  accept all"
    assert "proposal:" not in out and "0.4" not in out  # no refs or grades by default


async def test_verbose_adds_the_detail(actions: Actions) -> None:
    ref = await _propose(actions, "Detail", "osiris")
    _c, out, _e = await _run(actions, verbose=True)
    assert ref in out and "Practice in_repo" in out and "abstention-" in out


async def test_an_empty_list_says_so(actions: Actions) -> None:
    code, out, _e = await _run(actions)
    assert code == 0 and out == "osiris proposals: nothing waiting.\n"


# --- the batch ------------------------------------------------------------------------------

async def test_accept_goes_through_the_real_verb_and_mints_the_link(actions: Actions) -> None:
    a = await _propose(actions, "First", "osiris")
    b = await _propose(actions, "Second", "shellbiz")
    await _expire_in(actions, a, 3)
    await _expire_in(actions, b, 9)
    code, out, err = await _run(actions, "accept", "1")
    assert code == 0 and err == ""
    assert out.splitlines() == [" accepted 1: First  ->  osiris",
                                "osiris proposals: 1 accepted."]
    assert await _status(actions, a) == "accepted" and await _status(actions, b) == "proposed"
    # the real accept ran: the link exists, graded self_declared, citing the proposal, and the
    # proposal records who resolved it
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
    code, _out, _e = await _run(actions, "reject", "1")
    assert code == 0
    assert await _status(actions, ref) == "rejected"
    reason = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='reject_reason'", ref)
    assert reason == "not a match (judged in one pass)"
    ref2 = await _propose(actions, "Another", "osiris")
    await _run(actions, "reject", "1", reason="wrong project")
    reason2 = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='reject_reason'", ref2)
    assert reason2 == "wrong project"


async def test_accept_all_and_mixed_numbers_and_ranges(actions: Actions) -> None:
    refs = [await _propose(actions, f"P{i}", "osiris") for i in range(5)]
    for i, r in enumerate(refs):
        await _expire_in(actions, r, i + 2)
    code, out, _e = await _run(actions, "accept", "1,3", "5")
    assert code == 0 and out.splitlines()[-1] == "osiris proposals: 3 accepted."
    assert [await _status(actions, r) for r in refs] == [
        "accepted", "proposed", "accepted", "proposed", "accepted"]
    code, out, _e = await _run(actions, "accept", "all")
    assert code == 0 and "2 accepted" in out
    assert {await _status(actions, r) for r in refs} == {"accepted"}


async def test_a_bad_number_acts_on_nothing(actions: Actions) -> None:
    ref = await _propose(actions, "Only", "osiris")
    code, out, err = await _run(actions, "accept", "1", "7")
    assert code == 1 and out == ""
    assert err == "osiris proposals: there is no number 7: the list runs 1 to 1.\n"
    assert await _status(actions, ref) == "proposed"
    code, _o, err = await _run(actions, "accept", "banana")
    assert code == 1 and "is not a number" in err
    code, _o, err = await _run(actions, "accept")
    assert code == 1 and "accept which?" in err
    code, _o, err = await _run(actions, "1")
    assert code == 1 and "say accept or reject" in err
    assert await _status(actions, ref) == "proposed"


async def test_a_refusal_on_one_does_not_stop_the_rest(actions: Actions) -> None:
    good = await _propose(actions, "Fine", "osiris")
    results = await pq.judge(actions.pool, "test:operator", "accept",
                             ["proposal:" + str(uuid.uuid4()), good])
    assert [r["ok"] for r in results] == [False, True]
    assert "no such Proposal" in results[0]["error"]
    assert await _status(actions, good) == "accepted"
    again = await pq.judge(actions.pool, "test:operator", "accept", [good])
    assert again[0]["ok"] is False  # already judged: the real verb refuses


async def test_the_list_json_for_scripts(actions: Actions) -> None:
    import json
    await _propose(actions, "J", "osiris")
    code, out, _e = await _run(actions, as_json=True)
    assert code == 0
    (item,) = json.loads(out)
    assert item["number"] == 1 and item["title"] == "J" and item["ref"].startswith("proposal:")


def test_parse_numbers() -> None:
    assert pq.parse_numbers(["1", "3", "4"], 5) == [1, 3, 4]
    assert pq.parse_numbers(["2-4", "4,1"], 5) == [2, 3, 4, 1]
    assert pq.parse_numbers(["all"], 3) == [1, 2, 3]
    assert pq.parse_numbers(["0"], 3) == "there is no number 0: the list runs 1 to 3."
    assert pq.parse_numbers(["1"], 0) == "nothing is waiting."


# --- the desk -------------------------------------------------------------------------------

async def test_the_desk_routes_list_and_judge_by_ref(actions: Actions) -> None:
    from httpx import ASGITransport, AsyncClient
    from src.api.app import create_app, get_pool

    app = create_app()
    app.dependency_overrides[get_pool] = lambda: actions.pool
    a = await _propose(actions, "Desk one", "osiris")
    b = await _propose(actions, "Desk two", "shellbiz")
    await _expire_in(actions, a, 3)
    await _expire_in(actions, b, 8)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        listing = (await c.get("/proposals")).json()
        assert listing["count"] == 2 and listing["items"][0]["ref"] == a
        bad = (await c.post("/proposals/judge", json={"decision": "burn", "refs": [a]})).json()
        assert bad == {"error": "decision must be accept or reject"}
        res = (await c.post("/proposals/judge",
                            json={"decision": "reject", "refs": [b], "reason": "no"})).json()
        assert res["done"] == 1 and res["by"] == "operator"
        res = (await c.post("/proposals/judge", json={"decision": "accept", "refs": [a]})).json()
        assert res["done"] == 1
    assert await _status(actions, a) == "accepted" and await _status(actions, b) == "rejected"
    resolver = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='resolved_by'", a)
    assert resolver == "analyst:operator"


# --- the console ----------------------------------------------------------------------------

def test_the_console_draws_the_list_with_buttons_that_act_by_ref() -> None:
    from pathlib import Path
    js = (Path(__file__).resolve().parent.parent / "src/ui/static/console.js").read_text()
    assert "function renderProposalsHtml(data)" in js
    assert "Proposals to judge (" in js and "Accept all" in js
    assert "fetch('/proposals')" in js and "fetch('/proposals/judge'" in js
    # buttons carry the proposal's own ref, not its position in the list
    assert "data-ref=" in js and "getAttribute('data-ref')" in js
    desk = js.split("async function renderSettingsSectionDesk()", 1)[1][:1200]
    assert "renderProposalsHtml(proposals) + renderDeskHtml(desk)" in desk


def test_the_flags_and_the_command() -> None:
    from src import cli
    parser = cli._build_parser()
    a = parser.parse_args(["proposals"])
    assert a.words == [] and a.verbose is False and a.as_json is False
    a = parser.parse_args(["proposals", "reject", "2", "--reason", "nope"])
    assert a.words == ["reject", "2"] and a.reason == "nope"

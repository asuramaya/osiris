"""The desk side of proposal judging: the routes behind the buttons and the console code that
draws them. The list, the handles and the verbs are in test_proposal_queue.py."""
from __future__ import annotations

from src.actions.core import Actions

from tests.test_proposal_queue import _expire_in, _propose, _status

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
        undo = (await c.post("/proposals/reopen", json={"ref": b})).json()
        assert undo == {"ok": True, "ref": b, "by": "operator"}
        nope = (await c.post("/proposals/reopen", json={"ref": a})).json()
        assert nope["ok"] is False and "minted a link" in nope["error"]
    assert await _status(actions, a) == "accepted" and await _status(actions, b) == "proposed"
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
    # a rejection can be undone from the page that made it
    assert "function undoProposal(btn)" in js and "fetch('/proposals/reopen'" in js
    assert "'Undo</button>'" in js or "Undo</button>" in js
    desk = js.split("async function renderSettingsSectionDesk()", 1)[1][:1200]
    assert "renderProposalsHtml(proposals) + renderDeskHtml(desk)" in desk

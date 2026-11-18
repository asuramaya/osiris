"""The console's /time-machine/* routes: loopback only, sentence errors, one write."""
from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.orchestrator import time_machine as tm
from src.orchestrator import time_machine_console as tmc

from tests.test_time_machine_restore import _put, moment  # noqa: F401  (a fixture)

HELPERS = Path(__file__).resolve().parent.parent / "helpers"


def _client(actions: Actions, host: str = "127.0.0.1") -> httpx.AsyncClient:
    from src.orchestrator.manifests import load_manifests

    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(HELPERS)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=(host, 4321)), base_url="http://test")


@pytest_asyncio.fixture
async def client(actions: Actions, tmp_path: Path,
                 monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[httpx.AsyncClient]:
    monkeypatch.setenv("OSIRIS_TIME_MACHINE_STATE", str(tmp_path / "rec.json"))
    monkeypatch.setenv("OSIRIS_RESTORE_POINTS_FILE", str(tmp_path / "no-restore-points.json"))
    monkeypatch.setattr(tm, "VAULT_DIR", tmp_path / "vault")
    async with _client(actions) as c:
        yield c
    await tmc._drop_pool()


def _vault(tmp_path: Path, size: int = 1000) -> Path:
    v = tmp_path / "vault"
    v.mkdir(exist_ok=True)
    (v / "osiris-20261115-201856.dump").write_bytes(b"x" * size)
    return v


ALL_ROUTES = [
    ("get", "/time-machine/moments"), ("get", "/time-machine/status"),
    ("post", "/time-machine/start"), ("post", "/time-machine/extend"),
    ("post", "/time-machine/teardown"), ("get", "/time-machine/browse/types"),
    ("get", "/time-machine/browse/objects"), ("get", "/time-machine/browse/object/abcdef"),
    ("get", "/time-machine/browse/sessions"),
    ("get", "/time-machine/browse/session/x/lines"),
    ("post", "/time-machine/plan"), ("post", "/time-machine/apply"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("method,path", ALL_ROUTES)
async def test_every_route_answers_only_on_this_machine(
        actions: Actions, method: str, path: str) -> None:
    async with _client(actions, host="10.1.2.3") as c:
        resp = await getattr(c, method)(path)
    assert resp.status_code == 403
    assert "only answers on the machine" in resp.json()["error"]
    assert resp.headers["cache-control"] == "no-store"


@pytest.mark.asyncio
async def test_moments_and_an_empty_status(client: httpx.AsyncClient, tmp_path: Path) -> None:
    _vault(tmp_path)
    got = (await client.get("/time-machine/moments")).json()
    assert [m["id"] for m in got["moments"]] == ["osiris-20261115-201856"]
    assert (await client.get("/time-machine/status")).json() == {"throwaway": None}


@pytest.mark.asyncio
async def test_start_checks_then_hands_off_to_a_detached_run(
        client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _vault(tmp_path)
    spawned: list[str] = []
    monkeypatch.setattr(tm, "spawn_run", lambda mid, **_k: spawned.append(mid) or 77)
    monkeypatch.setattr(tm, "docker_root", lambda *_a: "/")
    nothing = await client.post("/time-machine/start", json={})
    assert nothing.status_code == 400 and "which moment" in nothing.json()["error"]
    unknown = await client.post("/time-machine/start", json={"moment": "../../etc/passwd"})
    assert unknown.status_code == 400 and "no such moment" in unknown.json()["error"]
    ok = await client.post("/time-machine/start", json={"moment": "osiris-20261115-201856"})
    assert ok.status_code == 200 and ok.json()["started"] and ok.json()["pid"] == 77
    assert spawned == ["osiris-20261115-201856"]
    garbage = await client.post("/time-machine/start", content=b"not json")
    assert garbage.status_code == 400


@pytest.mark.asyncio
async def test_start_refuses_when_one_is_open_or_the_disk_is_short(
        client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _vault(tmp_path, size=10**6)
    monkeypatch.setattr(tm, "spawn_run", lambda *_a, **_k: 1)
    monkeypatch.setattr(tm, "docker_root", lambda *_a: "/")
    monkeypatch.setattr(tm, "free_bytes", lambda _p: 100)
    short = await client.post("/time-machine/start", json={"moment": "osiris-20261115-201856"})
    assert short.status_code == 400 and "not enough disk" in short.json()["error"].lower()
    monkeypatch.setattr(tm, "free_bytes", lambda _p: 10**12)
    m = tm.list_moments()[0]
    tm.record_failure(m, "x")
    again = await client.post("/time-machine/start", json={"moment": m.id})
    assert again.status_code == 200                       # a failed one does not block
    rec = tm.read_record()
    assert rec is not None
    rec.state = "ready"
    tm._write_record(rec)
    busy = await client.post("/time-machine/start", json={"moment": m.id})
    assert busy.status_code == 400 and "already" in busy.json()["error"]


@pytest.mark.asyncio
async def test_extend_and_teardown(client: httpx.AsyncClient, tmp_path: Path,
                                   monkeypatch: pytest.MonkeyPatch) -> None:
    assert (await client.post("/time-machine/extend")).status_code == 400
    _vault(tmp_path)
    m = tm.list_moments()[0]
    rec = tm.record_failure(m, "x")
    rec.state = "ready"
    tm._write_record(rec)
    ok = (await client.post("/time-machine/extend")).json()
    assert ok["throwaway"]["extensions"] == 1 and "reader_password" not in str(ok)
    twice = await client.post("/time-machine/extend")
    assert twice.status_code == 400 and "already extended" in twice.json()["error"]
    monkeypatch.setattr(tm, "teardown", lambda **_k: ["container osiris-tm-x"])
    assert (await client.post("/time-machine/teardown")).json() == {
        "removed": ["container osiris-tm-x"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("method,path", [
    ("get", "/time-machine/browse/types"), ("get", "/time-machine/browse/objects"),
    ("get", "/time-machine/browse/object/abcdef"), ("get", "/time-machine/browse/sessions"),
    ("get", "/time-machine/browse/session/x/lines"), ("post", "/time-machine/plan"),
    ("post", "/time-machine/apply")])
async def test_browsing_with_nothing_open_is_a_sentence(
        client: httpx.AsyncClient, method: str, path: str) -> None:
    resp = await getattr(client, method)(path)
    assert resp.status_code == 400 and "No past moment is open" in resp.json()["error"]


@pytest.mark.asyncio
async def test_browse_plan_and_apply_end_to_end(
        moment: Actions, client: httpx.AsyncClient, actions: Actions,  # noqa: F811
        monkeypatch: pytest.MonkeyPatch) -> None:
    # `moment` first: its template database must be idle when it is cloned
    async def pool() -> tuple[Any, None]:
        return moment.pool, None

    monkeypatch.setattr(tmc, "_browse_pool", pool)
    await _put(moment, "Thread", "tm-route-t", {"summary": "an old thread"})
    types = (await client.get("/time-machine/browse/types")).json()["types"]
    assert any(t["type"] == "Thread" for t in types)
    found = (await client.get("/time-machine/browse/objects",
                              params={"q": "old thread", "type": "Thread"})).json()
    assert [r["canonical"] for r in found["rows"]] == ["tm-route-t"]
    one = (await client.get("/time-machine/browse/object/tm-route-t")).json()
    assert one["object"]["canonical"] == "tm-route-t"
    miss = await client.get("/time-machine/browse/object/tm-nothing")
    assert miss.status_code == 400 and "Nothing in that moment" in miss.json()["error"]

    plan = (await client.post("/time-machine/plan", json={"ref": "tm-route-t"})).json()
    assert plan["counts"]["copy"] == 1
    no_digest = await client.post("/time-machine/apply", json={"ref": "tm-route-t"})
    assert no_digest.status_code == 400 and "digest" in no_digest.json()["error"]
    bad = await client.post("/time-machine/apply", json={
        "ref": "tm-route-t", "digest": plan["digest"], "resolutions": ["nope"]})
    assert bad.status_code == 400 and "map of conflict" in bad.json()["error"]
    stale = await client.post("/time-machine/apply", json={"ref": "tm-route-t", "digest": "0"})
    assert stale.status_code == 400 and "changed since" in stale.json()["error"]
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE canonical='tm-route-t'") == 0     # nothing yet

    done = (await client.post("/time-machine/apply", json={
        "ref": "tm-route-t", "digest": plan["digest"], "note": "clicked"})).json()
    assert done["changed"] and done["created"] and done["assertions_copied"] == 1
    ev = await actions.pool.fetchrow(
        "SELECT actor FROM object_events WHERE event_type='restore' "
        "AND object_id=$1::uuid", done["object_id"])
    assert ev["actor"] == "analyst:operator"

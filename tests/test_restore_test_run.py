"""The console's "Run the restore test now": LAN proof, the shared lock, one line per drill step,
the receipt saved (or not, when the NAS could not be reached), and the soul-key door's lock."""
from __future__ import annotations

import contextlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
from src.actions.core import Actions
from src.api.app import create_app
from src.orchestrator import restore_test_run
from src.orchestrator.at_home_nas import stream_nas

TARGET = {"name": "nas", "kind": "restic", "enabled": True,
          "path_or_url": "sftp:truenas:/mnt/Bunker/Vault/osiris-restic/repo"}
HOME = {"present": True, "reason": None, "dev": "wlp0s20f3"}


class Fake:
    def __init__(self, *, presence: dict[str, Any] | None = None, locked: bool = True,
                 failure: str | None = None) -> None:
        self.presence, self.locked, self.failure = presence or HOME, locked, failure
        self.recorded: list[dict[str, Any]] = []
        self.drilled: list[str] = []

    def deps(self) -> restore_test_run.Deps:
        @contextlib.contextmanager
        def lock() -> Any:
            yield self.locked

        def drill(url: str, on_step: Callable[[str, float | None], None]) -> str | None:
            self.drilled.append(url)
            for name, secs in (("repository check", 199.4), ("sample restore", 12.0)):
                on_step(name, None)
                on_step(name, secs)
            return self.failure

        return restore_test_run.Deps(
            presence=lambda t: self.presence, drill=drill, hold_lock=lock,
            record=lambda url, **kw: self.recorded.append({"url": url, **kw}))


async def _lines(fake: Fake) -> list[dict[str, Any]]:
    async def runner(say: Any) -> int:
        return await restore_test_run.run(TARGET, fake.deps(), say=say)

    return [json.loads(x) async for x in stream_nas(runner)]


async def test_a_pass_streams_each_drill_step_and_saves_the_receipt() -> None:
    fake = Fake()

    lines = await _lines(fake)

    texts = [x["text"] for x in lines]
    assert [x.get("n") for x in lines if x.get("n")] == [1, 2, 3, 4]
    assert "repository check: started" in texts and "repository check: done in 199 s" in texts
    assert "sample restore: done in 12 s" in texts
    assert lines[-1]["done"] is True and lines[-1]["ok"] is True
    assert fake.recorded == [{"url": TARGET["path_or_url"], "ok": True, "error": None,
                              "note": None}]


async def test_it_runs_even_after_a_recent_failure_and_a_failure_is_saved_and_named() -> None:
    fake = Fake(failure="the sample restore did not match")

    lines = await _lines(fake)

    assert fake.drilled == [TARGET["path_or_url"]]  # no six hour wait
    assert lines[-1]["ok"] is False
    assert any(x.get("status") == "failed" and "did not match" in x["text"] for x in lines)
    assert fake.recorded[0]["ok"] is False and "did not match" in fake.recorded[0]["error"]


async def test_not_at_home_stops_before_anything_runs() -> None:
    fake = Fake(presence={"present": False, "reason": "reachable only over tailscale0"})

    lines = await _lines(fake)

    assert fake.drilled == [] and fake.recorded == []
    assert lines[-1]["ok"] is False and "tailscale0" in json.dumps(lines)
    noproof = await _lines(Fake(presence={"present": True, "dev": None}))
    assert noproof[-1]["ok"] is False and "not going without that proof" in json.dumps(noproof)


async def test_a_busy_lock_stops_without_running() -> None:
    fake = Fake(locked=False)

    lines = await _lines(fake)

    assert fake.drilled == [] and lines[-1]["ok"] is False
    assert "running right now" in json.dumps(lines)


async def test_a_nas_that_could_not_be_reached_saves_nothing() -> None:
    fake = Fake(failure="ssh: connect to host 192.168.4.198 port 22: No route to host")

    lines = await _lines(fake)

    assert fake.recorded == [] and lines[-1]["ok"] is False
    assert "nothing was saved" in json.dumps(lines)


async def test_a_drill_that_raises_is_a_sentence_and_a_saved_failure() -> None:
    fake = Fake()

    def boom(url: str, on_step: Any) -> str | None:
        raise RuntimeError("scratch full")

    deps = fake.deps()
    deps = restore_test_run.Deps(presence=deps.presence, drill=boom, hold_lock=deps.hold_lock,
                                 record=deps.record)

    async def runner(say: Any) -> int:
        return await restore_test_run.run(TARGET, deps, say=say)

    lines = [json.loads(x) async for x in stream_nas(runner)]

    assert lines[-1]["ok"] is False and fake.recorded[0]["ok"] is False


# ---- the route and the soul-key door -----------------------------------------------------------


def _client(actions: Actions, host: str = "127.0.0.1") -> httpx.AsyncClient:
    from src.orchestrator.manifests import load_manifests

    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(Path(__file__).resolve().parent.parent / "helpers")
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=(host, 4321)), base_url="http://test")


async def test_the_route_is_loopback_only_and_streams_ndjson_without_caching(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_command(*, pool: Any, say: Any, deps: Any = None) -> int:
        say("[1/4] home network: ok.")
        return 0

    monkeypatch.setattr(restore_test_run, "command", fake_command)
    async with _client(actions, host="192.168.1.50") as far:
        assert (await far.post("/at-home/restore-test/run")).status_code == 403
    async with _client(actions) as here:
        resp = await here.post("/at-home/restore-test/run")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/x-ndjson")
    assert resp.headers["cache-control"] == "no-store"
    rows = [json.loads(x) for x in resp.text.splitlines()]
    assert rows[0]["n"] == 1 and rows[-1] == {"done": True, "ok": True, "text": "home network: ok."}


async def test_the_soul_key_door_takes_the_same_lock_and_refuses_while_it_is_held(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    from src.orchestrator import offload_runner, soul_key

    monkeypatch.setenv("OSIRIS_OFFLOAD_RECEIPTS_FILE", str(tmp_path / "r.json"))
    monkeypatch.setenv("OSIRIS_RESTORE_DRILL_RECEIPTS_FILE", str(tmp_path / "d.json"))
    with offload_runner.hold_tick_lock() as mine:
        assert mine
        out = await soul_key.soul_key_restore_drill(actions.pool, repo_url="sftp:x:/r")
    assert "running right now" in out["error"]

"""THE SETTINGS PANE's own new REST routes (Thoth mail 13350) — API tests for the four
genuinely thin doors added because no read door existed before them: GET /restic-key/
status, GET /deploy-status, GET /operator/desk, POST /operator/desk/reply. Every other
section of the pane reads through an EXISTING door (see the frontend's own
src/ui/static/console.js header comment for the full inventory).

A new file, not an edit to any existing test file with its own established convention."""
from __future__ import annotations

import subprocess
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app

HELPERS = Path(__file__).parent.parent / "helpers"


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    from src.orchestrator.manifests import load_manifests

    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(HELPERS)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# --- GET /restic-key/status -------------------------------------------------------------

async def test_restic_key_status_route_absent(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD_FILE", str(tmp_path / "no-such-file"))
    r = await client.get("/restic-key/status")
    assert r.status_code == 200
    body = r.json()
    assert body["present"] is False
    assert "password" not in body  # never the secret bytes


async def test_restic_key_status_route_present(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pw_file = tmp_path / "restic.password"
    pw_file.write_bytes(b"a-real-password")
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD_FILE", str(pw_file))
    r = await client.get("/restic-key/status")
    body = r.json()
    assert body["present"] is True
    assert body["backend"] == "file"
    assert "a-real-password" not in r.text


# --- GET /deploy-status -----------------------------------------------------------------

def _init_git_repo(path: Path) -> str:
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q"], cwd=path, check=True, timeout=30)
    subprocess.run(["git", "config", "user.email", "t@t"], cwd=path, check=True, timeout=30)
    subprocess.run(["git", "config", "user.name", "t"], cwd=path, check=True, timeout=30)
    (path / "f.txt").write_text("x")
    subprocess.run(["git", "add", "."], cwd=path, check=True, timeout=30)
    subprocess.run(["git", "commit", "-q", "-m", "init"], cwd=path, check=True, timeout=30)
    sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=path, check=True, capture_output=True, text=True,
        timeout=30,
    ).stdout.strip()
    return sha


async def test_deploy_status_route_running_sha_is_this_real_checkout(
    client: httpx.AsyncClient,
) -> None:
    # no snapshot env override here -- exercises the REAL repo root this test itself
    # runs from, proving running_sha resolves to a real git sha, never a guess.
    r = await client.get("/deploy-status")
    body = r.json()
    assert body["running_sha"] is not None
    assert len(body["running_sha"]) == 40


async def test_deploy_status_route_degrades_when_no_snapshot_ever_pinned(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_DEPLOY_SNAPSHOT_DIR", str(tmp_path / "never-pinned"))
    r = await client.get("/deploy-status")
    body = r.json()
    assert body["deploy_snapshot_sha"] is None
    assert body["in_sync"] is False


async def test_deploy_status_route_reports_in_sync_when_shas_match(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator import deploy_status as mod

    snapshot_dir = tmp_path / "snapshot"
    sha = _init_git_repo(snapshot_dir)
    monkeypatch.setenv("OSIRIS_DEPLOY_SNAPSHOT_DIR", str(snapshot_dir))
    monkeypatch.setattr(mod, "_running_repo_root", lambda: snapshot_dir)
    r = await client.get("/deploy-status")
    body = r.json()
    assert body["running_sha"] == sha
    assert body["deploy_snapshot_sha"] == sha
    assert body["in_sync"] is True


# --- GET /operator/desk -------------------------------------------------------------------

async def test_operator_desk_json_route_returns_the_same_shape_read_desk_does(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    from src.orchestrator.mailbox import send_message

    await send_message(
        actions.pool, from_agent="agent:test", from_project="osiris",
        to_project="operator", body="needs a decision", desk_kind="decision", grade="ask")
    r = await client.get("/operator/desk")
    assert r.status_code == 200
    body = r.json()
    assert "needs_decision" in body
    assert "needs_hands" in body
    assert "fyi" in body
    assert len(body["needs_decision"]) == 1
    assert body["needs_decision"][0]["body"] == "needs a decision"


# --- POST /operator/desk/reply -------------------------------------------------------------

async def test_operator_desk_reply_route_sends_as_the_operator(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    from src.orchestrator.mailbox import send_message

    sent = await send_message(
        actions.pool, from_agent="agent:test", from_project="osiris",
        to_project="operator", body="needs a decision", desk_kind="decision", grade="ask")
    r = await client.post("/operator/desk/reply", json={
        "id": sent["id"], "body": "yes, go ahead"})
    assert r.status_code == 200
    body = r.json()
    assert "error" not in body
    row = await actions.pool.fetchrow(
        "SELECT from_agent, body FROM fleet_messages WHERE id=$1", body["id"])
    assert row["from_agent"] == "operator"
    assert row["body"] == "yes, go ahead"


async def test_operator_desk_reply_route_reports_an_unroutable_reply_as_an_error(
    client: httpx.AsyncClient,
) -> None:
    r = await client.post("/operator/desk/reply", json={
        "id": 2**31 - 1, "body": "whatever"})
    body = r.json()
    assert "error" in body

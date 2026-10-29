"""The status bar's per-session render cache: a refresh within the TTL is a file read with no
server call, a sent message or changed inputs make it stale, and only a live render is kept."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import pytest
import scripts.osiris_hook as osiris_hook
from src.actions.core import Actions
from src.orchestrator import mail_bump
from src.orchestrator.mailbox import send_message

_HOOK = Path(__file__).resolve().parent.parent / "scripts" / "osiris_hook.py"


class _Heartbeat(BaseHTTPRequestHandler):
    hits = 0

    def do_POST(self) -> None:  # noqa: N802
        type(self).hits += 1
        body = json.dumps({"result": {"mail": 3, "dm": 0, "needs": 3,
                                      "resolved_project": "cachetest"}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: Any) -> None:
        pass


@pytest.fixture
def heartbeat() -> Iterator[type[_Heartbeat]]:
    _Heartbeat.hits = 0
    server = HTTPServer(("127.0.0.1", 0), _Heartbeat)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    _Heartbeat.url = f"http://127.0.0.1:{server.server_port}/heartbeat"  # type: ignore[attr-defined]
    yield _Heartbeat
    server.shutdown()
    server.server_close()


def _hook_input(session: str = "sess-cache-0001", ctx: float = 40.0, cwd: str = "/tmp") -> str:
    return json.dumps({"cwd": cwd, "session_id": session, "model": {"id": "claude-sonnet-5-5"},
                       "context_window": {"used_percentage": ctx, "context_window_size": 200000},
                       "workspace": {"current_dir": cwd}})


def _run(tmp_path: Path, stdin: str, heartbeat_url: str | None) -> subprocess.CompletedProcess[str]:
    env = {**os.environ,
           "OSIRIS_STATUSLINE_RENDER_DIR": str(tmp_path / "render"),
           "OSIRIS_MAIL_BUMP_FILE": str(tmp_path / "mail-bump"),
           "HOME": str(tmp_path / "home")}
    if heartbeat_url:
        env["OSIRIS_HEARTBEAT_URL"] = heartbeat_url
    return subprocess.run([sys.executable, str(_HOOK), "statusline"], input=stdin,
                          capture_output=True, text=True, check=False, env=env, timeout=30)


def test_a_second_refresh_within_the_ttl_is_served_from_the_cache_with_no_server_call(
    tmp_path: Path, heartbeat: type[_Heartbeat],
) -> None:
    url = heartbeat.url  # type: ignore[attr-defined]
    first = _run(tmp_path, _hook_input(), url)
    second = _run(tmp_path, _hook_input(), url)

    assert first.returncode == 0 and "✉" in first.stdout
    assert second.stdout == first.stdout
    assert heartbeat.hits == 1  # the second call never reached the server


def test_a_message_sent_since_the_render_makes_it_stale(
    tmp_path: Path, heartbeat: type[_Heartbeat],
) -> None:
    url = heartbeat.url  # type: ignore[attr-defined]
    _run(tmp_path, _hook_input(), url)
    time.sleep(0.05)
    (tmp_path / "mail-bump").touch()  # what send_message does
    _run(tmp_path, _hook_input(), url)

    assert heartbeat.hits == 2


def test_changed_inputs_or_another_session_are_not_served_from_someone_elses_cache(
    tmp_path: Path, heartbeat: type[_Heartbeat],
) -> None:
    url = heartbeat.url  # type: ignore[attr-defined]
    _run(tmp_path, _hook_input(ctx=40.0), url)
    _run(tmp_path, _hook_input(ctx=41.0), url)                    # context moved
    _run(tmp_path, _hook_input(session="sess-cache-0002"), url)   # another session
    assert heartbeat.hits == 3


def test_an_expired_entry_is_rendered_again(
    tmp_path: Path, heartbeat: type[_Heartbeat],
) -> None:
    url = heartbeat.url  # type: ignore[attr-defined]
    _run(tmp_path, _hook_input(), url)
    entry = tmp_path / "render" / "sess-cache-0001.json"
    raw = json.loads(entry.read_text())
    raw["ts"] = time.time() - osiris_hook._RENDER_TTL - 5
    entry.write_text(json.dumps(raw))

    _run(tmp_path, _hook_input(), url)

    assert heartbeat.hits == 2


def test_no_session_and_no_answer_are_never_cached(tmp_path: Path) -> None:
    dead = "http://127.0.0.1:9/heartbeat"  # nothing listens on the discard port
    out = _run(tmp_path, _hook_input(), dead)
    assert out.returncode == 0
    assert not (tmp_path / "render").exists() or not list((tmp_path / "render").glob("*.json"))


def test_a_corrupt_cache_file_falls_back_to_a_normal_render(
    tmp_path: Path, heartbeat: type[_Heartbeat],
) -> None:
    (tmp_path / "render").mkdir()
    (tmp_path / "render" / "sess-cache-0001.json").write_text("{not json")
    out = _run(tmp_path, _hook_input(), heartbeat.url)  # type: ignore[attr-defined]
    assert "✉" in out.stdout and heartbeat.hits == 1


def test_the_hook_and_the_mailbox_agree_on_the_default_bump_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OSIRIS_MAIL_BUMP_FILE", raising=False)
    assert str(mail_bump.bump_path()) == osiris_hook._mail_bump_path()


async def test_sending_a_message_touches_the_bump_file(actions: Actions) -> None:
    path = mail_bump.bump_path()
    assert not path.exists()
    await send_message(actions.pool, from_agent="agent:bumper", from_project="osiris",
                       to_project="operator", body="bump test", grade="fyi")
    assert path.exists()


def test_the_installed_statusline_refreshes_every_thirty_seconds() -> None:
    from src.orchestrator.onboard import merge_settings

    doc, _ = merge_settings({}, Path("/home/x/code/osiris"))
    assert doc["statusLine"]["refreshInterval"] == 30

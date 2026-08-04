from __future__ import annotations

import http.server
import json
import threading
import uuid
from collections.abc import Iterator

import pytest
from src.connectors.github import fetch_github_user
from src.parsers.base import InputObject
from src.parsers.github import parse_github_user


def _inp(handle: str) -> InputObject:
    return InputObject(id=str(uuid.uuid4()), type="Username", canonical=handle)


def test_parse_github_user_emits_account_email_url_twitter() -> None:
    resp = {"found": True, "user": {
        "login": "asuramaya", "name": "Priya", "company": "@acme",
        "blog": "asuramaya.com", "twitter_username": "@asuramaya_hq",
        "email": "Priya@Kowalski.dev", "location": "Earth", "bio": "hi",
        "html_url": "https://github.com/asuramaya",
    }}
    r = parse_github_user(resp, _inp("asuramaya"))
    objs = {(o.type, o.canonical): o for o in r.objects}
    assert objs[("Account", "github:asuramaya")].properties["name"] == "Priya"
    assert ("Email", "priya@kowalski.dev") in objs  # canonicalized lowercase
    assert ("URL", "https://asuramaya.com") in objs   # blog scheme-completed
    assert ("Account", "twitter:asuramaya_hq") in objs
    assert {link.type for link in r.links} == {"is_profile", "has_email", "has_url", "co_occurs"}


def test_parse_github_user_noop_when_not_found() -> None:
    assert parse_github_user({"found": False}, _inp("ghost")).objects == []
    assert parse_github_user({"found": False, "rate_limited": True}, _inp("x")).objects == []


class _Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a: object) -> None:
        pass

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/users/real":
            body = json.dumps({"login": "real", "name": "Real Person"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/users/ghost":
            self.send_response(404)
            self.end_headers()
        elif self.path == "/users/limited":
            self.send_response(403)
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()


@pytest.fixture
def gh(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}/users/"
    monkeypatch.setattr("src.connectors.github._API", base)
    try:
        yield base
    finally:
        server.shutdown()


async def test_fetch_github_maps_statuses(gh: str) -> None:
    assert (await fetch_github_user(_inp("real")))["found"] is True
    assert (await fetch_github_user(_inp("ghost")))["found"] is False
    limited = await fetch_github_user(_inp("limited"))
    assert limited["found"] is False and limited["rate_limited"] is True

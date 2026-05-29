from __future__ import annotations

import http.server
import threading
import uuid
from collections.abc import Iterator

import pytest
from src.connectors.urlfetch import fetch_webpage
from src.orchestrator.challenges import ChallengeDetected
from src.parsers.base import InputObject


class _Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args: object) -> None:  # silence
        pass

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/ok":
            body = b"<html><head><title>ok</title></head><body>hi</body></html>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/big":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<html>" + b"x" * 5_000_000 + b"</html>")
        elif self.path == "/binary":
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()
            self.wfile.write(b"\x00\x01\x02not html")
        elif self.path == "/wall":
            self.send_response(403)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<title>Just a moment...</title>")
        else:
            self.send_response(404)
            self.end_headers()


@pytest.fixture
def site() -> Iterator[str]:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()


def _inp(url: str) -> InputObject:
    return InputObject(id=str(uuid.uuid4()), type="URL", canonical=url)


async def test_fetch_ok_html(site: str) -> None:
    r = await fetch_webpage(_inp(f"{site}/ok"))
    assert r["fetched"] is True
    assert "<title>ok</title>" in r["html"]


async def test_fetch_caps_oversize_body(site: str) -> None:
    r = await fetch_webpage(_inp(f"{site}/big"), max_bytes=100_000)
    assert r["fetched"] is True
    assert len(r["html"]) <= 100_000  # never read the whole 5MB body


async def test_fetch_rejects_non_html(site: str) -> None:
    r = await fetch_webpage(_inp(f"{site}/binary"))
    assert r["fetched"] is False and r["reason"] == "not-html"


async def test_fetch_rejects_non_http() -> None:
    r = await fetch_webpage(_inp("ftp://example.com/file"))
    assert r["fetched"] is False and r["reason"] == "non-http"


async def test_fetch_raises_on_bot_wall(site: str) -> None:
    with pytest.raises(ChallengeDetected):
        await fetch_webpage(_inp(f"{site}/wall"))

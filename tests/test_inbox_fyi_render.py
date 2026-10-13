"""An inbox that holds only fyi mail used to read as "3 fyi message(s), ack to settle": no
sender, no text, and no way to ack from there. The glance now shows the fyi rows in the same
one-line shape as asks (asks first, capped), and closes with the exact command that settles
them, `osiris inbox --ack`, which is an act and so never served by the read hook."""
from __future__ import annotations

from typing import Any

import pytest
from src import cli
from src.orchestrator.textrender import MAIL_ACK_COMMAND, render_mail_text


def _msg(i: int, grade: str, body: str = "hello", sender: str = "agent:abc12345",
         thread: int | None = None) -> dict[str, Any]:
    return {"id": i, "grade": grade, "from": sender, "thread": thread or i, "body": body}


def test_an_inbox_of_only_fyi_shows_who_and_what() -> None:
    out = render_mail_text([
        _msg(7, "fyi", "deploy 1234: osiris-mcp is back up.", sender="deploy:disconnect-warning"),
        _msg(8, "fyi", "DEPLOYED and PUSHED: main = deployed", sender="agent:ad1a1cb0-g129"),
    ])
    lines = out.splitlines()
    assert lines[0] == "7 fyi from:deploy:disconnect-warning thread:7: " \
        "deploy 1234: osiris-mcp is back up."
    assert lines[1].startswith("8 fyi from:agent:ad1a1cb0-g129 thread:8: DEPLOYED and PUSHED")
    assert "2 fyi message(s)" in lines[-1] and MAIL_ACK_COMMAND in lines[-1]


def test_asks_come_first_then_the_fyi_rows() -> None:
    out = render_mail_text([_msg(1, "fyi"), _msg(2, "ask", "needs a reply"), _msg(3, "fyi")])
    assert [line.split()[1] for line in out.splitlines()[:3]] == ["ask", "fyi", "fyi"]


def test_ungraded_mail_is_listed_as_fyi_never_dropped() -> None:
    out = render_mail_text([_msg(4, "")])
    assert out.splitlines()[0].startswith("4 fyi from:")


def test_the_fyi_rows_are_capped_and_the_rest_counted() -> None:
    out = render_mail_text([_msg(i, "fyi") for i in range(1, 21)])
    lines = out.splitlines()
    assert sum(1 for line in lines if " fyi from:" in line) == 15
    assert "+5 more fyi" in lines
    assert "20 fyi message(s)" in lines[-1]


def test_a_cut_fyi_row_says_how_much_is_missing() -> None:
    out = render_mail_text([_msg(5, "fyi", "word " * 100)])
    assert "more characters: inbox() reads it whole" in out.splitlines()[0]


def test_only_asks_means_no_ack_line() -> None:
    out = render_mail_text([_msg(1, "ask", "needs a reply")])
    assert "fyi message" not in out and MAIL_ACK_COMMAND not in out


def test_an_empty_inbox_is_still_empty() -> None:
    assert render_mail_text([]) == "mail: empty"


# --- the CLI ------------------------------------------------------------------------------

class _FakeServer:
    """Stands in for the MCP server behind `osiris inbox`: records every call."""

    def __init__(self, messages: list[dict[str, Any]]) -> None:
        self.messages = messages
        self.calls: list[dict[str, Any]] = []

    async def __call__(self, url: str, tool: str, params: dict[str, Any]) -> Any:
        assert tool == "inbox"
        self.calls.append(dict(params))
        if "ack" in params:
            return {"settled": list(params["ack"]), "messages": []}
        if params.get("render") == "text":
            return {"text": render_mail_text(self.messages)}
        return {"messages": self.messages}


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch) -> _FakeServer:
    fake = _FakeServer([_msg(1, "fyi", "one"), _msg(2, "ask", "reply please"),
                        _msg(3, "fyi", "three")])

    async def _url() -> str:
        return "http://fake"

    monkeypatch.setattr(cli, "_mcp_url", _url)
    monkeypatch.setattr("src.orchestrator.mcp_client.call_mcp_tool", fake)
    return fake


async def test_the_text_read_names_the_exact_command_for_this_mailbox(
    server: _FakeServer, capsys: pytest.CaptureFixture[str],
) -> None:
    assert await cli.cmd_inbox(project="osiris", session="sess1234-0000", text=True) == 0
    out = capsys.readouterr().out
    assert "osiris inbox --session sess1234-0000 --project osiris --ack all-fyi" in out
    assert all("ack" not in call for call in server.calls)   # a read never acks


async def test_ack_all_fyi_settles_only_the_fyi_messages(
    server: _FakeServer, capsys: pytest.CaptureFixture[str],
) -> None:
    assert await cli.cmd_inbox(project="osiris", session="sess1234-0000", ack="all-fyi") == 0
    ack_call = server.calls[-1]
    assert ack_call["ack"] == [1, 3] and "peek" not in ack_call
    assert ack_call["session_anchor"]
    assert "acked 2 message(s): 1, 3" in capsys.readouterr().out


async def test_ack_named_ids_settles_exactly_those(
    server: _FakeServer, capsys: pytest.CaptureFixture[str],
) -> None:
    assert await cli.cmd_inbox(project="osiris", ack="5, 9") == 0
    assert server.calls[-1]["ack"] == [5, 9]
    assert "acked 2" in capsys.readouterr().out


async def test_ack_with_nothing_to_ack_says_so_and_writes_nothing(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    fake = _FakeServer([_msg(2, "ask", "needs a reply")])

    async def _url() -> str:
        return "http://fake"

    monkeypatch.setattr(cli, "_mcp_url", _url)
    monkeypatch.setattr("src.orchestrator.mcp_client.call_mcp_tool", fake)
    assert await cli.cmd_inbox(project="osiris", ack="all-fyi") == 0
    assert "no fyi messages to ack" in capsys.readouterr().out
    assert all("ack" not in call for call in fake.calls)


async def test_a_bad_ack_spec_is_refused(
    server: _FakeServer, capsys: pytest.CaptureFixture[str],
) -> None:
    assert await cli.cmd_inbox(project="osiris", ack="everything") == 2
    assert "takes message ids" in capsys.readouterr().err
    assert server.calls == []


def test_the_parser_takes_ack_and_hands_it_to_cmd_inbox(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    async def _fake(**kwargs: Any) -> int:
        seen.update(kwargs)
        return 0

    monkeypatch.setattr(cli, "cmd_inbox", _fake)
    assert cli.main(["inbox", "--project", "osiris", "--ack", "all-fyi"]) == 0
    assert seen["ack"] == "all-fyi" and seen["project"] == "osiris"

"""Rebuild a stored session as a NEW session in another harness's native format (crush).

The fixture is a session another Claude account owns, ingested into the soul store. The rules
pinned here: a new session id and a new database, the original untouched, no owner or account
stamp written anywhere, content carried faithfully, and what was folded reported."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from src.ingest.harness import SessionLocator
from src.ingest.harness.crush_sqlite import CrushSqliteAdapter
from src.ingest.rebuild import rebuild_session, turns_from_claude_lines
from src.ingest.soul_store import SoulStore

from tests.test_session_brief import OLD, SID, _a, _inbox_dm, _ingest, _result, _text, _u, _use


def _lines() -> list[str]:
    split_a = json.loads(_a([_text("Looking at the widget first.")]))
    split_b = json.loads(_a([_use("Bash", command="ls")]))
    for d in (split_a, split_b):
        d["message"]["id"] = "msg_one"            # one API message written as two lines
    tool_use_id = "toolu_01"
    split_b["message"]["content"][0]["id"] = tool_use_id
    return [
        _u("<system-reminder>noise</system-reminder>", isMeta=True),
        json.dumps({"type": "attachment", "attachment": {"type": "date"}}),
        json.dumps({"type": "last-prompt", "lastPrompt": "x"}),
        _u("Please build the widget."),
        json.dumps(split_a), json.dumps(split_b),
        json.dumps({"type": "user", "timestamp": "2026-09-01T10:06:00.000Z", "message": {
            "content": [{"type": "tool_result", "tool_use_id": tool_use_id,
                         "content": [{"type": "text", "text": "widget.py"},
                                     {"type": "image", "source": {}}]}]}}),
        _a([{"type": "thinking", "thinking": "the file exists"},
            _text("Done: widget.py is there.")]),
        _inbox_dm(5, "agent:ad1a1cb0-g129", "ok"),
        _result("[widget-lane 1a2b3c4] x"),
    ]


def test_the_neutral_turns_merge_split_messages_and_name_tool_results() -> None:
    r = turns_from_claude_lines((i, ln.encode()) for i, ln in enumerate(_lines()))
    roles = [t.role for t in r.turns]
    assert roles == ["user", "assistant", "tool", "assistant", "tool", "tool"]
    assert [p.kind for p in r.turns[1].parts] == ["text", "tool_call"]    # two lines, one turn
    assert r.turns[2].parts[0].name == "Bash" and r.turns[2].parts[0].call_id == "toolu_01"
    assert "[image not carried]" in r.turns[2].parts[0].text
    assert [p.kind for p in r.turns[3].parts] == ["reasoning", "text"]
    assert r.folded["attachment"] == 1 and r.folded["last-prompt"] == 1
    assert r.folded["user (meta)"] == 1 and r.folded["image inside a tool result"] == 1


async def test_a_stored_session_is_rebuilt_as_a_new_crush_session(
    actions: Any, tmp_path: Path,
) -> None:
    (tmp_path / "src").mkdir()
    await _ingest(actions, tmp_path / "src", _lines())
    before = await actions.pool.fetchval(
        "SELECT count(*) FROM soul_lines WHERE harness='claude-code' AND anchor_sid=$1", SID)
    ws = tmp_path / "workspace"
    out = await rebuild_session(actions.pool, SID, to="crush", into=ws)

    assert out["to"] == "crush" and out["session_id"] != SID
    db = ws / ".crush" / "crush.db"
    assert out["path"] == str(db) and db.is_file()
    conn = sqlite3.connect(db)
    sess = conn.execute("SELECT id, title, message_count FROM sessions").fetchall()
    assert sess == [(out["session_id"], f"Rebuilt from {SID}", 7)]   # marker + 6 turns
    rows = conn.execute("SELECT role, parts, model, is_summary_message FROM messages "
                        "ORDER BY rowid").fetchall()
    assert [r[0] for r in rows] == ["user", "user", "assistant", "tool", "assistant",
                                    "tool", "tool"]
    marker = json.loads(rows[0][1])[0]["data"]["text"]
    assert f"rebuilt by osiris from stored session {SID}" in marker
    assert "no owner or account is recorded" in marker and "attachment 1" in marker
    assert json.loads(rows[1][1])[0]["data"]["text"] == "Please build the widget."
    a1 = json.loads(rows[2][1])
    assert [p["type"] for p in a1] == ["text", "tool_call", "finish"]
    assert a1[1]["data"]["name"] == "Bash"
    assert json.loads(a1[1]["data"]["input"]) == {"command": "ls"}
    assert a1[2]["data"]["reason"] == "tool_use"
    res = json.loads(rows[3][1])[0]["data"]
    assert res["tool_call_id"] == "toolu_01" and res["name"] == "Bash"
    assert res["content"].startswith("widget.py")
    assert [p["type"] for p in json.loads(rows[4][1])] == ["reasoning", "text", "finish"]
    assert rows[2][2] == "claude-opus-5"
    dump = "\n".join(conn.iterdump())
    assert OLD not in dump and "ownerAccountUuid" not in dump and "accountUuid" not in dump
    conn.close()

    # the original is untouched
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM soul_lines WHERE harness='claude-code' AND anchor_sid=$1",
        SID) == before
    assert await SoulStore(actions.pool).verify_chain(SID) is True


async def test_the_rebuilt_session_reads_back_through_the_crush_adapter(
    actions: Any, tmp_path: Path,
) -> None:
    """Round trip: what the existing crush adapter reads from the new database has the same
    roles, in the same order, as the turns that went in."""
    (tmp_path / "src").mkdir()
    await _ingest(actions, tmp_path / "src", _lines())
    ws = tmp_path / "workspace"
    out = await rebuild_session(actions.pool, SID, to="crush", into=ws)
    loc = SessionLocator(anchor_sid=out["session_id"][:8], session_id=out["session_id"],
                         harness="crush", source_path=str(ws / ".crush" / "crush.db"),
                         cwd=str(ws), project="workspace")
    read = list(CrushSqliteAdapter().read_turns(loc))
    assert [t.role for t in read] == ["user", "user", "assistant", "tool", "assistant",
                                      "tool", "tool"]
    assert read[2].model == "claude-opus-5"


async def test_a_second_rebuild_joins_the_same_database_as_another_session(
    actions: Any, tmp_path: Path,
) -> None:
    (tmp_path / "src").mkdir()
    await _ingest(actions, tmp_path / "src", _lines())
    ws = tmp_path / "workspace"
    one = await rebuild_session(actions.pool, SID, to="crush", into=ws)
    two = await rebuild_session(actions.pool, SID, to="crush", into=ws)
    assert one["session_id"] != two["session_id"]
    conn = sqlite3.connect(ws / ".crush" / "crush.db")
    assert conn.execute("SELECT count(*) FROM sessions").fetchone()[0] == 2
    conn.close()


async def test_an_unknown_session_or_target_refuses(actions: Any, tmp_path: Path) -> None:
    import pytest

    with pytest.raises(ValueError, match="holds no turns"):
        await rebuild_session(actions.pool, "00000000", to="crush", into=tmp_path)
    with pytest.raises(ValueError, match="not available yet"):
        await rebuild_session(actions.pool, SID, to="nonesuch", into=tmp_path)


# --- the claude writer: tmp dirs only -------------------------------------------------------

def _claude_lines(path: Path) -> list[dict[str, Any]]:
    return [json.loads(ln) for ln in path.read_text().splitlines()]


async def test_a_stored_session_is_rebuilt_as_a_new_claude_file_with_no_owner(
    actions: Any, tmp_path: Path,
) -> None:
    (tmp_path / "src").mkdir()
    await _ingest(actions, tmp_path / "src", _lines())
    projects = tmp_path / "projects"
    out = await rebuild_session(actions.pool, SID, to="claude", into=projects,
                                cwd="/var/tmp/osiris-scratch/replay")
    assert out["session_id"] != SID
    path = Path(out["path"])
    assert path.parent == projects / "-var-tmp-osiris-scratch-replay"
    assert path.name == out["session_id"] + ".jsonl"
    lines = _claude_lines(path)
    # no ownership of anyone: not the old account, not any account, no session-owner record
    text = path.read_text()
    assert "ownerAccountUuid" not in text and "ownerOrganizationUuid" not in text
    assert "session-owner" not in text and OLD not in text and "accountUuid" not in text
    # a consistent chain under one new session id
    assert lines[0]["parentUuid"] is None
    for prev, cur in zip(lines, lines[1:], strict=False):
        assert cur["parentUuid"] == prev["uuid"]
    assert len({d["uuid"] for d in lines}) == len(lines)
    assert {d["sessionId"] for d in lines} == {out["session_id"]}
    assert {d["cwd"] for d in lines} == {"/var/tmp/osiris-scratch/replay"}
    # it opens with the import note
    assert lines[0]["type"] == "user"
    assert f"rebuilt by osiris from stored session {SID}" in lines[0]["message"]["content"]


async def test_the_claude_file_reads_back_to_the_same_turns(
    actions: Any, tmp_path: Path,
) -> None:
    """Round trip: the written file, read as a Claude transcript again, yields the same turns
    as the ones that went in (after the import note)."""
    (tmp_path / "src").mkdir()
    await _ingest(actions, tmp_path / "src", _lines())
    original = turns_from_claude_lines(
        (i, ln.encode()) for i, ln in enumerate(_lines()))
    out = await rebuild_session(actions.pool, SID, to="claude", into=tmp_path / "p",
                                cwd="/var/tmp/osiris-scratch/replay")
    again = turns_from_claude_lines(
        (i, ln.encode()) for i, ln in enumerate(Path(out["path"]).read_text().splitlines()))
    assert [t.role for t in again.turns[1:]] == [t.role for t in original.turns]
    for a, b in zip(again.turns[1:], original.turns, strict=True):
        assert [(p.kind, p.text, p.name, p.call_id) for p in a.parts] == [
            (p.kind, p.text, p.name, p.call_id) for p in b.parts]
        assert a.model == b.model


async def test_the_claude_writer_refuses_the_real_session_store_and_needs_a_cwd(
    actions: Any, tmp_path: Path,
) -> None:
    import pytest
    from src.ingest.rebuild import real_claude_projects_dir

    (tmp_path / "src").mkdir()
    await _ingest(actions, tmp_path / "src", _lines())
    with pytest.raises(ValueError, match="held until the operator allows it"):
        await rebuild_session(actions.pool, SID, to="claude", into=real_claude_projects_dir(),
                              cwd="/var/tmp/osiris-scratch/replay")
    with pytest.raises(ValueError, match="needs --cwd"):
        await rebuild_session(actions.pool, SID, to="claude", into=tmp_path / "p")


async def test_no_target_may_write_into_claude_codes_session_store_by_any_route(
    actions: Any, tmp_path: Path, monkeypatch: Any,
) -> None:
    """The store guard covers every target, a sub-directory of the store, and symlinks that
    lead into it: the store is never written, whichever way the path is spelled."""
    import pytest

    home = tmp_path / "home"
    store = home / ".claude" / "projects"
    store.mkdir(parents=True)
    monkeypatch.setenv("HOME", str(home))
    (tmp_path / "src").mkdir()
    await _ingest(actions, tmp_path / "src", _lines())
    cwd = "/var/tmp/osiris-scratch/replay"
    sub = store / "-some-slug"
    sub.mkdir()
    for to, into in (("claude", sub), ("crush", store), ("crush", sub), ("dsh", store),
                     ("dsh", sub)):
        with pytest.raises(ValueError, match="held until the operator allows it"):
            await rebuild_session(actions.pool, SID, to=to, into=into, cwd=cwd)
    # a scratch workspace whose .crush is a link into the store
    work = tmp_path / "work"
    work.mkdir()
    (work / ".crush").symlink_to(sub)
    with pytest.raises(ValueError, match="symlink"):
        await rebuild_session(actions.pool, SID, to="crush", into=work)
    # a scratch workspace whose .crush is real but whose crush.db is a link into the store
    work2 = tmp_path / "work2"
    (work2 / ".crush").mkdir(parents=True)
    (work2 / ".crush" / "crush.db").symlink_to(sub / "crush.db")
    with pytest.raises(ValueError, match="symlink"):
        await rebuild_session(actions.pool, SID, to="crush", into=work2)
    # a claude projects dir (scratch) whose slug directory is a link into the store
    scratch = tmp_path / "scratch-projects"
    scratch.mkdir()
    from src.ingest.rebuild import _slug

    (scratch / _slug(cwd)).symlink_to(sub)
    with pytest.raises(ValueError, match="symlink"):
        await rebuild_session(actions.pool, SID, to="claude", into=scratch, cwd=cwd)
    # the same for a dsh slug directory
    sessions = tmp_path / "dsh-sessions"
    sessions.mkdir()
    from src.ingest.harness.dsh import _cwd_to_slug

    (sessions / _cwd_to_slug(cwd)).symlink_to(sub)
    with pytest.raises(ValueError, match="symlink"):
        await rebuild_session(actions.pool, SID, to="dsh", into=sessions, cwd=cwd)
    assert not list(store.rglob("*.jsonl")) and not list(store.rglob("*.db"))
    assert not list(store.rglob("session.jsonl*"))


async def test_a_broken_stored_chain_is_reported_as_truncated_never_as_complete(
    actions: Any, tmp_path: Path, capsys: Any,
) -> None:
    """`stream_lines` ends at the last verified line when the chain breaks. The rebuild must
    say so: "truncated at line N of M" in the receipt, the opening note and the CLI output."""
    from src.cli import cmd_rebuild_transcript

    (tmp_path / "src").mkdir()
    await _ingest(actions, tmp_path / "src", _lines())
    whole = await rebuild_session(actions.pool, SID, to="crush", into=tmp_path / "whole")
    assert whole["truncated"] is None
    total = await actions.pool.fetchval(
        "SELECT last_line_idx FROM soul_sessions WHERE harness='claude-code' AND anchor_sid=$1",
        SID)
    await actions.pool.execute(
        "DELETE FROM soul_lines WHERE harness='claude-code' AND anchor_sid=$1 AND line_idx=5",
        SID)
    cut = await rebuild_session(actions.pool, SID, to="crush", into=tmp_path / "cut")
    assert cut["truncated"] == f"stored chain broke at line 5 of {total}"
    conn = sqlite3.connect(tmp_path / "cut" / ".crush" / "crush.db")
    first = conn.execute("SELECT parts FROM messages ORDER BY rowid LIMIT 1").fetchone()[0]
    conn.close()
    assert f"TRUNCATED: the stored chain broke at line 5 of {total}" in first
    assert "TRUNCATED" not in sqlite3.connect(tmp_path / "whole" / ".crush" / "crush.db").execute(
        "SELECT parts FROM messages ORDER BY rowid LIMIT 1").fetchone()[0]
    rc = await cmd_rebuild_transcript(SID, to="crush", into=str(tmp_path / "cut2"),
                                      pool=actions.pool)
    assert rc == 0
    assert f"this copy is incomplete: the stored chain broke at line 5 of {total}" in (
        capsys.readouterr().out)


# --- the dsh writer: tmp dirs only ----------------------------------------------------------

def _dsh_events(path: Path) -> list[dict[str, Any]]:
    from src.ingest.harness.dsh import _decompress

    return [json.loads(ln) for ln in (_decompress(path) or [])]


async def test_a_stored_session_is_rebuilt_as_a_new_dsh_session_with_no_owner(
    actions: Any, tmp_path: Path,
) -> None:
    (tmp_path / "src").mkdir()
    await _ingest(actions, tmp_path / "src", _lines())
    sessions = tmp_path / "dsh-sessions"
    out = await rebuild_session(actions.pool, SID, to="dsh", into=sessions,
                                cwd="/var/tmp/osiris-scratch/replay")
    path = Path(out["path"])
    assert path.name == "session.jsonl.zstd" and path.parent.name == out["session_id"]
    assert path.parent.parent.parent == sessions
    assert not (path.parent / "session.jsonl").exists()          # only the compressed file
    events = _dsh_events(path)
    assert events[0]["type"] == "session" and events[0]["id"] == out["session_id"]
    assert events[0]["cwd"] == "/var/tmp/osiris-scratch/replay"
    assert [e["seq"] for e in events[1:]] == list(range(1, len(events)))
    blob = json.dumps(events)
    assert OLD not in blob and "ownerAccountUuid" not in blob and "accountUuid" not in blob
    kinds = [e["type"] for e in events[1:]]
    assert kinds == ["request/header", "user/message", "user/message", "assistant/message",
                     "tool/result", "assistant/message", "tool/result", "tool/result"]
    note = events[2]["data"]["content"][0]["text"]
    assert f"rebuilt by osiris from stored session {SID}" in note
    assert "reasoning carried as marked text 1" in note


async def test_the_dsh_session_reads_back_through_the_dsh_adapter(
    actions: Any, tmp_path: Path,
) -> None:
    """Round trip: the existing dsh adapter reads the same roles, in order, from the new
    session, and the tool call and its result keep their ids and text."""
    from src.ingest.harness.dsh import DshSessionAdapter

    (tmp_path / "src").mkdir()
    await _ingest(actions, tmp_path / "src", _lines())
    out = await rebuild_session(actions.pool, SID, to="dsh", into=tmp_path / "dsh",
                                cwd="/var/tmp/osiris-scratch/replay")
    loc = DshSessionAdapter().discover_at(Path(out["path"]))
    assert loc is not None
    read = list(DshSessionAdapter().read_turns(loc))
    assert [t.role for t in read] == ["user", "user", "assistant", "assistant"]
    assert read[2].model == "claude-opus-5" or read[2].model is None
    events = _dsh_events(Path(out["path"]))
    call = events[4]["data"]["message"]["content"][1]
    assert call["type"] == "tool-call" and call["id"] == "toolu_01" and call["name"] == "Bash"
    assert json.loads(call["arguments"]) == {"command": "ls"}
    res = events[5]["data"]["message"]["content"][0]
    assert res["toolCallId"] == "toolu_01" and res["content"][0]["text"].startswith("widget.py")
    reasoning = events[6]["data"]["message"]["content"][0]["text"]
    assert reasoning == "[reasoning] the file exists"


async def test_dsh_needs_a_cwd(actions: Any, tmp_path: Path) -> None:
    import pytest

    (tmp_path / "src").mkdir()
    await _ingest(actions, tmp_path / "src", _lines())
    with pytest.raises(ValueError, match="needs --cwd"):
        await rebuild_session(actions.pool, SID, to="dsh", into=tmp_path / "d")


# --- closing the loop for the operator: --install and the printed commands ------------------

async def test_install_places_a_dsh_session_under_the_dsh_home_and_prints_how_to_open_it(
    actions: Any, tmp_path: Path, monkeypatch: Any, capsys: Any,
) -> None:
    from src.cli import cmd_rebuild_transcript

    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "src").mkdir()
    await _ingest(actions, tmp_path / "src", _lines())
    rc = await cmd_rebuild_transcript(
        SID, to="dsh", into=None, cwd="/var/tmp/osiris-scratch/replay", install=True,
        pool=actions.pool)
    out = capsys.readouterr().out
    assert rc == 0
    placed = list((tmp_path / "home" / ".dsh" / "sessions").rglob("session.jsonl.zstd"))
    assert len(placed) == 1 and placed[0].parent.name.startswith("session-")
    sid = placed[0].parent.name
    assert f"choose session {sid}" in out
    assert "cd /var/tmp/osiris-scratch/replay && npx @deepseek-ai/dsh web" in out


async def test_install_is_for_dsh_only_and_an_into_is_otherwise_required(
    actions: Any, tmp_path: Path, capsys: Any,
) -> None:
    from src.cli import cmd_rebuild_transcript

    assert await cmd_rebuild_transcript(
        SID, to="crush", into=None, install=True, pool=actions.pool) == 1
    assert "--install is for --to dsh" in capsys.readouterr().err
    assert await cmd_rebuild_transcript(SID, to="crush", into=None, pool=actions.pool) == 1
    assert "--into is required" in capsys.readouterr().err


async def test_crush_prints_the_one_command_that_opens_the_session(
    actions: Any, tmp_path: Path, capsys: Any,
) -> None:
    from src.cli import cmd_rebuild_transcript

    (tmp_path / "src").mkdir()
    await _ingest(actions, tmp_path / "src", _lines())
    ws = tmp_path / "ws"
    assert await cmd_rebuild_transcript(
        SID, to="crush", into=str(ws), pool=actions.pool) == 0
    out = capsys.readouterr().out
    sid = sqlite3.connect(ws / ".crush" / "crush.db").execute(
        "SELECT id FROM sessions").fetchone()[0]
    assert f"open it: cd {ws} && crush --session {sid}" in out


async def test_a_dsh_session_never_overwrites_an_existing_one(
    actions: Any, tmp_path: Path, monkeypatch: Any,
) -> None:
    import uuid

    import pytest
    from src.ingest import rebuild

    (tmp_path / "src").mkdir()
    await _ingest(actions, tmp_path / "src", _lines())
    fixed = uuid.UUID("11111111-1111-4111-8111-111111111111")
    monkeypatch.setattr(rebuild.uuid, "uuid4", lambda: fixed)
    await rebuild_session(actions.pool, SID, to="dsh", into=tmp_path / "d", cwd="/x/y")
    with pytest.raises(ValueError, match="refusing to overwrite"):
        await rebuild_session(actions.pool, SID, to="dsh", into=tmp_path / "d", cwd="/x/y")


def test_the_voice_names_the_alternatives_in_one_short_line() -> None:
    from src import launch_voice

    line = launch_voice.rebuild_hint("4ffb15db-0000-4000-8000-000000000000")
    assert line == "Also: osiris rebuild-transcript 4ffb15db --to crush|dsh"

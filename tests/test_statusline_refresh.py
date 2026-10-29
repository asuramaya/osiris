"""Deploy keeps the status bar's refresh interval at 30 s in every settings file osiris owns,
touching nothing else; and a status bar render that needs no cache no longer pays for the
resolver and settings stacks."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import scripts.osiris_hook as osiris_hook
from src.orchestrator import statusline_refresh as sr
from src.orchestrator.agents import read_project_label, read_project_model

_HOOK = Path(__file__).resolve().parent.parent / "scripts" / "osiris_hook.py"
_OSIRIS = "/x/.venv/bin/python /x/scripts/osiris_hook.py statusline"


def _write(path: Path, doc: object, indent: int = 2) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=indent) + "\n")
    return path


def test_an_owned_file_is_set_to_thirty_and_every_other_key_is_untouched(tmp_path: Path) -> None:
    doc = {"statusLine": {"type": "command", "command": _OSIRIS, "padding": 0,
                          "refreshInterval": 10},
           "permissions": {"allow": ["Bash(ls)"]}, "env": {"A": "1"}}
    path = _write(tmp_path / "settings.json", doc, indent=4)
    path.chmod(0o600)

    out = sr.ensure_refresh_interval([path])

    assert out["changed"] == [str(path)]
    after = json.loads(path.read_text())
    assert after["statusLine"]["refreshInterval"] == 30
    assert {k: v for k, v in after.items() if k != "statusLine"} == \
        {k: v for k, v in doc.items() if k != "statusLine"}
    assert {k: v for k, v in after["statusLine"].items() if k != "refreshInterval"} == \
        {k: v for k, v in doc["statusLine"].items() if k != "refreshInterval"}
    assert oct(path.stat().st_mode & 0o777) == "0o600"
    assert '\n    "statusLine"' in path.read_text()  # the file's own 4-space indent kept


def test_a_second_run_changes_nothing_and_says_so(tmp_path: Path) -> None:
    path = _write(tmp_path / "s.json", {"statusLine": {"command": _OSIRIS, "refreshInterval": 10}})
    sr.ensure_refresh_interval([path])
    before = path.read_text()

    again = sr.ensure_refresh_interval([path])

    assert again["changed"] == [] and again["current"] == [str(path)]
    assert path.read_text() == before


def test_another_tools_status_line_and_files_without_one_are_left_alone(tmp_path: Path) -> None:
    other = _write(tmp_path / "a.json",
                   {"statusLine": {"command": "/usr/bin/other-tool", "refreshInterval": 5}})
    none = _write(tmp_path / "b.json", {"permissions": {}})
    before = (other.read_text(), none.read_text())

    out = sr.ensure_refresh_interval([other, none, tmp_path / "missing.json"])

    assert out["changed"] == [] and out["current"] == [] and out["skipped"] == []
    assert (other.read_text(), none.read_text()) == before


def test_a_file_that_is_not_json_is_reported_and_not_rewritten(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    out = sr.ensure_refresh_interval([bad])
    assert out["changed"] == [] and "not readable JSON" in out["skipped"][0]["why"]
    assert bad.read_text() == "{not json"


def test_a_dry_run_reports_without_writing(tmp_path: Path) -> None:
    path = _write(tmp_path / "s.json", {"statusLine": {"command": _OSIRIS, "refreshInterval": 10}})
    before = path.read_text()
    out = sr.ensure_refresh_interval([path], dry_run=True)
    assert out["changed"] == [str(path)] and path.read_text() == before


def test_the_candidates_are_the_users_the_repos_and_every_seat_office(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_CLAUDE_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    repo = tmp_path / "repo"
    user = _write(tmp_path / "home" / ".claude" / "settings.json",
                  {"statusLine": {"command": _OSIRIS, "refreshInterval": 10}})
    mine = _write(repo / ".claude" / "settings.json",
                  {"statusLine": {"command": _OSIRIS, "refreshInterval": 10}})
    office = _write(tmp_path / "seats" / "abc" / ".claude" / "settings.json",
                    {"statusLine": {"command": _OSIRIS, "refreshInterval": 10}})

    paths = sr.candidate_paths(repo)
    out = sr.ensure_refresh_interval(paths)

    assert set(out["changed"]) == {str(user), str(mine), str(office)}
    assert "3 file(s) set to 30s" in sr.refresh_line(repo) or "0 file(s) set to 30s" in \
        sr.refresh_line(repo)


def test_the_line_deploy_prints_counts_changed_and_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_CLAUDE_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    _write(tmp_path / "home" / ".claude" / "settings.json",
           {"statusLine": {"command": _OSIRIS, "refreshInterval": 10}})
    assert sr.refresh_line() == "statusline refresh: 1 file(s) set to 30s, 0 already current"
    assert sr.refresh_line() == "statusline refresh: 0 file(s) set to 30s, 1 already current"


def test_the_repos_committed_settings_already_say_thirty() -> None:
    committed = json.loads((Path(__file__).resolve().parent.parent / ".claude"
                            / "settings.json").read_text())
    assert committed["statusLine"]["refreshInterval"] == sr.REFRESH_SECONDS


def test_onboard_and_deploy_agree_on_the_interval() -> None:
    from src.orchestrator.onboard import merge_settings

    doc, _ = merge_settings({}, Path("/home/x/code/osiris"))
    assert doc["statusLine"]["refreshInterval"] == sr.REFRESH_SECONDS


# --- the render without the resolver stack ------------------------------------------------


def test_the_hooks_pin_reader_agrees_with_the_resolvers_everywhere_it_matters(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    (root / ".git").mkdir(parents=True)
    (root / ".osiris").write_text('project = "pinned"\nmodel = "claude-haiku-4-5"\n')
    deep = root / "a" / "b"
    deep.mkdir(parents=True)
    layered = root / "wt"
    layered.mkdir()
    (layered / ".osiris").write_text('seat = "x"\n')  # a file without the key: climb on
    (layered / ".git").write_text("gitlink")           # a worktree gitlink does not stop it
    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / ".osiris").write_text("= = not toml")
    bare = tmp_path / "bare"
    bare.mkdir()

    for cwd in (root, deep, layered, broken, bare, tmp_path / "missing", None):
        arg = str(cwd) if cwd is not None else None
        assert osiris_hook._pin_value(arg, "project") == read_project_label(arg), cwd
        assert osiris_hook._pin_value(arg, "model") == read_project_model(arg), cwd


def test_a_render_with_nothing_to_show_does_not_import_the_settings_stack(
    tmp_path: Path,
) -> None:
    """The cost that remained on a cache miss: pydantic settings, httpx, asyncio. Run the real
    hook against a stub heartbeat and ask the interpreter which modules it loaded."""
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class _H(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            body = json.dumps({"result": {"mail": 0, "dm": 0, "needs": 0,
                                          "resolved_project": "p"}}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a: object) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), _H)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        env = {**os.environ, "OSIRIS_HEARTBEAT_URL": f"http://127.0.0.1:{server.server_port}/h",
               "OSIRIS_STATUSLINE_RENDER_DIR": str(tmp_path / "r"),
               "OSIRIS_MAIL_BUMP_FILE": str(tmp_path / "b")}
        probe = (
            "import runpy, sys, io\n"
            f"sys.argv = ['{_HOOK}', 'statusline']\n"
            "sys.stdin = io.StringIO(%r)\n"
            f"try:\n    runpy.run_path('{_HOOK}', run_name='__main__')\n"
            "except SystemExit:\n    pass\n"
            "heavy = ('pydantic', 'httpx', 'asyncpg', 'src.config.settings',\n"
            "         'src.orchestrator.agents', 'src.ingest.providers')\n"
            "bad = [m for m in heavy if m in sys.modules]\n"
            "sys.stderr.write('LOADED ' + ','.join(bad))\n"
        ) % json.dumps({"cwd": str(tmp_path), "session_id": "s-imp-0001",
                        "model": {"id": "claude-sonnet-5-5"},
                        "context_window": {"used_percentage": 10.0}})
        out = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True,
                             env=env, timeout=60, check=False)
    finally:
        server.shutdown()
        server.server_close()
    assert "LOADED " in out.stderr, out.stderr
    assert out.stderr.split("LOADED ", 1)[1].strip() == "", out.stderr

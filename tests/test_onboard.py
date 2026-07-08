"""onboard — the LOCAL half of fleet onboarding: `.mcp.json` + statusline config generation.

Pure filesystem (no DB, no graph) — the graph half is the `bootstrap` MCP tool, tested
separately. These guard the merge contract that makes onboarding safe to re-run and safe over a
repo that already has its own MCP servers: never clobber, idempotent, refuse the unmergeable.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from src.orchestrator.onboard import (
    OSIRIS_MCP_URL,
    InvalidConfigError,
    merge_mcp,
    onboard,
)


def _read(path: Path) -> dict:
    return json.loads(path.read_text())


def test_fresh_repo_gets_both_files(tmp_path: Path) -> None:
    repo = tmp_path / "greenfield"
    repo.mkdir()
    home = tmp_path / "osiris"

    onboard(repo, statusline=True, osiris_home=home)

    mcp = _read(repo / ".mcp.json")
    srv = mcp["mcpServers"]["osiris"]
    assert srv["type"] == "http"
    assert srv["url"] == OSIRIS_MCP_URL
    assert srv["headers"]["X-Osiris-Job"] == "${CLAUDE_JOB_DIR}"

    settings = _read(repo / ".claude" / "settings.json")
    cmd = settings["statusLine"]["command"]
    assert settings["statusLine"]["type"] == "command"
    assert Path(cmd.split()[0]).is_absolute()  # absolute venv python
    assert cmd.endswith("scripts/osiris_statusline.py")
    assert ".venv/bin/python" in cmd


def test_existing_mcp_server_is_merged_not_clobbered(tmp_path: Path) -> None:
    repo = tmp_path / "hasconfig"
    repo.mkdir()
    other = {"type": "stdio", "command": "run-other"}
    (repo / ".mcp.json").write_text(json.dumps({"mcpServers": {"other": other}}))

    result = onboard(repo, osiris_home=tmp_path)

    servers = _read(repo / ".mcp.json")["mcpServers"]
    assert servers["other"] == other  # the pre-existing server survives untouched
    assert servers["osiris"]["url"] == OSIRIS_MCP_URL  # and osiris is added alongside it
    assert result["changes"][0].status == "patched"  # an existing file is patched, not recreated


def test_second_run_is_idempotent(tmp_path: Path) -> None:
    repo = tmp_path / "repeat"
    repo.mkdir()
    home = tmp_path / "osiris"

    first = onboard(repo, statusline=True, osiris_home=home)
    bytes_after_first = (repo / ".mcp.json").read_bytes()
    settings_after_first = (repo / ".claude" / "settings.json").read_bytes()

    second = onboard(repo, statusline=True, osiris_home=home)

    assert (repo / ".mcp.json").read_bytes() == bytes_after_first  # identical bytes
    assert (repo / ".claude" / "settings.json").read_bytes() == settings_after_first
    assert all(c.wrote for c in first["changes"])  # first run wrote
    assert all(c.status == "unchanged" for c in second["changes"])  # second changed nothing


def test_invalid_json_is_refused_and_left_untouched(tmp_path: Path) -> None:
    repo = tmp_path / "broken"
    repo.mkdir()
    garbage = "{ this is not json"
    (repo / ".mcp.json").write_text(garbage)

    with pytest.raises(InvalidConfigError):
        onboard(repo, osiris_home=tmp_path)

    assert (repo / ".mcp.json").read_text() == garbage  # never overwritten


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    repo = tmp_path / "preview"
    repo.mkdir()

    result = onboard(repo, statusline=True, dry_run=True, osiris_home=tmp_path)

    assert not (repo / ".mcp.json").exists()
    assert not (repo / ".claude").exists()
    assert [c.status for c in result["changes"]] == ["would-create", "would-create"]


def test_settings_merge_preserves_other_keys(tmp_path: Path) -> None:
    repo = tmp_path / "hassettings"
    (repo / ".claude").mkdir(parents=True)
    prior = {"permissions": {"allow": ["Bash(ls)"]}, "statusLine": {"stale": True}}
    (repo / ".claude" / "settings.json").write_text(json.dumps(prior))

    onboard(repo, statusline=True, osiris_home=tmp_path)

    settings = _read(repo / ".claude" / "settings.json")
    assert settings["permissions"] == {"allow": ["Bash(ls)"]}  # untouched
    assert settings["statusLine"]["type"] == "command"  # replaced with the real statusline


def test_user_scope_prints_one_liner_and_writes_no_mcp(tmp_path: Path) -> None:
    repo = tmp_path / "userscope"
    repo.mkdir()

    result = onboard(repo, user_scope=True, osiris_home=tmp_path)

    assert not (repo / ".mcp.json").exists()  # user scope needs no per-repo file
    assert result["changes"][0].status == "skipped"
    checklist = result["checklist"]
    assert "claude mcp add --scope user --transport http osiris" in checklist
    assert "'X-Osiris-Job: ${CLAUDE_JOB_DIR}'" in checklist  # single-quoted literal preserved


def test_default_checklist_leads_with_user_scope(tmp_path: Path) -> None:
    repo = tmp_path / "leadwith"
    repo.mkdir()

    checklist = onboard(repo, osiris_home=tmp_path)["checklist"]

    # the box-wide default is offered BEFORE the per-repo boot stanza
    assert "RECOMMENDED" in checklist
    assert checklist.index("RECOMMENDED") < checklist.index("boot sector")


def test_boot_stanza_carries_the_mount_ritual_and_confession(tmp_path: Path) -> None:
    repo = tmp_path / "stanza"
    repo.mkdir()

    checklist = onboard(repo, osiris_home=tmp_path)["checklist"]

    for token in ("mount(cwd=", "orient()", "inbox()", "record_decision", "rug-pull"):
        assert token in checklist, token
    assert "`bootstrap` MCP tool" in checklist  # the md-migration note


def test_merge_mcp_rejects_malformed_servers_map() -> None:
    with pytest.raises(InvalidConfigError):
        merge_mcp({"mcpServers": ["not", "an", "object"]})

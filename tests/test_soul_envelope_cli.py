"""osiris soul-key envelope <list|check|add-key|add-code|remove|verify>.

The module's own rules are tested in test_soul_envelope.py; this pins the terminal door: exit
codes, that a recovery code is shown only on a terminal and read back with getpass (never an
argument, never in --json or a pipe), and that a refusal is a plain line, not a traceback."""
from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import pytest
from cryptography.fernet import Fernet
from src.cli import cmd_soul_key
from src.ingest import soul_envelope
from src.ingest.soul_crypto import key_fingerprint, recovery_file_path

KEY = Fernet.generate_key()          # a real Fernet key, as the soul key is


@pytest.fixture
def keyfile(tmp_path: Path) -> str:
    return str(tmp_path / "soul.key")


def _hw(rid: str = "k1") -> dict[str, Any]:
    token = Fernet(Fernet.generate_key()).encrypt(KEY)
    return {"id": rid, "kind": "fido2-prf", "label": "yubikey", "added_at": "2026-11-12T00:00:00Z",
            "last_opened_at": "2026-11-12T01:00:00Z", "credential_id": "Y3JlZA==",
            "salt": "c2FsdA==", "wrapped_key": base64.urlsafe_b64encode(token).decode(),
            "rp_id": "localhost"}


def _envelope(keyfile: str) -> Path:
    path = recovery_file_path(path=keyfile)
    path.write_text(json.dumps({"version": 2, "key_fingerprint": key_fingerprint(KEY),
                                "recipients": [_hw()], "serial": 1}))
    return path


async def _run(action: str | None, keyfile: str, **kw: Any) -> int:
    return await cmd_soul_key("envelope", path=keyfile, envelope_action=action, **kw)


async def test_list_and_check_report_the_envelope(keyfile: str, capsys: Any) -> None:
    _envelope(keyfile)
    assert await _run("list", keyfile) == 0
    assert "k1" in capsys.readouterr().out
    assert await _run("check", keyfile) == 0
    path = recovery_file_path(path=keyfile)
    blob = json.loads(path.read_text())
    blob["recipients"][0].pop("salt")
    path.write_text(json.dumps(blob))
    assert await _run("check", keyfile) == 1


async def test_a_new_recovery_code_is_shown_only_on_a_terminal(
    keyfile: str, capsys: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _envelope(keyfile)
    monkeypatch.setattr(soul_envelope, "_live_key", lambda p: KEY)
    # captured output is not a terminal: refused, nothing written, no code anywhere
    assert await _run("add-code", keyfile) == 1
    out = capsys.readouterr()
    assert "only on a terminal" in out.err
    assert len(json.loads(path.read_text())["recipients"]) == 1
    # --json is refused too, even on a terminal
    import sys

    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    assert await _run("add-code", keyfile, as_json=True) == 1
    capsys.readouterr()
    # on a terminal the code is printed once, with the warning, and stored nowhere
    assert await _run("add-code", keyfile, label="paper") == 0
    shown = capsys.readouterr().out
    assert "WRITE THIS RECOVERY CODE DOWN NOW" in shown
    code = next(line.strip() for line in shown.splitlines() if line.strip().count("-") == 7)
    assert code not in path.read_text()
    assert [r["id"] for r in json.loads(path.read_text())["recipients"]] == ["k1", "c2"]
    # and verifying it asks for the code with getpass, never from an argument
    asked: list[str] = []

    import getpass

    monkeypatch.setattr(getpass, "getpass", lambda prompt="": asked.append(prompt) or code)
    assert await _run("verify", keyfile, recipient="c2") == 0
    assert asked == ["Recovery code: "]
    assert json.loads(path.read_text())["recipients"][1]["last_opened_at"]


async def test_a_wrong_code_fails_and_a_missing_one_says_nobody_acted(
    keyfile: str, capsys: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _envelope(keyfile)
    monkeypatch.setattr(soul_envelope, "_live_key", lambda p: KEY)
    import getpass
    import sys

    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    assert await _run("add-code", keyfile) == 0
    capsys.readouterr()
    monkeypatch.setattr(getpass, "getpass", lambda prompt="": "A" * 32)
    assert await _run("verify", keyfile, recipient="c2") == 1
    assert "the code does not match" in capsys.readouterr().out
    monkeypatch.setattr(getpass, "getpass", lambda prompt="": None)
    assert await _run("verify", keyfile, recipient="c2") == 1
    assert "nobody acted in time" in capsys.readouterr().err


async def test_remove_and_the_plain_refusals(keyfile: str, capsys: Any) -> None:
    _envelope(keyfile)
    assert await _run("remove", keyfile) == 1
    assert "--recipient <id> is required" in capsys.readouterr().err
    assert await _run("remove", keyfile, recipient="k1") == 1
    assert "only recipient" in capsys.readouterr().err
    assert await _run("remove", keyfile, recipient="zz") == 1
    assert await _run(None, keyfile) == 1
    assert "choose one of" in capsys.readouterr().err


async def test_add_key_without_an_envelope_refuses_plainly(keyfile: str, capsys: Any) -> None:
    assert await _run("add-key", keyfile) == 1
    assert "enroll the first key" in capsys.readouterr().err

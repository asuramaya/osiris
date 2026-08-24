"""scripts/osiris_hook.py — the unified, stdlib-only hook client (dispatch 5441, the hook
migration). This file proves the pieces this session's parity audit found genuinely
missing before the port: the `stop_hook_active` loop guard, the swap confession (ported
verbatim from osiris_stophook.py's own `_swap_confession`), and that `_fire_stage_a` is
actually called on every ALLOW path and never on a BLOCKED one (confessing "stopping" on a
path that ends up blocked would be a lie — the same law the original `_stage_a` names).
Pure/no-DB throughout: `_post` is monkeypatched, never a real HTTP round trip."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import scripts.osiris_hook as osiris_hook
from scripts.osiris_hook import (
    ALARM_PCT,
    HARD_ALARM_PCT,
    _cmd_stop,
    _fire_stage_a,
    _swap_confession,
)


def _write_transcript(path: Path, *entries: dict) -> None:
    path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")


# --- the ONE authority: ALARM_PCT/HARD_ALARM_PCT come from context_lens, never a second,
# independently-drifting pair of literals in this stdlib-only file ------------------------

def test_alarm_thresholds_match_the_one_authority() -> None:
    from src.orchestrator.context_lens import ALARM_PCT as _A
    from src.orchestrator.context_lens import HARD_ALARM_PCT as _H

    assert ALARM_PCT == _A == 80
    assert HARD_ALARM_PCT == _H == 95


# --- stop_hook_active: never loop on unsettleable mail -----------------------------------

def test_stop_hook_active_never_re_blocks(monkeypatch: Any) -> None:
    """The exact loop-guard the pre-port main() had (`if payload.get("stop_hook_active")`)
    and the new _cmd_stop was missing entirely before this fix: a session that already
    forced a continuation once this turn must always be allowed to stop, even with mail
    still waiting or a swap unconfessed."""
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(osiris_hook, "_post", lambda url, data, timeout=3: calls.append(data))
    assert _cmd_stop({"session_id": "abcdefgh", "stop_hook_active": True}) == 0
    assert len(calls) == 1
    assert calls[0]["phase"] == "stage_a"  # the courtesy ping still fires, best-effort


def test_no_session_id_is_a_clean_noop(monkeypatch: Any) -> None:
    def _unreachable(*a: Any, **k: Any) -> None:
        raise AssertionError("must never POST with no session_id")

    monkeypatch.setattr(osiris_hook, "_post", _unreachable)
    assert _cmd_stop({}) == 0


# --- the swap confession — pure, local, no DB, ONE per change ----------------------------

def _entry(model: str, content: str = "ok") -> dict[str, Any]:
    return {"type": "assistant", "isSidechain": False,
            "message": {"model": model, "content": content}}


def test_swap_confession_fires_on_a_real_mid_session_model_change(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    t = tmp_path / "t.jsonl"
    _write_transcript(t, _entry("claude-fable-5"), _entry("claude-opus-5"))
    hook = {"transcript_path": str(t), "session_id": "aaaahook1"}
    reason = _swap_confession(hook)
    assert reason is not None
    assert "claude-fable-5 -> claude-opus-5" in reason


def test_swap_confession_only_fires_once_per_pair(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    t = tmp_path / "t.jsonl"
    _write_transcript(t, _entry("claude-fable-5"), _entry("claude-opus-5"))
    hook = {"transcript_path": str(t), "session_id": "bbbbhook2"}
    assert _swap_confession(hook) is not None
    assert _swap_confession(hook) is None  # already confessed, same pair


def test_swap_confession_ignores_a_1m_variant_suffix(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    """Bracket variants ([1m]) are the SAME weights — never a swap."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    t = tmp_path / "t.jsonl"
    _write_transcript(t, _entry("claude-opus-5[1m]"), _entry("claude-opus-5"))
    hook = {"transcript_path": str(t), "session_id": "ccccchook3"}
    assert _swap_confession(hook) is None


def test_swap_confession_ignores_a_synthetic_tail_line(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    t = tmp_path / "t.jsonl"
    _write_transcript(t, _entry("claude-fable-5"), _entry("<synthetic>"))
    hook = {"transcript_path": str(t), "session_id": "ddddhook4"}
    assert _swap_confession(hook) is None


def test_swap_confession_none_with_no_change(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    t = tmp_path / "t.jsonl"
    _write_transcript(t, _entry("claude-sonnet-5"), _entry("claude-sonnet-5"))
    hook = {"transcript_path": str(t), "session_id": "eeeehook5"}
    assert _swap_confession(hook) is None


def test_swap_confession_none_when_transcript_missing_or_session_short() -> None:
    assert _swap_confession({}) is None
    assert _swap_confession({"transcript_path": "/nope", "session_id": "short"}) is None


# --- _cmd_stop's overall ordering: identity outranks mail, stage_a fires only on ALLOW ---

def test_swap_confession_blocks_before_any_mail_check(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    t = tmp_path / "t.jsonl"
    _write_transcript(t, _entry("claude-fable-5"), _entry("claude-opus-5"))

    def _unreachable(*a: Any, **k: Any) -> None:
        raise AssertionError("must never reach the mail check once a swap is confessed")

    monkeypatch.setattr(osiris_hook, "_post", _unreachable)
    rc = _cmd_stop({"session_id": "ffffhook6", "transcript_path": str(t)})
    assert rc == 1


def test_stage_a_never_fires_when_mail_blocks_the_stop(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []

    def _fake_post(url: str, data: dict[str, Any], timeout: int = 3) -> dict[str, Any] | None:
        calls.append(data)
        if data["phase"] == "deliverable":
            return {"result": {"n": 1, "senders": ["agent:x"], "window": None, "bands": {}}}
        raise AssertionError("must not reach stage_a on a blocked stop")

    monkeypatch.setattr(osiris_hook, "_post", _fake_post)
    rc = _cmd_stop({"session_id": "mailblock1", "cwd": "/x"})
    assert rc == 1
    assert [c["phase"] for c in calls] == ["deliverable"]  # never got to stage_a


def test_stage_a_fires_on_a_clean_allowed_stop(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []

    def _fake_post(url: str, data: dict[str, Any], timeout: int = 3) -> dict[str, Any] | None:
        calls.append(data)
        if data["phase"] == "deliverable":
            return {"result": {"n": 0, "senders": [], "window": 200000, "bands": {}}}
        return {"result": "ok"}

    monkeypatch.setattr(osiris_hook, "_post", _fake_post)
    rc = _cmd_stop({"session_id": "allowok1", "cwd": "/x"})  # no transcript -> pct is None
    assert rc == 0
    phases = [c["phase"] for c in calls]
    assert phases == ["deliverable", "stage_a"]


def test_fire_stage_a_posts_the_expected_shape(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def _fake_post(url: str, data: dict[str, Any], timeout: int = 3) -> None:
        captured["url"] = url
        captured["data"] = data
        captured["timeout"] = timeout

    monkeypatch.setattr(osiris_hook, "_post", _fake_post)
    _fire_stage_a({"session_id": "x"}, "x", "/cwd", pct=42)
    assert captured["data"]["phase"] == "stage_a"
    assert captured["data"]["pct"] == 42
    assert captured["data"]["payload"] == {"session_id": "x"}
    assert captured["url"] == osiris_hook._URLS["stop"]

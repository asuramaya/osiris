"""scripts/osiris_hook.py — the unified, stdlib-only hook client (dispatch 5441, the hook
migration). This file proves the pieces this session's parity audit found genuinely
missing before the port: the `stop_hook_active` loop guard, the swap confession (ported
verbatim from osiris_stophook.py's own `_swap_confession`), and that `_fire_stage_a` is
actually called on every ALLOW path and never on a BLOCKED one (confessing "stopping" on a
path that ends up blocked would be a lie — the same law the original `_stage_a` names).
Pure/no-DB throughout: `_post` is monkeypatched, never a real HTTP round trip."""
from __future__ import annotations

import json
import subprocess
import sys
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

_HOOK_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "osiris_hook.py"


def _run_anchor(payload: dict[str, object], env: dict[str, str] | None = None) -> dict[str, object]:
    out = subprocess.run([sys.executable, str(_HOOK_SCRIPT), "anchor"],
                         input=json.dumps(payload), capture_output=True, text=True,
                         check=False, env=env)
    return dict(json.loads(out.stdout)) if out.stdout.strip() else {}


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
    out = []
    monkeypatch.setattr("builtins.print", lambda s="", **kw: out.append(s))
    rc = _cmd_stop({"session_id": "ffffhook6", "transcript_path": str(t)})
    # Stop/SubagentStop hooks ONLY block on exit code 2 (dispatch 5599's own parity proof) —
    # the old, live osiris_stophook.py never used exit codes at all: it prints the JSON
    # decision to stdout and exits 0.
    assert rc == 0
    assert json.loads(out[0])["decision"] == "block"


def test_stage_a_never_fires_when_mail_blocks_the_stop(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []

    def _fake_post(url: str, data: dict[str, Any], timeout: int = 3) -> dict[str, Any] | None:
        calls.append(data)
        if data["phase"] == "deliverable":
            return {"result": {"n": 1, "senders": ["agent:x"], "window": None, "bands": {}}}
        raise AssertionError("must not reach stage_a on a blocked stop")

    monkeypatch.setattr(osiris_hook, "_post", _fake_post)
    out = []
    monkeypatch.setattr("builtins.print", lambda s="", **kw: out.append(s))
    rc = _cmd_stop({"session_id": "mailblock1", "cwd": "/x"})
    assert rc == 0
    decision = json.loads(out[0])
    assert decision["decision"] == "block"
    assert "agent:x" in decision["reason"]
    assert [c["phase"] for c in calls] == ["deliverable"]  # never got to stage_a


def test_pure_fyi_mail_never_blocks_the_stop(monkeypatch: Any) -> None:
    """obligation 6ad2f400 (msg 6029): the stop-block is reserved for grade='ask' (+
    ungraded) — an fyi that dispatch #151's own grammar already promises "never wakes
    anyone" must not still interrupt a live turn. n=2, both counted in bands['fyi'] ->
    blocking=0 -> stop proceeds to stage_a, same as a clean inbox."""
    calls: list[dict[str, Any]] = []

    def _fake_post(url: str, data: dict[str, Any], timeout: int = 3) -> dict[str, Any] | None:
        calls.append(data)
        if data["phase"] == "deliverable":
            return {"result": {"n": 2, "senders": ["agent:x"], "window": 200000,
                               "bands": {"ask": 0, "fyi": 2}}}
        return {"result": "ok"}

    monkeypatch.setattr(osiris_hook, "_post", _fake_post)
    rc = _cmd_stop({"session_id": "fyionly1", "cwd": "/x"})
    assert rc == 0
    assert [c["phase"] for c in calls] == ["deliverable", "stage_a"]  # never blocked


def test_an_ask_still_blocks_alongside_a_non_blocking_fyi(monkeypatch: Any) -> None:
    """A mixed inbox (1 ask, 1 fyi) still blocks — the ask carries the debt, the fyi
    rides along in the message text but is never counted toward it."""
    out = []

    def _fake_post(url: str, data: dict[str, Any], timeout: int = 3) -> dict[str, Any] | None:
        if data["phase"] == "deliverable":
            return {"result": {"n": 2, "senders": ["agent:x"], "window": None,
                               "bands": {"ask": 1, "fyi": 1}}}
        raise AssertionError("must not reach stage_a on a blocked stop")

    monkeypatch.setattr(osiris_hook, "_post", _fake_post)
    monkeypatch.setattr("builtins.print", lambda s="", **kw: out.append(s))
    rc = _cmd_stop({"session_id": "mixedask1", "cwd": "/x"})
    assert rc == 0
    decision = json.loads(out[0])
    assert decision["decision"] == "block"
    assert "1 deliverable" in decision["reason"]  # blocking count excludes the fyi
    assert "ask(s) something of you" in decision["reason"]


def test_ungraded_mail_still_blocks_never_guessed_as_fyi(monkeypatch: Any) -> None:
    """#151's own law: ungraded mail is never assumed to be fyi. n=1 with an empty bands
    dict (ungraded, unscored) must still block — blocking = n - bands.get('fyi', 0) = 1."""
    out = []

    def _fake_post(url: str, data: dict[str, Any], timeout: int = 3) -> dict[str, Any] | None:
        if data["phase"] == "deliverable":
            return {"result": {"n": 1, "senders": ["agent:y"], "window": None, "bands": {}}}
        raise AssertionError("must not reach stage_a on a blocked stop")

    monkeypatch.setattr(osiris_hook, "_post", _fake_post)
    monkeypatch.setattr("builtins.print", lambda s="", **kw: out.append(s))
    rc = _cmd_stop({"session_id": "ungraded1", "cwd": "/x"})
    assert rc == 0
    assert json.loads(out[0])["decision"] == "block"


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


# --- the offload gate: ONE AUTHORITY + the assumed-window safety law (dispatch 5599) ------
# Found in the retirement's own parity proof: the stub used here reimplemented context
# occupancy by hand (no `window_assumed` concept at all) instead of delegating to
# context_lens.last_usage/occupancy/window_for, the SAME authority osiris_stophook.py's own
# `_offload_pct` already uses. A hand-rolled guess ("200000 or 1000000") that never tracks
# whether it guessed can alarm exactly where the live script's own safety law — NEVER on an
# unknown or assumed window (the Anubis VII false-eulogy law) — forbids it.

def test_offload_pct_delegates_to_the_context_lens_authority(tmp_path: Path) -> None:
    t = tmp_path / "t.jsonl"
    t.write_text(json.dumps({
        "type": "assistant",
        "message": {"model": "claude-sonnet-5", "usage": {"input_tokens": 180000}},
    }) + "\n")
    pct, assumed = osiris_hook._offload_pct(
        {"transcript_path": str(t)}, window_hint=200000)
    assert pct == 90
    assert assumed is False


def test_offload_pct_never_asserts_a_window_it_assumed(tmp_path: Path) -> None:
    t = tmp_path / "t.jsonl"
    t.write_text(json.dumps({
        "type": "assistant",
        "message": {"model": "claude-sonnet-5", "usage": {"input_tokens": 50000}},
    }) + "\n")
    pct, assumed = osiris_hook._offload_pct({"transcript_path": str(t)}, window_hint=None)
    assert assumed is True  # no harness-stamped window and no [1m] tag: a guess, flagged


def test_offload_pct_none_with_no_transcript() -> None:
    assert osiris_hook._offload_pct({}, window_hint=200000) == (None, True)


def test_cmd_stop_never_blocks_on_an_assumed_window(monkeypatch: Any, tmp_path: Path) -> None:
    """Even a HIGH guessed pct must never trigger the offload block — assumed=True short-
    circuits before ALARM_PCT is even consulted, matching osiris_stophook.py's own law."""
    t = tmp_path / "t.jsonl"
    t.write_text(json.dumps({
        "type": "assistant",
        "message": {"model": "claude-sonnet-5", "usage": {"input_tokens": 190000}},
    }) + "\n")
    calls: list[dict[str, Any]] = []

    def _fake_post(url: str, data: dict[str, Any], timeout: int = 3) -> dict[str, Any] | None:
        calls.append(data)
        if data["phase"] == "deliverable":
            return {"result": {"n": 0, "senders": [], "window": None, "bands": {}}}
        if data["phase"] == "offload":
            raise AssertionError("must never reach the offload phase on an assumed window")
        return None  # stage_a: fire-and-forget, expected on this allow path

    monkeypatch.setattr(osiris_hook, "_post", _fake_post)
    out = []
    monkeypatch.setattr("builtins.print", lambda s="", **kw: out.append(s))
    rc = osiris_hook._cmd_stop({"session_id": "assumedwin1", "cwd": "/x",
                                "transcript_path": str(t)})
    assert rc == 0
    assert not out  # never printed a block decision
    assert [c["phase"] for c in calls] == ["deliverable", "stage_a"]


def test_missing_boxes_delegates_to_the_settle_authority() -> None:
    """ONE AUTHORITY (dispatch 5599): this used to carry its own friendlier name_map,
    drifting from src.orchestrator.settle.missing_boxes — the same pure function /settle's
    own confirm step and osiris_stophook.py's own offload verdict already share."""
    from src.orchestrator.settle import missing_boxes

    boxes = {"decisions": False, "open_threads": True, "charter_md": None}
    assert osiris_hook._missing_boxes(boxes) == missing_boxes(boxes)


def test_cmd_stop_offload_block_uses_the_decision_json_protocol_not_exit_1(
    monkeypatch: Any, tmp_path: Path,
) -> None:
    """Stop/SubagentStop hooks ONLY block on exit code 2 (dispatch 5599's own parity proof,
    verified against Claude Code's own hooks reference) — exit 1 is a non-blocking error,
    the stop proceeds anyway, silently. The proven, live osiris_stophook.py never used exit
    codes for this: it prints {"decision": "block", "reason": ...} to stdout and exits 0."""
    t = tmp_path / "t.jsonl"
    t.write_text(json.dumps({
        "type": "assistant",
        "message": {"model": "claude-sonnet-5", "usage": {"input_tokens": 180000}},
    }) + "\n")

    def _fake_post(url: str, data: dict[str, Any], timeout: int = 3) -> dict[str, Any] | None:
        if data["phase"] == "deliverable":
            return {"result": {"n": 0, "senders": [], "window": 200000, "bands": {}}}
        return {"result": {"decisions": False}}

    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    monkeypatch.setattr(osiris_hook, "_post", _fake_post)
    out = []
    monkeypatch.setattr("builtins.print", lambda s="", **kw: out.append(s))
    rc = osiris_hook._cmd_stop({"session_id": "exitcodecheck1", "cwd": "/x",
                                "transcript_path": str(t)})
    assert rc == 0
    decision = json.loads(out[0])
    assert decision["decision"] == "block"


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


# --- _cmd_statusline: the chrome rendering (dispatch 5441/5492 parity fix — /heartbeat has
# only ever returned raw counts, never a pre-rendered line; found live, the day of the flip,
# that this was ENTIRELY missing and would have rendered a blank status bar) --------------

def _heartbeat_result(**overrides: Any) -> dict[str, Any]:
    base = {
        "briefs": 0, "mail": 0, "dm": 0, "flight": 0, "souls": 3, "wakes": 2,
        "owed": 0, "owed_here": 0, "sick": [], "spend": [0.0, 0.0, 0],
        "resolved_project": "osiris", "resolved_intent": "claude-sonnet-5",
        "resolved_seat_handle": "Seshat",
    }
    base.update(overrides)
    return base


def test_statusline_renders_the_full_line_on_a_clean_heartbeat(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        osiris_hook, "_post", lambda url, data, timeout=3: {"result": _heartbeat_result()})
    out = []
    monkeypatch.setattr("builtins.print", lambda s="": out.append(s))
    rc = osiris_hook._cmd_statusline({
        "workspace": {"current_dir": "/repo"}, "model": {"id": "claude-sonnet-5"},
        "session_id": "abcd1234",
    })
    assert rc == 0
    assert len(out) >= 1
    assert "Seshat" in out[0] and "osiris" in out[0]
    assert "fleet 3●" in out[0]
    assert "wakes 2/h" in out[0]
    assert "mail 0" in out[0]
    assert "owe 0" in out[0]


def test_statusline_route_failure_reports_the_probe_not_a_diagnosis(
    monkeypatch: Any, tmp_path: Path,
) -> None:
    """CONTRACT DELIBERATELY CHANGED, 2026-09-01. This test previously asserted
    `graph unreachable` — the exact string that sent the operator after osiris-pg while
    that container had been up 27 hours and the real cause was an osiris-mcp restart from
    his own deploy. A failed POST is evidence about the PROBE and nothing else, so the
    render now says so. Kept as a test (not deleted) because the fallback path still must
    exist, exit 0, and never invent counts.

    Also now ISOLATES THE CACHE: the original read the real ~/.osiris/statusline-cache,
    so once the cache landed this test's outcome depended on whatever other agents had
    written — a test that passes or fails on the machine's state, not the code's."""
    monkeypatch.setattr(osiris_hook, "_statusline_cache_path",
                        lambda project: tmp_path / "empty" / f"{project}.json")
    monkeypatch.setattr(osiris_hook, "_post", lambda url, data, timeout=3: None)
    out = []
    monkeypatch.setattr("builtins.print", lambda s="": out.append(s))
    rc = osiris_hook._cmd_statusline({"workspace": {"current_dir": "/repo"}})
    assert rc == 0
    assert "no answer" in out[0]
    assert "unreachable" not in out[0]


def test_statusline_marks_owed_here_red_when_nonzero(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        osiris_hook, "_post",
        lambda url, data, timeout=3: {"result": _heartbeat_result(owed_here=3)})
    out = []
    monkeypatch.setattr("builtins.print", lambda s="": out.append(s))
    osiris_hook._cmd_statusline({"workspace": {"current_dir": "/repo"}})
    assert "owe 3" in out[0]
    assert osiris_hook._RED in out[0]


def test_statusline_shows_a_dm_doorbell_even_with_zero_plain_mail(monkeypatch: Any) -> None:
    """thread from the old script: `dm` must light the segment by itself — mail 0 + flight 0
    + 7 DMs waiting must never render as a dim 'mail 0'."""
    monkeypatch.setattr(
        osiris_hook, "_post",
        lambda url, data, timeout=3: {"result": _heartbeat_result(dm=7)})
    out = []
    monkeypatch.setattr("builtins.print", lambda s="": out.append(s))
    osiris_hook._cmd_statusline({"workspace": {"current_dir": "/repo"}})
    assert "✉7" in out[0]
    assert f"{osiris_hook._DIM}mail 0{osiris_hook._RESET}" not in out[0]


def test_statusline_model_matches_declared_intent_renders_green(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        osiris_hook, "_post", lambda url, data, timeout=3: {"result": _heartbeat_result(
            resolved_intent="claude-sonnet-5")})
    out = []
    monkeypatch.setattr("builtins.print", lambda s="": out.append(s))
    osiris_hook._cmd_statusline({
        "workspace": {"current_dir": "/repo"}, "model": {"id": "claude-sonnet-5"}})
    assert len(out) == 2
    assert f"{osiris_hook._GREEN}sonnet-5{osiris_hook._RESET}" in out[1]


def test_statusline_model_mismatch_with_no_operator_swap_renders_red(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        osiris_hook, "_post", lambda url, data, timeout=3: {"result": _heartbeat_result(
            resolved_intent="claude-opus-5")})
    monkeypatch.setattr(osiris_hook, "_operator_swap", lambda t, s, m: False)
    out = []
    monkeypatch.setattr("builtins.print", lambda s="": out.append(s))
    osiris_hook._cmd_statusline({
        "workspace": {"current_dir": "/repo"}, "model": {"id": "claude-sonnet-5"}})
    assert "⚠ sonnet-5 (declared: opus-5)" in out[1]
    assert osiris_hook._RED in out[1]


def test_statusline_model_mismatch_confirmed_as_operator_choice_renders_amber(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        osiris_hook, "_post", lambda url, data, timeout=3: {"result": _heartbeat_result(
            resolved_intent="claude-opus-5")})
    monkeypatch.setattr(osiris_hook, "_operator_swap", lambda t, s, m: True)
    out = []
    monkeypatch.setattr("builtins.print", lambda s="": out.append(s))
    osiris_hook._cmd_statusline({
        "workspace": {"current_dir": "/repo"}, "model": {"id": "claude-sonnet-5"}})
    assert "your /model" in out[1]
    assert osiris_hook._AMBER in out[1]


def test_statusline_no_declared_intent_is_silent_not_an_alarm(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        osiris_hook, "_post", lambda url, data, timeout=3: {"result": _heartbeat_result(
            resolved_intent=None)})
    out = []
    monkeypatch.setattr("builtins.print", lambda s="": out.append(s))
    osiris_hook._cmd_statusline({
        "workspace": {"current_dir": "/repo"}, "model": {"id": "claude-sonnet-5"},
        "context_window": {"used_percentage": 40}})
    # only the ctx segment on the vitals line, no model warning at all
    assert len(out) == 2
    assert "sonnet-5" not in out[1]
    assert "⚠" not in out[1]


def test_statusline_ctx_pct_from_payload_colors_by_threshold(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        osiris_hook, "_post", lambda url, data, timeout=3: {"result": _heartbeat_result()})
    out = []
    monkeypatch.setattr("builtins.print", lambda s="": out.append(s))
    osiris_hook._cmd_statusline({
        "workspace": {"current_dir": "/repo"},
        "context_window": {"used_percentage": 90, "context_window_size": 200000}})
    assert f"{osiris_hook._RED}ctx 90%{osiris_hook._RESET}" in out[1]


def test_operator_swap_reads_the_transcript_command_record(tmp_path: Any) -> None:
    t = tmp_path / "t.jsonl"
    t.write_text(json.dumps({
        "type": "user", "isSidechain": False,
        "message": {"content": "<command-name>/model</command-name>"},
    }) + "\n")
    assert osiris_hook._operator_swap(str(t), "swapop01", "claude-opus-5") is True


def test_operator_swap_false_with_no_command_in_transcript(tmp_path: Any) -> None:
    t = tmp_path / "t.jsonl"
    t.write_text(json.dumps({"type": "assistant", "message": {"content": "ok"}}) + "\n")
    assert osiris_hook._operator_swap(str(t), "swapop02", "claude-opus-5") is False


# --- render_whisper: ported verbatim from osiris_whisper.py (dispatch 5441/5492 parity
# fix — /automount has only ever returned automount()'s own raw structured output, never a
# rendered "intro"/"message" string; found live, the same day as the flip). Same test
# suite tests/test_whisper.py already proved against the pre-port function. -------------

def _whisper_base(**extra: Any) -> dict[str, Any]:
    out = {"agent": "agent:abc12345", "project": "osiris", "model": "claude-sonnet-5",
           "resolved": True, "mail": 0, "mail_asks": 0, "desk": 0, "seat": None, "thin": False,
           "job_dir": "/home/asuramaya/.claude/jobs/abc12345"}
    out.update(extra)
    return out


def test_render_whisper_drops_the_top_of_wall_reprint() -> None:
    """Context-bloat diet (decision e1fbde18, Thoth msg 6884): orient() already shows the
    wall, so the whisper no longer re-prints it — even when the payload carries
    obligations, the key is simply ignored."""
    out = _whisper_base(obligations=[
        {"id": "12a58447", "summary": "HANDOFF — Thoth L to LI", "kind": "obligation"},
        {"id": "9dc3ce8b", "summary": "READ-SIDE ADOPTION OF THE VISIT CLASS"},
    ])
    text = osiris_hook.render_whisper(out, cwd="/home/asuramaya/.osiris/seats/seshat", env_job="")
    assert "Top of your project's wall" not in text
    assert "[12a58447]" not in text


def test_render_whisper_hedges_an_unconfirmed_swap() -> None:
    out = _whisper_base(swap="claude-fable-5 → claude-haiku-4-5")
    text = osiris_hook.render_whisper(out, cwd="/x", env_job="")
    assert "Possible model seam" in text and "unconfirmed" in text


def test_render_whisper_speaks_plainly_on_a_witnessed_operator_swap() -> None:
    out = _whisper_base(swap="claude-fable-5 → claude-opus-4-8 [operator /model]")
    text = osiris_hook.render_whisper(out, cwd="/x", env_job="")
    assert "the OPERATOR's own deliberate choice" in text
    assert "Possible model seam" not in text


def test_render_whisper_succession_pointer_by_query() -> None:
    out = _whisper_base(minted="agent:ad1a1cb0-g40-xiii", succession={
        "thread_id": "12a58447", "thread_summary": "HANDOFF — Thoth L to LI"})
    text = osiris_hook.render_whisper(out, cwd="/x", env_job="")
    assert "MINTED as this lineage's successor" in text
    assert "[12a58447] HANDOFF — Thoth L to LI" in text


def test_render_whisper_succession_falls_back_with_no_query_result() -> None:
    out = _whisper_base(minted="agent:ad1a1cb0-g40-xiii")
    text = osiris_hook.render_whisper(out, cwd="/x", env_job="")
    assert "check your project's open threads for the succession note" in text


def test_render_whisper_identity_anchor_unconditional_no_mint_needed() -> None:
    out = _whisper_base(identity_anchor={
        "charter_file": "/home/asuramaya/.osiris/seats/thoth/charter.md"})
    text = osiris_hook.render_whisper(out, cwd="/x", env_job="")
    assert "MINTED" not in text
    assert "your charter file is /home/asuramaya/.osiris/seats/thoth/charter.md" in text


def test_render_whisper_names_a_missing_charter_loudly() -> None:
    """Thread e2326ab7: the same fact settle()'s terminal box checks, said at first
    breath instead — must render with the ⚠ marker every other loud whisper line uses."""
    out = _whisper_base(charter_missing="UNDECLARED — call charter(repos=[...]) naming "
                                         "the repos you govern before writing anywhere")
    text = osiris_hook.render_whisper(out, cwd="/x", env_job="")
    assert "⚠ CHARTER: UNDECLARED — call charter(repos=[...])" in text


def test_render_whisper_omits_charter_line_when_chartered() -> None:
    text = osiris_hook.render_whisper(_whisper_base(), cwd="/x", env_job="")
    assert "CHARTER:" not in text


def test_render_whisper_already_mounted_when_env_matches_anchor() -> None:
    out = _whisper_base(job_dir="/home/asuramaya/.claude/jobs/abc12345")
    text = osiris_hook.render_whisper(
        out, cwd="/x", env_job="/home/asuramaya/.claude/jobs/abc12345")
    assert "ALREADY MOUNTED" in text


def test_render_whisper_mail_count_names_asks() -> None:
    out = _whisper_base(mail=3, mail_asks=1)
    text = osiris_hook.render_whisper(out, cwd="/x", env_job="")
    assert "3 unread fleet messages" in text
    # ported verbatim, pre-existing pluralization quirk and all (osiris_whisper.py's own
    # `'s' if asks == 1 else ''` is inverted from what "asks == 1" suggests) — out of scope
    # for this parity fix to silently correct.
    assert "1 asks something of you" in text


def test_render_whisper_anonymous_offers_the_claim() -> None:
    text = osiris_hook.render_whisper(_whisper_base(seat=None), cwd="/x", env_job="")
    assert "You are ANONYMOUS" in text


def test_render_whisper_named_seat_names_the_dm_address() -> None:
    out = _whisper_base(seat="Seshat XXXVIII")
    text = osiris_hook.render_whisper(out, cwd="/x", env_job="")
    assert "You answer to the name Seshat XXXVIII" in text
    assert "send(to_agent='Seshat')" in text


def test_render_whisper_collapses_to_one_orient_pointer_naming_get_status_first() -> None:
    """Context-bloat diet (decision e1fbde18, Thoth msg 6884): the whisper used to point
    at orient() up to 6 separate times across its own optional bits; now exactly one
    remains, in the closing ritual line, and it names get_status() — the cheap door — before
    orient() — the deep one — so a fresh session doesn't default to the 59K-char briefing."""
    out = _whisper_base(minted="agent:ad1a1cb0-g40-xiii", away={"threads": ["t1"]})
    text = osiris_hook.render_whisper(out, cwd="/x", env_job="")
    assert text.count("orient()") == 1
    assert "get_status() for a quick check, orient() for the deep briefing" in text


def test_render_whisper_drops_the_durable_anchor_mechanics_paragraph() -> None:
    """Same diet: the mount-again mechanics paragraph moves to orient()'s own output,
    read on demand rather than pushed on every boot."""
    out = _whisper_base(job_dir="/home/asuramaya/.claude/jobs/abc12345")
    text = osiris_hook.render_whisper(
        out, cwd="/x", env_job="/home/asuramaya/.claude/jobs/abc12345")
    assert "YOUR DURABLE ANCHOR" not in text


# --- _cmd_whisper: the request body must carry the env-derived attach/bridge/spawn fields
# the harness's own stdin JSON never provides (found live, the SAME day as the flip — a
# structural continuity gap, not merely a missing banner) ------------------------------

def test_cmd_whisper_carries_the_attach_ceremony_env_vars(monkeypatch: Any) -> None:
    monkeypatch.setenv("OSIRIS_SEAT_ID", "seat:abc123")
    monkeypatch.setenv("OSIRIS_ATTACH_TOKEN", "tok-xyz")
    monkeypatch.setenv("OSIRIS_SPAWNED_BY", "agent:parent001")
    monkeypatch.setenv("OSIRIS_SPAWN_TYPE", "subagent")
    monkeypatch.setenv("CLAUDE_CODE_BRIDGE_SESSION_ID", "bridge-001")
    captured: dict[str, Any] = {}

    def _fake_post(url: str, data: dict[str, Any], timeout: int = 3) -> dict[str, Any]:
        captured.update(data)
        return {"agent": "agent:x", "seat": None, "mail": 0}

    monkeypatch.setattr(osiris_hook, "_post", _fake_post)
    monkeypatch.setattr("builtins.print", lambda s="", **kw: None)
    osiris_hook._cmd_whisper({"session_id": "s1", "cwd": "/repo"})
    assert captured["seat_id"] == "seat:abc123"
    assert captured["attach_token"] == "tok-xyz"
    assert captured["spawned_by"] == "agent:parent001"
    assert captured["spawn_type"] == "subagent"
    assert captured["bridge_session_id"] == "bridge-001"


def test_cmd_whisper_no_session_id_or_cwd_is_a_clean_noop(monkeypatch: Any) -> None:
    def _unreachable(*a: Any, **k: Any) -> None:
        raise AssertionError("must never POST with no session_id/cwd")

    monkeypatch.setattr(osiris_hook, "_post", _unreachable)
    assert osiris_hook._cmd_whisper({}) == 0


def test_cmd_whisper_prints_the_rendered_banner(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        osiris_hook, "_post",
        lambda url, data, timeout=3: {"agent": "agent:x", "seat": None, "mail": 0,
                                      "job_dir": "/j/x"})
    out = []
    monkeypatch.setattr("builtins.print", lambda s="", **kw: (None if kw else out.append(s)))
    rc = osiris_hook._cmd_whisper({"session_id": "s1", "cwd": "/repo"})
    assert rc == 0
    assert len(out) == 1
    assert "◈ OSIRIS" in out[0]


def test_cmd_whisper_route_unreachable_prints_the_manual_hint(monkeypatch: Any) -> None:
    monkeypatch.setattr(osiris_hook, "_post", lambda url, data, timeout=3: None)
    out = []
    monkeypatch.setattr("builtins.print", lambda s="", **kw: (None if kw else out.append(s)))
    osiris_hook._cmd_whisper({"session_id": "s1", "cwd": "/repo"})
    assert "unreachable" in out[0]
    assert "mount(cwd='/repo')" in out[0]


def test_cmd_whisper_logs_the_connected_diagnostic_to_stderr(monkeypatch: Any, capsys: Any) -> None:
    monkeypatch.setattr(
        osiris_hook, "_post",
        lambda url, data, timeout=3: {"agent": "agent:x", "seat": None, "mail": 0,
                                      "job_dir": "/j/x"})
    osiris_hook._cmd_whisper({"session_id": "s1", "cwd": "/repo"})
    assert "connected" in capsys.readouterr().err


# --- session-end/precompact/spawn: stderr diagnostic parity (dispatch 5599) ----------------
# The retirement's own parity proof (run both paths against a real capture server, diff the
# wire output, never a read-and-conclude claim) found the request bodies matched byte-for-
# byte, but the retired scripts always logged "posted {url} — connected/failed" to stderr on
# every attempt and the ported versions were silent — no replacement, unlike whisper's own
# user-visible fallback message on failure. Restored via the shared `_log_post` helper.

def test_session_end_logs_the_connected_diagnostic(monkeypatch: Any) -> None:
    monkeypatch.setattr(osiris_hook, "_post", lambda url, data, timeout=3: {"ok": True})
    out = []
    monkeypatch.setattr("sys.stderr", type("F", (), {"write": lambda self, s: out.append(s),
                                                       "flush": lambda self: None})())
    osiris_hook._cmd_session_end({"session_id": "sid-x"})
    assert "connected" in "".join(out)


def test_session_end_logs_the_failed_diagnostic(monkeypatch: Any) -> None:
    monkeypatch.setattr(osiris_hook, "_post", lambda url, data, timeout=3: None)
    out = []
    monkeypatch.setattr("sys.stderr", type("F", (), {"write": lambda self, s: out.append(s),
                                                       "flush": lambda self: None})())
    osiris_hook._cmd_session_end({"session_id": "sid-x"})
    assert "failed" in "".join(out)


def test_precompact_logs_the_diagnostic_only_for_absolute_transcripts(monkeypatch: Any) -> None:
    monkeypatch.setattr(osiris_hook, "_post", lambda url, data, timeout=3: {"ok": True})
    out = []
    monkeypatch.setattr("sys.stderr", type("F", (), {"write": lambda self, s: out.append(s),
                                                       "flush": lambda self: None})())
    osiris_hook._cmd_precompact({"transcript_path": "relative.jsonl"})
    assert not out
    osiris_hook._cmd_precompact({"transcript_path": "/abs/path.jsonl"})
    assert "connected" in "".join(out)


def test_spawn_logs_the_diagnostic_only_with_an_agent_id(monkeypatch: Any) -> None:
    monkeypatch.setattr(osiris_hook, "_post", lambda url, data, timeout=3: {"ok": True})
    out = []
    monkeypatch.setattr("sys.stderr", type("F", (), {"write": lambda self, s: out.append(s),
                                                       "flush": lambda self: None})())
    osiris_hook._cmd_spawn({"session_id": "s", "agent_id": ""})
    assert not out
    osiris_hook._cmd_spawn({"session_id": "s", "agent_id": "agent-x"})
    assert "connected" in "".join(out)


def _fake_stdout_print(out: list[str]) -> Any:
    """Captures only STDOUT prints (the hookSpecificOutput JSON) — _log_post's own
    diagnostic always targets file=sys.stderr and must not be conflated with it."""
    def _p(s: str = "", **kw: Any) -> None:
        if kw.get("file") is None:
            out.append(s)
    return _p


def test_spawn_start_prints_the_fork_orientation_as_additional_context(
    monkeypatch: Any,
) -> None:
    """obligation 706c27dc's second half (msg 6034): SubagentStart is NOT in Claude Code's
    plain-stdout-as-context exception list, so a fork only ever sees this if it's shaped as
    hookSpecificOutput.additionalContext JSON — the one place this client emits that shape."""
    monkeypatch.setattr(
        osiris_hook, "_post",
        lambda url, data, timeout=3: {"spawn": "agent:forkabcd", "of": "agent:parent",
                                      "fork_orientation": "you are a FORK — Khnum XLII.15"})
    out: list[str] = []
    monkeypatch.setattr("builtins.print", _fake_stdout_print(out))
    rc = osiris_hook._cmd_spawn({"session_id": "s", "agent_id": "agent-forkabcd",
                                 "agent_type": "fork", "hook_event_name": "SubagentStart"})
    assert rc == 0
    payload = json.loads(out[0])
    assert payload["hookSpecificOutput"]["hookEventName"] == "SubagentStart"
    assert "you are a FORK" in payload["hookSpecificOutput"]["additionalContext"]


def test_spawn_stop_never_prints_orientation(monkeypatch: Any) -> None:
    """Stop has nothing left to orient — even if the server echoed a fork_orientation, the
    Stop phase must never print it (it would be a stray, unexplained stdout line)."""
    monkeypatch.setattr(
        osiris_hook, "_post",
        lambda url, data, timeout=3: {"spawn": "agent:forkabcd", "of": "agent:parent",
                                      "fork_orientation": "you are a FORK"})
    out: list[str] = []
    monkeypatch.setattr("builtins.print", _fake_stdout_print(out))
    rc = osiris_hook._cmd_spawn({"session_id": "s", "agent_id": "agent-forkabcd",
                                 "agent_type": "fork", "hook_event_name": "SubagentStop"})
    assert rc == 0
    assert out == []


def test_spawn_start_with_no_orientation_prints_nothing(monkeypatch: Any) -> None:
    """An ordinary (non-fork) subagent's start carries no fork_orientation — silent, as
    before this fix."""
    monkeypatch.setattr(
        osiris_hook, "_post",
        lambda url, data, timeout=3: {"spawn": "agent:plainxyz", "of": "agent:parent"})
    out: list[str] = []
    monkeypatch.setattr("builtins.print", _fake_stdout_print(out))
    rc = osiris_hook._cmd_spawn({"session_id": "s", "agent_id": "agent-plainxyz",
                                 "agent_type": "general-purpose",
                                 "hook_event_name": "SubagentStart"})
    assert rc == 0
    assert out == []


# --- anchor: the CHANNEL parity audit (dispatch 5547) --------------------------------------
# The stub that shipped at the hook migration only stamped a raw session_id onto EVERY
# osiris call, ungated, and echoed the whole hook payload back in the WRONG envelope (no
# hookSpecificOutput wrapper) — meaning even that stamp would never have reached a real
# tool call. Ported verbatim from osiris_mount_anchor.py: job_dir derivation, ANCHOR_AWARE/
# SPAWN_AWARE gating, the MAIN-session strip, and the CLAUDE_CODE_BRIDGE_SESSION_ID door —
# an OS-environment read the stub dropped entirely, the same CHANNEL-class gap already found
# in whisper and statusline.

def test_anchor_the_hook_and_the_SERVER_stay_in_lockstep() -> None:
    import inspect

    import src.mcp_server as srv
    from scripts.osiris_hook import _ANCHOR_AWARE

    for name in sorted(_ANCHOR_AWARE):
        fn = getattr(srv, name.removeprefix("mcp__osiris__"), None)
        assert fn is not None, f"{name} is stamped by the hook but does not exist on the server"
        assert "session_anchor" in inspect.signature(fn).parameters, (
            f"{name} is in _ANCHOR_AWARE but its signature does not accept `session_anchor`")


def test_anchor_SPAWN_AWARE_stays_in_lockstep_with_the_server() -> None:
    import inspect

    import src.mcp_server as srv
    from scripts.osiris_hook import _SPAWN_AWARE

    for name in sorted(_SPAWN_AWARE):
        fn = getattr(srv, name.removeprefix("mcp__osiris__"), None)
        assert fn is not None, f"{name} is stamped by the hook but does not exist on the server"
        assert "subagent_id" in inspect.signature(fn).parameters, (
            f"{name} is in _SPAWN_AWARE but its signature does not accept `subagent_id`")


def test_anchor_rides_every_call_not_only_mount() -> None:
    for tool in ("inbox", "send", "orient", "record_decision", "open_thread", "resolve_thread"):
        out = _run_anchor({"tool_name": f"mcp__osiris__{tool}",
                           "session_id": "513aa520-6f1e-4807-948d-2e0820af1574",
                           "tool_input": {}})
        anchor = out["hookSpecificOutput"]["updatedInput"]["session_anchor"]  # type: ignore[index]
        assert str(anchor).endswith("/jobs/513aa520"), f"{tool} rode without its anchor"


def test_anchor_a_spawn_never_gets_a_seat_anchor() -> None:
    out = _run_anchor({"tool_name": "mcp__osiris__open_thread",
                       "session_id": "513aa520-6f1e-4807-948d-2e0820af1574",
                       "agent_id": "agent-abc", "agent_type": "Explore",
                       "tool_input": {}})
    ti = out["hookSpecificOutput"]["updatedInput"]  # type: ignore[index]
    assert "session_anchor" not in ti
    assert ti["subagent_id"] == "agent-abc"  # type: ignore[index]


def test_anchor_main_session_strips_a_stale_spawn_stamp() -> None:
    out = _run_anchor({"tool_name": "mcp__osiris__open_thread",
                       "session_id": "513aa520-6f1e-4807-948d-2e0820af1574",
                       "tool_input": {"subagent_id": "agent-stale"}})
    ti = out["hookSpecificOutput"]["updatedInput"]  # type: ignore[index]
    assert "subagent_id" not in ti


def test_anchor_an_agents_own_anchor_is_never_overwritten() -> None:
    out = _run_anchor({"tool_name": "mcp__osiris__orient",
                       "session_id": "513aa520-6f1e-4807-948d-2e0820af1574",
                       "tool_input": {"session_anchor": "/home/x/.claude/jobs/deadbeef"}})
    if out:
        ti = out["hookSpecificOutput"]["updatedInput"]  # type: ignore[index]
        assert ti["session_anchor"] == "/home/x/.claude/jobs/deadbeef"  # type: ignore[index]


def test_anchor_mount_gets_transcript_path_and_bridge_id_stamped(monkeypatch: Any) -> None:
    import os as _os

    env = dict(_os.environ)
    env["CLAUDE_CODE_BRIDGE_SESSION_ID"] = "bridge-xyz"
    out = _run_anchor({"tool_name": "mcp__osiris__mount",
                       "session_id": "513aa520-6f1e-4807-948d-2e0820af1574",
                       "transcript_path": "/home/x/.claude/projects/p/other-sid.jsonl",
                       "tool_input": {}}, env=env)
    ti = out["hookSpecificOutput"]["updatedInput"]  # type: ignore[index]
    assert ti["transcript_path"] == "/home/x/.claude/projects/p/other-sid.jsonl"
    assert ti["bridge_session_id"] == "bridge-xyz"


def test_anchor_a_non_osiris_tool_is_never_touched() -> None:
    assert _run_anchor({"tool_name": "Bash", "session_id": "513aa520-aaaa",
                        "tool_input": {"command": "ls"}}) == {}


def test_anchor_output_uses_the_hookSpecificOutput_envelope_only_when_changed() -> None:
    """The stub this fix replaces echoed the WHOLE hook payload unconditionally, in a shape
    the harness's PreToolUse contract does not recognize — no `hookSpecificOutput.updatedInput`
    means the harness never applies the edit at all, silently. Unchanged input must print
    NOTHING (the harness leaves the call alone), never a no-op envelope."""
    import os as _os

    env = dict(_os.environ)
    env.pop("CLAUDE_CODE_BRIDGE_SESSION_ID", None)  # this session's own env must not leak in
    out = subprocess.run([sys.executable, str(_HOOK_SCRIPT), "anchor"],
                         input=json.dumps({"tool_name": "mcp__osiris__mount",
                                          "session_id": "",
                                          "tool_input": {}}),
                         capture_output=True, text=True, check=False, env=env)
    assert out.stdout.strip() == ""


# ---------------------------------------------------------------------------
# THE STATUSLINE MUST SELF-HEAL ACROSS A RESTART (operator, 2026-09-01: "everything has
# to be self-healing over restarts and such"). It made ONE POST on a 1-second budget with
# no retry and no cache, so any miss — every `osiris deploy` restarts osiris-mcp — painted
# the bar `graph unreachable`, naming a subsystem that was up the whole time. These prove
# the three states stay APART: LIVE, STALE (cached, marked), SILENT (nothing known).
# ---------------------------------------------------------------------------

_COUNTS = {"briefs": 3, "mail": 1, "dm": 0, "flight": 0, "souls": 7, "wakes": 4,
           "owed_here": 2, "sick": [], "spend": [0.0, 0.0, 0], "resolved_project": "osiris"}


def _statusline(
    monkeypatch: Any, tmp_path: Path, *, answer: Any, project_hint: str | None = "testproj",
) -> str:
    """Render one statusline with `_post` stubbed and the cache redirected into tmp.

    `project_hint` stands in for `read_project_label(cwd)`'s real filesystem pin-climb —
    defaulted to a resolvable name so these tests exercise the cache's ordinary, keyed
    behavior; pass `None` to exercise the unresolved-project path instead (no `.osiris`
    pin anywhere up the tree, e.g. a bare seat-office container or an unpinned repo)."""
    monkeypatch.setattr(osiris_hook, "_statusline_cache_path",
                        lambda project: tmp_path / f"{project}.json")
    monkeypatch.setattr(osiris_hook, "_post",
                        lambda *a, **k: answer() if callable(answer) else answer)
    import src.orchestrator.agents as agents_mod
    monkeypatch.setattr(agents_mod, "read_project_label", lambda cwd: project_hint)
    import contextlib
    import io
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        osiris_hook._cmd_statusline({"workspace": {"current_dir": "/tmp/x"},
                                     "model": {"id": "claude-opus-5"}})
    return buf.getvalue()


def test_statusline_live_answer_renders_counts_and_no_stale_marker(
    monkeypatch: Any, tmp_path: Path,
) -> None:
    out = _statusline(monkeypatch, tmp_path, answer={"result": _COUNTS})
    assert "fleet 7" in out and "owe 2" in out
    assert "ago" not in out                # a live answer is never marked stale
    assert "graph" not in out              # and never carries a failure word


def test_statusline_falls_back_to_cache_and_marks_it_rather_than_crying_unreachable(
    monkeypatch: Any, tmp_path: Path,
) -> None:
    """THE OPERATOR'S ACTUAL FAILURE. A miss must render the last good counts with a
    marker — never `graph unreachable`, which blamed Postgres while it was up 27 hours."""
    _statusline(monkeypatch, tmp_path, answer={"result": _COUNTS})   # warm the cache
    out = _statusline(monkeypatch, tmp_path, answer=None)            # now the probe misses
    assert "fleet 7" in out and "owe 2" in out    # last-known-good survives the miss
    assert "ago" in out                           # ...and is HONESTLY marked as cached
    assert "unreachable" not in out


def test_statusline_marks_a_zero_second_old_cache_as_stale_too(
    monkeypatch: Any, tmp_path: Path,
) -> None:
    """Gating the marker on the AGE (`if stale_age`) instead of on the SOURCE made a
    cache written under a second ago render IDENTICALLY to a live answer — collapsing the
    two states in the very line meant to keep them apart. Caught in live testing, not review."""
    _statusline(monkeypatch, tmp_path, answer={"result": _COUNTS})
    out = _statusline(monkeypatch, tmp_path, answer=None)
    assert "0s ago" in out   # age really is 0 here; the marker must still appear


def test_statusline_never_caches_the_callers_own_identity(
    monkeypatch: Any, tmp_path: Path,
) -> None:
    """The cache file is SHARED by every agent on the project. Caching caller-scoped fields
    let one seat's bar wear another's name (seen live: a probe from the osiris tree rendered
    `imhotep·osiris` off Imhotep's row). Counts are the project's; identity is not."""
    _statusline(monkeypatch, tmp_path,
                answer={"result": {**_COUNTS, "resolved_seat_handle": "imhotep",
                                   "resolved_intent": "claude-opus-5"}})
    # Find the file by GLOB, not by guessing its name: the cache key comes from
    # read_project_label(cwd), a real filesystem pin-climb, so hard-coding "osiris.json"
    # asserted a resolution this test never controls.
    written = list(tmp_path.glob("*.json"))
    assert len(written) == 1, f"expected exactly one cache file, got {written}"
    cached = json.loads(written[0].read_text())["payload"]
    assert "resolved_seat_handle" not in cached
    assert "resolved_intent" not in cached
    assert cached["souls"] == 7          # the shared counts DO survive


def test_statusline_with_no_answer_and_no_cache_says_only_what_it_knows(
    monkeypatch: Any, tmp_path: Path,
) -> None:
    """SILENT: never invent zeroed counts (a confident `owe 0` is worse than silence), and
    never name a subsystem this probe cannot see. It knows one thing: it got no answer."""
    out = _statusline(monkeypatch, tmp_path, answer=None)
    assert "no answer" in out
    assert "unreachable" not in out
    assert "owe 0" not in out            # absence of data is NOT a count of zero


def test_statusline_never_shares_the_ignorance_bucket_across_sessions(
    monkeypatch: Any, tmp_path: Path,
) -> None:
    """THE LIVE SPECIMEN (Thoth, msg 6655/6654): two DIFFERENT sessions whose cwd carries no
    resolvable `.osiris` pin (a bare seat-office container; an unpinned repo like lilguy) both
    got `project_hint is None`, both fell into `cache_key = project_hint or "_"` — the SAME
    file — and one session's counts (not just its label) leaked into the other's bar on the
    next fallback read. `"_"` is not a project; it is "we could not tell," and a bucket keyed
    on ignorance has no shared identity to hold. Fix: never write, and never read, that bucket."""
    # Session A: unresolved project, live answer — must NOT write any cache file at all.
    out_a = _statusline(monkeypatch, tmp_path, answer={"result": _COUNTS}, project_hint=None)
    assert "fleet 7" in out_a
    assert list(tmp_path.glob("*.json")) == []

    # Session B: also unresolved, but its own probe MISSES — it must render SILENT, never
    # borrow session A's counts (there is nothing to borrow: A wrote nothing above, but the
    # law holds even if some other unresolved caller had — the bucket is never read either).
    out_b = _statusline(monkeypatch, tmp_path, answer=None, project_hint=None)
    assert "no answer" in out_b
    assert "fleet 7" not in out_b
    assert "ago" not in out_b


def test_statusline_resolved_project_still_caches_normally(
    monkeypatch: Any, tmp_path: Path,
) -> None:
    """The fix is scoped to the unresolved/`None` case only — a real project keeps its
    ordinary shared, keyed cache (this is what every other project-scoped test here relies
    on continuing to work)."""
    _statusline(monkeypatch, tmp_path, answer={"result": _COUNTS}, project_hint="realproj")
    written = list(tmp_path.glob("*.json"))
    assert len(written) == 1
    assert written[0].name == "realproj.json"


def test_statusline_retries_once_before_giving_up(monkeypatch: Any, tmp_path: Path) -> None:
    """A restarting server is back within seconds; the retry is what makes a deploy
    invisible to the operator instead of a red bar on every live agent."""
    calls: list[int] = []

    def _flaky() -> Any:
        calls.append(1)
        return {"result": _COUNTS} if len(calls) > 1 else None

    out = _statusline(monkeypatch, tmp_path, answer=_flaky)
    assert len(calls) == 2               # first missed, second landed
    assert "fleet 7" in out and "ago" not in out   # and it counts as LIVE, not stale

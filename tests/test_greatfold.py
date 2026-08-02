"""THE GREAT FOLD's machine (greatfold.py) — one-soul-per-seat as evidence-driven code.

Witnesses: the signature scan reads the append-only transcripts the way the delivery gate
does; the survey drops quoted ids the graph never registered and flags cross-seat bases;
the fold is dry-run by default, estate-carrying on execute, and briefs the desk AFTER; the
doorbell sweep demotes only families with NO tie to the living graph.
"""
from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.folds import canonical_agent
from src.orchestrator.greatfold import (
    _bare,
    demote_visits,
    fold_census,
    fold_seat,
    seat_roster,
    signed_matches_sync,
    survey_seats,
)
from src.orchestrator.mailbox import send_message

_SEND = '{{"type":"user","toolUseResult":"{{\\"sent\\":1,\\"from\\":\\"{agent}\\"}}"}}'
_WHISPER = '{{"type":"user","text":"osiris knows you as {agent} (project x)"}}'


def _office(root: Path, handle: str, house: str) -> Path:
    d = root / handle
    d.mkdir(parents=True)
    (d / ".osiris").write_text(f'project = "{house}"\n')
    return d


def _transcript(projects: Path, handle: str, sid: str, *lines: str,
                age_secs: int = 0) -> Path:
    slug = projects / f"-test--osiris-seats-{handle}"
    slug.mkdir(parents=True, exist_ok=True)
    t = slug / f"{sid}.jsonl"
    t.write_text("\n".join(lines) + "\n")
    if age_secs:
        past = datetime.now(UTC).timestamp() - age_secs
        os.utime(t, (past, past))
    return t


async def _agent(actions: Actions, label: str, *, handle: str | None = None,
                 at: datetime | None = None) -> None:
    oid = await actions.create_or_find_object("Agent", label, "test")
    if handle:
        await actions.assert_property(oid, "handle", handle, "test",
                                      at or datetime.now(UTC), 0.9,
                                      evidence_class="self_declared")


def test_bare_strips_the_numeral_and_the_case() -> None:
    assert _bare("Soundwave VIII") == "soundwave"
    assert _bare("TJMAX") == "tjmax"
    assert _bare("alfred") == "alfred"
    assert _bare("Thoth L") == "thoth"


def test_signature_scan_returns_ordered_testimony_per_session(tmp_path: Path) -> None:
    _transcript(tmp_path, "khnum", "sid-old", _SEND.format(agent="agent:aaaa1111"),
                age_secs=3600)
    _transcript(tmp_path, "khnum", "sid-new",
                _WHISPER.format(agent="agent:bbbb2222-ii"),
                _SEND.format(agent="agent:bbbb2222-ii"))
    out = signed_matches_sync(tmp_path, "khnum")
    assert out == [["agent:aaaa1111"],                       # mtime-ascending file order
                   ["agent:bbbb2222-ii", "agent:bbbb2222-ii"]]


async def test_survey_takes_each_sessions_own_resident_never_its_quotes(
        actions: Actions, tmp_path: Path) -> None:
    offices, projects = tmp_path / "offices", tmp_path / "projects"
    _office(offices, "khnum", "riverhouse")
    await _agent(actions, "agent:aaaa1111")
    await _agent(actions, "agent:bbbb2222-ii")
    # one session: quotes bbbb2222 mid-file (a census query, a read fixture), quotes an id
    # the graph never registered LAST — its own resident signature sits between them
    _transcript(projects, "khnum", "s1",
                _SEND.format(agent="agent:bbbb2222-ii"),
                _SEND.format(agent="agent:aaaa1111"),
                _SEND.format(agent="agent:dddd9999"))
    sv = await survey_seats(actions.pool, office_root=offices, projects_root=projects)
    signed = sv["seats"]["khnum"]["signed"]
    assert set(signed) == {"agent:aaaa1111"}   # the resident, not the quoted sibling
    assert "agent:dddd9999" not in signed      # an unregistered id is reading material
    assert sv["seats"]["khnum"]["resident_signed"] == "agent:aaaa1111"
    assert sv["seats"]["khnum"]["house"] == "riverhouse"


async def test_survey_flags_a_base_resident_in_two_offices(
        actions: Actions, tmp_path: Path) -> None:
    offices, projects = tmp_path / "offices", tmp_path / "projects"
    _office(offices, "khnum", "riverhouse")
    _office(offices, "sobek", "riverhouse")
    await _agent(actions, "agent:aaaa1111")
    await _agent(actions, "agent:bbbb2222-ii")
    _transcript(projects, "khnum", "s1", _SEND.format(agent="agent:aaaa1111"))
    _transcript(projects, "sobek", "s2", _SEND.format(agent="agent:bbbb2222-ii"),
                _SEND.format(agent="agent:aaaa1111"))
    sv = await survey_seats(actions.pool, office_root=offices, projects_root=projects)
    assert sv["conflicts"] == {"agent:aaaa1111": ["khnum", "sobek"]}


async def test_fold_seat_dry_run_names_the_folds_and_writes_nothing(
        actions: Actions, tmp_path: Path) -> None:
    offices, projects = tmp_path / "offices", tmp_path / "projects"
    _office(offices, "khnum", "riverhouse")
    await _agent(actions, "agent:aaaa1111")
    await _agent(actions, "agent:aaaa1111-ii")
    await _agent(actions, "agent:bbbb2222-ii")
    _transcript(projects, "khnum", "s-old", _SEND.format(agent="agent:aaaa1111-ii"),
                age_secs=3600)
    _transcript(projects, "khnum", "s-new", _SEND.format(agent="agent:bbbb2222-ii"))
    out = await fold_seat(actions, handle="khnum", actor="agent:test",
                          office_root=offices, projects_root=projects)
    assert out["living_head"] == "agent:bbbb2222-ii"
    assert [f["label"] for f in out["will_fold"]] == ["agent:aaaa1111",
                                                      "agent:aaaa1111-ii"]
    assert "resident signer" in out["will_fold"][0]["evidence"]
    assert await canonical_agent(actions.pool, "agent:aaaa1111") == "agent:aaaa1111"


async def test_fold_seat_execute_folds_mints_the_seat_and_briefs_after(
        actions: Actions, tmp_path: Path) -> None:
    offices, projects = tmp_path / "offices", tmp_path / "projects"
    _office(offices, "khnum", "riverhouse")
    await _agent(actions, "agent:aaaa1111")
    await _agent(actions, "agent:bbbb2222-ii")
    _transcript(projects, "khnum", "s-old", _SEND.format(agent="agent:aaaa1111"),
                age_secs=3600)
    _transcript(projects, "khnum", "s-new", _SEND.format(agent="agent:bbbb2222-ii"))
    # fold_agent's own gate (census a5e53ed8) requires the operator's actor for a real
    # fold — greatfold.fold_seat forwards `actor` unchanged, so the caller must be one
    out = await fold_seat(actions, handle="khnum", actor="operator", execute=True,
                          office_root=offices, projects_root=projects)
    assert [f["folded"] for f in out["folded"]] == ["agent:aaaa1111"]
    assert not out["refused"]
    assert await canonical_agent(actions.pool, "agent:aaaa1111") == "agent:bbbb2222-ii"
    assert out["seat_minted"] and str(out["seat_id"]).startswith("seat:")
    roster = await seat_roster(actions.pool, office_root=offices)
    assert next(r for r in roster if r["handle"] == "khnum")["seat_id"] == out["seat_id"]
    brief = await actions.pool.fetchval(
        "SELECT body FROM fleet_messages WHERE to_project='operator' "
        "ORDER BY id DESC LIMIT 1")
    assert brief and brief.startswith("GREAT FOLD — seat khnum")
    assert out["briefed"] is not None
    # idempotent: a second run finds nothing left to fold
    again = await fold_seat(actions, handle="khnum", actor="operator", execute=True,
                            office_root=offices, projects_root=projects)
    assert not again["will_fold"]


async def test_fold_seat_never_folds_a_cross_seat_base(
        actions: Actions, tmp_path: Path) -> None:
    offices, projects = tmp_path / "offices", tmp_path / "projects"
    _office(offices, "khnum", "riverhouse")
    _office(offices, "sobek", "riverhouse")
    await _agent(actions, "agent:aaaa1111")
    await _agent(actions, "agent:bbbb2222-ii")
    await _agent(actions, "agent:cccc3333")
    _transcript(projects, "khnum", "s1", _SEND.format(agent="agent:aaaa1111"),
                age_secs=3600)
    _transcript(projects, "khnum", "s2", _SEND.format(agent="agent:bbbb2222-ii"))
    _transcript(projects, "sobek", "s3", _SEND.format(agent="agent:cccc3333"),
                _SEND.format(agent="agent:aaaa1111"))
    out = await fold_seat(actions, handle="khnum", actor="agent:test", execute=True,
                          office_root=offices, projects_root=projects)
    assert [f["base"] for f in out["flagged"]] == ["agent:aaaa1111"]
    assert not out["folded"]
    assert await canonical_agent(actions.pool, "agent:aaaa1111") == "agent:aaaa1111"


async def test_named_testimony_outranks_an_unnamed_newest_session(
        actions: Actions, tmp_path: Path) -> None:
    offices, projects = tmp_path / "offices", tmp_path / "projects"
    _office(offices, "khnum", "riverhouse")
    await _agent(actions, "agent:aaaa1111", handle="Khnum")
    await _agent(actions, "agent:bbbb2222")   # the fresh mint that never claimed
    _transcript(projects, "khnum", "s-named", _SEND.format(agent="agent:aaaa1111"),
                age_secs=3600)
    _transcript(projects, "khnum", "s-doorbell", _SEND.format(agent="agent:bbbb2222"))
    out = await fold_seat(actions, handle="khnum", actor="agent:test",
                          office_root=offices, projects_root=projects)
    assert out["resident"] == "agent:aaaa1111"          # the name, not the doorbell
    assert [f["label"] for f in out["will_fold"]] == ["agent:bbbb2222"]


async def test_fold_seat_by_handle_claim_alone(actions: Actions, tmp_path: Path) -> None:
    offices = tmp_path / "offices"
    projects = tmp_path / "projects"
    projects.mkdir()
    _office(offices, "khnum", "riverhouse")
    old, new = datetime.now(UTC) - timedelta(days=9), datetime.now(UTC)
    await _agent(actions, "agent:aaaa1111", handle="Khnum VIII", at=old)
    await _agent(actions, "agent:bbbb2222-ii", handle="Khnum", at=new)
    out = await fold_seat(actions, handle="khnum", actor="agent:test",
                          office_root=offices, projects_root=projects)
    assert out["resident"] == "agent:bbbb2222-ii"  # the newest claim, no transcripts needed
    assert [f["label"] for f in out["will_fold"]] == ["agent:aaaa1111"]
    assert "claimed the name" in out["will_fold"][0]["evidence"]


async def test_a_rebased_lineage_is_never_swallowed_by_its_ancestors_prefix(
        actions: Actions, tmp_path: Path) -> None:
    offices, projects = tmp_path / "offices", tmp_path / "projects"
    _office(offices, "khnum", "riverhouse")
    await _agent(actions, "agent:aaaa1111")            # the old base...
    await _agent(actions, "agent:aaaa1111-iii")        # ...and its generations
    await _agent(actions, "agent:aaaa1111-g40")        # the REBASE — the living lineage
    await _agent(actions, "agent:aaaa1111-g40-ii")
    _transcript(projects, "khnum", "s-old", _SEND.format(agent="agent:aaaa1111-iii"),
                age_secs=3600)
    _transcript(projects, "khnum", "s-new", _SEND.format(agent="agent:aaaa1111-g40-ii"))
    out = await fold_seat(actions, handle="khnum", actor="agent:test",
                          office_root=offices, projects_root=projects)
    assert out["living_head"] == "agent:aaaa1111-g40-ii"
    assert [f["label"] for f in out["will_fold"]] == ["agent:aaaa1111",
                                                      "agent:aaaa1111-iii"]


async def test_demote_visits_touches_only_the_tieless(actions: Actions) -> None:
    await _agent(actions, "agent:11110000", handle="Named")
    await _agent(actions, "agent:22220000")
    await _agent(actions, "agent:33330000")  # the pure doorbell
    await send_message(actions.pool, from_agent="agent:11110000", from_project="osiris",
                       to_agent="agent:22220000", body="a letter ties you to the living")
    dry = await demote_visits(actions, actor="agent:test")
    assert "agent:33330000" in dry["sample"]
    out = await demote_visits(actions, actor="agent:test", execute=True)
    assert out["kept"].get("named", 0) >= 1
    assert out["kept"].get("has-mail", 0) >= 1
    census = await fold_census(actions.pool)
    assert census["visit_families"] >= 1
    demoted = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
        "ON a.object_id=o.id AND a.name='agent_class' WHERE o.canonical='agent:33330000'")
    assert demoted == "visit"
    named = await actions.pool.fetchval(
        "SELECT count(*) FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "AND a.name='agent_class' WHERE o.canonical='agent:11110000'")
    assert named == 0

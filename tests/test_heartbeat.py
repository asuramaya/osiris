"""compute_heartbeat's project-label resolution (operator bug, msg 6934, thread
19d6bdcb7fa9): the PIN wins outright the instant it resolves to anything — the old (A)/(B)
split (thread 6483/6487/6492) let a seat's own mechanical pin copies (office / anchor_cwd
courtesy copy / `~/code/<handle>` scratch convention) lose to `seat.house` on the premise
that nobody ever DECLARES a value there, which broke the day found_seat/mint_seat stopped
fabricating `project` from the handle (decision 24e0b761/commit cf201a9) — the office pin
became exactly where a seat's project IS deliberately declared. Absent a pin, the fallback
is charter (exactly one governed repo) then lineage_works_in (merge-normalized, whole-
lineage-agrees) — `house` NEVER stands in for `project` anywhere in this order.
"""
from __future__ import annotations

from pathlib import Path

from src.actions.core import Actions
from src.orchestrator import mounts
from src.orchestrator.heartbeat import _seat_owns_cwd, compute_heartbeat
from src.orchestrator.offices import _default_office_root
from src.orchestrator.seats import ensure_seat

# --- _seat_owns_cwd: the pure discriminator, no DB, no I/O beyond Path.resolve() ---------

def test_seat_owns_cwd_matches_the_office_root(tmp_path: Path) -> None:
    office = _default_office_root() / "ownercwd1"
    office.mkdir(parents=True, exist_ok=True)
    assert _seat_owns_cwd(str(office), handle="ownercwd1", anchor_cwd=None) is True


def test_seat_owns_cwd_matches_the_anchor_cwd_exactly(tmp_path: Path) -> None:
    anchor = tmp_path / "anchor-a"
    anchor.mkdir()
    assert _seat_owns_cwd(str(anchor), handle="nobodyshandle", anchor_cwd=str(anchor)) is True


def test_seat_owns_cwd_matches_a_subdirectory_of_the_anchor_cwd(tmp_path: Path) -> None:
    """Containment, not exact match — read_project_label's own climb-to-repo-root means a
    cwd inside the anchor's own tree is still answered by the anchor's `.osiris`."""
    anchor = tmp_path / "anchor-b"
    sub = anchor / "nested" / "deeper"
    sub.mkdir(parents=True)
    assert _seat_owns_cwd(str(sub), handle="nobodyshandle", anchor_cwd=str(anchor)) is True


def test_seat_owns_cwd_matches_the_scratch_workspace_convention(
    tmp_path: Path, monkeypatch: object,
) -> None:
    fake_home = tmp_path / "fakehome"
    (fake_home / "code" / "scratchhandle").mkdir(parents=True)
    import src.orchestrator.heartbeat as hb
    monkeypatch.setattr(hb.Path, "home", classmethod(lambda cls: fake_home))  # type: ignore[attr-defined]
    assert _seat_owns_cwd(str(fake_home / "code" / "scratchhandle"),
                          handle="scratchhandle", anchor_cwd=None) is True


def test_seat_owns_cwd_false_for_an_unrelated_governed_checkout(tmp_path: Path) -> None:
    anchor = tmp_path / "anchor-c"
    anchor.mkdir()
    elsewhere = tmp_path / "REPOS" / "Godel"
    elsewhere.mkdir(parents=True)
    assert _seat_owns_cwd(str(elsewhere), handle="jesus", anchor_cwd=str(anchor)) is False


def test_seat_owns_cwd_false_for_a_nonexistent_cwd(tmp_path: Path) -> None:
    assert _seat_owns_cwd(str(tmp_path / "does-not-exist-at-all"),
                          handle="whoever", anchor_cwd=None) is False


# --- compute_heartbeat: the live (A)/(B) split -------------------------------------------

async def _seated(actions: Actions, *, handle: str, house: str, anchor_cwd: str,
                  session_id: str) -> str:
    """Seats `handle` and mounts it under a job_dir find_session_row's own lane 1 actually
    matches (`%/jobs/<sid8>`, mounts.py:427) — a job_dir that doesn't fit that shape leaves
    `agent` unresolved and every downstream assertion in a caller's test passes VACUOUSLY
    (resolved_project just echoes project_hint straight through, proving nothing)."""
    agent = f"agent:{handle.lower()}"
    seat = await ensure_seat(actions, house=house, handle=handle, anchor_cwd=anchor_cwd,
                             source="test")
    from src.orchestrator.seats import bind_holder
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id=agent)
    await actions.create_or_find_object("Agent", agent, agent)
    job_dir = f"/home/test/.claude/jobs/{session_id[:8]}"
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project=house,
                            cwd=anchor_cwd, model="claude-fable-5", session_key="k")
    return agent


async def test_a_pin_at_the_seats_own_anchor_wins_over_house(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE LIVE SPECIMEN THIS FIX CLOSES (operator bug, msg 6934): a seat's own OFFICE pin
    is exactly where found_seat/mint_seat now writes a DELIBERATELY declared project
    (cf201a9) — house (here "Godel", itself the mint-time handle fabrication for a
    self-managed seat) must never override it, even though this cwd is the seat's own
    mechanical pin copy."""
    anchor = tmp_path / "jesuslike"
    anchor.mkdir()
    (anchor / ".osiris").write_text('project = "Jesus"\n')  # the seat's own DECLARATION
    await _seated(actions, handle="Jesuslike", house="Godel", anchor_cwd=str(anchor),
                 session_id="jesuslike01")

    out = await compute_heartbeat(
        actions.pool, project_hint="Jesus", session_id="jesuslike01",
        model_id="claude-fable-5", lease_secs=3600, cwd=str(anchor))
    assert out.resolved_seat_handle == "Jesuslike"  # agent resolution actually ran
    assert out.resolved_project == "Jesus"  # the pin wins outright — house never overrides


async def test_case_b_a_pin_at_a_genuinely_separate_checkout_still_wins(
    actions: Actions, tmp_path: Path,
) -> None:
    anchor = tmp_path / "jesushome"
    anchor.mkdir()
    checkout = tmp_path / "REPOS" / "Somewhere"
    checkout.mkdir(parents=True)
    (checkout / ".osiris").write_text('project = "Somewhere"\n')
    await _seated(actions, handle="Jesushome", house="Godel", anchor_cwd=str(anchor),
                 session_id="jesushome01")

    out = await compute_heartbeat(
        actions.pool, project_hint="Somewhere", session_id="jesushome01",
        model_id="claude-fable-5", lease_secs=3600, cwd=str(checkout))
    assert out.resolved_seat_handle == "Jesushome"  # agent resolution actually ran
    assert out.resolved_project == "Somewhere"  # 577988ed: a separate checkout's own word stands


async def test_no_cwd_given_keeps_the_old_file_wins_behavior(
    actions: Actions, tmp_path: Path,
) -> None:
    """Backward compatible: an old caller (or a body with no cwd resolvable) gets exactly
    the pre-existing precedence — file wins when present."""
    anchor = tmp_path / "nocwdcase"
    anchor.mkdir()
    await _seated(actions, handle="Nocwdcase", house="Godel", anchor_cwd=str(anchor),
                 session_id="nocwdcase01")

    out = await compute_heartbeat(
        actions.pool, project_hint="StaleHandleName", session_id="nocwdcase01",
        model_id="claude-fable-5", lease_secs=3600)
    assert out.resolved_seat_handle == "Nocwdcase"  # agent resolution actually ran
    assert out.resolved_project == "StaleHandleName"


async def test_case_a_a_matching_pin_at_the_anchor_is_a_no_op(
    actions: Actions, tmp_path: Path,
) -> None:
    """The pin already agrees with the graph — case (A) still resolves to the same value,
    never a spurious change."""
    anchor = tmp_path / "agreeing"
    anchor.mkdir()
    (anchor / ".osiris").write_text('project = "Godel"\n')
    await _seated(actions, handle="Agreeing", house="Godel", anchor_cwd=str(anchor),
                 session_id="agreeing01")

    out = await compute_heartbeat(
        actions.pool, project_hint="Godel", session_id="agreeing01",
        model_id="claude-fable-5", lease_secs=3600, cwd=str(anchor))
    assert out.resolved_project == "Godel"


# --- no pin: charter, then lineage works_in, then nothing — house NEVER appears ----------

async def test_no_pin_falls_back_to_a_single_chartered_repo_never_house(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.charter import set_charter

    anchor = tmp_path / "chartered"
    anchor.mkdir()  # no .osiris here at all — project_hint resolves to None
    agent = await _seated(actions, handle="Chartered", house="Chartered",
                          anchor_cwd=str(anchor), session_id="chartered01")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:cdking", "test")
    from datetime import UTC, datetime
    await actions.assert_property(proj, "name", "cdking", "test", datetime.now(UTC), 0.9)
    seat_id = (await actions.pool.fetchval(
        "SELECT o.canonical FROM objects o WHERE o.type='Seat' AND o.status='active' "
        "AND EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "AND a.name='handle' AND a.value #>> '{}' = 'Chartered')"))
    await set_charter(actions, seat_id, ["cdking"], actor="test")

    out = await compute_heartbeat(
        actions.pool, project_hint="", session_id="chartered01",
        model_id="claude-fable-5", lease_secs=3600, cwd=str(anchor))
    assert out.resolved_seat_handle == "Chartered"
    assert out.resolved_project == "cdking"  # the charter, never "Chartered" (house)
    _ = agent


async def test_no_pin_no_charter_falls_back_to_lineage_works_in_never_house(
    actions: Actions, tmp_path: Path,
) -> None:
    anchor = tmp_path / "workedin"
    anchor.mkdir()
    agent = await _seated(actions, handle="Workedin", house="Workedin",
                          anchor_cwd=str(anchor), session_id="workedin01")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:realproject", "test")
    from datetime import UTC, datetime
    await actions.assert_property(proj, "name", "realproject", "test", datetime.now(UTC), 0.9)
    agent_oid = await actions.create_or_find_object("Agent", agent, agent)
    await actions.create_link(agent_oid, proj, "works_in", agent, datetime.now(UTC), 0.9)

    out = await compute_heartbeat(
        actions.pool, project_hint="", session_id="workedin01",
        model_id="claude-fable-5", lease_secs=3600, cwd=str(anchor))
    assert out.resolved_seat_handle == "Workedin"
    assert out.resolved_project == "realproject"  # lineage works_in, never "Workedin" (house)


async def test_no_pin_no_charter_no_works_in_stays_unresolved_never_house(
    actions: Actions, tmp_path: Path,
) -> None:
    """Absent every real signal, the answer is honest absence — NOT the seat's own house,
    which is exactly the fabrication this whole fix exists to stop propagating."""
    anchor = tmp_path / "nosignal"
    anchor.mkdir()
    await _seated(actions, handle="Nosignal", house="Nosignal",
                 anchor_cwd=str(anchor), session_id="nosignal01")

    out = await compute_heartbeat(
        actions.pool, project_hint="", session_id="nosignal01",
        model_id="claude-fable-5", lease_secs=3600, cwd=str(anchor))
    assert out.resolved_seat_handle == "Nosignal"
    assert out.resolved_project is None

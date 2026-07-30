"""THE BOOT COMPILER (thread 4951d818, task #53) — standing orders as a compiled
artifact. Pure marker/version logic first, then the four sources against a real graph,
then reissue_office's three acceptance tests: a fresh compiled mint carries the ordered
checklist, a reissue on a hand-mutated seat preserves everything outside the managed
section byte-for-byte, and a mangled marker refuses loudly rather than guessing.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.boot_compiler import (
    MarkerError,
    _armed_practices,
    _has_any_markers,
    boot_rollout_gap_notes,
    boot_rollout_gaps,
    compile_managed_body,
    derive_role,
    locate_managed_section,
    reissue_office,
    template_version,
    wrap_managed,
)
from src.orchestrator.capture import (
    _witness_link,
    record_decision,
    record_practice,
    refute_practice,
)
from src.orchestrator.mintseat import mint_seat
from src.orchestrator.seats import ensure_seat, peer_seats

# ═══════════ PURE: version + marker location ═══════════


def test_template_version_is_deterministic() -> None:
    assert template_version() == template_version()
    assert len(template_version()) == 12


def test_wrap_and_locate_round_trip() -> None:
    wrapped = wrap_managed("hello world", "abc123")
    text = "before\n" + wrapped + "after\n"
    b_start, b_end, e_start, e_end, version = locate_managed_section(text)
    assert version == "abc123"
    assert text[b_end:e_start] == "\nhello world\n"
    assert text[:b_start] == "before\n"
    assert text[e_end:] == "\nafter\n"


def test_has_any_markers_pure() -> None:
    assert _has_any_markers("plain text") is False
    assert _has_any_markers(wrap_managed("x", "v1")) is True
    assert _has_any_markers("<!-- osiris:compiled:begin v=v1 -->") is True
    assert _has_any_markers("<!-- osiris:compiled:end -->") is True


def test_locate_managed_section_refuses_when_none_found() -> None:
    try:
        locate_managed_section("no markers here at all")
        raise AssertionError("expected MarkerError")
    except MarkerError as exc:
        assert "never been compiled" in str(exc)


def test_locate_managed_section_refuses_duplicate_begin() -> None:
    text = (wrap_managed("a", "v1") + "\n"
            + "<!-- osiris:compiled:begin v=v2 -->\n")
    try:
        locate_managed_section(text)
        raise AssertionError("expected MarkerError")
    except MarkerError as exc:
        assert "2 BEGIN markers" in str(exc)


def test_locate_managed_section_refuses_duplicate_end() -> None:
    text = wrap_managed("a", "v1") + "<!-- osiris:compiled:end -->\n"
    try:
        locate_managed_section(text)
        raise AssertionError("expected MarkerError")
    except MarkerError as exc:
        assert "2 END markers" in str(exc)


def test_locate_managed_section_refuses_an_orphan_begin() -> None:
    text = "<!-- osiris:compiled:begin v=v1 -->\nno end here\n"
    try:
        locate_managed_section(text)
        raise AssertionError("expected MarkerError")
    except MarkerError as exc:
        assert "no matching END" in str(exc)


def test_locate_managed_section_refuses_an_orphan_end() -> None:
    text = "no begin here\n<!-- osiris:compiled:end -->\n"
    try:
        locate_managed_section(text)
        raise AssertionError("expected MarkerError")
    except MarkerError as exc:
        assert "no matching BEGIN" in str(exc)


def test_locate_managed_section_refuses_end_before_begin() -> None:
    text = ("<!-- osiris:compiled:end -->\n"
            "<!-- osiris:compiled:begin v=v1 -->\nbody\n")
    try:
        locate_managed_section(text)
        raise AssertionError("expected MarkerError")
    except MarkerError as exc:
        assert "before its BEGIN" in str(exc)


# ═══════════ derive_role + practice arming, against a real graph ═══════════


async def test_derive_role_worker_vs_coordinator(actions: Actions) -> None:
    coordinator = (await ensure_seat(actions, house="bctest", handle="BCBoss",
                                     source="test"))["seat_id"]
    worker_seat_id = (await ensure_seat(actions, house="bctest", handle="BCWorker",
                                        source="test"))["seat_id"]
    worker_obj = await actions.create_or_find_object("Seat", worker_seat_id, "test")
    coord_obj = await actions.create_or_find_object("Seat", coordinator, "test")
    await actions.create_link(worker_obj, coord_obj, "managed_by", "test",
                              datetime.now(UTC), 0.9, evidence_class="self_declared")

    assert await derive_role(actions.pool, coordinator) == "coordinator"
    assert await derive_role(actions.pool, worker_seat_id) == "worker"


async def test_armed_practices_caps_at_top_n_by_confirmed(actions: Actions) -> None:
    # a pool of distinct decisions to witness with — witnesses is idempotent per
    # (practice, evidence) pair, so distinguishing confirmed counts needs distinct
    # evidence objects, not repeated calls against the same one
    evidence = [await record_decision(actions, f"boot-compiler cap-test witness {i}")
                for i in range(7)]
    ids = []
    for i in range(7):
        pid = await record_practice(actions, f"boot compiler cap-test lesson number {i}")
        ids.append(pid)
        for e in evidence[:7 - i]:  # lesson 0 gets the most witnesses, lesson 6 the fewest
            await _witness_link(actions, pid, e, "test", datetime.now(UTC))

    armed = await _armed_practices(actions.pool, "coordinator", limit=5)
    assert len(armed) == 5
    statements = [p["statement"] for p in armed]
    # the 5 HIGHEST-witnessed lessons (0-4) survive the cap; 5 and 6 are dropped
    for i in range(5):
        assert f"boot compiler cap-test lesson number {i}" in statements
    assert "boot compiler cap-test lesson number 5" not in statements
    assert "boot compiler cap-test lesson number 6" not in statements
    assert [p["confirmed"] for p in armed] == sorted(
        (p["confirmed"] for p in armed), reverse=True)


async def test_armed_practices_excludes_refuted_and_filters_by_role_surface(
    actions: Actions,
) -> None:
    d = await record_decision(actions, "boot-compiler role-filter witness")
    dead = await record_practice(actions, "boot compiler refuted lesson never arms")
    await _witness_link(actions, dead, d, "test", datetime.now(UTC))
    await refute_practice(actions, str(dead), killed_by=str(d))
    # role-tagged practices
    await record_practice(actions, "boot compiler coordinator-only lesson",
                          surface="coordinator")
    await record_practice(actions, "boot compiler worker-only lesson", surface="worker")
    # a domain-tagged practice (BlindSpot vocabulary, NOT a role) — must arm for both
    await record_practice(actions, "boot compiler deploy domain lesson", surface="deploy")

    worker_armed = await _armed_practices(actions.pool, "worker", limit=20)
    coord_armed = await _armed_practices(actions.pool, "coordinator", limit=20)

    worker_statements = {p["statement"] for p in worker_armed}
    coord_statements = {p["statement"] for p in coord_armed}
    assert "boot compiler refuted lesson never arms" not in worker_statements
    assert "boot compiler refuted lesson never arms" not in coord_statements
    assert "boot compiler worker-only lesson" in worker_statements
    assert "boot compiler worker-only lesson" not in coord_statements
    assert "boot compiler coordinator-only lesson" in coord_statements
    assert "boot compiler coordinator-only lesson" not in worker_statements
    assert "boot compiler deploy domain lesson" in worker_statements
    assert "boot compiler deploy domain lesson" in coord_statements


# ═══════════ compile_managed_body: worker vs coordinator shape ═══════════


async def test_compile_managed_body_worker_has_gates_coordinator_does_not(
    actions: Actions,
) -> None:
    coordinator = (await ensure_seat(actions, house="cbtest2", handle="CBBoss",
                                     source="test"))["seat_id"]
    worker_seat_id = (await ensure_seat(actions, house="cbtest2", handle="CBWorker",
                                        source="test"))["seat_id"]

    worker_body = await compile_managed_body(
        actions, seat_id=worker_seat_id, handle="CBWorker", house="cbtest2",
        office="/tmp/cbworker", seat_line=" — durable identity `x`.",
        charter_block="You govern: none.", peer_block="\n",
        role="worker", manager_seat_id=coordinator)
    coord_body = await compile_managed_body(
        actions, seat_id=coordinator, handle="CBBoss", house="cbtest2",
        office="/tmp/cbboss", seat_line=" — durable identity `y`.",
        charter_block="You govern: none.", peer_block="\n", role="coordinator")

    assert "Gates before any commit" in worker_body
    assert "The review loop" in worker_body
    assert "Your role: WORKER" in worker_body
    assert "Gates before any commit" not in coord_body
    assert "Your role: COORDINATOR" in coord_body
    assert "The desk pattern" in coord_body


# ═══════════ reissue_office: the three acceptance tests ═══════════


async def test_fresh_mint_carries_the_ordered_first_breath_checklist(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE WAKE CONTRACT (a891c152) as the compiler's acceptance test: a fresh compiled
    mint still guides a new mind through mount -> claim_name -> orient() -> report, the
    exact ordered-checklist form ruling 90babd6e proved matters."""
    await ensure_seat(actions, house="wakehouse", handle="WakeBoss", source="test")
    out = await mint_seat(actions, manager="WakeBoss", handle="WakeWorker",
                          office_root=tmp_path / "seats", actor="agent:test")
    orders = (tmp_path / "seats" / "wakeworker" / "CLAUDE.md").read_text()
    assert "First breath, in order" in orders
    assert "1. `mount(cwd=" in orders
    assert "claim_name('WakeWorker')" in orders
    assert "orient()" in orders
    assert "report for duty" in orders or "send(to_agent=" in orders
    assert "<!-- osiris:compiled:begin v=" in orders
    assert "<!-- osiris:compiled:end -->" in orders
    assert out["office"]["standing_orders"] == "written"


async def test_reissue_preserves_hand_written_content_outside_the_managed_section(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE NEVER-CLOBBER BOUNDARY's real test: a seat that organically accumulated
    hand-composed content post-mint (Khnum's own "WHY YOU EXIST" narrative is the real
    precedent) keeps that content byte-for-byte across a reissue, while a live fact that
    changed AFTER mint (a peer bond — the exact gap decision e9e7cac5 named) now reaches
    the file for the first time."""
    await ensure_seat(actions, house="reissuehouse", handle="ReissueBoss", source="test")
    minted = await mint_seat(actions, manager="ReissueBoss", handle="ReissueWorker",
                             office_root=tmp_path / "seats", actor="agent:test")
    seat_id = minted["seat_id"]
    orders_path = tmp_path / "seats" / "reissueworker" / "CLAUDE.md"

    narrative = "\n## WHY YOU EXIST\nThis seat was minted for a very specific reason.\n"
    orders_path.write_text(orders_path.read_text() + narrative)
    before_edit = orders_path.read_text()
    assert before_edit.endswith(narrative)

    peer = (await ensure_seat(actions, house="reissuehouse", handle="ReissuePeer",
                              source="test"))["seat_id"]
    await peer_seats(actions, seat_id, peer, because="test peer bond for reissue",
                     actor="agent:test")

    out = await reissue_office(actions, seat_id=seat_id,
                               because="peer bond landed, recompiling", actor="agent:test")
    assert out["changed"] is True
    after = orders_path.read_text()
    assert after.endswith(narrative)  # the hand-written narrative survived, untouched
    assert "## Peer" in after
    assert "peered with **ReissuePeer**" in after
    assert "<!-- osiris:compiled:begin v=" in after
    assert after.count("<!-- osiris:compiled:begin") == 1
    assert after.count("<!-- osiris:compiled:end") == 1


async def test_reissue_refuses_loudly_naming_the_seat_when_markers_are_mangled(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE REFUSAL IS THE BOUNDARY'S REAL TEETH (Thoth's added requirement, msg 1819):
    a hand-edit that duplicates a marker must never be silently guessed past, re-wrapped,
    or have a second section appended beside it."""
    await ensure_seat(actions, house="mangledhouse", handle="MangledBoss", source="test")
    minted = await mint_seat(actions, manager="MangledBoss", handle="MangledWorker",
                             office_root=tmp_path / "seats", actor="agent:test")
    seat_id = minted["seat_id"]
    orders_path = tmp_path / "seats" / "mangledworker" / "CLAUDE.md"
    mangled = orders_path.read_text() + "\n<!-- osiris:compiled:begin v=rogue -->\n"
    orders_path.write_text(mangled)

    out = await reissue_office(actions, seat_id=seat_id, because="attempted reissue",
                               actor="agent:test")
    assert "error" in out
    assert "MangledWorker" in out["error"]
    assert seat_id in out["error"]
    assert "2 BEGIN markers" in out["error"]
    assert orders_path.read_text() == mangled  # untouched — no guess, no rewrap


async def test_reissue_adopt_appends_a_managed_section_to_a_legacy_office(
    actions: Actions, tmp_path: Path,
) -> None:
    manager = (await ensure_seat(actions, house="adopthouse", handle="AdoptBoss",
                                 source="test"))["seat_id"]
    worker = await ensure_seat(actions, house="adopthouse", handle="AdoptWorker",
                               source="test")
    seat_id = worker["seat_id"]
    worker_obj = await actions.create_or_find_object("Seat", seat_id, "test")
    mgr_obj = await actions.create_or_find_object("Seat", manager, "test")
    await actions.create_link(worker_obj, mgr_obj, "managed_by", "test",
                              datetime.now(UTC), 0.9, evidence_class="self_declared")
    await actions.assert_property(worker_obj, "anchor_cwd",
                                  str(tmp_path / "legacy" / "adoptworker"), "test",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    office = tmp_path / "legacy" / "adoptworker"
    office.mkdir(parents=True)
    legacy_text = "# AdoptWorker's hand-written orders\nWritten by hand, long ago.\n"
    (office / "CLAUDE.md").write_text(legacy_text)

    out = await reissue_office(actions, seat_id=seat_id, because="first compile",
                               actor="agent:test", adopt=True)
    assert out["changed"] is True
    after = (office / "CLAUDE.md").read_text()
    assert after.startswith(legacy_text)
    assert "<!-- osiris:compiled:begin v=" in after

    again = await reissue_office(actions, seat_id=seat_id, because="second adopt attempt",
                                 actor="agent:test", adopt=True)
    assert "error" in again
    assert "already carries managed-section" in again["error"]


async def test_reissue_requires_because_and_refuses_an_unknown_seat(
    actions: Actions,
) -> None:
    blank = await reissue_office(actions, seat_id="seat:doesnotexist", because="",
                                 actor="agent:test")
    assert "error" in blank and "because is required" in blank["error"]

    unknown = await reissue_office(actions, seat_id="seat:doesnotexist",
                                   because="a reason", actor="agent:test")
    assert "error" in unknown and "no such seat" in unknown["error"]


# ═══════════ THE ROLLOUT CHECK (thread 0e5bae06, #84) — names, never counts ═══════════

async def test_boot_rollout_gaps_names_each_of_the_four_reasons_distinctly(
    actions: Actions, tmp_path: Path,
) -> None:
    """The check's whole point: a seat missing its section for FOUR different reasons must
    never collapse into one count, because only `never_compiled` is what adopt=True fixes —
    the other three need a completely different act (a hand fix, establish_office, or the
    anchor_cwd bug thread 7a9c3c46 already tracks)."""
    # compiled: not a gap.
    compiled = await ensure_seat(actions, house="rollouthouse", handle="RolloutCompiled",
                                 anchor_cwd=str(tmp_path / "compiled"), source="test")
    (tmp_path / "compiled").mkdir()
    (tmp_path / "compiled" / "CLAUDE.md").write_text(
        "# hand prose\n\n" + wrap_managed("compiled body", "v1"))

    # never_compiled: has an office and a CLAUDE.md, zero markers.
    never = await ensure_seat(actions, house="rollouthouse", handle="RolloutNever",
                              anchor_cwd=str(tmp_path / "never"), source="test")
    (tmp_path / "never").mkdir()
    (tmp_path / "never" / "CLAUDE.md").write_text("# hand-written orders, pre-compiler\n")

    # malformed: markers present but damaged (a duplicated BEGIN).
    malformed = await ensure_seat(actions, house="rollouthouse", handle="RolloutMalformed",
                                  anchor_cwd=str(tmp_path / "malformed"), source="test")
    (tmp_path / "malformed").mkdir()
    (tmp_path / "malformed" / "CLAUDE.md").write_text(
        wrap_managed("body", "v1") + "\n<!-- osiris:compiled:begin v=rogue -->\n")

    # no_claude_md: has a handle and an anchor_cwd, but no file there yet.
    no_file = await ensure_seat(actions, house="rollouthouse", handle="RolloutNoFile",
                                anchor_cwd=str(tmp_path / "nofile"), source="test")

    # no_office: no anchor_cwd on record at all.
    no_office = await ensure_seat(actions, house="rollouthouse", handle="RolloutNoOffice",
                                  source="test")

    gaps = await boot_rollout_gaps(actions.pool)
    by_seat = {g["seat_id"]: g for g in gaps}

    assert compiled["seat_id"] not in by_seat
    assert by_seat[never["seat_id"]]["reason"] == "never_compiled"
    assert by_seat[malformed["seat_id"]]["reason"] == "malformed"
    assert by_seat[no_file["seat_id"]]["reason"] == "no_claude_md"
    assert by_seat[no_office["seat_id"]]["reason"] == "no_office"


async def test_boot_rollout_gaps_silent_when_every_active_seat_is_compiled(
    actions: Actions, tmp_path: Path,
) -> None:
    seat = await ensure_seat(actions, house="cleanhouse", handle="AllCompiled",
                             anchor_cwd=str(tmp_path / "clean"), source="test")
    (tmp_path / "clean").mkdir()
    (tmp_path / "clean" / "CLAUDE.md").write_text(wrap_managed("body", "v1"))
    gaps = await boot_rollout_gaps(actions.pool)
    assert seat["seat_id"] not in {g["seat_id"] for g in gaps}


def test_boot_rollout_gap_notes_names_the_seat_the_house_and_the_fix() -> None:
    notes = boot_rollout_gap_notes([
        {"seat_id": "seat:aaa", "handle": "Atlas", "house": "atlas", "reason": "never_compiled"},
        {"seat_id": "seat:bbb", "handle": "Bort", "house": "", "reason": "no_office"},
    ])
    assert len(notes) == 2
    assert "Atlas (atlas)" in notes[0] and "reissue_office(adopt=True)" in notes[0]
    assert "Bort (no house)" in notes[1] and "not an adopt target" in notes[1]


def test_boot_rollout_gap_notes_silent_on_an_empty_list() -> None:
    assert boot_rollout_gap_notes([]) == []

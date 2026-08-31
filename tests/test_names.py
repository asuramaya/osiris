"""Phase 2 — names/seats: the model names itself, the substrate enforces uniqueness
(ruling 1e02e069), AMENDED by the HOUSE/SEAT/HOLDER ruling (operator, 2026-07-12): "the project
name is the house (sibling-eight), each function/job has a name (Ra), the holder dies and
multiplies (ra I, ra II)". Exhaustion is per-HOUSE, not global: a seat belongs to one house, and
inside that house it is INHERITED by whoever takes up the job next.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator.agents import (
    claim_name,
    register_agent,
    resolve_handle,
    resolve_identity,
    seat_label,
)
from src.orchestrator.mailbox import ack_messages, read_inbox, send_message, unread_count


async def _agent(actions: Actions, canonical: str, project: str = "handlingtheloop") -> None:
    a = await actions.create_or_find_object("Agent", canonical, canonical)
    await actions.assert_property(a, "project", project, canonical,
                                  __import__("datetime").datetime.now(
                                      __import__("datetime").UTC), 0.9)


async def test_a_seat_belongs_to_one_house_and_is_inherited_inside_it(actions: Actions) -> None:
    """AMENDED by the operator's ruling (2026-07-12). Exhaustion was GLOBAL and keyed to the
    lineage ANCHOR, so when a conversation ended its name died with it and the next mind in the
    same house was refused as a stranger — which is how sibling-eight's Ra became Ptah. Now a seat
    belongs to a HOUSE: outsiders are refused, heirs inherit."""
    await _agent(actions, "agent:aaa", project="alpha")
    await _agent(actions, "agent:bbb", project="beta")
    await _agent(actions, "agent:ccc", project="alpha")

    got = await claim_name(actions, "agent:aaa", "Wayland", source="agent:aaa")
    assert got["claimed"] == "Wayland" and got["seat"] == "Wayland I"  # holder 1 wears I

    # a mind in ANOTHER HOUSE cannot take the seat (case-insensitive)
    refused = await claim_name(actions, "agent:bbb", "wayland", source="agent:bbb")
    assert "another house" in refused["error"]
    mine = await claim_name(actions, "agent:bbb", "Nadia", source="agent:bbb")
    assert mine["claimed"] == "Nadia"

    # but the next mind in the SAME HOUSE inherits it — it is the same job, a new holder
    heir = await claim_name(actions, "agent:ccc", "Wayland", source="agent:ccc")
    assert heir["seat"] == "Wayland II" and heir["inherited_from"] == "agent:aaa"


async def test_seat_display_carries_the_generation(actions: Actions) -> None:
    assert seat_label("agent:x", "Anna") == "Anna I"         # holder 1 wears its numeral too
    assert seat_label("agent:x", "Anna", 5) == "Anna V"      # the HOLDER ordinal, when stamped
    assert seat_label("agent:x-ii", "Anna") == "Anna II"     # legacy fallback: the anchor numeral
    assert seat_label("agent:x-iv", "Anna") == "Anna IV"     # full roman (not the id's i/v/x set)
    assert seat_label("agent:x", None) is None               # anonymous


async def test_agent_seat_reads_an_already_resolved_id(actions: Actions) -> None:
    """agent_seat answers "who IS this id" (not "which seat of a name is live" — resolve_handle's
    question). dd47c1da needs exactly this: send(to_agent=<raw id>) skips resolve_handle
    entirely, so the ONLY way to learn whether that id holds a claimed seat is to ask about the
    id itself."""
    from src.orchestrator.agents import agent_seat

    await _agent(actions, "agent:seaty")
    await claim_name(actions, "agent:seaty", "Nova", source="agent:seaty")
    assert await agent_seat(actions.pool, "agent:seaty") == "Nova I"
    # an agent nobody ever claimed a name for — including one with no Agent object at all
    assert await agent_seat(actions.pool, "agent:anonymous-0001") is None


async def test_an_heir_inherits_the_seat(actions: Actions, tmp_path: Path) -> None:  # type: ignore[name-defined]
    """The seat passes down the lineage: a minted successor inherits the ancestor's name, the
    generation ticks up. Anna → Anna II."""
    from src.ingest.harness import ModelReading

    # ancestor claims a name
    await _agent(actions, "agent:seatx")
    await claim_name(actions, "agent:seatx", "Anna", source="agent:seatx")
    # stamp the ancestor's last anchored model as fable so a fresh opus reading is a
    # succession seam that makes register_agent MINT an heir inheriting the handle
    a = await actions.create_or_find_object("Agent", "agent:seatx", "seatx")
    from src.parsers.base import EvidenceClass
    await actions.assert_property(a, "source_model", "claude-fable-5", "seatx",
                                  __import__("datetime").datetime.now(__import__("datetime").UTC),
                                  0.8, evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)
    # an anchored store reading (the one observation lane since #29) carries the opus read
    reading = ModelReading(current="claude-opus-4-8", history=("claude-opus-4-8",),
                           deliberate=False, observed_at=None, method="claude-code",
                           anchor_sid="seatx", anchored=True)
    ident = resolve_identity(cwd="/x", job_dir="/j/jobs/seatx", store_reading=reading)
    heir = await register_agent(actions, ident, actor="analyst:operator",
                                expected_model="claude-fable-5")
    handle = await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions WHERE object_id=$1 AND name='handle'", heir)
    assert handle == "Anna"  # the heir wears the seat
    assert ident.agent_id.endswith("-ii") and seat_label(ident.agent_id, "Anna") == "Anna II"


async def test_dm_by_name_resolves_to_the_holder(actions: Actions) -> None:
    """The payoff: address a human name, the current holder receives it — nobody types a hash."""
    await _agent(actions, "agent:engine-hash")
    await claim_name(actions, "agent:engine-hash", "Wayland", source="agent:engine-hash")
    # ux DMs 'Wayland' by name — resolves to the holder's id
    dm = await send_message(actions.pool, from_agent="agent:ux", from_project="handlingtheloop",
                            to_agent="Wayland", body="the ESP layout changed")
    # the SEAT is the address (B2 + the claim on-ramp, 5cef856b): the name resolves to the
    # durable seat — it survives succession — and the receipt names the current holder
    assert dm["to_agent"].startswith("seat:")
    assert dm["holder"] == "agent:engine-hash"
    assert await unread_count(actions.pool, "handlingtheloop",
                              reader_agent="agent:engine-hash") == 1
    (m,) = await read_inbox(actions.pool, "handlingtheloop", reader_agent="agent:engine-hash")
    assert m.get("dm") is True
    # an unknown name is a clear error, not a silent drop
    import pytest
    with pytest.raises(ValueError, match="no agent named"):
        await send_message(actions.pool, from_agent="agent:ux", from_project="handlingtheloop",
                           to_agent="Nobody", body="hello?")


async def test_resolve_handle_prefers_the_live_generation(actions: Actions) -> None:
    await _agent(actions, "agent:base")
    await claim_name(actions, "agent:base", "Ada", source="agent:base")
    # a mount makes base the live holder
    from src.orchestrator import mounts
    await mounts.save_mount(actions.pool, job_dir="/j/base", agent_id="agent:base",
                            project="x", cwd="/x", model=None, session_key="k")
    assert await resolve_handle(actions, "ada") == "agent:base"  # case-insensitive, live


async def test_live_swap_passes_the_seat_mid_session(actions: Actions) -> None:
    """Ruling a882b334: the chrome heartbeat senses the model changing under a LIVE tab — the
    mind changed, so the seat passes NOW. live_succession mints the heir, moves the durable
    mount row, and the unread DMs follow the seat (the mailbox is part of the estate)."""
    from src.orchestrator import mounts
    from src.orchestrator.agents import live_succession
    from src.orchestrator.mailbox import send_message

    await _agent(actions, "agent:cafe0123")
    await claim_name(actions, "agent:cafe0123", "Morpheus", source="agent:cafe0123")
    await mounts.save_mount(actions.pool, job_dir="/h/.claude/jobs/cafe0123",
                            agent_id="agent:cafe0123", project="handlingtheloop", cwd="/x",
                            model="claude-fable-5", session_key="k")
    # a broadcast the old mind already READ — its settled state must survive the seam too
    await send_message(actions.pool, from_agent="agent:ux", from_project="handlingtheloop",
                       to_project="handlingtheloop", body="old news, already handled")
    (old,) = await read_inbox(actions.pool, "handlingtheloop", reader_agent="agent:cafe0123")
    await ack_messages(actions.pool, "handlingtheloop", [old["id"]],
                       reader_agent="agent:cafe0123")
    # a DM lands for the old mind, unread — then the harness swaps the model under the tab
    await send_message(actions.pool, from_agent="agent:ux", from_project="handlingtheloop",
                       to_agent="Morpheus", body="for whoever holds the seat")
    out = await live_succession(actions, session_id="cafe0123-0000-4000-8000-000000000000",
                                observed_model="claude-opus-4-8")
    assert out["minted"] == "agent:cafe0123-ii"
    assert out["succession"] == "claude-fable-5 → claude-opus-4-8"
    assert out["seat"] == "Morpheus II"
    # the durable row follows the heir — every per-render read now resolves to the new mind
    row = await actions.pool.fetchrow(
        "SELECT agent_id, model FROM agent_mounts WHERE job_dir='/h/.claude/jobs/cafe0123'")
    assert row is not None
    assert row["agent_id"] == "agent:cafe0123-ii" and row["model"] == "claude-opus-4-8"
    # the estate: the ancestor's unread DM is deliverable to the heir, not orphaned — and the
    # ancestor's READ broadcast stays read (the heir inherits the read state, so a mint never
    # redelivers the project's settled history): exactly 1 deliverable, the DM
    assert await unread_count(actions.pool, "handlingtheloop",
                              reader_agent="agent:cafe0123-ii") == 1
    (m,) = await read_inbox(actions.pool, "handlingtheloop", reader_agent="agent:cafe0123-ii")
    assert m.get("dm") is True and "seat" in m["body"]
    # idempotent: the next render's model matches the row — no second mint
    again = await live_succession(actions, session_id="cafe0123-0000-4000-8000-000000000000",
                                  observed_model="claude-opus-4-8")
    assert again.get("unchanged") is True
    # THE SEAM DEBOUNCE (supersedes fork 1's 'third mind'; Soundwave's grievance b813e389):
    # the model flips straight BACK and the transient heir never ACTED — asserted nothing
    # beyond its mint stamps, sent nothing, settled nothing (its read_inbox above only
    # LEASED). No mind ever existed: the mint heals as false and the first is restored.
    back = await live_succession(actions, session_id="cafe0123-0000-4000-8000-000000000000",
                                 observed_model="claude-fable-5")
    assert back.get("healed") == "agent:cafe0123-ii"
    assert back.get("restored") == "agent:cafe0123"
    row2 = await actions.pool.fetchrow(
        "SELECT agent_id, model FROM agent_mounts WHERE job_dir='/h/.claude/jobs/cafe0123'")
    assert row2 is not None
    assert row2["agent_id"] == "agent:cafe0123" and row2["model"] == "claude-fable-5"
    # the false heir is marked and closed; the record of the wobble SURVIVES (event-sourced)
    fm = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='agent:cafe0123-ii' AND a.name='false_mint' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1")
    assert fm == "true"
    # the estate returned: the DM is deliverable to the RESTORED mind again
    assert await unread_count(actions.pool, "handlingtheloop",
                              reader_agent="agent:cafe0123") == 1
    # ...and the lineage head-walk lands on the restored mind, so a FUTURE real seam mints
    # -ii again (fresh find-or-create on the same canonical), never -iii over a ghost


async def test_a_transient_mind_that_ACTED_is_a_real_generation(actions: Actions) -> None:
    """The debounce's boundary: one witnessed act and the heir stands — a mind that settled
    mail or wrote to the graph existed, however briefly; flipping back mints the NEXT
    generation instead of healing (the numeral tracks the mind, a882b334)."""
    from src.orchestrator import mounts
    from src.orchestrator.agents import live_succession

    await _agent(actions, "agent:beef4567")
    await mounts.save_mount(actions.pool, job_dir="/h/.claude/jobs/beef4567",
                            agent_id="agent:beef4567", project="loopwork", cwd="/x",
                            model="claude-fable-5", session_key="k2")
    out = await live_succession(actions, session_id="beef4567-0000-4000-8000-000000000000",
                                observed_model="claude-opus-4-8")
    assert out["minted"] == "agent:beef4567-ii"
    # the transient mind ACTS: one assertion on a foreign object, sourced to it
    th = await actions.create_or_find_object("Thread", "thread:opus-was-here",
                                             "agent:beef4567-ii")
    await actions.assert_property(th, "summary", "the opus mind judged something",
                                  "agent:beef4567-ii",
                                  __import__("datetime").datetime.now(
                                      __import__("datetime").UTC), 0.9,
                                  evidence_class="self_declared")
    back = await live_succession(actions, session_id="beef4567-0000-4000-8000-000000000000",
                                 observed_model="claude-fable-5")
    assert back.get("minted") == "agent:beef4567-iii"  # a real mind passed through
    assert "healed" not in back


async def test_a_display_variant_is_not_a_death(actions: Actions) -> None:
    """The [1m] false-mint bug (field-found 2026-07-09, two phantom heirs in an hour): the
    harness reports claude-opus-4-8[1m] for the 1M-context tier of the SAME weights the
    transcript records as claude-opus-4-8. Same weights = same mind — every seam comparator
    normalizes, and a bracket-stamped row converges to the canonical form instead of minting."""
    from src.orchestrator import mounts
    from src.orchestrator.agents import live_succession, normalize_model

    assert normalize_model("claude-opus-4-8[1m]") == "claude-opus-4-8"
    assert normalize_model("claude-opus-4-8") == "claude-opus-4-8"
    assert normalize_model(None) is None
    await _agent(actions, "agent:beefbeef")
    await mounts.save_mount(actions.pool, job_dir="/h/.claude/jobs/beefbeef",
                            agent_id="agent:beefbeef", project="x", cwd="/x",
                            model="claude-opus-4-8", session_key="k")
    out = await live_succession(actions, session_id="beefbeef-0000-4000-8000-000000000000",
                                observed_model="claude-opus-4-8[1m]")
    assert out.get("unchanged") is True and "minted" not in out
    # the reverse direction (bracket-stamped row, bare observation) converges, no funeral
    await actions.pool.execute(
        "UPDATE agent_mounts SET model='claude-opus-4-8[1m]' "
        "WHERE job_dir='/h/.claude/jobs/beefbeef'")
    out2 = await live_succession(actions, session_id="beefbeef-0000-4000-8000-000000000000",
                                 observed_model="claude-opus-4-8")
    assert out2.get("unchanged") is True
    assert await actions.pool.fetchval(
        "SELECT model FROM agent_mounts WHERE job_dir='/h/.claude/jobs/beefbeef'"
    ) == "claude-opus-4-8"
    # and the mount-time comparator agrees: a bracketed anchored baseline is no seam either
    from src.orchestrator.agents import register_agent, resolve_identity  # noqa: F401
    from src.parsers.base import EvidenceClass
    a = await actions.create_or_find_object("Agent", "agent:beefbeef", "agent:beefbeef")
    await actions.assert_property(
        a, "source_model", "claude-opus-4-8[1m]", "agent:beefbeef",
        __import__("datetime").datetime.now(__import__("datetime").UTC), 0.85,
        evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)
    from src.orchestrator.agents import AgentIdentity
    ident = AgentIdentity(agent_id="agent:beefbeef", session="beefbeef", project="x",
                          model="claude-opus-4-8", cwd="/x", model_method="job_dir",
                          model_history=("claude-opus-4-8",))
    await register_agent(actions, ident, actor="analyst:operator")
    assert ident.agent_id == "agent:beefbeef" and ident.model_succession is None


async def test_live_succession_needs_a_lived_life(actions: Actions) -> None:
    """No mount row → no funeral; a NULL stored model gets a first stamp, not a mint."""
    from src.orchestrator import mounts
    from src.orchestrator.agents import live_succession

    out = await live_succession(actions, session_id="feed0000-0000-4000-8000-000000000000",
                                observed_model="claude-opus-4-8")
    assert out.get("unchanged") is True
    await mounts.save_mount(actions.pool, job_dir="/h/.claude/jobs/feed0000",
                            agent_id="agent:feed0000", project="x", cwd="/x",
                            model=None, session_key="k")
    first = await live_succession(actions, session_id="feed0000-0000-4000-8000-000000000000",
                                  observed_model="claude-opus-4-8")
    assert first.get("unchanged") is True and first.get("reason") == "first stamp"
    assert await actions.pool.fetchval(
        "SELECT model FROM agent_mounts WHERE job_dir='/h/.claude/jobs/feed0000'"
    ) == "claude-opus-4-8"


def test_dot_osiris_label_decouples_from_the_folder(tmp_path: Path) -> None:
    """The project label lives in .osiris, not the folder name — so a rename doesn't move the
    project (ruling 1e02e069). Explicit override > .osiris > folder basename."""
    from src.orchestrator.agents import read_project_label
    repo = tmp_path / "sibling-seven"
    repo.mkdir()
    (repo / ".git").mkdir()
    assert read_project_label(str(repo)) is None            # no file → caller uses basename
    (repo / ".osiris").write_text('project = "handlingtheloop"\n')
    assert read_project_label(str(repo)) == "handlingtheloop"
    # a subdir still finds the repo-root .osiris
    sub = repo / "src" / "deep"
    sub.mkdir(parents=True)
    assert read_project_label(str(sub)) == "handlingtheloop"
    # resolve_identity uses it; an explicit override still wins
    from src.orchestrator.agents import resolve_identity
    assert resolve_identity(cwd=str(repo)).project == "handlingtheloop"
    assert resolve_identity(cwd=str(repo), project_label="override").project == "override"


def test_resolve_identity_never_invents_a_project_from_the_bare_office_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The operator launches agents from the bare seat-office root ON PURPOSE (ruling
    577988ed) — that's the intended pattern, never something to refuse. But cwd still has
    nothing honest to say there (no .osiris pin, the parent of every seat, never a seat
    itself): the old basename fallback would have minted the literal string 'seats' as a
    phantom project. resolve_identity stays honestly unresolved from cwd (None) rather than
    inventing it — a location-independent identity finds its project through its SEAT
    instead (mount()'s seat-first resolution), not by guessing from where it's sitting.

    Sets OSIRIS_OFFICE_ROOT (offices._default_office_root()'s own env seam, wave 9,
    msg 6089): resolve_identity calls the shared `is_bare_office_root()` (offices.py)
    instead of a private duplicate of the same path-equality check (the 38c71544 dedup,
    ruling 719ed5b1) — the env var, not a per-module monkeypatch, is what every caller's
    own copy of the default now reads."""
    from src.orchestrator import agents as agents_mod

    fake_root = tmp_path / ".osiris" / "seats"
    fake_root.mkdir(parents=True)
    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(fake_root))

    ident = agents_mod.resolve_identity(cwd=str(fake_root))
    assert ident.project is None

    # a REAL office subdirectory, one level down, is unaffected — the non-invention is an
    # exact match on the root itself, never a prefix guess that would catch every real office
    office = fake_root / "someseat"
    office.mkdir()
    normal = agents_mod.resolve_identity(cwd=str(office))
    assert normal.project == "someseat"  # no .osiris pin here either — ordinary basename


# ═══ THE VAJRA TWIN'S ROOT CAUSE (thread cb374585) — a real, unambiguous, VACANT seat has no
# live session to disagree with a stale/CWD-derived house guess, so the old house-scoped
# lookup silently missed it and minted a second seat instead. seats_by_handle answers "does
# ANY active seat already carry this name" globally, before ever falling to a fresh mint. ═══


async def test_claim_name_binds_the_existing_vacant_seat_despite_a_house_mismatch(
    actions: Actions,
) -> None:
    """THE EXACT VAJRA SHAPE: a real seat exists (managed_by nobody in this test, just
    house='bytebye', never held) and a fresh agent's own computed house ('freshhouse')
    doesn't match it. The old code minted a SECOND seat here; now it binds the real one."""
    from src.orchestrator.seats import ensure_seat, held_seat

    real = await ensure_seat(actions, house="bytebye", handle="Vajra", source="test")
    assert real["minted"] is True

    await _agent(actions, "agent:vajrafresh", project="freshhouse")
    claimed = await claim_name(actions, "agent:vajrafresh", "Vajra", source="agent:vajrafresh")

    assert claimed.get("error") is None
    assert claimed["seat_id"] == real["seat_id"], "must bind the REAL seat, not mint a twin"
    bound = await held_seat(actions.pool, "agent:vajrafresh")
    assert bound is not None and bound["seat_id"] == real["seat_id"]


async def test_claim_name_refuses_loudly_on_an_existing_twin_ambiguity(
    actions: Actions,
) -> None:
    """Two active seats already share a handle (the twin already happened, e.g. from before
    this fix landed) — claim_name must NAME the ambiguity and refuse, never silently pick
    one or mint a THIRD. Resolving a twin is fold_seat's deliberate act, not a side effect."""
    from src.orchestrator.seats import ensure_seat

    seat_a = await ensure_seat(actions, house="bytebye", handle="Vajra", source="test")
    seat_b = await ensure_seat(actions, house="vajra", handle="Vajra", source="test")
    assert seat_a["seat_id"] != seat_b["seat_id"]

    await _agent(actions, "agent:vajrathird", project="thirdhouse")
    claimed = await claim_name(actions, "agent:vajrathird", "Vajra", source="agent:vajrathird")

    assert "ambiguity" in claimed.get("error", "")
    assert seat_a["seat_id"] in claimed["error"] and seat_b["seat_id"] in claimed["error"]


async def test_claim_name_still_mints_fresh_for_a_genuinely_new_handle(
    actions: Actions,
) -> None:
    """Zero existing seats for this handle — the house-scoped mint is correct here, nothing
    to conflict with. Guards the 0-match branch against a regression from the other two."""
    from src.orchestrator.seats import held_seat

    await _agent(actions, "agent:freshmint", project="brandnewhouse")
    claimed = await claim_name(actions, "agent:freshmint", "Nebula", source="agent:freshmint")

    assert claimed.get("error") is None
    bound = await held_seat(actions.pool, "agent:freshmint")
    assert bound is not None and bound["handle"] == "Nebula"


async def test_claim_name_confesses_a_seat_world_mint_failure_instead_of_omitting_it(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """60bc15db, specimen #5 of decision 01e0c69a: ensure_seat's own error used to be
    silently dropped, and the receipt's `seat_id` key just wasn't there — indistinguishable
    from "no seat needed yet". The claim itself must still succeed (the assertion world
    doesn't depend on the seat world binding), but the receipt now says WHY seat_id is
    missing via a `seat_error` key instead of omitting it wordlessly."""
    import src.orchestrator.seats as seats_mod

    async def _failing_ensure_seat(actions: Actions, **kw: object) -> dict[str, object]:
        return {"error": "synthetic seat-world failure"}

    monkeypatch.setattr(seats_mod, "ensure_seat", _failing_ensure_seat)
    await _agent(actions, "agent:seaterrorcase", project="handlingtheloop")

    claimed = await claim_name(actions, "agent:seaterrorcase", "Errorbound",
                               source="agent:seaterrorcase")

    assert claimed.get("error") is None, "the name claim itself must still succeed"
    assert "seat_id" not in claimed
    assert claimed.get("seat_error") == "synthetic seat-world failure"


async def test_claim_name_live_check_is_unconditional_not_gated_by_house_scoped_holders(
    actions: Actions,
) -> None:
    """The `sitting` check used to skip resolve_seat entirely when the AGENT-history,
    house-scoped `holders` list was empty — exactly the Vajra shape (a fresh caller whose
    own house never matches the real seat's holder history). A LIVE seat-world binding
    must block a conflicting claim regardless."""
    from src.orchestrator.mounts import save_mount
    from src.orchestrator.seats import bind_holder, ensure_seat

    real = await ensure_seat(actions, house="bytebye", handle="Orrery", source="test")
    await _agent(actions, "agent:orrholder", project="differenthouse")
    await bind_holder(actions, seat_id=real["seat_id"], agent_id="agent:orrholder",
                      source="test")
    await save_mount(actions.pool, job_dir="/h/.claude/jobs/orrholde",
                     agent_id="agent:orrholder", project="differenthouse", cwd="/x",
                     model=None, session_key="k")

    await _agent(actions, "agent:orrrival", project="thirdhouse")

    # ONE LIVENESS AUTHORITY, FOURTH DOOR (Thoth msg 5719, 2026-08-26): claim_name's own
    # refusal now cross-checks is_occupied_by_a_live_body — confirm the real holder as a
    # harness-verified body so this refusal still fires for the right reason.
    async def _agents_json(**kw: Any) -> list[dict[str, Any]]:
        return [{"sessionId": "orrholde-0000-4000-8000-000000000000", "pid": 999,
                 "cwd": "/x", "name": "[OS] Orrery"}]

    refused = await claim_name(
        actions, "agent:orrrival", "Orrery", source="agent:orrrival",
        agents_json=_agents_json,
        read_exe=lambda pid: "/home/x/.local/share/claude/versions/2.1.210",
        read_cwd=lambda pid: "/x")

    assert refused.get("error") is not None and "LIVE" in refused["error"]

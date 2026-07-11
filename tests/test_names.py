"""Phase 2 — names/seats: the model names itself, the substrate enforces uniqueness
(ruling 1e02e069). Covers claim_name (global permanent exhaustion), the roman seat display,
seat inheritance down a lineage, and DM-by-name addressing.
"""
from __future__ import annotations

from pathlib import Path

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


async def test_an_agent_names_itself_and_the_name_is_exhausted(actions: Actions) -> None:
    await _agent(actions, "agent:aaa")
    await _agent(actions, "agent:bbb")
    got = await claim_name(actions, "agent:aaa", "Wayland", source="agent:aaa")
    assert got["claimed"] == "Wayland" and got["seat"] == "Wayland"  # generation 1 = bare name
    # a DIFFERENT lineage cannot take it — permanent, global exhaustion
    refused = await claim_name(actions, "agent:bbb", "wayland", source="agent:bbb")  # case-insens
    assert "taken" in refused["error"]
    # bbb picks its own
    mine = await claim_name(actions, "agent:bbb", "Nadia", source="agent:bbb")
    assert mine["claimed"] == "Nadia"


async def test_seat_display_carries_the_generation(actions: Actions) -> None:
    assert seat_label("agent:x", "Anna") == "Anna"           # gen 1 → bare
    assert seat_label("agent:x-ii", "Anna") == "Anna II"     # roman generation
    assert seat_label("agent:x-iv", "Anna") == "Anna IV"     # full roman (not the id's i/v/x set)
    assert seat_label("agent:x", None) is None               # anonymous


async def test_an_heir_inherits_the_seat(actions: Actions, tmp_path: Path) -> None:  # type: ignore[name-defined]
    """The seat passes down the lineage: a minted successor inherits the ancestor's name, the
    generation ticks up. Anna → Anna II."""
    # ancestor claims a name
    await _agent(actions, "agent:seatx")
    await claim_name(actions, "agent:seatx", "Anna", source="agent:seatx")
    # force a succession seam so register_agent MINTS an heir that inherits the handle
    proj = tmp_path / "-x"
    proj.mkdir()
    (proj / "seatx-0000-4000-8000-000000000000.jsonl").write_text(
        __import__("json").dumps({"type": "assistant",
                                  "message": {"model": "claude-opus-4-8", "content": []}}) + "\n")
    # stamp the ancestor's last anchored model as fable so a fresh opus read is a succession seam
    a = await actions.create_or_find_object("Agent", "agent:seatx", "seatx")
    from src.parsers.base import EvidenceClass
    await actions.assert_property(a, "source_model", "claude-fable-5", "seatx",
                                  __import__("datetime").datetime.now(__import__("datetime").UTC),
                                  0.8, evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)
    ident = resolve_identity(cwd="/x", job_dir="/j/jobs/seatx", root=tmp_path)
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
    assert dm["to_agent"] == "agent:engine-hash"  # resolved name → holder id
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
    repo = tmp_path / "xxit"
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

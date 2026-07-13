"""THE MINER CLEANS UP AFTER ITSELF — and the BOUNDARIES are the whole design.

The operator, 2026-07-12: "the miner should not only shit out slop, it should also clean up and
check and balance itself on the same pass so we don't end up with a noisy garbage graph."

The fault was architectural: the miner was WRITE-ONLY. It emitted and never retracted, so every
bug in it laid permanent sediment — 81% of the graph became machine inference, and 959 of 1059
open threads were untouched guesses nobody had ever read. A memory that only accretes is a landfill.

But a janitor is a dangerous thing to own, so what it MAY NOT touch is tested harder than what it
sweeps.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.ingest.janitor import janitor_pass, wake_origins
from src.orchestrator.capture import open_thread, record_decision

NOW = datetime.now(UTC)


def _transcript(root: Path, project: str, stem: str, first_user: str) -> Path:
    d = root / project
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{stem}.jsonl"
    p.write_text(json.dumps({"type": "user", "message": {"content": first_user}}) + "\n")
    return p


async def _mined(actions: Actions, canon: str, summary: str, origin: str,
                 otype: str = "Thread") -> object:
    """A row the MINER wrote: DERIVED, sourced to the agent whose transcript it read."""
    o = await actions.create_or_find_object(otype, canon, origin)
    await actions.assert_property(o, "summary", summary, origin, NOW, 0.4,
                                  evidence_class="derived")
    if otype == "Thread":
        await actions.assert_property(o, "status", "open", origin, NOW, 0.4,
                                      evidence_class="derived")
    return o


async def test_it_sweeps_what_it_mined_from_osiris_own_alarm_clock(
    actions: Actions, tmp_path: Path,
) -> None:
    """The trigger rings its own doorbell, the woken agent talks, and the miner files the echo as
    something the fleet LEARNED. That was never knowledge — it was feedback."""
    _transcript(tmp_path, "-x-demo", "wake111", "You have unread Osiris mail. Call mount(...)")
    t = await _mined(actions, "thread:from-wake", "a thought a wake session had",
                     "agent:wake111")

    rep = await janitor_pass(actions, root=tmp_path, dry_run=False)
    assert rep["from_wake"] == 1 and rep["retracted"] == 1

    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", t)
    assert status == "retracted"     # off every open-thread lens (they all read `status`)
    why = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='retracted_because' LIMIT 1", t)
    assert "wake session Osiris spawned itself" in why


async def test_it_sweeps_what_it_plagiarised_from_a_diligent_author(
    actions: Actions, tmp_path: Path,
) -> None:
    """The ownership boundary said: backfill the SILENT, never second-guess the diligent. It never
    once fired — it compared a session's transcript-derived id against the seat it actually writes
    under — so the miner spent its life re-minting reworded copies of the very decisions its best
    authors had already written by hand."""
    _transcript(tmp_path, "-x-demo", "abc12345", "fix the renderer please")
    await actions.pool.execute(
        "INSERT INTO agent_mounts (agent_id, job_dir, cwd, last_seen) VALUES ($1,$2,$3,now()) "
        "ON CONFLICT (job_dir) DO UPDATE SET agent_id=EXCLUDED.agent_id",
        "agent:seat-vii", "/home/x/.claude/jobs/abc12345", "/repo/demo")
    for i in range(3):  # the author documents itself, deliberately, under its SEAT
        await record_decision(actions, f"a ruling I wrote by hand, {i}", source="agent:seat-vii")

    slop = await _mined(actions, "thread:slop", "a reworded copy of what he already said",
                        "agent:abc12345")
    rep = await janitor_pass(actions, root=tmp_path, dry_run=False)
    assert rep["plagiarised"] == 1

    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", slop)
    assert status == "retracted"


async def test_it_NEVER_touches_what_a_mind_touched(actions: Actions, tmp_path: Path) -> None:
    """THE ABSOLUTE GUARD. The instant an agent triages, adopts, or comments on a mined thread, it
    is THEIRS. A mind's attention is testimony, and testimony outranks the machine that produced
    the row. Even from a wake session. Even from a plagiarised one."""
    _transcript(tmp_path, "-x-demo", "wake222", "You have unread Osiris mail. Call mount(...)")
    adopted = await _mined(actions, "thread:adopted", "the miner guessed — and a mind AGREED",
                           "agent:wake222")
    # a mind engages with it: that is testimony
    await actions.assert_property(adopted, "kind", "obligation", "agent:someone", NOW, 0.9,
                                  evidence_class="self_declared")

    rep = await janitor_pass(actions, root=tmp_path, dry_run=False)
    assert rep["candidates"] == 0, "a thread a mind has touched is never the miner's to retract"
    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", adopted)
    assert status == "open"


async def test_it_NEVER_touches_a_minds_own_declaration(actions: Actions, tmp_path: Path) -> None:
    """It may retract what the MINER wrote. A deliberate open_thread is a mind's word, and the
    janitor has no standing over it — whatever its origin, whatever its age."""
    _transcript(tmp_path, "-x-demo", "wake333", "You have unread Osiris mail. Call mount(...)")
    declared = await open_thread(actions, "a duty I declared myself", source="agent:wake333")

    rep = await janitor_pass(actions, root=tmp_path, dry_run=False)
    assert rep["candidates"] == 0
    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", declared)
    assert status == "open"


async def test_it_NEVER_sweeps_on_suspicion(actions: Actions, tmp_path: Path) -> None:
    """LEXICAL SIMILARITY MAY ASK, BUT MUST NEVER ASSERT (e27f7c3). A miner row from an ORDINARY
    session — one that does not document itself, and that Osiris did not spawn — is exactly the
    backfill the miner exists to provide. Stale is not a crime. Unread is not a crime. A janitor
    that throws away what it merely suspects is worse than the mess it was cleaning."""
    _transcript(tmp_path, "-x-demo", "quiet77", "just a normal conversation")
    kept = await _mined(actions, "thread:legit", "a real loose end nobody wrote down",
                        "agent:quiet77")
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '90 days' WHERE id=$1", kept)

    rep = await janitor_pass(actions, root=tmp_path, dry_run=False)
    assert rep["candidates"] == 0, "old + unread + derived is NOT proof of garbage"


async def test_a_dry_run_writes_nothing_and_is_the_default(
    actions: Actions, tmp_path: Path,
) -> None:
    """A janitor that cannot be rehearsed is a shredder."""
    _transcript(tmp_path, "-x-demo", "wake444", "You have unread Osiris mail. Call mount(...)")
    t = await _mined(actions, "thread:dry", "would be swept", "agent:wake444")

    rep = await janitor_pass(actions, root=tmp_path)          # dry_run defaults TRUE
    assert rep["dry_run"] is True and rep["candidates"] == 1 and "retracted" not in rep
    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", t)
    assert status == "open"                                   # untouched


async def test_a_retraction_is_a_compensating_event_never_a_delete(
    actions: Actions, tmp_path: Path,
) -> None:
    """Invariant 3: heal with compensating events, NEVER DELETE. The row stays readable, auditable
    and reversible forever — the lens stops hauling it; the record never forgets we swept it."""
    _transcript(tmp_path, "-x-demo", "wake555", "You have unread Osiris mail. Call mount(...)")
    t = await _mined(actions, "thread:gone", "swept, but not erased", "agent:wake555")
    await janitor_pass(actions, root=tmp_path, dry_run=False)

    # the object still EXISTS, with its summary and its whole history intact
    assert await actions.pool.fetchval("SELECT count(*) FROM objects WHERE id=$1", t) == 1
    summary = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='summary' LIMIT 1", t)
    assert summary == "swept, but not erased"
    # and the sweep is IDEMPOTENT — a second pass does not re-retract what it already swept
    rep2 = await janitor_pass(actions, root=tmp_path, dry_run=False)
    assert rep2["candidates"] == 0


def test_a_mention_of_the_wake_prompt_is_not_a_wake_spawn(tmp_path: Path) -> None:
    """The fingerprint is the FIRST TURN, never a mention. This very session has quoted the wake
    prompt at length while diagnosing it — and its work is real."""
    _transcript(tmp_path, "-x-demo", "spawn", "You have unread Osiris mail. Call mount(...)")
    _transcript(tmp_path, "-x-demo", "about",
                "why does the wake prompt say 'You have unread Osiris mail'?")
    assert wake_origins(tmp_path) == {"spawn"}

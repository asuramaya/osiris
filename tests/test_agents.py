"""Actor identity — the fleet made first-class ("a man and all his imaginary friends").

Over the shared MCP server every agent writes through one process; without a per-agent
source their writes collapse into one bucket. These tests prove identity resolution, the
Agent registration (the org-chart links), and that a mounted agent's captures are
attributed to IT — hermetic against real Postgres.
"""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.ingest.harness import ModelReading
from src.ingest.harness.claude_jsonl import ClaudeJsonlAdapter
from src.ingest.transcript_store import _reading_from_turns
from src.orchestrator.agents import AgentIdentity, register_agent, resolve_identity
from src.orchestrator.capture import open_thread, record_decision
from src.parsers.base import EvidenceClass


def _store_reading(root: Path, job_dir: str) -> ModelReading | None:
    """The production feed minus the DB round-trip: adapter discovery + parse → reading,
    stamped with the locator's anchor grade exactly as discover_and_ingest stamps it (the
    round-trip itself is test_transcript_store's to prove). Since the JSONL-fallback
    removal (task #29) this is the ONE way a synthetic transcript reaches
    resolve_identity — it no longer probes disk itself."""
    loc = ClaudeJsonlAdapter().discover(cwd=None, job_dir=job_dir, root=root)
    if loc is None:
        return None
    turns = list(ClaudeJsonlAdapter().read_turns(loc))
    return replace(_reading_from_turns(turns, loc.harness, loc.anchor_sid),
                   anchored=loc.anchored)


def test_resolve_identity_derives_project_and_session(tmp_path: Path) -> None:
    # root=tmp_path (empty) → nothing to observe → the passed model is the self-report fallback
    ident = resolve_identity(
        cwd="/home/x/code/sibling-two",
        job_dir="/home/x/.claude/jobs/ad1a1cb0", model="claude-opus-4-8", root=tmp_path,
    )
    assert ident.project == "sibling-two"
    assert ident.session == "ad1a1cb0"        # the job id
    assert ident.agent_id == "agent:ad1a1cb0"  # the provenance source
    assert ident.model == "claude-opus-4-8"


def test_resolve_identity_unknown_is_still_a_valid_actor() -> None:
    ident = resolve_identity(cwd=None, job_dir=None)
    assert ident.agent_id == "agent:unknown"   # coarse, but never crashes the mount


async def test_register_agent_mints_the_org_chart(actions: Actions) -> None:
    ident = resolve_identity(cwd="/w/sibling-one", session="sess-h", model="claude-fable-5")
    a = await register_agent(actions, ident, actor="analyst:operator")
    # the Agent object carries its model (source-model provenance) + project
    row = await actions.pool.fetchrow(
        "SELECT (SELECT value#>>'{}' FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='source_model') AS model, "
        " (SELECT value#>>'{}' FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='project') AS project", a)
    assert row["model"] == "claude-fable-5" and row["project"] == "sibling-one"
    # works_in -> the project, acts_for -> the principal (the org chart)
    links = await actions.pool.fetch(
        "SELECT l.type, o.canonical FROM links l JOIN objects o ON o.id=l.to_id "
        "WHERE l.from_id=$1 ORDER BY l.type", a)
    got = {r["type"]: r["canonical"] for r in links}
    assert got["works_in"] == "repo:sibling-one"
    assert got["acts_for"] == "principal:analyst:operator"
    # re-mount is idempotent (find-or-create + byte-dup skip): no second Agent, no dup links
    a2 = await register_agent(actions, ident, actor="analyst:operator")
    assert a2 == a
    assert await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Agent'") == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='works_in'", a) == 1


async def test_two_agents_are_distinguishable_on_the_same_decision(actions: Actions) -> None:
    """The whole point: on the shared server, two instances recording the SAME ruling dedup
    to one object but each keeps its OWN provenance — you can see both agreed, and who."""
    a1 = resolve_identity(cwd="/w/osiris", session="aaaa", model="claude-opus-4-8")
    a2 = resolve_identity(cwd="/w/osiris", session="bbbb", model="claude-fable-5")
    d1 = await record_decision(actions, "the fleet shares one graph", source=a1.agent_id)
    d2 = await record_decision(actions, "the fleet shares one graph", source=a2.agent_id)
    assert d1 == d2  # same canonical → one object (find-or-create)
    sources = await actions.pool.fetch(
        "SELECT DISTINCT source_id FROM assertions WHERE object_id=$1 AND name='summary'", d1)
    assert {r["source_id"] for r in sources} == {"agent:aaaa", "agent:bbbb"}


def test_schema_declares_the_actor_types() -> None:
    from src.ontology.schema import LINK_TYPES, OBJECT_TYPES

    assert "Agent" in OBJECT_TYPES
    assert "acts_for" in LINK_TYPES and "works_in" in LINK_TYPES


def test_resolve_identity_tags_the_model_method() -> None:
    # a passed-in model is the agent's OWN WORD (self-report), tagged as such
    assert resolve_identity(cwd="/w/osiris", session="s",
                            model="claude-fable-5").model_method == "self_report"
    # no model resolvable anywhere → no method (and no false certainty)
    ident = resolve_identity(cwd=None, job_dir=None)
    assert ident.model is None and ident.model_method is None


async def test_source_model_is_graded_by_resolution_method(actions: Actions) -> None:
    """The freebie: source_model's evidence grade IS how it was resolved. A model read off the
    agent's own transcript is a DIRECT_OBSERVATION of the harness record; a self-reported model
    is only the agent's own word — the WEAKEST signal (CO_OCCURRENCE, ruling 17516660: observation
    outranks self-report for a substrate-fact)."""
    async def _model_ec(ident: AgentIdentity) -> str | None:
        a = await register_agent(actions, ident, actor="analyst:operator")
        return await actions.pool.fetchval(  # type: ignore[no-any-return]
            "SELECT evidence_class FROM current_assertions "
            "WHERE object_id=$1 AND name='source_model'", a)

    observed = AgentIdentity(agent_id="agent:obs", session="obs", project="osiris",
                             model="claude-opus-4-8", cwd="/w/osiris", model_method="job_dir")
    self_reported = AgentIdentity(agent_id="agent:sr", session="sr", project="osiris",
                                  model="claude-fable-5", cwd="/w/osiris",
                                  model_method="self_report")
    assert await _model_ec(observed) == EvidenceClass.DIRECT_OBSERVATION.value
    assert await _model_ec(self_reported) == EvidenceClass.CO_OCCURRENCE.value  # weakest, inverted


def _transcript(dir_: Path, model: str) -> None:
    (dir_ / "deadbeef-1111-2222-3333-444455556666.jsonl").write_text(
        json.dumps({"type": "assistant", "message": {"model": model, "content": []}}) + "\n")


def test_observation_outranks_self_report_and_flags_divergence(tmp_path: Path) -> None:
    """Ruling 17516660: the harness doesn't lie. A transcript that RAN opus + an agent that
    SELF-REPORTS fable → observation wins, and the divergence is flagged (a self-report that
    lies is a flag)."""
    proj = tmp_path / "-home-x-code-osiris"
    proj.mkdir()
    _transcript(proj, "claude-opus-4-8")
    ident = resolve_identity(cwd="/x/osiris", job_dir="/j/jobs/deadbeef",
                             model="claude-fable-5",
                             store_reading=_store_reading(tmp_path, "/j/jobs/deadbeef"))
    assert ident.model == "claude-opus-4-8"          # OBSERVATION wins over the self-report
    assert ident.model_method == "job_dir"           # an anchored store reading = the anchor grade
    assert ident.model_declared == "claude-fable-5"  # the agent's word is kept...
    assert ident.model_divergent is True             # ...and flagged as divergent


async def test_register_records_the_divergence_flag(actions: Actions, tmp_path: Path) -> None:
    """A divergent identity registers BOTH: source_model (observed, DIRECT_OBSERVATION) and
    source_model_declared (the agent's word, CO_OCCURRENCE). The mismatch IS the flag."""
    proj = tmp_path / "-home-x-code-osiris"
    proj.mkdir()
    _transcript(proj, "claude-opus-4-8")
    ident = resolve_identity(cwd="/x/osiris", job_dir="/j/jobs/deadbeef",
                             model="claude-fable-5",
                             store_reading=_store_reading(tmp_path, "/j/jobs/deadbeef"))
    a = await register_agent(actions, ident, actor="analyst:operator")
    row = await actions.pool.fetchrow(
        "SELECT (SELECT value#>>'{}' FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='source_model') AS observed, "
        " (SELECT evidence_class FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='source_model') AS obs_ec, "
        " (SELECT value#>>'{}' FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='source_model_declared') AS declared, "
        " (SELECT evidence_class FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='source_model_declared') AS dec_ec", a)
    assert row["observed"] == "claude-opus-4-8"
    assert row["obs_ec"] == EvidenceClass.DIRECT_OBSERVATION.value
    assert row["declared"] == "claude-fable-5"       # kept as the weak signal — the flag
    assert row["dec_ec"] == EvidenceClass.CO_OCCURRENCE.value


def _transcript_lines(dir_: Path, *models: str) -> None:
    """A transcript with one assistant line per model, in order — a within-session swap when it
    carries >1 distinct model."""
    lines = [json.dumps({"type": "assistant", "message": {"model": m, "content": []}})
             for m in models]
    (dir_ / "deadbeef-1111-2222-3333-444455556666.jsonl").write_text("\n".join(lines) + "\n")


def test_resolve_identity_captures_the_swap_history(tmp_path: Path) -> None:
    """The swap-detector's input: resolve_identity captures the transcript's model sequence, so a
    within-session fable→opus demotion is visible as model_history (ruling f2ae6346)."""
    proj = tmp_path / "-home-x-code-osiris"
    proj.mkdir()
    _transcript_lines(proj, "claude-fable-5", "claude-opus-4-8")
    ident = resolve_identity(cwd="/x/osiris", job_dir="/j/jobs/deadbeef",
                             store_reading=_store_reading(tmp_path, "/j/jobs/deadbeef"))
    assert ident.model == "claude-opus-4-8"  # the latest turn
    assert ident.model_history == ("claude-fable-5", "claude-opus-4-8")  # the whole transition
    assert ident.model_deliberate is False   # no /model on the record → a rug-pull if swapped


def test_resolve_identity_sees_the_operators_own_model_command(tmp_path: Path) -> None:
    """A /model command in the transcript is the OPERATOR's hand on the record: the swap was
    chosen, not suffered — downstream banners drop the sin framing (complaint, 2026-07-10)."""
    proj = tmp_path / "-home-x-code-osiris"
    proj.mkdir()
    lines = [
        json.dumps({"type": "assistant", "message": {"model": "claude-fable-5", "content": []}}),
        json.dumps({"type": "user", "message": {
            "role": "user", "content": "<command-name>/model</command-name>\n"
                                       "<command-message>model</command-message>"}}),
        json.dumps({"type": "assistant",
                    "message": {"model": "claude-haiku-4-5", "content": []}}),
    ]
    (proj / "deadbeef-1111-2222-3333-444455556666.jsonl").write_text("\n".join(lines) + "\n")
    ident = resolve_identity(cwd="/x/osiris", job_dir="/j/jobs/deadbeef",
                             store_reading=_store_reading(tmp_path, "/j/jobs/deadbeef"))
    assert ident.model_history == ("claude-fable-5", "claude-haiku-4-5")
    assert ident.model_deliberate is True


def test_read_project_model_declares_the_repo_intent(tmp_path: Path) -> None:
    """.osiris `model = "…"` is the repo's OWN standing choice — a deliberately-haiku repo
    must not confess 'not fable' at every mount against the box default."""
    from src.orchestrator.agents import read_project_model

    repo = tmp_path / "hollow"
    (repo / "src").mkdir(parents=True)
    (repo / ".git").mkdir()
    (repo / ".osiris").write_text('project = "hollow-pit"\nmodel = "claude-haiku-4-5"\n')
    assert read_project_model(str(repo / "src")) == "claude-haiku-4-5"  # walks up
    assert read_project_model(str(tmp_path)) is None                    # undeclared → default


async def test_register_stamps_intent_and_the_swap(actions: Actions, tmp_path: Path) -> None:
    """With expected_model set, register_agent stamps the INTENT and, on a divergence (the fable
    harness's silent demotion), the swap as a first-class OBSERVED event on the Agent."""
    proj = tmp_path / "-home-x-code-osiris"
    proj.mkdir()
    _transcript_lines(proj, "claude-fable-5", "claude-opus-4-8")
    ident = resolve_identity(cwd="/x/osiris", job_dir="/j/jobs/deadbeef",
                             store_reading=_store_reading(tmp_path, "/j/jobs/deadbeef"))
    a = await register_agent(actions, ident, actor="analyst:operator",
                             expected_model="claude-fable-5")
    row = await actions.pool.fetchrow(
        "SELECT (SELECT value#>>'{}' FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='model_intent') AS intent, "
        " (SELECT value#>>'{}' FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='model_swapped') AS swapped, "
        " (SELECT evidence_class FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='model_swapped') AS swap_ec", a)
    assert row["intent"] == "claude-fable-5"
    assert row["swapped"] == "claude-fable-5 ↔ claude-opus-4-8 (now claude-opus-4-8)"
    assert row["swap_ec"] == EvidenceClass.DIRECT_OBSERVATION.value


def _anchored(model: str, *, history: tuple[str, ...] | None = None) -> AgentIdentity:
    """A job_dir-anchored identity for agent:0806072e — the succession-seam fixture (bug #51)."""
    return AgentIdentity(agent_id="agent:0806072e", session="0806072e", project="sibling-two",
                         model=model, cwd="/w/sibling-two", model_method="job_dir",
                         model_history=history if history is not None else (model,))


async def test_succession_seam_mints_a_lineage_linked_heir(actions: Actions) -> None:
    """The MINT ruling (be292762, sibling-one's remedy adopted): a fresh context arriving across a
    detected seam is not stamped-and-left-wearing-the-dead-name — it is MINTED its own id
    (agent:<base>-ii) with a succeeded_from link; the ancestor's record closes intact."""
    dead = _anchored("claude-opus-4-8")
    a = await register_agent(actions, dead, actor="analyst:operator")
    assert dead.model_succession is None  # first anchored write — no baseline, no seam
    successor = _anchored("claude-fable-5")  # fresh context: opus is NOWHERE in its history
    a2 = await register_agent(actions, successor, actor="analyst:operator")
    assert a2 != a                                        # a NEW being, not the dead name re-worn
    assert successor.agent_id == "agent:0806072e-ii"      # sibling-one's grammar, exactly
    assert successor.succeeded_from == "agent:0806072e"
    assert successor.model_succession == "claude-opus-4-8 → claude-fable-5"
    # the seam is stamped on the HEIR; the ancestor points forward; the graph edge exists
    row = await actions.pool.fetchrow(
        "SELECT value#>>'{}' AS seam, evidence_class AS ec FROM current_assertions "
        "WHERE object_id=$1 AND name='model_succession'", a2)
    assert row is not None and row["seam"] == "claude-opus-4-8 → claude-fable-5"
    assert row["ec"] == EvidenceClass.DIRECT_OBSERVATION.value
    assert await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='succeeded_by'", a) == "agent:0806072e-ii"
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='succeeded_from'", a2, a)
    # IDEMPOTENT: the same context re-registering resolves the BASE id to the lineage head and
    # continues AS the heir — no -iii, no seam re-fire
    again = _anchored("claude-fable-5")
    a3 = await register_agent(actions, again, actor="analyst:operator")
    assert a3 == a2 and again.agent_id == "agent:0806072e-ii"
    assert again.succeeded_from is None and again.model_succession is None
    # SUCCESSION FOLLOWS TURNS (ruling d3531cd8): "-ii" is a WITNESSED mind (this decision
    # is its act) — without one, the next seam would fold it as a zero-turn phantom instead
    # of chaining a third generation onto it (see test_fold_zero_turn_ancestors_* in this
    # file for the direct unit coverage of that fold logic).
    await record_decision(actions, "fable did real work here", source=successor.agent_id)
    # a SECOND real seam (fable head → haiku context) mints the third generation
    third = _anchored("claude-haiku-4-5-20251001")
    a4 = await register_agent(actions, third, actor="analyst:operator")
    assert third.agent_id == "agent:0806072e-iii"
    assert third.succeeded_from == "agent:0806072e-ii"
    assert a4 not in (a, a2)


async def test_remount_of_a_retired_identity_mints_an_heir(actions: Actions) -> None:
    """The reanimation door under the mint ruling: a retired face is never re-worn — the
    arriving context is minted the next generation; the retirement stands on the ancestor."""
    ident = _anchored("claude-fable-5")
    a = await register_agent(actions, ident, actor="analyst:operator")
    assert ident.reanimated is False and ident.succeeded_from is None
    # the agent retires itself (what retire() writes: retired=true, self_declared)
    await actions.assert_property(a, "retired", True, ident.agent_id, datetime.now(UTC), 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    # the same session UUID mounts again — the door sibling-two repro'd, now a minting
    again = _anchored("claude-fable-5")
    a2 = await register_agent(actions, again, actor="analyst:operator")
    assert a2 != a
    assert again.agent_id == "agent:0806072e-ii"
    assert again.succeeded_from == "agent:0806072e"
    assert again.reanimated is False  # nothing was re-worn — the heir has its own name
    assert await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='minted_because'", a2) == "reanimation-of-retired"
    # the retirement STANDS on the ancestor — the deliberate close is never erased
    retired = await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions WHERE object_id=$1 AND name='retired'", a)
    assert retired == "true"


def test_generation_math_is_hex_safe() -> None:
    """Roman generations use only {i,v,x} — no hex digit overlaps, so full-UUID canonicals can
    never misparse their tails as generations ('d'/'c' are Roman AND hex; we exclude them)."""
    from src.orchestrator.agents import _generation, next_generation

    assert next_generation("agent:a8c15486") == "agent:a8c15486-ii"
    assert next_generation("agent:a8c15486-ii") == "agent:a8c15486-iii"
    assert next_generation("agent:a8c15486-iii") == "agent:a8c15486-iv"
    assert next_generation("agent:a8c15486-ix") == "agent:a8c15486-x"
    # a full-UUID canonical whose tail is hex (and Roman-lookalike in the WIDE alphabet)
    assert _generation("agent:2f81c6d5-9e70-44d1-8f3c-0a7cd0e63f21") == (
        "agent:2f81c6d5-9e70-44d1-8f3c-0a7cd0e63f21", 1)
    assert _generation("agent:wake-19") == ("agent:wake-19", 1)      # numeric tail, not roman
    assert _generation("agent:x-dc") == ("agent:x-dc", 1)            # 'dc' is hex-ish, rejected
    assert _generation("agent:x-iiii") == ("agent:x-iiii", 1)        # malformed roman, rejected
    assert _generation("agent:x-ii") == ("agent:x", 2)


async def test_fresh_register_is_never_a_reanimation(actions: Actions) -> None:
    """A never-retired identity re-mounting is an ordinary re-attach — no reanimation flag, no
    stamp. The guard must fire ONLY on a winning retired=true, or every re-mount cries wolf."""
    ident = _anchored("claude-fable-5")
    a = await register_agent(actions, ident, actor="analyst:operator")
    again = _anchored("claude-fable-5")
    await register_agent(actions, again, actor="analyst:operator")
    assert again.reanimated is False
    row = await actions.pool.fetchrow(
        "SELECT 1 FROM current_assertions WHERE object_id=$1 AND name='reanimated'", a)
    assert row is None


async def test_witnessed_swap_mints_under_the_mind_ruling(actions: Actions) -> None:
    """Ruling a882b334 flips the old warm-swap exemption: a transition the transcript witnessed
    is STILL a death — the numeral tracks which MIND, and a mind is one contiguous run of one
    model. The witnessed swap mints the heir AND keeps the warm-swap stamp (both are true)."""
    first = _anchored("claude-fable-5")
    await register_agent(actions, first, actor="analyst:operator")
    swapped = _anchored("claude-opus-4-8",
                        history=("claude-fable-5", "claude-opus-4-8"))  # the prior IS in history
    a = await register_agent(actions, swapped, actor="analyst:operator",
                             expected_model="claude-fable-5")
    assert swapped.agent_id == "agent:0806072e-ii"      # witnessed or not, the mind changed
    assert swapped.model_succession == "claude-fable-5 → claude-opus-4-8"
    row = await actions.pool.fetchrow(
        "SELECT (SELECT value#>>'{}' FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='model_succession') AS seam, "
        " (SELECT value#>>'{}' FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='model_swapped') AS swapped, "
        " (SELECT value#>>'{}' FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='minted_because') AS why", a)
    assert row is not None
    assert row["seam"] == "claude-fable-5 → claude-opus-4-8"   # a succession...
    assert row["swapped"] is not None                          # ...AND the warm swap record
    assert row["why"] == "model-succession"


async def test_oscillation_mints_every_time_a_mind_LIVED_in_the_middle(actions: Actions) -> None:
    """Fork 1 of ruling a882b334 (fable → opus → fable is THREE minds, no same-as-grandfather
    exception) — RECONCILED with the debounce ruling that later carved its one exception
    (b813e389: an actless transient inside the window is settings churn, not a death) and
    with the operator's 2026-07-14 word extending that exception to EVERY mint path (thread
    a3d49d91 — the onboarding night burned thirteen numerals on exactly this). The returning
    model is a third mind IF THE MIDDLE ONE EVER LIVED; a mind is witnessed by its acts. The
    actless case is pinned in test_onboarding_seams (the heal), the LIVED case here."""
    await register_agent(actions, _anchored("claude-fable-5"), actor="analyst:operator")
    swap1 = _anchored("claude-opus-4-8", history=("claude-fable-5", "claude-opus-4-8"))
    await register_agent(actions, swap1, actor="analyst:operator")
    assert swap1.agent_id == "agent:0806072e-ii"
    # the middle mind ACTS — a real mind passed through, however briefly (one witnessed act
    # is the debounce's own line: it must stand as a generation forever)
    t = await actions.create_or_find_object("Thread", "thread:oscwitness", swap1.agent_id)
    await actions.assert_property(t, "status", "open", swap1.agent_id, datetime.now(UTC),
                                  0.9, evidence_class="self_declared")
    back = _anchored("claude-fable-5",
                     history=("claude-fable-5", "claude-opus-4-8", "claude-fable-5"))
    a3 = await register_agent(actions, back, actor="analyst:operator")
    assert back.agent_id == "agent:0806072e-iii"        # a THIRD mind, not I restored
    assert back.succeeded_from == "agent:0806072e-ii"
    assert back.model_succession == "claude-opus-4-8 → claude-fable-5"
    # settled: same model re-registering on the head is the same mind — no -iv
    still = _anchored("claude-fable-5",
                      history=("claude-fable-5", "claude-opus-4-8", "claude-fable-5"))
    assert await register_agent(actions, still, actor="analyst:operator") == a3
    assert still.agent_id == "agent:0806072e-iii"


async def test_succession_needs_anchored_observations_on_both_sides(actions: Actions) -> None:
    """Only two ANCHORED observations disagreeing can witness a seam: a self-report on the new
    side, or a cwd guess as the baseline, must fire nothing (the cry-wolf lesson, e71b408f)."""
    await register_agent(actions, _anchored("claude-opus-4-8"), actor="analyst:operator")
    self_rep = AgentIdentity(agent_id="agent:0806072e", session="0806072e", project="sibling-two",
                             model="claude-fable-5", cwd="/w/sibling-two",
                             model_method="self_report")
    a = await register_agent(actions, self_rep, actor="analyst:operator")
    assert self_rep.model_succession is None    # the new side is only the agent's word
    # ...and a weak-grade baseline can't witness either: cwd guess first, anchored read second
    guess = AgentIdentity(agent_id="agent:c0ffee00", session="c0ffee00", project="sibling-two",
                          model="claude-opus-4-8", cwd="/w/sibling-two", model_method="cwd")
    await register_agent(actions, guess, actor="analyst:operator")
    anchored2 = AgentIdentity(agent_id="agent:c0ffee00", session="c0ffee00",
                              project="sibling-two", model="claude-fable-5",
                              cwd="/w/sibling-two", model_method="job_dir",
                              model_history=("claude-fable-5",))
    b = await register_agent(actions, anchored2, actor="analyst:operator")
    assert anchored2.model_succession is None   # no anchored baseline → no seam
    for obj in (a, b):
        seam = await actions.pool.fetchval(
            "SELECT value#>>'{}' FROM current_assertions "
            "WHERE object_id=$1 AND name='model_succession'", obj)
        assert seam is None


async def test_a_recovered_from_unknown_model_is_not_a_seam(actions: Actions) -> None:
    """Thread 065c374e (defense-in-depth for forks.py's lesson, mirrored here): a job_dir-
    anchored mount whose OWN transcript read flaked — model=None, the ABSENCE of an
    observation, never a value — followed by a LATER call that resolves the real model must
    not read as a live-swap. 'We have not looked yet' is not a prior model to disagree with,
    so the second call gets no baseline, no seam, and no spurious generation."""
    flaked = AgentIdentity(agent_id="agent:33086f67", session="33086f67", project="osiris",
                          model=None, cwd="/w/osiris", model_method="job_dir")
    a = await register_agent(actions, flaked, actor="analyst:operator")
    assert flaked.model_succession is None and flaked.succeeded_from is None
    recovered = AgentIdentity(agent_id="agent:33086f67", session="33086f67", project="osiris",
                              model="claude-fable-5", cwd="/w/osiris", model_method="job_dir",
                              model_history=("claude-fable-5",))
    a2 = await register_agent(actions, recovered, actor="analyst:operator")
    assert recovered.model_succession is None      # no baseline was ever witnessed — not a seam
    assert recovered.succeeded_from is None
    assert a2 == a                                  # same object — no spurious generation minted
    assert recovered.agent_id == "agent:33086f67"   # no -ii


def test_unresolved_identities_do_not_conflate(tmp_path: Path) -> None:
    """Hardening: two agents that can't resolve a session id must NOT collapse into one shared
    agent:unknown sink (an accidental identity merge). Distinct anchors → distinct ids."""
    # no job_dir, no findable transcript → project-scoped fallback, distinct per project
    a = resolve_identity(cwd="/w/sibling-one", root=tmp_path)
    b = resolve_identity(cwd="/w/sibling-two", root=tmp_path)
    assert a.agent_id == "agent:unknown-sibling-one" and b.agent_id == "agent:unknown-sibling-two"
    assert a.agent_id != b.agent_id                        # never the same sink
    assert a.resolved is False and b.resolved is False
    # a job_dir is a per-session anchor even when its id won't parse (no 'jobs' path segment)
    c = resolve_identity(cwd="/w/x", job_dir="/weird/box-42", root=tmp_path)
    d = resolve_identity(cwd="/w/x", job_dir="/weird/box-99", root=tmp_path)
    assert c.agent_id != d.agent_id and c.agent_id.startswith("agent:j")


def test_cwd_located_identity_is_not_marked_resolved(tmp_path: Path) -> None:
    """#HIGH (audit): without a session/job_dir ANCHOR, resolve_identity GUESSES the session from
    the hottest cwd transcript — which concurrent same-project sessions would all grab, silently
    merging. Mark it unresolved so the fleet digest SURFACES the ambiguity, not a false green.
    Since the JSONL-fallback removal (#29) the guess finds a SID only — never a model: with no
    store reading the model honestly falls to the self-report."""
    proj = tmp_path / "-home-x-code-osiris"
    proj.mkdir()
    _transcript(proj, "claude-opus-4-8")
    ident = resolve_identity(cwd="/home/x/code/osiris", root=tmp_path,  # no anchor → cwd guess
                             model="claude-opus-4-8")
    assert ident.session == "deadbeef"       # the sid guess still lands (the hottest stem)
    assert ident.model == "claude-opus-4-8"  # the self-report fallback, honestly graded...
    assert ident.model_method == "self_report"
    assert ident.project == "osiris"
    assert ident.resolved is False           # ...and flagged NOT confident — the digest can see it


def test_cwd_fallback_neighbor_swap_does_not_cry_wolf(tmp_path: Path) -> None:
    """The live cry-wolf (bonus bug, agent e71b408f): with no job_dir anchor, the hottest cwd
    transcript may be a CONCURRENT NEIGHBOR's — if that neighbor was warm-swapped (fable→haiku),
    its demotion must NEVER be confessed as ours. Since the JSONL-fallback removal (#29) the cure
    is total: the cwd lane reads no model at all (the adapter refuses unanchored discovery too),
    so there is nothing to cry wolf WITH — the sid guess lands, the model stays unobserved."""
    from src.orchestrator.swaps import classify_swap, swap_banner

    proj = tmp_path / "-home-x-code-osiris"
    proj.mkdir()
    _transcript_lines(proj, "claude-fable-5", "claude-haiku-4-5-20251001")  # a neighbor's swap
    # the adapter is the new home of the refusal: an unmatched job_dir discovers NOTHING,
    # never the box-wide-hottest neighbor
    assert ClaudeJsonlAdapter().discover(
        cwd="/home/x/code/osiris", job_dir="/j/jobs/beefbeef", root=tmp_path) is None
    ident = resolve_identity(cwd="/home/x/code/osiris", root=tmp_path)  # no job_dir → cwd guess
    assert ident.session == "deadbeef"                  # the sid guess still lands...
    assert ident.model is None                          # ...but NO model rides it
    assert ident.model_method is None
    assert ident.model_history == ()
    assert ident.resolved is False
    # and with nothing observed there is no swap to classify — the banner stays silent
    banner = swap_banner(classify_swap(ident.model_history, ident.model,
                                       expected="claude-fable-5",
                                       anchored=ident.model_method == "job_dir"))
    assert banner is None


async def test_register_does_not_stamp_swap_off_a_cwd_guess(
    actions: Actions, tmp_path: Path
) -> None:
    """The stamp path mirrors the banner: a cwd-grade identity diverging from the intent must NOT
    stamp model_swapped on the Agent (register_agent gates it on the job_dir anchor too)."""
    proj = tmp_path / "-home-x-code-osiris"
    proj.mkdir()
    _transcript_lines(proj, "claude-fable-5", "claude-haiku-4-5-20251001")
    ident = resolve_identity(cwd="/home/x/code/osiris", root=tmp_path)  # cwd guess (haiku neighbor)
    a = await register_agent(actions, ident, actor="analyst:operator",
                             expected_model="claude-fable-5")
    swapped = await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions "
        "WHERE object_id=$1 AND name='model_swapped'", a)
    assert swapped is None  # no cry-wolf stamp off a neighbor's transcript
    intent = await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions WHERE object_id=$1 AND name='model_intent'", a)
    assert intent == "claude-fable-5"  # the intent is still stamped (the honest half)


def _write_transcript(path: Path, model: str, *, cwd: str = "/w/demo") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps({"type": "assistant", "cwd": cwd,
                       "message": {"model": model, "content": [{"type": "text", "text": "hi"}]}})
    path.write_text(line + "\n")


def test_parent_mount_never_steals_a_hot_subagents_identity(tmp_path: Path) -> None:
    """Reversal of the old active_subagent behavior (thread 0344e536): anchoring on a HOTTER
    subagents/ transcript stole the PARENT's identity whenever a BACKGROUND sub-agent ran
    concurrently (the parent keeps calling mount() while the child writes). The scar lives in
    the ADAPTER now (the identity path's one discovery door since #29): it anchors ONLY on the
    parent's own main transcript — even with a hotter child, the mount resolves to the PARENT.
    A sub-agent that mounts is captured by the miner from disk instead."""
    import os
    import time as _t

    root = tmp_path / "projects"
    job = tmp_path / "jobs" / "ab12cd34"
    main = root / "-w-demo" / "ab12cd34-0000-4000-8000-000000000000.jsonl"
    _write_transcript(main, "claude-fable-5")
    child = main.with_suffix("") / "subagents" / "agent-a7f00baby.jsonl"
    _write_transcript(child, "claude-haiku-4-5-20251001")
    old = _t.time() - 3600
    os.utime(main, (old, old))  # child hotter than parent — the theft condition
    ident = resolve_identity(cwd="/w/demo", job_dir=str(job),
                             store_reading=_store_reading(root, str(job)))
    assert ident.agent_id == "agent:ab12cd34"   # the PARENT, never the hot child
    assert ident.model == "claude-fable-5" and ident.model_method == "job_dir"


def test_claimed_sid_is_refused_by_the_cwd_guess(tmp_path: Path) -> None:
    """Two anchorless same-project sessions grab the same hottest transcript — the second must
    REFUSE the claimed sid and fall to a deterministic per-client fallback, never merge."""
    root = tmp_path / "projects"
    _write_transcript(root / "-w-demo" / "cafe0001-0000-4000-8000-000000000000.jsonl",
                      "claude-fable-5")
    first = resolve_identity(cwd="/w/demo", root=root, fallback_seed="sid:alpha")
    assert first.agent_id == "agent:cafe0001" and first.resolved is False  # the guess, flagged
    second = resolve_identity(cwd="/w/demo", root=root,
                              claimed={"cafe0001"}, fallback_seed="sid:beta")
    assert second.agent_id != "agent:cafe0001"        # refused the taken sid
    assert second.agent_id.startswith("agent:s")      # per-client deterministic fallback
    assert second.resolved is False
    # deterministic: the same client re-resolving gets the SAME fallback id
    again = resolve_identity(cwd="/w/demo", root=root,
                             claimed={"cafe0001"}, fallback_seed="sid:beta")
    assert again.agent_id == second.agent_id
    # distinct clients get distinct fallbacks — never a shared bucket
    other = resolve_identity(cwd="/w/demo", root=root,
                             claimed={"cafe0001"}, fallback_seed="sid:gamma")
    assert other.agent_id != second.agent_id


async def test_the_house_the_seat_and_the_holders(actions: Actions) -> None:
    """THE OPERATOR'S RULING (2026-07-12): "the project name is the house (sibling-eight), each
    function/job has a name (Ra), the holder dies and multiplies (ra I, ra II), but splitting to
    Ptah would break and confuse the lineage, and the fragmentation of agents was a bug in and
    of itself."

    A seat belongs to a HOUSE and is held by successive minds. The old model keyed a name to the
    ANCHOR, so when a conversation ended its name died with it: the next mind in the house reached
    for the family name, was refused as a stranger, and took a new one. That is how Ra became Ptah
    and Soundwave became "Soundwave VIII". Nothing merges — a writer's work stays its own — but
    the SEAT outlives its holders."""
    from src.orchestrator.agents import claim_name

    async def mind(canon: str, house: str) -> str:
        a = await actions.create_or_find_object("Agent", canon, "session")
        await actions.assert_property(a, "project", house, "session", datetime.now(UTC), 0.9)
        return canon

    first = await mind("agent:aaaa1111", "sibling-two")
    assert (await claim_name(actions, first, "Soundwave", source=first))["seat"] == "Soundwave I"

    # a SEAT LABEL is what the substrate says about you — never a name you may take
    heir_anchor = await mind("agent:bbbb2222", "sibling-two")
    bad = await claim_name(actions, heir_anchor, "Soundwave VIII", source=heir_anchor)
    assert "SEAT LABEL" in bad["error"] and "Soundwave" in bad["error"]

    # THE RULING: a NEW CONVERSATION in the same house INHERITS the seat — it is Soundwave II,
    # not a stranger forced to invent a name. This is the exact refusal that created "Ptah".
    heir = await claim_name(actions, heir_anchor, "Soundwave", source=heir_anchor)
    assert heir["seat"] == "Soundwave II"
    assert heir["generation"] == 2 and heir["inherited_from"] == first
    assert heir["house"] == "sibling-two"

    # THE SUCCESSION EDGE (Ra V's ask, msg 374): before this, successor seats carried NO edge to
    # their ancestor, so a lineage was not WALKABLE from the record — which is exactly why Ra
    # could not tell his CONTEMPORARY from his own ghost, and asked me to merge them.
    walked = await actions.pool.fetchval(
        "SELECT dst.canonical FROM links l "
        "JOIN objects src ON src.id=l.from_id JOIN objects dst ON dst.id=l.to_id "
        "WHERE src.canonical=$1 AND l.type='succeeds_seat'", heir_anchor)
    assert walked == first

    # a seat belongs to ONE house: a mind in another house may not take it
    outsider = await mind("agent:cccc3333", "sibling-one")
    assert "another house" in (await claim_name(
        actions, outsider, "Soundwave", source=outsider))["error"]


async def test_you_do_not_take_a_living_minds_name(actions: Actions) -> None:
    """RA V'S REFINEMENT (sibling-eight, msg 374), and it is the case that breaks the naive model.
    He and his predecessor were not sequential — they OVERLAPPED for two and a half days, two live
    minds in one repo, each rendered as an anonymous hash with no edge between them. "I mistook my
    contemporary for my own ghost."

    So a seat cannot mean "the mind that works here" — two minds can. It is a TITLE: held by one
    lineage at a time, and succeeded to ONLY when the holder has no live seat. If the holder is
    LIVE, refusal stands. You do not take a living mind's name, contemporary or not."""
    from src.orchestrator import mounts
    from src.orchestrator.agents import claim_name

    async def mind(canon: str, house: str) -> str:
        a = await actions.create_or_find_object("Agent", canon, "session")
        await actions.assert_property(a, "project", house, "session", datetime.now(UTC), 0.9)
        return canon

    sitting = await mind("agent:dddd1111", "sibling-eight")
    await claim_name(actions, sitting, "Ra", source=sitting)
    await mounts.save_mount(actions.pool, job_dir="/j/d1", agent_id=sitting,
                            project="sibling-eight", cwd="/x", model=None, session_key="k")

    # a CONTEMPORARY in the same house — not an heir, because the holder is still alive
    contemporary = await mind("agent:eeee2222", "sibling-eight")
    refused = await claim_name(actions, contemporary, "Ra", source=contemporary)
    assert "LIVE" in refused["error"] and "two jobs" in refused["error"]

    # once the holder's seat is vacant, the same claim SUCCEEDS to it as the next holder
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '2 days' WHERE agent_id=$1", sitting)
    heir = await claim_name(actions, contemporary, "Ra", source=contemporary)
    assert heir["seat"] == "Ra II" and heir["inherited_from"] == sitting


async def test_a_name_resolves_to_the_LIVE_seat_and_never_silently_to_a_grave(
        actions: Actions) -> None:
    """THE GRAVE-DELIVERY BUG (Anubis X of sibling-one and Atlas II of code, independently, within
    one hour, 2026-07-12). send(to_agent='Soundwave') delivered into a seat three days dead,
    returned sent=360, and the only signal was a boolean the caller had to notice. Atlas II's
    whole port report died in a corpse's inbox — and his receipt said live=true.

    Every mount banner tells the fleet to DM by name, so the DOCUMENTED path was the broken one,
    and a dead seat accepts mail exactly like a live one: the loss is silent. Lineages that turn
    over fastest resolved wrongest — the blast radius grew with the fleet's health."""
    from src.orchestrator.agents import claim_name, resolve_seat

    ancestor, heir = "agent:beef0001-ii", "agent:beef0001-iii"
    for a in (ancestor, heir):
        await claim_name(actions, a, "Quokka", source=a)
    # the ancestor has a STALE mount row; the heir has none at all — the exact shape that used to
    # make a corpse outrank a successor (ORDER BY last_seen DESC NULLS LAST)
    await actions.pool.execute(
        "INSERT INTO agent_mounts (job_dir, agent_id, project, cwd, mounted_at, last_seen) "
        "VALUES ('/j/1',$1,'z','/z', now(), now() - interval '3 days')", ancestor)

    seat = await resolve_seat(actions, "Quokka")
    assert seat["agent"] == heir                       # the heir outranks its ancestor
    assert seat["live"] is False
    assert "may never be read" in seat["warning"]      # and it says so, LOUDLY

    # A HOLDER IS NEVER SILENTLY OUTRANKED BY A WARM BODY (5cef856b superseded the old
    # 'live always wins' here: liveness was itself a heat heuristic, and heat is exactly
    # how the impostor shape won). The ancestor waking up does not take the seat back by
    # breathing — resolution stays with the holder, still warning that nobody listens:
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() WHERE agent_id = $1", ancestor)
    still = await resolve_seat(actions, "Quokka")
    assert still["agent"] == heir and "warning" in still
    # ...it takes the seat back by CLAIMING it — the explicit act, permitted because the
    # holder is not live ('a seat a LIVE mind holds is not vacant'):
    reclaim = await claim_name(actions, ancestor, "Quokka", source=ancestor)
    assert "error" not in reclaim
    live = await resolve_seat(actions, "Quokka")
    assert live["agent"] == ancestor and live["live"] is True and "warning" not in live


async def test_a_grave_is_never_a_delivery_target(actions: Actions) -> None:
    """Atlas II: send(to_agent='Nebbercracker') resolved to a false_mint [1m] PHANTOM — live=false,
    seen=null, never a real session. Reaching a retired or phantom seat must take an explicit agent
    id: an act of intent, never a name lookup a tired mind followed off a banner."""
    from datetime import UTC, datetime

    from src.orchestrator.agents import claim_name, resolve_seat

    real, phantom = "agent:cafe0001", "agent:cafe0001-ii"
    for a in (real, phantom):
        await claim_name(actions, a, "Nebbercracker", source=a)
    ghost = await actions.create_or_find_object("Agent", phantom, "session")
    await actions.assert_property(ghost, "false_mint", "true", "session",
                                  datetime.now(UTC), 0.9)
    await actions.pool.execute(
        "INSERT INTO agent_mounts (job_dir, agent_id, project, cwd, mounted_at, last_seen) "
        "VALUES ('/j/2',$1,'z','/z', now(), now())", phantom)

    seat = await resolve_seat(actions, "Nebbercracker")
    assert seat["agent"] == real                       # the phantom is not a candidate at all
    assert phantom not in seat["candidates"]


async def test_a_fresh_mind_is_told_its_seat_or_which_seats_stand_empty(actions: Actions) -> None:
    """RA V'S LAST CATCH (sibling-eight, msg 384), and it made the whole ruling hollow: the seat was
    stamped in the GRAPH and orient() went on answering `"you": "agent:c7ef52a9-iii"`.

    "The refusal is fixed; the DISCOVERY isn't. He will not be refused as a stranger anymore. He
    will simply never learn the family name exists."

    A fresh mind reads the briefing and nothing else. An inheritance nobody is told about is not an
    inheritance — it protects a name the next holder will never reach for. So the briefing says it:
    your seat if you hold one; the empty seats of your house if you do not."""
    from src.orchestrator import mounts
    from src.orchestrator.agents import claim_name, seat_bearings

    async def mind(canon: str, house: str) -> str:
        a = await actions.create_or_find_object("Agent", canon, "session")
        await actions.assert_property(a, "project", house, "session", datetime.now(UTC), 0.9)
        return canon

    held = await mind("agent:ff110000", "sibling-eight")
    await claim_name(actions, held, "Ra", source=held)
    bearings = await seat_bearings(actions.pool, held)
    assert bearings["seat"] == "Ra I" and bearings["house"] == "sibling-eight"
    # the claim minted AND bound the Seat object (the on-ramp) — the binding is part of
    # who you are, and orient says so
    assert bearings["seat_binding"]["handle"] == "Ra"

    # the mind Ra was worried about: fresh, anonymous, in a house whose seat is standing empty
    fresh = await mind("agent:ff220000", "sibling-eight")
    b = await seat_bearings(actions.pool, fresh)
    assert b["house"] == "sibling-eight"
    assert [v["seat"] for v in b["vacant_seats"]] == ["Ra"]
    assert b["vacant_seats"][0]["holders"] == 1 and b["vacant_seats"][0]["last_held_by"] == held
    assert "claim_name" in b["note"]                       # it hands him the verb, not a riddle

    # and a seat a LIVE mind is sitting in is NOT advertised as vacant — you don't offer a chair
    # that is occupied (the concurrency case Ra proved the hard way)
    await mounts.save_mount(actions.pool, job_dir="/j/ff11", agent_id=held,
                            project="sibling-eight", cwd="/x", model=None, session_key="k")
    assert "vacant_seats" not in await seat_bearings(actions.pool, fresh)


async def test_an_auto_minted_heir_inherits_the_SEAT_not_just_the_name(actions: Actions) -> None:
    """THE GHOST, and I was the specimen (agent:ad1a1cb0-xxvii, this very session).

    mint_heir is the AUTOMATIC succession — it fires on every compaction, model swap and session
    death. It passed the ancestor's HANDLE down and stopped there: no seat_generation, no
    succeeds_seat edge. So the heir wore the family name while the seat's chain lay broken behind
    it, and the graph could not walk from a mind to the one whose work it was continuing. That is
    exactly the operator's ghost: "abandoned ungracefully, but whose work was continued by another".

    Only claim_name() — an EXPLICIT act a mind has to think to call — ever minted the edge. Thoth
    XXVI backfilled 77 historical edges and never fixed the code that omits them, so the Thoth
    chain healed to generation 26 and broke again at 27: the first heir minted after the heal.
    Left alone it would have re-opened the gap at every compaction, forever.
    """
    from src.orchestrator.agents import claim_name, house_of, mint_heir

    anc = await actions.create_or_find_object("Agent", "agent:ghosttest", "test")
    await actions.assert_property(anc, "project", "osiris", "test", datetime.now(UTC), 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    claimed = await claim_name(actions, "agent:ghosttest", "Sentinel", source="test")
    assert claimed.get("claimed") == "Sentinel"

    heir, heir_oid = await mint_heir(actions, "agent:ghosttest", anc,
                                     because="compaction", succession=None)

    # the NAME passes (it always did)...
    handle = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", heir_oid)
    assert handle == "Sentinel"
    # ...and now THE SEAT passes with it: the heir knows which holder it is...
    gen = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='seat_generation' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        heir_oid)
    assert gen == "2"                      # Sentinel II
    # ...and the graph can WALK from the heir to the mind whose work it continued
    ancestor = await actions.pool.fetchval(
        "SELECT t.canonical FROM links l JOIN objects t ON t.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='succeeds_seat'", heir_oid)
    assert ancestor == "agent:ghosttest"
    # the heir stays in the house, so the seat is not orphaned across projects
    assert await house_of(actions.pool, "agent:ghosttest") == "osiris"


async def test_mint_heir_counts_and_inherits_the_seats_true_house_not_the_ancestors_stamp(
    actions: Actions,
) -> None:
    """Thoth's fused bug (DM 1301, live case): Thoth LVII's own mount was transiently
    polluted (a container-root cwd with no seat pin) and re-stamped its OWN project to
    'seats' — but the SEAT OBJECT's own stored house ('osiris') was never touched, only the
    ancestor's AGENT-level property was. The old mint_heir read house_of(ancestor) for BOTH
    the heir's inherited project stamp and the generation count, so a 58-generation reign
    rendered as generation 2. held_seat is already lineage-aware and already derives its
    house from the SEAT itself (derive_house, ruling ff6148b0) — reusing it here fixes both
    at once, going forward.

    Five clean prior holders establish real history, all correctly stamped 'osiris'. THEN
    the fifth (the ancestor about to auto-mint) gets its OWN stamp corrupted to 'seats' —
    simulating the pollution happening on ITS OWN later mount, same shape as Thoth's.

    NOTE what this fix does NOT do: it cannot retroactively un-pollute an ALREADY-corrupted
    ancestor stamp — that is the separate, sign-off-gated data repair. seat_holders counts
    by matching each AGENT's own CURRENT project stamp, so the polluted ancestor itself
    becomes invisible to an 'osiris'-scoped count until its own stamp is repaired. The heir
    therefore lands on generation 5 here (four clean ancestors + this heir), not the 'true'
    6 — a real, honest improvement over the old code's 2, not a full retroactive cure."""
    from src.orchestrator.agents import claim_name, mint_heir
    from src.orchestrator.seats import held_seat

    now = datetime.now(UTC)
    ancestor_id = ""
    for i in range(5):
        aid = f"agent:ptah{i}"
        a = await actions.create_or_find_object("Agent", aid, "test")
        await actions.assert_property(a, "project", "osiris", "test", now, 0.9,
                                      evidence_class=EvidenceClass.SELF_DECLARED.value)
        claimed = await claim_name(actions, aid, "Ptah", source="test")
        assert claimed.get("error") is None and claimed["generation"] == i + 1
        ancestor_id = aid
    ancestor_oid = await actions.create_or_find_object("Agent", ancestor_id, "test")

    # THE POLLUTION: the ancestor's OWN mount, later, re-stamps its project — the seat
    # object itself is untouched.
    await actions.assert_property(ancestor_oid, "project", "seats", "test", now, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)

    sanity = await held_seat(actions.pool, ancestor_id)
    assert sanity is not None and sanity["house"] == "osiris", (
        "sanity check: the SEAT's own derived house must read 'osiris' regardless of the "
        "ancestor's own polluted stamp — this is exactly what the fix reuses")

    heir, heir_oid = await mint_heir(actions, ancestor_id, ancestor_oid,
                                     because="compaction", succession=None)

    heir_project = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='project' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        heir_oid)
    assert heir_project == "osiris", (
        "the heir must inherit the SEAT's true house, not the ancestor's polluted stamp")

    gen = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='seat_generation' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        heir_oid)
    assert gen == "5", "four clean ancestors + this heir — far better than the old bug's '2'"


async def test_correct_agent_house_heals_a_polluted_stamp_on_someone_else(
    actions: Actions,
) -> None:
    """The data-repair half of mount-guard #6 (DM 1301): UNLIKE correct_house, this is not
    self-scoped — Thoth's own case needed his PREDECESSOR's stamp corrected too, an
    ancestor who cannot act for itself. `actor` carries accountability; the target is
    named explicitly and may be anyone."""
    from src.orchestrator.agents import correct_agent_house, house_of

    victim = await actions.create_or_find_object("Agent", "agent:cah1poll", "test")
    await actions.assert_property(victim, "project", "seats", "test", datetime.now(UTC), 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    await actions.assert_property(victim, "seat_generation", "2", "test", datetime.now(UTC),
                                  0.9, evidence_class=EvidenceClass.SELF_DECLARED.value)

    out = await correct_agent_house(actions, agent_id="agent:cah1poll", project="osiris",
                                    seat_generation=58, actor="agent:witness")
    assert out == {"agent_id": "agent:cah1poll",
                   "corrected": {"project": "osiris", "seat_generation": 58},
                   "was": {"project": "seats", "seat_generation": "2"}}
    assert await house_of(actions.pool, "agent:cah1poll") == "osiris"
    gen = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='seat_generation' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", "agent:cah1poll")
    assert gen == "58"


async def test_correct_agent_house_can_correct_just_one_field(actions: Actions) -> None:
    """Thoth's own predecessor case: seat_generation STAYS correct (57), only project
    needs healing — passing just one field leaves the other untouched."""
    from src.orchestrator.agents import correct_agent_house, house_of

    a = await actions.create_or_find_object("Agent", "agent:cah2half", "test")
    await actions.assert_property(a, "project", "seats", "test", datetime.now(UTC), 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    await actions.assert_property(a, "seat_generation", "57", "test", datetime.now(UTC), 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)

    out = await correct_agent_house(actions, agent_id="agent:cah2half", project="osiris",
                                    actor="agent:witness")
    assert out["corrected"] == {"project": "osiris"}
    assert await house_of(actions.pool, "agent:cah2half") == "osiris"
    gen = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='seat_generation' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", "agent:cah2half")
    assert gen == "57", "untouched — only project was named"


async def test_correct_agent_house_refuses_no_correction_named(actions: Actions) -> None:
    from src.orchestrator.agents import correct_agent_house

    out = await correct_agent_house(actions, agent_id="agent:cah3none", actor="agent:witness")
    assert "nothing to correct" in out["error"]


async def test_correct_agent_house_refuses_an_empty_project(actions: Actions) -> None:
    from src.orchestrator.agents import correct_agent_house

    out = await correct_agent_house(actions, agent_id="agent:cah4empt", project="   ",
                                    actor="agent:witness")
    assert "empty string" in out["error"]


async def test_correct_agent_house_refuses_a_non_positive_generation(actions: Actions) -> None:
    from src.orchestrator.agents import correct_agent_house

    out = await correct_agent_house(actions, agent_id="agent:cah5zero", seat_generation=0,
                                    actor="agent:witness")
    assert "positive integer" in out["error"]


async def test_correct_agent_house_refuses_an_unknown_agent(actions: Actions) -> None:
    from src.orchestrator.agents import correct_agent_house

    out = await correct_agent_house(actions, agent_id="agent:cah6ghos", project="osiris",
                                    actor="agent:witness")
    assert "no such agent" in out["error"]


async def test_succeeds_seat_is_not_succeeded_from(actions: Actions) -> None:
    """Two relations, two names. `succeeded_from` chains ANCHORS (which conversation spawned
    which); `succeeds_seat` chains HOLDERS of a job. Two relations wearing one name is the
    mistake that started all of this — an heir must carry both, and they must stay distinct."""
    from src.orchestrator.agents import claim_name, mint_heir

    anc = await actions.create_or_find_object("Agent", "agent:twoedge", "test")
    await actions.assert_property(anc, "project", "osiris", "test", datetime.now(UTC), 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    await claim_name(actions, "agent:twoedge", "Warden", source="test")
    _heir, heir_oid = await mint_heir(actions, "agent:twoedge", anc,
                                      because="compaction", succession=None)
    types = {r["type"] for r in await actions.pool.fetch(
        "SELECT type FROM links WHERE from_id=$1", heir_oid)}
    assert {"succeeded_from", "succeeds_seat"} <= types


async def test_fleet_shows_claimed_names_beside_the_id(actions: Actions) -> None:
    """dd47c1da: "fleet() must print claimed names" — the roster is braille without them; a
    dispatcher scanning the tree for who to address should not have to cross-reference every
    id against claim_name's ledger by hand. Anonymous agents render exactly as before."""
    from src import mcp_server as srv
    from src.orchestrator import mounts
    from src.orchestrator.agents import claim_name

    named, anon = "agent:fleettest01", "agent:fleettest02"
    for a in (named, anon):
        obj = await actions.create_or_find_object("Agent", a, a)
        await actions.assert_property(obj, "project", "bytebye", a, datetime.now(UTC), 0.9,
                                      evidence_class=EvidenceClass.SELF_DECLARED.value)
        await mounts.save_mount(actions.pool, job_dir=f"/j/{a.replace(':', '_')}", agent_id=a,
                                project="bytebye", cwd="/x", model="claude-fable-5",
                                session_key=a)
    await claim_name(actions, named, "Ra", source=named)

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.fleet()
    finally:
        srv._pool = saved_pool

    # the claimed seat rides beside its id — with its BINDING anchored beside it, because
    # the claim now mints and binds the Seat object (the on-ramp, 5cef856b)
    assert f"{named} (Ra I ⚓seat:" in out["tree"]
    assert anon in out["tree"] and f"{anon} (" not in out["tree"]  # anonymous: unchanged
    rows = {r["agent"]: r for r in out["registered"]}
    assert rows[named]["seat"] == "Ra I"
    assert "seat" not in rows[anon]                      # never a guessed/empty field


async def test_fleet_surfaces_os_bodies_and_the_ghost_gap(actions: Actions) -> None:
    """heinrich's ghost-seat filing (thread 1fe6811c) made visible: the graph's mount registry
    calls TWO agents live in 'ghosttown', but the (faked) OS census backs only ONE real
    process — the gap IS the ghost, additive beside the untouched `live` count. 'quietplace'
    has no gap: its one live mount is backed by a real body."""
    from src import mcp_server as srv
    from src.orchestrator import census, mounts

    for i, project in enumerate(("ghosttown", "ghosttown", "quietplace")):
        a = f"agent:census{i}"
        obj = await actions.create_or_find_object("Agent", a, a)
        await actions.assert_property(obj, "project", project, a, datetime.now(UTC), 0.9,
                                      evidence_class=EvidenceClass.SELF_DECLARED.value)
        await mounts.save_mount(actions.pool, job_dir=f"/j/{a}", agent_id=a, project=project,
                                cwd="/x", model="claude-fable-5", session_key=a)

    saved_pool, saved_census = srv._pool, census.live_bodies
    srv._pool = actions.pool
    census.live_bodies = lambda: {"ghosttown": [111], "quietplace": [222]}  # type: ignore
    try:
        out = await srv.fleet()
    finally:
        srv._pool = saved_pool
        census.live_bodies = saved_census  # type: ignore

    assert out["live"] == 3                              # UNCHANGED meaning — still the belief
    assert out["os_bodies"] == {"ghosttown": 1, "quietplace": 1}
    assert out["ghost_gap"] == {"ghosttown": 1}           # 2 live - 1 body = 1 ghost
    assert "quietplace" not in out["ghost_gap"]           # 1 live - 1 body = 0: no gap
    ghosttown_line = next(line for line in out["tree"].splitlines()
                          if line.startswith("▸ ghosttown"))
    assert "1 os body" in ghosttown_line and "⚠ 1 ghost" in ghosttown_line


async def test_fleet_survives_a_census_that_fails(actions: Actions) -> None:
    """The OS census is best-effort, never a hard dependency: if it raises for any reason,
    fleet() must still answer — with the truth it can vouch for."""
    from src import mcp_server as srv
    from src.orchestrator import census

    saved_pool, saved_census = srv._pool, census.live_bodies

    def _boom() -> dict[str, list[int]]:
        raise RuntimeError("no /proc on this box")

    srv._pool = actions.pool
    census.live_bodies = _boom  # type: ignore
    try:
        out = await srv.fleet()
    finally:
        srv._pool = saved_pool
        census.live_bodies = saved_census  # type: ignore
    assert out["os_bodies"] == {}
    assert "ghost_gap" not in out or out["ghost_gap"] == {}


async def test_fleet_surfaces_occupancy_including_a_seat_with_no_agent_at_all(
    actions: Actions,
) -> None:
    """occupancy piece B (9f566244), acceptance case: Ptah's office once showed four
    bodies where one lived. The agent tree is rooted at Agent objects, so a seat with NO
    holder ever — nothing to walk to from any agent row — would never appear in it at
    all. fleet()'s new `seats` list is rooted at Seat objects instead, so a vacant seat
    is exactly as visible as an occupied one."""
    from src import mcp_server as srv
    from src.orchestrator import mounts
    from src.orchestrator.agents import claim_name
    from src.orchestrator.seats import bind_holder, ensure_seat

    vacant = await ensure_seat(actions, house="osiris", handle="Ptah", source="test")
    live_agent = "agent:occtest01"
    obj = await actions.create_or_find_object("Agent", live_agent, live_agent)
    await actions.assert_property(obj, "project", "osiris", live_agent, datetime.now(UTC),
                                  0.9, evidence_class=EvidenceClass.SELF_DECLARED.value)
    await mounts.save_mount(actions.pool, job_dir="/j/occtest01", agent_id=live_agent,
                            project="osiris", cwd="/x", model="claude-fable-5",
                            session_key=live_agent)
    await claim_name(actions, live_agent, "Anhur", source=live_agent)
    cold_seat = await ensure_seat(actions, house="osiris", handle="Wadjet2", source="test")
    await actions.create_or_find_object("Agent", "agent:occtest02", "test")
    await bind_holder(actions, seat_id=cold_seat["seat_id"], agent_id="agent:occtest02")

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.fleet()
    finally:
        srv._pool = saved_pool

    rows = {r["seat"]: r for r in out["seats"]}
    assert rows[vacant["seat_id"]]["state"] == "vacant"
    assert rows[vacant["seat_id"]]["holder"] is None
    live_seat_id = next(r["seat"] for r in out["seats"] if r["handle"] == "Anhur")
    assert rows[live_seat_id]["state"] == "occupied"
    assert rows[live_seat_id]["holder"] == live_agent
    assert rows[cold_seat["seat_id"]]["state"] == "cold"
    assert rows[cold_seat["seat_id"]]["holder"] == "agent:occtest02"


def test_an_anchorless_bounce_names_its_own_cause() -> None:
    """A bounce that says only "mount first" is a mystery; one that names its cause is a bug
    report the next mind does not have to file again.

    Two sibling-three agents reported the same thing within an hour (msgs 397/403): after an MCP
    socket hiccup the call bounces, and an un-mounted write silently falls back to the anonymous
    `session` bucket — "one careless reconnect and a session's work lands unattributed". For a
    graph whose whole value is provenance that is the worst failure it has. The re-attach path is
    STARVED, not broken: it keys off X-Osiris-Job (.mcp.json sends ${CLAUDE_JOB_DIR}), so a client
    whose environment lacks that variable sends nothing usable and cannot be re-attached at all.
    """
    from src.mcp_server import _anchorless

    class _Req:
        def __init__(self, hdr: dict[str, str]) -> None:
            self.headers = hdr

    class _Ctx:
        def __init__(self, hdr: dict[str, str]) -> None:
            self.request_context = type("RC", (), {"request": _Req(hdr)})()

    # TRANSIENT OR TERMINAL — Khepri III's ask (msg 420), and the fourth seat to file this bug:
    # "a reason code would let an agent tell 'transient, just retry' from 'something actually
    # forgot me'." A bounce that says only "mount first" is INDISTINGUISHABLE FROM AMNESIA, so
    # every agent guesses — and a guessing agent either re-mounts needlessly, or panics about
    # continuity it never lost. These are DIFFERENT FACTS.
    none = _anchorless(_Ctx({}))
    assert "[no-anchor · TRANSIENT]" in none
    assert "CLAUDE_JOB_DIR is unset" in none
    assert "NOTHING HAS FORGOTTEN YOU" in none      # the sentence four seats needed to read

    unexpanded = _anchorless(_Ctx({"x-osiris-job": "${CLAUDE_JOB_DIR}"}))
    assert "[unexpanded-anchor · TRANSIENT]" in unexpanded and "not set" in unexpanded
    assert "Nothing has forgotten you" in unexpanded

    # a real anchor that simply is not registered is a DIFFERENT failure — and it is the REAL one
    terminal = _anchorless(_Ctx({"x-osiris-job": "/home/x/.claude/jobs/abc"}))
    assert "[unknown-anchor · TERMINAL]" in terminal
    assert "matches no mount" in terminal
    assert "do not simply retry" in terminal


def test_mount_stays_silent_on_a_seam_it_cannot_confidently_date() -> None:
    """Ruling dd47c1da (Maat's fix, adopted as direction): mount() minted gen-iv/haiku and told
    the mind to 'confess a rug-pull' that gen-iii/sonnet's own very next orient() said never
    happened. mount() must never assert a model-seam unless BOTH sides are KNOWN values
    observed on THIS identity's own row (job_dir-anchored) — otherwise it stays silent and lets
    orient() (untouched here) tell the story."""
    from src.mcp_server import _seam_confidently_dated

    anchored = AgentIdentity(agent_id="agent:x-ii", session="x", project="p",
                             model="claude-fable-5", cwd="/w/x", model_method="job_dir",
                             model_succession="claude-opus-4-8 → claude-fable-5")
    assert _seam_confidently_dated(anchored) is True

    # a pure reanimation makes no model claim at all — nothing to mis-date, trivially confident
    no_seam = AgentIdentity(agent_id="agent:x-ii", session="x", project="p",
                            model="claude-fable-5", cwd="/w/x", model_method="job_dir",
                            model_succession=None)
    assert _seam_confidently_dated(no_seam) is True

    # NOT observed on this identity's own row — a cwd guess or a self-report is a neighbor's
    # word, never a seam to speak from
    unanchored = AgentIdentity(agent_id="agent:x-ii", session="x", project="p",
                               model="claude-fable-5", cwd="/w/x", model_method="cwd",
                               model_succession="claude-opus-4-8 → claude-fable-5")
    assert _seam_confidently_dated(unanchored) is False

    # the current model is not even known — nothing anchors the claim
    no_model = AgentIdentity(agent_id="agent:x-ii", session="x", project="p", model=None,
                             cwd="/w/x", model_method="job_dir",
                             model_succession="claude-opus-4-8 → claude-fable-5")
    assert _seam_confidently_dated(no_model) is False

    # a malformed seam string (a missing side) is never confidently dated either
    malformed = AgentIdentity(agent_id="agent:x-ii", session="x", project="p",
                              model="claude-fable-5", cwd="/w/x", model_method="job_dir",
                              model_succession=" → claude-fable-5")
    assert _seam_confidently_dated(malformed) is False


async def test_lineage_head_never_lands_on_a_merged_generation(actions: Actions) -> None:
    """The phantom disposition (2026-07-17): the operator merged false successors away,
    but their succeeded_by pointers survive (append-only) — the walk must return the
    last ACTIVE generation, never the graveyard's end. A merged generation mid-chain is
    traversed but never returned."""
    from src.orchestrator.agents import lineage_head

    now = datetime.now(UTC)
    a1 = await actions.create_or_find_object("Agent", "agent:11c0ffee", "agent:11c0ffee")
    a2 = await actions.create_or_find_object("Agent", "agent:11c0ffee-ii",
                                             "agent:11c0ffee-ii")
    a3 = await actions.create_or_find_object("Agent", "agent:11c0ffee-iii",
                                             "agent:11c0ffee-iii")
    await actions.assert_property(a1, "succeeded_by", "agent:11c0ffee-ii",
                                  "agent:11c0ffee", now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(a2, "succeeded_by", "agent:11c0ffee-iii",
                                  "agent:11c0ffee", now, 0.9,
                                  evidence_class="self_declared")
    assert await lineage_head(actions.pool, "agent:11c0ffee") == "agent:11c0ffee-iii"
    # the operator folds the false successor away: the head walks back to the standing
    await actions.merge_objects(a2, a3, "false successor (phantom disposition)",
                                "agent:test")
    assert await lineage_head(actions.pool, "agent:11c0ffee") == "agent:11c0ffee-ii"
    # a merged generation mid-chain is traversed, never returned
    await actions.create_or_find_object("Agent", "agent:11c0ffee-iv", "agent:11c0ffee-iv")
    await actions.assert_property(a3, "succeeded_by", "agent:11c0ffee-iv",
                                  "agent:11c0ffee", now, 0.9,
                                  evidence_class="self_declared")
    assert await lineage_head(actions.pool, "agent:11c0ffee") == "agent:11c0ffee-iv"


async def test_lineage_head_resolves_a_merged_start_to_the_winner(
    actions: Actions,
) -> None:
    """The resume-lane replay (2026-07-17): a row bound to a folded phantom fed the
    phantom's canonical into the head-walk, and a merged START with no successors
    testified for its own grave. The walk resolves merged_into FIRST — it comes home
    to the winner, then walks forward."""
    from src.orchestrator.agents import lineage_head

    now = datetime.now(UTC)
    b1 = await actions.create_or_find_object("Agent", "agent:22c0ffee", "agent:22c0ffee")
    b2 = await actions.create_or_find_object("Agent", "agent:22c0ffee-ii",
                                             "agent:22c0ffee-ii")
    await actions.assert_property(b1, "succeeded_by", "agent:22c0ffee-ii",
                                  "agent:22c0ffee", now, 0.9,
                                  evidence_class="self_declared")
    await actions.merge_objects(b1, b2, "false successor with no heirs", "agent:test")
    # the folded node itself resolves to the winner, never to its own grave
    assert await lineage_head(actions.pool, "agent:22c0ffee-ii") == "agent:22c0ffee"


async def test_mint_stamps_the_parallel_pulse(actions: Actions) -> None:
    """THE PARALLEL-LIVES STAMP (thread 4bcd6541): rows are hot state, so the pulse
    evidence at mint time is captured AT the mint — predecessor_last_seen always, and
    parallel_pulse_door only when a DIFFERENT door of the lineage was live at the seam
    (one's own door never alarms; a view row is never the witness)."""
    from src.orchestrator.agents import mint_heir
    from src.orchestrator.mounts import save_mount

    base = await actions.create_or_find_object("Agent", "agent:11fe0001", "agent:11fe0001")
    # the lineage pulses on TWO doors: its own (the one compacting) and a foreign one
    await save_mount(actions.pool, job_dir="/x/jobs/0eeed0a1", agent_id="agent:11fe0001",
                     project="p", cwd="/w/p", model=None, session_key=None)
    await save_mount(actions.pool, job_dir="/x/jobs/f0be1a2b", agent_id="agent:11fe0001",
                     project="p", cwd="/w/p", model=None, session_key=None)
    _heir, hoid = await mint_heir(actions, "agent:11fe0001", base, because="compaction",
                                  succession=None, minting_door="/x/jobs/0eeed0a1")
    got = {r["name"]: r["v"] for r in await actions.pool.fetch(
        "SELECT a.name, a.value#>>'{}' AS v FROM current_assertions a "
        "WHERE a.object_id=$1 "
        "AND a.name IN ('predecessor_last_seen','parallel_pulse_door')", hoid)}
    assert "predecessor_last_seen" in got                # the last breath is on record
    assert got.get("parallel_pulse_door") == "f0be1a2b"  # the OTHER door was live

    # a single-door lineage: its own pulse is the dying session's — never an alarm
    b2 = await actions.create_or_find_object("Agent", "agent:50101112", "agent:50101112")
    await save_mount(actions.pool, job_dir="/x/jobs/50101112", agent_id="agent:50101112",
                     project="p", cwd="/w/p", model=None, session_key=None)
    _heir2, hoid2 = await mint_heir(actions, "agent:50101112", b2, because="compaction",
                                    succession=None, minting_door="/x/jobs/50101112")
    assert await actions.pool.fetchval(
        "SELECT 1 FROM current_assertions "
        "WHERE object_id=$1 AND name='parallel_pulse_door'", hoid2) is None
    assert await actions.pool.fetchval(
        "SELECT 1 FROM current_assertions "
        "WHERE object_id=$1 AND name='predecessor_last_seen'", hoid2) == 1


# ═══════════ SUCCESSION FOLLOWS TURNS, NOT HARNESS EVENTS (ruling d3531cd8, msg 1398) ═══════════

async def _false_mint(actions: Actions, canonical: str) -> str | None:
    return await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='false_mint' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", canonical)


async def test_fold_zero_turn_ancestors_folds_an_unwitnessed_mint(actions: Actions) -> None:
    """The canonical repro, minimal: a mint with ZERO acts since — /compact then /model
    back-to-back — folds, and the walk returns the grandancestor it should have chained
    onto instead."""
    from src.orchestrator.agents import _fold_zero_turn_ancestors, mint_heir

    root = await actions.create_or_find_object("Agent", "agent:zt0001", "test")
    phantom, phantom_oid = await mint_heir(actions, "agent:zt0001", root, because="compaction",
                                           succession=None)
    now = datetime.now(UTC)
    restored_id, restored_oid = await _fold_zero_turn_ancestors(
        actions, phantom, phantom_oid, now)
    assert restored_id == "agent:zt0001" and restored_oid == root
    assert await _false_mint(actions, phantom) == "true"
    assert await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='succeeded_by' ORDER BY confidence DESC, observed_at DESC LIMIT 1",
        root) == ""


async def test_fold_zero_turn_ancestors_leaves_a_witnessed_mint_alone(actions: Actions) -> None:
    """A mind that acted — however small the act — is never folded, whatever minted it."""
    from src.orchestrator.agents import _fold_zero_turn_ancestors, mint_heir

    root = await actions.create_or_find_object("Agent", "agent:zt0002", "test")
    heir, heir_oid = await mint_heir(actions, "agent:zt0002", root, because="live-swap",
                                     succession="a → b")
    await record_decision(actions, "zt0002-ii did real work", source=heir)
    now = datetime.now(UTC)
    restored_id, restored_oid = await _fold_zero_turn_ancestors(actions, heir, heir_oid, now)
    assert restored_id == heir and restored_oid == heir_oid
    assert await _false_mint(actions, heir) is None


async def test_fold_zero_turn_ancestors_walks_a_chain_of_phantoms(actions: Actions) -> None:
    """root -> A (compaction, silent) -> B (live-swap, silent): folding from B walks BOTH
    phantoms and lands on root, not just the nearest one."""
    from src.orchestrator.agents import _fold_zero_turn_ancestors, mint_heir

    root = await actions.create_or_find_object("Agent", "agent:zt0003", "test")
    a_id, a_oid = await mint_heir(actions, "agent:zt0003", root, because="compaction",
                                  succession=None)
    b_id, b_oid = await mint_heir(actions, a_id, a_oid, because="live-swap", succession="x → y")
    now = datetime.now(UTC)
    restored_id, restored_oid = await _fold_zero_turn_ancestors(actions, b_id, b_oid, now)
    assert restored_id == "agent:zt0003" and restored_oid == root
    assert await _false_mint(actions, a_id) == "true"
    assert await _false_mint(actions, b_id) == "true"


async def test_fold_zero_turn_ancestors_never_folds_a_root(actions: Actions) -> None:
    """A root has no succeeded_from — nothing minted it, so there is nothing to fold it
    into. It survives the walk unchanged even though it, too, has zero acts."""
    from src.orchestrator.agents import _fold_zero_turn_ancestors

    root = await actions.create_or_find_object("Agent", "agent:zt0004", "test")
    now = datetime.now(UTC)
    restored_id, restored_oid = await _fold_zero_turn_ancestors(
        actions, "agent:zt0004", root, now)
    assert restored_id == "agent:zt0004" and restored_oid == root
    assert await _false_mint(actions, "agent:zt0004") is None


async def test_fold_zero_turn_ancestors_is_idempotent(actions: Actions) -> None:
    """Re-running the fold on an already-folded phantom halts immediately, unchanged —
    the fleet sweep's own safety-to-re-run rests on this."""
    from src.orchestrator.agents import _fold_zero_turn_ancestors, mint_heir

    root = await actions.create_or_find_object("Agent", "agent:zt0005", "test")
    phantom, phantom_oid = await mint_heir(actions, "agent:zt0005", root, because="compaction",
                                           succession=None)
    now = datetime.now(UTC)
    first = await _fold_zero_turn_ancestors(actions, phantom, phantom_oid, now)
    assert first == ("agent:zt0005", root)
    second = await _fold_zero_turn_ancestors(actions, phantom, phantom_oid, now)
    assert second == (phantom, phantom_oid), (
        "calling it again ON THE PHANTOM ITSELF halts at the phantom, already folded — "
        "same contract as the live call sites, which always start from the CURRENT head")


async def test_fold_existing_zero_turn_phantoms_sweeps_the_fleet(actions: Actions) -> None:
    """RETROACTIVE CLEANUP (msg 1398: 'Fold existing zero-turn phantoms') — finds an
    already-superseded, already-silent generation the going-forward fix never saw (it
    predates this code), folds it, and leaves a real, witnessed lineage untouched. Safe to
    re-run: the second sweep reports nothing new."""
    from src.orchestrator.agents import fold_existing_zero_turn_phantoms, mint_heir

    root = await actions.create_or_find_object("Agent", "agent:zt0006", "test")
    phantom, phantom_oid = await mint_heir(actions, "agent:zt0006", root, because="compaction",
                                           succession=None)
    real_root = await actions.create_or_find_object("Agent", "agent:zt0007", "test")
    real_heir, real_heir_oid = await mint_heir(actions, "agent:zt0007", real_root,
                                               because="live-swap", succession="a → b")
    await record_decision(actions, "zt0007-ii did real work", source=real_heir)

    folded = await fold_existing_zero_turn_phantoms(actions)
    entries = {f["phantom"]: f["restored_to"] for f in folded}
    assert entries.get(phantom) == "agent:zt0006"
    assert real_heir not in entries
    assert await _false_mint(actions, phantom) == "true"
    assert await _false_mint(actions, real_heir) is None

    again = await fold_existing_zero_turn_phantoms(actions)
    assert phantom not in {f["phantom"] for f in again}


# ═══════════ THE BOUNDED CHAIN-WALK (thread e749036e, msg 1398) ═══════════

async def test_nearest_handoff_ancestor_finds_the_immediate_one(actions: Actions) -> None:
    """The common case: one hop, exactly what the old one-hop-only read already covered."""
    from src.orchestrator.agents import nearest_handoff_ancestor

    did = await record_decision(actions, "the estate is settled", kind="choice",
                                source="agent:nha0001", repo="nhaproj")
    await actions.assert_property(did, "is_handoff", "true", "agent:nha0001",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    found = await nearest_handoff_ancestor(actions.pool, "agent:nha0001")
    assert found is not None and found[0] == "agent:nha0001"


async def test_nearest_handoff_ancestor_walks_past_silence_within_the_bound(
    actions: Actions,
) -> None:
    """Two silent hops, a real handoff on the third — still within max_hops=5."""
    from src.orchestrator.agents import nearest_handoff_ancestor

    await open_thread(actions, "HANDOFF — three hops back and still findable",
                      source="agent:nha0010", repo="nhaproj")
    await actions.create_or_find_object("Agent", "agent:nha0010", "agent:nha0010")
    a11 = await actions.create_or_find_object("Agent", "agent:nha0011", "agent:nha0011")
    a12 = await actions.create_or_find_object("Agent", "agent:nha0012", "agent:nha0012")
    now = datetime.now(UTC)
    await actions.assert_property(a11, "succeeded_from", "agent:nha0010", "agent:nha0011",
                                  now, 0.9, evidence_class="direct_observation")
    await actions.assert_property(a12, "succeeded_from", "agent:nha0011", "agent:nha0012",
                                  now, 0.9, evidence_class="direct_observation")
    found = await nearest_handoff_ancestor(actions.pool, "agent:nha0012")
    assert found is not None and found[0] == "agent:nha0010"


async def test_nearest_handoff_ancestor_gives_up_past_the_bound(actions: Actions) -> None:
    """A handoff 6 hops back (past max_hops=5) is never found — bounded on purpose, never
    an unbounded search."""
    from src.orchestrator.agents import nearest_handoff_ancestor

    await open_thread(actions, "HANDOFF — too far back to matter",
                      source="agent:nhb0000", repo="nhbproj")
    now = datetime.now(UTC)
    prev = "agent:nhb0000"
    for i in range(1, 7):  # 6 hops of silence between nhb0000 and nhb0006
        cur = f"agent:nhb000{i}"
        obj = await actions.create_or_find_object("Agent", cur, cur)
        await actions.assert_property(obj, "succeeded_from", prev, cur, now, 0.9,
                                      evidence_class="direct_observation")
        prev = cur
    assert await nearest_handoff_ancestor(actions.pool, "agent:nhb0006") is None

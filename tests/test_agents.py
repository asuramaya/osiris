"""Actor identity — the fleet made first-class ("a man and all his imaginary friends").

Over the shared MCP server every agent writes through one process; without a per-agent
source their writes collapse into one bucket. These tests prove identity resolution, the
Agent registration (the org-chart links), and that a mounted agent's captures are
attributed to IT — hermetic against real Postgres.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.agents import AgentIdentity, register_agent, resolve_identity
from src.orchestrator.capture import record_decision
from src.parsers.base import EvidenceClass


def test_resolve_identity_derives_project_and_session(tmp_path: Path) -> None:
    # root=tmp_path (empty) → nothing to observe → the passed model is the self-report fallback
    ident = resolve_identity(
        cwd="/home/x/code/decepticons",
        job_dir="/home/x/.claude/jobs/ad1a1cb0", model="claude-opus-4-8", root=tmp_path,
    )
    assert ident.project == "decepticons"
    assert ident.session == "ad1a1cb0"        # the job id
    assert ident.agent_id == "agent:ad1a1cb0"  # the provenance source
    assert ident.model == "claude-opus-4-8"


def test_resolve_identity_unknown_is_still_a_valid_actor() -> None:
    ident = resolve_identity(cwd=None, job_dir=None)
    assert ident.agent_id == "agent:unknown"   # coarse, but never crashes the mount


async def test_register_agent_mints_the_org_chart(actions: Actions) -> None:
    ident = resolve_identity(cwd="/w/heinrich", session="sess-h", model="claude-fable-5")
    a = await register_agent(actions, ident, actor="analyst:operator")
    # the Agent object carries its model (source-model provenance) + project
    row = await actions.pool.fetchrow(
        "SELECT (SELECT value#>>'{}' FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='source_model') AS model, "
        " (SELECT value#>>'{}' FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='project') AS project", a)
    assert row["model"] == "claude-fable-5" and row["project"] == "heinrich"
    # works_in -> the project, acts_for -> the principal (the org chart)
    links = await actions.pool.fetch(
        "SELECT l.type, o.canonical FROM links l JOIN objects o ON o.id=l.to_id "
        "WHERE l.from_id=$1 ORDER BY l.type", a)
    got = {r["type"]: r["canonical"] for r in links}
    assert got["works_in"] == "repo:heinrich"
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
                             model="claude-fable-5", root=tmp_path)
    assert ident.model == "claude-opus-4-8"          # OBSERVATION wins over the self-report
    assert ident.model_method == "job_dir"           # resolved by the harness probe
    assert ident.model_declared == "claude-fable-5"  # the agent's word is kept...
    assert ident.model_divergent is True             # ...and flagged as divergent


async def test_register_records_the_divergence_flag(actions: Actions, tmp_path: Path) -> None:
    """A divergent identity registers BOTH: source_model (observed, DIRECT_OBSERVATION) and
    source_model_declared (the agent's word, CO_OCCURRENCE). The mismatch IS the flag."""
    proj = tmp_path / "-home-x-code-osiris"
    proj.mkdir()
    _transcript(proj, "claude-opus-4-8")
    ident = resolve_identity(cwd="/x/osiris", job_dir="/j/jobs/deadbeef",
                             model="claude-fable-5", root=tmp_path)
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
    ident = resolve_identity(cwd="/x/osiris", job_dir="/j/jobs/deadbeef", root=tmp_path)
    assert ident.model == "claude-opus-4-8"  # the latest turn
    assert ident.model_history == ("claude-fable-5", "claude-opus-4-8")  # the whole transition


async def test_register_stamps_intent_and_the_swap(actions: Actions, tmp_path: Path) -> None:
    """With expected_model set, register_agent stamps the INTENT and, on a divergence (the fable
    harness's silent demotion), the swap as a first-class OBSERVED event on the Agent."""
    proj = tmp_path / "-home-x-code-osiris"
    proj.mkdir()
    _transcript_lines(proj, "claude-fable-5", "claude-opus-4-8")
    ident = resolve_identity(cwd="/x/osiris", job_dir="/j/jobs/deadbeef", root=tmp_path)
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
    return AgentIdentity(agent_id="agent:0806072e", session="0806072e", project="decepticons",
                         model=model, cwd="/w/decepticons", model_method="job_dir",
                         model_history=history if history is not None else (model,))


async def test_succession_seam_mints_a_lineage_linked_heir(actions: Actions) -> None:
    """The MINT ruling (be292762, heinrich's remedy adopted): a fresh context arriving across a
    detected seam is not stamped-and-left-wearing-the-dead-name — it is MINTED its own id
    (agent:<base>-ii) with a succeeded_from link; the ancestor's record closes intact."""
    dead = _anchored("claude-opus-4-8")
    a = await register_agent(actions, dead, actor="analyst:operator")
    assert dead.model_succession is None  # first anchored write — no baseline, no seam
    successor = _anchored("claude-fable-5")  # fresh context: opus is NOWHERE in its history
    a2 = await register_agent(actions, successor, actor="analyst:operator")
    assert a2 != a                                        # a NEW being, not the dead name re-worn
    assert successor.agent_id == "agent:0806072e-ii"      # heinrich's grammar, exactly
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
    # the same session UUID mounts again — the door decepticons repro'd, now a minting
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


async def test_warm_swap_is_not_a_succession(actions: Actions) -> None:
    """A transition the transcript DID witness is the warm-swap's (model_swapped) — same context,
    different seam. The succession stamp must stay quiet or every mid-run swap double-fires."""
    first = _anchored("claude-fable-5")
    await register_agent(actions, first, actor="analyst:operator")
    swapped = _anchored("claude-opus-4-8",
                        history=("claude-fable-5", "claude-opus-4-8"))  # the prior IS in history
    a = await register_agent(actions, swapped, actor="analyst:operator",
                             expected_model="claude-fable-5")
    assert swapped.model_succession is None
    row = await actions.pool.fetchrow(
        "SELECT (SELECT value#>>'{}' FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='model_succession') AS seam, "
        " (SELECT value#>>'{}' FROM current_assertions x WHERE x.object_id=$1 "
        "  AND x.name='model_swapped') AS swapped", a)
    assert row is not None
    assert row["seam"] is None                  # not a succession...
    assert row["swapped"] is not None           # ...it's the warm swap, already first-class


async def test_succession_needs_anchored_observations_on_both_sides(actions: Actions) -> None:
    """Only two ANCHORED observations disagreeing can witness a seam: a self-report on the new
    side, or a cwd guess as the baseline, must fire nothing (the cry-wolf lesson, e71b408f)."""
    await register_agent(actions, _anchored("claude-opus-4-8"), actor="analyst:operator")
    self_rep = AgentIdentity(agent_id="agent:0806072e", session="0806072e", project="decepticons",
                             model="claude-fable-5", cwd="/w/decepticons",
                             model_method="self_report")
    a = await register_agent(actions, self_rep, actor="analyst:operator")
    assert self_rep.model_succession is None    # the new side is only the agent's word
    # ...and a weak-grade baseline can't witness either: cwd guess first, anchored read second
    guess = AgentIdentity(agent_id="agent:c0ffee00", session="c0ffee00", project="decepticons",
                          model="claude-opus-4-8", cwd="/w/decepticons", model_method="cwd")
    await register_agent(actions, guess, actor="analyst:operator")
    anchored2 = AgentIdentity(agent_id="agent:c0ffee00", session="c0ffee00",
                              project="decepticons", model="claude-fable-5",
                              cwd="/w/decepticons", model_method="job_dir",
                              model_history=("claude-fable-5",))
    b = await register_agent(actions, anchored2, actor="analyst:operator")
    assert anchored2.model_succession is None   # no anchored baseline → no seam
    for obj in (a, b):
        seam = await actions.pool.fetchval(
            "SELECT value#>>'{}' FROM current_assertions "
            "WHERE object_id=$1 AND name='model_succession'", obj)
        assert seam is None


def test_unresolved_identities_do_not_conflate(tmp_path: Path) -> None:
    """Hardening: two agents that can't resolve a session id must NOT collapse into one shared
    agent:unknown sink (an accidental identity merge). Distinct anchors → distinct ids."""
    # no job_dir, no findable transcript → project-scoped fallback, distinct per project
    a = resolve_identity(cwd="/w/heinrich", root=tmp_path)
    b = resolve_identity(cwd="/w/decepticons", root=tmp_path)
    assert a.agent_id == "agent:unknown-heinrich" and b.agent_id == "agent:unknown-decepticons"
    assert a.agent_id != b.agent_id                        # never the same sink
    assert a.resolved is False and b.resolved is False
    # a job_dir is a per-session anchor even when its id won't parse (no 'jobs' path segment)
    c = resolve_identity(cwd="/w/x", job_dir="/weird/box-42", root=tmp_path)
    d = resolve_identity(cwd="/w/x", job_dir="/weird/box-99", root=tmp_path)
    assert c.agent_id != d.agent_id and c.agent_id.startswith("agent:j")


def test_cwd_located_identity_is_not_marked_resolved(tmp_path: Path) -> None:
    """#HIGH (audit): without a session/job_dir ANCHOR, resolve_identity GUESSES the session from
    the hottest cwd transcript — which concurrent same-project sessions would all grab, silently
    merging. Mark it unresolved so the fleet digest SURFACES the ambiguity, not a false green."""
    proj = tmp_path / "-home-x-code-osiris"
    proj.mkdir()
    _transcript(proj, "claude-opus-4-8")
    ident = resolve_identity(cwd="/home/x/code/osiris", root=tmp_path)  # no anchor → cwd guess
    assert ident.model == "claude-opus-4-8"  # still reads a best-guess model (stays functional)
    assert ident.project == "osiris"
    assert ident.resolved is False           # but flagged NOT confident — the digest can see it


def test_cwd_fallback_neighbor_swap_does_not_cry_wolf(tmp_path: Path) -> None:
    """The live cry-wolf (bonus bug, agent e71b408f): with no job_dir anchor, resolve_identity
    reads the project dir's HOTTEST transcript — which may be a CONCURRENT NEIGHBOR's. If that
    neighbor was warm-swapped (fable→haiku), a cwd-grade read must inform `model` but NEVER fire a
    swap confession — a verified fable session was falsely told it had been 'demoted to haiku'."""
    from src.orchestrator.swaps import classify_swap, swap_banner

    proj = tmp_path / "-home-x-code-osiris"
    proj.mkdir()
    _transcript_lines(proj, "claude-fable-5", "claude-haiku-4-5-20251001")  # a neighbor's swap
    ident = resolve_identity(cwd="/home/x/code/osiris", root=tmp_path)  # no job_dir → cwd guess
    assert ident.model_method == "cwd"                       # a GUESS, not an anchor
    assert ident.model == "claude-haiku-4-5-20251001"   # informs model (best-effort)...
    assert ident.model_history == ()                    # ...but no swap history from a cwd read
    assert ident.resolved is False
    # the confession is GATED on a job_dir anchor → the neighbor's demotion is NOT confessed as ours
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

"""Actor identity — the fleet made first-class ("a man and all his imaginary friends").

Over the shared MCP server every agent writes through one process; without a per-agent
source their writes collapse into one bucket. These tests prove identity resolution, the
Agent registration (the org-chart links), and that a mounted agent's captures are
attributed to IT — hermetic against real Postgres.
"""
from __future__ import annotations

import json
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

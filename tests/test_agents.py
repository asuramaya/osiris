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

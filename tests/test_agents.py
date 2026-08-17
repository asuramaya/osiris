"""Actor identity — the fleet made first-class ("a man and all his imaginary friends").

Over the shared MCP server every agent writes through one process; without a per-agent
source their writes collapse into one bucket. These tests prove identity resolution, the
Agent registration (the org-chart links), and that a mounted agent's captures are
attributed to IT — hermetic against real Postgres.
"""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.ingest.harness import ModelReading
from src.ingest.harness.claude_jsonl import ClaudeJsonlAdapter
from src.ingest.transcript_store import _reading_from_turns
from src.orchestrator.agents import (
    AgentIdentity,
    _resolve_or_mint_project,
    register_agent,
    resolve_identity,
)
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


# --- register_agent must never let a routine mount clobber a DECLARED rename (#137/#152,
# Thoth DM 3801) — measured live: repo:xxit's declared "handlingtheloop" (decision 8766acd7)
# was silently reverted to "xxit" by five ordinary metron/deckard mounts, because this write
# path used to reassert `name` from the caller's own stale pin at the SAME confidence a
# deliberate rename uses, and current_assertions' tie-break falls through to pure recency. ---


async def test_register_agent_mount_never_clobbers_a_declared_rename(actions: Actions) -> None:
    from src.orchestrator.project_identity import rename_project

    # first mount under the pre-rename label — the ordinary, first-ever-registration path
    ident = resolve_identity(cwd="/w/xxit", session="sess-a", model="claude-fable-5")
    a = await register_agent(actions, ident, actor="analyst:operator")
    row = await actions.pool.fetchrow(
        "SELECT o.id FROM objects o WHERE o.type='SoftwareProject' AND o.canonical=$1",
        "repo:xxit")
    proj = row["id"]
    assert await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions WHERE object_id=$1 AND name='name'",
        proj) == "xxit"

    # a deliberate, declared rename lands (rename_project, the graph-only verb)
    out = await rename_project(actions, project="xxit", new_name="handlingtheloop",
                               because="operator-approved #110 rename", actor="Thoth")
    assert out["new_name"] == "handlingtheloop"

    # a LATER, ordinary mount from a seat whose pin was never updated — the metron/deckard
    # shape — must NOT silently revert the declared name back to "xxit"
    ident2 = resolve_identity(cwd="/w/xxit", session="sess-b", model="claude-fable-5")
    await register_agent(actions, ident2, actor="analyst:operator")
    winning = await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions WHERE object_id=$1 AND name='name' "
        "ORDER BY confidence DESC, observed_at DESC LIMIT 1", proj)
    assert winning == "handlingtheloop", (
        "a routine mount reverted a declared rename — the exact bug this test guards")
    # the mismatch is still ON THE RECORD, never silently dropped — just outranked
    assert "xxit" in {r["v"] for r in await actions.pool.fetch(
        "SELECT value#>>'{}' AS v FROM current_assertions WHERE object_id=$1 AND name='name'",
        proj)}
    # and the works_in edge still lands on the SAME (renamed) object either way
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='works_in' "
        "AND (valid_until IS NULL OR valid_until > now())", a, proj) == 1


async def test_register_agent_mount_still_heals_case_whitespace_drift(actions: Actions) -> None:
    """The delegated-safe exception (ruling 1db1ff41/decision 8cf283f4) must keep working:
    a case-only difference is NOT a genuine rename, so an ordinary mount may still settle
    it at full confidence — only a REAL rename is protected from being overwritten.

    NOTE: register_agent resolves/creates the SoftwareProject by the LITERAL canonical
    `repo:{identity.project}` — case-sensitive, so "RAMstein" and "ramstein" pins mint TWO
    DIFFERENT objects (confirmed live: till's own repo:RAMstein and repo:ramstein are two
    separate SoftwareProjects today) — a real, separate #137 finding, not this fix's
    concern. This test exercises the exception the way it can actually fire: some OTHER
    writer (correct_project_name, a manual assert) case-normalizes the object's `name`
    property; a later mount from a pin matching the object's own canonical basename must
    still be free to settle it back at full confidence."""
    ident = resolve_identity(cwd="/w/RAMstein", session="sess-c", model="claude-fable-5")
    await register_agent(actions, ident, actor="analyst:operator")
    proj = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='SoftwareProject' AND canonical=$1",
        "repo:RAMstein")
    now = datetime.now(UTC)
    await actions.assert_property(proj, "name", "ramstein", "correct_project_name:test", now,
                                  0.9, evidence_class=EvidenceClass.SELF_DECLARED.value)

    ident2 = resolve_identity(cwd="/w/RAMstein", session="sess-e", model="claude-fable-5")
    await register_agent(actions, ident2, actor="analyst:operator")
    row = await actions.pool.fetchrow(
        "SELECT value#>>'{}' AS v, confidence FROM current_assertions "
        "WHERE object_id=$1 AND name='name' ORDER BY confidence DESC, observed_at DESC LIMIT 1",
        proj)
    assert row["v"] == "RAMstein"
    assert row["confidence"] > 0.8  # full self_declared confidence, not derived-tier


# --- _resolve_or_mint_project — a case-differing pin must FIND the existing SoftwareProject,
# never mint a twin (thread 69911d0c, Thoth dispatch 3824). NEVER lowercase-normalizes:
# cassandra's own "Like-Us" is genuine upstream truth, so this is about matching
# case-insensitively, not about which casing wins. Measured live: till carries exactly one
# pre-existing twin today (repo:RAMstein / repo:ramstein) out of 81 SoftwareProjects
# fleet-wide — this guards both the fix (no NEW twins) and the refusal (no over-merge, and a
# pre-existing twin is never silently arbitrated by a mint-time guess). ---


async def test_resolve_or_mint_project_finds_existing_object_case_insensitively(
    actions: Actions,
) -> None:
    first = await _resolve_or_mint_project(actions, "handlingtheloop", "test")
    second = await _resolve_or_mint_project(actions, "HandlingTheLoop", "test")
    assert first == second
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='SoftwareProject' "
        "AND lower(canonical) = lower('repo:handlingtheloop')") == 1


async def test_resolve_or_mint_project_never_lowercase_normalizes(actions: Actions) -> None:
    """cassandra's own counter-example: the FIRST case seen wins the canonical, whichever
    it is — this function must never coerce it to lowercase on its own authority."""
    proj = await _resolve_or_mint_project(actions, "Like-Us", "test")
    canonical = await actions.pool.fetchval("SELECT canonical FROM objects WHERE id=$1", proj)
    assert canonical == "repo:Like-Us"


async def test_resolve_or_mint_project_lets_a_genuinely_different_project_mint_separately(
    actions: Actions,
) -> None:
    """The refusal case Thoth asked for explicitly: over-merging two REAL, distinct
    projects would be a worse bug than the one being fixed."""
    a = await _resolve_or_mint_project(actions, "conker", "test")
    b = await _resolve_or_mint_project(actions, "conker-detect", "test")
    assert a != b
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='SoftwareProject' "
        "AND canonical IN ('repo:conker', 'repo:conker-detect')") == 2


async def test_resolve_or_mint_project_never_arbitrates_a_pre_existing_twin(
    actions: Actions,
) -> None:
    """till's own live shape: TWO objects already exist for one casefold-equal label
    (a pre-existing twin, not created by this function). This must never pick a winner
    (that's fold_project's job, thread 689d22a2) and must never mint a THIRD."""
    a = await actions.create_or_find_object("SoftwareProject", "repo:RAMstein", "test")
    b = await actions.create_or_find_object("SoftwareProject", "repo:ramstein", "test")
    assert a != b
    got = await _resolve_or_mint_project(actions, "RAMstein", "test")
    assert got == a  # the literal, exact match — unchanged, pre-existing behavior
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='SoftwareProject' "
        "AND lower(canonical) = lower('repo:RAMstein')") == 2  # still exactly 2, never 3


async def test_register_agent_mount_with_case_variant_pin_reuses_the_existing_project(
    actions: Actions,
) -> None:
    """The integration-level proof, matching till's own live shape exactly: two seats
    whose PINS were simply always case-variants of each other (no rename involved at
    all — rename_project never touches `canonical`, so a renamed object's canonical
    stays the pre-rename string forever; that is a different scenario from this one).
    Both mounts must land on ONE SoftwareProject and ONE works_in edge target each —
    not two objects."""
    ident1 = resolve_identity(cwd="/w/RAMstein", session="sess-f", model="claude-fable-5")
    a1 = await register_agent(actions, ident1, actor="analyst:operator")
    ident2 = resolve_identity(cwd="/w/ramstein", session="sess-g", model="claude-fable-5")
    a2 = await register_agent(actions, ident2, actor="analyst:operator")
    proj_ids = await actions.pool.fetch(
        "SELECT DISTINCT o.id FROM links l JOIN objects o ON o.id=l.to_id "
        "WHERE l.from_id = ANY($1::uuid[]) AND l.type='works_in'", [a1, a2])
    assert len(proj_ids) == 1, "a case-variant pin minted a second SoftwareProject object"
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='SoftwareProject' "
        "AND lower(canonical) = lower('repo:RAMstein')") == 1


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


def test_read_house_seat_kind_are_additive_parallel_reads(tmp_path: Path) -> None:
    """Ruling 719ed5b1's pin-schema build: house/seat/kind are new keys read through the SAME
    `_read_osiris_key` helper as project/model, not a new mechanism — so a pin that only ever
    declared project/model (every pin on disk today) answers None for all three without error,
    and a pin that declares them reads back exactly what it wrote. Proves the additive claim:
    old two-key pins need no migration to keep working."""
    from src.orchestrator.agents import (
        read_house_label,
        read_project_label,
        read_seat_handle,
        read_tree_kind,
    )

    old_style = tmp_path / "legacy"
    old_style.mkdir()
    (old_style / ".osiris").write_text('project = "osiris"\nmodel = "claude-sonnet-5"\n')
    assert read_house_label(str(old_style)) is None
    assert read_seat_handle(str(old_style)) is None
    assert read_tree_kind(str(old_style)) is None

    full = tmp_path / "office"
    full.mkdir()
    (full / ".osiris").write_text(
        'project = "osiris"\nhouse = "osiris"\nseat = "imhotep"\nkind = "office"\n')
    assert read_house_label(str(full)) == "osiris"
    assert read_seat_handle(str(full)) == "imhotep"
    assert read_tree_kind(str(full)) == "office"

    container = tmp_path / "container"
    container.mkdir()
    (container / ".osiris").write_text('kind = "container"\n')
    assert read_tree_kind(str(container)) == "container"
    assert read_project_label(str(container)) is None  # no project here — the whole point


def test_read_project_pin_distinguishes_all_three_no_project_shapes(
    tmp_path: Path,
) -> None:
    """THREE tell-apart-able shapes (Sekhmet's design, e3f4f159; widened task #128 wave 2,
    2026-08-03), not two: NO FILE AT ALL, FOUND-BUT-NEVER-SETS-THE-KEY (the heinrich
    boundary case — real, valid, just answering a different question), and COULD NOT READ
    (a file exists but fails to parse). Each must be its own shape, never collapsed with
    either neighbor."""
    from src.orchestrator.agents import read_project_pin

    no_file = tmp_path / "no-osiris"
    no_file.mkdir()
    out = read_project_pin(str(no_file))
    assert out.value is None and out.error is None and out.path is None

    # heinrich: valid TOML, never sets `project` — found, not broken, distinct from both
    # "no file" (path would be None) and "could not read" (error would be set)
    heinrich = tmp_path / "heinrich"
    heinrich.mkdir()
    (heinrich / ".osiris").write_text('model = "claude-fable-5"\n')
    out = read_project_pin(str(heinrich))
    assert out.value is None and out.error is None
    assert out.path == str(heinrich / ".osiris")

    # redmonth/sutra's own malformation: colon syntax is not valid TOML
    malformed = tmp_path / "redmonth"
    malformed.mkdir()
    (malformed / ".osiris").write_text('project: "redmonth"\n')
    out = read_project_pin(str(malformed))
    assert out.value is None
    assert out.error is not None and "TOMLDecodeError" in out.error  # the actual parse error
    assert out.path == str(malformed / ".osiris")


def test_read_project_pin_never_climbs_past_a_deleted_cwd_into_a_real_ancestors_pin(
    tmp_path: Path,
) -> None:
    """THE FOURTH SHAPE (Thoth's catch, msg 3928/thread 3937): flip68real/resumelanecheck
    were real, now-retired Seats whose office directories were DELETED, not merely
    unpinned. Before this fix, querying a nonexistent cwd climbed straight past it to the
    enclosing container's own pin and reported THAT as the deleted office's own state —
    exactly the shape reproduced here: a real container with its own `.osiris`, and a
    `deletedseat` subdirectory that is NEVER created. The queried path's own nonexistence
    must be the answer, never a borrowed ancestor's declaration."""
    from src.orchestrator.agents import read_project_pin

    container = tmp_path / "seats"
    container.mkdir()
    (container / ".osiris").write_text('kind = "container"\n')
    ghost_office = container / "deletedseat"  # never created — the office is GONE

    out = read_project_pin(str(ghost_office))
    assert out.cwd_missing is True
    assert out.value is None
    assert out.path is None, "must never report the container's own file as this path's pin"
    assert out.error is None


def test_read_project_pin_cwd_missing_is_false_for_a_real_but_unpinned_directory(
    tmp_path: Path,
) -> None:
    """The fourth state must never leak into the ordinary case: a real directory with no
    pin anywhere in its climb still reads cwd_missing=False — only a query against a
    directory that does not exist at all sets it."""
    from src.orchestrator.agents import read_project_pin

    real = tmp_path / "realbutunpinned"
    real.mkdir()
    out = read_project_pin(str(real))
    assert out.cwd_missing is False
    assert out.value is None and out.error is None and out.path is None


def test_resolve_identity_keeps_pin_missing_and_cwd_missing_disjoint(
    tmp_path: Path,
) -> None:
    """A deleted office and an unpinned-but-real office are opposite dispositions (one
    wants the graph's stale belief reaped, the other wants a pin written) — resolve_identity
    must never report both flags true for the same identity, and the deleted-office case
    must report project_pin_cwd_missing, never project_pin_missing (the pre-fix collapse)."""
    container = tmp_path / "seats"
    container.mkdir()
    (container / ".osiris").write_text('kind = "container"\n')
    ghost = container / "deletedseat"

    ident = resolve_identity(cwd=str(ghost), job_dir="/j/jobs/ghost0001")
    assert ident.project_pin_cwd_missing is True
    assert ident.project_pin_missing is False
    assert ident.project == "deletedseat"  # the basename guess still fires, unchanged

    real_unpinned = tmp_path / "realoffice"
    real_unpinned.mkdir()
    ident2 = resolve_identity(cwd=str(real_unpinned), job_dir="/j/jobs/real0001")
    assert ident2.project_pin_missing is True
    assert ident2.project_pin_cwd_missing is False


def test_project_pin_banner_names_a_deleted_cwd_distinctly(tmp_path: Path) -> None:
    from src.orchestrator.agents import project_pin_banner

    container = tmp_path / "seats"
    container.mkdir()
    ghost = container / "deletedseat"
    ident = resolve_identity(cwd=str(ghost), job_dir="/j/jobs/ghost0002")
    banner = project_pin_banner(ident)
    assert banner is not None
    assert "DOES NOT EXIST ON DISK" in banner
    assert str(ghost) in banner


def test_project_pin_banner_is_silent_for_a_declared_container_pin(tmp_path: Path) -> None:
    """Thoth LXXVI's live catch on his own mount: every mount at ~/.osiris/seats got the
    'NEVER DECLARES project' banner even though that pin deliberately declares
    `kind = "container"` (ruling 719ed5b1) and answers a DIFFERENT question on purpose —
    the sanctioned non-project case, not an oversight. The banner's own "fell back to a
    BASENAME GUESS" text was doubly wrong: no basename guess runs for a bare root at all
    (resolve_identity's own bare_root branch keeps project None there), and by the time a
    SEATED session's project shows in the rendered message it's the seat's own house
    resolution, unrelated to any guess. A container-kind pin — declared here explicitly,
    not just the hardcoded bare-office-root path — gets no banner at all."""
    from src.orchestrator.agents import project_pin_banner

    container = tmp_path / "seats"
    container.mkdir()
    (container / ".osiris").write_text('kind = "container"\n')
    ident = resolve_identity(cwd=str(container), job_dir="/j/jobs/container0001")
    assert ident.project_pin_path == str(container / ".osiris")  # the file WAS found
    assert project_pin_banner(ident) is None


def test_write_attribution_banner_fires_on_a_genuine_disagreement() -> None:
    from src.orchestrator.agents import write_attribution_banner

    ident = AgentIdentity(agent_id="agent:wa01", session="wa01", project="newproj",
                          model=None, cwd="/x", write_attribution_agreement="disagrees",
                          write_attribution_top="oldproj", write_attribution_total=4)
    banner = write_attribution_banner(ident)
    assert banner is not None
    assert "oldproj" in banner and "newproj" in banner


def test_write_attribution_banner_silent_when_agreement_says_so() -> None:
    from src.orchestrator.agents import write_attribution_banner

    confirms = AgentIdentity(agent_id="agent:wa02", session="wa02", project="proj",
                             model=None, cwd="/x", write_attribution_agreement="confirms",
                             write_attribution_top="proj", write_attribution_total=1)
    assert write_attribution_banner(confirms) is None

    no_signal = AgentIdentity(agent_id="agent:wa03", session="wa03", project=None,
                              model=None, cwd="/x", write_attribution_agreement="no-signal")
    assert write_attribution_banner(no_signal) is None


def test_write_attribution_banner_ignores_a_stale_disagreement_flag(
) -> None:
    """Thoth LXXVI's live catch on his own mount: `write_attribution_agreement` is
    stamped by register_agent BEFORE `_resolve_project_seat_first` runs (deliberately —
    see that function's own docstring), so a SEATED session's flag can be "disagrees"
    against the PRE-seat-override project even though `ident.project` is already the
    POST-override, final value by the time this banner would render. The stored flag
    said "disagrees"; the two values THIS message would show are equal ("osiris" and
    "osiris") — the banner must stay silent rather than show itself agreeing with itself."""
    from src.orchestrator.agents import write_attribution_banner

    ident = AgentIdentity(agent_id="agent:wa04", session="wa04", project="osiris",
                          model=None, cwd="/home/x/.osiris/seats",
                          write_attribution_agreement="disagrees",
                          write_attribution_top="osiris", write_attribution_total=6)
    assert write_attribution_banner(ident) is None


def test_read_project_pin_climbs_past_a_worktree_gitlink_to_the_real_root(
    tmp_path: Path,
) -> None:
    """THE ROOT CAUSE (task #128, 2026-08-05): a git worktree's own `.git` is a FILE (a
    gitlink), not a directory. The old `.exists()` climb-stop treated that file exactly
    like a real repo root and gave up one layer too early — so every seat's own code
    checkout (`.claude/worktrees/<seat>`, no `.osiris` of its own) silently fell back to
    its OWN directory name instead of ever seeing the real root's pin, on every single
    mount. The climb must see straight through a gitlink file to the true root."""
    from src.orchestrator.agents import read_project_pin

    root = tmp_path / "osiris"
    root.mkdir()
    (root / ".git").mkdir()
    (root / ".osiris").write_text('project = "osiris"\n')

    worktree = root / ".claude" / "worktrees" / "sekhmet"
    worktree.mkdir(parents=True)
    (worktree / ".git").write_text("gitdir: /somewhere/.git/worktrees/sekhmet\n")

    out = read_project_pin(str(worktree))
    assert out.value == "osiris"
    assert out.error is None


def test_read_project_pin_climbs_past_a_worktree_pin_that_never_sets_project(
    tmp_path: Path,
) -> None:
    """THE LIVE REGRESSION (caught and reverted the same minute, ruling 719ed5b1's schema
    rollout): a worktree pin declaring seat/house/kind (never project/model) sits BELOW its
    repo root's own project/model pin — the first LAYERED declaration this reader was ever
    exercised against. Before the fix, the worktree's OWN `.osiris` existing at all stopped
    the climb outright (`f.is_file()` was treated as terminal regardless of whether it
    answered `project`), so `read_project_label` silently went from the root's real value to
    None the instant a worktree pin was written — this is the exact live specimen (imhotep's
    own worktree), reproduced here rather than only described in a decision."""
    from src.orchestrator.agents import read_project_label, read_project_model

    root = tmp_path / "osiris"
    root.mkdir()
    (root / ".git").mkdir()
    (root / ".osiris").write_text('project = "osiris"\nmodel = "claude-sonnet-5"\n')

    worktree = root / ".claude" / "worktrees" / "imhotep"
    worktree.mkdir(parents=True)
    (worktree / ".git").write_text("gitdir: /somewhere/.git/worktrees/imhotep\n")
    (worktree / ".osiris").write_text('house = "osiris"\nkind = "worktree"\nseat = "imhotep"\n')

    assert read_project_label(str(worktree)) == "osiris"
    assert read_project_model(str(worktree)) == "claude-sonnet-5"


def test_read_project_pin_reports_the_nearest_unset_file_when_no_ancestor_sets_it_either(
    tmp_path: Path,
) -> None:
    """The heinrich diagnostic survives climb-continuation: when NEITHER the near file nor
    any ancestor ever sets the key, the NEAREST found-but-unset file's path is still what's
    reported — proof this isn't just "keep climbing and forget," the closest real answer to
    "why did this fall back to a basename guess" is preserved."""
    from src.orchestrator.agents import read_project_pin

    root = tmp_path / "root"
    root.mkdir()
    (root / ".git").mkdir()
    (root / ".osiris").write_text('model = "claude-opus-5"\n')  # never sets project either

    child = root / "nested"
    child.mkdir()
    (child / ".osiris").write_text('model = "claude-sonnet-5"\n')  # nearest, never sets it

    out = read_project_pin(str(child))
    assert out.value is None
    assert out.error is None
    assert out.path == str(child / ".osiris"), "the NEAREST unset file, not the root's"


def test_read_project_pin_still_stops_at_a_real_repo_root(tmp_path: Path) -> None:
    """The other half of the same fix: a REAL `.git` directory must still stop the climb —
    an unrelated ancestor's `.osiris` (a different repo entirely) must never leak in just
    because the fix widened what counts as a boundary. Unchanged behavior, proven not to
    have regressed."""
    from src.orchestrator.agents import read_project_pin

    grandparent = tmp_path / "unrelated-parent"
    (grandparent / ".osiris").parent.mkdir(parents=True)
    (grandparent / ".osiris").write_text('project = "wrong-project"\n')

    repo = grandparent / "real-repo"
    repo.mkdir()
    (repo / ".git").mkdir()  # a REAL root — no .osiris of its own

    out = read_project_pin(str(repo))
    assert out.value is None  # stops at repo's own real root, never sees the grandparent
    assert out.error is None and out.path is None


async def test_write_model_pin_creates_and_is_idempotent(tmp_path: Path,
                                                          monkeypatch: pytest.MonkeyPatch) -> None:
    """Unit-level proof of the write helper itself, isolated from register_agent's own
    plumbing: creates a fresh pin when none exists, updates an existing one in place while
    preserving `project`, and writes NOTHING on a second call with the same value — the
    idempotency `_link_once`/set_charter's own callers all rely on, applied to a filesystem
    write instead of a graph one."""
    from src.orchestrator import agents as agents_mod
    from src.orchestrator.agents import write_model_pin

    office_root = tmp_path / "offices"
    monkeypatch.setattr(agents_mod, "_DEFAULT_OFFICE_ROOT", office_root)
    pin = office_root / "freshseat" / ".osiris"

    wrote = await write_model_pin("FreshSeat", "claude-sonnet-5")
    assert wrote is True
    assert pin.read_text() == 'model = "claude-sonnet-5"\n'

    again = await write_model_pin("FreshSeat", "claude-sonnet-5")
    assert again is False, "an unchanged value must not churn the disk"
    assert pin.read_text() == 'model = "claude-sonnet-5"\n'   # byte-identical, not just equal

    changed = await write_model_pin("FreshSeat", "claude-opus-5")
    assert changed is True
    assert pin.read_text() == 'model = "claude-opus-5"\n'


async def test_write_model_pin_refuses_a_value_that_would_corrupt_the_toml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defensive floor, never a validated allowlist (model ids change; this file has no
    business hard-coding them) — but a value containing a quote or newline would corrupt
    the TOML it's embedded in, so those are refused outright rather than trusted."""
    from src.orchestrator import agents as agents_mod
    from src.orchestrator.agents import write_model_pin

    office_root = tmp_path / "offices"
    monkeypatch.setattr(agents_mod, "_DEFAULT_OFFICE_ROOT", office_root)

    assert await write_model_pin("Injecto", 'claude"; evil = "yes') is False
    assert await write_model_pin("Injecto", "claude-sonnet-5\nmalicious = true") is False
    assert await write_model_pin("Injecto", "") is False
    assert not (office_root / "injecto" / ".osiris").exists()


def test_resolve_identity_resolves_the_governed_project_from_a_seat_worktree(
    tmp_path: Path,
) -> None:
    """End to end, at the layer register_agent actually reads: a seated agent's own code
    worktree now resolves the ENCLOSING repo's governed project, not the worktree's own
    directory name (which, for every seat in this fleet, IS the seat's own name — the
    exact live specimen measured on sekhmet's and imhotep's own current identities)."""
    root = tmp_path / "osiris"
    root.mkdir()
    (root / ".git").mkdir()
    (root / ".osiris").write_text('project = "osiris"\n')

    worktree = root / ".claude" / "worktrees" / "sekhmet"
    worktree.mkdir(parents=True)
    (worktree / ".git").write_text("gitdir: /somewhere/.git/worktrees/sekhmet\n")

    ident = resolve_identity(cwd=str(worktree), job_dir="/j/jobs/worktree1")
    assert ident.project == "osiris"          # not "sekhmet", the old basename guess
    assert ident.project_pin_error is None
    assert ident.project_pin_missing is False


def test_resolve_identity_still_falls_back_to_basename_when_truly_unpinned_anywhere(
    tmp_path: Path,
) -> None:
    """THE REFUSAL, TESTED NOT JUST THE SUCCESS (577988ed still governs): the climb widening
    must not manufacture a pin out of nothing. A worktree of a repo that has NO `.osiris`
    anywhere in its own climb — real root included — still honestly falls back to the
    basename guess, unchanged from before this fix."""
    root = tmp_path / "unpinned-repo"
    root.mkdir()
    (root / ".git").mkdir()  # a real root, but never pinned

    worktree = root / ".claude" / "worktrees" / "someseat"
    worktree.mkdir(parents=True)
    (worktree / ".git").write_text("gitdir: /somewhere/.git/worktrees/someseat\n")

    ident = resolve_identity(cwd=str(worktree), job_dir="/j/jobs/worktree2")
    assert ident.project == "someseat"        # honest basename fallback, unbroken
    assert ident.project_pin_missing is True


def test_read_project_label_and_model_still_collapse_both_no_declaration_causes(
    tmp_path: Path,
) -> None:
    """Backward compatibility for the 6 existing plain-string callers (seats.py,
    mcp_server.py, project_identity.py, census.py): a couldn't-read file must still return
    a bare None here — the richer signal is additive, reached only through
    `read_project_pin`, never a behavior change for callers that don't ask for it."""
    from src.orchestrator.agents import read_project_label, read_project_model

    malformed = tmp_path / "sutra"
    malformed.mkdir()
    (malformed / ".osiris").write_text('project: "sutra"\n')
    assert read_project_label(str(malformed)) is None
    assert read_project_model(str(malformed)) is None


def test_resolve_identity_carries_the_could_not_read_signal_but_still_falls_back(
    tmp_path: Path,
) -> None:
    """A broken pin must not break resolution — `project` still falls back to the
    basename exactly as a no-declaration would — but the identity now carries ENOUGH to
    confess it (path + error), where before it silently looked identical to "never
    pinned"."""
    repo = tmp_path / "redmonth"
    repo.mkdir()
    (repo / ".osiris").write_text('project: "redmonth"\n')
    ident = resolve_identity(cwd=str(repo), job_dir="/j/jobs/brokenpin1")
    assert ident.project == "redmonth"                    # basename fallback, unbroken
    assert ident.project_pin_error is not None
    assert ident.project_pin_path == str(repo / ".osiris")


def test_resolve_identity_flags_project_pin_missing_on_genuine_no_declaration(
    tmp_path: Path,
) -> None:
    """No .osiris anywhere in the climb: still no error, still no found-but-unset path —
    but project_pin_missing is now True (task #128 wave 2), the third leg that used to be
    indistinguishable from the heinrich shape."""
    repo = tmp_path / "undeclared"
    repo.mkdir()
    ident = resolve_identity(cwd=str(repo), job_dir="/j/jobs/undeclared1")
    assert ident.project_pin_error is None and ident.project_pin_path is None
    assert ident.project_pin_missing is True


def test_resolve_identity_carries_found_but_unset_without_masquerading_as_broken(
    tmp_path: Path,
) -> None:
    """The heinrich boundary case, exercised through the real resolution path (task #128
    wave 2): a valid but incomplete .osiris file must never masquerade as a broken one
    (project_pin_error stays None) — but it must ALSO no longer masquerade as no file at
    all (project_pin_path is now set, project_pin_missing is False), so a caller can build
    the "found, valid, never declares project" message rather than the "no pin anywhere"
    one for this exact shape."""
    heinrich = tmp_path / "heinrich"
    heinrich.mkdir()
    (heinrich / ".osiris").write_text('model = "claude-fable-5"\n')
    ident = resolve_identity(cwd=str(heinrich), job_dir="/j/jobs/heinrich1")
    assert ident.project_pin_error is None
    assert ident.project_pin_path == str(heinrich / ".osiris")
    assert ident.project_pin_missing is False


def test_resolve_identity_never_confesses_an_explicit_project_label_override(
    tmp_path: Path,
) -> None:
    """An explicit `project_label=` (the env-override lane) short-circuits the cwd read
    entirely — even a malformed .osiris underfoot must never surface a confession for a
    file the resolution never actually touched."""
    repo = tmp_path / "redmonth"
    repo.mkdir()
    (repo / ".osiris").write_text('project: "redmonth"\n')
    ident = resolve_identity(cwd=str(repo), job_dir="/j/jobs/override1",
                             project_label="explicit-override")
    assert ident.project == "explicit-override"
    assert ident.project_pin_error is None and ident.project_pin_path is None
    assert ident.project_pin_missing is False


def test_project_pin_banner_fires_only_on_the_two_real_errors(tmp_path: Path) -> None:
    """Task #128 wave 2, NARROWED by ruling fe8ec7ff mechanism (1): the operator's own
    standard is that an unset project is a VALID state, not an error — so only the two
    genuinely unrepairable shapes (cwd gone, TOML broken) still warn here. The other two
    (no pin anywhere, pin found but never declares project) moved to `project_pin_state`,
    covered separately below. Silent when a real pin was found and used, or for the bare
    seat-office root carve-out (ruling 577988ed)."""
    from src.orchestrator.agents import project_pin_banner

    clean = AgentIdentity(agent_id="agent:clean1", session="clean1", project="fine",
                          model=None, cwd="/x")
    assert project_pin_banner(clean) is None

    broken = AgentIdentity(agent_id="agent:broken1", session="broken1", project="redmonth",
                           model=None, cwd="/x/redmonth",
                           project_pin_error="TOMLDecodeError: expected '='",
                           project_pin_path="/x/redmonth/.osiris")
    banner = project_pin_banner(broken)
    assert banner is not None
    assert "/x/redmonth/.osiris" in banner
    assert "TOMLDecodeError" in banner
    assert "redmonth" in banner  # the basename-fallback value it landed on, named

    found_unset = AgentIdentity(agent_id="agent:heinrich1", session="heinrich1",
                                project="heinrich", model=None, cwd="/x/heinrich",
                                project_pin_path="/x/heinrich/.osiris")
    assert project_pin_banner(found_unset) is None  # unset is valid, not a banner anymore

    missing = AgentIdentity(agent_id="agent:tantra1", session="tantra1", project="tantra",
                            model=None, cwd="/x/tantra", project_pin_missing=True)
    assert project_pin_banner(missing) is None  # same — no pin at all is also just unset

    # bare seat-office root carve-out (577988ed): resolve_identity never sets
    # project_pin_missing here even though nothing is pinned — the banner must stay silent
    bare_root = AgentIdentity(agent_id="agent:anon1", session="anon1", project=None,
                              model=None, cwd="/home/x/.osiris/seats")
    assert project_pin_banner(bare_root) is None


def test_project_pin_state_reports_the_two_unset_shapes_calmly(tmp_path: Path) -> None:
    """The two shapes carved OUT of project_pin_banner by ruling fe8ec7ff: unset is a valid
    state (general-purpose / not yet named), never an alarm — no `⚠`, no "BASENAME GUESS"
    framing. Silent for everything project_pin_banner itself still owns (real errors) and
    for a clean pin."""
    from src.orchestrator.agents import project_pin_state

    clean = AgentIdentity(agent_id="agent:clean2", session="clean2", project="fine",
                          model=None, cwd="/x")
    assert project_pin_state(clean) is None

    broken = AgentIdentity(agent_id="agent:broken2", session="broken2", project="redmonth",
                           model=None, cwd="/x/redmonth",
                           project_pin_error="TOMLDecodeError: expected '='",
                           project_pin_path="/x/redmonth/.osiris")
    assert project_pin_state(broken) is None  # a real error, project_pin_banner's own case

    found_unset = AgentIdentity(agent_id="agent:heinrich2", session="heinrich2",
                                project="heinrich", model=None, cwd="/x/heinrich",
                                project_pin_path="/x/heinrich/.osiris")
    state = project_pin_state(found_unset)
    assert state is not None
    assert "⚠" not in state
    assert "BASENAME GUESS" not in state
    assert "valid state, not an error" in state
    assert "/x/heinrich/.osiris" in state

    missing = AgentIdentity(agent_id="agent:tantra2", session="tantra2", project="tantra",
                            model=None, cwd="/x/tantra", project_pin_missing=True)
    state = project_pin_state(missing)
    assert state is not None
    assert "⚠" not in state
    assert "valid state, not an error" in state
    assert "/x/tantra" in state


def test_resolve_identity_never_flags_project_pin_missing_at_the_bare_seat_root(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    """Ruling 577988ed's own carve-out, through the real resolution path: the bare seat-
    office container has no pin and no single project of its own — project stays None AND
    project_pin_missing stays False, so no wave-2 banner ever fires there.

    Patches `offices._DEFAULT_OFFICE_ROOT` (not agents.py's own imported name): resolve_identity
    now calls the shared `is_bare_office_root()` (offices.py) instead of a private duplicate of
    the same path-equality check (the 38c71544 dedup, ruling 719ed5b1's pin-schema build) — the
    module that OWNS the comparison is the one whose global must move for the test to see it."""
    from src.orchestrator import offices as offices_mod

    seats_root = tmp_path / "seats"
    seats_root.mkdir()
    monkeypatch.setattr(offices_mod, "_DEFAULT_OFFICE_ROOT", seats_root)
    ident = resolve_identity(cwd=str(seats_root), job_dir="/j/jobs/bareroot1")
    assert ident.project is None
    assert ident.project_pin_missing is False


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


async def _seat_intended_model(actions: Actions, seat_id: str) -> str | None:
    return await actions.pool.fetchval(
        "SELECT x.value #>> '{}' FROM current_assertions x JOIN objects o ON o.id=x.object_id "
        "WHERE o.canonical=$1 AND x.name='intended_model' "
        "ORDER BY x.confidence DESC, x.observed_at DESC LIMIT 1", seat_id)


async def test_a_deliberate_swap_stamps_the_seats_intended_model(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE STANDING-CHOICE WRITE SIDE (operator ruling e0e0955d, confirming his own 1aca1fcc
    from 2026-07-19): a /model command on the record IS the operator re-pinning this seat's
    standing choice — register_agent now auto-stamps intended_model on the held Seat so
    successions and relaunches inherit it with no manual re-pin. The READ side already
    existed (mint_seat's own pin, launch()'s precedence since 70ae3c3); this is the write."""
    from src.orchestrator.agents import claim_name
    from src.orchestrator.seats import held_seat

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
    assert ident.model_deliberate is True   # sanity: this fixture IS the /model-on-record case

    await register_agent(actions, ident, actor="analyst:operator")   # mints the Agent, no seat
    await claim_name(actions, ident.agent_id, "Deliberato", source=ident.agent_id)

    await register_agent(actions, ident, actor="analyst:operator", expected_model="claude-fable-5")

    seat = await held_seat(actions.pool, ident.agent_id)
    assert seat is not None
    assert await _seat_intended_model(actions, seat["seat_id"]) == "claude-haiku-4-5"


async def test_an_unexplained_swap_never_overwrites_the_standing_choice(
    actions: Actions, tmp_path: Path,
) -> None:
    """The other half of e0e0955d: a rug-pull (no /model on the record — the harness's own
    silent demotion) must NEVER overwrite the seat's standing choice. Pre-seeds an existing
    intended_model to prove it survives untouched, not merely that a fresh one is absent."""
    from src.orchestrator.agents import claim_name
    from src.orchestrator.seats import held_seat

    proj = tmp_path / "-home-x-code-osiris"
    proj.mkdir()
    _transcript_lines(proj, "claude-fable-5", "claude-opus-4-8")   # no /model command
    ident = resolve_identity(cwd="/x/osiris", job_dir="/j/jobs/deadbeef",
                             store_reading=_store_reading(tmp_path, "/j/jobs/deadbeef"))
    assert ident.model_deliberate is False   # sanity: this fixture is the rug-pull case

    await register_agent(actions, ident, actor="analyst:operator")
    await claim_name(actions, ident.agent_id, "Rugpuller", source=ident.agent_id)
    seat = await held_seat(actions.pool, ident.agent_id)
    assert seat is not None
    now = datetime.now(UTC)
    soid = await actions.create_or_find_object("Seat", seat["seat_id"], "test")
    await actions.assert_property(soid, "intended_model", "claude-sonnet-5", "test", now, 0.9,
                                  evidence_class="self_declared")

    await register_agent(actions, ident, actor="analyst:operator", expected_model="claude-fable-5")

    assert await _seat_intended_model(actions, seat["seat_id"]) == "claude-sonnet-5"  # untouched


# ═══ task #146 (/model MUST BE AUTHORITATIVE, the operator's own complaint): the graph-side
# intended_model stamp above already existed, but nothing durable ever fed back into the
# .osiris file itself — the ONE thing launch()'s own precedence and the swap-divergence
# banner both actually read first. And model_swapped fired for a WITNESSED, deliberate
# /model exactly like a harness rug-pull — a false positive on the danger map the property
# exists to serve. Both close here. ═══


async def test_a_deliberate_swap_writes_the_seats_osiris_pin(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE WRITE (task #146): the operator's own /model command on the record now updates
    the seat's `.osiris` file, not just the graph's intended_model stamp — the pin becomes a
    CACHE of the decision instead of a hand-edited, permanently-stale competing claim.
    Existing `project` in the pin must survive untouched — this writes model, never invents
    a project the seat never declared."""
    from src.orchestrator import agents as agents_mod
    from src.orchestrator.agents import claim_name
    from src.orchestrator.seats import held_seat

    office_root = tmp_path / "offices"
    monkeypatch.setattr(agents_mod, "_DEFAULT_OFFICE_ROOT", office_root)

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
    assert ident.model_deliberate is True

    await register_agent(actions, ident, actor="analyst:operator")
    await claim_name(actions, ident.agent_id, "Pinwriter", source=ident.agent_id)
    seat = await held_seat(actions.pool, ident.agent_id)
    assert seat is not None
    pin = office_root / str(seat["handle"]).lower() / ".osiris"
    pin.parent.mkdir(parents=True)
    pin.write_text('project = "osiris"\n')  # a real pin already declares the project

    await register_agent(actions, ident, actor="analyst:operator", expected_model="claude-fable-5")

    text = pin.read_text()
    assert 'model = "claude-haiku-4-5"' in text
    assert 'project = "osiris"' in text, "an existing project declaration must survive"


async def test_a_deliberate_swap_writes_the_office_pin_even_when_mounted_from_a_worktree(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """VERIFICATION for Thoth's #146 close-out ask (bears_on 600a6f85): a session mounted
    from a code WORKTREE cwd (`.../.claude/worktrees/<name>`, several directories below the
    real repo root, and never the seat's own office) must still land the pin at the SEAT'S
    OFFICE, never at or near the worktree — write_model_pin resolves the office purely from
    the seat handle via held_seat, structurally blind to identity.cwd, so the sibling test
    above (cwd='/x/osiris', an office-shaped path) already proves this by construction; this
    test proves it empirically with a cwd that is deliberately NOT office-shaped, so the two
    together cover both mount contexts named in the ask rather than assuming the code path
    is cwd-independent from reading it alone."""
    from src.orchestrator import agents as agents_mod
    from src.orchestrator.agents import claim_name
    from src.orchestrator.seats import held_seat

    office_root = tmp_path / "offices"
    monkeypatch.setattr(agents_mod, "_DEFAULT_OFFICE_ROOT", office_root)

    worktree_cwd = "/home/x/code/osiris/.claude/worktrees/some-branch"
    proj = tmp_path / "-home-x-code-osiris--claude-worktrees-some-branch"
    proj.mkdir()
    lines = [
        json.dumps({"type": "assistant", "message": {"model": "claude-fable-5", "content": []}}),
        json.dumps({"type": "user", "message": {
            "role": "user", "content": "<command-name>/model</command-name>\n"
                                       "<command-message>model</command-message>"}}),
        json.dumps({"type": "assistant",
                    "message": {"model": "claude-haiku-4-5", "content": []}}),
    ]
    (proj / "deadbeef2-1111-2222-3333-444455556667.jsonl").write_text("\n".join(lines) + "\n")
    ident = resolve_identity(cwd=worktree_cwd, job_dir="/j/jobs/deadbeef2",
                             store_reading=_store_reading(tmp_path, "/j/jobs/deadbeef2"))
    assert ident.model_deliberate is True

    await register_agent(actions, ident, actor="analyst:operator")
    await claim_name(actions, ident.agent_id, "Worktreewriter", source=ident.agent_id)
    seat = await held_seat(actions.pool, ident.agent_id)
    assert seat is not None

    await register_agent(actions, ident, actor="analyst:operator", expected_model="claude-fable-5")

    office_pin = office_root / str(seat["handle"]).lower() / ".osiris"
    assert office_pin.exists(), "the pin must land at the OFFICE, even mounted from a worktree"
    assert 'model = "claude-haiku-4-5"' in office_pin.read_text()
    assert not (Path(worktree_cwd) / ".osiris").exists(), \
        "the worktree cwd itself must never receive a pin write"


async def test_an_unexplained_swap_never_touches_the_pin_file(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE REFUSAL, TESTED NOT JUST THE SUCCESS (577988ed still governs — this sits on the
    mount path every seat traverses): a harness rug-pull (no /model on the record) must
    never touch the pin file, exactly as it must never overwrite the graph's intended_model
    (the sibling test above it). No pin, before or after — the fallback writes nothing."""
    from src.orchestrator import agents as agents_mod
    from src.orchestrator.agents import claim_name
    from src.orchestrator.seats import held_seat

    office_root = tmp_path / "offices"
    monkeypatch.setattr(agents_mod, "_DEFAULT_OFFICE_ROOT", office_root)

    proj = tmp_path / "-home-x-code-osiris"
    proj.mkdir()
    _transcript_lines(proj, "claude-fable-5", "claude-opus-4-8")   # no /model command
    ident = resolve_identity(cwd="/x/osiris", job_dir="/j/jobs/deadbeef",
                             store_reading=_store_reading(tmp_path, "/j/jobs/deadbeef"))
    assert ident.model_deliberate is False

    await register_agent(actions, ident, actor="analyst:operator")
    await claim_name(actions, ident.agent_id, "Rugpuller2", source=ident.agent_id)
    seat = await held_seat(actions.pool, ident.agent_id)
    assert seat is not None

    await register_agent(actions, ident, actor="analyst:operator", expected_model="claude-fable-5")

    pin = office_root / str(seat["handle"]).lower() / ".osiris"
    assert not pin.exists(), "a harness swap must never mint or touch the seat's pin"


async def test_a_deliberate_swap_does_not_stamp_model_swapped(
    actions: Actions, tmp_path: Path,
) -> None:
    """RE-SCOPED (task #146, the operator's own words: "a rug pull ... vs a direct /model
    swap on my part is different"): model_swapped is the digest's danger-map signal — a
    WITNESSED, deliberate /model must never trip it. The choice is recorded durably
    elsewhere (intended_model + the pin); it is not a danger sighting."""
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
    assert ident.model_deliberate is True

    a = await register_agent(actions, ident, actor="analyst:operator",
                             expected_model="claude-fable-5")

    swapped = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='model_swapped'", a)
    assert swapped is None, "a witnessed, deliberate /model must never read as a rug-pull"


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
    # succeeded_from IS repopulated on this idempotent, non-minting call (the reconstruction
    # fix, msg 4673): a fresh AgentIdentity built for an ALREADY-minted agent used to lose
    # this fact permanently the moment resolve_identity() rebuilt it with no DB access —
    # register_agent now recovers it from the graph's own durable property instead of
    # leaving it None. model_succession stays None: no seam fired on this call, unrelated.
    assert again.succeeded_from == "agent:0806072e" and again.model_succession is None
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


async def test_register_agent_recovers_succeeded_from_after_a_simulated_bounce(
    actions: Actions,
) -> None:
    """THE RECONSTRUCTION FIX (msg 4673, operator-authorized): a fresh `AgentIdentity`
    built the way `resolve_identity()` genuinely builds one — no pool, `succeeded_from`
    at its dataclass default of None — for an agent that ALREADY has a real predecessor
    in the graph must NOT lose that fact just because it wasn't reconstructed via the
    in-memory `_agents[key]` cache (a server bounce, `_reattach`'s own cache-miss path).
    Before this fix, `identity.succeeded_from` stayed None forever from that point on;
    orient()'s `if ident and ident.succeeded_from:` gate never opened again for that
    session, though the Agent's own `succeeded_from` property was correct throughout."""
    ancestor = _anchored("claude-opus-4-8")
    await register_agent(actions, ancestor, actor="analyst:operator")
    heir = _anchored("claude-fable-5")
    heir_id = await register_agent(actions, heir, actor="analyst:operator")
    assert heir.agent_id == "agent:0806072e-ii"
    assert heir.succeeded_from == "agent:0806072e"  # the real mint, correct as before

    # SIMULATE THE BOUNCE: a brand-new AgentIdentity for the SAME (already-minted) agent,
    # exactly as resolve_identity() would build one on a cache-miss re-attach — no memory
    # of the mint that just happened, succeeded_from unset.
    reconstructed = AgentIdentity(agent_id="agent:0806072e-ii", session="0806072e",
                                  project="sibling-two", model="claude-fable-5",
                                  cwd="/w/sibling-two", model_method="job_dir",
                                  model_history=("claude-fable-5",))
    assert reconstructed.succeeded_from is None  # the bug's own precondition, confirmed

    second_id = await register_agent(actions, reconstructed, actor="analyst:operator")

    assert second_id == heir_id  # no new mint — same agent, correctly recognized as head
    assert reconstructed.succeeded_from == "agent:0806072e"  # recovered, not lost


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


# ═══ thread 20af2c95, the mint_heir edge leak (measured fleet-wide 2026-08-03: 906 of
# 6,245 live works_in/governs edges point from a superseded generation, not the current
# head — mint_heir minted a fresh edge for the heir but never invalidated the
# ancestor's) ═══


async def test_mint_heir_moves_the_ancestors_works_in_and_governs_edges_to_the_heir(
    actions: Actions,
) -> None:
    """The fix itself: an ancestor's live works_in AND governs edges — to projects OTHER
    than the one it's about to inherit via `house` too, since a real agent can work_in/
    govern more than one project across its life — move to the fresh heir, invalidated on
    the ancestor, never duplicated on the heir."""
    from src.orchestrator.agents import mint_heir

    now = datetime.now(UTC)
    ancestor_id = "agent:leak1anc0"
    ancestor_oid = await actions.create_or_find_object("Agent", ancestor_id, "test")
    proj_a = await actions.create_or_find_object("SoftwareProject", "repo:leakproja", "test")
    proj_b = await actions.create_or_find_object("SoftwareProject", "repo:leakprojb", "test")
    await actions.create_link(ancestor_oid, proj_a, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    await actions.create_link(ancestor_oid, proj_b, "governs", "test", now, 0.9,
                              evidence_class="self_declared")

    heir, heir_oid = await mint_heir(actions, ancestor_id, ancestor_oid,
                                     because="compaction", succession=None)

    live_on_ancestor = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 "
        "AND type IN ('works_in','governs') AND (valid_until IS NULL OR valid_until > now())",
        ancestor_oid)
    assert live_on_ancestor == 0, "the ancestor's own edges must no longer read live"
    live_works_in_on_heir = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='works_in' "
        "AND (valid_until IS NULL OR valid_until > now())", heir_oid, proj_a)
    assert live_works_in_on_heir == 1
    live_governs_on_heir = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='governs' "
        "AND (valid_until IS NULL OR valid_until > now())", heir_oid, proj_b)
    assert live_governs_on_heir == 1
    # HISTORY PRESERVED, NEVER DELETED: the invalidated row is still there, walkable
    still_on_record = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type IN ('works_in','governs')",
        ancestor_oid)
    assert still_on_record == 2, "invalidated, not deleted — who worked here stays answerable"


async def test_mint_heir_never_duplicates_an_edge_the_heir_already_has(
    actions: Actions,
) -> None:
    """Idempotent, same discipline as every other estate-move in this codebase: if the
    heir (for whatever reason) already carries a live edge to the same project the
    ancestor is handing over, the move must not mint a second live row."""
    from src.orchestrator.agents import mint_heir

    now = datetime.now(UTC)
    ancestor_id = "agent:leak2anc0"
    ancestor_oid = await actions.create_or_find_object("Agent", ancestor_id, "test")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:leakproj2", "test")
    await actions.create_link(ancestor_oid, proj, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    # pre-seed the heir's own canonical with an ALREADY-live works_in edge to the same
    # project (simulating a caller that stamped it directly before this mint)
    heir_canonical = "agent:leak2anc0-ii"
    heir_oid_preseed = await actions.create_or_find_object("Agent", heir_canonical, "test")
    await actions.create_link(heir_oid_preseed, proj, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")

    heir, heir_oid = await mint_heir(actions, ancestor_id, ancestor_oid,
                                     because="compaction", succession=None)
    assert heir == heir_canonical and heir_oid == heir_oid_preseed

    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='works_in' "
        "AND (valid_until IS NULL OR valid_until > now())", heir_oid, proj)
    assert n == 1, "no duplicate live edge minted just because the ancestor also had one"


# ═══ threads f6f11d78/20af2c95 (decision 5b217d13, 2026-08-04): mint_heir's house-relink
# and register_agent's own identity.project assertion used to fire unconditionally in the
# SAME call, sharing one `now` — a seat's stale derived `house` disagreeing with the
# session's fresh, correctly-resolved project landed BOTH edges on the heir, byte-identical
# to the microsecond (John/agent:d5c671c1-*'s own specimen). upcoming_project narrows the
# house-relink to fire only when nothing else will ever assert a project for the heir. ═══


async def test_mint_heir_skips_the_house_relink_when_a_fresher_project_is_already_known(
    actions: Actions,
) -> None:
    """Direct unit-level proof: pass `upcoming_project` (what register_agent already knows
    moments before it asserts it) and the house-derived edge/property must never land at
    all — the caller's own later assertion is the sole source of truth here, not a race
    between two writers sharing one clock."""
    from src.orchestrator.agents import claim_name, mint_heir

    now = datetime.now(UTC)
    ancestor_id = "agent:staleh01"
    a = await actions.create_or_find_object("Agent", ancestor_id, "test")
    await actions.assert_property(a, "project", "redmonth", "test", now, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    claimed = await claim_name(actions, ancestor_id, "Staleheir", source="test")
    assert claimed.get("error") is None  # the seat's derived house now reads 'redmonth'

    heir, heir_oid = await mint_heir(actions, ancestor_id, a, because="compaction",
                                     succession=None, upcoming_project="ballgem")

    live = await actions.pool.fetch(
        "SELECT t.canonical FROM links l JOIN objects t ON t.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='works_in' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", heir_oid)
    assert live == [], (
        "the stale house must never land a works_in edge when the caller already knows "
        "a fresher project is coming right behind it")
    stamped = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='project' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        heir_oid)
    assert stamped is None, "no house-derived 'project' stamp either — same deferral"


# ═══ task #143 (decision 4607637a, resolving bac81acd): works_in narrows to ONE meaning —
# the session's live/current project — and the seat's durable role-house lives on `governs`
# instead. The house-relink fallback above is legacy inference; once a seat has declared a
# real charter, governs already answers "which house" and the fallback has nothing left to
# do. It is NOT redirected to write governs itself: set_charter replaces the WHOLE charter
# each call, so auto-firing it here with just `house` would heal away the rest of a real
# multi-repo charter the moment the lineage next compacted. ═══


async def test_mint_heir_skips_the_house_relink_once_the_seat_has_declared_a_charter(
    actions: Actions,
) -> None:
    """THE FIX: a seat that has declared ANY charter no longer gets a house-derived
    works_in edge on mint — governs is the fact of record now, and re-stamping works_in
    from `house` would be exactly the stale-role-meaning leak task #143 exists to close."""
    from src.orchestrator.agents import claim_name, mint_heir
    from src.orchestrator.charter import charter_of, set_charter
    from src.orchestrator.seats import held_seat

    now = datetime.now(UTC)
    ancestor_id = "agent:chartered01"
    a = await actions.create_or_find_object("Agent", ancestor_id, "test")
    await actions.assert_property(a, "project", "osiris", "test", now, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    claimed = await claim_name(actions, ancestor_id, "Chartered", source="test")
    assert claimed.get("error") is None  # seat's derived house now reads 'osiris'

    seat = await held_seat(actions.pool, ancestor_id)
    assert seat is not None
    # a DIFFERENT repo than `house` — proves this is charter-existence gating, not a
    # value match, and doubles as the reason auto-writing governs here would be unsafe:
    # set_charter's own call is never made by this fix, so this repo is untouched by it
    await actions.create_or_find_object("SoftwareProject", "repo:bytebye", "test")
    await set_charter(actions, seat["seat_id"], ["bytebye"], actor="agent:steward")

    heir, heir_oid = await mint_heir(actions, ancestor_id, a, because="compaction",
                                     succession=None)

    live = await actions.pool.fetch(
        "SELECT t.canonical FROM links l JOIN objects t ON t.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='works_in' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", heir_oid)
    assert live == [], "chartered — the house-relink must not fire onto works_in at all"
    stamped = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='project' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        heir_oid)
    assert stamped is None, "no house-derived 'project' stamp either — governs speaks now"
    # the charter itself must be untouched — this fix never calls set_charter
    assert await charter_of(actions.pool, seat["seat_id"]) == ["bytebye"]


async def test_mint_heir_still_relinks_the_house_when_the_seat_has_no_charter(
    actions: Actions,
) -> None:
    """THE REFUSAL, TESTED NOT JUST THE SUCCESS (577988ed still governs — this sits on the
    succession path every seat traverses): a seat that has NEVER declared a charter must
    keep today's behavior exactly as it was. charter_of's own docs say it plainly — 'works_in
    still names its home' until a charter exists — so the gate must default to firing, not
    to silence, or a majority of seats (undeclared, per ruling 5's own count) would go blind
    on their role-house with no replacement fact anywhere in the graph."""
    from src.orchestrator.agents import claim_name, mint_heir

    now = datetime.now(UTC)
    ancestor_id = "agent:uncharted01"
    a = await actions.create_or_find_object("Agent", ancestor_id, "test")
    await actions.assert_property(a, "project", "osiris", "test", now, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    claimed = await claim_name(actions, ancestor_id, "Uncharted", source="test")
    assert claimed.get("error") is None  # a seat exists, but no charter is ever declared

    heir, heir_oid = await mint_heir(actions, ancestor_id, a, because="compaction",
                                     succession=None)

    live = await actions.pool.fetchval(
        "SELECT t.canonical FROM links l JOIN objects t ON t.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='works_in' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", heir_oid)
    assert live == "repo:osiris", (
        "unchartered — the legacy house-relink must still fire, unchanged, or an "
        "undeclared seat's role-house silently vanishes with nothing to replace it")


async def test_register_agent_mint_does_not_duplicate_works_in_on_a_stale_house(
    actions: Actions,
) -> None:
    """End-to-end reproduction of John's own specimen: a seat's house stuck at 'redmonth'
    (never corrected) while the session's own cwd correctly resolves to 'ballgem' — before
    the narrowing, mint_heir's house-relink and register_agent's identity.project assertion
    each `_link_once`d their own project inside ONE call, sharing one `now`, so both edges
    landed live on the heir at the identical microsecond. A mint through a genuinely
    divergent house must now produce exactly ONE live works_in edge — the fresher one."""
    from src.orchestrator.agents import claim_name

    now = datetime.now(UTC)
    ancestor_id = "agent:staleh02"
    a = await actions.create_or_find_object("Agent", ancestor_id, "test")
    await actions.assert_property(a, "project", "redmonth", "test", now, 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    claimed = await claim_name(actions, ancestor_id, "Staleheir2", source="test")
    assert claimed.get("error") is None

    fresher = AgentIdentity(agent_id=ancestor_id, session="staleh02", project="ballgem",
                            model="claude-fable-5", cwd=None, model_method="job_dir",
                            model_observed_at=now)
    heir_oid = await register_agent(actions, fresher, actor="test", mint_reason="compaction")

    live = await actions.pool.fetch(
        "SELECT t.canonical FROM links l JOIN objects t ON t.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='works_in' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", heir_oid)
    assert {r["canonical"] for r in live} == {"repo:ballgem"}, (
        "exactly one live works_in edge — the fresher, correctly-resolved project — "
        "never the stale house's edge too")


# ═══ task #144, rule 1 of de3dfc18 ("where this lineage's work actually landed"): ported
# from project_identity.py's own _write_attribution (#110) — the SAME query — for the live
# mount path. THE ACCEPTANCE CONDITION (Thoth, msg 3854): "if it picks, it is wrong, however
# good the pick." This reports agreement/disagreement HONESTLY and never overwrites
# `identity.project` or the graph's own project/works_in write — a later, separately-scoped
# build decides whether rule 1 ever gets to WIN. ═══


async def test_write_attribution_reports_no_signal_with_no_history(actions: Actions) -> None:
    """A lineage that has never filed an in_repo edge anywhere gets an honest "no-signal" —
    never a guess, never silence that reads as agreement."""
    ident = AgentIdentity(agent_id="agent:wa1nosig0", session="wa1nosig0",
                          project="freshproj", model="claude-fable-5", cwd=None,
                          model_method="job_dir", model_observed_at=datetime.now(UTC))
    await register_agent(actions, ident, actor="test")

    assert ident.write_attribution_agreement == "no-signal"
    assert ident.write_attribution_top is None
    assert ident.write_attribution_total == 0
    assert ident.project == "freshproj", "no-signal must never touch the resolved project"


async def test_write_attribution_confirms_when_the_lineage_agrees(actions: Actions) -> None:
    """The healthy case: this lineage's own in_repo edges already point at the same project
    resolve_identity independently resolved — confirms, no drama."""
    now = datetime.now(UTC)
    base = "agent:wa2conf00"
    await actions.create_or_find_object("Agent", base, "test")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:confproj", "test")
    thread = await actions.create_or_find_object("Thread", "thread:wa2conf01", "test")
    await actions.create_link(thread, proj, "in_repo", base, now, 0.9)

    ident = AgentIdentity(agent_id=base, session="wa2conf00", project="confproj",
                          model="claude-fable-5", cwd=None, model_method="job_dir",
                          model_observed_at=now)
    await register_agent(actions, ident, actor="test")

    assert ident.write_attribution_agreement == "confirms"
    assert ident.write_attribution_top == "confproj"
    assert ident.write_attribution_total == 1


async def test_write_attribution_disagrees_without_ever_overriding_project(
    actions: Actions,
) -> None:
    """THE ACCEPTANCE CONDITION, tested directly: this lineage's own writes land almost
    entirely in `oldproj`, but the session resolved `newproj` (a real, legitimate case — a
    seat can genuinely move to a new repo). The signal must say "disagrees" — and `project`,
    the actual graph write, must be `newproj`, completely untouched by the disagreement.
    A naive port that silently picked oldproj (or refused to write newproj) would fail this
    test even though its answer might look "smarter"."""
    now = datetime.now(UTC)
    base = "agent:wa3disa00"
    await actions.create_or_find_object("Agent", base, "test")
    old_proj = await actions.create_or_find_object("SoftwareProject", "repo:oldproj", "test")
    for i in range(4):
        t = await actions.create_or_find_object("Thread", f"thread:wa3disa0{i}", "test")
        await actions.create_link(t, old_proj, "in_repo", base, now, 0.9)

    ident = AgentIdentity(agent_id=base, session="wa3disa00", project="newproj",
                          model="claude-fable-5", cwd=None, model_method="job_dir",
                          model_observed_at=now)
    a = await register_agent(actions, ident, actor="test")

    assert ident.write_attribution_agreement == "disagrees"
    assert ident.write_attribution_top == "oldproj"
    assert ident.write_attribution_total == 4
    assert ident.project == "newproj", "the pick constraint: disagreement never overrides"

    written = await actions.pool.fetchval(
        "SELECT a2.value #>> '{}' FROM current_assertions a2 WHERE a2.object_id=$1 "
        "AND a2.name='project' ORDER BY a2.confidence DESC, a2.observed_at DESC LIMIT 1", a)
    assert written == "newproj", "the graph's own project stamp is untouched by the signal"
    live_works_in = await actions.pool.fetch(
        "SELECT t.canonical FROM links l JOIN objects t ON t.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='works_in' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", a)
    assert {r["canonical"] for r in live_works_in} == {"repo:newproj"}, (
        "works_in follows the session's own resolution, never the lineage's history")


async def test_write_attribution_disagreement_is_stamped_durably(actions: Actions) -> None:
    """The disagreement is worth more than a one-time banner — a later dossier() read (no
    live mount() to have caught the confession) must be able to see it too. DERIVED evidence
    (an inference from write history), never SELF_DECLARED — weaker than a real declaration,
    on purpose."""
    now = datetime.now(UTC)
    base = "agent:wa4stmp00"
    await actions.create_or_find_object("Agent", base, "test")
    old_proj = await actions.create_or_find_object("SoftwareProject", "repo:stampold", "test")
    thread = await actions.create_or_find_object("Thread", "thread:wa4stmp01", "test")
    await actions.create_link(thread, old_proj, "in_repo", base, now, 0.9)

    ident = AgentIdentity(agent_id=base, session="wa4stmp00", project="stampnew",
                          model="claude-fable-5", cwd=None, model_method="job_dir",
                          model_observed_at=now)
    a = await register_agent(actions, ident, actor="test")

    row = await actions.pool.fetchrow(
        "SELECT value #>> '{}' AS v, evidence_class FROM current_assertions "
        "WHERE object_id=$1 AND name='write_attribution_disagreement'", a)
    assert row is not None
    assert "stampold" in row["v"] and "stampnew" in row["v"]
    assert row["evidence_class"] == EvidenceClass.DERIVED.value


async def test_write_attribution_degrades_when_the_check_itself_fails(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """577988ed, tested directly on this exact addition: a broken write-attribution query
    must degrade to an honest "could not determine," never block the mount it sits on. This
    is the diagnostic signal's OWN refusal proof, distinct from the disagreement tests above
    proving the signal doesn't act — this proves the signal doesn't break anything either."""
    from src.orchestrator import project_identity as pi_mod

    async def _boom(pool: object, bases: list[str]) -> dict[str, object]:
        raise RuntimeError("simulated DB hiccup")

    monkeypatch.setattr(pi_mod, "_write_attribution", _boom)

    ident = AgentIdentity(agent_id="agent:wa5boom00", session="wa5boom00",
                          project="survives", model="claude-fable-5", cwd=None,
                          model_method="job_dir", model_observed_at=datetime.now(UTC))
    a = await register_agent(actions, ident, actor="test")   # must not raise

    assert ident.write_attribution_agreement is None
    assert ident.write_attribution_top is None
    assert ident.write_attribution_total == 0
    assert ident.project == "survives", "the mount itself must complete untouched"
    written = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='project'",
        a)
    assert written == "survives"


async def test_write_attribution_normalizes_through_merged_into(actions: Actions) -> None:
    """OBLIGATION a980aff2 (henry->shellbiz): a seat's own `.osiris` pin can go on naming
    a label that has since been FOLDED into another project — the write side already
    tracks the survivor (`_write_attribution` reads the LIVE in_repo edge, healed onto
    shellbiz by the fold), so comparing the pin's raw string against it compares
    yesterday's name to today's. Must confirm, not permanently false-fire 'disagrees' on
    every mount from here on."""
    now = datetime.now(UTC)
    base = "agent:wa6fold00"
    await actions.create_or_find_object("Agent", base, "test")
    henry = await actions.create_or_find_object("SoftwareProject", "repo:henry", "test")
    shellbiz = await actions.create_or_find_object("SoftwareProject", "repo:shellbiz", "test")
    await actions.merge_objects(shellbiz, henry, "henry folded into shellbiz", "test")
    thread = await actions.create_or_find_object("Thread", "thread:wa6fold01", "test")
    await actions.create_link(thread, shellbiz, "in_repo", base, now, 0.9)

    ident = AgentIdentity(agent_id=base, session="wa6fold00", project="henry",
                          model="claude-fable-5", cwd=None, model_method="job_dir",
                          model_observed_at=now)
    await register_agent(actions, ident, actor="test")

    assert ident.write_attribution_agreement == "confirms", (
        "the pin still says 'henry', but henry IS shellbiz now (merged_into) — the raw-"
        "string comparison must not false-fire 'disagrees' on a folded project")
    assert ident.write_attribution_top == "shellbiz"
    assert ident.project == "henry", "normalization is for the COMPARISON only"


async def test_write_attribution_confesses_a_broken_merge_chain_without_picking_a_winner(
    actions: Actions,
) -> None:
    """A cyclic/broken merged_into chain must never be silently resolved to a guessed
    winner — CONFESS in the durable disagreement note and fall through to the raw,
    unnormalized comparison instead (this house names disagreement, it never crowns a
    side)."""
    now = datetime.now(UTC)
    base = "agent:wa7cycl00"
    await actions.create_or_find_object("Agent", base, "test")
    a_proj = await actions.create_or_find_object("SoftwareProject", "repo:cycla", "test")
    b_proj = await actions.create_or_find_object("SoftwareProject", "repo:cyclb", "test")
    # a cycle no real merge_objects call can produce (self-merge is refused) — forced
    # directly to exercise the chain-too-deep guard Actions.resolve_object_id already has.
    await actions.pool.execute(
        "UPDATE objects SET status='merged', merged_into=$1 WHERE id=$2", b_proj, a_proj)
    await actions.pool.execute(
        "UPDATE objects SET status='merged', merged_into=$1 WHERE id=$2", a_proj, b_proj)
    thread = await actions.create_or_find_object("Thread", "thread:wa7cycl01", "test")
    await actions.create_link(thread, b_proj, "in_repo", base, now, 0.9)

    ident = AgentIdentity(agent_id=base, session="wa7cycl00", project="cycla",
                          model="claude-fable-5", cwd=None, model_method="job_dir",
                          model_observed_at=now)
    a = await register_agent(actions, ident, actor="test")   # must not raise

    assert ident.write_attribution_agreement == "disagrees"
    assert ident.write_attribution_top == "cyclb"
    assert ident.project == "cycla", "a broken chain must never override the resolved project"
    row = await actions.pool.fetchrow(
        "SELECT value #>> '{}' AS v FROM current_assertions "
        "WHERE object_id=$1 AND name='write_attribution_disagreement'", a)
    assert row is not None
    assert "could not be resolved" in row["v"], (
        "the broken chain must be CONFESSED in the receipt, not silently swallowed")


async def test_backfill_agent_project_links_dry_run_writes_nothing(actions: Actions) -> None:
    """The one-time repair for the 906/907 edges that already existed before the write-
    side fixes landed — a genuinely OLD-shaped leak (an ancestor's edge, never moved,
    with a live heir already minted), the shape the write-side fix can no longer produce
    but which already exists fleet-wide. DRY RUN IS THE DEFAULT: reports the plan,
    changes nothing."""
    from src.orchestrator.agents import backfill_agent_project_links

    now = datetime.now(UTC)
    ancestor_oid = await actions.create_or_find_object("Agent", "agent:bf1anc0000", "test")
    await actions.create_or_find_object("Agent", "agent:bf1anc0000-ii", "test")
    await actions.assert_property(ancestor_oid, "succeeded_by", "agent:bf1anc0000-ii",
                                  "test", now, 0.9, evidence_class="self_declared")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:bf1proj", "test")
    await actions.create_link(ancestor_oid, proj, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")

    out = await backfill_agent_project_links(actions, actor="test", dry_run=True)

    assert out["dry_run"] is True
    item = next(p for p in out["plan"] if p["agent"] == "agent:bf1anc0000")
    assert item["head"] == "agent:bf1anc0000-ii" and item["would_move"] == 1
    still_live_on_ancestor = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND type='works_in' "
        "AND (valid_until IS NULL OR valid_until > now())", ancestor_oid)
    assert still_live_on_ancestor == 1, "dry run must write nothing"


async def test_backfill_agent_project_links_applied_moves_the_edge(actions: Actions) -> None:
    from src.orchestrator.agents import backfill_agent_project_links

    now = datetime.now(UTC)
    ancestor_oid = await actions.create_or_find_object("Agent", "agent:bf2anc0000", "test")
    heir_oid = await actions.create_or_find_object("Agent", "agent:bf2anc0000-ii", "test")
    await actions.assert_property(ancestor_oid, "succeeded_by", "agent:bf2anc0000-ii",
                                  "test", now, 0.9, evidence_class="self_declared")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:bf2proj", "test")
    await actions.create_link(ancestor_oid, proj, "governs", "test", now, 0.9,
                              evidence_class="self_declared")

    out = await backfill_agent_project_links(actions, actor="backfill:test", dry_run=False)

    item = next(p for p in out["plan"] if p["agent"] == "agent:bf2anc0000")
    assert item["moved"] == {"governs": 1}
    assert out["moved_total"] == {"governs": 1}
    live_on_ancestor = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND type='governs' "
        "AND (valid_until IS NULL OR valid_until > now())", ancestor_oid)
    assert live_on_ancestor is None
    live_on_heir = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='governs' "
        "AND (valid_until IS NULL OR valid_until > now())", heir_oid, proj)
    assert live_on_heir == 1


async def test_backfill_agent_project_links_scopes_to_only_bases(actions: Actions) -> None:
    """Staged rollout, same convention `backfill_unbound_seats`'s `only_seats` already
    uses: a scoped run touches only the named lineage bases and reports — never hides —
    how many other off-head agents it deliberately left untouched."""
    from src.orchestrator.agents import backfill_agent_project_links

    now = datetime.now(UTC)
    for base in ("agent:bf3scope0", "agent:bf3other0"):
        anc = await actions.create_or_find_object("Agent", base, "test")
        heir = f"{base}-ii"
        await actions.create_or_find_object("Agent", heir, "test")
        await actions.assert_property(anc, "succeeded_by", heir, "test", now, 0.9,
                                      evidence_class="self_declared")
        proj = await actions.create_or_find_object("SoftwareProject", f"repo:{base[6:]}",
                                                    "test")
        await actions.create_link(anc, proj, "works_in", "test", now, 0.9,
                                  evidence_class="self_declared")

    out = await backfill_agent_project_links(
        actions, actor="test", dry_run=False, only_bases={"agent:bf3scope0"})

    assert out["total_off_head"] == 2
    assert out["scoped"] == 1 and out["scoped_out"] == 1
    scoped_anc = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='agent:bf3scope0'")
    other_anc = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='agent:bf3other0'")
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND type='works_in' "
        "AND (valid_until IS NULL OR valid_until > now())", scoped_anc) is None
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND type='works_in' "
        "AND (valid_until IS NULL OR valid_until > now())", other_anc) == 1, (
        "the un-scoped lineage must be left exactly as it was")


# ═══ INVALIDATE_WORKS_IN (thread 8640a625, decision fce39baa) — the toolkit hole John
# XVII's own specimen exposed: unpeer heals peer_of, detach_seat heals managed_by, nothing
# healed works_in before this. Self-scoped identity hygiene, same posture as correct_house —
# but tested here at the underlying function's own generic layer (agent_id explicit),
# self-scoping is the MCP wrapper's own job. ═══════════════════════════════════════════════

async def test_invalidate_works_in_drops_the_named_edge_and_leaves_the_other(
    actions: Actions,
) -> None:
    from src.orchestrator.agents import invalidate_works_in

    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:iwi1aaaaa", "test")
    stale = await actions.create_or_find_object("SoftwareProject", "repo:iwi1stale", "test")
    keep = await actions.create_or_find_object("SoftwareProject", "repo:iwi1keep", "test")
    await actions.create_link(a, stale, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    await actions.create_link(a, keep, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")

    out = await invalidate_works_in(actions, "agent:iwi1aaaaa", "repo:iwi1stale",
                                    because="fork residue, ballgem is current", actor="test")

    assert out == {"invalidated": "agent:iwi1aaaaa", "was_working_in": "repo:iwi1stale",
                   "still_working_in": ["repo:iwi1keep"],
                   "because": "fork residue, ballgem is current"}
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='works_in' "
        "AND (valid_until IS NULL OR valid_until > now())", a, stale) is None
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='works_in' "
        "AND (valid_until IS NULL OR valid_until > now())", a, keep) == 1
    reason = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
        "ON a.object_id=o.id AND a.name='works_in_invalidated_because' "
        "WHERE o.canonical=$1", "agent:iwi1aaaaa")
    assert reason == "fork residue, ballgem is current"


async def test_invalidate_works_in_refuses_blank_because(actions: Actions) -> None:
    from src.orchestrator.agents import invalidate_works_in

    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:iwi2aaaaa", "test")
    p1 = await actions.create_or_find_object("SoftwareProject", "repo:iwi2p1", "test")
    p2 = await actions.create_or_find_object("SoftwareProject", "repo:iwi2p2", "test")
    await actions.create_link(a, p1, "works_in", "test", now, 0.9, evidence_class="self_declared")
    await actions.create_link(a, p2, "works_in", "test", now, 0.9, evidence_class="self_declared")

    out = await invalidate_works_in(actions, "agent:iwi2aaaaa", "repo:iwi2p1",
                                    because=" ", actor="test")
    assert "because is required" in out["error"]


async def test_invalidate_works_in_refuses_an_unknown_agent(actions: Actions) -> None:
    from src.orchestrator.agents import invalidate_works_in

    out = await invalidate_works_in(actions, "agent:no-such-one", "repo:whatever",
                                    because="test", actor="test")
    assert "no such active Agent" in out["error"]


async def test_invalidate_works_in_refuses_no_active_edge_to_that_project(
    actions: Actions,
) -> None:
    from src.orchestrator.agents import invalidate_works_in

    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:iwi3aaaaa", "test")
    p1 = await actions.create_or_find_object("SoftwareProject", "repo:iwi3p1", "test")
    p2 = await actions.create_or_find_object("SoftwareProject", "repo:iwi3p2", "test")
    await actions.create_link(a, p1, "works_in", "test", now, 0.9, evidence_class="self_declared")
    await actions.create_link(a, p2, "works_in", "test", now, 0.9, evidence_class="self_declared")
    unrelated = await actions.create_or_find_object(
        "SoftwareProject", "repo:iwi3unrelated", "test")

    out = await invalidate_works_in(actions, "agent:iwi3aaaaa", "repo:iwi3unrelated",
                                    because="test", actor="test")
    assert "no active works_in edge" in out["error"]
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='works_in' "
        "AND (valid_until IS NULL OR valid_until > now())", a, p1) == 1, (
        "a refused call must write nothing")
    _ = unrelated


async def test_invalidate_works_in_refuses_the_agents_only_live_edge(
    actions: Actions,
) -> None:
    """The safety net: dropping a lone works_in edge is not cleanup, it's amputation —
    this verb exists for duplicates, never for an agent's only project."""
    from src.orchestrator.agents import invalidate_works_in

    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:iwi4aaaaa", "test")
    only = await actions.create_or_find_object("SoftwareProject", "repo:iwi4only", "test")
    await actions.create_link(a, only, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")

    out = await invalidate_works_in(actions, "agent:iwi4aaaaa", "repo:iwi4only",
                                    because="test", actor="test")
    assert "ONLY" in out["error"] and "live works_in edge" in out["error"]
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='works_in' "
        "AND (valid_until IS NULL OR valid_until > now())", a, only) == 1


async def test_invalidate_works_in_mcp_wrapper_self_scopes_to_the_caller(
    actions: Actions,
) -> None:
    """No `agent_id` parameter exists on the tool surface — the caller IS the target,
    exactly correct_house's own self-scoping shape, never operator-fenced for this case."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:iwiwrap1", "test")
    stale = await actions.create_or_find_object("SoftwareProject", "repo:iwiwrapstale", "test")
    keep = await actions.create_or_find_object("SoftwareProject", "repo:iwiwrapkeep", "test")
    await actions.create_link(a, stale, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    await actions.create_link(a, keep, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    ident = AgentIdentity(agent_id="agent:iwiwrap1", session="iwiwrap1",
                          project="p", model="claude-sonnet-5", cwd=None,
                          model_method="job_dir", model_history=("claude-sonnet-5",))
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = ident
    try:
        out = await srv.invalidate_works_in("repo:iwiwrapstale", "cleaning up my own fork "
                                            "residue", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["invalidated"] == "agent:iwiwrap1" and out["was_working_in"] == "repo:iwiwrapstale"
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='works_in' "
        "AND (valid_until IS NULL OR valid_until > now())", a, stale) is None


async def test_invalidate_works_in_mcp_wrapper_refuses_before_mount(actions: Actions) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.invalidate_works_in("repo:whatever", "test")
    finally:
        srv._pool = saved_pool
    assert "mount first" in out["error"]


async def test_invalidate_works_in_mcp_wrapper_moves_orient_without_reconnecting(
    actions: Actions,
) -> None:
    """THE ACCEPTANCE TEST (Thoth's own ruling, thread 8640a625 / decision 4001f6d1's own
    finding — the gap John's fix appeared to close three steps late): a live session that
    drops an edge whose project WAS the resolved winner must see orient() move WITHOUT
    reconnecting. Not the DB row — the RESOLUTION. Same connection, same cached identity,
    two orient() calls, one invalidate_works_in call between them, zero re-mounts.

    A SEATED agent's own project is its seat's derived house, unconditionally — the seat's
    house here is ALREADY "ballgemtest" (the fix landed before this test starts), but the
    live cache still says "redmonthtest" (the stale mount-time snapshot, exactly John's own
    shape) until the wrapper patches it."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.seats import bind_holder

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:iwiacc1aa", "test")
    stale = await actions.create_or_find_object("SoftwareProject", "repo:redmonthtest", "test")
    keep = await actions.create_or_find_object("SoftwareProject", "repo:ballgemtest", "test")
    await actions.create_link(a, stale, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    await actions.create_link(a, keep, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    await bind_holder(actions, seat_id="seat:iwiacc1se", agent_id="agent:iwiacc1aa")
    seat_oid = await actions.create_or_find_object("Seat", "seat:iwiacc1se", "test")
    await actions.assert_property(seat_oid, "house", "ballgemtest", "test", now, 0.9,
                                  evidence_class="self_declared")

    ident = AgentIdentity(agent_id="agent:iwiacc1aa", session="iwiacc1", project="redmonthtest",
                          model="claude-sonnet-5", cwd=None, model_method="job_dir",
                          model_history=("claude-sonnet-5",))
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    key = srv._conn_key(ctx)
    srv._agents[key] = ident
    try:
        before = await srv.orient(ctx=ctx)
        assert before["project"] == "redmonthtest"          # the stale cache, before anything

        out = await srv.invalidate_works_in("repo:redmonthtest", "fork residue", ctx=ctx)
        assert out["invalidated"] == "agent:iwiacc1aa"

        after = await srv.orient(ctx=ctx)                    # SAME ctx — no reconnect
    finally:
        srv._pool = saved_pool
        srv._agents.pop(key, None)
    assert after["project"] == "ballgemtest"                 # RESOLUTION moved, not just the row


async def test_invalidate_works_in_mcp_wrapper_falls_back_to_the_remaining_edge_unseated(
    actions: Actions,
) -> None:
    """The sibling shape: an UNSEATED agent has no seat house to re-derive from (_resolve_
    project_seat_first is a documented no-op there), so the cache patch falls back to the
    one unambiguous remaining works_in edge instead — still no reconnect required."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:iwiacc2aa", "test")
    stale = await actions.create_or_find_object("SoftwareProject", "repo:acc2stale", "test")
    keep = await actions.create_or_find_object("SoftwareProject", "repo:acc2keep", "test")
    await actions.create_link(a, stale, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    await actions.create_link(a, keep, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    # deliberately no bind_holder call — this agent holds no seat

    ident = AgentIdentity(agent_id="agent:iwiacc2aa", session="iwiacc2", project="acc2stale",
                          model="claude-sonnet-5", cwd=None, model_method="job_dir",
                          model_history=("claude-sonnet-5",))
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    key = srv._conn_key(ctx)
    srv._agents[key] = ident
    try:
        before = await srv.orient(ctx=ctx)
        assert before["project"] == "acc2stale"

        await srv.invalidate_works_in("repo:acc2stale", "cleanup", ctx=ctx)
        after = await srv.orient(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(key, None)
    assert after["project"] == "acc2keep"


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


async def test_correct_agent_house_surfaces_prior_art_never_refuses_on_it(
    actions: Actions, monkeypatch,
) -> None:
    """obligation e4612853's sibling (Thoth DM 3169/3185) — same guard as rename_project/
    correct_house, generalized here: never blocks the write."""
    from src.orchestrator.agents import correct_agent_house

    await actions.create_or_find_object("Agent", "agent:cah3prior", "test")

    async def _fake_prior_art(pool, *, subject_canonical, field, new_value, actor, because=""):
        return {"prior_art": [{"id": "abcdef01", "type": "Decision"}],
               "prior_art_flag": f"a standing ruling (abcdef01) may already cover "
                                  f"{subject_canonical}'s {field!r}"}

    monkeypatch.setattr("src.orchestrator.capture.property_prior_art", _fake_prior_art)
    out = await correct_agent_house(actions, agent_id="agent:cah3prior", project="somewhere",
                                    actor="agent:witness")
    assert out["corrected"] == {"project": "somewhere"}  # the write still happened
    assert out["prior_art_flag"] == (
        "a standing ruling (abcdef01) may already cover agent:cah3prior's 'project'")


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


# ═══ retire_agent (task #74) — third-party retirement, the manager-scoped complement to
# self-scoped retire(). msg 1713's reap needed exactly this: no sanctioned verb reached a
# third party, forcing direct assert_property under an operator's live permission grant.

async def test_retire_agent_retires_someone_else(actions: Actions) -> None:
    from src.orchestrator.agents import retire_agent

    await actions.create_or_find_object("Agent", "agent:ra1dead", "test")
    out = await retire_agent(actions, agent_id="agent:ra1dead", actor="agent:witness",
                             because="Flip68Real residue, confirmed dead via harness roster")
    assert out["retired"] == "agent:ra1dead"
    assert out["because"] == "Flip68Real residue, confirmed dead via harness roster"
    assert out["was_live"] is False  # no mount row was ever set up for this agent
    assert "seat_vacated" not in out  # never seated in the first place -- nothing to release
    row = await actions.pool.fetchrow("SELECT status FROM objects WHERE canonical='agent:ra1dead'")
    assert row["status"] == "retired"
    retired_by = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
        "ON a.object_id=o.id AND a.name='retired_by' WHERE o.canonical='agent:ra1dead'")
    assert retired_by == "agent:witness"
    event = await actions.pool.fetchrow(
        "SELECT payload FROM object_events WHERE event_type='status_change' "
        "ORDER BY id DESC LIMIT 1")
    assert event["payload"]["status"] == "retired"


async def test_retire_agent_refuses_blank_because(actions: Actions) -> None:
    from src.orchestrator.agents import retire_agent

    await actions.create_or_find_object("Agent", "agent:ra2blnk", "test")
    out = await retire_agent(actions, agent_id="agent:ra2blnk", actor="agent:witness",
                             because="  ")
    assert "because is required" in out["error"]
    row = await actions.pool.fetchrow("SELECT status FROM objects WHERE canonical='agent:ra2blnk'")
    assert row["status"] == "active"


async def test_retire_agent_refuses_an_unknown_agent(actions: Actions) -> None:
    from src.orchestrator.agents import retire_agent

    out = await retire_agent(actions, agent_id="agent:ra3ghos", actor="agent:witness",
                             because="test")
    assert "no such agent" in out["error"]


async def test_retire_agent_refuses_an_already_retired_agent(actions: Actions) -> None:
    from src.orchestrator.agents import retire_agent

    await actions.create_or_find_object("Agent", "agent:ra4twic", "test")
    first = await retire_agent(actions, agent_id="agent:ra4twic", actor="agent:witness",
                               because="first pass")
    assert "retired" in first
    second = await retire_agent(actions, agent_id="agent:ra4twic", actor="agent:witness",
                                because="second pass")
    assert "already retired" in second["error"]


# --- thread 00b1c341: retire_agent had NO liveness check and NEVER released a held seat or
# mount row — Khnum's census found it, scoped it out of the authority build (not an authority
# defect), and it sat unowned as a live footgun: a caller could retire an agent that is
# seated and mid-turn RIGHT NOW, and the seat it held stayed held by a corpse forever.
#
# MEASURED FIRST (Thoth DM 3309): retire_seat REFUSES on a live holder (protects an
# occupant's ongoing work in ITS ROLE); vacate_holder releases a seat's holder but explicitly
# "TRUSTS ITS CALLER" with no liveness check of its own (its blast radius is one link + one
# property); retire()'s own self-retirement ALREADY solves the seat/mount release
# (mounts.release_mounts, thread b47b3814) and ALSO already carries the shape used below —
# refuse by default, but accept a deliberate, on-the-record override (acknowledge_leftovers)
# rather than a permanent, unconditional block. retire_agent's blast radius (deletes mount
# rows AND stamps a terminal Agent status) is bigger than vacate_holder's, so "trust the
# caller" alone under-protects a genuinely live third party — but retire_agent's own founding
# purpose (task #74: cleaning up agents that can never call retire() on themselves) means a
# PERMANENT refusal would defeat the tool. mounts.agent_liveness (already built for send()'s
# own listener receipt) gives a real, existing, non-invented signal — reused here as a
# refuse-by-default gate with retire()'s own override_live=True escape hatch, mirroring
# acknowledge_leftovers exactly. The seat/mount release always happens on a successful
# retirement, live or not — that half of the bug has no defensible reason to stay broken.

async def test_retire_agent_refuses_a_live_seated_agent_by_default(actions: Actions) -> None:
    """NEGATIVE CONTROL: today's code has no liveness check at all — this must fail against
    unfixed retire_agent (it would retire unconditionally, leaving both the seat and the
    mount row exactly as broken as thread 00b1c341 describes)."""
    from src.orchestrator import mounts
    from src.orchestrator.agents import retire_agent
    from src.orchestrator.seats import bind_holder

    await actions.create_or_find_object("Seat", "seat:ra5live0", "test")
    await bind_holder(actions, seat_id="seat:ra5live0", agent_id="agent:ra5live0",
                      source="test")
    await mounts.save_mount(actions.pool, job_dir="/j/ra5live0", agent_id="agent:ra5live0",
                            project="osiris", cwd="/x", model=None, session_key="k")

    out = await retire_agent(actions, agent_id="agent:ra5live0", actor="agent:witness",
                             because="mid-turn right now")
    assert "LIVE" in out["error"] and "override_live" in out["error"]
    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='agent:ra5live0'")
    assert row["status"] == "active"  # refused, not retired
    still_held = await actions.pool.fetchval(
        "SELECT 1 FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE f.canonical='agent:ra5live0' "
        "AND t.canonical='seat:ra5live0' AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())")
    assert still_held == 1
    assert await mounts.find_mount(actions.pool, job_dir="/j/ra5live0") is not None


async def test_retire_agent_override_live_retires_and_releases_seat_and_mount(
    actions: Actions,
) -> None:
    """NEGATIVE CONTROL: `override_live` does not exist on today's code at all — this fails
    against unfixed retire_agent with a TypeError (unexpected keyword argument), and even a
    caller that stripped the kwarg would find the seat/mount row untouched today."""
    from src.orchestrator import mounts
    from src.orchestrator.agents import retire_agent
    from src.orchestrator.seats import bind_holder

    await actions.create_or_find_object("Seat", "seat:ra6ovrd0", "test")
    await bind_holder(actions, seat_id="seat:ra6ovrd0", agent_id="agent:ra6ovrd0",
                      source="test")
    await mounts.save_mount(actions.pool, job_dir="/j/ra6ovrd0", agent_id="agent:ra6ovrd0",
                            project="osiris", cwd="/x", model=None, session_key="k")

    out = await retire_agent(actions, agent_id="agent:ra6ovrd0", actor="agent:witness",
                             because="operator confirms genuinely wedged, retiring anyway",
                             override_live=True)
    assert out["retired"] == "agent:ra6ovrd0"
    assert out["was_live"] is True
    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='agent:ra6ovrd0'")
    assert row["status"] == "retired"
    still_held = await actions.pool.fetchval(
        "SELECT 1 FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE f.canonical='agent:ra6ovrd0' "
        "AND t.canonical='seat:ra6ovrd0' AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())")
    assert still_held is None  # the holds link is released, not left dangling on a corpse
    assert await mounts.find_mount(actions.pool, job_dir="/j/ra6ovrd0") is None


async def test_retire_agent_releases_seat_and_mount_when_not_live(actions: Actions) -> None:
    """The common case task #74 was built for: a genuinely dead third party (no mount row at
    all, or one stale past the liveness window) retires cleanly, WITHOUT needing
    override_live — and must not leave its seat held by a corpse (thread 00b1c341's own
    headline complaint), which is the part of the bug that is unconditionally wrong."""
    from src.orchestrator.agents import retire_agent
    from src.orchestrator.seats import bind_holder

    await actions.create_or_find_object("Seat", "seat:ra7dead0", "test")
    await bind_holder(actions, seat_id="seat:ra7dead0", agent_id="agent:ra7dead0",
                      source="test")
    # no mount row at all -- e.g. a session that crashed before ever cleanly retiring itself

    out = await retire_agent(actions, agent_id="agent:ra7dead0", actor="agent:witness",
                             because="Flip68Real-shaped residue, confirmed dead")
    assert out["retired"] == "agent:ra7dead0"
    assert out["was_live"] is False
    assert out["seat_vacated"] == "seat:ra7dead0"
    still_held = await actions.pool.fetchval(
        "SELECT 1 FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE f.canonical='agent:ra7dead0' "
        "AND t.canonical='seat:ra7dead0' AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())")
    assert still_held is None


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


async def test_lineage_head_never_lands_on_a_healed_husk(actions: Actions) -> None:
    """thread 4a7da43a (reap Stage 1b, 2026-07-28): false_mint healing (heal.py /
    seam-debounce) never flips objects.status — a husk stays 'active' forever, same gap
    as retire_seat leaving Seat.status active. A husk that is the chain's CURRENT tail
    (no real successor minted yet) must not be handed back as the head."""
    from src.orchestrator.agents import lineage_head

    now = datetime.now(UTC)
    c1 = await actions.create_or_find_object("Agent", "agent:33c0ffee", "agent:33c0ffee")
    await actions.create_or_find_object("Agent", "agent:33c0ffee-ii", "agent:33c0ffee-ii")
    await actions.assert_property(c1, "succeeded_by", "agent:33c0ffee-ii",
                                  "agent:33c0ffee", now, 0.9, evidence_class="self_declared")
    assert await lineage_head(actions.pool, "agent:33c0ffee") == "agent:33c0ffee-ii"
    # the -ii mint is diagnosed as a phantom: healed, but its status stays 'active'
    husk = await actions.create_or_find_object("Agent", "agent:33c0ffee-ii",
                                                "agent:33c0ffee-ii")
    await actions.assert_property(husk, "false_mint", True, "seam-debounce", now, 0.6,
                                  evidence_class="direct_observation")
    still = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='agent:33c0ffee-ii'")
    assert still == "active"                          # the same gap retire_seat has
    assert await lineage_head(actions.pool, "agent:33c0ffee") == "agent:33c0ffee"


async def test_lineage_head_walks_through_a_healed_husk_to_the_real_tail(
    actions: Actions,
) -> None:
    """Walk CONTINUATION is unchanged: `cur` still steps through a husk exactly as before
    (verified live against real data, decision c41f74a6 — every husk checked still had its
    own real succeeded_by continuing the chain) — only the RETURNED head now also requires
    false_mint absent. A husk mid-chain is traversed, never returned."""
    from src.orchestrator.agents import lineage_head

    now = datetime.now(UTC)
    d1 = await actions.create_or_find_object("Agent", "agent:44c0ffee", "agent:44c0ffee")
    d2 = await actions.create_or_find_object("Agent", "agent:44c0ffee-ii",
                                             "agent:44c0ffee-ii")
    await actions.create_or_find_object("Agent", "agent:44c0ffee-iii", "agent:44c0ffee-iii")
    await actions.assert_property(d1, "succeeded_by", "agent:44c0ffee-ii",
                                  "agent:44c0ffee", now, 0.9, evidence_class="self_declared")
    await actions.assert_property(d2, "succeeded_by", "agent:44c0ffee-iii",
                                  "agent:44c0ffee-ii", now, 0.9, evidence_class="self_declared")
    await actions.assert_property(d2, "false_mint", True, "seam-debounce", now, 0.6,
                                  evidence_class="direct_observation")
    assert await lineage_head(actions.pool, "agent:44c0ffee") == "agent:44c0ffee-iii"


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


async def test_mint_heir_never_lands_on_a_stale_healed_canonical(actions: Actions) -> None:
    """THE GRAVE-GUARD SEES ASSERTIONS TOO (msg 2325, live case: John/d5c671c1-xv): a heal
    (husk-heal / phantom-fold) never flips objects.status away from 'active' — compensating
    events only, constitution 3 — so the numeral-walk's old status-only check silently
    reused a healed canonical, dragging a real generation onto marks that record a false
    start. THE HEAL IS A ONE-WAY DOOR WITH NO RE-ENTRY (decision 7a37327c) once it is
    STALE — John's was 20 hours cold with no seam in between. A heal outside the mint
    gate's own debounce window must be refused, exactly like a merged canonical."""
    from src.orchestrator.agents import _SEAM_DEBOUNCE_SECS, mint_heir

    stale = datetime.now(UTC) - timedelta(seconds=_SEAM_DEBOUNCE_SECS + 100)
    root = await actions.create_or_find_object("Agent", "agent:heal0001", "agent:heal0001")
    # -ii was minted once, diagnosed as a husk, and healed LONG AGO — status stays 'active'
    husk = await actions.create_or_find_object("Agent", "agent:heal0001-ii",
                                                "agent:heal0001-ii")
    await actions.assert_property(husk, "false_mint", True, "seam-debounce", stale, 0.6,
                                  evidence_class="direct_observation")
    await actions.assert_property(husk, "retired", True, "seam-debounce", stale, 0.6,
                                  evidence_class="direct_observation")
    assert await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='agent:heal0001-ii'") == "active"

    heir, heir_oid = await mint_heir(actions, "agent:heal0001", root, because="compaction",
                                     succession=None)
    assert heir == "agent:heal0001-iii"                # -ii was refused, not reused
    assert heir_oid != husk


async def test_mint_heir_reuses_a_freshly_folded_phantoms_numeral(actions: Actions) -> None:
    """The other half of the same law, and the case that would have shipped broken without
    it (caught by test_two_zero_turn_compactions_fold, which regressed against the first
    cut of this guard): _fold_zero_turn_ancestors heals a zero-turn phantom and hands its
    OWN caller the corrected ancestor to mint against — next_generation() naturally
    reproduces the exact numeral just folded, moments earlier, in the SAME seam-resolution
    operation. Refusing that would break the fold's whole point (MINT ONCE, not MINT ZERO,
    ruling d3531cd8) — a heal still inside the debounce window is not a resurrection."""
    from src.orchestrator.agents import mint_heir

    now = datetime.now(UTC)
    root = await actions.create_or_find_object("Agent", "agent:heal0002", "agent:heal0002")
    phantom = await actions.create_or_find_object("Agent", "agent:heal0002-ii",
                                                   "agent:heal0002-ii")
    await actions.assert_property(phantom, "false_mint", True, "phantom-fold", now, 0.9,
                                  evidence_class="direct_observation")
    await actions.assert_property(phantom, "retired", True, "phantom-fold", now, 0.9,
                                  evidence_class="direct_observation")

    heir, heir_oid = await mint_heir(actions, "agent:heal0002", root, because="compaction",
                                     succession=None, now=now)
    assert heir == "agent:heal0002-ii"                  # the fold's numeral, reused
    assert heir_oid == phantom


async def test_mint_heir_still_never_lands_on_a_merged_canonical(actions: Actions) -> None:
    """Regression guard: the healed-canonical check is ADDED to the walk, not a replacement
    for the original merge check (Ra's resurrection, 2026-07-17) — a merged -ii must still
    be skipped exactly as before."""
    from src.orchestrator.agents import mint_heir

    root = await actions.create_or_find_object("Agent", "agent:merge0001", "agent:merge0001")
    grave = await actions.create_or_find_object("Agent", "agent:merge0001-ii",
                                                 "agent:merge0001-ii")
    winner = await actions.create_or_find_object("Agent", "agent:mergewinner",
                                                  "agent:mergewinner")
    await actions.merge_objects(winner, grave, "test merge", "agent:test")
    assert await actions.pool.fetchval(
        "SELECT status FROM objects WHERE id=$1", grave) == "merged"

    heir, _heir_oid = await mint_heir(actions, "agent:merge0001", root, because="compaction",
                                      succession=None)
    assert heir == "agent:merge0001-iii"


async def test_mint_heir_a_fresh_canonical_is_unaffected_by_the_healed_grave_check(
    actions: Actions,
) -> None:
    """Sekhmet's negative-control standard (msg 2325): a guard that refuses everything
    passes the healed-reuse test and fails the job. A plain first-time mint — no prior
    generation, nothing healed, nothing merged — must land on the natural next numeral,
    unaffected by the new check."""
    from src.orchestrator.agents import mint_heir

    root = await actions.create_or_find_object("Agent", "agent:fresh0001", "agent:fresh0001")
    heir, heir_oid = await mint_heir(actions, "agent:fresh0001", root, because="compaction",
                                     succession=None)
    assert heir == "agent:fresh0001-ii"
    assert await actions.pool.fetchval(
        "SELECT status FROM objects WHERE id=$1", heir_oid) == "active"


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


async def test_fold_zero_turn_ancestors_respects_the_mint_gates_own_window(
    actions: Actions,
) -> None:
    """EXTENDS THE MINT GATE, NOT A NEW ONE BESIDE IT (Thoth LX, msg 1402): this fold shares
    _debounce_roundtrip's own _SEAM_DEBOUNCE_SECS window, not a second one — a zero-turn
    phantom minted long ago (outside the window) is no longer 'back-to-back' with anything
    and must never fold, however silent it's stayed."""
    from src.orchestrator.agents import _SEAM_DEBOUNCE_SECS, _fold_zero_turn_ancestors, mint_heir

    root = await actions.create_or_find_object("Agent", "agent:zt0008", "test")
    old_mint_time = datetime.now(UTC) - timedelta(seconds=_SEAM_DEBOUNCE_SECS + 60)
    phantom, phantom_oid = await mint_heir(actions, "agent:zt0008", root, because="compaction",
                                           succession=None, now=old_mint_time)
    now = datetime.now(UTC)
    restored_id, restored_oid = await _fold_zero_turn_ancestors(
        actions, phantom, phantom_oid, now)
    assert restored_id == phantom and restored_oid == phantom_oid, (
        "outside the window — never folded, whatever the acts-check would have said"
    )
    assert await _false_mint(actions, phantom) is None


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


# ═══ HALF-HEALED PHANTOMS (decision ee012ebc) — flag present ≠ fully healed ═══

async def _succeeded_by(actions: Actions, canonical: str) -> str | None:
    return await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='succeeded_by' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", canonical)


async def test_a_half_healed_phantom_is_reported_not_skipped_by_the_sweep(
    actions: Actions,
) -> None:
    """The live specimen shape (decision ee012ebc): false_mint/retired/retired_by landed,
    but the ancestor's succeeded_by pointer was never unwound — a heal interrupted
    partway. The sweep must NOT treat 'flag present' as 'done': it must detect the
    incompleteness, open an obligation naming both rows, and — critically — NEVER touch
    the ancestor's succeeded_by itself (auto-completing it now would be the exact hazard
    the finding named: rebinding a live seat backward)."""
    from src.orchestrator.agents import (
        _DEBOUNCE_SRC,
        EvidenceClass,
        confidence_for,
        fold_existing_zero_turn_phantoms,
        mint_heir,
    )

    root = await actions.create_or_find_object("Agent", "agent:hh0001", "test")
    phantom, phantom_oid = await mint_heir(actions, "agent:hh0001", root, because="live-swap",
                                           succession="a → b")
    # a REAL successor minted on top of the phantom — the lineage moved on, exactly the
    # shape that makes retroactive completion unsafe
    heir, heir_oid = await mint_heir(actions, phantom, phantom_oid, because="live-swap",
                                     succession="b → c")
    await record_decision(actions, "hh0001-iii did real work", source=heir)
    # hand-write ONLY the flag-stamp half of _debounce_roundtrip's heal — the interrupted
    # state, never the pointer unwind
    now = datetime.now(UTC)
    do = EvidenceClass.DIRECT_OBSERVATION
    conf = confidence_for(do)
    for k, v in (("false_mint", True), ("retired", True), ("retired_by", _DEBOUNCE_SRC)):
        await actions.assert_property(phantom_oid, k, v, _DEBOUNCE_SRC, now, conf,
                                      evidence_class=do.value)

    folded = await fold_existing_zero_turn_phantoms(actions)
    assert phantom not in {f["phantom"] for f in folded}, (
        "a half-healed row must never be counted as a fresh fold")
    # the ancestor's pointer is UNTOUCHED — still points at the phantom, proving no
    # auto-completion happened
    assert await _succeeded_by(actions, "agent:hh0001") == phantom
    # the live head is UNTOUCHED — the real successor still stands, nothing rebound
    assert await actions.pool.fetchval(
        "SELECT status FROM objects WHERE id=$1", heir_oid) == "active"
    thread = await actions.pool.fetchrow(
        "SELECT a.value #>> '{}' AS summary FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id "
        "WHERE o.type='Thread' AND a.name='summary' AND a.value #>> '{}' ILIKE $1",
        f"%HALF-HEALED PHANTOM: {phantom}%")
    assert thread is not None, "the incompleteness must be reported, not silently skipped"
    assert "never unwound" in thread["summary"]


async def test_a_half_healed_phantom_report_is_idempotent(actions: Actions) -> None:
    """Re-sweeping a half-healed row a second time converges on the SAME Thread rather
    than paging a fresh obligation every 15-minute tick."""
    from src.orchestrator.agents import (
        _DEBOUNCE_SRC,
        EvidenceClass,
        confidence_for,
        fold_existing_zero_turn_phantoms,
        mint_heir,
    )

    root = await actions.create_or_find_object("Agent", "agent:hh0002", "test")
    phantom, phantom_oid = await mint_heir(actions, "agent:hh0002", root, because="live-swap",
                                           succession="a → b")
    now = datetime.now(UTC)
    do = EvidenceClass.DIRECT_OBSERVATION
    conf = confidence_for(do)
    for k, v in (("false_mint", True), ("retired", True), ("retired_by", _DEBOUNCE_SRC)):
        await actions.assert_property(phantom_oid, k, v, _DEBOUNCE_SRC, now, conf,
                                      evidence_class=do.value)

    await fold_existing_zero_turn_phantoms(actions)
    await fold_existing_zero_turn_phantoms(actions)
    count = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Thread' AND canonical=("
        "  SELECT canonical FROM objects WHERE type='Thread' AND EXISTS ("
        "    SELECT 1 FROM current_assertions a WHERE a.object_id=objects.id "
        "    AND a.name='summary' AND a.value #>> '{}' ILIKE $1) LIMIT 1)",
        f"%HALF-HEALED PHANTOM: {phantom}%")
    assert count == 1, "open_thread's own idempotency must collapse repeat sightings to one"


# ═══ _debounce_roundtrip ATOMICITY (decision ee012ebc) ═══

async def test_debounce_roundtrip_heal_is_atomic_under_a_forced_mid_heal_exception(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Before this fix, the heal's six writes were independent statements — an exception
    partway (a crash, a dropped connection) left the flag stamped but the pointer never
    unwound, permanently. Force the exception at the LAST write (follow_binding, after
    the flag stamps AND the pointer unwind have already been issued inside the SAME
    atomic() block) and prove NOTHING landed: the transaction rolled back whole, not in
    pieces."""
    import src.orchestrator.seats as seats_mod
    from src.orchestrator.agents import _debounce_roundtrip, mint_heir

    root = await actions.create_or_find_object("Agent", "agent:hh0003", "test")
    phantom, phantom_oid = await mint_heir(actions, "agent:hh0003", root, because="live-swap",
                                           succession="opusA → fableB")

    async def _boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("forced mid-heal failure")

    monkeypatch.setattr(seats_mod, "follow_binding", _boom)

    with pytest.raises(RuntimeError, match="forced mid-heal failure"):
        await _debounce_roundtrip(actions, agent_id=phantom, observed="opusA",
                                  now=datetime.now(UTC))

    # NOTHING landed — not the flag, not the pointer unwind, not the estate move
    assert await _false_mint(actions, phantom) is None
    assert await _succeeded_by(actions, "agent:hh0003") == phantom


# ═══════════ THE BOUNDED CHAIN-WALK (thread e749036e, msg 1398) ═══════════

async def test_nearest_handoff_ancestor_finds_the_immediate_one(actions: Actions) -> None:
    """The common case: one hop, exactly what the old one-hop-only read already covered."""
    from src.orchestrator.agents import nearest_handoff_ancestor

    did = await record_decision(actions, "the estate is settled", kind="choice",
                                source="agent:nha0001", repo="nhaproj")
    await actions.assert_property(did, "is_handoff", "true", "agent:nha0001",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    found, complete = await nearest_handoff_ancestor(actions.pool, "agent:nha0001")
    assert found is not None and found[0] == "agent:nha0001"
    assert complete is True


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
    found, complete = await nearest_handoff_ancestor(actions.pool, "agent:nha0012")
    assert found is not None and found[0] == "agent:nha0010"
    assert complete is True


async def test_nearest_handoff_ancestor_gives_up_past_the_bound(actions: Actions) -> None:
    """A handoff 6 hops back (past max_hops=5) is never found — bounded on purpose, never
    an unbounded search. complete=False is the honest signal that this is a TRUNCATION,
    not a clean "nothing here" (decision 8b375ed7 — the specimen 4c303c43 flagged unverified
    on 2026-08-09, confirmed live at fleet scale: 64/636 real successions hit exactly this)."""
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
    found, complete = await nearest_handoff_ancestor(actions.pool, "agent:nhb0006")
    assert found is None
    assert complete is False


async def test_nearest_handoff_ancestor_complete_when_chain_genuinely_terminates(
    actions: Actions,
) -> None:
    """A chain that reaches a true succeeded_from-IS-NULL origin within max_hops, finding
    nothing, is COMPLETE — genuinely clean, distinguishable from a truncated walk (the
    other half of decision 8b375ed7's contract)."""
    from src.orchestrator.agents import nearest_handoff_ancestor

    await actions.create_or_find_object("Agent", "agent:nhc0000", "agent:nhc0000")
    child = await actions.create_or_find_object("Agent", "agent:nhc0001", "agent:nhc0001")
    await actions.assert_property(child, "succeeded_from", "agent:nhc0000", "agent:nhc0001",
                                  datetime.now(UTC), 0.9, evidence_class="direct_observation")
    found, complete = await nearest_handoff_ancestor(actions.pool, "agent:nhc0001")
    assert found is None
    assert complete is True


async def test_nearest_handoff_ancestor_respects_an_explicit_ack_by_default(
    actions: Actions,
) -> None:
    """An acked (is_handoff='false') record must not resurrect via the ILIKE fallback just
    because its own summary text still says "handoff" — the whole point of ack_handoff is
    that it stops being treated as live (the operator's read-receipt redesign, 2026-08-03).
    respect_ack=False (since_last_handoff's own need, handoff_compiler.py) still finds it,
    because that caller asks "when did this reign end", not "is this still unclaimed"."""
    from src.orchestrator.agents import nearest_handoff_ancestor

    did = await record_decision(actions, "HANDOFF — bleedack0001's own state of the board",
                                kind="choice", source="agent:bleedack0001", repo="nhaproj")
    now = datetime.now(UTC)
    await actions.assert_property(did, "is_handoff", "true", "agent:bleedack0001", now, 0.9,
                                  evidence_class="self_declared")
    # a DIFFERENT source acks it (the successor, not the original author) — the exact shape
    # ack_handoff itself writes; current_assertions legitimately holds both rows at once.
    await actions.assert_property(did, "is_handoff", "false", "agent:bleedack0001-ii",
                                  now + timedelta(seconds=1), 0.9,
                                  evidence_class="self_declared")

    acked, acked_complete = await nearest_handoff_ancestor(
        actions.pool, "agent:bleedack0001")
    assert acked is None
    assert acked_complete is True  # a respected ack is a clean answer, not a truncation
    found, complete = await nearest_handoff_ancestor(
        actions.pool, "agent:bleedack0001", respect_ack=False)
    assert found is not None and found[0] == "agent:bleedack0001"
    assert complete is True


async def test_nearest_handoff_ancestor_surfaces_the_object_id(actions: Actions) -> None:
    """Every pick now carries `id` — before this fix, callers had a summary and a type but
    nothing to name back to ack_handoff(ref=...)."""
    from src.orchestrator.agents import nearest_handoff_ancestor

    did = await record_decision(actions, "the estate is settled, nameable this time",
                                kind="choice", source="agent:nhaid0001", repo="nhaproj")
    await actions.assert_property(did, "is_handoff", "true", "agent:nhaid0001",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    found, complete = await nearest_handoff_ancestor(actions.pool, "agent:nhaid0001")
    assert found is not None
    assert str(found[1][0]["id"]) == str(did)
    assert complete is True


# ═══ RESOLVE_HANDLE / THE INELIGIBLE-HOLDER GUARD (task #142 punch-list item 3, Thoth's
# dispatch DM 4097) — resolve_handle wraps resolve_seat for establish_office/rebind_seat,
# both of which already have a correct "resolve to nothing -> use the Seat object directly"
# fallback. Before this fix, a name whose unique seat's only holder was ineligible fell
# through to resolve_seat's un-seated-lineage fallback and could return an OLDER, unmarked
# generation instead of None — the same grave-delivery shape rulings 1a64ae9a/aee67e6d
# named for send(), reached through this wrapper instead. ═══════════════════════════════


async def test_resolve_handle_returns_none_when_the_seats_only_holder_is_ineligible(
    actions: Actions,
) -> None:
    """John's exact live shape (DM 2360), reproduced against resolve_handle instead of
    send(): a unique seat, one active holder marked false_mint, and an OLDER generation
    still carrying the same `handle` assertion — the ancestor the old fallback would have
    silently returned. resolve_handle must say None, not the ancestor's agent id."""
    from src.orchestrator.seats import bind_holder, ensure_seat

    seat = await ensure_seat(actions, house="osiris", handle="RhGhost", source="test")
    now = datetime.now(UTC)
    ancestor = await actions.create_or_find_object(
        "Agent", "agent:rhghost-old", "agent:rhghost-old")
    await actions.assert_property(ancestor, "handle", "RhGhost", "agent:rhghost-old", now, 0.9,
                                  evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:rhghost-old")
    heir = await actions.create_or_find_object("Agent", "agent:rhghost-new", "agent:rhghost-new")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:rhghost-new")
    await actions.assert_property(heir, "false_mint", "true", "agent:rhghost-new", now, 0.9,
                                  evidence_class="self_declared")

    from src.orchestrator.agents import resolve_handle

    assert await resolve_handle(actions, "RhGhost") is None


async def test_resolve_handle_still_resolves_an_eligible_holder(actions: Actions) -> None:
    """REGRESSION PROOF: the ordinary, working case is unaffected by the new check."""
    from src.orchestrator.agents import resolve_handle
    from src.orchestrator.seats import bind_holder, ensure_seat

    seat = await ensure_seat(actions, house="osiris", handle="RhClean", source="test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:rhclean0001")

    assert await resolve_handle(actions, "RhClean") == "agent:rhclean0001"

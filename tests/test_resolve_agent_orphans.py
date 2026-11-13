"""The provenance sweep's Agent lane: resolve works_in for a
zero-live-link Agent from its own recorded `session` property, never a guess,
cardinality-1-or-abstain via derive_or_abstain.

Orphan fixtures build the Agent object directly (session + is_sidechain properties
only), matching the REAL population's own shape: register_swarm's CURRENT code
already stamps `project` and mints `works_in` itself when a project is resolvable, so
calling it here would never reproduce an orphan at all, every live orphan was
written by an OLDER version of that miner pass, before it did either."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.orchestrator.capture import resolve_agent_orphans

_NOW = datetime.now(UTC)


def _write_session_dir(root: Path, project_dashed: str, session_uuid: str, handle: str) -> Path:
    """A bare on-disk session dir with one subagent, enough for _session_dirs/
    _project_of to find and decode, without going through register_swarm at all."""
    session = root / project_dashed / session_uuid
    subs = session / "subagents"
    subs.mkdir(parents=True)
    (subs / f"agent-{handle}.meta.json").write_text(json.dumps(
        {"agentType": "general-purpose", "description": "probe", "spawnDepth": 1}))
    (subs / f"agent-{handle}.jsonl").write_text(json.dumps(
        {"type": "assistant", "message": {"model": "claude-sonnet-5",
                                           "content": [{"type": "text", "text": "ok"}]}}) + "\n")
    return session


async def _orphan_agent(actions: Actions, canonical: str, session: str) -> None:
    a = await actions.create_or_find_object("Agent", canonical, "fleet-observer")
    await actions.assert_property(a, "session", session, "fleet-observer", _NOW, 0.9,
                                  evidence_class="direct_observation")
    await actions.assert_property(a, "is_sidechain", True, "fleet-observer", _NOW, 0.9,
                                  evidence_class="direct_observation")


async def test_a_singleton_session_mints_works_in(actions: Actions, tmp_path: Path) -> None:
    session_uuid = "aaaaaaaa-0000-4000-8000-000000000001"
    _write_session_dir(tmp_path, "-home-x-code-onepro", session_uuid, "solo001")
    await _orphan_agent(actions, "agent:solo001", session_uuid)

    out = await resolve_agent_orphans(actions, root=tmp_path, dry_run=False, because="test")

    [entry] = [p for p in out["plan"] if p["canonical"] == "agent:solo001"]
    assert entry["verdict"] == "mint"
    project = await actions.pool.fetchval(
        "SELECT canonical FROM objects WHERE canonical=$1", "repo:onepro")
    assert project == "repo:onepro"
    [row] = await actions.pool.fetch(
        "SELECT id FROM objects WHERE canonical='agent:solo001'")
    linked = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='works_in'", row["id"])
    assert linked == 1


async def test_a_session_prefix_reused_across_two_projects_abstains(
    actions: Actions, tmp_path: Path,
) -> None:
    """The 40 multi-project sidechains the ruling names: an OLDER miner run
    stamped only an 8-hex `session` fragment, and that fragment now matches session
    directories under two DIFFERENT projects, ambiguous, never guessed."""
    shared_prefix = "bbbbbbbb"
    _write_session_dir(tmp_path, "-home-x-code-alpha",
                       f"{shared_prefix}-0000-4000-8000-000000000002", "ambi001")
    _write_session_dir(tmp_path, "-home-x-code-beta",
                       f"{shared_prefix}-1111-4000-8000-000000000003", "ambi002")
    await _orphan_agent(actions, "agent:ambishare", shared_prefix)

    out = await resolve_agent_orphans(actions, root=tmp_path, dry_run=False, because="test")

    [entry] = [p for p in out["plan"] if p["canonical"] == "agent:ambishare"]
    assert entry["verdict"] == "abstain"
    assert entry["candidate_count"] == 2
    [row] = await actions.pool.fetch("SELECT id FROM objects WHERE canonical='agent:ambishare'")
    linked = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='works_in'", row["id"])
    assert linked == 0
    stamp = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='derivation_abstained_works_in'", row["id"])
    assert stamp is not None and "2 distinct projects" in stamp


async def test_a_session_matching_nothing_on_disk_abstains_with_zero_candidates(
    actions: Actions, tmp_path: Path,
) -> None:
    await _orphan_agent(actions, "agent:ghosted01", "deadbeef")

    out = await resolve_agent_orphans(actions, root=tmp_path, dry_run=False, because="test")

    [entry] = [p for p in out["plan"] if p["canonical"] == "agent:ghosted01"]
    assert entry["verdict"] == "abstain"
    assert entry["candidate_count"] == 0


async def test_dry_run_never_writes(actions: Actions, tmp_path: Path) -> None:
    session_uuid = "cccccccc-0000-4000-8000-000000000004"
    _write_session_dir(tmp_path, "-home-x-code-dryonly", session_uuid, "dry0001")
    await _orphan_agent(actions, "agent:dry0001", session_uuid)

    out = await resolve_agent_orphans(actions, root=tmp_path)  # dry_run=True default
    assert out["dry_run"] is True
    [row] = await actions.pool.fetch("SELECT id FROM objects WHERE canonical='agent:dry0001'")
    linked = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='works_in'", row["id"])
    assert linked == 0


async def test_apply_without_because_refuses(actions: Actions, tmp_path: Path) -> None:
    out = await resolve_agent_orphans(actions, root=tmp_path, dry_run=False)
    assert "error" in out


async def test_an_abstention_also_writes_the_door_side_hatch(
    actions: Actions, tmp_path: Path,
) -> None:
    """resolve_agent_orphans used to
    write only derivation_abstained_works_in, the adoption meter's hatch count
    (adoption_meter._hatch_counts) reads unlinked_because/unlinked_because_kind, an
    entirely separate property, so a sweep confession was invisible to it."""
    await _orphan_agent(actions, "agent:hatch001", "deadbeef")

    await resolve_agent_orphans(actions, root=tmp_path, dry_run=False, because="test")

    [row] = await actions.pool.fetch("SELECT id FROM objects WHERE canonical='agent:hatch001'")
    because = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='unlinked_because'", row["id"])
    kind = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='unlinked_because_kind'", row["id"])
    assert because is not None
    assert kind == "standalone"


async def test_a_mint_never_writes_the_hatch(actions: Actions, tmp_path: Path) -> None:
    session_uuid = "dddddddd-0000-4000-8000-000000000005"
    _write_session_dir(tmp_path, "-home-x-code-nohatch", session_uuid, "nohatch1")
    await _orphan_agent(actions, "agent:nohatch1", session_uuid)

    await resolve_agent_orphans(actions, root=tmp_path, dry_run=False, because="test")

    [row] = await actions.pool.fetch("SELECT id FROM objects WHERE canonical='agent:nohatch1'")
    because = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='unlinked_because'", row["id"])
    assert because is None


async def test_dry_run_never_writes_the_hatch_either(actions: Actions, tmp_path: Path) -> None:
    await _orphan_agent(actions, "agent:dryhatch1", "deadbeef")

    await resolve_agent_orphans(actions, root=tmp_path)  # dry_run=True default

    [row] = await actions.pool.fetch("SELECT id FROM objects WHERE canonical='agent:dryhatch1'")
    because = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='unlinked_because'", row["id"])
    assert because is None


async def test_the_heartbeat_does_not_walk_agents_it_already_judged(
    actions: Actions, tmp_path: Path,
) -> None:
    """The heartbeat re-abstained the same ~98 agents (and walked the on-disk session tree
    for them) every tick. With skip_judged it judges each once; a manual call still sees all."""
    await _orphan_agent(actions, "agent:judged001", "deadbeef")
    first = await resolve_agent_orphans(
        actions, root=tmp_path, dry_run=False, because="test", skip_judged=True)
    assert "agent:judged001" in {e["canonical"] for e in first["plan"]}

    second = await resolve_agent_orphans(
        actions, root=tmp_path, dry_run=False, because="test", skip_judged=True)
    assert "agent:judged001" not in {e["canonical"] for e in second["plan"]}
    manual = await resolve_agent_orphans(actions, root=tmp_path)
    assert "agent:judged001" in {e["canonical"] for e in manual["plan"]}


async def test_with_nothing_to_judge_the_session_tree_is_not_walked(
    actions: Actions, tmp_path: Path, monkeypatch: Any,
) -> None:
    from src.orchestrator import lineage

    walks: list[int] = []

    def _walk(root: Path) -> list[Path]:
        walks.append(1)
        return []

    class _NoRows:
        pool = type("P", (), {"fetch": staticmethod(lambda *a, **k: _empty())})()

    async def _empty() -> list[Any]:
        return []

    monkeypatch.setattr(lineage, "_session_dirs", _walk)
    out = await resolve_agent_orphans(_NoRows(), root=tmp_path, skip_judged=True)  # type: ignore[arg-type]
    assert out["scanned"] == 0 and walks == []

"""The first miner (wave 16, decision 4d622aee): one generic abstention miner, lanes as
data. Round-robin across (Decision, Thread, Practice, Reference, Agent); each tick reads
its lane's own text fields against a project-name-mention candidate pool and calls
propose() with at most one candidate, or writes nothing. Agent's own pool is empty by
design. Budget/throttle/confidence cap are propose()'s own (wave 15), reused unchanged."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from src.actions.core import Actions
from src.config.settings import get_settings
from src.orchestrator import capture
from src.orchestrator.abstention_miner import _LANES, abstention_miner_tick
from src.orchestrator.monitor import set_cursor
from src.orchestrator.proposals import propose

_CURSOR_KEY = "abstention_miner:lane_index"


def _lane_index(object_type: str) -> int:
    return next(i for i, lane in enumerate(_LANES) if lane.object_type == object_type)


async def _mint_bare(actions: Actions, type_name: str) -> uuid.UUID:
    from src.ontology.catalog import ensure_type

    await ensure_type(actions, name=type_name, kind="object", actor="test")
    return await actions.create_or_find_object(
        type_name, f"{type_name.lower()}:{uuid.uuid4()}", "test")


async def _seed_resolved(
    actions: Actions, miner: str, owner: str, status: str, observed_at: datetime,
) -> None:
    """Same fixture shape test_proposals.py's own budget tests use — a resolved
    Proposal's own history, minted directly (never through propose())."""
    canonical = f"proposal:{uuid.uuid4()}"
    proposal_id = await actions.create_or_find_object("Proposal", canonical, miner)
    for name, value in (("miner", miner), ("owner", owner), ("status", status)):
        await actions.assert_property(proposal_id, name, value, miner, observed_at,
                                      0.4, evidence_class="derived", actor=miner)


async def test_a_lane_with_one_true_candidate_proposes_it(actions: Actions) -> None:
    """Decision lane: a live abstention plus a summary that mentions exactly one real,
    live SoftwareProject's own name proposes that project as the in_repo candidate."""
    p = await actions.create_or_find_object("SoftwareProject", "repo:widgetfactory", "test")
    d = await _mint_bare(actions, "Decision")
    await capture.derive_or_abstain(actions, d, "in_repo", [], "test")
    await actions.assert_property(
        d, "summary", "widgetfactory's own deploy gate needs a second look", "test",
        datetime.now(UTC), 0.9, evidence_class="self_declared")

    await set_cursor(actions.pool, _CURSOR_KEY, str(_lane_index("Decision")))
    out = await abstention_miner_tick(actions)

    assert out["lane"] == "Decision"
    assert out["action"] == "proposed", out
    assert out["object"] == str(d)
    candidate = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id="
        "(SELECT id FROM objects WHERE canonical=$1) AND a.name='candidate'", out["proposal"])
    assert candidate == {"kind": "link", "from_id": str(d), "to_id": str(p),
                         "link_type": "in_repo", "signal": "dominance"}
    # never a link write — miners remain last resort
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='in_repo'", d)
    assert n == 0


async def test_the_empty_agent_lane_proposes_nothing(actions: Actions) -> None:
    """Agent's own candidate pool is empty by design — 'never guessing from names'."""
    a = await _mint_bare(actions, "Agent")
    await capture.derive_or_abstain(actions, a, "works_in", [], "test")
    await actions.assert_property(
        a, "session", "some-session-id", "test", datetime.now(UTC), 0.9,
        evidence_class="self_declared")

    await set_cursor(actions.pool, _CURSOR_KEY, str(_lane_index("Agent")))
    out = await abstention_miner_tick(actions)

    assert out["lane"] == "Agent"
    assert out["action"] == "skipped"
    assert "empty candidate pool" in out["reason"]
    n = await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Proposal'")
    assert n == 0


async def test_a_dominant_mention_proposes_it(actions: Actions) -> None:
    """THE LANE SIGNAL (Thoth ruling, mail 9847, decision 2406c9c5): the project
    mentioned most often wins when it leads the runner-up by 2x — here widgetfactory
    (2 mentions) over sidecar (1), a 2x lead, so widgetfactory is proposed with
    signal='dominance'."""
    winner = await actions.create_or_find_object("SoftwareProject", "repo:widgetfactory", "test")
    await actions.create_or_find_object("SoftwareProject", "repo:sidecar", "test")
    d = await _mint_bare(actions, "Decision")
    await capture.derive_or_abstain(actions, d, "in_repo", [], "test")
    await actions.assert_property(
        d, "summary",
        "widgetfactory's own deploy gate needs a second look, unlike sidecar; "
        "widgetfactory again tomorrow",
        "test", datetime.now(UTC), 0.9, evidence_class="self_declared")

    await set_cursor(actions.pool, _CURSOR_KEY, str(_lane_index("Decision")))
    out = await abstention_miner_tick(actions)

    assert out["action"] == "proposed", out
    candidate = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id="
        "(SELECT id FROM objects WHERE canonical=$1) AND a.name='candidate'", out["proposal"])
    assert candidate == {"kind": "link", "from_id": str(d), "to_id": str(winner),
                         "link_type": "in_repo", "signal": "dominance"}


async def test_a_tied_mention_is_tie_broken_by_author_works_in(actions: Actions) -> None:
    """Two live projects mentioned EQUALLY OFTEN — no dominant mention — falls back to
    the object's own author's `works_in` project (the `produced` edge, Decision/Thread's
    only authorship edge), proposed with signal='author_tiebreak'."""
    await actions.create_or_find_object("SoftwareProject", "repo:alpha", "test")
    beta = await actions.create_or_find_object("SoftwareProject", "repo:beta", "test")
    author = await actions.create_or_find_object("Agent", "agent:tiebreaker", "test")
    await actions.create_link(author, beta, "works_in", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")
    d = await _mint_bare(actions, "Decision")
    await capture.mint_produced(actions, author, d)
    await capture.derive_or_abstain(actions, d, "in_repo", [], "test")
    await actions.assert_property(
        d, "summary", "touches both alpha and beta in the same breath", "test",
        datetime.now(UTC), 0.9, evidence_class="self_declared")

    await set_cursor(actions.pool, _CURSOR_KEY, str(_lane_index("Decision")))
    out = await abstention_miner_tick(actions)

    assert out["action"] == "proposed", out
    candidate = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id="
        "(SELECT id FROM objects WHERE canonical=$1) AND a.name='candidate'", out["proposal"])
    assert candidate == {"kind": "link", "from_id": str(d), "to_id": str(beta),
                         "link_type": "in_repo", "signal": "author_tiebreak"}


async def test_neither_signal_firing_abstains(actions: Actions) -> None:
    """Tied mentions AND no author (or an authorless object) — neither signal fires, so
    the tick writes nothing, same as the old zero/multiple-candidate case."""
    await actions.create_or_find_object("SoftwareProject", "repo:alpha", "test")
    await actions.create_or_find_object("SoftwareProject", "repo:beta", "test")
    d = await _mint_bare(actions, "Decision")
    await capture.derive_or_abstain(actions, d, "in_repo", [], "test")
    await actions.assert_property(
        d, "summary", "touches both alpha and beta in the same breath", "test",
        datetime.now(UTC), 0.9, evidence_class="self_declared")

    await set_cursor(actions.pool, _CURSOR_KEY, str(_lane_index("Decision")))
    out = await abstention_miner_tick(actions)

    assert out["action"] == "skipped"
    assert "no project dominates" in out["reason"] and "no author" in out["reason"]
    n = await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Proposal'")
    assert n == 0


async def test_round_robin_advances_to_the_next_lane_each_tick(actions: Actions) -> None:
    """No lane's own backlog starves another — consecutive ticks visit consecutive
    lanes, wrapping around."""
    await set_cursor(actions.pool, _CURSOR_KEY, "0")
    seen = []
    for _ in range(len(_LANES) + 1):
        out = await abstention_miner_tick(actions)
        seen.append(out["lane"])
    assert seen == [lane.object_type for lane in _LANES] + [_LANES[0].object_type]


async def test_budget_refuses_the_sixth_proposal_in_a_day(actions: Actions) -> None:
    """propose()'s own daily budget (wave 15), reused unchanged under the miner's own
    identity (miner='abstention', owner='operator'): a good trailing 30-day record earns
    the full base budget of 5/day; the sixth the same day is refused."""
    ten_days_ago = datetime.now(UTC) - timedelta(days=10)
    for _ in range(5):
        await _seed_resolved(actions, "abstention", "operator", "accepted", ten_days_ago)

    results = []
    for _ in range(6):
        d = await _mint_bare(actions, "Decision")
        await capture.derive_or_abstain(actions, d, "in_repo", [], "test")
        results.append(await propose(
            actions, from_id=d, link_type="in_repo",
            candidate={"kind": "link", "from_id": str(d), "to_id": str(d),
                      "link_type": "in_repo"},
            confidence=0.9, owner="operator", miner="abstention", actor="miner:abstention"))
    assert sum(1 for r in results if "error" not in r) == 5
    assert "budget" in results[5]["error"]
    assert get_settings().osiris_miner_new_pair_starter_budget == 1  # this test's own assumption


async def test_heartbeat_is_scheduled_as_a_cron_job() -> None:
    """Same proof shape as landing_audit_heartbeat's own registration test — a mechanism
    whose only trigger is `osiris deploy` succeeding is not adopted, it is hostage to
    whatever else can block a deploy."""
    from src.workers.arq_worker import WorkerSettings

    crons = {c.coroutine.__name__ for c in WorkerSettings.cron_jobs}
    assert "abstention_miner_heartbeat" in crons


async def test_tick_is_dark_when_the_flag_is_off(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_ABSTENTION_MINER_ENABLED", "0")
    out = await abstention_miner_tick(actions)
    assert out["action"] == "dark"
    n = await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Proposal'")
    assert n == 0


async def test_tick_is_dark_when_the_settings_registry_disables_it_live(
    actions: Actions,
) -> None:
    """THE SETTINGS MENU's own overlay (thread f4498ab304e4 piece 1): `miner.abstention.
    enabled` is registered with effect='immediate' — a write through the settings door
    (never touching env at all) takes hold on the very NEXT tick, no restart."""
    from src.orchestrator.settings_service import write_setting

    out = await write_setting(
        actions.pool, "miner.abstention.enabled", False, actor="operator")
    assert "error" not in out

    result = await abstention_miner_tick(actions)

    assert result["action"] == "dark"
    n = await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Proposal'")
    assert n == 0


async def test_a_lane_can_be_silenced_without_touching_the_others(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_ABSTENTION_MINER_LANES_OFF", "Decision")
    p = await actions.create_or_find_object("SoftwareProject", "repo:silencetest", "test")
    d = await _mint_bare(actions, "Decision")
    await capture.derive_or_abstain(actions, d, "in_repo", [], "test")
    await actions.assert_property(
        d, "summary", "silencetest needs its own home", "test",
        datetime.now(UTC), 0.9, evidence_class="self_declared")

    await set_cursor(actions.pool, _CURSOR_KEY, str(_lane_index("Decision")))
    out = await abstention_miner_tick(actions)
    assert out["lane"] == "Decision"
    assert out["action"] == "skipped"
    assert "silenced" in out["reason"]
    assert p  # the candidate project existed; the lane simply never looked

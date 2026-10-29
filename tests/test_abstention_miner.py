"""The first miner (decision): one generic abstention miner, lanes as
data. Round-robin across (Decision, Thread, Practice, Reference, Agent); each tick reads
its lane's own text fields against a project-name-mention candidate pool and calls
propose() with at most one candidate, or writes nothing. Agent's own pool is empty by
design. Budget/throttle/confidence cap are propose()'s own, reused unchanged."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from src.actions.core import Actions
from src.config.settings import get_settings
from src.orchestrator import capture
from src.orchestrator.abstention_miner import (
    _LANES,
    _next_abstained_object,
    abstention_miner_tick,
    abstention_survey,
)
from src.orchestrator.monitor import get_cursor, set_cursor
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
    """Same fixture shape test_proposals.py's own budget tests use, a resolved
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
    # never a link write, miners remain last resort
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='in_repo'", d)
    assert n == 0


async def test_the_empty_agent_lane_proposes_nothing(actions: Actions) -> None:
    """Agent's own candidate pool is empty by design: 'never guessing from names'."""
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
    """THE LANE SIGNAL (ruling): the project
    mentioned most often wins when it leads the runner-up by 2x, here widgetfactory
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
    """Two live projects mentioned EQUALLY OFTEN, no dominant mention, falls back to
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
    """Tied mentions AND no author (or an authorless object), neither signal fires, so
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
    """No lane's own backlog starves another, consecutive ticks visit consecutive
    lanes, wrapping around."""
    await set_cursor(actions.pool, _CURSOR_KEY, "0")
    seen = []
    for _ in range(len(_LANES) + 1):
        out = await abstention_miner_tick(actions)
        seen.append(out["lane"])
    assert seen == [lane.object_type for lane in _LANES] + [_LANES[0].object_type]


async def test_budget_refuses_the_sixth_proposal_in_a_day(actions: Actions) -> None:
    """propose()'s own daily budget, reused unchanged under the miner's own
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
    """Same proof shape as landing_audit_heartbeat's own registration test, a mechanism
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
    enabled` is registered with effect='immediate', a write through the settings door
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


async def _abstained_decision(actions: Actions, text: str) -> uuid.UUID:
    d = await _mint_bare(actions, "Decision")
    await capture.derive_or_abstain(actions, d, "in_repo", [], "test")
    await actions.assert_property(d, "summary", text, "test", datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")
    return d


async def test_an_expired_proposal_is_swept_and_stops_hiding_its_object(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator import proposals
    from src.orchestrator.proposals import sweep_expired

    await actions.create_or_find_object("SoftwareProject", "repo:widgetfactory", "test")
    d = await _abstained_decision(actions, "widgetfactory deploy gate")
    lane = _LANES[_lane_index("Decision")]
    monkeypatch.setattr(proposals, "_EXPIRY_DAYS", -1)       # minted already past its date
    out = await propose(
        actions, from_id=d, link_type="in_repo",
        candidate={"kind": "link", "from_id": str(d), "to_id": str(d), "link_type": "in_repo"},
        confidence=0.4, owner="operator", miner="abstention", actor="miner:abstention")
    assert out["status"] == "proposed"
    assert await _next_abstained_object(actions.pool, lane) is None      # hidden behind it

    assert await sweep_expired(actions) == 1
    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id="
        "(SELECT id FROM objects WHERE canonical=$1) AND a.name='status'", out["proposal"])
    assert status == "expired"
    assert await _next_abstained_object(actions.pool, lane) == d         # free to be retried
    assert await sweep_expired(actions) == 0                             # nothing left to move


async def test_a_proposal_still_inside_its_window_is_not_swept(actions: Actions) -> None:
    from src.orchestrator.proposals import sweep_expired

    d = await _abstained_decision(actions, "anything")
    await propose(
        actions, from_id=d, link_type="in_repo",
        candidate={"kind": "link", "from_id": str(d), "to_id": str(d), "link_type": "in_repo"},
        confidence=0.4, owner="operator", miner="abstention", actor="miner:abstention")
    assert await sweep_expired(actions) == 0
    assert await _next_abstained_object(actions.pool, _LANES[_lane_index("Decision")]) is None


async def test_every_counted_tick_outcome_lands_in_the_survey(actions: Actions) -> None:
    await _abstained_decision(actions, "says nothing about any project")
    await set_cursor(actions.pool, _CURSOR_KEY, str(_lane_index("Decision")))
    assert (await abstention_miner_tick(actions))["action"] == "skipped"
    await set_cursor(actions.pool, _CURSOR_KEY, str(_lane_index("Decision")))
    assert (await abstention_miner_tick(actions))["action"] == "skipped"
    await set_cursor(actions.pool, _CURSOR_KEY, str(_lane_index("Agent")))
    await abstention_miner_tick(actions)
    survey = await abstention_survey(actions.pool)
    assert survey["outcomes_total"]["Decision:skipped"] == 2
    assert survey["outcomes_total"]["Agent:skipped"] == 1
    assert list(survey["outcomes_by_day"].values())[0]["Decision:skipped"] == 2


async def test_the_survey_names_the_class_signal_and_pick_of_the_next_abstentions(
    actions: Actions,
) -> None:
    await actions.create_or_find_object("SoftwareProject", "repo:widgetfactory", "test")
    await actions.create_or_find_object("SoftwareProject", "repo:sidecar", "test")
    silent = await _abstained_decision(actions, "no project named here")
    lone = await _abstained_decision(actions, "widgetfactory only")
    both = await _abstained_decision(actions, "widgetfactory widgetfactory and sidecar")
    lane = next(r for r in (await abstention_survey(actions.pool, per_lane=10))["lanes"]
                if r["lane"] == "Decision:in_repo")
    rows = {r["object"]: r for r in lane["next"]}
    assert lane["eligible"] == 3 and lane["first_abstains"] is True
    assert rows[str(silent)]["mention_class"] == "0" and rows[str(silent)]["picks"] is None
    assert "no project is mentioned" in rows[str(silent)]["abstain_reason"]
    assert rows[str(lone)]["mention_class"] == "1" and rows[str(lone)]["picks"] == "widgetfactory"
    assert rows[str(both)]["mention_class"] == "2+" and rows[str(both)]["signal"] == "dominance"
    assert rows[str(both)]["mentions"] == {"widgetfactory": 2, "sidecar": 1}
    assert lane["next_abstaining"] == 1


async def test_the_survey_writes_nothing(actions: Actions) -> None:
    await _abstained_decision(actions, "nothing to see")
    before = await actions.pool.fetchval("SELECT count(*) FROM assertions")
    await abstention_survey(actions.pool)
    assert await actions.pool.fetchval("SELECT count(*) FROM assertions") == before


async def _tick_decision_lane(actions: Actions) -> dict[str, object]:
    await set_cursor(actions.pool, _CURSOR_KEY, str(_lane_index("Decision")))
    return await abstention_miner_tick(actions)


async def test_a_lane_moves_past_an_object_it_abstained_on(actions: Actions) -> None:
    """The stall: an abstained object was the oldest eligible one on every later tick as
    well, so the objects behind it were never reached."""
    await actions.create_or_find_object("SoftwareProject", "repo:widgetfactory", "test")
    silent = await _abstained_decision(actions, "says nothing about any project")
    lone = await _abstained_decision(actions, "widgetfactory only")
    first = await _tick_decision_lane(actions)
    assert first["action"] == "skipped" and first["object"] == str(silent)
    second = await _tick_decision_lane(actions)
    assert second["object"] == str(lone) and second["action"] == "proposed", second


async def test_the_position_wraps_to_the_oldest_when_nothing_is_left_after_it(
    actions: Actions,
) -> None:
    only = await _abstained_decision(actions, "nothing to say")
    a = await _tick_decision_lane(actions)
    b = await _tick_decision_lane(actions)
    assert a["object"] == b["object"] == str(only)       # one object: the second tick wrapped


async def test_a_refusal_does_not_move_the_position(actions: Actions) -> None:
    """A spent daily budget refuses the pick without having examined the object away, so the
    lane must not burn through its whole queue in a day."""
    await actions.create_or_find_object("SoftwareProject", "repo:widgetfactory", "test")
    one = await _abstained_decision(actions, "widgetfactory one")
    two = await _abstained_decision(actions, "widgetfactory two")
    first = await _tick_decision_lane(actions)
    assert first["action"] == "proposed" and first["object"] == str(one)
    second = await _tick_decision_lane(actions)
    assert second["action"] == "refused" and second["object"] == str(two)
    key = "abstention_miner:position:Decision"
    assert await get_cursor(actions.pool, key) == str(one)
    third = await _tick_decision_lane(actions)
    assert third["object"] == str(two)                   # still the same object, not skipped past


async def test_the_survey_counts_every_class_and_samples_the_hard_cases(
    actions: Actions,
) -> None:
    await actions.create_or_find_object("SoftwareProject", "repo:widgetfactory", "test")
    await actions.create_or_find_object("SoftwareProject", "repo:sidecar", "test")
    await _abstained_decision(actions, "nothing named")
    await _abstained_decision(actions, "widgetfactory alone")
    split = await _abstained_decision(actions, "widgetfactory and sidecar, once each")
    clear = await _abstained_decision(actions, "widgetfactory widgetfactory sidecar")
    lane = next(r for r in (await abstention_survey(
        actions.pool, per_lane=10, class2_per_lane=10, excerpts=True))["lanes"]
        if r["lane"] == "Decision:in_repo")
    assert lane["classes"] == {"0": 1, "1": 1, "2+": 2}
    assert lane["if_taken"] == {"dominance": 2, "author_tiebreak": 0, "abstain": 2}
    samples = {r["object"]: r for r in lane["class2_samples"]}
    assert set(samples) == {str(split), str(clear)}
    assert samples[str(split)]["signal"] == "none" and samples[str(split)]["picks"] is None
    assert samples[str(clear)]["picks"] == "widgetfactory"
    assert samples[str(clear)]["excerpt"].startswith("widgetfactory widgetfactory")
    plain = next(r for r in (await abstention_survey(actions.pool))["lanes"]
                 if r["lane"] == "Decision:in_repo")
    assert "excerpt" not in plain["class2_samples"][0]


async def test_the_survey_lists_the_next_objects_from_the_lane_position(
    actions: Actions,
) -> None:
    a = await _abstained_decision(actions, "first")
    b = await _abstained_decision(actions, "second")
    c = await _abstained_decision(actions, "third")
    await set_cursor(actions.pool, "abstention_miner:position:Decision", str(a))
    lane = next(r for r in (await abstention_survey(actions.pool, per_lane=3))["lanes"]
                if r["lane"] == "Decision:in_repo")
    assert [r["object"] for r in lane["next"]] == [str(b), str(c), str(a)]    # then wraps
    assert lane["position"] == str(a)

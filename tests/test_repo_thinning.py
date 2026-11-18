"""Time Machine style thinning of the NAS repository: the plan, the safety refusals, and that only a
reviewed plan is ever applied."""
from __future__ import annotations

import contextlib
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import repo_thinning as rt
from src.orchestrator.backup_settings import write_backup_settings
from src.orchestrator.instants import parse_time

NOW = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
TARGET = {"name": "nas", "kind": "restic", "enabled": True, "schedule": "daily",
          "path_or_url": "sftp:truenas:/mnt/Bunker/Vault/osiris-restic/repo"}
HOME = {"present": True, "reason": None, "dev": "wlp0s20f3"}


def snap(i: int, hours_ago: float, tags: list[str] | None = None) -> dict[str, Any]:
    return {"id": f"s{i:03d}", "time": (NOW - timedelta(hours=hours_ago)).isoformat(),
            "tags": tags or []}


SNAPS = [snap(0, 0.1), snap(1, 3), snap(2, 400), snap(3, 401), snap(4, 3000, ["history-import"])]


def proc(out: str = "", code: int = 0, err: str = "") -> Any:
    return SimpleNamespace(returncode=code, stdout=out, stderr=err)


class Fake:
    """A restic that answers from canned data and remembers every call."""

    def __init__(self, *, snaps: list[dict[str, Any]] | None = None, remove: tuple[str, ...] = (
            "s003",), presence: dict[str, Any] | None = None, locked: bool = True,
            drill: dict[str, Any] | None = None, offload: dict[str, Any] | None = None) -> None:
        self.snaps = snaps if snaps is not None else list(SNAPS)
        self.remove = set(remove)
        self.calls: list[list[str]] = []
        self.presence, self.locked = presence or HOME, locked
        self.drill = drill if drill is not None else {
            "last_passed_at": (NOW - timedelta(days=1)).isoformat(), "last_error": None}
        self.offload = offload if offload is not None else {
            "last_successful_offload": (NOW - timedelta(hours=2)).isoformat()}

    def run(self, args: list[str], *, timeout: float, text: bool = True) -> Any:
        self.calls.append(args)
        if args[0] == "snapshots":
            return proc(json.dumps(self.snaps))
        if args[0] == "forget" and "--dry-run" in args:
            keep = [s for s in self.snaps if s["id"] not in self.remove]
            gone = [s for s in self.snaps if s["id"] in self.remove]
            return proc(json.dumps([{"keep": keep, "remove": gone}]))
        if args[0] == "stats":
            ids = [a for a in args if a.startswith("s0")]
            return proc(json.dumps({"total_size": 100 * 10**9 if ids else 300 * 10**9}))
        return proc("")

    def deps(self) -> rt.Deps:
        @contextlib.contextmanager
        def lock() -> Any:
            yield self.locked

        return rt.Deps(
            run=self.run, presence=lambda t: self.presence, hold_lock=lock,
            drill_receipt=lambda u: self.drill, offload_receipt=lambda n: self.offload,
            repo_bytes=lambda t: 300 * 10**9, now=lambda: NOW, parse=parse_time)


def test_the_policy_is_the_time_machine_tiers_and_keeps_tagged_snapshots() -> None:
    args = rt.Policy(weeks=26).args()

    assert args[args.index("--keep-within") + 1] == "24h"
    assert args[args.index("--keep-within-hourly") + 1] == "48h"
    assert args[args.index("--keep-within-daily") + 1] == "30d"
    assert args[args.index("--keep-within-weekly") + 1] == "182d"
    # calendar windows, never "the last N days that have a snapshot"
    assert "--keep-hourly" not in args and "--keep-daily" not in args
    assert args[args.index("--keep-tag") + 1] == "history-import"
    assert args[args.index("--keep-last") + 1] == "3"


def test_over_the_size_limit_the_weeks_are_halved_but_never_below_four() -> None:
    assert rt.weeks_for(26, 100 * 10**9, 0) == (26, None)
    assert rt.weeks_for(26, 100 * 10**9, 200) == (26, None)
    weeks, why = rt.weeks_for(26, 900 * 10**9, 800)
    assert weeks == 13 and why and "over its 800 GB limit" in why
    assert rt.weeks_for(6, 900 * 10**9, 800)[0] == 4
    assert rt.weeks_for(1, 900 * 10**9, 800)[0] == 4


def test_the_plan_is_made_by_a_dry_run_and_measures_what_it_frees() -> None:
    fake = Fake()

    plan = rt.make_plan(TARGET, fake.deps(), weeks=26)

    forget = next(c for c in fake.calls if c[0] == "forget")
    assert "--dry-run" in forget and "--no-lock" in forget and "--keep-tag" in forget
    assert plan.remove == ["s003"] and plan.total == 5 and not plan.problems
    assert plan.freed_bytes == 200 * 10**9 and plan.repo_bytes == 300 * 10**9
    assert not any(c[0] in ("prune",) or (c[0] == "forget" and "--dry-run" not in c)
                   for c in fake.calls)
    text = "\n".join(rt.describe(plan))
    assert "1 of 5 snapshot(s) would be forgotten" in text and "frees about 200 GB" in text
    assert f"osiris repo-thin --apply {plan.id}" in text


def test_a_plan_that_drops_the_newest_a_tagged_one_or_too_many_is_refused() -> None:
    newest_gone = rt.make_plan(TARGET, Fake(remove=("s000",)).deps(), weeks=26)
    tagged_gone = rt.make_plan(TARGET, Fake(remove=("s004",)).deps(), weeks=26)
    many = [snap(i, i + 0.1) for i in range(12)]
    too_many = rt.make_plan(TARGET, Fake(snaps=many, remove=tuple(
        s["id"] for s in many[1:])).deps(), weeks=26)

    assert "the newest snapshot would not be kept" in newest_gone.problems
    assert "a snapshot tagged for keeping would be removed" in tagged_gone.problems
    assert any("too many" in p for p in too_many.problems)
    assert newest_gone.freed_bytes is None  # no measuring for a plan that will not run


def test_the_plan_id_follows_what_would_go_and_not_the_order() -> None:
    a = rt.fingerprint(["s1", "s2"], ["--keep-last", "3"])

    assert a == rt.fingerprint(["s2", "s1"], ["--keep-last", "3"])
    assert a != rt.fingerprint(["s1"], ["--keep-last", "3"])
    assert a != rt.fingerprint(["s1", "s2"], ["--keep-last", "4"])


def _pre(**kw: Any) -> str | None:
    base: dict[str, Any] = dict(
        presence=HOME, drill={"last_passed_at": (NOW - timedelta(days=1)).isoformat()},
        offload={"last_successful_offload": (NOW - timedelta(hours=1)).isoformat()}, now=NOW,
        parse=parse_time)
    base.update(kw)
    return rt.preconditions(**base)


def test_every_precondition_has_its_own_plain_reason() -> None:
    assert _pre() is None
    away = _pre(presence={"present": False, "reason": "tailscale0"})
    assert "not on the home network" in (away or "")
    assert "interface" in (_pre(presence={"present": True, "dev": None}) or "")
    assert "has not passed" in (_pre(drill={}) or "")
    assert "boom" in (_pre(drill={"last_passed_at": NOW.isoformat(), "last_error": "boom"}) or "")
    assert "older than 14 days" in (_pre(drill={
        "last_passed_at": (NOW - timedelta(days=20)).isoformat()}) or "")
    assert "no good offload" in (_pre(offload={}) or "")
    assert "no good offload" in (_pre(offload={
        "last_successful_offload": (NOW - timedelta(days=5)).isoformat()}) or "")


def test_apply_forgets_exactly_the_planned_snapshots_then_prunes_with_a_cap() -> None:
    fake = Fake()
    plan = rt.make_plan(TARGET, fake.deps(), weeks=26)
    fake.calls.clear()

    failure = rt.apply_plan(TARGET, fake.deps(), plan, say=lambda t: None)

    assert failure is None
    assert ["forget", "s003"] in fake.calls
    prune = next(c for c in fake.calls if c[0] == "prune")
    assert "--max-repack-size" in prune and rt.MAX_REPACK in prune
    assert fake.calls.index(["forget", "s003"]) < fake.calls.index(prune)


@pytest.mark.parametrize("kw,reason", [
    ({"presence": {"present": False, "reason": "tailscale0"}}, "not on the home network"),
    ({"drill": {}}, "has not passed"),
    ({"offload": {}}, "no good offload"),
    ({"locked": False}, "running right now"),
])
def test_apply_does_nothing_when_a_precondition_fails(kw: dict[str, Any], reason: str) -> None:
    fake = Fake()
    plan = rt.make_plan(TARGET, fake.deps(), weeks=26)
    broken = Fake(**kw)
    fake.calls.clear()

    failure = rt.apply_plan(TARGET, broken.deps(), plan, say=lambda t: None)

    assert failure and reason in failure
    assert not any(c[0] in ("forget", "prune") for c in broken.calls)


def test_apply_skips_snapshots_already_gone_and_refuses_a_plan_for_a_gone_repository() -> None:
    fake = Fake()
    plan = rt.make_plan(TARGET, fake.deps(), weeks=26)
    other = Fake(snaps=[s for s in SNAPS if s["id"] != "s003"])

    assert "none of the snapshots" in (rt.apply_plan(TARGET, other.deps(), plan) or "")
    assert not any(c[0] == "forget" and "--dry-run" not in c for c in other.calls)
    plan.remove = ["s000"]  # a stale plan that would now drop the newest
    assert "newest snapshot" in (rt.apply_plan(TARGET, Fake().deps(), plan) or "")


# ---- the command: plan, review, apply, schedule -------------------------------------------------


@pytest.fixture()
async def home(actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> Actions:
    monkeypatch.setenv(rt.STATE_ENV, str(tmp_path / "thin.json"))
    monkeypatch.setenv("OSIRIS_RESTORE_POINTS_FILE", str(tmp_path / "points.json"))
    from src.orchestrator import restic_credential

    monkeypatch.setattr(restic_credential, "get_restic_password", lambda: b"pw")
    await write_backup_settings(actions.pool, actor="operator", because="x",
                                offload_targets=[TARGET])
    return actions


async def _run(actions: Actions, fake: Fake, **kw: Any) -> tuple[int, list[str]]:
    said: list[str] = []
    code = await rt.command(pool=actions.pool, say=said.append, deps=fake.deps(), **kw)
    return code, said


async def test_with_no_flag_it_only_looks_and_saves_the_plan(home: Actions) -> None:
    fake = Fake()

    code, said = await _run(home, fake)

    assert code == 0, said
    assert any("would be forgotten" in line for line in said)
    assert not any(c[0] == "prune" or (c[0] == "forget" and "--dry-run" not in c)
                   for c in fake.calls)
    assert rt.read_state()["last_plan"]["remove"] == ["s003"]
    assert not rt.read_state().get("approved")


async def test_apply_needs_the_reviewed_plan_id_and_then_marks_the_first_run_approved(
        home: Actions) -> None:
    fake = Fake()
    await _run(home, fake)
    plan_id = rt.read_state()["last_plan"]["id"]

    wrong, said = await _run(home, Fake(), apply_id="deadbeef0000")
    assert wrong == 1 and any("not the latest one" in line for line in said)

    code, said = await _run(home, fake, apply_id=plan_id)

    assert code == 0 and ["forget", "s003"] in fake.calls
    state = rt.read_state()
    assert state["approved"]["plan_id"] == plan_id and state["last_run"]["failure"] is None
    assert any(c[0] == "find" for c in fake.calls)  # the restore map is rebuilt afterwards


async def test_a_failed_apply_is_recorded_and_does_not_approve(home: Actions) -> None:
    await _run(home, Fake())
    plan_id = rt.read_state()["last_plan"]["id"]

    code, said = await _run(home, Fake(drill={}), apply_id=plan_id)

    assert code == 1 and any("has not passed" in line for line in said)
    assert not rt.read_state().get("approved")
    assert rt.read_state()["last_run"]["failure"]


async def test_the_scheduled_run_only_refreshes_the_map_until_a_plan_was_applied_by_hand(
        home: Actions) -> None:
    fake = Fake()

    code, said = await _run(home, fake, scheduled=True)

    assert code == 0 and any("only the restore map is rebuilt" in line for line in said)
    assert not any(c[0] in ("forget", "prune") for c in fake.calls)


async def test_the_scheduled_run_does_the_work_once_approved_and_is_quiet_away_from_home(
        home: Actions) -> None:
    rt.write_state({"approved": {"at": "x", "plan_id": "y"}})
    fake = Fake()

    code, _ = await _run(home, fake, scheduled=True)

    assert code == 0 and ["forget", "s003"] in fake.calls
    away = Fake(presence={"present": False, "reason": "reachable only over tailscale0"})
    code, said = await _run(home, away, scheduled=True)
    assert code == 0 and not away.calls and any("not on the home network" in s for s in said)
    code, _ = await _run(home, Fake(presence={"present": False}))
    assert code == 1

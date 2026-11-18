"""Time Machine style thinning under the operator's rules: WAL and base backups never thin, dumps
thin by name, snapshots go only behind a coverage guard, and only a reviewed plan is applied."""
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
VAULT = "/home/u/osiris-vault"


def snap(i: int, hours_ago: float, tags: list[str] | None = None) -> dict[str, Any]:
    return {"id": f"s{i}", "time": (NOW - timedelta(hours=hours_ago)).isoformat(),
            "tags": tags or []}


# s0 newest .. s5 oldest; the tiers keep s0, s1, s2 and s5 (the weekly one)
SNAPS = [snap(0, 0.1), snap(1, 3), snap(2, 5), snap(3, 300), snap(4, 301), snap(5, 800)]
TIER_KEEP = {"s0", "s1", "s2", "s5"}
WAL_ONLY_IN_MIDDLE = "000000010000000100000001.zst"      # lives in s3 and s4 only
WAL_LONG = "000000010000000100000002.zst"                # lives in s5..s0
BASE = "osiris-basebackup-20261110-000000.tar.gz"
DUMPS = {  # name -> (mtime hours ago, snapshots holding it)
    "osiris-20261117-040000.dump": (27, ["s2", "s1", "s0"]),
    "osiris-20261117-100000.dump": (26, ["s2", "s1", "s0"]),
    "osiris-20261118-040000.dump": (5, ["s2", "s1", "s0"]),     # the newest of its day: kept
    "osiris-20261001-040000.dump": (800, ["s5"]),               # same week, older: dropped
    "osiris-20261002-040000.dump": (790, ["s5"]),               # newest of its week: kept
}


def _at(hours_ago: float) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def holders() -> dict[str, list[str]]:
    out = {WAL_ONLY_IN_MIDDLE: ["s3", "s4"], WAL_LONG: [s["id"] for s in SNAPS],
           BASE: [s["id"] for s in SNAPS], BASE + ".verified": [s["id"] for s in SNAPS]}
    out.update({n: v[1] for n, v in DUMPS.items()})
    return out


def path_of(name: str) -> str:
    if name.endswith(".zst"):
        return f"{VAULT}/wal_archive/{name}"
    if name.startswith("osiris-basebackup"):
        return f"{VAULT}/basebackups/{name}"
    return f"{VAULT}/{name}"


def proc(out: str = "", code: int = 0, err: str = "") -> Any:
    return SimpleNamespace(returncode=code, stdout=out, stderr=err)


class Fake:
    """A restic that answers from canned data and remembers every call."""

    def __init__(self, *, snaps: list[dict[str, Any]] | None = None,
                 presence: dict[str, Any] | None = None, locked: bool = True,
                 drill: dict[str, Any] | None = None,
                 offload: dict[str, Any] | None = None) -> None:
        self.snaps = snaps if snaps is not None else list(SNAPS)
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
            keep = [s for s in self.snaps if s["id"] in TIER_KEEP]
            gone = [s for s in self.snaps if s["id"] not in TIER_KEEP]
            return proc(json.dumps([{"keep": keep, "remove": gone}]))
        if args[0] == "find":
            ids = [args[i + 1] for i, a in enumerate(args) if a == "--snapshot"]
            out = []
            for sid in ids:
                matches = [{"type": "file", "path": path_of(n), "size": 1000,
                            "mtime": _at(DUMPS[n][0] if n in DUMPS else 100)}
                           for n, held in holders().items() if sid in held]
                out.append({"snapshot": sid, "matches": matches})
            return proc(json.dumps(out))
        if args[0] == "prune" and "--dry-run" in args:
            return proc("remaining:        433385 blobs / 500.000 GiB\n")
        return proc("")

    def deps(self) -> rt.Deps:
        @contextlib.contextmanager
        def lock() -> Any:
            yield self.locked

        return rt.Deps(
            run=self.run, presence=lambda t: self.presence, hold_lock=lock,
            drill_receipt=lambda u: self.drill, offload_receipt=lambda n: self.offload,
            repo_bytes=lambda t: 600 * 10**9, now=lambda: NOW, parse=parse_time)


def test_the_tiers_are_calendar_windows_and_keep_tagged_snapshots() -> None:
    args = rt.Policy().args()

    assert args[args.index("--keep-within") + 1] == "24h"
    assert args[args.index("--keep-within-hourly") + 1] == "48h"
    assert args[args.index("--keep-within-daily") + 1] == "30d"
    assert args[args.index("--keep-within-weekly") + 1] == "36500d"   # weekly for ever
    assert args[args.index("--keep-tag") + 1] == "history-import"
    assert "--keep-hourly" not in args and "--keep-daily" not in args


def _lives() -> dict[str, rt.Life]:
    return rt.file_lifetimes(Fake().run, SNAPS)


def test_every_wal_base_and_dump_gets_its_first_and_last_snapshot() -> None:
    lives = _lives()

    assert (lives[WAL_ONLY_IN_MIDDLE].first, lives[WAL_ONLY_IN_MIDDLE].last) == (
        SNAPS[4]["time"], SNAPS[3]["time"])
    assert lives[WAL_LONG].kind == "wal" and lives[BASE].kind == "base"
    assert lives[BASE + ".verified"].kind == "verified"
    assert lives["osiris-20261117-040000.dump"].kind == "dump"
    assert lives[WAL_LONG].path == f"{VAULT}/wal_archive/{WAL_LONG}"


def test_dumps_thin_to_the_newest_of_each_day_then_each_week_and_only_real_dump_names() -> None:
    lives = _lives()

    drop = rt.dump_drop_list(lives, now=NOW)

    assert sorted(drop) == ["osiris-20261001-040000.dump", "osiris-20261117-040000.dump"]
    assert rt.dump_drop_list(lives, now=NOW, weeks=2) == sorted(
        {"osiris-20261001-040000.dump", "osiris-20261002-040000.dump",
         "osiris-20261117-040000.dump"})  # past the cap even the weekly one goes
    assert all(rt._DUMP_NAME.match(n) for n in drop)


def test_the_cover_adds_the_snapshot_a_wal_file_would_otherwise_lose() -> None:
    kept, lost = rt.cover_snapshots(SNAPS, set(TIER_KEEP), [(SNAPS[4]["time"], SNAPS[3]["time"])])

    assert lost == 0 and kept == TIER_KEEP | {"s3"}   # the newest snapshot that still holds it
    same, none = rt.cover_snapshots(SNAPS, set(TIER_KEEP), [(SNAPS[2]["time"], SNAPS[0]["time"])])
    assert same == TIER_KEEP and none == 0            # already covered: nothing added


def test_the_plan_forgets_only_what_the_cover_allows_and_drops_dumps_by_name() -> None:
    fake = Fake()

    plan = rt.make_plan(TARGET, fake.deps())

    assert plan.remove == ["s4"] and "s3" in plan.keep and not plan.problems
    assert plan.uncovered == 0
    assert sorted(plan.drop_dumps) == ["osiris-20261001-040000.dump",
                                       "osiris-20261117-040000.dump"]
    assert all(p.startswith(VAULT + "/osiris-") for p in plan.drop_paths)
    assert "s5" in plan.rewrite and not any(i in plan.remove for i in plan.rewrite)
    assert plan.freed_bytes == int(plan.dump_bytes * rt.DUMP_SHARE_FREED)
    assert plan.prune_freed_bytes == 600 * 10**9 - int(500 * 1024 ** 3)
    text = "\n".join(rt.describe(plan))
    assert "every WAL segment, base backup and kept dump stays in a kept snapshot" in text
    assert f"osiris repo-thin --apply {plan.id}" in text
    # a dry run: nothing is forgotten, rewritten or pruned for real
    assert not any(c[0] in ("rewrite",) or (c[0] == "forget" and "--dry-run" not in c)
                   or (c[0] == "prune" and "--dry-run" not in c) for c in fake.calls)


def test_a_plan_that_drops_the_newest_a_tagged_snapshot_or_uncovers_wal_is_refused() -> None:
    lives = _lives()
    pk = [s for s in SNAPS if s["id"] in TIER_KEEP]
    tagged = [*SNAPS[:5], snap(5, 800, ["history-import"])]

    lost = rt.safety_problems(SNAPS, {"s1"}, [SNAPS[0]], [], 0)
    wal = rt.safety_problems(SNAPS, TIER_KEEP, [], [], 3)
    bad_name = rt.safety_problems(SNAPS, TIER_KEEP, [], ["osiris-repo.bundle"], 0)
    plan = rt.build_plan(tagged, pk, lives, [], now=NOW)

    assert "the newest snapshot would not be kept" in lost
    assert any("not be in any kept snapshot" in p for p in wal)
    assert any("not a dump file name" in p for p in bad_name)
    assert "s5" in plan.keep and "s5" not in plan.remove    # the tag protects it


def test_the_plan_id_follows_what_would_happen_and_not_the_order() -> None:
    a = rt.fingerprint(["s1", "s2"], ["--keep-last", "3"], ["d1"])

    assert a == rt.fingerprint(["s2", "s1"], ["--keep-last", "3"], ["d1"])
    assert a != rt.fingerprint(["s1", "s2"], ["--keep-last", "3"], ["d1", "d2"])
    assert a != rt.fingerprint(["s1"], ["--keep-last", "3"], ["d1"])


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


def test_apply_forgets_rewrites_by_exact_path_then_prunes_in_that_order() -> None:
    fake = Fake()
    plan = rt.make_plan(TARGET, fake.deps())
    fake.calls.clear()

    failure = rt.apply_plan(TARGET, fake.deps(), plan, say=lambda t: None)

    assert failure is None
    names = [c[0] for c in fake.calls]
    assert names.index("forget") < names.index("rewrite") < names.index("prune")
    assert ["forget", "s4"] in fake.calls
    rewrite = next(c for c in fake.calls if c[0] == "rewrite")
    assert "--forget" in rewrite and "--exclude-file" in rewrite and "s5" in rewrite
    prune = next(c for c in fake.calls if c[0] == "prune")
    assert "--max-repack-size" in prune and rt.MAX_REPACK in prune


@pytest.mark.parametrize("kw,reason", [
    ({"presence": {"present": False, "reason": "tailscale0"}}, "not on the home network"),
    ({"drill": {}}, "has not passed"),
    ({"offload": {}}, "no good offload"),
    ({"locked": False}, "running right now"),
])
def test_apply_does_nothing_when_a_precondition_fails(kw: dict[str, Any], reason: str) -> None:
    plan = rt.make_plan(TARGET, Fake().deps())
    broken = Fake(**kw)

    failure = rt.apply_plan(TARGET, broken.deps(), plan, say=lambda t: None)

    assert failure and reason in failure
    assert not any(c[0] in ("forget", "rewrite", "prune") for c in broken.calls)


def test_apply_refuses_an_unsafe_or_stale_plan_without_touching_anything() -> None:
    fake = Fake()
    plan = rt.make_plan(TARGET, fake.deps())
    fake.calls.clear()
    plan.uncovered = 2
    assert "uncovered" in (rt.apply_plan(TARGET, fake.deps(), plan) or "")
    plan.uncovered = 0
    plan.drop_dumps = ["wal_archive/0000.zst"]
    assert "not a dump file name" in (rt.apply_plan(TARGET, fake.deps(), plan) or "")
    plan.drop_dumps = []
    plan.remove = ["s0"]  # a stale plan that would now drop the newest
    assert "newest snapshot" in (rt.apply_plan(TARGET, fake.deps(), plan) or "")
    assert not any(c[0] in ("forget", "rewrite", "prune") and "--dry-run" not in c
                   for c in fake.calls)


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
    assert not any(c[0] in ("rewrite",) or (c[0] in ("prune", "forget") and "--dry-run" not in c)
                   for c in fake.calls)
    assert rt.read_state()["last_plan"]["remove"] == ["s4"]
    assert not rt.read_state().get("approved")


async def test_apply_needs_the_reviewed_plan_id_and_then_marks_the_first_run_approved(
        home: Actions) -> None:
    fake = Fake()
    await _run(home, fake)
    plan_id = rt.read_state()["last_plan"]["id"]

    wrong, said = await _run(home, Fake(), apply_id="deadbeef0000")
    assert wrong == 1 and any("not the latest one" in line for line in said)

    code, said = await _run(home, fake, apply_id=plan_id)

    assert code == 0 and ["forget", "s4"] in fake.calls
    state = rt.read_state()
    assert "approved" not in state and state["last_run"]["failure"] is None
    assert any(c[0] == "find" for c in fake.calls)  # the restore map is rebuilt afterwards


async def test_a_failed_apply_is_recorded_and_does_not_approve(home: Actions) -> None:
    await _run(home, Fake())
    plan_id = rt.read_state()["last_plan"]["id"]

    code, said = await _run(home, Fake(drill={}), apply_id=plan_id)

    assert code == 1 and any("has not passed" in line for line in said)
    assert rt.read_state()["last_run"]["failure"]


async def test_the_scheduled_run_only_plans_tells_the_desk_once_and_never_applies(
        home: Actions, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.orchestrator import mailbox

    sent: list[dict[str, Any]] = []

    async def fake_send(pool: Any, **kw: Any) -> None:
        sent.append(kw)

    monkeypatch.setattr(mailbox, "send_message", fake_send)
    rt.write_state({"approved": {"at": "x", "plan_id": "y"}})   # an old arming changes nothing
    fake = Fake()

    code, said = await _run(home, fake, scheduled=True)

    assert code == 0 and any("on the operator's desk" in line for line in said)
    assert not any(c[0] in ("forget", "rewrite", "prune") and "--dry-run" not in c
                   for c in fake.calls)
    assert len(sent) == 1 and sent[0]["to_project"] == "operator"
    assert sent[0]["desk_kind"] == "decision"
    plan_id = rt.read_state()["last_plan"]["id"]
    assert f"osiris repo-thin --apply {plan_id}" in sent[0]["body"]
    assert "files left uncovered" in sent[0]["body"] and "—" not in sent[0]["body"]
    code, _ = await _run(home, Fake(), scheduled=True)
    assert code == 0 and len(sent) == 1                           # the same plan is told once


async def test_the_scheduled_run_is_quiet_away_from_home(home: Actions) -> None:
    away = Fake(presence={"present": False, "reason": "reachable only over tailscale0"})

    code, said = await _run(home, away, scheduled=True)

    assert code == 0 and not away.calls and any("not on the home network" in s for s in said)
    code, _ = await _run(home, Fake(presence={"present": False}))
    assert code == 1


def test_the_desk_line_is_plain_and_absent_when_there_is_nothing_to_approve() -> None:
    plan = rt.make_plan(TARGET, Fake().deps())

    line = rt.desk_line(plan)

    assert line and "ready for your approval" in line and "2026-10-01" in line
    assert "rewind" in line and plan.id in line
    empty = rt.Plan(id="a", at="x", policy_args=[], keep=[], remove=[], total=3, oldest_keep=None,
                    newest_removed=None)
    assert rt.desk_line(empty) is None
    plan.problems = ["x"]
    assert rt.desk_line(plan) is None


async def test_the_weekly_run_also_refreshes_the_time_machine_list_and_survives_its_failure(
        home: Actions, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.orchestrator import time_machine_restic

    calls: list[str] = []

    def refresh(target: dict[str, Any], run: Any, *, presence: Any = None) -> str | None:
        calls.append(str(target.get("name")))
        return "the list could not be read"

    monkeypatch.setattr(time_machine_restic, "refresh_map", refresh)

    code, said = await _run(home, Fake(), scheduled=True)

    assert code == 0 and calls == ["nas"]
    assert any("a refresh could not be done: the list could not be read" in s for s in said)

    def boom(target: dict[str, Any], run: Any, *, presence: Any = None) -> str | None:
        raise RuntimeError("never reaches the run")

    monkeypatch.setattr(time_machine_restic, "refresh_map", boom)
    code, said = await _run(home, Fake(), scheduled=True)
    assert code == 0 and any("RuntimeError" in s for s in said)

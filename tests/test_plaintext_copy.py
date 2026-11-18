"""The old plaintext NAS copy, handled from the console (src/orchestrator/plaintext_copy.py): the
choice is recorded as an operator-authorized decision; the backup runs on the NAS and is proved to
hold every file by path and size; removal needs the recorded choice, its typed confirmation and a
fresh proof, and touches only the two fixed directories. The NAS is a fake: no ssh, no restic."""
from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.orchestrator import at_home, plaintext_copy

PASSWORD = b"correct-horse-battery-staple-9f31"
TARGET = {"name": "nas", "kind": "restic", "enabled": True,
          "path_or_url": "sftp:truenas:/mnt/Bunker/Vault/osiris-restic/repo"}
ROOT = plaintext_copy.SOURCE_ROOT
HELPERS = Path(__file__).parent.parent / "helpers"


class FakeNas:
    """Answers the few commands the job sends, and remembers each one with its stdin. When
    `snapshot` is given it is what a snapshot already in the repository holds; each new backup
    adds its own, from the file list the job wrote on the NAS."""

    DAYS = {"vault/a.sql": 1_756_000_000.0, "vault/b.dump": 1_756_100_000.0,
            "checkout-backups/c.sql": 1_756_100_500.0,  # the second and third share a day and week
            "vault/newer.dump": 1_757_000_000.0, "vault/older.dump": 1_755_900_000.0,
            "vault/000000010000000000000001": 1_756_200_000.0}

    def __init__(self, plain: dict[str, int], snapshot: dict[str, int] | None = None) -> None:
        self.plain = dict(plain)
        self.snaps: dict[str, dict[str, int]] = {}
        if snapshot is not None:
            self.snaps["abcdef01"] = dict(snapshot)
        self.rows: list[dict[str, Any]] = [
            {"short_id": "11112222", "hostname": "precision", "tags": None}] * 3
        self.commands: list[tuple[str, bytes | None]] = []
        self.removed = False
        self.check_rc = 0
        self.listfile: list[str] = []
        self.drop: set[str] = set()  # files a snapshot silently fails to hold
        self.shrink: dict[str, int] = {}  # files a snapshot holds at a different size

    def stream(self, host: str, command: str, stdin: bytes | None,
               result: dict[str, Any]) -> Iterator[str]:
        self.commands.append((command, stdin))
        result["rc"], result["err"] = 0, ""
        if "find " in command:
            for p, sz in self.plain.items():
                yield f"{p}\t{sz}\t{self.DAYS.get(p, 1_756_000_000.0)}"
        elif "cat > " in command:
            self.listfile = [x for x in (stdin or b"").decode().splitlines() if x]
        elif " snapshots " in command:
            yield json.dumps(self.rows)
        elif " backup " in command:
            sid = f"snap{len(self.snaps) + 1:04d}"
            tags = [command.split("--tag ")[i + 1].split()[0] for i in (0, 1)]
            self.snaps[sid] = {f[len(ROOT) + 1:]: self.plain[f[len(ROOT) + 1:]]
                               for f in self.listfile}
            self.rows = [*self.rows, {"short_id": sid, "hostname": "precision", "tags": tags}]
            yield json.dumps({"message_type": "status", "percent_done": 0.5,
                              "bytes_done": 1e9, "total_bytes": 2e9})
            yield json.dumps({"message_type": "summary", "snapshot_id": sid + "6789"})
        elif " ls " in command:
            ids = command.split("--no-lock ")[1].split()
            assert len(ids) == 1, "restic ls takes one snapshot; more are path filters"
            for sid in ids:
                for p, sz in self.snaps[sid].items():
                    if p not in self.drop:
                        yield json.dumps({"type": "file", "path": f"{ROOT}/{p}",
                                          "size": self.shrink.get(p, sz)})
        elif " check " in command:
            result["rc"], result["err"] = self.check_rc, "pack is damaged" if self.check_rc else ""
        elif "rm -rf" in command:
            self.removed = True
            yield "removed"


def _deps(nas: FakeNas, tmp_path: Path, *, choice: str | None, home: bool = True,
          locked: bool = True) -> plaintext_copy.Deps:
    written: list[dict[str, Any]] = []

    @contextmanager
    def lock() -> Iterator[bool]:
        yield locked

    deps = plaintext_copy.Deps(
        presence=lambda t: ({"present": True, "dev": "enp3s0"} if home
                            else {"present": False, "reason": "route leaves through tailscale0"}),
        stream=nas.stream, hold_lock=lock,
        write_coverage=lambda body: (written.append(body),
                                     (tmp_path / "cover.json").write_text(json.dumps(body))),
        read_choice=lambda: ({"choice": choice} if choice else None))
    deps.written = written  # type: ignore[attr-defined]
    return deps


@pytest.fixture(autouse=True)
def _world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OSIRIS_AT_HOME_PLAINTEXT_CHOICE_FILE", str(tmp_path / "choice.json"))
    monkeypatch.setenv("OSIRIS_AT_HOME_COVERAGE_FILE", str(tmp_path / "cover.json"))


PLAIN = {"vault/a.sql": 100, "vault/b.dump": 200, "checkout-backups/c.sql": 300}


async def run_encrypt(nas: FakeNas, tmp_path: Path, **kw: Any) -> tuple[int, list[str], Any]:
    said: list[str] = []
    deps = _deps(nas, tmp_path, **kw)
    rc = await plaintext_copy.encrypt(TARGET, PASSWORD, deps, say=said.append)
    return rc, said, deps


async def test_the_history_becomes_one_dated_snapshot_per_day_proved_by_path_and_size(
    tmp_path: Path,
) -> None:
    nas = FakeNas(PLAIN)

    rc, said, deps = await run_encrypt(nas, tmp_path, choice=plaintext_copy.ENCRYPT)

    assert rc == 0
    backups = [c for c, _s in nas.commands if " backup " in c]
    assert len(backups) == 2  # two different days
    first = backups[0]
    assert "restic -r /mnt/Bunker/Vault/osiris-restic/repo" in first  # the repo as the NAS sees it
    assert first.startswith("TZ=UTC ") and "--tag history-import" in first
    assert "--tag import-day:2025-08-24" in first and "--host precision" in first
    assert "--time '2025-08-24 01:46:40'" in first  # the day's newest file, in UTC
    assert "--files-from-verbatim /tmp/osiris-history-import.list" in first
    assert "--time '2025-08-25 05:41:40'" in backups[1]
    cover = deps.written[0]
    assert cover["only_files"] == 0 and cover["days"] == 2 and cover["tag"] == "history-import"
    assert cover["files_checked"] == 3 and len(cover["snapshots"]) == 2
    # the policy: of the second week's two dumps only the newest (c.sql) is imported
    assert cover["skipped"] == [{"path": "vault/b.dump", "size": 200}]
    assert cover["skipped_files"] == 1 and cover["skipped_bytes"] == 200
    assert cover["read_back"]["passed"] is True and cover["read_back"]["subset"] == "1%"
    assert cover["read_back"]["snapshots"] == cover["snapshots"] and cover["read_back"]["at"]
    assert any(" check --read-data-subset=1%" in c or " check " in c for c, _s in nas.commands)
    assert any("left out: vault/b.dump" in s for s in said)
    assert not any("vault/b.dump" in f for f in nas.listfile)  # it never went to the NAS's list
    assert at_home.read_coverage()["only_files"] == 0  # the checklist reads this very record
    assert any("day 1 of 2" in s and "50%" in s for s in said) and any("proved" in s for s in said)
    assert not nas.removed and not any("rm -rf" in c for c, _s in nas.commands)  # never removes
    assert any(c == "rm -f /tmp/osiris-history-import.list" for c, _s in nas.commands)


async def test_a_second_run_skips_the_days_already_in_the_repository(tmp_path: Path) -> None:
    nas = FakeNas(PLAIN)
    await run_encrypt(nas, tmp_path, choice=plaintext_copy.ENCRYPT)
    before = sum(" backup " in c for c, _s in nas.commands)

    rc, said, deps = await run_encrypt(nas, tmp_path, choice=plaintext_copy.ENCRYPT)

    assert rc == 0 and sum(" backup " in c for c, _s in nas.commands) == before  # none added
    assert deps.written[0]["only_files"] == 0 and deps.written[0]["days"] == 2


async def test_a_run_that_stopped_after_one_day_resumes_at_the_next(tmp_path: Path) -> None:
    nas = FakeNas(PLAIN)
    await run_encrypt(nas, tmp_path, choice=plaintext_copy.ENCRYPT)
    nas.rows = nas.rows[:-1]  # the second day's snapshot never landed
    before = sum(" backup " in c for c, _s in nas.commands)

    rc, _said, _deps2 = await run_encrypt(nas, tmp_path, choice=plaintext_copy.ENCRYPT)

    assert rc == 0 and sum(" backup " in c for c, _s in nas.commands) == before + 1


def test_the_policy_imports_the_newest_dump_of_each_week_and_everything_else() -> None:
    day = 86_400.0
    base = 1_756_000_000.0  # a Sunday: the next two days fall in the next ISO week
    files = {
        "vault/old-1.sql": (10, base), "vault/old-2.sql": (11, base + 2 * day),
        "vault/new.sql": (12, base + 3 * day),  # newest of its week
        "checkout-backups/new.sql": (12, base + 3 * day),  # its twin: same name and size
        "vault/wal_archive/000000010000000000000001": (5, base + day),
        "vault/basebackups/base.tar.gz": (7, base + day),
        "vault/lone.dump": (9, base + 40 * day),  # alone in its week
    }

    imported, left_out = plaintext_copy.select_import(files)

    assert left_out == ["vault/old-2.sql"]  # old-1 is the newest of the earlier week
    assert set(imported) == set(files) - {"vault/old-2.sql"}
    assert "checkout-backups/new.sql" in imported and "vault/new.sql" in imported


async def test_a_failed_read_back_is_recorded_as_not_passed(tmp_path: Path) -> None:
    nas = FakeNas(PLAIN)
    nas.check_rc = 1

    rc, _said, deps = await run_encrypt(nas, tmp_path, choice=plaintext_copy.ENCRYPT)

    assert rc == 1
    assert deps.written[-1]["read_back"]["passed"] is False  # the console sees that it did not


async def test_a_new_proof_never_inherits_an_old_read_back(tmp_path: Path) -> None:
    nas = FakeNas(PLAIN)
    nas.drop = {"vault/a.sql"}  # the proof stops at step 4, before any read-back

    rc, _said, deps = await run_encrypt(nas, tmp_path, choice=plaintext_copy.ENCRYPT)

    assert rc == 1 and "read_back" not in deps.written[0] and len(deps.written) == 1


def test_a_thinning_policy_keeps_every_history_import_snapshot() -> None:
    from src.orchestrator import restic_tags

    assert restic_tags.forget_protect_args() == ["--keep-tag", "history-import"]
    assert plaintext_copy.BACKUP_TAG in restic_tags.PROTECTED_TAGS


async def test_the_password_goes_only_on_stdin_never_in_a_command_or_a_line_said(
    tmp_path: Path,
) -> None:
    nas = FakeNas(PLAIN)

    _rc, said, deps = await run_encrypt(nas, tmp_path, choice=plaintext_copy.ENCRYPT)

    secret = PASSWORD.decode()
    assert all(secret not in c for c, _s in nas.commands)
    assert all(secret not in s for s in said)
    assert secret not in json.dumps(deps.written)
    with_stdin = [c for c, s in nas.commands if s is not None and "cat > " not in c]
    assert with_stdin and all("--password-file /dev/stdin" in c for c in with_stdin)
    assert all(s == PASSWORD + b"\n" for c, s in nas.commands
               if s is not None and "cat > " not in c)  # the file list is no secret


async def test_a_file_missing_from_the_snapshot_stops_the_proof_and_is_recorded(
    tmp_path: Path,
) -> None:
    nas = FakeNas(PLAIN)
    nas.drop = {"vault/a.sql"}
    nas.shrink = {"checkout-backups/c.sql": 299}  # a size that differs is as missing as a name

    rc, said, deps = await run_encrypt(nas, tmp_path, choice=plaintext_copy.ENCRYPT)

    assert rc == 1
    assert deps.written[0]["only_files"] == 2 and deps.written[0]["only_bytes"] == 400
    assert any("STOPPED" in s and "not in the snapshots" in s for s in said)
    assert not any(" check " in c for c, _s in nas.commands)  # it never reached the last step


async def test_a_failed_repository_check_stops_with_the_backup_not_proved_readable(
    tmp_path: Path,
) -> None:
    nas = FakeNas(PLAIN)
    nas.check_rc = 1

    rc, said, _ = await run_encrypt(nas, tmp_path, choice=plaintext_copy.ENCRYPT)

    assert rc == 1 and any("NOT proved readable" in s for s in said)


async def test_it_will_not_run_without_the_recorded_choice_or_off_the_home_lan(
    tmp_path: Path,
) -> None:
    nas = FakeNas(PLAIN)
    rc, said, _ = await run_encrypt(nas, tmp_path, choice=plaintext_copy.KEEP)
    assert rc == 1 and "not recorded" in " ".join(said) and nas.commands == []

    rc, said, _ = await run_encrypt(nas, tmp_path, choice=plaintext_copy.ENCRYPT, home=False)
    assert rc == 1 and "not on the home network" in " ".join(said) and nas.commands == []

    rc, said, _ = await run_encrypt(nas, tmp_path, choice=plaintext_copy.ENCRYPT, locked=False)
    assert rc == 1 and "running right now" in " ".join(said) and nas.commands == []


async def run_remove(nas: FakeNas, tmp_path: Path, confirm: object, **kw: Any) -> tuple[int, str]:
    said: list[str] = []
    rc = await plaintext_copy.remove(TARGET, PASSWORD, _deps(nas, tmp_path, **kw),
                                     confirm=confirm, say=said.append)
    return rc, " ".join(said)


def _proved(tmp_path: Path, only: int = 0, read_back: dict[str, Any] | None = None) -> None:
    (tmp_path / "cover.json").write_text(json.dumps(
        {"only_files": only, "only_bytes": 0, "snapshot": "abcdef01",
         "snapshots": ["abcdef01"],
         "read_back": read_back if read_back is not None else
         {"passed": True, "subset": "1%", "at": "2026-11-18T09:00:00+00:00",
          "snapshots": ["abcdef01"]},
         "skipped": [{"path": "vault/b.dump", "size": 200}]}))


async def test_removal_needs_the_recorded_choice_and_its_exact_confirmation(
    tmp_path: Path,
) -> None:
    nas = FakeNas(PLAIN, PLAIN)
    _proved(tmp_path)

    rc, said = await run_remove(nas, tmp_path, "remove", choice=plaintext_copy.KEEP)
    assert rc == 1 and "not recorded" in said and not nas.removed

    rc, said = await run_remove(nas, tmp_path, "yes", choice=plaintext_copy.ENCRYPT)
    assert rc == 1 and "typed exactly" in said and not nas.removed

    rc, said = await run_remove(nas, tmp_path, "remove", choice=plaintext_copy.ENCRYPT,
                                home=False)
    assert rc == 1 and "home network" in said and not nas.removed


async def test_removal_after_a_backup_proves_again_and_removes_only_the_two_directories(
    tmp_path: Path,
) -> None:
    nas = FakeNas(PLAIN, PLAIN)
    _proved(tmp_path)

    rc, said = await run_remove(nas, tmp_path, "Remove", choice=plaintext_copy.ENCRYPT)

    assert rc == 0 and nas.removed and "gone" in said
    rm = next(c for c, _s in nas.commands if "rm -rf" in c)
    assert rm.endswith("rm -rf -- vault checkout-backups && test ! -e vault "
                       "&& test ! -e checkout-backups && echo removed")
    assert "test ! -L vault" in rm and "test ! -L checkout-backups" in rm  # a link is refused
    assert ROOT in rm and rm.count("rm -rf") == 1


async def test_removal_needs_a_read_back_that_passed_for_these_snapshots(tmp_path: Path) -> None:
    for bad in ({"passed": False, "subset": "1%", "snapshots": ["abcdef01"]},
                {"passed": True, "subset": "1%", "snapshots": ["someother"]}, "yes", {}):
        nas = FakeNas(PLAIN, PLAIN)
        _proved(tmp_path, read_back=bad)  # type: ignore[arg-type]

        rc, said = await run_remove(nas, tmp_path, "remove", choice=plaintext_copy.ENCRYPT)

        assert rc == 1 and not nas.removed and "read-back" in said

    nas = FakeNas(PLAIN, PLAIN)
    (tmp_path / "cover.json").write_text(json.dumps(
        {"only_files": 0, "snapshot": "abcdef01", "snapshots": ["abcdef01"],
         "skipped": [{"path": "vault/b.dump", "size": 200}]}))  # no read_back field at all
    rc, said = await run_remove(nas, tmp_path, "remove", choice=plaintext_copy.ENCRYPT)
    assert rc == 1 and not nas.removed and "read-back" in said


async def test_a_file_that_appeared_after_the_proof_stops_the_removal(tmp_path: Path) -> None:
    nas = FakeNas({**PLAIN, "vault/newer.dump": 400}, PLAIN)  # newer than the snapshot
    _proved(tmp_path)

    rc, said = await run_remove(nas, tmp_path, "remove", choice=plaintext_copy.ENCRYPT)

    assert rc == 1 and not nas.removed and "vault/newer.dump" in said


async def test_removal_with_no_proof_on_record_does_nothing(tmp_path: Path) -> None:
    nas = FakeNas(PLAIN, PLAIN)
    for only in (None, 5):
        if only is not None:
            _proved(tmp_path, only)
        rc, said = await run_remove(nas, tmp_path, "remove", choice=plaintext_copy.ENCRYPT)
        assert rc == 1 and "no backup of this copy is recorded as proved" in said
        assert not nas.removed


async def test_delete_needs_its_choice_and_a_typed_sentence_and_skips_the_proof(
    tmp_path: Path,
) -> None:
    nas = FakeNas({**PLAIN, "vault/only-here.dump": 5}, PLAIN)

    rc, said = await run_remove(nas, tmp_path, "remove", choice=plaintext_copy.DELETE)
    assert rc == 1 and "typed exactly" in said and not nas.removed  # not the other mode's word

    rc, said = await run_remove(nas, tmp_path, plaintext_copy.CONFIRM_DELETE,
                                choice=plaintext_copy.DELETE)
    assert rc == 0 and nas.removed


async def test_a_dump_that_appeared_after_the_proof_stops_the_removal(tmp_path: Path) -> None:
    for extra, words in (("vault/newer.dump", "not in the imported snapshots"),
                         ("vault/older.dump", "did not list as left out")):
        nas = FakeNas({**PLAIN, extra: 400}, PLAIN)
        _proved(tmp_path)

        rc, said = await run_remove(nas, tmp_path, "remove", choice=plaintext_copy.ENCRYPT)

        assert rc == 1 and not nas.removed and words in said and extra in said


async def test_the_dump_left_out_on_purpose_does_not_block_a_removal_that_listed_it(
    tmp_path: Path,
) -> None:
    nas = FakeNas(PLAIN, {k: v for k, v in PLAIN.items() if k != "vault/b.dump"})
    _proved(tmp_path)

    rc, said = await run_remove(nas, tmp_path, "remove", choice=plaintext_copy.ENCRYPT)

    assert rc == 0 and nas.removed and "left out on purpose" in said


async def test_the_choice_is_recorded_as_an_operator_authorized_decision(
    actions: Actions, tmp_path: Path,
) -> None:
    bad = await plaintext_copy.record_choice(actions.pool, "burn it")
    assert "error" in bad and plaintext_copy.read_choice() is None

    out = await plaintext_copy.record_choice(actions.pool, plaintext_copy.ENCRYPT)

    assert out["choice"] == plaintext_copy.ENCRYPT
    assert plaintext_copy.read_choice()["choice"] == plaintext_copy.ENCRYPT
    ruled_by = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE type='ruled_by' AND from_id=$1::uuid", out["decision"])
    assert ruled_by == 1


# ---- the routes --------------------------------------------------------------------------------


def _client(actions: Actions, host: str = "127.0.0.1") -> httpx.AsyncClient:
    from src.orchestrator.manifests import load_manifests

    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(HELPERS)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=(host, 4321)), base_url="http://test")


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    async with _client(actions) as c:
        yield c


async def test_the_choice_route_records_it_and_answers_with_the_refreshed_checklist(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def sources(pool: Any) -> str:
        return "sources"

    monkeypatch.setattr(at_home, "real_sources", sources)
    monkeypatch.setattr(at_home, "checklist", lambda src: {"items": [], "src": src})

    bad = await client.post("/at-home/plaintext-copy/choice", json={"choice": "burn it"})
    ok = await client.post("/at-home/plaintext-copy/choice", json={"choice": "keep"})

    assert bad.status_code == 400 and "error" in bad.json()
    assert ok.status_code == 200 and ok.json()["choice"] == "keep"
    assert ok.json()["at_home"] == {"items": [], "src": "sources"}
    state = (await client.get("/at-home/plaintext-copy")).json()
    assert state["choice"]["choice"] == "keep"
    assert state["confirm"]["delete"] == plaintext_copy.CONFIRM_DELETE


async def test_nothing_here_answers_off_this_machine_and_the_probe_has_no_route(
    actions: Actions,
) -> None:
    async with _client(actions, host="192.168.1.50") as far:
        gets = [await far.get("/at-home/plaintext-copy")]
        posts = [await far.post(u, json={"choice": "delete", "confirm": "remove"}) for u in (
            "/at-home/plaintext-copy/choice", "/at-home/plaintext-copy/encrypt",
            "/at-home/plaintext-copy/remove")]
    assert all(r.status_code == 403 for r in [*gets, *posts])
    async with _client(actions) as here:
        assert (await here.post("/at-home/probe")).status_code in (404, 405)


async def test_the_checklist_item_carries_the_recorded_choice(tmp_path: Path) -> None:
    (tmp_path / "choice.json").write_text(json.dumps({"choice": "delete"}))
    src = at_home.Sources(
        envelope=lambda: {}, nas_target=lambda: None, offload_receipt=lambda n: {},
        drill_receipt=lambda u: {}, presence=lambda t: {}, plaintext_thread_open=lambda: True)

    item = at_home.plaintext_item(src, {"status": at_home.WAITING})

    assert item["choice"] == "delete" and item["key"] == "plaintext_copy"

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
    """Answers the few commands the job sends, and remembers each one (and its stdin)."""

    def __init__(self, plain: dict[str, int], snapshot: dict[str, int] | None = None) -> None:
        self.plain = dict(plain)
        self.snapshot = dict(plain) if snapshot is None else dict(snapshot)
        self.commands: list[tuple[str, bytes | None]] = []
        self.removed = False
        self.check_rc = 0

    def stream(self, host: str, command: str, stdin: bytes | None,
               result: dict[str, Any]) -> Iterator[str]:
        self.commands.append((command, stdin))
        result["rc"], result["err"] = 0, ""
        if "find " in command:
            yield from (f"{p}\t{s}" for p, s in self.plain.items())
        elif " backup " in command:
            yield json.dumps({"message_type": "status", "percent_done": 0.5,
                              "bytes_done": 1e9, "total_bytes": 2e9})
            yield json.dumps({"message_type": "summary", "snapshot_id": "abcdef0123456789"})
        elif " ls " in command:
            for p, s in self.snapshot.items():
                yield json.dumps({"type": "file", "path": f"{ROOT}/{p}", "size": s})
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


async def test_the_backup_runs_on_the_nas_and_is_proved_by_path_and_size(tmp_path: Path) -> None:
    nas = FakeNas(PLAIN)

    rc, said, deps = await run_encrypt(nas, tmp_path, choice=plaintext_copy.ENCRYPT)

    assert rc == 0
    backup = next(c for c, _s in nas.commands if " backup " in c)
    assert "restic -r /mnt/Bunker/Vault/osiris-restic/repo" in backup  # the repo as the NAS sees it
    assert backup.endswith("vault checkout-backups")  # only the two fixed directories
    cover = deps.written[0]
    assert cover["only_files"] == 0 and cover["snapshot"] == "abcdef01"
    assert cover["files_checked"] == 3
    assert at_home.read_coverage()["only_files"] == 0  # the checklist reads this very record
    assert any("progress: 50%" in s for s in said) and any("proved" in s for s in said)


async def test_the_password_goes_only_on_stdin_never_in_a_command_or_a_line_said(
    tmp_path: Path,
) -> None:
    nas = FakeNas(PLAIN)

    _rc, said, deps = await run_encrypt(nas, tmp_path, choice=plaintext_copy.ENCRYPT)

    secret = PASSWORD.decode()
    assert all(secret not in c for c, _s in nas.commands)
    assert all(secret not in s for s in said)
    assert secret not in json.dumps(deps.written)
    with_stdin = [c for c, s in nas.commands if s is not None]
    assert with_stdin and all("--password-file /dev/stdin" in c for c in with_stdin)
    assert all(s == PASSWORD + b"\n" for _c, s in nas.commands if s is not None)


async def test_a_file_missing_from_the_snapshot_stops_the_proof_and_is_recorded(
    tmp_path: Path,
) -> None:
    held = {k: v for k, v in PLAIN.items() if k != "vault/b.dump"}
    held["vault/a.sql"] = 99  # a size that differs is as missing as a name that does
    nas = FakeNas(PLAIN, held)

    rc, said, deps = await run_encrypt(nas, tmp_path, choice=plaintext_copy.ENCRYPT)

    assert rc == 1
    assert deps.written[0]["only_files"] == 2 and deps.written[0]["only_bytes"] == 300
    assert any("STOPPED" in s and "not in the snapshot" in s for s in said)
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


def _proved(tmp_path: Path, only: int = 0) -> None:
    (tmp_path / "cover.json").write_text(json.dumps(
        {"only_files": only, "only_bytes": 0, "snapshot": "abcdef01"}))


async def test_removal_needs_the_recorded_choice_and_its_exact_confirmation(
    tmp_path: Path,
) -> None:
    nas = FakeNas(PLAIN)
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
    nas = FakeNas(PLAIN)
    _proved(tmp_path)

    rc, said = await run_remove(nas, tmp_path, "Remove", choice=plaintext_copy.ENCRYPT)

    assert rc == 0 and nas.removed and "gone" in said
    rm = next(c for c, _s in nas.commands if "rm -rf" in c)
    assert rm.endswith("rm -rf -- vault checkout-backups && test ! -e vault "
                       "&& test ! -e checkout-backups && echo removed")
    assert "test ! -L vault" in rm and "test ! -L checkout-backups" in rm  # a link is refused
    assert ROOT in rm and rm.count("rm -rf") == 1


async def test_a_file_that_appeared_after_the_proof_stops_the_removal(tmp_path: Path) -> None:
    nas = FakeNas({**PLAIN, "vault/new.dump": 400}, PLAIN)  # newer than the snapshot
    _proved(tmp_path)

    rc, said = await run_remove(nas, tmp_path, "remove", choice=plaintext_copy.ENCRYPT)

    assert rc == 1 and not nas.removed and "vault/new.dump" in said


async def test_removal_with_no_proof_on_record_does_nothing(tmp_path: Path) -> None:
    nas = FakeNas(PLAIN)
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

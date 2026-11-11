"""`restic init` is reachable only when restic positively says the repository does not exist.
A timeout, a lock, a network or an authentication failure on the probe records its own error and
skips the target; a target that has ever offloaded is never initialised. Driven by a fake
`restic` on PATH (never a real repository, never the NAS)."""
from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path
from typing import Any

import pytest
from src.orchestrator import offload_runner

_FAKE = '''#!{python}
import json, os, sys, time

state_path = os.environ["FAKE_RESTIC_STATE"]
state = json.load(open(state_path))
args = sys.argv[1:]
state["calls"].append(args[:1] if args[:1] != ["list"] else ["list-locks"])
json.dump(state, open(state_path, "w"))
cmd = args[0] if args else ""
if cmd == "list":
    sys.exit(0)  # no locks
if cmd == "snapshots":
    if state["snapshots_sleep"]:
        time.sleep(state["snapshots_sleep"])
    if state["snapshots_stderr"]:
        print(state["snapshots_stderr"], file=sys.stderr)
        sys.exit(1)
    sys.exit(0)
if cmd == "init":
    print("Fatal: create repository failed: config file already exists", file=sys.stderr)
    sys.exit(1)
sys.exit(0)
'''

# restic's own wording for each situation
MISSING_LOCAL = ("Fatal: unable to open config file: Stat: stat /srv/repo/config: no such file "
                 "or directory\\nIs there a repository at the following location?\\n/srv/repo")
MISSING_SFTP = ("Fatal: repository does not exist: unable to open config file: "
                "file does not exist")
LOCKED = ("Fatal: unable to create lock in backend: repository is already locked exclusively "
          "by PID 7 on box")
NETWORK = ("Fatal: unable to open config file: ssh: connect to host truenas port 22: "
           "Connection timed out")
SFTP_DROPPED = ("Fatal: unable to open config file: sftp: ssh command exited: exit status 255: "
                "broken pipe")
WRONG_PASSWORD = "Fatal: wrong password or no key found"
PERMISSION = "Fatal: unable to open config file: permission denied"


class Fake:
    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.state_path = tmp_path / "state.json"
        self.state: dict[str, Any] = {"calls": [], "snapshots_stderr": "", "snapshots_sleep": 0}
        self.save()
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        exe = bin_dir / "restic"
        exe.write_text(_FAKE.format(python=sys.executable))
        exe.chmod(exe.stat().st_mode | stat.S_IXUSR)
        monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
        monkeypatch.setenv("FAKE_RESTIC_STATE", str(self.state_path))
        self.source = tmp_path / "vault"
        self.source.mkdir()

    def save(self) -> None:
        self.state_path.write_text(json.dumps(self.state))

    def commands(self) -> list[str]:
        calls = json.loads(self.state_path.read_text())["calls"]
        return [c[0] for c in calls]

    def backup(self, **kw: Any) -> str | None:
        return offload_runner._run_restic_backup(
            repository="fake", password=b"x", source=self.source, **kw)


@pytest.fixture
def fake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Fake:
    return Fake(tmp_path, monkeypatch)


@pytest.mark.parametrize("stderr", [LOCKED, NETWORK, SFTP_DROPPED, WRONG_PASSWORD, PERMISSION])
def test_a_probe_failure_that_is_not_a_missing_repository_never_inits(
    fake: Fake, stderr: str,
) -> None:
    fake.state["snapshots_stderr"] = stderr
    fake.save()

    failure = fake.backup()

    assert failure is not None
    # a lock held by someone else is a busy repository (a skip for the callers), anything else a
    # failure; neither ever initializes
    assert failure.startswith("repository busy") or failure.startswith("restic snapshots failed")
    assert "not initializing" in failure or failure.startswith("repository busy")
    assert "init" not in fake.commands() and "backup" not in fake.commands()


def test_a_probe_timeout_never_inits(fake: Fake, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(offload_runner, "_PROBE_TIMEOUT_SECONDS", 1)
    fake.state["snapshots_sleep"] = 4
    fake.save()

    failure = fake.backup()

    assert failure is not None and "timed out after 1s" in failure
    assert "nothing was initialized" in failure and "init" not in fake.commands()


@pytest.mark.parametrize("stderr", [MISSING_LOCAL, MISSING_SFTP])
def test_a_positively_missing_repository_is_initialised(fake: Fake, stderr: str) -> None:
    fake.state["snapshots_stderr"] = stderr.replace("\\n", "\n")
    fake.save()

    failure = fake.backup()

    assert "init" in fake.commands()  # the fake's init refuses, which is its own message
    assert failure is not None and failure.startswith("restic init failed")


def test_an_existing_repository_just_backs_up(fake: Fake) -> None:
    assert fake.backup() is None
    assert "init" not in fake.commands() and "backup" in fake.commands()


def test_a_target_that_ever_offloaded_is_never_initialised(fake: Fake) -> None:
    fake.state["snapshots_stderr"] = MISSING_SFTP
    fake.save()

    failure = fake.backup(init_if_needed=False)

    assert failure is not None and "has offloaded before" in failure
    assert "init" not in fake.commands() and "backup" not in fake.commands()


@pytest.mark.parametrize("text, missing", [
    (MISSING_LOCAL.replace("\\n", "\n"), True), (MISSING_SFTP, True),
    (LOCKED, False), (NETWORK, False), (WRONG_PASSWORD, False), (PERMISSION, False),
    ("", False), ("Fatal: something unexpected", False),
])
def test_the_missing_repository_classifier(text: str, missing: bool) -> None:
    assert offload_runner.repository_is_missing(text) is missing


SSH_NO_ROUTE = ("ssh: connect to host nas.local port 22: No route to host\n"
                "Fatal: unable to open repository at sftp:nas:/mnt/B/osiris: unexpected EOF")


def test_the_ssh_cause_before_restics_closing_line_is_kept_so_away_reads_as_away(
    fake: Fake,
) -> None:
    """restic ends a failed sftp run with 'unexpected EOF'; the ssh error that explains it is the
    line before. Only the last line was kept, so 'No route to host' was recorded as a backup
    failure instead of the quiet 'away from home' skip."""
    from src.orchestrator.network_presence import is_unreachable_error

    fake.state["snapshots_stderr"] = SSH_NO_ROUTE
    fake.save()

    failure = fake.backup()

    assert failure is not None and is_unreachable_error(failure)
    assert "No route to host" in failure and "unexpected EOF" in failure
    assert "init" not in fake.commands() and "backup" not in fake.commands()


def test_failure_summary_keeps_the_cause_only_when_the_last_line_lacks_it() -> None:
    from src.orchestrator.restic_locks import failure_summary

    assert failure_summary(SSH_NO_ROUTE).startswith("Fatal: unable to open repository")
    assert "(ssh: connect to host nas.local port 22: No route to host)" in failure_summary(
        SSH_NO_ROUTE)
    # a last line that already says why is returned alone, never doubled
    assert failure_summary("x\nssh: connect to host h port 22: Connection refused") == (
        "ssh: connect to host h port 22: Connection refused")
    # an earlier line that is not a cause adds nothing
    assert failure_summary("repository abc opened\nFatal: wrong password or no key found") == (
        "Fatal: wrong password or no key found")
    assert failure_summary("") == "no output" and failure_summary("  \n ") == "no output"

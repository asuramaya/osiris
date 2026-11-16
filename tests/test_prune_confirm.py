"""The prune ladder never removes a vault file the NAS has not confirmed (the 2026-11-15 loss:
3,341 WAL segments and many dumps deleted while the box was away and nothing had been offloaded)."""
from __future__ import annotations

import ast
import io
import json
import os
import tarfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import scripts.osiris_prune_ladder as ladder
from src.orchestrator import offload_confirm

SCRIPT = Path(ladder.__file__)
NOV1 = datetime(2026, 11, 1, 17, 15, tzinfo=UTC)
NOV5 = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
NOV15 = datetime(2026, 11, 15, 20, 18, tzinfo=UTC)
START = "0000000100000118000000B4"


def _touch(path: Path, when: datetime, size: int = 10) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    os.utime(path, (when.timestamp(), when.timestamp()))
    return path


def _base(vault: Path) -> Path:
    path = vault / "basebackups" / "osiris-basebackup-20261115-201856.tar.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    label = f"START WAL LOCATION: 118/B4000028 (file {START})\n".encode()
    with tarfile.open(path, "w:gz") as tf:
        info = tarfile.TarInfo("backup_label")
        info.size = len(label)
        tf.addfile(info, io.BytesIO(label))
    os.utime(path, (NOV15.timestamp(), NOV15.timestamp()))
    ladder.mark_base_backup_verified(path)
    return path


@pytest.fixture()
def box(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    receipts, held = tmp_path / "receipts.json", tmp_path / "held.json"
    monkeypatch.setenv("OSIRIS_OFFLOAD_RECEIPTS_FILE", str(receipts))
    monkeypatch.setenv("OSIRIS_PRUNE_HELD_FILE", str(held))
    monkeypatch.setattr(ladder, "_ALLOW_UNCONFIRMED", False)
    vault = tmp_path / "vault"
    _base(vault)
    old = [_touch(vault / "wal_archive" / f"0000000100000118000000B{n}", NOV5) for n in "01"]
    keepers = [_touch(vault / "wal_archive" / f"0000000100000118000000B{n}", NOV15) for n in "45"]
    return {"vault": vault, "backups": tmp_path / "backups", "receipts": receipts, "held": held,
            "old": old, "keep": keepers}


def _offloaded(box: dict[str, Any], started: datetime | None) -> None:
    entry: dict[str, Any] = {"tracked_since": "2026-10-05T17:00:02+00:00"}
    if started:
        entry["last_backup_start"] = started.isoformat()
    box["receipts"].write_text(json.dumps({"nas": entry}))


def test_with_no_offload_target_the_ladder_prunes_as_before(box: dict[str, Any]) -> None:
    plans, chains, wal, legacy = ladder._compute_plans(box["backups"], box["vault"])

    assert sorted(Path(s.path).name for s in wal["remove"]) == [p.name for p in box["old"]]
    assert not box["held"].exists()  # nothing tracked: nothing to report


def test_an_unconfirmed_wal_segment_is_never_planned_for_removal(box: dict[str, Any]) -> None:
    _offloaded(box, NOV1)  # the last offload began 11-01; these segments were written 11-05

    _, _, wal, _ = ladder._compute_plans(box["backups"], box["vault"])

    assert wal["remove"] == []
    assert {Path(s.path).name for s in wal["keep"]} >= {p.name for p in box["old"]}
    state = json.loads(box["held"].read_text())
    assert state["files"] == 2 and state["kinds"] == {"WAL segment": 2}
    assert state["over_ceiling"] is False and state["confirmed_through"].startswith("2026-11-01")


def test_a_confirmed_segment_is_removed_once_a_later_offload_began_after_it(
        box: dict[str, Any]) -> None:
    _offloaded(box, NOV15)

    _, _, wal, _ = ladder._compute_plans(box["backups"], box["vault"])

    assert sorted(Path(s.path).name for s in wal["remove"]) == [p.name for p in box["old"]]
    assert json.loads(box["held"].read_text())["files"] == 0


def test_a_record_that_never_stored_a_start_confirms_nothing(box: dict[str, Any]) -> None:
    _offloaded(box, None)

    _, _, wal, _ = ladder._compute_plans(box["backups"], box["vault"])
    box["receipts"].write_text("{broken")
    _, _, wal2, _ = ladder._compute_plans(box["backups"], box["vault"])

    assert wal["remove"] == [] and wal2["remove"] == []


def test_apply_is_the_last_line_even_for_a_plan_built_elsewhere(box: dict[str, Any]) -> None:
    _offloaded(box, NOV1)
    forged = {"keep": [], "remove": [ladder.WalSegment(str(p), NOV5, 10) for p in box["old"]]}

    refused = ladder._apply({}, {"keep": [], "remove": []}, forged)

    assert refused == 2 and all(p.exists() for p in box["old"])
    _offloaded(box, NOV15)
    assert ladder._apply({}, {"keep": [], "remove": []}, forged) == 0
    assert not any(p.exists() for p in box["old"])


def test_dumps_base_backups_and_a_whole_chain_are_held_the_same_way(box: dict[str, Any]) -> None:
    vault = box["vault"]
    dump = _touch(vault / "osiris-20261105-120000.dump", NOV5)
    old_base = _touch(vault / "basebackups" / "osiris-basebackup-20261101-165136.tar.gz", NOV1)
    _offloaded(box, datetime(2026, 11, 3, tzinfo=UTC))
    chain_files = [str(_touch(vault / f"transcripts-2026-W44-{n}.tar", NOV5)) for n in "ab"]
    plans = {"vault": {"keep": [], "remove": [ladder.DumpFile(str(dump), NOV5, 10)]},
             "vault/basebackups": {"keep": [],
                                   "remove": [ladder.DumpFile(str(old_base), NOV1, 10)]},
             "backups/": {"keep": [], "remove": []}}
    chain = ladder.TranscriptChain("2026-W44", NOV5, chain_files, 20)
    chains = {"keep": [], "remove": [chain]}
    wal: dict[str, list[Any]] = {"keep": [], "remove": []}
    legacy: dict[str, list[Any]] = {"keep": [], "remove": []}

    held = ladder._hold_unconfirmed(plans, chains, wal, legacy, offload_confirm.load_horizon())

    assert plans["vault"]["remove"] == [] and chains["remove"] == []  # written 11-05, after 11-03
    assert len(plans["vault/basebackups"]["remove"]) == 1  # 11-01 is before the 11-03 start
    assert held.kinds == {"dump": 1, "transcript chain": 1}


def test_the_override_flag_deletes_and_says_so(box: dict[str, Any],
                                                capsys: pytest.CaptureFixture[str]) -> None:
    _offloaded(box, NOV1)

    code = ladder.main(["--backups", str(box["backups"]), "--vault", str(box["vault"]),
                        "--apply", "--allow-unconfirmed"])

    assert code == 0 and "NOT being asked" in capsys.readouterr().out
    assert not any(p.exists() for p in box["old"])


def test_the_plain_apply_keeps_them(box: dict[str, Any]) -> None:
    _offloaded(box, NOV1)

    code = ladder.main(["--backups", str(box["backups"]), "--vault", str(box["vault"]), "--apply"])

    assert code == 0 and all(p.exists() for p in box["old"])


def test_past_the_ceiling_it_only_says_so_and_never_deletes(
        box: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OSIRIS_PRUNE_HOLD_CEILING_BYTES", "5")
    _offloaded(box, NOV1)

    ladder._compute_plans(box["backups"], box["vault"])
    state = offload_confirm.read_held()

    assert state is not None and state["over_ceiling"] is True
    text = offload_confirm.describe_held(state)
    assert text and "over the" in text and "nothing is deleted" in text
    assert all(p.exists() for p in box["old"])


def test_every_unlink_in_the_ladder_sits_behind_the_guard() -> None:
    tree = ast.parse(SCRIPT.read_text())
    sites: list[str] = []
    for fn in [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]:
        for node in ast.walk(fn):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                    and node.func.attr in {"unlink", "remove", "rmtree", "rmdir"}:
                sites.append(fn.name)
    # only `_apply` (and its inner guarded helper) may delete anything
    assert sites and set(sites) <= {"_apply", "unlink_vault"}
    apply_src = ast.get_source_segment(SCRIPT.read_text(), next(
        n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_apply"))
    assert apply_src and "horizon.confirms" in apply_src and "_current_horizon()" in apply_src

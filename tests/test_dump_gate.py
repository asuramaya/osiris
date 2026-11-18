"""Fewer dumps while away from the NAS, and the uncompressed dump format."""
from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import scripts.osiris_dump_gate as gate
from src.orchestrator import offload_confirm as oc

NOW = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
REPO = Path(__file__).resolve().parent.parent


def test_up_to_date_or_unknown_always_takes_the_dump() -> None:
    assert oc.take_dump(NOW - timedelta(hours=3), NOW - timedelta(minutes=5), NOW)[0] is True
    assert oc.take_dump(None, NOW - timedelta(minutes=5), NOW) == (
        True, "no offload record, taking the dump")      # fail toward taking it
    assert oc.take_dump(NOW - timedelta(days=9), None, NOW)[0] is True   # no dump yet


def test_away_for_more_than_a_day_takes_at_most_one_dump_a_day() -> None:
    away = NOW - timedelta(hours=30)

    skip, why = oc.take_dump(away, NOW - timedelta(hours=6), NOW)
    take, _ = oc.take_dump(away, NOW - timedelta(hours=25), NOW)

    assert skip is False and "skipping" in why
    assert take is True


def _write(tmp_path: Path, body: object) -> Path:
    path = tmp_path / "receipts.json"
    path.write_text(body if isinstance(body, str) else json.dumps(body))
    return path


def test_the_last_offload_end_reads_the_newest_and_cannot_fail_toward_skipping(
        tmp_path: Path) -> None:
    good = _write(tmp_path, {"nas": {"last_backup_end": "2026-11-17T10:00:00+00:00"},
                             "usb": {"last_successful_offload": "2026-11-17T12:00:00+00:00"}})
    assert oc.last_offload_end(good) == datetime(2026, 11, 17, 12, 0, tzinfo=UTC)
    assert oc.last_offload_end(tmp_path / "missing.json") is None
    assert oc.last_offload_end(_write(tmp_path, "{broken")) is None
    assert oc.last_offload_end(_write(tmp_path, {"nas": {"last_backup_end": "not a time"}})) is None
    assert oc.last_offload_end(_write(tmp_path, [1, 2])) is None


def test_the_script_exits_one_only_when_away_with_a_recent_dump(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    dump = vault / "osiris-20261118-060000.dump"
    dump.write_bytes(b"x")
    now = datetime.now(UTC)
    os.utime(dump, ((now - timedelta(hours=6)).timestamp(),) * 2)
    receipts = tmp_path / "r.json"
    monkeypatch.setenv("OSIRIS_OFFLOAD_RECEIPTS_FILE", str(receipts))

    def ended(ago: timedelta) -> str:
        return json.dumps({"nas": {"last_backup_end": (now - ago).isoformat()}})

    receipts.write_text(ended(timedelta(hours=2)))
    assert gate.main([str(vault)]) == 0                       # up to date
    receipts.write_text(ended(timedelta(days=3)))
    assert gate.main([str(vault)]) == 1                       # away, one dump already today
    os.utime(dump, ((now - timedelta(hours=30)).timestamp(),) * 2)
    assert gate.main([str(vault)]) == 0                       # away, but none in 24 hours
    receipts.write_text("{broken")
    os.utime(dump, ((now - timedelta(hours=1)).timestamp(),) * 2)
    assert gate.main([str(vault)]) == 0                       # unreadable record: take it


def test_the_backup_script_takes_the_uncompressed_format_and_asks_the_gate_first() -> None:
    text = (REPO / "scripts" / "osiris_backup.sh").read_text()

    assert "pg_dump -U osiris -d osiris -Fc -Z0" in text
    assert text.index("osiris_dump_gate.py") < text.index("pg_dump -U osiris")
    assert "osiris_disk_guard.py" in text

"""The retention ladder: pure logic only. Dry-run reporting and I/O stay in the CLI's
own thin shell, exercised separately."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from scripts.osiris_prune_ladder import (
    DumpFile,
    TranscriptChain,
    WalSegment,
    _scan_legacy_tarballs,
    _seat_handles,
    _seat_transcript_dir,
    attribute_sessions_to_seats,
    plan_prune,
    plan_prune_legacy_tarballs,
    plan_prune_transcript_chains,
    plan_prune_wal,
)
from scripts.osiris_transcript_cache_prune import SessionRow

NOW = datetime(2026, 9, 8, 12, 0, tzinfo=UTC)


def _f(path: str, hours_ago: float) -> DumpFile:
    return DumpFile(path, NOW - timedelta(hours=hours_ago))


def test_everything_inside_48h_survives_whole() -> None:
    files = [_f(f"h{i}", i * 6) for i in range(8)]  # 0h, 6h, 12h, ..., 42h, all under 48h
    plan = plan_prune(files, now=NOW)
    assert {f.path for f in plan["keep"]} == {f.path for f in files}
    assert plan["remove"] == []


def test_daily_tier_keeps_one_per_calendar_day() -> None:
    # three dumps on the SAME calendar day (2026-09-05), all past the 48h hot window
    # (NOW is 2026-09-08 12:00) and inside the 30d daily window, only the newest of the
    # three must survive.
    files = [
        DumpFile("morning", datetime(2026, 9, 5, 6, tzinfo=UTC)),
        DumpFile("noon", datetime(2026, 9, 5, 12, tzinfo=UTC)),
        DumpFile("evening", datetime(2026, 9, 5, 20, tzinfo=UTC)),
        DumpFile("other_day", datetime(2026, 9, 4, 12, tzinfo=UTC)),  # its own bucket too
    ]
    plan = plan_prune(files, now=NOW)
    kept = {f.path for f in plan["keep"]}
    assert "evening" in kept  # newest of the same-day trio
    assert "morning" not in kept and "noon" not in kept
    assert "other_day" in kept


def test_weekly_tier_keeps_one_per_calendar_week() -> None:
    # fixed absolute dates, both inside the 30d-1y weekly window relative to NOW
    # (2026-09-08): 2026-01-05/2026-01-07 share ISO week 2026-W02, so only the newer of
    # the two must survive.
    early = DumpFile("early", datetime(2026, 1, 5, tzinfo=UTC))
    later = DumpFile("later", datetime(2026, 1, 7, tzinfo=UTC))
    plan = plan_prune([early, later], now=NOW)
    assert {f.path for f in plan["keep"]} == {"later"}

    # a THIRD dump the following ISO week (2026-W03) is a different bucket, survives
    # independently
    next_week = DumpFile("next_week", datetime(2026, 1, 12, tzinfo=UTC))
    plan2 = plan_prune([early, later, next_week], now=NOW)
    assert {f.path for f in plan2["keep"]} == {"later", "next_week"}


def test_monthly_tier_keeps_one_per_calendar_month_forever() -> None:
    # two dumps well over a year apart, different months, both survive (one per month,
    # no upper bound on age)
    files = [_f("ancient", 800 * 24), _f("prehistoric", 1500 * 24)]
    plan = plan_prune(files, now=NOW)
    assert {f.path for f in plan["keep"]} == {"ancient", "prehistoric"}


def test_monthly_tier_thins_two_dumps_in_the_same_month() -> None:
    same_month_a = DumpFile("early", datetime(2024, 1, 3, tzinfo=UTC))
    same_month_b = DumpFile("late", datetime(2024, 1, 28, tzinfo=UTC))
    plan = plan_prune([same_month_a, same_month_b], now=NOW)
    assert {f.path for f in plan["keep"]} == {"late"}  # newer of the pair survives


def test_a_realistic_mixed_population_across_all_four_tiers() -> None:
    files = [
        _f("hot1", 1), _f("hot2", 47),  # hot (< 48h): both survive
        # daily (48h-30d): same calendar day 2026-09-05, newest of the pair survives
        DumpFile("day_old", datetime(2026, 9, 5, 6, tzinfo=UTC)),
        DumpFile("day_old_dup", datetime(2026, 9, 5, 20, tzinfo=UTC)),
        # weekly (30d-1y): same ISO week 2026-W23, newest of the pair survives
        DumpFile("week_old", datetime(2026, 6, 1, tzinfo=UTC)),
        DumpFile("week_old_dup", datetime(2026, 6, 3, tzinfo=UTC)),
        # monthly (>1y): alone in its month, survives
        DumpFile("month_old", datetime(2024, 5, 15, tzinfo=UTC)),
    ]
    plan = plan_prune(files, now=NOW)
    kept = {f.path for f in plan["keep"]}
    assert kept == {"hot1", "hot2", "day_old_dup", "week_old_dup", "month_old"}
    removed = {f.path for f in plan["remove"]}
    assert removed == {"day_old", "week_old"}


def test_keep_and_remove_partition_the_input_with_no_overlap() -> None:
    files = [_f(f"f{i}", i * 11) for i in range(30)]
    plan = plan_prune(files, now=NOW)
    kept = {f.path for f in plan["keep"]}
    removed = {f.path for f in plan["remove"]}
    assert kept | removed == {f.path for f in files}
    assert kept & removed == set()


# ── the CLI's own safety property: dry-run is the default, --apply is required ──────────

def test_cli_without_apply_never_deletes_anything(tmp_path, capsys) -> None:
    """Nothing gets deleted until a human has reviewed the removal list: the default
    invocation must be a pure report, whatever it finds."""
    from scripts.osiris_prune_ladder import main

    backups = tmp_path / "backups"
    vault = tmp_path / "vault"
    backups.mkdir()
    vault.mkdir()
    # backups/ is a one day cache: a copy older than that is listed once the vault holds it
    cached = backups / "osiris-20200101-000000.dump"
    cached.write_bytes(b"x" * 100)
    (vault / cached.name).write_bytes(b"x" * 100)

    rc = main(["--backups", str(backups), "--vault", str(vault)])

    assert rc == 0
    assert cached.exists(), "no --apply flag given, nothing may be deleted"
    out = capsys.readouterr().out
    assert "DRY RUN ONLY" in out
    assert "REMOVE  " + str(cached) in out


def test_cli_with_apply_deletes_exactly_the_planned_removals(tmp_path, capsys) -> None:
    from scripts.osiris_prune_ladder import main

    backups = tmp_path / "backups"
    vault = tmp_path / "vault"
    backups.mkdir()
    vault.mkdir()
    cached = backups / "osiris-20200101-000000.dump"
    cached.write_bytes(b"x" * 100)
    (vault / cached.name).write_bytes(b"x" * 100)
    only_here = backups / "osiris-20200115-000000.dump"  # the vault has no copy of this one
    only_here.write_bytes(b"x" * 100)
    # the CLI's own `main()` uses the REAL wall clock (never the test module's fixed
    # NOW), so name this one after it directly to land it inside the one day window
    real_now = datetime.now(UTC)
    fresh = backups / f"osiris-{real_now.strftime('%Y%m%d-%H%M%S')}.dump"
    fresh.write_bytes(b"y" * 100)

    rc = main(["--backups", str(backups), "--vault", str(vault), "--apply"])

    assert rc == 0
    assert not cached.exists(), "the planned removal must actually be gone under --apply"
    assert (vault / cached.name).exists(), "the vault copy is never part of the cache's plan"
    assert only_here.exists(), "a dump that exists nowhere else is never removed from backups/"
    assert fresh.exists(), "a file inside the one day window must never be touched"


# ── transcript CHAIN pruning: whole weekly chains, never a tarball out
# of the middle of one ─────────────────────────────────────────────────────────────────

def _chain(week_key: str, week_start: datetime, n_files: int = 3) -> TranscriptChain:
    return TranscriptChain(week_key, week_start,
                           [f"{week_key}-{i}.tar.gz" for i in range(n_files)])


def test_the_four_most_recent_chains_survive_whole() -> None:
    chains = [_chain(f"2026-W{30 + i:02d}", datetime(2026, 7, 20, tzinfo=UTC) + timedelta(weeks=i))
             for i in range(6)]  # W30..W35, oldest first
    plan = plan_prune_transcript_chains(chains, keep_recent=4)
    kept_keys = {c.week_key for c in plan["keep"]}
    # the 4 NEWEST (the last four weeks) survive whole regardless of month bucketing
    assert {"2026-W32", "2026-W33", "2026-W34", "2026-W35"} <= kept_keys


def test_older_chains_thin_to_one_per_calendar_month() -> None:
    # two chains from the SAME month, both well outside the 4-most-recent window
    older_a = _chain("2024-W02", datetime(2024, 1, 8, tzinfo=UTC))
    older_b = _chain("2024-W03", datetime(2024, 1, 15, tzinfo=UTC))
    recent = [_chain(f"2026-W{30 + i:02d}", datetime(2026, 7, 20, tzinfo=UTC) + timedelta(weeks=i))
             for i in range(4)]
    plan = plan_prune_transcript_chains([older_a, older_b, *recent], keep_recent=4)
    kept_keys = {c.week_key for c in plan["keep"]}
    assert "2024-W03" in kept_keys  # the newer of the same-month pair survives
    assert "2024-W02" not in kept_keys


def test_a_removed_chain_carries_every_one_of_its_own_files() -> None:
    """The whole-chain guarantee: removing a chain must never leave a partial one.
    Every file that chain owns comes back in the removal, together. A LONE old chain
    survives forever (the one-per-month rule); two chains in the SAME month are needed
    for the elder to actually be thinned."""
    old_elder = _chain("2024-W02", datetime(2024, 1, 8, tzinfo=UTC), n_files=5)
    old_younger = _chain("2024-W03", datetime(2024, 1, 15, tzinfo=UTC))
    recent = [_chain(f"2026-W{30 + i:02d}", datetime(2026, 7, 20, tzinfo=UTC) + timedelta(weeks=i))
             for i in range(4)]
    plan = plan_prune_transcript_chains([old_elder, old_younger, *recent], keep_recent=4)
    removed = next(c for c in plan["remove"] if c.week_key == "2024-W02")
    assert set(removed.files) == set(old_elder.files)


def test_cli_reports_transcript_chains_separately_from_dump_files(tmp_path, capsys) -> None:
    from scripts.osiris_prune_ladder import main

    backups = tmp_path / "backups"
    vault = tmp_path / "vault"
    backups.mkdir()
    vault.mkdir()
    # 6 weekly chains, all in the same ancient month (Jan 2024), every one of them falls
    # outside the keep_recent=4 window, so the whole set thins to just its own newest
    # survivor, guaranteeing at least one REMOVE CHAIN line.
    for i in range(6):
        week = datetime(2024, 1, 1, tzinfo=UTC) + timedelta(weeks=i)
        key = f"{week.isocalendar()[0]}-W{week.isocalendar()[1]:02d}"
        (vault / f"claude-transcripts-{key}-{week.strftime('%Y%m%d')}.tar.gz").write_bytes(b"x")

    rc = main(["--backups", str(backups), "--vault", str(vault)])

    assert rc == 0
    out = capsys.readouterr().out
    assert "transcript chains" in out
    assert "REMOVE CHAIN" in out


# ── base backups (item 3, osiris_base_backup.sh): same ladder as DB dumps, a distinct
# scanned population under <vault>/basebackups/ ─────────────────────────────────────────

def test_scan_parses_the_basebackup_filenames_own_timestamp(tmp_path) -> None:
    from scripts.osiris_prune_ladder import _scan

    (tmp_path / "osiris-basebackup-20240115-093000.tar.gz").write_bytes(b"x" * 42)
    [found] = _scan(tmp_path)
    assert found.when == datetime(2024, 1, 15, 9, 30, 0, tzinfo=UTC)
    assert found.size_bytes == 42


def test_scan_never_counts_the_repo_bundle_as_a_dump(tmp_path) -> None:
    """osiris_backup.sh writes osiris-repo.bundle straight into the SAME vault
    directory _scan's own default `osiris-*` glob matches, and rewrites it every
    6-hourly run, so its mtime is therefore ALWAYS the freshest thing there. Before this
    guard, the mtime fallback let it win `max(dumps, key=lambda f: f.when)` in
    osiris_disk_guard.py's own main(), silently substituting a ~10MB bundle for a real
    ~2.7GB dump as the last dump (a real bug found in production, 2026-09-09)."""
    from scripts.osiris_prune_ladder import _scan

    real_dump = tmp_path / "osiris-20260908-163007.dump"
    real_dump.write_bytes(b"x" * 100)
    bundle = tmp_path / "osiris-repo.bundle"
    bundle.write_bytes(b"y" * 10)
    import os
    import time
    future = time.time() + 3600  # the bundle is always refreshed LAST, i.e. newest
    os.utime(bundle, (future, future))

    found = _scan(tmp_path)
    assert {f.path for f in found} == {str(real_dump)}


def test_scan_still_falls_back_to_mtime_for_a_real_dump_shaped_stray(tmp_path) -> None:
    """The fallback isn't removed for genuine dump-shaped files, only narrowed away
    from unrelated ones: a .dump file with a name _NAME_RE doesn't parse still gets a
    real answer via mtime, same as before this guard."""
    from scripts.osiris_prune_ladder import _scan

    stray = tmp_path / "osiris-manual-export.dump"
    stray.write_bytes(b"z" * 7)
    [found] = _scan(tmp_path)
    assert found.path == str(stray)
    assert found.size_bytes == 7


def test_cli_reports_and_prunes_basebackups_as_their_own_population(
    tmp_path, capsys,
) -> None:
    from scripts.osiris_prune_ladder import main

    backups = tmp_path / "backups"
    vault = tmp_path / "vault"
    basebackups = vault / "basebackups"
    backups.mkdir()
    vault.mkdir()
    basebackups.mkdir()
    # two ancient same-month basebackups, the elder should be removable
    (basebackups / "osiris-basebackup-20240108-000000.tar.gz").write_bytes(b"x" * 10)
    (basebackups / "osiris-basebackup-20240115-000000.tar.gz").write_bytes(b"x" * 10)

    rc = main(["--backups", str(backups), "--vault", str(vault)])

    assert rc == 0
    out = capsys.readouterr().out
    assert "vault/basebackups" in out
    assert "osiris-basebackup-20240108-000000.tar.gz" in out  # the elder, listed to remove
    assert "osiris-basebackup-20240115-000000.tar.gz" not in out  # the survivor, not listed


# --- WAL retention -----------------------------------------------

def _seg(name: str) -> WalSegment:
    return WalSegment(f"/vault/wal_archive/{name}", NOW)


def _write_base_backup(path: Path, start_file: str) -> None:
    """A real gzip tarball whose first member is a `backup_label` naming `start_file`."""
    import io
    import tarfile

    label = (f"START WAL LOCATION: 1A/17A00028 (file {start_file})\n"
             "BACKUP METHOD: streamed\n").encode()
    with tarfile.open(path, "w:gz") as tf:
        info = tarfile.TarInfo("backup_label")
        info.size = len(label)
        tf.addfile(info, io.BytesIO(label))
        data = b"x" * 100
        info2 = tarfile.TarInfo("PG_VERSION")
        info2.size = len(data)
        tf.addfile(info2, io.BytesIO(data))


A = "000000010000001A00000017"


def test_wal_segments_before_the_oldest_kept_backups_start_are_removed() -> None:
    old, at, later = (_seg("000000010000001A00000016"), _seg(A),
                      _seg("000000010000001A00000018"))
    plan = plan_prune_wal([old, at, later], kept_start_segments=[A])
    assert plan["remove"] == [old]
    assert plan["keep"] == [at, later]


def test_wal_retention_is_by_position_never_by_file_time() -> None:
    """A segment's file time is when it was pulled into the vault and a backup's file name
    is local wall-clock time parsed as UTC; neither says what a restore needs. A segment
    that LOOKS ancient by file time is kept when the backup still needs it."""
    ancient = WalSegment("/v/000000010000001A00000020", NOW - timedelta(days=400))
    plan = plan_prune_wal([ancient], kept_start_segments=[A])
    assert plan["keep"] == [ancient] and plan["remove"] == []


def test_wal_retention_keeps_what_the_oldest_of_several_kept_backups_needs() -> None:
    newer_start = "000000010000001B00000005"
    mid, early = _seg("000000010000001A00000020"), _seg("000000010000001A00000010")
    plan = plan_prune_wal([early, mid], kept_start_segments=[newer_start, A])
    assert plan["keep"] == [mid] and plan["remove"] == [early]


def test_wal_retention_keeps_later_timelines_and_never_removes_other_files() -> None:
    """A backup replays its own timeline and then follows the history onto later ones, so a
    later-timeline segment is needed even though its name sorts after; an earlier-timeline
    segment is not. History, backup-label and partial files are never removed."""
    later_tl = _seg("000000020000000100000001")
    earlier_tl = _seg("000000010000000100000001")
    hist = _seg("0000000A.history")
    label = _seg("000000010000000100000001.000000A8.backup")
    plan = plan_prune_wal([later_tl, earlier_tl, hist, label],
                          kept_start_segments=["000000020000000000000005"])
    assert earlier_tl in plan["remove"]
    assert later_tl in plan["keep"] and hist in plan["keep"] and label in plan["keep"]


def test_no_kept_backup_at_all_keeps_every_segment() -> None:
    """Nothing to anchor a retention point to: refusing to guess is safer than
    deleting WAL that might still be needed for the very next backup taken."""
    segs = [_seg("000000010000001A00000001"), _seg("000000010000001A00000099")]
    for anchors in (None, []):
        plan = plan_prune_wal(segs, kept_start_segments=anchors)
        assert plan["keep"] == segs and plan["remove"] == []


def test_backup_start_segment_is_read_from_the_tarballs_own_label(tmp_path: Path) -> None:
    from scripts.osiris_prune_ladder import _backup_start_segment

    good = tmp_path / "first.tar.gz"
    _write_base_backup(good, A)
    assert _backup_start_segment(str(good)) == A
    junk = tmp_path / "second.tar.gz"
    junk.write_bytes(b"not a tarball")
    assert _backup_start_segment(str(junk)) is None
    assert _backup_start_segment(str(tmp_path / "missing.tar.gz")) is None


def test_wal_is_anchored_at_the_verified_backup_and_kept_whole_when_it_cannot_be_read(
    tmp_path: Path,
) -> None:
    from scripts.osiris_prune_ladder import _verified_start_segments

    good = tmp_path / "a.tar.gz"
    _write_base_backup(good, A)
    junk = tmp_path / "b.tar.gz"
    junk.write_bytes(b"x")
    assert _verified_start_segments(DumpFile(str(good), NOW)) == [A]
    assert _verified_start_segments(DumpFile(str(junk), NOW)) is None
    assert _verified_start_segments(None) is None  # nothing verified: keep every segment


def test_wal_retention_end_to_end_via_the_cli(tmp_path, capsys) -> None:  # noqa: ANN001
    from scripts.osiris_prune_ladder import main

    backups = tmp_path / "backups"
    vault = tmp_path / "vault"
    basebackups = vault / "basebackups"
    wal_dir = vault / "wal_archive"
    backups.mkdir()
    vault.mkdir()
    basebackups.mkdir()
    wal_dir.mkdir()
    # one recent base backup; its label's start segment is the retention anchor
    base = basebackups / "osiris-basebackup-20260908-000000.tar.gz"
    _write_base_backup(base, "000000010000000000000002")
    from scripts.osiris_prune_ladder import mark_base_backup_verified

    mark_base_backup_verified(base)  # proven restorable: only then is WAL before it removable
    (wal_dir / "000000010000000000000001").write_bytes(b"x" * 10)   # before the start: removable
    (wal_dir / "000000010000000000000002").write_bytes(b"x" * 10)   # the start segment: kept
    (wal_dir / "000000010000000000000003").write_bytes(b"x" * 10)   # after it: kept

    rc = main(["--backups", str(backups), "--vault", str(vault)])

    assert rc == 0
    out = capsys.readouterr().out
    assert "vault/wal_archive" in out
    assert "000000010000000000000001" in out
    assert "000000010000000000000002" not in out
    assert "000000010000000000000003" not in out


# ── legacy transcript tarballs: pre-week-key files that
# _scan_transcript_chains never recognized, removed in full, no ladder ─────────────────

def test_plan_prune_legacy_tarballs_removes_everything_given() -> None:
    files = [DumpFile("a.tar.gz.new", NOW), DumpFile("b.tar.gz", NOW - timedelta(days=40))]
    plan = plan_prune_legacy_tarballs(files)
    assert plan["keep"] == []
    assert set(f.path for f in plan["remove"]) == {"a.tar.gz.new", "b.tar.gz"}


def test_scan_legacy_tarballs_finds_new_and_finished_but_not_week_keyed(tmp_path) -> None:  # noqa: ANN001
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "claude-transcripts-20260809.tar.gz").write_bytes(b"x" * 10)  # finished, pre-week-key
    (vault / "claude-transcripts-20260903.tar.gz.new").write_bytes(b"x" * 10)  # abandoned staging
    (vault / "claude-transcripts-2026-W37-20260908.tar.gz").write_bytes(b"x" * 10)  # week-keyed

    found = {Path(f.path).name for f in _scan_legacy_tarballs(vault)}
    assert found == {"claude-transcripts-20260809.tar.gz",
                     "claude-transcripts-20260903.tar.gz.new"}


def test_cli_reports_and_prunes_legacy_tarballs_as_their_own_population(
    tmp_path, capsys,  # noqa: ANN001
) -> None:
    from scripts.osiris_prune_ladder import main

    backups = tmp_path / "backups"
    vault = tmp_path / "vault"
    backups.mkdir()
    vault.mkdir()
    stray = vault / "claude-transcripts-20260809.tar.gz.new"
    stray.write_bytes(b"x" * 10)

    rc = main(["--backups", str(backups), "--vault", str(vault)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "vault/legacy-transcripts" in out
    assert str(stray) in out
    assert stray.exists(), "no --apply flag given, nothing may be deleted"

    rc = main(["--backups", str(backups), "--vault", str(vault), "--apply"])
    assert rc == 0
    assert not stray.exists(), "--apply must actually remove the legacy tarball"


# --- dormant seat transcripts -------------------------------------

def test_seat_handles_lists_office_subdirectories(tmp_path) -> None:  # noqa: ANN001
    office_root = tmp_path / "seats"
    (office_root / "acorn").mkdir(parents=True)
    (office_root / "birch").mkdir()
    (office_root / "not-a-dir.txt").write_text("stray file, never a handle")
    assert _seat_handles(office_root) == ["acorn", "birch"]


def test_seat_handles_empty_when_office_root_is_missing(tmp_path) -> None:  # noqa: ANN001
    assert _seat_handles(tmp_path / "nonexistent") == []


def test_seat_transcript_dir_agrees_with_the_harness_own_slug_convention(tmp_path) -> None:  # noqa: ANN001
    from src.orchestrator.mounts import _harness_slug

    office_root = tmp_path / "seats"
    projects_root = tmp_path / "projects"
    d = _seat_transcript_dir("acorn", office_root=office_root, projects_root=projects_root)
    expected_cwd = str(office_root / "acorn")
    assert d == projects_root / _harness_slug(expected_cwd)


def test_attribute_sessions_to_seats_groups_only_matching_office_directories(
    tmp_path,  # noqa: ANN001
) -> None:
    office_root = tmp_path / "seats"
    projects_root = tmp_path / "projects"
    (office_root / "acorn").mkdir(parents=True)
    acorn_dir = _seat_transcript_dir(
        "acorn", office_root=office_root, projects_root=projects_root)
    other_project_dir = projects_root / "-some-unrelated-project"
    sessions = [
        SessionRow("sid-a", str(acorn_dir / "a.jsonl"), NOW, NOW),
        SessionRow("sid-b", str(acorn_dir / "b.jsonl"), NOW, NOW),
        SessionRow("sid-c", str(other_project_dir / "c.jsonl"), NOW, NOW),
    ]
    groups = attribute_sessions_to_seats(
        sessions, office_root=office_root, projects_root=projects_root)
    assert set(groups) == {"acorn"}
    assert {s.anchor_sid for s in groups["acorn"]} == {"sid-a", "sid-b"}


def test_attribute_sessions_to_seats_is_empty_with_no_seat_offices_at_all(
    tmp_path,  # noqa: ANN001
) -> None:
    sessions = [SessionRow("sid-a", "/tmp/somewhere/a.jsonl", NOW, NOW)]
    groups = attribute_sessions_to_seats(
        sessions, office_root=tmp_path / "no-such-office-root",
        projects_root=tmp_path / "projects")
    assert groups == {}


def test_cli_manifest_names_dormant_seat_transcripts_with_sizes(
    tmp_path, capsys, monkeypatch,  # noqa: ANN001
) -> None:
    """End to end through the actual CLI entrypoint's --seat-root/--projects-root test
    seams (never the real ~/.osiris/seats or ~/.claude/projects): proves the manifest text
    itself names the seat, the file count, and a real byte-derived size, not just that the
    pure helper functions above compose correctly in isolation."""
    import scripts.osiris_prune_ladder as ladder

    office_root = tmp_path / "seats"
    projects_root = tmp_path / "projects"
    (office_root / "acorn").mkdir(parents=True)
    acorn_dir = _seat_transcript_dir(
        "acorn", office_root=office_root, projects_root=projects_root)
    acorn_dir.mkdir(parents=True)
    (acorn_dir / "session-1.jsonl").write_bytes(b"x" * (2 * 1024 * 1024))  # 2 MB

    async def _fake_collect_session_prune_plan(*, dead_after_days: int = 30):  # noqa: ANN001, ANN202, ARG001
        return [SessionRow("sid-acorn", str(acorn_dir / "session-1.jsonl"), NOW, NOW)]

    async def _fake_mail_manifest(*args, **kwargs) -> int:  # noqa: ANN002, ANN003
        # captures the body the real function would have mailed, without a live DB
        _fake_mail_manifest.body = ladder.build_manifest_body(*args, **kwargs)  # type: ignore[attr-defined]
        return 999

    monkeypatch.setattr(ladder, "_collect_session_prune_plan", _fake_collect_session_prune_plan)
    monkeypatch.setattr(ladder, "mail_manifest", _fake_mail_manifest)

    rc = ladder.main([
        "--backups", str(tmp_path / "backups"), "--vault", str(tmp_path / "vault"),
        "--seat-root", str(office_root), "--projects-root", str(projects_root),
        "--manifest",
    ])
    assert rc == 0
    body = _fake_mail_manifest.body  # type: ignore[attr-defined]
    assert "dormant seat transcripts" in body
    assert "acorn: 1 file(s), 2.0 MB" in body


# --- THE CHAIN: verified base backup + WAL, short dump tail, one day cache -----------------

def _day(days_ago: float) -> datetime:
    return NOW - timedelta(days=days_ago)


def _dump(days_ago: float) -> DumpFile:
    when = _day(days_ago)
    return DumpFile(f"/vault/osiris-{when:%Y%m%d-%H%M%S}.dump", when)


def test_no_verified_base_backup_means_the_old_ladder_and_every_wal_segment(
    tmp_path: Path,
) -> None:
    from scripts.osiris_prune_ladder import plan_prune_dumps

    dumps = [_dump(d) for d in (0.1, 5, 10, 40)]
    assert plan_prune_dumps(dumps, now=NOW, verified=None) == plan_prune(dumps, now=NOW)
    segs = [_seg("000000010000000000000001"), _seg("000000010000000000000002")]
    assert plan_prune_wal(segs, kept_start_segments=None)["remove"] == []


def test_a_marker_is_written_by_the_drill_and_voided_by_a_changed_file(tmp_path: Path) -> None:
    from scripts.osiris_prune_ladder import is_base_backup_verified, mark_base_backup_verified

    base = tmp_path / "osiris-basebackup-20260908-000000.tar.gz"
    _write_base_backup(base, A)
    assert is_base_backup_verified(base) is False
    mark_base_backup_verified(base)
    assert is_base_backup_verified(base) is True
    base.write_bytes(base.read_bytes() + b"more")  # the backup changed after it was verified
    assert is_base_backup_verified(base) is False
    (tmp_path / "other.tar.gz").write_bytes(b"x")  # a marker copied under another name is no proof
    base2 = tmp_path / "other.tar.gz"
    base2_marker = tmp_path / "other.tar.gz.verified"
    base2_marker.write_text(base.with_name(base.name + ".verified").read_text())
    assert is_base_backup_verified(base2) is False


def test_the_vault_keeps_a_day_whole_then_one_dump_a_day_for_three_days_and_no_more() -> None:
    from scripts.osiris_prune_ladder import plan_prune_dumps

    base = DumpFile("/vault/basebackups/b.tar.gz", _day(1.5))
    dumps = [_dump(d) for d in (0.1, 0.4, 0.9, 1.2, 1.4, 2.2, 2.4, 3.5, 20)]
    plan = plan_prune_dumps(dumps, now=NOW, verified=base)
    kept_ages = {round((NOW - f.when).total_seconds() / 86400, 1) for f in plan["keep"]}
    assert {0.1, 0.4, 0.9} <= kept_ages                 # the last day: all of it
    assert 3.5 not in kept_ages and 20 not in kept_ages  # beyond three days: gone
    assert len([f for f in plan["keep"] if 1 < (NOW - f.when).days + 1 <= 3]) <= 3


def test_a_dump_newer_than_the_verified_base_backup_is_never_removed() -> None:
    from scripts.osiris_prune_ladder import plan_prune_dumps

    base = DumpFile("/vault/basebackups/b.tar.gz", _day(10))
    young_but_old_enough_to_thin = _dump(5)  # older than three days, newer than the base
    plan = plan_prune_dumps([young_but_old_enough_to_thin], now=NOW, verified=base)
    assert plan["remove"] == []


def test_base_backups_older_than_the_newest_verified_one_are_removed_with_their_wal_anchor(
    tmp_path: Path,
) -> None:
    from scripts.osiris_prune_ladder import plan_prune_basebackups

    old, verified, newer = (DumpFile(f"/v/b{i}.tar.gz", _day(d)) for i, d in
                            enumerate((20, 7, 1)))
    plan = plan_prune_basebackups([old, verified, newer], now=NOW, verified=verified)
    assert plan["remove"] == [old]
    assert plan["keep"] == [verified, newer]  # newer one may still be verified later


def test_the_cache_keeps_one_day_and_only_drops_what_the_vault_also_holds() -> None:
    from scripts.osiris_prune_ladder import plan_prune_cache

    fresh, in_vault, only_here = _dump(0.5), _dump(2), _dump(3)
    names = {Path(in_vault.path).name}
    plan = plan_prune_cache([fresh, in_vault, only_here], now=NOW, vault_names=names)
    assert plan["remove"] == [in_vault]
    assert fresh in plan["keep"] and only_here in plan["keep"]


def test_wal_end_to_end_keeps_everything_until_a_base_backup_is_verified(
    tmp_path, capsys,  # noqa: ANN001
) -> None:
    from scripts.osiris_prune_ladder import main

    vault = tmp_path / "vault"
    (vault / "basebackups").mkdir(parents=True)
    (vault / "wal_archive").mkdir()
    (tmp_path / "backups").mkdir()
    _write_base_backup(vault / "basebackups" / "osiris-basebackup-20260908-000000.tar.gz",
                       "000000010000000000000002")
    (vault / "wal_archive" / "000000010000000000000001").write_bytes(b"x")

    assert main(["--backups", str(tmp_path / "backups"), "--vault", str(vault)]) == 0
    assert "000000010000000000000001" not in capsys.readouterr().out


def test_apply_removes_a_pruned_base_backups_marker_with_it(tmp_path, capsys) -> None:  # noqa: ANN001
    from scripts.osiris_prune_ladder import main, mark_base_backup_verified, verified_marker_path

    vault = tmp_path / "vault"
    bases = vault / "basebackups"
    bases.mkdir(parents=True)
    (tmp_path / "backups").mkdir()
    old = bases / "osiris-basebackup-20260101-000000.tar.gz"
    new = bases / "osiris-basebackup-20260908-000000.tar.gz"
    for f in (old, new):
        _write_base_backup(f, "000000010000000000000002")
        mark_base_backup_verified(f)

    assert main(["--backups", str(tmp_path / "backups"), "--vault", str(vault), "--apply"]) == 0

    assert not old.exists() and not verified_marker_path(old).exists()
    assert new.exists() and verified_marker_path(new).exists()


def test_compressed_segments_are_pruned_by_position_like_raw_ones() -> None:
    """The archive step stores segments as `<name>.zst`; the same name is the same segment,
    so retention must remove an old compressed one and keep a needed one, and must never
    treat the suffix as an unrecognised file to keep forever."""
    old, at, later = (_seg("000000010000001A00000016.zst"), _seg(A + ".zst"),
                      _seg("000000010000001A00000018.zst"))
    raw_old = _seg("000000010000001A00000015")
    plan = plan_prune_wal([raw_old, old, at, later], kept_start_segments=[A])
    assert plan["remove"] == [raw_old, old]
    assert plan["keep"] == [at, later]

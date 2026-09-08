"""The retention ladder (vault lane, ruling 39384a87/c53a5fc0 item 2) — pure logic only,
dry-run report and I/O are the CLI's own thin shell, exercised separately."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from scripts.osiris_prune_ladder import (
    DumpFile,
    TranscriptChain,
    plan_prune,
    plan_prune_transcript_chains,
)

NOW = datetime(2026, 9, 8, 12, 0, tzinfo=UTC)


def _f(path: str, hours_ago: float) -> DumpFile:
    return DumpFile(path, NOW - timedelta(hours=hours_ago))


def test_everything_inside_48h_survives_whole() -> None:
    files = [_f(f"h{i}", i * 6) for i in range(8)]  # 0h, 6h, 12h, ..., 42h — all < 48h
    plan = plan_prune(files, now=NOW)
    assert {f.path for f in plan["keep"]} == {f.path for f in files}
    assert plan["remove"] == []


def test_daily_tier_keeps_one_per_calendar_day() -> None:
    # three dumps on the SAME calendar day (2026-09-05), all past the 48h hot window
    # (NOW is 2026-09-08 12:00) and inside the 30d daily window — only the newest of the
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

    # a THIRD dump the following ISO week (2026-W03) is a different bucket — survives
    # independently
    next_week = DumpFile("next_week", datetime(2026, 1, 12, tzinfo=UTC))
    plan2 = plan_prune([early, later, next_week], now=NOW)
    assert {f.path for f in plan2["keep"]} == {"later", "next_week"}


def test_monthly_tier_keeps_one_per_calendar_month_forever() -> None:
    # two dumps well over a year apart, different months — both survive (one per month,
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
        # daily (48h-30d): same calendar day 2026-09-05 — newest of the pair survives
        DumpFile("day_old", datetime(2026, 9, 5, 6, tzinfo=UTC)),
        DumpFile("day_old_dup", datetime(2026, 9, 5, 20, tzinfo=UTC)),
        # weekly (30d-1y): same ISO week 2026-W23 — newest of the pair survives
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
    """The operator's own word (ruling 39384a87/c53a5fc0): 'do not delete anything until
    I relay the operator's word on that list' — the default invocation must be a pure
    report, whatever it finds."""
    from scripts.osiris_prune_ladder import main

    backups = tmp_path / "backups"
    vault = tmp_path / "vault"
    backups.mkdir()
    vault.mkdir()
    # TWO dumps in the same ancient calendar month — a lone old dump is legitimately kept
    # forever (one-per-month survivor); pruning only happens when a bucket has a
    # DUPLICATE to thin, so the elder of this pair is what the plan would remove.
    elder = backups / "osiris-20200101-000000.dump"
    elder.write_bytes(b"x" * 100)
    younger = backups / "osiris-20200115-000000.dump"
    younger.write_bytes(b"x" * 100)

    rc = main(["--backups", str(backups), "--vault", str(vault)])

    assert rc == 0
    assert elder.exists() and younger.exists(), "no --apply flag given — nothing may be deleted"
    out = capsys.readouterr().out
    assert "DRY RUN ONLY" in out
    assert "REMOVE  " + str(elder) in out
    assert "REMOVE  " + str(younger) not in out


def test_cli_with_apply_deletes_exactly_the_planned_removals(tmp_path, capsys) -> None:
    from scripts.osiris_prune_ladder import main

    backups = tmp_path / "backups"
    vault = tmp_path / "vault"
    backups.mkdir()
    vault.mkdir()
    elder = backups / "osiris-20200101-000000.dump"
    elder.write_bytes(b"x" * 100)
    younger = backups / "osiris-20200115-000000.dump"
    younger.write_bytes(b"x" * 100)
    # the CLI's own `main()` uses the REAL wall clock (never the test module's fixed
    # NOW) — name this one after it directly so it lands in the hot (< 48h) window
    real_now = datetime.now(UTC)
    fresh = backups / f"osiris-{real_now.strftime('%Y%m%d-%H%M%S')}.dump"
    fresh.write_bytes(b"y" * 100)

    rc = main(["--backups", str(backups), "--vault", str(vault), "--apply"])

    assert rc == 0
    assert not elder.exists(), "the planned removal must actually be gone under --apply"
    assert younger.exists(), "the surviving bucket member must never be touched"
    assert fresh.exists(), "a hot-window survivor must never be touched"


# ── transcript CHAIN pruning (Thoth msg 8211): whole weekly chains, never a tarball out
# of the middle of one ─────────────────────────────────────────────────────────────────

def _chain(week_key: str, week_start: datetime, n_files: int = 3) -> TranscriptChain:
    return TranscriptChain(week_key, week_start,
                           [f"{week_key}-{i}.tar.gz" for i in range(n_files)])


def test_the_four_most_recent_chains_survive_whole() -> None:
    chains = [_chain(f"2026-W{30 + i:02d}", datetime(2026, 7, 20, tzinfo=UTC) + timedelta(weeks=i))
             for i in range(6)]  # W30..W35, oldest first
    plan = plan_prune_transcript_chains(chains, keep_recent=4)
    kept_keys = {c.week_key for c in plan["keep"]}
    # the 4 NEWEST (W32-W35) survive whole regardless of month bucketing
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
    """The whole-chain guarantee: removing a chain must never leave a partial one —
    every file that chain owns comes back in the removal, together. A LONE old chain
    survives forever (the one-per-month rule) — two chains in the SAME month are needed
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
    # 6 weekly chains, all in the same ancient month (Jan 2024) — every one of them falls
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

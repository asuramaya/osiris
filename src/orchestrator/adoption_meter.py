"""THE #189 ADOPTION METER — an instrument, not a fix (Thoth msg 5825, ruling d68c57e5,
obligation 8d510875). The risk this answers is stated in the ruling itself: "a gate that
exists in code and refuses nothing in production is the same artifact as a confession
nobody acts on." The acceptance test used to be a `triage(mode='census')` call somebody had
to REMEMBER to run and compare by hand against a baseline held in a decision's own prose —
exactly the shape that decays (5169686b diagnosed this identically on 2026-08-02 and the
population it named grew ~1,800 in the 24 days the record sat unbuilt).

THE HEADLINE METRIC WAS REPLACED (Thoth msg 5866, superseding ruling d68c57e5's own
"Decision median_links moving off 1"): Khnum measured (decision b71e1e0dcadf) that a
Thread structurally CANNOT declare a forward relational link at its own birth — `open_thread`
has no grounds=/relates_to= parameter, so a Thread's eventual connectivity is entirely a
function of whether a LATER Decision's `resolves=` cites it back. A Reference is the same
shape (its only route to connectivity is being named in a later Decision's `grounds=`).
Even a well-connected Decision gets most of its own links from LATER objects citing it
(supersedes/rediscovers/confirms/refutes), not from what its own writer declared. A
population-wide snapshot median cannot tell "born yesterday, correctly not yet cited" apart
from "born in June, never cited by anyone" — those are opposite conditions the old metric
reported identically, and at ~20 new Decisions/day the always-young, legitimately-uncited
population dominates the snapshot forever. median_links was never going to move off 1
regardless of whether declaration-at-the-door was actually working.

COHORT-AGED CONNECTIVITY replaces it: objects are bucketed by BIRTH WEEK
(`date_trunc('week', created_at)`), and each cohort's own live link count is measured at
THREE FIXED HISTORICAL AGES — at birth, +7 days, +30 days (`links.created_at <= objects.
created_at + N days`, using the SAME live-link definition `triage`'s own `_TRIAGE_LINK_CTE`
uses). The question stops being "are writes born connected" (they structurally cannot be,
per Khnum's own finding) and becomes "DO WRITES BECOME CONNECTED" — does a cohort's own
median link count climb between birth and day 30, or does it sit flat.

NO BASELINE ROW IS NEEDED HERE, unlike the metric this replaces (deliberately, Thoth's own
item 2 — "check that before building"): `links.created_at` has existed since migration
0001, so every cohort old enough to have reached a checkpoint age is a FIXED HISTORICAL
FACT the instant that window has fully elapsed — re-querying it tomorrow, next week, or a
year from now returns the identical number, because the query only ever counts links that
existed within a bounded historical interval, never "as of right now". This is structurally
different from the old metric (a live snapshot of an ever-growing present, which is exactly
what made a fixed comparison point necessary and hazardous to re-derive). Agreement by
construction, not by a persisted snapshot: the same principle this reign's preflight fix
(commit c0ea155) already applied to `wake_gate_preflight`.

THE HATCH HALF IS UNCHANGED (Imhotep's `unlinked_because`, msg 5828) — Thoth's own framing
was explicit that the METER is correct and the CRITERION it reported against was wrong;
this file's hatch-reading half was never implicated and needed no rebuild.

SCOPE, MATCHING THE OBLIGATION'S OWN EXCLUSIONS: File (#120 proved single-link Files
benign — in_repo only, zero (repo,relpath) collisions) and Type (does not participate in
`links` like an ordinary object, per `triage`'s own contract) are never counted here.

NEVER A GATE: this module makes zero writes anywhere — not to `objects`/`links`/
assertions, and (new, since the old metric's one write is gone) not even to `watermarks`."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import asyncpg

SCOPED_TYPES = ("Decision", "Thread", "Reference")
"""The obligation's own scope (8d510875): every other object type is either explicitly
carved out (File, Type — see this module's own docstring) or was never part of #189's
original diagnosis (5169686b/d68c57e5 named Decision/Thread/Reference specifically)."""

HEADLINE_TYPE = "Decision"
"""Kept as the deploy line's headlined type for continuity with ruling d68c57e5's own
framing (it singled out Decision specifically) — Thread's own cohort curve is arguably the
SHARPER signal going forward (its connectivity is *entirely* inbound-accrued per Khnum's
finding, so its birth->30d delta isolates citation discipline with none of a Decision's own
self-declared-link noise), but swapping the headlined type is a second, unrequested change
this build does not make unilaterally. Thread/Reference cohorts are computed and returned
alongside Decision's in `cohorts` regardless — a future call can re-point the headline
without touching this module's own query."""


async def _cohort_connectivity(pool: asyncpg.Pool) -> dict[str, dict[str, Any]]:
    """Per SCOPED_TYPES: the latest birth-week cohort old enough to have reached its own
    30-day checkpoint, plus that SAME cohort's 7-day figure (always available once 30 days
    have passed) and the PRIOR eligible cohort's own 30-day figure (so a reader watching
    this number move deploy over deploy sees trend without two cohorts crammed into one
    line — see `render_adoption_line`). `status='active'` only, matching the retired
    metric's own scope. Returns `{}` for a type with no 30-day-eligible cohort yet (an
    honest absence, not a zero)."""
    rows = await pool.fetch("""
        WITH scoped AS (
            SELECT id, type, created_at, date_trunc('week', created_at) AS cohort_week
            FROM objects WHERE type = ANY($1) AND status='active'
        ),
        per_object AS (
            SELECT s.id, s.type, s.cohort_week, s.created_at,
                count(*) FILTER (WHERE l.created_at <= s.created_at)
                    AS links_at_birth,
                count(*) FILTER (WHERE l.created_at <= s.created_at + interval '7 days')
                    AS links_at_7d,
                count(*) FILTER (WHERE l.created_at <= s.created_at + interval '30 days')
                    AS links_at_30d
            FROM scoped s
            LEFT JOIN links l
                ON (l.from_id = s.id OR l.to_id = s.id)
                AND (l.valid_until IS NULL OR l.valid_until > now())
            GROUP BY s.id, s.type, s.cohort_week, s.created_at
        )
        SELECT type, cohort_week, count(*) AS n,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY links_at_birth) AS median_at_birth,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY links_at_7d) AS median_at_7d,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY links_at_30d) AS median_at_30d
        FROM per_object
        GROUP BY type, cohort_week
        -- ELIGIBILITY FILTERED HERE, not in Python: a cohort only counts once it has
        -- genuinely reached 30 days of age — a fresher cohort's numbers would keep
        -- changing on every re-query (more links can still land within its own window),
        -- which is exactly the "live snapshot of an ever-growing present" hazard this
        -- metric was built to avoid.
        --
        -- MEASURED FROM THE COHORT'S YOUNGEST MEMBER, NOT ITS WEEK START (corrected
        -- 2026-08-31, thread 1ef3a6e1). This read `cohort_week + interval '30 days'`,
        -- which is the week's OPENING instant — so an object born on the Sunday of that
        -- week was declared 30-day-eligible at 24 DAYS OLD, and its links_at_30d was
        -- still moving. That is precisely the hazard the comment above says the gate
        -- exists to prevent: the gate as written admitted the very cohorts whose numbers
        -- had not finished changing. max(created_at) is the stated invariant exactly —
        -- no member is younger than its own full window — rather than the week-start
        -- approximation, and it needs no separate "+7 for the week to complete" fudge.
        HAVING max(created_at) + interval '30 days' <= now()
        ORDER BY type, cohort_week DESC
    """, list(SCOPED_TYPES))

    by_type: dict[str, list[Any]] = {t: [] for t in SCOPED_TYPES}
    for r in rows:
        by_type[r["type"]].append(r)

    out: dict[str, dict[str, Any]] = {}
    for t, cohort_rows in by_type.items():
        if not cohort_rows:  # no 30-day-eligible cohort yet for this type
            continue
        latest = cohort_rows[0]  # NEWEST eligible cohort (cohort_week DESC)
        entry: dict[str, Any] = {
            "week": latest["cohort_week"].date().isoformat(),
            "n": latest["n"],
            "median_at_birth": float(latest["median_at_birth"] or 0),
            "median_at_7d": float(latest["median_at_7d"] or 0),
            "median_at_30d": float(latest["median_at_30d"] or 0),
        }
        if len(cohort_rows) > 1:
            prev = cohort_rows[1]
            entry["prev_week"] = prev["cohort_week"].date().isoformat()
            entry["prev_median_at_30d"] = float(prev["median_at_30d"] or 0)
            entry["trend_30d_delta"] = (
                entry["median_at_30d"] - entry["prev_median_at_30d"])
        out[t] = entry
    return out


async def _orphan_birth_rate(pool: asyncpg.Pool) -> dict[str, dict[str, Any]]:
    """PREVENTION, not healing (thread eea88e1c, decision 185d5072's own correction to
    #189's headline): does the FRACTION OF EACH BIRTH-WEEK COHORT that never acquired a
    single live link WITHIN 7 DAYS OF ITS OWN BIRTH fall over time — the question
    Sekhmet's lineage-root fallback (main 5001f00) was shipped to move, and the one
    instrument this house had nothing to answer it with (Thoth msg 5936).

    A FIXED HISTORICAL FACT, same discipline as `_cohort_connectivity` above and for the
    identical reason: the link check is bounded to `created_at + 7 days`, so a LATER
    backfill (Lane 0/1, or any future one) can never revise an already-eligible week's
    number. Not a hypothetical risk — re-deriving 185d5072's own literal protocol (an
    UNBOUNDED "still orphan right now" check, no window) live this session found
    Thread's own weekly rate for 07-27..08-24 moved by up to 12 points, because Lane 1's
    boot-alarm backfill happened to land mid-session and retroactively linked ~127
    previously-orphaned Threads spanning those very weeks. An unbounded check is exactly
    the "live snapshot of an ever-growing present" trap this file's own cohort metric
    was already built to avoid (see this module's top docstring); 185d5072's protocol
    predates this file and inherited the trap. The 7-day bound closes it: generous
    against the congenital-settle finding itself (92.8%/92.3% of Decisions/Threads get
    every link they will ever declare within 60 SECONDS of birth), so it costs this
    metric nothing a stricter window would also catch, while making the number
    un-revisable by a later healing pass — the property `_cohort_connectivity` already
    has and an unbounded read does not.

    SCOPE/ELIGIBILITY: same SCOPED_TYPES, `status='active'`, and
    `cohort_week + 7 days <= now()` eligibility gate as `_cohort_connectivity` — a week
    is not reported until every member of it has actually had its own full 7-day window.
    Newest eligible week headlines; the prior eligible week rides along for trend, same
    shape as `_cohort_connectivity`'s own return."""
    rows = await pool.fetch("""
        WITH scoped AS (
            SELECT id, type, created_at, date_trunc('week', created_at) AS cohort_week
            FROM objects WHERE type = ANY($1) AND status='active'
        ),
        flagged AS (
            SELECT s.id, s.type, s.cohort_week, s.created_at,
                NOT EXISTS (
                    SELECT 1 FROM links l
                    WHERE (l.from_id = s.id OR l.to_id = s.id)
                    AND l.created_at <= s.created_at + interval '7 days'
                ) AS born_orphan
            FROM scoped s
        )
        SELECT type, cohort_week, count(*) AS n,
            count(*) FILTER (WHERE born_orphan) AS n_orphan
        FROM flagged
        GROUP BY type, cohort_week
        -- MEASURED FROM THE COHORT'S YOUNGEST MEMBER, NOT ITS WEEK START (corrected
        -- 2026-08-31, thread 1ef3a6e1). This read `cohort_week + interval '7 days'`,
        -- the week's OPENING instant, while this function's own docstring promises "a
        -- week is not reported until every member of it has actually had its own full
        -- 7-day window." Those differ by up to six days: an object born on the Sunday of
        -- a week became eligible at ONE DAY OLD, its born_orphan flag still able to flip.
        -- THE DOCSTRING WAS RIGHT AND THE SQL WAS WRONG. max(created_at) states the
        -- documented invariant exactly.
        HAVING max(created_at) + interval '7 days' <= now()
        ORDER BY type, cohort_week DESC
    """, list(SCOPED_TYPES))

    by_type: dict[str, list[Any]] = {t: [] for t in SCOPED_TYPES}
    for r in rows:
        by_type[r["type"]].append(r)

    out: dict[str, dict[str, Any]] = {}
    for t, cohort_rows in by_type.items():
        if not cohort_rows:  # no 7-day-eligible cohort yet for this type
            continue
        latest = cohort_rows[0]
        rate = (latest["n_orphan"] / latest["n"]) if latest["n"] else 0.0
        entry: dict[str, Any] = {
            "week": latest["cohort_week"].date().isoformat(),
            "n": latest["n"],
            "n_orphan": latest["n_orphan"],
            "rate": rate,
        }
        if len(cohort_rows) > 1:
            prev = cohort_rows[1]
            prev_rate = (prev["n_orphan"] / prev["n"]) if prev["n"] else 0.0
            entry["prev_week"] = prev["cohort_week"].date().isoformat()
            entry["prev_rate"] = prev_rate
            entry["trend_delta"] = rate - prev_rate
        out[t] = entry
    return out


_HATCH_CAVEAT = (
    "a 0 here is NOT proof the gate is broken (Imhotep msg 5828): the gate only fires on "
    "types that declare required_link_kinds, and none do yet — Khnum's content pass for "
    "that is separate and has not landed. ALL-TIME CUMULATIVE, NEVER A WINDOW OR A RATE "
    "(Lane C, obligation/thread from Thoth XC msg 6143): unlinked_because is asserted "
    "once per object at write time and never retracted, so total/split only ever grow — "
    "two readings taken weeks apart are not a before/after comparison of the SAME thing, "
    "they are two cumulative totals at different elapsed times. THE SPLIT IS ALSO EXACT-"
    "STRING SENSITIVE: `pending` below matches `_EXTENSION_LINK_PENDING_REASON`'s CURRENT "
    "wording only — that constant's own enumerated param list has changed at least three "
    "times as narrows=/cites= etc. were added (b7fee6c/57c9a0b/6fb6ba5), so an object "
    "written under an OLDER wording no longer exact-matches and silently counts as "
    "standalone_other today, even though nothing about that object changed. Measured live "
    "2026-09-01: 18 rows are genuinely extension-link-pending by MEANING (3 match the "
    "current string exactly; 15 carry one of two older wordings) but only 3 render as "
    "`extension=` in the deploy line — the other 15 render as `standalone=`, inflating it "
    "by the same 15. Do not compare this deploy's split against a prior deploy's, or "
    "against #189's own close-time snapshot, without re-deriving both from by_reason_raw"
)


async def _hatch_counts(pool: asyncpg.Pool) -> dict[str, Any]:
    """Imhotep's `unlinked_because` hatch (msg 5828): an ordinary property assertion, not a
    column — `current_assertions` has existed since migration 0001, so this read is always
    structurally live; an empty result is a REAL zero, never a missing instrument, which is
    why this reports raw counts unconditionally rather than an `available` flag gating on
    schema. THE SPLIT (Thoth's requirement #2 — an extension-link-only write must never be
    summed into the same bucket as a genuinely standalone one) needs Imhotep's own
    `_EXTENSION_LINK_PENDING_REASON` constant, imported live from `src.mcp_server` at
    census time (never copied — he named it as still liable to move before he commits).
    The `try` is NOT dead now that the gate has merged (main 45b42cd): it is the guard
    for a build where that constant has been renamed or removed out from under this
    reader, and `split=None` then degrades to raw per-value counts rather than
    silently reporting a zero that would read as "the gate refuses nothing".

    NEITHER COUNT IS A RATE OR A WINDOW (Lane C, Thoth XC msg 6143 — see `_HATCH_CAVEAT`
    for the full finding): `current_assertions` here is every `unlinked_because` ever
    written, all-time, monotonically growing, and the extension/standalone split is
    additionally sensitive to `_EXTENSION_LINK_PENDING_REASON`'s own CURRENT exact text —
    an older-worded write silently reclassifies as standalone the moment that constant's
    wording moves, with no change to the underlying object. This function's own numbers
    are correct census, unchanged by this finding; only the LABEL was wrong. Fixing the
    exact-match itself (e.g. a stable sub-string/prefix match instead of full equality) is
    a deliberate, separate build, not folded in here."""
    rows = await pool.fetch(
        "SELECT (a.value #>> '{}') AS reason, count(*) AS n FROM current_assertions a "
        "WHERE a.name = 'unlinked_because' GROUP BY reason ORDER BY n DESC")
    by_reason_raw = {r["reason"]: r["n"] for r in rows}
    total = sum(by_reason_raw.values())

    try:
        from src.mcp_server import _EXTENSION_LINK_PENDING_REASON
    except (ImportError, AttributeError):
        split = None
    else:
        pending = by_reason_raw.get(_EXTENSION_LINK_PENDING_REASON, 0)
        split = {"extension_link_pending": pending, "standalone_other": total - pending}

    return {
        "total": total, "by_reason_raw": by_reason_raw, "split": split,
        "note": _HATCH_CAVEAT,
    }


async def adoption_meter(pool: asyncpg.Pool) -> dict[str, Any]:
    """THE WHOLE INSTRUMENT: cohort-aged connectivity per SCOPED_TYPES, orphan BIRTH rate
    per SCOPED_TYPES (thread eea88e1c — prevention, not healing), plus Imhotep's hatch
    split. Zero writes anywhere — read-only in full, including against `watermarks` (the
    retired metric's one write; see this module's own docstring for why cohort
    connectivity needs no persisted baseline)."""
    cohorts = await _cohort_connectivity(pool)
    orphan_birth_rate = await _orphan_birth_rate(pool)
    hatch = await _hatch_counts(pool)
    return {
        "measured_at": datetime.now(UTC).isoformat(),
        "cohorts": cohorts,
        "orphan_birth_rate": orphan_birth_rate,
        "hatch": hatch,
    }


def render_adoption_line(meter: dict[str, Any]) -> str:
    """One terse line, the same discipline `cmd_deploy`'s own other checks already use
    (`chaos replay: ...`, `full suite: green ...`) — printed on EVERY deploy, not only when
    something moved. A conditional "only print on change" line was considered and rejected:
    it recreates exactly the failure this instrument exists to prevent (something that
    quietly stops being seen), and a deploy is not so frequent in this house that one more
    honest, terse line is real noise — the existing lines already accept that trade. Only
    the headlined type's cohort renders here (`HEADLINE_TYPE`); the full per-type detail
    lives in the returned dict for a caller who wants it."""
    headline = meter["cohorts"].get(HEADLINE_TYPE)
    ob_headline = meter.get("orphan_birth_rate", {}).get(HEADLINE_TYPE)
    hatch = meter["hatch"]
    # ALL-TIME CUMULATIVE, NEVER A WINDOW (Lane C, Thoth XC msg 6143 — a number printed on
    # every deploy with no window WILL be misread as a per-deploy or per-period figure;
    # see `_HATCH_CAVEAT` for the full finding, including the exact-string classification
    # fragility this label does not attempt to fix).
    if hatch["split"] is not None:
        hatch_str = (f"extension={hatch['split']['extension_link_pending']} "
                     f"standalone={hatch['split']['standalone_other']} (all-time total, "
                     f"not a window — do not diff against a prior deploy's line)")
    else:
        hatch_str = f"{hatch['total']} total (unsplit — reason constant not on this build)"
    if headline is None:
        cohort_str = "no 30-day-aged cohort yet"
    else:
        cohort_str = (
            f"cohort {headline['week']} (n={headline['n']}): "
            f"birth={headline['median_at_birth']:.1f} -> "
            f"30d={headline['median_at_30d']:.1f}"
            f" (Δ{headline['median_at_30d'] - headline['median_at_birth']:+.1f})"
        )
    if ob_headline is None:
        ob_str = "no 7d-aged cohort yet"
    else:
        ob_str = f"{ob_headline['week']} {ob_headline['rate'] * 100:.1f}%"
        if "trend_delta" in ob_headline:
            ob_str += f" (Δ{ob_headline['trend_delta'] * 100:+.1f})"
    return (
        f"adoption189: {HEADLINE_TYPE} {cohort_str} | "
        f"orphan_birth: {HEADLINE_TYPE} {ob_str} | unlinked_because: {hatch_str}"
    )

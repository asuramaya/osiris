"""PHANTOM/FOLD BACKLOG REAP — dispatch #185 item (e), ruling 696d302c ("this should not
be babysat by thoth or workers, it should manage itself by the system osiris or each
worker"), Thoth DM 5464. THE PATTERN THIS MIRRORS: fleet_reconcile.py (task #59, the
reaper's own precedent) — propose-then-separately-gated-act, dry_run/execute two-phase,
a DARK/BLIND/OVER_CAP/ACTS state machine, ZERO FALSE DROPS as the bar.

THREE POPULATIONS graph_lint already surfaces as TESTIMONY ONLY, never before swept on a
schedule, never before actionable:

  false-mint-live      — a generation carrying false_mint=true with a live mount (the
                          halcyon shape, obligation 6b1efacb). reinstate_generation is
                          the repair door built for exactly this, at 3014910.
  duplicate-works-in    — a live agent carrying >1 simultaneously-live works_in edges.
                          invalidate_works_in is the repair, but compositions.py's own
                          docstring is explicit: "it never judges which edge is the
                          stale one" — a mind, or an unambiguous rule, must name it.
  parallel-lives        — a generation minted while a predecessor's own door held a
                          live pulse. compositions.py's own docstring: "the seam may
                          still have been real (verify), never auto-fold."

PLUS a fourth, DISCOVERY-ONLY population this sweep makes visible on a schedule for the
first time: half-healed phantom threads (agents.py's own `_report_half_healed_phantom`,
source=`_HALF_HEAL_SRC`) — obligation Threads that already self-file at mint time but had
no counted, scheduled surface. The code's own standing law is explicit and NOT relitigated
here ("NEVER auto-complete... a human must judge"): this sweep only counts and surfaces
them, never acts.

TWO AUTO-ACT BUCKETS, both narrow, both reusing an already-built repair verb — never a
third notion of "safe":

  reinstate_false_mint_live            — false-mint-live AND registry_census (the SAME
                                          harness+/proc-confirmed occupancy authority
                                          is_occupied_by_a_live_body wraps) independently
                                          confirms it. HIGH CONFIDENCE: two signals, one
                                          coarse (agent_mounts freshness) and one hard
                                          (OS-verified), must agree before this sweep
                                          treats a graph-live claim as proof — the exact
                                          "graph-live alone is not proof" discipline
                                          fleet_reconcile's own ghost_gap check already
                                          established the hard way.
  drop_dead_project_duplicate_works_in — a duplicate-works-in agent where EXACTLY ONE of
                                          its live targets has SoftwareProject status NOT
                                          IN ('active', 'merged') — fleet_reconcile's own
                                          corrected rule (a MERGE is not a death; the
                                          label moved, the project didn't; d1775472).
                                          Unambiguous: the other target(s) are still
                                          alive, this one demonstrably isn't. Zero or more
                                          than one such target never auto-acts — that is
                                          exactly the ambiguity invalidate_works_in itself
                                          refuses to guess at.

Everything else — parallel-lives (always), half-healed-phantom-threads (always,
report-only), and any duplicate-works-in/false-mint-live row that does not clear its own
bucket's bar — lands in `leave_for_human`, the same terminal bucket fleet_reconcile uses
for a row it could not resolve mechanically.

SCHEDULED LEG stays DARK by default (`osiris_phantom_fold_reap_enabled=False`) — flipping
it is a second signature a human gives separately from approving this diff, the same law
every other scheduled writer in this house already follows (fleet_reconcile,
closure_miner, phantom_heal, tree_ingest_alarm)."""
from __future__ import annotations

from typing import Any

import asyncpg

from src.actions.core import Actions
from src.config.settings import Settings, get_settings

# the SAME liveness window every liveness read in the fleet uses (mounts.py's own
# _DOOR_WINDOW_SECS, fleet()'s "live" cutoff, fleet_reconcile.py's own _LIVE_WINDOW_SECS)
_LIVE_WINDOW_SECS = 900

# task #108's own batch-cap discipline, reused rather than re-measured: these two auto-act
# populations are narrower and rarer than fleet_reconcile's own (a false_mint-live
# mis-fire or a dead-project works_in straggler is not a steady drip either), so the same
# order-of-magnitude cap applies until a real tick history says otherwise.
_ACTIONABLE_BUCKETS = ("reinstate_false_mint_live", "drop_dead_project_duplicate_works_in")
_BATCH_CAP = 5

_SANCTIONED_REAP_ACTOR = "cron:phantom_fold_reap_heartbeat"


def _held(
    buckets: dict[str, list[dict[str, Any]]], row: dict[str, Any], rule: str,
) -> None:
    """A row that WOULD auto-act, held back for the stated reason instead — same
    mechanism fleet_reconcile.py's own `_held` uses, reused verbatim rather than
    reinvented: ONE hold implementation, several reasons, never a second copy to drift."""
    row["bucket"] = "leave_for_human"
    row["rule"] = rule
    buckets["leave_for_human"].append(row)


async def _false_mint_live_candidates(
    pool: asyncpg.Pool, *, agents_json: Any = None, read_exe: Any = None,
    read_cwd: Any = None, live_secs: int = _LIVE_WINDOW_SECS,
) -> tuple[list[dict[str, Any]], bool]:
    """graph_lint's own `false-mint-live` query (compositions.py), reused verbatim —
    false_mint=true with an agent_mounts row fresh within `live_secs`. Cross-checked
    against registry_census (the SAME harness+/proc-confirmed authority
    is_occupied_by_a_live_body wraps) — never trusted alone, exactly graph_lint's own
    docstring warning: "a real false positive here is still worth a human's glance, never
    worth silently trusting agent_mounts alone for a repair decision." Returns
    (rows, blind) — `blind=True` means the OS census itself could not run; the caller owns
    refusing to auto-act on anything while blind, never silently reading it as 'none
    confirmed'."""
    from src.orchestrator.mounts import registry_census

    rows = await pool.fetch(
        "SELECT o.canonical AS agent FROM objects o "
        "WHERE o.type='Agent' "
        "AND (SELECT value #>> '{}' FROM current_assertions "
        "     WHERE object_id=o.id AND name='false_mint' "
        "     ORDER BY confidence DESC, observed_at DESC LIMIT 1) = 'true' "
        "AND EXISTS (SELECT 1 FROM agent_mounts m WHERE m.agent_id=o.canonical "
        "  AND m.last_seen > now() - make_interval(secs => $1))",
        float(live_secs))
    census = await registry_census(
        pool, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
    if census.get("blind"):
        # STILL RETURN THE ROWS — a blind census means "cannot corroborate", not "no
        # candidates exist"; the caller (phantom_fold_dry_run) must be able to HOLD each
        # one in leave_for_human by name, never silently drop it from every bucket.
        return [{"agent_id": r["agent"], "harness_confirmed": False,
                 "bucket_eligible": False} for r in rows], True
    matched = {m.get("agent_id") for m in census.get("matched", [])}
    return [
        {"agent_id": r["agent"], "harness_confirmed": r["agent"] in matched,
         "bucket_eligible": r["agent"] in matched}
        for r in rows
    ], False


async def _duplicate_works_in_candidates(
    pool: asyncpg.Pool, *, live_secs: int = _LIVE_WINDOW_SECS,
) -> list[dict[str, Any]]:
    """graph_lint's own `duplicate-works-in` query (compositions.py), reused verbatim —
    scoped to currently-LIVE agents (the SAME liveness window every check here uses), plus
    each live target's OWN SoftwareProject status so a candidate whose live targets are
    unambiguous (exactly one non-active/non-merged) can be auto-cleaned. `status NOT IN
    ('active', 'merged')`, DELIBERATELY not just `<> 'active'` — fleet_reconcile.py's own
    corrected rule, found live against a real false drop (Werner/repo:bytebye): a project
    RENAMED via merge is not a death, the label moved, the project didn't."""
    rows = await pool.fetch(
        "WITH live_agents AS (SELECT DISTINCT agent_id FROM agent_mounts "
        "  WHERE last_seen > now() - make_interval(secs => $1)) "
        "SELECT o.canonical AS agent, p.canonical AS project, p.status AS project_status "
        "FROM links l "
        "JOIN objects o ON o.id=l.from_id AND o.type='Agent' AND o.status='active' "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
        "JOIN live_agents la ON la.agent_id=o.canonical "
        "WHERE l.type='works_in' AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY o.canonical, p.canonical", float(live_secs))
    by_agent: dict[str, list[tuple[str, str]]] = {}
    for r in rows:
        by_agent.setdefault(str(r["agent"]), []).append(
            (str(r["project"]), str(r["project_status"])))
    out: list[dict[str, Any]] = []
    for agent, targets in by_agent.items():
        if len(targets) <= 1:
            continue
        dead = [p for p, st in targets if st not in ("active", "merged")]
        out.append({
            "agent_id": agent, "projects": [p for p, _ in targets],
            "dead_targets": dead,
            "bucket_eligible": len(dead) == 1,
            "stale_project": dead[0] if len(dead) == 1 else None,
        })
    return out


async def _parallel_lives_rows(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """graph_lint's own `parallel-lives` query (compositions.py), reused verbatim. NEVER
    auto-acted on, by that check's own docstring ("the seam may still have been real
    (verify), never auto-fold") — this sweep's contribution here is COUNTING and
    SURFACING on a schedule, nothing more; there is no repair verb this bucket could ever
    hand a rule to."""
    rows = await pool.fetch(
        "SELECT o.canonical AS heir, "
        "  max(p.value #>> '{}') FILTER (WHERE p.name='parallel_pulse_door') AS door, "
        "  max(p.value #>> '{}') FILTER (WHERE p.name='predecessor_last_seen') AS pulse_at, "
        "  max(p.value #>> '{}') FILTER (WHERE p.name='minted_because') AS because "
        "FROM objects o JOIN current_assertions p ON p.object_id=o.id "
        "WHERE o.type='Agent' AND o.status='active' "
        "AND p.name IN ('parallel_pulse_door','predecessor_last_seen','minted_because') "
        "GROUP BY o.canonical "
        "HAVING max(p.value #>> '{}') FILTER (WHERE p.name='parallel_pulse_door') "
        "  IS NOT NULL")
    return [
        {"agent_id": r["heir"], "door": r["door"], "pulse_at": r["pulse_at"],
         "because": r["because"], "bucket": "parallel_lives",
         "rule": "parallel-lives — mint-time evidence a predecessor's own door held a "
                 "live pulse; graph_lint's own law: verify by hand, never auto-fold"}
        for r in rows
    ]


async def _half_healed_phantom_threads(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Obligation Threads `_report_half_healed_phantom` (agents.py) opens at
    `source='half-heal-detect'` — already self-filed at mint time, but with no counted,
    scheduled surface before this sweep. NEVER acted on: that function's own docstring is
    the standing law this sweep does not relitigate ("Do not auto-complete: a real
    successor may already be live past {phantom}. A human must judge.")."""
    from src.orchestrator.agents import _HALF_HEAL_SRC

    rows = await pool.fetch(
        "SELECT o.id, o.created_at, "
        " (SELECT value #>> '{}' FROM current_assertions WHERE object_id=o.id "
        "   AND name='status' ORDER BY confidence DESC, observed_at DESC LIMIT 1) AS st, "
        " (SELECT value #>> '{}' FROM current_assertions WHERE object_id=o.id "
        "   AND name='summary' ORDER BY confidence DESC, observed_at DESC LIMIT 1) "
        "   AS summary "
        "FROM objects o WHERE o.type='Thread' AND o.status='active' "
        "AND EXISTS (SELECT 1 FROM current_assertions ca WHERE ca.object_id=o.id "
        "  AND ca.name='status' AND ca.source_id=$1)", _HALF_HEAL_SRC)
    return [
        {"thread_id": str(r["id"]), "created_at": r["created_at"].isoformat(),
         "summary": r["summary"], "bucket": "half_healed_phantom",
         "rule": "half-healed phantom (source=half-heal-detect) — the code's own "
                 "standing law: never auto-complete, a human must judge whether a real "
                 "successor now exists past the phantom"}
        for r in rows if r["st"] == "open"
    ]


async def phantom_fold_dry_run(
    pool: asyncpg.Pool, *, agents_json: Any = None, read_exe: Any = None,
    read_cwd: Any = None,
) -> dict[str, Any]:
    """THE REPORT — buckets every row across all four populations and names the rule that
    placed it, exactly mirroring fleet_reconcile.reconcile_dry_run's own shape. Writes
    NOTHING. `agents_json`/`read_exe`/`read_cwd` are registry_census's own injection seam,
    passed straight through — tests drive this with fakes, production defaults to the
    real OS census.

    A BLIND CENSUS REFUSES TO BUCKET ANYTHING INTO reinstate_false_mint_live (the ONE
    auto-act bucket that depends on it) — held in `leave_for_human` instead, named as
    blind-held, `census_blind: true` at the top level. `drop_dead_project_duplicate_
    works_in` does not depend on the OS census at all (it is a pure graph read on project
    status) and is unaffected by blindness.

    AN OVER-CAP TICK holds BOTH auto-act buckets to `leave_for_human` (task #108's own
    discipline) — an anomalous batch is the signature of a bug upstream, not a thing to
    bulk-act on unwitnessed. `over_cap: true` names it."""
    fm_rows, blind = await _false_mint_live_candidates(
        pool, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
    dup_rows = await _duplicate_works_in_candidates(pool)
    buckets: dict[str, list[dict[str, Any]]] = {
        "reinstate_false_mint_live": [], "drop_dead_project_duplicate_works_in": [],
        "parallel_lives": [], "half_healed_phantom": [], "leave_for_human": [],
    }

    for r in fm_rows:
        row: dict[str, Any] = {"agent_id": r["agent_id"],
                               "harness_confirmed": r["harness_confirmed"]}
        if blind:
            _held(buckets, row, "[would be reinstate_false_mint_live] false_mint=true "
                  "with a graph-live mount — HELD: OS census is blind this tick, cannot "
                  "corroborate before reinstating")
        elif r["bucket_eligible"]:
            row["rule"] = ("false_mint=true AND registry_census independently confirms "
                           "a harness/proc-verified live body (not just agent_mounts "
                           "freshness) — both signals agree")
            row["bucket"] = "reinstate_false_mint_live"
            buckets["reinstate_false_mint_live"].append(row)
        else:
            _held(buckets, row, "false_mint=true with a graph-live mount, but "
                  "registry_census does NOT independently confirm a real harness/proc "
                  "body — a graph-live claim alone is never proof (ghost_gap's own law); "
                  "a human's eyes first")

    for r in dup_rows:
        row = {"agent_id": r["agent_id"], "projects": r["projects"]}
        if r["bucket_eligible"]:
            row["stale_project"] = r["stale_project"]
            row["rule"] = (f"{r['stale_project']!r} is the ONE non-active/non-merged "
                           "live works_in target among this agent's duplicates — "
                           "unambiguous residue (a merge is not a death, only status "
                           "NOT IN ('active','merged') counts)")
            row["bucket"] = "drop_dead_project_duplicate_works_in"
            buckets["drop_dead_project_duplicate_works_in"].append(row)
        else:
            n_dead = len(r["dead_targets"])
            _held(buckets, row,
                  f"{n_dead} of {len(r['projects'])} live works_in targets are "
                  "non-active/non-merged — invalidate_works_in needs exactly ONE "
                  "unambiguous candidate, never guesses among several nor invents one "
                  "when all targets are still alive")

    buckets["parallel_lives"] = await _parallel_lives_rows(pool)
    buckets["half_healed_phantom"] = await _half_healed_phantom_threads(pool)

    actionable_total = sum(len(buckets[b]) for b in _ACTIONABLE_BUCKETS)
    over_cap = actionable_total > _BATCH_CAP
    if over_cap:
        for name in _ACTIONABLE_BUCKETS:
            rows, buckets[name] = buckets[name], []
            for row in rows:
                _held(buckets, row, f"[would be {row['bucket']}] {row['rule']} — HELD: "
                      f"tick batch size {actionable_total} exceeds cap {_BATCH_CAP}, one "
                      "human look before bulk action")

    counts = {k: len(v) for k, v in buckets.items()}
    return {
        "buckets": buckets, "counts": counts, "total": sum(counts.values()),
        "census_blind": blind, "over_cap": over_cap,
        "note": ("REPORT ONLY — nothing reinstated, invalidated, folded, or resolved. "
                 "Every row above names its own bucket and the rule that put it there." +
                 (" OS CENSUS WAS BLIND THIS TICK — reinstate_false_mint_live rows are "
                  "held in leave_for_human instead; re-run once the census can see."
                  if blind else "") +
                 (f" BATCH CAP EXCEEDED THIS TICK ({actionable_total} > {_BATCH_CAP}) — "
                  "every row that would have auto-acted is held in leave_for_human "
                  "instead; an anomalous batch needs a human's eyes before bulk action."
                  if over_cap else "")),
    }


async def phantom_fold_execute(
    actions: Actions, *, actor: str, execute: bool = False, agents_json: Any = None,
    read_exe: Any = None, read_cwd: Any = None,
) -> dict[str, Any]:
    """THE ACTING HALF. DRY RUN IS THE DEFAULT (`execute=False`) — returns the exact plan
    without writing anything. Re-reads the tray itself via `phantom_fold_dry_run` (never
    trusts a caller-supplied stale report) — the plan and the act must see the same
    instant.

    reinstate_false_mint_live: `reinstate_generation` per agent — the exact repair door
    built for this shape, obligation 6b1efacb.
    drop_dead_project_duplicate_works_in: `invalidate_works_in` per agent, targeting the
    ONE unambiguous stale project the dry run already named.
    parallel_lives / half_healed_phantom / leave_for_human: NEVER acted on, by
    construction — absent from every write this function performs.

    A single row's reinstate/invalidate failing (a race, an already-healthy row) is
    caught and reported inline rather than aborting the batch — ZERO FALSE DROPS means
    every row that WAS acted on must be a true positive, not that one failure may
    silently swallow the rest of a correct plan.

    POST-ACT VERIFICATION (`execute=True` only): re-reads the tray a second time after
    acting and reports before/after counts.

    THE DESK RECEIPT, same shape as fleet_reconcile's own: a real execute (reinstated or
    invalidated nonzero) fires a durable operator-desk `fyi` brief with exact counts and
    row ids. An OVER-CAP tick fires its own `decision` brief instead — that genuinely
    needs a human call. Both try/excepted — a mail hiccup must never unwind a landed
    action."""
    from src.orchestrator.agents import invalidate_works_in, reinstate_generation
    from src.orchestrator.mailbox import send_message

    report = await phantom_fold_dry_run(
        actions.pool, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
    would_reinstate = [{"agent_id": r["agent_id"]}
                       for r in report["buckets"]["reinstate_false_mint_live"]]
    would_invalidate = [{"agent_id": r["agent_id"], "stale_project": r["stale_project"]}
                        for r in report["buckets"]["drop_dead_project_duplicate_works_in"]]
    plan: dict[str, Any] = {
        "would_reinstate": would_reinstate, "would_invalidate": would_invalidate,
        "left_for_human": len(report["buckets"]["leave_for_human"]),
        "parallel_lives_count": len(report["buckets"]["parallel_lives"]),
        "half_healed_phantom_count": len(report["buckets"]["half_healed_phantom"]),
        "census_blind": report["census_blind"], "over_cap": report["over_cap"],
        "execute": execute,
    }
    if not execute:
        plan["note"] = "PLAN ONLY — call with execute=True to write. Nothing touched."
        return plan

    if report["over_cap"]:
        body = (f"PHANTOM-FOLD-REAP OVER CAP — a tick's actionable rows "
                f"(reinstate_false_mint_live+drop_dead_project_duplicate_works_in) "
                f"totaled more than the cap of {_BATCH_CAP}; the whole tick was held in "
                f"leave_for_human rather than bulk-acting on an unreviewed anomaly. "
                f"counts: {report['counts']}. actor={actor!r}.")
        try:
            sent = await send_message(actions.pool, from_agent=actor, from_project="osiris",
                                      to_project="operator", body=body, desk_kind="decision")
            plan["desk_brief_id"] = sent.get("id")
        except Exception:  # noqa: BLE001 — a mail hiccup must not mask the hold that landed
            plan["desk_brief_id"] = None
        plan.update({
            "reinstated": [], "invalidated": [],
            "before_counts": report["counts"], "after_counts": report["counts"],
            "note": ("OVER CAP — nothing acted this tick, everything held in "
                     "leave_for_human, an operator decision brief was sent."),
        })
        return plan

    reinstated: list[dict[str, Any]] = []
    invalidated: list[dict[str, Any]] = []
    for item in would_reinstate:
        try:
            out = await reinstate_generation(
                actions, item["agent_id"],
                because="phantom_fold_reap scheduled sweep — false_mint=true with a "
                        "registry_census-confirmed live body", actor=actor)
        except Exception as exc:  # one bad row must not abort a correct batch
            out = {"ok": False, "detail": f"{type(exc).__name__}: {exc}"}
        reinstated.append({**item, "result": out})
    for item in would_invalidate:
        try:
            out = await invalidate_works_in(
                actions, item["agent_id"], item["stale_project"],
                because="phantom_fold_reap scheduled sweep — the sole non-active/"
                        "non-merged live works_in target among this agent's duplicates",
                actor=actor)
        except Exception as exc:
            out = {"error": f"{type(exc).__name__}: {exc}"}
        invalidated.append({**item, "result": out})

    after = await phantom_fold_dry_run(
        actions.pool, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
    plan.update({
        "reinstated": reinstated, "invalidated": invalidated,
        "before_counts": report["counts"], "after_counts": after["counts"],
        "note": "EXECUTED — before/after counts prove the acted rows left the tray; "
                "leave_for_human/parallel_lives/half_healed_phantom rows were never "
                "touched.",
    })

    acted_reinstate = sum(1 for r in reinstated if r["result"].get("ok"))
    acted_invalidate = sum(1 for r in invalidated if "error" not in r["result"])
    if acted_reinstate or acted_invalidate:
        body = (f"PHANTOM-FOLD-REAP ACTED — reinstated {acted_reinstate}, invalidated "
                f"{acted_invalidate} (of {len(reinstated)} attempted reinstates, "
                f"{len(invalidated)} attempted invalidations). "
                f"before={report['counts']} after={after['counts']}. actor={actor!r}. "
                f"reinstated rows: {reinstated}. invalidated rows: {invalidated}.")
        try:
            sent = await send_message(actions.pool, from_agent=actor, from_project="osiris",
                                      to_project="operator", body=body, desk_kind="fyi")
            plan["desk_brief_id"] = sent.get("id")
        except Exception:  # noqa: BLE001 — an acted row must not unwind on a mail hiccup
            plan["desk_brief_id"] = None
    return plan


# task #108's consecutive-blind alarm, mirrored: NO COUNTER, NO STATE ROW — open_thread's
# own idempotency-on-summary-text does the dedup, the thread's own age IS the darkness
# duration. Text must stay byte-for-byte stable across calls (the canonical hash derives
# from it) or every tick would mint a new thread instead of finding the one already open.
_BLIND_ALARM_SUMMARY = (
    "PHANTOM-FOLD-REAP'S SCHEDULED TICK WENT CENSUS-BLIND — the OS census failed this "
    "tick, every reinstate_false_mint_live row was held in leave_for_human instead of "
    "trusted; if this persists across many ticks that auto-act path is silently dark. "
    "This thread's own age is the duration — no separate counter exists. Auto-resolved "
    "the next tick the census succeeds again."
)


async def phantom_fold_scheduled_tick(
    actions: Actions, *, settings: Settings | None = None, agents_json: Any = None,
    read_exe: Any = None, read_cwd: Any = None,
) -> dict[str, Any]:
    """THE SCHEDULED LEG's own tick — `arq_worker.phantom_fold_reap_heartbeat` calls this
    unconditionally, the same thin-shim shape every other scheduled writer in this house
    uses (real logic and the flag gate live in the orchestrator tick, not the cron
    wrapper).

    OFF unless `osiris_phantom_fold_reap_enabled` — the kill switch: the code ships
    inert, and flipping this flag is a SECOND signature a human gives separately from
    approving the diff, never a side effect of deploying it. When on, composes
    `phantom_fold_execute(execute=True)` — the exact same acting verb reachable by hand,
    so the schedule and a human's own manual call are provably the same path.

    `state`: DARK (flag off) -> BLIND (OS census failed, reinstate_false_mint_live held)
    -> OVER_CAP (batch too large, both auto-act buckets held) -> ACTS (neither hold
    fired; reinstated/invalidated may be nonzero, or genuinely empty).

    THE CONSECUTIVE-BLIND ALARM: a BLIND tick opens `_BLIND_ALARM_SUMMARY` as a
    `severity='alarm'` obligation Thread — idempotent on the summary text. The next
    NON-blind tick resolves it. Both try/excepted — a graph hiccup must never fail the
    tick's own verdict."""
    st = settings or get_settings()
    if not st.osiris_phantom_fold_reap_enabled:
        return {"enabled": False, "state": "DARK", "reinstated": [], "invalidated": [],
                "note": "the sweep's scheduled leg is dark "
                        "(osiris_phantom_fold_reap_enabled=0)"}
    out = await phantom_fold_execute(
        actions, actor=_SANCTIONED_REAP_ACTOR, execute=True, agents_json=agents_json,
        read_exe=read_exe, read_cwd=read_cwd)

    from src.orchestrator.capture import open_thread, resolve_thread

    if out.get("census_blind"):
        state = "BLIND"
        try:
            await open_thread(actions, summary=_BLIND_ALARM_SUMMARY, kind="obligation",
                              severity="alarm", source="cron:phantom_fold_reap_heartbeat")
        except Exception:  # noqa: BLE001 — a mint hiccup must not fail the tick's verdict
            pass
    else:
        state = "OVER_CAP" if out.get("over_cap") else "ACTS"
        try:
            await resolve_thread(
                actions, _BLIND_ALARM_SUMMARY,
                because="census recovered — this tick's OS body check succeeded again",
                source="cron:phantom_fold_reap_heartbeat")
        except Exception:  # noqa: BLE001 — same discipline: never fail the tick over this
            pass
    return {"enabled": True, "state": state, **out}

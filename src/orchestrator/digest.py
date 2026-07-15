"""The membrane — the operator's window into the autonomous fleet (credence phase 2).

Authority flows DOWN the fleet; results, accountability, and danger flow back UP to the operator.
This is that return path made visible — a stateless rolling-window digest that surfaces, with no
new writes:

  * ROSTER + HEALTH — the agents SEEN IN THE WINDOW, their model/project, and whether each
    identity RESOLVED cleanly (an onboarding that fell back to a best-effort id shows here).
    The window applies here exactly as it does to every other stream: a digest of the last day
    that also shipped every agent who ever lived was not a window, it was the firehose wearing
    one. The COUNTS in `summary` remain over the whole fleet — nothing is undercounted, only
    under-shown, and `roster_scope` says so.
  * ACTIVITY — what the fleet DECIDED / opened in your name since the window opened (agent-authored
    Decisions/Threads only — the miner's DERIVED backfill is excluded; this is deliberate work).
  * DANGER MAP — which agents were model-SWAPPED: the fable harness's silent demotion (ruling
    f2ae6346), i.e. where the harness got nervous.
  * LAUNDERING — credence_props run LIVE over objects several agents co-asserted: a relay that
    carried a fact ABOVE its origin grade (the citogenesis the credence floor exists to catch).
  * DISPUTES — the same live pass, its OTHER half: an ancestor whose value MATERIALLY DIFFERS
    from its subtree's origin is DISAGREEING, not relaying (Tier-2). The value-blind clamp would
    bury it as false laundering; the membrane shows the disagreement instead of flattening it.
  * CONVERSATIONS — the lateral mail threads, reconstructed straight from fleet_messages: who
    talked to whom, how much, how recently, and whether it settled. This is the COMPLIANCE-FREE
    half of the upward lane (membrane #6): an agent that shirks its report-up duty is still
    visible, because the membrane reads the substrate, not the self-reports.
  * OPERATOR INBOX — mail addressed to the operator's desk (send(to='operator')): the count and
    the freshest briefs, so the human sees what the fleet initiated upward.
  * COST — what the inference seam spent in the window (llm_usage): a `spend` head + a `costs`
    stream (per purpose/model/day). Rendered HONESTLY: only the session-miner's extract path is
    metered today, so a `coverage` note names exactly what is (and isn't) counted.
  * BODIES — the meter's OTHER dimension (ruling 7ff54707): core-seconds/RAM-gib-seconds off
    `body_usage`, grouped by provider/exit_cause, beside `costs` in the same report shape — the
    hypervisor/cgroup receipt sitting next to the vendor's dollar. Visibility only; the ceiling's
    dollar gate is untouched.

Read-side by default; the window is either an explicit rolling `since` OR the stored OPERATOR
WATERMARK ("what's new since I last looked"). Reading NEVER advances the watermark — advancing is
a deliberate `mark_seen` act (a peek must not change state). This is where credence_props finally
meets a live surface.
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.config.settings import get_settings
from src.orchestrator.credence import credence_props
from src.orchestrator.mailbox import OPERATOR_ADDR, unread_count
from src.orchestrator.monitor import miner_health, set_cursor

# The operator's digest watermark lives in the generic cursor store (watermarks table, migration
# 0006) — its (key, cursor, updated_at) shape fits exactly: `cursor` holds the last-seen instant
# as ISO text, `updated_at` gives the watermark's own age. One row, keyed here. No new migration.
OPERATOR_WATERMARK_KEY = "operator:digest"
_DEFAULT_WINDOW = timedelta(hours=24)  # the fallback when no watermark has ever been set


async def _roster(actions: Actions) -> list[dict[str, Any]]:
    """Every Agent with its model/project and the two health signals — identity_resolved (did the
    onboarding get a clean id?) and model_swapped (was it silently demoted?)."""
    # each scalar subquery takes the grade-then-recency WINNER (ORDER BY … LIMIT 1) — a bare
    # subquery assumes ≤1 row per (agent, property) and 500s the whole digest the moment any Agent
    # property lands from two sources.
    rows = await actions.pool.fetch(
        "SELECT o.canonical AS agent, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='project' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1"
        " ) AS project, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='source_model' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1"
        " ) AS model, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='identity_resolved' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1"
        " ) AS resolved, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='model_swapped' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1"
        " ) AS swapped, "
        # the LIVE model from the heartbeat (freshest mount by last_seen) — a mid-session swap
        # lands here via the statusline before it is ever re-stamped on the Agent object.
        " (SELECT m.model FROM agent_mounts m WHERE m.agent_id=o.canonical "
        "   ORDER BY m.last_seen DESC LIMIT 1) AS live_model, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1"
        " ) AS handle, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='seat_generation' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS seat_gen, "
        # WHEN was this mind last awake? The window needs it: a digest of 'the last 24 hours'
        # that ships every agent who ever lived is not a window, it is the firehose wearing one.
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='last_active' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS last_active, "
        " (SELECT max(m.last_seen) FROM agent_mounts m WHERE m.agent_id=o.canonical) AS seen "
        "FROM objects o WHERE o.type='Agent' ORDER BY project NULLS FIRST, o.canonical")
    from src.orchestrator.agents import seat_label
    out = []
    for r in rows:
        # a divergence between the last STAMPED model and the LIVE heartbeat model is a swap
        # the graph hasn't recorded yet — the danger map must show it NOW, not at re-mount, or
        # a mount-once agent silently runs the wrong model behind a stale-green roster.
        live_swap = (r["live_model"] and r["model"] and r["live_model"] != r["model"])
        out.append(
            {"agent": r["agent"],
             "seat": seat_label(r["agent"], r["handle"],
                                int(r["seat_gen"]) if r["seat_gen"] else None),
             "project": r["project"], "model": r["model"],
             "resolved": r["resolved"] != "false",  # None/‘true’ → treated resolved
             "swapped": r["swapped"],
             "live_model": r["live_model"] if live_swap else None,
             "live_swap": (f"{r['model']} → {r['live_model']} (unstamped)"
                           if live_swap else None),
             "last_seen": _last_seen(r)})
    return out


def _shipworthy(row: dict[str, Any], since: datetime) -> bool:
    """Does the digest have anything to SAY about this agent?

    Seen inside the window → yes, that's the digest's whole job. Carrying a health flag (an
    identity that never resolved, a live swap the heartbeat caught unstamped) → yes, regardless
    of the window: danger that is CURRENT does not expire because the window moved.

    What this deliberately does NOT do is treat a never-seen agent as newsworthy just for being
    unknowable. 208 of the fleet's 1026 have no liveness stamp of any kind. They are counted in
    the summary (`unseen`, `swapped_unseen`) and reachable through fleet(full=True) — because
    ABSENCE OF EVIDENCE IS NOT EVIDENCE OF ABSENCE, and a thing the graph cannot speak to must
    still be COUNTED, never quietly deleted. But a count is where they belong: it is a census to
    build, not a stream to skim.
    """
    if row["live_swap"] or not row["resolved"]:
        return True
    if row["last_seen"] is None:
        return False  # unknowable — counted in the summary, never shown as if it were news
    return bool(row["last_seen"] >= since)


def _last_seen(row: Any) -> datetime | None:
    """The freshest sign of life — the miner's transcript stamp OR the durable mount registry.

    (Both are stamped only when the agent SPEAKS to Osiris, so this measures chattiness and not
    aliveness — bug 456960e5. It is honest enough to window a digest by; it is NOT honest enough
    to declare a mind dead, and nothing here does.)
    """
    stamps = [s for s in (row["seen"],) if s is not None]
    if row["last_active"]:
        try:
            stamps.append(datetime.fromisoformat(row["last_active"]))
        except ValueError:
            pass
    return max(stamps) if stamps else None


async def _activity(actions: Actions, since: datetime, limit: int = 50) -> list[dict[str, Any]]:
    """What the fleet DELIBERATELY did in your name since the window opened — agent-authored,
    SELF_DECLARED Decisions/Threads. Two exclusions keep this the deliberate-work stream:
      * the miner's DERIVED backfill is now SOURCED to the originating agent too (origin
        attribution), so `agent:%` alone no longer separates it — the GRADE does: only
        `self_declared` summaries count (a mined echo is `derived`);
      * one object co-asserted by several sources carries one summary row PER source (the
        multi-source set), so a co-assertion would list the same activity twice; DISTINCT ON
        the object keeps the highest-grade (then most-recent) row — one line per decision.
    """
    rows = await actions.pool.fetch(
        "SELECT type, agent, summary, at FROM ("
        "  SELECT DISTINCT ON (o.id) o.type AS type, a.source_id AS agent, "
        "         a.value#>>'{}' AS summary, a.observed_at AS at "
        "  FROM objects o JOIN current_assertions a ON a.object_id=o.id AND a.name='summary' "
        "  WHERE o.type IN ('Decision','Thread') AND a.source_id LIKE 'agent:%' "
        "    AND a.evidence_class = 'self_declared' AND a.observed_at >= $1 "
        "  ORDER BY o.id, a.confidence DESC, a.observed_at DESC"
        ") sub ORDER BY at DESC LIMIT $2", since, limit)
    return [
        {"type": r["type"], "agent": r["agent"],
         "summary": (r["summary"] or "")[:200], "at": r["at"].isoformat()}
        for r in rows
    ]


async def _credence_streams(
    actions: Actions, since: datetime
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """ONE credence_props pass over objects >1 agent co-asserted in the window, split into its two
    upward streams — LAUNDERING (a relay carrying a fact above its origin grade) and DISPUTES (an
    ancestor whose value genuinely differs from its subtree's origin). Both empty while
    co-assertion is nascent; this is the wire that makes credence bite as the fleet grows."""
    # candidates = objects an agent touched IN the window whose (object,name) ALSO carries another
    # agent source ALL-TIME. The archetypal laundering re-reports an OLDER origin (origin before the
    # window, relay inside it) — requiring both sources inside the window would miss exactly that.
    oids = [
        r["object_id"] for r in await actions.pool.fetch(
            "SELECT DISTINCT ca.object_id FROM current_assertions ca "
            "WHERE ca.source_id LIKE 'agent:%' AND ca.observed_at >= $1 "
            "  AND EXISTS (SELECT 1 FROM current_assertions c2 "
            "    WHERE c2.object_id = ca.object_id AND c2.name = ca.name "
            "      AND c2.source_id LIKE 'agent:%' AND c2.source_id <> ca.source_id)", since)
    ]
    if not oids:
        return [], []
    result = await credence_props(actions, oids)
    laundering = [
        {"object_id": str(w.object_id), "name": w.name, "value": w.value,
         "laundering_sources": list(w.laundering)}
        for w in result.winners if w.laundering
    ]
    disputes = [
        {"object_id": str(d.object_id), "name": d.name,
         "positions": [{"source": s, "value": v, "grade": g} for s, v, g in d.positions]}
        for d in result.disputes
    ]
    return laundering, disputes


async def _conversations(
    actions: Actions, since: datetime, limit: int = 20
) -> list[dict[str, Any]]:
    """Lateral mail threads active in the window, newest first — thread key, participants,
    volume, last line, and how much is still unsettled. Reconstructed from fleet_messages
    directly: visibility that requires NO agent cooperation."""
    # 'settled' is now per-recipient: a message is settled once ANY recipient has read it.
    rows = await actions.pool.fetch(
        "SELECT COALESCE(thread_id, id) AS thread, count(*) AS msgs, "
        "  array_agg(DISTINCT from_project) FILTER (WHERE from_project IS NOT NULL) AS senders, "
        "  array_agg(DISTINCT to_project) FILTER (WHERE to_project IS NOT NULL) AS recipients, "
        "  max(created_at) AS last_at, "
        "  count(*) FILTER (WHERE to_agent IS NOT NULL) AS dms, "
        "  count(*) FILTER (WHERE NOT settled) AS unsettled "
        "FROM (SELECT fm.*, (fm.read_at IS NOT NULL OR EXISTS(SELECT 1 FROM message_recipients r "
        "        WHERE r.message_id=fm.id AND r.read_at IS NOT NULL)) AS settled "
        "      FROM fleet_messages fm) fm "
        "GROUP BY 1 HAVING max(created_at) >= $1 "
        "ORDER BY max(created_at) DESC LIMIT $2", since, limit)
    if not rows:
        return []
    last = {
        r["thread"]: (r["from_agent"], r["body"]) for r in await actions.pool.fetch(
            "SELECT DISTINCT ON (COALESCE(thread_id, id)) COALESCE(thread_id, id) AS thread, "
            "from_agent, body FROM fleet_messages WHERE COALESCE(thread_id, id) = ANY($1) "
            "ORDER BY COALESCE(thread_id, id), created_at DESC",
            [r["thread"] for r in rows])
    }
    return [
        {"thread": r["thread"],
         "between": sorted(set(r["senders"] or []) | set(r["recipients"] or [])),
         "msgs": r["msgs"], "dms": r["dms"], "unsettled": r["unsettled"],
         "last_at": r["last_at"].isoformat(),
         "last": {"from": last[r["thread"]][0], "body": last[r["thread"]][1][:200]}
         if r["thread"] in last else None}
        for r in rows
    ]


async def _operator_inbox(actions: Actions, *, lease_secs: int) -> dict[str, Any]:
    """The operator's desk, FOLDED: what the fleet initiated upward, one head per thread.

    The supersession tension, resolved as presentation (task #50): an agent that updates its
    prior brief (send(reply_to=<its own brief>) — the threading duty) stacks the thread; the
    desk shows only the NEWEST unread brief per thread, with the older ones counted under it
    (`supersedes`), and `unread` counts ACTIVE HEADS, not raw rows. The SYSTEM folds; only
    the human settles — nothing here leases or acks (a peek), and the superseded briefs stay
    unread underneath until the operator's explicit word clears the thread."""
    # 'unread by the operator' is now a per-recipient fact: no message_recipients row for the
    # 'operator' reader with read_at set (the human hasn't dismissed it).
    unseen = ("NOT EXISTS (SELECT 1 FROM message_recipients r WHERE r.message_id={m}.id "
              "AND r.agent_id=$1 AND r.read_at IS NOT NULL)")
    heads = await actions.pool.fetch(
        "SELECT DISTINCT ON (COALESCE(thread_id, id)) id, from_agent, from_project, "
        " body, created_at, "
        " (SELECT count(*) FROM fleet_messages s WHERE s.id <> m.id AND s.to_project=$1 "
        "   AND s.to_agent IS NULL AND " + unseen.format(m="s")
        + "   AND s.read_at IS NULL "
        + "   AND COALESCE(s.thread_id, s.id) = COALESCE(m.thread_id, m.id)) AS supersedes "
        "FROM fleet_messages m WHERE m.to_project=$1 AND m.to_agent IS NULL AND m.read_at IS NULL "
        "AND " + unseen.format(m="m")
        + " ORDER BY COALESCE(thread_id, id), created_at DESC", OPERATOR_ADDR)
    ordered = sorted(heads, key=lambda r: r["created_at"], reverse=True)  # newest head first
    return {
        "unread": len(ordered),  # active heads — the number that should nag, not the backlog
        "unread_raw": await unread_count(actions.pool, OPERATOR_ADDR,
                                         reader_agent=OPERATOR_ADDR, lease_secs=lease_secs),
        "latest": [
            {"from": m["from_agent"], "from_project": m["from_project"],
             "body": m["body"][:300], "when": m["created_at"].isoformat(),
             "supersedes": int(m["supersedes"])}
            for m in ordered[:5]
        ],
    }


async def _read_watermark(actions: Actions) -> tuple[datetime | None, float | None]:
    """The stored operator-watermark instant and its age in seconds, or (None, None) when none
    has been set (or its stored text is unparseable). One round trip — cursor AND freshness."""
    row = await actions.pool.fetchrow(
        "SELECT cursor, extract(epoch FROM (now() - updated_at)) AS age "
        "FROM watermarks WHERE key=$1", OPERATOR_WATERMARK_KEY)
    if row is None:
        return None, None
    try:
        wm = datetime.fromisoformat(row["cursor"])
    except (ValueError, TypeError):
        return None, None
    return wm, (float(row["age"]) if row["age"] is not None else None)


async def _resolve_since(
    actions: Actions, since: datetime | None
) -> tuple[datetime, dict[str, Any]]:
    """Resolve the window's lower bound plus a `watermark` block describing HOW it was chosen.

    An explicit `since` is an ad-hoc rolling window (mode='explicit') — the watermark is untouched
    and unread. `since=None` is WATERMARK MODE: the lower bound is the stored operator watermark
    ('what's new since I last looked'), or a 24h fallback when none has ever been set."""
    if since is not None:
        return since, {"mode": "explicit", "value": None, "age_secs": None}
    wm, age = await _read_watermark(actions)
    if wm is not None:
        return wm, {"mode": "watermark", "value": wm.isoformat(), "age_secs": age}
    return datetime.now(UTC) - _DEFAULT_WINDOW, {"mode": "watermark", "value": None,
                                                 "age_secs": None}


# What the meter can honestly claim to cover. Only the miner's extract path records usage
# today; wake sessions, interactive tabs, and the document-extract path are UNMETERED (the
# parked cost-levers thread) — a spend figure without this caveat would read as total burn.
_COST_COVERAGE = ("session-extract only — wake sessions, interactive tabs and "
                  "document-extract are unmetered (cost-levers thread)")


async def _costs(actions: Actions, since: datetime) -> dict[str, Any]:
    """The spend stream — llm_usage aggregated over the window, grouped by (purpose, model),
    biggest burner first. Telemetry, not the graph; rendered with its coverage note so the
    number never overclaims (the operator's 'where are the tokens burnt', metered)."""
    rows = await actions.pool.fetch(
        "SELECT purpose, model, count(*) AS calls, "
        " coalesce(sum(input_tokens+output_tokens),0) AS tokens, sum(cost_usd) AS usd "
        "FROM llm_usage WHERE ran_at >= $1 "
        "GROUP BY purpose, model ORDER BY tokens DESC LIMIT 10", since)
    by = [{"purpose": r["purpose"], "model": r["model"], "calls": int(r["calls"]),
           "tokens": int(r["tokens"]),
           "usd": round(float(r["usd"]), 4) if r["usd"] is not None else None}
          for r in rows]
    return {
        "calls": sum(g["calls"] for g in by),
        "tokens": sum(g["tokens"] for g in by),
        "usd": round(sum(g["usd"] for g in by if g["usd"] is not None), 2),
        "by": by,
        "coverage": _COST_COVERAGE,
    }


_BODY_COVERAGE = ("body_usage rows arrive via meter_bodies (src/ingest/wake_cost.py), swept from "
                  "~/.osiris/body-receipts — zero here until a BodyProvider mints receipts and "
                  "a periodic tick sweeps them; visibility only, the daily $ ceiling is untouched")


async def _bodies(actions: Actions, since: datetime) -> dict[str, Any]:
    """The resource-second stream — body_usage aggregated over the window, grouped by
    (provider, exit_cause), heaviest first. Ruling 7ff54707: hypervisor/cgroup receipts are
    recorded UNIFORM across provider tiers, surfaced here beside `costs`' vendor dollars — same
    report shape, the meter's OTHER dimension. 'A hand you cannot cost is a hand you cannot
    govern.' This is visibility only: it invents no enforcement, the ceiling's dollar gate
    (orchestrator/ceiling.py) reads neither this stream nor this table."""
    # totals come from an UNBOUNDED aggregate; `by` is a bounded top-10. exit_cause is an open
    # set (exit:N for any N), so summing the limited groups would assert a prefix as the whole —
    # the digest's oldest sin (see the COST stream's coverage note).
    total = await actions.pool.fetchrow(
        "SELECT count(*) AS n, coalesce(sum(core_seconds),0) AS core_seconds, "
        " coalesce(sum(ram_gib_seconds),0) AS ram_gib_seconds "
        "FROM body_usage WHERE receipt_mtime >= $1", since)
    rows = await actions.pool.fetch(
        "SELECT provider, exit_cause, count(*) AS n, "
        " coalesce(sum(core_seconds),0) AS core_seconds, "
        " coalesce(sum(ram_gib_seconds),0) AS ram_gib_seconds "
        "FROM body_usage WHERE receipt_mtime >= $1 "
        "GROUP BY provider, exit_cause ORDER BY core_seconds DESC LIMIT 10", since)
    by = [{"provider": r["provider"], "exit_cause": r["exit_cause"], "count": int(r["n"]),
           "core_seconds": round(float(r["core_seconds"]), 2),
           "ram_gib_seconds": round(float(r["ram_gib_seconds"]), 2)}
          for r in rows]
    return {
        "count": int(total["n"]) if total else 0,
        "core_seconds": round(float(total["core_seconds"]), 2) if total else 0.0,
        "ram_gib_seconds": round(float(total["ram_gib_seconds"]), 2) if total else 0.0,
        "by": by,
        "coverage": _BODY_COVERAGE,
    }


async def _retrieval(actions: Actions, since: datetime) -> dict[str, Any]:
    """Retrieval telemetry off search_log — the embeddings tripwire made visible: how often
    the fleet searched, how often it found NOTHING, and the queries that missed most. A memory
    system that doesn't measure its own recall failures rots invisibly."""
    row = await actions.pool.fetchrow(
        "SELECT count(*) AS queries, count(*) FILTER (WHERE hits = 0) AS zero_hits, "
        "count(*) FILTER (WHERE relaxed) AS relaxed, "
        "count(*) FILTER (WHERE fuzzy) AS fuzzy, "
        "count(*) FILTER (WHERE semantic) AS semantic "
        "FROM search_log WHERE searched_at >= $1", since)
    missed = await actions.pool.fetch(
        "SELECT query, count(*) AS n FROM search_log "
        "WHERE searched_at >= $1 AND hits = 0 GROUP BY query ORDER BY n DESC LIMIT 3", since)
    # relaxed/fuzzy = searches that only survived on a fallback door; semantic = the
    # embedding door contributed to the final answer. Together they say which doors
    # actually carry recall — the quality telemetry the max-level engine is judged by
    # (ruling a0cfcca1; zero-hits retired as the tripwire in 40e68cb1).
    return {"queries": int(row["queries"]), "zero_hits": int(row["zero_hits"]),
            "relaxed": int(row["relaxed"]), "fuzzy": int(row["fuzzy"]),
            "semantic": int(row["semantic"]),
            "top_missed": [{"query": m["query"], "times": int(m["n"])} for m in missed]}


def _backlog_bytes(root: str, cursors: dict[str, int]) -> tuple[int, int]:
    """Sync (runs via to_thread): total un-mined bytes past the watermark cursors, and how
    many files carry them. Unplanted files (no cursor yet) don't count — forward-only
    sensing will plant them at their end, mining nothing."""
    from src.ingest.sessions import _list_transcripts, _watermark_key

    total = files = 0
    for p in _list_transcripts(Path(root).expanduser()):
        cur = cursors.get(_watermark_key(p))
        if cur is None:
            continue
        try:
            lag = p.stat().st_size - cur
        except OSError:
            continue
        if lag > 0:
            total += lag
            files += 1
    return total, files


async def _miner(actions: Actions, since: datetime) -> dict[str, Any]:
    """Sensing-tick health off miner:ticks telemetry — the instrument the onboarding-day
    outage demanded (decision 3191e0df): a fail-open cron died for a DAY behind a green
    heartbeat. Shows whether ticks FINISH, how long they run, how saturated the LLM budget
    is, and how far behind the fleet's transcripts the miner sits."""
    blob = await miner_health(actions.pool)
    window = [t for t in blob["ticks"] if t.get("at", "") >= since.isoformat()]
    errors = [t for t in window if t.get("error")]
    saturated = [t for t in window if t.get("chunks", 0) >= t.get("budget", 1)]
    ok = [t for t in blob["ticks"] if not t.get("error")]
    out: dict[str, Any] = {
        "configured": bool(get_settings().osiris_sense_sessions),
        "ticks": len(window), "errors": len(errors), "saturated": len(saturated),
        "max_secs": max((t.get("secs", 0.0) for t in window), default=0.0),
        "last_ok": ok[-1]["at"] if ok else None,
        # starts that never confessed a completion — timeout cancels that outran the
        # shield (at most one is legitimately in flight right now)
        "unaccounted": blob["starts"] - blob["completions"],
        "last_errors": [t["error"] for t in errors[-3:]],
    }
    root = get_settings().osiris_sense_sessions
    if root:
        rows = await actions.pool.fetch(
            "SELECT key, cursor FROM watermarks WHERE key LIKE 'session:%'")
        cursors = {r["key"]: int(r["cursor"]) for r in rows if str(r["cursor"]).isdigit()}
        lag, behind = await asyncio.to_thread(_backlog_bytes, root, cursors)
        out["backlog_mb"] = round(lag / 1e6, 1)
        out["files_behind"] = behind
    return out


async def fleet_digest(
    actions: Actions, *, since: datetime | None = None, mark_seen: bool = False,
    lease_secs: int = 900,
) -> dict[str, Any]:
    """The membrane: the upward streams over the window, with a summary head.

    `since` given → an ad-hoc rolling window. `since=None` → WATERMARK MODE: the window opens at
    the stored operator watermark (24h fallback). Reading is a peek — it NEVER advances the
    watermark. `mark_seen=True` is the DELIBERATE act that does: after computing, it stamps the
    watermark to now, so the next glance is 'since this one'. Nothing else here writes to the
    graph (the operator-inbox read is a peek; spend reads telemetry, not the graph)."""
    effective_since, watermark = await _resolve_since(actions, since)
    roster = await _roster(actions)
    activity = await _activity(actions, effective_since)
    laundering, disputes = await _credence_streams(actions, effective_since)
    conversations = await _conversations(actions, effective_since)
    costs = await _costs(actions, effective_since)
    bodies = await _bodies(actions, effective_since)
    retrieval = await _retrieval(actions, effective_since)
    miner = await _miner(actions, effective_since)
    operator_inbox = await _operator_inbox(actions, lease_secs=lease_secs)
    # the danger map: a STAMPED swap (durable, from the transcript at mount) OR a LIVE swap
    # (the heartbeat caught the harness swapping the model since the last stamp — not yet in
    # the graph, but real and current). Both are "the harness got nervous"; the operator must
    # see either without waiting for a re-mount.
    # THE WINDOW APPLIES TO THE ROSTER TOO. It didn't, and that was the whole bug: every other
    # stream here honours `since`, so a 24h digest shipped 24h of activity beside the fleet's
    # ENTIRE lifetime of agents (1026 rows, 173k chars) and a danger map of swaps on minds that
    # died weeks ago. The counts stay whole; only the ROWS are windowed.
    shown = [r for r in roster if _shipworthy(r, effective_since)]
    unresolved = [r for r in roster if not r["resolved"]]
    unseen = [r for r in roster if r["last_seen"] is None]
    # DANGER = what the operator can still ACT on: a swap on a mind seen inside the window, or a
    # live swap the heartbeat caught unstamped (current by definition). A swap on an agent the
    # graph has NEVER seen is an artifact of a fleet that has run for months — 91 of them. Showing
    # all 91 at every glance would rebuild the scary-red-desk the operator already ruled against:
    # a permanent, unactionable number teaches you to stop reading it. They are COUNTED below
    # (`swapped_unseen`) and reachable — counted is not dropped, and dropped is what we refuse.
    danger = [r for r in shown if r["live_swap"]
              or (r["swapped"] and r["last_seen"] is not None)]
    swapped_unseen = [r for r in roster if r["swapped"] and r["last_seen"] is None]
    if mark_seen:  # the deliberate advance — the ONLY state change a digest can make
        marked_at = datetime.now(UTC)
        await set_cursor(actions.pool, OPERATOR_WATERMARK_KEY, marked_at.isoformat())
        watermark = {**watermark, "marked": True, "advanced_to": marked_at.isoformat()}
    else:
        watermark = {**watermark, "marked": False}
    return {
        "since": effective_since.isoformat(),
        "watermark": watermark,
        "summary": {
            "agents": len(roster), "unresolved": len(unresolved),
            "swapped": len(danger), "activity": len(activity), "laundering": len(laundering),
            "disputes": len(disputes), "conversations": len(conversations),
            "operator_unread": operator_inbox["unread"],
            "spend_tokens": costs["tokens"], "spend_usd": costs["usd"],
            "body_core_seconds": bodies["core_seconds"],
            "body_ram_gib_seconds": bodies["ram_gib_seconds"],
            "miner_errors": miner["errors"],
            # the graph has NO sighting of these minds, ever — neither a transcript stamp nor a
            # mount. Counted, never silently dropped. This is a CENSUS gap, not (yet) a ghost
            # count: walking the 208 found 17 spawns still on disk, 25 bare lineage anchors, 4
            # wake jobs, and 162 whose transcripts the disk no longer has. A ghost (thread
            # 53729dd6) is a SUCCESSION gap — work continued with no handoff — which is a
            # question about links, not about liveness. Do not read one for the other.
            "unseen": len(unseen), "swapped_unseen": len(swapped_unseen),
        },
        "roster": shown,
        "roster_scope": (f"{len(shown)} of {len(roster)} agents — those seen since "
                         f"{effective_since.isoformat()}, plus any carrying a health flag "
                         f"(unresolved identity, or a swap we cannot rule out as historical)"),
        "activity": activity,
        "danger": danger,
        "laundering": laundering,
        "disputes": disputes,
        "conversations": conversations,
        "costs": costs,
        "bodies": bodies,
        "retrieval": retrieval,
        "miner": miner,
        "operator_inbox": operator_inbox,
    }

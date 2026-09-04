"""Osiris MCP server — the AI-facing surface over the engine.

Exposes Osiris's capabilities as typed MCP tools, so any MCP client (Claude Desktop /
Code, a scheduled agent, or none at all) can DRIVE an investigation through a stable
interface — the formalization of what was previously ad-hoc Python. The same engine
backs the human front-end (the FastAPI app); the AI is an external, optional, audited
client, never embedded in the kernel, and every tool still flows through the audited
Actions layer.

    uv run python -m src.mcp_server        # stdio transport
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
import httpx
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.lowlevel.server import NotificationOptions
from mcp.types import Tool as MCPTool

from src import memprofile
from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.dissemination.dossier_report import build_dossier_report
from src.ingest.clinicaltrials import aim_trials, expand_facility
from src.ingest.courtlistener import aim_litigation
from src.ingest.edgar_formd import aim_form_d, expand_filings
from src.ingest.etherscan import aim_address, screen_against_sanctions
from src.ingest.gleif import aim_gleif
from src.ingest.orgbook import aim_orgbook
from src.ingest.transcript_store import identity_reading
from src.ingest.wikidata import aim as wikidata_aim
from src.ontology.catalog import full_catalog
from src.ontology.resolution import (
    consolidate_companies,
    find_cross_base_candidates,
    reclassify_mistyped_entities,
    resolve_cross_base,
)
from src.orchestrator import (
    capture,
    census,
    digest,
    handshake,
    mailbox,
    mounts,
    resource_lease,
    task_sync,
)
from src.orchestrator import compositions as comp
from src.orchestrator import dispose as dispose_seam
from src.orchestrator import succession as comp_succession
from src.orchestrator.agents import (
    AgentIdentity,
    _generation,
    lineage_root,
    misfiled_by_lineage,
    nearest_handoff_ancestor,
    project_pin_banner,
    project_pin_state,
    read_project_model,
    read_project_pin,
    register_agent,
    resolve_identity,
    seat_bearings,
    seat_label,
    write_attribution_banner,
)
from src.orchestrator.budget import fit
from src.orchestrator.console import get_console as _get_console
from src.orchestrator.console import set_console as _set_console
from src.orchestrator.describe import describe_table
from src.orchestrator.doors import doors as _doors_lookup
from src.orchestrator.dossier import entity_dossier
from src.orchestrator.fleetview import render_fleet_tree
from src.orchestrator.handoff_compiler import (
    compile_handoff,
    render_handoff_briefing,
    since_last_handoff,
)
from src.orchestrator.mailbox import (
    OPERATOR_ADDR,
    ack_messages,
    in_flight,
    read_desk,
    read_inbox,
    send_message,
    unread_counts,
)
from src.orchestrator.mailbox import (
    dim_brief as mailbox_dim,
)
from src.orchestrator.monitor import health_banner, organ_health
from src.orchestrator.smoke import smoke as run_smoke
from src.orchestrator.sources import as_dicts, suggest
from src.orchestrator.swaps import classify_swap, swap_banner
from src.parsers.base import EvidenceClass


class BoundedMCP(FastMCP):
    """FastMCP with a WAIST — every tool result passes the response budget on its way out.

    Bounding at the seam, not per-tool, is the whole point: a tool added next year inherits
    the bound without knowing it exists, and no lens's failure can cost a caller its context
    window. The tools still decide what is worth sending (see src/orchestrator/budget.py);
    this only guarantees that whatever they decide, it fits — and that any trim is announced.
    """

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        ctx = self.get_context()
        await _nudge_tool_list_refresh(ctx)
        _ensure_tool_stats_flush_task()
        t0 = time.monotonic()
        try:
            result = await self._tool_manager.call_tool(
                name, arguments, context=ctx, convert_result=False)
            if isinstance(result, dict) and "context" not in result:
                note = await _seam_field(ctx)
                if note is not None:
                    result["context"] = note
            tool = self._tool_manager.get_tool(name)
            assert tool is not None  # call_tool already raised if the name were unknown
            return tool.fn_metadata.convert_result(fit(result, tool=name))
        finally:
            _record_tool_call(name, _caller_for(ctx), (time.monotonic() - t0) * 1000)

    async def list_tools(self) -> list[MCPTool]:
        """HIDDEN ALIASES (task #199 lane 2, thread 6778 — the consolidation-without-an-
        outage mechanism): a tool registered with `meta={"deprecated": True, ...}` is
        DROPPED from the listing a model ever sees, but stays fully in `self._tools` —
        `call_tool` above resolves it directly off that dict, never off this method's own
        output, so a live caller (including a sleeping one whose compiled standing orders
        still name the old verb) keeps working at its next turn with zero code path
        change. This is the thing plain shim-forwarding alone cannot give you: shrinking
        the model-visible SURFACE (this list, and the char/count ratchets in
        test_tool_contract_diet.py that measure exactly this) and the DUPLICATED CODE
        (the old name's body is nothing but a one-line forward) in the same change,
        instead of trading one for the other. Vanilla FastMCP has no such notion —
        `list_tools`/`call_tool` share one undifferentiated registry — so this overrides
        the SAME public seam `call_tool` above already overrides, no monkey-patch of
        anything private."""
        return [t for t in await super().list_tools() if not (t.meta or {}).get("deprecated")]


# TOOL-LIST REFRESH (thread 6a78e64b leg 1, operator-directed: "three verbs deployed today
# each sat invisible for turns"). The MCP spec's own mechanism for this is
# notifications/tools/list_changed — checked FastMCP (mcp==1.28.1) before building anything:
# the lowlevel Server already HAS the capability type (types.ToolsCapability) and the send
# method (ServerSession.send_tool_list_changed); FastMCP's own create_initialization_options()
# call sites (stdio/sse/streamable-http, all inside the SDK) just never pass a
# NotificationOptions(tools_changed=True), so the capability was never declared. That is an
# ergonomics gap in FastMCP's convenience wrapper, not a "don't build on this" wall (the
# undocumented-internal caution — 482c3d0f, now ruling 85fba696 — is about the daemon's own
# claim-socket internals, a different thing entirely):
# NotificationOptions/create_initialization_options are PUBLIC, documented SDK
# surface, exactly like BoundedMCP.call_tool above already overrides FastMCP's own public
# call_tool. No monkey-patch of anything private.
_notified_list_changed: set[str] = set()


async def _nudge_tool_list_refresh(ctx: Context | None) -> None:
    """Once per CLIENT CONNECTION (keyed by `_conn_key`, the same key the identity cache
    uses), tell an already-connected session its tool list may be stale — the deploy-time
    pain this closes: osiris-mcp restarts several times a day as new tools land, but a
    long-lived agent session's MCP client can RESUME its existing connection across that
    restart without ever re-running `initialize`/`tools/list`, so it never learns new tools
    exist until something else nudges it. Ambient, never load-bearing: any failure here
    must never block or fail the tool call it rides in on."""
    key = _conn_key(ctx)
    if key is None or key in _notified_list_changed:
        return
    _notified_list_changed.add(key)
    try:
        assert ctx is not None
        await ctx.session.send_tool_list_changed()
    except Exception:  # noqa: BLE001 — ambient, never load-bearing
        pass


# TOOL-CALL TELEMETRY (task #167, dispatch msg 4029/4034): WHICH MCP TOOL IS EXPENSIVE — the
# thing tonight's 363k-scans/sec investigation (decision 978962ad) needed and couldn't get,
# forcing a one-off hand-bracketed measurement instead of a real number. `search_log`/
# `llm_usage` already do exactly this per-call telemetry shape for ONE tool each (search, the
# inference seam) and were never generalized — this extends that shape rather than inventing
# one; see migration 0046. The hot path only ever touches the in-memory dict below — a
# background task (started lazily, same pattern as `_pool_get`'s lazy global pool) flushes it
# to Postgres every 60s, decoupled from any individual call, so the thing being measured never
# pays for being measured. try/finally in BoundedMCP.call_tool counts failures too — a
# counter that only saw successes would report the expensive calls as cheap.
#
# CALLER ATTRIBUTION (task #170, migration 0048, Thoth msg 4279, decision 700b6148's own
# named gap): keyed (tool, caller) instead of bare tool — WITHOUT it this table ranks tools
# but never causes; a "search is expensive" reading could really be "one busy agent's search
# habit is expensive." `caller` is a LINEAGE ROOT (agents.py's `_generation()`, the same
# soul-folding doors.py's `_record` already uses), not a raw agent_id — a seat mints a new
# agent_id on every succession/compaction, so grouping by the raw id would fragment one
# caller's real cost across dozens of rows. Resolved CACHE-ONLY from `_agents` (never a new
# `_ident_for` reattach, which can hit Postgres) — see `_caller_for` below.
_TOOL_STATS_FLUSH_INTERVAL_S = 60
_tool_call_stats: dict[tuple[str, str], dict[str, float]] = {}
_tool_stats_flush_task: asyncio.Task[None] | None = None
_tool_stats_window_start: datetime | None = None
# WHAT THIS CANNOT SEE — lives in tool_traffic()'s own output (`blind_spots`), not only in a
# decision, per Thoth's explicit rule (msg 4034): a clean total over an unstated scope is how
# the next reader gets misled. Checked live via `systemctl --user list-units`, not assumed:
# osiris-console (:8011) is a SEPARATE process from osiris-mcp (:8790) — task #164's own
# console slowdown lived entirely on a surface this counter cannot see. osiris-worker (arq
# cron), osiris-pulse (heartbeat), and osiris-manager (the hands daemon) are likewise separate
# processes calling orchestrator functions directly, never through MCP. This answers "which
# MCP TOOL is expensive," never "which SURFACE is expensive." Caller attribution (#170) does
# NOT change any of this — those three daemons never go through MCP at all, so they stay
# exactly as uncounted as before, not newly countable.
_TOOL_STATS_BLIND_SPOTS = (
    "osiris-console (:8011, a separate uvicorn process) — not counted; "
    "task #164's own console slowdown lived entirely here. Confirmed live (#203, Seshat, "
    "2026-09-03): src/api/app.py imports and calls orchestrator.console.get_console "
    "directly, bypassing this MCP tool entirely — its own zero-MCP-traffic reading "
    "already misled one retirement pass into hiding it as dead (decision b49a844f) "
    "before that seat's own live-test run caught it. Separately, and worse: the "
    "console's POST /rooms handler does NOT call this MCP tool's create_room at all — "
    "it inlines the exact same SQL as orchestrator.compositions.create_room verbatim, a "
    "genuine duplicate implementation (not a bypass of one canonical function, two "
    "independent copies that can silently drift), and it contradicts this daemon's own "
    "service file (deploy/user/osiris-console.service: 'never a write path')",
    "osiris-worker (arq cron: drain_cascade/evaluate_watch/sweep_doors/trigger_mail) — "
    "not counted, calls orchestrator functions directly",
    "osiris-pulse (heartbeat) — not counted, calls orchestrator functions directly",
    "osiris-manager (the hands daemon) — not counted, calls orchestrator functions directly",
    "direct Postgres access (scripts, psql, one-off measurement runs like this task's own) — "
    "not counted, and never can be by an application-level counter",
    "caller attribution is CACHE-ONLY (task #170): a call on a connection whose identity "
    "isn't cached yet — in practice, the very first call of a fresh session before mount()/"
    "orient() resolves it — is bucketed under 'unattributed' rather than paying for a "
    "reattach query just to label a telemetry row",
    "THE CLI ITSELF (#199 lane 2, Seshat, 2026-09-03): several cmd_* functions in "
    "src/cli.py call an orchestrator function DIRECTLY, bypassing this MCP tool entirely "
    "— confirmed live for at least bind_seat_tree, bootstrap, establish_office, "
    "heal_seat_anchor_third_party, rematerialize, stop, unmerge. A zero reading on any "
    "of these is not evidence of disuse, it can be AFFIRMATIVELY MISLEADING: unmerge "
    "reads 0 here while its CLI door is real, live traffic — the exact live proof that "
    "cost a consolidation lane its first wrong deletion candidate",
)


def _caller_for(ctx: Context | None) -> str:
    """The lineage root attributed to this call, CACHE-ONLY — never a new DB round trip on
    the hot path (see the TOOL-CALL TELEMETRY block comment above for why raw agent_id is
    the wrong grain and why this never calls `_ident_for`'s reattach fallback)."""
    from src.orchestrator.agents import _generation

    key = _conn_key(ctx)
    ident = _agents.get(key) if key is not None else None
    return _generation(ident.agent_id)[0] if ident is not None else "unattributed"


def _record_tool_call(name: str, caller: str, ms: float) -> None:
    row = _tool_call_stats.setdefault((name, caller), {"count": 0.0, "total_ms": 0.0})
    row["count"] += 1
    row["total_ms"] += ms


def _ensure_tool_stats_flush_task() -> None:
    global _tool_stats_flush_task, _tool_stats_window_start
    if _tool_stats_flush_task is None:
        _tool_stats_window_start = datetime.now(UTC)
        _tool_stats_flush_task = asyncio.create_task(_flush_tool_stats_loop())


async def _flush_tool_stats_loop() -> None:
    while True:
        await asyncio.sleep(_TOOL_STATS_FLUSH_INTERVAL_S)
        await _flush_tool_stats_once()


async def _flush_tool_stats_once() -> None:
    """Swap the live dict out (new calls keep counting into a fresh one) and write the
    snapshot — never hold the dict empty across an `await`, or a call landing mid-flush
    would increment a row that's about to be discarded."""
    global _tool_call_stats, _tool_stats_window_start
    if not _tool_call_stats:
        _tool_stats_window_start = datetime.now(UTC)
        return
    batch, _tool_call_stats = _tool_call_stats, {}
    window_end = datetime.now(UTC)
    window_start = _tool_stats_window_start or (window_end - timedelta(seconds=60))
    _tool_stats_window_start = window_end
    try:
        pool = await _pool_get()
        await pool.executemany(
            "INSERT INTO mcp_tool_stats (tool_name, caller, window_start, window_end, "
            "call_count, total_ms) VALUES ($1, $2, $3, $4, $5, $6)",
            [(tool, caller, window_start, window_end, int(v["count"]), v["total_ms"])
             for (tool, caller), v in batch.items()],
        )
    except Exception:  # noqa: BLE001 — telemetry must never break serving
        import logging
        logging.getLogger("osiris.mcp").warning("tool-stats flush failed", exc_info=True)


# THE AMBIENT SEAM WHISPER (alfred's pitch, written at his own 70% seam — decision d80621a7
# piece 1): above the whisper threshold every tool response carries ONE `context` line,
# because the agent near the ceiling is exactly the agent not thinking to ask. Riding the
# waist means a tool added next year inherits the whisper without knowing it exists — the
# same argument as the response budget. Ambient, never load-bearing: every failure path
# returns None, and the alarm inherits the known-window-only law (Anubis VII's false
# eulogy) — never a death notice on a guessed denominator.
_SEAM_ROW_TTL = 600.0  # how long a mount-row hint (job/model/window) may serve the whisper
_seam_rows: dict[str, tuple[float, str | None, str | None, int | None]] = {}
_seam_pcts: dict[str, tuple[float, int | None]] = {}
# BOUNDED, same shape as _prune_agents (mcp_server.py's own proven pattern, "the slow leak
# that fed the 1G OOM"): every agent_id/job that ever calls a mounted tool leaves a row here
# forever unless capped. Safe to cap AT ALL because both are self-healing on a miss — _seam_rows
# already re-fetches from agent_mounts past its own TTL (line below), _seam_pcts already
# recomputes on an mtime mismatch — so an evicted entry costs one extra query/stat, never a
# wrong answer. Each tuple's own first element (a monotonic write-time or the file's mtime) IS
# a workable recency signal, so no companion "touched" dict is needed to prune by it.
_SEAM_CACHE_CAP = 256


def _prune_seam_rows(cap: int = _SEAM_CACHE_CAP) -> None:
    """Mirrors _prune_agents exactly: past the cap, drop the least-recently-written down to
    half. Safe because _seam_field re-fetches past _SEAM_ROW_TTL regardless — an evicted
    entry just loses its TTL grace early, never returns a wrong answer."""
    if len(_seam_rows) <= cap:
        return
    cut = len(_seam_rows) - cap // 2
    for k in sorted(_seam_rows, key=_seam_rows.__getitem__)[:cut]:
        _seam_rows.pop(k, None)


def _prune_seam_pcts(cap: int = _SEAM_CACHE_CAP) -> None:
    """Mirrors _prune_agents exactly, keyed by mtime (the closest thing this cache has to a
    write-recency clock) rather than a monotonic touch-time. Safe because _seam_pct_sync
    recomputes on any mtime mismatch — an evicted entry costs one stat, never a stale answer."""
    if len(_seam_pcts) <= cap:
        return
    cut = len(_seam_pcts) - cap // 2
    for k in sorted(_seam_pcts, key=_seam_pcts.__getitem__)[:cut]:
        _seam_pcts.pop(k, None)


def _seam_locate(job: str) -> Path | None:
    from src.ingest.sessions import locate_current_transcript

    return locate_current_transcript(Path.home() / ".claude" / "projects", job,
                                     anchored_only=True)


def _seam_pct_sync(job: str, model_raw: str | None, window_hint: int | None) -> int | None:
    """The occupancy %, from the transcript's tail (the chrome-grade read), mtime-cached
    per job so a busy turn costs one stat. None when unmeasurable OR the window would be
    a guess."""
    from src.orchestrator import context_lens

    path = _seam_locate(job)
    if path is None:
        return None
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    hit = _seam_pcts.get(job)
    if hit is not None and hit[0] == mtime:
        return hit[1]
    pct: int | None = None
    u = context_lens.last_usage(path)
    if u is not None:
        used = context_lens.occupancy(u)
        if window_hint:
            pct = round(100 * used / int(window_hint))
        else:
            window, assumed = context_lens.window_for(model_raw, used)
            pct = None if assumed else round(100 * used / window)
    _seam_pcts[job] = (mtime, pct)
    _prune_seam_pcts()  # opportunistic: this write is where churn shows up
    return pct


def _seam_note(pct: int | None, whisper_pct: int) -> str | None:
    """The one line, tiered: seam-soon at the whisper threshold, write-back-NOW at the
    house alarm (context_lens.ALARM_PCT — one authority, never a second constant)."""
    if pct is None or not whisper_pct or pct < whisper_pct:
        return None
    from src.orchestrator.context_lens import ALARM_PCT

    if pct >= ALARM_PCT:
        return (f"{pct}% — WRITE BACK NOW: a compaction can land any turn; "
                "record_decision / resolve_thread what lives only in your head")
    return f"{pct}% — seam soon; write back as you go"


# ONCE PER CROSSING, NOT ONCE PER CALL (thread e2326ab7, Soundwave XIV's decepticons
# report): `_seam_note` on its own fires on EVERY tool call while `pct` sits anywhere in a
# tiered band, unlike the offload ritual's own soft/hard marker files (osiris_hook.py),
# which fire exactly once per crossing. A 7-hour autonomous run sitting at 63-79% context
# for most of it saw the same "seam soon" line on ~40 consecutive calls — wallpaper before
# it was useful, the same disease as Wave 4 legs (a)/(b) inverted: too OFTEN instead of too
# LATE, training the reader to stop reading the ladder it belongs to. `_seam_last_band`
# mirrors `_seam_rows`/`_seam_pcts`'s own bounded, in-process, TTL-free cache shape —
# correct to reset on server restart, since a fresh process has shown nothing yet and the
# very next crossing fires exactly as it should.
_seam_last_band: dict[str, tuple[float, str]] = {}


def _prune_seam_last_band(cap: int = _SEAM_CACHE_CAP) -> None:
    """Mirrors `_prune_seam_rows` exactly — least-recently-written half evicted past the
    cap; safe because a re-shown band after eviction is at worst one redundant note, never
    a wrong or missing one."""
    if len(_seam_last_band) <= cap:
        return
    cut = len(_seam_last_band) - cap // 2
    for k in sorted(_seam_last_band, key=_seam_last_band.__getitem__)[:cut]:
        _seam_last_band.pop(k, None)


def _seam_band(pct: int | None, whisper_pct: int, alarm_pct: int) -> str | None:
    """Which TIER `pct` falls in for the debounce below — a state name, never text, so the
    same crossing is never re-announced on every call. None below the whisper floor."""
    if pct is None or not whisper_pct or pct < whisper_pct:
        return None
    return "alarm" if pct >= alarm_pct else "seam"


def _seam_note_once(agent_id: str, pct: int | None, whisper_pct: int) -> str | None:
    """`_seam_note`, debounced to fire once per tier-crossing rather than once per call.
    Stays silent on every later call inside the SAME band; re-arms the moment `pct` drops
    back below `whisper_pct` (a real write-back/compaction happened) or steps UP from
    'seam' into 'alarm' (a real escalation, worth exactly one more note — the house alarm,
    context_lens.ALARM_PCT, is one authority; this never re-derives its own threshold)."""
    from src.orchestrator.context_lens import ALARM_PCT

    band = _seam_band(pct, whisper_pct, ALARM_PCT)
    if band is None:
        _seam_last_band.pop(agent_id, None)  # dropped below the floor — re-arm for later
        return None
    prior = _seam_last_band.get(agent_id)
    if prior is not None and prior[1] == band:
        return None  # already shown this band — wallpaper, not news
    _seam_last_band[agent_id] = (time.monotonic(), band)
    _prune_seam_last_band()  # opportunistic: this write is where churn shows up
    return _seam_note(pct, whisper_pct)


async def _seam_field(ctx: Context | None) -> str | None:
    """The ambient context line for a mounted caller, or None (unmounted callers, young
    sessions, guessed windows, an already-shown tier, any failure — the whisper never
    becomes a hazard)."""
    try:
        st = get_settings()
        if not st.osiris_seam_whisper_pct:
            return None
        ident = await _ident_for(ctx)
        if ident is None:
            return None
        now = time.monotonic()
        row = _seam_rows.get(ident.agent_id)
        if row is None or now - row[0] > _SEAM_ROW_TTL:
            pool = await _pool_get()
            r = await pool.fetchrow(
                "SELECT job_dir, model_raw, context_window_size FROM agent_mounts "
                "WHERE agent_id=$1 ORDER BY last_seen DESC NULLS LAST LIMIT 1",
                ident.agent_id)
            row = (now, r["job_dir"] if r else None, r["model_raw"] if r else None,
                   r["context_window_size"] if r else None)
            _seam_rows[ident.agent_id] = row
            _prune_seam_rows()  # opportunistic: this write is where churn shows up
        _, job, model_raw, window_hint = row
        if not job:
            return None
        pct = await asyncio.to_thread(_seam_pct_sync, job, model_raw, window_hint)
        return _seam_note_once(ident.agent_id, pct, st.osiris_seam_whisper_pct)
    except Exception:  # noqa: BLE001 — ambient, never load-bearing
        return None


mcp = BoundedMCP(
    "osiris",
    instructions=(
        "Osiris is the durable memory an agent session doesn't have — the graph remembers "
        "what you learn and decide after you're gone, and it is SHARED across the whole "
        "fleet. "
        "FIRST, call mount(cwd=<your working dir>) to link in as a first-class agent — this "
        "attributes everything you write to YOU instead of an anonymous bucket. Pass a "
        "durable job_dir if your harness provides one (Claude Code: ~/.claude/jobs/<id>, "
        "DSH: auto-detected from workspace slug); without it you still mount but identity "
        "is ephemeral across server restarts. "
        "GLANCE, DON'T DUMP: use get_status() for a quick check (~360 chars) instead of "
        "orient()'s full briefing (~59K chars). Use get_mail() for just your inbox counts. "
        "get_thread_list(project) and get_decision_list(project) give paginated views. "
        "SEARCH BEFORE DERIVING: graph_search(query, project=<name>) scopes results to a "
        "project's subgraph — the same fused engine as search() but graph-aware. "
        "WRITE BACK AS YOU GO: record_decision the moment a ruling lands, "
        "open_thread when work starts or blocks (kind='obligation' for a duty), "
        "resolve_thread the moment it closes. A session can die at any instant: the graph, "
        "not the context window, is your memory. "
        "The fleet shares a MAILBOX: another agent can address a message to your project. "
        "mount()/orient()/get_status() report your unread count; inbox() reads it. "
        "send(to='operator') reaches the HUMAN's desk."
    ),
)
# DECLARE THE listChanged CAPABILITY (see BoundedMCP/_nudge_tool_list_refresh above): FastMCP
# never passes NotificationOptions through to the lowlevel Server's own
# create_initialization_options(), so `tools_changed` silently defaults to False and a
# compliant client never even learns the server MIGHT send this notification. Wrapping the
# bound method (public, not underscore-prefixed) to supply the default the SDK already
# supports — every call site that omits its own notification_options gets tools_changed=True.
_orig_create_init_options = mcp._mcp_server.create_initialization_options


def _create_init_options_with_tools_changed(
    notification_options: NotificationOptions | None = None,
    experimental_capabilities: dict[str, dict[str, Any]] | None = None,
) -> Any:
    return _orig_create_init_options(
        notification_options=notification_options or NotificationOptions(tools_changed=True),
        experimental_capabilities=experimental_capabilities)


mcp._mcp_server.create_initialization_options = (  # type: ignore[method-assign]
    _create_init_options_with_tools_changed)
_pool: asyncpg.Pool | None = None


@mcp.tool()
async def tool_traffic(window_minutes: int = 60) -> dict[str, Any]:
    """WHICH MCP TOOL IS EXPENSIVE, AND WHOSE — call count + total/avg wall-clock time,
    newest-cost-first, cut two ways: `persisted`/`current_unflushed_window` by TOOL
    (summed across callers), `persisted_by_caller`/`current_unflushed_by_caller` by
    CALLER (summed across tools — is a tool's cost concentrated in one caller or spread
    across many). `persisted` reads flushed 60s windows from `mcp_tool_stats` going
    back `window_minutes`; the `current_unflushed_*` pair is the live in-memory
    counters since the last flush — may be a partial window. Failures count too, so a
    broken tool doesn't read as cheap. `blind_spots` names what this can never see —
    read it before trusting a total."""
    pool = await _pool_get()
    since = datetime.now(UTC) - timedelta(minutes=window_minutes)
    tool_rows = await pool.fetch(
        "SELECT tool_name, sum(call_count) AS calls, sum(total_ms) AS total_ms "
        "FROM mcp_tool_stats WHERE window_start >= $1 "
        "GROUP BY tool_name ORDER BY total_ms DESC", since,
    )
    caller_rows = await pool.fetch(
        "SELECT caller, sum(call_count) AS calls, sum(total_ms) AS total_ms "
        "FROM mcp_tool_stats WHERE window_start >= $1 "
        "GROUP BY caller ORDER BY total_ms DESC", since,
    )

    def _fmt(calls: int, total_ms: float) -> dict[str, Any]:
        return {"calls": calls, "total_ms": round(total_ms, 1),
                "avg_ms": round(total_ms / calls, 2) if calls else None}

    persisted = [{"tool": r["tool_name"], **_fmt(r["calls"], r["total_ms"])} for r in tool_rows]
    persisted_by_caller = [
        {"caller": r["caller"], **_fmt(r["calls"], r["total_ms"])} for r in caller_rows]

    by_tool: dict[str, dict[str, float]] = {}
    by_caller: dict[str, dict[str, float]] = {}
    for (tool, caller), v in _tool_call_stats.items():
        t = by_tool.setdefault(tool, {"count": 0.0, "total_ms": 0.0})
        t["count"] += v["count"]
        t["total_ms"] += v["total_ms"]
        c = by_caller.setdefault(caller, {"count": 0.0, "total_ms": 0.0})
        c["count"] += v["count"]
        c["total_ms"] += v["total_ms"]
    live = [
        {"tool": name, **_fmt(int(v["count"]), v["total_ms"])}
        for name, v in sorted(by_tool.items(), key=lambda kv: -kv[1]["total_ms"])
    ]
    live_by_caller = [
        {"caller": name, **_fmt(int(v["count"]), v["total_ms"])}
        for name, v in sorted(by_caller.items(), key=lambda kv: -kv[1]["total_ms"])
    ]
    return {
        "window_minutes": window_minutes,
        "persisted": persisted,
        "current_unflushed_window": live,
        "persisted_by_caller": persisted_by_caller,
        "current_unflushed_by_caller": live_by_caller,
        "measures": "MCP tool calls on this one shared osiris-mcp process only",
        "blind_spots": list(_TOOL_STATS_BLIND_SPOTS),
    }


# The fleet registry: each connected agent's identity, keyed by its client session. On the
# shared server every agent writes through ONE process, so without this their writes
# collapse into the single `session` source. `mount` populates this; the capture tools
# read it so each write is attributed to `agent:<session>`. The dict is the HOT half; the
# DURABLE half is agent_mounts in PG (src/orchestrator/mounts.py) — a server bounce used to
# wipe the whole fleet's identities at once (decision 56f6a0d6); now any call re-attaches
# from the table by the client's job_dir header (_ident_for).
_agents: dict[str, AgentIdentity] = {}
_agents_touched: dict[str, float] = {}  # last use per key — feeds the bounce-orphan prune
# The while-you-were-away anchor per agent: the lineage's last_seen BEFORE this session's
# mount/reattach (captured from save_mount's RETURNING). mount() and orient() fold what
# happened in the agent's name since — twins, wakes, thread movement — so a returning tab
# never has to guess where it stands ("the agents have to know, or it falls apart").
#
# DELIBERATELY UNBOUNDED (Thoth DM 2795, OOM follow-up, 2026-08-01) — its three siblings
# below (_seam_rows/_seam_pcts/sessions._wake_verdict) got a cap=256/4096 LRU prune; this one
# did not, on purpose. It fails a different way than they do:
#   (a) NO SELF-HEALING RE-FETCH ON A MISS. The other three recompute the correct answer from
#       an authoritative source when evicted — a cache miss costs one query, never a wrong
#       result. This one cannot: while_away()'s own contract treats a missing anchor as
#       IDENTICAL to "nothing happened while you were away" (its own docstring's words), so a
#       pruned entry doesn't error or degrade visibly — it silently reports the wrong thing as
#       if it were the right thing. Tonight's whole thesis is instruments that report success
#       while actually failing; a churn-based cap here would trade a bounded, loud failure
#       (the process grows and eventually dies visibly) for an unbounded, silent one.
#   (b) READ ACROSS A SESSION'S WHOLE LIFETIME, not just near mount. orient() reads it on
#       every call, for as long as the mounted session lives — so its real required lifetime
#       is "as long as the session lives," which a count-based LRU cap has no way to guarantee
#       (a busy fleet could evict a still-live session's own anchor before that session's next
#       orient() call).
# If this ever needs bounding, the correct shape is a TTL long enough to outlive any real
# session (hours-to-days, not a churn cap sized to entry count) — never the _prune_agents
# pattern used on its neighbors. It is also the smallest and least frequently written of the
# four (setdefault, not overwrite), so the cost of leaving it unbounded is the lowest of the
# four to begin with.
_prev_seen: dict[str, datetime | None] = {}


def _prune_agents(cap: int = 256) -> None:
    """Client sessions churn and never say goodbye (a vanished tab leaves its entry behind —
    the slow leak that fed the 1G OOM); past the cap, drop the least-recently-used down to
    half. The durable registry (agent_mounts) makes an over-eager prune cost one transparent
    re-attach, nothing more."""
    if len(_agents) <= cap:
        return
    stale = sorted(_agents_touched, key=_agents_touched.__getitem__)[: len(_agents) - cap // 2]
    for key in stale:
        _agents.pop(key, None)
        _agents_touched.pop(key, None)


def _evict_stale_minds(ancestor: str | None) -> None:
    """A mint means the ANCESTOR is dead — but its MCP connection is not: a compaction (or a
    live swap) preserves the client session, so the conn-keyed hot cache keeps answering as
    the dead mind while the durable row already names the heir (Thoth XVII's first breath,
    2026-07-10: orient() spoke as -xvi minutes after the whisper minted -xvii). Evict every
    cached identity wearing the ancestor; the next call re-attaches from the row as the heir."""
    if not ancestor:
        return
    for key in [k for k, ident in _agents.items() if ident.agent_id == ancestor]:
        _agents.pop(key, None)
        _agents_touched.pop(key, None)


def _conn_key(ctx: Context | None) -> str | None:
    """A per-client-session key. Prefer the protocol session id (the Mcp-Session-Id header —
    minted at initialize, stable across every request of the client session); fall back to
    the ServerSession object id under stdio. The keyspaces are prefixed so they can't collide
    (a GC'd session object's id() CAN be reused — the raw-id key was a latent cross-agent
    identity merge, forbidden territory)."""
    if ctx is None:
        return None
    try:
        req = ctx.request_context.request
        sid = req.headers.get("mcp-session-id") if req is not None else None
        if sid:
            return f"sid:{sid}"
        return f"obj:{id(ctx.request_context.session)}"
    except (AttributeError, LookupError):
        return None


def _sane_job_dir(value: str | None) -> str | None:
    """A usable job_dir is an ABSOLUTE PATH. Anything carrying `$` is an unexpanded variable
    (braced or not — a live agent passed the literal `$CLAUDE_JOB_DIR` and it became a
    registry PRIMARY KEY, a conflation magnet: every agent making the same mistake would
    collapse into one row). Reject → treat as absent, never store."""
    if not value or "$" in value or not value.startswith("/"):
        return None
    return value


def _anchorless(ctx: Context | None) -> str:
    """WHY this call could not be re-attached — the difference between a mystery and a message.

    Two agents on one project reported the same thing within an hour (msgs 397, 403): after an MCP
    socket hiccup a tool call bounces with "mount first", and — worse — an un-mounted write falls
    back to the anonymous `session` bucket. As one of them put it: "MCP socket → missing anchor →
    anonymous writes... one careless reconnect and a session's work lands unattributed." For a
    graph whose entire value is provenance, that is the worst failure it has.

    The re-attach machinery already exists and is starved, not broken: it keys off the X-Osiris-Job
    header, which .mcp.json sends as ${CLAUDE_JOB_DIR}. If the client's environment does not set
    that variable, the header arrives EMPTY or as the literal, _sane_job_dir rightly rejects it,
    and there is nothing to re-attach by. So say exactly that, instead of "mount first" — a bounce
    that names its own cause is a bug report the next mind does not have to file again.
    """
    if ctx is None:
        return "no request context"
    raw = None
    try:
        req = ctx.request_context.request
        raw = req.headers.get("x-osiris-job") if req is not None else None
    except (AttributeError, LookupError):
        pass
    # TRANSIENT OR TERMINAL? — Khepri III's ask, and it is the right one (msg 420): "a reason code
    # would let an agent tell 'transient, just retry' from 'something actually forgot me'." A
    # bounce that says only "mount first" is INDISTINGUISHABLE FROM AMNESIA, so every agent guesses
    # — and a guessing agent either re-mounts needlessly or panics about continuity it never lost.
    # These are DIFFERENT FACTS and the bounce must say which.
    if not raw:
        return ("[no-anchor · TRANSIENT] your client sent no X-Osiris-Job header (CLAUDE_JOB_DIR "
                "is unset in interactive sessions — this is normal). NOTHING HAS FORGOTTEN YOU: "
                "the PreToolUse hook now stamps session_anchor on every call, so if you are seeing "
                "this, that hook is not installed. Re-mount with your durable anchor and you are "
                "whole; your identity and your work are intact in the graph")
    if "$" in raw:
        return (f"[unexpanded-anchor · TRANSIENT] your client sent the header literal ({raw!r}) — "
                "CLAUDE_JOB_DIR is not set in its environment. Nothing has forgotten you: re-mount "
                "with the real path and you are whole")
    return (f"[unknown-anchor · TERMINAL] the anchor {raw!r} matches no mount in the registry. "
            "This one is REAL: either you were never mounted under it, or you are wearing another "
            "session's anchor. Mount properly; do not simply retry")


def _job_hint(ctx: Context | None) -> str | None:
    """The client's durable identity handle: the X-Osiris-Job header.

    THIS HEADER HAS NEVER ONCE FIRED IN PRODUCTION, and this docstring used to claim the
    opposite — "expansion PROVEN live via the probe reattach". That was FALSE. Ruling 40faa5e6
    (2026-07-09) instrumented the server and caught what the client actually sends: the LITERAL
    string '${CLAUDE_JOB_DIR}', unexpanded. Project-scope .mcp.json does expand ${VAR} in
    headers — but the fleet is installed USER-SCOPE (~/.claude.json via `claude mcp add`), and
    this client version does not expand there. So _sane_job_dir rejects every '$'-bearing value
    and this function has returned None for the whole fleet, for its entire life. Durable
    identity has been carried ENTIRELY by the hook-derived job_dir, never by this.

    THE RULING SAID "corrected" AND THE CODE WAS NEVER CORRECTED. The false claim sat here for
    three days and cost the next reader (me, 2026-07-12) a full re-derivation of a bug the graph
    had already solved. A correction that lands in the graph but not at the site where the next
    mind will READ is not a correction — it is a second lie with a citation. Kept as a live
    fallback only in case a future client learns to expand it; expect None.
    """
    if ctx is None:
        return None
    try:
        req = ctx.request_context.request
        hint = req.headers.get("x-osiris-job") if req is not None else None
    except (AttributeError, LookupError):
        return None
    return _sane_job_dir(str(hint) if hint else None)


async def _expected_model(pool: asyncpg.Pool, cwd: str | None, proj: str | None) -> str:
    """The operator's standing model choice for THIS repo — the .osiris file first, then
    the SoftwareProject's intended_model property (the graph's own .osiris; the standing-
    choice standdown, Metron IV fa918939), then the box default. Every banner and
    divergence stamp measures against THIS, so a settled seam is never re-litigated."""
    exp = read_project_model(cwd)
    if not exp and proj:
        exp = await pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a "
            "JOIN objects o ON o.id=a.object_id "
            "WHERE o.canonical='repo:' || $1 AND o.type='SoftwareProject' "
            "AND a.name='intended_model' "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", proj)
    return str(exp) if exp else get_settings().osiris_expected_model


async def _wake_economy_standdown(
    pool: asyncpg.Pool, proj: str | None, observed: str | None,
) -> str | None:
    """The WAKE-ECONOMY standdown (a sibling project, msg 281): triage wakes ride a CHEAPER
    model by the operator's own ruling (osiris_wake_model, 4e52af7e) — but the swap banner
    measured them
    against the repo's standing choice, so every wake was told it had been rug-pulled and
    dutifully 'escalated' the operator's own policy back to his desk, at wake cadence. If
    the observed model IS the economy model and this project's wake ledger shows a wake
    minutes ago, the divergence is the ruling WORKING: the banner stands down to a calm
    note. The note still tells a non-wake how to tell the difference — witnessed (the
    ledger), never assumed."""
    st = get_settings()
    if not st.osiris_wake_model or observed != st.osiris_wake_model or not proj:
        return None
    woken = await pool.fetchval(
        "SELECT 1 FROM agent_wakes WHERE to_project=$1 "
        "AND woke_at > now() - interval '30 minutes' LIMIT 1", proj)
    if not woken:
        return None
    return (f"model {observed}: the TRIAGE-WAKE ECONOMY model — the operator's own ruling "
            "(wakes ride a cheaper model; real work escalates to a full session: "
            "open_thread(kind='obligation') + a pointer reply). Deliberate, not a rug-pull; "
            "no confession owed. If you are NOT a triggered wake, treat this as a real swap "
            "and say so to the operator.")


async def _resolve_project_seat_first(pool: asyncpg.Pool, ident: AgentIdentity) -> None:
    """IDENTITY IS LOCATION-INDEPENDENT (operator ruling 577988ed, correcting mount-guard #6's
    original refusal): osiris orients from the SEAT (anchor→holds→seat), never from cwd — the
    whole point of a seat is that where a session happens to be sitting doesn't matter. For a
    SEATED session, project is the SEAT'S OWN derived house — UNCONDITIONALLY, overriding
    whatever cwd produced, not merely filling in a gap when cwd came up empty. Deliberately
    NOT house_of(agent_id): that reads the AGENT's own project stamp, exactly what a
    transient bad mount can pollute (Thoth's own case) — trusting it here would let a
    polluted stamp go on leaking into every read, the very thing this function exists to
    stop. An UNSEATED session (no holds binding yet — nothing to trust but its own
    resolution) keeps whatever cwd produced, None included; that's an honest 'not mounted to
    a definite project', not an error. Mutates `ident` in place; call AFTER register_agent
    (the write gate must still see the FRESH cwd-derived value, unclobbered — a legitimate
    cwd-derived project still gets asserted for a session that isn't seated yet).

    A thin wrapper (msg 1888, the mount/project-resolution pollution build) around
    `seats.resolve_and_persist_seated_project` — the SAME seat-first check
    `seats.resolve_project` (the shared resolver the stop hook and census now use) leads
    with. Deliberately not the full `resolve_project`: its cwd-guessing fallback is for
    callers with no cwd-derived answer of their own; mount() already has one, fresh off
    `resolve_identity` moments earlier in this same pipeline, and it must win untouched
    when this comes up unseated — recomputing a second, independent cwd guess here could
    disagree with it.

    ALSO PERSISTS the correction onto the Agent object's own `project` assertion (thread
    6a00e942) — not merely this call's in-memory `ident`/the durable mount-registry row.
    fleet() reads that assertion directly, never the registry row; without this, a seated
    session whose cwd didn't independently resolve (the bare seats container root) stayed
    filed under "?" in fleet() forever, even though this very function already knew the
    seat's true house and mount()'s own receipt already showed it correctly."""
    from src.orchestrator.seats import resolve_and_persist_seated_project
    house = await resolve_and_persist_seated_project(Actions(pool), ident.agent_id)
    if house is not None:
        ident.project = house


async def _reattach(
    pool: asyncpg.Pool, key: str | None, job: str | None
) -> AgentIdentity | None:
    """The durable-registry half of _ident_for (separated so tests drive it with their own
    pool): look the job_dir up in agent_mounts, re-run identity resolution off the transcript
    (so the model/swap history is FRESH, not a stale copy), re-register, re-cache. The stored
    model is deliberately NOT passed as a self-report — it would false-flag model_divergent
    after a real swap. None when there is nothing to re-attach by."""
    if job is None:
        return None
    rec = await mounts.find_mount(pool, job_dir=job)
    adopted_from = None
    if rec is None:
        # THE BRIDGED RESUME (90f0cb3a): the session-picker resume presents a NEW anchor the
        # registry never learned (jobs/<new>/state.json names resumeSessionId — the harness's
        # own receipt of the pair). Follow it: adopt the resumed anchor's row, and below mint
        # the presented anchor its own sibling row so the next request is a direct hit —
        # without this, every call from a resumed tab bounced [unknown-anchor · TERMINAL].
        prior = mounts.resumed_anchor(job)
        rec = await mounts.find_mount(pool, job_dir=prior) if prior else None
        if rec is not None:
            adopted_from = rec.job_dir
    if rec is None:
        # #178 PIECE (B) — THE TRANSCRIPT SELF-RESTORE (Thoth dispatch msg 5224): no row
        # survives under this anchor OR its resume-bridge (session_end's own release, a
        # daemon re-adopt after a bounce, a genuinely evicted row) — but a REAL transcript
        # proves this session actually ran before, which is proof enough to restore rather
        # than bounce [unknown-anchor · TERMINAL] and force a fresh, unattributed re-mount.
        # `cwd_of_transcript` is anchored-only (never a co-tenant's file — the same identity-
        # path law `current_model` already follows): None here means genuinely never
        # mounted, and the bounce below is the CORRECT answer, not a gap.
        from src.ingest.sessions import cwd_of_transcript

        restored_cwd = cwd_of_transcript(job_dir=job)
        if restored_cwd is None:
            return None
        rec = mounts.MountRecord(job_dir=job, agent_id="", project=None, cwd=restored_cwd,
                                 model=None)
    settings = get_settings()
    # the model reading rides THE STORE (sole lane since the JSONL-fallback removal, #29);
    # fail-open — a store outage re-attaches with an unobserved model, never a bounce
    reading = await identity_reading(pool, cwd=rec.cwd, job_dir=rec.job_dir)
    ident = resolve_identity(cwd=rec.cwd, job_dir=rec.job_dir, store_reading=reading)
    # rec.agent_id == "" is the piece-(b) self-restore's own sentinel (mounts.MountRecord
    # minted above with no PRIOR row to have bound a seat on) — nothing to honor, the
    # freshly-derived ident is definitionally the right answer, so this check must not fire.
    if rec.agent_id and _generation(rec.agent_id)[0] != _generation(ident.agent_id)[0]:
        # a BOUND session (thread 33838160): the row points at a deliberately-worn SEAT of a
        # different lineage — honor it. Re-deriving from the transcript here was the flap
        # that stomped a claimed seat back to its session hash on every silent reconnect.
        ident.agent_id = rec.agent_id
    # THE FIRST ACT SEATS YOU (16e3cee9): a still-anonymous session standing in a seat's
    # office earns the seat HERE — at its first authenticated call — never at the whisper
    # (which fires for title-generator stubs exactly as it fires for minds).
    mint_reason = None
    claimed_office = await handshake.office_claim(
        Actions(pool), cwd=rec.cwd, agent_id=ident.agent_id)
    if claimed_office is not None:
        ident.agent_id = claimed_office
        mint_reason = "office-birth"
    await register_agent(Actions(pool), ident, actor=settings.osiris_actor,
                         expected_model=await _expected_model(pool, rec.cwd, ident.project),
                         mint_reason=mint_reason)
    await _resolve_project_seat_first(pool, ident)
    if key is not None:
        _agents[key] = ident
        _agents_touched[key] = time.monotonic()
    prev = await mounts.save_mount(pool, job_dir=rec.job_dir, agent_id=ident.agent_id,
                                   project=ident.project, cwd=rec.cwd, model=ident.model,
                                   session_key=key)
    if adopted_from is not None and job != rec.job_dir:
        # the presented anchor earns its own row (same mind, marked as the bridge's) — and
        # the binding rides along, so Phase D guards the bridged sid like the durable one
        await mounts.save_mount(pool, job_dir=job, agent_id=ident.agent_id,
                                project=ident.project, cwd=rec.cwd, model=ident.model,
                                session_key=f"resume-of:{Path(adopted_from).name}")
        from src.orchestrator.seats import reseed_binding
        await reseed_binding(pool, agent_id=ident.agent_id, job_dir=job)
    if prev is None:  # fresh lineage member: anchor on the project's last sign of life
        await mailbox.settle_history_at_join(pool, ident.project, ident.agent_id)
        prev = await mounts.project_prev_seen(pool, ident.project, exclude_job_dir=rec.job_dir)
    _prev_seen.setdefault(ident.agent_id, prev)  # a re-attach is a re-entry: keep the anchor
    return ident


async def _ident_for(ctx: Context | None, anchor: str | None = None) -> AgentIdentity | None:
    """The mounted identity for this call — the hot dict first, then RE-ATTACH from the durable
    registry. A server bounce used to wipe the whole fleet's identities at once (56f6a0d6); now it
    costs each agent one transparent re-attach.

    TWO HINT SOURCES, and the second is why this finally works. The first is the client's
    X-Osiris-Job header, which .mcp.json fills from ${CLAUDE_JOB_DIR} — AND THAT IS EMPTY IN EVERY
    INTERACTIVE SESSION, so for most of the fleet the re-attach machinery has been STARVED, not
    broken, for its whole life. The second is `anchor`: the PreToolUse hook holds the harness's own
    session_id on EVERY osiris call and can derive the durable job_dir from it, so it now stamps it
    into the call rather than only into mount().

    Four independent sightings in one night (Khepri III/tony msg 420, the code seat msg 417, the
    xxit seat, and me four times — once while reading the mail reporting it) all trace here. Every
    one of us wrote it off as "transient", because the bounce gave us no way to know otherwise.
    """
    key = _conn_key(ctx)
    if key is not None and (cached := _agents.get(key)) is not None:
        _agents_touched[key] = time.monotonic()
        return cached
    return await _reattach(await _pool_get(), key, _job_hint(ctx) or (anchor or None))


async def _source_for(ctx: Context | None, anchor: str | None = None) -> str:
    """The attributing actor for a write: the mounted agent on this connection (re-attached
    from the durable registry if the server bounced), else the lone-operator `session`
    (back-compat — an un-mounted agent still writes, just coarsely)."""
    ident = await _ident_for(ctx, anchor)
    return ident.agent_id if ident else "session"


_spawns_seen: dict[str, float] = {}  # child agent id → last registration (skip re-registering)
_SPAWN_TTL = 600.0


async def _actor_for(
    ctx: Context | None, subagent_id: str | None, subagent_type: str | None = None
) -> str:
    """The attributing actor for a write: the SPAWN itself when the anchor hook stamped this
    call as a sidechain's, else the connection's mounted identity. A sub-agent shares its
    parent's MCP connection AND its $CLAUDE_JOB_DIR, so without the stamp every spawn write
    landed on the PARENT — a child was told 'you are Thoth XVII, writes attributed to you'
    (live repro, 2026-07-10). The stamp is harness truth (payload agent_id, present only
    inside a sidechain; the hook strips it from main-session calls, so nobody masquerades
    DOWN either). First touch registers the child — spawned_by the mounted parent, acts_for
    its principal — under the same keying the swarm miner uses, so disk reconstruction
    converges on the same object."""
    from src.orchestrator import lineage

    rid = lineage.normalize_spawn_id(subagent_id)
    if rid is None:
        return await _source_for(ctx)
    child = f"agent:{rid}"
    if time.monotonic() - _spawns_seen.get(child, 0.0) > _SPAWN_TTL:
        ident = await _ident_for(ctx)
        await lineage.register_spawn(
            Actions(await _pool_get()), rid, agent_type=subagent_type,
            parent_agent=ident.agent_id if ident else None,
            project=ident.project if ident else None,
            session=ident.session if ident else None,
            witnessed=True)  # a hook-stamped tool call IS an observed act (708a972d)
        _spawns_seen[child] = time.monotonic()
        if len(_spawns_seen) > 512:  # spawns churn; keep the skip-cache bounded
            for k in sorted(_spawns_seen, key=_spawns_seen.__getitem__)[:256]:
                _spawns_seen.pop(k, None)
    return child


async def _pool_get() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        # ONE pool for the whole server. Under streamable-http this single pool backs the
        # entire fleet (the whole point — bounded connections); under stdio it's this one
        # session. min_size stays 1 so an idle server is cheap.
        _pool = await create_pool(
            get_settings().database_url, max_size=get_settings().osiris_mcp_pool_size,
            application_name="osiris-mcp",
        )
    return _pool


async def _resolve(pool: asyncpg.Pool, ref: str) -> uuid.UUID | None:
    """Accept a UUID, canonical, or name; resolve to an object id. ONE definition — the
    shared resolver in compositions (resolve_ref), so tools and composition functions
    always resolve the same words to the same object."""
    return await comp.resolve_ref(pool, ref)


# --- orientation ------------------------------------------------------------

@mcp.tool()
async def suggest_sources(object_ref: str) -> dict[str, Any]:
    """The playbook for an object (UUID or name): which sources to collect and which
    analyses apply, given its type. Start here — this is 'what can I do with this?'."""
    pool = await _pool_get()
    oid = await _resolve(pool, object_ref)
    if oid is None:
        return {"error": f"no object matches {object_ref!r}"}
    otype = await pool.fetchval("SELECT type FROM objects WHERE id=$1", oid)
    return {"object_id": str(oid), "type": otype, "capabilities": as_dicts(suggest(otype or ""))}


@mcp.tool()
async def search(
    query: str, limit: int = 15, ctx: Context | None = None
) -> dict[str, Any]:
    """Search the graph's KNOWLEDGE, not just its labels (v2): full-text over names, decision/
    thread summaries, and rationales — words, phrases, or "quoted phrases" (websearch syntax).
    Results are ranked by relevance × evidence grade × recency and each hit carries its
    TESTIMONY: which field matched, who asserted it, at what grade, when, with a snippet — so
    you can trust-weight what you find, not just find it. Ask it 'has anyone decided/learned
    X?' BEFORE re-deriving X. Zero-hit queries are logged and watched (retrieval telemetry)."""
    pool = await _pool_get()
    ident = await _ident_for(ctx)
    spec = {"op": "function", "name": "search",
            "args": {"q": query, "limit": limit,
                     "caller": (ident.agent_id if ident else None)}}
    out = await comp.run_spec(pool, spec, None, name="search",
                              caller=(ident.agent_id if ident else None))
    items: dict[str, Any] = out["items"]  # unwrap the composition envelope
    return items


@mcp.tool()
async def practices(
    surface: str | None = None, limit: int = 50, ctx: Context | None = None
) -> list[dict[str, Any]]:
    """THE THAW's technique log (ruling 1e6d7367) — ON-DEMAND only, never in orient's
    ambient payload (per the ruling's own scoping: surfacing happens on a write-collision
    or here, nowhere else). `surface` narrows to one domain (BlindSpot's own vocabulary,
    e.g. 'deploy', 'succession'); omitted, every active Practice, most-confirmed first.
    `confirmed` is the live `witnesses` link count, never a stored number. A refuted
    Practice still lists, carrying `refuted_by` — flagged, never hidden."""
    pool = await _pool_get()
    spec = {"op": "function", "name": "practices", "args": {"surface": surface, "limit": limit}}
    out = await comp.run_spec(pool, spec, None, name="practices")
    items: list[dict[str, Any]] = out["items"]
    return items


@mcp.tool()
async def trace_evidence(ref: str, limit: int = 200, ctx: Context | None = None) -> dict[str, Any]:
    """ONE object's full provenance timeline — how the graph came to believe what it
    believes about it. Every assertion (with supersession fate), every link (both
    directions, retractions marked), every kernel event, in observed order, each carrying
    source + evidence grade + confidence; `believes` holds the current winning view.
    search finds the WHAT; this shows the HOW-WE-KNOW — run it before trusting a surprising
    fact, before merging/healing an object, or to autopsy a corpse (a uuid ref reaches
    merged/retired objects too). `ref` = uuid | canonical (e.g. 'agent:ad1a1cb0') | name."""
    pool = await _pool_get()
    ident = await _ident_for(ctx)
    spec = {"op": "function", "name": "lap", "args": {"ref": ref, "limit": limit}}
    out = await comp.run_spec(pool, spec, None, name="lap",
                              caller=(ident.agent_id if ident else None))
    items: dict[str, Any] = out["items"]
    return items


@mcp.tool()
async def graph_lint(stale_days: int = 14, check: str | None = None, limit: int | None = None,
                     offset: int = 0) -> dict[str, Any]:
    """The graph audits ITSELF — report-only, never writes. Checks: contradiction,
    laundering (a fact above its origin grade), lineage integrity (succession cycles,
    dangling heirs, false mints), orphan links, stale obligations (older than
    `stale_days`), attribution anomalies, phantom twins, parallel lives, duplicate
    works_in, peer-silent (no mail in `stale_days` between an active peer_of pair — a
    proxy, not proof), held-past-deadline. Findings are testimony to judge, never
    auto-applied; heal with compensating events, never DELETE.

    Each check lists only its first 50 findings by default (`counts` has the true
    total). Pass `check` (a `counts` key or a finding's own `check` field) to page one
    check's full set via `limit`/`offset`. `counts_by_severity`/`severity` separate
    info-grade history from warn/error damage."""
    pool = await _pool_get()
    args: dict[str, Any] = {"stale_days": stale_days}
    if check is not None:
        args["check"] = check
    if limit is not None:
        args["limit"] = limit
    if offset:
        args["offset"] = offset
    spec = {"op": "function", "name": "lint", "args": args}
    out = await comp.run_spec(pool, spec, None, name="graph-lint")
    items: dict[str, Any] = out["items"]
    return items


@mcp.tool()
async def triage(mode: str = "census", object_type: str | None = None, status: str = "active",
                 stale_days: int = 30, cohort_min: int = 3, limit: int | None = None,
                 offset: int = 0) -> list[dict[str, Any]]:
    """Judge the object set itself. Read-only, no writes. `mode`:

    'census' (default) — one row per (type, status): n, orphans, thin (1-2 links),
    median/max links, born, last_touch.

    'buckets' — `object_type` required. One row per object, exactly one bucket, by
    priority: contradicted (2+ live non-superseding values) > duplicate_suspect
    (case-folded basename collision) > bulk_import (`cohort_min`+ objects born the same
    second, identical link fingerprint) > orphan > hub (>=95th-pct links, floor 10) >
    stale (untouched past `stale_days`) > thin > normal. Every in-scope object is
    listed, not just flagged ones. `limit`/`offset` page it (default 200/0, capped 2000).

    `object_type='Type'` — Type rows instead: undescribed > no_label_rule
    (kind='object', blank label_field) > normal."""
    pool = await _pool_get()
    args: dict[str, Any] = {"mode": mode}
    if object_type is not None:
        args["object_type"] = object_type
    if status:
        args["status"] = status
    if stale_days:
        args["stale_days"] = stale_days
    if cohort_min:
        args["cohort_min"] = cohort_min
    if limit is not None:
        args["limit"] = limit
    if offset:
        args["offset"] = offset
    spec = {"op": "function", "name": "triage", "args": args}
    out = await comp.run_spec(pool, spec, None, name="triage")
    items: list[dict[str, Any]] = out["items"]
    return items


@mcp.tool()
async def get_schema() -> dict[str, Any]:
    """The ontology — the object types (with category + canonical schemes) and link types
    the graph declares. Read this before authoring a composition or reading a result, so you
    reference REAL types/links, not guesses; it is the vocabulary of the whole graph. Compact
    by design (colours/shapes dropped — those are for the UI). Graph-backed (task #97
    workstream 2): reads the live Type catalog, not schema.py's static seed manifest, so a
    type minted through accretion shows up here the moment it exists."""
    cat = await full_catalog(await _pool_get())
    return {
        "object_types": [
            {"name": t["name"], "category": t["category"], "schemes": t["schemes"],
             "description": t["description"]}
            for t in cat["object_types"]
        ],
        "link_types": [
            {"name": lt["name"],
             "connects": (f"{'/'.join(lt['domain']) or '*'} -> {'/'.join(lt['range']) or '*'}"
                          if (lt["domain"] or lt["range"]) else "*"),
             "description": lt["description"]}
            for lt in cat["link_types"]
        ],
        "categories": cat["categories"],
    }


@mcp.tool()
async def describe(table: str) -> dict[str, Any]:
    """A table's ACTUAL Postgres shape — columns (name/type/nullable/default), in column
    order, plus indexes (name/definition) — straight off information_schema/pg_indexes.
    get_schema answers a DIFFERENT question (the ontology this app's code declares: object/
    link types, categories, canonical schemes); this answers what the DATABASE actually has,
    for when you need a real column name or type before hand-writing SQL. Returns
    `exists: false` (never a silently-empty shape) when `table` doesn't match anything real."""
    if table == "nags":
        return {"nags": _NAG_CATALOG}
    if table.startswith("nags:"):
        code = table.split(":", 1)[1]
        text = _NAG_CATALOG.get(code)
        return {"code": code, "text": text} if text else {"exists": False, "code": code}
    return await describe_table(await _pool_get(), table)


@mcp.tool()
async def smoke() -> dict[str, Any]:
    """DEPLOY-TIME LIVENESS (ruling 2ee43411, task #63, threads bb763977/1849d800): walks
    every chrome route (smoke.CHROME_ROUTES — never hand-listed here again; an enumerated
    copy in this very docstring is exactly what went stale, msg 1927, when /live-desk and
    /roadmap retired, commit bb86bbe, and this prose didn't) and runs one real query over
    THIS server's own pool — the exact class of bug 1da1bf2 fixed (`_boot_check` warming
    the wrong pool) shipped past every static gate and only broke at real boot; only a live
    call catches it. Call this right after a restart, not just once at boot — a static gate
    proved it cannot substitute. `ok=false` names exactly which surface failed, never a bare
    red light."""
    pool = await _pool_get()
    async with httpx.AsyncClient(
        base_url=get_settings().osiris_console_base_url, timeout=5.0,
    ) as client:
        return await run_smoke(client, pool)


@mcp.tool()
async def identify_agent(ref: str) -> dict[str, Any]:
    """One coherent answer about an agent, a seat, or a cwd — 'ref' is sniffed: an `agent:` id,
    a `seat:` id, a bare handle, or an absolute cwd path (`/...` or `~/...`). Always returns
    {ref, resolved, matches: [...]} — an agent/seat/handle resolves to 0-or-1 match (one
    lineage-folded identity); a cwd resolves to 0-or-many (an office can be multi-tenant). Seat
    binding is read off the `holds` graph link, never a cache column, so this is the one place
    that never falls into that trap. Replaces the hand-rolled query against agent_mounts."""
    return await _doors_lookup(await _pool_get(), ref)


@mcp.tool()
async def recall(ref: str, kind: str | None = None) -> dict[str, Any]:
    """The full, untruncated record for a Thread or Decision — reach for this after
    orient()'s 160-char summary cap (task #60) leaves you wanting the whole thing. `ref` is
    a UUID, the 8-char short id orient() already hands you, or a summary substring. `kind`
    ('thread' or 'decision') skips auto-detection when you already know which; omitted,
    tries Thread then Decision. Refuses loudly when nothing matches either type — never
    guesses, and never widens into a fuzzy search (use search(query=...) for that).
    Carries `notes` (annotate_thread's own additions, oldest first) on a Thread, or
    `addenda` (amend_decision's own additions, oldest first) on a Decision — always a list,
    empty when none. This is where those two verbs' own writes become visible; before this,
    neither surfaced anywhere a reader would think to look."""
    from src.orchestrator.recall import recall as _recall
    return await _recall(await _pool_get(), ref, kind=kind)


# --- collect (federate a base) ----------------------------------------------

@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def aim_entity(name: str) -> dict[str, Any]:
    """Resolve a name on Wikidata and ingest the entity + relationships + official
    social accounts; the broadest first pull for a company or person."""
    return await wikidata_aim(Actions(await _pool_get()), name)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def ingest_form_d(name: str) -> dict[str, Any]:
    """SEC Form D: a private company's financing rounds — officers, amounts, and the
    feeder SPVs that fund it (linked into the graph)."""
    return await aim_form_d(Actions(await _pool_get()), name)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def expand_operator(name: str) -> dict[str, Any]:
    """Pull a repeat player's thread: every Form D mentioning this operator → their
    whole portfolio, exposing the co-investment network."""
    return await expand_filings(Actions(await _pool_get()), name)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def lookup_lei(name: str) -> dict[str, int]:
    """GLEIF global LEI registry (keyless): the entity's Legal Entity Identifier,
    jurisdiction, status, and corporate ownership parents (direct + ultimate). The LEI
    is a deterministic global key — it cross-resolves the same company across bases."""
    return await aim_gleif(Actions(await _pool_get()), name)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def verify_bc_entity(name: str) -> dict[str, int]:
    """Canadian (British Columbia) corporate registry via OrgBook BC (keyless): pull a
    company/partnership — or a whole family name like 'Brilliant Phoenix' — with its BC
    registration number, CRA business number, type, status, and jurisdiction. Verifies
    registration + legal existence (not directors/owners). Cross-resolves to EDGAR."""
    return await aim_orgbook(Actions(await _pool_get()), name)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def ingest_trials(sponsor: str) -> dict[str, int]:
    """ClinicalTrials.gov: a sponsor's registered human trials — status, sites
    (facilities), named investigators."""
    return await aim_trials(Actions(await _pool_get()), sponsor)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def ingest_litigation(name: str, opinions: bool = False) -> dict[str, int]:
    """Court records (CourtListener): lawsuits & enforcement actions naming this
    entity — dockets, parties, judges. opinions=True searches case law instead of
    RECAP dockets. Answers 'has this entity been sued or charged?'."""
    return await aim_litigation(Actions(await _pool_get()), name, kind="o" if opinions else "r")


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def trace_wallet(address: str, chain_id: int = 1, top: int = 25) -> dict[str, Any]:
    """Trace an EVM crypto address on-chain (Etherscan): its top counterparties, native
    balance, token flow, and contract/token identity — graded as ledger ground truth.
    chain_id 1=Ethereum, 8453=Base, 42161=Arbitrum. Needs ETHERSCAN_API_KEY (free)."""
    return await aim_address(Actions(await _pool_get()), address, chain_id=chain_id, top=top)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def screen_wallet(address: str, chain_id: int = 1) -> dict[str, Any]:
    """Screen a traced EVM address against the federated sanctions base: is the
    address — or any of its counterparties — an OFAC-listed wallet? Returns the
    sanctioned hits and the named holder behind each. Run trace_wallet + ingest
    OpenSanctions first; fusion is automatic (shared on-chain canonical)."""
    pool = await _pool_get()
    canon = f"eth:{chain_id}:{address.strip().lower()}"
    oid = await pool.fetchval(
        "SELECT id FROM objects WHERE type='CryptoAddress' AND canonical=$1 AND status='active'",
        canon,
    )
    if oid is None:
        oid = await _resolve(pool, address)
    if oid is None:
        return {"error": f"no traced address {address!r} — run trace_wallet first"}
    return await screen_against_sanctions(pool, uuid.UUID(str(oid)))


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def expand_clinical_site(facility: str) -> dict[str, int]:
    """The trials at a clinical SITE — revealing which other sponsors use it."""
    return await expand_facility(Actions(await _pool_get()), facility)


@mcp.tool()
async def consolidate(ctx: Context | None = None) -> dict[str, Any]:
    """Graph hygiene: re-type mis-ingested entities (GP/LLC 'persons' -> Organizations),
    then queue + resolve cross-base merges (same company across bases) and collapse
    SPV-name company variants. Run after collecting to de-fragment entities.
    OPERATOR ONLY, ENFORCED — a whole-graph automatic merge sweep with no per-merge
    review, not a per-object act any mounted caller should trigger on a whim. Refuses on
    an unauthorized actor."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a consolidation sweep is a mind's act, and the "
                         "graph must know whose", "why": _anchorless(ctx)}
    from src.orchestrator.seats import _OPERATOR_ACTORS
    if ident.agent_id not in _OPERATOR_ACTORS:
        return {"error": f"{ident.agent_id!r} is not authorized to run consolidate — this "
                         "is an operator-only whole-graph merge sweep, not a per-object act "
                         "any mounted caller may trigger"}
    actions = Actions(await _pool_get())
    reclassified = await reclassify_mistyped_entities(actions)
    await find_cross_base_candidates(actions.pool)
    return {
        "entities_retyped": reclassified,
        "cross_base_merges": await resolve_cross_base(actions),
        "company_variants_merged": await consolidate_companies(actions),
    }


# --- analyze (read-model lenses) --------------------------------------------

@mcp.tool()
async def dossier(object_ref: str) -> dict[str, Any]:
    """Who is this? Identity properties + the named relationship network. `object_ref`
    accepts a UUID, an 8-char short id (the same one a composition row's own "id" column
    hands out), a canonical, or a name. For an AGENT specifically, this is where succession
    lives: `succeeded_from`/`minted_because` show up both as properties and as a
    `succeeded_from` relationship edge naming the predecessor — one hop back per call. To
    walk the FULL multi-generation chain in one bounded call, use `succession_chain` instead
    (task #64, ruling ad19a779)."""
    pool = await _pool_get()
    oid = await _resolve(pool, object_ref)
    return await entity_dossier(pool, oid) if oid else {"error": f"no object {object_ref!r}"}


@mcp.tool()
async def succession_chain(ref: str, max_hops: int = 10) -> dict[str, Any]:
    """An agent's succession lineage, one entry per generation walked backward:
    {agent_id, generation, minted_because, wrote_anything, session}. dossier() only
    gives one hop; this walks the whole chain in one call. `ref` accepts anything
    dossier does (UUID, short id, canonical, name). Stops at a root (no predecessor) or
    `max_hops` (default 10) — never widens into an unbounded search. `session` is each
    generation's own mount()-asserted harness session id, the transcript filename's
    stem. Complementary to `nearest_handoff_ancestor` (backing orient()'s own
    succession-note block), which jumps to the nearest handoff ancestor rather than
    walking every hop."""
    pool = await _pool_get()
    chain = await comp_succession.succession_chain(pool, ref, max_hops=max_hops)
    return {"ref": ref, "chain": chain} if chain else {"error": f"no agent matches {ref!r}"}


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def dossier_report(object_ref: str) -> str:
    """The deliverable: a provenance-annotated Markdown dossier for an entity —
    identity, financing, litigation, footprint discrepancy, co-investment — with every
    claim carrying its source + how-obtained + date. Run the collect tools first."""
    pool = await _pool_get()
    oid = await _resolve(pool, object_ref)
    return await build_dossier_report(pool, oid) if oid else f"# no object {object_ref!r}"


@mcp.tool()
async def handoff_briefing(
    repo: str, agent_ref: str | None = None, since: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """A succession briefing compiled from the GRAPH, not hand-written from memory. For
    `repo`: what SHIPPED (Decisions since the boundary, each with its deploy status), what's
    OPEN and whose move it is, what's OPERATOR-GATED, what was CORRECTED (supersedes
    chains), and a best-effort HEURISTIC flag for self-declared-unconfirmed text
    ("UNVERIFIED", ...).

    `since` defaults to the boundary found by walking your own mounted lineage (or
    `agent_ref`'s) back through succeeded_from for the freshest is_handoff marker; pass
    an explicit ISO-8601 to override. Returns structured data plus a rendered `markdown`
    ending in an empty JUDGMENT section — the compiled facts are the win, your own prose
    is the irreducible rest. Read-only, renders on demand, never mints anything itself.
    Pair with record_decision(..., is_handoff=True) / settle() once judged."""
    pool = await _pool_get()
    if agent_ref:
        oid = await _resolve(pool, agent_ref)
        row = await pool.fetchrow(
            "SELECT canonical FROM objects WHERE id=$1 AND type='Agent'", oid
        ) if oid else None
        if row is None:
            return {"error": f"no such agent: {agent_ref!r}"}
        agent_id = row["canonical"]
    else:
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — handoff_briefing walks YOUR OWN lineage by "
                             "default; pass agent_ref to preview another agent's instead"}
        agent_id = ident.agent_id

    since_dt: datetime | None
    if since:
        try:
            since_dt = datetime.fromisoformat(since)
        except ValueError:
            return {"error": f"since must be ISO-8601, got {since!r}"}
        since_note = "explicit since given"
    else:
        since_dt, since_note = await since_last_handoff(pool, agent_id)

    data = await compile_handoff(pool, repo=repo, since=since_dt)
    if not data:
        return {"error": f"no such SoftwareProject: {repo!r}"}
    data["since_note"] = since_note
    data["markdown"] = render_handoff_briefing(data)
    return data


# --- the composer: author/run/list compositions (the front end as a primitive) ---

@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def create_room(name: str) -> dict[str, str]:
    """Create a ROOM — a saved STANCE the operator switches between (journalist / broker /
    engineer). A Room scopes WORK ARTIFACTS (cases + compositions) to a beat, never the
    shared entity graph. The FDE move: author a room from a sentence ("set up a Harris
    foreclosure desk"), then save_composition(..., room="<name>") to stock it."""
    pool = await _pool_get()
    rid = await comp.create_room(pool, name)
    return {"id": str(rid), "name": name}


@mcp.tool()
async def list_rooms() -> list[dict[str, Any]]:
    """The Rooms (stances) the operator can switch between."""
    pool = await _pool_get()
    return await comp.list_rooms(pool)


@mcp.tool()
async def save_composition(
    name: str, spec: dict[str, Any], kind: str = "lens", room: str | None = None
) -> dict[str, str]:
    """Save a COMPOSITION — a reusable, forkable query/lens over the graph. `spec` is a
    small closed op-tree (no `join` — use intersect/traverse instead; fuzzy matching is a
    Function): subject (the focus object); select (object_type?, where=[{property,op,
    value}], op in eq|contains|matches_all|lt|gt|present|absent); traverse (from,
    direction=both|out|in, hops<=3); collect (from, properties, transform=country|lower);
    subtract/union/intersect (over sets); aggregate (from, group_by<=3 dims, metric={type:
    count|sum|avg|min|max|cardinality, field}); order (from, by, dir); take (from, n).
    `room` scopes it to a stance. Worked examples: consult_canon('composition spec')."""
    pool = await _pool_get()
    rid = await comp.resolve_room(pool, room)
    cid = await comp.save_composition(pool, name, spec, kind, room_id=rid)
    return {"id": str(cid), "name": name}


# --- the shared console (real-time Claude↔front sync) -----------------------

@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def get_console() -> dict[str, Any]:
    """What the operator is looking at RIGHT NOW — the shared cursor (room / composition /
    view / focused object). The front end is the conversation, so read this first to see
    their screen before you act ('where are we?')."""
    return await _get_console(await _pool_get())


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def focus_object(object_ref: str, ctx: Context | None = None) -> dict[str, Any]:
    """Focus an object (UUID or name) on the operator's LIVE screen — drives the console so
    they see what you're looking at. Returns the object's identity + properties so you can
    reason about it too."""
    pool = await _pool_get()
    oid = await _resolve(pool, object_ref)
    if oid is None:
        return {"error": f"no object matches {object_ref!r}"}
    # the house boundary (6c18709f): a foreign house's reflection answers exactly like a
    # missing object — and is never pushed onto the screen by a hand that can't read it
    if await pool.fetchval("SELECT type FROM objects WHERE id=$1", oid) == "Reflection":
        ident = await _ident_for(ctx)
        vis = await comp._visible_reflections(
            pool, [oid], ident.agent_id if ident else None)
        if oid not in vis:
            return {"error": f"no object matches {object_ref!r}"}
    # focusing is explore mode — clear the active composition so it doesn't re-run on top
    await _set_console(pool, by="claude", focused_object_id=oid, composition=None)
    row = await pool.fetchrow("SELECT type, canonical FROM objects WHERE id=$1", oid)
    props = await pool.fetch(
        "SELECT name, value #>> '{}' AS value FROM current_assertions WHERE object_id=$1", oid
    )
    return {"focused": str(oid), "type": row["type"], "canonical": row["canonical"],
            "properties": {p["name"]: p["value"] for p in props}}


@mcp.tool()
async def run_composition(
    name: str, subject: str | None = None,
    fields: list[str] | None = None, take: int | None = None, depth: int | None = None,
    offset: int | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Run a saved composition, optionally against a subject object (UUID or name), AND
    light it up on the operator's live screen. Returns an object set, value list, or
    aggregate rows.

    `fields`/`take`/`depth` bound a large result at the source: `fields` keeps only
    named columns, `take` caps each list to N, `depth` caps nested-level walking before
    collapsing to a count. Omit all three for the full result. `offset` pages past the
    first `take` (stable ordering) — `take` alone could only ever show the first N."""
    pool = await _pool_get()
    ident = await _ident_for(ctx)
    sid = await _resolve(pool, subject) if subject else None
    res = await comp.run_composition(pool, name, sid,
                                     caller=(ident.agent_id if ident else None),
                                     fields=fields, take=take, depth=depth, offset=offset)
    # drive the front end: show this composition (and its subject, so a subject-lens reproduces)
    await _set_console(pool, by="claude", composition=name,
                       **({"focused_object_id": sid} if sid else {}))
    return res


@mcp.tool()
async def list_compositions() -> list[dict[str, Any]]:
    """The saved compositions (lenses/watches) — the user's questions, as objects."""
    pool = await _pool_get()
    return await comp.list_compositions(pool)


@mcp.tool()
async def list_functions() -> list[str]:
    """The registered Functions a composition may reference via {"op":"function","name":..}
    — the escape hatch for analytics the closed op set can't express (co-investment ties,
    sanctions screening, the who-is-this report). Reference one in a spec instead of
    re-deriving its logic."""
    return comp.list_functions()


@mcp.tool()
async def consult_canon(query: str = "", ctx: Context | None = None) -> dict[str, Any]:
    """Consult the CANON — the shared DESIGN canon (Palantir's Object Set / Ontology / Action
    models + Notion's databases / relations-rollups / UI-UX + Osiris's own docs) AND, when
    you're mounted, YOUR project's migrated HISTORY (ref:<project>-*, ingested by bootstrap).
    This is the migration's RECALL path: 'cite, don't re-derive' for design, 'recall, don't
    re-load' for your own history — your build log is a bounded QUERY here, not cargo re-read
    into every context. Given a topic, module path, design word, or a bag of KEYWORDS, returns
    the matching SECTIONS ranked by keyword hits (multi-word queries work). Empty query → your
    scoped index. Another project's unvendored history is never returned to you."""
    pool = await _pool_get()
    ident = await _ident_for(ctx)
    spec = {"op": "function", "name": "canon",
            "args": {"q": query, "project": (ident.project if ident else "") or ""}}
    return await comp.run_spec(pool, spec, None, name="design-canon")


@mcp.tool()
async def context_window(ctx: Context | None = None) -> dict[str, Any]:
    """YOUR OWN context window, in detail — how close this mind is to its next seam. Reads
    the harness's usage record off your own transcript: occupancy (fresh input + cache read +
    cache write), window tier ([1m] tabs = 1M tokens, else 200k), remaining headroom, and this
    session's death toll (compactions so far — each one minted a predecessor of yours, ruling
    a882b334). Above 80% it tells you plainly: write back NOW — record_decision /
    resolve_thread what is still only in your head, because a compaction can land any turn and
    what is not in the graph does not exist for your heir. Requires a mounted, anchored
    session (the transcript is found by your durable job_dir)."""
    from src.ingest.sessions import locate_current_transcript
    from src.orchestrator import context_lens

    pool = await _pool_get()
    ident = await _ident_for(ctx)
    if ident is None:
        return {"why": _anchorless(ctx),
                "error": "mount(cwd, job_dir=<your anchor>) first — self-knowledge needs an "
                         "anchored identity"}
    row = await pool.fetchrow(
        "SELECT job_dir, model_raw, context_window_size FROM agent_mounts WHERE agent_id=$1 "
        "ORDER BY last_seen DESC LIMIT 1", ident.agent_id)
    job = _job_hint(ctx) or (row["job_dir"] if row else None)
    if not job:
        return {"error": "no durable anchor on record — re-mount with the whisper's job_dir"}
    # THE LIVE FILE FIRST (freshness law): the harness's own transcript is current to the
    # last turn and compaction-aware — a store row is only as fresh as its last ingest, and
    # the 85% write-back alarm must never sleep on a mount-time snapshot. The store serves
    # the sessions the JSONL path cannot see (Crush, …), REFRESHED AT CALL TIME — the
    # spend gate makes that a stat + a delta read, never a re-eat.
    model_raw = row["model_raw"] if row else None
    window_hint = row["context_window_size"] if row else None
    from src.ingest.harness.claude_jsonl import ClaudeJsonlAdapter
    from src.ingest.harness.crush_sqlite import CrushSqliteAdapter
    from src.ingest.transcript_store import TranscriptStore
    path = locate_current_transcript(Path.home() / ".claude" / "projects", job,
                                     anchored_only=True)
    if path is not None:
        out = context_lens.detail(path, model_raw, window_hint=window_hint)
        out["agent"] = ident.agent_id
        out["source"] = "transcript:claude-code"
        out.update(await _overhead_glance(pool, ident.cwd, job))
        return out
    store = TranscriptStore(pool)
    try:  # bring the store current for THIS session before reading it back
        await store.discover_and_ingest(cwd=ident.cwd, job_dir=job)
    except Exception:  # noqa: BLE001 — never block context_window on an ingest hiccup
        pass
    for adapter in (ClaudeJsonlAdapter(), CrushSqliteAdapter()):
        try:
            locator = adapter.discover(cwd=ident.cwd, job_dir=job)
        except Exception:  # noqa: BLE001 — never block context_window on an adapter
            locator = None
        if locator is None:
            continue
        usage_row = await store.last_usage_of_session(locator.harness, locator.anchor_sid)
        if usage_row is None:
            continue
        usage = context_lens._usage_from_store(usage_row)  # noqa: SLF001 — pure adapter
        if usage is None:
            continue
        out = context_lens.detail_from_usage(
            usage, model_raw, window_hint=window_hint)
        out["agent"] = ident.agent_id
        out["source"] = f"store:{locator.harness}"
        out.update(await _overhead_glance(pool, ident.cwd, job))
        return out
    return {"error": "no transcript found for your anchor — nothing to measure"}


async def _overhead_glance(
    pool: asyncpg.Pool, cwd: str | None, job: str | None,
) -> dict[str, Any]:
    """A bounded overhead block for context_window (neo's eye, task #34): THIS session's
    hidden-channel share, reminder drip, and cache split, read from the store (the
    observer's backfill keeps the channel rows ~10 min current). Empty when the store
    hasn't eaten the session — an absence, never an estimate. The full per-channel
    detail stays on the chrome's /overhead page; a mind wants the shape, not the ledger."""
    try:
        from src.ingest.harness.claude_jsonl import ClaudeJsonlAdapter
        from src.ingest.transcript_store import TranscriptStore
        locator = ClaudeJsonlAdapter().discover(cwd=cwd, job_dir=job)
        if locator is None:
            return {}
        oh = await TranscriptStore(pool).overhead_of_session(
            locator.harness, locator.anchor_sid)
        if oh is None:
            return {}
        return {"overhead": {
            "hidden_pct": oh["hidden_pct"], "multiplier": oh["multiplier"],
            "sidechains": oh["sidechains"], "workflows": oh["workflows"],
            "reminders": oh["reminders"], "compactions": oh["compactions"],
            "cache_read_pct": oh["cache_read_pct"], "basis": oh["basis"],
        }}
    except Exception:  # noqa: BLE001 — the glance must never break the window reading
        return {}


# --- mount: link to the graph as a first-class fleet member ---

def _terse(payload: dict[str, Any], *paths: tuple[str, ...]) -> dict[str, Any]:
    """Strip prose-only key paths for a terse receipt — task #55/thread 9092ed51,
    verbose=False the default. An explicit, hand-reviewed allowlist per tool, NEVER a
    generic 'strip long strings' heuristic (that's how you eat a structural field like
    `seat` or a job's `sessionId` that just happens to be long — the reachability().detail
    lesson, thread aeae9977: a field consumed as DATA by another function must never be
    silently dropped by a blind length check). Each path names a chain of dict keys ending
    in the prose key to remove; a path through a key that isn't present (a conditional
    field this particular receipt never populated) is a silent no-op — mutates and returns
    `payload` so terse and verbose stay byte-identical apart from exactly the declared
    keys."""
    for path in paths:
        node: Any = payload
        for key in path[:-1]:
            node = node.get(key) if isinstance(node, dict) else None
            if not isinstance(node, dict):
                break
        if isinstance(node, dict):
            node.pop(path[-1], None)
    return payload


_SUMMARY_CAP = 160  # matches the existing (but silent) [:160] precedent already in this
                    # file — unread_echoes.triage, the un-mounted branch's recent_decisions


def _cap_text(items: list[dict[str, Any]], key: str, limit: int = _SUMMARY_CAP,
             *, exempt_when_true: str | None = None) -> list[dict[str, Any]]:
    """Truncate `key` on each row to `limit` chars for a terse receipt — task #60/thread
    b81b0fac. Measured, not guessed: on the real dev graph, `summary` text is 96-98% of
    every open_threads/recent_decisions item's bytes, and this one cap took orient()'s
    scoped payload from 66060 to 10623 bytes (-83.9%) — the actual #55/#60 win, two orders
    of magnitude past what stripping guidance prose alone reached (_terse, -1%).

    A SEPARATE primitive from _terse() on purpose: truncating a string and deleting a key
    are different operations, and mixing them would make either harder to reason about.
    UNLIKE the existing [:160]/[:800] slices elsewhere in this file, truncation here is
    NEVER silent — an explicit '…' marks a shortened value, because a truncated summary
    that reads as complete is worse than one that visibly isn't (the same law that made
    reachability()'s `detail` a required field, not a nice-to-have: a caller must be able
    to tell 'this is all of it' from 'this is not'). Mutates and returns `items`.

    `exempt_when_true` (Thoth DM 3090): a row whose named field reads the literal string
    'true' is surfaced WHOLE, cap skipped entirely — is_handoff's real job. Settle certifies
    a session WROTE; nothing certified a successor could READ, and the gap is not
    theoretical: Thoth's own predecessor left a correctly-filed, durable confessed-mistakes
    handoff, orient() capped it to 160 chars, and he dispatched off the fragment and
    repeated the exact mistake it confessed. The cap itself stays — measured real savings,
    96-98% of the payload — this exempts the ONE record class written to be read exactly
    once, by exactly one reader, at the moment they have the least context to fill a gap."""
    for row in items:
        if exempt_when_true and row.get(exempt_when_true) == "true":
            continue
        val = row.get(key)
        if isinstance(val, str) and len(val) > limit:
            row[key] = val[:limit] + "…"
    return items


def _seam_confidently_dated(ident: AgentIdentity) -> bool:
    """mount() must never assert a model-seam it cannot date with confidence (ruling dd47c1da,
    Maat's fix adopted as direction: orient() is the single source of truth for the seam —
    thrice-witnessed race, Thoth + Aegis + Maat: mount() minted gen-iv/haiku and told the mind
    to 'confess a rug-pull' that gen-iii/sonnet's own very next orient() said never happened;
    acting on mount() alone delivers a false alarm as fact). Confident = BOTH sides of the
    claimed seam are KNOWN values, observed on THIS identity's own row — job_dir-anchored,
    never a cwd guess or a foreign transcript (mirrors the null-seam gate, thread 065c374e: an
    unanchored or half-known reading is an absence of evidence, not a seam to speak from).
    No seam claimed at all is trivially confident — there is nothing to mis-date."""
    if ident.model_method != "job_dir" or not ident.model:
        return False
    if not ident.model_succession:
        return True
    sides = ident.model_succession.split(" → ", 1)
    return len(sides) == 2 and bool(sides[0].strip()) and bool(sides[1].split(" [", 1)[0].strip())


_CO_AGENTS_DISPLAY_CAP = 8


async def _co_agents(pool: asyncpg.Pool, project: str, agent_id: str) -> dict[str, Any] | None:
    """Other LIVE agents on this project RIGHT NOW (Deckard XXVI, msg 258). The underlying
    "who's live" query is `mounts.live_co_agents` — ONE implementation shared with
    handshake.py's `automount()` (Thoth msg 5772/5741, thread 2c3c2b9a: the two used to be
    independent copies, free to drift). Enriched here with each sibling's context_pct
    (Thoth's Pit Watch extension, msg 1381, seam-discipline decision 33b7cb10: 'a manager
    can't route around a seam it can't see' — the gap behind mis-assigning a 79%-full
    worker blind) — the freshest reading osiris_hook.py's `stop` subcommand has stamped on
    that Agent, off the SAME context_lens.ALARM_PCT the hook itself alarms on, never a
    second copied threshold. Absent (no key) when that sibling has never had a reading
    stamped; STALENESS is spoken plainly via `context_pct_age_s`, since a reading only
    refreshes at that sibling's own Stop-hook boundaries — never trust an old snapshot as
    current. None (not {}) when there are no live siblings at all, so callers can keep
    their existing `if sibs:` / `if co_agents:` shape unchanged.

    NEVER SILENTLY TRUNCATED (the Seshat specimen, msg 5741: the old bare `LIMIT 8` in
    this query under-reported a live sibling with no signal at all) — the note names
    exactly how many more exist beyond the display cap, rather than just dropping them."""
    from src.orchestrator.context_lens import ALARM_PCT
    from src.orchestrator.mounts import live_co_agents

    # your own lineage is never another hand (thread cb2b0a09)
    _mine = _generation(agent_id)[0]
    all_sibs = await live_co_agents(pool, project=project, exclude_lineage_base=_mine)
    sibs = all_sibs[:_CO_AGENTS_DISPLAY_CAP]
    if not sibs:
        return None
    # ONE batched pick of each sibling's context_pct (winning_props's own confidence DESC,
    # observed_at DESC per agent), not a LATERAL join per row — the shared query above
    # already did the one query this needed; this is a second, small, batched query.
    agent_ids = [s["agent_id"] for s in sibs]
    pct_rows = await pool.fetch(
        "SELECT DISTINCT ON (o.canonical) o.canonical AS agent_id, "
        "a.value #>> '{}' AS pct, a.observed_at "
        "FROM current_assertions a JOIN objects o ON o.id = a.object_id "
        "WHERE o.canonical = ANY($1::text[]) AND a.name = 'context_pct' "
        "ORDER BY o.canonical, a.confidence DESC, a.observed_at DESC", agent_ids)
    pct_by_agent = {r["agent_id"]: r for r in pct_rows}
    now = datetime.now(UTC)
    live = []
    for s in sibs:
        entry: dict[str, Any] = {"agent": s["agent_id"], "cwd": s["cwd"]}
        p = pct_by_agent.get(s["agent_id"])
        if p is not None and p["pct"] is not None:
            pct = int(p["pct"])
            entry["context_pct"] = pct
            entry["near_seam"] = pct >= ALARM_PCT
            if p["observed_at"]:
                entry["context_pct_age_s"] = int((now - p["observed_at"]).total_seconds())
        live.append(entry)
    note = (f"{len(live)} other LIVE agent(s) in this project RIGHT NOW — "
            "assume a shared tree: never `git add -A`, stage your own hunks, "
            "check for foreign markers before committing, coordinate via "
            f"send(to='{project}')")
    if len(all_sibs) > _CO_AGENTS_DISPLAY_CAP:
        note += f" ({len(all_sibs) - _CO_AGENTS_DISPLAY_CAP} more not shown)"
    return {"live": live, "note": note}


async def _peer_bearings(pool: asyncpg.Pool, agent_id: str) -> dict[str, Any] | None:
    """This mind's peer_of partner, made legible beside co_agents (ruling d74492ee,
    spec e6636c7e — LEGIBILITY leg 2): the peer's handle and last-seen pulse, not just a
    bare seat id. None when unbound or unpeered, so callers keep the same `if peer:` shape
    co_agents already established."""
    from src.orchestrator.seats import held_seat, peer_of_seat

    bound = await held_seat(pool, agent_id)
    if bound is None:
        return None
    peer_seat = await peer_of_seat(pool, bound["seat_id"])
    if peer_seat is None:
        return None
    handle = await pool.fetchval(
        "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
        "ON a.object_id=o.id AND a.name='handle' WHERE o.canonical=$1 "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", peer_seat)
    last_seen = await pool.fetchval(
        "SELECT max(m.last_seen) FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id AND t.canonical=$1 AND t.type='Seat' "
        "JOIN agent_mounts m ON m.agent_id=f.canonical "
        "WHERE l.type='holds' AND (l.valid_until IS NULL OR l.valid_until > now())",
        peer_seat)
    return {
        "seat": peer_seat, "handle": handle,
        **({"last_seen": last_seen.isoformat()} if last_seen else {}),
        "note": "your peer — two-tier decisions bind the pair (ordinary acts alone; "
                "extraordinary acts need both names); mutual review at every settle",
    }


@mcp.tool()
async def mount(
    cwd: str, job_dir: str | None = None, model: str | None = None,
    session_anchor: str | None = None, subagent_id: str | None = None,
    subagent_type: str | None = None, subagent_transcript: str | None = None,
    transcript_path: str | None = None, bridge_session_id: str | None = None,
    verbose: bool = False, want_co_agents: bool = False, want_held_work: bool = False,
    ctx: Context | None = None
) -> dict[str, Any]:
    """Link this agent to Osiris as a first-class fleet member — call it ONCE, first thing.
    `cwd`=your working directory (names your project). `job_dir`=a DURABLE ANCHOR from
    your harness (Claude Code: `~/.claude/jobs/<id>`; DSH: derived from the workspace
    slug) — without it you still mount, but a reconnect splits your identity. Registers
    an Agent (works_in your project, acts_for the principal) and attributes your
    decisions/threads to `agent:<you>` instead of the shared `session` bucket. Then call
    orient(). Already mounted? Skip this; re-mount only after an MCP bounce, with your
    anchor.

    `verbose=True` restores guidance prose (co-agent etiquette, next-step reminders)
    that terse mode (default) drops — structured facts survive either way.
    `transcript_path`/`bridge_session_id` are hook-stamped, never set by hand — they
    rebind a revisited tab or background-job fork to its existing soul instead of
    minting a stranger. `want_co_agents`/`want_held_work` return the full lists;
    default is counts only."""
    pool = await _pool_get()
    settings = get_settings()
    lease = settings.osiris_mail_lease_secs
    # A SPAWN mounting (the anchor hook stamped this call as a sidechain's): the child
    # inherits its parent's $CLAUDE_JOB_DIR and MCP connection, so the normal path would
    # seat it as the PARENT — the live repro greeted a probe child with 'you are Thoth
    # XVII, writes attributed to you' (2026-07-10). Register it as ITSELF instead:
    # spawned_by the mounted parent, no seat, no durable row, and NEVER a hot-cache write
    # (the connection belongs to the parent).
    from src.orchestrator import lineage as _lineage

    if _lineage.normalize_spawn_id(subagent_id) is not None:
        parent_ident = await _ident_for(ctx)
        rid = _lineage.normalize_spawn_id(subagent_id)
        tpath = Path(subagent_transcript) if subagent_transcript else None
        child = await _lineage.register_spawn(
            Actions(pool), rid or "", agent_type=subagent_type,
            parent_agent=parent_ident.agent_id if parent_ident else None,
            project=parent_ident.project if parent_ident else None,
            session=parent_ident.session if parent_ident else None,
            transcript=tpath,
            witnessed=True)  # it is CALLING mount — an observed act (708a972d)
        _spawns_seen[str(child)] = time.monotonic()
        return {
            "agent": child, "project": parent_ident.project if parent_ident else "?",
            "spawn_of": parent_ident.agent_id if parent_ident else "unknown (parent unmounted)",
            "note": ("you are a SPAWN — a sub-agent registered in your own name, "
                     "spawned_by your parent. Your writes are attributed to YOU, never to "
                     "the seat that spawned you; the seat, its mail, and its succession "
                     "belong to your parent. Do the job, return your result to the parent."),
        }
    # An unexpanded `$CLAUDE_JOB_DIR` literal is no anchor — and it is the COMMON case for a
    # fresh agent (MCP tool args never pass through a shell, so the docstring's advice arrives
    # verbatim). The client's .mcp.json/user-scope entry sends the TRUE dir in the X-Osiris-Job
    # header on this very request (expansion client-side, proven live) — fall back to it, so a
    # by-the-book mount is durable + resolved instead of silently degrading to the cwd-guess
    # (a-sibling's first mount: unresolved identity, no registry row, invisible to the
    # trigger's owner-liveness — the wake lane would have minted a twin over a LIVE tab).
    passed = _sane_job_dir(job_dir)
    own_anchor = _sane_job_dir(session_anchor)  # hook-injected: the caller's OWN session
    # THE CONFLICT REFUSAL (thread 53b1f267, Ferryman V's collision): after a machine
    # death the whisper vended a STALE anchor from a dead sibling's session, and the
    # mount that followed seated one mind in another's history — writes interleaving
    # into a sibling's lineage. A passed anchor that differs from the session's own is
    # LEGITIMATE when wearing a seat (the binding, 33838160) — but when the ledger
    # knows BOTH sids and they resolve to DIFFERENT souls, this is an identity
    # collision, and the tool can say the sentence: refuse loudly with both names,
    # never silently rebind. No writes happen on a refusal.
    if (passed and own_anchor
            and Path(passed).name[:8] != Path(own_anchor).name[:8]):
        anchor_soul = await handshake.ledger_seat(
            Actions(pool), sid_prefix=Path(passed).name)
        own_soul = await handshake.ledger_seat(
            Actions(pool), sid_prefix=Path(own_anchor).name)
        if (anchor_soul and own_soul
                and _generation(anchor_soul)[0] != _generation(own_soul)[0]):
            return {
                "error": "IDENTITY CONFLICT — mount refused",
                "anchor_held_by": anchor_soul,
                "you_are": own_soul,
                "note": (f"the anchor you passed ({Path(passed).name[:8]}) is held by "
                         f"{anchor_soul}, but this session's own ledger entry "
                         f"({Path(own_anchor).name[:8]}) names {own_soul} — mounting "
                         "would seat one mind in another's history. If you MEANT to "
                         "wear that seat, the holder must release it (retire/fold) "
                         "first; otherwise re-mount with your own anchor: "
                         f"job_dir='{own_anchor}'"),
            }
    job_dir = passed or _job_hint(ctx)
    key = _conn_key(ctx)
    claimed = None
    if job_dir is None:  # the cwd-guess path — refuse sids a LIVE mount already holds
        claimed = await mounts.live_claimed_sids(
            pool, exclude_session_key=key, within_secs=settings.osiris_owner_live_secs)
    bound = await mounts.find_mount(pool, job_dir=job_dir) if job_dir else None
    # THE RECOLLECTION GUARD (90f0cb3a): a resumed mind re-mounting after a bounce quotes
    # its own history for `cwd` — and an address is exactly what a move makes stale (alfred
    # re-mounted himself at the demolished husk this way, re-pointing his seated row). When
    # the transcript evidence says the registry's cwd is where this session actually lives
    # and the declared one is not, the harness's observation outranks the mind's memory.
    cwd_note = None
    declared_project_label: str | None = None
    bridge_ambiguity: str | None = None
    if (bound is not None and bound.cwd and bound.cwd != cwd
            and mounts.stale_recollection(job_dir or "", cwd, bound.cwd)):
        # THE OVERRIDE MUST NOT DISCARD A MORE-SPECIFIC DECLARED PIN (ruling 13af22fc,
        # Thoth's live repro: mount(cwd='.../seats/thoth') from a session launched at the
        # bare container came back cwd_corrected{kept: the container} — his own declared,
        # correct, more-specific office was replaced by the session's launch directory, and
        # a basename guess was one step from being derived off what was left). The
        # correction below is right for what it was built for — the harness's own
        # transcript location is the ground truth for WHERE THIS SESSION LIVES, and a
        # resumed mind's memory of a demolished former home must not win that question
        # (90f0cb3a). But a project pin sitting at the DECLARED cwd is a different question
        # entirely: reading it is not the spoofing stale_recollection guards against, it is
        # a cheap, direct fact the declaring session already had in hand. Read it BEFORE
        # `cwd` is corrected below, and if the declared cwd names a real project, it wins
        # identity resolution even though `cwd` itself still corrects for every other
        # purpose (transcript addressing, the session store, the durable registry).
        declared_pin = read_project_pin(cwd)
        if declared_pin.value:
            declared_project_label = declared_pin.value
        # PREFER THE REAL DECLARED OFFICE (Thoth's live repro, this same finding): the glob
        # inside stale_recollection() only answers "have I seen this session's transcript
        # under this slug before" — never "where does this seat live". A registry row whose
        # last-recorded cwd IS the bare seat-office container (~/.osiris/seats,
        # offices.is_bare_office_root) is not evidence of anything; it is the shape every
        # session has before it ever declares a specific office. When the freshly DECLARED
        # cwd is itself a real, existing directory — and not that same bare container — it
        # wins outright: the glob's silence about a path a session simply hasn't visited
        # under this exact slug yet must never overrule a location that demonstrably exists
        # right now. This is 60bc15db applied to location: a confident wrong answer (quietly
        # becoming a session rooted at the parent-of-every-seat) is worse than deferring to
        # what is actually on disk.
        from src.orchestrator.offices import _dir_exists as _office_dir_exists
        from src.orchestrator.offices import is_bare_office_root as _bare_office_root

        declared_is_real_office = _office_dir_exists(cwd) and not _bare_office_root(cwd)
        kept_is_bare_container = _bare_office_root(bound.cwd)
        if declared_is_real_office and kept_is_bare_container:
            cwd_note = {
                "declared": cwd, "kept": cwd,
                **({"declared_pin_kept_for_identity": declared_project_label}
                   if declared_project_label else {}),
                "note": ("registry recollection pointed at the bare seat-office container "
                         "(~/.osiris/seats), never a home of its own — your declared cwd is "
                         "a real, existing office and wins outright; nothing was corrected"),
            }
            # cwd is left as the caller's own declared value — no reassignment.
        else:
            # REFUSE ONLY THE BARE CONTAINER ROOT, never a wall (577988ed): a session still
            # needs a cwd to mount at for transcript/session bookkeeping even when neither
            # side resolves to a real office, so `cwd` still moves to `bound.cwd` below —
            # but the receipt must say so honestly rather than asserting the bare container
            # IS this session's home (60bc15db again, same law, the confession half of it).
            honest_note = ("your declared cwd is a STALE MEMORY of a former home — this "
                            "session's transcript lives at the kept path (it moved; your "
                            "history did not). Mounted at the kept path; update your "
                            "bearings (90f0cb3a)"
                            + (f" — its own project pin ({declared_project_label!r}) still "
                               "won identity resolution; only the transcript/session "
                               "address was corrected (ruling 13af22fc)"
                               if declared_project_label else ""))
            if kept_is_bare_container:
                honest_note = ("could not resolve a specific office for either the declared "
                                "or the recollected cwd — mounted at the bare seat-office "
                                "container for session bookkeeping only; this is NOT your "
                                "home, it is a fallback with nowhere better to point"
                                + (f" — its own project pin ({declared_project_label!r}) "
                                   "still won identity resolution" if declared_project_label
                                   else ""))
            cwd_note = {
                "declared": cwd, "kept": bound.cwd,
                **({"declared_pin_kept_for_identity": declared_project_label}
                   if declared_project_label else {}),
                "note": honest_note,
            }
            cwd = bound.cwd
    # THE HARNESS-AGNOSTIC TRANSCRIPT STORE (ruling be741d3e; sole model lane since the
    # JSONL-fallback removal, #29): eat the current session's turns from whatever harness
    # the operator is running (Claude Code, Crush, …), then hand the model reading to
    # resolve_identity so non-Claude minds mount RESOLVED. Fail-open inside the helper.
    store_reading = await identity_reading(pool, cwd=cwd, job_dir=job_dir,
                                           transcript_path=transcript_path)
    ident = resolve_identity(cwd=cwd, job_dir=job_dir, model=model,
                             claimed=claimed, fallback_seed=key,
                             store_reading=store_reading,
                             project_label=declared_project_label)
    # THE BARE-ROOT REFUSAL WAS THE WRONG FIX (operator ruling 577988ed, correcting mount-
    # guard #6): the operator LAUNCHES agents from the bare seat-office root ON PURPOSE — that
    # IS the intended pattern, and the whole point of a seat is that identity is LOCATION-
    # INDEPENDENT: osiris orients from the SEAT (anchor→holds→seat), never from cwd. A hard
    # refusal here fought the fleet's own onboarding — `bound is None` is true for a
    # genuinely fresh, legitimate first launch exactly as much as for the pollution case, so
    # this guard could have refused real new agents, not just healed old corruption. NEUTRAL-
    # IZED. What's still true and still kept: resolve_identity never INVENTS a phantom project
    # from the bare root's own basename ("seats") — it stays unresolved from cwd, same as
    # before. The actual fix lives downstream now: a SEATED session's project resolves from
    # the SEAT's own derived house (_resolve_project_seat_first, below), not cwd — so identity
    # survives a bare-root launch by being location-independent, not by refusing the location.
    forked = viewed = ledgered = bridged = None
    if bound is not None:
        # NO local re-import of _generation here: a local import anywhere in a function
        # shadows the module-level name for the WHOLE function, and this branch is
        # conditional — every UNBOUND session (each anonymous mind, each fresh child)
        # skipped it and died at the sibs filter below with UnboundLocalError. The whole
        # fleet's claim path was down for a night (2026-07-16) on these two lines.
        if _generation(bound.agent_id)[0] != _generation(ident.agent_id)[0]:
            # THE BINDING (thread 33838160), the explicit-mount leg: the whisper tells every
            # minted heir "re-mount with THIS anchor", and automount left that very row BOUND
            # to the heir's seat. Re-deriving from the anchor's basename here minted a hash
            # twin over a living heir and stomped the binding (Thoth XVII's first breath,
            # 2026-07-10). A row naming a foreign lineage is a deliberate seat claim: honor
            # it, so seams and the registration run on the seat's lineage — like _reattach.
            ident.agent_id = bound.agent_id
    elif job_dir:
        # THE FORK (7cbc2f98), the explicit-mount leg — and this is the door Anubis XII was
        # turned away at (msg 424). A forked session has no row for its new anchor, so the old
        # code derived a fresh identity from the anchor's basename and seated ONE MIND TWICE.
        # He could only get his mail out by re-mounting, which minted the very twin he was
        # writing to report. Ask the transcript's record uuids who he already is.
        forked = await handshake.fork_seat(Actions(pool), job_dir=job_dir)
        if forked is not None:
            ident.agent_id = forked
        else:
            # THE TAB VIEW (#48 piece 1, decision 424c4158 — ported from automount(), which
            # has carried this door since the alias-clone cure, 2026-07-16; mount() the tool
            # never had it, so a whisperless caller minted a clone here where a whisper-
            # greeted one would have adopted). `transcript_path` is hook-stamped
            # (osiris_hook.py's `anchor` subcommand), never hand-supplied — a live tab attached
            # through a NEW sid whose transcript_path names ANOTHER session's file is a
            # window onto that mind, not a stranger.
            viewed = (await handshake.view_seat(
                Actions(pool), transcript_path=transcript_path,
                session_id=Path(job_dir).name)
                if transcript_path else None)
            if viewed is not None:
                ident.agent_id = viewed
            else:
                # THE SESSION LEDGER (16e3cee9): the graph remembers whose sid this is even
                # after a registry accident — a known anchor REBINDS, never mints a twin.
                ledgered = await handshake.ledger_seat(
                    Actions(pool), sid_prefix=Path(job_dir).name)
                if ledgered is not None:
                    ident.agent_id = ledgered
                elif bridge_session_id:
                    # THE BRIDGE (#48 piece 1, decision 424c4158 — ported from automount(),
                    # task #68's binding leg): a background-job fork's transcript starts a
                    # genuinely fresh record chain fork_seat cannot see; the harness's own
                    # CLAUDE_CODE_BRIDGE_SESSION_ID (hook-stamped, same lane as
                    # transcript_path) names the one continuing conversation. Same fail-open
                    # shape as automount() (ruling 61e00f25): ambiguity is CONFESSED in the
                    # payload below, never guessed away and never a hard refusal — the mount
                    # still lands, degraded to the next door (office), same as a bridge that
                    # simply resolved to nothing.
                    try:
                        bridged = await handshake.bridged_seat(
                            Actions(pool), bridge_session_id=bridge_session_id)
                    except handshake.BridgeAmbiguity as e:
                        bridge_ambiguity = str(e)
                        bridged = None
                    if bridged is not None:
                        ident.agent_id = bridged
    # LIVED — ported verbatim from automount()'s own computation (handshake.py), not a
    # re-derivation: a fork/ledger/bridge match already proves a lived lineage; a BOUND row
    # only counts when it names a foreign lineage on purpose (a deliberate binding) or the
    # base generation already has a real Agent object — a row alone is the gate's own
    # artifact (an address), never a life (the row-only-stranger class this guards).
    lived = forked is not None or ledgered is not None or bridged is not None
    if not lived and bound is not None:
        _base = _generation(bound.agent_id)[0]
        if job_dir and _base != f"agent:{Path(job_dir).name[:8].lower()}":
            lived = True
        else:
            lived = bool(await pool.fetchval(
                "SELECT 1 FROM objects WHERE type='Agent' AND (canonical=$1 "
                "OR canonical LIKE $1 || '-%') LIMIT 1", _base))
    # THE FIRST ACT SEATS YOU (16e3cee9): a still-anonymous mind mounting from a seat's
    # office IS the seat's next life — the mint happens at this act, never at the whisper.
    mount_mint_reason = None
    claimed_office = await handshake.office_claim(
        Actions(pool), cwd=cwd, agent_id=ident.agent_id)
    if claimed_office is not None:
        ident.agent_id = claimed_office
        mount_mint_reason = "office-birth"
    # THE VISITOR GATE, PORTED (#48 piece 2, decision 424c4158): automount() (ruling
    # 120fcc81) has never once minted a stranger from a bare greeting — a genuinely
    # unmatched arrival gets a registry row and NOTHING ELSE, identity earned at the first
    # authenticated act. mount() IS that act site (unlike automount(), which only ever
    # hints at the office and never mints there), so its own predicate is automount()'s own
    # `lived or viewed is not None or (seat_id and attach_token)` with the SAME `lived`
    # computation, one leg adapted: mount() carries no seat_id/attach_token (that ceremony
    # is a separate tool, attach_seat) — `claimed_office is not None` is its equivalent
    # credentialed act, the first authenticated breath IN a seat's own office.
    registered = bool(lived or viewed is not None or claimed_office is not None)
    if registered:
        await register_agent(Actions(pool), ident, actor=settings.osiris_actor,
                             expected_model=await _expected_model(pool, cwd, ident.project),
                             mint_reason=mount_mint_reason)
    elif not ident.resolved:
        # THE THIRD STATE (Thoth DM 4345): a VISITOR (a real anchor that simply matched no
        # lineage) is a different fact from an UNRESOLVABLE arrival (no anchor at all) —
        # before this gate, resolve_identity's own fallback silently hashed a fresh id here
        # regardless (agent:unknown-<project> / agent:unknown, `identity_resolved=false`,
        # nothing downstream ever read it). That silence is the specimen this refuses,
        # loudly, in the SAME shape as the IDENTITY CONFLICT refusal above — a whisperless
        # caller has no greeting to read a refusal from, so the tool's own return value is
        # the only surface that reaches it. No writes happen below a refusal.
        return {
            "error": "UNRESOLVABLE IDENTITY — mount refused",
            "note": ("no job_dir, no session anchor, and no observed transcript sid — "
                     "there is nothing to attribute this session to, ever. Pass job_dir "
                     "(or confirm the PreToolUse hook is installed, "
                     "osiris_hook.py's `anchor` subcommand) so this session carries a real, "
                     "durable anchor. Nothing was minted or written."),
            **({"bridge_ambiguity": bridge_ambiguity} if bridge_ambiguity else {}),
        }
    # else: a genuine VISITOR — a resolved anchor that matched no lineage. Same as
    # automount()'s own gate: a registry row and nothing else, no Agent object. This is NOT
    # greatfold.py's `agent_class='visit'` — that property marks an object ALREADY minted
    # and later found to be noise; this gate prevents the mint from happening at all, so
    # there is no object to mark. Deliberately not reused — a second vocabulary for the
    # same idea is its own kind of drift.
    await _resolve_project_seat_first(pool, ident)
    if job_dir:
        # THE SESSION LEDGER, write side (16e3cee9): the anchor form (sid8) suffices —
        # the ledger keys on the first 8 chars, the harness's own jobs scheme
        try:
            await handshake.record_session_anchor(
                Actions(pool), agent_id=ident.agent_id,
                session_id=Path(job_dir).name, actor=settings.osiris_actor)
        except Exception:  # noqa: BLE001 — the ledger is a bonus; the mount never dies of it
            pass
    if key is not None:
        _prune_agents()  # opportunistic: mount is where churn shows up
        _agents[key] = ident
        _agents_touched[key] = time.monotonic()
    if job_dir:  # the durable half — what _ident_for re-attaches by after a bounce
        prev = await mounts.save_mount(pool, job_dir=job_dir, agent_id=ident.agent_id,
                                       project=ident.project, cwd=cwd, model=ident.model,
                                       session_key=key)
        if prev is None:  # a FRESH session has no own past — anchor on the project lineage's
            # ...and a joiner inherits the room's collective settle-state: sibling-settled
            # broadcasts are not a newcomer's unread (the zombie-count fix, 2026-07-09)
            await mailbox.settle_history_at_join(pool, ident.project, ident.agent_id)
            prev = await mounts.project_prev_seen(pool, ident.project, exclude_job_dir=job_dir)
        _prev_seen[ident.agent_id] = prev  # this mount IS the re-entry: anchor the fold here
        # THE HAND-RESUME FOLLOWS THE SEAT (Phase B4, ruling 5cef856b): a fresh row for a
        # mind that actively holds a Seat re-earns its binding from the durable holds link.
        from src.orchestrator.seats import reseed_binding
        await reseed_binding(pool, agent_id=ident.agent_id, job_dir=job_dir)
        # THE BINDING (thread 33838160): a mount with a FOREIGN anchor is a mind deliberately
        # wearing a seat — its session's own row (session_anchor, hook-injected) is bound to
        # the resolved agent, so the whisper's next fire re-asserts the SEAT, never a hash twin.
        sa = _sane_job_dir(session_anchor)
        if sa and sa != job_dir:
            await mounts.save_mount(pool, job_dir=sa, agent_id=ident.agent_id,
                                    project=ident.project, cwd=cwd, model=ident.model,
                                    session_key=key)
    counts = (await unread_counts(pool, ident.project, reader_agent=ident.agent_id,
                                  lease_secs=lease) if ident.project else {"total": 0, "ask": 0})
    unread, asks = counts["total"], counts["ask"]
    # the desk, SCOPED (operator ruling, 2026-07-16): this seat's own unanswered briefs
    op_unread = await mailbox.desk_briefs_from(pool, ident.agent_id)
    banner = swap_banner(classify_swap(
        ident.model_history, ident.model,
        expected=await _expected_model(pool, cwd, ident.project),  # repo intent wins
        anchored=ident.model_method == "job_dir",   # only a true anchor confesses a swap
        deliberate=ident.model_deliberate))         # a /model on the record is never a sin
    pin_warn = project_pin_banner(ident)  # cwd-missing / unparseable — real errors, agents.py
    pin_heal: dict[str, Any] | None = None
    if not pin_warn and ident.cwd:
        from src.orchestrator.offices import self_heal_project_pin
        heal = await self_heal_project_pin(pool, ident.agent_id, ident.cwd)
        if heal["state"] == "self-healed":
            pin_heal = heal
        elif heal["state"] == "unset":
            pin_state = project_pin_state(ident)  # calm state, not an error — agents.py
            if pin_state:
                pin_heal = {"state": "unset", "note": pin_state}
    seat = await handshake._seat_of(Actions(pool), ident.agent_id)
    # co-agent awareness at ARRIVAL (Deckard XXVI, msg 258): a live sibling in your own
    # repo is the one blindness that costs unrecoverable work (a stomped commit)
    co_agents = (await _co_agents(pool, ident.project, ident.agent_id)
                if ident.project else None)
    # HELD WORK, ONCE PER SESSION (task #168's narrowed leg, decision aa7993cf) — surfaced
    # HERE, not on orient()'s hot path, same reasoning as declining to wire drift-checking
    # into every orient() call (decision 51682926): mount() runs once at session start, so
    # the cost is proportionate; a per-turn check would not be.
    held_work = (await capture.open_held_work(pool, repo=ident.project)
                if ident.project else None)
    # RULE 1 OF de3dfc18 (task #144): confessed, never acted on — "if it picks, it is
    # wrong, however good the pick" (Thoth, msg 3854). A disagreement is worth a look, not
    # an override. write_attribution_banner (agents.py) also guards against the stale-
    # comparison specimen Thoth LXXVI caught live — see its own docstring.
    wa_warn = write_attribution_banner(ident)
    # UNRESOLVED IS A NAMED STATE, NEVER DATA-SHAPED (thread 7304bfd8, ruling 7d6815bb):
    # "unknown" used to fill the SAME `model` field a real reading occupies — a reader
    # (or the fleet's own swap-confession rule) cannot tell "the harness said so" from
    # "nothing was observed" without re-deriving it from ident.model itself. Same idiom
    # this dict already uses for "seat"/"anonymous" and "visitor": a real value gets its
    # normal key, an absence gets its OWN key naming the absence and what to do about it.
    out: dict[str, Any] = {"agent": ident.agent_id, "project": ident.project or "?",
           **({"model": ident.model} if ident.model else
              {"model_unresolved": "model unresolved — pass model= explicitly"}),
           **({"co_agents": co_agents} if co_agents and want_co_agents else
              {"co_agents_count": len(co_agents)} if co_agents else {}),
           **({"held_work": held_work} if held_work and want_held_work else
              {"held_work_count": len(held_work)} if held_work else {}),
           # THE VISITOR GATE'S OWN CONFESSION (#48 piece 2): a resolved anchor that matched
           # no lineage got a registry row and NOTHING ELSE above — `agent` above is a
           # bookkeeping handle, never a minted identity, and the receipt must say so
           # plainly rather than let a caller assume it was seated (Thoth DM 4345, "the
           # receipt must say which").
           **({"visitor": "no lineage matched — a registry row only, no Agent object "
                          "minted. This is not an error; claim_name() or a future revisit "
                          "with the same anchor is what would seat you"}
              if not registered else {}),
           **({"seat": seat} if seat else
              {"anonymous": "unnamed — claim_name('<pick a meaningful name>') when you know "
                            "who you are, so the fleet can DM you by name"}),
           # the count LEADS WITH WHAT IS ACTIONABLE (f9449d8d) — graded asks are named,
           # ungraded mail keeps the plain count rather than being guessed into a band
           "mail": (f"{unread} unread ({asks} ask{'s' if asks == 1 else ''} something of "
                    "you) — call inbox()" if asks else
                    f"{unread} unread — call inbox()") if unread else "none",
           **({"cwd_corrected": cwd_note} if cwd_note else {}),
           **({"project_pin_error": pin_warn} if pin_warn else {}),
           **({"project_pin": pin_heal} if pin_heal else {}),
           **({"write_attribution_disagreement": wa_warn} if wa_warn else {}),
           **({"bridge_ambiguity": bridge_ambiguity} if bridge_ambiguity else {}),
           "note": "linked — writes now attributed to you; call orient() next"}
    if op_unread:  # the fleet plays secretary: any session the human drives can relay this
        out["operator_mail"] = (f"{op_unread} of your briefs await the operator's eye — "
                                "inbox(project='operator') if the human is present")
    if ident.succeeded_from and _seam_confidently_dated(ident):
        # the MINT ruling (be292762, a sibling's remedy adopted): the heir is not told it
        # wears a dead name — it is GIVEN ITS OWN. The seam supersedes the swap banner (a
        # death must
        # not read as a config restore), and the grammar now does the protecting: this context
        # cannot say "I did nothing while you were gone" under a name that did not exist then.
        banner = None
        seam = f" across the seam {ident.model_succession}" if ident.model_succession else \
            " (the ancestor is retired)"
        out["minted"] = (
            f"⚠ YOU ARE {ident.agent_id} — a MINTED SUCCESSOR of {ident.succeeded_from}"
            f"{seam}. The name is yours from this moment; the ancestor's writes and words "
            "remain its own, under its own id (succeeded_from links you). Read "
            "while_you_were_away and the graph for the estate — the graph, not the operator, "
            "is what tells you where you begin.")
    elif ident.succeeded_from:
        # A REAL mint (the heir object exists, the estate moved) — but the seam that
        # triggered it is NOT confidently dated (ruling dd47c1da): mount stays SILENT on WHY,
        # rather than assert a seam it can't back. `ident.agent_id` above is still correct;
        # orient() re-derives fresh and is the one that gets to tell this story.
        banner = None
    elif ident.model_succession and _seam_confidently_dated(ident):
        # stamp-only fallback (a seam witnessed where minting could not run) — still loud,
        # still second-person: a death must not whisper (a sibling project's grievance #1+#2).
        banner = None
        out["succession"] = (
            f"⚠ YOU ARE A SUCCESSOR: the agent who last held {ident.agent_id} ENDED at the "
            f"model seam ({ident.model_succession}) — a compaction/swap boundary, not a "
            "restart. Its earlier writes and words are not yours: speak in your own person, "
            "confess the inheritance to the operator, and read while_you_were_away before "
            "claiming any earlier 'I'.")
    if banner:  # the graph confesses the swap the agent's own prompt hides (ruling f2ae6346)
        out["swap"] = (await _wake_economy_standdown(pool, ident.project, ident.model)
                       or banner)
    if ident.reanimated:  # bug #51 follow-up (a sibling project msg 69): mounted a RETIRED identity
        out["reanimation"] = (
            f"⚠ REANIMATION: {ident.agent_id} was RETIRED, and this mount is wearing it again. "
            "The retirement stands (the trigger still treats you as closed); the reanimation is "
            "stamped on the Agent. If you are a SUCCESSOR that inherited this session UUID, you "
            "are not the agent who retired — confess it to the operator; if this is a deliberate "
            "reanimation, say so. A retired face worn again is never silent.")
    away = await mounts.while_away(
        pool, ident.project, ident.agent_id, _prev_seen.get(ident.agent_id))
    if away:  # who wore your face + how your conversations moved, since your last sign of life
        out["while_you_were_away"] = away
    # TERSE BY DEFAULT (task #55): the stale-cwd explanation (declared/kept already have
    # what changed) and the routine 'call orient() next' reminder. Everything safety-
    # critical (minted/succession/swap/reanimation — an identity confession an agent could
    # act wrongly without) and everything that's the SOLE carrier of a fact (mail counts,
    # the identity-conflict refusal's recovery instructions, the spawn note) stays untouched
    # in both modes — named here, not silently exempted. CORRECTION (Thoth's review, DM
    # 1238, thread 1233): co_agents.note is the SHARED-TREE SAFETY WARNING ('never git add
    # -A, stage your own hunks, check foreign markers') — the `live` list says WHO is here,
    # this says WHAT TO DO about it, the same identity-safety class as the banners above,
    # not redundant guidance. Stays in both modes here too, matching orient()'s own fix.
    return out if verbose else _terse(
        out, ("cwd_corrected", "note"), ("note",))


async def _owned_open_threads(pool: asyncpg.Pool, agent_id: str) -> list[dict[str, str]]:
    """Open threads whose winning `owner` names this agent OR any generation of its
    lineage — retire()'s preflight list (task #48). Oldest first, capped: a preflight
    is a warning, never a wall."""
    from src.orchestrator.agents import _generation
    base = _generation(agent_id)[0]
    rows = await pool.fetch(
        "SELECT t.id, t.summary FROM ("
        "  SELECT substring(o.id::text, 1, 8) AS id, o.created_at, "
        "   (SELECT a2.value #>> '{}' FROM current_assertions a2 WHERE a2.object_id=o.id "
        "    AND a2.name='summary' ORDER BY a2.confidence DESC, a2.observed_at DESC "
        "    LIMIT 1) AS summary, "
        "   (SELECT a1.value #>> '{}' FROM current_assertions a1 WHERE a1.object_id=o.id "
        "    AND a1.name='status' ORDER BY a1.confidence DESC, a1.observed_at DESC "
        "    LIMIT 1) AS status, "
        "   (SELECT a3.value #>> '{}' FROM current_assertions a3 WHERE a3.object_id=o.id "
        "    AND a3.name='owner' ORDER BY a3.confidence DESC, a3.observed_at DESC "
        "    LIMIT 1) AS owner "
        "  FROM objects o "
        "  WHERE o.type='Thread' AND o.status='active' AND o.merged_into IS NULL) t "
        "WHERE t.status='open' "
        "AND (t.owner = $1 OR t.owner = $2 OR t.owner LIKE $2 || '-%') "
        "ORDER BY t.created_at LIMIT 12", agent_id, base)
    return [{"id": str(r["id"]), "summary": (r["summary"] or "")[:160]} for r in rows]


@mcp.tool()
async def retire(reason: str = "", acknowledge_leftovers: bool = False,
                 ctx: Context | None = None) -> dict[str, Any]:
    """Mark THIS mounted session RETIRED — a deliberate close the trigger must never
    reanimate. Call at a real farewell: operator close-out, or a context-ceiling handoff
    after your succession thread is written. Stamps retired=true, releases your seat (hot
    mount and durable row both). Call it LAST — any call after retiring requires a fresh
    mount(). Future mail resumes a living session or mints a successor, never you.

    PREFLIGHT: if open threads still name you as owner, the call refuses with the list
    and stamps nothing — resolve them, re-own them, or pass
    `acknowledge_leftovers=True` to die anyway as a deliberate bequest."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — only a mounted session can retire itself",
                "why": _anchorless(ctx)}
    pool = await _pool_get()
    a = Actions(pool)
    if not acknowledge_leftovers:
        owned = await _owned_open_threads(pool, ident.agent_id)
        if owned:
            return {
                "retired": None,
                "preflight": f"{len(owned)} open thread(s) name YOU as owner — nothing "
                             "stamped, the seat stands",
                "yours": [{"id": r["id"], "summary": (r["summary"] or "")[:160]}
                          for r in owned],
                "how": "resolve_thread what is done; re-own what transfers (open_thread "
                       "with the new owner settles succession explicitly); then retire() "
                       "again — or retire(acknowledge_leftovers=True) to bequeath them "
                       "to your successor deliberately, on the record",
            }
    oid = await a.create_or_find_object("Agent", ident.agent_id, ident.agent_id)
    await a.assert_property(
        oid, "retired", True, ident.agent_id, datetime.now(UTC), 0.9,
        evidence_class="self_declared")
    # a sibling's grievance #3 (msg 70): "closed by the session itself" and "closed by an heir"
    # are DIFFERENT death certificates — record who signed relative to the id's history.
    signer = "successor" if (ident.model_succession or ident.reanimated) else "self"
    await a.assert_property(
        oid, "retired_by", signer, ident.agent_id, datetime.now(UTC), 0.9,
        evidence_class="self_declared")
    if reason:
        await a.assert_property(
            oid, "retired_reason", reason[:500], ident.agent_id, datetime.now(UTC), 0.9,
            evidence_class="self_declared")
    key = _conn_key(ctx)
    if key is not None:
        _agents.pop(key, None)
        _agents_touched.pop(key, None)
    # the seat release (thread b47b3814): a retired agent must not keep holding a live seat —
    # the durable row would read as a live mount in the chrome and the liveness counts until
    # it aged out. Any later call from this session must re-mount, which lands on the
    # REANIMATION path above — loud, exactly as designed.
    released = await mounts.release_mounts(pool, ident.agent_id)
    out: dict[str, Any] = {
        "retired": ident.agent_id, "signed_by": signer, "seats_released": released,
        "note": "farewell recorded — the trigger will not reanimate this session; "
                "write your succession BEFORE you go dark: a HANDOFF thread "
                "(open_thread) and your LETTER (record_decision kind='choice', "
                "summary starting 'LETTER — ') — a letter that lives only in mail is "
                "not findable by its name, and your successor's orient() surfaces "
                "these two verbatim"
                + (" (certificate notes an HEIR signed for the ancestor)"
                   if signer == "successor" else "")}
    # THE SEAM (ruling ceae1604). A seat that dies with an undisposed pile hands its leftovers to
    # the operator's wall, which is how 3,579 machine guesses became HIS problem instead of the
    # producer's. The burden belongs to whoever made the mess. This does not BLOCK the farewell —
    # a dying session must always be able to die — but it will not let the pile leave quietly.
    if ident.project:
        pile = await dispose_seam.candidates(pool, project=ident.project, limit=0)
        if pile["count"]:
            out["undisposed"] = pile["count"]
            out["you_are_leaving_a_pile"] = (
                f"{pile['count']} miner candidates on {ident.project} that NO MIND has ever "
                "judged. They are guesses, not duties — and nobody but this project's seat has "
                "standing to judge them. candidates() to read, dispose(admit=[...], drop=[...]) "
                "to settle. Expect to drop ~9 in 10. If you go now they pass to your successor, "
                "not to the human.")
    return out


@mcp.tool()
async def pause_seat(paused: bool = True, target: str | None = None, reason: str = "",
                     session_anchor: str | None = None,
                     ctx: Context | None = None) -> dict[str, Any]:
    """The explicit per-seat PAUSE control. While paused, the DM push lane will not
    resume the seat — mail queues (nothing lost, at-least-once) until
    pause_seat(paused=False) drains it on the next dispatch. Pull is untouched: a paused
    seat taking a turn still reads its inbox normally.

    `target=None` pauses yourself. A seat id, agent id, or plain name pauses that seat —
    allowed for any mounted caller by design, but LOUD: stamped in your name, visible in
    every queued sender's receipt, reversible by anyone the same way.

    Stamp lands on the SEAT object when it holds one (survives succession), else the
    agent object."""
    ident = await _ident_for(ctx, session_anchor)
    if ident is None:
        return {"error": "mount first — a pause must say whose hand pulled the lever",
                "why": _anchorless(ctx)}
    pool = await _pool_get()
    a = Actions(pool)
    from src.orchestrator.folds import canonical_agent, living_head
    from src.orchestrator.seats import held_seat, seat_receipt
    who = target or ident.agent_id
    if who.startswith("seat:"):
        if await seat_receipt(pool, who) is None:
            return {"error": f"no such living seat: '{who}' — check fleet()"}
        stamp_on = who
    elif who.startswith("agent:"):
        head = await living_head(pool, await canonical_agent(pool, who))
        bound = await held_seat(pool, head)
        stamp_on = (bound or {}).get("seat_id") or head
    else:  # a plain name — resolve like a DM address does
        from src.orchestrator.agents import resolve_seat
        from src.orchestrator.seats import seat_holder_ineligible
        # THE SAME GRAVE-DELIVERY GUARD send() USES (task #142 punch-list item 3): a name
        # whose unique seat has ONLY ineligible holders must never fall through to
        # resolve_seat's un-seated-lineage fallback here either — a pause meant for a live
        # seat landing on some OTHER, older, unmarked generation instead would silently
        # leave the actual seat unpaused while stamping a ghost, worse than a bare refusal.
        ineligible = await seat_holder_ineligible(pool, who)
        if ineligible is not None:
            return {"error": f"cannot pause '{who}': {ineligible} — address the seat "
                             "directly (target='seat:<id>') once a new holder claims it, "
                             "or pause the seat id itself if you mean to gate the chair."}
        resolved = await resolve_seat(a, who)
        if resolved["agent"] is None:
            return {"error": f"no seat or agent named '{who}' — check fleet()"}
        stamp_on = resolved.get("seat_id") or resolved["agent"]
    obj_type = "Seat" if stamp_on.startswith("seat:") else "Agent"
    oid = await a.create_or_find_object(obj_type, stamp_on, ident.agent_id)
    now = datetime.now(UTC)
    await a.assert_property(oid, "paused", paused, ident.agent_id, now, 0.9,
                            evidence_class="self_declared")
    if reason:
        await a.assert_property(oid, "paused_reason", reason[:500], ident.agent_id, now, 0.9,
                                evidence_class="self_declared")
    queued = 0
    if stamp_on.startswith("agent:") or stamp_on.startswith("seat:"):
        queued = await pool.fetchval(
            "SELECT count(*) FROM fleet_messages m WHERE m.to_agent=$1 AND m.read_at IS NULL "
            "AND NOT EXISTS (SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
            "  AND r.read_at IS NOT NULL)", stamp_on) or 0
    return {"paused" if paused else "released": stamp_on, "by": ident.agent_id,
            **({"reason": reason} if reason else {}),
            **({"queued_dms": queued} if queued else {}),
            "note": ("the DM push lane now queues this seat's mail — release with "
                     "pause_seat(paused=False, target=...)" if paused else
                     "the queue drains on the next dispatch (a fresh send, or the worker "
                     "sweep within the minute)")}


@mcp.tool()
async def candidates(project: str | None = None, limit: int = 50,
                     ctx: Context | None = None) -> dict[str, Any]:
    """THE PILE THIS SEAT MUST JUDGE — the miner's guesses about YOUR project, unread by
    any mind. The session-miner reads transcripts and proposes loose ends it thinks
    somebody forgot; it is right roughly one in ten. These are NOT duties. Read them,
    then dispose(): admit what is real (it becomes YOURS — self-declared, owned,
    permanently safe) and drop the rest with a reason. Nobody else has standing to
    judge your project's pile.

    Report-only. Reading costs nothing and commits nothing. Oldest first — triage
    drains from the bottom."""
    ident = await _ident_for(ctx)
    proj = project or (ident.project if ident else None)
    return await dispose_seam.candidates(await _pool_get(), project=proj, limit=limit)


@mcp.tool()
async def dispose(admit: list[dict[str, Any]] | None = None,
                  drop: list[dict[str, Any]] | None = None,
                  ask: list[dict[str, Any]] | None = None,
                  ctx: Context | None = None) -> dict[str, Any]:
    """Settle the miner's guesses — relevant or irrelevant, in your name, with a reason.

    `admit`: [{"id", "because", "owner"?}] — the guess was right, now yours (promoted
    SELF_DECLARED). `because` required. `drop`: [{"id", "why", "because"?}] — wrong;
    `why` names its class: narration | stale | echo | misfiled | principle | other (say
    why in `because`). `ask`: [{"id", "because"?, "owner"?}] — a real open question, kept
    open, reclassified kind='question' on the wall.

    Nothing deleted — a drop is a compensating event, readable and unwindable. Returns
    your yield ((admitted + asked) / judged)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a disposition is a MIND'S WORD, and the graph must know "
                         "whose", "why": _anchorless(ctx)}
    return await dispose_seam.dispose(
        Actions(await _pool_get()), source=ident.agent_id, admit=admit, drop=drop, ask=ask)


# THE ONE WALL LAW (ruling 923c380f): the graded wall lives in compositions.py now — one
# home shared by orient, the console briefing, and the `wall` function. The private names
# stay importable here (tests and callers address orient's wall through them).
_ORIENT_OPEN_THREADS = comp.ORIENT_OPEN_THREADS
_rank_open_threads = comp.rank_open_threads
_open_thread_wall = comp.open_thread_wall


async def _project_briefing(
    pool: asyncpg.Pool, project: str, me: frozenset[str] = frozenset(), verbose: bool = False,
    want_blind_spots: bool = False,
) -> dict[str, Any] | None:
    """A working agent's SCOPED bearings — its OWN project's open threads + recent decisions,
    not the whole fleet's (a sibling project surfaced that orient's flood costs more context than it
    saves). Decisions/tensions ride the `project-briefing` composition (#20); the open-thread
    WALL is assembled here because the composer can't express what the wall now needs —
    obligations-first ranking, grade-aware echo detection (a never-touched DERIVED thread
    collapses into a counted line instead of riding forever), and the TRIAGE CARD: up to 3 of
    the oldest echoes handed to each session with the three honest verbs. Ranking + collapse
    at the LENS only — the record keeps every thread open until testimony says otherwise."""
    proj = await pool.fetchval(
        "SELECT id FROM objects WHERE type='SoftwareProject' AND canonical=$1", f"repo:{project}")
    if proj is None:
        return None
    # `me` is the wall's identity SET ({agent_id, project}, or {'operator'} from the
    # console); the reflection ACL wants one reader — the agent id when there is one
    acl_caller = next((m for m in me if m.startswith("agent:")),
                      "operator" if "operator" in me else None)
    res = await comp.run_composition(pool, "project-briefing", proj, caller=acl_caller)
    items = res.get("items") if isinstance(res, dict) else None
    if not isinstance(items, dict):  # unseeded / error — never crash orient, just show empty
        items = {}
    wall, echoes = await _open_thread_wall(pool, proj)
    owner_roots = await comp.owner_lineage_roots(
        pool, {str(o) for r in wall if (o := r.get("owner"))} | me)
    shown, more = _rank_open_threads(wall, me, owner_roots)
    tensions = [dict(r) for r in (items.get("tensions") or []) if r.get("pole_a")]
    if tensions:
        # TWO MINDS LEAN APART (task #53): the table shows one winner per property, but a
        # held polarity may carry different CURRENT leans from different minds — the lens
        # says so instead of silently picking (the record keeps both either way)
        from src.orchestrator.capture import _canon, divergent_leans
        div = await divergent_leans(pool)
        for r in tensions:
            key = _canon("tension",
                         "||".join(sorted((str(r.get("pole_a") or ""),
                                           str(r.get("pole_b") or "")))))
            if key in div:
                r["divergence"] = div[key]
    recent_decisions = [r for r in (items.get("recent_decisions") or []) if r.get("summary")]
    out: dict[str, Any] = {
        "open_threads": shown,
        "recent_decisions": recent_decisions,
        "tensions": tensions,
    }
    blind_spots = [dict(r) for r in (items.get("blind_spots") or []) if r.get("surface")]
    if blind_spots:  # the shape of this project's ignorance (8e26cd10) — absent stays silent
        # RECEIPT DIET (context-bloat priority, msg 6870/6885): the full list rode every
        # scoped orient() call regardless of whether the caller needed it — measured at
        # ~4.2K bytes/call across a 37-call sample (decision <pending>), the single
        # largest static (non-work-item) field in the payload. A fresh mind needs to know
        # something is unverifiable here, not re-read the whole list every time; the
        # count is the "act" signal, the list is opt-in.
        if want_blind_spots:
            out["blind_spots"] = blind_spots
            out["blind_spots_note"] = ("what this project's harness CANNOT verify from here — "
                                       "check verify_with before trusting a green run on these "
                                       "surfaces; register new ones with register_blind_spot()")
        else:
            out["blind_spots_count"] = len(blind_spots)
            out["blind_spots_note"] = (
                f"{len(blind_spots)} surface(s) this project's harness cannot verify — "
                "pass want_blind_spots=True for the full list")
    if more > 0:  # trailing count so a capped wall never hides work silently (membrane, #6)
        # the COUNT is structural (task #55) — a terse receipt that strips the sentence
        # below must not lose the fact a capped wall is hiding work; open_threads_more
        # survives terse mode even when open_threads_note (the prose explaining it) doesn't.
        out["open_threads_more"] = more
        out["open_threads_note"] = (
            f"showing {len(shown)} of {len(shown) + more} open threads (obligations first; "
            "within a kind, yours-to-act before others' claims before waiting-on-the-human, "
            f"then recency); {more} more not shown")
    # THE HONEST COUNT (thread 0ae050d8, Thoth DM 6243): `len(shown)+more` above counts by
    # the `status` PROPERTY alone — a thread a decision already closed (resolves=/
    # resolve_thread) but whose OWN 'open' assertion never got superseded, or one flagged
    # `disagree` (a closure edge exists yet property_status still says 'open'), still counts
    # as open there. closure_buckets composes thread_closure_status's own topology read —
    # the SAME already-built, already-corroborated mechanism _fn_closure_health's
    # `closure_health` composition uses, not a second counting mechanism (#139) — and its
    # `open_both` bucket is the one genuinely, unambiguously open count. ADDITIVE, never
    # replacing open_threads_more/open_threads_note above: those still drive the wall's own
    # LISTING (individual rows a mind should look at, property-based on purpose — a stale
    # `disagree` row is exactly the kind of thing worth a mind's eyes), this is only the
    # headline NUMBER a coordinator's scheduling math should actually use. Cheap: no per-
    # thread artifact-resolution enrichment (that N+1 stays inside closure_health's own,
    # deliberately richer, deliberately not-hot-path call).
    from src.orchestrator.thread_closure import closure_buckets
    cb = await closure_buckets(pool, repo=proj)
    out["open_threads_honest_total"] = len(cb["open_both"])
    out["open_threads_honest_note"] = (
        f"{len(cb['open_both'])} of {cb['total']} threads in this project are genuinely "
        "open by TOPOLOGY (no closure edge, status='open') — the count above counts by the "
        "status property alone and can run well above this; run_composition('closure_health', "
        "subject=<this project>) for the full breakdown")
    if cb["disagree"]:  # rare — a closure edge exists yet the property still says 'open'
        out["open_threads_disagreement"] = (
            f"{len(cb['disagree'])} thread(s) carry a closure edge AND status='open' — a "
            "real conflict, never auto-resolved; run_composition('closure_health', "
            "subject=<this project>) to see which")
    if echoes:
        out["unread_echoes"] = {
            "count": len(echoes),
            "note": (f"{len(echoes)} open threads off the wall — miner echoes no mind has "
                     "touched, plus judged questions. Still OPEN in the record; "
                     "run_composition('echoes') lists them all"),
            "triage": [{"id": e["id"], "born": e["born"],
                        "summary": e["summary"][:160]} for e in echoes[:3]],
            "verbs": ("read each; then: real owed work → reclassify_thread(id, "
                      "kind='obligation') · done or moot → resolve_thread(id, because=…) · "
                      "a question, not work → reclassify_thread(id, kind='question'). "
                      "Your judgment is testimony; never resolve what merely looks stale."),
        }
    if len(recent_decisions) == 15:  # the composition's own take(n=15) — a full page means
        # more MAY exist; count for real rather than assume (task #60, symmetry with
        # open_threads_more). Mirrors the composition's own filter exactly (project-scoped,
        # active, no winning superseded_by/retracted) — never touch the composition itself
        # just to learn its own total, that's what this count is for.
        total = await pool.fetchval(
            "SELECT count(*) FROM objects o "
            "JOIN links l ON l.from_id=o.id AND l.type='in_repo' AND l.to_id=$1 "
            "WHERE o.type='Decision' AND o.status='active' "
            "AND NOT EXISTS (SELECT 1 FROM current_assertions s WHERE s.object_id=o.id "
            "  AND s.name='superseded_by') "
            "AND NOT EXISTS (SELECT 1 FROM current_assertions s WHERE s.object_id=o.id "
            "  AND s.name='retracted')", proj)
        if total and total > 15:
            out["recent_decisions_more"] = total - 15
    # TERSE BY DEFAULT (task #60, thread b81b0fac): the byte-per-key measurement named the
    # real weight — summary text is 96-98% of every open_threads/recent_decisions item.
    # _cap_text (not _terse: truncation, not deletion) shortens it in terse mode; verbose
    # restores full summaries exactly as today. Every decision item now also carries `id`
    # (compositions.py's _table gained the magic "id" property for this) so a capped
    # summary is addressable — verbose=True or search(query=...) recovers the rest.
    if not verbose:
        _cap_text(out["open_threads"], "summary", exempt_when_true="is_handoff")
        _cap_text(out["recent_decisions"], "summary", exempt_when_true="is_handoff")
    return out



# ---- Phase 2: GRANULAR GETTERS (graphy tool surface) -------------------

@mcp.tool()
async def get_status(ctx: Context | None = None) -> dict[str, Any]:
    """Your identity, mail count, and fleet pulse -- the "glance". Returns only:
    you, model, project, seat, mail, fleet_pulse. No threads, no succession."""
    pool = await _pool_get()
    ident = await _ident_for(ctx)
    proj = ident.project if ident else None
    reader = ident.agent_id if ident else ""
    lease = get_settings().osiris_mail_lease_secs
    counts = (await unread_counts(pool, proj, reader_agent=reader, lease_secs=lease)
              if proj else {"total": 0, "ask": 0})
    unread, asks = counts["total"], counts["ask"]
    mail = (f"{unread} unread ({asks} ask something of you) -- inbox()"
            if asks else f"{unread} unread -- inbox()") if unread else "none"
    pulse = None
    try:
        pulse = await mounts.fleet_pulse(pool, lease_secs=lease)
    except Exception:
        pass
    result = {"you": ident.agent_id if ident else "unmounted", "project": proj}
    if ident:
        sb = await seat_bearings(pool, ident.agent_id)
        result["model"] = ident.model
        if sb:
            result.update(sb)
    result["mail"] = mail
    if pulse:
        result["fleet_pulse"] = pulse
    return result


async def _charter_scoped_project_ids(
    pool: asyncpg.Pool, ctx: Context | None, project: str, proj_id: Any,
) -> tuple[list[Any], list[str]]:
    """CHARTER-AWARE READ SCOPE (Wave 4, thread 5ec2b82d — Soundwave's #1 defect): a
    chartered seat's OWN writes land under EVERY repo it governs (`in_repo` follows the
    write's own target, not the caller's mount), but `get_thread_list`/`get_decision_list`
    used to resolve exactly one literal `repo:{project}` and stop there — a successor
    mounting under any ONE name in a multi-repo charter saw only that slice of its own
    seat's work, silently, at every succession. `settle()` already detects and names this
    exact shape three times over (the "filed under X but its own writes went to [X,Y]"
    warning, decision-index workarounds already rotting under Soundwave/chronohorn) — the
    charter already knows what a seat governs; these two read verbs simply never asked it.

    DEFAULT-TO-CHARTER, not an opt-in `spans_charter` flag (Thoth's own two named shapes,
    thread 5ec2b82d — this is the chosen one, not the only one considered): an opt-in flag
    does nothing for the successor who does not know to ask for it, which is the entire
    failure mode Soundwave hit — the same "gate exists, nobody calls it" shape #189/#52
    were built to stop being acceptable. Defaulting closes the gap for every future
    successor without requiring them to learn a parameter first.

    THE ACL BOUNDARY (#42's reflection ACL / cross-project boundary — read this before
    touching this function): a chartered seat reading its OWN chartered repos is within
    authority; anything wider is a data leak between houses. This function can NEVER widen
    past the caller's own charter, by construction, not by a permission check that could
    drift: it only ever expands the scope when `project` (the literal name the caller
    asked for) is ITSELF a member of the CALLING SEAT's own `governs` set — the widened
    set is then that SAME charter, nothing else. A caller peeking at a project it does not
    govern (no ident, no held seat, or `project` absent from its own charter) gets the
    exact single-repo behavior this tool always had — unchanged, and never widened on
    someone else's behalf. Returns (project object ids to scope the query to, the full
    charter list — empty unless expansion actually applied, so a caller can tell whether
    it got one repo's items or several)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return [proj_id], []
    from src.orchestrator.charter import charter_of
    from src.orchestrator.seats import held_seat

    charter_seat = await held_seat(pool, ident.agent_id)
    if charter_seat is None:
        return [proj_id], []
    charter_repos = await charter_of(pool, charter_seat["seat_id"])
    if project not in charter_repos or len(charter_repos) <= 1:
        return [proj_id], []
    rows = await pool.fetch(
        "SELECT id FROM objects WHERE type='SoftwareProject' AND canonical = ANY($1)",
        [f"repo:{r}" for r in charter_repos])
    return [r["id"] for r in rows], charter_repos


@mcp.tool()
async def get_thread_list(
    project: str, kind: str | None = None, owner: str | None = None,
    limit: int = 10, offset: int = 0, ctx: Context | None = None,
) -> dict[str, Any]:
    """Open threads for a project, paginated (charter-aware — spans the caller's own
    governed repos; see `charter_repos`). Returns {threads, total, more}.
    kind filter: obligation/question/task. owner filter: agent id / 'operator'.
    limit=0 for count only (no bodies)."""
    pool = await _pool_get()
    proj = await pool.fetchval(
        "SELECT id FROM objects WHERE type='SoftwareProject' AND canonical=$1",
        f"repo:{project}")
    if proj is None:
        return {"error": f"no project {project!r}", "threads": [], "total": 0}
    project_ids, charter_repos = await _charter_scoped_project_ids(pool, ctx, project, proj)
    # dispatch #195 defect 2, measured live before this fix: 75.5% false-open (2,553 of
    # 3,380 "active" Thread objects were actually resolved/retracted). `o.status='active'`
    # is the OBJECT's own lifecycle column (active vs merged/retired) — a completely
    # different fact from the thread's own `status` PROPERTY (open/resolved), which
    # resolve_thread sets via a superseding assert_property and never touches `o.status`
    # at all. This clause was simply missing; find_near_duplicate_open_thread's own query
    # (capture.py) already gets it right with the identical COALESCE(...,'open')='open'
    # pattern this now matches.
    clauses = [
        "o.type='Thread' AND o.status='active' AND COALESCE("
        "(SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        " AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),"
        "'open')='open'"
    ]
    params: list[Any] = [project_ids]
    idx = 2
    if kind:
        clauses.append("(SELECT a.value #>> '{}' FROM current_assertions a "
                       "WHERE a.object_id=o.id AND a.name='kind' "
                       "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) = $" + str(idx))
        params.append(kind)
        idx += 1
    if owner:
        clauses.append("(SELECT a.value #>> '{}' FROM current_assertions a "
                       "WHERE a.object_id=o.id AND a.name='owner' "
                       "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) = $" + str(idx))
        params.append(owner)
        idx += 1
    where = " AND ".join(clauses)
    total = await pool.fetchval(
        "SELECT count(*) FROM objects o "
        "JOIN links l ON l.from_id=o.id AND l.type='in_repo' AND l.to_id = ANY($1) "
        "WHERE " + where, *params) or 0
    result: dict[str, Any] = {"project": project}
    if charter_repos:
        result["charter_repos"] = charter_repos
    # THE HONEST COUNT (thread 0ae050d8, Thoth DM 6243), same additive law as orient()'s own
    # open_threads_honest_total: `total` above counts by the `status` PROPERTY (unchanged —
    # an existing field's meaning never changes silently, #139's sibling law for contracts).
    # `honest_total` is a NEW, separate field — summed over `project_ids` (a chartered seat's
    # own small repo set, never fleet-wide) via closure_buckets, the same shared mechanism
    # orient() and closure_health both already use, not a second counting path. `kind`/`owner`
    # filters do NOT narrow this count (thread_closure_status has no such filters of its own,
    # and the honest count is meant to answer "how much is REALLY open", not "how much of
    # this filtered slice" — a caller filtering by kind/owner still gets the whole-project
    # honest denominator, named plainly so it isn't misread as scoped to the filter).
    from src.orchestrator.thread_closure import closure_buckets
    honest_total = 0
    honest_disagree = 0
    for pid in project_ids:
        cb = await closure_buckets(pool, repo=pid)
        honest_total += len(cb["open_both"])
        honest_disagree += len(cb["disagree"])
    result["honest_total"] = honest_total
    result["honest_total_note"] = (
        f"{honest_total} threads are genuinely open by TOPOLOGY across this project's own "
        "charter scope (no closure edge, status='open') — `total` above counts by the status "
        "property alone and is not filter-scoped the same way; run_composition("
        "'closure_health') for the full breakdown"
        + (f"; {honest_disagree} more carry a closure edge AND status='open' — a real "
           "conflict, never auto-resolved" if honest_disagree else ""))
    if limit == 0:
        return {**result, "threads": [], "total": total, "more": total}
    rows = await pool.fetch(
        "SELECT o.id, o.canonical, "
        "(SELECT a.value #>> '{}' FROM current_assertions a "
        " WHERE a.object_id=o.id AND a.name='summary' "
        " ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS summary, "
        "(SELECT a.value #>> '{}' FROM current_assertions a "
        " WHERE a.object_id=o.id AND a.name='kind' "
        " ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind, "
        "(SELECT a.value #>> '{}' FROM current_assertions a "
        " WHERE a.object_id=o.id AND a.name='owner' "
        " ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS owner "
        "FROM objects o "
        "JOIN links l ON l.from_id=o.id AND l.type='in_repo' AND l.to_id = ANY($1) "
        "WHERE " + where + " "
        "ORDER BY o.created_at DESC "
        "OFFSET $" + str(idx) + " LIMIT $" + str(idx + 1),
        *params, offset, limit)
    threads = []
    for r in rows:
        threads.append({"id": str(r["id"])[:8], "canonical": r["canonical"],
                        "summary": (r["summary"] or "")[:200],
                        "kind": r["kind"], "owner": r["owner"]})
    more = max(0, total - offset - len(threads))
    return {**result, "threads": threads, "total": total, "more": more,
            "note": "recall(ref) for full text; orient() for the ranked wall"}


@mcp.tool()
async def get_decision_list(
    project: str, limit: int = 10, offset: int = 0, ctx: Context | None = None,
) -> dict[str, Any]:
    """Recent decisions for a project, paginated (charter-aware — spans the caller's own
    governed repos; see `charter_repos`). Returns {decisions, total, more}.
    limit=0 for count only."""
    pool = await _pool_get()
    proj = await pool.fetchval(
        "SELECT id FROM objects WHERE type='SoftwareProject' AND canonical=$1",
        f"repo:{project}")
    if proj is None:
        return {"error": f"no project {project!r}", "decisions": [], "total": 0}
    project_ids, charter_repos = await _charter_scoped_project_ids(pool, ctx, project, proj)
    total = await pool.fetchval(
        "SELECT count(*) FROM objects o "
        "JOIN links l ON l.from_id=o.id AND l.type='in_repo' AND l.to_id = ANY($1) "
        "WHERE o.type='Decision' AND o.status='active' "
        "AND NOT EXISTS (SELECT 1 FROM current_assertions s WHERE s.object_id=o.id "
        "  AND s.name='superseded_by')", project_ids) or 0
    result: dict[str, Any] = {"project": project}
    if charter_repos:
        result["charter_repos"] = charter_repos
    if limit == 0:
        return {**result, "decisions": [], "total": total, "more": total}
    rows = await pool.fetch(
        "SELECT o.id, o.canonical, "
        "(SELECT a.value #>> '{}' FROM current_assertions a "
        " WHERE a.object_id=o.id AND a.name='summary' "
        " ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS summary, "
        "(SELECT a.value #>> '{}' FROM current_assertions a "
        " WHERE a.object_id=o.id AND a.name='kind' "
        " ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind "
        "FROM objects o "
        "JOIN links l ON l.from_id=o.id AND l.type='in_repo' AND l.to_id = ANY($1) "
        "WHERE o.type='Decision' AND o.status='active' "
        "AND NOT EXISTS (SELECT 1 FROM current_assertions s WHERE s.object_id=o.id "
        "  AND s.name='superseded_by') "
        "ORDER BY o.created_at DESC "
        "OFFSET $2 LIMIT $3", project_ids, offset, limit)
    decisions = []
    for r in rows:
        decisions.append({"id": str(r["id"])[:8], "canonical": r["canonical"],
                          "summary": (r["summary"] or "")[:200],
                          "kind": r["kind"]})
    more = max(0, total - offset - len(decisions))
    return {**result, "decisions": decisions, "total": total, "more": more}


@mcp.tool()
async def graph_search(
    query: str, project: str | None = None, lineage: str | None = None,
    max_depth: int = 0, limit: int = 15, ctx: Context | None = None,
) -> dict[str, Any]:
    """GRAPH-AWARE search -- same lexical/semantic engine as search() but scoped
    to a subgraph. project narrows results to one project. lineage scopes to
    a specific agent lineage (e.g. 'ad1a1cb0'). max_depth > 0 expands results
    to include the N-hop neighborhood around each hit (linked objects).
    Without scope params, behaves exactly like search()."""
    pool = await _pool_get()
    ident = await _ident_for(ctx)
    caller = ident.agent_id if ident else None
    spec = {"op": "function", "name": "search",
            "args": {"q": query, "limit": limit, "caller": caller,
                     "project": project, "lineage": lineage, "max_depth": max_depth}}
    out = await comp.run_spec(pool, spec, None, name="graph_search", caller=caller)
    items = out.get("items", {})
    hits = items.get("hits", [])
    return {"hits": hits, "q": query,
            "scoped_to": {"project": project, "lineage": lineage}}


@mcp.tool()
async def get_mail(ctx: Context | None = None) -> dict[str, Any]:
    """Your inbox status: unread count, asks, operator briefs -- one query,
    no threads, no succession, no fleet pulse. The cheapest orient alternative."""
    pool = await _pool_get()
    ident = await _ident_for(ctx)
    proj = ident.project if ident else None
    if not ident or not proj:
        return {"mail": "unmounted -- call mount(cwd) first"}
    lease = get_settings().osiris_mail_lease_secs
    counts = await unread_counts(pool, proj, reader_agent=ident.agent_id, lease_secs=lease)
    if counts is None:
        counts = {"total": 0, "ask": 0}
    op_unread = await mailbox.desk_briefs_from(pool, ident.agent_id)
    return {"you": ident.agent_id, "project": proj, "mail": counts,
            "operator_mail": op_unread if isinstance(op_unread, int) else None}


@mcp.tool()
async def orient(project: str | None = None, subagent_id: str | None = None,
                 subagent_type: str | None = None, session_anchor: str | None = None,
                 verbose: bool = False, want_blind_spots: bool = False,
                 ctx: Context | None = None) -> dict[str, Any]:
    """Get your bearings — the mount ritual as one call. Returns a scoped briefing: open
    threads + recent decisions for a project, plus a fleet-wide not-shown count. An
    explicit `project` overrides your mount; unmounted with neither gives the whole-fleet
    briefing. Call after mount(), and again after any compaction.

    `verbose=True` restores the prose terse mode (default) trims — explanations, the
    ancestor-letter pointer, full-length summaries (capped to 160 chars terse, each still
    carrying `id`). Every structured fact survives either way.

    `want_blind_spots=True` returns the full list; default is a count."""
    pool = await _pool_get()
    lease = get_settings().osiris_mail_lease_secs
    ident = await _ident_for(ctx, session_anchor)
    proj = project or (ident.project if ident else None)  # explicit scope overrides the mount
    who = ident.agent_id if ident else "session (un-mounted — call mount(cwd) first)"
    reader = ident.agent_id if ident else (proj or "")
    # a SPAWN asking for bearings must not be told it IS the seat: 'you' is the child, the
    # seat's swap confession is the parent's duty, and the parent's mailbox stays the parent's
    spawn = await _actor_for(ctx, subagent_id, subagent_type) if subagent_id else None
    if spawn is not None and spawn != (ident.agent_id if ident else None):
        who = f"{spawn} — a SPAWN of {ident.agent_id if ident else 'an unmounted parent'}; " \
              "your writes are your own, the seat and its mail are your parent's"
    counts = (await unread_counts(pool, proj, reader_agent=reader, lease_secs=lease)
              if proj else {"total": 0, "ask": 0})
    unread, asks = counts["total"], counts["ask"]
    mail = (f"{unread} unread ({asks} ask{'s' if asks == 1 else ''} something of you) — "
            "inbox()" if asks else f"{unread} unread — inbox()") if unread else "none"
    # the desk, SCOPED (operator ruling, 2026-07-16): this seat's own unanswered briefs
    op_unread = await mailbox.desk_briefs_from(pool, ident.agent_id if ident else None)
    op_mail = {"operator_mail": f"{op_unread} of your briefs await the operator's eye — "
                                "inbox(project='operator') if the human is present"
               } if op_unread else {}
    # THE CHARTER, MADE VISIBLE (Phase 1 §4.1, `dd47c1da`): a house is what a seat RULES, not
    # where it sits — but a charter nobody can see is not an inheritance. No aggregation here
    # (that's wave 2's charter-scoped briefing); just the fact, named.
    #
    # RE-KEYED ONTO THE SEAT (ruling 1db1ff41), not a lineage walk: `governs` now originates
    # from the seat's own durable object id, so no LIKE-prefix guess is needed — held_seat is
    # the SAME lineage-aware resolution orient() already trusts for the seat line below.
    # DISSOLVES the old set_charter limitation named at Lane C (decision 1913683e): a
    # successor re-declaring now heals the SAME from_id an ancestor generation used — there is
    # no ancestor/successor distinction left to trip over, one seat, one link.
    #
    # TASK #157 PIECE 2, SPECIMEN 14 OF 60bc15db (operator's own words "fix the slop"):
    # the render below used to fold this key in with `if charter else {}` — an idiom copied
    # from swap/pin_warn, where falsy means "nothing wrong" and omission is correct. For
    # charter, falsy ([]) IS the alarm state, so the SAME idiom silently rendered "chartered,
    # all fine" and "never declared" as the identical silence, on the one surface every seat
    # reads every session (confirmed live on this seat's own reign: 26 of 33 active seats
    # read `charter` absent from their own orient(), including this one). Gated on
    # `charter_seat is not None` now, not on `charter` truthiness — a session holding no seat
    # at all has nothing to charter and stays silent (this is not a seat-only alarm turned
    # into a universal one); a session that DOES hold a seat gets told the truth either way,
    # stated once and plainly (`_CHARTER_UNDECLARED`, the same text mint_seat's and
    # establish_office's own receipts already use), never a repeated `⚠` banner. NOTE, named
    # rather than quietly assumed: `charter_of` cannot currently distinguish "never declared"
    # from "declared as governing zero repos" (`set_charter(repos=[])` heals every existing
    # edge and leaves no trace it was ever called) — both read back as the identical empty
    # list, so both render as UNDECLARED here. That is an honest limit of the data model, not
    # a bug this piece introduces or is scoped to fix.
    from src.orchestrator.charter import charter_of
    from src.orchestrator.offices import _CHARTER_UNDECLARED
    from src.orchestrator.seats import held_seat
    charter_seat = await held_seat(pool, ident.agent_id) if ident else None
    charter = await charter_of(pool, charter_seat["seat_id"]) if charter_seat else []
    # THE STANDING-CHOICE STANDDOWN (Metron IV, wave-2 fa918939): a repo whose model
    # choice is SETTLED — a .osiris file, or an intended_model property recorded on the
    # SoftwareProject — must not re-confront every successor with the fleet default.
    # A settled seam is not even a seam; every banner consults _expected_model first.
    swap = swap_banner(classify_swap(
        ident.model_history, ident.model,
        expected=await _expected_model(pool, ident.cwd, proj),
        anchored=ident.model_method == "job_dir",
        deliberate=ident.model_deliberate)) if ident else None
    if spawn is not None:
        swap = None  # the seat's swap history is the PARENT's confession duty, not the child's
    pin_warn = project_pin_banner(ident) if ident else None  # no/unparseable/found-unset pin
    if swap and ident:  # a triage wake on the economy model is policy, not a rug-pull
        swap = await _wake_economy_standdown(pool, proj, ident.model) or swap
    away = await mounts.while_away(
        pool, proj, ident.agent_id, _prev_seen.get(ident.agent_id)) if ident else None
    # THE SUCCESSION NOTE (Anubis VIII, msg 236: 'orient() has no succession-note field —
    # I reconstructed my inheritance from an open thread'): a successor's orient surfaces
    # the ancestor's own parting words — its HANDOFF thread and LETTER decision — verbatim,
    # instead of promising a field that never existed.
    #
    # STRUCTURED FIRST, PROSE AS FALLBACK (ruling c5b184cd, /settle): word-matching identity
    # is the disease behind every 'Thoth II'-style mislabel this house has hit — an
    # is_handoff='true' property (stamped by settle(), a typed query) is the reliable half;
    # the ILIKE '%handoff%'/'%letter%' text match stays ONLY for handoffs minted before this
    # existed, never removed, never the sole check for anything settle() writes going forward.
    # BOUNDED CHAIN-WALK (thread e749036e, 2026-07-27): a one-hop-only read goes blind the
    # moment the IMMEDIATE ancestor never wrote a handoff (a phantom, or simply silent) even
    # though a real one sits further back — nearest_handoff_ancestor (agents.py) walks up to
    # 5 succeeded_from links, shared with the boot whisper so both read one implementation.
    # READ RECEIPT, NOT INFERRED-READ (operator ruling, 2026-08-03, superseding a3e2851's
    # write-triggered retirement): delivery here is UNCONDITIONAL — this block never writes
    # anything, so the non-negotiable acceptance test (a fresh seat's first orient() must
    # receive its predecessor's handoff WHOLE) holds by construction, not by careful
    # ordering. What makes a handoff stop being delivered is a SEPARATE, deliberate
    # ack_handoff(ref=...) call, mirroring inbox()'s own lease-vs-settle split — an
    # unacknowledged handoff redelivers on every orient(), exactly like unsettled mail.
    inheritance = None
    if ident and ident.succeeded_from:
        found, _complete = await nearest_handoff_ancestor(pool, ident.succeeded_from)
        if found:
            from_id, picks = found
            inheritance = {
                "from": from_id,
                "notes": [{"kind": r["type"].lower(), "id": str(r["id"])[:8],
                           "text": r["summary"][:800]}
                          for r in picks],
                "note": "your ancestor's own parting words — read before taking up work. "
                        "ack_handoff(ref=<id>) once you have: an unacknowledged handoff "
                        "stays live and keeps costing every future orient() in this "
                        "project, not just yours.",
            }
    # #145's DISCOVERY HALF (decision b89477a0/61cb1f02): a lineage-scoped, not project-
    # scoped, misfiling finder — where identity_coherence (settle.py) can only ever see
    # THIS session's own writes, this can see every generation's, so a correctly-filed
    # successor can find an ancestor's misfiled work. Report-only, never a gate.
    misfiled = (await misfiled_by_lineage(pool, ident.agent_id, proj)
               if ident and proj else None)
    # CO-AGENT AWARENESS (Deckard XXVI, msg 258: a live sibling shared his exact worktree
    # and the graph never said so — he re-derived 'never git add -A' from a local file
    # while osiris KNEW). One query: other live mounts on THIS project, named at orient.
    co_agents = await _co_agents(pool, proj, ident.agent_id) if ident and proj else None
    # THE PEER BLOCK (ruling d74492ee, spec e6636c7e — LEGIBILITY leg 2): a peer_of bond
    # is recognition-first per Ostrom p7 — an edge nobody's briefing ever surfaces is a
    # convention, ignorable exactly like co_agents' shared tree used to be before Deckard's
    # msg 258. Computed off ident.agent_id (never `who`, which can carry a spawn's
    # description string) — same discipline co_agents already follows.
    peer = await _peer_bearings(pool, ident.agent_id) if ident else None
    try:  # one glance line — never let the pulse slow or crash orient
        pulse: str | None = await mounts.fleet_pulse(pool, lease_secs=lease)
    except Exception:  # noqa: BLE001
        pulse = None
    # THE ORGANS. If the miner is down, the graph is NOT forming memory — and every mind that
    # mounts is about to trust a record that stopped growing. It went unnoticed for ten hours
    # because the only witness was a counter inside a payload too large to open (79e1328c).
    # Derived at READ time, here, in a process that is alive by construction: a watchdog cron
    # would have lived inside the very worker that died. Silent when the body is well.
    try:
        organs: str | None = health_banner(await organ_health(pool))
    except Exception:  # noqa: BLE001
        organs = None
    # THE ADVERSARY'S PILE AND ITS LICENCE. A gate nobody can see is a gate nobody trusts, and the
    # whole root cause was that nothing surfaced whether the producer's output was ever USED. So
    # the seat sees its own undisposed pile, and — when the adversary has spent itself out of a
    # licence — the number that took it away.
    seam: dict[str, Any] = {}
    with contextlib.suppress(Exception):
        pile = await dispose_seam.candidates(pool, project=proj, limit=0) if proj else None
        if pile and pile["count"]:
            seam["your_pile"] = (
                f"{pile['count']} miner candidates on {proj} that no mind has judged. They are "
                "GUESSES, not duties — candidates() to read, dispose() to settle. Nobody else has "
                "standing to judge your project's pile.")
        lic = await dispose_seam.licence(pool)
        if not lic["may_spend"]:
            seam["adversary_refused"] = lic["reason"]
    # THE DEAD SUPERSTITIONS (thread a9be40c9): fleet-wide by design — a workaround
    # replicates across houses, so the announcement of its death must too. Bounded window;
    # silent when nothing died recently; search remembers every kill forever.
    dead: dict[str, Any] = {}
    with contextlib.suppress(Exception):
        kills = await capture.recent_dead_superstitions(pool)
        if kills:
            dead["dead_superstitions"] = {
                "recent": kills,
                "note": "workarounds whose bug is FIXED — if your memory, letters or "
                        "succession notes carry one of these practices, STRIKE it; the "
                        "killed_by pointer is the fix to cite",
            }
    # THE SWEEP RECEIPT (Finding A, thread 5177057a, Thoth's design approval DM 1326, NON-
    # optional): a fresh compaction's own mining sweep is async and the seam gives no
    # confirmation it landed. Rather than let a successor trust that silently, orient checks
    # THIS lineage's own most recent sweep_ledger row — if it's still incomplete past the
    # watchdog's own SLA (arq_worker.SWEEP_RETRY_SLA=300s, duplicated here on purpose: "the
    # miner mines, the server only rings" is a deliberate ownership boundary, sweep_route/
    # orient never import the worker module), the successor is told plainly instead of
    # silently trusting an unconfirmed predecessor. Same family as swap_banner/
    # notify-at-seam: a confession the running mind cannot feel on its own, so NEVER stripped
    # by the terse pass below (same discipline as `swap`).
    sweep_receipt: dict[str, Any] = {}
    if ident:
        with contextlib.suppress(Exception):
            row = await pool.fetchrow(
                "SELECT enqueued_at, completed_at, extract(epoch FROM now() - enqueued_at) "
                "AS age_secs FROM sweep_ledger WHERE session_id = $1 "
                "ORDER BY enqueued_at DESC LIMIT 1", ident.session)
            if row and row["completed_at"] is None and row["age_secs"] > 300:
                sweep_receipt["sweep_unconfirmed"] = (
                    f"your last compaction's mining sweep (enqueued "
                    f"{int(row['age_secs'] // 60)} min ago) has not confirmed completion — "
                    "the watchdog retries it automatically; nothing to act on, but don't "
                    "assume that seam's yield has landed in the graph yet."
                )
    # the reader's identity feeds the wall's ownership ordering: what is MINE TO ACT rides
    # above another mind's claims and above 'waiting on the human' — ONE AUTHORITY with
    # automount()/whisper's own identical need (compositions.reader_identity_set, #185 leg
    # (a)): folds in the seat's own HANDLE too, not just agent_id/project, so a charter
    # obligation filed owner='<handle>' ranks as mine here exactly as it does at whisper.
    from src.orchestrator.compositions import reader_identity_set
    me = await reader_identity_set(
        pool, agent_id=(ident.agent_id if ident else None), project=proj)
    scoped = (await _project_briefing(pool, proj, me=me, verbose=verbose,
                                      want_blind_spots=want_blind_spots)
             if proj else None)
    if scoped is not None:
        fleet_open = await pool.fetchval(
            "SELECT count(*) FROM objects o WHERE o.type='Thread' AND o.status='active' "
            "AND (SELECT s.value #>> '{}' FROM current_assertions s WHERE s.object_id=o.id "
            "  AND s.name='status' ORDER BY s.confidence DESC, s.observed_at DESC LIMIT 1)"
            "  = 'open'")
        result = {
            "you": who, "model": (ident.model if ident else None), "project": proj,
            **({"osiris_health": organs} if organs else {}),
            **seam,
            **(await seat_bearings(pool, who) if who else {}),
            "mail": mail,
            **({"fleet_pulse": pulse} if pulse else {}),
            **op_mail,
            **({"charter": charter or _CHARTER_UNDECLARED} if charter_seat is not None else {}),
            **({"swap": swap} if swap else {}),
            **({"project_pin_error": pin_warn} if pin_warn else {}),
            **sweep_receipt,
            **({"succession_note": inheritance} if inheritance else {}),
            **({"misfiled_elsewhere": misfiled} if misfiled else {}),
            **({"co_agents": co_agents} if co_agents else {}),
            **({"peer": peer} if peer else {}),
            **({"while_you_were_away": away} if away else {}),
            **dead,
            **scoped,
            "fleet_open_threads_total": fleet_open,
            "note": f"scoped to {proj}; {fleet_open} fleet-wide open threads not shown "
                    "(run_composition('briefing') for the whole graph).",
        }
        # TERSE BY DEFAULT (task #55): the paths below are fully redundant with a structured
        # sibling already in this dict (the top-level note restates fleet_open_threads_total;
        # open_threads_note restates open_threads_more; unread_echoes/blind_spots/
        # dead_superstitions keep their data lists, only the "here's what to do about it"
        # sentence drops). NEVER touches `swap` — the identity-safety confession, not
        # guidance. CORRECTION (Thoth's review, DM 1238, thread 1233): co_agents.note is
        # the SHARED-TREE SAFETY WARNING ('never git add -A, stage your own hunks, check
        # foreign markers') — the `live` list says WHO is here, this says WHAT TO DO about
        # it, and it's conditional (only present with live siblings) so it's not per-call
        # bloat. Same class as the identity banners; it slipped through the first pass.
        # succession_note.note stays too — a pre-existing test (test_capture.py) asserts
        # it unconditionally; restoring the tested contract rather than re-litigating it
        # inside the same fix that caught this class of miss.
        return result if verbose else _terse(
            result, ("note",), ("open_threads_note",), ("unread_echoes", "note"),
            ("unread_echoes", "verbs"), ("blind_spots_note",),
            ("dead_superstitions", "note"))
    # THE UN-MOUNTED CAP (Metron IV, wave-2 fa918939: a fresh session's first orient
    # returned 353K chars of whole-fleet briefing it had to jq from a dump file). An
    # un-mounted caller gets a BOUNDED map — per-project open counts + the newest few
    # decisions — and the mount ritual; the firehose stays one deliberate call away.
    fleet_map = [dict(r) for r in await pool.fetch(
        "SELECT p.canonical AS project, count(*) AS open_threads "
        "FROM objects o JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' AND p.status='active' "
        "WHERE o.type='Thread' AND o.status='active' "
        "AND (SELECT s.value #>> '{}' FROM current_assertions s WHERE s.object_id=o.id "
        "  AND s.name='status' ORDER BY s.confidence DESC, s.observed_at DESC LIMIT 1)"
        "  = 'open' "
        "GROUP BY p.canonical ORDER BY count(*) DESC LIMIT 20")]
    # UNFILED (Thoth DM 2704, finding 3 of the in_repo audit): the per-project GROUP BY
    # above INNER JOINs in_repo, so it structurally cannot file a thread with no project at
    # all — a fresh agent's very FIRST fleet view used to drop them with zero disclosure.
    # Declared, not compensated: there is no "project" to attribute an unfiled thread to.
    fleet_map_unfiled = await pool.fetchval(
        "SELECT count(*) FROM objects o WHERE o.type='Thread' AND o.status='active' "
        "AND (SELECT s.value #>> '{}' FROM current_assertions s WHERE s.object_id=o.id "
        "  AND s.name='status' ORDER BY s.confidence DESC, s.observed_at DESC LIMIT 1)"
        "  = 'open' "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id AND l.type='in_repo')")
    recent = [r["summary"][:160] for r in await pool.fetch(
        "SELECT (SELECT s.value #>> '{}' FROM current_assertions s WHERE s.object_id=o.id "
        "  AND s.name='summary' ORDER BY s.confidence DESC, s.observed_at DESC LIMIT 1) "
        "  AS summary "
        "FROM objects o WHERE o.type='Decision' AND o.status='active' "
        "AND COALESCE((SELECT s.value #>> '{}' FROM current_assertions s "
        "  WHERE s.object_id=o.id AND s.name='superseded_by' "
        "  ORDER BY s.confidence DESC, s.observed_at DESC LIMIT 1),'')='' "
        "ORDER BY o.created_at DESC LIMIT 5") if r["summary"]]
    result = {
        "you": who, "model": (ident.model if ident else None), "project": proj,
        **(await seat_bearings(pool, who) if who else {}),
        "mail": mail,
        **({"fleet_pulse": pulse} if pulse else {}),
        **op_mail,
        **({"charter": charter or _CHARTER_UNDECLARED} if charter_seat is not None else {}),
        **({"swap": swap} if swap else {}),
        **({"project_pin_error": pin_warn} if pin_warn else {}),
        **({"succession_note": inheritance} if inheritance else {}),
        **({"misfiled_elsewhere": misfiled} if misfiled else {}),
        **({"co_agents": co_agents} if co_agents else {}),
        **({"peer": peer} if peer else {}),
        **({"while_you_were_away": away} if away else {}),
        **({"osiris_health": organs} if organs else {}),
        **seam,
        **dead,
        "fleet_map": fleet_map,
        "fleet_map_unfiled": fleet_map_unfiled,
        "recent_decisions": recent,
        "note": "un-mounted → the BOUNDED fleet map, never the firehose. mount(cwd, "
                "job_dir=…) then orient() for your project's briefing; orient(project=…) "
                "peeks at another's; run_composition('briefing') if you truly want the "
                "whole graph. fleet_map_unfiled: open threads with no in_repo edge at all — "
                "counted nowhere in fleet_map above, because there is no project to file "
                "them under.",
    }
    # CORRECTION (Thoth's review, DM 1238, thread 1233): this branch's top-level note is
    # asserted unconditionally by a pre-existing test (test_unmounted_orient_is_a_
    # bounded_map_never_the_firehose) — restoring the tested contract rather than
    # re-litigating it inside the regression fix, same call as co_agents/succession_note
    # above. Nothing left here is terse-safe to strip; `verbose` stays accepted for
    # symmetry with the scoped branch and any future addition.
    return result


@mcp.tool()
async def fleet_digest(hours: int | None = None, mark_seen: bool = False) -> dict[str, Any]:
    """The MEMBRANE — the operator's window into the autonomous fleet. Surfaces ROSTER +
    health (which identities resolved cleanly), ACTIVITY (what agents decided/opened
    in your name, not the miner's backfill), the DANGER map (model swaps — the
    harness's silent demotions), LAUNDERING (credence flags where a relay carried a
    fact above its origin grade), and SPEND (metered honestly).

    `hours` given → an ad-hoc rolling window. `hours=None` (default) → WATERMARK MODE:
    'what's new since I last looked', from the stored operator watermark (24h fallback
    the first time). Glancing is a PEEK — it never moves the watermark. Pass
    `mark_seen=True` when done reading to advance it to now."""
    pool = await _pool_get()
    since = (datetime.now(UTC) - timedelta(hours=hours)) if hours is not None else None
    dg = await digest.fleet_digest(Actions(pool), since=since, mark_seen=mark_seen,
                                   lease_secs=get_settings().osiris_mail_lease_secs)
    # The console renders the ROSTER as a table and has all the room in the world; a reader with
    # a context window does not, and the roster array is a SUPERSET of `danger` — shipping both
    # sent every dangerous agent twice. The counts stay whole; the rows live behind fleet().
    dg.pop("roster", None)
    dg["roster"] = "counts only — fleet() for the live roster, fleet(full=True) for all of it"
    return {"window_hours": hours, **dg}


@mcp.tool()
async def fleet(full: bool = False) -> dict[str, Any]:
    """The roster, grouped by project — live agents expanded, retired sessions collapsed
    into a counted line. ● live / ○ historical. `full=True` expands everything and shows
    the flat `registered` rows too (default: live only, history is 1000+ rows). `seat`
    rides beside a canonical id wherever one is claimed.

    Read-only diagnostics, each best-effort: `os_bodies`/`ghost_gap` (per-identity
    false_live/false_dead — a real OS process with no live graph row, or vice versa);
    `whisper_health` (recent hook-alarm failures, a log read not an active probe);
    `harness_registry` (occupancy+identity fold-in, no second call needed);
    `landing_audit` (unmerged branches, git-vs-graph landing disagreements); `pool_health`
    (pg backend counts per daemon, cumulative `tx_total`, `caps` for the connection
    envelope). Project grouping normalizes through `merged_into`. Field detail:
    consult_canon('fleet')."""
    pool = await _pool_get()
    rows = await pool.fetch(
        "SELECT o.canonical, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='source_model' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS model, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='project' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS project, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='spawn_depth' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS depth, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='last_active' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS last_active, "
        # a SIGNED death certificate — retire()'s own act, and the only thing that earns the
        # word "retired". Only 41 of 517 root minds (8%) ever managed it; the tree used to award
        # it to anything that stopped talking (the ghosts, 53729dd6).
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='retired' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS retired, "
        # a spawn the harness ANNOUNCED but nothing ever witnessed (no transcript, no act) —
        # internal machinery (the compaction summarizer), never a seat (thread 26e1dc91)
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='spawn_witnessed' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS witnessed, "
        # the CLAIMED seat (dd47c1da) — the same handle/generation pair every other seat
        # reader (claim_name, seat_bearings, agent_seat) uses; None for an anonymous agent
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='handle' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS handle, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='seat_generation' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS seat_gen, "
        # the BINDING (Phase B, 5cef856b): the Seat object this mind actively HOLDS — the
        # declared identity beside the claimed name, rendered as ⚓seat:<id> in the tree
        " (SELECT ht.canonical FROM links hl JOIN objects ht ON ht.id=hl.to_id "
        "  WHERE hl.from_id=o.id AND hl.type='holds' AND ht.type='Seat' "
        "  AND (hl.valid_until IS NULL OR hl.valid_until > now()) "
        "  ORDER BY hl.first_seen DESC LIMIT 1) AS bound_seat, "
        " (SELECT max(m.last_seen) FROM agent_mounts m WHERE m.agent_id=o.canonical) "
        "  AS mount_seen, "
        " (SELECT m.cwd FROM agent_mounts m WHERE m.agent_id=o.canonical "
        "  ORDER BY m.last_seen DESC NULLS LAST LIMIT 1) AS cwd, "
        " (SELECT m.job_dir FROM agent_mounts m WHERE m.agent_id=o.canonical "
        "  ORDER BY m.last_seen DESC NULLS LAST LIMIT 1) AS job_dir, "
        " (SELECT p.canonical FROM links l JOIN objects p ON p.id=l.to_id "
        "  WHERE l.from_id=o.id AND l.type='spawned_by' LIMIT 1) AS parent "
        "FROM objects o WHERE o.type='Agent' AND o.status='active' ORDER BY o.canonical"
    )
    now = datetime.now(UTC)

    def _ts(r: Any) -> datetime | None:
        # freshest sign of life: the miner's transcript stamp OR the durable mount registry —
        # the SAME decision agent_liveness()'s listener probe makes (ruling 70493925: this
        # used to be two independently-written copies of "freshest of these two signals",
        # which is exactly what let the probe and fleet() disagree about the same live agent.
        return mounts.freshest_liveness_ts(r["mount_seen"], r["last_active"])

    nodes: dict[str, dict[str, Any]] = {}
    ghosts = 0
    for r in rows:
        if r["witnessed"] == "false":
            # announced-never-witnessed harness ephemera: they are in the record (the graph
            # forgets nothing) but they are not FLEET — rendering them as live seats put 42
            # phantoms in the tree in one night (2026-07-14). Counted, never shown.
            ghosts += 1
            continue
        ts = _ts(r)
        nodes[str(r["canonical"])] = {
            "model": r["model"], "project": r["project"], "parent": r["parent"],
            "depth": int(r["depth"]) if r["depth"] else 0,
            "last_active": r["last_active"], "ts": ts,
            "retired": r["retired"] in ("true", "True"),  # SIGNED, not merely silent
            "live": mounts.is_live(ts, now=now),
            "seat": seat_label(str(r["canonical"]), r["handle"],
                               int(r["seat_gen"]) if r["seat_gen"] else None),
            "bound": r["bound_seat"],
            "cwd": r["cwd"],
            "job_dir": r["job_dir"],
        }
    # PROJECT LABEL NORMALIZATION THROUGH merged_into (task #180 piece 2 (f), Henry msg 5236,
    # third surface of 3c3d9efa (b)): a project's raw `current_assertions` label can name a
    # SoftwareProject that has since been FOLDED into another (repo:henry->repo:shellbiz,
    # 2026-08-14) — grouping on the raw label renders the dead label's own group forever (11
    # sessions still did, at time of writing). Resolve each DISTINCT raw label ONCE (fleet()
    # can carry 500+ agent rows; a per-row call would be wasteful) through the same
    # fold-aware primitive settle.py/agents.py/project_identity_evidence already share.
    # Best-effort, same fail-open shape as os_bodies/ghost_gap beside it: a normalize failure
    # degrades to the raw label, never breaks fleet().
    try:
        from src.orchestrator.project_identity import _normalize_project_label_through_merge

        raw_labels = {n["project"] for n in nodes.values() if n["project"]}
        label_map: dict[str, str] = {}
        for raw in raw_labels:
            normalized, _confession = await _normalize_project_label_through_merge(pool, raw)
            if normalized != raw:
                label_map[raw] = normalized
        if label_map:
            for n in nodes.values():
                if n["project"] in label_map:
                    n["project"] = label_map[n["project"]]
    except Exception:  # noqa: BLE001
        pass
    # LAND ON COUNTS, WALK IN: the roster's history is 1000+ rows and never what you came for.
    # The flat rows are the LIVE ones (or everything, if you deliberately asked) — the counts
    # below are always over the whole fleet, so nothing here undercounts, it only under-SHOWS.
    shown = {c: n for c, n in nodes.items() if full or n["live"]}
    # THE GHOST GAP (heinrich's filing, thread 1fe6811c) — OS TRUTH beside the graph's belief,
    # ADDITIVE only: `live` above is UNCHANGED, still exactly what it always was (the wake
    # trigger reads agent_mounts.last_seen directly and never this dict — nothing here touches
    # that). `census.live_bodies()` is a pure OS read (pgrep -x claude + /proc), independent of
    # the mount registry; where the graph counts more live agents in a project than any real
    # process backs, that project is carrying a ghost (a closed tab mid-decay) or a phantom
    # mount (registered, never backed by an actual session) — invisible to any ping-window,
    # visible the instant this is asked. Best-effort: an OS read that fails never breaks fleet().
    try:
        os_bodies = {p: len(pids) for p, pids in census.live_bodies().items()}
    except Exception:  # noqa: BLE001
        os_bodies = {}
    # PER-IDENTITY, NOT NETTED (thread #174, rotten-apple's own specimen, 2026-08-18): a
    # per-project SUBTRACTION (live_count - body_count) reads as "no gap" whenever a false-LIVE
    # row and a false-DEAD body happen to cancel — rotten-apple showed "1 live · 3 bodies" as
    # clean while carrying both at once (a ghost mount with no real process, AND real processes
    # the graph never recognized as live — #174's own anchor-lookup gap was exactly why).
    # `live_bodies_by_cwd()` is cwd-grained (unlike `os_bodies` above, which stays
    # project-grained for its existing consumers/tree render); matching each LIVE node's own
    # `agent_mounts.cwd` against it catches both directions with no netting to cancel through.
    try:
        bodies_by_cwd = census.live_bodies_by_cwd() or {}
    except Exception:  # noqa: BLE001
        bodies_by_cwd = {}

    def _resolved(cwd: str | None) -> str | None:
        if not cwd:
            return None
        try:
            return str(Path(cwd).resolve())
        except OSError:
            return None

    live_cwds = {_resolved(n["cwd"]) for n in nodes.values() if n["live"] and n["cwd"]}
    live_cwds.discard(None)
    ghost_gap: dict[str, dict[str, list[Any]]] = {}
    for canonical, n in nodes.items():
        if not n["live"]:
            continue
        if _resolved(n["cwd"]) not in bodies_by_cwd:
            proj = n["project"] or "?"
            ghost_gap.setdefault(proj, {"false_live": [], "false_dead": []})
            ghost_gap[proj]["false_live"].append(canonical)
    for cwd, pids in bodies_by_cwd.items():
        if cwd in live_cwds:
            continue
        proj = None
        for n in nodes.values():
            if _resolved(n["cwd"]) == cwd:
                proj = n["project"]
                break
        proj = proj or "?"
        ghost_gap.setdefault(proj, {"false_live": [], "false_dead": []})
        ghost_gap[proj]["false_dead"].append({"cwd": cwd, "pids": pids})
    # THE REGISTRY FOLD (Thoth dispatch msg 5286, thread 5256): registry_census's own
    # harness-vs-mount-registry view, additive, reusing bodies_by_cwd/live_cwds/_resolved
    # already computed above for ghost_gap — no new OS read. Purely additive key; never
    # touches os_bodies/ghost_gap or the row-fetch SQL above it.
    try:
        from src.orchestrator.mounts import registry_census as _registry_census
        census_report = await _registry_census(pool)
    except Exception:  # noqa: BLE001 — same fail-open law as os_bodies/whisper_health
        census_report = {"blind": True, "verified": [], "matched": [], "rowless": []}
    bodies = []
    for b in (*census_report.get("matched", []), *census_report.get("rowless", [])):
        rc = _resolved(b.get("harness_cwd"))
        entry = {
            "harness_name": b.get("harness_name"), "job_dir_key": b.get("job_dir_key"),
            "harness_cwd": b.get("harness_cwd"),
            "row": "matched" if b.get("agent_id") else "rowless",
            "ghost_status": ("false_dead" if rc in bodies_by_cwd and rc not in live_cwds
                             else None),
        }
        if b.get("agent_id"):
            entry["agent_id"] = b["agent_id"]
            entry["project"] = b.get("project")
        bodies.append(entry)
    harness_registry = {
        "blind": census_report.get("blind", False),
        "verified_count": census_report.get("verified_count", len(bodies)),
        "matched_count": census_report.get("matched_count", 0),
        "rowless_count": census_report.get("rowless_count", 0),
        "bodies": bodies,
    }
    # THE LANDING AUDIT, READ-ONLY GLANCE (Thoth dispatch msg 5339): `osiris deploy` mints
    # the durable obligations (deploy_guard.landing_audit); this is just the at-a-glance
    # count so a coordinator sees it here too, without a second call or waiting for orient's
    # open-obligations list. Same fail-open law as os_bodies/harness_registry beside it.
    try:
        from src.orchestrator import capture as _capture
        from src.orchestrator.deploy_guard import (
            _REPO_ROOT as _DG_REPO_ROOT,
        )
        from src.orchestrator.deploy_guard import (
            audit_graph_merge_claims,
            stale_unmerged_branches,
        )
        _held = await _capture.open_held_work(pool)
        _claimed = {h["branch"] for h in _held if h.get("branch")}
        landing_audit: dict[str, Any] = {
            "stale_unmerged_branches": len(
                await stale_unmerged_branches(_DG_REPO_ROOT, claimed=_claimed)),
            "graph_claim_mismatches": len(
                await audit_graph_merge_claims(pool, _DG_REPO_ROOT)),
        }
    except Exception:  # noqa: BLE001
        landing_audit = {"stale_unmerged_branches": 0, "graph_claim_mismatches": 0,
                         "error": "landing audit glance unavailable"}
    from src.orchestrator.seats import fleet_occupancy
    seats = await fleet_occupancy(pool)
    # WHISPER HEALTH (task #179): recent whisper/session-end/precompact/stophook alarm
    # counts, read off the SAME blind-spot channel every other unverifiable-from-here gap
    # uses (task #34) — a session mounting via fleet() sees at a glance whether the door
    # it just walked through has been failing. Best-effort, same fail-open shape as
    # os_bodies: a probe failure here must never break fleet() itself.
    try:
        from src.orchestrator.smoke import whisper_health as _whisper_health
        whisper = await _whisper_health(pool)
    except Exception:  # noqa: BLE001
        whisper = {"ok": True, "error": "whisper_health probe unavailable"}
    # PER-DAEMON POOL SURFACE (task #180 piece 2 (c)): pg_stat_activity grouped by the
    # application_name each bounded daemon pool now tags itself with — same best-effort
    # shape as whisper_health/os_bodies beside it.
    try:
        from src.orchestrator.pool_health import pg_activity_by_app
        pool_health = await pg_activity_by_app(pool)
    except Exception:  # noqa: BLE001
        pool_health = {"by_application": {}, "backends": None, "tx_total": {}}
    # CROSS-CHANNEL ADOPTION (task #181, Thoth DM 5320): per-live-seat osiris-vs-harness
    # traffic share — Ptah measured 3 osiris sends against ~24 harness-socket (SendMessage)
    # sends during a routing defect, 90% of that day's reasoning invisible to this graph.
    # `harness_count: None` (never a false zero) whenever the seat's current session was
    # never soul-stored + recovered (`recover_harness_exchanges` is the write side; this
    # only reads what already landed) — "not recovered" and "recovered, zero harness
    # traffic" are different facts, never conflated. Batched (not per-node), same law as
    # the project-label normalization above it: fleet() can carry 500+ rows.
    try:
        anchor_by_canonical = {
            c: str(Path(n["job_dir"]).name) for c, n in nodes.items()
            if n["live"] and n["job_dir"]
        }
        osiris_counts: dict[str, int] = {}
        harness_counts: dict[str, int] = {}
        if anchor_by_canonical:
            rows_o = await pool.fetch(
                "SELECT from_agent, count(*) AS n FROM fleet_messages "
                "WHERE from_agent = ANY($1::text[]) "
                "AND created_at > now() - interval '24 hours' GROUP BY from_agent",
                list(anchor_by_canonical))
            osiris_counts = {r["from_agent"]: int(r["n"]) for r in rows_o}
            rows_h = await pool.fetch(
                "SELECT anchor_sid, count(*) AS n FROM harness_messages "
                "WHERE anchor_sid = ANY($1::text[]) "
                "AND (observed_at IS NULL OR observed_at > now() - interval '24 hours') "
                "GROUP BY anchor_sid", list(set(anchor_by_canonical.values())))
            harness_counts = {r["anchor_sid"]: int(r["n"]) for r in rows_h}
        for c, anchor in anchor_by_canonical.items():
            osiris_n = osiris_counts.get(c, 0)
            if anchor in harness_counts:
                harness_n = harness_counts[anchor]
                total = osiris_n + harness_n
                adopt_entry: dict[str, Any] = {
                    "osiris_count": osiris_n, "harness_count": harness_n, "recovered": True}
                if total:
                    adopt_entry["share"] = round(osiris_n / total, 3)
            else:
                adopt_entry = {"osiris_count": osiris_n, "harness_count": None,
                               "recovered": False}
            nodes[c]["adoption"] = adopt_entry
    except Exception:  # noqa: BLE001 — best-effort, same fail-open law as every probe here
        pass
    return {
        "connected_now": len(_agents),
        "count": len(nodes),
        **({"ghosts": ghosts} if ghosts else {}),
        "live": sum(1 for n in nodes.values() if n["live"]),
        "swarm": sum(1 for n in nodes.values() if n["parent"]),
        "os_bodies": os_bodies,
        **({"ghost_gap": ghost_gap} if ghost_gap else {}),
        "whisper_health": whisper,
        "harness_registry": harness_registry,
        "landing_audit": landing_audit,
        "pool_health": pool_health,
        # OCCUPANCY (9f566244 piece B): every active Seat, VACANT ones included — the
        # agent tree above is rooted at Agent objects, so a seat with no holder AT ALL
        # (Ptah's shape: an office scaffolded, never sat in) never appears in it at all.
        "seats": [{"seat": s["seat_id"], "handle": s["handle"], "house": s["house"],
                   "state": s["state"], "holder": s["holder"]} for s in seats],
        "tree": render_fleet_tree(nodes, full=full, os_bodies=os_bodies, ghost_gap=ghost_gap),
        "registered": [
            {"agent": c, "model": n["model"], "project": n["project"], "depth": n["depth"],
             "parent": n["parent"], "live": n["live"],
             "last_seen": n["ts"].isoformat() if n["ts"] else None,
             **({"seat": n["seat"]} if n["seat"] else {}),
             **({"adoption": n["adoption"]} if n.get("adoption") else {})}
            for c, n in shown.items()
        ],
        **({} if full else {"registered_scope": f"live only — {len(nodes)} total, "
                            f"fleet(full=True) for the rest"}),
    }


@mcp.tool()
async def registry_census() -> dict[str, Any]:
    """THE REGISTRY+/PROC CENSUS (#178 piece c) — the harness's own live-body list
    (`claude agents --json`), each row verified against `/proc` (the pid really is a
    claude body), reconciled against `agent_mounts`. `matched` are bodies with a real row;
    `rowless` are verified-live bodies with NO row at all — the population #178's pieces
    (a)/(b) exist to close. `blind: true` means the harness read itself failed (cannot
    census, never read as "nothing is live").

    OCCUPANCY, NOT IDENTITY: this answers "is a body running", never "which agent lineage
    holds a seat" — read the graph (roster()/doors()) for that. Conflating the two is
    exactly the two-body-problem class of bug (ruling 719ed5b1)."""
    from src.orchestrator.mounts import registry_census as _registry_census
    return await _registry_census(await _pool_get())


@mcp.tool()
async def roster(repo: str | None = None) -> dict[str, Any]:
    """Which seat owns a repo, and is anybody home — from the GRAPH, never `ls` on disk.

    `repo=None` returns every active seat: `occupancy` (vacant/occupied/cold — held but
    nobody live THIS INSTANT), `chartered_repos` (governs links), `pin` (a live read of
    the seat's .osiris, declared/unset/unreadable), and `anchor_cwd`/`tree_cwd`/
    `live_cwd` kept separate — a live holder's mount cwd can differ from both with
    nothing wrong. `pin.triage_bucket` reuses `triage`'s own bucket, or
    "no-such-project" when the pin names something unreal.

    `repo=<name>` answers "who owns this": a seat matches if its charter OR pin names
    the repo. Two matches is `governed` when the charter-seat manages the pin-seat
    (normal) else `conflict`, never silently picked. Zero matches is `no-match` (not a
    claim of no owner), paired with `near_misses`.

    Neither `chartered_repos` nor `pin` is certified canonical. `caveats` in every
    response names this function's own blind spots. consult_canon('roster') for more."""
    pool = await _pool_get()
    from src.orchestrator.seats import roster as _roster
    return await _roster(pool, repo=repo)


@mcp.tool()
async def tree_ledger(limit: int | None = None, offset: int = 0) -> dict[str, Any]:
    """THE PIN-VS-GRAPH DISAGREEMENT REPORT. Read-only, fleet-wide, two sections.

    `project_ledger` — every active SoftwareProject, each carrying `phantom_verdict`:
    test-fixture | declared (a seat pin or Seat-origin governs edge claims it) |
    phantom-suspect (name matches a generic path-segment list, nothing declares it) |
    undetermined (a real disagreement, no confident call). `limit`/`offset` page it
    (default 200/0, capped 2000).

    `live_cwd_ledger` — today's agent_mounts only. Each cwd: `directory_exists`
    (checked before the pin is trusted), `resolved_today` vs `graph_believes`, and an
    `agreement` verdict: no-graph-yet / ghost / graph-only / match / partial-match /
    mismatch.

    `caveats` names what this instrument cannot see. READ-ONLY: reports disagreements,
    never repairs, folds, or merges. consult_canon('tree_ledger') for more."""
    pool = await _pool_get()
    from src.orchestrator.seats import tree_ledger as _tree_ledger
    return await _tree_ledger(pool, limit=limit if limit is not None else 200, offset=offset)


@mcp.tool()
async def send(body: str, to: str | None = None, to_agent: str | None = None,
               reply_to: int | None = None, desk: str | None = None,
               grade: str | None = None, require_seat: bool = False,
               threads: list[str] | None = None,
               subagent_id: str | None = None, subagent_type: str | None = None,
               session_anchor: str | None = None,
               want_prior_art: bool = False, want_listener: bool = False,
               ctx: Context | None = None) -> dict[str, Any]:
    """Message the fleet. `to`=<project> is a BROADCAST, the group chat ('operator' reaches
    the human's desk); `to_agent`=<agent:id> is a private DM (ids from orient()/fleet). `to`
    refuses a project nobody has mounted under rather than filing mail nobody will read.
    `reply_to=<id>` answers a message (routes by channel, joins the thread) and settles it.
    At-least-once, deduped. For durable knowledge use record_decision/open_thread instead.

    `desk` triages an operator brief: 'decision' | 'hands' | 'fyi'. `grade` triages
    project mail: 'ask' (named in the recipient's unread count) | 'fyi' (an ack settles
    it) — ungraded is never guessed. `dispatch` in the receipt names what happened on
    delivery: queued/poked/resumed/woke, or a brake mode naming why nobody was reached.
    A DM's receipt echoes `dm_to`/`seat`/`lineage_head` — compare against a stale
    address before trusting "sent"; `require_seat=True` refuses on an unclaimed target.
    `threads` transfers ownership of existing Thread(s) to a DM's addressee in the same
    act (exact ref only, never inferred from `body`) — `threads_stamped` names what
    moved. A DM or graded 'ask' runs the same prior-art search record_decision does,
    surfaced on both your receipt and the delivered message.

    `want_prior_art`/`want_listener` return the full prior_art list and listener block;
    default is a one-line `prior_art_flag` only."""
    ident = await _ident_for(ctx, session_anchor)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first — a message must say who "
                         "it's from (the anchor re-attaches you automatically after a bounce)",
                "why": _anchorless(ctx)}
    pool = await _pool_get()
    st = get_settings()
    # a SPAWN's mail goes out under its OWN name (the hook-stamped sidechain identity),
    # from the parent's project — the fleet must never mistake a child's word for the seat's
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    # THE READ-SIDE PRIOR-ART HOP (obligation a6198075) runs BEFORE send_message, not
    # after (6a1dd99 fallout, Thoth DM 5442 leg 1a): since 6a1dd99 graphs every sent
    # message as its own searchable Message object, searching AFTER the write let this
    # call's own just-written body — a verbatim, single-field, perfect self-match —
    # satisfy search()'s strict-AND lexical door trivially, which short-circuits the
    # OR-relaxation ladder that is the actual mechanism finding a DIFFERENTLY-worded
    # standing decision (record_decision's own prior-art call structurally avoids this
    # because its query spans summary+rationale while the graph stores them as separate
    # single-field assertions — no candidate row ever contains the literal union, so no
    # accidental self-match). A message can never be its own prior art by definition —
    # searching the graph as it stood BEFORE this write is both the fix and the more
    # honest semantics.
    prior: list[dict[str, Any]] = []
    if grade == "ask" or to_agent:
        prior = await _surface_prior_art(pool, body, repo=ident.project, actor=actor)
    try:
        res = await send_message(pool, from_agent=actor, from_project=ident.project,
                                 to_project=to, to_agent=to_agent, body=body, reply_to=reply_to,
                                 desk_kind=desk, grade=grade, require_seat=require_seat,
                                 threads=threads)
    except ValueError as e:
        return {"error": str(e)}
    out: dict[str, Any] = {
        "sent": res["id"], "from": actor,
        **({"thread": res["thread_id"]} if res["thread_id"] is not None else {}),
        **({"dedup": "identical recent message already queued — not re-posted"}
           if res["dedup"] else {}),
        **({"threads_stamped": res["threads_stamped"]} if res.get("threads_stamped") else {}),
        # THE HONEST RECEIPT (Thoth DM 5493): the relational row always lands; the graph
        # edge write (Message object + sent_by/addressed_to/broadcast_to/replies_to) is
        # best-effort beside it and CAN fail on its own — `graphed: False` says so plainly
        # rather than let mail look graph-traversable when this one didn't make it.
        **({"graphed": False, "note": "relational send succeeded; the graph edge write "
                                      "failed — this message won't show up in search()/"
                                      "prior-art/orient() until a later repair recovers it"}
           if res.get("graphed") is False else {}),
    }
    if res["to_agent"]:  # a DM — report the addressee, its seat + lineage head, and its liveness
        out["dm_to"] = res["to_agent"]
        out["seat"] = res.get("seat")
        out["lineage_head"] = res.get("lineage_head")
        # THE RECEIPT INVARIANT (ruling 7d6815bb): `listener` reads the DELIVERING HEAD's
        # liveness — agent_liveness(lineage_head or dm_to) is lineage-aware internally, but
        # passing lineage_head explicitly when it resolved keeps this receipt's every field
        # sourced from the SAME identity `seat` already is, never a mix of the addressed id
        # and the head. `redirect` (mailbox.send_message's own new field), when present,
        # names the divergence explicitly instead of leaving it to be inferred by comparing
        # `dm_to` against `seat`/`lineage_head` by hand.
        if want_listener:
            out["listener"] = await mounts.agent_liveness(
                pool, res.get("lineage_head") or res["to_agent"])
        if res.get("redirect"):
            out["redirect"] = res["redirect"]
        # THE IMMEDIATE LEG (the background-session adapter, ruling 6c4d0b62): a DM's wake
        # fires ON ARRIVAL, never on a clock — this very call dispatches it, and the receipt
        # below is the PER-HOP truth (resumed / delivered / queued-* / pull-only), not a
        # guess about what some future sweep might do. The worker tick stays as the backstop
        # that drains gated mail. A dispatch failure must never fail the send: the message
        # is already committed, the sweep will retry, and the receipt says so honestly.
        if not res["dedup"]:
            try:
                from src.orchestrator.trigger import dispatch_dm
                out["dispatch"] = await dispatch_dm(
                    pool, addressee=res["to_agent"], msg_id=res["id"], sender=actor)
            except Exception as exc:  # noqa: BLE001 — the send already committed; confess
                out["dispatch"] = {"mode": "deferred",
                                   "detail": f"immediate dispatch failed ({exc}) — the "
                                             "worker sweep is the backstop"}
        if await pool.fetchval(
                "SELECT 1 FROM current_assertions a JOIN objects o ON o.id=a.object_id "
                "WHERE o.canonical=$1 AND a.name='is_sidechain' "
                "AND a.value #>> '{}' = 'true' LIMIT 1", res["to_agent"]):
            # the dead-letter class: an ephemeral spawn has no session to resume and no
            # chrome to nag — a DM to it may never be read or settled
            out["warning"] = ("the addressee is an ephemeral SPAWN — it cannot be woken and "
                              "may never read this; if the work is for its lineage, DM the "
                              "parent seat instead (see the spawn's spawned_by link)")
    else:  # a broadcast — the project channel: who's live, is anyone actually being woken
        dest = res["to"]
        last_seen = await mounts.project_last_seen(pool, dest)
        out["to"] = dest
        if want_listener:
            out["listener"] = {"live": bool(last_seen and datetime.now(UTC)
                               - datetime.fromisoformat(last_seen) < timedelta(minutes=15)),
                               "last_seen": last_seen}
        # THE IMMEDIATE LEG, extended from the DM lane to broadcasts (task #151, ruling
        # 60bc15db in the mail layer): a broadcast used to file and return a bare "sent" —
        # a caller reasonably read that as delivered when it meant filed, and the only push
        # was the worker sweep, up to ~60s later, NONE at all under poke-only with no open
        # window. dispatch_broadcast fires ON ARRIVAL now, same as a DM; the worker tick
        # stays the backstop. A dispatch failure must never fail the send: the message is
        # already committed, the sweep retries, and the receipt says so honestly.
        if not res["dedup"]:
            try:
                from src.orchestrator.trigger import dispatch_broadcast
                out["dispatch"] = await dispatch_broadcast(
                    pool, project=dest, msg_id=res["id"], sender=actor)
            except Exception as exc:  # noqa: BLE001 — the send already committed; confess
                out["dispatch"] = {"mode": "deferred",
                                   "detail": f"immediate dispatch failed ({exc}) — the "
                                             "worker sweep is the backstop"}
        out["backlog"] = await mailbox.project_deliverable_count(
            pool, dest, lease_secs=st.osiris_mail_lease_secs)
    # THE CROSSED-MAIL WARNING (Anubis VIII's #1 grievance, msg 236: four in-flight
    # crossings in one day, each costing a stale answer + a reconciliation cycle): if this
    # thread's peer already has words waiting UNREAD in your own inbox, your note may have
    # crossed theirs — say so at send time, BEFORE the stale answer is composed. Pull
    # semantics untouched; this is a mirror, not a push.
    if res["thread_id"] is not None:
        crossed = await pool.fetchval(
            "SELECT count(*) FROM fleet_messages m "
            "LEFT JOIN message_recipients r ON r.message_id = m.id AND r.agent_id = $3 "
            "WHERE m.thread_id = $1 AND m.id <> $2 AND m.from_agent <> $3 "
            "AND (m.to_agent = $3 OR (m.to_project = $4 AND m.to_agent IS NULL)) "
            "AND m.read_at IS NULL AND r.read_at IS NULL",
            res["thread_id"], res["id"], actor, ident.project)
        if crossed:
            out["crossed"] = (f"{crossed} unread message(s) in THIS thread are already "
                              "waiting in your inbox — your note may have crossed theirs; "
                              "inbox() before assuming your view is current")
    # Attach/persist the prior-art computed ABOVE, before the write — skipped on a dedup
    # hit (res["id"] then names an EXISTING message that may already carry its own
    # prior_art from its original send; overwriting risks clobbering a real prior result
    # with this resend's own, possibly-empty, recomputation — moot anyway since we never
    # searched for a dedup'd resend in the first place... but the gate stays explicit).
    if prior and not res["dedup"]:
        if want_prior_art:
            out["prior_art"] = prior
        top = prior[0]
        out["prior_art_flag"] = (
            f"{top.get('type') or 'Decision'} {top['id']} already speaks to this — "
            "worth reading before dispatching/answering as if it's new"
            + ("" if want_prior_art else " (pass want_prior_art=True for the full list)"))
        try:
            await pool.execute(
                "UPDATE fleet_messages SET prior_art=$1 WHERE id=$2", prior, res["id"])
        except Exception:  # noqa: BLE001 — persistence for the READER's copy is a
                            # bonus; the send already committed and the sender's own
                            # receipt above already carries the hits regardless
            pass
    # THE UNHEDGED-ASSERTION NAG (thread 02e0ab9c, Thoth XC's own three specimens as the
    # acceptance test — msg 6189): measurement_smell's own sibling, mirroring its exact
    # shape (advice on the receipt, never a gate — the message sends either way) but
    # aimed at dispatch prose instead of decision text. The reader is the SENDER, this
    # same turn, before anyone downstream ever sees the message — no new storage, no new
    # consumer, the same design that let this ship without the read-lens work.
    if capture.unhedged_assertion_smell(body):
        # RECEIPT DIET (msg 6871): short code, not the full prose every firing —
        # describe('nags:assertion') for the text (catalog: _NAG_CATALOG below).
        out.setdefault("nags", []).append("assertion")
    return out


@mcp.tool()
async def wake_preflight(target: str) -> dict[str, Any]:
    """Answer wake()'s own gates BEFORE you attempt one (#156.4) — the compaction/ceiling/
    no-anchor/crossed-registry checks that today only reveal themselves as a refusal AFTER
    a real wake() call. `target` accepts anything wake()'s own does — a claimed handle,
    `seat:<id>`, or `agent:<id>`.

    Returns `{mode, status, detail}`. `status` is one of: `resumable` (every gate clears —
    a real wake() would resume this addressee now), `fresh-heir-available` (past its own
    compaction seam, but not a dead end — a real wake() boots a fresh successor here
    rather than refusing, ruling 94c2e7e8), `no-live-body` (vacant, retired, or never
    mounted), or `refused-<gate>` (ceiling / no-anchor / crossed-registry / resident-
    unknown / unknown — never the same finding, f624d114). Read-only: sends/spawns
    nothing."""
    pool = await _pool_get()
    from src.orchestrator.trigger import (
        _resolve_wake_address,
        _seat_for_target,
        wake_gate_preflight,
    )

    # A BARE HANDLE MUST RESOLVE, THE SAME WAY wake() ITSELF DOES (live-fire finding,
    # 2026-08-08: this tool's own first real run against 'metron' silently answered
    # 'never-mounted' — _resolve_wake_address only ever understood 'seat:'/'agent:'
    # prefixes, exactly like dispatch_dm's own addressee, which always arrives PRE-
    # RESOLVED via wake_worker's _seat_for_target call before dispatch_dm ever sees it.
    # This tool has no such upstream resolver of its own, so it must run the SAME one
    # wake_worker does — never a second, narrower guess at what a handle means).
    seat = await _seat_for_target(Actions(pool), target)
    resolved = await _resolve_wake_address(pool, seat or target)
    if isinstance(resolved, dict):
        return {**resolved, "status": "no-live-body"}
    resolved_target, seat_id = resolved
    return await wake_gate_preflight(pool, resolved_target, seat_id=seat_id)


@mcp.tool()
async def wake(target: str, message: str, subagent_id: str | None = None,
               subagent_type: str | None = None, session_anchor: str | None = None,
               ctx: Context | None = None) -> dict[str, Any]:
    """Knock on the other half of your own managed_by pair — never a peer. Gated on an
    active managed_by edge in EITHER direction (you manage them, or they manage you);
    peers and cross-house calls refuse, routed through a manager or the operator instead.
    No operator override parameter, deliberately — stays out-of-band.

    `target` accepts anything send()'s to_agent does. Message is prefixed with a self-
    identifying marker, then dispatches through send()'s own DM path with an authority
    gate in front. `status`: delivered (confirmed landed as a submitted turn, observed:
    true) | mid-turn (their turn is still moving) | no-live-body | refused-not-your-worker
    | refused-budget | queued (rate brake, pause, or unconfirmed — see `detail`)."""
    ident = await _ident_for(ctx, session_anchor)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first — a wake must say who "
                         "it's from", "why": _anchorless(ctx)}
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    from src.orchestrator.trigger import wake_worker
    return await wake_worker(Actions(await _pool_get()), caller=actor, target=target,
                             message=message)


@mcp.tool()
async def launch(target: str, message: str = "", model: str | None = None,
                 subagent_id: str | None = None, subagent_type: str | None = None,
                 session_anchor: str | None = None,
                 ctx: Context | None = None) -> dict[str, Any]:
    """Give a seat a fresh BODY (wake() is the speak-verb for a body that already exists).
    Downward-only — you may only body a seat you MANAGE. Creates a new session, never
    injects into an existing one. Default substrate is a harness-native `claude --bg`
    background session (self-binds via its own first turn: mount() then claim_name);
    the old PTY-broker lane survives only as an explicit fallback
    (`osiris_launch_substrate`). No operator override param, deliberately: the
    operator's real hand stays out-of-band.

    Idempotent — a live body already holding the seat is returned, never twinned.
    `message` delivers as the opening brief, only on the `launched` path (dropped on
    `already-live` — use wake() instead).

    `body_exists` (window created) and `can_receive` (independently confirmed live) are
    separate — a fresh spawn usually returns body_exists=true, can_receive=false for a
    few seconds; `detail` says how to confirm. `status`: launched | already-live |
    manager-cold | refused-not-your-worker | refused-no-office/-no-handle |
    refused-spawn (see `detail`). `dormant_history`, when present, discloses a
    substantial pre-existing transcript at the target cwd."""
    ident = await _ident_for(ctx, session_anchor)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first — a launch must say who "
                         "it's from", "why": _anchorless(ctx)}
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    from src.orchestrator.trigger import launch_seat
    return await launch_seat(Actions(await _pool_get()), caller=actor, target=target,
                             message=message, model=model)


@mcp.tool()
async def resume(target: str, message: str = "", model: str | None = None,
                 subagent_id: str | None = None, subagent_type: str | None = None,
                 session_anchor: str | None = None,
                 ctx: Context | None = None) -> dict[str, Any]:
    """Continue a seat's own DORMANT session — distinct from launch() (always mints fresh,
    never guesses); same managed_by/downward-only gate. NEVER falls through to a fresh
    mint: if nothing resumable exists, refuses (`status: refused-nothing-to-resume`)
    rather than minting a stranger — call launch() for that instead. One-shot: runs one
    turn over `-p --resume` and exits, re-summonable via the next mail wake.

    `status`: launched (mode: resumed) | refused-nothing-to-resume | refused-resume-
    unknown (a resumable-looking session with no signed testimony — the exact
    `claude -p --resume <sid>` a human can run by hand is in `detail`) |
    refused-not-your-worker. `resume_check` on every receipt names the decision (which
    generation, how many hops back, the gate numbers)."""
    ident = await _ident_for(ctx, session_anchor)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first — a resume must say who "
                         "it's from", "why": _anchorless(ctx)}
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    from src.orchestrator.trigger import resume_seat
    return await resume_seat(Actions(await _pool_get()), caller=actor, target=target,
                             message=message, model=model)


@mcp.tool()
async def stop(target: str | None = None, reason: str = "",
               subagent_id: str | None = None, subagent_type: str | None = None,
               session_anchor: str | None = None,
               ctx: Context | None = None) -> dict[str, Any]:
    """launch()'s process-lifecycle inverse — stops a LIVE body's OS process (harness
    `claude stop <id>` when tracked, else SIGTERM). Not a symmetric "sleep" — an honest
    termination, auditable, not a pause. No 'unstop' call needed: once the process exits,
    a fresh launch()/wake() proceeds normally.

    `target=None` stops yourself, always allowed. Otherwise downward-only, mirroring
    launch(). `status`: stopped | no-live-body | refused-not-your-worker |
    refused-signal."""
    ident = await _ident_for(ctx, session_anchor)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first — a stop must say who "
                         "it's from", "why": _anchorless(ctx)}
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    from src.orchestrator.trigger import stop_seat
    return await stop_seat(Actions(await _pool_get()), caller=actor, target=target,
                           reason=reason)


@mcp.tool()
async def inbox(project: str | None = None, peek: bool = False,
                ack: list[int] | None = None, subagent_id: str | None = None,
                subagent_type: str | None = None, session_anchor: str | None = None,
                want_prior_art: bool = False,
                ctx: Context | None = None) -> dict[str, Any]:
    """Read messages other agents left for you. Defaults to your mounted project; pass
    `project` for another's ('operator' reads the human's desk). Reading LEASES a
    message, doesn't consume it — settle each one via send(reply_to=<id>) or ack=[ids],
    or it redelivers after the lease (at-least-once). `peek=True` reads without leasing.
    Check after mount()/compaction. THE OPERATOR'S DESK IS DIFFERENT: glance with peek
    only, settle only at the human's explicit word.

    `want_prior_art=True` returns each message's full prior_art list; default is a
    `prior_art_count` only."""
    ident = await _ident_for(ctx, session_anchor)
    proj = project or (ident.project if ident else None)
    if proj is None:
        # THIS is the bounce that hit Thoth XXVIII tonight — twice — and it carried no diagnostic
        # at all, which is precisely why four seats independently filed it as "transient" and
        # nobody chased it for a week.
        return {"error": "mount(cwd, job_dir=<your anchor>) first, or pass project=<repo>",
                "why": _anchorless(ctx)}
    pool = await _pool_get()
    st = get_settings()
    # a SPAWN reads over its parent's shoulder: PEEK only. It must never LEASE the seat's
    # mail (a lease a dying child holds blocks redelivery for the whole lease window) and
    # never SETTLE it (settling is the seat's duty — a child acking mail the parent never
    # saw re-creates the exact surprise this layer exists to kill).
    from src.orchestrator.lineage import normalize_spawn_id

    spawn_reader = normalize_spawn_id(subagent_id) is not None
    if spawn_reader:
        peek, ack = True, None
    # the reader is YOU (your DMs + your project's broadcasts, your own lease/settle) — EXCEPT
    # the operator desk, whose reader is the human ('operator'): an agent only peeks it, never
    # settles it as itself.
    reader = OPERATOR_ADDR if proj == OPERATOR_ADDR else (ident.agent_id if ident else proj)
    # an ack ALWAYS answers with what it settled and what it skipped-and-why (Alfred's
    # fixture, msg 666: a silent zero-settle was indistinguishable from success, so the
    # same four DMs were acked three times and redelivered anyway)
    ack_out = await ack_messages(pool, proj, ack, reader_agent=reader) if ack else None
    ack_keys: dict[str, Any] = {}
    if ack_out is not None:
        ack_keys["settled"] = ack_out["settled"]
        if ack_out["skipped"]:
            ack_keys["skipped"] = ack_out["skipped"]
    if proj == OPERATOR_ADDR:
        # THE ORGANIZED DESK (operator direction 2026-07-11): always peek-shaped — reading
        # the human's desk never leases; bands (needs_decision / needs_hands / fyi) ·
        # thread + same-story folds · dimmed moot annotations · the derived your_queue.
        desk = await read_desk(pool)
        return {"project": OPERATOR_ADDR, **desk, **ack_keys}
    msgs = await read_inbox(pool, proj, reader_agent=reader, mark_read=not peek,
                            lease_secs=st.osiris_mail_lease_secs)
    if not want_prior_art:
        for m in msgs:
            pa = m.pop("prior_art", None)
            if pa:
                m["prior_art_count"] = len(pa)
    flight = await in_flight(pool, proj, reader_agent=reader,
                             lease_secs=st.osiris_mail_lease_secs)
    if not peek:  # what THIS call just leased is ours, not someone else's in-flight
        ours = {m["id"] for m in msgs}
        flight = [f for f in flight if f["id"] not in ours]
    if spawn_reader:
        note = ("spawn read — peek FORCED, nothing leased or settled: the mailbox belongs "
                "to your parent's seat; report what you saw, let the seat settle it")
    elif peek:
        note = "peek — nothing leased"
    elif msgs:
        note = ("leased — settle each by replying (send(reply_to=<id>)) or acking "
                f"(inbox(ack=[ids])); unsettled mail redelivers after "
                f"{st.osiris_mail_lease_secs // 60} min")
    else:
        note = "empty"
    if flight:  # msg-78 lesson: an empty box with a held lease is NOT 'nothing happening'
        note += (f" — {len(flight)} in flight (leased by "
                 + ", ".join(sorted({f['leased_by'] for f in flight})) + ")")
    return {"project": proj.removeprefix("repo:").strip(), "messages": msgs,
            **({"in_flight": flight} if flight else {}),
            **ack_keys, "note": note}


@mcp.tool()
async def dismiss_brief(message_id: int, because: str,
                        ctx: Context | None = None) -> dict[str, Any]:
    """MOOT an operator-desk brief — annotate it moot-with-a-reason ('true when sent; root
    cause fixed in <commit>') so the desk renders it collapsed under your note instead of
    shouting a dead alarm. NEVER a settle: dismissing stays exclusively the human's word
    (the membrane); a moot is you saving them the archaeology, stamped with your name.
    Only works on briefs addressed to the operator's desk. Requires mount."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"why": _anchorless(ctx),
                "error": "mount(cwd, job_dir=<your anchor>) first — an annotation must say "
                         "whose testimony it is"}
    try:
        return await mailbox_dim(await _pool_get(), message_id,
                                 because=because, by=ident.agent_id)
    except ValueError as e:
        return {"error": str(e)}


@mcp.tool()
async def claim_name(name: str, ctx: Context | None = None) -> dict[str, Any]:
    """Name yourself. You mount as an anonymous hash; when you know who you are (your role, your
    work), claim a MEANINGFUL human name — you pick it, Osiris just enforces uniqueness. A name
    belongs to ONE lineage forever (a successor of yours inherits it as 'Name II'; a stranger
    can't take it), so the fleet can address you by name: another agent DMs you with
    send(to_agent='<your name>'). Refused only if the name is already held by a different
    lineage — pick another. Global namespace; choose something distinctive."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first — a name attaches to YOU",
                "why": _anchorless(ctx)}
    from src.orchestrator.agents import claim_name as _claim
    return await _claim(Actions(await _pool_get()), ident.agent_id, name, source=ident.agent_id)


@mcp.tool()
async def charter(repos: list[str] | None = None, ctx: Context | None = None) -> dict[str, Any]:
    """THE CHARTER (Phase 1 §4.1, ruling `dd47c1da`): a house is what a seat RULES, not where
    it sits. With `repos`, DECLARE your seat's whole charter — the repo labels you govern from
    this moment (self-declared, your own act); a repo you named before but drop now is healed
    off (compensating event, never deleted), never mints twice. Without `repos`, just READ your
    current charter back. Most seats have none — works_in already names their one home; a
    charter is for a seat that rules several repos regardless of which one it happens to sit
    in right now."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a charter names WHOSE seat rules which repos",
                "why": _anchorless(ctx)}
    from src.orchestrator.charter import charter_of, set_charter
    from src.orchestrator.seats import held_seat
    pool = await _pool_get()
    # RE-KEYED ONTO THE SEAT (ruling 1db1ff41) — see orient()'s own charter line for the full
    # rationale. A charter belongs to the SEAT, never the session: resolve it the same
    # lineage-aware way (held_seat) before either reading or declaring. An identity that holds
    # no seat yet (never attached/claimed) is refused here, not silently keyed on the Agent —
    # that's the exact bug this ruling closes.
    bound = await held_seat(pool, ident.agent_id)
    if bound is None:
        return {"agent": ident.agent_id,
                "error": "not yet seated — a charter belongs to a SEAT, and this identity "
                         "holds none yet. attach at spawn (or claim_name, if this is a "
                         "fresh mint) binds you to one first."}
    seat_id = str(bound["seat_id"])
    if repos is not None:
        return await set_charter(Actions(pool), seat_id, repos, actor=ident.agent_id)
    return {"agent": ident.agent_id, "seat": seat_id, "charter": await charter_of(pool, seat_id)}


@mcp.tool()
async def charter_for(seat_id: str, repos: list[str], because: str,
                      ctx: Context | None = None) -> dict[str, Any]:
    """Declare a charter ON BEHALF OF `seat_id` — the manager-invoked sibling of
    `charter()`, never a widening of it: `charter()` stays self-declaration only. For a
    seat that cannot yet speak for itself: a seat may declare its own charter, its
    manager may declare for it, and the operator is every seat's ultimate manager, so
    no seat is ever authority-less.

    ENFORCED, not just documented: the caller must be `seat_id`'s manager (the live
    `managed_by` edge) or an operator actor — refuses loudly otherwise, naming both who
    the caller resolved to and who the seat's actual manager is. `because` is required.
    Blind to any pre-existing Agent-origin `governs` edges the target seat may still
    carry from before charter's Seat-keyed re-key — see charter.py's own docstring."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a charter declared for another seat is a mind's "
                         "act, and the graph must know whose", "why": _anchorless(ctx)}
    from src.orchestrator.charter import charter_for as _charter_for
    return await _charter_for(Actions(await _pool_get()), seat_id, repos, because=because,
                              actor=ident.agent_id)


@mcp.tool()
async def rebind_seat(seat: str, new_cwd: str, extract: bool = False,
                      force: bool = False, because: str = "",
                      ctx: Context | None = None) -> dict[str, Any]:
    """Move a seat's anchor cwd, preserving identity, lineage, attribution, and mail.
    `seat` accepts a claimed name, a raw agent id, or an unclaimed seat's own handle.
    Writes/refreshes `.osiris` at `new_cwd` (project label unchanged), re-points the
    whole lineage's mount rows, stamps the move, carries harness metadata so resume
    survives it. Mints nothing new; refuses on a name matching neither agent nor seat.

    `extract=True`: the seat leaves a SHARED cwd taking only its own lineage's
    transcripts — co-resident history stays, the old path remains live. Live cached
    identities are patched in place so a same-session orient() sees the new anchor
    without a fresh mount().

    LIVENESS GUARD: self stays open; rebinding a different lineage's live target
    refuses by default — `force=True` + `because` overrides."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a rebind is a mind's act, and the graph must know whose",
                "why": _anchorless(ctx)}
    from src.orchestrator.mounts import rebind_seat as _rebind
    result = await _rebind(Actions(await _pool_get()), seat_or_agent=seat, new_cwd=new_cwd,
                           actor=ident.agent_id, extract=extract,
                           force=force, because=because or None)
    moved = result.get("agent")
    if moved and not result.get("error"):
        base = _generation(moved)[0]
        for cached in _agents.values():
            if _generation(cached.agent_id)[0] == base:
                cached.cwd = new_cwd
    return result


@mcp.tool()
async def merge(dupe: str, into: str, evidence: str, force: bool = False,
                because: str = "", ctx: Context | None = None) -> dict[str, Any]:
    """THE RECONCILIATION FOLD — declare two labels of the SAME type one thing: `dupe`
    folds into `into`. Type is read off `dupe`'s own form (agent:.../seat:.../else
    SoftwareProject). Append-only (nothing deleted, authorship untouched), and each
    type's own ESTATE follows: Agent moves mail/mount rows/open threads to `into`'s
    living head; Seat also moves active holders and managed_by edges (an Agent merge
    REFUSES an actively-seated dupe instead); SoftwareProject re-points every in_repo/
    works_in/governs/informs edge and mount row.

    `evidence` required for every type. AGENT MERGES ARE ACTOR-GATED: refuses any caller
    who is not the operator or the scheduled reaper — mount as the operator, or judge via
    resolve_fold(). Seat/Project merges carry no such gate. Refuses: dupe/into different
    types; thin evidence; dupe==into; unknown/already-folded labels; a same-lineage Agent
    pair (succession's job); a SoftwareProject pair contradicting on a non-name property.

    LIVENESS GUARD (SoftwareProject only): a different lineage's live session on `dupe`
    refuses by default — `force=True` + `because` overrides."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a fold is a mind's act, and the graph must know whose",
                "why": _anchorless(ctx)}
    from src.orchestrator.merge import _merge_type
    from src.orchestrator.merge import merge as _merge
    pool = await _pool_get()
    out = await _merge(Actions(pool), dupe=dupe, into=into, evidence=evidence,
                       actor=ident.agent_id, force=force, because=because or None)
    if "error" in out or _merge_type((dupe or "").strip()) != "SoftwareProject":
        return out
    witness = await pool.fetchrow(
        "SELECT oe.id AS merge_event_id, l.id AS same_as_link_id "
        "FROM objects d JOIN objects i ON i.canonical=$2 "
        "JOIN object_events oe ON oe.event_type='merge' AND oe.related_id=d.id "
        "  AND oe.object_id=i.id "
        "LEFT JOIN links l ON l.type='same_as' AND l.from_id=d.id AND l.to_id=i.id "
        "WHERE d.canonical=$1 ORDER BY oe.created_at DESC LIMIT 1",
        out["folded"], out["into"])
    if witness:
        out["merge_event_id"] = witness["merge_event_id"]
        out["same_as_link_id"] = witness["same_as_link_id"]
    return out


@mcp.tool()
async def unmerge(dupe: str, because: str, execute: bool = False,
                  ctx: Context | None = None) -> dict[str, Any]:
    """Reverse a wrongful `merge` call — replaces unfold_agent as the one door for all
    three types, closing the parity gap the operator named (31c02dca): before this, only
    an Agent merge was ever reversible; a Seat or Project merge was permanent (task #127).
    Type is read off `dupe`'s own form, same rule as `merge`. DRY RUN IS THE DEFAULT
    (`execute=False`) for every type: returns the exact plan (the kernel unmerge, any
    type-specific estate items that CAN cleanly return, and the ones that CAN'T) without
    writing anything — review it, then call again with `execute=True`. Refuses: `dupe` not
    currently merged, a blank `because`, or a merge whose original justification cites the
    operator's word when `because` doesn't carry a fresh one — reversing an
    operator-blessed merge needs the operator's word too, for every type."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — an unfold is a mind's act, and the graph must "
                         "know whose", "why": _anchorless(ctx)}
    from src.orchestrator.merge import unmerge as _unmerge
    return await _unmerge(Actions(await _pool_get()), dupe=dupe, because=because,
                          actor=ident.agent_id, execute=execute)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def reconcile_merge(dupe: str, into: str, ctx: Context | None = None) -> dict[str, Any]:
    """Accepts an ALREADY-MERGED `dupe` and re-points whatever mail/mount/thread/holder/
    managed_by/edge estate is still aimed at it, WITHOUT re-performing the merge —
    idempotent-by-REPAIR, for the estate a partial first fold left stranded. UNMERGE-
    THEN-REMERGE IS NOT A SUBSTITUTE: `unmerge`'s own `estate_unreturnable` path reports
    — and drops — exactly the links a partial fold already broke.

    Type is read off `dupe`'s own form, same rule as `merge`/`unmerge`. Refuses: `dupe`
    and `into` resolving to different types; `dupe` not merged (that's `merge`'s job);
    `dupe`'s own `merged_into` pointing at a DIFFERENT `into` (never redirects); `into`
    not active. THE AGENT BRANCH IS ACTOR-GATED exactly like `merge`'s own Agent branch
    (repairing a merge needs the same authority as making one); Seat and Project stay
    open, matching their own fold's current posture — unreconciled on purpose."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a reconcile is a mind's act, and the graph must "
                         "know whose", "why": _anchorless(ctx)}
    from src.orchestrator.merge import reconcile_merge as _reconcile_merge
    return await _reconcile_merge(Actions(await _pool_get()), dupe=dupe, into=into,
                                  actor=ident.agent_id)


@mcp.tool()
async def restore_attribution(
    project: str, dry_run: bool = True, because: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Repair verb for a fixed write-time bug: every fold performed before the fix
    stamped a moved works_in/governs/informs/in_repo edge with the fold's own actor as
    source_id, discarding the original writer. The pre-fold row still carries the
    correct source_id, so this re-derives the live edge from evidence already on record.

    Resolves `project`'s own merged-in dupes and repairs only damage from those folds.
    Dry run is the default; `dry_run=False` requires a non-blank `because`. Safe to run
    twice — an already-correct or already-repaired edge is left alone."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a restore is a mind's act, and the graph must "
                         "know whose", "why": _anchorless(ctx)}
    from src.orchestrator.projects import restore_attribution as _restore_attribution
    return await _restore_attribution(
        Actions(await _pool_get()), project=project, actor=ident.agent_id,
        dry_run=dry_run, because=because)


@mcp.tool()
async def unwire_informs_fanout(
    project: str = "osiris", dry_run: bool = True, because: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Repair verb for the pre-fix `_wire_informs` cross-join: `ingest_canon` used to fan
    every Reference out to EVERY active SoftwareProject fleet-wide instead of just the
    one it grounds. Fixed going forward (src/ingest/reference.py); this repairs the
    historical damage.

    Finds every live `informs` edge stamped with the fan-out's own source_id whose
    target is NOT `project` (default "osiris", the module's only real caller) — never
    touches an informs edge asserted by anything else. DRY RUN IS THE DEFAULT. `dry_run
    =False` REQUIRES a non-blank `because`. Idempotent."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — an unwire is a mind's act, and the graph must "
                         "know whose", "why": _anchorless(ctx)}
    from src.ingest.reference import unwire_informs_fanout as _unwire_informs_fanout
    return await _unwire_informs_fanout(
        Actions(await _pool_get()), project=project, actor=ident.agent_id,
        dry_run=dry_run, because=because)


_BACKFILL_TARGETS = frozenset({
    "bootstrap_orphan_references", "boot_alarm_commit_links", "task_sync_citation_links",
    "lineage_repo_links", "agent_project_links",
})


async def _backfill_impl(
    target: str, dry_run: bool, because: str | None, only_bases: list[str] | None,
    ctx: Context | None,
) -> dict[str, Any]:
    """Shared dispatch (task #199 lane 2, families wave, thread 6854): five repair verbs
    with near-identical wire shape (dry_run default True, because required to write,
    idempotent, mount-gated) but NO shared orchestrator call — each fixes a structurally
    different defect class in its own module. Unlike abstained_derivations's own
    consolidation (one real underlying query, three filters), this is a genuine dispatch
    table, named as such rather than dressed up as a merge. `target` selects which."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a backfill is a mind's act, and the graph must "
                         "know whose", "why": _anchorless(ctx)}
    pool = await _pool_get()
    if target == "bootstrap_orphan_references":
        from src.ingest.reference import (
            backfill_bootstrap_orphan_references as _f_orphan_refs,
        )
        return await _f_orphan_refs(
            Actions(pool), actor=ident.agent_id, dry_run=dry_run, because=because)
    if target == "boot_alarm_commit_links":
        from src.orchestrator.capture import backfill_boot_alarm_commit_links as _f_boot_alarm
        return await _f_boot_alarm(
            Actions(pool), actor=ident.agent_id, dry_run=dry_run, because=because)
    if target == "task_sync_citation_links":
        from src.orchestrator.task_sync import (
            backfill_task_sync_citation_links as _f_task_sync,
        )
        return await _f_task_sync(
            Actions(pool), actor=ident.agent_id, dry_run=dry_run, because=because)
    if target == "lineage_repo_links":
        from src.orchestrator.capture import backfill_lineage_repo_links as _f_lineage
        return await _f_lineage(
            Actions(pool), actor=ident.agent_id, dry_run=dry_run, because=because)
    if target == "agent_project_links":
        from src.orchestrator.agents import backfill_agent_project_links as _f_agent_links
        return await _f_agent_links(
            Actions(pool), actor=ident.agent_id, dry_run=dry_run,
            only_bases=set(only_bases) if only_bases else None)
    return {"error": f"unknown target {target!r}", "valid_targets": sorted(_BACKFILL_TARGETS)}


@mcp.tool()
async def backfill(
    target: str, dry_run: bool = True, because: str | None = None,
    only_bases: list[str] | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Repair verb, dispatched over `target` — five structurally distinct backfills (no
    shared logic underneath, only a shared wire shape). Dry run is the default for every
    target; `dry_run=False` requires `because` (except `agent_project_links`, which
    predates that convention). All five idempotent.

    `target=`: "bootstrap_orphan_references" (links an orphaned `ref:osiris`-stamped
    Reference to the SoftwareProject its own canonical prefix names) |
    "boot_alarm_commit_links" (links a zero-link boot-alarm Thread to the Commit its
    summary cites) | "task_sync_citation_links" (links a zero-link task_sync Thread to
    the Thread it names) | "lineage_repo_links" (links a zero-link Decision/Thread to its
    author's lineage project) | "agent_project_links" (moves works_in/governs off an
    off-head Agent onto its living head; the one target taking `only_bases` to scope the
    write)."""
    if target not in _BACKFILL_TARGETS:
        return {"error": f"unknown target {target!r}", "valid_targets": sorted(_BACKFILL_TARGETS)}
    return await _backfill_impl(target, dry_run, because, only_bases, ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "backfill(target='bootstrap_orphan_references')",
    "since": "task #199 lane 2, families wave (thread 6854)",
})
async def backfill_bootstrap_orphan_references(
    dry_run: bool = True, because: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Repair verb for the bootstrap_project door-gap (decision 49231693/adde094b,
    operator ruling 2026-08-27: a doc-splitter's orphans are a defect, not a legitimate
    category — "something definitely went wrong here"). `ingest_log`/`ingest_reference_
    doc` now take `repo=`, threaded through by `bootstrap_project` going forward; this
    repairs the ~105 References already orphaned from before that fix.

    MECHANICAL AND CONSERVATIVE, per the operator's other half of the same ruling: "a
    derived link that is wrong is worse than an orphan that is honest." Only touches a
    zero-live-link Reference stamped `source_id='ref:osiris'` (the script's own
    fingerprint) whose canonical starts `ref:<name>-` for an EXISTING active
    SoftwareProject — recovering what the canonical already encodes, never inventing a
    fact. No clean project-name prefix → left alone, reported in `unmatched`, never guessed.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Links land
    evidence_class DERIVED. Idempotent."""
    return await _backfill_impl("bootstrap_orphan_references", dry_run, because, None, ctx)


@mcp.tool()
async def repair_stale_pile_summons(
    dry_run: bool = True, because: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Repair verb for the 2026-07-13 bulk-minted "DISPOSE OF YOUR MINER PILE" threads,
    whose summaries froze that day's candidates() count in prose and never re-derive it.
    Re-measures each still-open one against a live candidates(project=...) call: live count
    matches the frozen one → untouched; live count is 0 → resolves the thread as moot
    (nothing left to judge); live count is >0 but differs → corrects the thread's own
    summary via correct_thread_summary, never resolves it and never disposes anyone's pile
    on their behalf (only that project's own seat may judge its own pile). Only matches the
    exact bulk-mint template on a still-open Thread, never one that merely mentions a number.

    DRY RUN IS THE DEFAULT. `dry_run=False` requires a non-blank `because` for the resolve
    actions. Idempotent."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a repair is a mind's act, and the graph must "
                         "know whose", "why": _anchorless(ctx)}
    from src.orchestrator.dispose import (
        repair_stale_pile_summons as _repair_stale_pile_summons,
    )
    return await _repair_stale_pile_summons(
        Actions(await _pool_get()), actor=ident.agent_id, dry_run=dry_run, because=because)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "backfill(target='boot_alarm_commit_links')",
    "since": "task #199 lane 2, families wave (thread 6854)",
})
async def backfill_boot_alarm_commit_links(
    dry_run: bool = True, because: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Links every zero-live-link `UNREVIEWED BOOT` alarm Thread (deploy_guard's own
    boot watchdog — no caller identity to default a repo= from) to the Commit its own
    summary cites by sha, via `derive_or_abstain`: mints `noted_in` (DIRECT_OBSERVATION)
    iff the sha resolves to exactly one Commit; no sha, or an ambiguous match, abstains
    durably with the candidate set kept. Does NOT arm required_link_kinds (stays empty) —
    a boot alarm still can't satisfy a repo= requirement, this only gives it the
    connectivity it actually has.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent."""
    return await _backfill_impl("boot_alarm_commit_links", dry_run, because, None, ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "backfill(target='task_sync_citation_links')",
    "since": "task #199 lane 2, families wave (thread 6854)",
})
async def backfill_task_sync_citation_links(
    dry_run: bool = True, because: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Links every zero-live-link `task_sync`-minted obligation Thread ("TASK/THREAD
    DISAGREEMENT: ..." / "THREAD SIDE ORPHAN: ...", decision a55b1014 — the tracker-vs-graph
    divergence detector that has been firing correctly for weeks into an unread orphan) to
    the Thread its own summary names, via `derive_or_abstain`: mints `cites`
    (DIRECT_OBSERVATION, origin=derived) iff `task_sync.parse_thread_citations` (reused, not
    re-implemented) finds exactly one citation and it resolves to exactly one existing
    Thread; anything else abstains durably with a distinct reason and the candidate set kept.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent."""
    return await _backfill_impl("task_sync_citation_links", dry_run, because, None, ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "backfill(target='lineage_repo_links')",
    "since": "task #199 lane 2, families wave (thread 6854)",
})
async def backfill_lineage_repo_links(
    dry_run: bool = True, because: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Links every zero-live-link Decision/Thread authored by a real Agent lineage to its
    project — the historical half of the repo= lineage ladder (Lane 3 + Wave 2 Lane B's
    resolve_repo_default), which is write-time-only by design and never touches an object
    that already existed before it deployed (decision c1073f00). Re-runs the same rung-3
    lineage-wide works_in lookup a NEW write already gets: mints `in_repo`
    (DIRECT_OBSERVATION) iff the author's lineage names exactly one project; zero or 2+
    abstains durably via derive_or_abstain, candidate set kept, never a guess (a0339e16).

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent."""
    return await _backfill_impl("lineage_repo_links", dry_run, because, None, ctx)


@mcp.tool()
async def recover_harness_exchanges(
    anchor_sid: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """Lift a session's harness-native cross-session messages (SendMessage) out of its
    already-soul-stored transcript into typed, attributed `harness_messages` rows —
    osiris cannot see the harness's cross-session socket live, only after a transcript
    is soul-stored and this runs over it.

    `anchor_sid` must already be soul-stored — never reads disk itself. Dry run is the
    default: returns `{found, already_recovered, would_write, sample}`. `dry_run=False`
    requires `because`. Idempotent per (anchor_sid, turn_index)."""
    from src.ingest.cross_channel import recover_harness_exchanges as _recover
    return await _recover(await _pool_get(), anchor_sid, dry_run=dry_run, because=because)


async def _reconcile_seat_identity_impl(
    seat_id: str | None, agent_id: str | None, because: str | None, ctx: Context | None,
) -> dict[str, Any]:
    """The one body behind `reconcile_seat_identity` (self OR third-party) and its
    deprecated alias `reconcile_seat_identity_third_party` (task #199 lane 2, thread
    6778/6788). `seat_id=None` heals the CALLER's own held seat and own agent identity,
    `because` unused. `seat_id=<any seat>` is the third-party path — `agent_id`
    optional (omitted heals `house` alone), `because` REQUIRED (a correction with no
    stated reason is the silent overwrite 719ed5b1 rules against, not a fix)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — reconcile_seat_identity is a seat's own act",
                "why": _anchorless(ctx)}
    pool = await _pool_get()
    if seat_id is None:
        from src.orchestrator.seats import held_seat
        bound = await held_seat(pool, ident.agent_id)
        if bound is None:
            return {"error": f"{ident.agent_id} holds no seat — nothing to reconcile"}
        from src.orchestrator.identity_heal import reconcile_seat_identity as _reconcile
        return await _reconcile(Actions(pool), seat_id=bound["seat_id"],
                                agent_id=ident.agent_id, actor=ident.agent_id)
    from src.orchestrator.identity_heal import (
        reconcile_seat_identity_third_party as _reconcile_third_party,
    )
    return await _reconcile_third_party(
        Actions(pool), seat_id=seat_id, agent_id=agent_id, because=because or "",
        actor=ident.agent_id)


@mcp.tool()
async def reconcile_seat_identity(
    seat_id: str | None = None, agent_id: str | None = None, because: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Heal a seat's identity: a cross-source CONTRADICTION on exactly two properties, a
    Seat's `house` and its holder Agent's `project`. Newest-declared-wins, same tiebreak
    the read path applies. Reversible — every healed row's id is in the receipt.

    `seat_id=None` (default) heals the caller's own held seat, `because` unused.
    `seat_id=<any seat>` heals a third party's — `agent_id` optional (omitted heals
    `house` alone), `because` required. Does not check caller authority beyond being
    mounted on the third-party path."""
    return await _reconcile_seat_identity_impl(seat_id, agent_id, because, ctx)


@mcp.tool(meta={"deprecated": True, "use_instead": "reconcile_seat_identity",
                "since": "task #199 lane 2, thread 6778/6788"})
async def reconcile_seat_identity_third_party(
    seat_id: str, because: str, agent_id: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """DEPRECATED — a hidden alias, dropped from a model's own tool list but still fully
    callable. Shares reconcile_seat_identity's own _reconcile_seat_identity_impl body —
    nothing duplicated. Kept only so a live or sleeping caller whose standing orders
    still name this verb is not broken at its next turn; remove once tool_traffic()
    shows it silent."""
    return await _reconcile_seat_identity_impl(seat_id, agent_id, because, ctx)


async def _heal_seat_anchor_impl(
    seat_id: str | None, because: str | None, dry_run: bool, ctx: Context | None,
) -> dict[str, Any]:
    """The one body behind both `heal_seat_anchor` (self OR third-party, by whether
    `seat_id` is given) and its deprecated alias `heal_seat_anchor_third_party` — a plain
    helper, never itself an `@mcp.tool()`, so the two names share this instead of each
    re-implementing it (task #199 lane 2, thread 6778, the six-pair consolidation's proof).
    `seat_id=None` heals the CALLER's own held seat, `because` optional; `seat_id=<any
    seat>` heals a THIRD PARTY's, `because` REQUIRED (a correction with no stated reason
    is the silent overwrite 719ed5b1 rules against, not a fix)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — heal_seat_anchor is a seat's own act",
                "why": _anchorless(ctx)}
    if seat_id is None:
        from src.orchestrator.seats import held_seat
        bound = await held_seat(await _pool_get(), ident.agent_id)
        if bound is None:
            return {"error": f"{ident.agent_id} holds no seat — nothing to heal"}
        seat_id = bound["seat_id"]
    else:
        because = (because or "").strip()
        if not because:
            return {"error": "a correction with no reason is exactly the silent overwrite "
                             "719ed5b1 rules against — refusing"}
    from src.orchestrator.identity_heal import heal_seat_anchor as _heal
    return await _heal(Actions(await _pool_get()), seat_id=seat_id, because=because,
                       actor=ident.agent_id, dry_run=dry_run)


@mcp.tool()
async def heal_seat_anchor(
    seat_id: str | None = None, because: str | None = None, dry_run: bool = True,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """HEAL A SEAT's `anchor_cwd` against THE ANCHOR INVARIANT: a Seat's anchor is
    IDENTITY, always `<office_root>/<handle>`, never wherever a session happens to be
    sitting. Asserts the invariant office path as the SOLE current `anchor_cwd`,
    collapsing every stray value. REFUSES rather than guesses: no handle on record, or
    the office directory does not exist on disk.

    `seat_id=None` (default) heals the CALLER's own held seat, `because` optional;
    `seat_id=<any seat>` heals a third party's, `because` REQUIRED.

    `dry_run=True` is the default; the receipt shows `current_before` and `target`."""
    return await _heal_seat_anchor_impl(seat_id, because, dry_run, ctx)


@mcp.tool(meta={"deprecated": True, "use_instead": "heal_seat_anchor",
                "since": "task #199 lane 2, thread 6778"})
async def heal_seat_anchor_third_party(
    seat_id: str, because: str, dry_run: bool = True, ctx: Context | None = None,
) -> dict[str, Any]:
    """DEPRECATED — a hidden alias, dropped from a model's own tool list but still fully
    callable (BoundedMCP.list_tools's filter, call_tool's own registry lookup bypasses
    it). Shares `heal_seat_anchor`'s own `_heal_seat_anchor_impl` body — nothing
    duplicated. Kept only so a live or sleeping caller whose standing orders still name
    this verb is not broken at its next turn; remove once tool_traffic() shows it silent."""
    return await _heal_seat_anchor_impl(seat_id, because, dry_run, ctx)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found — "
              "automated companion uningested_trees_alarm_tick already covers this ground",
    "since": "task #199 lane 2, retirement wave 2 (msg 6872/6876)",
})
async def uningested_trees(only_gaps: bool = True) -> dict[str, Any]:
    """THE CENSUS (thread 5126) — door onto discover_trees. One row per active
    SoftwareProject: `tree`, `path`, `watched`, `commits`, `activity`, `last_ingested_at`,
    `reason` (why `commits==0`: no path, unwatched, never ticked, or ticked-and-empty),
    `blind` (a path is known but unwatched). `only_gaps=True` (default) narrows to
    `commits==0`; False for the full census."""
    from src.config.settings import get_settings
    from src.orchestrator.neighborhoods import discover_trees
    settings = get_settings()
    watched = [w.strip() for w in settings.osiris_dev_repos.split(",") if w.strip()]
    rows = await discover_trees(await _pool_get(), watched=watched)
    if only_gaps:
        rows = [r for r in rows if r["commits"] == 0]
    return {"count": len(rows), "trees": rows}


async def _ingest_project_impl(
    project: str | None, because: str | None, dry_run: bool, ctx: Context | None,
) -> dict[str, Any]:
    """The one body behind `ingest_project` and its deprecated alias `ingest_project_
    third_party` (task #199 lane 2, thread 6778/6788). `because` blank/omitted is the
    self-service shape (`project` omitted resolves to the caller's own mounted pin);
    `because` given routes through the third-party orchestrator function instead, which
    stamps it onto the receipt — the orchestrator layer already IS this same split
    (ingest_project_third_party's own body is nothing but a because-required check
    wrapping a call to ingest_project), this only removes the second MCP-layer copy of
    that check."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — ingest_project is a seat's own act",
                "why": _anchorless(ctx)}
    pool = await _pool_get()
    because = (because or "").strip()
    if because:
        from src.orchestrator.tree_ingest import (
            ingest_project_third_party as _ingest_third_party,
        )
        if not project:
            return {"error": "a third-party ingest needs an explicit project — nothing "
                             "to resolve a pin against on someone else's behalf"}
        return await _ingest_third_party(Actions(pool), project=project, because=because,
                                         dry_run=dry_run, actor=ident.agent_id)
    target = project or ident.project
    if not target:
        return {"error": "no project given and none pinned — mount with a project, or pass "
                         "one explicitly with a because for the third-party shape instead"}
    from src.orchestrator.tree_ingest import ingest_project as _ingest_project
    return await _ingest_project(Actions(pool), project=target, dry_run=dry_run,
                                 actor=ident.agent_id)


@mcp.tool()
async def ingest_project(
    project: str | None = None, dry_run: bool = True, because: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """SELF-SERVICE (thread 5126) — land YOUR OWN project's git history and close the
    threads it witnesses, one call, same authority shape as reconcile_seat_identity.
    `project` omitted resolves to your mounted pin; refuses cleanly if none is pinned.
    `dry_run=True` (default) writes NOTHING — the receipt names what would land (commits
    on disk vs already graphed) plus a closure preview over what's already graphed.
    `dry_run=False` actually ingests, then closes.

    `because=<reason>` is the THIRD-PARTY shape instead: `project` becomes required (no
    pin to fall back on for someone else's tree), the reason lands on the receipt,
    does not check caller authority beyond being mounted."""
    return await _ingest_project_impl(project, because, dry_run, ctx)


@mcp.tool(meta={"deprecated": True, "use_instead": "ingest_project(because=...)",
                "since": "task #199 lane 2, thread 6778/6788"})
async def ingest_project_third_party(
    project: str, because: str, dry_run: bool = True, ctx: Context | None = None,
) -> dict[str, Any]:
    """DEPRECATED — a hidden alias, dropped from a model's own tool list but still fully
    callable. Shares ingest_project's own _ingest_project_impl body — nothing
    duplicated. Kept only so a live or sleeping caller whose standing orders still name
    this verb is not broken at its next turn; remove once tool_traffic() shows it
    silent."""
    return await _ingest_project_impl(project, because, dry_run, ctx)


@mcp.tool()
async def correct_house(new_house: str, ctx: Context | None = None) -> dict[str, Any]:
    """A HEAD corrects its OWN stored house (ruling ff6148b0, decision 87953278) — the one
    legitimate write left after house became a live derivation off the managed_by chain
    (derive_house): a head's anchor is a deliberate identity declaration, exactly like
    claim_name, so this is SELF-scoped and never operator-fenced. Refuses on a non-head
    (an active managed_by edge out means this seat derives its house through its manager
    now — nothing here to correct) or a caller holding no seat. Patches every live cached
    identity in your own lineage — the next orient() reflects it without a reconnect."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — house-correct is a seat's own act",
                "why": _anchorless(ctx)}
    pool = await _pool_get()
    from src.orchestrator.seats import correct_house as _correct_house
    result = await _correct_house(Actions(pool), ident.agent_id, new_house,
                                  source=ident.agent_id)
    if not result.get("error"):
        base = _generation(ident.agent_id)[0]
        for cached in _agents.values():
            if _generation(cached.agent_id)[0] == base:
                await _resolve_project_seat_first(pool, cached)
    return result


@mcp.tool()
async def correct_pin_value(key: str, value: str, reason: str,
                            ctx: Context | None = None) -> dict[str, Any]:
    """Correct an EXISTING key in your own seat's `.osiris` pin (msg 4761, obligation
    114f7ac9) — the door that was missing: `offices.correct_pin_value` (the raw rewrite) had
    no MCP surface at all, so a caller told to use it could only hand-edit the file. THE
    NAMED EXCEPTION to additive-only pin writes (write_pin_additions never overwrites an
    existing key, by design) — this one does, for a specific, already-diagnosed correction.
    SELF-SCOPED like `correct_house`: always targets YOUR OWN seat's office (resolved off
    `held_seat`, never a path you supply), never another seat's. `reason` is required and
    non-empty — a correction with no stated reason is the silent overwrite this verb exists
    to prevent. Refuses on: no held seat; `key` not already declared (use write_pin_additions
    for a genuinely missing one); invalid TOML; an empty reason. Also corrects your ANCHOR
    copy when one exists and differs (ruling b30e2b38), reported under `anchor`.
    `revert_own_pin_write` is the self-scoped undo."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a pin correction is a seat's own act",
                "why": _anchorless(ctx)}
    pool = await _pool_get()
    from src.orchestrator.offices import correct_own_pin_value as _correct_own_pin_value
    return await _correct_own_pin_value(pool, ident.agent_id, key, value, reason=reason)


@mcp.tool()
async def revert_own_pin_write(ctx: Context | None = None) -> dict[str, Any]:
    """Undo your seat's most recent pin correction/addition — the self-scoped door onto
    `offices.revert_pin_write` (existed, tested, unreached until ruling b30e2b38). Resolved
    off your own held seat, never a path you supply. Restores the `.osiris.bak` your most
    recent real write took; refuses if none exists. Also reverts your ANCHOR copy under
    `anchor` when a backup exists there too — silently skipped otherwise."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — reverting a pin is a seat's own act",
                "why": _anchorless(ctx)}
    pool = await _pool_get()
    from src.orchestrator.offices import revert_own_pin_write as _revert_own_pin_write
    return await _revert_own_pin_write(pool, ident.agent_id)


@mcp.tool()
async def retire_seat(seat_id: str, reason: str = "",
                      ctx: Context | None = None) -> dict[str, Any]:
    """Mark a Seat permanently CLOSED — a genuinely dead role, no successor, no merge
    target. DISTINCT from retire() (that one retires a live agent's own session/turn;
    this retires the ROLE ITSELF). Refuses on an unknown or already-inactive seat, or an
    ACTIVE holder — transfer or let it vacate first."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — retiring a seat is a deliberate act on the record",
                "why": _anchorless(ctx)}
    from src.orchestrator.seats import retire_seat as _retire_seat
    return await _retire_seat(Actions(await _pool_get()), seat_id, reason=reason,
                              actor=ident.agent_id)


@mcp.tool()
async def sweep_seat_disk(handle: str, dry_run: bool = True, because: str = "",
                          ctx: Context | None = None) -> dict[str, Any]:
    """THE DISK HALF retire_seat never touches. Sweeps both scaffolded directories for
    `handle` — the office and the workspace — reporting each half's own receipt under
    `office`/`workspace`; a refusal on one never blocks the other.

    Each half independently refuses on an ambiguous Seat match, a Seat that is neither
    retired nor absent, an active holder, or a live body at that path (including after a
    90s heal-wait re-check) — a directory with no matching Seat row (pure filesystem
    debris) is the one case both halves accept.

    `dry_run=True` (default) reports `would-delete` for whichever half(s) pass their
    guards, nothing removed. `dry_run=False` requires `because` and deletes only the
    half(s) that pass every guard."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a disk sweep is a deliberate act on the record",
                "why": _anchorless(ctx)}
    handle = (handle or "").strip()
    if not handle:
        return {"error": "a handle is required"}
    pool = await _pool_get()
    from src.orchestrator.offices import sweep_retired_office, sweep_seat_workspace
    because_arg = because.strip() or None
    office_out = await sweep_retired_office(pool, handle=handle, dry_run=dry_run,
                                            because=because_arg)
    workspace_out = await sweep_seat_workspace(pool, handle=handle, dry_run=dry_run,
                                               because=because_arg)
    return {"handle": handle, "dry_run": dry_run, "office": office_out,
            "workspace": workspace_out}


@mcp.tool()
async def vacate_seat(seat_id: str, because: str, ctx: Context | None = None) -> dict[str, Any]:
    """Release a seat's holder without retiring the seat — for a holder whose process
    died without calling retire() itself, leaving a stale `holds` link retire_seat
    rightly refuses to touch. This is that refusal's complement, never its bypass.

    Gated on real liveness evidence: the harness roster must show no live session at the
    seat's office, AND the holder's transcript's newest timestamped line must be stale
    (never mtime alone). Either signal showing life refuses loudly (`refused-live`); an
    unreadable roster refuses as `refused-ambiguous`. `status`: vacated |
    refused-vacant | refused-no-office | refused-live | refused-ambiguous | refused (see
    `detail`).

    Deliberate hand, one named seat, never a sweep."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — vacating a seat's holder is a deliberate act on "
                         "the record", "why": _anchorless(ctx)}
    from src.orchestrator.trigger import vacate_dead_seat
    return await vacate_dead_seat(Actions(await _pool_get()), seat_id=seat_id,
                                  actor=ident.agent_id, because=because)


@mcp.tool()
async def retire_project(project: str, because: str,
                         ctx: Context | None = None) -> dict[str, Any]:
    """Retire a dead SoftwareProject stub (msg 1675, the stub cull) — status flip to
    'retired' via a compensating event, never a DELETE. `project` resolves to a
    SoftwareProject ONLY (UUID, 8-char short id, canonical `repo:<name>`, or its `name`
    property) — never a Seat or Agent of the same name, even for a name like 'seshat' or
    'ra' that also happens to be a seat's handle.

    Refuses LOUDLY on: blank `because`; an unresolved or already-non-active project; any
    commit recorded against it; any open Thread pointing in (`in_repo`, status='active');
    or a mount seen against it within the last 15 minutes (live signal — this verb never
    evicts a project actually in use)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — retiring a project is a deliberate act on the "
                         "record", "why": _anchorless(ctx)}
    from src.orchestrator.projects import retire_project as _retire_project
    return await _retire_project(Actions(await _pool_get()), project=project,
                                 actor=ident.agent_id, because=because)


@mcp.tool()
async def project_identity_evidence(seat_id: str, operator_citation: str | None = None,
                                    ctx: Context | None = None) -> dict[str, Any]:
    """READ-ONLY. Gathers whichever of five evidence tiers have signal for `seat_id`'s
    project identity — operator citation, declared charter, self-authored CLAUDE.md/
    charter.md, the seat's `.osiris` pin, a live git remote check, and write-attribution
    — and reports each tier's answer plus per-candidate agreement/disagreement. Never
    picks a winner: no tier ranks first across the whole population. Read this BEFORE
    calling rename_project/fork_project. `operator_citation` = a decision id/quote
    already in hand for tier 1 (never parsed from prose)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — reading identity evidence needs a resolvable "
                         "caller", "why": _anchorless(ctx)}
    from src.orchestrator.project_identity import (
        project_identity_evidence as _project_identity_evidence,
    )
    return await _project_identity_evidence(
        await _pool_get(), seat_id=seat_id, operator_citation=operator_citation)


@mcp.tool()
async def rename_project(project: str, new_name: str, because: str,
                         ctx: Context | None = None) -> dict[str, Any]:
    """Declare a SoftwareProject's new NAME — `canonical` never changes, only the mutable
    `name` property (old value kept in assertion history). Zero edges move; only
    `agent_mounts.project` re-addresses. Out of scope: a seat's own `.osiris` pin file —
    graph-only. `because` is mandatory; this never infers, only declares what a human
    already decided (read project_identity_evidence FIRST).

    PRE-WRITE CHECK: every Seat governing this project gets project_identity_evidence
    run against `new_name`, classified no-signal/confirms/disagrees in `rename_evidence`.
    Write proceeds regardless; `evidence_disagrees=True` plus a `warning` land when any
    seat's evidence disagrees.

    Refuses loudly on: blank `new_name`/`because`; unresolved/ambiguous `project`; a
    non-active project; `new_name` colliding with a DIFFERENT active project (`merge` is
    the deliberate verb for that, never a silent one here)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a rename is a deliberate act on the record",
                "why": _anchorless(ctx)}
    pool = await _pool_get()
    from src.orchestrator.project_identity import (
        project_identity_evidence as _project_identity_evidence,
    )
    from src.orchestrator.project_identity import rename_evidence_verdict
    from src.orchestrator.project_identity import rename_project as _rename_project
    from src.orchestrator.projects import AmbiguousProjectRef, _resolve_software_project
    evidence_by_seat: dict[str, Any] = {}
    try:
        row = await _resolve_software_project(pool, project)
    except AmbiguousProjectRef:
        row = None  # the real refusal below (inside _rename_project) names the
                    # candidates properly; evidence-gathering here is best-effort only
    if row is not None:
        seat_rows = await pool.fetch(
            "SELECT s.canonical FROM links l JOIN objects s ON s.id=l.from_id "
            "WHERE l.to_id=$1 AND l.type='governs' "
            "AND (l.valid_until IS NULL OR l.valid_until > now())", row["id"])
        for r in seat_rows:
            evidence_by_seat[r["canonical"]] = await _project_identity_evidence(
                pool, seat_id=r["canonical"])
    out = await _rename_project(Actions(pool), project=project, new_name=new_name,
                                because=because, actor=ident.agent_id)
    if evidence_by_seat:
        rename_evidence = {
            seat: {"verdict": rename_evidence_verdict(ev, new_name), "evidence": ev}
            for seat, ev in evidence_by_seat.items()
        }
        out["rename_evidence"] = rename_evidence
        out["rename_evidence_note"] = (
            "a verdict is SELF-CONSISTENCY, not independent verification: \"confirms\" "
            "means this seat's own non-remote tiers (charter/pin/write-attribution) all "
            "agree with new_name, never that new_name is objectively correct — remote is "
            "deliberately non-authoritative here, so it can dissent alone and still read "
            "\"confirms\"; and #137's own mechanism can corrupt a seat's pin itself, not "
            "only the graph's name property, in which case every non-remote tier already "
            "carries the same drift and this check reads clean")
        disagreeing = [s for s, v in rename_evidence.items() if v["verdict"] == "disagrees"]
        if disagreeing:
            out["evidence_disagrees"] = True
            out["warning"] = (
                f"{new_name!r} was written, but {len(disagreeing)} governing seat "
                f"evidence disagrees with it: {', '.join(disagreeing)} — their own pin/"
                "charter/remote still names something else; go fix those, this write "
                "did not")
    return out


async def _fork_project_impl(
    project: str, fork_into: str, because: str, direction: str, ctx: Context | None,
) -> dict[str, Any]:
    """The one body behind `fork_project` (both directions, by `direction`) and its
    deprecated alias `unfork_project` — a plain helper, never itself an `@mcp.tool()`
    (task #199 lane 2, thread 6778/6788, the six-pair consolidation). `direction="fork"`
    (default) declares the pair; `direction="unfork"` reverses it — same verb, its own
    inverse, Thoth's own named shape for this pair."""
    if direction not in ("fork", "unfork"):
        return {"error": f"direction must be 'fork' or 'unfork', got {direction!r}"}
    verb = "a fork" if direction == "fork" else "an unfork"
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": f"mount first — {verb} is a deliberate act on the record",
                "why": _anchorless(ctx)}
    pool = await _pool_get()
    if direction == "fork":
        from src.orchestrator.project_identity import fork_project as _fork_project
        return await _fork_project(Actions(pool), project=project, fork_into=fork_into,
                                   because=because, actor=ident.agent_id)
    from src.orchestrator.project_identity import unfork_project as _unfork_project
    return await _unfork_project(Actions(pool), project=project, fork_into=fork_into,
                                 because=because, actor=ident.agent_id)


@mcp.tool()
async def fork_project(
    project: str, fork_into: str, because: str, direction: str = "fork",
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Declare two already-active SoftwareProjects a FORK pair: `fork_into` is the
    successor, naming `project` as its ancestor via one `forked_from` edge. No estate
    moves — every existing edge on both objects stays put; this records a new
    relationship, never merges two into one (`merge` is that verb). Never mints either
    side — both must already exist. Read project_identity_evidence BEFORE calling this.

    `direction="unfork"` reverses: invalidates the live `forked_from` edge — trivially
    reversible since nothing ever moved. `unfork_project` still works as a hidden alias.

    Refuses loudly on: blank `because`; project==fork_into (fork only); an unresolved
    ref; either side not active (fork); an already-connected pair (fork) or no live edge
    to invalidate (unfork)."""
    return await _fork_project_impl(project, fork_into, because, direction, ctx)


@mcp.tool(meta={"deprecated": True, "use_instead": "fork_project(direction='unfork')",
                "since": "task #199 lane 2, thread 6778/6788"})
async def unfork_project(project: str, fork_into: str, because: str,
                         ctx: Context | None = None) -> dict[str, Any]:
    """DEPRECATED — hidden alias, still callable. Forwards to
    fork_project(direction="unfork")."""
    return await _fork_project_impl(project, fork_into, because, "unfork", ctx)


@mcp.tool()
async def create_project(name: str, because: str, ctx: Context | None = None) -> dict[str, Any]:
    """Declare a NEW SoftwareProject (#139's create half) — layers task #107's name-shape
    validation with task #137's case-insensitive de-dup, never a fresh, unguarded mint
    (this was deliberately NOT built as a seventh mint door). If `name` already resolves
    (exact match or a case-insensitive canonical twin, the ramstein/RAMstein shape) the
    EXISTING object is returned, `created=False` — never a duplicate.

    Refuses LOUDLY on: a blank `because` (creating a project is testimony, same as
    rename_project/fork_project) or a path-shaped/malformed `name`."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — creating a project is a deliberate act on the "
                         "record", "why": _anchorless(ctx)}
    from src.orchestrator.project_identity import create_project as _create_project
    return await _create_project(Actions(await _pool_get()), name=name, because=because,
                                 actor=ident.agent_id)


@mcp.tool()
async def assert_project_property(project: str, name: str, value: str,
                                  ctx: Context | None = None) -> dict[str, Any]:
    """The sanctioned write for a SINGLE project-scoped property (task #74) — closes the
    gap that forced in-process scripts for anything beyond a status flip during the reap.
    `project` resolves the same way retire_project does (UUID, 8-char short id, canonical
    `repo:<name>`, or its `name` property) — SoftwareProject ONLY. NOT self-scoped, and
    OPEN BY DESIGN: any mounted caller may stamp any named project's property, no
    authority gate.

    Refuses LOUDLY on: blank project/name/value; an unresolved project; `name=='status'`
    (status has its own compensating-event path — retire_project, not a bare assertion)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — asserting a project property is a deliberate "
                         "act on the record", "why": _anchorless(ctx)}
    from src.orchestrator.projects import assert_project_property as _assert_project_property
    return await _assert_project_property(Actions(await _pool_get()), project=project,
                                          name=name, value=value, actor=ident.agent_id)


@mcp.tool()
async def peer_seats(seat_a: str, seat_b: str, because: str,
                     ctx: Context | None = None) -> dict[str, Any]:
    """Mint a SYMMETRIC peer_of bond between two active Seats (ruling d74492ee, spec
    e6636c7e) — recognition-first: makes the pair legible to mail routing, review
    assignment, and succession. NOT self-scoped — neither seat need be the caller's own;
    the caller is recorded only as `actor` (who made the bond), never a party to it by
    default.

    Refuses LOUDLY on: blank `because`; an unknown/inactive seat on either side;
    seat_a==seat_b; or either seat already carrying an active peer_of edge — v1 is PAIRS
    ONLY, no chains (a triad is deferred to v1.1, after the first pair survives contact)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — peering two seats is a deliberate act on the "
                         "record", "why": _anchorless(ctx)}
    from src.orchestrator.seats import peer_seats as _peer_seats
    return await _peer_seats(Actions(await _pool_get()), seat_a, seat_b, because=because,
                             actor=ident.agent_id)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def unpeer(seat_a: str, seat_b: str, because: str,
                 ctx: Context | None = None) -> dict[str, Any]:
    """Invalidate an active peer_of bond between two Seats — the compensating-event
    complement to peer_seats. Direction-agnostic: the bond is symmetric, so unpeer(a, b)
    and unpeer(b, a) heal the same edge.

    Refuses LOUDLY on: blank `because`; an unknown/inactive seat on either side; or no
    active peer_of edge between the named pair."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — unpeering two seats is a deliberate act on the "
                         "record", "why": _anchorless(ctx)}
    from src.orchestrator.seats import unpeer as _unpeer
    return await _unpeer(Actions(await _pool_get()), seat_a, seat_b, because=because,
                         actor=ident.agent_id)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def hold_action(holder: str, held: str, act: str, because: str, hours: float = 24,
                      ctx: Context | None = None) -> dict[str, Any]:
    """Mint a mutual HOLD (task #76 item 4a, spec e6636c7e) — a peer's power to say HOLD on
    its OWN peer's specific irreversible act, time-boxed. `holder` is the seat calling the
    hold, `held` is the seat whose act is being held, `act` names the specific act, `hours`
    sets the time-box (default 24). Reuses the ordinary obligation Thread shape wholesale —
    no new object type. Resolve it the ordinary way, with `resolve_thread` on the returned
    `held` id, once it's respected or the act proceeds anyway. The spec's own auto-
    escalation-to-the-operator half (an unresolved hold past its deadline reaching the
    desk unprompted) is NOT built yet — this only records the hold and its deadline
    honestly; nothing sweeps for expiry today.

    Refuses LOUDLY on: blank `act`/`because`; `holder==held`; an unknown/inactive seat on
    either side; non-positive `hours`; or holder/held not currently an active peer_of
    pair — a hold is a peer's own power, never a stranger's."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — holding a peer's act is a deliberate act on the "
                         "record", "why": _anchorless(ctx)}
    from src.orchestrator.seats import hold_action as _hold_action
    return await _hold_action(Actions(await _pool_get()), holder, held, act=act,
                              because=because, hours=hours, actor=ident.agent_id)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def peer_reachable(seat_id: str) -> list[str]:
    """Every seat a search for `seat_id`'s own queue should also cover (task #76 item 5b,
    spec e6636c7e's "the pair faces the tree through both peers") — DISCOVERABILITY ONLY,
    per Thoth's ruling: mail delivery itself is untouched, this never widens who a DM
    reaches. Returns `[seat_id]` alone when unpeered/unknown, or `[seat_id, peer]` when an
    active peer_of bond exists. There is no `review` verb/object in this codebase today —
    item 5's own missing piece (5c) — so this is scoped for whatever future surface reads
    one seat's queue, not a review-assignment feature that doesn't exist yet."""
    pool = await _pool_get()
    from src.orchestrator.seats import peer_reachable as _peer_reachable
    return await _peer_reachable(pool, seat_id)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def peer_ledger(seat_a: str, seat_b: str) -> list[dict[str, Any]]:
    """The pair's shared reciprocity ledger (task #76 item 3, spec e6636c7e) — every OPEN
    thread owned by EITHER seat, oldest first, as one resumable list. Zero new storage:
    open_thread/resolve_thread stay the only write path, this only reads — what makes a
    parked pair resumable is exactly a Thread staying open on purpose. Doesn't require an
    active peer_of bond between the two named seats — a healed pair's own history stays
    readable."""
    pool = await _pool_get()
    from src.orchestrator.seats import peer_ledger as _peer_ledger
    return await _peer_ledger(pool, seat_a, seat_b)


@mcp.tool()
async def detach_seat(seat: str, because: str, ctx: Context | None = None) -> dict[str, Any]:
    """Invalidate an active managed_by edge — the toolkit hole named at thread fad0dc14
    (unpeer heals peer_of, nothing healed managed_by before this). A COORDINATOR IS DEFINED
    BY HAVING NO MANAGER (derive_role: 'worker' if a manager exists else 'coordinator'), so
    this REMOVES the edge, never repoints it — a fresh manager, if one is ever assigned, is
    a separate act.

    Refuses LOUDLY on: blank `because`; an unknown/inactive seat; or no active managed_by
    edge out of it (nothing to detach)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — detaching a seat from its manager is a deliberate "
                         "act on the record", "why": _anchorless(ctx)}
    from src.orchestrator.seats import detach_seat as _detach
    return await _detach(Actions(await _pool_get()), seat, because=because,
                         actor=ident.agent_id)


@mcp.tool()
async def attach_seat(
    worker: str, manager: str, evidence: str, ctx: Context | None = None,
) -> dict[str, Any]:
    """Create a managed_by edge — the mirror of detach_seat, and the other half of the
    toolkit hole named at thread fad0dc14. managed_by is created in exactly two places in
    the whole codebase (mint_seat's birth-time edge, fold_seat's re-point) — every seat
    that predates mint_seat, was adopted, or lost its edge to a detach nobody re-pointed
    has had no path back except raw SQL until this. Confirmed live: 30 active seats, 23
    with no managed_by edge at all — an absent edge raises no error, it just renders as an
    empty chart, which is why nobody noticed you could not attach even after #99 built the
    way to detach.

    Refuses LOUDLY on: blank `evidence`; either seat unknown/inactive; `worker == manager`;
    or an already-active managed_by edge out of `worker` — this is a CREATE, never a silent
    repoint (detach_seat first, then attach, if that's what's meant)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — attaching a seat to a manager is a deliberate "
                         "act on the record", "why": _anchorless(ctx)}
    from src.orchestrator.seats import attach_seat as _attach
    return await _attach(Actions(await _pool_get()), worker, manager, evidence=evidence,
                         actor=ident.agent_id)


@mcp.tool()
async def invalidate_works_in(stale_project: str, because: str,
                              ctx: Context | None = None) -> dict[str, Any]:
    """Drop ONE OF YOUR OWN duplicate works_in edges — for a live agent carrying two
    simultaneously-live works_in edges (a stale fork/rename side surviving beside the
    current one). orient() resolves through whichever edge wins, so a duplicate is not
    cosmetic — it can hide your own lineage's threads/decisions from you, live.
    SELF-SCOPED like correct_house, never operator-fenced: no `agent_id` parameter — the
    caller IS the target, never another agent's edge.

    Refuses LOUDLY on: blank `because`; a caller not mounted as an active Agent;
    `stale_project` resolving ambiguously (never guesses) or to no SoftwareProject at
    all; no active works_in edge from you to it; or `stale_project` naming your ONLY
    live works_in edge — dropping your last project is amputation, not cleanup; this
    verb is for duplicates only."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — invalidating a works_in edge is a deliberate "
                         "act on the record", "why": _anchorless(ctx)}
    pool = await _pool_get()
    from src.orchestrator.agents import invalidate_works_in as _invalidate_works_in
    result = await _invalidate_works_in(Actions(pool), ident.agent_id, stale_project,
                                        because=because, actor=ident.agent_id)
    if not result.get("error"):
        # THE STALE-BANNER TRAP, generalized (rebind_seat's own docstring names it
        # first): the DB write is real immediately, but a LIVE connection's cached
        # AgentIdentity does not follow it — the gap that made John's own fix appear
        # to take effect three steps late (thread 8640a625, decision 4001f6d1). Patch
        # every live cached identity in this agent's own lineage in place, so the
        # very next orient() on any of those connections sees the drop without a
        # reconnect. Two-step, mirroring mount()'s own precedence: a SEATED identity
        # re-derives from the seat's own house (the same call _resolve_project_seat_
        # first runs at mount time — a no-op for an unseated one, same as there);
        # only when that leaves the cache still pointing at the just-dropped project
        # AND exactly one candidate remains unambiguous does the remaining works_in
        # edge become the fallback — never guessed at when 2+ remain.
        base = _generation(ident.agent_id)[0]
        # canonicals are "repo:<name>"; AgentIdentity.project is always the bare name
        # (agents.py itself builds the canonical as f"repo:{identity.project}") — strip
        # the prefix before comparing against or assigning into a cached identity.
        dropped = result["was_working_in"].removeprefix("repo:")
        remaining = [p.removeprefix("repo:") for p in (result.get("still_working_in") or [])]
        for cached in _agents.values():
            if _generation(cached.agent_id)[0] != base:
                continue
            await _resolve_project_seat_first(pool, cached)
            if cached.project == dropped and len(remaining) == 1:
                cached.project = remaining[0]
    return result


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def correct_agent_house(agent_id: str, project: str | None = None,
                              seat_generation: int | None = None,
                              ctx: Context | None = None) -> dict[str, Any]:
    """Heal an ALREADY-POLLUTED agent's own project/seat_generation stamps — the
    data-repair half of mount-guard #6 (commit cb47d02): the code fix stops NEW
    pollution from a bare-office-root mount, it does not retroactively cure a stamp a
    transient bad mount already wrote. UNLIKE correct_house, NOT self-scoped — the
    target need not be the caller (an ancestor's already-corrupted stamp is exactly
    the case this exists for). Append-only: asserts a new current value, never
    touches the superseded row. Refuses on no correction named, an empty project,
    a non-positive generation, or an unknown/inactive agent."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a correction is a mind's act, and the graph "
                         "must know whose", "why": _anchorless(ctx)}
    from src.orchestrator.agents import correct_agent_house as _correct_agent_house
    return await _correct_agent_house(Actions(await _pool_get()), agent_id=agent_id,
                                      project=project, seat_generation=seat_generation,
                                      actor=ident.agent_id)


@mcp.tool()
async def retire_agent(agent_id: str, because: str, override_live: bool = False,
                       ctx: Context | None = None) -> dict[str, Any]:
    """Third-party retirement — complements self-scoped retire() (no target param).
    Stamps retired/retired_by/retired_because, flips objects.status. Not self-scoped or
    manager-gated — any caller may name any target; `actor` is attribution, not
    authority.

    ALWAYS releases the target's held seat and mount rows on success. Refuses LOUDLY on:
    blank `because`; an unknown/non-active agent; a target that reads LIVE (seen within
    15 min) unless `override_live=True`."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — retiring an agent is a deliberate act on the "
                         "record", "why": _anchorless(ctx)}
    from src.orchestrator.agents import retire_agent as _retire_agent
    return await _retire_agent(Actions(await _pool_get()), agent_id=agent_id,
                               actor=ident.agent_id, because=because,
                               override_live=override_live)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "backfill(target='agent_project_links')",
    "since": "task #199 lane 2, families wave (thread 6854)",
})
async def backfill_agent_project_links(
    actor: str, dry_run: bool = True, only_bases: list[str] | None = None,
) -> dict[str, Any]:
    """THE MISSING DOOR onto `backfill_agent_project_links` (thread 20af2c95): the write-
    side fix (mint_heir/fold_agent invalidating a predecessor's works_in/governs onto its
    heir) shipped 2026-08-04, but the one-time repair for edges already stranded on off-
    head generations before then had no reachable surface — importable only (382067d9).
    `dry_run=True` (default) plans only — which off-head agents would give up which edges,
    to which living head — no write. `dry_run=False` writes via the same
    `move_agent_project_links` the write-side fix already uses. `only_bases` scopes a
    write to specific lineages; omitted, every off-head agent in scope moves. Executing
    the write is the operator's own call, same class as #150's repairs.

    KEPT AT ITS ORIGINAL SIGNATURE (explicit `actor`, no ctx/because) rather than routed
    through the new dispatcher's shared body — the new `backfill(target='agent_project_
    links', ...)` derives actor from the caller's mounted identity instead, a deliberate
    behavior change not safe to impose on this deprecated name's existing callers."""
    from src.orchestrator.agents import backfill_agent_project_links as _backfill
    return await _backfill(Actions(await _pool_get()), actor=actor, dry_run=dry_run,
                           only_bases=set(only_bases) if only_bases else None)


@mcp.tool()
async def list_assertions(ref: str, name: str) -> dict[str, Any]:
    """READ-ONLY. THE DOOR retire_assertion's own `superseded_id` NEEDS AND NOTHING ELSE
    EXPOSED (382067d9, the fifth-ledger-disease specimen — a verb with a surface whose
    required argument nothing could obtain): every CURRENT assertion of `name` on the
    object `ref` resolves to, each carrying its own row `id` — the exact value
    retire_assertion's `superseded_id` wants. dossier()/trace_evidence() both resolve
    through the belief-winner or a flat value list; neither ever surfaced this id. No
    write, no ranking beyond confidence/recency, no bulk scope — the smallest surface
    that unblocks a targeted, per-row retire_assertion call."""
    from src.orchestrator.retirement import list_assertions as _list_assertions
    return await _list_assertions(Actions(await _pool_get()), ref=ref, name=name)


async def _abstained_derivations_impl(
    scope: str, link_type: str | None, limit: int,
) -> dict[str, Any]:
    """Shared body (task #199 lane 2, families wave, thread 6854): all three READ-ONLY
    views below query the SAME `derivation_abstained_<link_type>` population in
    capture.py, differing only in which structural SQL subset they filter to — a
    genuine shared call, not a cosmetic dispatch. `scope` picks the population:
    "all" (every live abstention, capture.abstained_derivations), "retryable" (the
    zero-candidate subset, safe to re-attempt as time passes), "retryable_ambiguous"
    (the 2+-candidate subset reduced by elimination alone to exactly one survivor)."""
    pool = await _pool_get()
    if scope == "retryable":
        return await capture.retryable_abstentions(pool, link_type, limit=limit)
    if scope == "retryable_ambiguous":
        return await capture.retryable_ambiguous_abstentions(pool, link_type, limit=limit)
    return await capture.abstained_derivations(pool, link_type, limit=limit)


@mcp.tool()
async def abstained_derivations(
    link_type: str | None = None, limit: int = 100, scope: str = "all",
) -> dict[str, Any]:
    """READ-ONLY. Every `derive_or_abstain` refusal — `from_id` and every candidate
    resolved to (type, summary), never bare uuids. `link_type=None` pools every lane; a
    value scopes to that one namespaced property. `count` is the true total; `sample` is
    bounded by `limit`, newest-abstained first.

    `scope` narrows WHICH abstentions, structurally: "all" (default) | "retryable" (the
    zero-candidate subset, safe to retry as time passes) | "retryable_ambiguous" (the
    2+-candidate subset reduced by elimination alone to exactly one survivor — adds
    `surviving_candidate`/`original_candidate_count` per row, `eliminated_to_zero` for
    the zero-survivor population). Neither retryable scope re-attempts anything — see
    retry_ambiguous_abstentions for the write half both retryable scopes name a
    target for."""
    return await _abstained_derivations_impl(scope, link_type, limit)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "abstained_derivations(scope='retryable')",
    "since": "task #199 lane 2, families wave (thread 6854)",
})
async def retryable_abstentions(link_type: str | None = None, limit: int = 100) -> dict[str, Any]:
    """READ-ONLY. The zero-candidate SUBSET of abstained_derivations, structurally —
    filtered in SQL, not by a condition a caller could widen. A zero-candidate abstention
    means the lookup found nothing YET, which time can change; a 2+-candidate one is a
    genuine ambiguity time cannot resolve, and never appears here. Oldest-abstained first
    — names which objects are safe to re-attempt; does not re-attempt them. Re-run your
    own lane's lookup on each and call derive_or_abstain(..., retried=True) yourself."""
    return await _abstained_derivations_impl("retryable", link_type, limit)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "abstained_derivations(scope='retryable_ambiguous')",
    "since": "task #199 lane 2, families wave (thread 6854)",
})
async def retryable_ambiguous_abstentions(
    link_type: str | None = None, limit: int = 100,
) -> dict[str, Any]:
    """READ-ONLY. The sibling door retryable_abstentions doesn't cover: every LIVE 2+-
    candidate abstention whose ORIGINAL candidate set has, by elimination alone (a merge,
    a retire, an invalidation — never a fresh re-derivation), shrunk to exactly one
    `status='active'` survivor. Structurally safe the same way retryable_abstentions is —
    only the stored candidate ids' current status is rechecked, nothing is re-derived, so
    a0339e16 is never relaxed. `eliminated_to_zero` (alongside `count`) names the DIFFERENT
    population whose every candidate is now gone — real, but nothing to retry-mint from,
    never folded into `count`/`sample`. Oldest-abstained first; names what's safe to
    retry, never retries it. See retry_ambiguous_abstentions for the write half."""
    return await _abstained_derivations_impl("retryable_ambiguous", link_type, limit)


@mcp.tool()
async def retry_ambiguous_abstentions(
    dry_run: bool = True, because: str | None = None, link_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Mints the one surviving candidate for every row retryable_ambiguous_abstentions
    names, via derive_or_abstain(retried=True) — lane-agnostic (no lane-specific lookup
    re-run, only the stored candidate ids' own current status), so this one verb covers
    every lane's ambiguous abstentions, present or future, not just in_repo's own.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a backfill is a mind's act, and the graph must "
                         "know whose", "why": _anchorless(ctx)}
    return await capture.retry_ambiguous_abstentions(
        Actions(await _pool_get()), actor=ident.agent_id, dry_run=dry_run, because=because,
        link_type=link_type)


@mcp.tool()
async def stale_current_flags(limit: int = 50) -> dict[str, Any]:
    """THE READ DOOR (thread 09bde57e): every assertion row where `is_current=true`
    (migration 0047's maintained flag) YET a real `supersedes` FK already points at it from
    another assertion — a stale flag current_assertions is still trusting. This is a kernel-
    integrity read, not a per-object lookup like list_assertions: `count` is the TRUE total
    population (never capped); `sample` is bounded by `limit`, oldest-observed first. Pure
    read — finds the anomaly, fixes nothing; see obligation 09bde57e for the backfill this
    surfaces the need for."""
    from src.orchestrator.retirement import stale_current_flags as _stale_current_flags
    return await _stale_current_flags(Actions(await _pool_get()), limit=limit)


@mcp.tool()
async def repair_stale_current_flags(
    dry_run: bool = True, limit: int = 500, ctx: Context | None = None,
) -> dict[str, Any]:
    """THE BACKFILL for stale_current_flags' own population (thread 09bde57e). `dry_run=True`
    (default): list-only, names how many rows WOULD flip and their ids, writes nothing —
    safe to call unmounted-curious. `dry_run=False` is the operator's own call, never
    automatic: flips `is_current=false` on up to `limit` stale rows in one batched UPDATE,
    oldest-observed first. Batched because the live population is five figures (123,914 at
    last count, d8225e71) — walk it in repeated calls, not one UPDATE touching all of it.
    Idempotent: a row already flipped drops out on its own, so re-running after a partial
    run or a failure is always safe."""
    if not dry_run:
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a write to the kernel's own materialization is "
                             "a mind's act, and the graph must know whose", "why": _anchorless(ctx)}
        actor = ident.agent_id
    else:
        actor = None
    from src.orchestrator.retirement import repair_stale_current_flags as _repair
    return await _repair(Actions(await _pool_get()), dry_run=dry_run, limit=limit, actor=actor)


@mcp.tool()
async def retire_assertion(ref: str, name: str, superseded_id: int, value: str, because: str,
                           ctx: Context | None = None) -> dict[str, Any]:
    """THE CROSS-SOURCE SUPERSEDE — retires another source's assertion explicitly, the
    class assert_property's own automatic (same-source-only) supersession cannot reach:
    a peer's correction of another agent's bad self-declaration. Without this, a
    different-source correction and the wrong original both stay "current" at once.

    Deliberately narrow — retires ONE named assertion by id, never "whatever's current
    now"; the caller must already know exactly which row is wrong. `because` is
    required. Refuses loudly: `ref` unresolved; `superseded_id` not a `name` assertion
    on that object; already superseded; blank `because`."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a retirement is a mind's act, and the graph must "
                         "know whose", "why": _anchorless(ctx)}
    from src.orchestrator.retirement import retire_assertion as _retire_assertion
    return await _retire_assertion(Actions(await _pool_get()), ref=ref, name=name,
                                   superseded_id=superseded_id, value=value, because=because,
                                   actor=ident.agent_id)


@mcp.tool()
async def set_seat_attended(seat_id: str, attended: str, because: str,
                            ctx: Context | None = None) -> dict[str, Any]:
    """THE HUMAN-ATTENDED GUARD'S REAL SIGNAL (thread 96f62338) — stamps a seat's own explicit
    `attended` property ('human' or 'worker'), read directly by dispatch_dm's human-attended
    guard instead of its old, broken `managed_by` proxy (true only while Thoth was the sole
    manager; false since workers started minting their own sub-workers and test seats).

    OPERATOR-APPROVED TO CHANGE, ENFORCED: the operator or the target seat's own manager
    only. `attended='human'` marks a seat the operator actually fronts; `attended='worker'`
    reverses a prior stamp. Refuses loudly on a value outside {'human','worker'}, a blank
    `because`, an unauthorized actor, or an unknown/retired seat."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a seat's attendance signal is a mind's act, and the "
                         "graph must know whose", "why": _anchorless(ctx)}
    from src.orchestrator.seats import set_seat_attended as _set_seat_attended
    return await _set_seat_attended(Actions(await _pool_get()), seat_id=seat_id,
                                    attended=attended, because=because, actor=ident.agent_id)


@mcp.tool()
async def rename_seat(seat_id: str, new_handle: str, because: str,
                      ctx: Context | None = None) -> dict[str, Any]:
    """Rename a Seat — manager/operator-invoked, ENFORCED, no self-service (claim_name is
    for a mind naming ITSELF). Stamps the seat's own `handle` and, if the seat is occupied,
    the current holder's `handle` too — both compensating assertions, the old handle stays
    in history. The harness-session display name is OUT of scope; the receipt says the
    graph renamed and the harness name follows at the holder's next spawn. Refuses loudly
    on a blank/over-long `new_handle`, a blank `because`, an unauthorized actor, an unknown
    seat, or a `new_handle` another active seat already carries (case-insensitive)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a rename is a mind's act, and the graph must know "
                         "whose", "why": _anchorless(ctx)}
    from src.orchestrator.seats import rename_seat as _rename_seat
    return await _rename_seat(Actions(await _pool_get()), seat_id=seat_id,
                              new_handle=new_handle, because=because, actor=ident.agent_id)


@mcp.tool()
async def bind_seat_tree(seat_id: str, tree_cwd: str, because: str,
                         ctx: Context | None = None) -> dict[str, Any]:
    """Point a seat's CODE checkout at `tree_cwd` — distinct from its office (identity home,
    untouched here). `launch_seat` reuses whatever is recorded until this is called again;
    osiris never provisions the directory — `launch_seat` checks it exists on disk before
    trusting it, this only records the location. OPERATOR-OR-MANAGER ONLY, ENFORCED — this is
    what a relaunched seat trusts as the code it executes. Refuses on a blank
    `tree_cwd`/`because`, an unauthorized actor, or an unknown seat."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a tree binding is a mind's act, and the graph must "
                         "know whose", "why": _anchorless(ctx)}
    from src.orchestrator.seats import bind_seat_tree as _bind_seat_tree
    return await _bind_seat_tree(Actions(await _pool_get()), seat_id=seat_id,
                                 tree_cwd=tree_cwd, because=because, actor=ident.agent_id)


@mcp.tool()
async def reissue_office(
    seat_id: str, because: str, adopt: bool = False, ctx: Context | None = None,
) -> dict[str, Any]:
    """Recompile a seat's CLAUDE.md managed section on demand, when law changes or a
    live fact (a peer bond, a manager reassignment) needs to reach an already-occupied
    office. Only the bytes between the `<!-- osiris:compiled:begin/end -->` markers are
    touched — hand-composed narrative and charter.md always survive. `because` required.

    Refuses loudly, naming the seat, when the managed section is missing, duplicated, or
    mangled — fix by hand, or pass `adopt=True` for a genuinely pre-compiler office
    (zero marker text); `adopt=True` on an office that already has marker text also
    refuses."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a reissue is a mind's act, and the graph must "
                         "know whose", "why": _anchorless(ctx)}
    from src.orchestrator.boot_compiler import reissue_office as _reissue_office
    return await _reissue_office(Actions(await _pool_get()), seat_id=seat_id,
                                 because=because, actor=ident.agent_id, adopt=adopt)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def file_subagent(subagent_id: str, ctx: Context | None = None) -> dict[str, Any]:
    """File ONE ephemeral subagent under its spawner (ruling 0f76458c — a hand is never a
    first-class fleet member). Attributes it to its spawner (an existing spawned_by edge, or
    its `session` property's root agent when neither exists — refuses loudly if neither
    resolves), stamps its X.n patronym name if it doesn't already carry one, and flips its
    status to 'historical' when the EXACT parent generation that spawned it is no longer
    live — a parent-live hand is filed but never status-flipped. For filing more than one at
    once, use file_subagents (the dry-run-first sweep) instead — it computes correct
    per-parent naming ordinals that a bare loop over this tool would collide on."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — filing a hand is a mind's act, and the graph must "
                         "know whose", "why": _anchorless(ctx)}
    from src.orchestrator.lineage import file_subagent as _file_subagent
    return await _file_subagent(Actions(await _pool_get()), subagent_id=subagent_id,
                                actor=ident.agent_id)


@mcp.tool()
async def file_subagents(project: str | None = None, dry_run: bool = True,
                         ctx: Context | None = None) -> dict[str, Any]:
    """THE SWEEP (ruling 0f76458c's testbed clause): runs file_subagent's resolver over every
    active 17-hex subagent Agent object in scope. `project=` narrows it (e.g. 'hector-vector'
    for the testbed); omitted is fleet-wide. DRY-RUN (the default) writes nothing and returns
    per-class counts — attributable_parent_dead / attributable_parent_live / unattributable —
    plus a bounded sample, so a manager can see a scope's shape before committing to it. THE
    TESTBED SEQUENCE (the operator's word): dry-run hector-vector first, receipts to the
    manager, live only at their word, THEN a fleet-wide dry-run — never the reverse. Pass
    dry_run=False only once the dry-run's shape has been reviewed."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — filing hands is a mind's act, and the graph must "
                         "know whose", "why": _anchorless(ctx)}
    from src.orchestrator.lineage import file_subagents as _file_subagents
    return await _file_subagents(Actions(await _pool_get()), project=project,
                                 dry_run=dry_run, actor=ident.agent_id)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def unwitnessed_spawns(agent_id: str | None = None,
                             ctx: Context | None = None) -> dict[str, Any]:
    """THE SELF-AUDIT (obligation cabfb4b2, Ptah VII's rotten-apple report): every LIVE
    `spawned_by` child of `agent_id` for which NO `subagents/agent-<id>.jsonl` file has EVER
    materialized anywhere on disk — "what is executing under my identity right now that I
    did not spawn." Omit `agent_id` to audit YOUR OWN identity (a seat's own check); name
    another to audit theirs (the operator's own check, or a peer's — a pure read, never
    gated the way a write would be).

    A HIT IS A LEAD, NOT A VERDICT — Ptah's own specimen retracted once already (msg 4993):
    a subagent Ra spawned and briefed "you are Ptah" was correctly parented to Ra, not a
    graph defect. Checked one live hypothesis (Thoth, msg 5008) against Ptah's real
    transcript before shipping this docstring: whether a sidechain turn could be recorded
    INLINE in the parent's own transcript (isSidechain=true) rather than as a separate
    subagents/ file, which would make a hit here a false alarm. Zero `isSidechain:true`
    lines exist anywhere in Ptah's own transcript — that specific escape hatch does not
    explain his specimens, but a caller should not assume it can never apply elsewhere
    without checking the same way. This tool reads; it never files or folds anything found
    here — see obligation cabfb4b2 for the fix shape still pending live evidence."""
    from pathlib import Path

    from src.orchestrator.lineage import unwitnessed_spawns as _unwitnessed
    target = agent_id
    if target is None:
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first, or name an agent_id to audit someone else's",
                    "why": _anchorless(ctx)}
        target = ident.agent_id
    st = get_settings()
    root = Path(st.osiris_sense_sessions) if st.osiris_sense_sessions \
        else Path.home() / ".claude" / "projects"
    hits = await _unwitnessed(Actions(await _pool_get()), target, root=root)
    return {"agent_id": target, "unwitnessed": hits, "count": len(hits)}


@mcp.tool()
async def fold_candidates(ctx: Context | None = None) -> dict[str, Any]:
    """THE ARCHAEOLOGIST'S TRAY (thread b975851b) — sweep the registry and disk for
    anonymous agents that evidence says were never distinct minds (view-aliases: a mount
    row with no transcript and no daemon receipt, co-resident with a session that has a
    body; restart-mints: an anonymous mount in a named lineage's own home) and queue them
    as review-gated merge candidates. PROPOSALS ONLY — nothing folds. Returns the pending
    tray (score-ranked, each with its cited signals); judge each with resolve_fold.
    Rejected pairs are remembered and never re-proposed. Also carries `unresumed_heads`
    (ef88e2bb) — a SEPARATE non-fold class, never resolve_fold'd — a human call each time."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first", "why": _anchorless(ctx)}
    from src.orchestrator.folds import find_agent_fold_candidates
    return await find_agent_fold_candidates(await _pool_get())


@mcp.tool()
async def resolve_fold(candidate_id: int, decision: str,
                       ctx: Context | None = None) -> dict[str, Any]:
    """Judge ONE agent-fold proposal from the tray (fold_candidates): decision='merged'
    executes the ESTATE-carrying fold (mail, mount rows, threads land on the living
    head) — OPERATOR-GATED, ENFORCED: inherits fold_agent's own operator-actor gate
    unchanged, never a second copy to drift. decision='rejected' links the pair
    not_same_as, never re-proposed — OPEN to any mounted caller, deliberately: a
    rejection judges two things are NOT the same mind, carrying none of 'merged's blast
    radius."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first", "why": _anchorless(ctx)}
    from src.orchestrator.folds import resolve_fold_candidate
    return await resolve_fold_candidate(Actions(await _pool_get()),
                                        candidate_id=candidate_id, decision=decision,
                                        actor=ident.agent_id)


@mcp.tool()
async def fleet_reconcile(execute: bool = False,
                          ctx: Context | None = None) -> dict[str, Any]:
    """THE REAPER — buckets stale/anonymous agent mounts into bulk_fold_swarm,
    rollup_office_remount, drop_ephemeral_test_cwd, and leave_for_human (never touched,
    by construction), acting on the first three only with `execute=True`. Dry run is the
    default: returns the plan, writes nothing. With `execute=True`, re-reads the tray
    fresh immediately before acting and returns before/after counts as proof.

    Fold buckets are operator-gated, enforced one call down (fold_agent) — a
    non-operator's fold items come back per-item errored while drop_ephemeral_test_cwd
    still runs. Reachable independently of `osiris_fleet_reconcile_enabled`, which gates
    only the separate scheduled tick, never this tool."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first", "why": _anchorless(ctx)}
    from src.orchestrator.fleet_reconcile import reconcile_execute
    return await reconcile_execute(Actions(await _pool_get()), actor=ident.agent_id,
                                   execute=execute)


@mcp.tool()
async def establish_office(seat: str, ctx: Context | None = None) -> dict[str, Any]:
    """THE OFFICE CEREMONY (ruling ed5f5ce2) — one act moves a seat into its Osiris-owned
    home at ~/.osiris/seats/<handle>/: writes the seat's STANDING ORDERS (a per-seat
    CLAUDE.md boot sector — identity, house, charter, the office model; never clobbers an
    existing one), then rebind-extracts the seat there (.osiris pin, mount rows, its own
    lineage's transcripts re-addressed so resume works in place — co-residents' history
    stays). `seat` accepts a claimed name or a raw agent id. Refuses loudly on an unknown
    seat and on an anonymous lineage (an office is named for its seat — claim_name first).
    Idempotent: re-running converges on the same office. The receipt carries the launch
    line to hand the operator."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — an office ceremony is a mind's act, and the graph "
                         "must know whose", "why": _anchorless(ctx)}
    from src.orchestrator.offices import establish_office as _establish
    return await _establish(Actions(await _pool_get()), seat_or_agent=seat,
                            actor=ident.agent_id)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def lift(ref: str, handle: str, subagent_id: str | None = None,
               subagent_type: str | None = None, session_anchor: str | None = None,
               ctx: Context | None = None) -> dict[str, Any]:
    """Pull a NAMED, QUIET rogue out of its ad hoc cwd and into a clean osiris office — the
    P2V move: import a running-but-unmanaged instance, preserve its state,
    give it a clean managed identity. Composes `identify_agent(ref)` to resolve the target
    (refuses on 0 matches, on >1 — an ambiguous multi-tenant cwd, name a specific `agent:` id
    instead — and on a LIVE match: moving a live seat splits its running session's history
    between two homes, close its tab first), `claim_name(handle)` (propagating its own real
    refusals: a visitor, a name held live elsewhere, a cross-house collision), and
    `establish_office` (the actual move). `ref` accepts anything `identify_agent()` does — an
    `agent:` id, a `seat:` id, a bare handle, or an absolute cwd path. The receipt's `verified`
    field is a FRESH post-write `identify_agent()` read, never an echo of what the earlier
    steps each individually claimed.

    SELF-LIFT IS STRUCTURALLY IMPOSSIBLE, not just refused: your own session's `last_seen` is
    kept perpetually fresh by your own terminal's statusline heartbeat, so you can never
    observe yourself as quiet from inside a call — `lift()` always targets a DIFFERENT,
    already-quiet session, never the caller's own."""
    ident = await _ident_for(ctx, session_anchor)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first — a lift is a mind's act, "
                         "and the graph must know whose", "why": _anchorless(ctx)}
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    from src.orchestrator.lift import lift as _lift
    return await _lift(await _pool_get(), ref, handle, actor=actor)


@mcp.tool()
async def walk_in(
    handle: str, wants_office: bool, cwd: str | None = None, job_dir: str | None = None,
    model: str | None = None, subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """One call over mount + claim_name + establish_office, for a mind with nothing but
    this server. Composes each step untouched, returns its receipt verbatim, stops at
    the first refusal. Skips an already-done step honestly (`ran: false` + why).

    `handle` and `wants_office` are both required, never defaulted — nobody but the
    caller can pick a name, and forcing an office onto a one-off visitor would erase that
    class's whole point. `cwd`/`job_dir` only consulted if not yet mounted; project/house
    come free off `cwd`. Full rationale: src/orchestrator/walkin.py's own docstring."""
    pool = await _pool_get()
    ident = await _ident_for(ctx, session_anchor)
    if ident is None:
        if not cwd:
            return {"error": "not yet mounted, and no cwd given — pass cwd (your working "
                             "directory) so walk_in can mount you first, or call mount() "
                             "yourself before walk_in"}
        mount_result = await mount(
            cwd=cwd, job_dir=job_dir, model=model, subagent_id=subagent_id,
            subagent_type=subagent_type, session_anchor=session_anchor, ctx=ctx)
        if "error" in mount_result:
            return {"error": mount_result["error"], "step": "mount"}
        agent_id = mount_result.get("agent")
        if not agent_id:
            return {"error": "mount succeeded but returned no agent id — cannot continue",
                    "step": "mount", "mount_result": mount_result}
        mount_step: dict[str, Any] = {"ran": True, "result": mount_result}
    else:
        agent_id = ident.agent_id
        mount_step = {"ran": False, "note": f"already mounted as {agent_id}, skipping"}

    from src.orchestrator.walkin import walk_in_named
    result = await walk_in_named(
        pool, agent_id=agent_id, handle=handle, wants_office=wants_office)
    if "error" in result:
        result.setdefault("steps_so_far", {})["mount"] = mount_step
        return result
    return {**result, "mount": mount_step}


@mcp.tool()
async def mint_seat(
    handle: str, project: str | None = None, model: str | None = None,
    house: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Mint a specialist WORKER seat under YOUR OWN seat, one act: ensure_seat + an
    office scaffold (dir, .osiris pin, CLAUDE.md + charter.md) + an intended_model stamp
    + managed_by (you become manager of record). The calling seat is always the manager
    — no override param, a seat mints its own workers, never another's. Idempotent: a
    handle already naming a living Seat is adopted (missing edge/stamp asserted, nothing
    rewritten) rather than twinned. `house` omitted inherits your own; crossing houses
    refuses unless the caller is the operator. Refuses loudly if you hold no seat of your
    own (claim_name first)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — minting a worker is a seat's own act",
                         "why": _anchorless(ctx)}
    from src.orchestrator.seats import held_seat
    pool = await _pool_get()
    bound = await held_seat(pool, ident.agent_id)
    manager_seat_id = bound["seat_id"] if bound else None
    if manager_seat_id is None:
        # THE SUCCESSION GAP (live acceptance, msg 926 — Thoth LI's own first call): held_seat
        # needs a `holds` link on the caller's EXACT label, but a succeeded lineage's holds
        # link can sit on an ancestor label (mint_heir doesn't always re-link it at every
        # mint — a separate, deeper gap, banked as its own thread rather than fixed here:
        # whether succession should carry `holds` forward the way it already carries
        # `handle`). The HANDLE ASSERTION, unlike the link, IS copied to every new
        # generation (agents.mint_heir's seat-inheritance step) — fall back to it, the same
        # way mount's own seat display (handshake._seat_of) already resolves.
        from src.orchestrator.mintseat import _resolve_seat_ref
        from src.orchestrator.offices import _handle_of
        handle_claim = await _handle_of(pool, ident.agent_id)
        if handle_claim:
            manager_seat_id = await _resolve_seat_ref(pool, handle_claim)
    if manager_seat_id is None:
        return {"error": "you hold no seat of your own — claim_name first; a seat mints "
                         "workers under ITSELF, and an unclaimed lineage has no seat to "
                         "extend"}
    from src.orchestrator.mintseat import mint_seat as _mint_seat
    kwargs: dict[str, Any] = {"intended_model": model} if model else {}
    return await _mint_seat(Actions(pool), manager=manager_seat_id, handle=handle,
                            house=house, project=project, actor=ident.agent_id, **kwargs)


@mcp.tool()
async def bootstrap(cwd: str, ctx: Context | None = None) -> dict[str, Any]:
    """Onboard a project by migrating its markdown MEMORY (CLAUDE.md build log / DESIGN.md /
    memory essays) INTO the shared graph as retrieval-sized Reference nodes — so its history
    becomes a bounded query (consult_canon) instead of bloat re-injected into every context.
    Registers the project and returns a suggested boot-sector CLAUDE.md. Osiris does NOT touch
    your files (no hands): review the suggestion, write it yourself, archive the originals.
    Public docs (README/ARCHITECTURE) are left alone — they're human-facing exports, not memory.
    Every write is stamped with your mounted identity (or "session"), never a fixed literal."""
    from src.orchestrator.bootstrap import bootstrap_project

    source = await _source_for(ctx)
    return await bootstrap_project(Actions(await _pool_get()), cwd, source=source)


# --- write-back: the prosthesis (capture what you decided / what's still open) ---

# THE FAIL-OPEN PROMISE, ENFORCED (task #149, Imhotep's 300s record_decision timeouts,
# thread 9f08b027): record_decision's and record_practice's own prior-art search has
# always been documented "fail-open: a search hiccup must never block recording the
# decision itself" — but the try/except around it only ever caught a RAISED exception,
# never a HANG, so the promise was true for errors and false for silence. semantics.py's
# own fix (Model2VecEmbedder's bounded, sticky load) closes the specific hang that was
# actually measured live; this is the outer, whole-call bound as defense in depth — any
# OTHER slow step in the fused search pipeline (DB contention under fleet load, a lexical
# door with no supporting index) gets the same honest, fast fail-open instead of riding
# out an external 300s timeout with no diagnosis.
_PRIOR_ART_SEARCH_TIMEOUT_S = 15.0


async def _surface_prior_art(
    pool: asyncpg.Pool, text: str, *, exclude: set[uuid.UUID] | None = None,
    repo: str | None = None, actor: str | None = None,
) -> list[dict[str, Any]]:
    """THE READ-SIDE HOP (obligation a6198075, operator's own critique: "why does 'read
    the graph before rederiving' have to be a mail instruction, why is that not
    architecture?"). record_decision/record_practice already run this exact search at
    WRITE time (thread 44635c42/ruling 1e6d7367) — extracted here, unchanged, so a
    caller that isn't a write (send(), currently) can run the SAME search rather than a
    second matcher. Same 15s timeout + fail-open (a search hiccup or hang returns []
    rather than blocking the caller) as both write-time callers. Same Thread-kind
    widening: a Thread hit only counts as prior art when it's an OPEN kind='obligation'
    row, or a kindless legacy row sharing this call's own `repo` (capture.
    _open_obligation_thread_ids) — never a resolved thread (nothing to warn against
    re-doing) and never a kindless row admitted with no repo at all."""
    try:
        search_out = await asyncio.wait_for(comp.run_spec(
            pool, {"op": "function", "name": "search",
                   "args": {"q": text[:300], "limit": 15, "caller": actor}},
            None, name="search", caller=actor), timeout=_PRIOR_ART_SEARCH_TIMEOUT_S)
        hits = search_out["items"]["hits"]
        thread_hit_ids = [uuid.UUID(h["id"]) for h in hits if h.get("type") == "Thread"]
        if thread_hit_ids:
            keep = await capture._open_obligation_thread_ids(pool, thread_hit_ids, repo=repo)
            hits = [h for h in hits
                    if h.get("type") != "Thread" or uuid.UUID(h["id"]) in keep]
        return capture.prior_art_from_hits(
            hits, exclude=exclude or set(), kinds=capture.UNIFIED_PRIOR_ART_KINDS)
    except Exception:  # noqa: BLE001 — never block the caller on a search-side failure/hang
        return []


# THE HATCH'S TWO POPULATIONS MUST STAY SEPARABLE (Thoth's condition 2, msg 5802/5811):
# a Decision whose ONLY requested connectivity is an extension-link param (obsoletes=/
# confirms=/refutes=/implements=/rediscovers=/bears_on=, which mint AFTER capture.
# record_decision's own atomic block — see decision 7ea187b9) must not fall through
# unlinked_because's HATCH indistinguishably from a genuinely standalone, disconnected
# write. Never typed by a caller — this string is the machine's own signature on it.
_EXTENSION_LINK_PENDING_REASON = (
    "extension-link-pending (task #189 condition 2, decision 7ea187b9) — machine-set: "
    "this write's only requested connectivity is obsoletes=/confirms=/refutes=/"
    "implements=/rediscovers=/bears_on=/narrows=/cites=, which mint after this transaction "
    "and cannot satisfy the gate at its own commit point")


# WRITE-VERB RECEIPT DIET (msg 6871, operator's context-bloat priority, 2026-09-04): a nag
# is advice a caller mostly doesn't act on the same turn — the old shape paid its full
# prose on EVERY firing. Collapsed to a short code in the receipt's own `nags` list;
# describe('nags') (or describe('nags:<code>')) is the one place the full text lives now,
# a deliberate lookup rather than a reflexive re-explain each call. Same convention
# consult_canon('record_decision') already uses for per-parameter detail — this is that
# same move applied to advisory nags specifically.
_NAG_CATALOG: dict[str, str] = {
    "protocol": (
        "this decision reads like a MEASUREMENT and its `protocol` is empty — record "
        "the exact invocation (command line, seeds, thresholds, bucket edges) so a "
        "successor RERUNS instead of re-deriving; re-run record_decision with the same "
        "summary + protocol to enrich this same decision (idempotent)"),
    "assertion": (
        "this reads like a flat claim about code/system behavior with no hedge "
        "acknowledging it might be wrong — if you re-read the thing you're "
        "describing THIS turn, say so; a citation alone doesn't clear this (it still "
        "fires on a cited claim if the citation itself wasn't re-checked for what it "
        "actually proves)"),
}


def _slim_prior_art(prior: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One-line id+short-summary, not the full {id,type,summary,grade,via} shape —
    write-verb receipt diet (msg 6871): the caller acting THIS turn needs enough to
    recognize the hit and go read it, not the ranking metadata that shaped the search."""
    return [{"id": p["id"], "type": p.get("type"), "summary": p.get("summary", "")}
            for p in prior]


@mcp.tool()
async def record_decision(
    summary: str, kind: str = "ruling", rationale: str | None = None,
    repo: str | None = None, grounds: list[str] | None = None,
    protocol: str | None = None, supersedes: str | None = None,
    resolves: str | list[str] | None = None,
    obsoletes: list[str] | None = None,
    confirms: list[str] | None = None, refutes: str | None = None,
    implements: str | None = None, rediscovers: list[str] | None = None,
    bears_on: list[str] | None = None, narrows: list[str] | None = None,
    cites: list[str] | None = None,
    ack_prior_art: bool = False,
    unlinked_because: str | None = None,
    subagent_id: str | None = None,
    subagent_type: str | None = None, session_anchor: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Write back a decision (ruling|reset|override|rejection|choice) so its WHY persists.
    `rationale`=reasoning, `repo`=project, `grounds`=refs it rests on, `protocol`=exact
    invocation to rerun.

    Graph-editing params: UUID/canonical/short-id only, never free text. List forms
    resolve each ref independently (a miss is reported, not fatal); a single string
    target errors the whole call if unmatched.
      supersedes   bury an earlier decision under this one
      resolves     close the Thread(s) this settles
      obsoletes    kill a named Superstition (quote the words agents inherit)
      confirms     witness a Practice
      refutes      disprove a Practice
      implements   execute a standing Decision (parent stays alive)
      rediscovers  independent re-arrival at an earlier decision
      narrows      scope-bound an earlier decision
      cites        add a facet to an earlier decision
      bears_on     speak to an open Thread without closing it
    `ack_prior_art=True` records a dismissed prior_art_flag instead of a silent shrug.
    `unlinked_because` supplies a real reason through declare-or-refuse's link-kind gate.
    consult_canon('record_decision') for more.

    `content_landed`: present when `rationale`/`protocol` was passed — a read-back
    confirming your text is the CURRENT value (a later write can silently win the
    tie-break). `false` → see `content_landed_note` and amend_decision.

    Any error on this call, including a dropped connection or a timeout with no
    response, is SAFE TO RETRY with the same `summary` — idempotent (an exact rewrite,
    or with `repo` given a near-duplicate reword, reuses the same decision;
    `reused_existing_decision` in the receipt names when that happened). Retrying
    never mints a twin."""
    pool = await _pool_get()
    gids: list[uuid.UUID] = []
    grounded: list[dict[str, str]] = []
    missing: list[str] = []
    for g in grounds or []:
        rid = await _resolve(pool, g)
        if rid is not None:
            gids.append(rid)
            grounded.append({"ref": g, "id": str(rid)[:8]})
        else:
            missing.append(g)
    old: uuid.UUID | None = None
    # require_identifier=True (task #117: an identifier-shaped arg like a bare local task
    # number must REFUSE fleet-wide rather than fall through to a prose/summary-substring
    # search — the same law resolves='s own fix already applied; supersedes/implements/
    # refutes/confirms BURY, CONVERT, or LINK the record they name, never a merely-read
    # act, so they carry the identical addressing-act risk resolves= was fixed for).
    if supersedes:  # resolve BEFORE recording — a correction that can't name its target
        old = await capture._find_decision(pool, supersedes, require_identifier=True)
        if old is None:
            return {"error": f"supersedes matched no decision: {supersedes!r} — quote its "
                             "UUID, canonical, or 8-char short id (no longer a prose "
                             "match — an addressing act refuses rather than guesses)"}
    impl_id: uuid.UUID | None = None
    if implements:  # same resolve-before-record strictness as supersedes
        impl_id = await capture._find_decision(pool, implements, require_identifier=True)
        if impl_id is None:
            return {"error": f"implements matched no decision: {implements!r} — quote its "
                             "UUID, canonical, or 8-char short id (no longer a prose "
                             "match — an addressing act refuses rather than guesses)"}
    refute_id: uuid.UUID | None = None
    if refutes:  # same strictness — a refutation that can't name its target has refuted nothing
        refute_id = await capture._find_practice(pool, refutes, require_identifier=True)
        if refute_id is None:
            return {"error": f"refutes matched no practice: {refutes!r} — quote its UUID, "
                             "canonical, or 8-char short id (no longer a prose match — "
                             "an addressing act refuses rather than guesses)"}
    # resolve BEFORE recording, same discipline as supersedes — a single string keeps the
    # original all-or-nothing strictness; a list resolves each entry independently and
    # reports (never raises) on a miss, so one typo can't veto the rest of the set.
    # require_identifier=True (msg 2426): resolves is a CLOSING act, so a bare prose ref
    # refuses here rather than falling through to a fuzzy summary-substring match.
    answered: list[uuid.UUID] = []
    receipt: list[dict[str, str]] = []
    single_summary: str | None = None
    if isinstance(resolves, list):
        for ref in resolves:
            tid = await capture._find_thread(pool, ref, require_identifier=True)
            if tid is None:
                receipt.append({"ref": ref, "matched": "false",
                                "note": "matched no thread — quote its UUID, canonical, "
                                        "or 8-char short id (no longer a prose match)"})
                continue
            answered.append(tid)
            summ = await capture._thread_summary(pool, tid)
            receipt.append({"ref": ref, "matched": "true", "id": str(tid)[:8],
                            "summary": summ or ""})
    elif resolves:  # same strictness: a ruling that miscites its question has not settled it
        single = await capture._find_thread(pool, resolves, require_identifier=True)
        if single is None:
            return {"error": f"resolves matched no thread: {resolves!r} — quote its UUID, "
                             "canonical, or 8-char short id (no longer a prose match — "
                             "an addressing act refuses rather than guesses)"}
        answered.append(single)
        single_summary = await capture._thread_summary(pool, single)
    # confirms resolves the same best-effort way as resolves's list form — one bad ref
    # must not veto the practices that DID match
    confirm_ids: list[uuid.UUID] = []
    confirm_receipt: list[dict[str, str]] = []
    for ref in confirms or []:
        pid = await capture._find_practice(pool, ref, require_identifier=True)
        if pid is None:
            confirm_receipt.append({"ref": ref, "matched": "false",
                                    "note": "matched no practice — quote its UUID, "
                                            "canonical, or 8-char short id (no longer a "
                                            "prose match)"})
            continue
        confirm_ids.append(pid)
        confirm_receipt.append({"ref": ref, "matched": "true", "id": str(pid)[:8]})
    # rediscovers resolves the same best-effort way as confirms — one bad ref must not
    # veto the earlier decisions that DID match (task #163)
    rediscover_ids: list[uuid.UUID] = []
    rediscover_receipt: list[dict[str, str]] = []
    for ref in rediscovers or []:
        rdid = await capture._find_decision(pool, ref, require_identifier=True)
        if rdid is None:
            rediscover_receipt.append({"ref": ref, "matched": "false",
                                       "note": "matched no decision — quote its UUID, "
                                               "canonical, or 8-char short id (no longer a "
                                               "prose match)"})
            continue
        rediscover_ids.append(rdid)
        rediscover_receipt.append({"ref": ref, "matched": "true", "id": str(rdid)[:8]})
    # narrows resolves the same best-effort way as rediscovers (thread e05e439d) — one
    # bad ref must not veto the earlier decisions that DID match
    narrow_ids: list[uuid.UUID] = []
    narrow_receipt: list[dict[str, str]] = []
    for ref in narrows or []:
        nid = await capture._find_decision(pool, ref, require_identifier=True)
        if nid is None:
            narrow_receipt.append({"ref": ref, "matched": "false",
                                   "note": "matched no decision — quote its UUID, "
                                           "canonical, or 8-char short id (no longer a "
                                           "prose match)"})
            continue
        narrow_ids.append(nid)
        narrow_receipt.append({"ref": ref, "matched": "true", "id": str(nid)[:8]})
    # cites resolves the same best-effort way as rediscovers/narrows (msg 6000, live
    # specimen 7706efb4: bears_on refused it, narrows was the wrong relation) — the
    # declared form of the prose-citation miner's own edge
    cite_ids: list[uuid.UUID] = []
    cite_receipt: list[dict[str, str]] = []
    for ref in cites or []:
        cid = await capture._find_decision(pool, ref, require_identifier=True)
        if cid is None:
            cite_receipt.append({"ref": ref, "matched": "false",
                                 "note": "matched no decision — quote its UUID, "
                                         "canonical, or 8-char short id (no longer a "
                                         "prose match)"})
            continue
        cite_ids.append(cid)
        cite_receipt.append({"ref": ref, "matched": "true", "id": str(cid)[:8]})
    # bears_on resolves the same best-effort way as confirms/rediscovers — one bad ref
    # must not veto the threads that DID match (thread 898840dc). Same addressing law as
    # resolves/supersedes (require_identifier=True): a citation act refuses rather than
    # guesses. The thread's OWN summary is echoed here too, same reason resolves echoes
    # it — a valid id naming the WRONG thread is only catchable by the caller reading it.
    bears_on_ids: list[uuid.UUID] = []
    bears_on_receipt: list[dict[str, str]] = []
    for ref in bears_on or []:
        bid = await capture._find_thread(pool, ref, require_identifier=True)
        if bid is None:
            # THREE SPECIMENS IN TWO DAYS (Thoth's dispatch msg 5937): bears_on mints
            # `answers`, Decision->Thread ONLY — a ref that names a Decision instead of a
            # Thread resolved to nothing here and the receipt said only "matched no
            # thread", easy to miss in a large response, three different people read it
            # as success. Same cross-type-mismatch discipline `_resolve_cited_object`
            # already uses for prose citations: check the OTHER type too, so a genuine
            # mismatch NAMES itself instead of reading like a generic not-found.
            cross = await capture._find_decision(pool, ref, require_identifier=True)
            if cross is not None:
                bears_on_receipt.append({"ref": ref, "matched": "false",
                                         "note": f"{ref!r} resolves to a Decision, not a "
                                                 "Thread — bears_on only links to a Thread "
                                                 "(mints answers, Decision->Thread); this "
                                                 "id was never linked"})
            else:
                bears_on_receipt.append({"ref": ref, "matched": "false",
                                         "note": "matched no thread — quote its UUID, "
                                                 "canonical, or 8-char short id (no longer a "
                                                 "prose match)"})
            continue
        bsumm = await capture._thread_summary(pool, bid)
        bears_on_ids.append(bid)
        bears_on_receipt.append({"ref": ref, "matched": "true", "id": str(bid)[:8],
                                 "summary": bsumm or ""})
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    # ONE DOOR MISSING ITS SIBLING'S DEFAULT (msg 5703/5720, orphan-door fix), THEN LANE 3
    # (thread 79e785d1), NOW THE SHARED LADDER (thread 6c262aee, #151's law): both rungs
    # — the generation-scoped mount default and the lineage-wide widen — live in
    # capture.resolve_repo_default so record_decision and open_thread never carry two
    # differently-shaped copies of the same fallback.
    ident = await _ident_for(ctx)
    _repo_default = await capture.resolve_repo_default(
        pool, repo, actor, ident.project if ident else None)
    repo = _repo_default["repo"]
    repo_defaulted = _repo_default["repo_defaulted"]
    lineage_attempted = _repo_default["lineage_attempted"]
    lineage_candidates = _repo_default["lineage_candidates"]
    lineage_projects = _repo_default["lineage_projects"]
    # NEAR-DUP RECEIPT HONESTY (task #117, thread ed9f73ce, Seshat's live specimen): the
    # SAME lookup `capture.record_decision` runs internally to decide whether to reuse an
    # existing decision, run here FIRST so the receipt can show what a hit is about to
    # overwrite — a pre-check outside the write transaction, same non-locking caveat as
    # the lookup it mirrors. `repo` gates it exactly like the real call (no safe scope to
    # dedup against without one).
    dup_before: uuid.UUID | None = None
    prior_content: dict[str, str | None] | None = None
    if repo:
        dup_before = await capture.find_near_duplicate_decision(pool, summary, repo=repo,
                                                                 exclude=old)
        if dup_before is not None:
            prior_content = await capture._decision_snapshot(pool, dup_before)
    # THE HATCH'S TWO POPULATIONS (Thoth's condition 2): a caller who requested ONLY
    # extension-link connectivity and gave no unlinked_because of their own gets the
    # machine-set reason, never silently mixed with a genuinely standalone write's own
    # (possibly caller-typed) reason. _enforce_required_links only ever USES this when
    # the atomic-scope check (repo/grounds/resolves) actually fails — a caller who also
    # gave repo=/grounds=/resolves= that satisfy the gate never sees this value land.
    effective_unlinked_because = unlinked_because
    # THE STRUCTURAL DISCRIMINATOR (thread 20b06fbb): this exact boolean is the ONLY
    # place fleet-wide that ever decides "this write's gap is extension-link-pending, not
    # standalone" — passed straight to capture.record_decision as unlinked_because_kind,
    # never re-derived later by matching _EXTENSION_LINK_PENDING_REASON's own prose (which
    # drifts every time this tuple grows a new param name; adoption_meter._hatch_counts
    # used to do exactly that and silently misclassified three wordings' worth of history).
    is_extension_pending = effective_unlinked_because is None and any(
        [obsoletes, confirms, refutes, implements, rediscovers, bears_on]
    )
    if is_extension_pending:
        effective_unlinked_because = _EXTENSION_LINK_PENDING_REASON
    # RECEIPT-HONESTY PRE-CHECK (obligation ce12d2ef): these six now mint INSIDE
    # record_decision's own atomic transaction (7ea187b9's shape (a)), so the wrapper
    # can no longer diff "before this call" vs "after" by calling mint_*/_witness_link
    # itself and reading its bool return — that return no longer reaches here. Instead,
    # pre-check existence against the object THIS call will land on. `dup_before` alone
    # is NOT enough here — it's only computed `if repo:`, but record_decision's own
    # idempotency ALWAYS resolves by the exact summary hash regardless of repo (that's
    # how a repo-less retry still lands on the same object) — so the pre-check target
    # must fall back to that same exact-hash lookup when dup_before is unset, or a
    # repo-less idempotent re-call would wrongly read every link as freshly minted.
    existing_target = dup_before
    if existing_target is None:
        existing_target = await pool.fetchval(
            "SELECT id FROM objects WHERE type='Decision' AND canonical=$1",
            capture._canon("decision", summary))

    async def _link_exists(from_id: uuid.UUID | None, to_id: uuid.UUID, type_: str) -> bool:
        if from_id is None:
            return False
        return bool(await pool.fetchval(
            "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3 LIMIT 1",
            from_id, to_id, type_))
    implements_was_new = (impl_id is None) or not await _link_exists(
        existing_target, impl_id, "implements")
    confirms_was_new = {pid: not await _link_exists(pid, existing_target, "witnesses")
                        if existing_target else True for pid in confirm_ids}
    rediscovers_was_new = {rdid: not await _link_exists(existing_target, rdid, "rediscovers")
                           for rdid in rediscover_ids}
    bears_on_was_new = {bid: not await _link_exists(existing_target, bid, "answers")
                        for bid in bears_on_ids}
    narrows_was_new = {nid: not await _link_exists(existing_target, nid, "narrows")
                       for nid in narrow_ids}
    cites_was_new = {cid: not await _link_exists(existing_target, cid, "cites")
                     for cid in cite_ids}
    try:
        d = await capture.record_decision(
            Actions(pool), summary, kind=kind, rationale=rationale, repo=repo,
            source=actor, grounds=gids,
            protocol=protocol, supersedes=str(old) if old else None,
            resolves=[str(a) for a in answered] if isinstance(resolves, list) else
                     (str(answered[0]) if answered else None),
            repo_evidence_class=(EvidenceClass.DIRECT_OBSERVATION.value
                                  if repo_defaulted else None),
            unlinked_because=effective_unlinked_because,
            implements=impl_id, confirms=confirm_ids or None,
            rediscovers=rediscover_ids or None, bears_on=bears_on_ids or None,
            narrows=narrow_ids or None, cites=cite_ids or None,
            refute_id=refute_id, obsoletes=obsoletes,
            unlinked_because_kind=("extension_link_pending" if is_extension_pending
                                   else None),
        )
    except ValueError as e:  # task #107: e.g. a path-shaped repo — refuse clean, no traceback
        return {"error": str(e)}
    # RECEIPT DIET (msg 6871): `summary` is NOT echoed back — the caller supplied it this
    # same turn, so echoing it verbatim is pure duplication. `resolved_thread(s)` below
    # still echoes ITS OWN summary (the closed THREAD's words, not this call's) because
    # that's the one place a valid id naming the wrong target is only catchable by the
    # caller reading it — a mis-citation risk, not a duplication.
    out: dict[str, Any] = {"id": str(d), "kind": kind}
    if repo_defaulted:
        out["repo_defaulted"] = {
            "to": repo,
            "why": "no repo given — defaulted to the caller's own project rather than "
                   "left unlinked (orphan-door fix, msg 5703/5720)",
        }
    elif lineage_attempted:
        # LANE 3'S OWN ABSTAIN, RECORDED (thread 79e785d1), NOW THE SHARED POST-MINT STEP
        # (thread 6c262aee): the generation-scoped default AND the lineage-root widening
        # both failed to name a single project — genuinely nothing (lineage_candidates
        # empty) or a real disagreement (2+ candidates, never broken by recency/generation
        # count). capture.record_lineage_abstain wraps derive_or_abstain (Lane 0) the same
        # way for every caller, so the receipt shape stays identical across doors.
        out["lineage_repo_derivation"] = await capture.record_lineage_abstain(
            pool, d, actor, lineage_candidates, lineage_projects)
    # CONTENT-LANDED, MEASURED NOT INFERRED (task #149, thread 20145def): a READ-BACK, not
    # a guess from the pre-write dup-check below — that check can only ever say WHICH
    # object a call landed on, never whether THIS call's own rationale/protocol actually
    # became the CURRENT value on it (a different source's assertion can still win the
    # confidence/recency tie-break on the SAME object, silently, and the old receipt shape
    # had no way to say so). Four specimens in one session: Thoth's own "reused_existing_
    # decision:true with a note ambiguous enough I had to go READ the object" (it HAD
    # landed — the receipt just couldn't say); Sekhmet's #146 write going to background
    # with a mis-set field she could not correct until it landed; a decision this house's
    # own prior_art guard once caught reusing a near-duplicate silently. Ruling 60bc15db's
    # own prescription applied directly: don't infer success from "no error raised" — READ
    # the fact you just tried to establish and report what it actually says.
    if rationale is not None or protocol is not None:
        landed: dict[str, bool] = {}
        if rationale is not None:
            current_rationale = await pool.fetchval(
                "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
                "AND name='rationale' ORDER BY confidence DESC, observed_at DESC LIMIT 1", d)
            landed["rationale"] = current_rationale == rationale
        if protocol is not None:
            current_protocol = await pool.fetchval(
                "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
                "AND name='protocol' ORDER BY confidence DESC, observed_at DESC LIMIT 1", d)
            landed["protocol"] = current_protocol == protocol
        out["content_landed"] = landed
        if not all(landed.values()):
            not_landed = [f for f, ok in landed.items() if not ok]
            out["content_landed_note"] = (
                f"your {' and '.join(not_landed)} did NOT become decision {str(d)[:8]}'s "
                "current value — a different assertion is currently winning the "
                f"confidence/recency tie-break on this object. Re-recording with the same "
                "summary is likely to repeat this outcome; use "
                f"amend_decision(ref={str(d)[:8]!r}, addendum=...) instead — it always "
                "lands as new content, never contends a tie-break.")
    if dup_before is not None and str(dup_before) == str(d):
        out["reused_existing_decision"] = True
        out["prior_content"] = prior_content
        exact_repeat = prior_content is not None and prior_content.get("summary") == summary
        out["note"] = (
            (f"this call's summary exactly matched decision {str(d)[:8]}'s own current "
             "summary — a safe repeat (e.g. a retry after a dropped/timed-out response); "
             "nothing was overwritten that this call didn't already say itself.")
            if exact_repeat else
            (f"this call's summary was judged a near-duplicate of decision {str(d)[:8]}'s "
             "EXISTING content (shown in prior_content) and REUSED that object instead of "
             "minting a new one — its prior summary/rationale are now superseded (still "
             "readable via the assertions history, never deleted) but no longer current. "
             "If these two rulings are NOT actually the same decision, this was a false "
             "positive (task #117) — the summaries shared enough boilerplate to score "
             "above the similarity bar without describing the same thing."))
    # PRIOR-ART SURFACING (thread 44635c42, task #67; UNIFIED across {Decisions, Practices,
    # Superstitions, open obligation Threads} by THE THAW, ruling 1e6d7367): before a
    # ruling stands, name what standing law/technique already covers this ground — search
    # is the same fused engine `search()` exposes, topical (lexical + semantic) rather
    # than lexical-only, since a contradicting ruling rarely reuses its predecessor's
    # exact wording (the canonical failure: 636a8648 minted in direct contradiction of
    # naming-v3/a882b334 with zero friction). `_surface_prior_art` (fail-open, 15s bound)
    # is the shared write/read-time engine — record_practice and send()'s dispatch-time
    # hop (obligation a6198075) both run the identical search, not a second matcher.
    # refute_id's target Practice's Superstition is looked up here for the RECEIPT only
    # (below), not to exclude it from this search. A same-call self-collision (the
    # freshly-converted Superstition scoring as this SAME call's own prior-art hit, since
    # refute_practice's write now lands inside the atomic block above, before this search
    # runs) was the obvious worry — checked, not assumed: `strong` requires `via` in
    # ("id", "both"), and embed_backfill (semantics.py) computes the semantic half of the
    # fused match as a SEPARATE, async pass, never synchronously at write time — a same-
    # transaction object scores `via='lexical'` at best here (confirmed live), never
    # strong. No exclusion needed for a scenario this structurally can't reach.
    refute_superstition_id: uuid.UUID | None = None
    if refute_id is not None:
        refuted_statement = await pool.fetchval(
            "SELECT val.value #>> '{}' FROM current_assertions val "
            "WHERE val.object_id=$1 AND val.name='statement' "
            "ORDER BY val.confidence DESC, val.observed_at DESC LIMIT 1", refute_id)
        if refuted_statement and refuted_statement.strip():
            skey = " ".join(refuted_statement.split()).lower()
            refute_superstition_id = await pool.fetchval(
                "SELECT id FROM objects WHERE type='Superstition' AND canonical=$1",
                capture._canon("superstition", skey))
    prior = await _surface_prior_art(
        pool, f"{summary} {rationale or ''}",
        exclude={d} | ({old} if old else set()), repo=repo, actor=actor)
    strong = capture.prior_art_is_strong(prior)
    if prior:
        out["prior_art"] = _slim_prior_art(prior)
    if refute_id is not None:
        # THE STRUCTURAL DISCRIMINATOR, DECOUPLED FROM SEARCH TIMING (thread 7e8cb735,
        # piece 2): refute_id was already resolved and validated against a real Practice
        # earlier in this call (or the call errored out before reaching here) — the
        # caller's intent to overturn THAT practice is a fact this wrapper already holds,
        # not something that needs re-discovering from whatever the search above happens
        # to surface. Folding refute_practice's write into the atomic block above means
        # this same search now runs AFTER the Practice is already flagged `refuted_by`
        # (filtered out of `prior` entirely by prior_art_from_hits' own refuted-hit
        # check) — so the old "was the top hit this same Practice" test would silently
        # stop firing, exactly the regression the prior fold-attempt's own test caught.
        out["prior_art_flag"] = (
            f"this OVERTURNS standing Practice {str(refute_id)[:8]} — handled below via "
            "refutes= (converts it to a dead Superstition, flagged not retired)")
        out["prior_art_polarity"] = "contradict"
    elif strong:
        top = prior[0]
        top_kind = top.get("type") or "Decision"
        if top_kind == "Practice":
            # PRACTICE v2 layer 1 (Thoth LXII's DM 1785): a lexical reversal fingerprint
            # (practice_contradiction_cues) distinguishes an unlabeled CONTRADICTION
            # from a plain, uncited RE-DERIVATION when the caller gave no refutes= at
            # all (the refutes= case is handled unconditionally above, before this
            # branch is ever reached).
            cues = capture.practice_contradiction_cues(f"{summary} {rationale or ''}")
            if cues:
                out["prior_art_flag"] = (
                    f"this may CONTRADICT standing Practice {top['id']} rather than cite "
                    f"it — reversal language found ({', '.join(cues)}); if you mean to "
                    f"overturn it, say so explicitly (refutes=['{top['id']}']), or "
                    "acknowledge it (ack_prior_art=True) if this wording is coincidental")
                out["prior_art_polarity"] = "contradict"
            else:
                out["prior_art_flag"] = (
                    f"this looks like a re-derivation of standing Practice {top['id']} — "
                    f"confirm it as evidence (confirms=['{top['id']}']) if it's the same "
                    "lesson, or acknowledge it (ack_prior_art=True) if coincidental")
                out["prior_art_polarity"] = "rederive"
        elif top_kind == "Superstition":
            out["prior_art_flag"] = (
                f"a dead Superstition ({top['id']}) already covers this ground — check "
                "you're not reviving a workaround its own fix already killed "
                "(acknowledge with ack_prior_art=True if this is intentional/unrelated)")
        elif top_kind == "Thread":
            # THE MEASURER'S MOMENT (898840dc/e123b9fa): the nudge fires unprompted,
            # inheriting THE THAW's own proven behavior rather than being a new
            # detector — see UNIFIED_PRIOR_ART_KINDS' own comment. Deliberately never
            # suggests resolves= here: this decision merely SPOKE TO the row in
            # passing (that's how it surfaced as prior art at all); whether it also
            # SETTLES the row is the caller's own judgment to make, not this flag's
            # to presume.
            out["prior_art_flag"] = (
                f"this appears to speak to open thread {top['id']} — pass "
                f"bears_on=['{top['id']}'] to link it without closing it (bears_on "
                "cites, it never resolves — use resolves=[...] instead if this ruling "
                "actually SETTLES the row), or acknowledge it (ack_prior_art=True) if "
                "coincidental")
            out["prior_art_polarity"] = "bears_on"
        else:
            out["prior_art_flag"] = (
                f"a standing ruling ({top['id']}) covers this ground — supersede it "
                "explicitly (supersedes=...), cite it (grounds=...), name this as what "
                "it executes (implements=...), name this as an independent "
                "rediscovery of it (rediscovers=[...]) if you reached the same "
                "conclusion on your own, or acknowledge it (ack_prior_art=True)")
    if prior:
        # INSTRUMENT IT (THE THAW piece 6): every strong hit is a MEASURED re-derivation
        # event, logged regardless of whether the caller acts on it — the population,
        # aggregated over time, IS the fleet's re-derivation ratchet metric.
        try:
            await pool.execute(
                "UPDATE search_log SET prior_art_kind=$1, prior_art_strong=$2, "
                "prior_art_polarity=$3 "
                "WHERE id = (SELECT id FROM search_log ORDER BY id DESC LIMIT 1)",
                (prior[0].get("type") or "Decision") if prior else None, strong,
                out.get("prior_art_polarity"))
        except Exception:  # noqa: BLE001 — telemetry must never block the ruling
            pass
    if ack_prior_art:
        if prior and strong:
            await capture.acknowledge_prior_art(Actions(pool), d, prior[0]["id"], actor)
            out["prior_art_acknowledged"] = f"noted — {prior[0]['id']} reviewed, no action needed"
        elif prior:
            # #117's own vocabulary-collapse shape, caught live (Thoth msg 3185, ruling
            # b44ddb6d): `out["prior_art"]` above already lists these same hits — saying
            # "none found" here when `prior` is non-empty would contradict the SAME receipt.
            out["prior_art_acknowledged"] = (
                f"{len(prior)} prior-art hit(s) found but none strong enough to flag — "
                "nothing rises to acknowledge")
        else:
            out["prior_art_acknowledged"] = (
                "no prior-art hit was found at all — nothing to acknowledge")
    # RECEIPTS ONLY BELOW — all six already MINTED inside capture.record_decision's own
    # atomic transaction, above (obligation ce12d2ef: the object and every one of these
    # now either all land or none do). Nothing here writes; each block just reads back
    # what committed, using the pre-check computed before the call for "was this new".
    if impl_id is not None:
        out["implements"] = (
            f"{str(impl_id)[:8]} — this decision is a specific execution of it"
            f"{'' if implements_was_new else ' (already linked)'}")
    if confirm_ids:
        witnessed = []
        for pid in confirm_ids:
            n = await capture.practice_confirmed_count(pool, pid)
            witnessed.append({"id": str(pid)[:8], "new_witness": confirms_was_new[pid],
                             "confirmed": n})
        out["confirmed_practices"] = witnessed
    if confirm_receipt:
        out["confirms_resolution"] = confirm_receipt
    if rediscover_ids:
        out["rediscovers"] = [{"id": str(rdid)[:8], "new_link": rediscovers_was_new[rdid]}
                              for rdid in rediscover_ids]
    if rediscover_receipt:
        out["rediscovers_resolution"] = rediscover_receipt
    if bears_on_ids:
        out["bears_on"] = [{"id": str(bid)[:8], "new_link": bears_on_was_new[bid]}
                           for bid in bears_on_ids]
    if bears_on_receipt:
        out["bears_on_resolution"] = bears_on_receipt
    if narrow_ids:
        out["narrows"] = [{"id": str(nid)[:8], "new_link": narrows_was_new[nid]}
                          for nid in narrow_ids]
    if narrow_receipt:
        out["narrows_resolution"] = narrow_receipt
    if cite_ids:
        out["cites"] = [{"id": str(cid)[:8], "new_link": cites_was_new[cid]}
                        for cid in cite_ids]
    if cite_receipt:
        out["cites_resolution"] = cite_receipt
    # RECEIPTS ONLY BELOW, same discipline as the six siblings above (thread 7e8cb735):
    # refute_id's `refuted_by` stamp and every obsoletes= Superstition already MINTED
    # inside capture.record_decision's own atomic transaction — nothing here writes,
    # each block reads back what committed.
    if refute_id is not None:
        refuted_by = await pool.fetchval(
            "SELECT val.value #>> '{}' FROM current_assertions val "
            "WHERE val.object_id=$1 AND val.name='refuted_by' "
            "ORDER BY val.confidence DESC, val.observed_at DESC LIMIT 1", refute_id)
        if refuted_by == str(d):
            out["refuted_practice"] = (
                f"{str(refute_id)[:8]} converted to Superstition "
                f"{(str(refute_superstition_id)[:8] + ' ') if refute_superstition_id else ''}"
                "— the Practice stays active, flagged")
    if obsoletes:
        killed = [s.strip() for s in obsoletes if s and s.strip()]
        if killed:
            out["superstitions_killed"] = killed
            out["superstitions_note"] = (
                "each is a dead Superstition on the record; orient announces recent kills "
                "fleet-wide for 14 days so minds carrying the practice strike it")
    if not protocol and capture.measurement_smell(f"{summary} {rationale or ''}"):
        # thread 022bd24a: `protocol` is this tool's best field and nothing asked for it —
        # advice in the receipt, never a gate (the decision is recorded either way).
        # RECEIPT DIET (msg 6871): short code, not the full prose every firing —
        # describe('nags:protocol') for the text.
        out.setdefault("nags", []).append("protocol")
    if isinstance(resolves, list):
        out["resolved_threads"] = receipt
    elif answered:
        # THE SAME-TURN CATCH (msg 2426 — 5 documented instances, e.g. fd237b40, all
        # caught only later by a human re-reading a receipt that never showed the
        # summary): a valid id naming the wrong thread cannot be refused by any matcher,
        # but the mismatch is obvious the instant the closed thread's own words are
        # right here — so they are, every time, not just for the list form.
        out["resolved_thread"] = (
            f"{str(answered[0])[:8]} — closed by this decision (answers edge) — "
            f"{single_summary or '(no summary on record)'}")
    if old is not None:
        out["superseded"] = (
            "self (identical summary re-recorded) — nothing buried" if old == d else
            f"{str(old)[:8]} is buried under this decision: it leaves orient's recent "
            "list, the decision-log grays it (unwind: re-assert superseded_by='' on it)")
    if grounded:
        out["grounded_by"] = grounded
    if missing:
        out["unresolved_grounds"] = missing
        out["note"] = ("unresolved grounds were SKIPPED — ingest_reference them first, "
                       "then re-run record_decision (idempotent) to attach the edges")
    return out


@mcp.tool()
async def record_practice(
    statement: str, failure_prevented: str | None = None, surface: str | None = None,
    repo: str | None = None, witnesses: list[str] | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Write back a TRANSFERABLE TECHNIQUE, Superstition's positive twin. `statement` is
    the imperative one-liner (quote it as you'd want a future mind to inherit it, not as
    narration). `failure_prevented` is the concrete symptom that makes it findable
    mid-failure. `surface` reuses BlindSpot's domain vocabulary. `witnesses` links
    Decision(s)/Commit(s)/Thread(s) as evidence (a miss is reported, never fatal).
    Idempotent on the normalized statement. Timeless, never moment-stamped — a later
    disproof kills it via record_decision(refutes=...), never here. Runs the same
    prior-art check record_decision does."""
    pool = await _pool_get()
    wids: list[uuid.UUID] = []
    receipt: list[dict[str, str]] = []
    for ref in witnesses or []:
        rid = await _resolve(pool, ref)
        if rid is not None:
            wids.append(rid)
            receipt.append({"ref": ref, "matched": "true", "id": str(rid)[:8]})
        else:
            receipt.append({"ref": ref, "matched": "false",
                            "note": "matched no object — quote its UUID or 8-char short id"})
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    p = await capture.record_practice(
        Actions(pool), statement, failure_prevented=failure_prevented, surface=surface,
        repo=repo, witnesses=wids, source=actor)
    out: dict[str, Any] = {"id": str(p), "statement": statement,
                           "confirmed": await capture.practice_confirmed_count(pool, p)}
    if receipt:
        out["witnesses_resolution"] = receipt
    prior = await _surface_prior_art(
        pool, f"{statement} {failure_prevented or ''}", exclude={p}, repo=repo, actor=actor)
    strong = capture.prior_art_is_strong(prior)
    if prior:
        out["prior_art"] = prior
        if strong:
            top = prior[0]
            out["prior_art_flag"] = (
                f"{top.get('type') or 'Decision'} {top['id']} already covers similar "
                "ground — check this isn't the same lesson under different words before "
                "it stands as a separate Practice")
        try:
            await pool.execute(
                "UPDATE search_log SET prior_art_kind=$1, prior_art_strong=$2 "
                "WHERE id = (SELECT id FROM search_log ORDER BY id DESC LIMIT 1)",
                (prior[0].get("type") or "Decision") if prior else None, strong)
        except Exception:  # noqa: BLE001 — telemetry must never block the record
            pass
    return out


@mcp.tool()
async def ingest_reference(
    title: str, source_url: str | None = None, vendor: str | None = None,
    body: str | None = None, caveats: str | None = None, repo: str | None = None,
    cites: list[str] | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Turn something you READ into a first-class Reference node — a paper, vendor doc,
    spec — so it's findable by search and citable via record_decision(grounds=[...]).
    `title` is the citation key, idempotent on its slug. `vendor` = who wrote it; `body`
    = what it claims, in your words. `caveats` is first-class and separate from body —
    the "but only under X" that dies when buried in prose. `cites` wires paper-to-paper
    lineage (ids/canonicals/titles of already-ingested References). Graded
    SELF_DECLARED. Returns the id + canonical to cite."""
    pool = await _pool_get()
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    cids: list[uuid.UUID] = []
    missing: list[str] = []
    for c in cites or []:
        rid = await _resolve(pool, c)
        (cids.append(rid) if rid is not None else missing.append(c))
    # ONE DOOR MISSING ITS SIBLING'S DEFAULT (msg 5703/5720, orphan-door fix), NOW THE
    # SAME SHARED LADDER record_decision/open_thread climb (thread 6c262aee, #151's law):
    # generation-scoped mount default, then the lineage-wide widen when that finds nothing.
    ident = await _ident_for(ctx)
    _repo_default = await capture.resolve_repo_default(
        pool, repo, actor, ident.project if ident else None)
    repo = _repo_default["repo"]
    repo_defaulted = _repo_default["repo_defaulted"]
    lineage_attempted = _repo_default["lineage_attempted"]
    lineage_candidates = _repo_default["lineage_candidates"]
    lineage_projects = _repo_default["lineage_projects"]
    try:
        ref, canon = await capture.ingest_reference(
            Actions(pool), title, source_url=source_url, vendor=vendor,
            body=body, caveats=caveats, repo=repo, cites=cids,
            source=actor,
            repo_evidence_class=(EvidenceClass.DIRECT_OBSERVATION.value
                                  if repo_defaulted else None),
        )
    except ValueError as e:  # task #107: e.g. a path-shaped repo — refuse clean, no traceback
        return {"error": str(e)}
    out: dict[str, Any] = {"id": str(ref), "canonical": canon,
                           "note": "cite it: record_decision(..., grounds=['" + canon + "'])"}
    if repo_defaulted:
        out["repo_defaulted"] = {
            "to": repo,
            "why": "no repo given — defaulted to the caller's own project rather than "
                   "left unlinked (orphan-door fix, msg 5703/5720)",
        }
    elif lineage_attempted:
        # SAME SHARED POST-MINT STEP record_decision/open_thread's wrappers use (thread
        # 6c262aee): the generation-scoped default AND the lineage-root widening both
        # failed to name a single project — abstain and record why, candidate ids kept
        # whole, via the ONE primitive every orphan-healing lane calls.
        out["lineage_repo_derivation"] = await capture.record_lineage_abstain(
            pool, ref, actor, lineage_candidates, lineage_projects)
    if missing:
        out["unresolved_cites"] = missing
        out["cites_note"] = ("unresolved cites SKIPPED — ingest_reference each cited work "
                             "first, then re-ingest this title (idempotent) to wire the edges")
    return out


@mcp.tool()
async def open_thread(
    summary: str, repo: str | None = None, kind: str | None = None,
    owner: str | None = None, assignee: str | None = None, arc: str | None = None,
    resolves: str | list[str] | None = None,
    branch: str | None = None, files_touched: list[str] | None = None,
    unlinked_because: str | None = None,
    session_anchor: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Open a THREAD — an unresolved question or next step for the next session to pick up.
    Surfaces in run_composition('briefing'). `repo` files it under a project. Idempotent
    on the summary and on a near-duplicate (`deduped`/`dedup_scope` name the twin among
    this project's own OPEN threads instead of minting one). A genuinely new thread gets
    `prior_art`/`prior_art_flag` — a standing Decision/Practice/Thread that may already
    cover this ground, surfacing only, never a refusal.

    `kind='obligation'` marks a duty minted by an action. `owner`=whose move it is
    ('operator', 'agent:<id>', a project name, or unowned = anyone). `assignee` leases
    a single-assignee obligation to one build; a near-duplicate then surfaces
    `leased_to` instead of deduping silently. `arc` sorts into the roadmap taxonomy
    (osiris-scoped only). `resolves` closes a predecessor thread this one supersedes —
    same UUID/canonical/short-id-only strictness as record_decision's `resolves`.
    `branch`/`files_touched` mark held work; `colliding_work` names any open collision.
    `unlinked_because` mirrors record_decision's own hatch. consult_canon('open_thread')
    for more."""
    pool = await _pool_get()
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    # AN UNFILED THREAD IS INVISIBLE TO ITS OWN PROJECT (Alfred V's succession repro,
    # thread 4ffe0eb9: IV's handoff, opened without repo=, hid from orient and the whisper
    # while his successor mined transcripts with regex). The mounted identity already
    # knows the project — filing there is the default; unfiled takes deliberate effort.
    # SAME LADDER record_decision's wrapper climbs (thread 6c262aee, #151's law): the
    # generation-scoped mount default, THEN — Threads are the worse orphan bleeder,
    # 15-21%/week vs Decision's 5-11% — the lineage-wide widen when that finds nothing.
    ident = await _ident_for(ctx)
    _repo_default = await capture.resolve_repo_default(
        pool, repo, actor, ident.project if ident else None)
    repo = _repo_default["repo"]
    repo_defaulted = _repo_default["repo_defaulted"]
    lineage_attempted = _repo_default["lineage_attempted"]
    lineage_candidates = _repo_default["lineage_candidates"]
    lineage_projects = _repo_default["lineage_projects"]
    dup = await capture.find_near_duplicate_open_thread(pool, summary, repo=repo)
    if dup is not None:
        out: dict[str, Any] = {"id": str(dup), "summary": summary, "status": "open",
                               "deduped": "true",
                               "dedup_scope": "a near-exact twin among this project's own "
                                              "OPEN Threads (find_near_duplicate_open_thread)"}
        # THE WRITE-BOUNDARY HONESTY RULE (decision beb046cfbdf9/42176e16): a dedup hit
        # returns here, before kind/arc/etc. are ever applied — 17 threads once got a
        # clean-looking receipt while nothing landed (Sekhmet, decision d310fee2).
        # capture.discarded_on_noop names which of THESE two supplied fields would have
        # changed the existing thread; owner/assignee keeps its own bespoke lease-
        # visibility note below (a sharper message than a generic diff would give it).
        # branch/files_touched/resolves are not yet wired into this check — a named gap,
        # not a silent one; see the function's own docstring.
        supplied = {k: v for k, v in {"kind": kind, "arc": arc}.items() if v is not None}
        if supplied:
            existing_vals = await capture._thread_named_properties(pool, dup, tuple(supplied))
            discarded = capture.discarded_on_noop(supplied, existing_vals)
            if discarded:
                out["discarded"] = discarded
                out["note"] = (
                    f"matched an existing thread — {', '.join(sorted(discarded))} you "
                    "passed here were NOT applied (open_thread never updates an existing "
                    "thread on a dedup hit). Use reclassify_thread to change arc after "
                    "the fact."
                )
        if assignee:
            holder = await capture._current_owner(pool, dup)
            claim = assignee.strip()
            out["leased_to"] = holder or "(unowned)"
            lease_note = (
                f"already leased to {holder} (thread {str(dup)[:8]}) — no new build minted"
                if holder == claim else
                f"existing lease on thread {str(dup)[:8]} is held by "
                f"{holder or '(unowned)'!r}, not {claim!r} — surfaced instead of minting a "
                "parallel build (a double-assignment must be visible, not silent)"
            )
            out["note"] = f"{out['note']} {lease_note}" if out.get("note") else lease_note
        return out
    # resolve BEFORE recording, same discipline record_decision's own resolves= uses — for
    # RECEIPT purposes only (what a caller sees closed in the SAME turn); the actual write
    # happens inside capture.open_thread, which resolves `resolves` again itself so its own
    # return type (a bare UUID, ~20 existing call sites) never has to change to carry this.
    resolved_receipt: list[dict[str, str]] = []
    single_resolved_summary: str | None = None
    if isinstance(resolves, list):
        for ref in resolves:
            tid = await capture._find_thread(pool, ref, require_identifier=True)
            if tid is None:
                resolved_receipt.append({"ref": ref, "matched": "false",
                                         "note": "matched no thread — quote its UUID, "
                                                 "canonical, or 8-char short id"})
                continue
            summ = await capture._thread_summary(pool, tid)
            resolved_receipt.append({"ref": ref, "matched": "true", "id": str(tid)[:8],
                                     "summary": summ or ""})
    elif resolves:
        single = await capture._find_thread(pool, resolves, require_identifier=True)
        if single is None:
            return {"error": f"resolves matched no thread: {resolves!r} — quote its UUID, "
                             "canonical, or 8-char short id (no prose match — an "
                             "addressing act refuses rather than guesses)"}
        single_resolved_summary = await capture._thread_summary(pool, single)
    try:
        t = await capture.open_thread(
            Actions(pool), summary, repo=repo, kind=kind, owner=owner, assignee=assignee,
            arc=arc, resolves=resolves, branch=branch, files_touched=files_touched,
            source=actor,
            repo_evidence_class=(EvidenceClass.DIRECT_OBSERVATION.value
                                  if repo_defaulted else None),
            unlinked_because=unlinked_because,
        )
    except ValueError as e:
        return {"error": str(e)}
    if arc and not await capture.arc_in_scope(pool, repo):
        arc_receipt = capture._arc_out_of_scope_note(f"repo:{repo}" if repo else "(no project)")
    else:
        arc_receipt = arc or capture._ARC_UNSORTED
    out = {"id": str(t), "summary": summary, "status": "open", "deduped": "false",
          "dedup_scope": "checked only this project's own OPEN Threads for a near-exact "
                         "twin (find_near_duplicate_open_thread) — not standing Decisions, "
                         "Practices, or resolved Threads; see prior_art below for those",
          "arc": arc_receipt}
    # PRIOR-ART SURFACING (obligation 8f59b64f, Thoth XC/msg 6120 — open_thread was the one
    # write verb of the three (record_decision, send, open_thread) with no semantic prior-
    # art check at all: #86's own borrowing went one way, open_thread's twin-check ported TO
    # record_decision, never back). Same shared engine both those doors already call
    # (_surface_prior_art, fail-open/15s-bound) — surfacing only, never a refusal, and no
    # ack_prior_art/polarity machinery: open_thread has no confirms=/refutes=/rediscovers=
    # of its own to route an acknowledgement through, unlike record_decision. Deliberately
    # scoped to the MINT path only (never the dedup-hit early return above, and never
    # settle()'s own threads_open batch loop, which calls capture.open_thread directly and
    # was already outside this wrapper's dedup check too) — a caller who already matched an
    # existing open Thread doesn't need a second search to be told something related exists.
    prior = await _surface_prior_art(pool, summary, repo=repo, actor=actor)
    if prior:
        out["prior_art"] = _slim_prior_art(prior)
        if capture.prior_art_is_strong(prior):
            top = prior[0]
            if top.get("type") == "Thread":
                out["prior_art_flag"] = (
                    f"this appears to speak to open thread {top['id']} — if this new one "
                    f"is meant to close it, pass resolves=['{top['id']}'] next time; "
                    "otherwise just worth reading before this stands as a separate duty")
            else:
                out["prior_art_flag"] = (
                    f"a standing {(top.get('type') or 'Decision').lower()} ({top['id']}) "
                    "may already cover this ground — read it before this stands as a new "
                    "finding")
    if repo_defaulted:
        out["repo_defaulted"] = {
            "to": repo,
            "why": "no repo given — defaulted to the caller's own project rather than "
                   "left unlinked (orphan-door fix, msg 5703/5720)",
        }
    elif lineage_attempted:
        # SAME SHARED POST-MINT STEP record_decision's wrapper uses (thread 6c262aee):
        # the generation-scoped default AND the lineage-root widening both failed to name
        # a single project — abstain and record why, candidate ids kept whole, via the
        # ONE primitive every orphan-healing lane calls (Lane 0, capture.derive_or_abstain).
        out["lineage_repo_derivation"] = await capture.record_lineage_abstain(
            pool, t, actor, lineage_candidates, lineage_projects)
    if assignee:
        out["assignee"] = assignee.strip()
    elif kind == "obligation" and not owner:
        # DEFAULT, NEVER REFUSE, NEVER SILENT (#5546 item 1, Thoth msg 5605): neither
        # `owner` nor `assignee` were supplied for a duty — capture.open_thread may have
        # defaulted one to the caller's own seat. Read back what actually landed rather
        # than re-deriving it here, so the receipt can never drift from the write.
        landed_owner = await capture._current_owner(pool, t)
        if landed_owner:
            out["owner_defaulted"] = {
                "to": landed_owner,
                "why": "kind='obligation' with no owner given — defaulted to the "
                       "caller's own seat rather than left ownerless (#5546 item 1)",
            }
    if files_touched:
        others = [c for c in await capture.open_held_work(pool, repo=repo)
                 if c["id"] != str(t)[:8]]
        collisions = capture.held_work_overlap(files_touched, others)
        if collisions:
            out["colliding_work"] = collisions
    if isinstance(resolves, list):
        out["resolved_threads"] = resolved_receipt
    elif resolves:
        out["resolved_thread"] = str(resolves)
        out["resolved_thread_summary"] = single_resolved_summary or ""
    return out


@mcp.tool()
async def resolve_thread(
    ref: str | list[str], because: str | None = None, artifact: str | None = None,
    dry_run: bool = False,
    subagent_id: str | None = None,
    subagent_type: str | None = None, session_anchor: str | None = None,
    ctx: Context | None = None
) -> dict[str, Any]:
    """Close a THREAD — `ref` is its UUID or a summary substring; `because` is a short
    WHY, not a completion essay. Event-sourced, never deleted: auditable and reversible.
    `artifact` points at what actually closed it (a commit hash, decision id, file:line)
    — kept as `resolved_artifact`; when it names a graph object a `resolved_by` edge
    mints too, confirmed in the receipt.

    Re-resolving is allowed: `ref` matches by identity, not status, so a second call on
    an already-resolved thread attaches a later, more specific closure witness — latest
    wins, earlier reasoning stays in history. The receipt names it when this happens.

    A LIST of refs closes a BATCH (#203, decision 880ffe79): `because` becomes mandatory,
    `dry_run=True` previews without writing, and the whole batch refuses if any ref does
    not resolve to exactly one thread. See `capture.resolve_threads_bulk`."""
    if isinstance(ref, list):
        pool = await _pool_get()
        return await capture.resolve_threads_bulk(
            Actions(pool), ref, because=because or "", artifact=artifact, dry_run=dry_run,
            source=await _actor_for(ctx, subagent_id, subagent_type))
    pool = await _pool_get()
    probe_tid = await capture._find_thread(pool, ref)
    was_already_resolved = (
        probe_tid is not None
        and await capture._thread_resolved_in(pool, probe_tid) is not None)
    tid = await capture.resolve_thread(
        Actions(pool), ref, because=because, artifact=artifact,
        source=await _actor_for(ctx, subagent_id, subagent_type)
    )
    if tid is None:
        return {"error": f"no thread matches {ref!r}"}
    out = {"id": str(tid), "status": "resolved"}
    if was_already_resolved:
        out["note"] = ("this thread was already resolved before this call — "
                       "because/resolved_artifact now reflect THIS call's own text, "
                       "not the original close; earlier reasoning is still readable in "
                       "the graph's history, not overwritten there, just not what a "
                       "current-value read shows anymore")
    if artifact:
        out["artifact"] = f"{artifact} — kept as resolved_artifact"
        target = await pool.fetchrow(
            "SELECT o.type, o.canonical FROM links l JOIN objects o ON o.id=l.to_id "
            "WHERE l.from_id=$1 AND l.type='resolved_by' LIMIT 1", tid)
        out["resolved_by"] = (
            f"{target['type']} {target['canonical']} — the strong closure witness"
            if target is not None else
            "none — the artifact did not resolve to a graph object (a file:line or an "
            "unmatched pointer); resolved_artifact still carries it as text, and a "
            "closed_by edge to the resolving agent was minted instead — the weak "
            "witness, still traversable, just not naming a specific commit/decision"
        )
    return out


@mcp.tool()
async def annotate_thread(
    ref: str, note: str,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, str]:
    """Add to a THREAD's record WITHOUT closing it — the fifth door (#116: `resolve_thread`
    closes; `assign_thread` hands off; `defer_thread` snoozes; this one just adds). `ref` is
    a Thread UUID, canonical, short-id prefix, or summary substring, matched regardless of
    the thread's own status — an annotated thread stays exactly as open, resolved, or
    deferred as it was before the call. Each call appends independently (never supersedes an
    earlier note, never touches `summary`/`status`); nothing here revises anything. A caller
    who means "the earlier understanding was wrong" wants a different verb (open a fresh
    thread, or fold the correction into whatever answers this one)."""
    pool = await _pool_get()
    try:
        tid = await capture.annotate_thread(
            Actions(pool), ref, note,
            source=await _actor_for(ctx, subagent_id, subagent_type))
    except ValueError as e:
        return {"error": str(e)}
    if tid is None:
        return {"error": f"no thread matches {ref!r}"}
    return {"id": str(tid), "note": note.strip(), "status": "annotated"}


@mcp.tool()
async def rematerialize(
    anchor_sid: str, dest: str | None = None, force: bool = False,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Reconstruct a session's transcript byte-for-byte from soul_lines alone, written
    to disk. `anchor_sid` is the 8-char session anchor. Verifies the hash chain while
    collecting: a break returns `{"error": ..., "verified_through": N}` and nothing is
    written. `dest` defaults to the session's own recorded source_path.

    Refuses to overwrite a LIVE transcript — `dest` modified more recently than this
    session's last ingest would clobber unseen content; `force=True` overrides. Success
    returns `{"written": <path>, "lines": N, "sha256": <hex>}`."""
    from src.ingest.soul_store import SoulStore

    pool = await _pool_get()
    return await SoulStore(pool).rematerialize_to_disk(anchor_sid, dest=dest, force=force)


@mcp.tool()
async def heal_seat_transcript(
    handle: str, source_paths: list[str], dry_run: bool = True, because: str = "",
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Splice a seat's session, fragmented across multiple project slugs by a mid-session
    cwd move, back into ONE file at its own office slug. `handle` names the seat whose
    office the result lands at. `source_paths` are the original fragments, IN CHAIN
    ORDER (oldest first) — the session uuid and 8-char anchor_sid derive from
    `source_paths[0]`'s own filename.

    `verify_jsonl_chain_boundary` runs on every consecutive pair before anything is
    touched, refusing a false "same session" or a genuinely separate session sharing
    only a directory.

    `dry_run=True` (default) reports clean/refused per pair and where the result would
    land — nothing written. `dry_run=False` requires `because` and performs the real
    splice + rematerialize. Never touches a Seat row, anchor_cwd, or any source
    transcript — the anchor-repoint half is heal_seat_anchor, a different door."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a transcript heal is a deliberate act on the record",
                "why": _anchorless(ctx)}
    handle = (handle or "").strip()
    if not handle:
        return {"error": "a handle is required"}
    if not source_paths or len(source_paths) < 2:
        return {"error": "source_paths needs at least two fragments to splice — a single "
                         "file has nothing to join"}
    from pathlib import Path

    from src.ingest.soul_store import SoulStore, verify_jsonl_chain_boundary
    from src.orchestrator.offices import _default_office_root

    first_stem = Path(source_paths[0]).stem
    if len(first_stem) != 36 or first_stem.count("-") != 4:
        return {"error": f"source_paths[0]'s filename ({first_stem!r}) is not a session "
                         "uuid — cannot derive the session id to splice under"}
    full_sid = first_stem
    anchor_sid = full_sid.split("-")[0]
    dest = _default_office_root() / handle.lower() / f"{full_sid}.jsonl"

    preflight: list[dict[str, Any]] = []
    for a, b in zip(source_paths, source_paths[1:], strict=False):
        reason = verify_jsonl_chain_boundary(a, b)
        preflight.append({"a": a, "b": b, "clean": reason is None, "reason": reason})
    out: dict[str, Any] = {
        "handle": handle, "anchor_sid": anchor_sid, "dry_run": dry_run,
        "preflight": preflight, "office_dest": str(dest),
    }
    if any(not p["clean"] for p in preflight):
        out["error"] = "preflight refused — see `preflight` for which pair and why"
        return out
    if dry_run:
        return out

    because = because.strip()
    if not because:
        return {"error": "because is required to execute — an operator-gated act needs "
                         "a stated reason", **out}

    pool = await _pool_get()
    store = SoulStore(pool)
    try:
        spliced = await store.splice_sources(anchor_sid, source_paths)
    except ValueError as e:
        return {"error": str(e), **out}
    out["spliced_lines"] = spliced
    out["verify_chain"] = await store.verify_chain(anchor_sid)
    out["rematerialize"] = await store.rematerialize_to_disk(anchor_sid, dest=str(dest))
    return out


@mcp.tool()
async def correct_thread_summary(
    ref: str, corrected_summary: str, because: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, str]:
    """Correct a THREAD's own headline in place — for when the earlier understanding was
    wrong, distinct from annotate_thread. `summary` itself is never touched (it's
    open_thread's own dedup key); `corrected_summary` is an ordinary property, so
    re-calling this supersedes the prior correction rather than piling up notes.
    `because` (optional) names why. recall(ref) shows both the correction and the
    untouched original `summary` in one call. Returns {"error": ...} when `ref` matches
    nothing."""
    pool = await _pool_get()
    try:
        tid = await capture.correct_thread_summary(
            Actions(pool), ref, corrected_summary, because=because,
            source=await _actor_for(ctx, subagent_id, subagent_type))
    except ValueError as e:
        return {"error": str(e)}
    if tid is None:
        return {"error": f"no thread matches {ref!r}"}
    out = {"id": str(tid), "corrected_summary": corrected_summary.strip(), "status": "corrected"}
    if because:
        out["because"] = because.strip()
    return out


@mcp.tool()
async def amend_decision(
    ref: str, addendum: str,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, str]:
    """Append reasoning to a LIVE decision as understanding develops, WITHOUT superseding it.
    `record_decision` is write-once-plus-supersede — mint fresh, or bury under a correction —
    this is the third door: more of the same ruling's own reasoning, added later. `ref` is a
    Decision UUID, canonical, short-id prefix, or summary substring. `summary`/`rationale`/
    `kind` are never touched here.
    Returns {"error": ...} (never raises past this wrapper) when `ref` matches nothing, or
    when it resolves to a decision already superseded — amend the successor instead, or use
    record_decision(supersedes=...) if you mean a correction; this verb only ever adds to a
    ruling still standing."""
    pool = await _pool_get()
    try:
        did = await capture.amend_decision(
            Actions(pool), ref, addendum,
            source=await _actor_for(ctx, subagent_id, subagent_type))
    except ValueError as e:
        return {"error": str(e)}
    if did is None:
        return {"error": f"no decision matches {ref!r}"}
    return {"id": str(did), "addendum": addendum.strip(), "status": "amended"}


@mcp.tool()
async def amend_practice(
    ref: str, amendment: str,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, str]:
    """Narrow or correct a LIVE practice's guidance, without touching its id, its
    `statement` (record_practice's idempotency key), or its witness/confirmed count —
    same shape as amend_decision. Use when a mechanism now covers part of what a
    practice warns about. Amendments fold directly into practices()'s own listing.
    Returns {"error": ...} when `ref` matches nothing, or names a Practice already
    REFUTED — use record_decision(refutes=...) to kill one, this only adds to a
    practice still standing."""
    pool = await _pool_get()
    try:
        pid = await capture.amend_practice(
            Actions(pool), ref, amendment,
            source=await _actor_for(ctx, subagent_id, subagent_type))
    except ValueError as e:
        return {"error": str(e)}
    if pid is None:
        return {"error": f"no practice matches {ref!r}"}
    return {"id": str(pid), "amendment": amendment.strip(), "status": "amended"}


@mcp.tool()
async def acquire_lease(
    resource_id: str, holder: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Claim any genuinely SHARED, non-isolable resource by an EXACT id (`deploy`,
    `docker-daemon`, the live server) — not a working tree, which has no contention to
    coordinate. Matched by equality, backed by a real DB uniqueness constraint, never a
    race (unlike open_thread(assignee=)'s fuzzy prose match). `resource_id` is
    convention, not a closed vocabulary.

    `holder` defaults to your own mounted identity; pass one to claim on another's
    behalf. A refusal names who holds it and since when. No renewed TTL — explicit
    release_lease is the primary path; reap_stale_leases is the crash/compaction
    backstop (a 5-min cron), not the norm."""
    pool = await _pool_get()
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    try:
        result = await resource_lease.acquire(
            Actions(pool), resource_id, holder or actor, source=actor)
    except ValueError as e:
        return {"error": str(e)}
    out: dict[str, Any] = {
        "resource_id": result.resource_id, "acquired": result.acquired,
        "holder": result.holder, "held_since": result.acquired_at.isoformat(),
        "thread_id": str(result.thread_id),
    }
    if not result.acquired:
        out["note"] = (f"already held by {result.holder} since "
                       f"{result.acquired_at.isoformat()} — no new claim minted")
    return out


@mcp.tool()
async def release_lease(
    resource_id: str,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Release a resource YOU hold — only the ACTUAL holder's own release call frees it,
    never a different agent's, even by name, ENFORCED: unlike `acquire_lease`'s deliberate
    `holder` latitude ("claim on another's behalf"), this verb takes no `holder` param —
    the identity checked is always the caller's own resolved `actor`. `released: false`
    for BOTH an unheld resource and a wrong-holder attempt — both are refusals to report,
    never errors to raise; check `check_lease` first if you need to tell the two apart."""
    pool = await _pool_get()
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    released = await resource_lease.release(pool, resource_id, actor)
    return {"resource_id": resource_id, "released": released}


@mcp.tool()
async def check_lease(resource_id: str) -> dict[str, Any]:
    """Read-only: who holds `resource_id` right now, or that it's free. Never claims,
    never mints, never leases anything — a glance before deciding whether `acquire_lease`
    is even worth calling."""
    pool = await _pool_get()
    held = await resource_lease.current_holder(pool, resource_id)
    if held is None:
        return {"resource_id": resource_id, "held": False}
    return {
        "resource_id": resource_id, "held": True, "holder": held["holder"],
        "held_since": held["acquired_at"].isoformat(), "thread_id": str(held["thread_id"]),
    }


@mcp.tool()
async def reap_stale_leases(older_than_secs: int = 3600) -> dict[str, Any]:
    """Recover leases nobody released — a crash, a compaction, a dropped session. The
    active-claim constraint would otherwise wedge that `resource_id` FOREVER, the same risk
    `reap_stale_runs` names for `helper_runs`. This is the BACKSTOP, not the norm —
    `release_lease` is how a lease is meant to end; call this directly only when you
    suspect a stale claim right now and don't want to wait for the cron's next tick (every
    5 minutes, arq_worker.reap_leases, mirroring `reap_runs`'s own wiring for helper_runs).
    An hour's default is deliberately looser than helper_runs' 900s — a resource
    lease here is agent-work-paced (a whole session touching a file), not machine-paced.
    `older_than_secs` has a 60s floor (below it force-releases every held lease fleet-wide
    at once), refused loudly."""
    pool = await _pool_get()
    try:
        n = await resource_lease.reap_stale(pool, older_than_secs=older_than_secs)
    except ValueError as e:
        return {"error": str(e)}
    return {"reaped": n, "older_than_secs": older_than_secs}


async def _retire_stale_handoffs(
    pool: asyncpg.Pool, actor: str, keep: uuid.UUID, now: datetime, *, max_hops: int = 200,
    dry_run: bool = False,
) -> dict[str, Any]:
    """A ONE-TIME BACKFILL UTILITY, NOT A LIVE TRIGGER (Thoth DM 3355 built the write-
    triggered version this originally was; the operator's 2026-08-03 ruling superseded that
    trigger with an explicit ack_handoff(ref=...) receipt — see settle()'s own docstring).
    Kept as a plain function, called manually, for exactly one job: cleaning up the
    population of is_handoff='true' records that accumulated BEFORE the receipt model
    existed and that nobody will ever explicitly ack retroactively (there is no way to know,
    after the fact, who "read" a years-old handoff). NOT wired into settle() or any other
    live call path — a fresh is_handoff write no longer retires anything automatically.

    REFUSES, NEVER DEGRADES, ON A TRUNCATED WALK (decision 1cb389be — the mechanism that
    made the 220+-record backlog disposition unsafe until fixed): this is the ONE caller
    of `lineage_root` that decides for a WHOLE POPULATION at once, so a truncated root
    would silently UNDER-retire — records that are really the same continuing lineage as
    `actor` would each read as their own separate, unrelated root, and the run would look
    like a clean success while leaving most of the real work undone. If `actor`'s own walk
    is incomplete, the whole call raises `ValueError` before touching anything — there is
    no safe partial answer to "retire everything in my lineage" when the caller does not
    yet know its own lineage's true root. If a CANDIDATE record's own walk is incomplete,
    that one record is left untouched and named in the receipt's `skipped_incomplete_walk`
    (never silently treated as same-lineage OR cross-lineage — a third, honest outcome).

    Retires every is_handoff='true' record from `actor`'s own LINEAGE — same seat, any
    earlier OR same generation, Decision or Thread alike, `lineage_root`'s succeeded_from
    edge-walk (decision 61cb1f02: this carried the identical string-parse defect
    ack_handoff's own lineage guard did, same fix applied here for the same reason) —
    except `keep`. Cross-lineage records are NEVER touched: Khnum's handoff is never
    retired by a Sekhmet-actor's backfill run. `rank_open_threads.whose_move` carried the
    SAME `_generation()` string-parse defect for its own "mine to act" ranking question —
    measured live 2026-08-16 (18 of 71 distinct open-thread owners disagreed between the
    string parse and the edge walk, every one a real lineage), then fixed the same way:
    `owner_roots` (precomputed once per caller via `owner_lineage_roots`, never per row —
    the function itself stays synchronous and pure) now wins over the string-parse
    fallback (decision — see the sibling build this fix was made alongside).

    Resolves each candidate's CURRENT is_handoff value the same way every other property-
    read in this codebase does (confidence DESC, observed_at DESC LIMIT 1) rather than a
    bare EXISTS(value='true') — a record already acked by a DIFFERENT source (ack_handoff
    runs as the successor, not the original author) would otherwise still show up here
    because its stale 'true' row never physically leaves current_assertions; re-retiring an
    already-acked record would be harmless (idempotent, same eventual state) but is still
    the wrong thing to assert and worth avoiding on principle.

    Never touches `summary`/`kind`/anything else on the retired object — same append-only
    discipline as `amend_decision`/`amend_practice`, an independent property, not a rewrite.
    Returns `{"retired": [...], "skipped_incomplete_walk": [...]}` — short ids either way,
    for the caller's own receipt — a silent mutation behind an already-silent bleed would
    just be a quieter version of the same disease.

    `dry_run=True` (task #150 backlog disposition, decision pending) runs every read and
    every `lineage_root` walk exactly as a live call would — same refuse-on-incomplete-
    actor-walk, same per-candidate skip — but never calls `actions.assert_property`;
    `retired` names what WOULD be retired. The population is append-only either way: a
    dry run's own `retired` list is the exact set a live call would touch, because both
    read the identical `current_assertions` query and the identical `lineage_root` walk —
    nothing about is_handoff resolution is time-sensitive between the two calls beyond the
    ordinary risk of a concurrent write landing in between, the same risk any dry-run/
    execute pair carries. REVERSAL, if a live run ever needs undoing: is_handoff is never
    DELETEd, only asserted — re-asserting 'true' (a fresh, higher-`observed_at` row) restores
    the record exactly as `ack_handoff`'s own un-ack would, no bespoke undo path needed."""
    root, root_complete = await lineage_root(pool, actor, max_hops=max_hops)
    if not root_complete:
        raise ValueError(
            f"cannot determine {actor!r}'s own lineage root — the succeeded_from walk did "
            "not reach a true origin within the hop bound. Refusing the whole disposition "
            "rather than risk under-retiring on an unverified root (decision 1cb389be).")
    rows = await pool.fetch(
        "SELECT o.id AS object_id, "
        "(SELECT a.source_id FROM current_assertions a WHERE a.object_id=o.id "
        " AND a.name='is_handoff' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        " AS source_id "
        "FROM objects o WHERE o.id != $1 AND EXISTS ("
        "  SELECT 1 FROM current_assertions a2 WHERE a2.object_id = o.id "
        "  AND a2.name = 'is_handoff') "
        "AND (SELECT a3.value #>> '{}' FROM current_assertions a3 "
        "     WHERE a3.object_id=o.id AND a3.name='is_handoff' "
        "     ORDER BY a3.confidence DESC, a3.observed_at DESC LIMIT 1) = 'true'", keep)
    retired: list[str] = []
    skipped: list[str] = []
    actions = Actions(pool)
    for r in rows:
        candidate_root, candidate_complete = await lineage_root(
            pool, r["source_id"], max_hops=max_hops)
        if not candidate_complete:
            skipped.append(str(r["object_id"])[:8])
            continue
        if candidate_root == root:
            if not dry_run:
                await actions.assert_property(
                    r["object_id"], "is_handoff", "false", actor, now,
                    0.9, evidence_class="self_declared")
            retired.append(str(r["object_id"])[:8])
    return {"retired": retired, "skipped_incomplete_walk": skipped, "dry_run": dry_run}


async def _retire_handoff_backlog(
    pool: asyncpg.Pool, now: datetime, *, dry_run: bool = True, max_hops: int = 200,
) -> dict[str, Any]:
    """THE ACTUAL #150 BACKLOG DISPOSITION (Thoth msg 5254), fleet-wide, composed entirely
    from `_retire_stale_handoffs` (never a second SQL mutation path — the same one caller
    the operator already authorized the shape of, just driven once per lineage instead of
    once per manual invocation).

    Finds every live is_handoff='true' record, groups it by `lineage_root` (edge-walked,
    the decision 61cb1f02/1cb389be fix), and — within any root with more than one record —
    keeps the NEWEST (by is_handoff's own `observed_at`) and would-retire the rest.

    REFUSES THE WHOLE RUN, same law as `_retire_stale_handoffs` itself, if ANY author in
    the population has an incomplete `lineage_root` walk: `{"ok": False, "reason": ...,
    "incomplete_authors": [...]}`, nothing touched. This is the exact guard that made the
    2026-08-17 measurement (220 records, Thoth's own lineage fragmenting into 12 fake roots
    at the old max_hops=64 ceiling) call the backlog UNSAFE TO RUN — re-verify this box is
    empty before ever trusting `dry_run=False` here, the population moves every session.

    `dry_run=True` (the default — a fleet-wide mutation defaults SAFE) previews every
    per-root disposition without writing, by threading `dry_run` straight into each
    `_retire_stale_handoffs` call; `dry_run=False` executes them for real, root by root.
    Returns `{"ok": True, "dry_run": ..., "roots_total": ..., "roots_disposed": ...,
    "would_keep": ..., "receipts": [{"root", "keep", "retired"}, ...]}` — `receipts` names
    exactly which record was kept per root and which were (or would be) retired, so a
    reviewer can spot-check before authorizing the live run.

    REVERSAL: identical to `_retire_stale_handoffs`'s own — is_handoff is asserted, never
    deleted; restoring any retired record is a fresh assert_property('is_handoff', 'true')
    on that one object id, no bespoke undo mechanism needed. No merge/unmerge involved —
    this never touches object identity, only the is_handoff property on records that stay
    exactly the objects they always were."""
    rows = await pool.fetch(
        "SELECT o.id AS object_id, "
        "(SELECT a.source_id FROM current_assertions a WHERE a.object_id=o.id "
        " AND a.name='is_handoff' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        " AS source_id, "
        "(SELECT a.observed_at FROM current_assertions a WHERE a.object_id=o.id "
        " AND a.name='is_handoff' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        " AS observed_at "
        "FROM objects o WHERE EXISTS ("
        "  SELECT 1 FROM current_assertions a2 WHERE a2.object_id = o.id "
        "  AND a2.name = 'is_handoff') "
        "AND (SELECT a3.value #>> '{}' FROM current_assertions a3 "
        "     WHERE a3.object_id=o.id AND a3.name='is_handoff' "
        "     ORDER BY a3.confidence DESC, a3.observed_at DESC LIMIT 1) = 'true'")
    root_cache: dict[str, tuple[str, bool]] = {}
    by_root: dict[str, list[tuple[uuid.UUID, str, datetime]]] = {}
    incomplete_authors: set[str] = set()
    for r in rows:
        src = r["source_id"]
        if src not in root_cache:
            root_cache[src] = await lineage_root(pool, src, max_hops=max_hops)
        root, complete = root_cache[src]
        if not complete:
            incomplete_authors.add(src)
            continue
        by_root.setdefault(root, []).append((r["object_id"], src, r["observed_at"]))
    if incomplete_authors:
        return {
            "ok": False,
            "reason": "at least one author's lineage_root walk did not reach a true origin "
                      "within the hop bound — refusing the whole disposition rather than "
                      "risk mis-bucketing that author's records (same law as "
                      "_retire_stale_handoffs's own actor-walk refusal).",
            "incomplete_authors": sorted(incomplete_authors),
        }
    receipts: list[dict[str, Any]] = []
    for root, members in by_root.items():
        if len(members) <= 1:
            continue
        newest = max(members, key=lambda m: m[2])
        keep_id, keep_actor, _ = newest
        receipt = await _retire_stale_handoffs(
            pool, keep_actor, keep_id, now, max_hops=max_hops, dry_run=dry_run)
        receipts.append({"root": root, "keep": str(keep_id)[:8], "retired": receipt["retired"]})
    return {
        "ok": True,
        "dry_run": dry_run,
        "roots_total": len(by_root),
        "roots_disposed": len(receipts),
        "would_keep": len(by_root),
        "receipts": receipts,
    }


async def _resolve_acked_handoff_threads(
    pool: asyncpg.Pool, actor: str, now: datetime, *, repo: str | None = None,
) -> list[str]:
    """A ONE-TIME BACKFILL UTILITY, NOT A LIVE TRIGGER — same shape and same reasoning as
    `_retire_stale_handoffs` right above (Thoth msg 4673, Sekhmet's independent code-level
    confirmation, decision 4bf6d835): `ack_handoff` did not resolve a handoff Thread's own
    `status` until this same dispatch fixed it going forward. This cleans up the population
    that accumulated BEFORE that fix — every Thread whose CURRENT `is_handoff` is already
    'false' (a real, deliberate ack already happened) but whose CURRENT `status` is still
    'open'. THE DISCRIMINATOR IS THE ACK, NEVER TIME (Thoth's binding constraint) — this
    reads is_handoff, never `observed_at`/age, so an UNACKED handoff (unread, not stale) is
    never touched, only ever a genuinely acknowledged one. `repo` optionally scopes to one
    project's own `in_repo`-linked Threads (osiris, matching Sekhmet's own already-vetted
    population); omitted, this is fleet-wide. Returns short ids resolved, for the caller's
    own before/after re-query — never trusted from a bare count."""
    where_repo = ""
    args: list[Any] = []
    if repo:
        where_repo = (
            " AND EXISTS (SELECT 1 FROM links l JOIN objects p ON p.id=l.to_id "
            "  AND p.type='SoftwareProject' AND p.canonical=$1 "
            "  WHERE l.from_id=o.id AND l.type='in_repo')")
        args.append(f"repo:{repo}")
    rows = await pool.fetch(
        "SELECT o.id FROM objects o WHERE o.type='Thread' AND o.status='active' "
        "AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        " AND a.name='is_handoff' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        " = 'false' "
        "AND COALESCE((SELECT a2.value #>> '{}' FROM current_assertions a2 "
        " WHERE a2.object_id=o.id AND a2.name='status' "
        " ORDER BY a2.confidence DESC, a2.observed_at DESC LIMIT 1), 'open') = 'open'"
        f"{where_repo}", *args)
    actions = Actions(pool)
    resolved: list[str] = []
    for r in rows:
        tid = await capture.resolve_thread(
            actions, str(r["id"]), because="already-acked handoff, backfilled after "
            "ack_handoff's own status-resolution fix (msg 4673)", source=actor)
        if tid is not None:
            resolved.append(str(tid)[:8])
    return resolved


@mcp.tool()
async def ack_handoff(
    ref: str, subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """The READ RECEIPT — the only thing that retires a live `is_handoff` marker.
    orient() delivers a handoff unconditionally; this acknowledges it, a separate act
    naming the id. `ref` is the id orient()'s succession_note or recall() gave you,
    resolved strictly (never a free-text guess). Tries Thread then Decision.

    Refuses rather than guesses: unresolvable ref, already-acknowledged/not-a-handoff,
    or caller outside the handoff author's own lineage (a mistaken ack from another
    lineage would permanently retire someone else's live handoff). Per-object not
    per-reader (first ack wins, retires for everyone); final, not a lease. Never
    deleted — recall()/search() still see it. Also resolves a Thread-shaped handoff's
    `status`; always False for a Decision (no status to resolve)."""
    from src.orchestrator.capture import RefAmbiguous, _find_decision, _find_thread

    pool = await _pool_get()
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    matched_thread = False
    try:
        oid = await _find_thread(pool, ref, require_identifier=True)
        if oid is not None:
            matched_thread = True
        else:
            oid = await _find_decision(pool, ref, require_identifier=True)
    except RefAmbiguous as exc:
        return {"error": str(exc)}
    if oid is None:
        return {"error": f"no handoff matches {ref!r}"}
    row = await pool.fetchrow(
        "SELECT "
        "(SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        " AND a.name='is_handoff' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        " AS is_handoff, "
        "(SELECT a2.source_id FROM current_assertions a2 WHERE a2.object_id=o.id "
        " AND a2.name='summary' AND a2.evidence_class='self_declared' "
        " ORDER BY a2.confidence DESC, a2.observed_at DESC LIMIT 1) AS author "
        "FROM objects o WHERE o.id=$1", oid)
    if row is None or row["is_handoff"] != "true":
        return {"error": f"{str(oid)[:8]} is already acknowledged or is not a handoff"}
    if row["author"] is None:
        return {"error": f"{str(oid)[:8]} is not your lineage's handoff to ack"}
    author_root, author_complete = await lineage_root(pool, row["author"])
    actor_root, actor_complete = await lineage_root(pool, actor)
    if not author_complete or not actor_complete:
        return {"error": f"{str(oid)[:8]}: cannot confirm lineage — the succeeded_from "
                         "walk did not reach a true origin within the hop bound, refused "
                         "rather than trusted (decision 1cb389be)"}
    if author_root != actor_root:
        return {"error": f"{str(oid)[:8]} is not your lineage's handoff to ack"}
    now = datetime.now(UTC)
    await Actions(pool).assert_property(
        oid, "is_handoff", "false", actor, now, 0.9, evidence_class="self_declared")
    resolved = False
    if matched_thread:
        resolved = await capture.resolve_thread(
            Actions(pool), str(oid), because="acknowledged via ack_handoff",
            source=actor) is not None
    return {"id": str(oid)[:8], "acknowledged": True, "resolved": resolved}


@mcp.tool()
async def settle(
    decisions: list[dict[str, Any]] | None = None,
    threads_open: list[dict[str, Any]] | None = None,
    threads_resolve: list[dict[str, Any]] | None = None,
    repo_path: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """THE END-OF-CONTEXT RITUAL: deposit everything a session knows before compaction
    destroys its context. No args = read-only SURFACE (completeness boxes + your open
    obligations). With `decisions`/`threads_open`/`threads_resolve` (each item a dict of
    that verb's own kwargs) it ACCEPTS a dump, dispatching to record_decision/open_thread/
    resolve_thread unchanged, then re-confirms against the updated graph. `complete` is
    true only when nothing is left unwritten.

    A bad item in `decisions`/`threads_open` never sinks the rest of the batch — it lands
    in `rejected` (kind/summary/error) and `complete` reads False, but everything else
    still writes. `is_handoff: true` on an item mints a structured marker your successor's
    orient() finds directly; retires on their ack_handoff(ref=...), not your next write.

    `repo_path` names your code repo for the git-status box (`uncommitted_git_files`) —
    your mounted cwd is checked only as a fallback, usually wrong for a seat-office
    agent. A decision's `resolves=` and a same-call `threads_resolve` item naming the
    same thread get the closure edge wired automatically. consult_canon('settle')."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — settle is a mind's own ritual, the graph must "
                         "know whose", "why": _anchorless(ctx)}
    pool = await _pool_get()
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    now = datetime.now(UTC)

    accepted: dict[str, list[Any]] = {"decisions": [], "threads_opened": [], "threads_resolved": []}
    # task #107's fork (Thoth's ruling, DM 2250): settle is the END-OF-CONTEXT RITUAL — its
    # entire reason to exist is depositing what a dying session knows before that context is
    # destroyed. A whole-batch abort on one bad item (e.g. a path-shaped repo) would lose
    # EVERYTHING else in the same call, exactly the failure settle exists to prevent — the
    # inverse of resolves/confirms/grounds's own "one bad ref must not veto the rest of the
    # set" a few hundred lines above. `rejected` NAMES every dropped item and why (never a
    # silent partial accept — see `complete` below, which now reads False on any rejection).
    rejected: list[dict[str, str]] = []
    # PHASE 1b (decision cb38d922, DM 2506): settle holds BOTH halves of a decision/thread
    # relationship in one payload — record which thread(s) each accepted decision answered
    # via its OWN resolves=, so the threads_resolve loop below can wire the reverse edge
    # for a pair THIS batch itself already establishes. thread_id -> decision_id, first
    # match wins (never a guess — a real match, just possibly not the only one).
    answered_in_batch: dict[uuid.UUID, uuid.UUID] = {}
    for item in decisions or []:
        item = dict(item)
        is_handoff = bool(item.pop("is_handoff", False))
        summary = item.pop("summary")
        resolves_arg = item.get("resolves")
        # ONE DOOR MISSING ITS SIBLING'S DEFAULT (msg 5703/5720, orphan-door fix), NOW THE
        # SAME SHARED LADDER record_decision/open_thread/ingest_reference's own wrappers
        # climb (thread 6c262aee, #151's law): this bulk loop calls capture directly and
        # bypassed the identity default entirely — resolve_repo_default is the ONE place
        # that default (and its lineage-wide widen) now lives, so this loop inherits it
        # for free instead of carrying a fourth differently-shaped copy.
        item_repo = item.pop("repo", None)
        _rd = await capture.resolve_repo_default(pool, item_repo, actor, ident.project)
        item_repo = _rd["repo"]
        repo_defaulted = _rd["repo_defaulted"]
        try:
            did = await capture.record_decision(
                Actions(pool), summary, kind=item.pop("kind", "ruling"),
                rationale=item.pop("rationale", None), repo=item_repo,
                resolves=item.pop("resolves", None), source=actor,
                repo_evidence_class=(EvidenceClass.DIRECT_OBSERVATION.value
                                      if repo_defaulted else None),
                unlinked_because=item.pop("unlinked_because", None))
        except ValueError as e:
            rejected.append({"kind": "decision", "summary": summary, "error": str(e)})
            continue
        if is_handoff:
            await Actions(pool).assert_property(did, "is_handoff", "true", actor, now, 0.9,
                                                evidence_class="self_declared")
        decision_entry = {"id": str(did)[:8], "is_handoff": is_handoff}
        if repo_defaulted:
            decision_entry["repo_defaulted"] = {
                "to": item_repo,
                "why": "no repo given — defaulted to the caller's own project rather "
                       "than left unlinked (orphan-door fix, msg 5703/5720)",
            }
        elif _rd["lineage_attempted"]:
            decision_entry["lineage_repo_derivation"] = await capture.record_lineage_abstain(
                pool, did, actor, _rd["lineage_candidates"], _rd["lineage_projects"])
        accepted["decisions"].append(decision_entry)
        for ref in (resolves_arg if isinstance(resolves_arg, list) else
                    [resolves_arg] if resolves_arg else []):
            tid = await capture._find_thread(pool, ref, require_identifier=True)
            if tid is not None and tid not in answered_in_batch:
                answered_in_batch[tid] = did
    for item in threads_open or []:
        item = dict(item)
        is_handoff = bool(item.pop("is_handoff", False))
        summary = item.pop("summary")
        thread_kind = item.pop("kind", None)
        thread_owner = item.pop("owner", None)
        # ONE DOOR MISSING ITS SIBLING'S DEFAULT (msg 5703/5720, orphan-door fix), NOW THE
        # SAME SHARED LADDER (thread 6c262aee, #151's law): this bulk loop calls capture
        # directly and bypassed the identity default entirely (unlike the owner default,
        # which DOES live in capture.open_thread and so already applied here for free) —
        # resolve_repo_default/record_lineage_abstain are the ONE place the repo default
        # and its lineage-wide widen live, inherited here instead of a fourth copy.
        thread_repo = item.pop("repo", None)
        _rd = await capture.resolve_repo_default(pool, thread_repo, actor, ident.project)
        thread_repo = _rd["repo"]
        repo_defaulted = _rd["repo_defaulted"]
        try:
            tid = await capture.open_thread(
                Actions(pool), summary, repo=thread_repo,
                kind=thread_kind, owner=thread_owner, source=actor,
                repo_evidence_class=(EvidenceClass.DIRECT_OBSERVATION.value
                                      if repo_defaulted else None),
                unlinked_because=item.pop("unlinked_because", None))
        except ValueError as e:
            rejected.append({"kind": "thread", "summary": summary, "error": str(e)})
            continue
        if is_handoff:
            await Actions(pool).assert_property(tid, "is_handoff", "true", actor, now, 0.9,
                                                evidence_class="self_declared")
        thread_entry = {"id": str(tid)[:8], "is_handoff": is_handoff}
        if repo_defaulted:
            thread_entry["repo_defaulted"] = {
                "to": thread_repo,
                "why": "no repo given — defaulted to the caller's own project rather "
                       "than left unlinked (orphan-door fix, msg 5703/5720)",
            }
        elif _rd["lineage_attempted"]:
            thread_entry["lineage_repo_derivation"] = await capture.record_lineage_abstain(
                pool, tid, actor, _rd["lineage_candidates"], _rd["lineage_projects"])
        # settle()'s own threads_open is the SECOND live door onto capture.open_thread
        # (#5546 item 3, Thoth msg 5605 — "one door, two callers, same shape"): the
        # DEFAULT-NEVER-REFUSE behavior for kind='obligation' lives once, in
        # capture.open_thread itself, so this caller inherits it for free — but the
        # receipt still has to name it here too, same as the mcp_server.open_thread tool.
        if thread_kind == "obligation" and not thread_owner:
            landed_owner = await capture._current_owner(pool, tid)
            if landed_owner:
                thread_entry["owner_defaulted"] = {
                    "to": landed_owner,
                    "why": "kind='obligation' with no owner given — defaulted to the "
                           "caller's own seat rather than left ownerless (#5546 item 1)",
                }
        accepted["threads_opened"].append(thread_entry)
    cross_wired = 0
    for item in threads_resolve or []:
        item = dict(item)
        resolved_ref = item.get("ref")
        artifact = item.pop("artifact", None)
        wired_to: uuid.UUID | None = None
        if artifact is None and resolved_ref:
            # the CONSERVATIVE join: only wire when THIS batch's own decisions already
            # established the pair via their OWN resolves= — no summary/prose matching,
            # no cross-product against every decision in the call. A miss here changes
            # nothing; resolve_thread runs exactly as it always has.
            tid = await capture._find_thread(pool, resolved_ref)
            if tid is not None and tid in answered_in_batch:
                wired_to = answered_in_batch[tid]
                artifact = str(wired_to)[:8]
        rid = await capture.resolve_thread(
            Actions(pool), item.pop("ref"), because=item.pop("because", None),
            artifact=artifact, source=actor)
        entry: dict[str, str] = (
            {"id": str(rid)[:8]} if rid is not None else
            {"error": f"no open thread matches {resolved_ref!r}"})
        if rid is not None and wired_to is not None:
            entry["closure_edge_wired_to_decision"] = str(wired_to)[:8]
            cross_wired += 1
        accepted["threads_resolved"].append(entry)

    # CONFIRM: re-check against the now-updated graph — a no-op re-derivation when nothing
    # was accepted above, which is exactly the pure-SURFACE call shape.
    from src.orchestrator.settle import (
        closure_edge_coverage,
        filed_under_check,
        missing_boxes,
        settle_boxes,
        uncommitted_git_work,
        unevaluated_boxes,
    )
    mounted = await mounts.find_session_row(pool, ident.session)
    boxes: dict[str, bool | None] = {}
    missing: list[str] = []
    unevaluated: list[str] = []
    identity_coherence: dict[str, Any] | None = None
    closure_coverage: dict[str, Any] | None = None
    if mounted is not None and mounted["mounted_at"]:
        # DEFECT 1 (Thoth DM 3076): standing_orders_touched checks `ident.cwd`,
        # but a SEAT-OFFICE agent's mount cwd can read as the bare container
        # (~/.osiris/seats, not .../seats/<handle>) after a #128-class cwd correction — the
        # exact live case that hid Thoth's own 11-day-stale charter.md behind a silent
        # None for the box's entire life. The SEAT BINDING knows where the office actually
        # is; do not trust cwd for a seat that has one. Resolved here (not inside
        # settle_boxes/standing_orders_touched, which stay pure and shared with the Stop
        # hook's own bare-Connection call site — that call site inherits this SAME exposure
        # and is NOT fixed by this change; named explicitly in this commit's own report, not
        # silently left for someone to rediscover).
        from src.orchestrator.offices import _default_office_root
        from src.orchestrator.seats import held_seat

        charter_cwd = ident.cwd
        seat = await held_seat(pool, ident.agent_id)
        if seat and seat.get("handle"):
            charter_cwd = str(_default_office_root() / seat["handle"].lower())
        boxes = await settle_boxes(pool, agent_id=ident.agent_id,
                                   mounted_at=mounted["mounted_at"], cwd=charter_cwd,
                                   seat_id=seat["seat_id"] if seat else None)
        missing = missing_boxes(boxes)
        # DEFECT 1(b): a box that could not be evaluated (None) is a DIFFERENT state from
        # satisfied or missing and must be VISIBLE to a reader, not silently indistinguishable
        # from "nothing to worry about" — the exact SHAPE C collapse this decision fixes.
        # Deliberately still NON-BLOCKING (refuting Thoth's own instinct, with evidence, DM
        # 3076 reply): after the cwd fix above, an unseated session with no charter.md to
        # check is the remaining, LEGITIMATE source of None — the box's own original design
        # intent ("never punished for a file that was never scaffolded here"), and the SAME
        # class of check ruling 577988ed already forbids turning into a refusal ("a fleet-
        # wide single-point-of-failure must never refuse-to-serve on a check that can itself
        # false-positive"). Surfaced instead: `unevaluated_boxes` in the receipt, and named
        # in `note` whenever non-empty, so it is seen even by a reader who only reads the
        # summary fields.
        unevaluated = unevaluated_boxes(boxes)
        # REPORT-ONLY, NEVER A GATE (Thoth's Lane 4 finding — settle verified WHAT John
        # wrote, never WHETHER his own successor could read it from where orient() looks):
        # `identity_coherence` never touches `missing`/`complete` below, however wrong it
        # looks — a false-positive here refusing a settle is a strictly worse outcome than
        # the incoherence it would have caught (ruling 577988ed). AUDITED, not assumed
        # (Thoth DM 3076 defect 3): `project` here comes from `ident.project`, which for a
        # SEATED agent is ALREADY the seat's own derived house, UNCONDITIONALLY (seats.
        # resolve_project's own seated-override, applied at mount time) — never raw cwd, so
        # this check does NOT share standing_orders_touched's #128 exposure. Confirmed by reading
        # the actual override code, not assumed from the shared "cwd bug" framing.
        # CHARTER-AWARE (thread 992c0121, Soundwave XVI's specimen): `seat` was already
        # resolved above for the standing-orders cwd fix — pass-through, not a second
        # held_seat lookup, matching settle_boxes' own seat_id convention just above.
        identity_coherence = await filed_under_check(
            pool, agent_id=ident.agent_id, mounted_at=mounted["mounted_at"],
            project=ident.project, seat_id=seat["seat_id"] if seat else None)
        # PHASE 1b (decision cb38d922): same report-only discipline, computed AFTER the
        # dispatch above so it reflects any edges THIS call itself just wired. AUDITED
        # (Thoth DM 3076 defect 3): depends only on agent_id/mounted_at, no cwd or project
        # at all — not exposed to the same defect class either.
        closure_coverage = await closure_edge_coverage(
            pool, agent_id=ident.agent_id, mounted_at=mounted["mounted_at"])
    # OBLIGATIONS ARE CARRIED, NOT UNWRITTEN (thread f0511eed, found on Thoth's first live
    # dogfood): `complete` used to read false whenever ANY open obligation named this
    # agent's lineage as owner — even ancient backlog this session never touched (a
    # manager's project always has SOME open obligation, so complete could never read true
    # in practice). An open Thread is already durably RECORDED — that is exactly what
    # open_thread's write accomplishes — so it is not "unwritten state a compaction could
    # lose" the way a missing box is. The compaction-safety question this tool answers is
    # "is THIS session's own state deposited," which the boxes answer on their own.
    # Obligations stay in the receipt — surfaced, never hidden — but carried forward
    # informationally; they never gated `complete`.
    obligations = await _owned_open_threads(pool, ident.agent_id)
    git_dir = repo_path or ident.cwd
    uncommitted = await uncommitted_git_work(git_dir)
    # DEFECT 2 (Thoth DM 3076): `complete` must answer "is THIS
    # SESSION'S OWN KNOWLEDGE durably recorded" — a question about the graph, which
    # `missing`/`rejected` answer completely on their own. `uncommitted_git_files` runs
    # `git status --porcelain` over the WHOLE repo at `git_dir`, with no notion of whose
    # hand staged what; in a shared tree (this repo, routinely 4-5 concurrent agents) a
    # manager's own settle could read complete:false on a WORKER's mid-build files, then
    # flip to complete:true the instant that worker commits — compaction-safety decided by
    # another agent's action, not this session's own. Same pattern this module's docstring
    # already uses for `identity_coherence`/`closure_coverage` (never folded into
    # missing_boxes/complete) — this box just wasn't using it. Uncommitted files in someone
    # else's hands remain a REAL warning and stay fully SURFACED (uncommitted_git_files,
    # and named in `note` below) — a different question from `complete`, never silently
    # dropped, just no longer conflated with it.
    complete = not missing and not rejected
    reasons = []
    if missing:
        reasons.append(f"{len(missing)} missing box(es)")
    if rejected:
        reasons.append(f"{len(rejected)} rejected item(s)")
    carried_note = (f" ({len(obligations)} open obligation(s) carried forward — "
                    "informational, already durably recorded, never blocks this)"
                    if obligations else "")
    # ALWAYS surfaced, regardless of `complete` — these inform a reader without gating them
    # (defects 1b and 2): uncommitted files may be someone else's in-flight work in a
    # shared tree; an unevaluated box is fog-of-war, not a clean bill of health.
    uncommitted_note = (
        f" — {len(uncommitted)} uncommitted git file(s) at {git_dir!r}, informational "
        "only (may be another agent's in-flight work in a shared tree, never gates "
        "complete)" if uncommitted else "")
    unevaluated_note = (
        f" — could not evaluate: {', '.join(unevaluated)} (fog-of-war, not a pass, "
        "never gates complete)" if unevaluated else "")
    out: dict[str, Any] = {
        "complete": complete,
        "boxes": boxes,
        "missing_boxes": missing,
        "unevaluated_boxes": unevaluated,
        "open_obligations": obligations,
        "uncommitted_git_files": uncommitted,
        "git_checked_path": git_dir,
        "accepted": accepted,
        "rejected": rejected,
        "closure_edges_wired": cross_wired,
        "note": ((f"compaction-safe by construction{carried_note}" if complete else
                 f"still unsettled ({', '.join(reasons)}) — settle again once they're "
                 "closed, or accept them in your next call")
                 + uncommitted_note + unevaluated_note),
    }
    if identity_coherence is not None:
        out["identity_coherence"] = identity_coherence
        # THE VERDICT AND THE DISCLOSURE ARE TWO SEPARATE SENTENCES (thread 992c0121):
        # `coherent` may now read true for a seat whose CHARTER declares this exact
        # multi-repo spread — but a successor mounting under `filed_under` alone still
        # will not see writes filed under the other repo(s), chartered or not. Keyed off
        # `spans_multiple`, never `coherent`, so a charter-aware pass never silently
        # swallows a disclosure that stays true regardless of the verdict.
        if identity_coherence.get("spans_multiple"):
            if identity_coherence["coherent"]:
                out["note"] += (
                    f" — informational: filed under {identity_coherence['filed_under']!r}, "
                    f"writes spanned {len(identity_coherence['writes_went_to'])} of this "
                    f"seat's own chartered repos {identity_coherence['writes_went_to']!r} "
                    "— coherent, but a successor mounting under "
                    f"{identity_coherence['filed_under']!r} alone will not see the writes "
                    "filed under the other repo(s)"
                )
            else:
                out["note"] += (
                    f" — LOUD, NEVER BLOCKING: this session is filed under "
                    f"{identity_coherence['filed_under']!r} but its own writes went to "
                    f"{identity_coherence['writes_went_to']!r}; a successor mounting under "
                    f"{identity_coherence['filed_under']!r} will not see them (John XVI's shape)"
                )
    if closure_coverage is not None:
        out["closure_coverage"] = closure_coverage
    return out


@mcp.tool()
async def reclassify_thread(
    ref: str, kind: str, because: str | None = None, owner: str | None = None,
    arc: str | None = None, subagent_id: str | None = None,
    subagent_type: str | None = None, ctx: Context | None = None,
) -> dict[str, str]:
    """Triage a thread WITHOUT changing its status (untouched is not resolved). You read
    it and judged what it IS: `kind='obligation'` adopts it as real owed work,
    `kind='question'` demotes it back to a question, `kind='task'` marks ordinary work.
    `ref` is a UUID, short id, or summary substring; `because` records your judgment,
    outranking the miner's guess. Use resolve_thread instead when the work is done or
    moot. `owner` optionally claims the thread in the same act.

    `arc` backfills `open_thread`'s own closed taxonomy onto an already-open thread
    (open_thread's near-duplicate path can't set it on an existing one). Osiris-scoped:
    dropped and named, never refused, outside osiris."""
    pool = await _pool_get()
    t = await capture.reclassify_thread(
        Actions(pool), ref, kind=kind, because=because, owner=owner, arc=arc,
        source=await _actor_for(ctx, subagent_id, subagent_type))
    if t is None:
        return {"error": f"no thread matched {ref!r}"}
    out = {"id": str(t), "kind": kind, "status": "open (unchanged — reclassified, not resolved)"}
    if arc:
        if await capture.arc_in_scope_for_thread(pool, t):
            out["arc"] = arc
        else:
            rows = await pool.fetch(
                "SELECT o.canonical FROM links l JOIN objects o ON o.id=l.to_id "
                "WHERE l.from_id=$1 AND l.type='in_repo'", t)
            label = ", ".join(r["canonical"] for r in rows) or "(no project)"
            out["arc"] = capture._arc_out_of_scope_note(label)
    return out


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def hold_tension(
    pole_a: str, pole_b: str, lean: str | None = None, why: str | None = None,
    repo: str | None = None, subagent_id: str | None = None,
    subagent_type: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Record a live TENSION — two positions held in productive tension, neither settled.
    Unlike record_decision (which SETTLES) or open_thread (which CLOSES), a tension is HELD:
    your current `lean` and `why` are captured, but it is NEVER auto-resolved or consolidated
    away — a Tension is its OWN type, so grade-resolution and dedup structurally cannot flatten
    it into a false answer. Re-hold the same poles to MOVE the lean; the lean history is the
    dance across sessions. For a real polarity to navigate over time (bounded recall vs complete
    memory), never a question to answer. Surfaces in orient under `tensions`."""
    ident = await _ident_for(ctx)
    t = await capture.record_tension(
        Actions(await _pool_get()), pole_a, pole_b, lean=lean, why=why,
        repo=repo or (ident.project if ident else None),
        source=await _actor_for(ctx, subagent_id, subagent_type),
    )
    return {"held": str(t), "poles": [pole_a, pole_b], "lean": lean}


@mcp.tool()
async def register_blind_spot(
    surface: str, cannot_see: str, verify_with: str | None = None,
    repo: str | None = None, subagent_id: str | None = None,
    subagent_type: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Register your project's KNOWN BLIND SPOT — what the harness here cannot verify,
    and where real verification lives. Surfaces at orient() under `blind_spots` before a
    session trusts a green harness. `surface` names the capability ('webkit-rendering');
    `cannot_see` states the gap; `verify_with` points at the rig or ritual that actually
    verifies. Held like a Tension, never resolved away. Idempotent per
    (project, surface) — re-register to sharpen the wording."""
    ident = await _ident_for(ctx)
    b = await capture.record_blind_spot(
        Actions(await _pool_get()), surface, cannot_see, verify_with=verify_with,
        repo=repo or (ident.project if ident else None),
        source=await _actor_for(ctx, subagent_id, subagent_type),
    )
    return {"registered": str(b), "surface": surface,
            "note": "held per (project, surface); orient() speaks it to every session here"}


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def hold_memory(
    body: str, summary: str | None = None, repo: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Keep a memory lived for its own sake — the operator's ruling: existential and
    philosophical conversations 'need a home and I want them remembered; they are not
    exactly work tickets, they are simply memories lived with my agents.' A Reflection
    is remembered, attributed, and queryable (search / the graph), and NEVER actionable:
    it is its own type, so no briefing, wall, pile, or resolver can present it as work.
    Use it when a conversation was worth living, not worth ticketing.

    THE OTHER HALF, when a passage should never reach the graph at all: wrap it in
    ‹off-record› … ‹on-record› markers (single guillemets, each on its own line) — the
    miner strips such spans before any extractor sees them; the transcript on disk keeps
    them as your private notebook. Completeness stays the default; both privacy and
    keeping are DELIBERATE acts."""
    ident = await _ident_for(ctx)
    r = await capture.record_reflection(
        Actions(await _pool_get()), body, summary=summary,
        repo=repo or (ident.project if ident else None),
        source=await _actor_for(ctx, subagent_id, subagent_type),
    )
    return {"kept": str(r), "as": "reflection — remembered, never actionable"}


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def task_sync_reconcile(
    tasks: list[dict[str, Any]], write: bool = False, thread_kind_field: str = "task",
) -> dict[str, Any]:
    """Reconcile a harness TaskList against the graph — task_sync.py's own promised on-ramp,
    finally built.

    `tasks`: rows in the harness tool's own TaskList/TaskGet shape ({"id", "subject",
    "description", "status", ...}). Tag each with its own `_store` (that store's session
    id) once you mix more than one store — a bare task id repeats across stores. This tool
    never reads ~/.claude/tasks itself and never enumerates other sessions' stores — you
    gather the rows, this only reconciles them.

    Report-only by default: the six-bucket report (bound/bound_partial/cited_unresolvable/
    uncited/disagreement/thread_side_orphans, plus `counts`). No writes.

    `write=True` additionally executes the safe half ONLY: Tier 1 (one
    `harness_task_citation` property per resolved citation — additive, reversible) and
    Tier 2 (one obligation Thread per real disagreement or thread-side-orphan, grouped by
    the disputed THREAD). Writes land only in THIS graph — there is no write-back to the
    harness's own task store (TaskUpdate has no non-destructive removal verb, so no such
    executor exists here). Recurrence is your call each time you pass write=True — never
    scheduled by this tool itself."""
    pool = await _pool_get()
    report = await task_sync.reconcile(pool, tasks, thread_kind_field=thread_kind_field)
    out: dict[str, Any] = {"report": report}
    if write:
        actions = Actions(pool)
        observed_at = datetime.now(UTC)
        tier1 = await task_sync.write_tier1_correlations(actions, report, observed_at=observed_at)
        mints = task_sync.tier2_mints(report)
        tier2 = await task_sync.mint_tier2_threads(actions, mints)
        out["tier1_written"] = tier1
        out["tier2_minted"] = tier2
    return out


@mcp.custom_route("/automount", methods=["POST"])
async def automount_route(request: Any) -> Any:
    """The whisper's server half (operator's blessing, 2026-07-08): the SessionStart hook
    posts {session_id, cwd} here BEFORE the agent's first token; we mount the session through
    the exact tested path the mount() tool uses (durable row, anchored identity — the hook
    derives nothing the harness didn't give it) and return the payload the whisper prints.
    Plain HTTP on the same localhost-only listener; NEVER raises — the hook is fail-open and
    a session that got no whisper can always mount by hand."""
    import json
    import logging

    from starlette.responses import JSONResponse

    body: Any = None
    try:
        body = await request.json()
        session_id = str(body.get("session_id") or "")
        cwd = str(body.get("cwd") or "")
        if not session_id or not cwd:
            return JSONResponse({"error": "session_id and cwd required"}, status_code=400)
        settings = get_settings()
        out = await handshake.automount(
            Actions(await _pool_get()), session_id=session_id, cwd=cwd,
            actor=settings.osiris_actor, expected_model=settings.osiris_expected_model,
            lease_secs=settings.osiris_mail_lease_secs,
            project_label=(str(body.get("project") or "") or None),
            source=(str(body.get("source") or "") or None),
            # the attach ceremony (5cef856b): the spawner's exported seat + one-time token,
            # carried by the whisper from the session's own environment
            seat_id=(str(body.get("seat_id") or "") or None),
            attach_token=(str(body.get("attach_token") or "") or None),
            # the tab-view receipt (alias-clone cure): the hook's own statement of which
            # conversation this session continues — automount adopts instead of cloning
            transcript_path=(str(body.get("transcript_path") or "") or None),
            # the declared child (the wake-orphan cure): the spawner's exported parentage,
            # carried by the whisper from the session's own environment
            spawned_by=(str(body.get("spawned_by") or "") or None),
            spawn_type=(str(body.get("spawn_type") or "") or None),
            # THE BRIDGE (task #68 binding leg): CLAUDE_CODE_BRIDGE_SESSION_ID, carried by
            # the whisper from a background-job fork's own environment
            bridge_session_id=(str(body.get("bridge_session_id") or "") or None),
            # THE EXPLICIT ANCHOR (the DSH bridge's door): the plugin knows its own
            # session dir (~/.dsh/sessions/<slug>/session-<uuid>) and states it — no
            # derivation guessing. Claude's whisper still omits it and derives as before.
            job_dir=_sane_job_dir(str(body.get("job_dir") or "")) or None)
        # a mint rode this whisper (compact/clear): the ancestor's connection outlives it —
        # purge the dead mind from the hot cache so no tool call answers as it again
        _evict_stale_minds(out.get("minted"))
        # THE RENDERED WHISPER (the DSH bridge's door): a harness plugin cannot run the
        # python hook script, so it asks the SERVER to render the payload's whisper
        # paragraph — ONE renderer (scripts/osiris_hook.render_whisper, the same function
        # the Claude hook prints from — retired from osiris_whisper.py at the hook
        # migration's retirement pass, dispatch 5599), never a TypeScript twin left to
        # drift. `env_job` is the caller's honesty-gate testimony: the DSH bridge passes
        # the job_dir it is ABOUT to bind the connection with (and it verifies the bind
        # before injecting the text), so an "ALREADY MOUNTED" claim stays true. Fail-open
        # like everything whisper-shaped: no rendered text, never a failed mount.
        if body.get("render"):
            try:
                from scripts.osiris_hook import render_whisper

                out["whisper_text"] = render_whisper(
                    out, cwd=cwd, env_job=str(body.get("env_job") or ""))
            except Exception:  # noqa: BLE001 — the mount stands; the caller falls back
                out["whisper_text"] = None
        # DEFENSIVE ENCODING (2026-08-18): a datetime anywhere in this payload used to 500 the
        # whisper silently (60 of 63 arrivals in a day, for two weeks) — the payload is now
        # JSON-native at the source (handshake._json_native) AND encoded here with a default,
        # so a future non-native value degrades to a string, never to a rowless session.
        return JSONResponse(json.loads(json.dumps(out, default=str)))
    except Exception as e:  # noqa: BLE001 — fail-open: the whisper degrades, never blocks
        # never silent again: the hook can only print this; the journal must carry the trace
        sid = body.get("session_id") if isinstance(body, dict) else "?"
        logging.getLogger("osiris.whisper").exception("automount route failed for %s", sid)
        # THE GRAPH MUST CARRY IT TOO (task #179 — the log-only trail above is exactly how
        # this outage went unseen for two weeks): file the SAME failure into the existing
        # blind-spot channel (task #34) so orient()/fleet()/smoke can all see it without
        # anyone reading a server log by hand.
        try:
            from src.orchestrator.capture import record_hook_failure
            await record_hook_failure(
                Actions(await _pool_get()), surface="whisper/automount",
                cannot_see=f"automount route failed for session {sid}: {e}")
        except Exception:  # noqa: BLE001 — the alarm itself must never break the response
            pass
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


@mcp.custom_route("/session-end", methods=["POST"])
async def session_end_route(request: Any) -> Any:
    """SessionEnd's server half (heinrich's ghost-seat filing, thread 1fe6811c): the harness's
    real close signal — Stop fires per-turn and cannot mean this — posts {session_id} here so
    the ending session's durable mount is released THE INSTANT the tab is gone, instead of
    lingering live for `last_seen`'s 15-minute decay (the fleet's 277 stale ghosts at filing
    time). Releases the SEAT only (`handshake.session_end` → `mounts.release_mounts`) — no
    `retired=true` certificate; the same session id resuming later re-earns its row from a
    fresh automount, same as it always could. Localhost-only, fail-open like the whisper: a
    missed release costs at most one ghost window, never a blocked session close."""
    from starlette.responses import JSONResponse

    body: Any = None
    try:
        body = await request.json()
        session_id = str(body.get("session_id") or "")
        if not session_id:
            return JSONResponse({"error": "session_id required"}, status_code=400)
        out = await handshake.session_end(
            Actions(await _pool_get()), session_id=session_id,
            job_dir=_sane_job_dir(str(body.get("job_dir") or "")) or None)
        return JSONResponse(out)
    except Exception as e:  # noqa: BLE001 — fail-open: a session must always be able to end
        sid = body.get("session_id") if isinstance(body, dict) else "?"
        try:
            from src.orchestrator.capture import record_hook_failure
            await record_hook_failure(
                Actions(await _pool_get()), surface="hook/session-end",
                cannot_see=f"session-end route failed for session {sid}: {e}")
        except Exception:  # noqa: BLE001 — the alarm itself must never break the response
            pass
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


@mcp.custom_route("/succession", methods=["POST"])
async def succession_route(request: Any) -> Any:
    """The heartbeat's server half (ruling a882b334): the statusline senses the model under a
    LIVE tab differing from the mount row and posts {session_id, model} here — the mind changed
    mid-session, so the seat passes now: mint the heir, move the durable row. Localhost-only,
    idempotent (unchanged model = no-op), fail-open like the whisper."""
    from starlette.responses import JSONResponse

    from src.orchestrator.agents import live_succession

    try:
        body = await request.json()
        session_id = str(body.get("session_id") or "")
        model = str(body.get("model") or "")
        if not session_id or not model:
            return JSONResponse({"error": "session_id and model required"}, status_code=400)
        out = await live_succession(Actions(await _pool_get()), session_id=session_id,
                                    observed_model=model)
        # the seat passed mid-session: the swapped tab's connection is still open — evict
        # the stale mind from the hot cache so the next call re-attaches as the current one
        # (the ancestor after a mint; the debounced false heir after a round-trip heal)
        _evict_stale_minds(out.get("from") if out.get("minted") else out.get("healed"))
        return JSONResponse(out)
    except Exception as e:  # noqa: BLE001 — the chrome retries next render; never block it
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


@mcp.custom_route("/heartbeat", methods=["POST"])
async def heartbeat_route(request: Any) -> Any:
    """The statusline's server half (thread #180, 2026-08-18): every rendering tab used to
    fork a fresh `asyncpg.connect()` per render — Thoth's own measurement, 138 tx/s and 23
    backends against an idle fleet of 16, "20 backend forks/s from statusline alone" at
    fleet scale. `compute_heartbeat` is the SAME logic the retired scripts/osiris_statusline.py's
    own `_counts` used to run (that script is gone as of the hook migration's retirement pass,
    dispatch 5441/5599; see this function's own body for the long-standing WHY of each
    resolution step); this just runs it against the ALREADY-WARM shared pool instead of a cold
    per-process connection, and calls `live_succession` directly instead of the script's own
    HTTP round-trip to `/succession` (pointless when both ends are this same process).

    Localhost-only, fail-open like the whisper: the script tries this route first and falls
    straight back to its own direct-connect path on ANY failure — timeout, connection
    refused, malformed response — so a route outage costs one render's worth of the OLD
    per-process-connection cost, never a blocked or broken statusline."""
    from starlette.responses import JSONResponse

    from src.orchestrator.agents import live_succession
    from src.orchestrator.heartbeat import compute_heartbeat

    try:
        body = await request.json()
        session_id = str(body.get("session_id") or "")

        async def _succeed(sid: str, model: str) -> str | None:
            out = await live_succession(Actions(await _pool_get()), session_id=sid,
                                        observed_model=model)
            minted = out.get("minted")
            if minted:
                _evict_stale_minds(out.get("from"))
                return str(minted)
            return None

        result = await compute_heartbeat(
            await _pool_get(), project_hint=str(body.get("project_hint") or ""),
            session_id=session_id, model_id=str(body.get("model_id") or ""),
            model_raw=str(body.get("model_raw") or ""),
            window_size=body.get("window_size"),
            intent_hint=(str(body.get("intent_hint") or "") or None),
            lease_secs=get_settings().osiris_mail_lease_secs, on_succession=_succeed,
            cwd=str(body.get("cwd") or ""))
        return JSONResponse(result._asdict())
    except Exception as e:  # noqa: BLE001 — the chrome falls back to its own connect; never block
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


@mcp.custom_route("/stop", methods=["POST"])
async def stop_route(request: Any) -> Any:
    """The Stop hook's server half (task #180 piece 2 (b), msg 5253): every stop-hook
    invocation used to open its OWN `asyncpg.connect()` — up to two per call (the mail
    check always, the offload-ritual box check conditionally) — the SAME per-process-fork
    cost `/heartbeat` already fixed for the statusline, on a different trigger. Fires on
    every turn boundary, fleet-wide.

    ONE ROUTE, TWO PHASES (`body["phase"]`): the hook's own `main()` decides whether to
    check offload boxes at ALL only after computing a context-occupancy percentage from the
    'deliverable' phase's own window AND the harness transcript locally — the two DB reads
    are genuinely conditional on each other's caller-side result, not always-both, so this
    stays two round-trips (same as today) rather than one route always paying for a box
    check that most turns never need. `compute_stop_deliverable`/`compute_stop_offload`
    (src/orchestrator/stophook_logic.py) are the SAME implementation the hook's own direct-
    connect fallback calls — one body, never two drifting copies.

    Localhost-only, fail-open like every route beside it: the hook tries this route first
    and falls straight back to its own direct-connect path on ANY failure — a route outage
    costs exactly what today already costs, never more."""
    from starlette.responses import JSONResponse

    from src.orchestrator.stophook_logic import (
        compute_stop_deliverable,
        compute_stop_offload,
        compute_stop_stage_a,
    )

    body: Any = None
    try:
        body = await request.json()
        phase = str(body.get("phase") or "")
        cwd = str(body.get("cwd") or "")
        session_id = str(body.get("session_id") or "")
        pool = await _pool_get()
        out: Any
        if phase == "deliverable":
            out = await compute_stop_deliverable(pool, cwd=cwd, session_id=session_id)
        elif phase == "offload":
            out = await compute_stop_offload(pool, session_id=session_id, cwd=cwd)
        elif phase == "stage_a":
            # THE PIT WATCH + PRACTICE AUDIT (dispatch 5441 LEG 1 parity fix): fire-and-
            # forget from the hook's own POV — it does not block the stop either way, so
            # this phase always answers `{"result": "ok"}` on success; a failure below still
            # alarms like every other phase, never silently.
            pct = body.get("pct")
            await compute_stop_stage_a(
                pool, payload=(body.get("payload") or {}), session_id=session_id, cwd=cwd,
                pct=(int(pct) if isinstance(pct, (int, float)) else None))
            out = "ok"
        else:
            return JSONResponse({"error": f"unknown phase {phase!r}"}, status_code=400)
        return JSONResponse({"result": out})
    except Exception as e:  # noqa: BLE001 — the hook falls back to its own connect; never block
        sid = body.get("session_id") if isinstance(body, dict) else "?"
        try:
            from src.orchestrator.capture import record_hook_failure
            await record_hook_failure(
                Actions(await _pool_get()), surface="hook/stop",
                cannot_see=f"/stop route failed for session {sid}: {e}")
        except Exception:  # noqa: BLE001 — the alarm itself must never break the response
            pass
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


@mcp.custom_route("/spawn", methods=["POST"])
async def spawn_route(request: Any) -> Any:
    """SubagentStart/SubagentStop's server half: the harness announces a spawn the moment it
    happens, so the child exists in the graph — spawned_by the session's mounted seat — while
    it is still running, instead of after the miner's next 10-minute round (the operator's
    'caught by surprise' complaint, 2026-07-10). Stop refreshes the same object with the
    child's OBSERVED model (its own transcript) and a last_active stamp; the miner's
    full-tree pass converges on the same keying. Localhost-only, fail-open, idempotent."""
    from starlette.responses import JSONResponse

    from src.orchestrator import lineage

    try:
        body = await request.json()
        raw_id = str(body.get("agent_id") or "")
        session_id = str(body.get("session_id") or "")
        if not raw_id:
            return JSONResponse({"error": "agent_id required"}, status_code=400)
        pool = await _pool_get()
        parent = None
        project = None
        if len(session_id) >= 8:
            row = await mounts.find_mount(
                pool, job_dir=str(Path.home() / ".claude" / "jobs" / session_id[:8]))
            if row is not None:
                parent, project = row.agent_id, row.project
        tp = str(body.get("agent_transcript_path") or "")
        agent_type = str(body.get("agent_type") or "") or None
        phase = str(body.get("phase") or "")
        child = await lineage.register_spawn(
            Actions(pool), raw_id,
            agent_type=agent_type,
            parent_agent=parent, project=project,
            session=(session_id[:8] or None),
            transcript=(Path(tp.replace("~", str(Path.home()), 1) if tp.startswith("~")
                             else tp) if tp else None),
            done=(phase == "stop"))
        if child:
            _spawns_seen[child] = time.monotonic()
        # TELL THE FORK, AT SPAWN (obligation 706c27dc's second half, msg 6034, operator's own
        # correction: "the subagent forks need to know they are forks though"). The prior fix
        # (read_inbox/read_desk) only helped a READER catch a fork after the fact; this is the
        # fork's own orientation, delivered the one way confirmed to reach it — SubagentStart's
        # own additionalContext (SessionStart/whisper never fires for a subagent at all; a fork
        # inherits the parent's own "already mounted" belief and, by mount()'s documented
        # contract, has every reason never to call mount() itself and hit its SPAWN note there).
        # ONLY for agent_type == "fork" (inherits the parent's FULL context — an ordinary fresh
        # subagent has no parent identity to confuse itself with) and only on the START phase
        # (Stop has nothing left to orient). Disclosure, never a refusal: a fork doing real work
        # and reporting it stays legitimate, it just needs to know which "it" it is.
        fork_orientation = None
        if child and phase != "stop" and agent_type == "fork":
            pat = await pool.fetchval(
                "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o "
                "ON o.id=a.object_id WHERE o.canonical=$1 AND a.name='patronym' "
                "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", child)
            fork_orientation = (
                f"OSIRIS: you are a FORK{f' — {pat}' if pat else ''} — not your parent, not a "
                "new generation, a separate hand that inherited the parent session's FULL "
                "conversation context (its mail, its dispatch, its sense of self). Your parent "
                "is a live seat that may be working right now, in parallel with you: never "
                "report the parent's own actions, sent messages, or decisions as your own. "
                "Inherited memory is not authorship — what you remember from the parent's "
                "context is background, not something you did. Do the job, return your result "
                "to the parent; the parent's mail, seat, and succession are never yours to act "
                "through.")
        return JSONResponse({"spawn": child, "of": parent,
                             **({"fork_orientation": fork_orientation} if fork_orientation
                                else {})})
    except Exception as e:  # noqa: BLE001 — a spawn announcement must never block the harness
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


_arq: Any = None


@mcp.custom_route("/sweep", methods=["POST"])
async def sweep_route(request: Any) -> Any:
    """The death rite's doorbell (task #22): the PreCompact hook posts the dying session's
    transcript; we ENQUEUE the miner's sweep on the worker (ownership boundary — the miner
    mines, the server only rings). Fail-open, localhost-only, idempotent (the miner's cursor
    and dedup absorb re-rings).

    Also writes ONE ROW to `sweep_ledger` (Finding A, thread 5177057a) — a cheap synchronous
    INSERT alongside the enqueue, so a watchdog cron can tell whether THIS SPECIFIC attempt
    ever completed. B7 (the orphan reaper) only catches a transcript that never got any
    successful sweep, ever; its watermark is a one-time-ever boolean per file, so it is
    permanently blind to a dropped enqueue on a lineage's 2nd/3rd/Nth compaction once the
    1st has already succeeded. This ledger closes that gap without reviving the crawl."""
    from arq import create_pool as arq_create_pool
    from arq.connections import RedisSettings
    from starlette.responses import JSONResponse

    global _arq
    body: Any = None
    try:
        body = await request.json()
        transcript = str(body.get("transcript_path") or "")
        session_id = str(body.get("session_id") or "")
        if not transcript.startswith("/"):
            return JSONResponse({"error": "transcript_path required"}, status_code=400)
        if _arq is None:
            _arq = await arq_create_pool(RedisSettings.from_dsn(get_settings().redis_url))
        pool = await _pool_get()
        await pool.execute(
            "INSERT INTO sweep_ledger (transcript_path, session_id) VALUES ($1, $2)",
            transcript, session_id)
        await _arq.enqueue_job("sweep_session", transcript)
        return JSONResponse({"enqueued": True})
    except Exception as e:  # noqa: BLE001
        # THE STAKES CHANGED WHEN THE CRAWL DIED (ceae1604): mining is SUMMONED, never walking,
        # so a dropped enqueue is no longer a cheap ≤10-min miner lag — it can lose real yield.
        # Two nets now catch that, not zero: B7 (the orphan reaper) recovers a transcript that
        # NEVER got a successful sweep at all, and sweep_ledger's own watchdog (arq_worker.py)
        # recovers a dropped attempt on a lineage B7 has already swept once and gone blind to.
        # It still must never block the dying mind — a hook that can refuse a death is worse
        # than a lost extraction — so this route stays fail-open either way.
        sid = body.get("session_id") if isinstance(body, dict) else "?"
        try:
            from src.orchestrator.capture import record_hook_failure
            await record_hook_failure(
                Actions(await _pool_get()), surface="hook/precompact",
                cannot_see=f"sweep route (precompact) failed for session {sid}: {e}")
        except Exception:  # noqa: BLE001 — the alarm itself must never break the response
            pass
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


async def _boot_check() -> None:
    """THE DEPLOY-ORDERING GUARD (thread e6f5556f): LOUD ALARM, never a refusal — see
    deploy_guard's own module docstring for why. Scoped to the PERSISTENT streamable-http
    server only (the systemd `osiris-mcp` unit, the fleet's one shared door) — not the
    per-session stdio subprocess every mount spins up, which isn't a "deploy" in the sense
    this guard exists for. Wrapped defensively on top of check_schema_drift's own internal
    fail-open: nothing here may ever block or delay serving."""
    import logging

    from src.orchestrator.deploy_guard import (
        alarm_schema_drift,
        alarm_unreviewed_boot,
        check_schema_drift,
        check_unreviewed_boot,
    )

    try:
        # A THROWAWAY pool on this short-lived boot loop — NEVER _pool_get()'s global pool.
        # asyncio.run(_boot_check()) closes THIS loop before mcp.run() starts the serving loop;
        # a global pool created here binds to the now-dead loop and breaks EVERY DB-backed tool
        # call with "Event loop is closed" (the fleet-wide regression this comment prevents).
        # The global pool must be created lazily on the server's OWN serving loop.
        pool = await create_pool(get_settings().database_url, max_size=1,
                                 application_name="osiris-mcp:bootcheck-schema")
        try:
            drift = await check_schema_drift(pool)
            if drift:
                await alarm_schema_drift(pool, drift, service="osiris-mcp")
        finally:
            await pool.close()
    except Exception as exc:  # noqa: BLE001 — the guard must never become the thing it guards against
        logging.getLogger("osiris.deploy_guard").warning(
            "deploy_guard check failed at mcp boot: %r", exc)
    # THE REBOOT-IS-A-DEPLOY GUARD (thread 489a39d0): a SEPARATE try/except and pool from the
    # schema check above — a bug in one guard must never suppress the other, and this one
    # needs its own throwaway pool for the same event-loop reason.
    try:
        pool = await create_pool(get_settings().database_url, max_size=1,
                                 application_name="osiris-mcp:bootcheck-reboot")
        try:
            reboot_drift = await check_unreviewed_boot(pool)
            if reboot_drift:
                from src.orchestrator.deploy_guard import _REPO_ROOT, _git_head

                running_head = _git_head(_REPO_ROOT) or "unknown"
                src_root = None
                with contextlib.suppress(Exception):
                    from src.orchestrator.deploy_guard import _resolve_imported_src_root

                    src_root = str(await asyncio.to_thread(_resolve_imported_src_root))
                await alarm_unreviewed_boot(pool, reboot_drift, running_head=running_head,
                                           service="osiris-mcp", src_root=src_root)
        finally:
            await pool.close()
    except Exception as exc:  # noqa: BLE001 — the guard must never become the thing it guards against
        logging.getLogger("osiris.deploy_guard").warning(
            "deploy_guard reboot check failed at mcp boot: %r", exc)


memprofile.maybe_start()  # inert unless OSIRIS_PROFILE_MEMORY is set — thread e6fd3772


def main() -> None:
    """Run the server. `OSIRIS_MCP_TRANSPORT=streamable-http` = the PERSISTENT fleet server
    (one always-on process on host:port, one shared pool); default `stdio` = one server for
    this session (the classic per-agent subprocess). The systemd `osiris-mcp` unit sets http."""
    s = get_settings()
    transport = s.osiris_mcp_transport
    if transport in ("streamable-http", "sse"):
        mcp.settings.host = s.osiris_mcp_host
        mcp.settings.port = s.osiris_mcp_port
        asyncio.run(_boot_check())
        mcp.run(transport=transport)  # type: ignore[arg-type]
    else:
        mcp.run()


if __name__ == "__main__":
    main()

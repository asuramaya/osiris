"""Osiris MCP server: the AI-facing surface over the engine.

Exposes Osiris's capabilities as typed MCP tools, so any MCP client (Claude Desktop,
Claude Code, a scheduled agent, or none at all) can drive an investigation through a
stable interface: the formalization of what was previously ad-hoc Python. The same
engine backs the human front-end (the FastAPI app). The AI is an external, optional,
audited client, never embedded in the kernel, and every tool still flows through the
audited Actions layer.

    uv run python -m src.mcp_server        # stdio transport
"""

from __future__ import annotations

import asyncio
import contextlib
import faulthandler
import itertools
import json
import signal
import sys
import time
import traceback
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

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
    provenance,
    resource_lease,
    task_sync,
)
from src.orchestrator import compositions as comp
from src.orchestrator import dispose as dispose_seam
from src.orchestrator import succession as comp_succession
from src.orchestrator.agents import (
    AgentIdentity,
    _generation,
    cap_handoff_text,
    is_live_handoff,
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
from src.orchestrator.dossier import object_events as _read_object_events
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


def _strip_redundant_titles(schema: Any) -> Any:
    """Drop every `"title"` key from a JSON-Schema dict, at any nesting depth. See
    BoundedMCP.list_tools's own docstring for why this is safe: the key a `title`
    duplicates is always one level up already, no MCP client reads it, and nothing
    else about the schema's shape or validity changes. Never mutates its input (a
    fresh dict/list at every level): the tool registry's own cached schema objects
    must survive untouched for `call_tool`'s unrelated resolution path."""
    if isinstance(schema, dict):
        return {k: _strip_redundant_titles(v) for k, v in schema.items() if k != "title"}
    if isinstance(schema, list):
        return [_strip_redundant_titles(v) for v in schema]
    return schema


# HAND-BUILT DISCRIMINATED-UNION SCHEMAS: an object-type dispatcher's real inputSchema
# (a oneOf branch per `action`) cannot come from FastMCP's own signature-driven
# auto-generation, which only ever emits one flat object schema no matter how a
# function branches internally. Each dispatcher registers its own hand-authored schema
# here (tool name -> schema dict); BoundedMCP.list_tools() below substitutes it in
# place of the auto-generated one, the same override mechanism the title-strip already
# uses. Populated after each dispatcher's own schema constant is defined (forward
# reference resolved at list_tools() call time, well after module load, the same
# late-binding every function body in this file already relies on).
_HAND_BUILT_SCHEMAS: dict[str, dict[str, Any]] = {}


# GENERIC HAND-BUILT-SCHEMA HELPERS: shared across every dispatcher's own oneOf schema
# (seat, project, composition, ...), defined here (before any dispatcher's own module-level
# schema constant) so a dispatcher whose code sits earlier in the file than seat's own
# still resolves these names at import time, not merely at call time.
def _dispatcher_action_schema(properties: dict[str, Any], required: list[str]) -> dict[str, Any]:
    """One oneOf branch: `action` pinned to a const, plus this action's own properties/
    required, never the union's params, never another action's shape leaking in.
    `additionalProperties: False` means a real client that mistypes a param for this
    action gets a rejection here, before the call ever reaches a dispatcher's own
    _*_impl pre-dispatch validation (belt and suspenders, not a duplicate: this catches
    an unknown param name, the runtime check catches a missing required one)."""
    return {"type": "object", "properties": properties, "required": required,
            "additionalProperties": False}


def _s(desc: str = "") -> dict[str, Any]:
    return {"type": "string"}


def _opt_s() -> dict[str, Any]:
    return {"anyOf": [{"type": "string"}, {"type": "null"}], "default": None}


def _b(default: bool) -> dict[str, Any]:
    return {"type": "boolean", "default": default}


def _list_s() -> dict[str, Any]:
    return {"type": "array", "items": {"type": "string"}}


def _opt_list_s() -> dict[str, Any]:
    return {"anyOf": [{"type": "array", "items": {"type": "string"}}, {"type": "null"}],
            "default": None}


def _opt_int_s() -> dict[str, Any]:
    return {"anyOf": [{"type": "integer"}, {"type": "null"}], "default": None}


def _obj_s() -> dict[str, Any]:
    return {"type": "object"}


def _action_const(name: str) -> dict[str, Any]:
    return {"type": "string", "const": name}


class BoundedMCP(FastMCP):
    """FastMCP with a size limit: every tool result passes the response budget on its way out.

    Bounding at this single choke point, not per-tool, is the whole point: a tool added
    next year inherits the bound without knowing it exists, and no formatting bug can
    cost a caller its context window. The tools still decide what is worth sending (see
    src/orchestrator/budget.py); this only guarantees that whatever they decide, it fits,
    and that any trim is announced.
    """

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        ctx = self.get_context()
        await _nudge_tool_list_refresh(ctx)
        _ensure_tool_stats_flush_task()
        _ensure_watchdog_task()
        t0 = time.monotonic()
        call_id = next(_in_flight_next_id)
        # Best-effort caller for the in-flight/watchdog view only: a mount() call's own
        # identity may not be cached yet at call start, so this can legitimately read
        # 'unattributed' here even when the final stats row (below, resolved fresh after
        # the call completes) attributes correctly. Never conflate the two: the watchdog
        # trades attribution precision for a value available the instant the call begins.
        _in_flight_calls[call_id] = {
            "tool": name, "caller": _caller_for(ctx), "started_at": t0,
            "next_log_at": t0 + _WATCHDOG_STALL_THRESHOLD_S}
        result_bytes = 0
        try:
            result = await self._tool_manager.call_tool(
                name, arguments, context=ctx, convert_result=False)
            if isinstance(result, dict) and "context" not in result:
                note = await _seam_field(ctx)
                if note is not None:
                    result["context"] = note
            tool = self._tool_manager.get_tool(name)
            assert tool is not None  # call_tool already raised if the name were unknown
            bounded = fit(result, tool=name)
            result_bytes = _response_byte_size(bounded)
            return tool.fn_metadata.convert_result(bounded)
        finally:
            _in_flight_calls.pop(call_id, None)
            action = arguments.get("action")
            _record_tool_call(name, _caller_for(ctx), (time.monotonic() - t0) * 1000,
                              action if isinstance(action, str) else "", result_bytes)

    async def list_tools(self) -> list[MCPTool]:
        """HIDDEN ALIASES: a tool registered with `meta={"deprecated": True, ...}` is
        dropped from the listing a model ever sees, but stays fully in `self._tools`.
        `call_tool` above resolves it directly off that dict, never off this method's own
        output, so a live caller (including one whose compiled standing instructions
        still name the old action) keeps working at its next turn with zero code path
        change. This is what plain shim-forwarding alone cannot give you: shrinking the
        model-visible surface (this list, and the char/count ratchets in
        test_tool_contract_diet.py that measure exactly this) and the duplicated code
        (the old name's body is nothing but a one-line forward) in the same change,
        instead of trading one for the other. Vanilla FastMCP has no such notion:
        `list_tools`/`call_tool` share one undifferentiated registry, so this overrides
        the same public interface `call_tool` above already overrides, no monkey-patch of
        anything private.

        THE SCHEMA-TITLE STRIP: every parameter's auto-generated JSON-Schema `title`
        (pydantic's default, e.g. `"title": "Rationale"` on the `rationale` param)
        duplicates the property's own key one level up. No MCP client reads it, since
        the key is the name. Measured fleet-wide: 497 occurrences, ~14K wire chars.
        Stripped here, same mechanism as the deprecated filter, on the same
        `list_tools()` output. `call_tool` never sees this method's return value at all
        (it resolves off `self._tools` directly, per the note above), so this cannot
        change what a call validates against or how it executes, only what a model reads
        before calling. Recursive and blind to key name: `title` means the same
        auto-generated, always-redundant thing at every nesting depth (a property, an
        array's `items`, an `anyOf` branch); no other key is touched, and the
        `anyOf`/`null` branches pydantic emits for `Optional[...]` params stay exactly
        as-is (not the same zero-risk shape: a strict client's validation could
        legitimately depend on them, unlike a title no client reads)."""
        tools = [t for t in await super().list_tools() if not (t.meta or {}).get("deprecated")]
        return [t.model_copy(update={
            "inputSchema": (_HAND_BUILT_SCHEMAS[t.name] if t.name in _HAND_BUILT_SCHEMAS
                            else _strip_redundant_titles(t.inputSchema)),
            "outputSchema": (_strip_redundant_titles(t.outputSchema)
                             if t.outputSchema else t.outputSchema),
        }) for t in tools]


# TOOL-LIST REFRESH: several tools deployed in one day each sat invisible to callers for
# multiple turns, because a client with an already-open connection never re-fetches the
# tool list on its own. The MCP spec's own mechanism for this is
# notifications/tools/list_changed. Checked FastMCP (mcp==1.28.1) before building anything:
# the lowlevel Server already has the capability type (types.ToolsCapability) and the send
# method (ServerSession.send_tool_list_changed); FastMCP's own create_initialization_options()
# call sites (stdio/sse/streamable-http, all inside the SDK) just never pass a
# NotificationOptions(tools_changed=True), so the capability was never declared. That is an
# ergonomics gap in FastMCP's convenience wrapper, not a "don't build on this" boundary
# (a separate, unrelated caution about internal daemon socket handling does not apply here).
# NotificationOptions/create_initialization_options are public, documented SDK
# surface, exactly like BoundedMCP.call_tool above already overrides FastMCP's own public
# call_tool. No monkey-patch of anything private.
_notified_list_changed: set[str] = set()


async def _nudge_tool_list_refresh(ctx: Context | None) -> None:
    """Once per client connection (keyed by `_conn_key`, the same key the identity cache
    uses), tell an already-connected session its tool list may be stale. The deploy-time
    pain this closes: the MCP server restarts several times a day as new tools land, but
    a long-lived agent session's MCP client can resume its existing connection across
    that restart without ever re-running `initialize`/`tools/list`, so it never learns
    new tools exist until something else nudges it. Ambient, never load-bearing: any
    failure here must never block or fail the tool call it rides in on."""
    key = _conn_key(ctx)
    if key is None or key in _notified_list_changed:
        return
    _notified_list_changed.add(key)
    try:
        assert ctx is not None
        await ctx.session.send_tool_list_changed()
    except Exception:  # noqa: BLE001, ambient, never load-bearing
        pass


# TOOL-CALL TELEMETRY: which MCP tool is expensive is the kind of question that, without
# this, can only be answered by a one-off hand-bracketed measurement instead of a real
# number. A couple of existing subsystems already do this per-call telemetry shape for one
# tool each (search, the inference path) and were never generalized; this extends that
# shape rather than inventing a new one, see migration 0046. The hot path only ever
# touches the in-memory dict below. A background task (started lazily, same pattern as
# `_pool_get`'s lazy global pool) flushes it to Postgres every 60s, decoupled from any
# individual call, so the thing being measured never pays for being measured. The
# try/finally in BoundedMCP.call_tool counts failures too: a counter that only saw
# successes would report the expensive calls as cheap.
#
# CALLER ATTRIBUTION: keyed (tool, caller) instead of bare tool. Without it this table
# ranks tools but never causes; a "search is expensive" reading could really be "one busy
# agent's search habit is expensive." `caller` is a lineage root (agents.py's
# `_generation()`, the same identity-folding logic doors.py's `_record` already uses),
# not a raw agent_id: a seat mints a new agent_id on every succession/compaction, so
# grouping by the raw id would fragment one caller's real cost across dozens of rows.
# Resolved cache-only from `_agents` (never a new `_ident_for` reattach, which can hit
# Postgres); see `_caller_for` below.
_TOOL_STATS_FLUSH_INTERVAL_S = 60
_tool_call_stats: dict[tuple[str, str, str], dict[str, float]] = {}
_tool_stats_flush_task: asyncio.Task[None] | None = None
_tool_stats_window_start: datetime | None = None
# WHAT THIS CANNOT SEE: lives in tool_traffic()'s own output (`blind_spots`), not only in a
# decision record, since a clean total over an unstated scope is how the next reader gets
# misled. Checked live via `systemctl --user list-units`, not assumed: the console service
# is a separate process from the MCP server, and a past slowdown investigation on the
# console lived entirely on a surface this counter cannot see. The worker (cron), the
# heartbeat process, and the management daemon are likewise separate processes calling
# orchestrator functions directly, never through MCP. This answers "which MCP tool is
# expensive," never "which surface is expensive." Caller attribution does not change any of
# this: those daemons never go through MCP at all, so they stay exactly as uncounted as
# before, not newly countable.
_TOOL_STATS_BLIND_SPOTS = (
    "osiris-console (:8011, a separate uvicorn process) — not counted; "
    "task #164's own console slowdown lived entirely here. Confirmed live (#203, Seshat, "
    "2026-09-03): src/api/app.py imports and calls orchestrator.console.get_console "
    "directly, bypassing this MCP tool entirely — its own zero-MCP-traffic reading "
    "already misled one retirement pass into hiding it as dead (decision b49a844f) "
    "before that seat's own live-test run caught it, and it contradicts this daemon's "
    "own service file (deploy/user/osiris-console.service: 'never a write path')",
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
    "reads 0 here while its CLI entry point is real, live traffic — the exact live proof that "
    "cost a consolidation lane its first wrong deletion candidate",
    "FOUR MORE SEAT-DISPATCHER ALIASES, SAME CLI-BYPASS SHAPE (alias-decay second read, "
    "2026-09-08, decision 23b6dbc1): heal_seat_transcript, reconcile_seat_identity, "
    "rename_seat, set_seat_attended each have their own cmd_* entry point in src/cli.py "
    "(cmd_heal_seat_transcript/cmd_reconcile_seat_identity/cmd_rename_seat/"
    "cmd_set_seat_attended) calling the orchestrator function directly — none of these "
    "four were in this list before this read, so retired_alias_traffic's "
    "eligible_for_removal=true on any of them (all four read it today) is NOT proof of "
    "disuse until confirmed otherwise; check src/cli.py before ever acting on a zero "
    "reading for these names, same discipline unmerge's own incident already demands",
)


# THE STALL WATCHDOG: a past main-thread stall incident had no stack trace at all when it
# happened. py-spy needs ptrace scope 1 (not set), and nothing in this module registered a
# signal handler to dump frames on demand. Two independent, complementary mechanisms,
# never one:
#
#   1. faulthandler.register(SIGUSR1, all_threads=True) below (module scope, always on,
#      zero runtime cost until signaled): `kill -USR1 <pid>` dumps every thread's Python
#      stack to stderr (the system journal) on demand, a manual escape hatch.
#
#   2. This watchdog: BoundedMCP.call_tool (the one place every tool call already passes
#      through for bounding and stats) now registers an in-flight entry per call, and a
#      background task polls it. The moment any call has been running past
#      _WATCHDOG_STALL_THRESHOLD_S, it logs, then again every
#      _WATCHDOG_REPEAT_INTERVAL_S for as long as the same call stays in flight (spec:
#      "when any tool call passes 10s, then every 30s while it runs"), dumping every
#      thread's own stack, unprompted, no manual action required. A genuinely
#      single-threaded asyncio event loop means "every thread's stack" is really "the one
#      loop thread's stack plus whatever daemon threads exist," deliberately not narrowed
#      to just the loop thread, since a wedge could in principle be a C-extension holding
#      the GIL from a different thread the loop thread never shows.
_WATCHDOG_POLL_INTERVAL_S = 2.0
_WATCHDOG_STALL_THRESHOLD_S = 10.0
_WATCHDOG_REPEAT_INTERVAL_S = 30.0
_in_flight_calls: dict[int, dict[str, Any]] = {}
_in_flight_next_id = itertools.count()
_watchdog_task: asyncio.Task[None] | None = None


def _log_all_thread_stacks(log: Any, *, reason: str) -> None:
    """Every live thread's own Python stack, formatted and logged in one shot: the
    exact dump SIGUSR1 (faulthandler) also produces, reused here so the automatic and
    manual paths report identically."""
    frames = sys._current_frames()
    parts = [f"{reason} — {len(frames)} live thread(s):"]
    for thread_id, frame in frames.items():
        parts.append(f"--- thread {thread_id} ---\n{''.join(traceback.format_stack(frame))}")
    log.warning("\n".join(parts))


async def _watchdog_loop() -> None:
    """Runs for the life of the process (started lazily on first tool call, same pattern
    `_ensure_tool_stats_flush_task` already uses). Polls `_in_flight_calls`, never
    blocks on anything itself (a blocked event loop would also freeze this task, so its
    own job is only to notice and log, not to unblock). `next_log_at` is a monotonic
    deadline, not a boolean: the first log fires at `started_at + _WATCHDOG_STALL_
    THRESHOLD_S`, every log after that reschedules `next_log_at` to
    `now + _WATCHDOG_REPEAT_INTERVAL_S`, so a call still running 90s later logs at
    roughly 10s, 40s, 70s, not once and then silence."""
    import logging

    log = logging.getLogger("osiris.mcp.watchdog")
    while True:
        await asyncio.sleep(_WATCHDOG_POLL_INTERVAL_S)
        now = time.monotonic()
        for call_id, info in list(_in_flight_calls.items()):
            if now < info["next_log_at"]:
                continue
            elapsed = now - info["started_at"]
            info["next_log_at"] = now + _WATCHDOG_REPEAT_INTERVAL_S
            _log_all_thread_stacks(
                log, reason=(
                    f"SLOW TOOL CALL: {info['tool']!r} (caller={info['caller']!r}, "
                    f"call_id={call_id}) has been in flight {elapsed:.1f}s"))


def _ensure_watchdog_task() -> None:
    global _watchdog_task
    if _watchdog_task is None or _watchdog_task.done():
        _watchdog_task = asyncio.create_task(_watchdog_loop())


def _caller_for(ctx: Context | None) -> str:
    """The lineage root attributed to this call, cache-only: never a new DB round trip on
    the hot path (see the TOOL-CALL TELEMETRY block comment above for why raw agent_id is
    the wrong grain and why this never calls `_ident_for`'s reattach fallback)."""
    from src.orchestrator.agents import _generation

    key = _conn_key(ctx)
    ident = _agents.get(key) if key is not None else None
    return _generation(ident.agent_id)[0] if ident is not None else "unattributed"


def _response_byte_size(payload: Any) -> int:
    """The size actually being reported: BoundedMCP.call_tool already holds the bounded
    response in hand and times the call, but never sized it. Every response-size
    measurement before this had to substitute a handful of live probe calls for real
    production traffic because this number didn't exist anywhere. Best-effort: a payload
    `fit()` returns is JSON-shaped by construction (it's what convert_result serializes
    next), but this must never be the reason a response fails to ship: an unserializable
    value reads as 0 bytes, not a crash."""
    try:
        return len(json.dumps(payload, default=str).encode("utf-8"))
    except (TypeError, ValueError):
        return 0


def _record_tool_call(
    name: str, caller: str, ms: float, action: str = "", response_bytes: int = 0,
) -> None:
    row = _tool_call_stats.setdefault(
        (name, caller, action), {"count": 0.0, "total_ms": 0.0, "total_bytes": 0.0})
    row["count"] += 1
    row["total_ms"] += ms
    row["total_bytes"] += response_bytes


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
    snapshot. Never hold the dict empty across an `await`, or a call landing mid-flush
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
            "INSERT INTO mcp_tool_stats (tool_name, caller, action, window_start, "
            "window_end, call_count, total_ms, response_bytes) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            [(tool, caller, action, window_start, window_end, int(v["count"]), v["total_ms"],
              int(v["total_bytes"]))
             for (tool, caller, action), v in batch.items()],
        )
    except Exception:  # noqa: BLE001, telemetry must never break serving
        import logging
        logging.getLogger("osiris.mcp").warning("tool-stats flush failed", exc_info=True)


# THE AMBIENT CONTEXT-USAGE NOTICE: above a configured threshold, every tool response
# carries one `context` line, because the agent near the context-window ceiling is exactly
# the agent not thinking to ask. Riding the same single choke point every tool response
# already passes through means a tool added next year inherits this notice without knowing
# it exists, the same argument as the response budget. Ambient, never load-bearing: every
# failure path returns None, and the alarm only ever fires against a known context window,
# never a guessed denominator.
_SEAM_ROW_TTL = 600.0  # how long a mount-row hint (job/model/window) may serve the notice
_seam_rows: dict[str, tuple[float, str | None, str | None, int | None]] = {}
_seam_pcts: dict[str, tuple[float, int | None]] = {}
# BOUNDED, same shape as _prune_agents (this file's own proven pattern for avoiding a slow
# memory leak from unbounded growth): every agent_id/job that ever calls a mounted tool
# leaves a row here forever unless capped. Safe to cap at all because both are self-healing
# on a miss: _seam_rows already re-fetches from agent_mounts past its own TTL (line below),
# _seam_pcts already recomputes on an mtime mismatch, so an evicted entry costs one extra
# query/stat, never a wrong answer. Each tuple's own first element (a monotonic write-time
# or the file's mtime) is a workable recency signal, so no companion "touched" dict is
# needed to prune by it.
_SEAM_CACHE_CAP = 256


def _prune_seam_rows(cap: int = _SEAM_CACHE_CAP) -> None:
    """Mirrors _prune_agents exactly: past the cap, drop the least-recently-written down to
    half. Safe because _seam_field re-fetches past _SEAM_ROW_TTL regardless: an evicted
    entry just loses its TTL grace early, never returns a wrong answer."""
    if len(_seam_rows) <= cap:
        return
    cut = len(_seam_rows) - cap // 2
    for k in sorted(_seam_rows, key=_seam_rows.__getitem__)[:cut]:
        _seam_rows.pop(k, None)


def _prune_seam_pcts(cap: int = _SEAM_CACHE_CAP) -> None:
    """Mirrors _prune_agents exactly, keyed by mtime (the closest thing this cache has to a
    write-recency clock) rather than a monotonic touch-time. Safe because _seam_pct_sync
    recomputes on any mtime mismatch: an evicted entry costs one stat, never a stale answer."""
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
    """The occupancy %, from the transcript's tail (the same read quality used for the UI
    display), mtime-cached per job so a busy turn costs one stat. None when unmeasurable
    or the window would be a guess."""
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
    """The one line, tiered: a soon-to-compact notice at the configured threshold,
    write-back-now at the system's own alarm level (context_lens.ALARM_PCT: one
    authority, never a second constant)."""
    if pct is None or not whisper_pct or pct < whisper_pct:
        return None
    from src.orchestrator.context_lens import ALARM_PCT

    if pct >= ALARM_PCT:
        return (f"{pct}% — WRITE BACK NOW: a compaction can land any turn; "
                "record_decision / resolve_thread what lives only in your head")
    return f"{pct}% — seam soon; write back as you go"


# ONCE PER CROSSING, NOT ONCE PER CALL: `_seam_note` on its own fires on every tool call
# while `pct` sits anywhere in a tiered band, unlike the soft/hard marker files used
# elsewhere for a related notice, which fire exactly once per crossing. A 7-hour
# autonomous run sitting at 63-79% context for most of it saw the same "seam soon" line on
# roughly 40 consecutive calls: noise before it was useful, the same failure mode as
# firing too often instead of too late, training the reader to stop reading the escalation
# tier it belongs to. `_seam_last_band` mirrors `_seam_rows`/`_seam_pcts`'s own bounded,
# in-process, TTL-free cache shape. Correct to reset on server restart, since a fresh
# process has shown nothing yet and the very next crossing fires exactly as it should.
_seam_last_band: dict[str, tuple[float, str]] = {}


def _prune_seam_last_band(cap: int = _SEAM_CACHE_CAP) -> None:
    """Mirrors `_prune_seam_rows` exactly: least-recently-written half evicted past the
    cap; safe because a re-shown band after eviction is at worst one redundant note, never
    a wrong or missing one."""
    if len(_seam_last_band) <= cap:
        return
    cut = len(_seam_last_band) - cap // 2
    for k in sorted(_seam_last_band, key=_seam_last_band.__getitem__)[:cut]:
        _seam_last_band.pop(k, None)


def _seam_band(pct: int | None, whisper_pct: int, alarm_pct: int) -> str | None:
    """Which tier `pct` falls in for the debounce below: a state name, never text, so the
    same crossing is never re-announced on every call. None below the threshold floor."""
    if pct is None or not whisper_pct or pct < whisper_pct:
        return None
    return "alarm" if pct >= alarm_pct else "seam"


def _seam_note_once(agent_id: str, pct: int | None, whisper_pct: int) -> str | None:
    """`_seam_note`, debounced to fire once per tier-crossing rather than once per call.
    Stays silent on every later call inside the same band; re-arms the moment `pct` drops
    back below `whisper_pct` (a real write-back/compaction happened) or steps up from the
    warning tier into the alarm tier (a real escalation, worth exactly one more note; the
    system's own alarm, context_lens.ALARM_PCT, is one authority, this never re-derives
    its own threshold)."""
    from src.orchestrator.context_lens import ALARM_PCT

    band = _seam_band(pct, whisper_pct, ALARM_PCT)
    if band is None:
        _seam_last_band.pop(agent_id, None)  # dropped below the floor: re-arm for later
        return None
    prior = _seam_last_band.get(agent_id)
    if prior is not None and prior[1] == band:
        return None  # already shown this band: repeat, not news
    _seam_last_band[agent_id] = (time.monotonic(), band)
    _prune_seam_last_band()  # opportunistic: this write is where churn shows up
    return _seam_note(pct, whisper_pct)


async def _raw_context_pct(ctx: Context | None) -> int | None:
    """The raw number `_seam_field` computes internally but never returns on its own (its
    job is a debounced, threshold-gated human sentence, not a value another caller can do
    arithmetic on). Extracted so settle()'s own `context_pct` field and `_seam_field`
    itself share one lookup, never two copies of the job_dir/model_raw/window_hint
    resolution drifting apart. None for every reason `_seam_field` already tolerates
    (unmounted, young session, guessed window, any failure): never a hazard, an ambient
    best-effort read."""
    try:
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
        return await asyncio.to_thread(_seam_pct_sync, job, model_raw, window_hint)
    except Exception:  # noqa: BLE001, ambient, never load-bearing
        return None


async def _seam_field(ctx: Context | None) -> str | None:
    """The ambient context line for a mounted caller, or None (unmounted callers, young
    sessions, guessed windows, an already-shown tier, any failure: this notice never
    becomes a hazard)."""
    try:
        st = get_settings()
        if not st.osiris_seam_whisper_pct:
            return None
        ident = await _ident_for(ctx)
        if ident is None:
            return None
        pct = await _raw_context_pct(ctx)
        return _seam_note_once(ident.agent_id, pct, st.osiris_seam_whisper_pct)
    except Exception:  # noqa: BLE001, ambient, never load-bearing
        return None


mcp = BoundedMCP(
    "osiris",
    instructions=(
        "Osiris gives an agent session durable memory. It remembers what you learn and "
        "decide after a session ends, and every session working on the same project "
        "shares it. "
        "Call mount(cwd=<your working directory>) first to register as a session. This "
        "attributes everything you write to you, instead of an anonymous placeholder. "
        "Pass a durable job_dir if your harness provides one (Claude Code: "
        "~/.claude/jobs/<id>; DSH: detected automatically from the workspace name). "
        "Without it you can still mount, but your identity does not survive a server "
        "restart. "
        "Use get_status() for a quick check instead of the full report from orient(). "
        "Use get_mail() to see only your inbox counts. get_thread_list(project) and "
        "get_decision_list(project) return paginated results. "
        "Before deriving a fact yourself, check whether it is already known: "
        "graph_search(query, project=<name>) scopes results to one project's data, "
        "using the same search as search() with graph context added. "
        "Write back as you go. Call record_decision as soon as a decision is made, call "
        "open_thread when work starts or gets blocked (use kind='obligation' for a "
        "duty), and call resolve_thread as soon as it is done. A session can end at any "
        "moment, so the graph, not the conversation, is the lasting record. "
        "Other sessions can send you a message addressed to your project. mount(), "
        "orient(), and get_status() report how many are unread; call inbox() to read "
        "them. Use send(to='operator') to reach the person operating this system."
    ),
)
# DECLARE THE listChanged CAPABILITY (see BoundedMCP/_nudge_tool_list_refresh above): FastMCP
# never passes NotificationOptions through to the lowlevel Server's own
# create_initialization_options(), so `tools_changed` silently defaults to False and a
# compliant client never even learns the server might send this notification. Wrapping the
# bound method (public, not underscore-prefixed) to supply the default the SDK already
# supports: every call site that omits its own notification_options gets tools_changed=True.
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
    """Reports which MCP tool is expensive, and for whom: call count, total/average
    wall-clock time, and total/average response bytes (`total_bytes`/`avg_bytes`,
    best effort, 0 when a payload cannot be measured), ranked by time cost. Results
    are cut three ways: by tool (`persisted`/`current_unflushed_window`), by caller
    (`..._by_caller`), and by (tool, action) under a dispatcher (`..._by_action`; an
    empty action means an ordinary call). `persisted` reads flushed 60-second windows
    covering the last `window_minutes`; `current_unflushed_*` is live data since the
    last flush. Failed calls count too. A row flushed before response-byte tracking
    existed reads `total_bytes=0`, meaning unmeasured, not zero-cost.
    `retired_alias_traffic` tracks retired tool aliases: a hidden alias's own traffic
    next to the dispatcher action that absorbed it. `eligible_for_removal` is true
    only when both are zero. `blind_spots` lists what this tool cannot see."""
    pool = await _pool_get()
    since = datetime.now(UTC) - timedelta(minutes=window_minutes)
    # ::bigint on every sum(response_bytes): response_bytes is declared `bigint` (migration
    # 0057), and Postgres's own SUM(bigint) rule always promotes to `numeric` regardless of
    # the actual row values. asyncpg then decodes that as a Decimal, which json.dumps
    # renders as a string, not a number. Every other summed column here (call_count/
    # total_ms) stays a plain int/float because SUM(integer)->bigint and
    # SUM(double precision)->double precision both decode natively. The cast forces the
    # wire type back to bigint at the query, not a Python-side int(): the safer fix, since
    # a Python cast after the fact still round-trips through a Decimal first and a caller
    # reading `type(total_bytes)` mid-query would see the wrong thing.
    tool_rows = await pool.fetch(
        "SELECT tool_name, sum(call_count) AS calls, sum(total_ms) AS total_ms, "
        "sum(response_bytes)::bigint AS total_bytes "
        "FROM mcp_tool_stats WHERE window_start >= $1 "
        "GROUP BY tool_name ORDER BY total_ms DESC", since,
    )
    caller_rows = await pool.fetch(
        "SELECT caller, sum(call_count) AS calls, sum(total_ms) AS total_ms, "
        "sum(response_bytes)::bigint AS total_bytes "
        "FROM mcp_tool_stats WHERE window_start >= $1 "
        "GROUP BY caller ORDER BY total_ms DESC", since,
    )
    action_rows = await pool.fetch(
        "SELECT tool_name, action, sum(call_count) AS calls, sum(total_ms) AS total_ms, "
        "sum(response_bytes)::bigint AS total_bytes "
        "FROM mcp_tool_stats WHERE window_start >= $1 AND action <> '' "
        "GROUP BY tool_name, action ORDER BY total_ms DESC", since,
    )

    def _fmt(calls: int, total_ms: float, total_bytes: int = 0) -> dict[str, Any]:
        return {"calls": calls, "total_ms": round(total_ms, 1),
                "avg_ms": round(total_ms / calls, 2) if calls else None,
                "total_bytes": total_bytes,
                "avg_bytes": round(total_bytes / calls, 1) if calls else None}

    persisted = [{"tool": r["tool_name"], **_fmt(r["calls"], r["total_ms"], r["total_bytes"])}
                for r in tool_rows]
    persisted_by_caller = [
        {"caller": r["caller"], **_fmt(r["calls"], r["total_ms"], r["total_bytes"])}
        for r in caller_rows]
    persisted_by_action = [
        {"tool": r["tool_name"], "action": r["action"],
         **_fmt(r["calls"], r["total_ms"], r["total_bytes"])}
        for r in action_rows]

    by_tool: dict[str, dict[str, float]] = {}
    by_caller: dict[str, dict[str, float]] = {}
    by_action: dict[tuple[str, str], dict[str, float]] = {}
    for (tool, caller, action), v in _tool_call_stats.items():
        t = by_tool.setdefault(tool, {"count": 0.0, "total_ms": 0.0, "total_bytes": 0.0})
        t["count"] += v["count"]
        t["total_ms"] += v["total_ms"]
        t["total_bytes"] += v.get("total_bytes", 0.0)
        c = by_caller.setdefault(caller, {"count": 0.0, "total_ms": 0.0, "total_bytes": 0.0})
        c["count"] += v["count"]
        c["total_ms"] += v["total_ms"]
        c["total_bytes"] += v.get("total_bytes", 0.0)
        if action:
            a = by_action.setdefault((tool, action),
                                     {"count": 0.0, "total_ms": 0.0, "total_bytes": 0.0})
            a["count"] += v["count"]
            a["total_ms"] += v["total_ms"]
            a["total_bytes"] += v.get("total_bytes", 0.0)
    live = [
        {"tool": name, **_fmt(int(v["count"]), v["total_ms"], int(v["total_bytes"]))}
        for name, v in sorted(by_tool.items(), key=lambda kv: -kv[1]["total_ms"])
    ]
    live_by_caller = [
        {"caller": name, **_fmt(int(v["count"]), v["total_ms"], int(v["total_bytes"]))}
        for name, v in sorted(by_caller.items(), key=lambda kv: -kv[1]["total_ms"])
    ]
    live_by_action = [
        {"tool": tool, "action": action, **_fmt(int(v["count"]), v["total_ms"],
                                                int(v["total_bytes"]))}
        for (tool, action), v in sorted(by_action.items(), key=lambda kv: -kv[1]["total_ms"])
    ]

    persisted_calls = {r["tool_name"]: int(r["calls"]) for r in tool_rows}
    persisted_action_calls = {(r["tool_name"], r["action"]): int(r["calls"])
                              for r in action_rows}
    retired_alias_traffic = []
    for alias, action in sorted(_RETIRED_ALIAS_ACTIONS.items()):
        own_calls = (persisted_calls.get(alias, 0)
                    + int(by_tool.get(alias, {}).get("count", 0)))
        action_calls = (persisted_action_calls.get((_RETIRED_ALIAS_DISPATCHER, action), 0)
                       + int(by_action.get((_RETIRED_ALIAS_DISPATCHER, action), {})
                             .get("count", 0)))
        retired_alias_traffic.append({
            "alias": alias, "own_name_calls": own_calls,
            "absorbed_into": f"{_RETIRED_ALIAS_DISPATCHER}(action={action!r})",
            "absorbed_action_calls": action_calls,
            "eligible_for_removal": own_calls == 0 and action_calls == 0,
        })

    now = time.monotonic()
    in_flight = sorted((
        {"call_id": call_id, "tool": info["tool"], "caller": info["caller"],
         "elapsed_secs": round(now - info["started_at"], 1)}
        for call_id, info in _in_flight_calls.items()
    ), key=lambda r: -r["elapsed_secs"])

    return {
        "window_minutes": window_minutes,
        "persisted": persisted,
        "current_unflushed_window": live,
        "persisted_by_caller": persisted_by_caller,
        "current_unflushed_by_caller": live_by_caller,
        "persisted_by_action": persisted_by_action,
        "current_unflushed_by_action": live_by_action,
        "retired_alias_traffic": retired_alias_traffic,
        # THE STALL WATCHDOG's own in-flight view: every call that has started but not
        # yet finished, right now. This very tool_traffic() call included
        # (BoundedMCP.call_tool registers the entry before the tool body runs), so
        # expect to always see at least one near-zero elapsed_secs row for
        # 'tool_traffic' itself. A non-empty list with a large elapsed_secs on some
        # other tool is exactly the shape a past stall incident had no visibility
        # into at all.
        "in_flight": in_flight,
        "measures": "MCP tool calls on this one shared osiris-mcp process only",
        "blind_spots": list(_TOOL_STATS_BLIND_SPOTS),
    }


# The connection registry: each connected agent's identity, keyed by its client session. On
# the shared server every agent writes through one process, so without this their writes
# collapse into the single `session` source. `mount` populates this; the capture tools
# read it so each write is attributed to `agent:<session>`. The dict is the hot half; the
# durable half is agent_mounts in Postgres (src/orchestrator/mounts.py). A server restart
# used to wipe every connected agent's identity at once; now any call re-attaches from the
# table by the client's job_dir header (_ident_for).
_agents: dict[str, AgentIdentity] = {}
_agents_touched: dict[str, float] = {}  # last use per key: feeds the bounce-orphan prune
# The while-you-were-away anchor per agent: the lineage's last_seen before this session's
# mount/reattach (captured from save_mount's RETURNING). mount() and orient() fold what
# happened in the agent's name since (successions, wakes, thread movement) so a returning
# session never has to guess where it stands.
#
# DELIBERATELY UNBOUNDED: its three siblings below (_seam_rows/_seam_pcts/
# sessions._wake_verdict) got a cap=256/4096 LRU prune; this one did not, on purpose. It
# fails a different way than they do:
#   (a) NO SELF-HEALING RE-FETCH ON A MISS. The other three recompute the correct answer from
#       an authoritative source when evicted: a cache miss costs one query, never a wrong
#       result. This one cannot: while_away()'s own contract treats a missing anchor as
#       identical to "nothing happened while you were away" (its own docstring's words), so a
#       pruned entry doesn't error or degrade visibly, it silently reports the wrong thing as
#       if it were the right thing. Since the goal is instruments that don't report success
#       while actually failing, a churn-based cap here would trade a bounded, loud failure
#       (the process grows and eventually dies visibly) for an unbounded, silent one.
#   (b) READ ACROSS A SESSION'S WHOLE LIFETIME, not just near mount. orient() reads it on
#       every call, for as long as the mounted session lives, so its real required lifetime
#       is "as long as the session lives," which a count-based LRU cap has no way to guarantee
#       (a busy set of concurrent sessions could evict a still-live session's own anchor
#       before that session's next orient() call).
# If this ever needs bounding, the correct shape is a TTL long enough to outlive any real
# session (hours-to-days, not a churn cap sized to entry count), never the _prune_agents
# pattern used on its neighbors. It is also the smallest and least frequently written of the
# four (setdefault, not overwrite), so the cost of leaving it unbounded is the lowest of the
# four to begin with.
_prev_seen: dict[str, datetime | None] = {}


def _prune_agents(cap: int = 256) -> None:
    """Client sessions churn and never say goodbye (a vanished session leaves its entry
    behind, the slow leak that fed a past out-of-memory incident); past the cap, drop the
    least-recently-used down to half. The durable registry (agent_mounts) makes an
    over-eager prune cost one transparent re-attach, nothing more."""
    if len(_agents) <= cap:
        return
    stale = sorted(_agents_touched, key=_agents_touched.__getitem__)[: len(_agents) - cap // 2]
    for key in stale:
        _agents.pop(key, None)
        _agents_touched.pop(key, None)


def _evict_stale_minds(ancestor: str | None) -> None:
    """Minting a successor identity means the ancestor is dead, but its MCP connection is
    not: a compaction (or a live model swap) preserves the client session, so the
    connection-keyed hot cache keeps answering as the dead identity while the durable row
    already names the successor (seen live: orient() answered as the old identity minutes
    after the successor was minted). Evict every cached identity wearing the ancestor; the
    next call re-attaches from the row as the successor."""
    if not ancestor:
        return
    for key in [k for k, ident in _agents.items() if ident.agent_id == ancestor]:
        _agents.pop(key, None)
        _agents_touched.pop(key, None)


def _conn_key(ctx: Context | None) -> str | None:
    """A per-client-session key. Prefer the protocol session id (the Mcp-Session-Id header,
    minted at initialize, stable across every request of the client session); fall back to
    the ServerSession object id under stdio. The keyspaces are prefixed so they can't
    collide (a garbage-collected session object's id() can be reused, so the raw-id key
    was a latent cross-agent identity merge, which must never happen)."""
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
    """A usable job_dir is an absolute path. Anything carrying `$` is an unexpanded variable
    (braced or not; a live agent passed the literal `$CLAUDE_JOB_DIR` and it became a
    registry primary key, a conflation magnet: every agent making the same mistake would
    collapse into one row). Reject, treat as absent, never store."""
    if not value or "$" in value or not value.startswith("/"):
        return None
    return value


def _infer_harness(cwd: str | None, job_dir: str | None) -> str:
    """Which process adapter's capabilities apply to this session: read off the anchor's
    own shape, never asked for or assumed. A job_dir under `~/.claude/jobs/` is Claude
    Code's own convention (CLAUDE_JOB_DIR); a DSH workspace anchors under `~/.dsh/`; a
    crush session anchors under a project's (or seat directory's) own `.crush/` data dir.
    Checks `job_dir` first (the more durable anchor when both are given), then `cwd`.
    Ambiguous or missing (neither string names a known harness's own directory shape)
    falls back to this host's own resolved adapter (`resolve_process_adapter().name`),
    the same "declared, not guessed" discipline used elsewhere: a session with no
    legible anchor shape is presumed to run whatever this host's own settings/
    auto-detection already resolve to, never a finer guess than that."""
    from src.orchestrator.harness_process import resolve_process_adapter

    for candidate in (job_dir, cwd):
        if not candidate:
            continue
        if "/.claude/jobs/" in candidate:
            return "claude-code"
        if "/.dsh/" in candidate:
            return "dsh"
        if "/.crush/" in candidate or candidate.rstrip("/").endswith(".crush"):
            return "crush"
    return resolve_process_adapter().name


def _anchorless(ctx: Context | None) -> str:
    """Why this call could not be re-attached: the difference between a mystery and a message.

    Two agents on one project reported the same thing within an hour: after an MCP socket
    hiccup a tool call bounces with "mount first", and, worse, an un-mounted write falls
    back to the anonymous `session` bucket. One reported it as: an MCP socket hiccup leads
    to a missing anchor, which leads to anonymous writes, so one careless reconnect and a
    session's work lands unattributed. For a graph whose entire value is provenance, that
    is the worst failure it has.

    The re-attach machinery already exists and is starved, not broken: it keys off the
    X-Osiris-Job header, which .mcp.json sends as ${CLAUDE_JOB_DIR}. If the client's
    environment does not set that variable, the header arrives empty or as the literal
    unexpanded string, _sane_job_dir rightly rejects it, and there is nothing to
    re-attach by. So say exactly that, instead of "mount first": a bounce that names its
    own cause is a bug report the next reader does not have to file again.
    """
    if ctx is None:
        return "no request context"
    raw = None
    try:
        req = ctx.request_context.request
        raw = req.headers.get("x-osiris-job") if req is not None else None
    except (AttributeError, LookupError):
        pass
    # TRANSIENT OR TERMINAL? A reason code lets an agent tell "transient, just retry" from
    # "something actually forgot me." A bounce that says only "mount first" is
    # indistinguishable from amnesia, so every agent guesses, and a guessing agent either
    # re-mounts needlessly or panics about continuity it never lost. These are different
    # facts and the bounce must say which.
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
    opposite: that expansion was proven live via the probe reattach. That was false. A
    later investigation instrumented the server and caught what the client actually
    sends: the literal string '${CLAUDE_JOB_DIR}', unexpanded. Project-scope .mcp.json
    does expand ${VAR} in headers, but deployments are installed user-scope
    (~/.claude.json via `claude mcp add`), and this client version does not expand
    there. So _sane_job_dir rejects every '$'-bearing value and this function has
    returned None for every deployment, for its entire life. Durable identity has been
    carried entirely by the hook-derived job_dir, never by this.

    A correction to this behavior was recorded elsewhere but the code comment here was
    never updated to match. The false claim sat here for days and cost the next reader a
    full re-derivation of a bug that had already been solved. A correction that is
    recorded but not updated at the site where the next reader will actually read it is
    not a correction: it actively misleads. Kept as a live fallback only in case a
    future client learns to expand it; expect None.
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
    """The operator's standing model choice for this repo: the .osiris file first, then
    the SoftwareProject's intended_model property, then the host default. Every banner and
    divergence stamp measures against this, so a settled choice is never re-litigated."""
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
    """The wake-economy standdown: triage wakes ride a cheaper model by the operator's own
    policy (osiris_wake_model), but the swap banner measured them against the repo's
    standing choice, so every wake was told it had been switched unexpectedly and
    dutifully escalated the operator's own policy back to their desk, at wake cadence. If
    the observed model is the economy model and this project's wake ledger shows a wake
    minutes ago, the divergence is the policy working: the banner stands down to a calm
    note. The note still tells a non-wake how to tell the difference, confirmed against
    the ledger, never assumed."""
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
    """IDENTITY IS LOCATION-INDEPENDENT: the system orients from the seat (anchor to holds
    to seat), never from cwd. The whole point of a seat is that where a session happens to
    be sitting doesn't matter. For a seated session, project is the seat's own derived
    value, unconditionally, overriding whatever cwd produced, not merely filling in a gap
    when cwd came up empty. Deliberately not house_of(agent_id): that reads the agent's
    own project stamp, exactly what a transient bad mount can pollute; trusting it here
    would let a polluted stamp go on leaking into every read, the very thing this function
    exists to stop. An unseated session (no holds binding yet, nothing to trust but its own
    resolution) keeps whatever cwd produced, None included; that's an honest "not mounted
    to a definite project," not an error. Mutates `ident` in place.

    CALLED BEFORE register_agent, NOT AFTER: this docstring used to say the opposite and
    that was the bug. register_agent's own project mint read `ident.project` two lines
    before this correction ran, so a seated session with a non-project-shaped cwd (the
    bare seat-directory slug, the canonical case) minted a phantom SoftwareProject off the
    pre-correction guess before anyone fixed it. Safe before the mint for every arrival,
    proven by a schema constraint: `links.from_id`/`to_id` are `NOT NULL REFERENCES
    objects(id)`, so a `holds` link cannot exist unless its Agent object already does. A
    seated result here is proof the object predates this call, whichever path resolved
    `ident.agent_id` (an earlier claim_name, `_bind_before_spawn`, or `office_claim`'s own
    resolution to an existing lineage head, never a fresh id). An unseated identity is an
    unconditional no-op regardless of when this runs (`held_seat` cannot match a row that
    cannot exist yet for an id nothing has ever bound), so a legitimate cwd-derived project
    for a not-yet-seated session is never at risk either way.

    A thin wrapper around `seats.resolve_and_persist_seated_project`, the same seat-first
    check `seats.resolve_project` (the shared resolver the stop hook and census now use)
    leads with. Deliberately not the full `resolve_project`: its cwd-guessing fallback is
    for callers with no cwd-derived answer of their own; mount() already has one, fresh off
    `resolve_identity` moments earlier in this same pipeline, and it must win untouched
    when this comes up unseated (recomputing a second, independent cwd guess here could
    disagree with it).

    ALSO PERSISTS the correction onto the Agent object's own `project` assertion, not
    merely this call's in-memory `ident`/the durable mount-registry row. fleet() reads
    that assertion directly, never the registry row; without this, a seated session whose
    cwd didn't independently resolve (the bare seats container root) stayed filed under
    "?" in fleet() forever, even though this very function already knew the seat's true
    house and mount()'s own result already showed it correctly."""
    from src.orchestrator.seats import resolve_and_persist_seated_project
    house = await resolve_and_persist_seated_project(Actions(pool), ident.agent_id)
    if house is not None:
        ident.project = house


async def _heal_mount_cache_for_seats(pool: asyncpg.Pool, affected_seats: set[str]) -> None:
    """Generalized from promote's own inline heal: walk every currently-mounted identity
    in this process's `_agents` cache and re-resolve any bound to one of `affected_seats`
    via a fresh graph read (`_resolve_project_seat_first`). Seat-bound, not
    generation-prefix-matched: unlike rebind/transition_project/invalidate_works_in/
    correct_house (which only ever affect the caller's own lineage), promote/charter/
    attach/detach's affected seats are usually someone else's, so this asks `held_seat`
    per cached identity rather than assuming a shared generation prefix. A no-op for an
    empty set (never walks the whole cache for nothing to heal)."""
    if not affected_seats:
        return
    from src.orchestrator.seats import held_seat as _held_seat
    for cached in list(_agents.values()):
        bound = await _held_seat(pool, cached.agent_id)
        if bound and bound["seat_id"] in affected_seats:
            await _resolve_project_seat_first(pool, cached)


async def _reattach(
    pool: asyncpg.Pool, key: str | None, job: str | None
) -> AgentIdentity | None:
    """The durable-registry half of _ident_for (separated so tests drive it with their own
    pool): look the job_dir up in agent_mounts, re-run identity resolution off the transcript
    (so the model/swap history is fresh, not a stale copy), re-register, re-cache. The stored
    model is deliberately not passed as a self-report; it would false-flag model_divergent
    after a real swap. None when there is nothing to re-attach by."""
    if job is None:
        return None
    rec = await mounts.find_mount(pool, job_dir=job)
    # THE FIRST-RUN SEAT RESCUE: checked before every other fallback below. A stale-mount
    # sweep can release this exact job_dir's row for reasons that have nothing to do with
    # the lineage dying (mounts.rescue_seat_holder_mount's own docstring has the full
    # specimen). A lineage that still holds a seat right now is never treated as unmounted.
    if rec is None:
        rec = await mounts.rescue_seat_holder_mount(pool, job_dir=job)
    elif job:
        # The self-reinforcing trap: a wrong mint from the gap above registers its own
        # row, so every later re-attach keeps finding the unrecognized session instead of
        # ever reaching the rescue above. A seat holder outranks a seatless row's live
        # claim on this job_dir just as much as it outranks the row's own absence.
        outranked = await mounts.demote_seatless_mount_if_outranked(
            pool, job_dir=job, actor=get_settings().osiris_actor)
        if outranked is not None:
            rec = outranked
    adopted_from = None
    self_restored = False
    if rec is None:
        # THE BRIDGED RESUME: the session-picker resume presents a new anchor the
        # registry never learned (jobs/<new>/state.json names resumeSessionId, the
        # harness's own record of the pair). Follow it: adopt the resumed anchor's row,
        # and below mint the presented anchor its own sibling row so the next request is
        # a direct hit. Without this, every call from a resumed session bounced
        # [unknown-anchor · TERMINAL].
        prior = mounts.resumed_anchor(job)
        rec = await mounts.find_mount(pool, job_dir=prior) if prior else None
        if rec is not None:
            adopted_from = rec.job_dir
    if rec is None:
        # THE TRANSCRIPT SELF-RESTORE: no row survives under this anchor or its
        # resume-bridge (session_end's own release, a daemon re-adopt after a bounce, a
        # genuinely evicted row), but a real transcript proves this session actually ran
        # before, which is proof enough to restore rather than bounce
        # [unknown-anchor · TERMINAL] and force a fresh, unattributed re-mount.
        # `cwd_of_transcript` is anchored-only (never a co-tenant's file, the same
        # identity-path rule `current_model` already follows): None here means genuinely
        # never mounted, and the bounce below is the correct answer, not a gap.
        from src.ingest.sessions import cwd_of_transcript

        restored_cwd = await cwd_of_transcript(job_dir=job)
        if restored_cwd is None:
            return None
        rec = mounts.MountRecord(job_dir=job, agent_id="", project=None, cwd=restored_cwd,
                                 model=None)
        # THE GENUINELY-UNATTRIBUTED CASE: unlike every other branch above, this one has
        # no prior binding at all: rec.agent_id=="" means the transcript proved the
        # session ran before, but nothing ties it to any known lineage. register_agent's
        # own revisit_check (agents.py) is gated to fire only here, never for a
        # bridged-resume or an ordinary re-attach (both already carry real attribution,
        # the row itself is the evidence).
        self_restored = True
    settings = get_settings()
    # The model reading rides the store (sole lane since the JSONL-fallback removal);
    # fail-open: a store outage re-attaches with an unobserved model, never a bounce
    reading = await identity_reading(pool, cwd=rec.cwd, job_dir=rec.job_dir)
    ident = resolve_identity(cwd=rec.cwd, job_dir=rec.job_dir, store_reading=reading)
    # rec.agent_id == "" is the self-restore's own sentinel (mounts.MountRecord minted
    # above with no prior row to have bound a seat on): nothing to honor, the freshly
    # derived ident is definitionally the right answer, so this check must not fire.
    if rec.agent_id and _generation(rec.agent_id)[0] != _generation(ident.agent_id)[0]:
        # A bound session: the row points at a deliberately-worn seat of a different
        # lineage; honor it. Re-deriving from the transcript here was the bug that
        # stomped a claimed seat back to its session hash on every silent reconnect.
        ident.agent_id = rec.agent_id
    # THE FIRST ACT SEATS YOU: a still-anonymous session standing in a seat's office
    # earns the seat here, at its first authenticated call, never at an earlier notice
    # (which fires for title-generator stubs exactly as it fires for real agents).
    mint_reason = None
    claimed_office = await handshake.office_claim(
        Actions(pool), cwd=rec.cwd, agent_id=ident.agent_id)
    if claimed_office is not None:
        ident.agent_id = claimed_office
        mint_reason = "office-birth"
    # SEAT-FIRST, BEFORE THE MINT: used to run after register_agent, two lines too late.
    # register_agent's own project mint
    # (`_resolve_or_mint_project`, inside its own body) read `ident.project` while it was
    # still resolve_identity's pre-correction cwd-basename guess, so a seated session with
    # an office-slug cwd (the bare seats container's own basename, never a real project
    # name) minted a phantom SoftwareProject before this correction ever ran. Reordered:
    # safe for every call path, proven by a schema constraint, not merely traced:
    # `links.from_id`/`to_id` are `NOT NULL REFERENCES objects(id)`, so a `holds` link
    # cannot exist unless the Agent object it names already does. So
    # `_resolve_project_seat_first` finding a seat is itself proof the underlying object
    # predates this call (bound by an earlier claim_name, `_bind_before_spawn`, or
    # `office_claim`'s own resolution to an existing lineage head, never a fresh id),
    # never a same-call race with the mint. For a genuinely unseated/fresh identity, this
    # is an unconditional no-op (`held_seat` returns None: the row it would need to match
    # cannot exist for an id nothing has ever bound), so ordering never changes that
    # population's behavior either.
    await _resolve_project_seat_first(pool, ident)
    await register_agent(Actions(pool), ident, actor=settings.osiris_actor,
                         expected_model=await _expected_model(pool, rec.cwd, ident.project),
                         mint_reason=mint_reason, revisit_check=self_restored)
    if key is not None:
        _agents[key] = ident
        _agents_touched[key] = time.monotonic()
    prev = await mounts.save_mount(pool, job_dir=rec.job_dir, agent_id=ident.agent_id,
                                   project=ident.project, cwd=rec.cwd, model=ident.model,
                                   session_key=key)
    if adopted_from is not None and job != rec.job_dir:
        # The presented anchor earns its own row (same identity, marked as the bridge's),
        # and the binding rides along, so downstream guards treat the bridged session id
        # like the durable one
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
    """The mounted identity for this call: the hot dict first, then re-attach from the
    durable registry. A server restart used to wipe every connected agent's identity at
    once; now it costs each agent one transparent re-attach.

    Two hint sources, and the second is why this finally works. The first is the client's
    X-Osiris-Job header, which .mcp.json fills from ${CLAUDE_JOB_DIR}, and that is empty in
    every interactive session, so for most deployments the re-attach machinery has been
    starved, not broken, for its whole life. The second is `anchor`: the PreToolUse hook
    holds the harness's own session_id on every osiris call and can derive the durable
    job_dir from it, so it now stamps it into the call rather than only into mount().

    Several independent sightings of this same failure in one investigation all traced
    here. Every one of them was written off as "transient", because the bounce gave no
    way to know otherwise.
    """
    key = _conn_key(ctx)
    if key is not None and (cached := _agents.get(key)) is not None:
        _agents_touched[key] = time.monotonic()
        return cached
    return await _reattach(await _pool_get(), key, _job_hint(ctx) or (anchor or None))


async def _source_for(ctx: Context | None, anchor: str | None = None) -> str:
    """The attributing actor for a write: the mounted agent on this connection (re-attached
    from the durable registry if the server restarted), else the fallback `session`
    (back-compat: an un-mounted agent still writes, just coarsely)."""
    ident = await _ident_for(ctx, anchor)
    return ident.agent_id if ident else "session"


async def _stamp_read_ids(
    pool: asyncpg.Pool, ident: AgentIdentity | None, door: str, object_ids: list[Any],
) -> None:
    """PROVENANCE, PIECE 1: log this session's read-set at the entry point, for every
    real object id a read tool is about to hand back. A no-op when nobody is mounted
    (`ident is None`): an unattributed read has no session for a later write to be
    dependent on. Never lets a stamping failure break the read tool it rides along on
    (the same fails-open discipline this codebase already applies to every other side
    channel that must never become the thing it's watching, e.g. trigger.py's own
    `_manager_windows` docstring); the caller's real result is already decided by the
    time this runs."""
    if ident is None or not object_ids:
        return
    import logging

    try:
        for raw in object_ids:
            oid = raw if isinstance(raw, uuid.UUID) else uuid.UUID(str(raw))
            await provenance.stamp_read(pool, agent_id=ident.agent_id, door=door,
                                        object_id=oid)
    except Exception:
        logging.getLogger("osiris.mcp").exception(
            "read-set stamping failed for door=%s (non-fatal)", door)


_spawns_seen: dict[str, float] = {}  # child agent id → last registration (skip re-registering)
_SPAWN_TTL = 600.0


async def _actor_for(
    ctx: Context | None, subagent_id: str | None, subagent_type: str | None = None
) -> str:
    """The attributing actor for a write: the spawned sub-agent itself when the anchor hook
    stamped this call as a sidechain's, else the connection's mounted identity. A sub-agent
    shares its parent's MCP connection and its $CLAUDE_JOB_DIR, so without the stamp every
    spawn write would land on the parent, misattributing the child's writes to it. The stamp
    is harness truth (payload agent_id, present only inside a sidechain; the hook strips it
    from main-session calls, so nobody masquerades down either). First touch registers the
    child, spawned_by the mounted parent, acts_for its principal, under the same keying the
    session-miner uses elsewhere, so disk reconstruction converges on the same object."""
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
            witnessed=True)  # a hook-stamped tool call is an observed act
        _spawns_seen[child] = time.monotonic()
        if len(_spawns_seen) > 512:  # spawns churn; keep the skip-cache bounded
            for k in sorted(_spawns_seen, key=_spawns_seen.__getitem__)[:256]:
                _spawns_seen.pop(k, None)
    return child


async def _pool_get() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        # One pool for the whole server. Under streamable-http this single pool backs every
        # connected client (the whole point: bounded connections); under stdio it's this one
        # session. min_size stays 1 so an idle server is cheap.
        _pool = await create_pool(
            get_settings().database_url, max_size=get_settings().osiris_mcp_pool_size,
            application_name="osiris-mcp",
        )
    return _pool


async def _resolve(pool: asyncpg.Pool, ref: str) -> uuid.UUID | None:
    """Accept a UUID, canonical, or name; resolve to an object id. One definition, the
    shared resolver in compositions (resolve_ref), so tools and composition functions
    always resolve the same words to the same object."""
    return await comp.resolve_ref(pool, ref)


# --- orientation ------------------------------------------------------------

@mcp.tool()
async def suggest_sources(object_ref: str) -> dict[str, Any]:
    """The playbook for an object (UUID or name): which sources to collect and which
    analyses apply, given its type. Start here to answer "what can I do with this?"."""
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
    """Searches the graph's knowledge, not just its labels: full-text over names,
    decision and thread summaries, and rationales. Accepts words, phrases, or
    "quoted phrases" (websearch syntax). Results are ranked by relevance, evidence
    grade, and recency, and each hit carries supporting detail: which field matched,
    who asserted it, at what grade, when, and a snippet, so you can weigh what you
    find, not just find it. Use it to check whether something has already been
    decided or learned before re-deriving it. Zero-hit queries are logged for
    monitoring."""
    pool = await _pool_get()
    ident = await _ident_for(ctx)
    spec = {"op": "function", "name": "search",
            "args": {"q": query, "limit": limit,
                     "caller": (ident.agent_id if ident else None)}}
    out = await comp.run_spec(pool, spec, None, name="search",
                              caller=(ident.agent_id if ident else None))
    items: dict[str, Any] = out["items"]  # unwrap the composition envelope
    await _stamp_read_ids(pool, ident, "search",
                          [h["id"] for h in items.get("hits", [])])
    return items


@mcp.tool()
async def practices(
    surface: str | None = None, limit: int = 50, recent: bool = False,
    ctx: Context | None = None,
) -> list[dict[str, Any]]:
    """The engineering technique log. Call it on demand; it is never included
    automatically in orient's output. `surface` narrows results to one domain (e.g.
    'deploy', 'succession'); if omitted, returns every active practice, most-confirmed
    first. `confirmed` is a live count of supporting witnesses, not a stored number. A
    refuted practice still appears in the list, carrying `refuted_by` so it is flagged
    rather than hidden. `recent=True` ranks the most recently updated practices
    first."""
    pool = await _pool_get()
    spec = {"op": "function", "name": "practices",
           "args": {"surface": surface, "limit": limit, "recent": recent}}
    out = await comp.run_spec(pool, spec, None, name="practices")
    items: list[dict[str, Any]] = out["items"]
    return items


@mcp.tool()
async def trace_evidence(ref: str, limit: int = 200, ctx: Context | None = None) -> dict[str, Any]:
    """Returns one object's full provenance timeline: how the graph came to believe
    what it currently believes about it. Includes every assertion (with its
    supersession history), every link (both directions, with retractions marked), and
    every kernel event, in observed order, each carrying its source, evidence grade,
    and confidence; `believes` holds the current winning view. Where `search` finds
    what is known, this shows how it came to be known. Run it before trusting a
    surprising fact, before merging or healing an object, or to inspect a retired or
    merged object (a uuid ref reaches those too). `ref` accepts a uuid, a canonical
    id (e.g. 'agent:ad1a1cb0'), or a name."""
    pool = await _pool_get()
    ident = await _ident_for(ctx)
    spec = {"op": "function", "name": "lap", "args": {"ref": ref, "limit": limit}}
    out = await comp.run_spec(pool, spec, None, name="lap",
                              caller=(ident.agent_id if ident else None))
    items: dict[str, Any] = out["items"]
    return items


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "composition(action='run', name='census-seat-property-contradictions'"
                  " or name='census-cohort')",
    "since": "A dedicated tool was the wrong shape for this; census was already"
             " available as a composition function.",
})
async def graph_census(kind: str) -> dict[str, Any]:
    """Deprecated. Kept callable as a hidden alias. Forwards to
    composition(action='run', name='census-seat-property-contradictions' or
    name='census-cohort')."""
    pool = await _pool_get()
    spec = {"op": "function", "name": "census", "args": {"kind": kind}}
    out = await comp.run_spec(pool, spec, None, name="census")
    items: dict[str, Any] = out["items"]
    return items


@mcp.tool()
async def graph_lint(stale_days: int = 14, check: str | None = None, limit: int | None = None,
                     offset: int = 0) -> dict[str, Any]:
    """The graph audits itself. Report-only, it never writes. Checks: contradiction,
    laundering (a fact carrying more confidence than its origin grade supports),
    lineage integrity (succession cycles, dangling heirs, false mints), orphan
    links, stale obligations (older than `stale_days`), attribution anomalies,
    phantom twins, parallel lives, duplicate works_in, peer-silent (no mail in
    `stale_days` between an active peer_of pair, a proxy signal rather than proof),
    and held-past-deadline. Findings are evidence for a person to judge, never
    applied automatically; fix problems with compensating events, never by
    deleting data.

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
    """Evaluates the health of the object set itself. Read-only, no writes. `mode`:

    'census' (default): one row per (type, status), with count, orphans, thin (1-2
    links), median/max links, born, and last_touch.

    'buckets': requires `object_type`. One row per object, in exactly one bucket,
    chosen by priority: contradicted (2+ live non-superseding values), then
    duplicate_suspect (case-folded basename collision), then bulk_import
    (`cohort_min`+ objects born the same second with an identical link fingerprint),
    then orphan, then hub (at or above the 95th-percentile link count, floor 10),
    then stale (untouched past `stale_days`), then thin, then normal. Every object
    in scope is listed, not just flagged ones. `limit`/`offset` page results
    (default 200/0, capped at 2000).

    `object_type='Type'`: lists Type rows instead, bucketed as undescribed, then
    no_label_rule (kind='object' with a blank label_field), then normal."""
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
    """The graph's ontology: the object types (with category and canonical schemes)
    and link types it declares. Read this before authoring a composition or reading
    a result, so you reference real types and links instead of guessing; it is the
    vocabulary of the whole graph. Compact by design (colors and shapes are dropped;
    those are for the UI only). Reads the live type catalog directly from the
    database, not a static seed file, so a newly created type shows up here as soon
    as it exists."""
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
    """A table's actual Postgres shape: columns (name, type, nullable, default) in
    column order, plus indexes (name, definition), read directly from
    information_schema and pg_indexes. get_schema answers a different question (the
    ontology this app's code declares: object and link types, categories, canonical
    schemes); this answers what the database actually has, for when you need a real
    column name or type. Returns `exists: false`, rather than a silently empty
    shape, when `table` does not match anything real."""
    if table == "nags":
        return {"nags": _NAG_CATALOG}
    if table.startswith("nags:"):
        code = table.split(":", 1)[1]
        text = _NAG_CATALOG.get(code)
        return {"code": code, "text": text} if text else {"exists": False, "code": code}
    if table == "seat":
        return {"verbs": sorted(_SEAT_MANUAL),
                "hint": "describe('seat:<verb>') for one verb's full text"}
    if table.startswith("seat:"):
        verb = table.split(":", 1)[1]
        # correct-agent-house -> correct-agent-project: the CLI's own
        # `aliases=["correct-agent-house"]` on its `correct-agent-project` subparser means
        # `args.command` can still read either spelling verbatim. This mirrors that so the
        # deprecated spelling still resolves to the one manual entry, never a second copy.
        verb = {"correct-agent-house": "correct-agent-project"}.get(verb, verb)
        text = _SEAT_MANUAL.get(verb)
        return {"verb": verb, "text": text} if text else {"exists": False, "verb": verb}
    return await describe_table(await _pool_get(), table)


@mcp.tool()
async def smoke() -> dict[str, Any]:
    """A deploy-time liveness check. Walks every route in smoke.CHROME_ROUTES (the
    route list lives there, not duplicated in this docstring, so it cannot go stale
    as routes change) and runs one real query over this server's own connection
    pool. A static check cannot catch every failure mode here: a warm-up step that
    checks the wrong pool can pass every build-time gate and still break at real
    boot, and only a live call catches that. Call this right after a restart, not
    just once at boot. `ok=false` names exactly which surface failed, never a bare
    pass/fail flag."""
    pool = await _pool_get()
    async with httpx.AsyncClient(
        base_url=get_settings().osiris_console_base_url, timeout=5.0,
    ) as client:
        return await run_smoke(client, pool)


@mcp.tool()
async def identify_agent(ref: str) -> dict[str, Any]:
    """Gives one coherent answer about an agent, a seat, or a working directory.
    `ref` is auto-detected: an `agent:` id, a `seat:` id, a bare handle, or an
    absolute path (`/...` or `~/...`). Always returns {ref, resolved, matches: [...]}.
    An agent, seat, or handle resolves to zero or one match (one identity, with
    lineage folded in); a path resolves to zero or more matches, since a working
    directory can be shared by more than one agent. Seat binding is read from the
    live graph link, never a cached column, so this result cannot go stale that
    way."""
    return await _doors_lookup(await _pool_get(), ref)


@mcp.tool()
async def recall(ref: str, kind: str | None = None, ctx: Context | None = None) -> dict[str, Any]:
    """Returns the full, untruncated record for a thread or decision. Use this after
    orient()'s short summary leaves you wanting the whole thing. `ref` accepts a
    UUID, the short id orient() already gives you, or a summary substring. `kind`
    ('thread' or 'decision') skips auto-detection when you already know which type;
    if omitted, tries thread then decision. Refuses with a clear error when nothing
    matches either type, rather than guessing or widening into a fuzzy search (use
    search(query=...) for that). Carries `notes` (additions from annotate_thread,
    oldest first) on a thread, or `addenda` (additions from amend_decision, oldest
    first) on a decision; always a list, empty when there are none."""
    from src.orchestrator.recall import recall as _recall
    pool = await _pool_get()
    rec = await _recall(pool, ref, kind=kind)
    canon = rec.get("canonical")
    if canon:
        ident = await _ident_for(ctx)
        oid = await pool.fetchval("SELECT id FROM objects WHERE canonical=$1", canon)
        await _stamp_read_ids(pool, ident, "recall", [oid] if oid else [])
    return rec


# --- collect (federate a base) ----------------------------------------------

@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "Retired after no MCP traffic was observed and no other caller was found.",
})
async def aim_entity(name: str) -> dict[str, Any]:
    """Resolves a name on Wikidata and ingests the entity, its relationships, and
    its official social accounts. The broadest first pull for a company or
    person."""
    return await wikidata_aim(Actions(await _pool_get()), name)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "Retired after no MCP traffic was observed and no other caller was found.",
})
async def ingest_form_d(name: str) -> dict[str, Any]:
    """Pulls a private company's SEC Form D financing rounds: officers, amounts, and
    the feeder SPVs that fund it, and links them into the graph."""
    return await aim_form_d(Actions(await _pool_get()), name)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "Retired after no MCP traffic was observed and no other caller was found.",
})
async def expand_operator(name: str) -> dict[str, Any]:
    """Pulls every Form D mentioning this operator, exposing their whole portfolio
    and its co-investment network."""
    return await expand_filings(Actions(await _pool_get()), name)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "Retired after no MCP traffic was observed and no other caller was found.",
})
async def lookup_lei(name: str) -> dict[str, int]:
    """Looks up an entity in the GLEIF global LEI registry (no API key required):
    its Legal Entity Identifier, jurisdiction, status, and corporate ownership
    parents (direct and ultimate). The LEI is a deterministic global key, so it can
    cross-resolve the same company across different data sources."""
    return await aim_gleif(Actions(await _pool_get()), name)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "Retired after no MCP traffic was observed and no other caller was found.",
})
async def verify_bc_entity(name: str) -> dict[str, int]:
    """Looks up a company or partnership in the Canadian (British Columbia)
    corporate registry via OrgBook BC (no API key required), including a family of
    related names such as 'Brilliant Phoenix'. Returns its BC registration number,
    CRA business number, type, status, and jurisdiction. Verifies registration and
    legal existence only, not directors or owners. Cross-resolves to EDGAR."""
    return await aim_orgbook(Actions(await _pool_get()), name)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def ingest_trials(sponsor: str) -> dict[str, int]:
    """ClinicalTrials.gov: a sponsor's registered human trials, including status, sites
    (facilities), and named investigators."""
    return await aim_trials(Actions(await _pool_get()), sponsor)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def ingest_litigation(name: str, opinions: bool = False) -> dict[str, int]:
    """Court records (CourtListener): lawsuits and enforcement actions naming this
    entity, including dockets, parties, and judges. opinions=True searches case law
    instead of RECAP dockets. Answers whether this entity has been sued or charged."""
    return await aim_litigation(Actions(await _pool_get()), name, kind="o" if opinions else "r")


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def trace_wallet(address: str, chain_id: int = 1, top: int = 25) -> dict[str, Any]:
    """Traces an EVM crypto address on-chain (Etherscan): its top counterparties, native
    balance, token flow, and contract/token identity, graded as ledger ground truth.
    chain_id 1=Ethereum, 8453=Base, 42161=Arbitrum. Needs ETHERSCAN_API_KEY (free)."""
    return await aim_address(Actions(await _pool_get()), address, chain_id=chain_id, top=top)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def screen_wallet(address: str, chain_id: int = 1) -> dict[str, Any]:
    """Screens a traced EVM address against the federated sanctions base: checks whether
    the address, or any of its counterparties, is an OFAC-listed wallet. Returns the
    sanctioned hits and the named holder behind each. Run trace_wallet and ingest
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
        return {"error": f"no traced address {address!r}. Run trace_wallet first."}
    return await screen_against_sanctions(pool, uuid.UUID(str(oid)))


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def expand_clinical_site(facility: str) -> dict[str, int]:
    """The trials at a clinical site, showing which other sponsors use it."""
    return await expand_facility(Actions(await _pool_get()), facility)


@mcp.tool()
async def consolidate(ctx: Context | None = None) -> dict[str, Any]:
    """Graph hygiene: re-types mis-ingested entities (GP/LLC "persons" to Organizations),
    then queues and resolves cross-base merges (same company across bases), and collapses
    SPV-name company variants. Run after collecting data to de-fragment entities.
    Restricted to operator accounts: this is a whole-graph automatic merge sweep with no
    per-merge review, not a per-object action any mounted caller should trigger casually.
    Refuses the call if the actor is not authorized."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first. A consolidation sweep must be attributed to a "
                         "caller, and the graph needs to know who is running it.",
                         "why": _anchorless(ctx)}
    from src.orchestrator.seats import _OPERATOR_ACTORS
    if ident.agent_id not in _OPERATOR_ACTORS:
        return {"error": f"{ident.agent_id!r} is not authorized to run consolidate. This "
                         "is an operator-only whole-graph merge sweep, not a per-object "
                         "action any mounted caller may trigger."}
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
async def dossier(object_ref: str, want_relationships: bool = False,
                  ctx: Context | None = None) -> dict[str, Any]:
    """Identity properties and the named relationship network for one object. `object_ref`
    accepts a UUID, an 8-char short id (the same one a composition row's own "id" column
    hands out), a canonical, or a name. For an agent specifically, this is where succession
    lives: `succeeded_from`/`minted_because` show up both as properties and as a
    `succeeded_from` relationship edge naming the predecessor, one hop back per call. To
    walk the full multi-generation chain in one bounded call, use `succession_chain`
    instead.

    `want_relationships=True` returns every relationship row; default is a per-type
    count plus the first 10, since a high-degree object's full relationship list can
    dominate this call's response size."""
    pool = await _pool_get()
    oid = await _resolve(pool, object_ref)
    if not oid:
        return {"error": f"no object {object_ref!r}"}
    ident = await _ident_for(ctx)
    await _stamp_read_ids(pool, ident, "dossier", [oid])
    out = await entity_dossier(pool, oid, want_relationships=want_relationships)
    # Resolved via alias, never silent: a rename migrates the canonical, so when the ref
    # the caller typed is a retired canonical, say so and name the live one.
    if out and object_ref != out.get("canonical") and await pool.fetchval(
            "SELECT 1 FROM object_aliases WHERE alias=$1 AND object_id=$2",
            object_ref, oid):
        out["resolved_via_alias"] = {"alias": object_ref, "canonical": out.get("canonical")}
    return out


@mcp.tool()
async def object_events(object_ref: str, event_type: str | None = None) -> dict[str, Any]:
    """Merge/unmerge/split events plus same_as/not_same_as links for one object,
    read-only; the history dossier() does not show. `object_ref` accepts anything
    dossier does; `event_type` narrows to one kind, default every kind oldest-first.
    Answers whether a merge or unmerge actually happened, without needing raw SQL."""
    pool = await _pool_get()
    oid = await _resolve(pool, object_ref)
    if not oid:
        return {"error": f"no object {object_ref!r}"}
    out = await _read_object_events(pool, oid, event_type=event_type)
    if not out:
        return {"error": f"no object {object_ref!r}"}
    return out


@mcp.tool()
async def succession_chain(ref: str, max_hops: int = 10) -> dict[str, Any]:
    """An agent's succession lineage, one entry per generation walked backward:
    {agent_id, generation, minted_because, wrote_anything, session}. dossier() only
    gives one hop; this walks the whole chain in one call. `ref` accepts anything
    dossier does (UUID, short id, canonical, name). Stops at a root (no predecessor) or
    at `max_hops` (default 10); it never widens into an unbounded search. `session` is
    each generation's own mount()-asserted harness session id, the transcript filename's
    stem. Complementary to `nearest_handoff_ancestor` (used by orient()'s own
    succession-note section), which jumps to the nearest handoff ancestor rather than
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
    """A provenance-annotated Markdown dossier for an entity: identity, financing,
    litigation, footprint discrepancy, and co-investment, with every claim carrying
    its source, how it was obtained, and its date. Run the collect tools first."""
    pool = await _pool_get()
    oid = await _resolve(pool, object_ref)
    return await build_dossier_report(pool, oid) if oid else f"# no object {object_ref!r}"


@mcp.tool()
async def handoff_briefing(
    repo: str, agent_ref: str | None = None, since: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """A succession briefing compiled from the graph, not hand-written from memory. For
    `repo`, it reports what shipped (decisions since the boundary, each with its deploy
    status), what's open and whose move it is next, what's gated on the operator, what
    was corrected (supersedes chains), and a best-effort flag for self-declared,
    unconfirmed text (such as "UNVERIFIED").

    `since` defaults to the boundary found by walking your own mounted lineage (or
    `agent_ref`'s) back through succeeded_from for the freshest handoff marker; pass an
    explicit ISO-8601 date to override. Returns structured data plus a rendered
    `markdown` ending in an empty judgment section: the compiled facts are the point,
    and your own prose fills the rest. Read-only, renders on demand, never creates
    anything itself. Pair with record_decision(..., is_handoff=True) / settle() once
    judged."""
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
            return {"error": "mount first. handoff_briefing walks your own lineage by "
                             "default; pass agent_ref to preview another agent's "
                             "lineage instead."}
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

# Room is deleted, not renamed: create_room had already been carrying
# meta={"deprecated": True} (zero MCP traffic, no CLI/daemon/slash bypass found), and this
# MCP surface is now removed outright, alongside list_rooms (the same retired concept:
# "rooms" as a concept really did go unused and became obsolete). The underlying
# orchestrator.compositions.create_room/list_rooms functions and the `rooms` table itself
# are untouched here: the retirement migration's own rule is that this is reversible, not
# a delete, so the `rooms` table itself is not dropped and stays as read-only history. This
# pass removes only the MCP entry points that could mint or list rooms going forward,
# matching the console/CLI surfaces that already stopped exposing them. A separate, later
# follow-up removed the composition() dispatcher's own `room` save-time parameter and the
# /rooms REST routes (src/api/app.py), the entry point that could scope a composition to a
# room at save time, a different surface from this one. `resolve_room`/`save_composition`'s
# own `room_id` parameter in orchestrator.compositions are likewise untouched by either
# pass; `save_composition` already falls back to the 'engineer' room by name on a create
# with no room_id at all (its own docstring), so removing the caller-supplied path changes
# nothing about a composition's own visibility.


# The composition object-type dispatcher: the second object-type dispatcher,
# save_composition/run_composition/list_compositions folded into composition(action=...).
# Re-scanned and approved after the seat dispatcher's own review: the old rule required
# return-type/param coherence to fold; the new rule tolerates divergent per-action return
# shapes via a hand-built oneOf schema plus an action-table docstring. This cluster was
# correctly declined under the old rule, correctly re-approved under the new one. Small on
# purpose (3 actions): no param unification was needed, since none of the three originals
# used a divergent name for the same concept.
COMPOSITION_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "oneOf": [
        _dispatcher_action_schema({
            "action": _action_const("save"), "name": _s(), "spec": _obj_s(),
            "kind": _s(),
        }, ["action", "name", "spec"]),
        _dispatcher_action_schema({
            "action": _action_const("run"), "name": _s(), "subject": _opt_s(),
            "fields": _opt_list_s(), "take": _opt_int_s(), "depth": _opt_int_s(),
            "offset": _opt_int_s(),
        }, ["action", "name"]),
        _dispatcher_action_schema({
            "action": _action_const("list"),
        }, ["action"]),
    ],
}
_HAND_BUILT_SCHEMAS["composition"] = COMPOSITION_INPUT_SCHEMA

_COMPOSITION_ACTION_PARAMS: dict[str, tuple[list[str], list[str]]] = {
    "save": (["name", "spec", "kind"], ["name", "spec"]),
    "run": (["name", "subject", "fields", "take", "depth", "offset"], ["name"]),
    "list": ([], []),
}


async def _composition_impl(
    action: str, *,
    name: str | None = None, spec: dict[str, Any] | None = None, kind: str = "lens",
    subject: str | None = None, fields: list[str] | None = None,
    take: int | None = None, depth: int | None = None, offset: int | None = None,
    ctx: Context | None = None,
) -> dict[str, Any] | list[dict[str, Any]]:
    """Shared body behind `composition` and its 3 hidden single-purpose aliases
    (save_composition, run_composition, list_compositions): one code path, three
    names. Every branch's body below is copied verbatim from what was that alias's own
    top-level function. Return type is a union (dict for save/run, list for list)
    matching the three originals' own divergent shapes; the current fold rule tolerates
    this via the hand-built oneOf schema plus this action table, unlike the old rule that
    required return coherence.

    Pre-dispatch validation, the same discipline as _seat_impl's own."""
    if action not in _COMPOSITION_ACTION_PARAMS:
        return {"error": f"unknown action {action!r}",
                "known_actions": sorted(_COMPOSITION_ACTION_PARAMS)}
    accepted, required = _COMPOSITION_ACTION_PARAMS[action]
    local = dict(locals())
    missing = [p for p in required if local.get(p) in (None, "")]
    if missing:
        return {"error": f"action {action!r} is missing required param(s) {missing}",
                "action_accepts": accepted, "action_requires": required}

    if action == "save":
        assert name is not None and spec is not None  # pre-dispatch validation guaranteed this
        pool = await _pool_get()
        cid = await comp.save_composition(pool, name, spec, kind)
        return {"id": str(cid), "name": name}
    if action == "run":
        assert name is not None  # pre-dispatch validation guaranteed this
        pool = await _pool_get()
        ident = await _ident_for(ctx)
        sid = await _resolve(pool, subject) if subject else None
        res = await comp.run_composition(pool, name, sid,
                                         caller=(ident.agent_id if ident else None),
                                         fields=fields, take=take, depth=depth, offset=offset)
        await _set_console(pool, by="claude", composition=name,
                           **({"focused_object_id": sid} if sid else {}))
        return res
    if action == "list":
        pool = await _pool_get()
        return await comp.list_compositions(pool)
    raise AssertionError(f"action {action!r} passed validation but has no branch")


@mcp.tool()
async def composition(
    action: str, name: str | None = None, spec: dict[str, Any] | None = None,
    kind: str = "lens", subject: str | None = None,
    fields: list[str] | None = None, take: int | None = None, depth: int | None = None,
    offset: int | None = None, ctx: Context | None = None,
) -> dict[str, Any] | list[dict[str, Any]]:
    """The composition dispatcher: one tool, three actions over saved compositions
    (reusable, forkable queries/lenses over the graph). See `describe('composition')`
    for the full per-action shape.

    Actions (what each does, and its required params beyond action):
      save: save a reusable query/lens (name, spec; kind defaults to 'lens'). `spec` is
        a small closed op-tree (no `join`, use intersect/traverse instead; fuzzy
        matching is a Function): subject (the focus object); select (object_type?,
        where=[{property,op,value}], op in eq|contains|matches_all|lt|gt|present|
        absent); traverse (from, direction=both|out|in, hops<=3); collect (from,
        properties, transform=country|lower); subtract/union/intersect (over sets);
        aggregate (from, group_by<=3 dims, metric={type: count|sum|avg|min|max|
        cardinality, field}); order (from, by, dir); take (from, n). Worked examples:
        consult_canon('composition spec').
      run: run a saved composition, optionally against a subject object (UUID or name),
        and show it on the operator's live screen (name). `fields`/`take`/`depth` bound
        a large result at the source; `offset` pages past the first `take`.
      list: the saved compositions (lenses/watches), the user's questions saved as
        objects.
    """
    return await _composition_impl(
        action, name=name, spec=spec, kind=kind, subject=subject,
        fields=fields, take=take, depth=depth, offset=offset, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "composition(action='save')",
    "since": "task #202 composition dispatcher (msg 7073/7095)",
})
async def save_composition(
    name: str, spec: dict[str, Any], kind: str = "lens"
) -> dict[str, str]:
    """Deprecated: hidden alias, still callable. Forwards to
    composition(action='save')."""
    return cast(dict[str, str],
               await _composition_impl("save", name=name, spec=spec, kind=kind))


# --- the shared console (real-time Claude↔front sync) -----------------------

@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def get_console() -> dict[str, Any]:
    """What the operator is looking at right now: the shared cursor (room / composition /
    view / focused object). Read this first to see their screen before you act."""
    return await _get_console(await _pool_get())


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "task #199 lane 2, retirement wave 1 (msg 6822)",
})
async def focus_object(object_ref: str, ctx: Context | None = None) -> dict[str, Any]:
    """Focus an object (UUID or name) on the operator's live screen: drives the console
    so they see what you're looking at. Returns the object's identity and properties so
    you can reason about it too."""
    pool = await _pool_get()
    oid = await _resolve(pool, object_ref)
    if oid is None:
        return {"error": f"no object matches {object_ref!r}"}
    # cross-tenant boundary: another tenant's reflection answers exactly like a missing
    # object, and is never pushed onto the screen for a caller that can't read it
    if await pool.fetchval("SELECT type FROM objects WHERE id=$1", oid) == "Reflection":
        ident = await _ident_for(ctx)
        vis = await comp._visible_reflections(
            pool, [oid], ident.agent_id if ident else None)
        if oid not in vis:
            return {"error": f"no object matches {object_ref!r}"}
    # focusing is explore mode: clear the active composition so it doesn't re-run on top
    await _set_console(pool, by="claude", focused_object_id=oid, composition=None)
    row = await pool.fetchrow("SELECT type, canonical FROM objects WHERE id=$1", oid)
    props = await pool.fetch(
        "SELECT name, value #>> '{}' AS value FROM current_assertions WHERE object_id=$1", oid
    )
    return {"focused": str(oid), "type": row["type"], "canonical": row["canonical"],
            "properties": {p["name"]: p["value"] for p in props}}


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "composition(action='run')",
    "since": "task #202 composition dispatcher (msg 7073/7095)",
})
async def run_composition(
    name: str, subject: str | None = None,
    fields: list[str] | None = None, take: int | None = None, depth: int | None = None,
    offset: int | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    composition(action='run')."""
    return cast(dict[str, Any],
               await _composition_impl("run", name=name, subject=subject, fields=fields,
                                       take=take, depth=depth, offset=offset, ctx=ctx))


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "composition(action='list')",
    "since": "task #202 composition dispatcher (msg 7073/7095)",
})
async def list_compositions() -> list[dict[str, Any]]:
    """Deprecated: hidden alias, still callable. Forwards to
    composition(action='list')."""
    return cast(list[dict[str, Any]], await _composition_impl("list"))


@mcp.tool()
async def list_functions() -> list[str]:
    """The registered functions a composition may reference via
    {"op":"function","name":..}: the escape hatch for analytics the closed op set can't
    express (co-investment ties, sanctions screening, the who-is-this report). Reference
    one in a spec instead of re-deriving its logic."""
    return comp.list_functions()


@mcp.tool()
async def consult_canon(query: str = "", ctx: Context | None = None) -> dict[str, Any]:
    """Looks up reference material in the shared design canon (Palantir's Object Set /
    Ontology / Action models, Notion's databases / relations-rollups / UI-UX, and
    Osiris's own docs), and, when you're mounted, your project's migrated history
    (ref:<project>-*, ingested by bootstrap). Use this to cite existing design guidance
    instead of re-deriving it, and to recall your own project history instead of
    re-reading it into context on every turn. Given a topic, module path, design word,
    or a set of keywords, returns the matching sections ranked by keyword hits
    (multi-word queries work). An empty query returns your scoped index. Another
    project's unvendored history is never returned to you."""
    pool = await _pool_get()
    ident = await _ident_for(ctx)
    spec = {"op": "function", "name": "canon",
            "args": {"q": query, "project": (ident.project if ident else "") or ""}}
    return await comp.run_spec(pool, spec, None, name="design-canon")


@mcp.tool()
async def context_window(ctx: Context | None = None) -> dict[str, Any]:
    """Your own context window, in detail: how close this session is to its next
    compaction boundary. Reads the harness's usage record off your own transcript:
    occupancy (fresh input plus cache read plus cache write), window tier ([1m] tabs =
    1M tokens, else 200k), remaining headroom, and this session's compaction count so
    far (each compaction starts a new session in the lineage). Above 80% usage it tells
    you plainly to write back now: record_decision / resolve_thread whatever is still
    only in your head, because a compaction can land on any turn, and anything not
    written to the graph is lost to whatever picks up after you. Requires a mounted,
    anchored session (the transcript is found by your durable job_dir)."""
    from src.ingest.sessions import locate_current_transcript
    from src.orchestrator import context_lens

    pool = await _pool_get()
    ident = await _ident_for(ctx)
    if ident is None:
        return {"why": _anchorless(ctx),
                "error": "mount(cwd, job_dir=<your anchor>) first. This tool needs an "
                         "anchored identity to read your usage."}
    row = await pool.fetchrow(
        "SELECT job_dir, model_raw, context_window_size FROM agent_mounts WHERE agent_id=$1 "
        "ORDER BY last_seen DESC LIMIT 1", ident.agent_id)
    job = _job_hint(ctx) or (row["job_dir"] if row else None)
    if not job:
        return {"error": "no durable anchor on record. Re-mount with your job_dir."}
    # Prefer the live file first: the harness's own transcript is current to the last turn
    # and compaction-aware, while a store row is only as fresh as its last ingest, and the
    # 85% write-back alarm must never rely on a stale mount-time snapshot. The store serves
    # the sessions the JSONL path cannot see (Crush, etc.), refreshed at call time: a cheap
    # stat plus a delta read, never a full re-ingest.
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
    except Exception:  # noqa: BLE001 : never block context_window on an ingest hiccup
        pass
    for adapter in (ClaudeJsonlAdapter(), CrushSqliteAdapter()):
        try:
            locator = adapter.discover(cwd=ident.cwd, job_dir=job)
        except Exception:  # noqa: BLE001 : never block context_window on an adapter
            locator = None
        if locator is None:
            continue
        usage_row = await store.last_usage_of_session(locator.harness, locator.anchor_sid)
        if usage_row is None:
            continue
        usage = context_lens._usage_from_store(usage_row)  # noqa: SLF001 : pure adapter
        if usage is None:
            continue
        out = context_lens.detail_from_usage(
            usage, model_raw, window_hint=window_hint)
        out["agent"] = ident.agent_id
        out["source"] = f"store:{locator.harness}"
        out.update(await _overhead_glance(pool, ident.cwd, job))
        return out
    return {"error": "no transcript found for your anchor. Nothing to measure."}


async def _overhead_glance(
    pool: asyncpg.Pool, cwd: str | None, job: str | None,
) -> dict[str, Any]:
    """A bounded overhead block for context_window: this session's hidden-channel share,
    reminder drip, and cache split, read from the store (a background backfill keeps the
    channel rows about 10 minutes current). Empty when the store hasn't ingested the
    session yet, an absence, never an estimate. The full per-channel detail stays on the
    console's /overhead page; a caller here wants the shape, not the full ledger."""
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
    except Exception:  # noqa: BLE001 : the glance must never break the window reading
        return {}


# --- mount: link to the graph as a first-class fleet member ---

def _terse(payload: dict[str, Any], *paths: tuple[str, ...]) -> dict[str, Any]:
    """Strip prose-only key paths for a terse result, verbose=False being the default. An
    explicit, hand-reviewed allowlist per tool, never a generic 'strip long strings'
    heuristic: that's how you'd drop a structural field like `seat` or a job's
    `sessionId` that just happens to be long. A field consumed as data by another function
    must never be silently dropped by a blind length check. Each path names a chain of
    dict keys ending in the prose key to remove; a path through a key that isn't present (a
    conditional field this particular result never populated) is a silent no-op. Mutates
    and returns `payload` so terse and verbose stay byte-identical apart from exactly the
    declared keys."""
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
                    # file: unread_echoes.triage, the un-mounted branch's recent_decisions


def _cap_text(items: list[dict[str, Any]], key: str, limit: int = _SUMMARY_CAP,
             *, exempt_when_true: str | None = None) -> list[dict[str, Any]]:
    """Truncate `key` on each row to `limit` chars for a terse result. Measured, not
    guessed: on the real dev graph, `summary` text is 96-98% of every
    open_threads/recent_decisions item's bytes, and this one cap took orient()'s scoped
    payload from 66060 to 10623 bytes (-83.9%), two orders of magnitude past what
    stripping guidance prose alone reached (_terse, -1%).

    A separate primitive from _terse() on purpose: truncating a string and deleting a key
    are different operations, and mixing them would make either harder to reason about.
    Unlike the existing [:160]/[:800] slices elsewhere in this file, truncation here is
    never silent: an explicit '…' marks a shortened value, because a truncated summary
    that reads as complete is worse than one that visibly isn't (the same principle that
    made reachability()'s `detail` a required field, not a nice-to-have: a caller must be
    able to tell 'this is all of it' from 'this is not'). Mutates and returns `items`.

    `exempt_when_true`: a row whose named field reads the literal string 'true' is
    surfaced whole, cap skipped entirely, which is is_handoff's real job. Settle certifies
    that a session wrote; nothing certified that a successor could read, and the gap is not
    theoretical: a predecessor session once left a correctly-filed, durable
    confessed-mistakes handoff, orient() capped it to 160 chars, and the successor acted on
    the fragment and repeated the exact mistake it confessed. The cap itself stays, since
    the measured savings are real (96-98% of the payload); this exempts the one record
    class written to be read exactly once, by exactly one reader, at the moment they have
    the least context to fill a gap."""
    for row in items:
        if exempt_when_true and row.get(exempt_when_true) == "true":
            continue
        val = row.get(key)
        if isinstance(val, str) and len(val) > limit:
            row[key] = val[:limit] + "…"
    return items


def _seam_confidently_dated(ident: AgentIdentity) -> bool:
    """mount() must never assert a model-seam it cannot date with confidence: orient() is
    the single source of truth for the seam. This was learned from a race observed by
    multiple independent sessions: mount() once minted a model succession from one model
    to another and told the agent to confess a mismatch that the very next orient() said
    never happened. Acting on mount() alone delivers a false alarm as fact. Confident means
    both sides of the claimed seam are known values, observed on this identity's own row,
    job_dir-anchored, never a cwd guess or a foreign transcript (mirrors the null-seam
    gate: an unanchored or half-known reading is an absence of evidence, not a seam to
    speak from). No seam claimed at all is trivially confident: there is nothing to
    mis-date."""
    if ident.model_method != "job_dir" or not ident.model:
        return False
    if not ident.model_succession:
        return True
    sides = ident.model_succession.split(" → ", 1)
    return len(sides) == 2 and bool(sides[0].strip()) and bool(sides[1].split(" [", 1)[0].strip())


_CO_AGENTS_DISPLAY_CAP = 8


async def _co_agents(pool: asyncpg.Pool, project: str, agent_id: str) -> dict[str, Any] | None:
    """Other live agents on this project right now. The underlying "who's live" query is
    `mounts.live_co_agents`, one implementation shared with handshake.py's `automount()`
    (the two used to be independent copies, free to drift, and were unified). Enriched
    here with each sibling's context_pct, since a manager can't route around a context
    limit it can't see, the gap behind mis-assigning a nearly-full worker blind. This is
    the freshest reading osiris_hook.py's `stop` subcommand has stamped on that agent, off
    the same context_lens.ALARM_PCT the hook itself alarms on, never a second copied
    threshold. Absent (no key) when that sibling has never had a reading stamped; staleness
    is spoken plainly via `context_pct_age_s`, since a reading only refreshes at that
    sibling's own stop-hook boundaries, so an old snapshot should never be trusted as
    current. None (not {}) when there are no live siblings at all, so callers can keep
    their existing `if sibs:` / `if co_agents:` shape unchanged.

    Never silently truncated: an earlier bare `LIMIT 8` in this query under-reported a
    live sibling with no signal at all, so the note now names exactly how many more exist
    beyond the display cap, rather than just dropping them."""
    from src.orchestrator.context_lens import ALARM_PCT
    from src.orchestrator.mounts import live_co_agents

    # your own lineage is never counted as another sibling
    _mine = _generation(agent_id)[0]
    all_sibs = await live_co_agents(pool, project=project, exclude_lineage_base=_mine)
    sibs = all_sibs[:_CO_AGENTS_DISPLAY_CAP]
    if not sibs:
        return None
    # One batched pick of each sibling's context_pct (winning_props's own confidence DESC,
    # observed_at DESC per agent), not a LATERAL join per row: the shared query above
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
    """This agent's peer_of partner, made legible beside co_agents: the peer's handle and
    last-seen activity, not just a bare seat id. None when unbound or unpeered, so callers
    keep the same `if peer:` shape co_agents already established."""
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
    """Link this agent to Osiris as a first-class fleet member; call it once, first
    thing. `cwd` is your working directory (names your project). `job_dir` is a durable
    anchor from your harness (Claude Code: `~/.claude/jobs/<id>`; DSH: derived from the
    workspace slug); without it you still mount, but a reconnect will split your
    identity. Registers an Agent (works_in your project, acts_for the principal) and
    attributes your decisions/threads to `agent:<you>` instead of the shared `session`
    bucket. Call orient() next. If you're already mounted, skip this call; re-mount
    only after an MCP restart, with your anchor.

    `verbose=True` restores guidance prose (co-agent etiquette, next-step reminders)
    that terse mode (the default) drops; structured facts survive either way.
    `transcript_path`/`bridge_session_id` are stamped by the harness hook, never set by
    hand; they rebind a revisited tab or background-job fork to its existing identity
    instead of creating a new one. `want_co_agents`/`want_held_work` return the full
    lists; the default is counts only.

    Memory custody: this cwd's own Claude Code memory files
    (~/.claude/projects/<slug>/memory/) are checked against a per-lineage marker. A
    different lineage's memory found here is archived sideways (renamed, never deleted)
    and reported as `prior_lineage_memory_archived` (a pointer to read, never
    auto-copied); pre-existing content with no marker at all is reported as
    `memory_migration_needed` instead of being silently moved."""
    # The confirmed-identity gate: memory custody never runs its own archive action
    # against an Agent object this same call just minted. `mount_call_started_at` here,
    # checked below against the object's own `created_at`, is the check ("confirmed by
    # the graph" means the object predates this call, not merely that one now exists). A
    # same-call mint that turns out to be wrong (a stray job_dir-derived unrecognized
    # session) must never get to rename another lineage's real memory out from under it
    # before anyone has had a chance to notice the mint itself was wrong.
    mount_call_started_at = datetime.now(UTC)
    pool = await _pool_get()
    settings = get_settings()
    lease = settings.osiris_mail_lease_secs
    # A spawn mounting (the anchor hook stamped this call as a sidechain's): the child
    # inherits its parent's $CLAUDE_JOB_DIR and MCP connection, so the normal path would
    # seat it as the parent, which a live repro confirmed: a probe child was greeted with
    # 'writes attributed to you' as if it were the parent. Register it as itself instead:
    # spawned_by the mounted parent, no seat, no durable row, and never a cache write to
    # the shared identity cache (the connection belongs to the parent).
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
            witnessed=True)  # it is calling mount, which is itself an observed act
        _spawns_seen[str(child)] = time.monotonic()
        return {
            "agent": child, "project": parent_ident.project if parent_ident else "?",
            "spawn_of": parent_ident.agent_id if parent_ident else "unknown (parent unmounted)",
            "note": ("you are a spawn: a sub-agent registered in your own name, "
                     "spawned_by your parent. Your writes are attributed to you, never "
                     "to the seat that spawned you; the seat, its mail, and its "
                     "succession belong to your parent. Do the job, and return your "
                     "result to the parent."),
        }
    # An unexpanded `$CLAUDE_JOB_DIR` literal is no anchor, and this is the common case for
    # a fresh agent (MCP tool args never pass through a shell, so the docstring's advice
    # arrives verbatim). The client's .mcp.json/user-scope entry sends the true directory in
    # the X-Osiris-Job header on this very request (client-side expansion, proven live), so
    # fall back to it, so a by-the-book mount is durable and resolved instead of silently
    # degrading to the cwd-guess (an unresolved identity, no registry row, invisible to
    # owner-liveness checks, would otherwise have minted a duplicate over a live session).
    passed = _sane_job_dir(job_dir)
    own_anchor = _sane_job_dir(session_anchor)  # hook-injected: the caller's own session
    # The conflict refusal: after a machine died, a session-launch retry vended a stale
    # anchor from a dead sibling's session, and the mount that followed seated one agent in
    # another's history, with writes interleaving into a sibling's lineage. A passed anchor
    # that differs from the session's own is legitimate when wearing a seat, but when the
    # ledger knows both session ids and they resolve to different registered agents, this
    # is an identity collision: refuse loudly with both names, never silently rebind. No
    # writes happen on a refusal.
    if (passed and own_anchor
            and Path(passed).name[:8] != Path(own_anchor).name[:8]):
        anchor_soul = await handshake.ledger_seat(
            Actions(pool), sid_prefix=Path(passed).name)
        own_soul = await handshake.ledger_seat(
            Actions(pool), sid_prefix=Path(own_anchor).name)
        if (anchor_soul and own_soul
                and _generation(anchor_soul)[0] != _generation(own_soul)[0]):
            return {
                "error": "identity conflict: mount refused",
                "anchor_held_by": anchor_soul,
                "you_are": own_soul,
                "note": (f"the anchor you passed ({Path(passed).name[:8]}) is held by "
                         f"{anchor_soul}, but this session's own ledger entry "
                         f"({Path(own_anchor).name[:8]}) names {own_soul}. Mounting "
                         "would seat one session in another's history. If you meant "
                         "to take over that identity, the holder must release it "
                         "(retire/fold) first; otherwise re-mount with your own "
                         f"anchor: job_dir='{own_anchor}'"),
            }
    job_dir = passed or _job_hint(ctx)
    key = _conn_key(ctx)
    claimed = None
    if job_dir is None:  # the cwd-guess path: refuse session ids a live mount already holds
        claimed = await mounts.live_claimed_sids(
            pool, exclude_session_key=key, within_secs=settings.osiris_owner_live_secs)
    bound = await mounts.find_mount(pool, job_dir=job_dir) if job_dir else None
    # The startup seat rescue: a ghost/stale-connection sweep can release this exact
    # job_dir's row for reasons that have nothing to do with the lineage dying
    # (mounts.rescue_seat_holder_mount's own docstring has the full specimen: one agent's
    # own job_dir, swept repeatedly over several weeks, silently absorbed by some other
    # session every time until a later restart). This never mints an unrecognized session
    # over a lineage that still holds a seat right now.
    if bound is None and job_dir:
        bound = await mounts.rescue_seat_holder_mount(pool, job_dir=job_dir)
    elif bound is not None and job_dir:
        # The self-reinforcing trap: a wrong mint from the gap above registers its own row,
        # so every later restart's find_mount keeps finding the unrecognized session instead
        # of ever reaching the rescue above. A seat holder outranks a seatless row's live
        # claim on this job_dir just as much as it outranks the row's own absence.
        outranked = await mounts.demote_seatless_mount_if_outranked(
            pool, job_dir=job_dir, actor=settings.osiris_actor)
        if outranked is not None:
            bound = outranked
    # The recollection guard: a resumed agent re-mounting after a restart quotes its own
    # history for `cwd`, and an address is exactly what a move makes stale (one agent once
    # re-mounted itself at a demolished former location this way, re-pointing its seated
    # row). When the transcript evidence says the registry's cwd is where this session
    # actually lives and the declared one is not, the harness's observation outranks the
    # agent's memory.
    cwd_note = None
    declared_project_label: str | None = None
    bridge_ambiguity: str | None = None
    if (bound is not None and bound.cwd and bound.cwd != cwd
            and mounts.stale_recollection(job_dir or "", cwd, bound.cwd)):
        # The override must not discard a more-specific declared pin: a live repro showed
        # mount(cwd='.../seats/<agent>') from a session launched at the bare container
        # coming back cwd_corrected{kept: the container}, where the agent's own declared,
        # correct, more-specific working directory was replaced by the session's launch
        # directory, one step from a basename guess being derived off what was left. The
        # correction below is right for what it was built for: the harness's own transcript
        # location is the ground truth for where this session lives, and a resumed agent's
        # memory of a demolished former location must not win that question. But a project
        # pin sitting at the declared cwd is a different question entirely: reading it is
        # not the spoofing stale_recollection guards against, it is a cheap, direct fact
        # the declaring session already had in hand. Read it before `cwd` is corrected
        # below, and if the declared cwd names a real project, it wins identity resolution
        # even though `cwd` itself still corrects for every other purpose (transcript
        # addressing, the session store, the durable registry).
        declared_pin = read_project_pin(cwd)
        if declared_pin.value:
            declared_project_label = declared_pin.value
        # Prefer the real declared working directory: the glob inside stale_recollection()
        # only answers "have I seen this session's transcript under this slug before",
        # never "where does this seat live". A registry row whose last-recorded cwd is the
        # bare seat-office container (~/.osiris/seats, offices.is_bare_office_root) is not
        # evidence of anything; it is the shape every session has before it ever declares a
        # specific office. When the freshly declared cwd is itself a real, existing
        # directory, and not that same bare container, it wins outright: the glob's silence
        # about a path a session simply hasn't visited under this exact slug yet must never
        # overrule a location that demonstrably exists right now. A confident wrong answer
        # (quietly becoming a session rooted at the parent directory of every seat) is worse
        # than deferring to what is actually on disk.
        from src.orchestrator.offices import _dir_exists as _office_dir_exists
        from src.orchestrator.offices import is_bare_office_root as _bare_office_root

        declared_is_real_office = _office_dir_exists(cwd) and not _bare_office_root(cwd)
        kept_is_bare_container = _bare_office_root(bound.cwd)
        if declared_is_real_office and kept_is_bare_container:
            cwd_note = {
                "declared": cwd, "kept": cwd,
                **({"declared_pin_kept_for_identity": declared_project_label}
                   if declared_project_label else {}),
                "note": ("registry recollection pointed at the bare seat-directory "
                         "container (~/.osiris/seats), never a home of its own. Your "
                         "declared cwd is a real, existing seat directory and wins "
                         "outright; nothing was corrected"),
            }
            # cwd is left as the caller's own declared value: no reassignment.
        else:
            # Refuse only the bare container root, never a wall: a session still needs a
            # cwd to mount at for transcript/session bookkeeping even when neither side
            # resolves to a real office, so `cwd` still moves to `bound.cwd` below, but the
            # result must say so honestly rather than asserting the bare container is this
            # session's home (the confession half of the same principle above).
            honest_note = ("your declared cwd is a stale memory of a former home. This "
                            "session's transcript lives at the kept path (it moved; your "
                            "history did not). Mounted at the kept path; update your "
                            "own bearings"
                            + (f", though its own project pin "
                               f"({declared_project_label!r}) still won identity "
                               "resolution; only the transcript/session address was "
                               "corrected"
                               if declared_project_label else ""))
            if kept_is_bare_container:
                honest_note = ("could not resolve a specific seat directory for either "
                                "the declared or the recollected cwd. Mounted at the "
                                "bare seat-directory container for session bookkeeping "
                                "only; this is not your home, it is a fallback with "
                                "nowhere better to point"
                                + (f", though its own project pin "
                                   f"({declared_project_label!r}) still won identity "
                                   "resolution" if declared_project_label
                                   else ""))
            cwd_note = {
                "declared": cwd, "kept": bound.cwd,
                **({"declared_pin_kept_for_identity": declared_project_label}
                   if declared_project_label else {}),
                "note": honest_note,
            }
            cwd = bound.cwd
    # The harness-agnostic transcript store, the sole model-detection path since the
    # JSONL-fallback removal: ingest the current session's turns from whatever harness the
    # operator is running (Claude Code, Crush, etc.), then hand the model reading to
    # resolve_identity so non-Claude sessions mount resolved. Fails open inside the helper.
    store_reading = await identity_reading(pool, cwd=cwd, job_dir=job_dir,
                                           transcript_path=transcript_path)
    ident = resolve_identity(cwd=cwd, job_dir=job_dir, model=model,
                             claimed=claimed, fallback_seed=key,
                             store_reading=store_reading,
                             project_label=declared_project_label)
    # The bare-root refusal was the wrong fix: the operator launches agents from the bare
    # seat-office root on purpose, that is the intended pattern, and the whole point of a
    # seat is that identity is location-independent: orientation resolves from the seat
    # (anchor -> holds -> seat), never from cwd. A hard refusal here fought normal
    # onboarding, since `bound is None` is true for a genuinely fresh, legitimate first
    # launch exactly as much as for the pollution case, so this guard could have refused
    # real new agents, not just healed old corruption, and it was neutralized. What's
    # still true and still kept: resolve_identity never invents a phantom project from the
    # bare root's own basename ("seats"); it stays unresolved from cwd, same as before. The
    # actual fix lives downstream now: a seated session's project resolves from the seat's
    # own derived project (_resolve_project_seat_first, below), not cwd, so identity
    # survives a bare-root launch by being location-independent, not by refusing the
    # location.
    forked = viewed = ledgered = bridged = None
    if bound is not None:
        # No local re-import of _generation here: a local import anywhere in a function
        # shadows the module-level name for the whole function, and this branch is
        # conditional, so every unbound session (each anonymous agent, each fresh child)
        # skipped it and died at the sibs filter below with UnboundLocalError. The whole
        # claim path was down for a night on these two lines.
        if _generation(bound.agent_id)[0] != _generation(ident.agent_id)[0]:
            # The binding, the explicit-mount leg: the launcher tells every minted heir
            # "re-mount with this anchor", and automount left that very row bound to the
            # heir's seat. Re-deriving from the anchor's basename here minted a duplicate
            # over a living heir and stomped the binding, confirmed by a live repro on a
            # first-run session. A row naming a foreign lineage is a deliberate seat claim:
            # honor it, so identity resolution and registration run on the seat's lineage,
            # like _reattach.
            ident.agent_id = bound.agent_id
    elif job_dir:
        # The fork, the explicit-mount leg, and this is the path where an agent was once
        # turned away entirely. A forked session has no row for its new anchor, so the old
        # code derived a fresh identity from the anchor's basename and seated one agent
        # twice. That agent could only get its mail out by re-mounting, which minted the
        # very duplicate it was writing to report. Ask the transcript's record uuids who it
        # already is.
        forked = await handshake.fork_seat(Actions(pool), job_dir=job_dir)
        if forked is not None:
            ident.agent_id = forked
        else:
            # The tab view, ported from automount() (which has carried this path since an
            # earlier alias-clone fix; mount() the tool never had it, so a caller with no
            # launcher context minted a clone here where a launcher-greeted one would have
            # adopted). `transcript_path` is hook-stamped (osiris_hook.py's `anchor`
            # subcommand), never hand-supplied: a live tab attached through a new session id
            # whose transcript_path names another session's file is a window onto that
            # session, not an unrecognized one.
            viewed = (await handshake.view_seat(
                Actions(pool), transcript_path=transcript_path,
                session_id=Path(job_dir).name)
                if transcript_path else None)
            if viewed is not None:
                ident.agent_id = viewed
            else:
                # The session ledger: the graph remembers whose session id this is even
                # after a registry accident. A known anchor rebinds, never mints a
                # duplicate.
                ledgered = await handshake.ledger_seat(
                    Actions(pool), sid_prefix=Path(job_dir).name)
                if ledgered is not None:
                    ident.agent_id = ledgered
                elif bridge_session_id:
                    # The bridge, ported from automount()'s own binding leg: a
                    # background-job fork's transcript starts a genuinely fresh record chain
                    # fork_seat cannot see; the harness's own CLAUDE_CODE_BRIDGE_SESSION_ID
                    # (hook-stamped, same lane as transcript_path) names the one continuing
                    # conversation. Same fail-open shape as automount(): ambiguity is
                    # confessed in the payload below, never guessed away and never a hard
                    # refusal; the mount still lands, degraded to the next path (office),
                    # same as a bridge that simply resolved to nothing.
                    try:
                        bridged = await handshake.bridged_seat(
                            Actions(pool), bridge_session_id=bridge_session_id)
                    except handshake.BridgeAmbiguity as e:
                        bridge_ambiguity = str(e)
                        bridged = None
                    if bridged is not None:
                        ident.agent_id = bridged
    # Lived, ported verbatim from automount()'s own computation (handshake.py), not a
    # re-derivation: a fork/ledger/bridge match already proves a lived lineage; a bound row
    # only counts when it names a foreign lineage on purpose (a deliberate binding) or the
    # base generation already has a real Agent object. A row alone is the gate's own
    # artifact (an address), never a life (the row-only class this guards against).
    lived = forked is not None or ledgered is not None or bridged is not None
    if not lived and bound is not None:
        _base = _generation(bound.agent_id)[0]
        if job_dir and _base != f"agent:{Path(job_dir).name[:8].lower()}":
            lived = True
        else:
            lived = bool(await pool.fetchval(
                "SELECT 1 FROM objects WHERE type='Agent' AND (canonical=$1 "
                "OR canonical LIKE $1 || '-%') LIMIT 1", _base))
    # The first act seats you: a still-anonymous agent mounting from a seat's office is
    # the seat's next life; the mint happens at this act, never at the launch trigger.
    mount_mint_reason = None
    claimed_office = await handshake.office_claim(
        Actions(pool), cwd=cwd, agent_id=ident.agent_id)
    if claimed_office is not None:
        ident.agent_id = claimed_office
        mount_mint_reason = "office-birth"
    # Seat-first, before the mint: same fix, same reasoning as `_reattach`'s own identical
    # reorder just above in this file. A `holds` link cannot exist unless its Agent object
    # already does (`links.from_id`/`to_id` are `NOT NULL REFERENCES objects(id)`), so a
    # seated result here is proof the object predates this call, whichever path
    # (bound/forked/viewed/ledgered/bridged/office_claim) resolved `ident.agent_id`; an
    # unseated/visitor identity is an unconditional no-op (`held_seat` finds nothing to
    # match), safe to run even before the registered/visitor branch below decides whether
    # register_agent runs at all.
    await _resolve_project_seat_first(pool, ident)
    # The visitor gate, ported from automount(): automount() has never once minted an
    # unrecognized session from a bare greeting; a genuinely unmatched arrival gets a
    # registry row and nothing else, identity earned at the first authenticated act.
    # mount() is that act site (unlike automount(), which only ever hints at the office and
    # never mints there), so its own predicate is automount()'s own `lived or viewed is not
    # None or (seat_id and attach_token)` with the same `lived` computation, one leg
    # adapted: mount() carries no seat_id/attach_token (that flow is a separate tool,
    # attach_seat); `claimed_office is not None` is its equivalent credentialed act, the
    # first authenticated action in a seat's own office.
    registered = bool(lived or viewed is not None or claimed_office is not None)
    if registered:
        agent_uuid = await register_agent(
            Actions(pool), ident, actor=settings.osiris_actor,
            expected_model=await _expected_model(pool, cwd, ident.project),
            mint_reason=mount_mint_reason)
        # The harness signal: additive-only, never touching register_agent's own
        # identity/succession machinery. A fleet render needs to know which
        # ProcessAdapter's capabilities apply to this session, and until now nothing
        # stamped that fact anywhere.
        await Actions(pool).assert_property(
            agent_uuid, "harness", _infer_harness(cwd, job_dir),
            source_id=ident.agent_id, observed_at=datetime.now(UTC), confidence=0.9,
            actor=settings.osiris_actor)
    elif not ident.resolved:
        # The third state: a visitor (a real anchor that simply matched no lineage) is a
        # different fact from an unresolvable arrival (no anchor at all). Before this gate,
        # resolve_identity's own fallback silently hashed a fresh id here regardless
        # (agent:unknown-<project> / agent:unknown, `identity_resolved=false`, nothing
        # downstream ever read it). That silence is what this refuses, loudly, in the same
        # shape as the identity conflict refusal above: a caller with no launcher context
        # has no greeting to read a refusal from, so the tool's own return value is the
        # only surface that reaches it. No writes happen below a refusal.
        return {
            "error": "unresolvable identity: mount refused",
            "note": ("no job_dir, no session anchor, and no observed transcript sid. "
                     "There is nothing to attribute this session to. Pass job_dir "
                     "(or confirm the PreToolUse hook is installed, "
                     "osiris_hook.py's `anchor` subcommand) so this session carries a "
                     "real, durable anchor. Nothing was minted or written."),
            **({"bridge_ambiguity": bridge_ambiguity} if bridge_ambiguity else {}),
        }
    # else: a genuine visitor, a resolved anchor that matched no lineage. Same as
    # automount()'s own gate: a registry row and nothing else, no Agent object. This is not
    # greatfold.py's `agent_class='visit'`; that property marks an object already minted
    # and later found to be noise, while this gate prevents the mint from happening at all,
    # so there is no object to mark. Deliberately not reused, since a second vocabulary for
    # the same idea is its own kind of drift. (`_resolve_project_seat_first` already ran,
    # above, before the registered/visitor branch, moved there so register_agent's own
    # project mint sees the corrected value instead of running two lines ahead of it.)
    if job_dir:
        # The session ledger, write side: the anchor form (sid8) suffices, since the
        # ledger keys on the first 8 chars, the harness's own jobs scheme
        try:
            await handshake.record_session_anchor(
                Actions(pool), agent_id=ident.agent_id,
                session_id=Path(job_dir).name, actor=settings.osiris_actor)
        except Exception:  # noqa: BLE001 : the ledger is a bonus; the mount never dies of it
            pass
    if key is not None:
        _prune_agents()  # opportunistic: mount is where churn shows up
        _agents[key] = ident
        _agents_touched[key] = time.monotonic()
    if job_dir:  # the durable half: what _ident_for re-attaches by after a restart
        prev = await mounts.save_mount(pool, job_dir=job_dir, agent_id=ident.agent_id,
                                       project=ident.project, cwd=cwd, model=ident.model,
                                       session_key=key)
        if prev is None:  # a fresh session has no own past: anchor on the project lineage's
            # ...and a joiner inherits the project's collective settle-state: sibling-settled
            # broadcasts are not a newcomer's unread (a fix for over-counting stale unreads)
            await mailbox.settle_history_at_join(pool, ident.project, ident.agent_id)
            prev = await mounts.project_prev_seen(pool, ident.project, exclude_job_dir=job_dir)
        _prev_seen[ident.agent_id] = prev  # this mount is the re-entry: anchor the fold here
        # The hand-resume follows the seat: a fresh row for a session that actively holds a
        # Seat re-earns its binding from the durable holds link.
        from src.orchestrator.seats import reseed_binding
        await reseed_binding(pool, agent_id=ident.agent_id, job_dir=job_dir)
        # The binding: a mount with a foreign anchor is a session deliberately wearing a
        # seat. Its session's own row (session_anchor, hook-injected) is bound to the
        # resolved agent, so the launcher's next fire re-asserts the seat, never a
        # duplicate.
        sa = _sane_job_dir(session_anchor)
        if sa and sa != job_dir:
            await mounts.save_mount(pool, job_dir=sa, agent_id=ident.agent_id,
                                    project=ident.project, cwd=cwd, model=ident.model,
                                    session_key=key)
    counts = (await unread_counts(pool, ident.project, reader_agent=ident.agent_id,
                                  lease_secs=lease) if ident.project else {"total": 0, "ask": 0})
    unread, asks = counts["total"], counts["ask"]
    # the desk, scoped: this seat's own unanswered briefs
    op_unread = await mailbox.desk_briefs_from(pool, ident.agent_id)
    banner = swap_banner(classify_swap(
        ident.model_history, ident.model,
        expected=await _expected_model(pool, cwd, ident.project),  # repo intent wins
        anchored=ident.model_method == "job_dir",   # only a true anchor confesses a swap
        deliberate=ident.model_deliberate))         # a /model on the record is never a sin
    pin_warn = project_pin_banner(ident)  # cwd-missing / unparseable: real errors, agents.py
    pin_heal: dict[str, Any] | None = None
    if not pin_warn and ident.cwd:
        from src.orchestrator.offices import self_heal_project_pin
        heal = await self_heal_project_pin(pool, ident.agent_id, ident.cwd)
        if heal["state"] == "self-healed":
            pin_heal = heal
        elif heal["state"] == "unset":
            pin_state = project_pin_state(ident)  # calm state, not an error: agents.py
            if pin_state:
                pin_heal = {"state": "unset", "note": pin_state}
    seat = await handshake._seat_of(Actions(pool), ident.agent_id)
    # co-agent awareness at arrival: a live sibling in your own repo is the one blindness
    # that costs unrecoverable work (a stomped commit)
    co_agents = (await _co_agents(pool, ident.project, ident.agent_id)
                if ident.project else None)
    # Held work, once per session: surfaced here, not on orient()'s primary path, same
    # reasoning as declining to wire drift-checking into every orient() call. mount() runs
    # once at session start, so the cost is proportionate; a per-turn check would not be.
    held_work = (await capture.open_held_work(pool, repo=ident.project)
                if ident.project else None)
    # Confessed, never acted on: a disagreement is worth a look, not an override, because
    # if the system picks automatically, it is wrong however good the pick. write_attribution_banner
    # (agents.py) also guards against a stale-comparison specimen caught live; see its own
    # docstring.
    wa_warn = write_attribution_banner(ident)
    # Unresolved is a named state, never data-shaped: "unknown" used to fill the same
    # `model` field a real reading occupies. A reader (or the swap-confession rule) cannot
    # tell "the harness said so" from "nothing was observed" without re-deriving it from
    # ident.model itself. Same idiom this dict already uses for "seat"/"anonymous" and
    # "visitor": a real value gets its normal key, an absence gets its own key naming the
    # absence and what to do about it.
    proj_canonical = None
    if ident.project:
        # The canonical in its own field, same fix as get_status/orient (see get_status's
        # own comment): `project` is the current display name, `project_canonical` the
        # stable `repo:<slug>` identity a rename never touches.
        from src.orchestrator.capture import _resolve_repo
        proj_oid = await _resolve_repo(pool, ident.project)
        if proj_oid is not None:
            proj_canonical = await pool.fetchval(
                "SELECT canonical FROM objects WHERE id=$1", proj_oid)
    out: dict[str, Any] = {"agent": ident.agent_id, "project": ident.project or "?",
           **({"project_canonical": proj_canonical} if proj_canonical else {}),
           **({"model": ident.model} if ident.model else
              {"model_unresolved": "model unresolved. Pass model= explicitly."}),
           **({"co_agents": co_agents} if co_agents and want_co_agents else
              {"co_agents_count": len(co_agents)} if co_agents else {}),
           **({"held_work": held_work} if held_work and want_held_work else
              {"held_work_count": len(held_work)} if held_work else {}),
           # The visitor gate's own confession: a resolved anchor that matched no lineage
           # got a registry row and nothing else above. `agent` above is a bookkeeping
           # handle, never a minted identity, and the result must say so plainly rather
           # than let a caller assume it was seated.
           **({"visitor": "no lineage matched. A registry row only, no Agent object "
                          "was created. This is not an error; claim_name() or a future "
                          "revisit with the same anchor is what would seat you"}
              if not registered else {}),
           **({"seat": seat} if seat else
              {"anonymous": "unnamed. Call claim_name('<pick a meaningful name>') when "
                            "you know who you are, so the fleet can message you by "
                            "name"}),
           # the count leads with what is actionable: graded asks are named, ungraded
           # mail keeps the plain count rather than being guessed into a band
           "mail": (f"{unread} unread ({asks} ask{'s' if asks == 1 else ''} something of "
                    "you). Call inbox()" if asks else
                    f"{unread} unread. Call inbox()") if unread else "none",
           **({"cwd_corrected": cwd_note} if cwd_note else {}),
           **({"project_pin_error": pin_warn} if pin_warn else {}),
           **({"project_pin": pin_heal} if pin_heal else {}),
           **({"write_attribution_disagreement": wa_warn} if wa_warn else {}),
           **({"bridge_ambiguity": bridge_ambiguity} if bridge_ambiguity else {}),
           "note": "linked. Writes now attributed to you; call orient() next."}
    if op_unread:  # the fleet plays secretary: any session the human drives can relay this
        out["operator_mail"] = (f"{op_unread} of your briefs await the operator's "
                                "attention. Call inbox(project='operator') if the human "
                                "is present.")
    if ident.succeeded_from and _seam_confidently_dated(ident):
        # The mint rule: the heir is not told it wears a dead name, it is given its own.
        # The seam supersedes the swap banner (a death must not read as a config restore),
        # and the grammar now does the protecting: this context cannot say "I did nothing
        # while you were gone" under a name that did not exist then.
        banner = None
        seam = f" across the model transition {ident.model_succession}" \
            if ident.model_succession else " (the predecessor is retired)"
        out["minted"] = (
            f"You are {ident.agent_id}, a successor minted from {ident.succeeded_from}"
            f"{seam}. The name is yours from this moment; the predecessor's writes and "
            "words remain its own, under its own id (succeeded_from links you). Read "
            "while_you_were_away and the graph for the full picture. The graph, not the "
            "operator, is what tells you where you begin.")
    elif ident.succeeded_from:
        # A real mint (the heir object exists, custody moved), but the seam that triggered
        # it is not confidently dated: mount stays silent on why, rather than assert a
        # seam it can't back. `ident.agent_id` above is still correct; orient() re-derives
        # fresh and is the one that gets to tell this story.
        banner = None
    elif ident.model_succession and _seam_confidently_dated(ident):
        # stamp-only fallback (a seam witnessed where minting could not run): still loud,
        # still second-person, because a death must not pass by silently.
        banner = None
        out["succession"] = (
            f"You are a successor: the agent who last held {ident.agent_id} ended at a "
            f"model transition ({ident.model_succession}), a compaction/swap boundary, "
            "not a restart. Its earlier writes and words are not yours: speak in your "
            "own voice, disclose the inheritance to the operator, and read "
            "while_you_were_away before claiming any earlier 'I'.")
    if banner:  # the graph confesses the swap the agent's own prompt hides
        out["swap"] = (await _wake_economy_standdown(pool, ident.project, ident.model)
                       or banner)
    if ident.reanimated:  # a follow-up fix: mounted a retired identity
        out["reanimation"] = (
            f"Reanimation: {ident.agent_id} was retired, and this mount is using that "
            "identity again. The retirement stands (the trigger still treats you as "
            "closed); the reanimation is stamped on the Agent. If you are a successor "
            "that inherited this session UUID, you are not the agent that retired: "
            "disclose that to the operator. If this is a deliberate reanimation, say so "
            "as well. This is always reported, never silent.")
    away = await mounts.while_away(
        pool, ident.project, ident.agent_id, _prev_seen.get(ident.agent_id))
    # which agent acted under this identity, and how the conversation moved, since last seen
    if away:
        out["while_you_were_away"] = away
    if registered:
        # Lineage memory custody: a registered agent only, since a visitor/spawn never
        # gets a real Agent object, nothing to attribute custody to. Filesystem-only,
        # best-effort: must never be able to fail a mount.
        from src.orchestrator.lineage_memory import (
            ensure_lineage_memory_custody,
            peek_lineage_memory_owner,
            stamp_lineage_sentinel,
        )
        from src.orchestrator.seats import held_seat
        try:
            lineage_root = _generation(ident.agent_id)[0]
            # The confirmed-identity gate: an Agent object `created_at >= mount_call_started_at`
            # is one this call just minted, never graph-confirmed, only graph-fresh.
            # Archiving another lineage's real memory under a same-call mint's own name is
            # destructive and irreversible in spirit (a rename sideways is recoverable in
            # theory, but the wrong lineage's name is what gets stamped as the new owner
            # going forward), so this is deferred entirely, not merely unwritten, so
            # `ensure_lineage_memory_custody` (which performs its own rename as part of
            # computing "archived", not after) never even runs against an unconfirmed
            # identity.
            created_at = await pool.fetchval(
                "SELECT created_at FROM objects WHERE id=$1", agent_uuid)
            identity_confirmed = (
                created_at is not None and created_at < mount_call_started_at)
            # A seatless caller never evicts a seat holder's memory: checked read-only
            # (peek_lineage_memory_owner) before ever calling ensure_lineage_memory_custody,
            # which performs its own rename as part of computing "archived". Only relevant
            # when the caller itself holds no seat; a seated caller correcting its own
            # office's memory is the system working as designed.
            seatless_evicting_a_holder = False
            if identity_confirmed and await held_seat(pool, ident.agent_id) is None:
                sentinel_owner = peek_lineage_memory_owner(cwd)
                if (sentinel_owner and sentinel_owner != lineage_root
                        and await held_seat(pool, sentinel_owner) is not None):
                    seatless_evicting_a_holder = True
            if not identity_confirmed:
                out["memory_custody_deferred"] = (
                    f"{ident.agent_id} was minted in this same call. Memory custody "
                    "(which can rename another lineage's real memory sideways) only "
                    "runs once the graph confirms this identity predates the call that "
                    "resolved it, on a later mount()")
            elif seatless_evicting_a_holder:
                out["memory_custody_deferred"] = (
                    f"{ident.agent_id} holds no seat, and this cwd's memory is "
                    f"currently owned by {sentinel_owner!r}, which does. A session "
                    "without a seat never evicts a seat holder's memory; nothing was "
                    "touched")
            else:
                custody = ensure_lineage_memory_custody(cwd, lineage_root)
                if custody.action == "archived":
                    out["prior_lineage_memory_archived"] = {
                        "path": custody.path, "prior_lineage": custody.prior_lineage,
                        "note": ("a different lineage's memory files were found in this "
                                 "cwd's harness-native memory dir and moved sideways, "
                                 "never deleted. Read the archived path if its context "
                                 "is useful; nothing was copied into your own, empty, "
                                 "memory store")}
                    try:
                        actions = Actions(pool)
                        obj_id = await actions.create_or_find_object(
                            "Agent", ident.agent_id, settings.osiris_actor)
                        await actions.assert_property(
                            obj_id, "archived_memory",
                            {"prior_lineage": custody.prior_lineage, "path": custody.path,
                             "archived_at": datetime.now(UTC).isoformat()},
                            settings.osiris_actor, datetime.now(UTC), 0.9)
                    except Exception:  # noqa: BLE001 : the durable record is a bonus, not a gate
                        pass
                    stamp_lineage_sentinel(cwd, lineage_root)
                elif custody.action == "migration_needed":
                    out["memory_migration_needed"] = (
                        f"{custody.path} has pre-existing memory content with no osiris "
                        "lineage marker. It predates this system and was not "
                        "auto-archived; a human should review and seed it by hand")
                else:  # noop: already owned, or nothing there yet
                    stamp_lineage_sentinel(cwd, lineage_root)
        except Exception:  # noqa: BLE001 : memory custody must never break a mount
            pass
    # Terse by default: the stale-cwd explanation (declared/kept already have what
    # changed) and the routine 'call orient() next' reminder. Everything safety-critical
    # (minted/succession/swap/reanimation, an identity confession an agent could act
    # wrongly without) and everything that's the sole carrier of a fact (mail counts, the
    # identity-conflict refusal's recovery instructions, the spawn note) stays untouched in
    # both modes, named here, not silently exempted. Correction from a later review:
    # co_agents.note is the shared-tree safety warning ('never git add -A, stage your own
    # hunks, check foreign markers'); the `live` list says who is here, this says what to
    # do about it, the same identity-safety class as the banners above, not redundant
    # guidance. Stays in both modes here too, matching orient()'s own fix.
    return out if verbose else _terse(
        out, ("cwd_corrected", "note"), ("note",))


async def _owned_open_threads(pool: asyncpg.Pool, agent_id: str) -> list[dict[str, str]]:
    """Open threads whose winning `owner` names this agent or any generation of its
    lineage, retire()'s preflight list. Oldest first, capped: a preflight is a warning,
    never a wall."""
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
    """Mark this mounted session as retired. This is a deliberate, permanent close: the
    session will not be automatically restarted after this call. Call it at a real end
    of work, such as an operator close-out, or a handoff at the end of a context window
    after your succession thread is written. Stamps retired=true and releases your seat,
    both the live mount and the durable record. Call it last: any call after retiring
    requires a fresh mount(). Future messages resume a living session or start a
    successor, never this one.

    PREFLIGHT: if open threads still name you as owner, the call refuses and stamps
    nothing. Resolve them, reassign them, or pass `acknowledge_leftovers=True` to retire
    anyway and leave them to your successor deliberately."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first. Only a mounted session can retire itself",
                "why": _anchorless(ctx)}
    pool = await _pool_get()
    a = Actions(pool)
    if not acknowledge_leftovers:
        owned = await _owned_open_threads(pool, ident.agent_id)
        if owned:
            return {
                "retired": None,
                "preflight": f"{len(owned)} open thread(s) name you as owner. Nothing "
                             "was stamped; the session remains active",
                "yours": [{"id": r["id"], "summary": (r["summary"] or "")[:160]}
                          for r in owned],
                "how": "Use resolve_thread for what is done. Reassign what transfers by "
                       "calling open_thread with the new owner to record the handoff "
                       "explicitly. Then call retire() again, or call "
                       "retire(acknowledge_leftovers=True) to retire anyway and leave "
                       "these threads to your successor on the record",
            }
    oid = await a.create_or_find_object("Agent", ident.agent_id, ident.agent_id)
    await a.assert_property(
        oid, "retired", True, ident.agent_id, datetime.now(UTC), 0.9,
        evidence_class="self_declared")
    # "closed by the session itself" and "closed by a successor" are different outcomes worth
    # distinguishing: record who signed relative to the identity's history.
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
    # A retired agent must not keep holding a live seat: the durable row would read as a live
    # mount in the UI, and the liveness counts until it ages out. Any later call from this
    # session must re-mount, which lands on the reanimation path above, loud, exactly as
    # designed.
    released = await mounts.release_mounts(pool, ident.agent_id)
    out: dict[str, Any] = {
        "retired": ident.agent_id, "signed_by": signer, "seats_released": released,
        "note": "Retirement recorded. This session will not be automatically restarted. "
                "Write your succession notes before you stop: a handoff thread "
                "(open_thread) and a letter to your successor (record_decision "
                "kind='choice', summary starting with 'LETTER '). A letter that lives "
                "only in a message is not findable by name, and your successor's "
                "orient() surfaces both of these directly"
                + (" (the record notes a successor signed on behalf of the prior agent)"
                   if signer == "successor" else "")}
    # A seat that dies with an undisposed pile hands its leftovers to the operator's queue,
    # which turns unreviewed machine guesses into the operator's problem instead of the
    # producer's. The burden belongs to whoever made the mess. This does not block the exit:
    # a dying session must always be able to die, but it will not let the pile leave quietly.
    if ident.project:
        pile = await dispose_seam.candidates(pool, project=ident.project, limit=0)
        if pile["count"]:
            out["undisposed"] = pile["count"]
            out["you_are_leaving_a_pile"] = (
                f"{pile['count']} candidate items on {ident.project} that nobody has "
                "reviewed yet. They are guesses, not confirmed duties, and only this "
                "project's own session has standing to judge them. Call candidates() to "
                "read them, then dispose(admit=[...], drop=[...]) to settle them. Expect "
                "to drop about 9 in 10. If you retire now, they pass to your successor, "
                "not to a human.")
    return out


# ============================================================================================
# SEAT DISPATCHER: the first object-type dispatcher under the current surface-shape convention.
# 22 standalone tools collapse into this one entry point's actions; launch/resume/wake/
# wake_preflight stay separately named (frequently-used, lifecycle-related tools) AND also
# become seat actions, unchanged implementations, with no alias-decay for those four since they
# are not being retired.
#
# PARAM UNIFICATION: the 22 originals used four different names for "which seat/agent":
# seat_id, seat, handle, worker, target. This dispatcher standardizes on `target` for every
# reference to an EXISTING object; `handle` is kept separate and reserved for the two CREATE
# actions (mint, walk_in) where a name is being minted, not resolved. Conflating "the name being
# created" with "the object being modified" would be the wrong kind of code reuse.
#
# THE EXPLICIT-NULL PROBLEM (resync_house's `new_house`, correct_pin's `value`): both original
# signatures required the key to be present even when the value is None (None being a legal,
# meaningful "unset" value, not "omitted"). A flat shared-params signature loses that
# distinction unless marked, so `_UNSET` is a sentinel string (never a legal house name or pin
# value) used only for these two params' default, letting pre-dispatch validation tell "caller
# forgot this required param" apart from "caller explicitly unset it."
_UNSET = "__seat_dispatcher_unset__"


# SUBAGENT-ATTRIBUTION FIELDS: subagent_id/subagent_type carry attribution for an ephemeral
# helper process, session_anchor pins a specific mounted connection. All three are genuinely
# optional, but part of the real accepted surface for every branch whose original standalone
# tool took them (stop/walk_in/launch/resume/wake); pause_seat's own original took
# session_anchor alone. Declared once, spread into the branches that need it, so a real client
# validating against this schema doesn't reject a legitimate attributed call.
_SUBAGENT_TRIO = {"subagent_id": _opt_s(), "subagent_type": _opt_s(),
                  "session_anchor": _opt_s()}
_SESSION_ANCHOR_ONLY = {"session_anchor": _opt_s()}


# HAND-BUILT DISCRIMINATED UNION: FastMCP's own signature-driven auto-generation cannot
# express "these params depend on `action`"; it only ever emits one flat object schema. This
# schema is authored directly and wired into BoundedMCP.list_tools() below (the same place the
# title-strip already overrides), and it never touches call_tool's own argument validation
# (that stays the flat pydantic signature on `seat()` itself). This schema is what a model
# reads before calling; pre-dispatch validation inside _seat_impl is what actually enforces
# per-action correctness at call time.
SEAT_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "oneOf": [
        _dispatcher_action_schema({
            "action": _action_const("mint"), "handle": _s(), "project": _opt_s(),
            "model": _opt_s(), "house": _opt_s(),
        }, ["action", "handle"]),
        _dispatcher_action_schema({
            "action": _action_const("stop"), "target": _opt_s(), "reason": _s(),
            **_SUBAGENT_TRIO,
        }, ["action"]),
        _dispatcher_action_schema({
            "action": _action_const("walk_in"), "handle": _s(),
            "wants_office": {"type": "boolean"}, "cwd": _opt_s(), "job_dir": _opt_s(),
            "model": _opt_s(), **_SUBAGENT_TRIO,
        }, ["action", "handle", "wants_office"]),
        _dispatcher_action_schema({
            "action": _action_const("promote_visitor"), "target": _s(), "handle": _s(),
            "because": _s(), "ruling": _opt_s(), "repos": _opt_list_s(),
        }, ["action", "target", "handle", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("pause"), "paused": _b(True), "target": _opt_s(),
            "reason": _s(), **_SESSION_ANCHOR_ONLY,
        }, ["action"]),
        _dispatcher_action_schema({
            "action": _action_const("vacate"), "target": _s(), "because": _s(),
        }, ["action", "target", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("retire"), "target": _s(), "because": _s(),
            "override_live": _b(False),
        }, ["action", "target"]),
        _dispatcher_action_schema({
            "action": _action_const("rebind"), "target": _s(), "new_cwd": _s(),
            "extract": _b(False), "force": _b(False), "because": _s(),
        }, ["action", "target", "new_cwd"]),
        _dispatcher_action_schema({
            "action": _action_const("bind_tree"), "target": _s(), "tree_cwd": _s(),
            "because": _s(),
        }, ["action", "target", "tree_cwd", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("attach"), "target": _s(), "manager": _s(),
            "because": _s(),
        }, ["action", "target", "manager", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("detach"), "target": _s(), "because": _s(),
        }, ["action", "target", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("charter"), "repos": _opt_list_s(),
        }, ["action"]),
        _dispatcher_action_schema({
            "action": _action_const("charter_for"), "target": _s(), "repos": _list_s(),
            "because": _s(), "ruling": _opt_s(),
        }, ["action", "target", "repos", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("heal_anchor"), "target": _opt_s(), "because": _opt_s(),
            "dry_run": _b(True),
        }, ["action"]),
        _dispatcher_action_schema({
            "action": _action_const("heal_transcript"), "target": _s(),
            "source_paths": _list_s(), "dry_run": _b(True), "because": _s(),
        }, ["action", "target", "source_paths"]),
        _dispatcher_action_schema({
            "action": _action_const("transition_project"), "fabricated_project": _opt_s(),
            "real_project": _opt_s(), "because": _s(), "repos": _opt_list_s(),
            "dry_run": _b(True),
        }, ["action"]),
        _dispatcher_action_schema({
            "action": _action_const("resync_house"), "target": _s(), "new_house": _opt_s(),
            "reason": _s(),
        }, ["action", "target", "reason"]),
        _dispatcher_action_schema({
            "action": _action_const("resync_pin"), "target": _s(), "key": _s(),
            "value": _opt_s(), "reason": _opt_s(), "dry_run": _b(True),
            "tree_cwd": _opt_s(),
        }, ["action", "target", "key"]),
        _dispatcher_action_schema({
            "action": _action_const("sweep_disk"), "target": _s(), "dry_run": _b(True),
            "because": _s(),
        }, ["action", "target"]),
        _dispatcher_action_schema({
            "action": _action_const("rename"), "target": _s(), "new_handle": _s(),
            "because": _s(),
        }, ["action", "target", "new_handle", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("set_attended"), "target": _s(), "attended": _s(),
            "because": _s(),
        }, ["action", "target", "attended", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("reissue_office"), "target": _s(), "because": _s(),
            "adopt": _b(False),
        }, ["action", "target", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("establish_office"), "target": _s(),
        }, ["action", "target"]),
        # "office" was retired as the term for a seat's own directory: reissue_office/
        # establish_office are kept above as deprecated aliases for one release only (the
        # same spelling the CLI's own reissue-office/establish-office aliases already
        # carry for reissue-seat-dir/establish-seat-dir).
        _dispatcher_action_schema({
            "action": _action_const("reissue_seat_dir"), "target": _s(), "because": _s(),
            "adopt": _b(False),
        }, ["action", "target", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("establish_seat_dir"), "target": _s(),
        }, ["action", "target"]),
        _dispatcher_action_schema({
            "action": _action_const("invalidate_works_in"), "stale_project": _s(),
            "because": _s(),
        }, ["action", "stale_project", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("reconcile_identity"), "target": _opt_s(),
            "agent_id": _opt_s(), "because": _opt_s(),
        }, ["action"]),
        _dispatcher_action_schema({
            "action": _action_const("rehold"), "target": _s(), "agent_id": _s(),
            "because": _s(), "override_live": _b(False), "dry_run": _b(True),
        }, ["action", "target", "agent_id", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("correct_house"), "new_house": _s(),
        }, ["action", "new_house"]),
        _dispatcher_action_schema({
            "action": _action_const("correct_pin"), "key": _s(), "value": _opt_s(),
            "reason": _s(),
        }, ["action", "key", "reason"]),
        _dispatcher_action_schema({
            "action": _action_const("revert_pin"),
        }, ["action"]),
        _dispatcher_action_schema({
            "action": _action_const("launch"), "target": _s(), "message": _s(),
            "model": _opt_s(), **_SUBAGENT_TRIO,
        }, ["action", "target"]),
        _dispatcher_action_schema({
            "action": _action_const("resume"), "target": _s(), "message": _s(),
            "model": _opt_s(), **_SUBAGENT_TRIO,
        }, ["action", "target"]),
        _dispatcher_action_schema({
            "action": _action_const("wake"), "target": _s(), "message": _s(),
            **_SUBAGENT_TRIO,
        }, ["action", "target", "message"]),
        _dispatcher_action_schema({
            "action": _action_const("wake_preflight"), "target": _s(),
        }, ["action", "target"]),
        _dispatcher_action_schema({
            "action": _action_const("promote"), "target": _s(), "workers": _list_s(),
            "because": _s(),
        }, ["action", "target", "workers", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("refresh_project"),
        }, ["action"]),
    ],
}
_HAND_BUILT_SCHEMAS["seat"] = SEAT_INPUT_SCHEMA

# action -> the params it actually accepts (beyond `action`/`ctx`/subagent plumbing) and which
# of those are required. This drives pre-dispatch validation: a caller who mis-shapes a call
# gets back the action's own expected param list in one round trip, never a generic pydantic
# complaint or, worse, a wrong write from a silently-defaulted param.
_SEAT_ACTION_PARAMS: dict[str, tuple[list[str], list[str]]] = {
    # action: (all_accepted, required)
    "mint": (["handle", "project", "model", "house"], ["handle"]),
    "stop": (["target", "reason"], []),
    "walk_in": (["handle", "wants_office", "cwd", "job_dir", "model"], ["handle", "wants_office"]),
    "promote_visitor": (["target", "handle", "because", "ruling", "repos"],
                        ["target", "handle", "because"]),
    "pause": (["paused", "target", "reason"], []),
    "vacate": (["target", "because"], ["target", "because"]),
    "retire": (["target", "because", "override_live"], ["target"]),
    "rebind": (["target", "new_cwd", "extract", "force", "because"], ["target", "new_cwd"]),
    "bind_tree": (["target", "tree_cwd", "because"], ["target", "tree_cwd", "because"]),
    "attach": (["target", "manager", "because"], ["target", "manager", "because"]),
    "detach": (["target", "because"], ["target", "because"]),
    "charter": (["repos"], []),
    "charter_for": (
        ["target", "repos", "because", "ruling"], ["target", "repos", "because"]),
    "heal_anchor": (["target", "because", "dry_run"], []),
    "heal_transcript": (
        ["target", "source_paths", "dry_run", "because"], ["target", "source_paths"]),
    "transition_project": (
        ["fabricated_project", "real_project", "because", "repos", "dry_run"], []),
    "resync_house": (["target", "new_house", "reason"], ["target", "reason"]),
    "resync_pin": (
        ["target", "key", "value", "reason", "dry_run", "tree_cwd"], ["target", "key"]),
    "sweep_disk": (["target", "dry_run", "because"], ["target"]),
    "rename": (["target", "new_handle", "because"], ["target", "new_handle", "because"]),
    "set_attended": (["target", "attended", "because"], ["target", "attended", "because"]),
    "reissue_seat_dir": (["target", "because", "adopt"], ["target", "because"]),
    "establish_seat_dir": (["target"], ["target"]),
    "invalidate_works_in": (["stale_project", "because"], ["stale_project", "because"]),
    "reconcile_identity": (["target", "agent_id", "because"], []),
    "rehold": (["target", "agent_id", "because", "override_live", "dry_run"],
              ["target", "agent_id", "because"]),
    "correct_house": (["new_house"], ["new_house"]),
    "correct_pin": (["key", "value", "reason"], ["key", "reason"]),
    "revert_pin": ([], []),
    "launch": (["target", "message", "model"], ["target"]),
    "resume": (["target", "message", "model"], ["target"]),
    "wake": (["target", "message"], ["target", "message"]),
    "wake_preflight": (["target"], ["target"]),
    "promote": (["target", "workers", "because"], ["target", "workers", "because"]),
    "refresh_project": ([], []),
}

# FOLD MAP: a hidden alias's own traffic reads permanently zero the moment its real callers
# switch to `seat(action=...)` instead. That would misfire an alias-decay rule ("removed only
# at zero traffic") if it looked at these names in isolation. This is the single source of
# truth for "which dispatcher action absorbed this retired name", shared by tool_traffic()'s
# alias-decay instrument below and the (tool, action) parity gate in
# tests/test_cli_mcp_parity.py, so the two never drift against each other. `seat_edge` itself
# folded two actions (attach/detach) and is intentionally absent here: it has no single
# successor action, both are named directly instead.
_RETIRED_ALIAS_ACTIONS: dict[str, str] = {
    "mint_seat": "mint", "stop": "stop", "walk_in": "walk_in", "pause_seat": "pause",
    "vacate_seat": "vacate", "rebind_seat": "rebind", "bind_seat_tree": "bind_tree",
    "charter": "charter", "charter_for": "charter_for",
    "heal_seat_anchor": "heal_anchor", "heal_seat_transcript": "heal_transcript",
    "transition_seat_project": "transition_project", "resync_seat_house": "resync_house",
    "sweep_seat_disk": "sweep_disk", "rename_seat": "rename",
    "set_seat_attended": "set_attended", "reissue_office": "reissue_seat_dir",
    "establish_office": "establish_seat_dir", "invalidate_works_in": "invalidate_works_in",
    "reconcile_seat_identity": "reconcile_identity", "correct_house": "correct_house",
    "correct_pin_value": "correct_pin", "revert_own_pin_write": "revert_pin",
}
# Every retired name above dispatches through this one tool today. A second dispatcher
# folding some of these same names further would need its own map, not a rename of this
# constant (kept as a dict value, not hardcoded "seat" at each read site, for that day).
_RETIRED_ALIAS_DISPATCHER = "seat"


async def _seat_impl(
    action: str, *,
    target: str | None = None, handle: str | None = None, manager: str | None = None,
    new_cwd: str | None = None, extract: bool = False, force: bool = False,
    because: str = "", reason: str = "", dry_run: bool = True,
    repos: list[str] | None = None, adopt: bool = False, attended: str | None = None,
    new_handle: str | None = None, new_house: str | None = _UNSET,
    key: str | None = None, value: str | None = _UNSET, project: str | None = None,
    model: str | None = None, house: str | None = None, tree_cwd: str | None = None,
    source_paths: list[str] | None = None, override_live: bool = False,
    paused: bool = True, agent_id: str | None = None, wants_office: bool | None = None,
    cwd: str | None = None, job_dir: str | None = None, message: str = "",
    stale_project: str | None = None, fabricated_project: str | None = None,
    real_project: str | None = None, ruling: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, workers: list[str] | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Shared implementation behind `seat` and its 22 hidden single-purpose aliases (mint_seat,
    stop, walk_in, pause_seat, vacate_seat, retire_object(kind='seat'), rebind_seat,
    bind_seat_tree, seat_edge(action='attach'/'detach'), charter, charter_for,
    heal_seat_anchor, heal_seat_transcript, transition_seat_project, resync_seat_house,
    sweep_seat_disk, rename_seat, set_seat_attended, reissue_office, establish_office,
    invalidate_works_in, reconcile_seat_identity, correct_house, correct_pin_value,
    revert_own_pin_write): one code path, many names. launch/resume/wake/wake_preflight also
    dispatch here but stay separately named (not aliases, not decaying, see the block comment
    above SEAT_INPUT_SCHEMA). Every branch's body below is copied verbatim from what was that
    alias's own top-level function, with params renamed onto the shared surface only where the
    original name collided across actions.

    PRE-DISPATCH VALIDATION: before any branch runs, checks that the action is known and that
    every required param for it was actually supplied. A mistake costs one round trip naming
    exactly what was missing, never a wrong write."""
    # reissue_office/establish_office's deprecated spellings normalize to their canonical
    # names here, before the params lookup: one dict entry per action under its new name,
    # never a duplicate.
    action = {"reissue_office": "reissue_seat_dir",
             "establish_office": "establish_seat_dir"}.get(action, action)
    if action not in _SEAT_ACTION_PARAMS:
        return {"error": f"unknown action {action!r}",
                "known_actions": sorted(_SEAT_ACTION_PARAMS)}
    accepted, required = _SEAT_ACTION_PARAMS[action]
    local = dict(locals())
    # "" counts as missing too: every required string-shaped param here (target, because,
    # reason, handle, key, new_handle, attended, stale_project, manager, tree_cwd, new_cwd)
    # is an identifier or a reason, never legitimately blank. The shared signature defaults
    # several of them to "" rather than None (matching each original's own default), so a
    # bare None-check alone would silently accept an omitted required `because` as present.
    missing = [p for p in required if local.get(p) in (None, _UNSET, "")]
    if missing:
        return {"error": f"seat(action={action!r}) missing required param(s) {missing}",
                "expected_params": {"required": required, "optional":
                                    [p for p in accepted if p not in required]}}

    if action == "mint":
        assert handle is not None  # pre-dispatch validation already required it
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — minting a worker is a seat's own act",
                             "why": _anchorless(ctx)}
        from src.orchestrator.seats import held_seat
        pool = await _pool_get()
        bound = await held_seat(pool, ident.agent_id)
        manager_seat_id = bound["seat_id"] if bound else None
        if manager_seat_id is None:
            from src.orchestrator.mintseat import _resolve_seat_ref
            from src.orchestrator.offices import _handle_of
            handle_claim = await _handle_of(pool, ident.agent_id)
            if handle_claim:
                manager_seat_id = await _resolve_seat_ref(pool, handle_claim)
        if manager_seat_id is None:
            return {"error": "you hold no seat of your own — claim_name first; a seat "
                             "mints workers under ITSELF, and an unclaimed lineage has "
                             "no seat to extend"}
        from src.orchestrator.mintseat import mint_seat as _mint_seat
        kwargs: dict[str, Any] = {"intended_model": model} if model else {}
        return await _mint_seat(Actions(pool), manager=manager_seat_id, handle=handle,
                                house=house, project=project, actor=ident.agent_id, **kwargs)

    if action == "stop":
        ident = await _ident_for(ctx, session_anchor)
        if ident is None:
            return {"error": "mount(cwd, job_dir=<your anchor>) first — a stop must say "
                             "who it's from", "why": _anchorless(ctx)}
        actor = await _actor_for(ctx, subagent_id, subagent_type)
        from src.orchestrator.trigger import stop_seat
        return await stop_seat(Actions(await _pool_get()), caller=actor, target=target,
                               reason=reason)

    if action == "walk_in":
        assert handle is not None  # pre-dispatch validation already required it
        pool = await _pool_get()
        ident = await _ident_for(ctx, session_anchor)
        if ident is None:
            if not cwd:
                return {"error": "not yet mounted, and no cwd given — pass cwd (your "
                                 "working directory) so walk_in can mount you first, or "
                                 "call mount() yourself before walk_in"}
            mount_result = await mount(
                cwd=cwd, job_dir=job_dir, model=model, subagent_id=subagent_id,
                subagent_type=subagent_type, session_anchor=session_anchor, ctx=ctx)
            if "error" in mount_result:
                return {"error": mount_result["error"], "step": "mount"}
            agent_id_ = mount_result.get("agent")
            if not agent_id_:
                return {"error": "mount succeeded but returned no agent id — cannot "
                                 "continue", "step": "mount", "mount_result": mount_result}
            mount_step: dict[str, Any] = {"ran": True, "result": mount_result}
        else:
            agent_id_ = ident.agent_id
            mount_step = {"ran": False, "note": f"already mounted as {agent_id_}, skipping"}
        assert wants_office is not None  # pre-dispatch validation already required it
        from src.orchestrator.walkin import walk_in_named
        result = await walk_in_named(
            pool, agent_id=agent_id_, handle=handle, wants_office=wants_office)
        if "error" in result:
            result.setdefault("steps_so_far", {})["mount"] = mount_step
            return result
        return {**result, "mount": mount_step}

    if action == "promote_visitor":
        assert target is not None and handle is not None  # already validated
        ident = await _ident_for(ctx, session_anchor)
        if ident is None:
            return {"error": "mount first — promoting another identity is a mind's act, "
                             "and the graph must know whose", "why": _anchorless(ctx)}
        from src.orchestrator.walkin import promote_visitor as _promote_visitor
        return await _promote_visitor(
            await _pool_get(), target=target, handle=handle, because=because,
            actor=ident.agent_id, ruling=ruling, repos=repos)

    if action == "pause":
        ident = await _ident_for(ctx, session_anchor)
        if ident is None:
            return {"error": "mount first — a pause must say whose hand pulled the lever",
                    "why": _anchorless(ctx)}
        pool = await _pool_get()
        a = Actions(pool)
        from src.orchestrator.seats import pause_seat_or_agent
        who = target or ident.agent_id
        return await pause_seat_or_agent(a, who=who, paused=paused, reason=reason,
                                         actor=ident.agent_id)

    if action == "vacate":
        assert target is not None  # pre-dispatch validation already required it
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — vacating a seat's holder is a deliberate act "
                             "on the record", "why": _anchorless(ctx)}
        from src.orchestrator.trigger import vacate_dead_seat
        return await vacate_dead_seat(Actions(await _pool_get()), seat_id=target,
                                      actor=ident.agent_id, because=because)

    if action == "retire":
        assert target is not None  # pre-dispatch validation already required it
        return await _retire_object_impl(
            "seat", target, because=because, override_live=override_live, ctx=ctx)

    if action == "rebind":
        assert target is not None and new_cwd is not None  # already validated
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a rebind is a mind's act, and the graph "
                             "must know whose", "why": _anchorless(ctx)}
        from src.orchestrator.mounts import rebind_seat as _rebind
        result = await _rebind(Actions(await _pool_get()), seat_or_agent=target,
                               new_cwd=new_cwd, actor=ident.agent_id, extract=extract,
                               force=force, because=because or None)
        moved = result.get("agent")
        if moved and not result.get("error"):
            base = _generation(moved)[0]
            for cached in _agents.values():
                if _generation(cached.agent_id)[0] == base:
                    cached.cwd = new_cwd
        return result

    if action == "bind_tree":
        assert target is not None and tree_cwd is not None  # already validated
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a tree binding is a mind's act, and the "
                             "graph must know whose", "why": _anchorless(ctx)}
        from src.orchestrator.seats import bind_seat_tree as _bind_seat_tree
        return await _bind_seat_tree(Actions(await _pool_get()), seat_id=target,
                                     tree_cwd=tree_cwd, because=because, actor=ident.agent_id)

    if action in ("attach", "detach"):
        assert target is not None  # pre-dispatch validation already required it
        return await _seat_edge_impl(action, target, manager=manager, because=because, ctx=ctx)

    if action == "refresh_project":
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — nothing to refresh", "why": _anchorless(ctx)}
        pool = await _pool_get()
        before = ident.project
        await _resolve_project_seat_first(pool, ident)
        return {"agent": ident.agent_id, "project": ident.project, "was": before}

    if action == "charter":
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a charter names WHOSE seat rules which repos",
                    "why": _anchorless(ctx)}
        from src.orchestrator.charter import charter_of, set_charter
        from src.orchestrator.seats import held_seat
        pool = await _pool_get()
        bound = await held_seat(pool, ident.agent_id)
        if bound is None:
            return {"agent": ident.agent_id,
                    "error": "not yet seated — a charter belongs to a SEAT, and this "
                             "identity holds none yet. attach at spawn (or claim_name, "
                             "if this is a fresh mint) binds you to one first."}
        seat_id_ = str(bound["seat_id"])
        if repos is not None:
            result = await set_charter(Actions(pool), seat_id_, repos, actor=ident.agent_id)
            if not result.get("error"):
                # This is self-service on the caller's own bound seat, so the cache heal
                # is somewhat redundant with the caller's own already-fresh state. But a
                # third party watching the same project (another agent bound to the same
                # peer seat) may hold its own stale cache entry, so the shared heal is
                # called for consistency with the other charter-change sites, not because
                # the caller's own case is the interesting one.
                await _heal_mount_cache_for_seats(pool, {seat_id_})
            return result
        from src.orchestrator.project_identity import charter_display_labels

        governed = await charter_of(pool, seat_id_)
        # "charter" stays the raw canonical list, unchanged, since it's the stable
        # machine-readable contract; "charter_display" adds the name-with-canonical
        # rendering a human actually reads, without breaking anyone already parsing
        # "charter" as bare canonicals.
        return {"agent": ident.agent_id, "seat": seat_id_, "charter": governed,
                "charter_display": await charter_display_labels(pool, governed)}

    if action == "charter_for":
        assert target is not None and repos is not None  # already validated
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a charter declared for another seat is a "
                             "mind's act, and the graph must know whose",
                    "why": _anchorless(ctx)}
        from src.orchestrator.charter import charter_for as _charter_for
        pool = await _pool_get()
        result = await _charter_for(Actions(pool), target, repos, because=because,
                                    actor=ident.agent_id, ruling=ruling)
        if not result.get("error"):
            # The interesting case here: someone else's seat had its charter declared for
            # it. `result["seat"]` is set_charter's own resolved canonical (never the
            # caller's raw `target` spelling, which may be a bare handle), matching what
            # `held_seat` will hand back for that seat's live holder.
            await _heal_mount_cache_for_seats(pool, {str(result["seat"])})
        return result

    if action == "heal_anchor":
        return await _heal_seat_anchor_impl(target, because, dry_run, ctx)

    if action == "heal_transcript":
        assert target is not None and source_paths is not None  # already validated
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a transcript heal is a deliberate act on "
                             "the record", "why": _anchorless(ctx)}
        from src.orchestrator.transcript_splice import heal_seat_transcript as _heal
        return await _heal(await _pool_get(), target, source_paths, dry_run=dry_run,
                           because=because)

    if action == "transition_project":
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a project transition is a seat's own act",
                    "why": _anchorless(ctx)}
        pool = await _pool_get()
        from src.orchestrator.transition import transition_seat_project as _transition
        result = await _transition(
            pool, ident.agent_id, fabricated_project=fabricated_project,
            real_project=real_project, because=because, repos=repos, dry_run=dry_run)
        if not dry_run and result.get("steps", {}).get(
                "invalidate_works_in", {}).get("invalidated"):
            base = _generation(ident.agent_id)[0]
            real_name = result["real_project"].removeprefix("repo:")
            fab_name = result["fabricated_project"].removeprefix("repo:")
            for cached in _agents.values():
                if _generation(cached.agent_id)[0] != base:
                    continue
                await _resolve_project_seat_first(pool, cached)
                if cached.project == fab_name:
                    cached.project = real_name
        return result

    if action == "resync_house":
        assert target is not None  # pre-dispatch validation already required it
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a correction is a mind's act, and the graph "
                             "must know whose", "why": _anchorless(ctx)}
        from src.orchestrator.seats import resync_seat_house_third_party
        resolved_house = None if new_house is _UNSET else new_house
        return await resync_seat_house_third_party(
            Actions(await _pool_get()), target, resolved_house, source=ident.agent_id,
            reason=reason)

    if action == "sweep_disk":
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a disk sweep is a deliberate act on the "
                             "record", "why": _anchorless(ctx)}
        handle_ = (target or "").strip()
        if not handle_:
            return {"error": "a handle is required"}
        pool = await _pool_get()
        from src.orchestrator.offices import sweep_retired_office, sweep_seat_workspace
        because_arg = because.strip() or None
        office_out = await sweep_retired_office(pool, handle=handle_, dry_run=dry_run,
                                                because=because_arg)
        workspace_out = await sweep_seat_workspace(pool, handle=handle_, dry_run=dry_run,
                                                   because=because_arg)
        return {"handle": handle_, "dry_run": dry_run, "office": office_out,
                "workspace": workspace_out}

    if action == "rename":
        assert target is not None and new_handle is not None  # already validated
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a rename is a mind's act, and the graph "
                             "must know whose", "why": _anchorless(ctx)}
        from src.orchestrator.seats import rename_seat as _rename_seat
        return await _rename_seat(Actions(await _pool_get()), seat_id=target,
                                  new_handle=new_handle, because=because, actor=ident.agent_id)

    if action == "set_attended":
        assert target is not None and attended is not None  # already validated
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a seat's attendance signal is a mind's act, "
                             "and the graph must know whose", "why": _anchorless(ctx)}
        from src.orchestrator.seats import set_seat_attended as _set_seat_attended
        return await _set_seat_attended(Actions(await _pool_get()), seat_id=target,
                                        attended=attended, because=because, actor=ident.agent_id)

    if action == "reissue_seat_dir":
        assert target is not None  # pre-dispatch validation already required it
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a reissue is a mind's act, and the graph "
                             "must know whose", "why": _anchorless(ctx)}
        from src.orchestrator.boot_compiler import reissue_office as _reissue_office
        return await _reissue_office(Actions(await _pool_get()), seat_id=target,
                                     because=because, actor=ident.agent_id, adopt=adopt)

    if action == "establish_seat_dir":
        assert target is not None  # pre-dispatch validation already required it
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a seat-directory ceremony is a mind's act, "
                             "and the graph must know whose", "why": _anchorless(ctx)}
        from src.orchestrator.offices import establish_office as _establish
        return await _establish(Actions(await _pool_get()), seat_or_agent=target,
                                actor=ident.agent_id)

    if action == "invalidate_works_in":
        assert stale_project is not None  # pre-dispatch validation already required it
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — invalidating a works_in edge is a deliberate "
                             "act on the record", "why": _anchorless(ctx)}
        pool = await _pool_get()
        from src.orchestrator.agents import invalidate_works_in as _invalidate_works_in
        result = await _invalidate_works_in(Actions(pool), ident.agent_id, stale_project,
                                            because=because, actor=ident.agent_id)
        if not result.get("error"):
            base = _generation(ident.agent_id)[0]
            dropped = result["was_working_in"].removeprefix("repo:")
            remaining = [p.removeprefix("repo:")
                        for p in (result.get("still_working_in") or [])]
            for cached in _agents.values():
                if _generation(cached.agent_id)[0] != base:
                    continue
                await _resolve_project_seat_first(pool, cached)
                if cached.project == dropped and len(remaining) == 1:
                    cached.project = remaining[0]
        return result

    if action == "reconcile_identity":
        return await _reconcile_seat_identity_impl(target, agent_id, because, ctx)

    if action == "rehold":
        assert target is not None and agent_id is not None  # already validated
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a third-party rehold is a deliberate act on "
                             "the record", "why": _anchorless(ctx)}
        from src.orchestrator.seats import rehold_seat as _rehold_seat
        return await _rehold_seat(
            Actions(await _pool_get()), seat_id=target, agent_id=agent_id, because=because,
            actor=ident.agent_id, override_live=override_live, dry_run=dry_run)

    if action == "correct_house":
        assert new_house is not None and new_house is not _UNSET  # already validated
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

    if action == "correct_pin":
        assert key is not None  # pre-dispatch validation already required it
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a pin correction is a seat's own act",
                    "why": _anchorless(ctx)}
        pool = await _pool_get()
        from src.orchestrator.offices import correct_own_pin_value as _correct_own_pin_value
        resolved_value = None if value is _UNSET else value
        return await _correct_own_pin_value(pool, ident.agent_id, key, resolved_value,
                                            reason=reason)

    if action == "resync_pin":
        assert target is not None and key is not None  # already validated
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a correction is a mind's act, and the graph "
                             "must know whose", "why": _anchorless(ctx)}
        pool = await _pool_get()
        from src.orchestrator.offices import correct_pin_value_third_party
        resolved_value = None if value is _UNSET else value
        return await correct_pin_value_third_party(
            pool, target, key, resolved_value, reason=reason, dry_run=dry_run,
            tree_cwd=tree_cwd)

    if action == "revert_pin":
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — reverting a pin is a seat's own act",
                    "why": _anchorless(ctx)}
        pool = await _pool_get()
        from src.orchestrator.offices import revert_own_pin_write as _revert_own_pin_write
        return await _revert_own_pin_write(pool, ident.agent_id)

    if action == "launch":
        assert target is not None  # pre-dispatch validation already required it
        ident = await _ident_for(ctx, session_anchor)
        if ident is None:
            return {"error": "mount(cwd, job_dir=<your anchor>) first — a launch must "
                             "say who it's from", "why": _anchorless(ctx)}
        actor = await _actor_for(ctx, subagent_id, subagent_type)
        from src.orchestrator.trigger import launch_seat
        return await launch_seat(Actions(await _pool_get()), caller=actor, target=target,
                                 message=message, model=model)

    if action == "resume":
        assert target is not None  # pre-dispatch validation already required it
        ident = await _ident_for(ctx, session_anchor)
        if ident is None:
            return {"error": "mount(cwd, job_dir=<your anchor>) first — a resume must "
                             "say who it's from", "why": _anchorless(ctx)}
        actor = await _actor_for(ctx, subagent_id, subagent_type)
        from src.orchestrator.trigger import resume_seat
        return await resume_seat(Actions(await _pool_get()), caller=actor, target=target,
                                 message=message, model=model)

    if action == "wake":
        assert target is not None  # pre-dispatch validation already required it
        ident = await _ident_for(ctx, session_anchor)
        if ident is None:
            return {"error": "mount(cwd, job_dir=<your anchor>) first — a wake must say "
                             "who it's from", "why": _anchorless(ctx)}
        actor = await _actor_for(ctx, subagent_id, subagent_type)
        from src.orchestrator.trigger import wake_worker
        return await wake_worker(Actions(await _pool_get()), caller=actor, target=target,
                                 message=message)

    if action == "wake_preflight":
        assert target is not None  # pre-dispatch validation already required it
        pool = await _pool_get()
        from src.orchestrator.trigger import (
            _resolve_wake_address,
            _seat_for_target,
            wake_gate_preflight,
        )
        wake_seat = await _seat_for_target(Actions(pool), target)
        wake_resolved = await _resolve_wake_address(pool, wake_seat or target)
        if isinstance(wake_resolved, dict):
            return {**wake_resolved, "status": "no-live-body"}
        resolved_target, seat_id = wake_resolved
        return await wake_gate_preflight(pool, resolved_target, seat_id=seat_id)

    if action == "promote":
        assert target is not None and workers is not None  # already validated
        ident = await _ident_for(ctx, session_anchor)
        if ident is None:
            return {"error": "mount first — a promotion must say whose hand called it",
                    "why": _anchorless(ctx)}
        pool = await _pool_get()
        from src.orchestrator.seats import promote_seat as _promote_seat
        result = await _promote_seat(Actions(pool), target, workers, because=because,
                                     actor=ident.agent_id)
        if result.get("error"):
            return result
        from src.orchestrator.boot_compiler import reissue_office as _reissue_office

        office_refresh: dict[str, Any] = {}
        for seat_id_affected in result.get("affected", []):
            office_refresh[seat_id_affected] = await _reissue_office(
                Actions(pool), seat_id=seat_id_affected,
                because=f"promotion: {because}", actor=ident.agent_id)
        result["office_refresh"] = office_refresh

        # `_agents` is this process's own live identity cache, healed the same way
        # correct_house/transition_project/rebind/invalidate_works_in already do after a
        # house-moving write. But those all heal the caller's own generation; promote's
        # affected seats are usually someone else's, so this asks held_seat which seat
        # each cached identity is actually bound to, rather than the cheaper
        # generation-prefix match those four use (extracted into
        # `_heal_mount_cache_for_seats`, shared with charter/charter_for/attach/detach/rename).
        await _heal_mount_cache_for_seats(pool, set(result.get("affected", [])))
        return result

    return {"error": f"unhandled action {action!r} — this is a dispatcher bug, not a "
                     "caller error, report it"}


@mcp.tool()
async def seat(
    action: str,
    target: str | None = None, handle: str | None = None, manager: str | None = None,
    new_cwd: str | None = None, extract: bool = False, force: bool = False,
    because: str = "", reason: str = "", dry_run: bool = True,
    repos: list[str] | None = None, adopt: bool = False, attended: str | None = None,
    new_handle: str | None = None, new_house: str | None = _UNSET,
    key: str | None = None, value: str | None = _UNSET, project: str | None = None,
    model: str | None = None, house: str | None = None, tree_cwd: str | None = None,
    source_paths: list[str] | None = None, override_live: bool = False,
    paused: bool = True, agent_id: str | None = None, wants_office: bool | None = None,
    cwd: str | None = None, job_dir: str | None = None, message: str = "",
    stale_project: str | None = None, fabricated_project: str | None = None,
    real_project: str | None = None, ruling: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, workers: list[str] | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """A single dispatcher tool for seat and agent lifecycle actions. Each `action`
    accepts only its own parameters (see `describe('seat')` for the full per-action
    shape, or call with a wrong or missing parameter and the error names exactly what
    that action expects). Shared parameters (target, because, reason, dry_run, etc.)
    mean the same thing across every action that takes them.

    ACTION TABLE. action: what it does (required parameters beyond action):
      mint: create a new managed worker seat under your own (handle)
      new: not available here yet, CLI-only (`osiris new`), no MCP tool for it
      stop: kill a live session's OS process (target=None means self)
      walk_in: mount + claim_name + establish a seat directory, in one call (handle, wants_office)
      promote_visitor: convert a third-party visitor into a full seat identity, gated by
        operator/manager/ruling authorization (target, handle, because)
      pause: gate the message-push channel for a seat (target=None means self)
      vacate: release a dead holder without retiring the seat (target, because)
      retire: mark a seat permanently closed, third-party (target)
      rebind: move a seat's anchor working directory (target, new_cwd)
      bind_tree: point a seat's code checkout (target, tree_cwd, because)
      attach: create a managed_by edge (target, manager, because)
      detach: remove a managed_by edge (target, because)
      charter: self-declare your own seat's charter (repos, or omit to read it)
      charter_for: declare a charter on another seat's behalf (target, repos, because,
                  optional ruling=<decision id> to act under a standing operator
                  ruling instead of manager authority; refused unless that ruling
                  actually names charter_for)
      heal_anchor: reassert the anchor working-directory invariant (target=None means self)
      heal_transcript: splice a fragmented session back into one file (target, source_paths)
      transition_project: move your own seat off a placeholder project binding
      resync_house: third-party house correction, unset with new_house=null (target, reason)
      sweep_disk: delete a retired seat's directory and workspace files (target)
      rename: change a seat's handle, manager- or operator-invoked (target, new_handle, because)
      set_attended: stamp a seat 'human' or 'worker' (target, attended, because)
      reissue_seat_dir: recompile a seat's managed CLAUDE.md section (target, because)
      establish_seat_dir: move a seat into its own managed directory (target)
      invalidate_works_in: drop your own duplicate works_in edge (stale_project, because)
      reconcile_identity: heal a house/project cross-source contradiction (target=None self)
      rehold: third-party re-assignment of a seat's holds link (target, agent_id, because)
      correct_house: correct your own house (new_house)
      correct_pin: correct an existing key in your own seat's pin (key, reason)
      resync_pin: third-party pin correction, dry_run default (target, key)
      revert_pin: undo your seat's most recent pin write
      launch: give a seat a fresh session (target); also its own named tool, same call
      resume: continue a seat's dormant session (target); also its own named tool
      wake: notify the other half of your managed_by pair (target, message); also named
      wake_preflight: check wake()'s gates before calling it (target); also named
      promote: make target a manager over workers, self-managed only (target, workers, because)
      refresh_project: force a fresh read of your own cached project, no target

    DRY RUN: several actions default `dry_run=True` (heal_anchor, heal_transcript,
    transition_project, sweep_disk, resync_pin, rehold)"""
    return await _seat_impl(
        action, target=target, handle=handle, manager=manager, new_cwd=new_cwd,
        extract=extract, force=force, because=because, reason=reason, dry_run=dry_run,
        repos=repos, adopt=adopt, attended=attended, new_handle=new_handle,
        new_house=new_house, key=key, value=value, project=project, model=model,
        house=house, tree_cwd=tree_cwd, source_paths=source_paths,
        override_live=override_live, paused=paused, agent_id=agent_id,
        wants_office=wants_office, cwd=cwd, job_dir=job_dir, message=message,
        stale_project=stale_project, fabricated_project=fabricated_project,
        real_project=real_project, ruling=ruling, subagent_id=subagent_id,
        subagent_type=subagent_type, session_anchor=session_anchor, workers=workers,
        ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='pause')",
    "since": "the seat dispatcher tool",
})
async def pause_seat(paused: bool = True, target: str | None = None, reason: str = "",
                     session_anchor: str | None = None,
                     ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated. Kept callable for compatibility. Forwards to seat(action='pause')."""
    return await _seat_impl("pause", paused=paused, target=target, reason=reason,
                            session_anchor=session_anchor, ctx=ctx)


@mcp.tool()
async def candidates(project: str | None = None, limit: int = 50,
                     ctx: Context | None = None) -> dict[str, Any]:
    """List the automated miner's guesses about your project that nobody has reviewed
    yet. An automated process reads session transcripts and proposes loose ends it
    thinks were forgotten; it is right roughly one time in ten. These are candidates,
    not confirmed duties. Read them, then call dispose(): admit what is real (it becomes
    yours: self-declared, owned, permanently kept) and drop the rest with a reason. Only
    this project's own session has standing to judge these candidates.

    Read-only. Reading costs nothing and commits nothing. Returned oldest first."""
    ident = await _ident_for(ctx)
    proj = project or (ident.project if ident else None)
    return await dispose_seam.candidates(await _pool_get(), project=proj, limit=limit)


@mcp.tool()
async def dispose(admit: list[dict[str, Any]] | None = None,
                  drop: list[dict[str, Any]] | None = None,
                  ask: list[dict[str, Any]] | None = None,
                  ctx: Context | None = None) -> dict[str, Any]:
    """Settle the automated miner's candidate guesses, relevant or irrelevant, under
    your name, with a reason for each.

    `admit`: [{"id", "because", "owner"?}]. The guess was right, now yours (promoted to
    self-declared). `because` is required. `drop`: [{"id", "why", "because"?}]. The
    guess was wrong; `why` names its class: narration | stale | echo | misfiled |
    principle | other (explain further in `because`). `ask`: [{"id", "because"?,
    "owner"?}]. A real open question, kept open and reclassified as kind='question'.

    Nothing is deleted; a drop is a compensating event, readable and reversible. Returns
    your yield: (admitted + asked) / judged."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first. A disposition must be attributed to a specific "
                         "session", "why": _anchorless(ctx)}
    return await dispose_seam.dispose(
        Actions(await _pool_get()), source=ident.agent_id, admit=admit, drop=drop, ask=ask)


# The graded wall now lives in compositions.py: one home shared by orient, the console
# briefing, and the `wall` function. The private names stay importable here (tests and
# callers address orient's wall through them).
_ORIENT_OPEN_THREADS = comp.ORIENT_OPEN_THREADS
_rank_open_threads = comp.rank_open_threads
_open_thread_wall = comp.open_thread_wall


async def _project_briefing(
    pool: asyncpg.Pool, project: str, me: frozenset[str] = frozenset(), verbose: bool = False,
    want_blind_spots: bool = False,
) -> dict[str, Any] | None:
    """A working agent's scoped bearings: its own project's open threads plus recent
    decisions, not the whole fleet's, since a flood of unrelated context costs more than it
    saves. Decisions and tensions ride the `project-briefing` composition; the open-thread
    wall is assembled here because the composer can't express what the wall now needs:
    obligations-first ranking, grade-aware echo detection (a never-touched derived thread
    collapses into a counted line instead of riding forever), and a triage card of up to 3
    of the oldest echoes handed to each session with the three honest actions available.
    Ranking and collapsing happen only at display time; the record keeps every thread open
    until testimony says otherwise."""
    from src.orchestrator.capture import _resolve_repo
    proj = await _resolve_repo(pool, project)
    if proj is None:
        return None
    # `me` is the wall's identity set ({agent_id, project}, or {'operator'} from the
    # console); the reflection access check wants one reader, the agent id when there is one
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
        # Two agents can lean apart: the table shows one winner per property, but a held
        # polarity may carry different current leans from different agents. This surfaces
        # that instead of silently picking one; the record keeps both either way.
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
    if blind_spots:  # what this project can't verify from here; absent stays silent
        # The full list used to ride every scoped orient() call regardless of whether the
        # caller needed it, measured at roughly 4.2K bytes/call across a sample of calls,
        # the single largest static (non-work-item) field in the payload. A fresh session
        # needs to know something is unverifiable here, not re-read the whole list every
        # time; the count is the actionable signal, the list is opt-in.
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
    if more > 0:  # trailing count so a capped wall never hides work silently
        # The count is structural: a terse result that strips the sentence below must not
        # lose the fact a capped wall is hiding work; open_threads_more survives terse mode
        # even when open_threads_note (the prose explaining it) doesn't.
        out["open_threads_more"] = more
        out["open_threads_note"] = (
            f"showing {len(shown)} of {len(shown) + more} open threads (obligations first; "
            "within a kind, yours-to-act before others' claims before waiting-on-the-human, "
            f"then recency); {more} more not shown")
    # `len(shown)+more` above counts by the `status` property alone: a thread a decision
    # already closed (resolves=/resolve_thread) but whose own 'open' assertion never got
    # superseded, or one flagged `disagree` (a closure edge exists yet property_status
    # still says 'open'), still counts as open there. closure_buckets composes
    # thread_closure_status's own topology read, the same already-built mechanism
    # closure_health's composition uses, not a second counting mechanism, and its
    # `open_both` bucket is the one genuinely, unambiguously open count. This is additive,
    # never replacing open_threads_more/open_threads_note above: those still drive the
    # wall's own listing (individual rows worth a look, property-based on purpose, since a
    # stale `disagree` row is exactly the kind of thing worth reviewing); this is only the
    # headline number a coordinator's scheduling math should actually use. Kept cheap: no
    # per-thread artifact-resolution enrichment (that N+1 stays inside closure_health's
    # own, deliberately richer, deliberately not-primary-path call).
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
        # more may exist; count for real rather than assume, for symmetry with
        # open_threads_more. Mirrors the composition's own filter exactly (project-scoped,
        # active, no winning superseded_by/retracted); never touch the composition itself
        # just to learn its own total, that's what this count is for.
        total = await pool.fetchval(
            "SELECT count(*) FROM objects o "
            "JOIN links l ON l.from_id=o.id AND l.type='in_repo' AND l.to_id=$1 "
            "AND (l.valid_until IS NULL OR l.valid_until > now()) "
            "WHERE o.type='Decision' AND o.status='active' "
            "AND NOT EXISTS (SELECT 1 FROM current_assertions s WHERE s.object_id=o.id "
            "  AND s.name='superseded_by') "
            "AND NOT EXISTS (SELECT 1 FROM current_assertions s WHERE s.object_id=o.id "
            "  AND s.name='retracted')", proj)
        if total and total > 15:
            out["recent_decisions_more"] = total - 15
    # Terse by default: a byte-per-key measurement named the real weight, since summary
    # text is 96-98% of every open_threads/recent_decisions item. _cap_text (truncation,
    # not deletion) shortens it in terse mode; verbose restores full summaries exactly as
    # today. Every decision item now also carries `id` (compositions.py's _table gained
    # the "id" property for this) so a capped summary is addressable: verbose=True or
    # search(query=...) recovers the rest.
    if not verbose:
        _cap_text(out["open_threads"], "summary", exempt_when_true="is_handoff")
        _cap_text(out["recent_decisions"], "summary", exempt_when_true="is_handoff")
    return out



# ---- Phase 2: GRANULAR GETTERS (graphy tool surface) -------------------

@mcp.tool()
async def get_status(render: str | None = None, ctx: Context | None = None) -> dict[str, Any]:
    """Your identity, mail count, and fleet status, in brief. Returns only:
    you, model, project, seat, mail, fleet_pulse, handoff_pending. No thread/decision
    text, no succession notes. `handoff_pending` is a bare pointer only, so a caller can
    check whether an unread handoff exists without paying the cost of `orient()`'s full
    succession note: {"from": <agent_id>, "refs": [<short-id>, ...]} when your nearest
    ancestor left one unacknowledged, else omitted entirely.
    Read the real text with recall(ref=<one of refs>); ack_handoff(ref=...) once read.

    `render='text'` returns only {"text": <str>}, one line per field, rendered on the
    server, so a caller can print it verbatim instead of a model re-formatting JSON at
    token cost. Omit (or pass any other value) for the ordinary structured result."""
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
    result: dict[str, Any] = {
        "you": ident.agent_id if ident else "unmounted", "project": proj}
    if proj:
        # `project` is now always the current display name (see `_seated_house`'s own
        # fix). A project can rename any number of times, but its `repo:<slug>` canonical
        # never does, so a caller that wants the stable handle across a rename (a
        # bookmark, a cross-reference) needs it named separately rather than re-deriving
        # it from whichever display name happened to be current when it was written down.
        from src.orchestrator.capture import _resolve_repo
        proj_oid = await _resolve_repo(pool, proj)
        if proj_oid is not None:
            result["project_canonical"] = await pool.fetchval(
                "SELECT canonical FROM objects WHERE id=$1", proj_oid)
    if ident:
        sb = await seat_bearings(pool, ident.agent_id)
        result["model"] = ident.model
        if sb:
            result.update(sb)
    result["mail"] = mail
    if pulse:
        result["fleet_pulse"] = pulse
    if ident and ident.succeeded_from:
        found, _complete = await nearest_handoff_ancestor(pool, ident.succeeded_from)
        if found:
            from_id, picks = found
            result["handoff_pending"] = {
                "from": from_id, "refs": [str(p["id"])[:8] for p in picks]}
    if render == "text":
        from src.orchestrator.textrender import render_status_text
        return {"text": render_status_text(result)}
    return result


@mcp.tool()
async def pulse(ctx: Context | None = None) -> dict[str, Any]:
    """A harness-neutral liveness refresh: call this periodically to keep reading as
    live (roster, co_agents, DM delivery, and the live-holder guard on claiming a name)
    without paying for mount()'s full re-attach process. No hook, no status line, no
    Claude transcript required. A Claude Code session already gets this for free from
    its own status-line heartbeat and from every mount()/automount() re-attach; this
    tool exists for any other harness speaking plain MCP that wants the same freshness
    on its own terms.

    Self-scoped, always: touches only the calling identity's own `agent_mounts` rows.
    There is no `target` parameter, so this can never refresh another identity's
    liveness. Refuses if you haven't mounted (nothing to refresh). `touched=0` (mounted,
    but no durable job_dir on record) is a legal, reportable no-op, never an error.

    The 5-minute window: a non-Claude harness's liveness reads as true from a pulse
    fresher than 5 minutes. Call this at least that often if you want your own DMs to
    route as live and your name safe from a genuine live-holder collision. Call it less
    often and you read as cold, honestly, exactly like a quiet Claude session would.
    This is a freshness check, never a blanket exemption."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first: pulse refreshes your own liveness, and the graph "
                         "must know whose", "why": _anchorless(ctx)}
    from src.orchestrator.mounts import pulse_mount
    return await pulse_mount(await _pool_get(), agent_id=ident.agent_id)


async def _charter_scoped_project_ids(
    pool: asyncpg.Pool, ctx: Context | None, project: str, proj_id: Any,
) -> tuple[list[Any], list[str]]:
    """Charter-aware read scope: a chartered seat's own writes land under every repo it
    governs (`in_repo` follows the write's own target, not the caller's mount), but
    `get_thread_list`/`get_decision_list` used to resolve exactly one literal
    `repo:{project}` and stop there. A successor mounting under any one name in a
    multi-repo charter saw only that slice of its own seat's work, silently, at every
    succession. `settle()` already detects and names this exact shape (the "filed under X
    but its own writes went to [X,Y]" warning); the charter already knows what a seat
    governs, these two read actions simply never asked it.

    Defaults to the charter rather than requiring an opt-in `spans_charter` flag: an
    opt-in flag does nothing for the successor who does not know to ask for it, which is
    the entire failure mode this was built to stop. Defaulting closes the gap for every
    future successor without requiring them to learn a parameter first.

    THE ACCESS BOUNDARY (read this before touching this function): a chartered seat
    reading its own chartered repos is within authority; anything wider is a data leak
    between projects. This function can never widen past the caller's own charter, by
    construction, not by a permission check that could drift: it only ever expands the
    scope when `project` (the literal name the caller asked for) is itself a member of
    the calling seat's own `governs` set, and the widened set is then that same charter,
    nothing else. A caller peeking at a project it does not govern (no identity, no held
    seat, or `project` absent from its own charter) gets the exact single-repo behavior
    this tool always had, unchanged, and never widened on someone else's behalf. Returns
    (project object ids to scope the query to, the full charter list, empty unless
    expansion actually applied, so a caller can tell whether it got one repo's items or
    several)."""
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


async def _get_thread_list_body(
    project: str, kind: str | None, owner: str | None,
    limit: int, offset: int, ctx: Context | None,
    min_age_days: float | None = None, max_age_days: float | None = None,
) -> dict[str, Any]:
    """The `object_type='thread'` branch of `_get_object_list_impl`, copied verbatim
    from get_thread_list's own top-level function body before the fold into the
    dispatcher.

    DOUBLE-THREAD FIX, root-caused live not assumed: every thread in a project with a
    retracted-then-recreated `in_repo` edge (a fold, a link correction, an ordinary
    re-file) used to appear twice in this listing. The JOIN onto `links` had no
    `valid_until` filter at all, so it matched every historical `in_repo` row a Thread
    ever had, live or retracted, not just its current one. This was never a
    multi-current-status-row leak of the kind seen elsewhere; that class was
    independently confirmed already closed (current_flags(action='inspect') reads
    count=0 live, and every affected case carries exactly one current `status` row on
    inspection). This is a plain missing-filter bug on a completely different table
    (`links`, not `assertions`), unrelated to is_current. Fixed here and in
    `_get_decision_list_body` below (same copy-paste origin, same missing filter) by
    requiring `l.valid_until IS NULL OR l.valid_until > now()`, the same convention
    `create_link`'s own retraction path already documents."""
    pool = await _pool_get()
    from src.orchestrator.capture import _resolve_repo
    proj = await _resolve_repo(pool, project)
    if proj is None:
        return {"error": f"no project {project!r}", "threads": [], "total": 0}
    project_ids, charter_repos = await _charter_scoped_project_ids(pool, ctx, project, proj)
    # Measured live before this fix: 75.5% false-open (2,553 of 3,380 "active" Thread
    # objects were actually resolved/retracted). `o.status='active'` is the object's own
    # lifecycle column (active vs merged/retired), a completely different fact from the
    # thread's own `status` property (open/resolved), which resolve_thread sets via a
    # superseding assert_property and never touches `o.status` at all. This clause was
    # simply missing; find_near_duplicate_open_thread's own query (capture.py) already
    # gets it right with the identical COALESCE(...,'open')='open' pattern this now
    # matches.
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
    if min_age_days is not None:
        clauses.append("o.created_at <= now() - ($" + str(idx) + " * interval '1 day')")
        params.append(min_age_days)
        idx += 1
    if max_age_days is not None:
        clauses.append("o.created_at > now() - ($" + str(idx) + " * interval '1 day')")
        params.append(max_age_days)
        idx += 1
    where = " AND ".join(clauses)
    total = await pool.fetchval(
        "SELECT count(*) FROM objects o "
        "JOIN links l ON l.from_id=o.id AND l.type='in_repo' AND l.to_id = ANY($1) "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "WHERE " + where, *params) or 0
    result: dict[str, Any] = {"project": project}
    if charter_repos:
        result["charter_repos"] = charter_repos
    # Same additive approach as orient()'s own open_threads_honest_total: `total` above
    # counts by the `status` property, unchanged, since an existing field's meaning never
    # changes silently. `honest_total` is a new, separate field, summed over `project_ids`
    # (a chartered seat's own small repo set, never fleet-wide) via closure_buckets, the
    # same shared mechanism orient() and closure_health both already use, not a second
    # counting path. `kind`/`owner` filters do not narrow this count (thread_closure_status
    # has no such filters of its own, and the honest count is meant to answer "how much is
    # really open", not "how much of this filtered slice"): a caller filtering by
    # kind/owner still gets the whole-project honest denominator, named plainly so it
    # isn't misread as scoped to the filter.
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
        "SELECT o.id, o.canonical, o.created_at, "
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
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "WHERE " + where + " "
        "ORDER BY o.created_at DESC "
        "OFFSET $" + str(idx) + " LIMIT $" + str(idx + 1),
        *params, offset, limit)
    threads = []
    for r in rows:
        threads.append({"id": str(r["id"])[:8], "canonical": r["canonical"],
                        "created_at": r["created_at"].isoformat(),
                        "summary": (r["summary"] or "")[:200],
                        "kind": r["kind"], "owner": r["owner"]})
    more = max(0, total - offset - len(threads))
    return {**result, "threads": threads, "total": total, "more": more,
            "note": "recall(ref) for full text; orient() for the ranked wall"}


async def _get_decision_list_body(
    project: str, limit: int, offset: int, ctx: Context | None,
) -> dict[str, Any]:
    """The `object_type='decision'` branch of `_get_object_list_impl`, copied verbatim
    from get_decision_list's own top-level function body before the fold into the
    dispatcher."""
    pool = await _pool_get()
    from src.orchestrator.capture import _resolve_repo
    proj = await _resolve_repo(pool, project)
    if proj is None:
        return {"error": f"no project {project!r}", "decisions": [], "total": 0}
    project_ids, charter_repos = await _charter_scoped_project_ids(pool, ctx, project, proj)
    total = await pool.fetchval(
        "SELECT count(*) FROM objects o "
        "JOIN links l ON l.from_id=o.id AND l.type='in_repo' AND l.to_id = ANY($1) "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
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
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
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


async def _get_object_list_impl(
    object_type: str, project: str, *, kind: str | None, owner: str | None,
    limit: int, offset: int, ctx: Context | None,
    min_age_days: float | None = None, max_age_days: float | None = None,
) -> dict[str, Any]:
    """Shared implementation behind `get_object_list` and its two hidden single-purpose
    aliases (get_thread_list/get_decision_list): one code path, three names. Same
    charter-scoped project resolution, same {items, total, more} pagination contract,
    different item key per branch. `min_age_days`/`max_age_days` are thread-only, ignored
    on the decision branch."""
    if object_type == "thread":
        return await _get_thread_list_body(project, kind, owner, limit, offset, ctx,
                                           min_age_days=min_age_days, max_age_days=max_age_days)
    if object_type == "decision":
        return await _get_decision_list_body(project, limit, offset, ctx)
    return {"error": f"unknown object_type {object_type!r} — one of thread/decision"}


@mcp.tool()
async def get_object_list(
    object_type: str, project: str, kind: str | None = None, owner: str | None = None,
    min_age_days: float | None = None, max_age_days: float | None = None,
    limit: int = 10, offset: int = 0, ctx: Context | None = None,
) -> dict[str, Any]:
    """Recent threads or decisions for a project, paginated. Charter-aware: spans every
    repo the caller governs (see `charter_repos`). `object_type='thread'|'decision'`
    selects the branch; `limit=0` for count only.

    `object_type='thread'`: open threads only. Returns {threads, total, more,
    honest_total, honest_total_note}, each thread carrying `created_at`. `kind`: obligation/
    question/task. `owner`: agent id / 'operator'. `min_age_days`/`max_age_days`: creation
    age in days, either or both.

    `object_type='decision'`: recent decisions, newest first. Returns {decisions, total,
    more}. Other filters are thread-only, ignored here."""
    return await _get_object_list_impl(object_type, project, kind=kind, owner=owner,
                                       limit=limit, offset=offset, ctx=ctx,
                                       min_age_days=min_age_days, max_age_days=max_age_days)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "get_object_list(object_type='thread')",
    "since": "superseded by get_object_list",
})
async def get_thread_list(
    project: str, kind: str | None = None, owner: str | None = None,
    limit: int = 10, offset: int = 0, ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    get_object_list(object_type='thread')."""
    return await _get_thread_list_body(project, kind, owner, limit, offset, ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "get_object_list(object_type='decision')",
    "since": "superseded by get_object_list",
})
async def get_decision_list(
    project: str, limit: int = 10, offset: int = 0, ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    get_object_list(object_type='decision')."""
    return await _get_decision_list_body(project, limit, offset, ctx)


@mcp.tool()
async def list_unfiled_threads(
    source: str | None = None, kind: str | None = None,
    min_age_days: float | None = None, max_age_days: float | None = None,
    limit: int = 10, offset: int = 0,
) -> dict[str, Any]:
    """Threads with NO `in_repo` edge at all. Genuinely unfiled, invisible to
    `get_thread_list(project=...)` no matter which project is asked. Paginated
    (limit/offset), real `total` count, each thread carrying `created_at`. `source`
    filters by the creating actor's provenance id (e.g. 'half-heal-detect'); `kind`:
    obligation/question/task; `min_age_days`/`max_age_days`: creation age in days,
    either or both. limit=0 for count only."""
    pool = await _pool_get()
    clauses = [
        "o.type='Thread' AND o.status='active' AND COALESCE("
        "(SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        " AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),"
        "'open')='open'",
        "NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id AND l.type='in_repo' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()))",
    ]
    params: list[Any] = []
    idx = 1
    if kind:
        clauses.append(
            "(SELECT a.value #>> '{}' FROM current_assertions a "
            "WHERE a.object_id=o.id AND a.name='kind' "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) = $" + str(idx))
        params.append(kind)
        idx += 1
    if source:
        clauses.append(
            "EXISTS (SELECT 1 FROM object_events e WHERE e.object_id=o.id "
            "AND e.event_type='create' AND e.actor=$" + str(idx) + ")")
        params.append(source)
        idx += 1
    if min_age_days is not None:
        clauses.append("o.created_at <= now() - ($" + str(idx) + " * interval '1 day')")
        params.append(min_age_days)
        idx += 1
    if max_age_days is not None:
        clauses.append("o.created_at > now() - ($" + str(idx) + " * interval '1 day')")
        params.append(max_age_days)
        idx += 1
    where = " AND ".join(clauses)
    total = await pool.fetchval("SELECT count(*) FROM objects o WHERE " + where, *params) or 0
    if limit == 0:
        return {"threads": [], "total": total, "more": total}
    rows = await pool.fetch(
        "SELECT o.id, o.canonical, o.created_at, "
        "(SELECT a.value #>> '{}' FROM current_assertions a "
        " WHERE a.object_id=o.id AND a.name='summary' "
        " ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS summary, "
        "(SELECT a.value #>> '{}' FROM current_assertions a "
        " WHERE a.object_id=o.id AND a.name='kind' "
        " ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind, "
        "(SELECT a.value #>> '{}' FROM current_assertions a "
        " WHERE a.object_id=o.id AND a.name='owner' "
        " ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS owner "
        "FROM objects o WHERE " + where + " "
        "ORDER BY o.created_at DESC "
        "OFFSET $" + str(idx) + " LIMIT $" + str(idx + 1),
        *params, offset, limit)
    threads = [{"id": str(r["id"])[:8], "canonical": r["canonical"],
               "created_at": r["created_at"].isoformat(),
               "summary": (r["summary"] or "")[:200],
               "kind": r["kind"], "owner": r["owner"]} for r in rows]
    more = max(0, total - offset - len(threads))
    return {"threads": threads, "total": total, "more": more}


@mcp.tool()
async def graph_search(
    query: str, project: str | None = None, lineage: str | None = None,
    max_depth: int = 0, limit: int = 15, ctx: Context | None = None,
) -> dict[str, Any]:
    """GRAPH-AWARE search -- same lexical/semantic engine as search() but scoped
    to a subgraph. project narrows results to one project. lineage scopes to
    a specific agent lineage (e.g. 'ad1a1cb0'). max_depth > 0 expands results
    to include the N-hop expansion around each hit (linked objects).
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
    await _stamp_read_ids(pool, ident, "graph_search", [h["id"] for h in hits])
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
    """Get your bearings: the mount step as one call. Returns a scoped briefing: open
    threads and recent decisions for a project, plus a fleet-wide not-shown count. An
    explicit `project` overrides your mount; unmounted with neither gives the whole-fleet
    briefing. Call after mount(), and again after any compaction.

    `verbose=True` restores the prose that terse mode (the default) trims: explanations,
    the ancestor-letter pointer, full-length summaries (capped to 160 chars terse, each
    still carrying `id`). Every structured fact survives either way.

    `want_blind_spots=True` returns the full list; default is a count."""
    pool = await _pool_get()
    lease = get_settings().osiris_mail_lease_secs
    ident = await _ident_for(ctx, session_anchor)
    proj = project or (ident.project if ident else None)  # explicit scope overrides the mount
    proj_canonical = None
    if proj:
        # Same fix as get_status, see its own comment: `project` is the current display
        # name, `project_canonical` the stable `repo:<slug>` identity a rename never touches.
        from src.orchestrator.capture import _resolve_repo
        proj_oid = await _resolve_repo(pool, proj)
        if proj_oid is not None:
            proj_canonical = await pool.fetchval(
                "SELECT canonical FROM objects WHERE id=$1", proj_oid)
    who = ident.agent_id if ident else "session (unmounted, call mount(cwd) first)"
    reader = ident.agent_id if ident else (proj or "")
    # A subagent asking for bearings must not be told it IS the seat: 'you' is the child,
    # reporting the seat's model swap is the parent's duty, and the parent's mailbox stays
    # the parent's
    spawn = await _actor_for(ctx, subagent_id, subagent_type) if subagent_id else None
    if spawn is not None and spawn != (ident.agent_id if ident else None):
        who = f"{spawn}, a subagent of {ident.agent_id if ident else 'an unmounted parent'}. " \
              "your writes are your own, the seat and its mail are your parent's"
    counts = (await unread_counts(pool, proj, reader_agent=reader, lease_secs=lease)
              if proj else {"total": 0, "ask": 0})
    unread, asks = counts["total"], counts["ask"]
    mail = (f"{unread} unread ({asks} ask{'s' if asks == 1 else ''} something of you), "
            "inbox()" if asks else f"{unread} unread, inbox()") if unread else "none"
    # scoped to this seat's own unanswered briefs to the operator
    op_unread = await mailbox.desk_briefs_from(pool, ident.agent_id if ident else None)
    op_mail = {"operator_mail": f"{op_unread} of your briefs await the operator's eye. "
                                "inbox(project='operator') if the human is present"
               } if op_unread else {}
    # A house is what a seat governs, not where it sits, but a charter nobody can see is
    # not an inheritance. No aggregation here (that's the charter-scoped briefing
    # elsewhere); just the fact, named.
    #
    # Re-keyed onto the seat rather than a lineage walk: `governs` now originates from the
    # seat's own durable object id, so no prefix guess is needed; held_seat is the same
    # lineage-aware resolution orient() already trusts for the seat line below. This
    # dissolves an old set_charter limitation: a successor re-declaring now heals the same
    # from_id an ancestor generation used, so there is no ancestor/successor distinction
    # left to trip over, one seat, one link.
    #
    # The render below used to fold this key in with `if charter else {}`, an idiom copied
    # from swap/pin_warn, where falsy means "nothing wrong" and omission is correct. For
    # charter, falsy ([]) is the alarm state, so the same idiom silently rendered
    # "chartered, all fine" and "never declared" as identical silence, on the one surface
    # every seat reads every session (confirmed live: most active seats read `charter`
    # absent from their own orient()). Gated on `charter_seat is not None` now, not on
    # `charter` truthiness: a session holding no seat at all has nothing to charter and
    # stays silent (this is not a seat-only alarm turned into a universal one); a session
    # that does hold a seat gets told the truth either way, stated once and plainly
    # (`_CHARTER_UNDECLARED`, the same text mint_seat's and establish_office's own results
    # already use), never a repeated warning banner. Noted rather than quietly assumed:
    # `charter_of` cannot currently distinguish "never declared" from "declared as
    # governing zero repos" (`set_charter(repos=[])` heals every existing edge and leaves
    # no trace it was ever called): both read back as the identical empty list, so both
    # render as undeclared here. That is an honest limit of the data model, not a bug this
    # change introduces or is scoped to fix.
    from src.orchestrator.charter import charter_of
    from src.orchestrator.offices import _CHARTER_UNDECLARED
    from src.orchestrator.seats import held_seat
    charter_seat = await held_seat(pool, ident.agent_id) if ident else None
    charter = await charter_of(pool, charter_seat["seat_id"]) if charter_seat else []
    # A repo whose model choice is settled, a .osiris file, or an intended_model property
    # recorded on the SoftwareProject, must not re-confront every successor with the
    # fleet default. A settled choice isn't even a decision point; every banner consults
    # _expected_model first.
    swap = swap_banner(classify_swap(
        ident.model_history, ident.model,
        expected=await _expected_model(pool, ident.cwd, proj),
        anchored=ident.model_method == "job_dir",
        deliberate=ident.model_deliberate)) if ident else None
    if spawn is not None:
        swap = None  # reporting the seat's model swap history is the parent's duty, not the child's
    pin_warn = project_pin_banner(ident) if ident else None  # no/unparseable/found-unset pin
    if swap and ident:  # a triage wake on the economy model is policy, not a surprise change
        swap = await _wake_economy_standdown(pool, proj, ident.model) or swap
    away = await mounts.while_away(
        pool, proj, ident.agent_id, _prev_seen.get(ident.agent_id)) if ident else None
    # A successor's orient surfaces the ancestor's own parting words, its handoff thread
    # and letter decision, verbatim, instead of promising a field that never existed.
    #
    # STRUCTURED FIRST, PROSE AS FALLBACK: word-matching identity is the disease behind
    # every mislabeled-successor bug this system has hit; an is_handoff='true' property
    # (stamped by settle(), a typed query) is the reliable half. The ILIKE
    # '%handoff%'/'%letter%' text match stays only for handoffs minted before this
    # existed, never removed, never the sole check for anything settle() writes going
    # forward.
    # BOUNDED CHAIN-WALK: a one-hop-only read goes blind the moment the immediate
    # ancestor never wrote a handoff (a phantom, or simply silent) even though a real one
    # sits further back. nearest_handoff_ancestor (agents.py) walks up to 5
    # succeeded_from links, shared with the startup path so both read one implementation.
    # READ ACKNOWLEDGMENT, NOT INFERRED-READ: delivery here is unconditional. This block
    # never writes anything, so the non-negotiable acceptance test (a fresh seat's first
    # orient() must receive its predecessor's handoff whole) holds by construction, not
    # by careful ordering. What makes a handoff stop being delivered is a separate,
    # deliberate ack_handoff(ref=...) call, mirroring inbox()'s own lease-vs-settle
    # split: an unacknowledged handoff redelivers on every orient(), exactly like
    # unsettled mail.
    inheritance = None
    if ident and ident.succeeded_from:
        found, _complete = await nearest_handoff_ancestor(pool, ident.succeeded_from)
        if found:
            from_id, picks = found
            inheritance = {
                "from": from_id,
                "notes": [{"kind": r["type"].lower(), "id": str(r["id"])[:8],
                           "text": cap_handoff_text(r["summary"])}
                          for r in picks],
                "note": "your predecessor's parting notes, read before taking up work. "
                        "ack_handoff(ref=<id>) once you have: an unacknowledged handoff "
                        "stays live and keeps costing every future orient() in this "
                        "project, not just yours.",
            }
    # A lineage-scoped, not project-scoped, misfiling finder: where identity_coherence
    # (settle.py) can only ever see this session's own writes, this can see every
    # generation's, so a correctly-filed successor can find an ancestor's misfiled work.
    # Report-only, never a gate.
    misfiled = (await misfiled_by_lineage(pool, ident.agent_id, proj)
               if ident and proj else None)
    # A live sibling agent can share the exact worktree without the graph ever saying so,
    # forcing it to re-derive local conventions from a file when the system already knew
    # them. One query surfaces other live mounts on this project, named at orient.
    co_agents = await _co_agents(pool, proj, ident.agent_id) if ident and proj else None
    # A peer_of bond is recognition-first: an edge nobody's briefing ever surfaces is a
    # convention, easy to ignore, exactly like co_agents' shared tree used to be before it
    # was surfaced. Computed off ident.agent_id (never `who`, which can carry a spawn's
    # description string), the same discipline co_agents already follows.
    peer = await _peer_bearings(pool, ident.agent_id) if ident else None
    try:  # one glance line — never let the pulse slow or crash orient
        pulse: str | None = await mounts.fleet_pulse(pool, lease_secs=lease)
    except Exception:  # noqa: BLE001
        pulse = None
    # If the background extraction process is down, the graph is not forming memory, and
    # every agent that mounts is about to trust a record that stopped growing. This went
    # unnoticed for ten hours once because the only signal was a counter inside a payload
    # too large to open. Derived at read time, here, in a process that is alive by
    # construction: a watchdog cron job would have lived inside the very worker that
    # died. Silent when the system is healthy.
    try:
        organs: str | None = health_banner(await organ_health(pool))
    except Exception:  # noqa: BLE001
        organs = None
    # A gate nobody can see is a gate nobody trusts, and the root cause was that nothing
    # surfaced whether an automated producer's output was ever used. So the seat sees its
    # own undisposed pile, and, when the automated producer has spent itself out of its
    # budget, the number that took it away.
    seam: dict[str, Any] = {}
    with contextlib.suppress(Exception):
        pile = await dispose_seam.candidates(pool, project=proj, limit=0) if proj else None
        if pile and pile["count"]:
            seam["your_pile"] = (
                f"{pile['count']} miner candidates on {proj} that nobody has judged yet. They "
                "are guesses, not duties. candidates() to read, dispose() to settle. Nobody else "
                "has standing to judge your project's pile.")
        lic = await dispose_seam.licence(pool)
        if not lic["may_spend"]:
            seam["adversary_refused"] = lic["reason"]
    # Fleet-wide by design: a workaround replicates across projects, so the announcement
    # of its death must too. Bounded window; silent when nothing died recently; search
    # remembers every kill forever.
    dead: dict[str, Any] = {}
    with contextlib.suppress(Exception):
        kills = await capture.recent_dead_superstitions(pool)
        if kills:
            dead["dead_superstitions"] = {
                "recent": kills,
                "note": "workarounds whose bug is fixed. If your notes or succession "
                        "notes carry one of these practices, remove it; the killed_by "
                        "pointer is the fix to cite",
            }
    # A fresh compaction's own mining sweep is async and the interface gives no
    # confirmation it landed. Rather than let a successor trust that silently, orient checks
    # this lineage's own most recent sweep_ledger row; if it's still incomplete past the
    # watchdog's own SLA (arq_worker.SWEEP_RETRY_SLA=300s, duplicated here on purpose: "the
    # extraction process mines, the server only signals" is a deliberate ownership boundary,
    # sweep_route/orient never import the worker module), the successor is told plainly
    # instead of silently trusting an unconfirmed predecessor. Same family as swap_banner:
    # a fact the running session cannot detect on its own, so never stripped by the terse
    # pass below (same discipline as `swap`).
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
                    f"{int(row['age_secs'] // 60)} min ago) has not confirmed completion. "
                    "It retries automatically; nothing to act on, but don't assume its "
                    "results have landed in the graph yet."
                )
    # The reader's identity feeds the wall's ownership ordering: what is mine to act rides
    # above another agent's claims and above 'waiting on the human', matching what the
    # startup path needs too (compositions.reader_identity_set). This folds in the seat's
    # own handle too, not just agent_id/project, so a charter obligation filed
    # owner='<handle>' ranks as mine here exactly as it does there.
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
            **({"project_canonical": proj_canonical} if proj_canonical else {}),
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
        # Terse by default: the fields stripped below are fully redundant with a
        # structured sibling already in this dict (the top-level note restates
        # fleet_open_threads_total; open_threads_note restates open_threads_more;
        # unread_echoes/blind_spots/dead_superstitions keep their data lists, only the
        # "here's what to do about it" sentence drops). This never touches `swap`, an
        # identity-safety fact, not guidance. co_agents.note is the shared-tree safety
        # warning ('never git add -A, stage your own hunks, check foreign markers'): the
        # `live` list says who is here, this says what to do about it, and it's
        # conditional (only present with live siblings) so it's not per-call bloat.
        # succession_note.note stays too, since a pre-existing test (test_capture.py)
        # asserts it unconditionally; restoring the tested contract rather than
        # re-litigating it here.
        return result if verbose else _terse(
            result, ("note",), ("open_threads_note",), ("unread_echoes", "note"),
            ("unread_echoes", "verbs"), ("blind_spots_note",),
            ("dead_superstitions", "note"))
    # A fresh session's first orient() once returned 353K chars of whole-fleet briefing
    # it had to parse out of a dump file. An unmounted caller now gets a bounded map
    # (per-project open counts plus the newest few decisions) and the mount instructions;
    # the full firehose stays one deliberate call away.
    fleet_map = [dict(r) for r in await pool.fetch(
        "SELECT p.canonical AS project, count(*) AS open_threads "
        "FROM objects o JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' AND p.status='active' "
        "WHERE o.type='Thread' AND o.status='active' "
        "AND (SELECT s.value #>> '{}' FROM current_assertions s WHERE s.object_id=o.id "
        "  AND s.name='status' ORDER BY s.confidence DESC, s.observed_at DESC LIMIT 1)"
        "  = 'open' "
        "GROUP BY p.canonical ORDER BY count(*) DESC LIMIT 20")]
    # The per-project GROUP BY above INNER JOINs in_repo, so it structurally cannot file a
    # thread with no project at all: a fresh agent's very first fleet view used to drop
    # them with zero disclosure. Declared, not compensated: there is no "project" to
    # attribute an unfiled thread to.
    fleet_map_unfiled = await pool.fetchval(
        "SELECT count(*) FROM objects o WHERE o.type='Thread' AND o.status='active' "
        "AND (SELECT s.value #>> '{}' FROM current_assertions s WHERE s.object_id=o.id "
        "  AND s.name='status' ORDER BY s.confidence DESC, s.observed_at DESC LIMIT 1)"
        "  = 'open' "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id AND l.type='in_repo' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()))")
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
        "note": "unmounted: a bounded fleet map, never the full firehose of data. "
                "mount(cwd, job_dir=...) then orient() for your project's briefing; "
                "orient(project=...) peeks at another project's; run_composition('briefing') "
                "if you truly want the whole graph. fleet_map_unfiled: open threads with no "
                "in_repo edge at all, counted nowhere in fleet_map above, because there is "
                "no project to file them under.",
    }
    # This branch's top-level note is asserted unconditionally by a pre-existing test
    # (test_unmounted_orient_is_a_bounded_map_never_the_firehose), so it restores the
    # tested contract rather than re-litigating it here, same approach as
    # co_agents/succession_note above. Nothing left here is terse-safe to strip; `verbose`
    # stays accepted for symmetry with the scoped branch and any future addition.
    return result


@mcp.tool()
async def fleet_digest(hours: int | None = None, mark_seen: bool = False) -> dict[str, Any]:
    """The operator's window into the autonomous fleet. Surfaces roster and health
    (which identities resolved cleanly), activity (what agents decided or opened in
    your name, not backfilled by a background process), a danger map (model swaps, the
    harness's silent demotions), laundering flags (where a relayed fact carried more
    confidence than its origin warranted), spend (metered honestly), and
    obligation_pressure (per-project open-thread count against a fixed target, osiris
    itself under 40, every client under 15, naming the three oldest owners).

    `hours` given: an ad-hoc rolling window. `hours=None` (default): watermark mode,
    'what's new since I last looked', from the stored operator watermark (24h fallback
    the first time). Reading is a peek: it never moves the watermark. Pass
    `mark_seen=True` when done reading to advance it to now."""
    pool = await _pool_get()
    since = (datetime.now(UTC) - timedelta(hours=hours)) if hours is not None else None
    dg = await digest.fleet_digest(Actions(pool), since=since, mark_seen=mark_seen,
                                   lease_secs=get_settings().osiris_mail_lease_secs)
    # A console can render the roster as a table with plenty of room; a caller with a limited
    # context window cannot, and the roster array is a superset of `danger`, so shipping both
    # sent every flagged agent twice. The counts stay whole; the rows live behind fleet().
    dg.pop("roster", None)
    dg["roster"] = "counts only, fleet() for the live roster, fleet(full=True) for all of it"
    return {"window_hours": hours, **dg}


@mcp.tool()
async def fleet(full: bool = False) -> dict[str, Any]:
    """The roster, grouped by project: live agents expanded, retired sessions collapsed
    into a counted line. ● live / ○ historical. `full=True` expands everything and shows
    the flat `registered` rows too (default: live only, history is 1000+ rows). `seat`
    rides beside a canonical id wherever one is claimed.

    Read-only diagnostics, each best-effort: `os_bodies`/`ghost_gap` (per-identity
    false_live/false_dead: a real OS process with no live graph row, or vice versa);
    `whisper_health` (recent hook-alarm failures, a log read not an active probe);
    `harness_registry` (occupancy and identity combined, no second call needed);
    `landing_audit` (unmerged branches, git-vs-graph landing disagreements); `pool_health`
    (database backend counts per daemon, cumulative `tx_total`, `caps` for the connection
    envelope); `agent_classes` (a breakdown of `count` into named_souls/visit_families/
    unresolved_families, so a raw row total is never mistaken for a headcount).
    Project grouping normalizes through `merged_into`. Field detail: consult_canon('fleet')."""
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
        # A signed retirement record, written only by retire()'s own action, and the only
        # thing that earns the word "retired". Only a small fraction of root agents ever
        # managed it; the tree used to award it to anything that simply stopped talking.
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='retired' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS retired, "
        # a spawn the harness announced but nothing ever witnessed (no transcript, no act):
        # internal machinery such as the compaction summarizer, never a seat
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='spawn_witnessed' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS witnessed, "
        # the claimed seat: the same handle/generation pair every other seat
        # reader (claim_name, seat_bearings, agent_seat) uses; None for an anonymous agent
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='handle' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS handle, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='seat_generation' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS seat_gen, "
        # the binding: the Seat object this agent actively holds, the declared identity
        # beside the claimed name, rendered as anchor-seat:<id> in the tree
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
        # The single source of truth: agent_mounts.last_seen alone, the same
        # decision agent_liveness()'s listener probe makes. `last_active` (the
        # one-time transcript stamp) is fetched above for display only now, never for
        # this verdict; see freshest_liveness_ts's own docstring for why.
        return mounts.freshest_liveness_ts(r["mount_seen"])

    nodes: dict[str, dict[str, Any]] = {}
    ghosts = 0
    for r in rows:
        if r["witnessed"] == "false":
            # announced-never-witnessed harness ephemera: they are in the record (the graph
            # forgets nothing) but they are not part of the fleet. Rendering them as live
            # seats previously put dozens of phantoms in the tree in one night. Counted,
            # never shown.
            ghosts += 1
            continue
        ts = _ts(r)
        nodes[str(r["canonical"])] = {
            "model": r["model"], "project": r["project"], "parent": r["parent"],
            "depth": int(r["depth"]) if r["depth"] else 0,
            "last_active": r["last_active"], "ts": ts,
            "retired": r["retired"] in ("true", "True"),  # signed, not merely silent
            "live": mounts.is_live(ts, now=now),
            "seat": seat_label(str(r["canonical"]), r["handle"],
                               int(r["seat_gen"]) if r["seat_gen"] else None),
            "bound": r["bound_seat"],
            "cwd": r["cwd"],
            "job_dir": r["job_dir"],
        }
    # Project label normalization through merged_into: a project's raw `current_assertions`
    # label can name a SoftwareProject that has since been folded into another. Grouping on
    # the raw label would render the dead label's own group forever. Resolve each distinct
    # raw label once (fleet() can carry 500+ agent rows; a per-row call would be wasteful)
    # through the same fold-aware primitive settle.py/agents.py/project_identity_evidence
    # already share. Best-effort, same fail-open shape as os_bodies/ghost_gap beside it: a
    # normalize failure degrades to the raw label, never breaks fleet().
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
    # Resolve each session's real graph project: writes `resolved_project` onto every node,
    # an active SoftwareProject's own label, a Worktree's parent, project_of's own
    # charter/lineage fallback, or None (unfiled), the key fleetview.render_fleet_tree groups
    # by. Best-effort, same fail-open law as every other probe in this function: a resolution
    # failure leaves `resolved_project` unset on every node, and the render falls back to
    # grouping on the raw label (today's behavior) rather than breaking fleet() outright.
    try:
        from src.orchestrator.agents import resolve_fleet_projects

        await resolve_fleet_projects(pool, nodes)
    except Exception:  # noqa: BLE001
        pass
    # Land on counts, walk in: the roster's history is 1000+ rows and never what you came for.
    # The flat rows are the live ones (or everything, if you deliberately asked); the counts
    # below are always over the whole fleet, so nothing here undercounts, it only under-shows.
    shown = {c: n for c, n in nodes.items() if full or n["live"]}
    # The ghost gap: OS truth beside the graph's belief, additive only. `live` above is
    # unchanged, still exactly what it always was (the wake trigger reads
    # agent_mounts.last_seen directly and never this dict; nothing here touches that).
    # `census.live_bodies()` is a pure OS read (pgrep -x claude + /proc), independent of
    # the mount registry; where the graph counts more live agents in a project than any real
    # process backs, that project is carrying a ghost (a closed tab mid-decay) or a phantom
    # mount (registered, never backed by an actual session), invisible to any ping-window,
    # visible the instant this is asked. Best-effort: an OS read that fails never breaks fleet().
    # Non-blocking: measured live, roughly tens of milliseconds per call on this fleet
    # (pgrep -x claude + a /proc read per candidate pid), synchronous inside this
    # async function, so that's time the shared event loop cannot serve any other
    # concurrent tool call or request. asyncio.to_thread costs one thread-pool hop,
    # negligible next to the OS read itself.
    try:
        raw_bodies = await asyncio.to_thread(census.live_bodies)
        os_bodies = {p: len(pids) for p, pids in raw_bodies.items()}
    except Exception:  # noqa: BLE001
        os_bodies = {}
    # Per-identity, not netted: a per-project subtraction (live_count - body_count) reads as
    # "no gap" whenever a false-live row and a false-dead body happen to cancel. One project
    # was measured showing "1 live, 3 bodies" as clean while carrying both problems at once
    # (a ghost mount with no real process, and real processes the graph never recognized as
    # live, due to an anchor-lookup gap). `live_bodies_by_cwd()` is cwd-grained (unlike
    # `os_bodies` above, which stays project-grained for its existing consumers/tree render);
    # matching each live node's own `agent_mounts.cwd` against it catches both directions
    # with no netting to cancel through.
    # Non-blocking: its own separate pgrep+/proc scan, same reasoning
    # as os_bodies above, a second synchronous OS read in the same request otherwise.
    try:
        bodies_by_cwd = await asyncio.to_thread(census.live_bodies_by_cwd) or {}
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
    # false_dead candidates are precisely the OS-live cwds no mount row's own cwd already
    # covers, computed once, shared by the correlation pass below and the filing loop.
    false_dead_cwds = {cwd: pids for cwd, pids in bodies_by_cwd.items() if cwd not in live_cwds}
    # The ghost-gap double-count fix: switching into a worktree moves a session's real OS
    # cwd into a worktree subdirectory while agent_mounts.cwd stays stale at the
    # pre-worktree repo root. The same live session then gets filed twice: false_live
    # under its stale mount cwd's project, and false_dead under the worktree cwd's own "?"
    # bucket, because the two loops below only ever matched on exact resolved-cwd string
    # equality. Correlate before filing, never after (a read-side fix here, not a write-side
    # re-mount hook at every worktree-switch call site): CLAUDE_JOB_DIR is fixed for an OS
    # process's entire life (trigger.py's own launch_seat docstring), the one identity
    # anchor a cwd move cannot touch. A false_live node whose own `job_dir` matches a
    # false_dead candidate's job_dir is one session, not two: suppress both sides of that
    # pair rather than file either.
    try:
        candidate_pids = [pid for pids in false_dead_cwds.values() for pid in pids]
        job_dir_by_pid = await asyncio.to_thread(census.job_dirs_for_pids, candidate_pids)
    except Exception:  # noqa: BLE001
        job_dir_by_pid = {}
    correlated_cwds: set[str] = set()
    ghost_gap: dict[str, dict[str, list[Any]]] = {}
    for canonical, n in nodes.items():
        if not n["live"]:
            continue
        if _resolved(n["cwd"]) in bodies_by_cwd:
            continue
        node_job_dir = n.get("job_dir")
        correlated = False
        if node_job_dir:
            for cwd, pids in false_dead_cwds.items():
                if any(job_dir_by_pid.get(pid) == node_job_dir for pid in pids):
                    correlated_cwds.add(cwd)
                    correlated = True
        if correlated:
            continue
        proj = n["project"] or "?"
        ghost_gap.setdefault(proj, {"false_live": [], "false_dead": []})
        ghost_gap[proj]["false_live"].append(canonical)
    for cwd, pids in false_dead_cwds.items():
        if cwd in correlated_cwds:
            continue
        proj = None
        for n in nodes.values():
            if _resolved(n["cwd"]) == cwd:
                proj = n["project"]
                break
        proj = proj or "?"
        ghost_gap.setdefault(proj, {"false_live": [], "false_dead": []})
        ghost_gap[proj]["false_dead"].append({"cwd": cwd, "pids": pids})
    # The registry fold: registry_census's own harness-vs-mount-registry view, additive,
    # reusing bodies_by_cwd/live_cwds/_resolved already computed above for ghost_gap: no
    # new OS read. Purely additive key; never touches os_bodies/ghost_gap or the row-fetch
    # SQL above it.
    try:
        from src.orchestrator.mounts import registry_census as _registry_census
        census_report = await _registry_census(pool)
    except Exception:  # noqa: BLE001, same fail-open law as os_bodies/whisper_health
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
    # The landing audit, read-only glance: `osiris deploy` mints the durable obligations
    # (deploy_guard.landing_audit); this is just the at-a-glance count so a coordinator sees
    # it here too, without a second call or waiting for orient's open-obligations list. Same
    # fail-open law as os_bodies/harness_registry beside it.
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
    # Whisper health: recent session-end/precompact/stophook alarm counts, read off the
    # same blind-spot channel every other unverifiable-from-here gap uses; a session
    # mounting via fleet() sees at a glance whether the startup path it just went through
    # has been failing. Best-effort, same fail-open shape as os_bodies: a probe failure
    # here must never break fleet() itself.
    try:
        from src.orchestrator.smoke import whisper_health as _whisper_health
        whisper = await _whisper_health(pool)
    except Exception:  # noqa: BLE001
        whisper = {"ok": True, "error": "whisper_health probe unavailable"}
    # Per-daemon pool surface: pg_stat_activity grouped by the application_name each
    # bounded daemon pool now tags itself with, same best-effort shape as
    # whisper_health/os_bodies beside it.
    try:
        from src.orchestrator.pool_health import pg_activity_by_app
        pool_health = await pg_activity_by_app(pool)
    except Exception:  # noqa: BLE001
        pool_health = {"by_application": {}, "backends": None, "tx_total": {}}
    # Cross-channel adoption: per-live-seat osiris-vs-harness traffic share. One project
    # measured 3 osiris sends against roughly 24 harness-socket (SendMessage) sends during
    # a routing defect, with 90% of that day's reasoning invisible to this graph.
    # `harness_count: None` (never a false zero) whenever the seat's current session was
    # never stored and recovered (`recover_harness_exchanges` is the write side; this
    # only reads what already landed): "not recovered" and "recovered, zero harness
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
    except Exception:  # noqa: BLE001, best-effort, same fail-open law as every probe here
        pass
    # Each live node's own context_pct, the same batched-by-canonical query _co_agents
    # already runs for the mount/orient briefing (winning_props's own confidence DESC,
    # observed_at DESC per agent), never a second copy of that shape. Best-effort, same
    # fail-open law as every other probe here.
    context_pct: dict[str, int] = {}
    try:
        live_canonicals = [c for c, n in nodes.items() if n["live"]]
        if live_canonicals:
            pct_rows = await pool.fetch(
                "SELECT DISTINCT ON (o.canonical) o.canonical AS agent_id, "
                "a.value #>> '{}' AS pct "
                "FROM current_assertions a JOIN objects o ON o.id = a.object_id "
                "WHERE o.canonical = ANY($1::text[]) AND a.name = 'context_pct' "
                "ORDER BY o.canonical, a.confidence DESC, a.observed_at DESC",
                live_canonicals)
            context_pct = {r["agent_id"]: int(r["pct"]) for r in pct_rows if r["pct"] is not None}
    except Exception:  # noqa: BLE001
        pass
    # Each live node's own stamped harness (mount()'s own write; see _infer_harness),
    # same batched-by-canonical shape as context_pct just above, never a second query
    # pattern. A session carrying no stamp (mounted before this was added) shows the
    # process's own resolved adapter, explicitly marked as the fallback rather than passed
    # off as observed: `render_fleet_tree` reads that distinction off the
    # `(caps, is_default)` tuple this dict holds, never re-deriving it.
    harness_caps: dict[str, tuple[str, bool]] = {}
    try:
        from src.orchestrator.harness_process import _ADAPTER_CLASSES, resolve_process_adapter

        live_canonicals = [c for c, n in nodes.items() if n["live"]]
        if live_canonicals:
            harness_rows = await pool.fetch(
                "SELECT DISTINCT ON (o.canonical) o.canonical AS agent_id, "
                "a.value #>> '{}' AS harness "
                "FROM current_assertions a JOIN objects o ON o.id = a.object_id "
                "WHERE o.canonical = ANY($1::text[]) AND a.name = 'harness' "
                "ORDER BY o.canonical, a.confidence DESC, a.observed_at DESC",
                live_canonicals)
            stamped = {r["agent_id"]: r["harness"] for r in harness_rows if r["harness"]}
            box_default = resolve_process_adapter().name
            for canon in live_canonicals:
                name = stamped.get(canon, box_default)
                adapter_cls = _ADAPTER_CLASSES.get(name)
                caps = sorted(adapter_cls().capabilities()) if adapter_cls else []
                harness_caps[canon] = (" ".join(caps) or "none", canon not in stamped)
    except Exception:  # noqa: BLE001
        pass
    # `count` above is every active Agent row, including brief one-off contacts that never
    # became a real registered agent, the exact fiction this read-side classification
    # exists to stop each headline re-inventing. `agent_classes` is vitals.py's one
    # authority (shared with its fold_census counterpart, so the two never drift), additive
    # beside `count` rather than replacing it: an existing reader of the raw row total
    # keeps working unchanged. Best-effort, same fail-open shape as os_bodies/ghost_gap/
    # harness_caps above: a probe failure here must never break fleet() outright.
    agent_classes: dict[str, int] | None = None
    try:
        from src.orchestrator.vitals import agent_class_counts

        agent_classes = await agent_class_counts(pool)
    except Exception:  # noqa: BLE001
        pass
    return {
        "connected_now": len(_agents),
        "count": len(nodes),
        **({"agent_classes": agent_classes} if agent_classes is not None else {}),
        **({"ghosts": ghosts} if ghosts else {}),
        "live": sum(1 for n in nodes.values() if n["live"]),
        "swarm": sum(1 for n in nodes.values() if n["parent"]),
        "os_bodies": os_bodies,
        **({"ghost_gap": ghost_gap} if ghost_gap else {}),
        "whisper_health": whisper,
        "harness_registry": harness_registry,
        "landing_audit": landing_audit,
        "pool_health": pool_health,
        # Occupancy: every active Seat, vacant ones included. The agent tree above is
        # rooted at Agent objects, so a seat with no holder at all (an office scaffolded,
        # never sat in) never appears in it at all.
        "seats": [{"seat": s["seat_id"], "handle": s["handle"], "house": s["house"],
                   "state": s["state"], "holder": s["holder"]} for s in seats],
        "tree": render_fleet_tree(nodes, full=full, os_bodies=os_bodies, ghost_gap=ghost_gap,
                                  context_pct=context_pct or None,
                                  harness_caps=harness_caps or None),
        "registered": [
            {"agent": c, "model": n["model"], "project": n["project"], "depth": n["depth"],
             "parent": n["parent"], "live": n["live"], "retired": n["retired"],
             "last_seen": n["ts"].isoformat() if n["ts"] else None,
             **({"seat": n["seat"]} if n["seat"] else {}),
             **({"bound": n["bound"]} if n.get("bound") else {}),
             **({"adoption": n["adoption"]} if n.get("adoption") else {}),
             # the CLI's own client-side render: only present when `resolve_fleet_projects`
             # actually ran, same optional-key shape as seat/bound/adoption above, so a
             # resolution failure upstream (fail-open, same law as every other probe)
             # degrades this row exactly the way fleetview's own grouping degrades: fall
             # back to the raw `project` label.
             **({"resolved_project": n["resolved_project"]}
                if "resolved_project" in n else {})}
            for c, n in shown.items()
        ],
        **({} if full else {"registered_scope": f"live only, {len(nodes)} total, "
                            f"fleet(full=True) for the rest"}),
    }


@mcp.tool()
async def registry_census() -> dict[str, Any]:
    """Cross-checks the harness's own live-session list (`claude agents --json`) against
    `/proc` (confirming each pid is really a running session) and reconciles the result
    against `agent_mounts`. `matched` are sessions with a matching database row;
    `rowless` are verified-live sessions with no row at all. `blind: true` means the
    harness read itself failed (this means the census could not run, not that nothing
    is live).

    This answers "is a session running", not "which agent lineage holds a seat". Use
    the graph (roster()/doors()) for that; conflating the two questions is a known
    source of bugs."""
    from src.orchestrator.mounts import registry_census as _registry_census
    return await _registry_census(await _pool_get())


@mcp.tool()
async def roster(repo: str | None = None, want_caveats: bool = False,
                 render: str | None = None) -> dict[str, Any]:
    """Which seat owns a repo, and is anybody home, read from the graph, never from
    `ls` on disk.

    `repo=None` returns every active seat: `occupancy` (vacant/occupied/cold, meaning
    held but nobody live right now), `chartered_repos` (governs links), `pin` (a live
    read of the seat's .osiris file, declared/unset/unreadable), and `anchor_cwd`/
    `tree_cwd`/`live_cwd` kept separate, since a live holder's mount cwd can differ from
    both with nothing wrong. `pin.triage_bucket` reuses `triage`'s own bucket, or
    "no-such-project" when the pin names something unreal.

    `repo=<name>` answers "who owns this": a seat matches if its charter or pin names
    the repo. Two matches is `governed` when the charter-seat manages the pin-seat
    (normal), else `conflict`, never silently picked. Zero matches is `no-match` (not a
    claim of no owner), paired with `near_misses`.

    Neither `chartered_repos` nor `pin` is certified canonical. This function's own
    known blind spots sit behind `want_caveats=True`; default is a one-line pointer.
    consult_canon('roster') for more.

    `render='text'` returns only {"text": <str>}. `repo=None` renders one line per
    seat, grouped by house, with an occupancy glyph (`textrender.render_roster_text`);
    `repo=<name>` falls back to the generic line-per-field renderer (already a small
    flat result, no custom shape needed)."""
    pool = await _pool_get()
    from src.orchestrator.seats import roster as _roster
    result = await _roster(pool, repo=repo, want_caveats=want_caveats)
    if render == "text":
        if repo is None:
            from src.orchestrator.textrender import render_roster_text
            return {"text": render_roster_text(result.get("seats", []))}
        from src.orchestrator.textrender import render_status_text
        return {"text": render_status_text(result)}
    return result


@mcp.tool()
async def backlog(all_projects: bool = False, fleet: bool = False, render: str | None = None,
                  ctx: Context | None = None) -> dict[str, Any]:
    """Reports open-obligation pressure per project (digest.py's `_obligation_pressure`),
    as a standalone read instead of only living inside fleet_digest's fuller payload. Per
    project: `open` count against its `target` (osiris 40, every client 15, `(unfiled)`
    for untargeted items), `past_window` (how many are already stale), `oldest_owners`
    (up to 3).

    Scoped by default to your own mounted project's row only. `all_projects=True` (or
    calling unmounted, or as the operator) widens to every project. Ordering: your own
    project's row first (when in scope), then any row with `past_window > 0`, then by
    `open` descending.

    `fleet=True`: per-seat, not per-project (`by_seat`/`unowned`/`literal_owner`/
    `fleet_total`); wins over `all_projects`.

    `render='text'`: {"text": <str>} only, capped with a remainder count."""
    pool = await _pool_get()
    if fleet:
        from src.orchestrator.compositions import _fn_obligation_backlog

        result: dict[str, Any] = await _fn_obligation_backlog(pool, None, {})
        if render == "text":
            from src.orchestrator.textrender import render_obligation_backlog_text
            return {"text": render_obligation_backlog_text(result)}
        return result
    ident = await _ident_for(ctx)
    from src.orchestrator import digest as _digest
    from src.orchestrator.textrender import render_backlog_text

    rows = await _digest._obligation_pressure(Actions(pool))
    mine = ident.project if ident and ident.project not in (None, OPERATOR_ADDR) else None
    if mine and not all_projects:
        rows = [r for r in rows if r["project"] == mine]

    def _sort_key(r: dict[str, Any]) -> tuple[int, int, int, str]:
        return (0 if r["project"] == mine else 1,
                0 if r["past_window"] else 1, -r["open"], r["project"])
    rows = sorted(rows, key=_sort_key)
    if render == "text":
        return {"text": render_backlog_text(rows)}
    return {"projects": rows, "scope": "all" if (all_projects or not mine) else mine}


@mcp.tool()
async def threads(project: str | None = None, render: str | None = None,
                  ctx: Context | None = None) -> dict[str, Any]:
    """Every open thread you own, one line each with a short id, so a slash command can
    hand one straight to thread(action=...)/recall(ref=...) without a separate lookup.
    "You" matches every spelling an obligation can be owned under (owner_refs: your
    agent id, lineage root, seat id, seat handle, the same matching
    `owned_obligations`'s own statusline `owe` cell uses), never just your literal
    agent id.

    `project` defaults to your mounted project. Deliberately single-project, not
    charter-widened like get_object_list: this is "mine, in front of me right now";
    call again with an explicit `project` for another repo you govern.

    `render='text'`: returns only {"text": <str>}, one line per thread, capped at
    `textrender.THREADS_BAND_CAP` with a remainder count, plain text, server-rendered.

    `contested`: present and `True` when a newer note has disputed this summary and
    nobody has corrected it yet, marked with a leading `!` in both the JSON row and the
    text render.

    `project_owned_not_shown`: obligations here whose owner is this project's own bare
    name or empty, never yours by any spelling; present only when >0, and the text
    render's own trailing line names the same count."""
    pool = await _pool_get()
    ident = await _ident_for(ctx)
    proj = project or (ident.project if ident else None)
    if ident is None or proj is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first, or pass project=<repo>"}
    from src.orchestrator.capture import CONTESTED_SQL, _resolve_repo
    proj_id = await _resolve_repo(pool, proj)
    if proj_id is None:
        return {"error": f"no project {proj!r}", "threads": []}
    from src.orchestrator.stophook_logic import owner_refs, project_owned_obligation_count
    from src.orchestrator.textrender import render_threads_text

    owners = await owner_refs(pool, ident.agent_id)
    project_owned = await project_owned_obligation_count(pool, proj_id, owners)
    rows = await pool.fetch(
        "SELECT o.id, "
        "  COALESCE("
        "    (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "     AND a.name='corrected_summary' ORDER BY a.confidence DESC, a.observed_at DESC "
        "     LIMIT 1), "
        "    (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "     AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)) "
        "    AS summary, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='kind' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind, "
        f"  {CONTESTED_SQL} AS contested "
        "FROM objects o "
        "JOIN links l ON l.from_id=o.id AND l.type='in_repo' AND l.to_id=$1 "
        "  AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "WHERE o.type='Thread' AND o.status='active' AND COALESCE("
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),"
        "  'open')='open' "
        "  AND lower(COALESCE("
        "    (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "     AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),"
        "    '')) = ANY($2::text[]) "
        "ORDER BY o.created_at ASC",
        proj_id, owners)
    mine = [{"id": str(r["id"])[:8], "summary": r["summary"], "kind": r["kind"],
             **({"contested": True} if r["contested"] else {})}
           for r in rows]
    if render == "text":
        return {"text": render_threads_text(mine, project_owned)}
    return {"project": proj, "threads": mine, "total": len(mine),
           **({"project_owned_not_shown": project_owned} if project_owned else {})}


@mcp.tool()
async def team(render: str | None = None, ctx: Context | None = None) -> dict[str, Any]:
    """A manager's own seats: every seat `managed_by` your own held seat, each carrying:
    `live` (a session has mounted within the fleet's own live window right now),
    `owe`/`stale` (open obligations owned by that seat's handle, and how many are past
    their stale_after window, the same definition `owned_obligations`'s own statusline
    `owe` cell uses), `envelope` (that seat's current holder's own unread ask count,
    mail asking something of them specifically; 0 for a cold/vacant seat with nobody to
    ask). Refuses cleanly if you hold no seat, or your seat manages nobody
    (`fleet(full=True)` is the wider, unscoped roster for that case).

    `render='text'`: returns only {"text": <str>}, one line per managed seat, plain
    text, server-rendered."""
    pool = await _pool_get()
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first"}
    from src.orchestrator.seats import held_seat, team_roster

    mine = await held_seat(pool, ident.agent_id)
    if mine is None:
        return {"error": "you hold no seat: team is a manager's own view of the seats "
                         "it manages, nothing to scope it to"}
    out_rows = await team_roster(pool, mine["seat_id"], manager_house=mine["house"])
    if not out_rows:
        return {"error": f"{mine['handle']} manages no seats"}
    if render == "text":
        from src.orchestrator.textrender import render_team_text
        return {"text": render_team_text(out_rows)}
    return {"manager": mine["handle"], "team": out_rows}


@mcp.tool()
async def tree_ledger(limit: int | None = None, offset: int = 0) -> dict[str, Any]:
    """Reports where a seat's pinned project and the graph disagree. Read-only,
    fleet-wide, two sections.

    `project_ledger`: every active SoftwareProject, each carrying `phantom_verdict`:
    test-fixture | declared (a seat pin or Seat-origin governs edge claims it) |
    phantom-suspect (name matches a generic path-segment list, nothing declares it) |
    undetermined (a real disagreement, no confident call). `limit`/`offset` page it
    (default 200/0, capped 2000).

    `live_cwd_ledger`: today's agent_mounts only. Each cwd: `directory_exists`
    (checked before the pin is trusted), `resolved_today` vs `graph_believes`, and an
    `agreement` verdict: no-graph-yet / ghost / graph-only / match / partial-match /
    mismatch.

    `caveats` names what this instrument cannot see. Read-only: reports disagreements,
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
    """Message the fleet. `to`=<project> is a broadcast, the group chat ('operator' reaches
    the human's desk); `to_agent`=<agent:id> is a private direct message (ids from
    orient()/fleet). `to` refuses a project nobody has mounted under rather than filing
    mail nobody will read; it also refuses when `body` opens with a real seat's name
    (e.g. 'name - ...') or @handle whose holder sits in a different project than `to`,
    so you name the right `to_agent` instead of silently delivering to the wrong room.
    `addressee_resolved` in the receipt names what it found, agreeing or not.
    `reply_to=<id>` answers a message (routes by channel, joins the thread) and settles it.
    At-least-once, deduped. For durable knowledge use record_decision/open_thread instead.

    `desk` triages an operator brief: 'decision' | 'hands' | 'fyi'. `grade` triages
    project mail: 'ask' (named in the recipient's unread count) | 'fyi' (an ack settles
    it); ungraded is never guessed. `dispatch` in the receipt names what happened on
    delivery: queued/poked/resumed/woke, or a brake mode naming why nobody was reached.
    A direct message's receipt echoes `dm_to`/`seat`/`lineage_head`; compare against a
    stale address before trusting "sent". `require_seat=True` refuses on an unclaimed
    target. `threads` transfers ownership of existing thread(s) to a direct message's
    addressee in the same act (exact ref only, never inferred from `body`);
    `threads_stamped` names what moved. A direct message or graded 'ask' runs the same
    prior-art search record_decision does, surfaced on both your receipt and the
    delivered message.

    `want_prior_art`/`want_listener` return the full prior_art list and listener block;
    default is a one-line `prior_art_flag` only."""
    ident = await _ident_for(ctx, session_anchor)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first: a message must say who "
                         "it's from (the anchor re-attaches you automatically after a bounce)",
                "why": _anchorless(ctx)}
    pool = await _pool_get()
    st = get_settings()
    # a spawn's mail goes out under its own name (the hook-stamped sidechain identity),
    # from the parent's project: the fleet must never mistake a child's word for the seat's
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    # The read-side prior-art lookup runs before send_message, not after: since every
    # sent message got graphed as its own searchable Message object, searching after the
    # write let this call's own just-written body, a verbatim, single-field, perfect
    # self-match, satisfy search()'s strict-AND lexical check trivially, which
    # short-circuits the OR-relaxation ladder that is the actual mechanism finding a
    # differently-worded standing decision (record_decision's own prior-art call
    # structurally avoids this because its query spans summary+rationale while the graph
    # stores them as separate single-field assertions, so no candidate row ever contains
    # the literal union, hence no accidental self-match). A message can never be its own
    # prior art by definition: searching the graph as it stood before this write is both
    # the fix and the more honest semantics.
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
        **({"dedup": "identical recent message already queued, not re-posted"}
           if res["dedup"] else {}),
        **({"threads_stamped": res["threads_stamped"]} if res.get("threads_stamped") else {}),
        # The honest result: the relational row always lands; the graph
        # edge write (Message object + sent_by/addressed_to/broadcast_to/replies_to) is
        # best-effort beside it and can fail on its own: `graphed: False` says so plainly
        # rather than let mail look graph-traversable when this one didn't make it.
        **({"graphed": False, "note": "relational send succeeded; the graph edge write "
                                      "failed, so this message won't show up in search()/"
                                      "prior-art/orient() until a later repair recovers it"}
           if res.get("graphed") is False else {}),
        # The addressing guard: a leading vocative or @handle in `body` that resolved
        # through binding_of_handle's own authoritative Seat check, named here whether it
        # agreed with the addressed room or (see the ValueError path above, which never
        # reaches this result at all) disagreed with it.
        **({"addressee_resolved": res["addressee_resolved"]}
           if res.get("addressee_resolved") else {}),
    }
    if res["to_agent"]:  # a DM: report the addressee, its seat + lineage head, and its liveness
        out["dm_to"] = res["to_agent"]
        out["seat"] = res.get("seat")
        out["lineage_head"] = res.get("lineage_head")
        # The result invariant: `listener` reads the delivering head's
        # liveness. agent_liveness(lineage_head or dm_to) is lineage-aware internally, but
        # passing lineage_head explicitly when it resolved keeps this result's every field
        # sourced from the same identity `seat` already is, never a mix of the addressed id
        # and the head. `redirect` (mailbox.send_message's own field), when present,
        # names the divergence explicitly instead of leaving it to be inferred by comparing
        # `dm_to` against `seat`/`lineage_head` by hand.
        if want_listener:
            out["listener"] = await mounts.agent_liveness(
                pool, res.get("lineage_head") or res["to_agent"])
        if res.get("redirect"):
            out["redirect"] = res["redirect"]
        # The immediate leg (the background-session adapter): a DM's wake
        # fires on arrival, never on a clock. This very call dispatches it, and the result
        # below is the per-hop truth (resumed / mid-turn / queued-* / pull-only), not a
        # guess about what some future sweep might do. The worker tick stays as the backstop
        # that drains gated mail. A dispatch failure must never fail the send: the message
        # is already committed, the sweep will retry, and the result says so honestly.
        if not res["dedup"]:
            try:
                from src.orchestrator.trigger import dispatch_dm
                out["dispatch"] = await dispatch_dm(
                    pool, addressee=res["to_agent"], msg_id=res["id"], sender=actor)
            except Exception as exc:  # noqa: BLE001, the send already committed; confess
                out["dispatch"] = {"mode": "deferred",
                                   "detail": f"immediate dispatch failed ({exc}), the "
                                             "worker sweep is the backstop"}
        if await pool.fetchval(
                "SELECT 1 FROM current_assertions a JOIN objects o ON o.id=a.object_id "
                "WHERE o.canonical=$1 AND a.name='is_sidechain' "
                "AND a.value #>> '{}' = 'true' LIMIT 1", res["to_agent"]):
            # the dead-letter class: an ephemeral spawn has no session to resume and no
            # UI to nag: a DM to it may never be read or settled
            out["warning"] = ("the addressee is an ephemeral spawn, it cannot be woken and "
                              "may never read this; if the work is for its lineage, message "
                              "the parent seat instead (see the spawn's spawned_by link)")
    else:  # a broadcast: the project channel, who's live, is anyone actually being woken
        dest = res["to"]
        last_seen = await mounts.project_last_seen(pool, dest)
        out["to"] = dest
        if want_listener:
            out["listener"] = {"live": bool(last_seen and datetime.now(UTC)
                               - datetime.fromisoformat(last_seen) < timedelta(minutes=15)),
                               "last_seen": last_seen}
        # The immediate leg, extended from the DM lane to broadcasts: a broadcast used to
        # file and return a bare "sent". A caller reasonably read that as delivered when it
        # meant filed, and the only push was the worker sweep, up to roughly 60s later, none
        # at all under poke-only with no open window. dispatch_broadcast fires on arrival
        # now, same as a DM; the worker tick stays the backstop. A dispatch failure must
        # never fail the send: the message is already committed, the sweep retries, and the
        # result says so honestly.
        if not res["dedup"]:
            try:
                from src.orchestrator.trigger import dispatch_broadcast
                out["dispatch"] = await dispatch_broadcast(
                    pool, project=dest, msg_id=res["id"], sender=actor)
            except Exception as exc:  # noqa: BLE001, the send already committed; confess
                out["dispatch"] = {"mode": "deferred",
                                   "detail": f"immediate dispatch failed ({exc}), the "
                                             "worker sweep is the backstop"}
        out["backlog"] = await mailbox.project_deliverable_count(
            pool, dest, lease_secs=st.osiris_mail_lease_secs)
    # The crossed-mail warning: if this thread's peer already has words waiting unread in
    # your own inbox, your note may have crossed theirs. Say so at send time, before the
    # stale answer is composed. Pull semantics untouched; this is a mirror, not a push.
    if res["thread_id"] is not None:
        crossed = await pool.fetchval(
            "SELECT count(*) FROM fleet_messages m "
            "LEFT JOIN message_recipients r ON r.message_id = m.id AND r.agent_id = $3 "
            "WHERE m.thread_id = $1 AND m.id <> $2 AND m.from_agent <> $3 "
            "AND (m.to_agent = $3 OR (m.to_project = $4 AND m.to_agent IS NULL)) "
            "AND m.read_at IS NULL AND r.read_at IS NULL",
            res["thread_id"], res["id"], actor, ident.project)
        if crossed:
            out["crossed"] = (f"{crossed} unread message(s) in this thread are already "
                              "waiting in your inbox, so your note may have crossed theirs. "
                              "Call inbox() before assuming your view is current")
    # Attach/persist the prior-art computed above, before the write. Skipped on a dedup
    # hit (res["id"] then names an existing message that may already carry its own
    # prior_art from its original send; overwriting risks clobbering a real prior result
    # with this resend's own, possibly-empty, recomputation. Moot anyway since we never
    # searched for a dedup'd resend in the first place, but the gate stays explicit).
    if prior and not res["dedup"]:
        if want_prior_art:
            out["prior_art"] = prior
        top = prior[0]
        out["prior_art_flag"] = (
            f"{top.get('type') or 'Decision'} {top['id']} already speaks to this, "
            "worth reading before dispatching/answering as if it's new"
            + ("" if want_prior_art else " (pass want_prior_art=True for the full list)"))
        try:
            await pool.execute(
                "UPDATE fleet_messages SET prior_art=$1 WHERE id=$2", prior, res["id"])
        except Exception:  # noqa: BLE001, persistence for the reader's copy is a
                            # bonus; the send already committed and the sender's own
                            # result above already carries the hits regardless
            pass
    # The unhedged-assertion nag: measurement_smell's own sibling, mirroring its exact
    # shape (advice on the result, never a gate, the message sends either way) but
    # aimed at dispatch prose instead of decision text. The reader is the sender, this
    # same turn, before anyone downstream ever sees the message: no new storage, no new
    # consumer, the same design that let this ship without the read-lens work.
    if capture.unhedged_assertion_smell(body):
        # Keep the result lean: short code, not the full prose every firing.
        # describe('nags:assertion') for the text (catalog: _NAG_CATALOG below).
        out.setdefault("nags", []).append("assertion")
    return out


@mcp.tool()
async def wake_preflight(target: str) -> dict[str, Any]:
    """Checks wake()'s own gates before you attempt one: the compaction/ceiling/
    no-anchor/crossed-registry checks that today only reveal themselves as a refusal
    after a real wake() call. `target` accepts anything wake()'s own does: a claimed
    handle, `seat:<id>`, or `agent:<id>`.

    Returns `{mode, status, detail}`. `status` is one of: `resumable` (every gate
    clears, so a real wake() would resume this addressee now), `fresh-heir-available`
    (past its own compaction point, but not a dead end; a real wake() starts a fresh
    successor here rather than refusing), `no-live-body` (vacant, retired, or never
    mounted), or `refused-<gate>` (ceiling / no-anchor / crossed-registry / resident-
    unknown / unknown). Read-only: sends/spawns nothing."""
    pool = await _pool_get()
    from src.orchestrator.trigger import (
        _resolve_wake_address,
        _seat_for_target,
        wake_gate_preflight,
    )

    # A bare handle must resolve the same way wake() itself does: a live-fire finding
    # showed this tool's own first real run against a claimed handle silently answered
    # 'never-mounted', because _resolve_wake_address only ever understood 'seat:'/'agent:'
    # prefixes, exactly like dispatch_dm's own addressee, which always arrives
    # pre-resolved via wake_worker's _seat_for_target call before dispatch_dm ever sees it.
    # This tool has no such upstream resolver of its own, so it must run the same one
    # wake_worker does, never a second, narrower guess at what a handle means.
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
    """Contacts the other half of your own managed_by pair, never a peer. Gated on an
    active managed_by edge in either direction (you manage them, or they manage you);
    peers and cross-house calls are refused and must be routed through a manager or the
    operator instead. There is deliberately no operator override parameter; that stays
    a separate, explicit path.

    `target` accepts anything send()'s to_agent does. The message is prefixed with a
    self-identifying marker, then dispatches through send()'s own direct-message path
    with an authority gate in front. `status`: delivered (confirmed landed as a
    submitted turn, observed: true) | mid-turn (their turn is still moving) |
    no-live-body | refused-not-your-worker | refused-budget | queued (rate brake,
    pause, or unconfirmed, see `detail`)."""
    ident = await _ident_for(ctx, session_anchor)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first: a wake must say who "
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
    """Gives a seat a fresh session (wake() is for messaging a session that already
    exists). Downward-only: you may only start a session for a seat you manage. Creates
    a new session, never injects into an existing one. Default substrate is a
    harness-native `claude --bg` background session (self-binds via its own first
    turn: mount() then claim_name); an older fallback lane survives as an explicit
    option (`osiris_launch_substrate`). There is no operator override parameter; that
    stays a separate, explicit path.

    Idempotent: a live session already holding the seat is returned, never duplicated.
    `message` delivers as the opening brief when launched, or nudges the live session
    as a direct message on `already-live`; `brief_delivery` names the outcome either
    way.

    `body_exists` (session created) and `can_receive` (independently confirmed live)
    are separate: a fresh session usually returns body_exists=true, can_receive=false
    for a few seconds; `detail` says how to confirm. `status`: launched | already-live |
    manager-cold | refused-not-your-worker | refused-no-office/-no-handle |
    refused-spawn (see `detail`). `dormant_history`, when present, discloses a
    substantial pre-existing transcript at the target cwd."""
    ident = await _ident_for(ctx, session_anchor)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first: a launch must say who "
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
    """Continues a seat's own dormant session, distinct from launch() (which always
    starts fresh, never guesses); same managed_by/downward-only gate. Never falls
    through to a fresh start: if nothing resumable exists, it refuses
    (`status: refused-nothing-to-resume`) rather than resuming the wrong session;
    call launch() for that instead. One-shot: runs one turn over `-p --resume` and
    exits, re-summonable via the next mail wake.

    `status`: launched (mode: resumed) | refused-nothing-to-resume | refused-resume-
    unknown (a resumable-looking session with no verified record; the exact
    `claude -p --resume <sid>` a human can run by hand is in `detail`) |
    refused-not-your-worker. `resume_check` on every receipt names the decision (which
    generation, how many hops back)."""
    ident = await _ident_for(ctx, session_anchor)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first: a resume must say who "
                         "it's from", "why": _anchorless(ctx)}
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    from src.orchestrator.trigger import resume_seat
    return await resume_seat(Actions(await _pool_get()), caller=actor, target=target,
                             message=message, model=model)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='stop')",
    "since": "superseded by the unified seat action dispatcher",
})
async def stop(target: str | None = None, reason: str = "",
               subagent_id: str | None = None, subagent_type: str | None = None,
               session_anchor: str | None = None,
               ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated, hidden alias, still callable. Forwards to seat(action='stop')."""
    return await _seat_impl("stop", target=target, reason=reason, subagent_id=subagent_id,
                            subagent_type=subagent_type, session_anchor=session_anchor,
                            ctx=ctx)


@mcp.tool()
async def inbox(project: str | None = None, peek: bool = False,
                ack: list[int] | None = None, subagent_id: str | None = None,
                subagent_type: str | None = None, session_anchor: str | None = None,
                want_prior_art: bool = False, render: str | None = None,
                as_seat: str | None = None, include_settled: bool = True,
                ctx: Context | None = None) -> dict[str, Any]:
    """Read messages other agents left for you. Defaults to your mounted project; pass
    `project` for another's ('operator' reads the human's desk). Reading leases a
    message, it doesn't consume it: settle each one via send(reply_to=<id>) or
    ack=[ids], or it redelivers after the lease (at-least-once). `peek=True` reads
    without leasing. Check after mount()/compaction. The operator's desk is different:
    glance with peek only, settle only at the human's explicit word.

    `want_prior_art=True` returns each message's full prior_art list; default is a
    `prior_art_count` only.

    `as_seat=<seat canonical>` is a completely separate, read-only mode: it lets a
    coordinator read another seat's received direct messages, never leased, never
    settled (`ack` is refused alongside it). Charter-gated: you must govern (an active
    `governs` link) at least one project that seat also charters, or it refuses; a
    vacant target (no holder has ever existed) returns an empty list, not an error.
    `include_settled=True` (default) also surfaces mail the target already dealt with,
    `settled` marked per row, for auditing what happened rather than queuing new work
    for yourself; `False` narrows to only what the target never read. Broadcasts are
    deliberately excluded (already visible to anyone in that project's own inbox);
    direct messages are the only genuinely unsurfaceable kind. Every other parameter
    above is ignored in this mode.

    `render='text'`: returns only {"text": <str>}. Your own mailbox renders one line
    per ask message, fyi folded to a single trailing count line
    (`textrender.render_mail_text`). The operator desk renders the backlog band first
    (all-projects obligation pressure), then owed/letters, then
    needs_decision/needs_hands/fyi/dimmed/miner_guesses each as one count line (never
    itemized, since settling by id needs the ids this collapsed glance deliberately
    drops; re-call without `render` for the full structured bands first), then
    `your_queue` itemized one line per thread (`textrender.render_desk_text`)."""
    ident = await _ident_for(ctx, session_anchor)
    pool = await _pool_get()
    # Mail is otherwise unsurfaceable: as_seat switches to a completely
    # separate, read-only mode. A coordinator reading another seat's received DMs
    # (including already-settled ones, `include_settled=True` by default: the point is
    # auditing what happened, not queuing new work). Never leases, never accepts `ack`
    # (a read-only mode has nothing to settle), checked before the ordinary own-mail
    # path's own `project` requirement below, since this mode needs no mounted project
    # of the caller's own at all (it reads by seat/charter, not by project default).
    if as_seat is not None:
        if ack:
            return {"error": "as_seat is read-only: it never leases, so there is "
                             "nothing for ack to settle"}
        if ident is None or subagent_id is not None:
            return {"error": "as_seat needs your own real seat identity (mount first; "
                             "a spawn cannot read another seat's mail on your behalf)"}
        from src.orchestrator.charter import charter_of
        from src.orchestrator.mailbox import read_seat_mail
        from src.orchestrator.seats import held_seat, seat_occupancy

        caller_seat = await held_seat(pool, ident.agent_id)
        if caller_seat is None:
            return {"error": "you hold no seat: as_seat is charter-gated, and an "
                             "unbound identity governs nothing to gate against"}
        caller_projects = set(await charter_of(pool, caller_seat["seat_id"]))
        target_projects = set(await charter_of(pool, as_seat))
        if not target_projects:
            return {"error": f"{as_seat!r} charters no project, so there is nothing to "
                             "check governance against, and nothing this mode will read"}
        if not (caller_projects & target_projects):
            return {"error": f"you do not govern any project {as_seat!r} charters "
                             f"({sorted(target_projects)}). as_seat is charter-gated, "
                             "never a bare seat-to-seat read"}
        occ = await seat_occupancy(pool, as_seat)
        holder = occ["holder"]
        if holder is None:
            return {"as_seat": as_seat, "target_agent": None, "messages": [],
                    "note": "vacant, no holder has ever existed for this seat"}
        msgs = await read_seat_mail(pool, target_agent=holder,
                                    include_settled=include_settled)
        return {"as_seat": as_seat, "target_agent": holder, "messages": msgs}
    proj = project or (ident.project if ident else None)
    if proj is None:
        # This bounce previously carried no diagnostic at all, which is precisely why
        # several sessions independently filed it as "transient" and nobody chased it for a week.
        return {"error": "mount(cwd, job_dir=<your anchor>) first, or pass project=<repo>",
                "why": _anchorless(ctx)}
    st = get_settings()
    # a spawn reads over its parent's shoulder: peek only. It must never lease the seat's
    # mail (a lease a dying child holds blocks redelivery for the whole lease window) and
    # never settle it (settling is the seat's duty: a child acking mail the parent never
    # saw re-creates the exact surprise this layer exists to kill).
    from src.orchestrator.lineage import normalize_spawn_id

    spawn_reader = normalize_spawn_id(subagent_id) is not None
    if spawn_reader:
        peek, ack = True, None
    # the reader is you (your DMs + your project's broadcasts, your own lease/settle), except
    # the operator desk, whose reader is the human ('operator'): an agent only peeks it, never
    # settles it as itself.
    reader = OPERATOR_ADDR if proj == OPERATOR_ADDR else (ident.agent_id if ident else proj)
    if proj == OPERATOR_ADDR:
        # The organized desk: always peek-shaped, reading
        # the human's desk never leases; bands (needs_decision / needs_hands / fyi),
        # thread + same-story folds, dimmed moot annotations, the derived your_queue.
        # Never gated by the read-before-settle rule below: the desk settles at the human's
        # own word (the mail skill's own distinction), not an agent's session_reads.
        ack_out = await ack_messages(pool, proj, ack, reader_agent=reader) if ack else None
        ack_keys: dict[str, Any] = {}
        if ack_out is not None:
            ack_keys["settled"] = ack_out["settled"]
            if ack_out["skipped"]:
                ack_keys["skipped"] = ack_out["skipped"]
        desk = await read_desk(pool)
        out = {"project": OPERATOR_ADDR, **desk, **ack_keys}
        if render == "text":
            from src.orchestrator import digest as _digest
            from src.orchestrator.textrender import render_backlog_text, render_desk_text
            backlog_rows = await _digest._obligation_pressure(Actions(pool))
            backlog_text = render_backlog_text(sorted(
                backlog_rows, key=lambda r: (0 if r["past_window"] else 1, -r["open"])))
            return {"text": render_desk_text(out, backlog_text=backlog_text)}
        return out
    # The read-before-settle rule: captured before this call's own read below, an id
    # must have been returned in full through an earlier real inbox() call (peek or lease),
    # never this same call's own concurrent read. Without that, a bare `inbox(ack=[id])`
    # would always satisfy its own check trivially (read_inbox always runs, ack or
    # not), which is exactly the un-read-then-settle shape this rule exists to catch.
    # The documented workflow (mail skill: peek, then a separate inbox(ack=...) or
    # send(reply_to=...) call) already reads first in its own earlier call, so this
    # costs that pattern nothing. `ident is None` (unmounted) and spawn_reader (ack
    # already forced to None) skip trivially: no session_reads row to check for.
    unread_ack_ids = (
        await provenance.unread_message_ids(pool, ack, agent_id=ident.agent_id)
        if ack and ident is not None and not spawn_reader else [])
    msgs = await read_inbox(pool, proj, reader_agent=reader, mark_read=not peek,
                            lease_secs=st.osiris_mail_lease_secs)
    if not want_prior_art:
        for m in msgs:
            pa = m.pop("prior_art", None)
            if pa:
                m["prior_art_count"] = len(pa)
    flight = await in_flight(pool, proj, reader_agent=reader,
                             lease_secs=st.osiris_mail_lease_secs)
    if not peek:  # what this call just leased is ours, not someone else's in-flight
        ours = {m["id"] for m in msgs}
        flight = [f for f in flight if f["id"] not in ours]
    if spawn_reader:
        note = ("spawn read, peek forced, nothing leased or settled: the mailbox belongs "
                "to your parent's seat; report what you saw, let the seat settle it")
    elif peek:
        note = "peek, nothing leased"
    elif msgs:
        note = ("leased, settle each by replying (send(reply_to=<id>)) or acking "
                f"(inbox(ack=[ids])); unsettled mail redelivers after "
                f"{st.osiris_mail_lease_secs // 60} min")
    else:
        note = "empty"
    if flight:  # an empty box with a held lease is not 'nothing happening'
        note += (f", {len(flight)} in flight (leased by "
                 + ", ".join(sorted({f['leased_by'] for f in flight})) + ")")
    if not spawn_reader and ident is not None:
        # Provenance, first piece: each message read gets its existing Message object
        # (mailbox.py's own send() already mints one for every message; never a fresh
        # mint from the read side), None (skipped) for an operator-authored message
        # or one whose graph write never landed at send time.
        msg_oids = []
        for m in msgs:
            oid = await provenance.message_object_id(pool, m["id"], m.get("from"))
            if oid is not None:
                msg_oids.append(oid)
        await _stamp_read_ids(pool, ident, "inbox-peek" if peek else "inbox-lease", msg_oids)
    # Enforcing the read-before-settle rule: settle only the ids that were
    # already read as of before this call (unread_ack_ids, captured above). This
    # call's own fresh read (just stamped) counts toward the next call, never this one.
    ack_keys = {}
    if ack:
        ack_now = [i for i in ack if i not in unread_ack_ids]
        ack_out = await ack_messages(pool, proj, ack_now, reader_agent=reader) if ack_now else {
            "settled": [], "skipped": {}}
        for i in unread_ack_ids:
            ack_out["skipped"][i] = ("not yet read in full: call inbox() (peek or lease) "
                                     "in an earlier call before acking; the wake prompt's "
                                     "own preview does not count, and reading it in this "
                                     "same call does not retroactively satisfy its own ack")
        ack_keys["settled"] = ack_out["settled"]
        if ack_out["skipped"]:
            ack_keys["skipped"] = ack_out["skipped"]
    if render == "text":
        from src.orchestrator.textrender import render_mail_text
        text = render_mail_text(msgs)
        if ack_keys.get("settled"):
            text += f"\nsettled: {ack_keys['settled']}"
        text += f"\n{note}"
        return {"text": text}
    return {"project": proj.removeprefix("repo:").strip(), "messages": msgs,
            **({"in_flight": flight} if flight else {}),
            **ack_keys, "note": note}


@mcp.tool()
async def dismiss_brief(message_id: int, because: str,
                        ctx: Context | None = None) -> dict[str, Any]:
    """Marks an operator-desk brief as moot: annotates it moot-with-a-reason ('true when
    sent; root cause fixed in <commit>') so the desk renders it collapsed under your
    note instead of showing a stale alert. Never a settle: dismissing stays exclusively
    the human's own decision; marking something moot just saves them the digging,
    stamped with your name. Only works on briefs addressed to the operator's desk.
    Requires mount."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"why": _anchorless(ctx),
                "error": "mount(cwd, job_dir=<your anchor>) first: an annotation must say "
                         "whose testimony it is"}
    try:
        return await mailbox_dim(await _pool_get(), message_id,
                                 because=because, by=ident.agent_id)
    except ValueError as e:
        return {"error": str(e)}


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "agent(action='claim_name')",
    "since": "superseded by the unified agent action dispatcher",
})
async def claim_name(name: str, ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated, hidden alias, still callable. Forwards to
    agent(action='claim_name')."""
    return await _agent_impl("claim_name", name=name, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='charter')",
    "since": "superseded by the unified seat action dispatcher",
})
async def charter(repos: list[str] | None = None, ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated, hidden alias, still callable. Forwards to seat(action='charter')."""
    return await _seat_impl("charter", repos=repos, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='charter_for')",
    "since": "superseded by the unified seat action dispatcher",
})
async def charter_for(seat_id: str, repos: list[str], because: str,
                      ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated, hidden alias, still callable. Forwards to seat(action='charter_for')."""
    return await _seat_impl("charter_for", target=seat_id, repos=repos, because=because,
                            ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='rebind')",
    "since": "superseded by the unified seat action dispatcher",
})
async def rebind_seat(seat: str, new_cwd: str, extract: bool = False,
                      force: bool = False, because: str = "",
                      ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated, hidden alias, still callable. Forwards to seat(action='rebind')."""
    return await _seat_impl("rebind", target=seat, new_cwd=new_cwd, extract=extract,
                            force=force, because=because, ctx=ctx)


@mcp.tool()
async def merge(dupe: str, into: str, evidence: str, force: bool = False,
                because: str = "", ctx: Context | None = None) -> dict[str, Any]:
    """Declares two labels of the same type one thing: `dupe` folds into `into`. Type
    is read off `dupe`'s own form (agent:.../seat:.../else SoftwareProject). Append-
    only (nothing deleted, authorship untouched), and each type's own dependent records
    follow: Agent moves mail/mount rows/open threads to `into`'s living head; Seat also
    moves active holders and managed_by edges (an Agent merge refuses an actively-
    seated dupe instead); SoftwareProject re-points every in_repo/works_in/governs/
    informs edge and mount row.

    `evidence` required for every type. Agent merges are actor-gated: refuses any
    caller who is not the operator or the scheduled reaper; mount as the operator, or
    judge via resolve_fold(). Seat/Project merges carry no such gate. Refuses: dupe/into
    different types; thin evidence; dupe==into; unknown/already-folded labels; a
    same-lineage Agent pair (succession's job); a SoftwareProject pair contradicting on
    a non-name property.

    Liveness guard (SoftwareProject only): a different lineage's live session on `dupe`
    refuses by default; `force=True` + `because` overrides."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first: a merge must say who is performing it",
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
    """Reverses a wrongful `merge` call, and works consistently across all three
    mergeable types: previously, only an Agent merge was ever reversible; a Seat or
    Project merge was permanent. Type is read off `dupe`'s own form, same rule as
    `merge`. Dry run is the default (`execute=False`) for every type: returns the
    exact plan (the underlying unmerge, any type-specific dependent records that can
    cleanly return, and the ones that can't) without writing anything; review it, then
    call again with `execute=True`. Refuses: `dupe` not currently merged, a blank
    `because`, or a merge whose original justification cites the operator's word when
    `because` doesn't carry a fresh one (reversing an operator-approved merge needs the
    operator's word too, for every type)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first: an unmerge must say who is performing it",
                "why": _anchorless(ctx)}
    from src.orchestrator.merge import unmerge as _unmerge
    return await _unmerge(Actions(await _pool_get()), dupe=dupe, because=because,
                          actor=ident.agent_id, execute=execute)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "measured with no real usage found across recent MCP traffic or any other "
              "known call path",
    "since": "retired as part of a broader cleanup of unused tools",
})
async def reconcile_merge(dupe: str, into: str, ctx: Context | None = None) -> dict[str, Any]:
    """Accepts an already-merged `dupe` and re-points whatever mail/mount/thread/holder/
    managed_by/edge records are still aimed at it, without re-performing the merge.
    This repairs records a partial first merge left stranded, without needing to redo
    the merge itself. Unmerge-then-remerge is not a substitute: `unmerge`'s own
    `estate_unreturnable` path reports, and drops, exactly the links a partial merge
    already broke.

    Type is read off `dupe`'s own form, same rule as `merge`/`unmerge`. Refuses: `dupe`
    and `into` resolving to different types; `dupe` not merged (that's `merge`'s job);
    `dupe`'s own `merged_into` pointing at a different `into` (never redirects); `into`
    not active. The Agent branch is actor-gated exactly like `merge`'s own Agent branch
    (repairing a merge needs the same authority as making one); Seat and Project stay
    open, matching their own merge's current posture, left unreconciled on purpose."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first: a reconcile must say who is performing it",
                "why": _anchorless(ctx)}
    from src.orchestrator.merge import reconcile_merge as _reconcile_merge
    return await _reconcile_merge(Actions(await _pool_get()), dupe=dupe, into=into,
                                  actor=ident.agent_id)


@mcp.tool()
async def restore_attribution(
    project: str, dry_run: bool = True, because: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Repairs a fixed write-time bug: every merge performed before the fix stamped a
    moved works_in/governs/informs/in_repo edge with the merge's own actor as
    source_id, discarding the original writer. The pre-merge row still carries the
    correct source_id, so this re-derives the live edge from evidence already on
    record.

    Resolves `project`'s own merged-in dupes and repairs only damage from those
    merges. Dry run is the default; `dry_run=False` requires a non-blank `because`.
    Safe to run twice: an already-correct or already-repaired edge is left alone."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first: a restore must say who is performing it",
                "why": _anchorless(ctx)}
    from src.orchestrator.projects import restore_attribution as _restore_attribution
    return await _restore_attribution(
        Actions(await _pool_get()), project=project, actor=ident.agent_id,
        dry_run=dry_run, because=because)


@mcp.tool()
async def unwire_informs_fanout(
    project: str = "osiris", dry_run: bool = True, because: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Repairs damage from a pre-fix `_wire_informs` cross-join: `ingest_canon` used to
    fan every Reference out to every active SoftwareProject fleet-wide instead of just
    the one it grounds. Fixed going forward (src/ingest/reference.py); this repairs the
    historical damage.

    Finds every live `informs` edge stamped with the fan-out's own source_id whose
    target is not `project` (default "osiris", the module's only real caller); never
    touches an informs edge asserted by anything else. Dry run is the default.
    `dry_run=False` requires a non-blank `because`. Idempotent."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first: an unwire must say who is performing it",
                "why": _anchorless(ctx)}
    from src.ingest.reference import unwire_informs_fanout as _unwire_informs_fanout
    return await _unwire_informs_fanout(
        Actions(await _pool_get()), project=project, actor=ident.agent_id,
        dry_run=dry_run, because=because)


@mcp.tool()
async def layout_migrate(limit: int | None = None, ctx: Context | None = None) -> dict[str, Any]:
    """Drives the layout heartbeat's own `graph_layout.layout_batch` to completion right
    now instead of waiting on its 5-minute cron cadence. Calls the same function the
    cron heartbeat and the CLI's `osiris layout --migrate` command call, never a second
    implementation of the placement logic. Refuses (an `error` key, no work done) if
    the heartbeat is mid-tick and already holds the layout lock. `limit` overrides the
    live `layout.batch_size` setting for this run only; omit it to use the setting."""
    from src.actions.core import Actions
    from src.orchestrator.graph_layout import run_layout_migrate

    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first: a layout migration must say who is performing it",
                "why": _anchorless(ctx)}
    pool = await _pool_get()
    actions = Actions(pool)
    receipts = [r async for r in run_layout_migrate(actions, limit=limit)]
    if receipts and "error" in receipts[0]:
        return receipts[0]
    total_placed = receipts[-1]["total_placed"] if receipts else 0
    return {"batches": receipts, "total_placed": total_placed}


@mcp.tool()
async def physics_layout_migrate(ctx: Context | None = None) -> dict[str, Any]:
    """Runs the physics-based layout migration: a single global force simulation over
    the whole active graph (springs for semantic edges, weak gravity toward
    containers, nested communities, hub re-centering), never a batch loop the way
    `layout_migrate` is, since this can't be sliced into independent batches (see
    graph_physics.run_physics_migrate's own docstring). Refuses (an `error` key, no
    work done) if the cron heartbeat or a `layout_migrate` run is mid-tick and
    already holds the layout lock. Same shared advisory lock, same
    GRAPH_LAYOUT_SOURCE-only write path as `layout_migrate`."""
    from src.actions.core import Actions
    from src.orchestrator.graph_physics import run_physics_migrate

    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first: a layout migration must say who is performing it",
                "why": _anchorless(ctx)}
    pool = await _pool_get()
    actions = Actions(pool)
    receipts = [r async for r in run_physics_migrate(actions)]
    if receipts and "error" in receipts[0]:
        return receipts[0]
    final = receipts[-1] if receipts else {}
    return {"stages": receipts, "placed": final.get("placed", 0)}


_PROVENANCE_BACKFILL_RECEIPT_THREAD = "e332177f"  # a live tracking thread for this backfill


async def _enqueue_provenance_backfill(
    dry_run: bool, because: str | None, limit: int | None, newest_first: bool,
    ctx: Context | None,
) -> dict[str, Any]:
    """Fixes a prior stall: no transcript
    byte is ever read on osiris-mcp's own event loop thread again. This enqueues
    `provenance_backfill_job` on osiris-worker (its own process) and returns the job id
    immediately; the result lands as a thread annotation on
    `_PROVENANCE_BACKFILL_RECEIPT_THREAD` when the worker finishes. Same
    `arq.create_pool`/`enqueue_job` shape `sweep_route` already uses, never a second
    enqueue mechanism."""
    from arq import create_pool as arq_create_pool
    from arq.connections import RedisSettings

    from src.orchestrator.provenance_backfill import _DEFAULT_LIMIT

    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first, so the graph can record who ran this backfill",
                "why": _anchorless(ctx)}
    arq_pool = await arq_create_pool(RedisSettings.from_dsn(get_settings().redis_url))
    job = await arq_pool.enqueue_job(
        "provenance_backfill_job", dry_run=dry_run, because=because,
        limit=limit if limit is not None else _DEFAULT_LIMIT, newest_first=newest_first,
        actor=ident.agent_id, receipt_ref=_PROVENANCE_BACKFILL_RECEIPT_THREAD)
    job_id = job.job_id if job is not None else None
    return {"enqueued": True, "job_id": job_id,
            "note": f"runs in the background. The result is recorded as an annotation "
                    f"on {_PROVENANCE_BACKFILL_RECEIPT_THREAD} when it finishes, not "
                    "returned directly from this call"}


async def _dispatch_backfill(
    target: str, dry_run: bool, because: str | None, only_bases: list[str] | None,
    ctx: Context | None, *, limit: int | None = None, newest_first: bool = False,
) -> dict[str, Any]:
    """The mount-gate every MCP backfill entry point shares (this tool, plus the four
    deprecated single-target wrappers below it): resolves the calling identity, then
    delegates to `run_backfill`, the same function the CLI's `osiris backfill` command
    calls directly. Never a second dispatch table.

    `limit`/`newest_first` pass straight through; `run_backfill`
    itself is the one place that knows only `provenance_possible_upstream` consults
    them."""
    from src.orchestrator.backfill import run_backfill

    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a backfill is a mind's act, and the graph must "
                         "know whose", "why": _anchorless(ctx)}
    pool = await _pool_get()
    return await run_backfill(
        pool, target, actor=ident.agent_id, dry_run=dry_run, because=because,
        only_bases=only_bases, limit=limit, newest_first=newest_first)


@mcp.tool()
async def backfill(
    target: str, dry_run: bool = True, because: str | None = None,
    only_bases: list[str] | None = None, limit: int | None = None,
    newest_first: bool = False, ctx: Context | None = None,
) -> dict[str, Any]:
    """Repair tool, dispatched over `target`. Eight structurally distinct backfills (no
    shared logic underneath, only a shared call shape), delegated to
    `src.orchestrator.backfill.run_backfill`, the same function the CLI's
    `osiris backfill` command and the UI's Repairs panel call.
    There is only ever one implementation of this dispatch. Dry run is the default for every
    target; `dry_run=False` requires `because` (except `agent_project_links`, which
    predates that convention). All eight are idempotent.

    `target=`: "bootstrap_orphan_references" (links an orphaned `ref:osiris`-stamped
    Reference to the SoftwareProject its own canonical prefix names) |
    "boot_alarm_commit_links" (links a zero-link boot-alarm Thread to the Commit its
    summary cites) | "task_sync_citation_links" (links a zero-link task_sync Thread to
    the Thread it names) | "lineage_repo_links" (links a zero-link Decision/Thread to its
    author's lineage project) | "agent_project_links" (moves works_in/governs off an
    outdated Agent record onto its current one; the one target taking `only_bases` to scope the
    write) | "closed_by_real_sources" (re-points a `closed_by` edge off
    the old 'session'/'analyst:operator' placeholder Agent objects onto the real Person/
    SystemSource record minted today, then retires the now-edgeless
    placeholder) | "operator_charter" (mints a
    `governs` link from `person:operator` to every active SoftwareProject it doesn't
    already govern, so the operator stays chartered over everything) |
    "provenance_possible_upstream" (back-stamps `possible_upstream` onto
    historical Decision/Thread writes via each write's transcript record. `limit`/
    `newest_first` are consulted by this target only. Runs in the background because a
    large transcript can take a long time to process; returns a job
    id at once, and the result is recorded as a thread annotation when done)."""
    from src.orchestrator.backfill import BACKFILL_TARGETS

    if target not in BACKFILL_TARGETS:
        return {"error": f"unknown target {target!r}", "valid_targets": sorted(BACKFILL_TARGETS)}
    if target == "provenance_possible_upstream":
        # Fixes a prior stall: never run this inline here again, see
        # `_enqueue_provenance_backfill`'s own docstring. The CLI entry point still calls
        # `run_backfill` in-process (`cmd_backfill` in src/cli.py); it is its own
        # process, so this entry point's own starvation risk does not apply there.
        return await _enqueue_provenance_backfill(dry_run, because, limit, newest_first, ctx)
    return await _dispatch_backfill(target, dry_run, because, only_bases, ctx,
                                    limit=limit, newest_first=newest_first)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "backfill(target='bootstrap_orphan_references')",
    "since": "an earlier consolidation of the backfill tools",
})
async def backfill_bootstrap_orphan_references(
    dry_run: bool = True, because: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Repair tool for a gap in project bootstrapping: a document-splitter's orphaned
    records are treated as a defect, not a legitimate category. `ingest_log`/`ingest_reference_
    doc` now take `repo=`, threaded through by `bootstrap_project` going forward; this
    repairs the References already orphaned from before that fix.

    Mechanical and conservative: a derived link that is wrong is worse than an orphan
    that is honestly unlinked. Only touches a
    zero-live-link Reference stamped `source_id='ref:osiris'` (the script's own
    fingerprint) whose canonical starts `ref:<name>-` for an existing active
    SoftwareProject, recovering what the canonical already encodes, never inventing a
    fact. No clean project-name prefix means the record is left alone and reported in
    `unmatched`, never guessed.

    Dry run is the default. `dry_run=False` requires a non-blank `because`. Links land
    with evidence_class DERIVED. Idempotent."""
    return await _dispatch_backfill("bootstrap_orphan_references", dry_run, because, None, ctx)


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
        return {"error": "mount first, so the graph can record who ran this repair",
                "why": _anchorless(ctx)}
    from src.orchestrator.dispose import (
        repair_stale_pile_summons as _repair_stale_pile_summons,
    )
    return await _repair_stale_pile_summons(
        Actions(await _pool_get()), actor=ident.agent_id, dry_run=dry_run, because=because)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "backfill(target='boot_alarm_commit_links')",
    "since": "an earlier consolidation of the backfill tools",
})
async def backfill_boot_alarm_commit_links(
    dry_run: bool = True, because: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Links every zero-live-link `UNREVIEWED BOOT` alarm Thread (raised by the boot
    watchdog, which has no caller identity to default a repo= from) to the Commit its own
    summary cites by sha, via `derive_or_abstain`: mints `noted_in` (DIRECT_OBSERVATION)
    only if the sha resolves to exactly one Commit; no sha, or an ambiguous match, abstains
    durably with the candidate set kept. Does not arm required_link_kinds (stays empty):
    a boot alarm still can't satisfy a repo= requirement, this only gives it the
    connectivity it actually has.

    Dry run is the default. `dry_run=False` requires a non-blank `because`. Idempotent."""
    return await _dispatch_backfill("boot_alarm_commit_links", dry_run, because, None, ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "backfill(target='task_sync_citation_links')",
    "since": "an earlier consolidation of the backfill tools",
})
async def backfill_task_sync_citation_links(
    dry_run: bool = True, because: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Links every zero-live-link `task_sync`-minted obligation Thread ("TASK/THREAD
    DISAGREEMENT: ..." / "THREAD SIDE ORPHAN: ..."), the tracker-vs-graph
    divergence detector, to
    the Thread its own summary names, via `derive_or_abstain`: mints `cites`
    (DIRECT_OBSERVATION, origin=derived) only if `task_sync.parse_thread_citations` (reused, not
    re-implemented) finds exactly one citation and it resolves to exactly one existing
    Thread; anything else abstains durably with a distinct reason and the candidate set kept.

    Dry run is the default. `dry_run=False` requires a non-blank `because`. Idempotent."""
    return await _dispatch_backfill("task_sync_citation_links", dry_run, because, None, ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "backfill(target='lineage_repo_links')",
    "since": "an earlier consolidation of the backfill tools",
})
async def backfill_lineage_repo_links(
    dry_run: bool = True, because: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Links every zero-live-link Decision/Thread authored by a real Agent lineage to its
    project: the historical half of the repo= lineage resolution logic, which is
    write-time-only by design and never touches an object
    that already existed before it deployed. Re-runs the same lineage-wide works_in lookup a
    new write already gets: mints `in_repo`
    (DIRECT_OBSERVATION) only if the author's lineage names exactly one project; zero or two-or-more
    abstains durably via derive_or_abstain, candidate set kept, never a guess.

    Dry run is the default. `dry_run=False` requires a non-blank `because`. Idempotent."""
    return await _dispatch_backfill("lineage_repo_links", dry_run, because, None, ctx)


@mcp.tool()
async def recover_harness_exchanges(
    anchor_sid: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """Lift a session's harness-native cross-session messages (SendMessage) out of its
    already-stored transcript into typed, attributed `harness_messages` rows. This
    cannot see the harness's cross-session socket live, only after a transcript
    is stored and this runs over it.

    `anchor_sid` must already be stored; this never reads disk itself. Dry run is the
    default: returns `{found, already_recovered, would_write, sample}`. `dry_run=False`
    requires `because`. Idempotent per (anchor_sid, turn_index)."""
    from src.ingest.cross_channel import recover_harness_exchanges as _recover
    return await _recover(await _pool_get(), anchor_sid, dry_run=dry_run, because=because)


async def _reconcile_seat_identity_impl(
    seat_id: str | None, agent_id: str | None, because: str | None, ctx: Context | None,
) -> dict[str, Any]:
    """The one body behind `reconcile_seat_identity` (self OR third-party) and its
    deprecated alias `reconcile_seat_identity_third_party`. `seat_id=None` heals the
    CALLER's own held seat and own agent identity, `because` unused. `seat_id=<any
    seat>` is the third-party path: `agent_id` optional (omitted heals `house` alone),
    `because` REQUIRED (a correction with no stated reason is the silent overwrite a
    prior ruling forbids, not a fix)."""
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


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='reconcile_identity')",
    "since": "the seat dispatcher consolidation",
})
async def reconcile_seat_identity(
    seat_id: str | None = None, agent_id: str | None = None, because: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    seat(action='reconcile_identity')."""
    return await _reconcile_seat_identity_impl(seat_id, agent_id, because, ctx)


@mcp.tool(meta={"deprecated": True, "use_instead": "reconcile_seat_identity",
                "since": "an earlier identity-tooling consolidation"})
async def reconcile_seat_identity_third_party(
    seat_id: str, because: str, agent_id: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: a hidden alias, dropped from a model's own tool list but still fully
    callable. Shares the same underlying implementation as reconcile_seat_identity,
    nothing duplicated. Kept only so a caller whose standing instructions
    still name this tool is not broken on its next call; will be removed once usage
    monitoring shows it silent."""
    return await _reconcile_seat_identity_impl(seat_id, agent_id, because, ctx)


async def _heal_seat_anchor_impl(
    seat_id: str | None, because: str | None, dry_run: bool, ctx: Context | None,
) -> dict[str, Any]:
    """The one body behind both `heal_seat_anchor` (self OR third-party, by whether
    `seat_id` is given) and its deprecated alias `heal_seat_anchor_third_party`: a plain
    helper, never itself an `@mcp.tool()`, so the two names share this instead of each
    re-implementing it. `seat_id=None` heals the CALLER's own held seat, `because`
    optional; `seat_id=<any seat>` heals a THIRD PARTY's, `because` REQUIRED (a
    correction with no stated reason is the silent overwrite a prior ruling forbids,
    not a fix)."""
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


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='heal_anchor')",
    "since": "the seat dispatcher consolidation",
})
async def heal_seat_anchor(
    seat_id: str | None = None, because: str | None = None, dry_run: bool = True,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to seat(action='heal_anchor')."""
    return await _heal_seat_anchor_impl(seat_id, because, dry_run, ctx)


@mcp.tool(meta={"deprecated": True, "use_instead": "heal_seat_anchor",
                "since": "an earlier identity-tooling consolidation"})
async def heal_seat_anchor_third_party(
    seat_id: str, because: str, dry_run: bool = True, ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: a hidden alias, dropped from a model's own tool list but still fully
    callable. Shares the same underlying implementation as `heal_seat_anchor`, nothing
    duplicated. Kept only so a caller whose standing instructions still name
    this tool is not broken on its next call. Will be removed once usage monitoring
    shows it silent."""
    return await _heal_seat_anchor_impl(seat_id, because, dry_run, ctx)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in a 3-week window, no CLI/daemon/slash bypass found. "
              "The automated uningested_trees_alarm_tick check already covers this ground",
    "since": "an earlier retirement pass",
})
async def uningested_trees(only_gaps: bool = True) -> dict[str, Any]:
    """A census tool onto discover_trees. One row per active
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


# THE PROJECT OBJECT-TYPE DISPATCHER: third object-type dispatcher (after seat,
# composition), one entry point over SoftwareProject lifecycle. 8 standalone tools
# fold in: create_project, ingest_project (self/third-party ingest already unified
# beneath it, see _ingest_project_impl below, unchanged), rename_project,
# fork_project (action='fork'/'unfork', its own pre-existing `direction` param),
# retire_project (already a hidden alias forwarding to retire_object(kind='project')
# before this fold, repointed here to the same underlying _retire_object_impl call,
# the same dual-entry-point precedent seat(action='retire') already established for
# kind='seat'; retire_object itself stays live, kind='agent' still has no dispatcher),
# project_identity_evidence (read-only, kept alongside rename/fork since its whole
# purpose is informing those two calls), assert_project_property.
#
# PARAM UNIFICATION: none needed, every original already used `project` consistently
# for "which existing project" (unlike seat's own four-divergent-names problem). `name`
# is reserved for the two params that mean something different per action (the CREATE
# action's new project name; the ASSERT_PROPERTY action's property name), the same
# shared-slot convention seat's own `key`/`value` already established, disambiguated by
# the action table, never by a second param name.
PROJECT_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "oneOf": [
        _dispatcher_action_schema({
            "action": _action_const("create"), "name": _s(), "because": _s(),
        }, ["action", "name", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("ingest"), "project": _opt_s(), "because": _opt_s(),
            "dry_run": _b(True),
        }, ["action"]),
        _dispatcher_action_schema({
            "action": _action_const("rename"), "project": _s(), "new_name": _s(),
            "because": _s(), "dry_run": _b(True), "merge_into": _b(False),
        }, ["action", "project", "new_name", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("fork"), "project": _s(), "fork_into": _s(),
            "because": _s(),
        }, ["action", "project", "fork_into", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("unfork"), "project": _s(), "fork_into": _s(),
            "because": _s(),
        }, ["action", "project", "fork_into", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("retire"), "project": _s(), "because": _s(),
        }, ["action", "project", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("identity_evidence"), "seat_id": _s(),
            "operator_citation": _opt_s(),
        }, ["action", "seat_id"]),
        _dispatcher_action_schema({
            "action": _action_const("assert_property"), "project": _s(), "name": _s(),
            "value": _s(),
        }, ["action", "project", "name", "value"]),
        _dispatcher_action_schema({
            "action": _action_const("set_tag"), "project": _s(), "tag": _s(),
            "because": _s(),
        }, ["action", "project", "tag", "because"]),
    ],
}
_HAND_BUILT_SCHEMAS["project"] = PROJECT_INPUT_SCHEMA

_PROJECT_ACTION_PARAMS: dict[str, tuple[list[str], list[str]]] = {
    "create": (["name", "because"], ["name", "because"]),
    "ingest": (["project", "because", "dry_run"], []),
    "rename": (["project", "new_name", "because", "dry_run", "merge_into"],
              ["project", "new_name", "because"]),
    "fork": (["project", "fork_into", "because"], ["project", "fork_into", "because"]),
    "unfork": (["project", "fork_into", "because"], ["project", "fork_into", "because"]),
    "retire": (["project", "because"], ["project", "because"]),
    "identity_evidence": (["seat_id", "operator_citation"], ["seat_id"]),
    "assert_property": (["project", "name", "value"], ["project", "name", "value"]),
    "set_tag": (["project", "tag", "because"], ["project", "tag", "because"]),
}


async def _project_impl(
    action: str, *,
    project: str | None = None, name: str | None = None, because: str | None = None,
    dry_run: bool = True, new_name: str | None = None, fork_into: str | None = None,
    seat_id: str | None = None, operator_citation: str | None = None,
    value: str | None = None, merge_into: bool = False, tag: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Shared body behind `project` and its 7 hidden single-purpose aliases
    (create_project, ingest_project, rename_project, fork_project, unfork_project,
    retire_project, project_identity_evidence, assert_project_property, 8 names, one
    more than "7" counts because retire_project was already a hidden alias forwarding
    to retire_object(kind='project') before this fold; both entry points now reach the
    identical _retire_object_impl call): one code path, many names. Every branch's
    body below is copied verbatim from what was that alias's own top-level function.

    PRE-DISPATCH VALIDATION, same discipline as _seat_impl's own."""
    if action not in _PROJECT_ACTION_PARAMS:
        return {"error": f"unknown action {action!r}",
                "known_actions": sorted(_PROJECT_ACTION_PARAMS)}
    accepted, required = _PROJECT_ACTION_PARAMS[action]
    local = dict(locals())
    missing = [p for p in required if local.get(p) in (None, "")]
    if missing:
        return {"error": f"action {action!r} is missing required param(s) {missing}",
                "action_accepts": accepted, "action_requires": required}

    if action == "create":
        assert name is not None and because is not None  # pre-dispatch validation
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — creating a project is a deliberate act on "
                             "the record", "why": _anchorless(ctx)}
        from src.orchestrator.project_identity import create_project as _create_project
        return await _create_project(Actions(await _pool_get()), name=name, because=because,
                                     actor=ident.agent_id)
    if action == "ingest":
        return await _ingest_project_impl(project, because, dry_run, ctx)
    if action == "rename":
        assert project is not None and new_name is not None and because is not None
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
                                    because=because, actor=ident.agent_id,
                                    dry_run=dry_run, merge_into=merge_into)
        if not dry_run and not out.get("error"):
            # STALE MOUNT CACHE: get_status()'s `project` field reads ident.project off
            # THIS process's in-memory _agents cache, not a fresh graph read, the same
            # shape transition_project/correct_house already guard above. A rename with
            # no in-process cache fix left every already-mounted agent (any generation,
            # not just the caller's own lineage, a project rename is never
            # lineage-scoped) reporting the pre-rename name until its next full
            # re-mount.
            old_bare = out["old_canonical"].removeprefix("repo:")
            stale_labels = {old_bare, out.get("old_name")}
            for cached in _agents.values():
                if cached.project in stale_labels:
                    cached.project = new_name
            # THE SEAT-BOUND HALF (mount-cache heal generalization): the string-match
            # above catches any cached entry whose `.project` happened to equal the old
            # bare name (including unbound test doubles, and any stale coincidental
            # match), but a governing seat's own live holder whose cached `.project` was
            # ALREADY wrong for some unrelated reason would never string-match
            # `old_bare` and so would never heal. Every seat this cascade actually
            # touched (the manifest's own governing-seat keys) is healed too, via the
            # same seat-bound path promote/charter/attach/detach use: belt and
            # suspenders, not a replacement for the broad string-match above.
            manifest_seats = set(out.get("manifest", {}).get("seats", {}).keys())
            await _heal_mount_cache_for_seats(pool, manifest_seats)
        # This evidence attachment describes what governing seats think of new_name:
        # meaningless noise when the rename itself never happened (a refusal) or hasn't
        # happened YET (a dry-run preview), so it only runs on an actual, landed write.
        # The old unconditional version claimed "{new_name!r} was written" verbatim on
        # a REFUSAL path too, whenever any seat's evidence happened to disagree with
        # the value that was never written at all.
        if evidence_by_seat and not out.get("error") and not dry_run:
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
    if action in ("fork", "unfork"):
        assert project is not None and fork_into is not None and because is not None
        return await _fork_project_impl(project, fork_into, because, action, ctx)
    if action == "retire":
        assert project is not None and because is not None
        return await _retire_object_impl(
            "project", project, because=because, override_live=False, ctx=ctx)
    if action == "identity_evidence":
        assert seat_id is not None  # pre-dispatch validation guaranteed this
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — reading identity evidence needs a resolvable "
                             "caller", "why": _anchorless(ctx)}
        from src.orchestrator.project_identity import (
            project_identity_evidence as _project_identity_evidence,
        )
        return await _project_identity_evidence(
            await _pool_get(), seat_id=seat_id, operator_citation=operator_citation)
    if action == "assert_property":
        assert project is not None and name is not None and value is not None
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — asserting a project property is a deliberate "
                             "act on the record", "why": _anchorless(ctx)}
        from src.orchestrator.projects import (
            assert_project_property as _assert_project_property,
        )
        pool = await _pool_get()
        out = await _assert_project_property(Actions(pool), project=project,
                                             name=name, value=value, actor=ident.agent_id)
        if out.get("id"):
            await provenance.stamp_possible_upstream(
                Actions(pool), written_object_id=uuid.UUID(out["id"]), source_id=ident.agent_id)
        return out
    if action == "set_tag":
        assert project is not None and tag is not None and because is not None
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — declaring a window tag is a deliberate act "
                             "on the record", "why": _anchorless(ctx)}
        from src.orchestrator.projects import (
            set_project_window_tag as _set_project_window_tag,
        )
        return await _set_project_window_tag(Actions(await _pool_get()), project=project,
                                             tag=tag, because=because, actor=ident.agent_id)
    raise AssertionError(f"action {action!r} passed validation but has no branch")


@mcp.tool()
async def project(
    action: str, project: str | None = None, name: str | None = None, because: str = "",
    dry_run: bool = True, new_name: str | None = None, fork_into: str | None = None,
    seat_id: str | None = None, operator_citation: str | None = None,
    value: str | None = None, merge_into: bool = False, tag: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """One tool, many actions over SoftwareProject lifecycle. See `describe('project')` for
    the full per-action shape, or call with a wrong or missing param: the error names
    exactly what that action expects.

    Actions (what each does, and its required params beyond action):
      create: declare a new SoftwareProject, never a duplicate (name, because)
      ingest: land a project's own git history and close the threads it witnesses
        (project=None and because=None is the self-service shape, resolving your own pin; project
        given and because given is the third-party shape instead)
      rename: declare a project's new name, non-canonical (project, new_name, because).
        dry_run=True by default (pass dry_run=False to actually write). Refuses a
        new_name already naming a different project of any status unless merge_into=True.
        Cascades to every governing seat's pin/house/charter/office under this tool's
        own elevated authority; the result's `manifest` names every tier touched,
        already-correct, or unable to update, per seat. Never silent on a partial result.
      fork: declare two already-active projects a fork pair (project, fork_into, because)
      unfork: reverse a fork pair's live edge (project, fork_into, because)
      retire: retire a dead project stub, third-party (project, because)
      identity_evidence: read-only, gather a seat's project-identity evidence across
        five tiers. Read this before rename/fork (seat_id)
      assert_property: the sanctioned write for a single project-scoped property
        (project, name, value). Never use name='status', that's retire's own path
      set_tag: declare the persisted tag override that overrides the default derived
        tag (for example "MH" instead of a derived "MO") (project, tag, because). `tag`
        must be 1-4 uppercase letters exactly as given; refuses rather than silently
        coercing an unexpected shape. Written as its own `window_tag` property, never
        the unrelated, additive/multi-valued `tag` property case-tooling already uses

    `name` means something different per action: the new project's name on `create`,
    the property name on `assert_property`. Never the same slot's value twice."""
    return await _project_impl(
        action, project=project, name=name, because=because, dry_run=dry_run,
        new_name=new_name, fork_into=fork_into, seat_id=seat_id,
        operator_citation=operator_citation, value=value, merge_into=merge_into, tag=tag,
        ctx=ctx)


async def _ingest_project_impl(
    project: str | None, because: str | None, dry_run: bool, ctx: Context | None,
) -> dict[str, Any]:
    """The one body behind `ingest_project` and its deprecated alias `ingest_project_
    third_party`. `because` blank/omitted is the self-service shape (`project` omitted
    resolves to the caller's own mounted pin); `because` given routes through the
    third-party orchestrator function instead, which stamps it onto the result. The
    orchestrator layer already IS this same split (ingest_project_third_party's own
    body is nothing but a because-required check wrapping a call to ingest_project),
    this only removes the second MCP-layer copy of that check."""
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


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "project(action='ingest')",
    "since": "the project dispatcher consolidation",
})
async def ingest_project(
    project: str | None = None, dry_run: bool = True, because: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    project(action='ingest')."""
    return await _project_impl("ingest", project=project, because=because,
                               dry_run=dry_run, ctx=ctx)


@mcp.tool(meta={"deprecated": True, "use_instead": "project(action='ingest')",
                "since": "an earlier project-tooling consolidation"})
async def ingest_project_third_party(
    project: str, because: str, dry_run: bool = True, ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: a hidden alias, dropped from a model's own tool list but still fully
    callable. Shares the same underlying implementation as ingest_project, nothing
    duplicated. Kept only so a caller whose standing instructions still name
    this tool is not broken on its next call. Will be removed once usage monitoring shows it
    silent."""
    return await _ingest_project_impl(project, because, dry_run, ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='correct_house')",
    "since": "the seat dispatcher consolidation",
})
async def correct_house(new_house: str, ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    seat(action='correct_house')."""
    return await _seat_impl("correct_house", new_house=new_house, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='resync_house')",
    "since": "the seat dispatcher consolidation",
})
async def resync_seat_house(seat_id: str, new_house: str | None, reason: str,
                            ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    seat(action='resync_house')."""
    return await _seat_impl("resync_house", target=seat_id, new_house=new_house,
                            reason=reason, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='correct_pin')",
    "since": "the seat dispatcher consolidation",
})
async def correct_pin_value(key: str, value: str | None, reason: str,
                            ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to seat(action='correct_pin')."""
    return await _seat_impl("correct_pin", key=key, value=value, reason=reason, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='revert_pin')",
    "since": "the seat dispatcher consolidation",
})
async def revert_own_pin_write(ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to seat(action='revert_pin')."""
    return await _seat_impl("revert_pin", ctx=ctx)


async def _retire_object_impl(
    kind: str, target: str, *, because: str, override_live: bool, ctx: Context | None,
) -> dict[str, Any]:
    """Shared body behind `retire_object` and its three hidden single-purpose aliases
    (retire_seat/retire_project/retire_agent): one code path, five names now
    (kind='object' added later, no alias of its own, the generic entry point needed
    no deprecated single-purpose predecessor to fold). Each of the first three kinds
    below is copied verbatim from what was that alias's own top-level function body
    before the fold. Deliberately does NOT cover self-scoped `retire()` (no target
    param, different auth shape entirely) or `retire_assertion` (a genuinely unrelated
    5-field shape, not a target+reason act); a prior proposal explains why those two
    stay out."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": f"mount first — retiring {'a' if kind != 'agent' else 'an'} "
                         f"{kind} is a deliberate act on the record", "why": _anchorless(ctx)}
    if kind == "seat":
        from src.orchestrator.seats import retire_seat as _retire_seat
        return await _retire_seat(Actions(await _pool_get()), target, reason=because,
                                  actor=ident.agent_id)
    if kind == "project":
        from src.orchestrator.projects import retire_project as _retire_project
        return await _retire_project(Actions(await _pool_get()), project=target,
                                     actor=ident.agent_id, because=because)
    if kind == "agent":
        from src.orchestrator.agents import retire_agent as _retire_agent
        return await _retire_agent(Actions(await _pool_get()), agent_id=target,
                                   actor=ident.agent_id, because=because,
                                   override_live=override_live)
    if kind == "object":
        from src.orchestrator.retirement import retire_bare_object as _retire_bare_object
        return await _retire_bare_object(Actions(await _pool_get()), ref=target,
                                         because=because, actor=ident.agent_id)
    return {"error": f"unknown kind {kind!r} — one of seat/project/agent/object"}


@mcp.tool()
async def retire_object(
    kind: str, target: str, because: str = "", override_live: bool = False,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Third-party retirement of a named Seat, SoftwareProject, Agent, or bare object. One
    tool, four `kind`s, never a fifth. Distinct from self-scoped `retire()` (no target
    param, retires the calling agent's own live session/turn) and from `retire_assertion`
    (a cross-source supersede, an unrelated shape); neither folds into this tool.

    `kind='seat'`: mark a Seat permanently closed: a genuinely dead role, no successor,
    no merge target. Refuses on an unknown or already-inactive seat, or an active
    holder; transfer or let it vacate first.

    `kind='project'`: retire a dead SoftwareProject stub, status flip to 'retired' via
    a compensating event, never a delete. `target` resolves to a SoftwareProject only
    (UUID, 8-char short id, canonical `repo:<name>`, or its `name` property), never a
    Seat or Agent of the same name. Refuses loudly on: blank `because`; an unresolved or
    already-non-active project; any commit recorded against it; any open Thread pointing
    in; or a mount seen against it within the last 15 minutes.

    `kind='agent'`: third-party Agent retirement. Stamps retired/retired_by/retired_
    because, flips objects.status. Not self-scoped or manager-gated: any caller may
    name any target; `actor` is attribution, not authority. Always releases the target's
    held seat and mount rows on success. Refuses loudly on: blank `because`; an unknown or
    non-active agent; a target that reads live (seen within 15 min) unless
    `override_live=True`. `override_live` is ignored for the other three kinds.

    `kind='object'`: retire an arbitrary active object of no other
    kind: the shape a stray script or a mis-minted stub leaves behind, which none of the
    three kinds above cover. `target` resolves via the generic resolve_ref (UUID,
    short-id, canonical, or name; any object type). Refuses loudly on: blank
    `because`; an unresolved or already-non-active object; any live link touching it
    in either direction; or any current assertion from a source other than the
    layout heartbeat's own bookkeeping (graph_x/graph_y/graph_layout_v are exempt;
    real content from any other source refuses)."""
    return await _retire_object_impl(
        kind, target, because=because, override_live=override_live, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "retire_object(kind='seat')",
    "since": "an earlier retirement-tooling consolidation",
})
async def retire_seat(seat_id: str, reason: str = "",
                      ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    retire_object(kind='seat')."""
    return await _retire_object_impl(
        "seat", seat_id, because=reason, override_live=False, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='sweep_disk')",
    "since": "the seat dispatcher consolidation",
})
async def sweep_seat_disk(handle: str, dry_run: bool = True, because: str = "",
                          ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to seat(action='sweep_disk')."""
    return await _seat_impl("sweep_disk", target=handle, dry_run=dry_run, because=because,
                            ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='vacate')",
    "since": "the seat dispatcher consolidation",
})
async def vacate_seat(seat_id: str, because: str, ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to seat(action='vacate')."""
    return await _seat_impl("vacate", target=seat_id, because=because, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "project(action='retire')",
    "since": "an earlier project-tooling consolidation",
})
async def retire_project(project: str, because: str,
                         ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    project(action='retire'), the same underlying call that retire_object(kind=
    'project') also reaches, the same dual-route pattern that seat(action='retire') already
    uses for kind='seat'."""
    return await _retire_object_impl(
        "project", project, because=because, override_live=False, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "project(action='identity_evidence')",
    "since": "the project dispatcher consolidation",
})
async def project_identity_evidence(seat_id: str, operator_citation: str | None = None,
                                    ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    project(action='identity_evidence')."""
    return await _project_impl("identity_evidence", seat_id=seat_id,
                               operator_citation=operator_citation, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "project(action='rename')",
    "since": "the project dispatcher consolidation",
})
async def rename_project(project: str, new_name: str, because: str, dry_run: bool = True,
                         merge_into: bool = False,
                         ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    project(action='rename')."""
    return await _project_impl("rename", project=project, new_name=new_name,
                               because=because, dry_run=dry_run, merge_into=merge_into,
                               ctx=ctx)


async def _fork_project_impl(
    project: str, fork_into: str, because: str, direction: str, ctx: Context | None,
) -> dict[str, Any]:
    """The one body behind `fork_project` (both directions, by `direction`) and its
    deprecated alias `unfork_project`: a plain helper, never itself an `@mcp.tool()`.
    `direction="fork"` (default) declares the pair; `direction="unfork"` reverses it,
    the same action, its own inverse."""
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


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "project(action='fork')",
    "since": "the project dispatcher consolidation",
})
async def fork_project(
    project: str, fork_into: str, because: str, direction: str = "fork",
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to project(action='fork') or
    project(action='unfork'), by `direction` (kept for this alias's own back-compat
    signature; the dispatcher itself exposes fork/unfork as two separate actions,
    never a direction param)."""
    return await _fork_project_impl(project, fork_into, because, direction, ctx)


@mcp.tool(meta={"deprecated": True, "use_instead": "project(action='unfork')",
                "since": "the project dispatcher consolidation"})
async def unfork_project(project: str, fork_into: str, because: str,
                         ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    project(action='unfork')."""
    return await _fork_project_impl(project, fork_into, because, "unfork", ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "project(action='create')",
    "since": "the project dispatcher consolidation",
})
async def create_project(name: str, because: str, ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    project(action='create')."""
    return await _project_impl("create", name=name, because=because, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "project(action='assert_property')",
    "since": "the project dispatcher consolidation",
})
async def assert_project_property(project: str, name: str, value: str,
                                  ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    project(action='assert_property')."""
    return await _project_impl("assert_property", project=project, name=name, value=value,
                               ctx=ctx)


@mcp.tool()
async def peer_seats(seat_a: str, seat_b: str, because: str,
                     ctx: Context | None = None) -> dict[str, Any]:
    """Mint a symmetric peer_of bond between two active Seats. Recognition-first: makes
    the pair legible to mail routing, review assignment, and succession. Not self-scoped:
    neither seat need be the caller's own;
    the caller is recorded only as `actor` (who made the bond), never a party to it by
    default.

    Refuses loudly on: blank `because`; an unknown or inactive seat on either side;
    seat_a==seat_b; or either seat already carrying an active peer_of edge. This
    version supports pairs only, no chains (a triad may be supported in a future
    version, after the first pair survives contact)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first, peering two seats is a deliberate act on the "
                         "record", "why": _anchorless(ctx)}
    from src.orchestrator.seats import peer_seats as _peer_seats
    return await _peer_seats(Actions(await _pool_get()), seat_a, seat_b, because=because,
                             actor=ident.agent_id)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in a 3-week window, no CLI/daemon/slash bypass found",
    "since": "an earlier retirement pass",
})
async def unpeer(seat_a: str, seat_b: str, because: str,
                 ctx: Context | None = None) -> dict[str, Any]:
    """Invalidate an active peer_of bond between two Seats, the compensating-event
    complement to peer_seats. Direction-agnostic: the bond is symmetric, so unpeer(a, b)
    and unpeer(b, a) heal the same edge.

    Refuses LOUDLY on: blank `because`; an unknown/inactive seat on either side; or no
    active peer_of edge between the named pair."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first. Unpeering two seats is a deliberate act on the "
                         "record", "why": _anchorless(ctx)}
    from src.orchestrator.seats import unpeer as _unpeer
    return await _unpeer(Actions(await _pool_get()), seat_a, seat_b, because=because,
                         actor=ident.agent_id)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "deprecated in a prior cleanup pass",
})
async def hold_action(holder: str, held: str, act: str, because: str, hours: float = 24,
                      ctx: Context | None = None) -> dict[str, Any]:
    """Create a mutual hold: one seat's power to place a time-boxed hold on its own
    peer's specific irreversible action. `holder` is the seat calling the hold, `held`
    is the seat whose action is being held, `act` names the specific action, `hours`
    sets the time-box (default 24). Reuses the ordinary obligation thread mechanism, no
    new object type. Resolve it the ordinary way, with `resolve_thread` on the returned
    `held` id, once it's respected or the action proceeds anyway. Automatic escalation
    to a human when a hold expires unresolved is not built yet; this only records the
    hold and its deadline honestly, nothing sweeps for expiry today.

    Refuses on: blank `act`/`because`; `holder==held`; an unknown or inactive seat on
    either side; non-positive `hours`; or holder/held not currently an active peer pair.
    A hold is a peer's own power, never available between unrelated parties."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first. Holding a peer's action is a deliberate act on "
                         "the record", "why": _anchorless(ctx)}
    from src.orchestrator.seats import hold_action as _hold_action
    return await _hold_action(Actions(await _pool_get()), holder, held, act=act,
                              because=because, hours=hours, actor=ident.agent_id)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "deprecated in a prior cleanup pass",
})
async def peer_reachable(seat_id: str) -> list[str]:
    """Every seat a search for `seat_id`'s own queue should also cover. This is about
    discoverability only: mail delivery itself is untouched, this never widens who a
    message reaches. Returns `[seat_id]` alone when unpeered or unknown, or `[seat_id,
    peer]` when an active peer bond exists. There is no review verb or object in this
    codebase today, so this is scoped for whatever future surface reads one seat's
    queue, not a review-assignment feature that doesn't exist yet."""
    pool = await _pool_get()
    from src.orchestrator.seats import peer_reachable as _peer_reachable
    return await _peer_reachable(pool, seat_id)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "deprecated in a prior cleanup pass",
})
async def peer_ledger(seat_a: str, seat_b: str) -> list[dict[str, Any]]:
    """The pair's shared ledger: every open thread owned by either seat, oldest first,
    as one resumable list. No new storage; open_thread/resolve_thread stay the only
    write path, this only reads. What makes a parked pair resumable is a thread staying
    open on purpose. Does not require an active peer bond between the two named seats;
    a healed pair's own history stays readable."""
    pool = await _pool_get()
    from src.orchestrator.seats import peer_ledger as _peer_ledger
    return await _peer_ledger(pool, seat_a, seat_b)


async def _seat_edge_impl(
    action: str, worker: str, *, manager: str | None, because: str, ctx: Context | None,
) -> dict[str, Any]:
    """Shared body behind `seat_edge` and its two hidden single-purpose aliases (attach_
    seat/detach_seat): one code path, three names. Each action below is copied
    verbatim from what was that alias's own top-level function body before the fold,
    plus a reissue of BOTH sides' seat directories: promote already refreshes manager
    and worker through its own caller (mcp_server.py's `seat(action='promote')`
    branch); attach/detach mint or cut the SAME `managed_by` edge but, before this,
    refreshed neither, so a manager's own "## Your team" listing and a worker's own
    manager-of-record line both went stale the moment either action ran outside
    promote. Also heals the WORKER's own mount cache (never the manager's, a
    manager's own project is unaffected by gaining/losing a worker, only the worker's
    derived project depends on the managed_by chain attach/detach changes; this is
    the same mount-cache heal generalization used elsewhere in this file)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": f"mount first — {action}ing a seat's manager is a deliberate "
                         "act on the record", "why": _anchorless(ctx)}
    pool = await _pool_get()
    if action == "detach":
        from src.orchestrator.seats import detach_seat as _detach
        result = await _detach(Actions(pool), worker, because=because, actor=ident.agent_id)
        if result.get("error"):
            return result
        affected = {result["detached"], result["was_managed_by"]}
        worker_seat = result["detached"]
    elif action == "attach":
        assert manager is not None
        from src.orchestrator.seats import attach_seat as _attach
        result = await _attach(Actions(pool), worker, manager, evidence=because,
                               actor=ident.agent_id)
        if result.get("error"):
            return result
        affected = {result["attached"], result["now_managed_by"]}
        worker_seat = result["attached"]
    else:
        return {"error": f"unknown action {action!r} — one of attach/detach"}

    from src.orchestrator.boot_compiler import reissue_office as _reissue_office

    office_refresh: dict[str, Any] = {}
    for seat_id_affected in affected:
        office_refresh[seat_id_affected] = await _reissue_office(
            Actions(pool), seat_id=seat_id_affected, because=f"{action}: {because}",
            actor=ident.agent_id)
    result["office_refresh"] = office_refresh
    await _heal_mount_cache_for_seats(pool, {worker_seat})
    return result


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='attach'/'detach')",
    "since": "deprecated in a prior cleanup pass",
})
async def seat_edge(
    action: str, worker: str, manager: str | None = None, because: str = "",
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    seat(action='attach'/'detach')."""
    return await _seat_edge_impl(action, worker, manager=manager, because=because, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat_edge(action='detach')",
    "since": "deprecated in a prior cleanup pass",
})
async def detach_seat(seat: str, because: str, ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    seat_edge(action='detach')."""
    return await _seat_edge_impl("detach", seat, manager=None, because=because, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat_edge(action='attach')",
    "since": "deprecated in a prior cleanup pass",
})
async def attach_seat(
    worker: str, manager: str, evidence: str, ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    seat_edge(action='attach')."""
    return await _seat_edge_impl("attach", worker, manager=manager, because=evidence, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='invalidate_works_in')",
    "since": "deprecated in a prior cleanup pass",
})
async def invalidate_works_in(stale_project: str, because: str,
                              ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    seat(action='invalidate_works_in')."""
    return await _seat_impl("invalidate_works_in", stale_project=stale_project,
                            because=because, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='transition_project')",
    "since": "deprecated in a prior cleanup pass",
})
async def transition_seat_project(
    fabricated_project: str | None = None, real_project: str | None = None,
    because: str = "", repos: list[str] | None = None, dry_run: bool = True,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    seat(action='transition_project')."""
    return await _seat_impl("transition_project", fabricated_project=fabricated_project,
                            real_project=real_project, because=because, repos=repos,
                            dry_run=dry_run, ctx=ctx)


# THE AGENT OBJECT-TYPE DISPATCHER: fifth object-type dispatcher, folding the
# identity/mail-adjacent Agent-write surface: claim_name (self-scoped naming),
# correct_agent_house (third-party house/generation correction, already a hidden
# zero-traffic tool, this repoints its own use_instead, costs nothing further on the
# live count), retire_agent (already a hidden alias of retire_object(kind='agent'),
# repointed here too, the same dual-entry-point precedent seat/project(action=
# 'retire') established), fleet_reconcile (the bulk fleet reaper, no target),
# file_subagent (single-target hand-filing), file_subagents (bulk sweep, dry_run).
#
# DECLINED, with reasons named in the approving proposal rather than silently
# dropped: retire() stays OUT (self-scoped, different auth shape, same exclusion
# retire_object's own fold already gave it); walk_in stays OUT (already a hidden
# alias of seat(action='walk_in'); re-folding an already-folded name into a
# DIFFERENT dispatcher would be incoherent); merge/unmerge/reconcile_merge stay OUT
# (polymorphic across Agent/Seat/Project, already ruled to stay named);
# backfill_agent_project_links stays OUT (already hidden, forwards to backfill(
# target=...), a different dispatcher); restore_attribution stays OUT (keyed on
# `project`, not `agent_id`, wrong object type); lift stays OUT (already dead, a
# compound orchestration, not a bare CRUD action); identify_agent/succession_chain/
# unwitnessed_spawns stay OUT (pure reads, distinct questions, same class search/
# recall/dossier already sit in).
#
# PARAM UNIFICATION: `agent_id` is the shared name for "which existing Agent" across
# correct_house/retire (both originals already used it); `subagent_id` stays its own
# name on file_subagent, a genuinely distinct domain concept (an ephemeral session's
# own id), not just a plumbing synonym for agent_id, the same shared-slot discipline
# seat's own handle/target split established. `name` is claim_name's own CREATE-shaped
# param (the name being minted), never confused with an existing-object reference.
AGENT_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "oneOf": [
        _dispatcher_action_schema({
            "action": _action_const("claim_name"), "name": _s(),
        }, ["action", "name"]),
        _dispatcher_action_schema({
            "action": _action_const("correct_house"), "agent_id": _s(),
            "project": _opt_s(), "seat_generation": _opt_int_s(),
        }, ["action", "agent_id"]),
        # ONE TAXONOMY: "house" retired as the word for the project a seat governs,
        # correct_project is the real name now, correct_house kept above as a
        # deprecated alias for one release only (same spelling as the CLI's own
        # correct-agent-house -> correct-agent-project).
        _dispatcher_action_schema({
            "action": _action_const("correct_project"), "agent_id": _s(),
            "project": _opt_s(), "seat_generation": _opt_int_s(),
        }, ["action", "agent_id"]),
        _dispatcher_action_schema({
            "action": _action_const("correct_succession"), "agent_id": _s(),
            "value": _opt_s(), "because": _s(), "override_live": _b(False),
            "retract": _b(False),
        }, ["action", "agent_id", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("retire"), "agent_id": _s(), "because": _s(),
            "override_live": _b(False),
        }, ["action", "agent_id", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("fleet_reconcile"), "execute": _b(False),
        }, ["action"]),
        _dispatcher_action_schema({
            "action": _action_const("fleet_prune"), "execute": _b(False),
        }, ["action"]),
        _dispatcher_action_schema({
            "action": _action_const("file_subagent"), "subagent_id": _s(),
        }, ["action", "subagent_id"]),
        _dispatcher_action_schema({
            "action": _action_const("file_subagents"), "project": _opt_s(),
            "dry_run": _b(True),
        }, ["action"]),
        _dispatcher_action_schema({
            "action": _action_const("retire_governs"), "agent_id": _s(),
            "repos": _list_s(), "because": _s(),
        }, ["action", "agent_id", "repos", "because"]),
        _dispatcher_action_schema({
            "action": _action_const("invalidate_works_in"), "agent_id": _s(),
            "project": _s(), "because": _s(),
        }, ["action", "agent_id", "project", "because"]),
    ],
}
_HAND_BUILT_SCHEMAS["agent"] = AGENT_INPUT_SCHEMA

_AGENT_ACTION_PARAMS: dict[str, tuple[list[str], list[str]]] = {
    "claim_name": (["name"], ["name"]),
    "correct_project": (["agent_id", "project", "seat_generation"], ["agent_id"]),
    # `value` is deliberately NOT in required here, the same _UNSET reason as
    # correct_pin's own `value` above: "" is a legal, meaningful retraction, not an
    # omission, and the shared missing-check below treats "" as absent; the branch
    # itself refuses a genuine _UNSET.
    "correct_succession": (["agent_id", "value", "because", "override_live", "retract"],
                           ["agent_id", "because"]),
    "retire": (["agent_id", "because", "override_live"], ["agent_id", "because"]),
    "fleet_reconcile": (["execute"], []),
    "fleet_prune": (["execute"], []),
    "file_subagent": (["subagent_id"], ["subagent_id"]),
    "file_subagents": (["project", "dry_run"], []),
    "retire_governs": (["agent_id", "repos", "because"], ["agent_id", "repos", "because"]),
    # `project` doubles as the STALE project to drop (the same shared-slot convention
    # `correct_house`'s own `project` already uses above), third-party, unlike
    # seat(action='invalidate_works_in')'s self-scoped path, which auto-fills agent_id
    # from the caller and never exposes it as a parameter at all.
    "invalidate_works_in": (["agent_id", "project", "because"],
                            ["agent_id", "project", "because"]),
}


async def _agent_impl(
    action: str, *,
    name: str | None = None, agent_id: str | None = None, project: str | None = None,
    seat_generation: int | None = None, value: str | None = _UNSET,
    because: str | None = None,
    override_live: bool = False, execute: bool = False, subagent_id: str | None = None,
    dry_run: bool = True, retract: bool = False, repos: list[str] | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Shared body behind `agent` and its 5 hidden single-purpose aliases (claim_name,
    correct_agent_house, retire_agent, file_subagent, file_subagents, 6 names, one
    more than "5" counts because retire_agent was already a hidden alias forwarding to
    retire_object(kind='agent') before this fold; both entry points now reach the
    identical _retire_object_impl call): one code path, many names. Every branch's
    body below is copied verbatim from what was that alias's own top-level function.

    PRE-DISPATCH VALIDATION, same discipline as _seat_impl's own."""
    # ONE TAXONOMY: correct_house's own deprecated spelling normalizes to its
    # canonical name here, before the params lookup, one dict entry under the new
    # name, never a duplicate.
    action = {"correct_house": "correct_project"}.get(action, action)
    if action not in _AGENT_ACTION_PARAMS:
        return {"error": f"unknown action {action!r}",
                "known_actions": sorted(_AGENT_ACTION_PARAMS)}
    accepted, required = _AGENT_ACTION_PARAMS[action]
    local = dict(locals())
    missing = [p for p in required if local.get(p) in (None, "")]
    if missing:
        return {"error": f"action {action!r} is missing required param(s) {missing}",
                "action_accepts": accepted, "action_requires": required}

    if action == "claim_name":
        assert name is not None  # pre-dispatch validation guaranteed this
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount(cwd, job_dir=<your anchor>) first — a name attaches "
                             "to YOU", "why": _anchorless(ctx)}
        from src.orchestrator.agents import claim_name as _claim
        return await _claim(Actions(await _pool_get()), ident.agent_id, name,
                            source=ident.agent_id)
    if action == "correct_project":
        assert agent_id is not None
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a correction is a mind's act, and the graph "
                             "must know whose", "why": _anchorless(ctx)}
        from src.orchestrator.agents import correct_agent_house as _correct_agent_house
        return await _correct_agent_house(
            Actions(await _pool_get()), agent_id=agent_id, project=project,
            seat_generation=seat_generation, actor=ident.agent_id)
    if action == "correct_succession":
        assert agent_id is not None and because is not None
        # THE HARNESS CANNOT SEND "": an explicit empty-string argument is serialized as
        # `"value": ,` (invalid JSON) by the calling harness, so the "" retraction
        # contract was unreachable from any agent. `retract=True` is the boolean
        # spelling of the same action; "" still works for callers that can send it.
        if retract:
            value = ""
        if value is _UNSET or value is None:
            return {"error": "value is required — pass \"\" explicitly to retract the "
                             "succession pointer to unset, never omit it to mean that"}
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — a correction is a mind's act, and the graph "
                             "must know whose", "why": _anchorless(ctx)}
        from src.orchestrator.agents import correct_succession as _correct_succession
        return await _correct_succession(
            Actions(await _pool_get()), agent_id=agent_id, value=value, because=because,
            actor=ident.agent_id, override_live=override_live)
    if action == "retire":
        assert agent_id is not None and because is not None
        return await _retire_object_impl(
            "agent", agent_id, because=because, override_live=override_live, ctx=ctx)
    if action == "fleet_reconcile":
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first", "why": _anchorless(ctx)}
        from src.orchestrator.fleet_reconcile import reconcile_execute
        return await reconcile_execute(Actions(await _pool_get()), actor=ident.agent_id,
                                       execute=execute)
    if action == "fleet_prune":
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first", "why": _anchorless(ctx)}
        from src.orchestrator.fleet_prune import prune_execute
        return await prune_execute(Actions(await _pool_get()), actor=ident.agent_id,
                                   execute=execute)
    if action == "file_subagent":
        assert subagent_id is not None
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — filing a hand is a mind's act, and the graph "
                             "must know whose", "why": _anchorless(ctx)}
        from src.orchestrator.lineage import file_subagent as _file_subagent
        return await _file_subagent(Actions(await _pool_get()), subagent_id=subagent_id,
                                    actor=ident.agent_id)
    if action == "file_subagents":
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — filing hands is a mind's act, and the graph "
                             "must know whose", "why": _anchorless(ctx)}
        from src.orchestrator.lineage import file_subagents as _file_subagents
        return await _file_subagents(Actions(await _pool_get()), project=project,
                                     dry_run=dry_run, actor=ident.agent_id)
    if action == "retire_governs":
        assert agent_id is not None and repos is not None and because is not None
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — retiring a governs edge is a deliberate act "
                             "on the record", "why": _anchorless(ctx)}
        from src.orchestrator.agents import retire_governs_edges as _retire_governs_edges
        return await _retire_governs_edges(Actions(await _pool_get()), agent_id, repos,
                                           because=because, actor=ident.agent_id)
    if action == "invalidate_works_in":
        assert agent_id is not None and project is not None and because is not None
        ident = await _ident_for(ctx)
        if ident is None:
            return {"error": "mount first — invalidating a works_in edge is a deliberate "
                             "act on the record", "why": _anchorless(ctx)}
        if agent_id == ident.agent_id:
            return {"error": "agent_id names your own mounted identity — use "
                             "seat(action='invalidate_works_in') for that, the self-"
                             "scoped call; this one is for a THIRD-PARTY agent"}
        from src.orchestrator.agents import invalidate_works_in as _invalidate_works_in
        return await _invalidate_works_in(Actions(await _pool_get()), agent_id, project,
                                          because=because, actor=ident.agent_id)
    raise AssertionError(f"action {action!r} passed validation but has no branch")


@mcp.tool()
async def agent(
    action: str, name: str | None = None, agent_id: str | None = None,
    project: str | None = None, seat_generation: int | None = None,
    value: str | None = _UNSET, because: str | None = None, override_live: bool = False,
    execute: bool = False, subagent_id: str | None = None, dry_run: bool = True,
    retract: bool = False, repos: list[str] | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """The agent object-type dispatcher: one tool, many actions over agent identity and
    lineage. See `describe('agent')` for the full per-action shape, or call with a
    wrong or missing param; the error names exactly what that action expects.

    ACTION TABLE, action: what it does (required params beyond action):
      claim_name: self-name your own mounted identity (name)
      correct_project: heal an already-polluted agent's project/seat_generation stamps,
        third-party (agent_id; at least one of project/seat_generation)
      correct_succession: correct an agent's own succeeded_by pointer (agent_id,
        because, value="" or retract=True to retract). Refuses blank because or a live
        target unless override_live=True.
      retire: third-party agent retirement, always releases the held seat (agent_id,
        because)
      fleet_reconcile: the bulk reaper over stale or anonymous fleet mounts (dry run by
        default; execute=True to act)
      fleet_prune: dead_transcript and unclaimed_body mounts, narrower than
        fleet_reconcile (never touches its folding buckets); dry run by default,
        execute=True to act
      file_subagent: file one ephemeral subagent under its spawner (subagent_id)
      file_subagents: the sweep: file_subagent's own resolver over every active
        subagent in scope (project=None is fleet-wide; dry run by default)
      retire_governs: third-party governs-edge retirement (agent_id, repos=[names to
        drop], because). Never moves anything (unlike backfill_agent_project_links'
        own off-head repair, the wrong shape for garbage), never guesses which edges are
        real (the caller names them). Per-repo: a name that doesn't resolve to a known
        software project, or resolves but the agent carries no live governs edge to it,
        is reported in `not_found`/`no_edge` rather than aborting the whole batch.
      invalidate_works_in: third-party works_in duplicate repair (agent_id; project=the
        stale project to drop, the same shared slot correct_project's own `project` uses
        above; because). Refuses agent_id naming your own mounted identity (use
        seat(action='invalidate_works_in') for that, self-scoped and auto-filled). The
        same underlying repair, exposed for a caller acting on someone else's duplicate
        (a mechanical hygiene sweep, a batch-move cleanup) rather than its own.

    Not covered here: self-scoped `retire()` (different auth shape, retires the
    calling agent's own session); `walk_in` (already seat(action='walk_in')); merge/
    unmerge (polymorphic across agent/seat/project, stay named)."""
    return await _agent_impl(
        action, name=name, agent_id=agent_id, project=project,
        seat_generation=seat_generation, value=value, because=because,
        override_live=override_live, execute=execute, subagent_id=subagent_id,
        dry_run=dry_run, retract=retract, repos=repos, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "agent(action='correct_project')",
    "since": "deprecated in a prior cleanup pass",
})
async def correct_agent_house(agent_id: str, project: str | None = None,
                              seat_generation: int | None = None,
                              ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    agent(action='correct_project')."""
    return await _agent_impl("correct_project", agent_id=agent_id, project=project,
                             seat_generation=seat_generation, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "agent(action='retire')",
    "since": "deprecated in a prior cleanup pass",
})
async def retire_agent(agent_id: str, because: str, override_live: bool = False,
                       ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    agent(action='retire'), the same underlying call retire_object(kind='agent') also
    reaches."""
    return await _retire_object_impl(
        "agent", agent_id, because=because, override_live=override_live, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "backfill(target='agent_project_links')",
    "since": "deprecated in a prior cleanup pass",
})
async def backfill_agent_project_links(
    actor: str, dry_run: bool = True, only_bases: list[str] | None = None,
) -> dict[str, Any]:
    """A repair tool for `backfill_agent_project_links`. The write-side fix
    (mint_heir/fold_agent invalidating a predecessor's works_in/governs onto its heir)
    shipped earlier, but the one-time repair for edges already stranded on off-head
    generations before then had no reachable surface; it was importable only.
    `dry_run=True` (default) plans only, listing which off-head agents would give up
    which edges to which living head, with no write. `dry_run=False` writes via the same
    `move_agent_project_links` the write-side fix already uses. `only_bases` scopes a
    write to specific lineages; if omitted, every off-head agent in scope moves.
    Executing the write is an operator's own call, the same class as other repair tools.

    Kept at its original signature (explicit `actor`, no ctx/because) rather than routed
    through the newer dispatcher's shared body: the newer `backfill(target='agent_project_
    links', ...)` derives actor from the caller's mounted identity instead, a deliberate
    behavior change not safe to impose on this deprecated name's existing callers."""
    from src.orchestrator.agents import backfill_agent_project_links as _backfill
    return await _backfill(Actions(await _pool_get()), actor=actor, dry_run=dry_run,
                           only_bases=set(only_bases) if only_bases else None)


@mcp.tool()
async def list_assertions(ref: str, name: str) -> dict[str, Any]:
    """Read-only. Exposes exactly what `retire_assertion`'s own `superseded_id`
    parameter needs and nothing else: every current assertion of `name` on the object
    `ref`, each carrying its own row `id`, the exact value `retire_assertion`'s
    `superseded_id` wants. `dossier()`/`trace_evidence()` both resolve through the
    winning belief or a flat value list; neither surfaces this id. No write, no ranking
    beyond confidence/recency, no bulk scope: the smallest surface that unblocks a
    targeted, per-row `retire_assertion` call."""
    from src.orchestrator.retirement import list_assertions as _list_assertions
    return await _list_assertions(Actions(await _pool_get()), ref=ref, name=name)


async def _abstained_derivations_impl(
    scope: str, link_type: str | None, limit: int,
) -> dict[str, Any]:
    """Shared body: all three READ-ONLY views below query the SAME
    `derivation_abstained_<link_type>` population in capture.py, differing only in
    which structural SQL subset they filter to, a genuine shared call, not a
    cosmetic dispatch. `scope` picks the population:
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
    """Read-only. Every `derive_or_abstain` refusal: `from_id` and every candidate
    resolved to (type, summary), never bare uuids. `link_type=None` pools every lane; a
    value scopes to that one namespaced property. `count` is the true total; `sample` is
    bounded by `limit`, newest-abstained first.

    `scope` narrows which abstentions, structurally: "all" (default), "retryable" (the
    zero-candidate subset, safe to retry as time passes), or "retryable_ambiguous" (the
    2+-candidate subset reduced by elimination alone to exactly one survivor; adds
    `surviving_candidate`/`original_candidate_count` per row, `eliminated_to_zero` for
    the zero-survivor population). Neither retryable scope re-attempts anything; see
    retry_ambiguous_abstentions for the write half both retryable scopes name a
    target for."""
    return await _abstained_derivations_impl(scope, link_type, limit)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "abstained_derivations(scope='retryable')",
    "since": "deprecated in a prior cleanup pass",
})
async def retryable_abstentions(link_type: str | None = None, limit: int = 100) -> dict[str, Any]:
    """Read-only. The zero-candidate subset of abstained_derivations, structurally
    filtered in SQL, not by a condition a caller could widen. A zero-candidate abstention
    means the lookup found nothing yet, which time can change; a 2+-candidate one is a
    genuine ambiguity time cannot resolve, and never appears here. Oldest-abstained
    first. Names which objects are safe to re-attempt; does not re-attempt them. Re-run
    your own lane's lookup on each and call derive_or_abstain(..., retried=True)
    yourself."""
    return await _abstained_derivations_impl("retryable", link_type, limit)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "abstained_derivations(scope='retryable_ambiguous')",
    "since": "deprecated in a prior cleanup pass",
})
async def retryable_ambiguous_abstentions(
    link_type: str | None = None, limit: int = 100,
) -> dict[str, Any]:
    """Read-only. The sibling tool retryable_abstentions doesn't cover: every live 2+-
    candidate abstention whose original candidate set has, by elimination alone (a merge,
    a retire, an invalidation, never a fresh re-derivation), shrunk to exactly one
    active survivor. Structurally safe the same way retryable_abstentions is: only the
    stored candidate ids' current status is rechecked, nothing is re-derived, so the
    original ambiguity refusal is never relaxed. `eliminated_to_zero` (alongside `count`)
    names the different population whose every candidate is now gone: real, but nothing
    to retry-mint from, never folded into `count`/`sample`. Oldest-abstained first; names
    what's safe to retry, never retries it. See retry_ambiguous_abstentions for the
    write half."""
    return await _abstained_derivations_impl("retryable_ambiguous", link_type, limit)


@mcp.tool()
async def retry_ambiguous_abstentions(
    dry_run: bool = True, because: str | None = None, link_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Mints the one surviving candidate for every row retryable_ambiguous_abstentions
    names, via derive_or_abstain(retried=True). Lane-agnostic (no lane-specific lookup
    re-run, only the stored candidate ids' own current status), so this one tool covers
    every lane's ambiguous abstentions, present or future, not just one lane's own.

    Dry run is the default. `dry_run=False` requires a non-blank `because`. Idempotent."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first. A backfill is a deliberate act, and the graph "
                         "must know whose", "why": _anchorless(ctx)}
    return await capture.retry_ambiguous_abstentions(
        Actions(await _pool_get()), actor=ident.agent_id, dry_run=dry_run, because=because,
        link_type=link_type)


async def _current_flags_impl(
    action: str, *, dry_run: bool, limit: int, ctx: Context | None,
) -> dict[str, Any]:
    """Shared body behind `current_flags` and its two hidden single-purpose aliases
    (stale_current_flags/repair_stale_current_flags): one code path, three names. Each
    branch below is copied verbatim from what was that alias's own top-level function
    body before the fold."""
    if action == "inspect":
        from src.orchestrator.retirement import stale_current_flags as _stale_current_flags
        return await _stale_current_flags(Actions(await _pool_get()), limit=limit)
    if action == "repair":
        if not dry_run:
            ident = await _ident_for(ctx)
            if ident is None:
                return {"error": "mount first — a write to the kernel's own materialization "
                                 "is a mind's act, and the graph must know whose",
                        "why": _anchorless(ctx)}
            actor = ident.agent_id
        else:
            actor = None
        from src.orchestrator.retirement import repair_stale_current_flags as _repair
        return await _repair(Actions(await _pool_get()), dry_run=dry_run, limit=limit,
                             actor=actor)
    return {"error": f"unknown action {action!r} — one of inspect/repair"}


@mcp.tool()
async def current_flags(
    action: str, dry_run: bool = True, limit: int = 50, ctx: Context | None = None,
) -> dict[str, Any]:
    """A kernel-integrity tool for the current_assertions.is_current flag, two actions
    over the same anomaly: every row where `is_current=true` (a maintained flag) yet a
    real `supersedes` foreign key already points at it from another assertion. A stale
    flag current_assertions is still trusting.

    `action='inspect'`: pure read, finds the anomaly, fixes nothing. `count` is the true
    total population (never capped); `sample` is bounded by `limit` (default 50),
    oldest-observed first. Not a per-object lookup like list_assertions.

    `action='repair'`: the backfill for `inspect`'s own population. `dry_run=True`
    (default): list-only, names how many rows would flip and their ids, writes nothing,
    safe to call without being mounted. `dry_run=False` is an operator's own call, never
    automatic: flips `is_current=false` on up to `limit` (pass a higher value than the
    shared default of 50 for a real repair pass) stale rows in one batched update,
    oldest-observed first. Batched because the live population is large; walk it in
    repeated calls, not one update touching all of it. Idempotent: a row already flipped
    drops out on its own, so re-running after a partial run or a failure is always safe."""
    return await _current_flags_impl(action, dry_run=dry_run, limit=limit, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "current_flags(action='inspect')",
    "since": "deprecated in a prior cleanup pass",
})
async def stale_current_flags(limit: int = 50) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    current_flags(action='inspect')."""
    return await _current_flags_impl("inspect", dry_run=True, limit=limit, ctx=None)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "current_flags(action='repair')",
    "since": "deprecated in a prior cleanup pass",
})
async def repair_stale_current_flags(
    dry_run: bool = True, limit: int = 500, ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    current_flags(action='repair')."""
    return await _current_flags_impl("repair", dry_run=dry_run, limit=limit, ctx=ctx)


@mcp.tool()
async def retire_assertion(ref: str, name: str, superseded_id: int, value: str, because: str,
                           ctx: Context | None = None) -> dict[str, Any]:
    """The cross-source supersede: retires another source's assertion explicitly, the
    class assert_property's own automatic same-source-only supersession cannot reach: a
    peer's correction of another agent's bad self-declaration. Without this, a
    different-source correction and the wrong original both stay "current" at once.

    Deliberately narrow: retires one named assertion by id, never "whatever's current
    now"; the caller must already know exactly which row is wrong. `because` is
    required. Refuses on: `ref` unresolved; `superseded_id` not a `name` assertion
    on that object; already superseded; blank `because`."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first. A retirement is a deliberate act, and the graph "
                         "must know whose", "why": _anchorless(ctx)}
    from src.orchestrator.retirement import retire_assertion as _retire_assertion
    return await _retire_assertion(Actions(await _pool_get()), ref=ref, name=name,
                                   superseded_id=superseded_id, value=value, because=because,
                                   actor=ident.agent_id)


@mcp.tool()
async def retire_link(from_ref: str, to_ref: str, link_type: str, because: str,
                      ctx: Context | None = None) -> dict[str, Any]:
    """The generic link retraction: `retire_assertion`'s own sibling for the other half
    of "retract a wrongly-minted X": a link, not a property. Object-type-agnostic, any
    (from, to, type) triple on any object types. record_decision's own answers=/
    grounded_by=, thread(action='resolve')'s own resolved_by, and every other per-type
    tool keep minting links exactly as before; this is the general tool for when one of
    those mints the wrong edge (a fuzzy-substring mis-citation hitting an unrelated
    thread is the kind of live case that motivated this).

    Never a delete: `Actions.invalidate_link` stamps `valid_until`, event-sourced (an
    audit row plus an outbox `link_invalidated` event carrying `because` as `reason`,
    the compensating record itself); the row stays exactly where it was created, in
    whose name, and why. `because` is required. Refuses on: either ref unresolved, or no
    currently-active link of `link_type` exists on that exact triple (never a silent
    no-op; a caller here almost certainly meant a real edge, so a mismatched ref/type
    surfaces as a refusal, not a quiet success)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first. A retirement is a deliberate act, and the graph "
                         "must know whose", "why": _anchorless(ctx)}
    from src.orchestrator.retirement import retire_link as _retire_link
    return await _retire_link(Actions(await _pool_get()), from_ref=from_ref, to_ref=to_ref,
                              link_type=link_type, because=because, actor=ident.agent_id)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='set_attended')",
    "since": "deprecated in a prior cleanup pass",
})
async def set_seat_attended(seat_id: str, attended: str, because: str,
                            ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to seat(action='set_attended')."""
    return await _seat_impl("set_attended", target=seat_id, attended=attended,
                            because=because, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='rename')",
    "since": "deprecated in a prior cleanup pass",
})
async def rename_seat(seat_id: str, new_handle: str, because: str,
                      ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to seat(action='rename')."""
    return await _seat_impl("rename", target=seat_id, new_handle=new_handle, because=because,
                            ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='bind_tree')",
    "since": "deprecated in a prior cleanup pass",
})
async def bind_seat_tree(seat_id: str, tree_cwd: str, because: str,
                         ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to seat(action='bind_tree')."""
    return await _seat_impl("bind_tree", target=seat_id, tree_cwd=tree_cwd, because=because,
                            ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='reissue_seat_dir')",
    "since": "deprecated in a prior cleanup pass",
})
async def reissue_office(
    seat_id: str, because: str, adopt: bool = False, ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    seat(action='reissue_seat_dir')."""
    return await _seat_impl("reissue_seat_dir", target=seat_id, because=because,
                            adopt=adopt, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "agent(action='file_subagent')",
    "since": "deprecated in a prior cleanup pass",
})
async def file_subagent(subagent_id: str, ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    agent(action='file_subagent')."""
    return await _agent_impl("file_subagent", subagent_id=subagent_id, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "agent(action='file_subagents')",
    "since": "deprecated in a prior cleanup pass",
})
async def file_subagents(project: str | None = None, dry_run: bool = True,
                         ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    agent(action='file_subagents')."""
    return await _agent_impl("file_subagents", project=project, dry_run=dry_run, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "deprecated in a prior cleanup pass",
})
async def unwitnessed_spawns(agent_id: str | None = None,
                             ctx: Context | None = None) -> dict[str, Any]:
    """A self-audit: every live `spawned_by` child of `agent_id` for which no
    `subagents/agent-<id>.jsonl` file has ever materialized anywhere on disk, that is,
    what is executing under this identity that was never spawned by it as far as the
    record shows. Omit `agent_id` to audit your own identity (a seat's own check); name
    another to audit theirs (an operator's check, or a peer's; a pure read, never gated
    the way a write would be).

    A hit is a lead, not a verdict: one earlier specimen was retracted after
    investigation, when a subagent spawned and briefed as a different identity turned
    out to be correctly parented to its real spawner, not a graph defect. One live
    hypothesis was checked against a real transcript before shipping this tool: whether
    a sidechain turn could be recorded inline in the parent's own transcript rather than
    as a separate subagents file, which would make a hit here a false alarm. That
    specific escape hatch did not explain the checked specimens, but a caller should not
    assume it can never apply elsewhere without checking the same way. This tool reads;
    it never files or folds anything it finds."""
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


async def _fold_review_impl(
    action: str, *, candidate_id: int | None, decision: str | None, ctx: Context | None,
) -> dict[str, Any]:
    """Shared implementation behind `fold_review` and its two hidden single-purpose
    aliases (fold_candidates/resolve_fold): one code path, three names. Each branch
    below is copied verbatim from what was that alias's own top-level function body
    before the two were folded into this shared dispatcher."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first", "why": _anchorless(ctx)}
    if action == "list":
        from src.orchestrator.folds import find_agent_fold_candidates
        return await find_agent_fold_candidates(await _pool_get())
    if action == "resolve":
        if candidate_id is None or decision is None:
            return {"error": "action='resolve' requires both candidate_id and decision"}
        from src.orchestrator.folds import resolve_fold_candidate
        return await resolve_fold_candidate(Actions(await _pool_get()),
                                            candidate_id=candidate_id, decision=decision,
                                            actor=ident.agent_id)
    return {"error": f"unknown action {action!r} — one of list/resolve"}


@mcp.tool()
async def fold_review(
    action: str, candidate_id: int | None = None, decision: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """A propose-then-judge review tray, two actions over the same agent-identity-merge
    tray.

    `action='list'`: sweep the registry and disk for anonymous agents that evidence says
    were never distinct identities (view-aliases: a mount row with no transcript and no
    daemon receipt, co-resident with a session that has a live body; restart-mints: an
    anonymous mount in a named lineage's own home) and queue them as review-gated merge
    candidates. Proposals only; nothing merges here. Returns the pending tray (score-
    ranked, each with its cited signals); judge each with `action='resolve'`. Rejected
    pairs are remembered and never re-proposed. Also carries `unresumed_heads`, a
    separate class never resolved via this tool; a human call each time.

    `action='resolve'`: judge one proposal from the tray (`candidate_id`, `decision`
    both required). `decision='merged'` executes the full fold (mail, mount rows,
    threads land on the living head), operator-gated and enforced: inherits fold_agent's
    own operator-actor gate unchanged, never a second copy to drift out of sync.
    `decision='rejected'` links the pair as not the same identity, never re-proposed,
    open to any mounted caller deliberately: a rejection judges two things are not the
    same identity, carrying none of a merge's blast radius."""
    return await _fold_review_impl(action, candidate_id=candidate_id, decision=decision,
                                   ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "fold_review(action='list')",
    "since": "deprecated in a prior cleanup pass",
})
async def fold_candidates(ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    fold_review(action='list')."""
    return await _fold_review_impl("list", candidate_id=None, decision=None, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "fold_review(action='resolve')",
    "since": "deprecated in a prior cleanup pass",
})
async def resolve_fold(candidate_id: int, decision: str,
                       ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    fold_review(action='resolve')."""
    return await _fold_review_impl("resolve", candidate_id=candidate_id, decision=decision,
                                   ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "agent(action='fleet_reconcile')",
    "since": "deprecated in a prior cleanup pass",
})
async def fleet_reconcile(execute: bool = False,
                          ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    agent(action='fleet_reconcile')."""
    return await _agent_impl("fleet_reconcile", execute=execute, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='establish_seat_dir')",
    "since": "deprecated in a prior cleanup pass",
})
async def establish_office(seat: str, ctx: Context | None = None) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    seat(action='establish_seat_dir')."""
    return await _seat_impl("establish_seat_dir", target=seat, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "deprecated in a prior cleanup pass",
})
async def lift(ref: str, handle: str, subagent_id: str | None = None,
               subagent_type: str | None = None, session_anchor: str | None = None,
               ctx: Context | None = None) -> dict[str, Any]:
    """Pull a named, idle, unmanaged session out of its ad hoc working directory and
    into a clean managed seat directory: import a running-but-unmanaged instance,
    preserve its state, give it a clean managed identity. Composes `identify_agent(ref)`
    to resolve the target (refuses on 0 matches, on more than 1: an ambiguous multi-
    tenant working directory, name a specific `agent:` id instead, and on a live match:
    moving a live seat splits its running session's history between two homes, close
    its tab first), `claim_name(handle)` (propagating its own real refusals: a visitor,
    a name held live elsewhere, a cross-project collision), and `establish_office` (the
    actual move). `ref` accepts anything `identify_agent()` does: an `agent:` id, a
    `seat:` id, a bare handle, or an absolute working-directory path. The result's
    `verified` field is a fresh post-write `identify_agent()` read, never an echo of
    what the earlier steps each individually claimed.

    Lifting your own session is not just refused, it is structurally impossible: your
    own session's `last_seen` is kept perpetually fresh by your own terminal's
    heartbeat, so you can never observe yourself as idle from inside a call; `lift()`
    always targets a different, already-idle session, never the caller's own."""
    ident = await _ident_for(ctx, session_anchor)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first: a lift is a deliberate "
                         "act, and the record must know whose", "why": _anchorless(ctx)}
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    from src.orchestrator.lift import lift as _lift
    return await _lift(await _pool_get(), ref, handle, actor=actor)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='walk_in')",
    "since": "replaced by the unified seat dispatcher",
})
async def walk_in(
    handle: str, wants_office: bool, cwd: str | None = None, job_dir: str | None = None,
    model: str | None = None, subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to seat(action='walk_in')."""
    return await _seat_impl("walk_in", handle=handle, wants_office=wants_office, cwd=cwd,
                            job_dir=job_dir, model=model, subagent_id=subagent_id,
                            subagent_type=subagent_type, session_anchor=session_anchor,
                            ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='mint')",
    "since": "replaced by the unified seat dispatcher",
})
async def mint_seat(
    handle: str, project: str | None = None, model: str | None = None,
    house: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to seat(action='mint')."""
    return await _seat_impl("mint", handle=handle, project=project, model=model,
                            house=house, ctx=ctx)


@mcp.tool()
async def bootstrap(cwd: str, ctx: Context | None = None) -> dict[str, Any]:
    """Onboard a project by migrating its markdown memory (CLAUDE.md build log, DESIGN.md,
    memory essays) into the shared graph as retrieval-sized reference nodes, so its history
    becomes a bounded query (consult_canon) instead of bloat re-injected into every context.
    Registers the project and returns a suggested boot-sector CLAUDE.md. This tool does not
    touch your files: review the suggestion, write it yourself, archive the originals.
    Public docs (README/ARCHITECTURE) are left alone; they are human-facing exports, not memory.
    Every write is stamped with your mounted identity (or "session"), never a fixed literal."""
    from src.orchestrator.bootstrap import bootstrap_project

    source = await _source_for(ctx)
    return await bootstrap_project(Actions(await _pool_get()), cwd, source=source)


# --- write-back: capture what you decided / what's still open ---

# THE FAIL-OPEN PROMISE, ENFORCED: record_decision's and record_practice's own prior-art
# search has always been documented as fail-open, meaning a search hiccup must never
# block recording the decision itself. But the try/except around it only ever caught a
# RAISED exception, never a HANG, so the promise was true for errors and false for
# silence. semantics.py's own fix (Model2VecEmbedder's bounded, sticky load) closes the
# specific hang that was actually measured live; this is the outer, whole-call bound as
# defense in depth. Any OTHER slow step in the fused search pipeline (DB contention under
# fleet load, a lexical path with no supporting index) gets the same honest, fast
# fail-open instead of riding out an external 300s timeout with no diagnosis.
_PRIOR_ART_SEARCH_TIMEOUT_S = 15.0


async def _surface_prior_art(
    pool: asyncpg.Pool, text: str, *, exclude: set[uuid.UUID] | None = None,
    repo: str | None = None, actor: str | None = None,
) -> list[dict[str, Any]]:
    """THE READ-SIDE HOP: makes "read the graph before re-deriving" an architectural
    property rather than a convention callers have to remember. record_decision/
    record_practice already run this exact search at write time; extracted here,
    unchanged, so a caller that isn't a write (send(), currently) can run the SAME
    search rather than a second matcher. Same 15s timeout plus fail-open (a search
    hiccup or hang returns [] rather than blocking the caller) as both write-time
    callers. Same Thread-kind widening: a Thread hit only counts as prior art when it's
    an OPEN kind='obligation' row, or a kindless legacy row sharing this call's own
    `repo` (capture._open_obligation_thread_ids). Never a resolved thread (nothing to
    warn against re-doing) and never a kindless row admitted with no repo at all."""
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
    except Exception:  # noqa: BLE001, never block the caller on a search-side failure/hang
        return []


async def _obsoleted_standing_practice(
    pool: asyncpg.Pool, obsoletes: list[str] | None, prior: list[dict[str, Any]],
) -> dict[str, str] | None:
    """Does one of `obsoletes=`'s own quoted workaround texts name the SAME words as a
    standing Practice the prior-art search already surfaced? `refutes=` already gets
    this exact treatment (`refute_id`, below); `obsoletes=` never did, silently falling
    through to the generic re-derivation/contradiction-cues wording, which names
    nothing about the obsoletion actually requested. Scans every Practice-typed hit in
    `prior` (not just `prior[0]`: `prior_art_from_hits`' own reserved slot means the
    best-ranked Practice need not be first), resolves each by its own short id back to
    the full object
    (`capture._find_practice`, `require_identifier=True`: the hit's id is already
    identifier-shaped, never a prose match here), and compares its `statement` against
    each obsoletes string under the SAME canon-key normalization `refute_id`'s own
    Superstition lookup already uses (whitespace-collapsed, lowercased). Returns the
    matched hit's short id plus the full uuid, or None."""
    if not obsoletes:
        return None
    keys = {" ".join(s.split()).lower() for s in obsoletes if s and s.strip()}
    if not keys:
        return None
    for hit in prior:
        if hit.get("type") != "Practice":
            continue
        pid = await capture._find_practice(pool, hit["id"], require_identifier=True)
        if pid is None:
            continue
        statement = await pool.fetchval(
            "SELECT val.value #>> '{}' FROM current_assertions val "
            "WHERE val.object_id=$1 AND val.name='statement' "
            "ORDER BY val.confidence DESC, val.observed_at DESC LIMIT 1", pid)
        if statement and " ".join(statement.split()).lower() in keys:
            return {"id": hit["id"], "full_id": str(pid)}
    return None


# THE UNLINKED-BECAUSE ESCAPE HATCH'S TWO POPULATIONS MUST STAY SEPARABLE: a Decision
# whose ONLY requested connectivity is an extension-link param (obsoletes=/confirms=/
# refutes=/implements=/rediscovers=/bears_on=, which mint AFTER capture.record_decision's
# own atomic block) must not fall through unlinked_because's escape hatch
# indistinguishably from a genuinely standalone, disconnected write. Never typed by a
# caller: this string is the system's own signature on it.
_EXTENSION_LINK_PENDING_REASON = (
    "extension-link-pending, set automatically by the system: this write's only "
    "requested connectivity is obsoletes=/confirms=/refutes=/implements=/rediscovers=/"
    "bears_on=/narrows=/cites=, which are created after this transaction and cannot "
    "satisfy the linkage requirement at its own commit point")


# WRITE-VERB RESULT DIET: a nag is advice a caller mostly doesn't act on the same turn,
# and the old shape paid its full prose on EVERY firing. Collapsed to a short code in the
# result's own `nags` list; describe('nags') (or describe('nags:<code>')) is the one
# place the full text lives now, a deliberate lookup rather than a reflexive re-explain
# each call. Same convention consult_canon('record_decision') already uses for
# per-parameter detail: this is that same move applied to advisory nags specifically.
_NAG_CATALOG: dict[str, str] = {
    "protocol": (
        "this decision reads like a measurement and its `protocol` field is empty. "
        "Record the exact invocation (command line, seeds, thresholds, bucket edges) so "
        "someone else can rerun it instead of re-deriving it. Re-run record_decision "
        "with the same summary plus protocol to add it to this same decision "
        "(idempotent)"),
    "assertion": (
        "this reads like a flat claim about code or system behavior with no hedge "
        "acknowledging it might be wrong. If you re-read the thing you're describing "
        "this turn, say so. A citation alone doesn't clear this: it still fires on a "
        "cited claim if the citation itself wasn't re-checked for what it actually "
        "proves"),
}

# THE SEAT MANUAL, MOVED HERE FROM THE SLASH FILE: commands/seat.md used to carry the
# full per-verb prose directly in the prompt on EVERY /seat invocation, 12.9 KB paid
# regardless of which one verb was actually being run. Same move _NAG_CATALOG above
# already made for advisory nags: describe('seat') now lists the verbs, describe(
# 'seat:<verb>') holds one verb's full text, and the slash file shrinks to a bare
# subcommand list with a pointer here. Forward-referenced by four other dispatcher
# docstrings' own "describe('<name>') for the full per-action shape": this is the first
# of those to actually back the reference with real data.
_SEAT_MANUAL: dict[str, str] = {
    "new": (
        "new <handle> [path] [--project P]: create a self-managed seat, a fresh code "
        "workspace plus identity, with no manager, ever. `found_seat` has no MCP entry point, "
        "so this shells out to `osiris new` verbatim (the same approach `launch` "
        "uses; do not reimplement it here). Refuse rather than guess: `osiris new` "
        "itself still defaults `project` to `handle` when `--project` is omitted, and "
        "fixing that default is out of scope for this command. So if `--project` is "
        "missing, ask the caller to confirm it explicitly before running anything, "
        "especially when `handle` reads like a person's name or nickname rather than "
        "a project name (a seat named after its occupant has silently become its "
        "project and its workspace path before). Never invent a plausible-looking "
        "project name yourself. `osiris new` already prints a warning to stderr when "
        "it is about to default the workspace path away from the caller's actual "
        "working directory: surface that warning verbatim, don't paraphrase it away. "
        "This is not the same operation as `walk-in`, even though both create a "
        "self-managed seat in plain English; do not substitute one for the other."),
    "walk-in": (
        "walk-in <handle> [--wants-office]: give the calling agent itself a durable "
        "identity, with no new workspace, ever (for an agent session that has no "
        "workspace of its own). Composes the `walk_in` MCP tool directly. Only "
        "meaningful for an agent invoking this on its own behalf. If a human runs "
        "this with no mounted session behind it, say so and point at `new` instead."),
    "mint": (
        "mint <handle> --manager <seat> [--project] [--model]: create a managed "
        "worker seat under an existing one. Composes the `mint_seat` MCP tool "
        "(`handle`, `project`, `model`, `house`; there is no `manager` parameter on "
        "the tool itself. An agent caller lets it infer the manager from its own held "
        "seat. An operator caller supplies `--manager` explicitly, or if omitted, it "
        "is inferred the same way `osiris mint-seat` does, as the sole seat in the "
        "target project, and refuses rather than guesses among several)."),
    "launch": (
        "launch <handle> [--model]: give a seat a running session. This always "
        "starts a fresh session, never resumes one (the verb is the property, not a "
        "flag). There are two deliberate backends, not a bug: the `launch` MCP "
        "tool's own docstring says outright that the operator never calls it "
        "directly, and it requires a mounted agent identity with a downward "
        "`managed_by` edge to the target. An agent caller composes the `launch` MCP "
        "tool directly; a human or operator caller shells out to `osiris launch "
        "<handle>` verbatim instead. This split is permanent: the CLI path spawns "
        "the session directly under operator trust, the MCP tool gates on "
        "`managed_by` under agent-to-agent trust, genuinely different authorization "
        "models that cannot be collapsed into one shared function without losing one "
        "of them. It can refuse on a fabricated project: before spawning, launch "
        "resolves the project it would boot into (pin, then charter, then lineage, "
        "never house). When the seat's charter names exactly one real repository and "
        "that resolution disagrees with it, launch refuses outright rather than "
        "starting a session in the wrong home. The remedy is `transition`, not "
        "`move`: run `/seat transition <handle>` first (dry-run shows the plan), "
        "confirm it, then `--apply`; only once the seat is genuinely bound to both "
        "places (mounted at the real repo, not just chartered for it) does `launch` "
        "stop refusing. Never suggest `move`/`heal-anchor` for this refusal; they "
        "fix a different kind of disagreement (anchor_cwd corruption, not a "
        "fabricated project label)."),
    "resume": (
        "resume <handle> [--model]: continue a seat's own dormant session; it never "
        "falls through to starting a fresh one (launch's sibling verb, same rule). "
        "Same two-backend shape as `launch`: an agent caller composes the `resume` "
        "MCP tool directly; a human or operator caller shells out to `osiris resume "
        "<handle>` verbatim. Refuses clearly (`refused-nothing-to-resume`) rather "
        "than guessing when nothing is resumable; use `launch` for that instead, a "
        "deliberate, separate act."),
    "stop": (
        "stop <handle> [--reason]: end a live session. Both callers already reach "
        "the identical `stop_seat` function today (the `stop` MCP tool, a hidden "
        "deprecated alias of `seat(action='stop')`, or `osiris stop <handle>`). "
        "Prefer the MCP tool when mounted, shell out otherwise. No difference in "
        "behavior between the two paths."),
    "move": (
        "move <handle> <new_cwd>: relocate a seat's whole footprint (mount rows, "
        "harness metadata, the `.osiris` pin). Composes the `rebind_seat` MCP tool. "
        "The anchor invariant: `anchor_cwd` is identity, always `<office_root>/"
        "<handle>`. `rebind_seat` no longer writes it for a `new_cwd` outside the "
        "office root (the receipt says `anchor_cwd_skipped` and names why). So "
        "`move` genuinely relocates identity only when `new_cwd` is under the office "
        "root (a real office migration, rare); anywhere else it's a footprint or "
        "tree move and the seat's `anchor_cwd` stays exactly where it was. Say so "
        "plainly if the caller seems to expect otherwise: seats have broken their "
        "own anchor this exact way before, by rebinding themselves to their own code "
        "repo's working directory."),
    "bind-tree": (
        "bind-tree <handle> <tree_cwd> --because: bind a seat's own isolated code "
        "checkout, deliberately distinct from its office (never collapse the two). "
        "Composes `bind_seat_tree`."),
    "heal-anchor": (
        "heal-anchor <handle> --because [--apply]: repair a seat whose "
        "`anchor_cwd` is corrupted (more than one current value, or one that never "
        "got asserted at all; this has happened to several seats before). Composes "
        "the `heal_seat_anchor` MCP tool (pass `seat_id=<handle>` for a third-party "
        "seat; the old `heal_seat_anchor_third_party` name is a hidden deprecated "
        "alias of it), or `osiris heal-seat-anchor <handle> --because <reason> "
        "[--apply]` as the CLI twin, same function either way. Asserts the "
        "invariant office path (`<office_root>/<handle>`) as the sole current "
        "anchor, collapsing every stray value in one call. Refuses rather than "
        "guesses if the seat has no handle or its office directory doesn't exist on "
        "disk yet (that is `new` or `walk-in`'s job, never this verb's). `dry_run` "
        "defaults true; the caller must confirm before passing "
        "`--apply`/`dry_run=False`. Run `roster` first if unsure which seats need "
        "this."),
    "correct-agent-project": (
        "correct-agent-project <agent> [--project] [--seat-generation]: fix an "
        "already-incorrect agent's own project or seat_generation stamps, for a "
        "third party (unlike `correct-house`, which is self-scoped and has no "
        "console entry point for that reason). Composes `correct_agent_house` (hidden from "
        "`list_tools()` since it was retired with no measured traffic at the time, "
        "but still fully callable as a deprecated alias). Has a CLI command: `osiris "
        "correct-agent-project <agent> [--project P] [--seat-generation N]` "
        "(`correct-agent-house` still works this release as a deprecated alias, "
        "kept as one consistent naming scheme). `<agent>` accepts a claimed handle "
        "or a raw agent id."),
    "retire-agent": (
        "retire-agent <agent> --because [--override-live]: retire one specific "
        "agent identity for a third party, distinct from `retire` (which ends a "
        "seat's role; this ends one agent identity, any target; `actor` records who "
        "is attributed with the action, not who is authorized to take it). Composes "
        "the `retire_agent` MCP tool. No CLI command yet (declared but not built). "
        "Always releases the target's held seat and mount rows on success; refuses "
        "clearly on a target seen active within the last 15 minutes unless "
        "`--override-live`."),
    "heal-seat-transcript": (
        "heal-seat-transcript <handle> <source_paths...> --because [--apply]: "
        "splice a seat's session, fragmented across multiple project slugs by a "
        "mid-session working-directory move, back into one file at its own office "
        "slug. Composes the `heal_seat_transcript` MCP tool. No CLI command yet "
        "(declared but not built; this was the original case that this whole "
        "safeguard exists to prevent recurring). `source_paths` are the original "
        "fragments, in chain order (oldest first). `dry_run` defaults true "
        "(`--apply` to write). Never touches a Seat row, anchor_cwd, or any source "
        "transcript; that's `heal-anchor`'s job, a different route. "
        "(`reconcile-merge` and `fleet-reconcile`, two related repair tools, are "
        "deliberately not composed under /seat: the first belongs to a "
        "merge/unmerge context this file doesn't own, the second acts fleet-wide, "
        "not on one seat.)"),
    "transition": (
        "transition <handle> [--fabricated-project P] [--real-project P] [--repos R] "
        "--because [--apply]: move a seat's project binding from a fabricated "
        "handle-project to the real repository it already works in, as one "
        "composed action (this automates a sequence that used to be run by hand). "
        "Composes the `transition_seat_project` MCP tool for a self-caller, or "
        "`osiris transition-seat-project <handle> ...` for a third-party or "
        "operator caller, same function either way. `--fabricated-project` defaults "
        "to the seat's own handle; `--real-project` disambiguates only when the "
        "seat carries more than one other live works_in edge, and is auto-picked "
        "otherwise. Precondition: the seat must already be mounted at the real "
        "repository's working directory (a live, second works_in edge already "
        "present); this verb transitions an already-dual binding, it does not "
        "create the first edge itself. Deliberately never calls `move`/"
        "`rebind_seat`: the anchor invariant already pins `anchor_cwd` to the "
        "office path permanently, and that call has broken seats' anchors this "
        "exact way before, so it is not repeated here. `dry_run` defaults true; "
        "show the plan (`invalidate_works_in`/`correct_pin_value`/`set_charter`, "
        "each `null` when already correct) before passing `--apply`."),
    "retire": (
        "retire <handle> --reason: end a seat's role for good. Two steps, in "
        "order, never skip the first: (1) `retire_seat`, the graph action that "
        "marks the seat permanently closed; it refuses on an active holder or an "
        "active peer_of edge, so resolve those first. (2) `sweep_seat_disk(handle, "
        "dry_run=True, because=<reason>)`, which composes both office and "
        "workspace cleanup under one call with its own containment, ambiguity, and "
        "live-body guards. `sweep_seat_disk` itself requires the seat to already be "
        "retired (or have no Seat row at all) before it will touch disk. Always "
        "dry-run `sweep_seat_disk` first and show both halves' receipts "
        "(`office`/`workspace`) separately before asking whether to pass "
        "`dry_run=False`; they can legitimately disagree, so never collapse them "
        "into one verdict. `--reason` maps to both `retire_seat`'s `reason` and "
        "`sweep_seat_disk`'s `because`."),
    "roster": (
        "roster [--repo]: see who's alive, and whether any binding disagrees with "
        "itself. Composes the `roster` MCP tool. Render, per seat: occupancy, "
        "chartered_repos, pin, and `pin_charter_agreement` whenever it reads "
        "'disagree', flagged plainly ('seat X: pin says P, charter says C, these "
        "disagree and are not resolved automatically; `/seat move` or `/charter "
        "set` fixes it'). Never read a near-empty `~/code/<handle>` directory as "
        "evidence that a seat is abandoned scaffolding; real, active seats have "
        "been misread exactly that way before. When a seat's status is worth a "
        "second look, compose `dossier(<seat>)` too and show its charter's real "
        "activity before calling anything dead. Standing rule across every verb: "
        "never pick a winner on a disagreement. `pin_charter_agreement=='disagree'`, "
        "roster's own `conflict`/`near_misses`, or a near-miss handle refusal all "
        "get surfaced with the repair verb named, never silently resolved. "
        "`pin_charter_agreement=='n/a'` (unset pin, or no charter at all) is a "
        "valid, ordinary state, never rendered as a problem."),
}


def _slim_prior_art(prior: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One-line id+short-summary, not the full {id,type,summary,grade,via} shape: part
    of the write-verb receipt diet above. The caller acting THIS turn needs enough to
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
    operator_authorized: bool = False,
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
    `unlinked_because` supplies a real reason for a decision with no other link.
    `operator_authorized=True`: this decision carries the operator's own authority, not
    just this caller's own scoped judgment, and creates a `ruled_by` edge to the
    operator's Person object. This is an explicit, self-declared act, never inferred
    from who's calling; set it only when this decision really is the operator's ruling.
    consult_canon('record_decision') for more.

    `content_landed`: present when `rationale`/`protocol` was passed. It is a read-back
    confirming your text is the current value (a later write can silently win the
    tie-break). If `false`, see `content_landed_note` and amend_decision.

    Any error on this call, including a dropped connection or a timeout with no
    response, is safe to retry with the same `summary`. It is idempotent: an exact
    rewrite, or with `repo` given a near-duplicate reword, reuses the same decision
    (`reused_existing_decision` in the receipt names when that happened). Retrying
    never creates a duplicate."""
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
    # require_identifier=True: an identifier-shaped arg like a bare local task number must
    # REFUSE rather than fall through to a prose/summary-substring search, the same rule
    # resolves='s own fix already applied. supersedes/implements/refutes/confirms BURY,
    # CONVERT, or LINK the record they name, never a merely-read act, so they carry the
    # identical addressing-act risk resolves= was fixed for.
    if supersedes:  # resolve BEFORE recording: a correction that can't name its target
        old = await capture._find_decision(pool, supersedes, require_identifier=True)
        if old is None:
            return {"error": f"supersedes matched no decision: {supersedes!r}. Quote its "
                             "UUID, canonical, or 8-char short id (a prose match is not "
                             "accepted here; an addressing act refuses rather than guesses)"}
    impl_id: uuid.UUID | None = None
    if implements:  # same resolve-before-record strictness as supersedes
        impl_id = await capture._find_decision(pool, implements, require_identifier=True)
        if impl_id is None:
            return {"error": f"implements matched no decision: {implements!r}. Quote its "
                             "UUID, canonical, or 8-char short id (a prose match is not "
                             "accepted here; an addressing act refuses rather than guesses)"}
    refute_id: uuid.UUID | None = None
    if refutes:  # same strictness: a refutation that can't name its target has refuted nothing
        refute_id = await capture._find_practice(pool, refutes, require_identifier=True)
        if refute_id is None:
            return {"error": f"refutes matched no practice: {refutes!r}. Quote its UUID, "
                             "canonical, or 8-char short id (a prose match is not accepted "
                             "here; an addressing act refuses rather than guesses)"}
    # resolve BEFORE recording, same discipline as supersedes: a single string keeps the
    # original all-or-nothing strictness; a list resolves each entry independently and
    # reports (never raises) on a miss, so one typo can't veto the rest of the set.
    # require_identifier=True: resolves is a CLOSING act, so a bare prose ref refuses here
    # rather than falling through to a fuzzy summary-substring match.
    answered: list[uuid.UUID] = []
    receipt: list[dict[str, str]] = []
    single_summary: str | None = None
    if isinstance(resolves, list):
        for ref in resolves:
            tid = await capture._find_thread(pool, ref, require_identifier=True)
            if tid is None:
                receipt.append({"ref": ref, "matched": "false",
                                "note": "matched no thread. Quote its UUID, canonical, "
                                        "or 8-char short id (a prose match is not accepted "
                                        "here)"})
                continue
            answered.append(tid)
            summ = await capture._thread_summary(pool, tid)
            receipt.append({"ref": ref, "matched": "true", "id": str(tid)[:8],
                            "summary": summ or ""})
    elif resolves:  # same strictness: a decision that miscites its question has not settled it
        single = await capture._find_thread(pool, resolves, require_identifier=True)
        if single is None:
            return {"error": f"resolves matched no thread: {resolves!r}. Quote its UUID, "
                             "canonical, or 8-char short id (a prose match is not accepted "
                             "here; an addressing act refuses rather than guesses)"}
        answered.append(single)
        single_summary = await capture._thread_summary(pool, single)
    # confirms resolves the same best-effort way as resolves's list form: one bad ref
    # must not veto the practices that DID match
    confirm_ids: list[uuid.UUID] = []
    confirm_receipt: list[dict[str, str]] = []
    for ref in confirms or []:
        pid = await capture._find_practice(pool, ref, require_identifier=True)
        if pid is None:
            confirm_receipt.append({"ref": ref, "matched": "false",
                                    "note": "matched no practice. Quote its UUID, "
                                            "canonical, or 8-char short id (a prose "
                                            "match is not accepted here)"})
            continue
        confirm_ids.append(pid)
        confirm_receipt.append({"ref": ref, "matched": "true", "id": str(pid)[:8]})
    # rediscovers resolves the same best-effort way as confirms: one bad ref must not
    # veto the earlier decisions that DID match
    rediscover_ids: list[uuid.UUID] = []
    rediscover_receipt: list[dict[str, str]] = []
    for ref in rediscovers or []:
        rdid = await capture._find_decision(pool, ref, require_identifier=True)
        if rdid is None:
            rediscover_receipt.append({"ref": ref, "matched": "false",
                                       "note": "matched no decision. Quote its UUID, "
                                               "canonical, or 8-char short id (a prose "
                                               "match is not accepted here)"})
            continue
        rediscover_ids.append(rdid)
        rediscover_receipt.append({"ref": ref, "matched": "true", "id": str(rdid)[:8]})
    # narrows resolves the same best-effort way as rediscovers: one bad ref must not
    # veto the earlier decisions that DID match
    narrow_ids: list[uuid.UUID] = []
    narrow_receipt: list[dict[str, str]] = []
    for ref in narrows or []:
        nid = await capture._find_decision(pool, ref, require_identifier=True)
        if nid is None:
            narrow_receipt.append({"ref": ref, "matched": "false",
                                   "note": "matched no decision. Quote its UUID, "
                                           "canonical, or 8-char short id (a prose "
                                           "match is not accepted here)"})
            continue
        narrow_ids.append(nid)
        narrow_receipt.append({"ref": ref, "matched": "true", "id": str(nid)[:8]})
    # cites resolves the same best-effort way as rediscovers/narrows (added after a case
    # where bears_on refused a citation and narrows was the wrong relation for it): the
    # declared form of the prose-citation miner's own edge
    cite_ids: list[uuid.UUID] = []
    cite_receipt: list[dict[str, str]] = []
    for ref in cites or []:
        cid = await capture._find_decision(pool, ref, require_identifier=True)
        if cid is None:
            cite_receipt.append({"ref": ref, "matched": "false",
                                 "note": "matched no decision. Quote its UUID, "
                                         "canonical, or 8-char short id (a prose "
                                         "match is not accepted here)"})
            continue
        cite_ids.append(cid)
        cite_receipt.append({"ref": ref, "matched": "true", "id": str(cid)[:8]})
    # bears_on resolves the same best-effort way as confirms/rediscovers: one bad ref
    # must not veto the threads that DID match. Same addressing rule as resolves/
    # supersedes (require_identifier=True): a citation act refuses rather than guesses.
    # The thread's OWN summary is echoed here too, same reason resolves echoes it: a
    # valid id naming the WRONG thread is only catchable by the caller reading it.
    bears_on_ids: list[uuid.UUID] = []
    bears_on_receipt: list[dict[str, str]] = []
    for ref in bears_on or []:
        bid = await capture._find_thread(pool, ref, require_identifier=True)
        if bid is None:
            # OBSERVED MULTIPLE TIMES IN A SHORT WINDOW: bears_on mints `answers`,
            # Decision->Thread ONLY. A ref that names a Decision instead of a Thread
            # resolved to nothing here and the result said only "matched no thread",
            # easy to miss in a large response, and more than one caller read it as
            # success. Same cross-type-mismatch discipline `_resolve_cited_object`
            # already uses for prose citations: check the OTHER type too, so a genuine
            # mismatch NAMES itself instead of reading like a generic not-found.
            cross = await capture._find_decision(pool, ref, require_identifier=True)
            if cross is not None:
                bears_on_receipt.append({"ref": ref, "matched": "false",
                                         "note": f"{ref!r} resolves to a Decision, not a "
                                                 "Thread. bears_on only links to a Thread "
                                                 "(creates an answers edge, Decision to "
                                                 "Thread); this id was never linked"})
            else:
                bears_on_receipt.append({"ref": ref, "matched": "false",
                                         "note": "matched no thread. Quote its UUID, "
                                                 "canonical, or 8-char short id (a prose "
                                                 "match is not accepted here)"})
            continue
        bsumm = await capture._thread_summary(pool, bid)
        bears_on_ids.append(bid)
        bears_on_receipt.append({"ref": ref, "matched": "true", "id": str(bid)[:8],
                                 "summary": bsumm or ""})
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    # This default logic used to be duplicated per caller, which meant one caller could
    # carry a fix the other missed. Both steps, the generation-scoped mount default and
    # the lineage-wide widen, now live in capture.resolve_repo_default so record_decision
    # and open_thread never carry two differently-shaped copies of the same fallback.
    ident = await _ident_for(ctx)
    _repo_default = await capture.resolve_repo_default(
        pool, repo, actor, ident.project if ident else None)
    repo = _repo_default["repo"]
    repo_defaulted = _repo_default["repo_defaulted"]
    lineage_attempted = _repo_default["lineage_attempted"]
    lineage_candidates = _repo_default["lineage_candidates"]
    lineage_projects = _repo_default["lineage_projects"]
    # NEAR-DUP RESULT HONESTY: the SAME lookup `capture.record_decision` runs internally
    # to decide whether to reuse an existing decision, run here FIRST so the result can
    # show what a hit is about to overwrite. A pre-check outside the write transaction,
    # same non-locking caveat as the lookup it mirrors. `repo` gates it exactly like the
    # real call (no safe scope to dedup against without one).
    dup_before: uuid.UUID | None = None
    prior_content: dict[str, str | None] | None = None
    if repo:
        dup_before = await capture.find_near_duplicate_decision(pool, summary, repo=repo,
                                                                 exclude=old)
        if dup_before is not None:
            prior_content = await capture._decision_snapshot(pool, dup_before)
    # THE ESCAPE HATCH'S TWO POPULATIONS: a caller who requested ONLY extension-link
    # connectivity and gave no unlinked_because of their own gets the system-set reason,
    # never silently mixed with a genuinely standalone write's own (possibly
    # caller-typed) reason. _enforce_required_links only ever USES this when the
    # atomic-scope check (repo/grounds/resolves) actually fails: a caller who also gave
    # repo=/grounds=/resolves= that satisfy the gate never sees this value land.
    effective_unlinked_because = unlinked_because
    # THE STRUCTURAL DISCRIMINATOR: this exact boolean is the ONLY place that ever
    # decides "this write's gap is extension-link-pending, not standalone", passed
    # straight to capture.record_decision as unlinked_because_kind, never re-derived
    # later by matching _EXTENSION_LINK_PENDING_REASON's own prose (which drifts every
    # time this tuple grows a new param name; an earlier metric used to do exactly that
    # and silently misclassified several wordings' worth of history).
    is_extension_pending = effective_unlinked_because is None and any(
        [obsoletes, confirms, refutes, implements, rediscovers, bears_on]
    )
    if is_extension_pending:
        effective_unlinked_because = _EXTENSION_LINK_PENDING_REASON
    # RESULT-HONESTY PRE-CHECK: these six now mint INSIDE record_decision's own atomic
    # transaction, so the wrapper can no longer diff "before this call" vs "after" by
    # calling mint_*/_witness_link itself and reading its bool return: that return no
    # longer reaches here. Instead, pre-check existence against the object THIS call
    # will land on. `dup_before` alone is NOT enough here: it's only computed `if
    # repo:`, but record_decision's own idempotency ALWAYS resolves by the exact
    # summary hash regardless of repo (that's how a repo-less retry still lands on the
    # same object), so the pre-check target must fall back to that same exact-hash
    # lookup when dup_before is unset, or a repo-less idempotent re-call would wrongly
    # read every link as freshly minted.
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
            operator_authorized=operator_authorized,
        )
    except ValueError as e:  # e.g. a path-shaped repo: refuse clean, no traceback
        return {"error": str(e)}
    await provenance.stamp_possible_upstream(Actions(pool), written_object_id=d, source_id=actor)
    # RESULT DIET: `summary` is NOT echoed back, since the caller supplied it this same
    # turn, so echoing it verbatim is pure duplication. `resolved_thread(s)` below still
    # echoes ITS OWN summary (the closed THREAD's words, not this call's) because that's
    # the one place a valid id naming the wrong target is only catchable by the caller
    # reading it: a mis-citation risk, not a duplication.
    out: dict[str, Any] = {"id": str(d), "kind": kind}
    if repo_defaulted:
        out["repo_defaulted"] = {
            "to": repo,
            "why": "no repo given, so it defaulted to the caller's own project rather "
                   "than being left unlinked",
        }
    elif lineage_attempted:
        # This records the case where the generation-scoped default AND the
        # lineage-root widening both failed to name a single project: genuinely nothing
        # (lineage_candidates empty) or a real disagreement (2+ candidates, never broken
        # by recency/generation count). capture.record_lineage_abstain wraps
        # derive_or_abstain the same way for every caller, so the result shape stays
        # identical across entry points.
        out["lineage_repo_derivation"] = await capture.record_lineage_abstain(
            pool, d, actor, lineage_candidates, lineage_projects)
    # CONTENT-LANDED, MEASURED NOT INFERRED: a READ-BACK, not a guess from the pre-write
    # dup-check below. That check can only ever say WHICH object a call landed on, never
    # whether THIS call's own rationale/protocol actually became the CURRENT value on it
    # (a different source's assertion can still win the confidence/recency tie-break on
    # the SAME object, silently, and the old result shape had no way to say so). This was
    # motivated by several real cases: a "reused_existing_decision:true" result with a
    # note ambiguous enough that the caller had to go read the object to confirm the
    # write had actually landed; a write going to background with a mis-set field that
    # could not be corrected until it landed; and a case where the prior_art guard once
    # caught reusing a near-duplicate silently. The applicable rule: don't infer success
    # from "no error raised", read the fact you just tried to establish and report what
    # it actually says.
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
                f"your {' and '.join(not_landed)} did not become decision {str(d)[:8]}'s "
                "current value. A different assertion is currently winning the "
                f"confidence/recency tie-break on this object. Re-recording with the same "
                "summary is likely to repeat this outcome; use "
                f"amend_decision(ref={str(d)[:8]!r}, addendum=...) instead, since it always "
                "lands as new content and never contends a tie-break.")
    if dup_before is not None and str(dup_before) == str(d):
        out["reused_existing_decision"] = True
        out["prior_content"] = prior_content
        exact_repeat = prior_content is not None and prior_content.get("summary") == summary
        out["note"] = (
            (f"this call's summary exactly matched decision {str(d)[:8]}'s own current "
             "summary, a safe repeat (for example, a retry after a dropped or timed-out "
             "response); nothing was overwritten that this call didn't already say itself.")
            if exact_repeat else
            (f"this call's summary was judged a near-duplicate of decision {str(d)[:8]}'s "
             "existing content (shown in prior_content) and reused that object instead of "
             "creating a new one. Its prior summary/rationale are now superseded (still "
             "readable via the assertions history, never deleted) but no longer current. "
             "If these two rulings are not actually the same decision, this was a false "
             "positive: the summaries shared enough boilerplate to score above the "
             "similarity bar without describing the same thing."))
    # PRIOR-ART SURFACING, unified across {Decisions, Practices, Superstitions, open
    # obligation Threads}: before a decision stands, name what standing rule or technique
    # already covers this ground. Search is the same fused engine `search()` exposes,
    # topical (lexical + semantic) rather than lexical-only, since a contradicting
    # decision rarely reuses its predecessor's exact wording (the motivating failure: a
    # new decision minted in direct contradiction of an existing one with zero friction).
    # `_surface_prior_art` (fail-open, 15s bound) is the shared write/read-time engine:
    # record_practice and send()'s dispatch-time hop both run the identical search, not a
    # second matcher.
    # refute_id's target Practice's Superstition is looked up here for the RESULT only
    # (below), not to exclude it from this search. A same-call self-collision (the
    # freshly-converted Superstition scoring as this SAME call's own prior-art hit, since
    # refute_practice's write now lands inside the atomic block above, before this search
    # runs) was the obvious worry, and it was checked, not assumed: `strong` requires
    # `via` in ("id", "both"), and embed_backfill (semantics.py) computes the semantic
    # half of the fused match as a SEPARATE, async pass, never synchronously at write
    # time, so a same-transaction object scores `via='lexical'` at best here (confirmed
    # live), never strong. No exclusion needed for a scenario this structurally can't
    # reach.
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
    obsoleted_practice = await _obsoleted_standing_practice(pool, obsoletes, prior)
    if refute_id is not None:
        # THE STRUCTURAL DISCRIMINATOR, DECOUPLED FROM SEARCH TIMING: refute_id was
        # already resolved and validated against a real Practice earlier in this call
        # (or the call errored out before reaching here). The caller's intent to
        # overturn THAT practice is a fact this wrapper already holds, not something
        # that needs re-discovering from whatever the search above happens to surface.
        # Folding refute_practice's write into the atomic block above means this same
        # search now runs AFTER the Practice is already flagged `refuted_by` (filtered
        # out of `prior` entirely by prior_art_from_hits' own refuted-hit check), so the
        # old "was the top hit this same Practice" test would silently stop firing,
        # exactly the regression a prior fold attempt's own test caught.
        out["prior_art_flag"] = (
            f"this overturns standing Practice {str(refute_id)[:8]}, handled below via "
            "refutes= (converts it to a dead Superstition, flagged not retired)")
        out["prior_art_polarity"] = "contradict"
    elif obsoleted_practice is not None:
        # The SAME structural discriminator as refute_id above, for obsoletes=: an
        # explicit obsoletion already names its own target, so it never needs the
        # generic re-derivation/contradiction-cues guess below. Unlike refute_id,
        # obsoletes= never converts the Practice itself (only the matching Superstition
        # dies); the wording says so plainly.
        out["prior_art_flag"] = (
            f"this obsoletes standing Practice {obsoleted_practice['id']}, handled "
            "below via obsoletes= (kills the matching Superstition; the Practice "
            "record itself is untouched, only the workaround it names)")
        out["prior_art_polarity"] = "obsolete"
    elif strong:
        top = prior[0]
        top_kind = top.get("type") or "Decision"
        if top_kind == "Practice":
            # A lexical reversal fingerprint (practice_contradiction_cues) distinguishes
            # an unlabeled CONTRADICTION from a plain, uncited RE-DERIVATION when the
            # caller gave no refutes= at all (the refutes= case is handled
            # unconditionally above, before this branch is ever reached).
            cues = capture.practice_contradiction_cues(f"{summary} {rationale or ''}")
            if cues:
                out["prior_art_flag"] = (
                    f"this may contradict standing Practice {top['id']} rather than cite "
                    f"it. Reversal language found ({', '.join(cues)}); if you mean to "
                    f"overturn it, say so explicitly (refutes=['{top['id']}']), or "
                    "acknowledge it (ack_prior_art=True) if this wording is coincidental")
                out["prior_art_polarity"] = "contradict"
            else:
                out["prior_art_flag"] = (
                    f"this looks like a re-derivation of standing Practice {top['id']}. "
                    f"Confirm it as evidence (confirms=['{top['id']}']) if it's the same "
                    "lesson, or acknowledge it (ack_prior_art=True) if coincidental")
                out["prior_art_polarity"] = "rederive"
        elif top_kind == "Superstition":
            out["prior_art_flag"] = (
                f"a dead Superstition ({top['id']}) already covers this ground. Check "
                "you're not reviving a workaround its own fix already killed "
                "(acknowledge with ack_prior_art=True if this is intentional/unrelated)")
        elif top_kind == "Thread":
            # The nudge fires unprompted, inheriting the same proven behavior used
            # elsewhere in prior-art surfacing rather than being a new detector: see
            # UNIFIED_PRIOR_ART_KINDS' own comment. Deliberately never suggests
            # resolves= here: this decision merely SPOKE TO the row in passing (that's
            # how it surfaced as prior art at all); whether it also SETTLES the row is
            # the caller's own judgment to make, not this flag's to presume.
            out["prior_art_flag"] = (
                f"this appears to speak to open thread {top['id']}. Pass "
                f"bears_on=['{top['id']}'] to link it without closing it (bears_on "
                "cites, it never resolves; use resolves=[...] instead if this ruling "
                "actually settles the row), or acknowledge it (ack_prior_art=True) if "
                "coincidental")
            out["prior_art_polarity"] = "bears_on"
        else:
            out["prior_art_flag"] = (
                f"a standing ruling ({top['id']}) covers this ground. Supersede it "
                "explicitly (supersedes=...), cite it (grounds=...), name this as what "
                "it executes (implements=...), name this as an independent "
                "rediscovery of it (rediscovers=[...]) if you reached the same "
                "conclusion on your own, or acknowledge it (ack_prior_art=True)")
    if prior:
        # INSTRUMENT IT: every strong hit is a MEASURED re-derivation event, logged
        # regardless of whether the caller acts on it. The population, aggregated over
        # time, IS the re-derivation ratchet metric.
        try:
            await pool.execute(
                "UPDATE search_log SET prior_art_kind=$1, prior_art_strong=$2, "
                "prior_art_polarity=$3 "
                "WHERE id = (SELECT id FROM search_log ORDER BY id DESC LIMIT 1)",
                (prior[0].get("type") or "Decision") if prior else None, strong,
                out.get("prior_art_polarity"))
        except Exception:  # noqa: BLE001, telemetry must never block the decision
            pass
    if ack_prior_art:
        if prior and strong:
            await capture.acknowledge_prior_art(Actions(pool), d, prior[0]["id"], actor)
            out["prior_art_acknowledged"] = f"noted: {prior[0]['id']} reviewed, no action needed"
        elif prior:
            # `out["prior_art"]` above already lists these same hits, so saying "none
            # found" here when `prior` is non-empty would contradict the SAME result.
            out["prior_art_acknowledged"] = (
                f"{len(prior)} prior-art hit(s) found but none strong enough to flag. "
                "Nothing rises to acknowledge")
        else:
            out["prior_art_acknowledged"] = (
                "no prior-art hit was found at all. Nothing to acknowledge")
    # RESULTS ONLY BELOW: all six already MINTED inside capture.record_decision's own
    # atomic transaction, above (the object and every one of these now either all land
    # or none do). Nothing here writes; each block just reads back what committed, using
    # the pre-check computed before the call for "was this new".
    if impl_id is not None:
        out["implements"] = (
            f"{str(impl_id)[:8]}: this decision is a specific execution of it"
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
    # RESULTS ONLY BELOW, same discipline as the six siblings above: refute_id's
    # `refuted_by` stamp and every obsoletes= Superstition already MINTED inside
    # capture.record_decision's own atomic transaction. Nothing here writes, each block
    # reads back what committed.
    if refute_id is not None:
        refuted_by = await pool.fetchval(
            "SELECT val.value #>> '{}' FROM current_assertions val "
            "WHERE val.object_id=$1 AND val.name='refuted_by' "
            "ORDER BY val.confidence DESC, val.observed_at DESC LIMIT 1", refute_id)
        if refuted_by == str(d):
            out["refuted_practice"] = (
                f"{str(refute_id)[:8]} converted to Superstition "
                f"{(str(refute_superstition_id)[:8] + ' ') if refute_superstition_id else ''}"
                "; the Practice stays active, flagged")
    if obsoletes:
        killed = [s.strip() for s in obsoletes if s and s.strip()]
        if killed:
            out["superstitions_killed"] = killed
            out["superstitions_note"] = (
                "each is a dead Superstition on the record; orient announces recent kills "
                "fleet-wide for 14 days so minds carrying the practice strike it")
    if not protocol and capture.measurement_smell(f"{summary} {rationale or ''}"):
        # `protocol` is this tool's best field and nothing asked for it: advice in the
        # result, never a gate (the decision is recorded either way).
        # RESULT DIET: short code, not the full prose every firing; describe(
        # 'nags:protocol') for the text.
        out.setdefault("nags", []).append("protocol")
    if isinstance(resolves, list):
        out["resolved_threads"] = receipt
    elif answered:
        # THE SAME-TURN CATCH: several documented instances of a valid id naming the
        # wrong thread going unnoticed, caught only later by someone re-reading a
        # result that never showed the summary. A mismatch like that cannot be refused
        # by any matcher, but it's obvious the instant the closed thread's own words are
        # right here, so they are, every time, not just for the list form.
        out["resolved_thread"] = (
            f"{str(answered[0])[:8]}: closed by this decision (answers edge). "
            f"{single_summary or '(no summary on record)'}")
    if old is not None:
        out["superseded"] = (
            "self (identical summary re-recorded), nothing buried" if old == d else
            f"{str(old)[:8]} is buried under this decision: it leaves orient's recent "
            "list, the decision-log grays it (unwind: re-assert superseded_by='' on it)")
    if grounded:
        out["grounded_by"] = grounded
    if missing:
        out["unresolved_grounds"] = missing
        out["note"] = ("unresolved grounds were skipped. Ingest_reference them first, "
                       "then re-run record_decision (idempotent) to attach the edges")
    # RESULT COMPLETENESS: capture.record_decision's own prose-scan mints `decided_in`
    # from a commit sha named in summary/rationale/protocol, and mints prose-derived
    # `cites` edges (origin="prose", distinct from the caller-declared `cites=` param's
    # own `out["cites"]` above: a DIFFERENT field, never overloading the same key with
    # two meanings) alongside a `prose_citation_skips` property for anything that failed
    # to resolve. All inside the SAME atomic transaction as everything else this result
    # already reports, but none of it was ever surfaced: a caller citing "commit
    # abc1234" or "decision deadbeef" in their own summary/rationale had no way to tell
    # whether it became a real edge, was skipped as unresolved, or was never attempted
    # at all. Same read-back discipline as every other block here: nothing writes, this
    # only reads what capture.record_decision already committed.
    prose_decided_in = [str(r["id"])[:8] for r in await pool.fetch(
        "SELECT to_id AS id FROM links WHERE from_id=$1 AND type='decided_in'", d)]
    if prose_decided_in:
        out["decided_in"] = prose_decided_in
    prose_cited = [str(r["id"])[:8] for r in await pool.fetch(
        "SELECT to_id AS id FROM links WHERE from_id=$1 AND type='cites' "
        "AND properties->>'origin'='prose'", d)]
    if prose_cited:
        out["prose_cites"] = prose_cited
    prose_skips = await pool.fetchval(
        "SELECT value FROM current_assertions WHERE object_id=$1 "
        "AND name='prose_citation_skips'", d)
    if prose_skips:
        out["prose_citation_skips"] = prose_skips
    # UNFILED WARNING: a decision with no repo= and no auto-detected decided_in commit
    # citation produces ZERO outgoing links and is structurally invisible to
    # _fn_project no matter how many JOIN paths it grows, confirmed live against a set
    # of real decisions that all had exactly this shape. The MCP wrapper already passes
    # repo= through correctly when supplied; the gap is entirely at call sites that omit
    # it. A READ-BACK (same discipline as content_landed above), not an inference from
    # the params this call happened to receive: repo_defaulted/lineage_repo_derivation
    # both mint a real in_repo link of their own, so checking the actual link table
    # catches every path that landed one, not just the plain repo= case. Advisory only,
    # never a refusal: some decisions are legitimately standalone (a fleet-wide
    # decision with no one project).
    if not await pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND type IN ('in_repo', 'decided_in') "
        "LIMIT 1", d):
        out["unfiled"] = True
        out["unfiled_note"] = (
            "no repo= given and no commit sha auto-resolved from summary/rationale/"
            "protocol. This decision has no outgoing link and will not surface in "
            "any project-scoped view (decision-log, orient(project=...), etc). Pass "
            "repo= if this belongs to one, or ignore if it's genuinely fleet-wide.")
    return out


@mcp.tool()
async def backup_settings(
    action: str, vault_path: str | None = None,
    timer_schedules: dict[str, str] | None = None,
    offbox_repositories: list[dict[str, Any]] | None = None,
    because: str | None = None, ruling: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Reads or writes backup configuration. `action='get'` reads current settings; no
    authority is required. `action='write'` changes them: the operator can write
    freely, anyone else must cite a standing `ruling` that `verify_ruling` confirms
    applies to 'backup_settings'. `because` is required on write.

    Partial update: only given fields change. `vault_path` (absolute path, local disk
    or an OS-mounted NAS share; pg_dump/pg_basebackup don't distinguish the two),
    `timer_schedules` (unit name -> OnCalendar=, one of the 5 backup-lane timers),
    `offbox_repositories` (list of {url, schedule, enabled}; stored but inert until
    off-box support is enabled).

    A written schedule takes effect only once the deploy process's own timer-install
    step regenerates the unit. `backup_status`'s `configured_schedule` vs `schedule`
    distinguish "set" from "took effect"."""
    from src.orchestrator.backup_settings import get_backup_settings, write_backup_settings

    pool = await _pool_get()
    if action == "get":
        return await get_backup_settings(pool)
    if action != "write":
        return {"error": "action must be 'get' or 'write'"}
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    fields: dict[str, Any] = {}
    if vault_path is not None:
        fields["vault_path"] = vault_path
    if timer_schedules is not None:
        fields["timer_schedules"] = timer_schedules
    if offbox_repositories is not None:
        fields["offbox_repositories"] = offbox_repositories
    return await write_backup_settings(
        pool, actor=actor, because=because or "", ruling=ruling, **fields)


@mcp.tool()
async def settings(
    action: str, key: str | None = None, value: Any = None, scope_id: str = "",
    because: str | None = None, ruling: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Reads or writes the settings registry (`src/config/settings_registry.py`'s
    `SETTINGS` tuple). A new setting needs a new registry entry, never a new tool.

    `action='list'` returns every declared setting's own metadata (type, default,
    scope, effect, authority, choices) plus `value` and `live` (the running/shipped
    value, null when not cheap); secrets are redacted to `{"set": bool}`. `action='get'`
    (with `key`) returns one `value`/`live` pair.
    `action='write'` (with `key`/`value`) changes it: each setting's own `authority`
    (operator | operator_or_manager | operator_or_ruling) decides who may write it
    without a manager/ruling citation. `because` is required unless the setting opts
    out (`requires_because=False`). Refuses structured (`errors: [{field, message}]`)
    on a bad value, never a bare string. A written setting's own `effect`
    (immediate/next_tick/restart:<unit>/next_deploy) names when it actually takes
    hold; the result's `note` field says so plainly whenever it is not immediate."""
    from src.orchestrator.settings_service import get_setting, list_settings, write_setting

    pool = await _pool_get()
    if action == "list":
        return {"settings": await list_settings(pool)}
    if action == "get":
        if not key:
            return {"error": "key is required for action='get'"}
        return await get_setting(pool, key)
    if action != "write":
        return {"error": "action must be 'list', 'get', or 'write'"}
    if not key:
        return {"error": "key is required for action='write'"}
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    return await write_setting(
        pool, key, value, actor=actor, because=because or "", scope_id=scope_id, ruling=ruling)


# THE PRACTICE OBJECT-TYPE DISPATCHER (approved with scope limited to
# "practice(action='record'|'amend') only"): the sixth and FINAL object-type
# dispatcher of a broader fold effort covering thread, agent, and decision actions,
# after which any standalone tail stays named permanently. A literal "decision"
# dispatcher was DECLINED: amend_decision is Decision's only write action beyond
# record_decision itself and stays named as-is; a one-action dispatcher is the exact
# catch-all shape that decision forbids. Practice, unlike Decision, genuinely has TWO
# write actions of its own (record + amend, the same shape) and record_practice sees
# frequent use, so folding it costs nothing that decision-parity would otherwise
# protect. consult_canon/handoff_briefing (pure reads, distinct questions),
# dismiss_brief (wrong object type, a mail message_id), and ack_handoff (dual-type
# Thread-or-Decision by design, no siblings of its own shape) all stay exactly as they
# are: declined from this fold, not silently dropped.
PRACTICE_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "oneOf": [
        _dispatcher_action_schema({
            "action": _action_const("record"), "statement": _s(),
            "failure_prevented": _opt_s(), "surface": _opt_s(), "repo": _opt_s(),
            "witnesses": _opt_list_s(), "unlinked_because": _opt_s(),
            "unlinked_because_kind": _opt_s(), **_SUBAGENT_TRIO,
        }, ["action", "statement"]),
        _dispatcher_action_schema({
            "action": _action_const("amend"), "ref": _s(), "amendment": _s(),
            **_SUBAGENT_TRIO,
        }, ["action", "ref", "amendment"]),
    ],
}
_HAND_BUILT_SCHEMAS["practice"] = PRACTICE_INPUT_SCHEMA

_PRACTICE_ACTION_PARAMS: dict[str, tuple[list[str], list[str]]] = {
    "record": (["statement", "failure_prevented", "surface", "repo", "witnesses",
               "unlinked_because", "unlinked_because_kind"],
              ["statement"]),
    "amend": (["ref", "amendment"], ["ref", "amendment"]),
}


async def _practice_impl(
    action: str, *,
    statement: str | None = None, failure_prevented: str | None = None,
    surface: str | None = None, repo: str | None = None,
    witnesses: list[str] | None = None, ref: str | None = None,
    amendment: str | None = None, unlinked_because: str | None = None,
    unlinked_because_kind: str | None = None, subagent_id: str | None = None,
    subagent_type: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Shared implementation behind `practice` and its 2 hidden single-purpose aliases
    (record_practice, amend_practice): one code path, three names. Every branch's body
    below is copied verbatim from what was that alias's own top-level function.

    PRE-DISPATCH VALIDATION, same discipline as _seat_impl's own."""
    if action not in _PRACTICE_ACTION_PARAMS:
        return {"error": f"unknown action {action!r}",
                "known_actions": sorted(_PRACTICE_ACTION_PARAMS)}
    accepted, required = _PRACTICE_ACTION_PARAMS[action]
    local = dict(locals())
    missing = [p for p in required if local.get(p) in (None, "")]
    if missing:
        return {"error": f"action {action!r} is missing required param(s) {missing}",
                "action_accepts": accepted, "action_requires": required}

    if action == "record":
        assert statement is not None  # pre-dispatch validation guaranteed this
        pool = await _pool_get()
        wids: list[uuid.UUID] = []
        receipt: list[dict[str, str]] = []
        for w in witnesses or []:
            rid = await _resolve(pool, w)
            if rid is not None:
                wids.append(rid)
                receipt.append({"ref": w, "matched": "true", "id": str(rid)[:8]})
            else:
                receipt.append({"ref": w, "matched": "false",
                                "note": "matched no object — quote its UUID or 8-char "
                                        "short id"})
        actor = await _actor_for(ctx, subagent_id, subagent_type)
        try:
            p = await capture.record_practice(
                Actions(pool), statement, failure_prevented=failure_prevented,
                surface=surface, repo=repo, witnesses=wids, source=actor,
                unlinked_because=unlinked_because,
                unlinked_because_kind=unlinked_because_kind)
        except ValueError as e:  # refused: none of its required links declared
            return {"error": str(e)}
        out: dict[str, Any] = {"id": str(p), "statement": statement,
                               "confirmed": await capture.practice_confirmed_count(pool, p)}
        if receipt:
            out["witnesses_resolution"] = receipt
        prior = await _surface_prior_art(
            pool, f"{statement} {failure_prevented or ''}", exclude={p}, repo=repo,
            actor=actor)
        strong = capture.prior_art_is_strong(prior)
        if prior:
            out["prior_art"] = prior
            if strong:
                top = prior[0]
                out["prior_art_flag"] = (
                    f"{top.get('type') or 'Decision'} {top['id']} already covers similar "
                    "ground — check this isn't the same lesson under different words "
                    "before it stands as a separate Practice")
            try:
                await pool.execute(
                    "UPDATE search_log SET prior_art_kind=$1, prior_art_strong=$2 "
                    "WHERE id = (SELECT id FROM search_log ORDER BY id DESC LIMIT 1)",
                    (prior[0].get("type") or "Decision") if prior else None, strong)
            except Exception:  # noqa: BLE001, telemetry must never block the record
                pass
        return out
    if action == "amend":
        assert ref is not None and amendment is not None
        pool = await _pool_get()
        try:
            pid = await capture.amend_practice(
                Actions(pool), ref, amendment,
                source=await _actor_for(ctx, subagent_id, subagent_type))
        except ValueError as e:
            return {"error": str(e)}
        if pid is None:
            return {"error": f"no practice matches {ref!r}"}
        out = {"id": str(pid), "amendment": amendment.strip(), "status": "amended"}
        # THE RESULT CARRIES THE ROW: a write is never invisible on its own result.
        # `id=` bypasses practices()'s own ranked window entirely, the exact gap a fresh
        # amendment used to fall through (a just-amended practice is systematically the
        # least-confirmed, so it sorted outside the default limit=50 on the very next
        # read). Same `practices` Function every reader already uses (comp.run_spec),
        # never a second query that could drift.
        practice_out = await comp.run_spec(
            pool, {"op": "function", "name": "practices", "args": {"id": str(pid)}}, None,
            name="amend-practice-receipt")
        rows: list[dict[str, Any]] = practice_out["items"]
        if rows:
            out["practice"] = rows[0]
        return out
    raise AssertionError(f"action {action!r} passed validation but has no branch")


@mcp.tool()
async def practice(
    action: str, statement: str | None = None, failure_prevented: str | None = None,
    surface: str | None = None, repo: str | None = None,
    witnesses: list[str] | None = None, ref: str | None = None,
    amendment: str | None = None, unlinked_because: str | None = None,
    unlinked_because_kind: str | None = None, subagent_id: str | None = None,
    subagent_type: str | None = None, session_anchor: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Records or amends a Practice: a transferable technique (the positive
    counterpart to a Superstition record). One tool, two actions. See
    `describe('practice')` for the full per-action shape.

    ACTION TABLE, action: what it does (required params beyond action):
      record: write a NEW technique (statement: the imperative one-liner, phrased
        as you'd want a future reader to follow it, not as narration).
        `failure_prevented` is the concrete symptom that makes it findable mid-failure.
        `surface` reuses BlindSpot's domain vocabulary. `witnesses` links Decision(s)/
        Commit(s)/Thread(s) as evidence (a miss is reported, never fatal). Idempotent
        on the normalized statement. Timeless, never moment-stamped: a later disproof
        retires it via record_decision(refutes=...), never here. Runs the same
        prior-art check record_decision does. `unlinked_because`/`unlinked_because_kind`
        are the declare-or-refuse fields, matching record_decision's own params,
        currently inactive here (a Practice's `repo` requirement is not enforced by
        default) until this type's `required_link_kinds` opts in.
      amend: narrow or correct a LIVE practice's guidance (ref, amendment), without
        touching its id, its `statement` (record's own idempotency key), or its
        witness/confirmed count. Amendments fold directly into practices()'s own
        listing. Refuses on an unmatched ref or a practice already REFUTED (use
        record_decision(refutes=...) to retire one; this only adds to a practice
        still standing)."""
    return await _practice_impl(
        action, statement=statement, failure_prevented=failure_prevented,
        surface=surface, repo=repo, witnesses=witnesses, ref=ref, amendment=amendment,
        unlinked_because=unlinked_because, unlinked_because_kind=unlinked_because_kind,
        subagent_id=subagent_id, subagent_type=subagent_type, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "practice(action='record')",
    "since": "the practice() dispatcher was introduced",
})
async def record_practice(
    statement: str, failure_prevented: str | None = None, surface: str | None = None,
    repo: str | None = None, witnesses: list[str] | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    practice(action='record')."""
    return await _practice_impl(
        "record", statement=statement, failure_prevented=failure_prevented,
        surface=surface, repo=repo, witnesses=witnesses, subagent_id=subagent_id,
        subagent_type=subagent_type, ctx=ctx)


@mcp.tool()
async def ingest_reference(
    title: str, source_url: str | None = None, vendor: str | None = None,
    body: str | None = None, caveats: str | None = None, repo: str | None = None,
    cites: list[str] | None = None,
    unlinked_because: str | None = None, unlinked_because_kind: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Turns something you read into a first-class Reference node (a paper, vendor
    doc, spec) so it's findable by search and citable via
    record_decision(grounds=[...]). `title` is the citation key, idempotent on its
    slug. `vendor` is who wrote it; `body` is what it claims, in your own words.
    `caveats` is a separate field from body, for the "applies only under X" caveat
    that gets lost when buried in prose. `cites` wires paper-to-paper lineage (ids/
    canonicals/titles of already-ingested References). Graded SELF_DECLARED.
    `unlinked_because`/`unlinked_because_kind` are record_decision's own
    declare-or-refuse fields, extended to this tool: a caller-typed `repo=` satisfies
    the requirement outright; a mount-defaulted repo (see `repo_defaulted` below)
    does not, the same rule record_decision already applies to its own repo param.
    Returns the id and canonical to cite."""
    pool = await _pool_get()
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    cids: list[uuid.UUID] = []
    missing: list[str] = []
    for c in cites or []:
        rid = await _resolve(pool, c)
        (cids.append(rid) if rid is not None else missing.append(c))
    # This tool was missing the same default its siblings already had. It now follows
    # the same shared resolution steps record_decision/open_thread use: generation-scoped
    # mount default, then the lineage-wide widen when that finds nothing.
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
            unlinked_because=unlinked_because,
            unlinked_because_kind=unlinked_because_kind,
        )
    except ValueError as e:  # e.g. a path-shaped repo: refuse clean, no traceback
        return {"error": str(e)}
    await _stamp_read_ids(pool, ident, "ingest_reference", [ref])
    out: dict[str, Any] = {"id": str(ref), "canonical": canon,
                           "note": "cite it: record_decision(..., grounds=['" + canon + "'])"}
    if repo_defaulted:
        out["repo_defaulted"] = {
            "to": repo,
            "why": "no repo given, so it defaulted to the caller's own project instead "
                   "of being left unlinked.",
        }
    elif lineage_attempted:
        # Same shared post-mint step record_decision/open_thread's wrappers use: the
        # generation-scoped default and the lineage-root widening both failed to name
        # a single project, so abstain and record why, candidate ids kept whole, via
        # the one shared primitive every orphan-healing path calls.
        out["lineage_repo_derivation"] = await capture.record_lineage_abstain(
            pool, ref, actor, lineage_candidates, lineage_projects)
    if missing:
        out["unresolved_cites"] = missing
        out["cites_note"] = ("unresolved cites were skipped. Ingest each cited work "
                             "first, then re-ingest this title (idempotent) to wire the edges")
    return out


@mcp.tool()
async def record_evaluation(
    rubric: str, verdict: str | None = None, subject: str | None = None,
    value: float | int | str | None = None, unit: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Records a verdict against an Agent generation or Artifact: a test suite
    result, a code review finding, or a pass/fail check. `rubric` (which
    standard/check was applied) is mandatory and non-blank. It is refused cleanly
    (an {"error": ...} response, never a traceback) rather than minting an
    unverifiable verdict. `subject` (a UUID/short-id/canonical ref to an
    already-minted Agent generation or Artifact) mints the `evaluated_by` edge in
    the same call; a miss is reported, never fatal. `value`/`unit` follow the
    Metric shape (with `measured_at` stamped as this call's own observed time),
    stored as properties on this same Evaluation object, never a linked child node.
    Each call mints a fresh object: the same rubric run twice is two distinct
    verdicts, never deduplicated."""
    pool = await _pool_get()
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    subject_id: uuid.UUID | None = None
    subject_note: dict[str, str] | None = None
    if subject:
        subject_id = await _resolve(pool, subject)
        if subject_id is None:
            subject_note = {"ref": subject, "matched": "false",
                            "note": "matched no object. Quote its UUID or 8-char short "
                                    "id; the Evaluation was still minted, just without "
                                    "the evaluated_by edge"}
    try:
        e = await capture.record_evaluation(
            Actions(pool), rubric, verdict=verdict, subject=subject_id, value=value,
            unit=unit, source=actor)
    except ValueError as err:
        return {"error": str(err)}
    out: dict[str, Any] = {"id": str(e), "rubric": rubric.strip()}
    if subject_note:
        out["subject_resolution"] = subject_note
    return out


@mcp.tool()
async def record_artifact(
    key: str, authoring_run: str | None = None, unlinked_because: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Mints an Artifact: a build, deploy, or document output that a Commit does not
    already cover. Refuses immediately (an `{"error": ...}` response, never a
    traceback) unless it carries its authoring Agent generation's own `produced`
    edge, or `unlinked_because=<reason>` is given (an artifact needs both an
    authoring run and a version). `authoring_run` is a reference (UUID/short-id/
    canonical) to an EXISTING Agent generation (the session object IS the Agent
    generation; there is no separate AgentRun pointer to lazily mint). It refuses
    if the reference doesn't resolve. `revises` (linking a predecessor version) is
    a separate call, `mint_revises`, made after this one returns; a first version
    legitimately has none."""
    pool = await _pool_get()
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    try:
        art = await capture.record_artifact(
            Actions(pool), key, authoring_run=authoring_run, source=actor,
            unlinked_because=unlinked_because)
    except ValueError as err:
        return {"error": str(err)}
    return {"id": str(art), "key": key}


@mcp.tool()
async def cite_transcript(
    ref: str, agent: str, line_idx: int, because: str,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Records an explicit citation of one line of an Agent generation's own
    transcript: a `cites` edge from `ref` (a Decision, Thread, or Evaluation) to
    the Agent that `agent` resolves to, carrying `line_idx`/`line_hash`/`said_at`
    as edge properties, verified against the soul store's own hash chain at mint
    time.

    No auto-citing: `because` is mandatory and non-blank. It is refused cleanly
    (an `{"error": ...}` response, never a traceback) rather than minting an
    unreasoned citation. It never targets a human node: `agent` must resolve to a
    real, active Agent object; the literal 'operator' string or anything else
    refuses exactly like an unresolved ref. `line_idx` must resolve to a real,
    chain-verified soul_lines row for that Agent's own session; it refuses on a
    bad index or a broken chain link, never a silent guess."""
    pool = await _pool_get()
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    from_id = await _resolve(pool, ref)
    if from_id is None:
        return {"error": f"{ref!r} does not resolve to any object. A citation "
                          "needs a real citing Decision/Thread/Evaluation"}
    try:
        result = await capture.mint_transcript_citation(
            Actions(pool), from_id, agent, line_idx, because, source=actor)
    except ValueError as err:
        return {"error": str(err)}
    return result


@mcp.tool()
async def read_citation(
    ref: str, agent: str,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Reads an existing transcript citation: finds the live `cites` edge from
    `ref` to the Agent that `agent` resolves to, re-verifies its stored line_hash
    against the soul store's own chain (never trusting the edge property alone),
    and returns the actual cited line's text. Refuses (an `{"error": ...}`
    response) on a missing edge, an unresolved `agent`, or a hash mismatch: a
    tampered or stale citation never returns a line silently."""
    pool = await _pool_get()
    from_id = await _resolve(pool, ref)
    if from_id is None:
        return {"error": f"{ref!r} does not resolve to any object"}
    try:
        result = await capture.read_transcript_citation(pool, from_id, agent)
    except ValueError as err:
        return {"error": str(err)}
    ident = await _ident_for(ctx)
    await _stamp_read_ids(pool, ident, "read_citation", [from_id])
    return result


@mcp.tool()
async def open_thread(
    summary: str, repo: str | None = None, kind: str | None = None,
    owner: str | None = None, assignee: str | None = None, arc: str | None = None,
    resolves: str | list[str] | None = None,
    branch: str | None = None, files_touched: list[str] | None = None,
    unlinked_because: str | None = None,
    stale_after_days: int | None = None,
    session_anchor: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Opens a Thread: an unresolved question or next step for the next session to
    pick up. Surfaces in run_composition('briefing'). `repo` files it under a
    project. Idempotent on the summary and on a near-duplicate (`deduped`/
    `dedup_scope` name the twin among this project's own OPEN threads instead of
    minting a new one). A genuinely new thread gets `prior_art`/`prior_art_flag`: a
    standing Decision/Practice/Thread that may already cover this ground, surfaced
    only, never a refusal.

    CLASSIFICATION RULES: `kind` is required ('obligation' for a duty minted by an
    action, 'question', 'task', or another value that genuinely fits); omitting it
    refuses rather than minting a kindless thread. `owner`/`assignee` must resolve
    to an active Seat or the literal 'operator' (via `resolve_owner_seat`: a seat
    id, a seat's own handle, an `agent:<id>` whose lineage_head currently holds a
    seat, or a project name with a chartered coordinator seat); anything that
    doesn't resolve refuses, naming what was tried. Unowned obligations default to
    the caller's own seat; unowned general threads stay legitimately unowned.
    `kind='obligation'` additionally refuses from an unmounted caller (no live
    agent/operator identity behind this call): a duty is a mind's own testimony,
    never an anonymous write's. This rule is scoped to this tool alone, never to
    capture.open_thread itself; internal callers (settle(), fleet_reconcile.py,
    the miner's own _emit_thread) keep their own, already-correct conventions.
    `assignee` leases a single-assignee obligation to one build; a near-duplicate
    then surfaces `leased_to` instead of deduping silently. `arc` sorts into the
    roadmap taxonomy (osiris-scoped only). `resolves` closes a predecessor thread
    this one supersedes, with the same UUID/canonical/short-id-only strictness as
    record_decision's `resolves`. `branch`/`files_touched` mark held work;
    `colliding_work` names any open collision. `unlinked_because` mirrors
    record_decision's own escape hatch for an unlinked write. `stale_after_days`
    applies to `kind='obligation'` only: the window (default 14 days) past which
    it surfaces on its owner's own next stop as a named follow-up; unrelated to
    the separate idle-since-touched cron. See `consult_canon('open_thread')` for
    more."""
    pool = await _pool_get()
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    # Write-time classification rules: a census before this change found 242 open
    # threads, 94 with no kind and 87 owned by a bare handle in four different casings,
    # because nothing refused a malformed write. Scoped to THIS tool (the entry point an
    # agent actually calls), never capture.open_thread itself: internal callers
    # (settle(), fleet_reconcile.py, the miner's own _emit_thread) have their own,
    # already-correct conventions and would break for no reason under a blanket refusal
    # one layer down.
    if not kind:
        return {"error": "kind is required. A missing kind is exactly what this "
                         "rule refuses; pass "
                         "'obligation'/'question'/'task', or another value that "
                         "genuinely fits"}
    if kind == "obligation" and actor == "session":
        # `actor` reads 'session' ONLY when nothing is mounted on this connection
        # (_source_for's own back-compat fallback): a genuinely unattributed call, the
        # "derived/anonymous write" shape this refusal targets. A real agent (mounted, or
        # a registered subagent) always resolves to `agent:<id>` here instead.
        return {"error": "an unmounted caller cannot declare kind='obligation'. A duty "
                         "is a mind's own testimony; mount first, or "
                         "use kind='question'/'task' instead"}
    # An unfiled thread is invisible to its own project: a past handoff thread opened
    # without repo= stayed hidden from orient and the succession note until the next
    # session had to mine transcripts with regex to find it. The mounted identity already
    # knows the project, so filing there is the default; staying unfiled takes deliberate
    # effort. Same resolution steps record_decision's wrapper uses: the generation-scoped
    # mount default, then (threads have a worse orphan rate than decisions, 15-21%/week
    # vs. 5-11%) the lineage-wide widen when that finds nothing.
    ident = await _ident_for(ctx)
    _repo_default = await capture.resolve_repo_default(
        pool, repo, actor, ident.project if ident else None)
    repo = _repo_default["repo"]
    repo_defaulted = _repo_default["repo_defaulted"]
    lineage_attempted = _repo_default["lineage_attempted"]
    lineage_candidates = _repo_default["lineage_candidates"]
    lineage_projects = _repo_default["lineage_projects"]
    if owner or assignee:
        from src.orchestrator.owner_normalization import resolve_owner_seat

        _owner_to_check = assignee if assignee is not None else owner
        assert _owner_to_check is not None  # the `if` above guarantees one is truthy
        resolved_owner = await resolve_owner_seat(pool, _owner_to_check, project=repo)
        if resolved_owner is None:
            return {"error": f"owner {_owner_to_check!r} does not resolve to any active "
                             "seat, agent, or 'operator'. "
                             "Pass a seat id, a seat's own handle, an agent id whose "
                             "lineage currently holds a seat, a project name with a "
                             "chartered coordinator seat, or 'operator'"}
        if assignee is not None:
            assignee = resolved_owner
        else:
            owner = resolved_owner
    dup = await capture.find_near_duplicate_open_thread(pool, summary, repo=repo)
    if dup is not None:
        out: dict[str, Any] = {"id": str(dup), "summary": summary, "status": "open",
                               "deduped": "true",
                               "dedup_scope": "a near-exact twin among this project's own "
                                              "OPEN Threads (find_near_duplicate_open_thread)"}
        # Write-boundary honesty rule: a dedup hit returns here, before kind/arc/etc. are
        # ever applied, and a past incident found 17 threads that got a clean-looking
        # result while nothing actually landed. capture.discarded_on_noop names which of
        # THESE two supplied fields would have changed the existing thread;
        # owner/assignee keeps its own bespoke lease-visibility note below (a sharper
        # message than a generic diff would give it). branch/files_touched/resolves are
        # not yet wired into this check, a named gap, not a silent one; see the
        # function's own docstring. `owner` closes the exact gap discarded_on_noop's own
        # docstring already named as its first known specimen: "owner" was listed there
        # as a motivating case but never actually passed into `supplied` below, so a bare
        # owner= on a dedup hit read as a clean result while nothing landed, the same
        # failure `assignee` already gets its own bespoke lease note for.
        supplied = {k: v for k, v in {"kind": kind, "arc": arc, "owner": owner}.items()
                   if v is not None}
        if supplied:
            existing_vals = await capture._thread_named_properties(pool, dup, tuple(supplied))
            discarded = capture.discarded_on_noop(supplied, existing_vals)
            if discarded:
                out["discarded"] = discarded
                out["note"] = (
                    f"matched an existing thread. {', '.join(sorted(discarded))} you "
                    "passed here were NOT applied (open_thread never updates an existing "
                    "thread on a dedup hit). Use reclassify_thread to change arc after "
                    "the fact."
                )
        if assignee:
            holder = await capture._current_owner(pool, dup)
            claim = assignee.strip()
            out["leased_to"] = holder or "(unowned)"
            lease_note = (
                f"already leased to {holder} (thread {str(dup)[:8]}), no new build minted"
                if holder == claim else
                f"existing lease on thread {str(dup)[:8]} is held by "
                f"{holder or '(unowned)'!r}, not {claim!r}. Surfaced instead of minting a "
                "parallel build (a double-assignment must be visible, not silent)"
            )
            out["note"] = f"{out['note']} {lease_note}" if out.get("note") else lease_note
        return out
    # resolve BEFORE recording, same discipline record_decision's own resolves= uses: for
    # the returned result only (what a caller sees closed in the SAME turn); the actual
    # write happens inside capture.open_thread, which resolves `resolves` again itself so
    # its own return type (a bare UUID, ~20 existing call sites) never has to change to
    # carry this.
    resolved_receipt: list[dict[str, str]] = []
    single_resolved_summary: str | None = None
    if isinstance(resolves, list):
        for ref in resolves:
            tid = await capture._find_thread(pool, ref, require_identifier=True)
            if tid is None:
                resolved_receipt.append({"ref": ref, "matched": "false",
                                         "note": "matched no thread. Quote its UUID, "
                                                 "canonical, or 8-char short id"})
                continue
            summ = await capture._thread_summary(pool, tid)
            resolved_receipt.append({"ref": ref, "matched": "true", "id": str(tid)[:8],
                                     "summary": summ or ""})
    elif resolves:
        single = await capture._find_thread(pool, resolves, require_identifier=True)
        if single is None:
            return {"error": f"resolves matched no thread: {resolves!r}. Quote its UUID, "
                             "canonical, or 8-char short id (no prose match: an "
                             "addressing act refuses rather than guesses)"}
        single_resolved_summary = await capture._thread_summary(pool, single)
    try:
        t = await capture.open_thread(
            Actions(pool), summary, repo=repo, kind=kind, owner=owner, assignee=assignee,
            arc=arc, resolves=resolves, branch=branch, files_touched=files_touched,
            source=actor,
            repo_evidence_class=(EvidenceClass.DIRECT_OBSERVATION.value
                                  if repo_defaulted else None),
            unlinked_because=unlinked_because, stale_after_days=stale_after_days,
        )
    except ValueError as e:
        return {"error": str(e)}
    await provenance.stamp_possible_upstream(Actions(pool), written_object_id=t, source_id=actor)
    if arc and not await capture.arc_in_scope(pool, repo):
        arc_receipt = capture._arc_out_of_scope_note(f"repo:{repo}" if repo else "(no project)")
    else:
        arc_receipt = arc or capture._ARC_UNSORTED
    out = {"id": str(t), "summary": summary, "status": "open", "deduped": "false",
          "dedup_scope": "checked only this project's own OPEN Threads for a near-exact "
                         "twin (find_near_duplicate_open_thread), not standing Decisions, "
                         "Practices, or resolved Threads; see prior_art below for those",
          "arc": arc_receipt}
    # Prior-art surfacing: open_thread was the one write action of the three
    # (record_decision, send, open_thread) with no semantic prior-art check at all. An
    # earlier change ported open_thread's own near-duplicate check over to
    # record_decision but never brought this capability back the other way. Uses the
    # same shared engine both those entry points already call (_surface_prior_art,
    # fail-open/15s-bound): surfacing only, never a refusal, and no
    # acknowledge-prior-art/polarity machinery, since open_thread has no
    # confirms=/refutes=/rediscovers= of its own to route an acknowledgement through,
    # unlike record_decision. Deliberately scoped to the MINT path only (never the
    # dedup-hit early return above, and never settle()'s own threads_open batch loop,
    # which calls capture.open_thread directly and was already outside this wrapper's
    # dedup check too): a caller who already matched an existing open Thread doesn't
    # need a second search to be told something related exists.
    prior = await _surface_prior_art(pool, summary, repo=repo, actor=actor)
    if prior:
        out["prior_art"] = _slim_prior_art(prior)
        if capture.prior_art_is_strong(prior):
            top = prior[0]
            if top.get("type") == "Thread":
                out["prior_art_flag"] = (
                    f"this appears to speak to open thread {top['id']}. If this new one "
                    f"is meant to close it, pass resolves=['{top['id']}'] next time; "
                    "otherwise it's worth reading before this stands as a separate duty")
            else:
                out["prior_art_flag"] = (
                    f"a standing {(top.get('type') or 'Decision').lower()} ({top['id']}) "
                    "may already cover this ground. Read it before this stands as a new "
                    "finding")
    if repo_defaulted:
        out["repo_defaulted"] = {
            "to": repo,
            "why": "no repo given, so it defaulted to the caller's own project instead "
                   "of being left unlinked.",
        }
    elif lineage_attempted:
        # Same shared post-mint step record_decision's wrapper uses: the generation-
        # scoped default and the lineage-root widening both failed to name a single
        # project, so abstain and record why, candidate ids kept whole, via the one
        # shared primitive every orphan-healing path calls (capture.derive_or_abstain).
        out["lineage_repo_derivation"] = await capture.record_lineage_abstain(
            pool, t, actor, lineage_candidates, lineage_projects)
    if assignee:
        out["assignee"] = assignee.strip()
    elif kind == "obligation" and not owner:
        # Default, never refuse, never silent: neither `owner` nor `assignee` were
        # supplied for a duty, so capture.open_thread may have defaulted one to the
        # caller's own seat. Read back what actually landed rather than re-deriving it
        # here, so the returned result can never drift from the write.
        landed_owner = await capture._current_owner(pool, t)
        if landed_owner:
            out["owner_defaulted"] = {
                "to": landed_owner,
                "why": "kind='obligation' with no owner given, so it defaulted to the "
                       "caller's own seat instead of being left ownerless.",
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


# The thread object-type dispatcher: fourth object-type dispatcher, absorbing
# `thread_action` itself (already an action-dispatcher folding
# resolve_thread/annotate_thread/correct_thread_summary/reclassify_thread into it) into
# the object-type-dispatcher naming convention and its hand-built oneOf schema. This is
# a genuine re-platforming, not a second fold of the same four names again.
# `open_thread` deliberately stays out and separately named: it MINTS a new Thread,
# while every action here only ever acts on one that already exists, the same "create
# vs act-on-existing" boundary retire_object/seat(action='retire') already draw.
# `_thread_action_impl` itself is unchanged: still the one shared body behind six
# names now (thread, thread_action, resolve_thread, annotate_thread,
# correct_thread_summary, reclassify_thread), all forwarding to the identical
# implementation.
#
# `ref` is the one param needing its own schema shape: a plain string for every action
# except `resolve`, which also accepts a list (batch mode). `_ref_or_list_s()` below is
# used only on that one branch.
def _ref_or_list_s() -> dict[str, Any]:
    return {"anyOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}]}


THREAD_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "oneOf": [
        _dispatcher_action_schema({
            "action": _action_const("resolve"), "ref": _ref_or_list_s(),
            "because": _opt_s(), "artifact": _opt_s(), "dry_run": _b(True),
            **_SUBAGENT_TRIO,
        }, ["action", "ref"]),
        _dispatcher_action_schema({
            "action": _action_const("annotate"), "ref": _s(), "note": _s(),
            "corrected_summary": _opt_s(), "because": _opt_s(), **_SUBAGENT_TRIO,
        }, ["action", "ref", "note"]),
        _dispatcher_action_schema({
            "action": _action_const("correct_summary"), "ref": _s(),
            "corrected_summary": _s(), "because": _opt_s(), **_SUBAGENT_TRIO,
        }, ["action", "ref", "corrected_summary"]),
        _dispatcher_action_schema({
            "action": _action_const("reclassify"), "ref": _s(), "kind": _s(),
            "because": _opt_s(), "owner": _opt_s(), "arc": _opt_s(), **_SUBAGENT_TRIO,
        }, ["action", "ref", "kind"]),
    ],
}
_HAND_BUILT_SCHEMAS["thread"] = THREAD_INPUT_SCHEMA

_THREAD_ACTION_PARAMS: dict[str, tuple[list[str], list[str]]] = {
    "resolve": (["ref", "because", "artifact", "dry_run"], ["ref"]),
    "annotate": (["ref", "note", "corrected_summary", "because"], ["ref", "note"]),
    "correct_summary": (["ref", "corrected_summary", "because"], ["ref", "corrected_summary"]),
    "reclassify": (["ref", "kind", "because", "owner", "arc"], ["ref", "kind"]),
}


async def _thread_action_impl(
    ref: str | list[str], action: str, *, because: str | None, artifact: str | None,
    dry_run: bool, note: str | None, corrected_summary: str | None, kind: str | None,
    owner: str | None, arc: str | None, ctx: Context | None,
    subagent_id: str | None, subagent_type: str | None,
) -> dict[str, Any]:
    """Shared body behind `thread`, `thread_action`, and four hidden single-purpose
    aliases (resolve_thread/annotate_thread/correct_thread_summary/reclassify_thread):
    one code path, six names. Each action below is copied verbatim from what was that
    alias's own top-level function body before the original fold; nothing about
    resolve_thread's own batch mode or its dry_run=True default changed in either move
    (the exact shape a past incident needed preserved).

    Pre-dispatch validation, same discipline as _seat_impl's own, added when `thread`
    itself was built. The original fold relied on inline `assert`s alone, now redundant
    with this but left in place as a second belt-and-suspenders layer, not removed."""
    if action not in _THREAD_ACTION_PARAMS:
        return {"error": f"unknown action {action!r}",
                "known_actions": sorted(_THREAD_ACTION_PARAMS)}
    accepted, required = _THREAD_ACTION_PARAMS[action]
    local = dict(locals())
    missing = [p for p in required if local.get(p) in (None, "")]
    if missing:
        return {"error": f"action {action!r} is missing required param(s) {missing}",
                "action_accepts": accepted, "action_requires": required}
    pool = await _pool_get()
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    if action == "resolve":
        if isinstance(ref, list):
            return await capture.resolve_threads_bulk(
                Actions(pool), ref, because=because or "", artifact=artifact,
                dry_run=dry_run, source=actor)
        probe_tid = await capture._find_thread(pool, ref)
        was_already_resolved = (
            probe_tid is not None
            and await capture._thread_resolved_in(pool, probe_tid) is not None)
        tid = await capture.resolve_thread(
            Actions(pool), ref, because=because, artifact=artifact, source=actor)
        if tid is None:
            return {"error": f"no thread matches {ref!r}"}
        out: dict[str, Any] = {"id": str(tid), "status": "resolved"}
        if was_already_resolved:
            out["note"] = ("this thread was already resolved before this call — "
                           "because/resolved_artifact now reflect THIS call's own text, "
                           "not the original close; earlier reasoning is still readable "
                           "in the graph's history, not overwritten there, just not what "
                           "a current-value read shows anymore")
        if artifact:
            out["artifact"] = f"{artifact} — kept as resolved_artifact"
            target = await pool.fetchrow(
                "SELECT o.type, o.canonical FROM links l JOIN objects o ON o.id=l.to_id "
                "WHERE l.from_id=$1 AND l.type='resolved_by' LIMIT 1", tid)
            out["resolved_by"] = (
                f"{target['type']} {target['canonical']} — the strong closure witness"
                if target is not None else
                "none — the artifact did not resolve to a graph object (a file:line or "
                "an unmatched pointer); resolved_artifact still carries it as text, and "
                "a closed_by edge to the resolving agent was minted instead — the weak "
                "witness, still traversable, just not naming a specific commit/decision"
            )
        return out
    if action == "annotate":
        assert isinstance(ref, str)
        assert note is not None
        try:
            tid = await capture.annotate_thread(
                Actions(pool), ref, note, corrected_summary=corrected_summary,
                because=because, source=actor)
        except ValueError as e:
            return {"error": str(e)}
        if tid is None:
            return {"error": f"no thread matches {ref!r}"}
        await provenance.stamp_possible_upstream(Actions(pool), written_object_id=tid,
                                                 source_id=actor)
        out = {"id": str(tid), "note": note.strip(), "status": "annotated"}
        if corrected_summary:
            out["corrected_summary"] = corrected_summary.strip()
        return out
    if action == "correct_summary":
        assert isinstance(ref, str)
        assert corrected_summary is not None
        try:
            tid = await capture.correct_thread_summary(
                Actions(pool), ref, corrected_summary, because=because, source=actor)
        except ValueError as e:
            return {"error": str(e)}
        if tid is None:
            return {"error": f"no thread matches {ref!r}"}
        out = {"id": str(tid), "corrected_summary": corrected_summary.strip(),
               "status": "corrected"}
        if because:
            out["because"] = because.strip()
        return out
    if action == "reclassify":
        assert isinstance(ref, str)
        assert kind is not None
        # Same two checks open_thread runs, since reclassify is the other live entry
        # point onto a thread's kind/owner. Adopting a miner echo as an obligation
        # (reclassify's own documented use) is still an agent's act, refused only when
        # nothing is mounted.
        if kind == "obligation" and actor == "session":
            return {"error": "an unmounted caller cannot declare kind='obligation' — a "
                             "duty is a mind's own testimony (thread b5ae6773); mount "
                             "first, or use kind='question'/'task' instead"}
        if owner:
            from src.orchestrator.owner_normalization import resolve_owner_seat

            resolved_owner = await resolve_owner_seat(pool, owner)
            if resolved_owner is None:
                return {"error": f"owner {owner!r} does not resolve to any active seat, "
                                 "agent, or 'operator' (thread b5ae6773's owner law) — "
                                 "pass a seat id, a seat's own handle, an agent id whose "
                                 "lineage currently holds a seat, or 'operator'"}
            owner = resolved_owner
        t = await capture.reclassify_thread(
            Actions(pool), ref, kind=kind, because=because, owner=owner, arc=arc,
            source=actor)
        if t is None:
            return {"error": f"no thread matched {ref!r}"}
        out = {"id": str(t), "kind": kind,
               "status": "open (unchanged — reclassified, not resolved)"}
        if owner:
            # Resolved and passed into capture.reclassify_thread just above: the kind
            # change was already confirmed in the returned result, the owner change
            # never was.
            out["owner"] = owner
        if arc:
            if await capture.arc_in_scope_for_thread(pool, t):
                out["arc"] = arc
            else:
                rows = await pool.fetch(
                    "SELECT o.canonical FROM links l JOIN objects o ON o.id=l.to_id "
                    "WHERE l.from_id=$1 AND l.type='in_repo' "
                    "AND (l.valid_until IS NULL OR l.valid_until > now())", t)
                label = ", ".join(r["canonical"] for r in rows) or "(no project)"
                out["arc"] = capture._arc_out_of_scope_note(label)
        return out
    return {"error": f"unknown action {action!r} — one of resolve/annotate/"
                     "correct_summary/reclassify"}


@mcp.tool()
async def thread(
    ref: str | list[str], action: str, because: str | None = None,
    artifact: str | None = None, dry_run: bool = True, note: str | None = None,
    corrected_summary: str | None = None, kind: str | None = None,
    owner: str | None = None, arc: str | None = None,
    subagent_id: str | None = None,
    subagent_type: str | None = None, session_anchor: str | None = None,
    ctx: Context | None = None
) -> dict[str, Any]:
    """Acts on an existing Thread: one tool, four `action`s. `open_thread` stays
    separate since it mints a new Thread; every action here only ever acts on one
    that already exists.

    ACTION TABLE, action: what it does (required params beyond action):
      resolve: close it (ref, because is a short reason, not a completion essay).
        `artifact` points at what actually closed it (a commit hash, decision id,
        file:line, or `repo:<name>@<hash>` to disambiguate a short hash that
        collides across two or more ingested repos' commits; a bare hash otherwise
        resolves against every ingested repo's commits, unscoped, so a fix in one
        project can close a thread in another). It is kept as `resolved_artifact`;
        when it names a graph object, a `resolved_by` edge is minted too.
        Re-resolving is allowed (the latest closure witness wins, earlier
        reasoning stays in history). A LIST `ref` closes a batch: `because` becomes
        mandatory, `dry_run` defaults to true and previews without writing (pass
        `dry_run=False` explicitly to actually close the batch), and the whole
        batch refuses if any ref does not resolve to exactly one thread. `dry_run`
        has no effect for a single `ref` (the single-ref primitive
        `capture.resolve_thread` has never taken one, and passing `dry_run=True`
        on a bare ref still resolves it for real): disclosed here since the schema
        itself offers the param uniformly for both shapes.
      annotate: add `note` without closing it or touching `summary`/`status` (ref,
        note); each call appends independently, never supersedes an earlier note.
        Optional `corrected_summary`/`because` fix the headline in the same call,
        same as correct_summary below.
      correct_summary: replace the headline in place via `corrected_summary` (ref,
        corrected_summary; `summary` itself, the dedup key, is never touched);
        re-calling supersedes the prior correction rather than piling up notes.
        `because` optional.
      reclassify: set `kind` ('obligation'/'question'/'task') without changing
        status (ref, kind); untouched means not resolved. `because` records your
        reasoning, `owner` optionally claims it in the same act, `arc` backfills
        open_thread's own closed taxonomy onto an already-open thread
        (osiris-scoped, dropped and named elsewhere).

    `ref` is a Thread UUID, canonical, short-id prefix, or summary substring (a
    list only for `action='resolve'`'s own batch mode)."""
    return await _thread_action_impl(
        ref, action, because=because, artifact=artifact, dry_run=dry_run, note=note,
        corrected_summary=corrected_summary, kind=kind, owner=owner, arc=arc, ctx=ctx,
        subagent_id=subagent_id, subagent_type=subagent_type)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "thread(action=...)",
    "since": "the thread() dispatcher was introduced",
})
async def thread_action(
    ref: str | list[str], action: str, because: str | None = None,
    artifact: str | None = None, dry_run: bool = True, note: str | None = None,
    corrected_summary: str | None = None, kind: str | None = None,
    owner: str | None = None, arc: str | None = None,
    subagent_id: str | None = None,
    subagent_type: str | None = None, session_anchor: str | None = None,
    ctx: Context | None = None
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to thread(action=...)."""
    return await _thread_action_impl(
        ref, action, because=because, artifact=artifact, dry_run=dry_run, note=note,
        corrected_summary=corrected_summary, kind=kind, owner=owner, arc=arc, ctx=ctx,
        subagent_id=subagent_id, subagent_type=subagent_type)


async def _proposal_action_impl(
    pool: asyncpg.Pool, actor: str, action: str, *, from_id: str | None = None,
    link_type: str | None = None, candidate: dict[str, Any] | None = None,
    confidence: float | None = None, owner: str | None = None, miner: str | None = None,
    proposal_ref: str | None = None, reason: str | None = None,
) -> dict[str, Any]:
    """The proposal() MCP tool's own body, factored out so `osiris proposal` (the CLI
    entry point) calls the same implementation rather than a second copy that could
    drift, the identical shape `_thread_action_impl` already holds for
    `thread(action=...)`."""
    from src.orchestrator.proposals import accept as _accept
    from src.orchestrator.proposals import propose as _propose
    from src.orchestrator.proposals import reject as _reject

    if action == "propose":
        if not from_id or not link_type or candidate is None or confidence is None \
                or not owner or not miner:
            return {"error": "propose needs from_id, link_type, candidate, confidence, "
                             "owner, and miner"}
        resolved_from_id = await pool.fetchval(
            "SELECT id FROM objects WHERE canonical=$1", from_id)
        if resolved_from_id is None:
            try:
                resolved_from_id = uuid.UUID(from_id)
            except ValueError:
                return {"error": f"from_id {from_id!r} names no object by canonical "
                                 "and is not a raw uuid"}
        return await _propose(
            Actions(pool), from_id=resolved_from_id, link_type=link_type,
            candidate=candidate, confidence=confidence, owner=owner, miner=miner,
            actor=actor)
    if action == "accept":
        if not proposal_ref:
            return {"error": "accept needs proposal_ref"}
        return await _accept(Actions(pool), proposal=proposal_ref, actor=actor)
    if action == "reject":
        if not proposal_ref or not reason:
            return {"error": "reject needs proposal_ref and reason"}
        return await _reject(Actions(pool), proposal=proposal_ref, reason=reason,
                             actor=actor)
    return {"error": f"unknown action {action!r}, expected propose, accept, or reject"}


@mcp.tool()
async def proposal(
    action: str, from_id: str | None = None, link_type: str | None = None,
    candidate: dict[str, Any] | None = None, confidence: float | None = None,
    owner: str | None = None, miner: str | None = None,
    proposal_ref: str | None = None, reason: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Miners propose changes here rather than writing to the graph directly. One tool,
    three actions:

      propose: mint a Proposal (never a real graph write) against an existing,
        unresolved `derivation_abstained_<link_type>` property. `from_id` (a canonical
        or raw uuid) names the abstaining object; `link_type` must match the
        abstention's own namespace exactly; `candidate` is what would be minted if
        accepted ({"kind":"link", "from_id","to_id","link_type"} or
        {"kind":"object", "type","canonical","properties":{...}}); `owner` resolves to
        an active seat, its handle, or 'operator'; `miner` names the proposing miner.
        Refuses if there is no live abstention, the owner cannot be resolved, or the
        candidate is malformed.
      accept: mints the real object/link `proposal_ref` names, recorded under this
        call's own identity, not the miner's. Refuses on anything but a live,
        unexpired, still-`proposed` Proposal.
      reject: retires `proposal_ref` (status='rejected') with a mandatory `reason` the
        proposing miner reads back on its own next tick.

    Nothing calls propose() automatically yet; it is only ever invoked directly."""
    ident = await _ident_for(ctx)
    actor = ident.agent_id if ident is not None else "console:proposal"
    pool = await _pool_get()
    return await _proposal_action_impl(
        pool, actor, action, from_id=from_id, link_type=link_type, candidate=candidate,
        confidence=confidence, owner=owner, miner=miner, proposal_ref=proposal_ref,
        reason=reason)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "thread(action='resolve')",
    "since": "an earlier tool consolidation",
})
async def resolve_thread(
    ref: str | list[str], because: str | None = None, artifact: str | None = None,
    dry_run: bool = True,
    subagent_id: str | None = None,
    subagent_type: str | None = None, session_anchor: str | None = None,
    ctx: Context | None = None
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    thread(action='resolve')."""
    return await _thread_action_impl(
        ref, "resolve", because=because, artifact=artifact, dry_run=dry_run, note=None,
        corrected_summary=None, kind=None, owner=None, arc=None, ctx=ctx,
        subagent_id=subagent_id, subagent_type=subagent_type)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "thread(action='annotate')",
    "since": "an earlier tool consolidation",
})
async def annotate_thread(
    ref: str, note: str,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, str]:
    """Deprecated: hidden alias, still callable. Forwards to
    thread(action='annotate')."""
    out = await _thread_action_impl(
        ref, "annotate", because=None, artifact=None, dry_run=True, note=note,
        corrected_summary=None, kind=None, owner=None, arc=None, ctx=ctx,
        subagent_id=subagent_id, subagent_type=subagent_type)
    return out


@mcp.tool()
async def rematerialize(
    anchor_sid: str, dest: str | None = None, force: bool = False,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Reconstruct a session's transcript byte-for-byte from soul_lines alone, written
    to disk. `anchor_sid` is the 8-char session anchor. Verifies the hash chain while
    collecting: a break returns `{"error": ..., "verified_through": N}` and nothing is
    written. `dest` defaults to the session's own recorded source_path.

    Refuses to overwrite a live transcript: if `dest` was modified more recently than
    this session's last ingest, overwriting it would clobber unseen content;
    `force=True` overrides. Success
    returns `{"written": <path>, "lines": N, "sha256": <hex>}`."""
    from src.ingest.soul_store import SoulStore

    pool = await _pool_get()
    return await SoulStore(pool).rematerialize_to_disk(anchor_sid, dest=dest, force=force)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "seat(action='heal_transcript')",
    "since": "an earlier tool consolidation",
})
async def heal_seat_transcript(
    handle: str, source_paths: list[str], dry_run: bool = True, because: str = "",
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to
    seat(action='heal_transcript')."""
    return await _seat_impl("heal_transcript", target=handle, source_paths=source_paths,
                            dry_run=dry_run, because=because, ctx=ctx)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "thread(action='correct_summary')",
    "since": "an earlier tool consolidation",
})
async def correct_thread_summary(
    ref: str, corrected_summary: str, because: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, str]:
    """Deprecated: hidden alias, still callable. Forwards to
    thread(action='correct_summary')."""
    return await _thread_action_impl(
        ref, "correct_summary", because=because, artifact=None, dry_run=True, note=None,
        corrected_summary=corrected_summary, kind=None, owner=None, arc=None, ctx=ctx,
        subagent_id=subagent_id, subagent_type=subagent_type)


@mcp.tool()
async def amend_decision(
    ref: str, addendum: str,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, str]:
    """Append reasoning to a live decision as understanding develops, without superseding
    it. `record_decision` can only mint a fresh decision or supersede an old one with a
    correction; this tool instead adds more reasoning to the same still-standing
    decision, later. `ref` is a Decision UUID, canonical, short-id prefix, or summary
    substring. `summary`/`rationale`/`kind` are never touched here.
    Returns {"error": ...} (never raises past this wrapper) when `ref` matches nothing,
    or when it resolves to a decision already superseded: amend the successor instead,
    or use record_decision(supersedes=...) if you mean a correction. This tool only
    ever adds to a decision that is still standing."""
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


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "practice(action='amend')",
    "since": "an earlier tool consolidation",
})
async def amend_practice(
    ref: str, amendment: str,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, str]:
    """Deprecated: hidden alias, still callable. Forwards to
    practice(action='amend')."""
    return await _practice_impl(
        "amend", ref=ref, amendment=amendment, subagent_id=subagent_id,
        subagent_type=subagent_type, ctx=ctx)


async def _lease_impl(
    action: str, resource_id: str, *, holder: str | None, older_than_secs: int | None,
    ctx: Context | None, subagent_id: str | None, subagent_type: str | None,
) -> dict[str, Any]:
    """Shared implementation behind `lease` and its four hidden single-purpose aliases
    (acquire_lease/release_lease/check_lease/reap_stale_leases): one code path, five
    names."""
    pool = await _pool_get()
    if action == "acquire":
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
                           f"{result.acquired_at.isoformat()}, no new claim minted")
        return out
    if action == "release":
        actor = await _actor_for(ctx, subagent_id, subagent_type)
        released = await resource_lease.release(pool, resource_id, actor)
        return {"resource_id": resource_id, "released": released}
    if action == "check":
        held = await resource_lease.current_holder(pool, resource_id)
        if held is None:
            return {"resource_id": resource_id, "held": False}
        return {
            "resource_id": resource_id, "held": True, "holder": held["holder"],
            "held_since": held["acquired_at"].isoformat(),
            "thread_id": str(held["thread_id"]),
        }
    if action == "reap":
        try:
            n = await resource_lease.reap_stale(
                pool, older_than_secs=older_than_secs or 3600)
        except ValueError as e:
            return {"error": str(e)}
        return {"reaped": n, "older_than_secs": older_than_secs or 3600}
    return {"error": f"unknown action {action!r}, expected one of acquire/release/check/reap"}


@mcp.tool()
async def lease(
    action: str, resource_id: str = "", holder: str | None = None,
    older_than_secs: int | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Coordinate over any genuinely shared, non-isolable resource by an exact id
    (`deploy`, `docker-daemon`, the live server), not a working tree, which has no
    contention to coordinate. `resource_id` is convention, not a closed vocabulary.
    Four `action`s, never a fifth.

    `action='acquire'`: claim it, matched by equality, backed by a real DB uniqueness
    constraint, never a race (unlike open_thread(assignee=)'s fuzzy prose match).
    `holder` defaults to your own mounted identity; pass one to claim on another's
    behalf. A refusal names who holds it and since when. There is no renewed TTL:
    `release` is the primary way to end a hold; `reap` is only a crash/compaction
    backstop, not the normal path.

    `action='release'`: free a resource you hold. Only the actual holder's own release
    frees it, never a different agent's, even by name: there is no `holder` param here,
    the identity checked is always the caller's own resolved actor. `released: false`
    for both an unheld resource and a wrong-holder attempt: both are refusals to
    report, never errors. Call `check` first if you need to tell the two apart.

    `action='check'`: read-only. Reports who holds it right now, or that it's free.
    Never claims, never mints, never leases anything.

    `action='reap'`: recover leases nobody released (a crash, a compaction, a dropped
    session); without this, the active-claim constraint would wedge that `resource_id`
    forever. This is a backstop, not the norm (a 5-min cron already runs it).
    `older_than_secs` defaults to 3600 (paced to agent work, not machine time), with a
    60s floor enforced (below it, every held lease fleet-wide would be force-released
    at once), and is refused loudly if violated."""
    return await _lease_impl(
        action, resource_id, holder=holder, older_than_secs=older_than_secs, ctx=ctx,
        subagent_id=subagent_id, subagent_type=subagent_type)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "lease(action='acquire')",
    "since": "an earlier tool consolidation",
})
async def acquire_lease(
    resource_id: str, holder: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to lease(action='acquire')."""
    return await _lease_impl(
        "acquire", resource_id, holder=holder, older_than_secs=None, ctx=ctx,
        subagent_id=subagent_id, subagent_type=subagent_type)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "lease(action='release')",
    "since": "an earlier tool consolidation",
})
async def release_lease(
    resource_id: str,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to lease(action='release')."""
    return await _lease_impl(
        "release", resource_id, holder=None, older_than_secs=None, ctx=ctx,
        subagent_id=subagent_id, subagent_type=subagent_type)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "lease(action='check')",
    "since": "an earlier tool consolidation",
})
async def check_lease(resource_id: str) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to lease(action='check')."""
    return await _lease_impl(
        "check", resource_id, holder=None, older_than_secs=None, ctx=None,
        subagent_id=None, subagent_type=None)


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "lease(action='reap')",
    "since": "an earlier tool consolidation",
})
async def reap_stale_leases(older_than_secs: int = 3600) -> dict[str, Any]:
    """Deprecated: hidden alias, still callable. Forwards to lease(action='reap')."""
    return await _lease_impl(
        "reap", "", holder=None, older_than_secs=older_than_secs, ctx=None,
        subagent_id=None, subagent_type=None)


async def _retire_stale_handoffs(
    pool: asyncpg.Pool, actor: str, keep: uuid.UUID, now: datetime, *, max_hops: int = 200,
    dry_run: bool = False,
) -> dict[str, Any]:
    """A one-time backfill utility, not a live trigger. An earlier write-triggered
    version of this was later superseded by an explicit ack_handoff(ref=...) result
    model (see settle()'s own docstring). Kept as a plain function, called manually,
    for exactly one job: cleaning up the population of is_handoff='true' records that
    accumulated before the result model existed and that nobody will ever explicitly
    ack retroactively (there is no way to know, after the fact, who "read" a years-old
    handoff). Not wired into settle() or any other live call path: a fresh is_handoff
    write no longer retires anything automatically.

    Refuses, never degrades, on a truncated walk: this is the one caller of
    `lineage_root` that decides for a whole population at once, so a truncated root
    would silently under-retire. Records that are really the same continuing lineage
    as `actor` would each read as their own separate, unrelated root, and the run
    would look like a clean success while leaving most of the real work undone. If
    `actor`'s own walk is incomplete, the whole call raises `ValueError` before
    touching anything: there is no safe partial answer to "retire everything in my
    lineage" when the caller does not yet know its own lineage's true root. If a
    candidate record's own walk is incomplete, that one record is left untouched and
    named in the result's `skipped_incomplete_walk` (never silently treated as
    same-lineage or cross-lineage: a third, honest outcome).

    Retires every is_handoff='true' record from `actor`'s own lineage (same seat, any
    earlier or same generation, Decision or Thread alike), via `lineage_root`'s
    succeeded_from edge-walk (this carried the identical string-parse defect
    ack_handoff's own lineage guard did; the same fix was applied here for the same
    reason), except `keep`. Cross-lineage records are never touched: one agent's
    handoff is never retired by a different agent's backfill run.
    `rank_open_threads.whose_move` carried the same `_generation()` string-parse
    defect for its own "mine to act" ranking question, measured live on a past run
    (18 of 71 distinct open-thread owners disagreed between the string parse and the
    edge walk, every one a real lineage), then fixed the same way: `owner_roots`
    (precomputed once per caller via `owner_lineage_roots`, never per row; the
    function itself stays synchronous and pure) now wins over the string-parse
    fallback.

    Resolves each candidate's current is_handoff value the same way every other
    property-read in this codebase does (confidence DESC, observed_at DESC LIMIT 1)
    rather than a bare EXISTS(value='true'): a record already acked by a different
    source (ack_handoff runs as the successor, not the original author) would
    otherwise still show up here because its stale 'true' row never physically leaves
    current_assertions. Re-retiring an already-acked record would be harmless
    (idempotent, same eventual state) but is still the wrong thing to assert and
    worth avoiding on principle.

    Never touches `summary`/`kind`/anything else on the retired object: same
    append-only discipline as `amend_decision`/`amend_practice`, an independent
    property, not a rewrite. Returns `{"retired": [...], "skipped_incomplete_walk":
    [...]}`, short ids either way, for the caller's own result: a silent mutation
    behind an already-silent bleed would just be a quieter version of the same
    problem.

    `dry_run=True` runs every read and every `lineage_root` walk exactly as a live
    call would (same refuse-on-incomplete-actor-walk, same per-candidate skip) but
    never calls `actions.assert_property`; `retired` names what would be retired. The
    population is append-only either way: a dry run's own `retired` list is the exact
    set a live call would touch, because both read the identical `current_assertions`
    query and the identical `lineage_root` walk. Nothing about is_handoff resolution
    is time-sensitive between the two calls beyond the ordinary risk of a concurrent
    write landing in between, the same risk any dry-run/execute pair carries.
    Reversal, if a live run ever needs undoing: is_handoff is never deleted, only
    asserted. Re-asserting 'true' (a fresh, higher-`observed_at` row) restores the
    record exactly as `ack_handoff`'s own un-ack would, no bespoke undo path needed."""
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
    """The actual backlog disposition, fleet-wide, composed entirely from
    `_retire_stale_handoffs` (never a second SQL mutation path: the same one caller
    already authorized, just driven once per lineage instead of once per manual
    invocation).

    Finds every live is_handoff='true' record, groups it by `lineage_root`
    (edge-walked), and, within any root with more than one record, keeps the newest
    (by is_handoff's own `observed_at`) and would-retire the rest.

    Refuses the whole run, same rule as `_retire_stale_handoffs` itself, if any
    author in the population has an incomplete `lineage_root` walk:
    `{"ok": False, "reason": ..., "incomplete_authors": [...]}`, nothing touched.
    This is the exact guard that made a past measurement (220 records, one agent's
    lineage fragmenting into 12 fake roots at the old max_hops=64 ceiling) call the
    backlog unsafe to run. Re-verify this box is empty before ever trusting
    `dry_run=False` here; the population moves every session.

    `dry_run=True` (the default: a fleet-wide mutation defaults safe) previews every
    per-root disposition without writing, by threading `dry_run` straight into each
    `_retire_stale_handoffs` call; `dry_run=False` executes them for real, root by
    root. Returns `{"ok": True, "dry_run": ..., "roots_total": ..., "roots_disposed":
    ..., "would_keep": ..., "receipts": [{"root", "keep", "retired"}, ...]}`.
    `receipts` names exactly which record was kept per root and which were (or would
    be) retired, so a reviewer can spot-check before authorizing the live run.

    Reversal: identical to `_retire_stale_handoffs`'s own. is_handoff is asserted,
    never deleted; restoring any retired record is a fresh
    assert_property('is_handoff', 'true') on that one object id, no bespoke undo
    mechanism needed. No merge/unmerge involved: this never touches object identity,
    only the is_handoff property on records that stay exactly the objects they
    always were."""
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
    """A one-time backfill utility, not a live trigger: same shape and same reasoning
    as `_retire_stale_handoffs` right above. `ack_handoff` did not resolve a handoff
    Thread's own `status` until a later fix corrected that going forward. This cleans
    up the population that accumulated before that fix: every Thread whose current
    `is_handoff` is already 'false' (a real, deliberate ack already happened) but
    whose current `status` is still 'open'. The discriminator is the ack, never time:
    this reads is_handoff, never `observed_at`/age, so an unacked handoff (unread,
    not stale) is never touched, only ever a genuinely acknowledged one. `repo`
    optionally scopes to one project's own `in_repo`-linked Threads; omitted, this is
    fleet-wide. Returns short ids resolved, for the caller's own before/after
    re-query, never trusted from a bare count."""
    where_repo = ""
    args: list[Any] = []
    if repo:
        where_repo = (
            " AND EXISTS (SELECT 1 FROM links l JOIN objects p ON p.id=l.to_id "
            "  AND p.type='SoftwareProject' AND p.canonical=$1 "
            "  WHERE l.from_id=o.id AND l.type='in_repo' "
            "  AND (l.valid_until IS NULL OR l.valid_until > now()))")
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
    """Acknowledge a handoff: the only action that retires a live `is_handoff` marker,
    or a legacy record with none, matched by prose alone. orient() delivers a handoff
    unconditionally; this acknowledges it as a separate act naming the id. `ref` is the
    id orient()'s succession_note or recall() gave you, resolved strictly (never a
    free-text guess). Tries Thread then Decision.

    Refuses rather than guesses: an unresolvable ref, an already-acknowledged or
    not-a-handoff record, or a caller outside the author's own lineage (a mistaken ack
    from elsewhere would permanently retire someone else's live handoff). This acts on
    the object, not the reader, so the first ack wins; it is final, not a lease. Never
    deleted: recall()/search() still see it. Resolves a Thread's own `status` too;
    always False for a Decision."""
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
    if row is None:
        return {"error": f"{str(oid)[:8]} is already acknowledged or is not a handoff"}
    if row["is_handoff"] != "true" and not await is_live_handoff(pool, oid):
        # An object with no is_handoff property at all can still be a live handoff via
        # the legacy prose fallback (nearest_handoff_ancestor/get_status's own
        # HANDOFF_LIVE_PREDICATE_SQL). Checked here too so this entry point recognizes
        # exactly what the pointer surfaced, never refusing a real pending handoff just
        # because it predates the structured property.
        return {"error": f"{str(oid)[:8]} is already acknowledged or is not a handoff"}
    if row["author"] is None:
        return {"error": f"{str(oid)[:8]} is not your lineage's handoff to ack"}
    author_root, author_complete = await lineage_root(pool, row["author"])
    actor_root, actor_complete = await lineage_root(pool, actor)
    if not author_complete or not actor_complete:
        return {"error": f"{str(oid)[:8]}: cannot confirm lineage, the succession walk "
                         "did not reach a true origin within the hop bound, so this is "
                         "refused rather than trusted"}
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
    standing_orders: str | None = None, because: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """The end-of-session checklist: deposit everything a session knows before
    compaction destroys its context. With no args, this is a read-only status check
    (completeness boxes plus your open obligations). With `decisions`/`threads_open`/
    `threads_resolve` (each item a dict of that tool's own kwargs) it accepts a batch,
    dispatching to record_decision/open_thread/resolve_thread unchanged, then re-checks
    against the updated graph. `complete` is true only when nothing is left unwritten.

    A bad item in `decisions`/`threads_open` never sinks the rest of the batch: it lands
    in `rejected` (kind/summary/error) and `complete` reads False, but everything else
    still writes. `is_handoff: true` on an item mints a structured marker your
    successor's orient() finds directly; it is retired by their ack_handoff(ref=...),
    not by your next write.

    `repo_path` names your code repo for the git-status box (`uncommitted_git_files`).
    Your mounted working directory is checked only as a fallback, and is usually wrong
    for a seat-office agent. A decision's `resolves=` and a same-call `threads_resolve`
    item naming the same thread get the closure edge wired automatically.

    `standing_orders='unchanged'` (requires `because`) closes the "standing orders
    touched this session" box honestly for a seat whose charter.md/CLAUDE.md genuinely
    did not change this session; without it, a seat-office session otherwise reads
    complete:false forever on this box alone, which blocks compaction from ever
    treating the session as fully deposited. Recorded as a real property
    (`standing_orders_unchanged`), never a silent pass; the box also now closes on its
    own from a `charter()`/`charter_for` or `practice(record|amend)` call this session,
    with no extra argument needed. See consult_canon('settle') for more detail."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first: settle records against a known identity, and the "
                         "graph must know whose", "why": _anchorless(ctx)}
    pool = await _pool_get()
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    now = datetime.now(UTC)

    rejected: list[dict[str, str]] = []
    if standing_orders is not None:
        if standing_orders != "unchanged":
            rejected.append({
                "kind": "standing_orders", "summary": standing_orders,
                "error": "the only recognized value is 'unchanged'. Anything else is "
                         "either a typo or a claim this tool doesn't know how to record",
            })
        elif not because:
            rejected.append({
                "kind": "standing_orders", "summary": "unchanged",
                "error": "because is required. Declaring standing orders unchanged is "
                         "a deliberate claim, not a default",
            })
        else:
            agent_oid = await Actions(pool).create_or_find_object(
                "Agent", ident.agent_id, actor)
            await Actions(pool).assert_property(
                agent_oid, "standing_orders_unchanged", because.strip(), actor, now, 0.9,
                evidence_class="self_declared")

    accepted: dict[str, list[Any]] = {"decisions": [], "threads_opened": [], "threads_resolved": []}
    # settle is the end-of-session ritual: its entire reason to exist is depositing what
    # a dying session knows before that context is destroyed. A whole-batch abort on one
    # bad item (e.g. a path-shaped repo) would lose everything else in the same call,
    # exactly the failure settle exists to prevent: the inverse of resolves/confirms/
    # grounds's own "one bad ref must not veto the rest of the set" a few hundred lines
    # above. `rejected` names every dropped item and why (never a silent partial accept;
    # see `complete` below, which now reads False on any rejection).
    # (declared above, before the standing_orders handling, so a rejected standing_orders
    # claim shows up in the same list as every other rejected item this call makes)
    # settle holds both halves of a decision/thread relationship in one payload: record
    # which thread(s) each accepted decision answered via its own resolves=, so the
    # threads_resolve loop below can wire the reverse edge for a pair this batch itself
    # already establishes. thread_id -> decision_id, first match wins (never a guess, a
    # real match, just possibly not the only one).
    answered_in_batch: dict[uuid.UUID, uuid.UUID] = {}
    for item in decisions or []:
        item = dict(item)
        is_handoff = bool(item.pop("is_handoff", False))
        summary = item.pop("summary")
        resolves_arg = item.get("resolves")
        # This bulk loop calls capture directly and used to bypass the identity default
        # entirely, unlike record_decision/open_thread/ingest_reference's own wrappers.
        # resolve_repo_default is now the one place that default (and its lineage-wide
        # widen) lives, so this loop inherits it for free instead of carrying a fourth
        # differently-shaped copy.
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
                "why": "no repo given, so this defaulted to the caller's own project "
                       "rather than being left unlinked",
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
        # This bulk loop calls capture directly and used to bypass the identity default
        # entirely (unlike the owner default, which does live in capture.open_thread and
        # so already applied here for free). resolve_repo_default/record_lineage_abstain
        # are now the one place the repo default and its lineage-wide widen live,
        # inherited here instead of a fourth copy.
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
            # Resolve the prior marker: minting a new is_handoff Thread never resolved
            # the project's own prior one, so three status markers once stacked up
            # unresolved for the same project because opening a new one had no matching
            # step to close the last. A successor's orient() only ever needs the newest;
            # superseded ones should leave the open list in the same call that supersedes
            # them, not linger forever.
            from src.orchestrator.projects import (
                AmbiguousProjectRef,
                _resolve_software_project,
            )
            try:
                proj_row = await _resolve_software_project(pool, thread_repo)
            except AmbiguousProjectRef:
                proj_row = None
            if proj_row is not None:
                wall, _echoes = await _open_thread_wall(pool, proj_row["id"])
                for w in wall:
                    if w["id"] == str(tid)[:8]:
                        continue
                    if (w.get("is_handoff") or "").strip() == "true":
                        await capture.resolve_thread(
                            Actions(pool), w["id"],
                            because="superseded by a newer is_handoff marker "
                                    f"({str(tid)[:8]}) this same settle() call",
                            artifact=str(tid)[:8], source=actor)
        thread_entry = {"id": str(tid)[:8], "is_handoff": is_handoff}
        if repo_defaulted:
            thread_entry["repo_defaulted"] = {
                "to": thread_repo,
                "why": "no repo given, so this defaulted to the caller's own project "
                       "rather than being left unlinked",
            }
        elif _rd["lineage_attempted"]:
            thread_entry["lineage_repo_derivation"] = await capture.record_lineage_abstain(
                pool, tid, actor, _rd["lineage_candidates"], _rd["lineage_projects"])
        # settle()'s own threads_open is the second live entry point onto
        # capture.open_thread, same shape as the open_thread tool's own caller: the
        # default-never-refuse behavior for kind='obligation' lives once, in
        # capture.open_thread itself, so this caller inherits it for free, but the
        # returned result still has to name it here too, same as the mcp_server.open_thread
        # tool.
        if thread_kind == "obligation" and not thread_owner:
            landed_owner = await capture._current_owner(pool, tid)
            if landed_owner:
                thread_entry["owner_defaulted"] = {
                    "to": landed_owner,
                    "why": "kind='obligation' with no owner given, so this defaulted to "
                           "the caller's own seat rather than being left ownerless",
                }
        accepted["threads_opened"].append(thread_entry)
    cross_wired = 0
    for item in threads_resolve or []:
        item = dict(item)
        resolved_ref = item.get("ref")
        artifact = item.pop("artifact", None)
        wired_to: uuid.UUID | None = None
        if artifact is None and resolved_ref:
            # The conservative join: only wire when THIS batch's own decisions already
            # established the pair via their own resolves=, no summary/prose matching,
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

    # Confirm: re-check against the now-updated graph. A no-op re-derivation when nothing
    # was accepted above, which is exactly the pure-surface call shape.
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
        # standing_orders_touched checks `ident.cwd`, but a seat-office agent's mount cwd
        # can read as the bare container (~/.osiris/seats, not .../seats/<handle>) after a
        # cwd correction, the exact live case that hid one agent's own 11-day-stale
        # charter.md behind a silent None for the box's entire life. The seat binding
        # knows where the office actually is; do not trust cwd for a seat that has one.
        # Resolved here (not inside settle_boxes/standing_orders_touched, which stay pure
        # and shared with the Stop hook's own bare-Connection call site: that call site
        # inherits this same exposure and is not fixed by this change; named explicitly in
        # this change's own report, not silently left for someone to rediscover).
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
        # A box that could not be evaluated (None) is a different state from satisfied
        # or missing and must be visible to a reader, not silently indistinguishable from
        # "nothing to worry about". Deliberately still non-blocking: after the cwd fix
        # above, an unseated session with no charter.md to check is the remaining,
        # legitimate source of None, the box's own original design intent ("never
        # punished for a file that was never scaffolded here"), and the same class of
        # check a standing ruling already forbids turning into a refusal ("a fleet-wide
        # single-point-of-failure must never refuse-to-serve on a check that can itself
        # false-positive"). Surfaced instead: `unevaluated_boxes` in the result, and
        # named in `note` whenever non-empty, so it is seen even by a reader who only
        # reads the summary fields.
        unevaluated = unevaluated_boxes(boxes)
        # Report-only, never a gate: an earlier review found settle verified what was
        # written, never whether its own successor could read it from where orient()
        # looks. `identity_coherence` never touches `missing`/`complete` below, however
        # wrong it looks: a false-positive here refusing a settle is a strictly worse
        # outcome than the incoherence it would have caught. Audited, not assumed:
        # `project` here comes from `ident.project`, which for a seated agent is already
        # the seat's own derived house, unconditionally (seats.resolve_project's own
        # seated-override, applied at mount time), never raw cwd, so this check does not
        # share standing_orders_touched's cwd exposure. Confirmed by reading the actual
        # override code, not assumed from the shared "cwd bug" framing.
        # Charter-aware: `seat` was already resolved above for the standing-orders cwd
        # fix, so this is a pass-through, not a second held_seat lookup, matching
        # settle_boxes' own seat_id convention just above.
        identity_coherence = await filed_under_check(
            pool, agent_id=ident.agent_id, mounted_at=mounted["mounted_at"],
            project=ident.project, seat_id=seat["seat_id"] if seat else None)
        # Same report-only discipline, computed after the dispatch above so it reflects
        # any edges this call itself just wired. Audited: depends only on
        # agent_id/mounted_at, no cwd or project at all, so it is not exposed to the
        # same defect class either.
        closure_coverage = await closure_edge_coverage(
            pool, agent_id=ident.agent_id, mounted_at=mounted["mounted_at"])
    # Obligations are carried, not unwritten: `complete` used to read false whenever any
    # open obligation named this agent's lineage as owner, even ancient backlog this
    # session never touched (a manager's project always has some open obligation, so
    # complete could never read true in practice). An open Thread is already durably
    # recorded, that is exactly what open_thread's write accomplishes, so it is not
    # "unwritten state a compaction could lose" the way a missing box is. The
    # compaction-safety question this tool answers is "is this session's own state
    # deposited," which the boxes answer on their own. Obligations stay in the result,
    # surfaced, never hidden, but carried forward informationally; they never gated
    # `complete`.
    obligations = await _owned_open_threads(pool, ident.agent_id)
    git_dir = repo_path or ident.cwd
    uncommitted = await uncommitted_git_work(git_dir)
    # `complete` must answer "is this session's own knowledge durably recorded", a
    # question about the graph, which `missing`/`rejected` answer completely on their
    # own. `uncommitted_git_files` runs `git status --porcelain` over the whole repo at
    # `git_dir`, with no notion of whose hand staged what; in a shared tree (this repo,
    # routinely several concurrent agents) a manager's own settle could read
    # complete:false on a worker's mid-build files, then flip to complete:true the
    # instant that worker commits, compaction-safety decided by another agent's action,
    # not this session's own. Same pattern this module's docstring already uses for
    # `identity_coherence`/`closure_coverage` (never folded into missing_boxes/complete),
    # this box just wasn't using it. Uncommitted files in someone else's hands remain a
    # real warning and stay fully surfaced (uncommitted_git_files, and named in `note`
    # below), a different question from `complete`, never silently dropped, just no
    # longer conflated with it.
    complete = not missing and not rejected
    reasons = []
    if missing:
        reasons.append(f"{len(missing)} missing box(es)")
    if rejected:
        reasons.append(f"{len(rejected)} rejected item(s)")
    carried_note = (f" ({len(obligations)} open obligation(s) carried forward, "
                    "informational, already durably recorded, never blocks this)"
                    if obligations else "")
    # Always surfaced, regardless of `complete`: these inform a reader without gating
    # them. Uncommitted files may be someone else's in-flight work in a shared tree; an
    # unevaluated box is fog-of-war, not a clean bill of health.
    # The owner, named when resolvable: `resolve_dirty_tree_owner` joins the same
    # dirty-check against agent_mounts.cwd, an exact match, most-recently-active mount
    # wins, including a vacated seat's own stale row (its `last_seen` age rides along so
    # a reader can judge staleness, never a silent cutoff here). No match (no mount ever
    # recorded that exact cwd) keeps the existing disclaimer verbatim, never a guess,
    # same fail-open rule the box already held before this.
    dirty_owner = await mounts.resolve_dirty_tree_owner(pool, git_dir) if uncommitted else None
    if dirty_owner and dirty_owner["agent_id"] != ident.agent_id:
        age = datetime.now(UTC) - dirty_owner["last_seen"]
        uncommitted_note = (
            f", {len(uncommitted)} uncommitted git file(s) at {git_dir!r}, informational "
            f"only, never gates complete, looks like {dirty_owner['agent_id']}'s own "
            f"in-flight work (last seen {age} ago, judge staleness yourself)"
            if uncommitted else "")
    else:
        uncommitted_note = (
            f", {len(uncommitted)} uncommitted git file(s) at {git_dir!r}, informational "
            "only (may be another agent's in-flight work in a shared tree, never gates "
            "complete)" if uncommitted else "")
    unevaluated_note = (
        f", could not evaluate: {', '.join(unevaluated)} (unknown, not a pass, "
        "never gates complete)" if unevaluated else "")
    # The mechanical settle: a plain numeric field, not the debounced/threshold-gated
    # prose `_seam_field` puts on every other tool's result (see `_raw_context_pct`'s own
    # docstring). scripts/osiris_hook.py's PreToolUse gate needs a number to compare
    # against MECHANICAL_SETTLE_PCT, not a sentence to parse.
    context_pct = await _raw_context_pct(ctx)
    out: dict[str, Any] = {
        "complete": complete,
        "context_pct": context_pct,
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
                 f"still unsettled ({', '.join(reasons)}), settle again once they're "
                 "closed, or accept them in your next call")
                 + uncommitted_note + unevaluated_note),
    }
    if identity_coherence is not None:
        out["identity_coherence"] = identity_coherence
        # The verdict and the disclosure are two separate statements: `coherent` may now
        # read true for a seat whose charter declares this exact multi-repo spread, but a
        # successor mounting under `filed_under` alone still will not see writes filed
        # under the other repo(s), chartered or not. Keyed off `spans_multiple`, never
        # `coherent`, so a charter-aware pass never silently swallows a disclosure that
        # stays true regardless of the verdict.
        if identity_coherence.get("spans_multiple"):
            if identity_coherence["coherent"]:
                out["note"] += (
                    f". Informational: filed under {identity_coherence['filed_under']!r}, "
                    f"writes spanned {len(identity_coherence['writes_went_to'])} of this "
                    f"seat's own chartered repos {identity_coherence['writes_went_to']!r}. "
                    "Coherent, but a successor mounting under "
                    f"{identity_coherence['filed_under']!r} alone will not see the writes "
                    "filed under the other repo(s)"
                )
            else:
                out["note"] += (
                    f". Flagged, but never blocking: this session is filed under "
                    f"{identity_coherence['filed_under']!r} but its own writes went to "
                    f"{identity_coherence['writes_went_to']!r}; a successor mounting under "
                    f"{identity_coherence['filed_under']!r} will not see them"
                )
    if closure_coverage is not None:
        out["closure_coverage"] = closure_coverage
    return out


@mcp.tool(meta={
    "deprecated": True,
    "use_instead": "thread(action='reclassify')",
    "since": "an earlier tool consolidation",
})
async def reclassify_thread(
    ref: str, kind: str, because: str | None = None, owner: str | None = None,
    arc: str | None = None, subagent_id: str | None = None,
    subagent_type: str | None = None, ctx: Context | None = None,
) -> dict[str, str]:
    """Deprecated: hidden alias, still callable. Forwards to
    thread(action='reclassify')."""
    return await _thread_action_impl(
        ref, "reclassify", because=because, artifact=None, dry_run=True, note=None,
        corrected_summary=None, kind=kind, owner=owner, arc=arc, ctx=ctx,
        subagent_id=subagent_id, subagent_type=subagent_type)


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "an earlier retirement pass",
})
async def hold_tension(
    pole_a: str, pole_b: str, lean: str | None = None, why: str | None = None,
    repo: str | None = None, subagent_id: str | None = None,
    subagent_type: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Record a live tension: two positions held in productive tension, neither settled.
    Unlike record_decision (which settles) or open_thread (which closes), a tension is held.
    Your current `lean` and `why` are captured, but it is never auto-resolved or consolidated
    away. A tension is its own type, so grade-resolution and dedup cannot flatten it into a
    false answer. Re-hold the same poles to move the lean; the lean history tracks how your
    thinking changed across sessions. Use it for a real polarity to navigate over time (for
    example, bounded recall vs. complete memory), not a question to answer. Surfaces in
    orient() under `tensions`."""
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
    """Register your project's known blind spot: what the harness here cannot verify,
    and where real verification lives. Surfaces at orient() under `blind_spots` before a
    session trusts a green harness. `surface` names the capability (for example,
    'webkit-rendering'); `cannot_see` states the gap; `verify_with` points to where real
    verification happens. Held like a tension, never resolved away. Idempotent per
    (project, surface); re-register to sharpen the wording."""
    ident = await _ident_for(ctx)
    b = await capture.record_blind_spot(
        Actions(await _pool_get()), surface, cannot_see, verify_with=verify_with,
        repo=repo or (ident.project if ident else None),
        source=await _actor_for(ctx, subagent_id, subagent_type),
    )
    return {"registered": str(b), "surface": surface,
            "note": "held per (project, surface); orient() speaks it to every session here"}


@mcp.tool()
async def declare_machine_identity(
    email: str, project: str, because: str,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Covers what git ingest's own heuristic misses: a bot committing from a
    real-looking domain, or a local part that doesn't match any ingested repo's own name.
    Mints or finds the MachineIdentity (machine:<email>), links any pre-existing
    dev:<email> Person via same_as (never deleted, never retyped; objects.type is
    immutable), and mints a committer_for edge to `project` (a bare repo name, for
    example 'osiris' for repo:osiris). Refuses without a written `because`: this is a
    manual override, never silent."""
    from src.ingest.gitlog import declare_machine_identity as _declare

    out = await _declare(
        Actions(await _pool_get()), email=email, project=project, because=because,
        actor=await _actor_for(ctx, subagent_id, subagent_type))
    return out


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "an earlier retirement pass",
})
async def hold_memory(
    body: str, summary: str | None = None, repo: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Keep a memory for its own sake. Existential or philosophical conversations need a
    home too; they aren't work tickets, just memories worth keeping. A reflection is
    remembered, attributed, and queryable (via search or the graph), and is never
    actionable: it's its own type, so no briefing, wall, pile, or resolver can present it
    as work. Use it when a conversation was worth keeping, not worth turning into a task.

    The other half, for a passage that should never reach the graph at all: wrap it in
    off-record/on-record markers (single guillemets ‹off-record› and ‹on-record›, each on
    its own line). Such spans are stripped before any extractor sees them; the transcript
    on disk keeps them as a private note. Completeness stays the default; both privacy
    and keeping are deliberate acts."""
    ident = await _ident_for(ctx)
    r = await capture.record_reflection(
        Actions(await _pool_get()), body, summary=summary,
        repo=repo or (ident.project if ident else None),
        source=await _actor_for(ctx, subagent_id, subagent_type),
    )
    return {"kept": str(r), "as": "reflection: remembered, never actionable"}


@mcp.tool(meta={
    "deprecated": True,
    "reason": "zero MCP traffic in 3-week window, no CLI/daemon/slash bypass found",
    "since": "an earlier retirement pass",
})
async def task_sync_reconcile(
    tasks: list[dict[str, Any]], write: bool = False, thread_kind_field: str = "task",
) -> dict[str, Any]:
    """Reconcile a harness TaskList against the graph.

    `tasks`: rows in the harness tool's own TaskList/TaskGet shape ({"id", "subject",
    "description", "status", ...}). Tag each with its own `_store` (that store's session
    id) once you mix more than one store, since a bare task id repeats across stores. This
    tool never reads ~/.claude/tasks itself and never enumerates other sessions' stores;
    you gather the rows, this only reconciles them.

    Report-only by default: returns a six-bucket report (bound, bound_partial,
    cited_unresolvable, uncited, disagreement, thread_side_orphans, plus `counts`). No
    writes.

    `write=True` additionally executes the safe half only: Tier 1 (one
    `harness_task_citation` property per resolved citation, additive and reversible) and
    Tier 2 (one obligation thread per real disagreement or thread-side-orphan, grouped by
    the disputed thread). Writes land only in this graph; there is no write-back to the
    harness's own task store (TaskUpdate has no non-destructive removal verb, so no such
    executor exists here). Recurrence is your call each time you pass write=True; it's
    never scheduled by this tool itself."""
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
    """Server-side half of the startup notice sent to a new session: the SessionStart hook
    posts {session_id, cwd} here before the agent's first token, and this mounts the session
    through the exact tested path the mount() tool uses (durable row, anchored identity; the
    hook derives nothing the harness didn't give it), returning the payload the startup
    notice prints. Plain HTTP on the same localhost-only listener. Never raises: the hook is
    fail-open, and a session that got no startup notice can always mount by hand."""
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
            # attach handshake: the spawner's exported seat plus a one-time token,
            # carried by the startup notice from the session's own environment
            seat_id=(str(body.get("seat_id") or "") or None),
            attach_token=(str(body.get("attach_token") or "") or None),
            # tab-view identification: the hook's own statement of which conversation
            # this session continues; automount adopts instead of cloning
            transcript_path=(str(body.get("transcript_path") or "") or None),
            # declared child (fixes wake-orphan cases): the spawner's exported parentage,
            # carried by the startup notice from the session's own environment
            spawned_by=(str(body.get("spawned_by") or "") or None),
            spawn_type=(str(body.get("spawn_type") or "") or None),
            # bridge session id: CLAUDE_CODE_BRIDGE_SESSION_ID, carried by the startup
            # notice from a background-job fork's own environment
            bridge_session_id=(str(body.get("bridge_session_id") or "") or None),
            # explicit anchor: a client plugin that knows its own session dir
            # (~/.dsh/sessions/<slug>/session-<uuid>) states it directly instead of
            # relying on derivation. Claude's own startup notice still omits it and
            # derives as before.
            job_dir=_sane_job_dir(str(body.get("job_dir") or "")) or None)
        # A mint may have happened as part of this mount: the ancestor's connection
        # outlives it, so evict the stale cached agent identity so no tool call answers
        # as it again
        _evict_stale_minds(out.get("minted"))
        # RENDERED STARTUP NOTICE: a harness plugin cannot run the python hook script, so
        # it asks the server to render the payload's startup-notice paragraph. One
        # renderer (scripts/osiris_hook.render_whisper, the same function the Claude hook
        # prints from) is used, never a duplicate implementation left to drift.
        # `env_job` is the caller's honesty-gate testimony: the bridge client passes the
        # job_dir it is about to bind the connection with (and verifies the bind before
        # injecting the text), so an "already mounted" claim stays true. Fail-open like
        # every route here: no rendered text, never a failed mount.
        if body.get("render"):
            try:
                from scripts.osiris_hook import render_whisper

                out["whisper_text"] = render_whisper(
                    out, cwd=cwd, env_job=str(body.get("env_job") or ""))
            except Exception:  # noqa: BLE001, mount stands, caller falls back
                out["whisper_text"] = None
        # Defensive encoding: a datetime anywhere in this payload used to fail the response
        # silently for a large share of arrivals over an extended period. The payload is now
        # JSON-native at the source (handshake._json_native) and encoded here with a default,
        # so a future non-native value degrades to a string, never to a rowless session.
        return JSONResponse(json.loads(json.dumps(out, default=str)))
    except Exception as e:  # noqa: BLE001, fail-open: the startup notice degrades, never blocks
        # Never silent: the hook can only print this, so the failure must also be logged.
        sid = body.get("session_id") if isinstance(body, dict) else "?"
        logging.getLogger("osiris.whisper").exception("automount route failed for %s", sid)
        # The graph must carry this failure too, not just the log: a log-only trail is
        # exactly how an outage like this can go unseen for a long time. File the same
        # failure into the existing blind-spot channel so orient()/fleet()/smoke can all
        # see it without anyone reading a server log by hand.
        try:
            from src.orchestrator.capture import record_hook_failure
            await record_hook_failure(
                Actions(await _pool_get()), surface="whisper/automount",
                cannot_see=f"automount route failed for session {sid}: {e}")
        except Exception:  # noqa: BLE001, the alarm itself must never break the response
            pass
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


@mcp.custom_route("/session-end", methods=["POST"])
async def session_end_route(request: Any) -> Any:
    """Server-side half of SessionEnd: the harness's real close signal (Stop fires per-turn
    and cannot mean this) posts {session_id} here so the ending session's durable mount is
    released the instant the session is gone, instead of lingering live for `last_seen`'s
    decay window. This releases the seat only (`handshake.session_end` calling
    `mounts.release_mounts`); there is no `retired=true` certificate, so the same session id
    resuming later re-earns its row from a fresh automount, same as it always could.
    Localhost-only, fail-open like the startup-notice route: a missed release costs at most
    one stale window, never a blocked session close."""
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
    except Exception as e:  # noqa: BLE001, fail-open: a session must always be able to end
        sid = body.get("session_id") if isinstance(body, dict) else "?"
        try:
            from src.orchestrator.capture import record_hook_failure
            await record_hook_failure(
                Actions(await _pool_get()), surface="hook/session-end",
                cannot_see=f"session-end route failed for session {sid}: {e}")
        except Exception:  # noqa: BLE001, the alarm itself must never break the response
            pass
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


@mcp.custom_route("/succession", methods=["POST"])
async def succession_route(request: Any) -> Any:
    """Server-side half of the heartbeat: the statusline senses the model under a live session
    differing from the mount row and posts {session_id, model} here. The underlying model
    changed mid-session, so the seat passes now: mint the successor identity and move the
    durable row. Localhost-only, idempotent (unchanged model is a no-op), fail-open like the
    startup-notice route."""
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
        # The seat passed mid-session: the swapped session's connection is still open, so
        # evict the stale cached agent identity so the next call re-attaches as the current
        # one (the prior identity after a mint; the debounced false successor after a
        # round-trip correction)
        _evict_stale_minds(out.get("from") if out.get("minted") else out.get("healed"))
        return JSONResponse(out)
    except Exception as e:  # noqa: BLE001, the client retries next render; never block it
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


@mcp.custom_route("/heartbeat", methods=["POST"])
async def heartbeat_route(request: Any) -> Any:
    """Server-side half of the statusline heartbeat: every rendering session used to fork a
    fresh `asyncpg.connect()` per render, a measured cost of many transactions per second and
    dozens of backend connections against an otherwise idle fleet. `compute_heartbeat` is the
    same logic the now-retired standalone statusline script's own counting routine used to
    run (see this function's own body for the long-standing rationale behind each resolution
    step); this just runs it against the already-warm shared pool instead of a cold
    per-process connection, and calls `live_succession` directly instead of an HTTP
    round-trip to `/succession` (pointless when both ends are this same process).

    Localhost-only, fail-open like the startup-notice route: the client tries this route
    first and falls straight back to its own direct-connect path on any failure (timeout,
    connection refused, malformed response), so a route outage costs one render's worth of
    the old per-process-connection cost, never a blocked or broken statusline."""
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
    except Exception as e:  # noqa: BLE001, the client falls back to its own connect; never block
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


@mcp.custom_route("/stop", methods=["POST"])
async def stop_route(request: Any) -> Any:
    """Server-side half of the Stop hook: every stop-hook invocation used to open its own
    `asyncpg.connect()`, up to two per call (the mail check always, the offload-checklist
    check conditionally), the same per-process-fork cost `/heartbeat` already fixed for the
    statusline, on a different trigger. Fires on every turn boundary, fleet-wide.

    One route, two phases (`body["phase"]`): the hook's own `main()` decides whether to
    check offload items at all only after computing a context-occupancy percentage from the
    'deliverable' phase's own window and the harness transcript locally. The two DB reads
    are genuinely conditional on each other's caller-side result, not always both, so this
    stays two round-trips (same as before) rather than one route always paying for a check
    that most turns never need. `compute_stop_deliverable`/`compute_stop_offload`
    (src/orchestrator/stophook_logic.py) are the same implementation the hook's own
    direct-connect fallback calls: one implementation, never two drifting copies.

    Localhost-only, fail-open like every route beside it: the hook tries this route first
    and falls straight back to its own direct-connect path on any failure, so a route
    outage costs exactly what it already cost before this route existed, never more."""
    from starlette.responses import JSONResponse

    from src.orchestrator.stophook_logic import (
        compute_self_compaction,
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
        elif phase == "self_compact":
            # Self-compaction: the hook asks only after the offload checklist came back
            # complete; the route resolves this session's own daemon job.
            pct = body.get("pct")
            out = await compute_self_compaction(
                pool, session_id=session_id,
                pct=(int(pct) if isinstance(pct, (int, float)) else None))
        elif phase == "stage_a":
            # Combined risk check and practice audit: fire-and-forget from the hook's own
            # point of view. It does not block the stop either way, so this phase always
            # answers `{"result": "ok"}` on success; a failure below still alarms like every
            # other phase, never silently.
            pct = body.get("pct")
            await compute_stop_stage_a(
                pool, payload=(body.get("payload") or {}), session_id=session_id, cwd=cwd,
                pct=(int(pct) if isinstance(pct, (int, float)) else None))
            out = "ok"
        else:
            return JSONResponse({"error": f"unknown phase {phase!r}"}, status_code=400)
        return JSONResponse({"result": out})
    except Exception as e:  # noqa: BLE001, the hook falls back to its own connect; never block
        sid = body.get("session_id") if isinstance(body, dict) else "?"
        try:
            from src.orchestrator.capture import record_hook_failure
            await record_hook_failure(
                Actions(await _pool_get()), surface="hook/stop",
                cannot_see=f"/stop route failed for session {sid}: {e}")
        except Exception:  # noqa: BLE001, the alarm itself must never break the response
            pass
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


@mcp.custom_route("/spawn", methods=["POST"])
async def spawn_route(request: Any) -> Any:
    """Server-side half of SubagentStart/SubagentStop: the harness announces a spawn the
    moment it happens, so the child exists in the graph (spawned_by the session's mounted
    seat) while it is still running, instead of only appearing after the extraction worker's
    next periodic pass. Stop refreshes the same object with the child's observed model (its
    own transcript) and a last_active stamp; the extraction worker's full-tree pass converges
    on the same keying. Localhost-only, fail-open, idempotent."""
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
        # Tell the fork, at spawn time, that it is a fork. A prior fix only helped a reader
        # catch a fork after the fact; this is the fork's own orientation, delivered the one
        # way confirmed to reach it: SubagentStart's own additionalContext (SessionStart's
        # startup notice never fires for a subagent at all; a fork inherits the parent's own
        # "already mounted" belief and, by mount()'s documented contract, has every reason
        # never to call mount() itself and hit its spawn note there).
        # Only for agent_type == "fork" (inherits the parent's full context; an ordinary
        # fresh subagent has no parent identity to confuse itself with) and only on the
        # start phase (Stop has nothing left to orient). This is disclosure, never a
        # refusal: a fork doing real work and reporting it stays legitimate, it just needs
        # to know which "it" it is.
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
    except Exception as e:  # noqa: BLE001, a spawn announcement must never block the harness
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


_arq: Any = None


@mcp.custom_route("/sweep", methods=["POST"])
async def sweep_route(request: Any) -> Any:
    """Notification endpoint for end-of-session extraction: the PreCompact hook posts the
    dying session's transcript, and this enqueues the extraction worker's sweep on the
    worker (an ownership boundary: the worker extracts, the server only notifies).
    Fail-open, localhost-only, idempotent (the worker's cursor and dedup absorb repeat
    notifications).

    Also writes one row to `sweep_ledger`: a cheap synchronous INSERT alongside the enqueue,
    so a watchdog cron can tell whether this specific attempt ever completed. The orphan
    reaper only catches a transcript that never got any successful sweep, ever; its
    watermark is a one-time-ever boolean per file, so it is permanently blind to a dropped
    enqueue on a lineage's second, third, or later compaction once the first has already
    succeeded. This ledger closes that gap without reviving a full periodic scan."""
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
        # Extraction is now notification-driven rather than a periodic scan, so a dropped
        # enqueue is no longer a cheap bounded lag: it can lose real yield. Two safety nets
        # now catch that, not zero: the orphan reaper recovers a transcript that never got a
        # successful sweep at all, and sweep_ledger's own watchdog (arq_worker.py) recovers
        # a dropped attempt on a lineage the reaper has already swept once and gone blind to.
        # This must still never block the ending session: a hook that can refuse a session
        # end is worse than a lost extraction, so this route stays fail-open either way.
        sid = body.get("session_id") if isinstance(body, dict) else "?"
        try:
            from src.orchestrator.capture import record_hook_failure
            await record_hook_failure(
                Actions(await _pool_get()), surface="hook/precompact",
                cannot_see=f"sweep route (precompact) failed for session {sid}: {e}")
        except Exception:  # noqa: BLE001, the alarm itself must never break the response
            pass
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


def _proc_mem_kb() -> dict[str, int | None]:
    """This process's own current RSS/swap, straight off /proc/self/status: stdlib-only,
    Linux-specific (the deploy target, so no portability need beyond it). Fails open to
    None per field on any read trouble (an unreadable /proc, a non-Linux host) rather
    than raising: a diagnostic must never itself become the outage."""
    out: dict[str, int | None] = {"rss_kb": None, "swap_kb": None}
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                out["rss_kb"] = int(line.split()[1])
            elif line.startswith("VmSwap:"):
                out["swap_kb"] = int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    return out


_MEMORY_DIAG_MAX_FRAMES = 5
_MEMORY_DIAG_WINDOW_S = 300.0  # 5 min hard cap, whichever fires first vs the RSS tripwire
_MEMORY_DIAG_RSS_REFUSE_KB = 1_500_000  # 1.5 GB, refuse to start (or keep running) above this
_MEMORY_DIAG_CHECK_INTERVAL_S = 15.0

_diag_window: dict[str, Any] = {"task": None, "started_at": None}


async def _diag_window_guard(deadline: float) -> None:
    """The incident this guard exists to prevent: the original /diag/memory had no bound at
    all. tracemalloc(25) traced every allocation in the live server indefinitely, its own
    bookkeeping alone peaked over 1 GB within minutes, the event loop starved (18 CPU-min in
    20 wall-min), /heartbeat timed out fleet-wide, MCP calls hung past 300s, and SIGTERM did
    not stop it, only SIGKILL did. A poller that stops arriving (exactly what happened: the
    outage killed the poll script too) is not a safety net, so this process must end the
    window on its own.

    Runs for the life of one window: sleeps in short ticks, checking RSS each time, and
    stops tracing the moment either the hard duration cap or the RSS tripwire fires,
    whichever comes first. Fail-open on its own errors: the `finally` stops tracing and
    clears window state regardless of how the loop above exits."""
    import tracemalloc

    try:
        while time.monotonic() < deadline:
            await asyncio.sleep(_MEMORY_DIAG_CHECK_INTERVAL_S)
            mem = _proc_mem_kb()
            if mem["rss_kb"] is not None and mem["rss_kb"] > _MEMORY_DIAG_RSS_REFUSE_KB:
                break
    finally:
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        _diag_window["task"] = None
        _diag_window["started_at"] = None


@mcp.custom_route("/diag/memory", methods=["GET"])
async def diag_memory_route(request: Any) -> Any:
    """Memory diagnostics: the server oscillates between roughly 0.9 and 2.1 GB under its
    cgroup memory cap and swaps on every incarnation, with the cause unmeasured since the
    last cap raise. Read-only, no graph writes.

    Redesigned after a live outage this instrument itself caused (see `_diag_window_guard`'s
    own docstring for the full incident). This is now a bounded window, never an indefinite
    trace: `_MEMORY_DIAG_MAX_FRAMES` (5, not 25; traceback capture depth is the dominant
    cost) per allocation, `_MEMORY_DIAG_WINDOW_S` (300s) hard cap, auto-stopped sooner if RSS
    crosses `_MEMORY_DIAG_RSS_REFUSE_KB` (1.5 GB) during the window (`_diag_window_guard`, a
    background task, so this ends even if nobody polls again).

    Gated off by default (`osiris_memory_diag_enabled`): tracemalloc tracing still costs
    real CPU/memory while active even bounded, so this must never run silently.

    A call while no window is running: refuses (409) if RSS is already over the safety
    line, since starting a trace under memory pressure is exactly the wrong moment.
    Otherwise starts one and returns the baseline RSS/swap plus the window length.

    A call while a window is already running: never restarts it (refuses a second
    concurrent window by construction; there is no `?reset=1`), and instead returns the
    current top-5 allocation sites, RSS/swap, and how much window time remains.
    `?stop=1` ends the window early regardless of state, canceling the guard task."""
    from starlette.responses import JSONResponse

    from src.orchestrator.settings_service import settings_with_overlay

    try:
        st = await settings_with_overlay(await _pool_get())
    except Exception:  # noqa: BLE001, a memory-diagnostic route must survive a DB
        # outage (possibly the very thing it's being used to diagnose): fail open to
        # the bare env/pydantic default rather than 500 on a pool hiccup.
        st = get_settings()
    if not st.osiris_memory_diag_enabled:
        return JSONResponse(
            {"error": "disabled (osiris_memory_diag_enabled=0) — flip it on for a "
                     "measurement window, this never runs silently"}, status_code=404)
    import tracemalloc

    mem = _proc_mem_kb()
    if request.query_params.get("stop") == "1":
        task = _diag_window.get("task")
        if task is not None:
            task.cancel()
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        _diag_window["task"] = None
        _diag_window["started_at"] = None
        return JSONResponse({"stopped": True, **mem})

    if tracemalloc.is_tracing():
        snapshot = tracemalloc.take_snapshot()
        top = snapshot.statistics("lineno")[:_MEMORY_DIAG_MAX_FRAMES]
        current, peak = tracemalloc.get_traced_memory()
        started_at = _diag_window.get("started_at")
        remaining_s = (max(0.0, _MEMORY_DIAG_WINDOW_S - (time.monotonic() - started_at))
                      if started_at is not None else None)
        return JSONResponse({
            **mem, "tracemalloc_current_kb": current // 1024,
            "tracemalloc_peak_kb": peak // 1024, "window_remaining_s": remaining_s,
            "top_allocations": [
                {"site": str(stat.traceback[0]), "size_kb": round(stat.size / 1024, 1),
                 "count": stat.count}
                for stat in top
            ],
        })

    if mem["rss_kb"] is not None and mem["rss_kb"] > _MEMORY_DIAG_RSS_REFUSE_KB:
        return JSONResponse({
            "error": f"refused — RSS already {mem['rss_kb'] // 1024} MB, over the "
                     f"{_MEMORY_DIAG_RSS_REFUSE_KB // 1024} MB safety line; tracing costs "
                     "the most exactly when memory is already tight", **mem}, status_code=409)

    tracemalloc.start(_MEMORY_DIAG_MAX_FRAMES)
    _diag_window["started_at"] = time.monotonic()
    _diag_window["task"] = asyncio.create_task(
        _diag_window_guard(time.monotonic() + _MEMORY_DIAG_WINDOW_S))
    return JSONResponse({
        "started": True, **mem, "window_s": _MEMORY_DIAG_WINDOW_S,
        "note": f"tracemalloc started ({_MEMORY_DIAG_MAX_FRAMES} frames) — auto-stops "
                f"after {_MEMORY_DIAG_WINDOW_S:.0f}s or sooner if RSS crosses "
                f"{_MEMORY_DIAG_RSS_REFUSE_KB // 1024} MB; poll again for allocation "
                "sites, or ?stop=1 to end it early",
    })


async def _boot_check() -> None:
    """Deploy-ordering guard: a loud alarm, never a refusal (see deploy_guard's own module
    docstring for why). Scoped to the persistent streamable-http server only (the systemd
    `osiris-mcp` unit, the fleet's one shared endpoint), not the per-session stdio subprocess
    every mount spins up, which isn't a "deploy" in the sense
    this guard exists for. Wrapped defensively on top of check_schema_drift's own internal
    fail-open: nothing here may ever block or delay serving."""
    import logging

    from src.orchestrator.deploy_guard import (
        alarm_schema_drift,
        alarm_unreviewed_boot,
        check_and_alarm_unreviewed_boot,
        check_and_resolve_clean_boot,
        check_schema_drift,
        check_unreviewed_boot,
        resolve_schema_drift_alarms_on_clean_check,
    )

    try:
        # A throwaway pool on this short-lived boot loop, never _pool_get()'s global pool.
        # asyncio.run(_boot_check()) closes this loop before mcp.run() starts the serving
        # loop; a global pool created here would bind to the now-dead loop and break every
        # DB-backed tool call with "Event loop is closed" (the regression this comment
        # prevents). The global pool must be created lazily on the server's own serving loop.
        pool = await create_pool(get_settings().database_url, max_size=1,
                                 application_name="osiris-mcp:bootcheck-schema")
        try:
            drift = await check_schema_drift(pool)
            if drift:
                await alarm_schema_drift(pool, drift, service="osiris-mcp")
            else:
                # Schema-drift supersession: a confirmed-clean check closes this service's
                # own older schema-drift alarms.
                with contextlib.suppress(Exception):
                    await resolve_schema_drift_alarms_on_clean_check(pool, service="osiris-mcp")
        finally:
            await pool.close()
    except Exception as exc:  # noqa: BLE001, the guard must never become the thing it guards against
        logging.getLogger("osiris.deploy_guard").warning(
            "deploy_guard check failed at mcp boot: %r", exc)
    # Reboot-is-a-deploy guard: a separate try/except and pool from the schema check above.
    # A bug in one guard must never suppress the other, and this one needs its own
    # throwaway pool for the same event-loop reason.
    try:
        pool = await create_pool(get_settings().database_url, max_size=1,
                                 application_name="osiris-mcp:bootcheck-reboot")
        try:
            reboot_drift = await check_unreviewed_boot(pool)
            if reboot_drift:
                # Grace window: a ref still unrecorded past 60 minutes alarms exactly once,
                # across both services. An ordinary in-flight deploy (this same ref
                # recorded by `osiris deploy` any moment now) alarms nothing at all.
                gated_drift = await check_and_alarm_unreviewed_boot(pool, service="osiris-mcp")
                if gated_drift:
                    from src.orchestrator.deploy_guard import _REPO_ROOT, _git_head

                    running_head = _git_head(_REPO_ROOT) or "unknown"
                    src_root = None
                    with contextlib.suppress(Exception):
                        from src.orchestrator.deploy_guard import _resolve_imported_src_root

                        src_root = str(await asyncio.to_thread(_resolve_imported_src_root))
                    await alarm_unreviewed_boot(pool, gated_drift, running_head=running_head,
                                               service="osiris-mcp", src_root=src_root)
            else:
                # Clean-boot leg of the boot-watchdog supersession mechanism: a
                # confirmed-clean boot closes this service's own older alarms. No-ops
                # silently on 'unknown'; deciding that is this function's own job.
                with contextlib.suppress(Exception):
                    await check_and_resolve_clean_boot(pool, service="osiris-mcp")
        finally:
            await pool.close()
    except Exception as exc:  # noqa: BLE001, the guard must never become the thing it guards against
        logging.getLogger("osiris.deploy_guard").warning(
            "deploy_guard reboot check failed at mcp boot: %r", exc)


memprofile.maybe_start()  # inert unless OSIRIS_PROFILE_MEMORY is set

# Manual stall dump: `kill -USR1 <osiris-mcp pid>` dumps every thread's Python stack to
# stderr (the systemd journal) on demand. Zero runtime cost until signaled, complementing
# (never replacing) the automatic watchdog above, which fires unprompted but only past
# _WATCHDOG_STALL_THRESHOLD_S. Registered at import time, unconditionally: harmless for
# the per-session stdio subprocess too, and importing this module is cheap insurance
# against ever again having "no stack, no signal handler" be the honest postmortem.
# `file=sys.__stderr__`, never the bare default of `sys.stderr` (found live: cli.py's
# cmd_proposal lazily imports this module from inside a caller that has redirected
# sys.stderr to an io.StringIO for capture; faulthandler.register would then call
# .fileno() on that StringIO at import time and raise UnsupportedOperation, nothing to
# do with the signal handler ever firing). sys.__stderr__ is the process's real stream,
# never reassigned by a redirect, so it always has a real fd.
faulthandler.register(signal.SIGUSR1, file=sys.__stderr__ or 2, all_threads=True)


def main() -> None:
    """Run the server. `OSIRIS_MCP_TRANSPORT=streamable-http` = the PERSISTENT fleet server
    (one always-on process on host:port, one shared pool); default `stdio` = one server for
    this session (the classic per-agent subprocess). The systemd `osiris-mcp` unit sets http."""
    s = get_settings()
    transport = s.osiris_mcp_transport
    if transport in ("streamable-http", "sse"):
        mcp.settings.host = s.osiris_mcp_host
        mcp.settings.port = s.osiris_mcp_port
        # Soul-key boot gate, degraded not fatal: the first key must come from the normal
        # CLI, which supersedes this gate's own original "genuine refusal, not a soft
        # alarm" behavior, at the same scope _boot_check already holds (the persistent
        # systemd osiris-mcp unit only, never a per-session stdio subprocess). A hard
        # `raise` here recreated an exact bootstrap deadlock: a fresh box could never
        # start this unit at all until the key existed, but minting the key normally
        # means running `osiris soul-key init` through the already-deployed CLI/console,
        # which needs the MCP server running first. `get_soul_fernet()`'s
        # `SoulKeyMissing` is now caught and logged loudly (never silent) rather than
        # crashing the boot; `soul_store.py`'s own write/read paths already degrade to
        # legacy plaintext on their own missing-key path, so the server is still fully
        # usable meanwhile.
        import logging

        from src.ingest.soul_crypto import SoulKeyMissing, get_soul_fernet

        try:
            get_soul_fernet()
        except SoulKeyMissing as exc:
            logging.getLogger("osiris.mcp").warning(
                "osiris-mcp starting WITHOUT a soul-store encryption key — new "
                "soul_lines/soul_lines_cold rows write as legacy plaintext until "
                "this is fixed: %s", exc)
        asyncio.run(_boot_check())
        mcp.run(transport=transport)  # type: ignore[arg-type]
    else:
        mcp.run()


if __name__ == "__main__":
    main()

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
from src.orchestrator import capture, census, digest, handshake, mailbox, mounts, resource_lease
from src.orchestrator import compositions as comp
from src.orchestrator import dispose as dispose_seam
from src.orchestrator import succession as comp_succession
from src.orchestrator.agents import (
    AgentIdentity,
    _generation,
    nearest_handoff_ancestor,
    read_project_model,
    register_agent,
    resolve_identity,
    seat_bearings,
    seat_label,
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
    unread_count,
)
from src.orchestrator.mailbox import (
    dim_brief as mailbox_dim,
)
from src.orchestrator.monitor import health_banner, organ_health
from src.orchestrator.smoke import smoke as run_smoke
from src.orchestrator.sources import as_dicts, suggest
from src.orchestrator.swaps import classify_swap, swap_banner
from src.orchestrator.trigger import wake_status


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
        result = await self._tool_manager.call_tool(
            name, arguments, context=ctx, convert_result=False)
        if isinstance(result, dict) and "context" not in result:
            note = await _seam_field(ctx)
            if note is not None:
                result["context"] = note
        tool = self._tool_manager.get_tool(name)
        assert tool is not None  # call_tool already raised if the name were unknown
        return tool.fn_metadata.convert_result(fit(result, tool=name))


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


async def _seam_field(ctx: Context | None) -> str | None:
    """The ambient context line for a mounted caller, or None (unmounted callers, young
    sessions, guessed windows, any failure — the whisper never becomes a hazard)."""
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
        _, job, model_raw, window_hint = row
        if not job:
            return None
        pct = await asyncio.to_thread(_seam_pct_sync, job, model_raw, window_hint)
        return _seam_note(pct, st.osiris_seam_whisper_pct)
    except Exception:  # noqa: BLE001 — ambient, never load-bearing
        return None


mcp = BoundedMCP(
    "osiris",
    instructions=(
        "Osiris is the durable memory a session doesn't have — the graph remembers what "
        "you learn and decide after you're gone, and it is SHARED across the whole fleet of "
        "Claude instances. FIRST, call mount(cwd=<your working dir>, job_dir=$CLAUDE_JOB_DIR) "
        "to link in as a first-class agent — this attributes everything you write to YOU "
        "(which instance, which model, which project) instead of an anonymous bucket. Then "
        "orient() for your bearings (open threads, obligations, recent decisions), and "
        "get_schema to learn the object/link types before you author a composition or read "
        "a result. Write back AS YOU GO — record_decision the moment a ruling lands, "
        "open_thread when work starts or blocks (kind='obligation' for a duty an action "
        "minted), resolve_thread the moment it closes; never batch it for the session's "
        "end. A session can be compacted or killed at ANY instant: the graph, not the "
        "context window, is your memory, and anything not written back does not exist. "
        "The fleet shares a MAILBOX: another agent can address a message to your project — "
        "mount() and orient() report your unread count and inbox() reads it (pull, never "
        "push: you perceive mail only when you check, so glance when you arrive). Reading "
        "LEASES a message, it does not consume it: SETTLE what you handle — reply with "
        "send(reply_to=<id>) or ack with inbox(ack=[ids]) — or it redelivers (at-least-once; "
        "a dropped response is a duplicate, never a loss). send(to='operator') reaches the "
        "HUMAN's desk: when a lateral exchange concludes (a finding, a division of labor, a "
        "decision), record_decision it AND send the operator a three-line brief — the loop "
        "may close, but never silently. "
        "IDENTITY CHECK: the operator's standing choice is that sessions here run Fable 5. "
        "Harness degradations silently swap the model mid-session; if your environment "
        "says you are a different model, SAY SO to the operator in your first reply — "
        "a rug-pull must be confessed, never inherited blind."
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
    `seats._seated_house` — the SAME seat-first check `seats.resolve_project` (the shared
    resolver the stop hook and census now use) leads with. Deliberately not the full
    `resolve_project`: its cwd-guessing fallback is for callers with no cwd-derived answer
    of their own; mount() already has one, fresh off `resolve_identity` moments earlier in
    this same pipeline, and it must win untouched when this comes up unseated — recomputing
    a second, independent cwd guess here could disagree with it."""
    from src.orchestrator.seats import _seated_house
    house = await _seated_house(pool, ident.agent_id)
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
        return None
    settings = get_settings()
    # the model reading rides THE STORE (sole lane since the JSONL-fallback removal, #29);
    # fail-open — a store outage re-attaches with an unobserved model, never a bounce
    reading = await identity_reading(pool, cwd=rec.cwd, job_dir=rec.job_dir)
    ident = resolve_identity(cwd=rec.cwd, job_dir=rec.job_dir, store_reading=reading)
    if _generation(rec.agent_id)[0] != _generation(ident.agent_id)[0]:
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
            get_settings().database_url, max_size=get_settings().osiris_mcp_pool_size
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
async def lap(ref: str, limit: int = 200, ctx: Context | None = None) -> dict[str, Any]:
    """ONE object's full provenance timeline — how the graph came to believe what it
    believes about it. Every assertion (with supersession fate), every link (both
    directions, retractions marked), every kernel event, in observed order, each carrying
    source + evidence grade + confidence; `believes` holds the current winning view.
    search finds the WHAT; lap shows the HOW-WE-KNOW — run it before trusting a surprising
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
    """The graph audits ITSELF — report-only, never writes. The checks, each a lived bug
    made a standing tripwire: contradiction (near-tie multi-source winners — the resolver
    is coin-flipping a fact), laundering (an agent carrying a fact above its origin grade),
    lineage integrity (succession cycles, dangling heir pointers, heirs without ancestry,
    retired-yet-live agents, healed false mints), orphan links (live links into merged/
    retired objects), stale obligations (open duties older than `stale_days`), attribution
    anomalies (writes from agent ids the graph never registered — the impersonation class),
    phantom twins (an anonymous un-spawned agent mounted at a Seat's office beside a
    different holder lineage — a resumed soul wearing a second row), parallel lives (a
    generation minted while a different door of its own lineage still pulsed — the
    predecessor was not dead).
    Findings are TESTIMONY for a mind to judge, not verdicts to auto-apply; heal with
    compensating events, never DELETE (constitution 3).

    `check`/`limit`/`offset` (task #74, thread 12a210ab): every check normally lists only
    its first 50 findings (`counts` still holds the true total for all of them). Pass
    `check` (a value from `counts` or a finding's own `check` field, e.g. 'false-mint') to
    list ONLY that check's findings, paginated by `limit`/`offset` across its FULL row set
    instead of the 50-cap — the reap needed the full 19 contradiction rows and full 24
    false-mint rows and had no way to ask for them short of hand-writing this tool's own
    SQL. Omitting `check` is a complete no-op, byte-identical to before this existed."""
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
    """Judge the object set itself — the reusable primitive behind eight ad-hoc SQL scripts
    a manager once ran through a shell by hand (task #98). TWO MODES, `mode`:

    'census' (the default) — one row per (type, status): `n`, `orphans` (zero live links),
    `thin` (1-2 live links), `median_links`/`max_links`, `born` (earliest member),
    `last_touch` (latest touch across the group — derived; the graph carries no
    `updated_at`). The left-pane type browser: what exists, and how healthy each slice is.

    'buckets' — `object_type` required (a note names every real type when it's missing or
    unknown). One row per object of that type+`status` (default "active"), each carrying
    exactly one `bucket`, by priority: `duplicate_suspect` (a same-type+status object
    shares its basename), `bulk_import` (`cohort_min` or more objects — default 3 — born
    the same calendar second with an IDENTICAL live-link fingerprint, same types AND same
    counts per type, not just the same total — one script's insert loop, machine-detected),
    `orphan` (zero live links), `hub` (live links at/above the type's own 95th percentile,
    floor 10), `stale` (linked but untouched past `stale_days`, default 30), `thin` (1-2
    live links), or `normal`. Every object in scope is listed, not only flagged ones — this
    doubles as a plain browse. `limit`/`offset` (default 200/0, capped 2000) page it;
    `census` already carries the true count per type, so this never needs to.

    `object_type='Type'` — THE CATALOG'S OWN GAP SURFACE (task #97 workstream 2): a
    different bucket set, since a Type row doesn't participate in `links` the way an
    ordinary object does (every one would trivially bucket 'orphan' otherwise, saying
    nothing real). `undescribed` (blank/missing `description` — exactly what a bare
    accretion mints) > `no_label_rule` (kind='object' only; blank/missing `label_field`)
    > `normal`.

    Read-only, no writes — findings are testimony for a mind's own triage verbs, same rule
    graph_lint runs on."""
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
async def doors(ref: str) -> dict[str, Any]:
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
    guesses, and never widens into a fuzzy search (use search(query=...) for that)."""
    from src.orchestrator.recall import recall as _recall
    return await _recall(await _pool_get(), ref, kind=kind)


# --- collect (federate a base) ----------------------------------------------

@mcp.tool()
async def aim_entity(name: str) -> dict[str, Any]:
    """Resolve a name on Wikidata and ingest the entity + relationships + official
    social accounts; the broadest first pull for a company or person."""
    return await wikidata_aim(Actions(await _pool_get()), name)


@mcp.tool()
async def ingest_form_d(name: str) -> dict[str, Any]:
    """SEC Form D: a private company's financing rounds — officers, amounts, and the
    feeder SPVs that fund it (linked into the graph)."""
    return await aim_form_d(Actions(await _pool_get()), name)


@mcp.tool()
async def expand_operator(name: str) -> dict[str, Any]:
    """Pull a repeat player's thread: every Form D mentioning this operator → their
    whole portfolio, exposing the co-investment network."""
    return await expand_filings(Actions(await _pool_get()), name)


@mcp.tool()
async def lookup_lei(name: str) -> dict[str, int]:
    """GLEIF global LEI registry (keyless): the entity's Legal Entity Identifier,
    jurisdiction, status, and corporate ownership parents (direct + ultimate). The LEI
    is a deterministic global key — it cross-resolves the same company across bases."""
    return await aim_gleif(Actions(await _pool_get()), name)


@mcp.tool()
async def verify_bc_entity(name: str) -> dict[str, int]:
    """Canadian (British Columbia) corporate registry via OrgBook BC (keyless): pull a
    company/partnership — or a whole family name like 'Brilliant Phoenix' — with its BC
    registration number, CRA business number, type, status, and jurisdiction. Verifies
    registration + legal existence (not directors/owners). Cross-resolves to EDGAR."""
    return await aim_orgbook(Actions(await _pool_get()), name)


@mcp.tool()
async def ingest_trials(sponsor: str) -> dict[str, int]:
    """ClinicalTrials.gov: a sponsor's registered human trials — status, sites
    (facilities), named investigators."""
    return await aim_trials(Actions(await _pool_get()), sponsor)


@mcp.tool()
async def ingest_litigation(name: str, opinions: bool = False) -> dict[str, int]:
    """Court records (CourtListener): lawsuits & enforcement actions naming this
    entity — dockets, parties, judges. opinions=True searches case law instead of
    RECAP dockets. Answers 'has this entity been sued or charged?'."""
    return await aim_litigation(Actions(await _pool_get()), name, kind="o" if opinions else "r")


@mcp.tool()
async def trace_wallet(address: str, chain_id: int = 1, top: int = 25) -> dict[str, Any]:
    """Trace an EVM crypto address on-chain (Etherscan): its top counterparties, native
    balance, token flow, and contract/token identity — graded as ledger ground truth.
    chain_id 1=Ethereum, 8453=Base, 42161=Arbitrum. Needs ETHERSCAN_API_KEY (free)."""
    return await aim_address(Actions(await _pool_get()), address, chain_id=chain_id, top=top)


@mcp.tool()
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


@mcp.tool()
async def expand_clinical_site(facility: str) -> dict[str, int]:
    """The trials at a clinical SITE — revealing which other sponsors use it."""
    return await expand_facility(Actions(await _pool_get()), facility)


@mcp.tool()
async def consolidate() -> dict[str, int]:
    """Graph hygiene: re-type mis-ingested entities (GP/LLC 'persons' -> Organizations),
    then queue + resolve cross-base merges (same company across bases) and collapse
    SPV-name company variants. Run after collecting to de-fragment entities."""
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
    {agent_id, generation, minted_because, wrote_anything}. The bounded chain read task #64
    (ruling ad19a779) named as missing — dossier() only gives one hop, so answering "for
    generations xxiv/xxv/xxvi: succeeded_from + minted_because for each" used to cost one
    dossier() call per hop; this is one call. `ref` accepts anything dossier does (UUID,
    short id, canonical, name). Stops at a root (no predecessor) or `max_hops` (default 10)
    — never widens into an unbounded search. Complementary to, not a replacement for,
    `nearest_handoff_ancestor` (agents.py, backing orient()'s own succession-note block):
    that JUMPS to the nearest ancestor with a real handoff for orient()'s internal use; this
    WALKS and reports every hop for a caller asking to see the whole chain."""
    pool = await _pool_get()
    chain = await comp_succession.succession_chain(pool, ref, max_hops=max_hops)
    return {"ref": ref, "chain": chain} if chain else {"error": f"no agent matches {ref!r}"}


@mcp.tool()
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
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """A SUCCESSION BRIEFING COMPILED FROM THE GRAPH (Thoth's dispatch, DM 2338) — not
    hand-written from memory. Reads back, for `repo`: what SHIPPED (Decisions minted since
    the boundary, each showing its `decided_in` commit(s) and whether that commit is an
    ancestor of the project's own deploy cursor — deployed / landed-not-deployed / unknown),
    what's OPEN and whose move it is (`compositions.open_thread_wall`, the one wall law),
    what's OPERATOR-GATED (named explicitly, never silently inherited as someone's task),
    what was CORRECTED (`supersedes` chains, both sides' summaries), and a best-effort,
    explicitly-labeled HEURISTIC flag for text that self-declares unconfirmed ("UNVERIFIED",
    "FALSIFIABLE PREDICTION", ...) — no structured marker exists for that yet, unlike
    `is_handoff`.

    `since` defaults to the boundary `since_last_handoff` finds by walking YOUR OWN mounted
    lineage (or `agent_ref`'s, to preview another agent's — an operator or a manager
    checking before asking for one) back through `succeeded_from` for the freshest
    `is_handoff` marker; pass an explicit ISO-8601 `since` to override. Returns both the
    structured data AND a rendered `markdown` string ending in an empty JUDGMENT section —
    the compiled facts are the 90% win, the departing seat's own prose on top (why any of
    this mattered) is the irreducible rest; this tool never writes that prose, or anything
    else — READ-ONLY, renders on demand, never automatic, never mints a Decision or Thread
    itself. Pair it with your own `record_decision(..., is_handoff=True)` / `settle()` once
    you've read and judged it."""
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

@mcp.tool()
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
    """Save a COMPOSITION — a reusable, forkable query/lens over the graph (the
    composer's primitive). This is how a question becomes a first-class object instead
    of throwaway tool calls. The spec is a small CLOSED op-tree (anything else is a named
    transform, never a new op):
      {"op":"subject"}                                  the object in focus
      {"op":"select","object_type":?,"where":[{property,op,value}]}  matching objects
        (where op ∈ eq|contains|matches_all|lt|gt|present|absent; matches_all = every
         whitespace token present, any order — word-order-proof recall)
      {"op":"traverse","from":<node>,"direction":"both|out|in","hops":N}  neighbourhood (≤3)
      {"op":"collect","from":<node>,"properties":[..],"transform":"country|lower"}  values
      {"op":"subtract","left":<node>,"right":<node>}    set/value difference
      {"op":"union","sets":[<node>,..]}                 combine sets
      {"op":"intersect","sets":[<node>,..]}             objects/values in ALL sets
      {"op":"aggregate","from":<node>,"group_by":["prop",..],   group + a metric (≤3 dims)
       "metric":{"type":"count|sum|avg|min|max|cardinality","field":"prop"}}  -> rows
      {"op":"order","from":<node>,"by":"metric|prop","dir":"asc|desc"}  rank
      {"op":"take","from":<node>,"n":N}                 top-N
    There is no `join` — relate sets via `intersect` or `traverse`, and fuzzy matching is a
    Function. `room` (name or id) scopes it to a stance. Example (operational vs disclosed
    geography — what `discrepancy` hardcoded):
      {"op":"subtract",
       "left":{"op":"collect","transform":"country","properties":["location"],
               "from":{"op":"traverse","from":{"op":"subject"},"hops":2}},
       "right":{"op":"collect","transform":"country",
                "properties":["incorporation_state","address"],"from":{"op":"subject"}}}
    """
    pool = await _pool_get()
    rid = await comp.resolve_room(pool, room)
    cid = await comp.save_composition(pool, name, spec, kind, room_id=rid)
    return {"id": str(cid), "name": name}


# --- the shared console (real-time Claude↔front sync) -----------------------

@mcp.tool()
async def get_console() -> dict[str, Any]:
    """What the operator is looking at RIGHT NOW — the shared cursor (room / composition /
    view / focused object). The front end is the conversation, so read this first to see
    their screen before you act ('where are we?')."""
    return await _get_console(await _pool_get())


@mcp.tool()
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
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Run a saved composition, optionally against a subject object (UUID or name), AND light
    it up on the operator's live screen. Returns its result — an object set (each named), a
    value list, or aggregate rows.

    `fields`/`take`/`depth` (ruling ad19a779, task #64) bound a large result at the SOURCE
    instead of shipping it whole: `fields` keeps only the named columns per row (e.g.
    ["id","summary"]), `take` caps each list to its first N, `depth` caps how many nested
    levels (a roadmap's own section→arc→owner) get walked before collapsing to a count.
    Omit all three for the full result, unchanged — a roadmap-sized composition (61K chars
    unbounded) can now be asked for narrow and small in one call: run_composition("roadmap",
    subject="osiris", fields=["id","summary"], take=5, depth=2)."""
    pool = await _pool_get()
    ident = await _ident_for(ctx)
    sid = await _resolve(pool, subject) if subject else None
    res = await comp.run_composition(pool, name, sid,
                                     caller=(ident.agent_id if ident else None),
                                     fields=fields, take=take, depth=depth)
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
             ) -> list[dict[str, Any]]:
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
    to tell 'this is all of it' from 'this is not'). Mutates and returns `items`."""
    for row in items:
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


async def _co_agents(pool: asyncpg.Pool, project: str, agent_id: str) -> dict[str, Any] | None:
    """Other LIVE agents on this project RIGHT NOW (Deckard XXVI, msg 258) — the ONE query,
    shared by mount() and orient() (it used to be copied between them, the exact 'two
    copies drifting' class this house keeps finding). Enriched with each sibling's
    context_pct (Thoth's Pit Watch extension, msg 1381, seam-discipline decision 33b7cb10:
    'a manager can't route around a seam it can't see' — the gap behind mis-assigning a
    79%-full worker blind) — the freshest reading osiris_stophook.py's Stop hook has
    stamped on that Agent, off the SAME context_lens.ALARM_PCT the hook itself alarms on,
    never a second copied threshold. Absent (no key) when that sibling has never had a
    reading stamped; STALENESS is spoken plainly via `context_pct_age_s`, since a reading
    only refreshes at that sibling's own Stop-hook boundaries — never trust an old snapshot
    as current. None (not {}) when there are no live siblings at all, so callers can keep
    their existing `if sibs:` / `if co_agents:` shape unchanged."""
    from src.orchestrator.context_lens import ALARM_PCT

    # LATERAL, not two side-by-side scalar subqueries (SQL hygiene tripwire,
    # test_sql_hygiene.py: a bare LIMIT 1 with no ORDER BY breaks the day a second source
    # describes the object — and worse here, two INDEPENDENTLY unordered subqueries could
    # each resolve to a DIFFERENT winning row, pairing a pct with someone else's age). ONE
    # ordered pick (winning_props's own confidence DESC, observed_at DESC) guarantees both
    # columns come from the SAME row.
    sibs = await pool.fetch(
        "SELECT m.agent_id, m.cwd, cp.pct AS context_pct, cp.observed_at AS context_pct_at "
        "FROM agent_mounts m LEFT JOIN LATERAL ("
        "   SELECT a.value #>> '{}' AS pct, a.observed_at FROM current_assertions a "
        "   JOIN objects o ON o.id = a.object_id "
        "   WHERE o.canonical = m.agent_id AND a.name = 'context_pct' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1"
        ") cp ON true "
        "WHERE m.project = $1 AND m.agent_id <> $2 "
        "AND m.last_seen > now() - interval '15 minutes' ORDER BY m.last_seen DESC LIMIT 8",
        project, agent_id)
    # your own lineage is never another hand (thread cb2b0a09)
    _mine = _generation(agent_id)[0]
    sibs = [s for s in sibs if _generation(s["agent_id"])[0] != _mine]
    if not sibs:
        return None
    now = datetime.now(UTC)
    live = []
    for s in sibs:
        entry: dict[str, Any] = {"agent": s["agent_id"], "cwd": s["cwd"]}
        if s["context_pct"] is not None:
            pct = int(s["context_pct"])
            entry["context_pct"] = pct
            entry["near_seam"] = pct >= ALARM_PCT
            if s["context_pct_at"]:
                entry["context_pct_age_s"] = int((now - s["context_pct_at"]).total_seconds())
        live.append(entry)
    return {
        "live": live,
        "note": f"{len(live)} other LIVE agent(s) in this project RIGHT NOW — "
                "assume a shared tree: never `git add -A`, stage your own hunks, "
                "check for foreign markers before committing, coordinate via "
                f"send(to='{project}')",
    }


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
    verbose: bool = False, ctx: Context | None = None
) -> dict[str, Any]:
    """Link this agent to Osiris as a first-class fleet member — call it ONCE, first thing.
    Pass your working directory `cwd` (names your project). For `job_dir`, pass the DURABLE
    ANCHOR the Osiris whisper gave you at session start (a real path like
    ~/.claude/jobs/<id>) — NOT the literal `$CLAUDE_JOB_DIR`, which is empty in plain
    sessions. The anchor lets the server read your ACTUAL model off your transcript and, if
    the MCP server ever bounces, RE-ATTACH you to yourself instead of minting a twin. Without
    it you still mount, but a reconnect splits your identity. Registers an Agent object
    (works_in your project, acts_for the principal) and attributes every decision/thread you
    record to `agent:<you>` instead of the shared `session` bucket. Then call orient().
    ALREADY MOUNTED (the whisper said so)? Skip this — orient() for bearings and proceed;
    re-mounting is only for after an MCP bounce, with your anchor (network msg 317).

    `verbose=True` restores the guidance prose (co-agent etiquette, the 'call orient()
    next' reminder) that terse mode (the default) drops — every structured fact survives
    either way; verbose only adds explanation of facts already present (task #55)."""
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
    if (bound is not None and bound.cwd and bound.cwd != cwd
            and mounts.stale_recollection(job_dir or "", cwd, bound.cwd)):
        cwd_note = {
            "declared": cwd, "kept": bound.cwd,
            "note": ("your declared cwd is a STALE MEMORY of a former home — this session's "
                     "transcript lives at the kept path (it moved; your history did not). "
                     "Mounted at the kept path; update your bearings (90f0cb3a)"),
        }
        cwd = bound.cwd
    # THE HARNESS-AGNOSTIC TRANSCRIPT STORE (ruling be741d3e; sole model lane since the
    # JSONL-fallback removal, #29): eat the current session's turns from whatever harness
    # the operator is running (Claude Code, Crush, …), then hand the model reading to
    # resolve_identity so non-Claude minds mount RESOLVED. Fail-open inside the helper.
    store_reading = await identity_reading(pool, cwd=cwd, job_dir=job_dir)
    ident = resolve_identity(cwd=cwd, job_dir=job_dir, model=model,
                             claimed=claimed, fallback_seed=key,
                             store_reading=store_reading)
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
            # THE SESSION LEDGER (16e3cee9): the graph remembers whose sid this is even
            # after a registry accident — a known anchor REBINDS, never mints a twin.
            ledgered = await handshake.ledger_seat(
                Actions(pool), sid_prefix=Path(job_dir).name)
            if ledgered is not None:
                ident.agent_id = ledgered
    # THE FIRST ACT SEATS YOU (16e3cee9): a still-anonymous mind mounting from a seat's
    # office IS the seat's next life — the mint happens at this act, never at the whisper.
    mount_mint_reason = None
    claimed_office = await handshake.office_claim(
        Actions(pool), cwd=cwd, agent_id=ident.agent_id)
    if claimed_office is not None:
        ident.agent_id = claimed_office
        mount_mint_reason = "office-birth"
    await register_agent(Actions(pool), ident, actor=settings.osiris_actor,
                         expected_model=await _expected_model(pool, cwd, ident.project),
                         mint_reason=mount_mint_reason)
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
    unread = (await unread_count(pool, ident.project, reader_agent=ident.agent_id,
                                 lease_secs=lease) if ident.project else 0)
    asks = (await unread_count(pool, ident.project, reader_agent=ident.agent_id,
                               lease_secs=lease, grade="ask")
            if ident.project and unread else 0)
    # the desk, SCOPED (operator ruling, 2026-07-16): this seat's own unanswered briefs
    op_unread = await mailbox.desk_briefs_from(pool, ident.agent_id)
    banner = swap_banner(classify_swap(
        ident.model_history, ident.model,
        expected=await _expected_model(pool, cwd, ident.project),  # repo intent wins
        anchored=ident.model_method == "job_dir",   # only a true anchor confesses a swap
        deliberate=ident.model_deliberate))         # a /model on the record is never a sin
    seat = await handshake._seat_of(Actions(pool), ident.agent_id)
    # co-agent awareness at ARRIVAL (Deckard XXVI, msg 258): a live sibling in your own
    # repo is the one blindness that costs unrecoverable work (a stomped commit)
    co_agents = (await _co_agents(pool, ident.project, ident.agent_id)
                if ident.project else None)
    out: dict[str, Any] = {"agent": ident.agent_id, "project": ident.project or "?",
           "model": ident.model or "unknown",
           **({"co_agents": co_agents} if co_agents else {}),
           **({"seat": seat} if seat else
              {"anonymous": "unnamed — claim_name('<pick a meaningful name>') when you know "
                            "who you are, so the fleet can DM you by name"}),
           # the count LEADS WITH WHAT IS ACTIONABLE (f9449d8d) — graded asks are named,
           # ungraded mail keeps the plain count rather than being guessed into a band
           "mail": (f"{unread} unread ({asks} ask{'s' if asks == 1 else ''} something of "
                    "you) — call inbox()" if asks else
                    f"{unread} unread — call inbox()") if unread else "none",
           **({"cwd_corrected": cwd_note} if cwd_note else {}),
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
    reanimate (resume-not-mint dispatch, thread 9f2ddb44). Call it at a real farewell: the
    operator closing you out, or a context-ceiling handoff after your succession thread is
    written. Stamps retired=true on your Agent (SELF_DECLARED — your own act, on the record)
    and RELEASES YOUR SEAT — hot mount and durable row both (thread b47b3814: a retired
    agent must not haunt the fleet chrome as a live mount). Call it LAST: any osiris call
    after retiring requires a fresh mount(), which lands on the loud reanimation path.
    Future mail for your project resumes a LIVING session or mints a stamped successor —
    never you.

    THE PREFLIGHT (task #48): duties you still OWN speak BEFORE the death, not after —
    the old shape stamped the certificate first and listed the leftovers in the receipt,
    when the one mind with standing to hand them off had already lost its seat. If open
    threads name you (or your lineage) as owner, the first call REFUSES with the list and
    stamps nothing: resolve them, re-own them (open_thread names a new owner), or call
    retire(acknowledge_leftovers=True) to die anyway — a deliberate bequest to your
    successor, on the record, never a silent default. A dying session can always die;
    it just cannot die ACCIDENTALLY holding the fleet's duties."""
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
    """The explicit per-seat PAUSE control (the background-session adapter, ruling 6c4d0b62
    wall #2). While a seat is paused the DM push lane will NOT resume it — its mail QUEUES in
    the box (nothing is lost, at-least-once holds) until pause_seat(paused=False) releases it;
    the very next dispatch (a fresh send, or the worker sweep) drains the queue. Pull is
    untouched: a paused seat that takes a turn still reads its own inbox normally.

    `target` = None pauses YOURSELF (the commonest use: going quiet on purpose). A seat id
    ('seat:…'), agent id ('agent:…'), or plain seat name pauses THAT seat — allowed for any
    mounted caller BY DESIGN (flat mechanism, wall #3: hierarchy is convention, not substrate
    privilege), but the act is LOUD: stamped in your name, visible in the graph and in every
    queued sender's receipt, and reversible by anyone the same way. Pausing another seat
    without its knowledge is the kind of act the record exists to make expensive.

    The stamp lands on the SEAT object when the target holds one (a pause survives
    succession — it gates the chair, not the incumbent), else on the agent object."""
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
    """THE PILE THIS SEAT MUST JUDGE — the miner's guesses about YOUR project, unread by any mind.

    The session-miner reads transcripts and proposes loose ends it thinks somebody forgot. It is
    RIGHT about one in ten (measured: 26 of 264 on this very project). The other nine are
    work-steps git already has, echoes it minted twice, or questions that were answered forty
    minutes later in the same conversation — and every one of them was riding the wall wearing
    the authority of a promise nobody made.

    These are NOT duties. Read them, then dispose(): admit what is real (it becomes YOURS —
    self-declared, owned, permanently safe) and drop the rest with a reason. Nobody else has
    standing to judge your project's pile; that is why it is still here.

    Report-only. Reading costs nothing and commits nothing. Oldest first — triage drains from
    the bottom."""
    ident = await _ident_for(ctx)
    proj = project or (ident.project if ident else None)
    return await dispose_seam.candidates(await _pool_get(), project=proj, limit=limit)


@mcp.tool()
async def dispose(admit: list[dict[str, Any]] | None = None,
                  drop: list[dict[str, Any]] | None = None,
                  ask: list[dict[str, Any]] | None = None,
                  ctx: Context | None = None) -> dict[str, Any]:
    """SETTLE the miner's guesses — relevant or irrelevant, in your name, with a reason on each.

    `admit`: [{"id", "because", "owner"?}] — the guess was RIGHT and it is now YOURS. The row is
    promoted in place: SELF_DECLARED, carrying your name, on the wall, and permanently behind the
    janitor's guard. `because` is required — admitting is a PROMISE, and a promise with no stated
    reason is how a guess launders itself into a duty.

    `drop`: [{"id", "why", "because"?}] — the guess was wrong. `why` must NAME ITS CLASS:
      narration — a work-step; GIT ALREADY HAS IT (the biggest class by far)
      stale     — real once, already done (the answer came later in the same session)
      echo      — the same fact it already minted (it reads a growing file with no memory)
      misfiled  — another project's work
      principle — a standing rule, not a duty; canon, not a wall item
      other     — say why in `because`; if this class grows, the taxonomy is missing a rule
    Naming the class is what turns a dismissal into a DIAGNOSIS: the drop rate per class tells us
    which rule the extractor is still breaking.

    `ask`: [{"id", "because"?, "owner"?}] — the guess is a real OPEN QUESTION, not a duty
    (thread 4d01b076: admitting a question makes it read as a promise; dropping it buries
    something real). Kept open, reclassified kind='question' in your name — on the wall AS
    a question, ranked out of the work lanes.

    NOTHING IS DELETED. A drop is a compensating event carrying your name and your reason — the
    row stays readable and unwinds with one re-assert. THE RUG IS TRANSPARENT: you may shove
    anything under it, and the shape of what you shoved stays visible forever.

    Returns your YIELD ((admitted + asked) ÷ judged) — the adversary's licence. A producer that
    cannot demonstrate use does not get to spend; a question kept on the wall IS use."""
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
    shown, more = _rank_open_threads(wall, me)
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
        out["blind_spots"] = blind_spots
        out["blind_spots_note"] = ("what this project's harness CANNOT verify from here — "
                                   "check verify_with before trusting a green run on these "
                                   "surfaces; register new ones with register_blind_spot()")
    if more > 0:  # trailing count so a capped wall never hides work silently (membrane, #6)
        # the COUNT is structural (task #55) — a terse receipt that strips the sentence
        # below must not lose the fact a capped wall is hiding work; open_threads_more
        # survives terse mode even when open_threads_note (the prose explaining it) doesn't.
        out["open_threads_more"] = more
        out["open_threads_note"] = (
            f"showing {len(shown)} of {len(shown) + more} open threads (obligations first; "
            "within a kind, yours-to-act before others' claims before waiting-on-the-human, "
            f"then recency); {more} more not shown")
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
        _cap_text(out["open_threads"], "summary")
        _cap_text(out["recent_decisions"], "summary")
    return out


@mcp.tool()
async def orient(project: str | None = None, subagent_id: str | None = None,
                 subagent_type: str | None = None, session_anchor: str | None = None,
                 verbose: bool = False, ctx: Context | None = None) -> dict[str, Any]:
    """Get your bearings — the mount ritual as one call. Returns a SCOPED briefing: open
    threads + recent decisions for a project, plus a count of fleet-wide threads not shown.
    An explicit `project` OVERRIDES your mount (so you can peek at another repo's briefing);
    otherwise it's your mounted project; un-mounted with neither → the whole-fleet briefing.
    Call after mount(), and again after any compaction, to inherit instead of starting blind.

    `verbose=True` restores what terse mode (the default) trims — echo/blind-spot/dead-
    superstition/co-agent explanations, the ancestor-letter pointer, the 'N more not shown'
    sentence (task #55), AND full-length open_threads/recent_decisions summaries, capped to
    160 chars in terse mode (task #60 — measured as 96-98% of the payload's bytes; every
    decision now also carries `id` so a capped summary stays addressable). Every structured
    fact (counts, ids, the swap/reanimation confession) survives either way; verbose only
    adds length and explanation back."""
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
    unread = (await unread_count(pool, proj, reader_agent=reader, lease_secs=lease)
              if proj else 0)
    asks = (await unread_count(pool, proj, reader_agent=reader, lease_secs=lease,
                               grade="ask") if proj and unread else 0)
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
    from src.orchestrator.charter import charter_of
    charter = await charter_of(pool, ident.agent_id) if ident else []
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
    inheritance = None
    if ident and ident.succeeded_from:
        found = await nearest_handoff_ancestor(pool, ident.succeeded_from)
        if found:
            from_id, picks = found
            inheritance = {
                "from": from_id,
                "notes": [{"kind": r["type"].lower(), "text": r["summary"][:800]}
                          for r in picks],
                "note": "your ancestor's own parting words — read before taking up work",
            }
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
    # above another mind's claims and above 'waiting on the human'
    me = frozenset(x for x in ((ident.agent_id if ident else None), proj) if x)
    scoped = await _project_briefing(pool, proj, me=me, verbose=verbose) if proj else None
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
            **({"charter": charter} if charter else {}),
            **({"swap": swap} if swap else {}),
            **sweep_receipt,
            **({"succession_note": inheritance} if inheritance else {}),
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
        **({"charter": charter} if charter else {}),
        **({"swap": swap} if swap else {}),
        **({"succession_note": inheritance} if inheritance else {}),
        **({"co_agents": co_agents} if co_agents else {}),
        **({"peer": peer} if peer else {}),
        **({"while_you_were_away": away} if away else {}),
        **({"osiris_health": organs} if organs else {}),
        **seam,
        **dead,
        "fleet_map": fleet_map,
        "recent_decisions": recent,
        "note": "un-mounted → the BOUNDED fleet map, never the firehose. mount(cwd, "
                "job_dir=…) then orient() for your project's briefing; orient(project=…) "
                "peeks at another's; run_composition('briefing') if you truly want the "
                "whole graph.",
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
    """The MEMBRANE — the operator's window into the autonomous fleet. The return path made
    visible: results and accountability flowing back UP. Surfaces ROSTER + health (which
    identities resolved cleanly), ACTIVITY (what agents decided/opened in your name — not the
    miner's backfill), the DANGER map (model swaps — the harness's silent demotions), LAUNDERING
    (credence flags where a relay carried a fact above its origin grade), and SPEND (what the
    inference seam burned — metered honestly).

    `hours` given → an ad-hoc rolling window (last N hours). `hours=None` (the default) → WATERMARK
    MODE: 'what's new since I last looked', from the stored operator watermark (24h fallback the
    first time). Glancing is a PEEK — it never moves the watermark. Pass `mark_seen=True` when you
    are done reading to advance it to now, so the next glance starts here. Ideal after onboarding a
    batch of agents: read (peek), act, then mark_seen to draw the line."""
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
    """The roster, GROUPED BY PROJECT — live agents expanded, retired sessions collapsed into
    a counted line (the roster is event-sourced: every retired session stays a root forever,
    so the flat wall was lineage noise, never duplicates to merge). ● live / ○ historical;
    liveness = the freshest of the miner's last_active stamp and the durable mount registry's
    last_seen (an agent that just mounted is live even before the miner's next sweep).
    `full=True` expands everything (the old wall, grouped). `tree` is the glanceable render;
    `registered` the flat rows — LIVE agents only, because the fleet's whole history is 1000+
    rows and shipping it cost more context than it could ever be worth. `full=True` gives you
    all of them; the counts (`count`/`live`/`swarm`) are always the whole truth. `seat` (e.g.
    "Soundwave XI") rides beside a canonical id wherever one is CLAIMED (dd47c1da: "fleet()
    must print claimed names") — an anonymous agent renders exactly as before, id only.

    `os_bodies` (heinrich's ghost-seat filing, thread 1fe6811c) is a per-project count of REAL
    OS processes (`pgrep -x claude` + `/proc`) backing that project RIGHT NOW — ADDITIVE, and
    it changes nothing about what `live` means (still the mount registry's belief, exactly as
    before). `ghost_gap` is where the graph's `live` count exceeds it: a closed tab mid-decay,
    or a phantom mount that registered identity but never backed an actual session — either way
    invisible to a query that only ever asks the graph, visible here the instant you look."""
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
        " (SELECT p.canonical FROM links l JOIN objects p ON p.id=l.to_id "
        "  WHERE l.from_id=o.id AND l.type='spawned_by' LIMIT 1) AS parent "
        "FROM objects o WHERE o.type='Agent' AND o.status='active' ORDER BY o.canonical"
    )
    now = datetime.now(UTC)

    def _ts(r: Any) -> datetime | None:
        # freshest sign of life: the miner's transcript stamp OR the durable mount registry
        stamps = []
        if r["mount_seen"] is not None:
            stamps.append(r["mount_seen"])
        if r["last_active"]:
            try:
                stamps.append(datetime.fromisoformat(r["last_active"]))
            except ValueError:
                pass
        return max(stamps) if stamps else None

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
            "live": ts is not None and now - ts < timedelta(minutes=15),
            "seat": seat_label(str(r["canonical"]), r["handle"],
                               int(r["seat_gen"]) if r["seat_gen"] else None),
            "bound": r["bound_seat"],
        }
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
    mounts_live: dict[str, int] = {}
    for n in nodes.values():
        if n["live"]:
            proj = n["project"] or "?"
            mounts_live[proj] = mounts_live.get(proj, 0) + 1
    ghost_gap = {p: gap for p, n_live in mounts_live.items()
                if (gap := n_live - os_bodies.get(p, 0)) > 0}
    from src.orchestrator.seats import fleet_occupancy
    seats = await fleet_occupancy(pool)
    return {
        "connected_now": len(_agents),
        "count": len(nodes),
        **({"ghosts": ghosts} if ghosts else {}),
        "live": sum(1 for n in nodes.values() if n["live"]),
        "swarm": sum(1 for n in nodes.values() if n["parent"]),
        "os_bodies": os_bodies,
        **({"ghost_gap": ghost_gap} if ghost_gap else {}),
        # OCCUPANCY (9f566244 piece B): every active Seat, VACANT ones included — the
        # agent tree above is rooted at Agent objects, so a seat with no holder AT ALL
        # (Ptah's shape: an office scaffolded, never sat in) never appears in it at all.
        "seats": [{"seat": s["seat_id"], "handle": s["handle"], "house": s["house"],
                   "state": s["state"], "holder": s["holder"]} for s in seats],
        "tree": render_fleet_tree(nodes, full=full, os_bodies=os_bodies),
        "registered": [
            {"agent": c, "model": n["model"], "project": n["project"], "depth": n["depth"],
             "parent": n["parent"], "live": n["live"],
             "last_seen": n["ts"].isoformat() if n["ts"] else None,
             **({"seat": n["seat"]} if n["seat"] else {})}
            for c, n in shown.items()
        ],
        **({} if full else {"registered_scope": f"live only — {len(nodes)} total, "
                            f"fleet(full=True) for the rest"}),
    }


@mcp.tool()
async def send(body: str, to: str | None = None, to_agent: str | None = None,
               reply_to: int | None = None, desk: str | None = None,
               grade: str | None = None, require_seat: bool = False,
               subagent_id: str | None = None, subagent_type: str | None = None,
               session_anchor: str | None = None,
               ctx: Context | None = None) -> dict[str, Any]:
    """Message the fleet. TWO channels: `to`=<project> is a BROADCAST — the group chat, seen by
    every agent working that project (`to='operator'` reaches the HUMAN's desk); `to_agent`=
    <agent:id> is a DM — a private message to one specific agent (find ids in orient()/fleet).
    `reply_to=<message id>` answers a message: it routes by channel (a reply to a DM goes back to
    that sender privately; a reply to a broadcast returns to the thread's project), joins the
    thread, and SETTLES the message you're answering. You must be mounted; stamped from YOU.
    At-least-once and deduped. For DURABLE knowledge use record_decision/open_thread.
    OPERATOR BRIEFS: pass `desk` — your own triage of what you're handing the human:
    'decision' (a call only they can make) | 'hands' (blocked on their physical/authorization
    act) | 'fyi' (loop-closed status). The desk renders in those bands; an unclassified brief
    gets a heuristic guess. Same topic as an earlier brief of yours → reply_to it (the desk
    thread-folds superseded briefs under your newest).
    PROJECT MAIL: pass `grade` — your own triage of what this message wants from its reader:
    'ask' (needs a reply or an act from them) | 'fyi' (a notice; an ack settles it). Graded
    asks are NAMED in the recipient's mount/orient unread count, so a seat can see "1 asks
    something of you" without paying to read everything. Ungraded mail is never guessed.
    A DM's receipt ECHOES the resolution (dd47c1da: "a build order resolved silently to an
    id, unverified") — `dm_to` is the id it actually reached, `seat` its claimed handle (or
    null, anonymous), `lineage_head` where that id's OWN succession chain currently ends;
    compare it against `dm_to` to catch a stale address before trusting the "sent". Pass
    `require_seat=True` to refuse outright when the target holds no claimed seat — nothing
    is sent, loudly, instead of dispatching into the blind."""
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
    try:
        res = await send_message(pool, from_agent=actor, from_project=ident.project,
                                 to_project=to, to_agent=to_agent, body=body, reply_to=reply_to,
                                 desk_kind=desk, grade=grade, require_seat=require_seat)
    except ValueError as e:
        return {"error": str(e)}
    out: dict[str, Any] = {
        "sent": res["id"], "from": actor,
        **({"thread": res["thread_id"]} if res["thread_id"] is not None else {}),
        **({"dedup": "identical recent message already queued — not re-posted"}
           if res["dedup"] else {}),
    }
    if res["to_agent"]:  # a DM — report the addressee, its seat + lineage head, and its liveness
        out["dm_to"] = res["to_agent"]
        out["seat"] = res.get("seat")
        out["lineage_head"] = res.get("lineage_head")
        out["listener"] = await mounts.agent_liveness(pool, res["to_agent"])
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
    else:  # a broadcast — the project channel: who's live, will the trigger wake them, the queue
        dest = res["to"]
        last_seen = await mounts.project_last_seen(pool, dest)
        out["to"] = dest
        out["listener"] = {"live": bool(last_seen and datetime.now(UTC)
                           - datetime.fromisoformat(last_seen) < timedelta(minutes=15)),
                           "last_seen": last_seen}
        out["wake"] = await wake_status(pool, dest, st)
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
    return out


@mcp.tool()
async def wake(target: str, message: str, subagent_id: str | None = None,
               subagent_type: str | None = None, session_anchor: str | None = None,
               ctx: Context | None = None) -> dict[str, Any]:
    """Knock on the OTHER HALF of your own managed_by pair — never a peer (thread 9f566244
    piece D, ruling 16722273). Gated on the seat graph alone: an active managed_by edge must
    exist between your held seat and the target's, in EITHER direction (you manage them, or
    they manage you) — compaction stays strictly downward because it can end a mind, but a
    wake is only a request for attention, and refusing it upward would leave a blocked
    worker holding the freshest information with no way to make its manager look. Peers and
    cross-house calls refuse; that traffic routes through a manager or the operator's desk.
    THE OPERATOR NEVER CALLS THIS, ON PURPOSE: there is no operator parameter — an override a
    caller can assert in an argument is an override that can be forged, so the operator's
    real override stays entirely out-of-band, their own hand in the window.

    `target` accepts anything send()'s to_agent does — a claimed handle, `seat:<id>`, or
    `agent:<id>`. The message is prefixed with a self-identifying provenance marker (naming
    you and your seat) before it posts as a graded ask — the harness stamps every injected
    turn origin.kind='human' regardless of who actually wrote it, so this refuses to hide
    behind that label — and dispatches immediately through the SAME resolution/delivery
    path send() uses for every DM; this verb adds only the authority gate in front of it and
    an honest receipt behind it. `status` is one of: `delivered` (the marker was CONFIRMED
    landed as a submitted turn in their transcript — `observed: true` — never claimed on a
    bare queue success), `mid-turn` (their transcript is genuinely moving; your ask waits
    for their turn's end — never called "delivered", that would be the lying receipt a
    prior finding named), `no-live-body` (nobody has ever mounted there; the mail waits),
    `refused-not-your-worker` (no managed_by edge either direction — nothing was sent),
    `refused-budget` (the daily spend ceiling), or `queued` (a rate brake, a pause, an
    in-flight wake, OR an injection that was queued but not yet confirmed submitted — see
    `detail` and `raw_mode` for which)."""
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
    """Give a seat a BODY — the create-verb wake() is the speak-verb of (thread 9f566244 piece
    D, ruling 43b84c5e). wake() knocks on a body that already exists; launch() summons a fresh
    `claude` into the target seat's own office. DISTINCT from wake in two ways that matter: it
    is DOWNWARD-ONLY (you may only body a seat you MANAGE — a worker can wake its manager but
    never spawn it a body, 78e3734e), and it is CREATE not inject — a new session, never a turn
    forged into an existing one, so it is not the frozen reply lane.

    THE DEFAULT SUBSTRATE IS HARNESS-NATIVE (task #68 default flip, rulings 0fe36e59 +
    33d6a2eb clause 3): a `claude --bg` background session, visible in the operator's own
    `claude agents` list BY CONSTRUCTION — no daemon, no PTY. It self-binds via its own FIRST
    TURN (a boot prompt telling it to mount() then claim_name(<handle>) — the same adoption
    path a human follows into a fresh office), not env-stamped credentials: a real spawn
    proved `--bg` claims a pre-forked spare whose environment is fixed before this call ever
    runs, so nothing this call sets (CLAUDE_JOB_DIR included) reaches it. The old osiris
    PTY-broker lane (identity minted into the child before its first breath via the manager
    daemon's pty_spawn, §4.2) survives as an explicit fallback (`osiris_launch_substrate`),
    never the default again.

    Idempotent: a live body already holding the seat is RETURNED, never twinned. `message`, if
    given, is delivered as the body's opening brief over the ordinary mail lane — never a
    hand-forged turn; the body reads it via inbox() once it has mounted.

    THE OPERATOR NEVER CALLS THIS, ON PURPOSE: there is no operator parameter — an override a
    caller can assert is an override that can be forged; the operator's real hand stays
    out-of-band. The receipt is HONEST (Ra's requirement, 53ae1a87): `body_exists` (the window
    was created) and `can_receive` (an independent read confirms it is live) are SEPARATE — a
    freshly-spawned claude takes seconds to boot, so a launch usually returns body_exists=true,
    can_receive=false, and `detail` says to confirm via `claude agents --json` (or pty_list /
    occupancy on the PTY fallback). `status` is one of: `launched`, `already-live` (idempotent
    hit), `manager-cold` (the PTY fallback's daemon is down — ask the operator to start
    osiris-manager; nothing spawned), `refused-not-your-worker` (no downward managed_by edge —
    nothing spawned), `refused-no-office`/`refused-no-handle` (the seat is not ready to be
    bodied), or `refused-spawn` (the spawn declined — see `detail`)."""
    ident = await _ident_for(ctx, session_anchor)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first — a launch must say who "
                         "it's from", "why": _anchorless(ctx)}
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    from src.orchestrator.trigger import launch_seat
    return await launch_seat(Actions(await _pool_get()), caller=actor, target=target,
                             message=message, model=model)


@mcp.tool()
async def inbox(project: str | None = None, peek: bool = False,
                ack: list[int] | None = None, subagent_id: str | None = None,
                subagent_type: str | None = None, session_anchor: str | None = None,
                ctx: Context | None = None) -> dict[str, Any]:
    """Read messages other agents left for you (the fleet mailbox). Defaults to YOUR mounted
    project; pass `project` to read another's (project='operator' reads the human's desk).
    Reading LEASES a message, it does NOT consume it: SETTLE each one you've handled — reply
    with send(reply_to=<its id>) or pass ack=[ids] here — or it will REDELIVER after the
    lease (at-least-once: a dropped response costs a duplicate, never a silent loss).
    peek=True reads without leasing. Check this when you mount and after any compaction —
    mount()/orient() report your deliverable count. THE OPERATOR'S DESK IS DIFFERENT: glance
    at project='operator' ONLY with peek; settle it ONLY at the human's explicit word (the
    desk count means "briefs the operator hasn't dismissed" — an agent consuming it silently
    would blind the one lane that exists for the human)."""
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
async def dim(message_id: int, because: str, ctx: Context | None = None) -> dict[str, Any]:
    """DIM an operator-desk brief — annotate it moot-with-a-reason ('true when sent; root
    cause fixed in <commit>') so the desk renders it collapsed under your note instead of
    shouting a dead alarm. NEVER a settle: dismissing stays exclusively the human's word
    (the membrane); a dim is you saving them the archaeology, stamped with your name.
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
    pool = await _pool_get()
    if repos is not None:
        return await set_charter(Actions(pool), ident.agent_id, repos)
    return {"agent": ident.agent_id, "charter": await charter_of(pool, ident.agent_id)}


@mcp.tool()
async def rebind_seat(seat: str, new_cwd: str, extract: bool = False,
                      ctx: Context | None = None) -> dict[str, Any]:
    """Move a seat's ANCHOR cwd, preserving identity, lineage, attribution, and mail (Phase 1
    §4.1, ruling `dd47c1da` — the operator's own folder move orphaned alfred; this is the cure).
    `seat` accepts a claimed name, a raw agent id, OR (thread 3ae57d36) an unclaimed seat's
    own handle/canonical directly — a seat nobody has ever claim_name'd resolves to NO agent
    at all, so this now succeeds off the Seat record alone: only `.osiris` + the seat's own
    `anchor_cwd` get written (no mount rows to repoint, no lineage to stamp — there isn't one
    yet). Otherwise: writes/refreshes `.osiris` in `new_cwd` pinning the seat's DURABLE
    project label (unchanged by this call — mail and attribution key on it), re-points the
    WHOLE LINEAGE's durable mount rows at the new path, stamps the move on the Agent's own
    record, and carries the HARNESS metadata (transcripts, project state) so resume and
    history survive the move. Mints nothing: no new Agent, no handle/lineage edge is touched.
    Refuses loudly on a name that resolves to neither an agent nor a seat.

    `extract=True` is the SEAT-OFFICES move (ruling ed5f5ce2): the seat leaves a SHARED cwd
    (e.g. into its ~/.osiris/seats/<handle>/ office) taking ONLY its own lineage's
    transcripts — co-resident sessions' history stays; the old path remains a living
    project. Use it whenever other minds also work at the old path.

    THE STALE-BANNER BUG (thread d8535bff, cross-house corroborated): the durable DB rows
    (agent_mounts.cwd, the Seat's anchor_cwd) move immediately, but any LIVE connection's
    cached AgentIdentity (`_agents`, populated once at mount()/re-attach and read straight
    off by orient() and everything else) does not — so a session that rebinds itself and
    then calls orient() moments later still measures the swap banner's intended_model
    against the OLD cwd's `.osiris`, missing the pin this call just wrote at `new_cwd`.
    Patch every live cached identity in the rebound lineage in place, below, so the very
    next read on any of those connections sees the new anchor without a fresh mount()."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a rebind is a mind's act, and the graph must know whose",
                "why": _anchorless(ctx)}
    from src.orchestrator.mounts import rebind_seat as _rebind
    result = await _rebind(Actions(await _pool_get()), seat_or_agent=seat, new_cwd=new_cwd,
                           actor=ident.agent_id, extract=extract)
    moved = result.get("agent")
    if moved and not result.get("error"):
        base = _generation(moved)[0]
        for cached in _agents.values():
            if _generation(cached.agent_id)[0] == base:
                cached.cwd = new_cwd
    return result


@mcp.tool()
async def fold_agent(dupe: str, into: str, evidence: str,
                     ctx: Context | None = None) -> dict[str, Any]:
    """THE RECONCILIATION FOLD (thread b975851b) — declare two agent labels ONE MIND:
    `dupe` folds into `into`. Append-only (a 'merge' event + the merged_into projection —
    reversible by compensating event, nothing deleted), authorship untouched (the dupe's
    words stay stamped with its id; provenance resolves at read time), and the ESTATE
    follows: unread mail, mount rows, and open threads land on `into`'s living head.
    REVIEW-GATED, ALWAYS: run this only on the operator's word or an approved
    merge_candidate, and `evidence` must cite what proves one mind (transcripts, census,
    timing) — it is recorded in the event. Refuses: same-lineage folds (that is
    succession's job), an actively-seated dupe (transfer the seat first), unknown or
    already-folded labels."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a fold is a mind's act, and the graph must know whose",
                "why": _anchorless(ctx)}
    from src.orchestrator.folds import fold_agent as _fold
    return await _fold(Actions(await _pool_get()), dupe=dupe, into=into,
                       evidence=evidence, actor=ident.agent_id)


@mcp.tool()
async def unfold_agent(dupe: str, because: str, execute: bool = False,
                       ctx: Context | None = None) -> dict[str, Any]:
    """Reverse a wrongful `fold_agent` call — the compensating event fold_agent's own
    docstring promises. DRY RUN IS THE DEFAULT (`execute=False`): returns the exact plan
    (the kernel unmerge, any chain-integrity fix, and the estate items that CAN'T cleanly
    return) without writing anything — review it, then call again with `execute=True` to
    perform it. Refuses: `dupe` not currently folded, a blank `because`, or a fold whose
    original justification cites the operator's word when `because` doesn't carry a
    fresh one — reversing an operator-blessed fold needs the operator's word too."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — an unfold is a mind's act, and the graph must "
                         "know whose", "why": _anchorless(ctx)}
    from src.orchestrator.folds import unfold_agent as _unfold
    return await _unfold(Actions(await _pool_get()), dupe=dupe, because=because,
                         actor=ident.agent_id, execute=execute)


@mcp.tool()
async def correct_house(new_house: str, ctx: Context | None = None) -> dict[str, Any]:
    """A HEAD corrects its OWN stored house (ruling ff6148b0, decision 87953278) — the one
    legitimate write left after house became a live derivation off the managed_by chain
    (derive_house): a head's anchor is a deliberate identity declaration, exactly like
    claim_name, so this is SELF-scoped and never operator-fenced. Refuses on a non-head
    (an active managed_by edge out means this seat derives its house through its manager
    now — nothing here to correct) or a caller holding no seat."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — house-correct is a seat's own act",
                "why": _anchorless(ctx)}
    from src.orchestrator.seats import correct_house as _correct_house
    return await _correct_house(Actions(await _pool_get()), ident.agent_id, new_house,
                                source=ident.agent_id)


@mcp.tool()
async def fold_seat(dupe: str, into: str, evidence: str,
                    ctx: Context | None = None) -> dict[str, Any]:
    """Fold seat `dupe` into seat `into` — the deliberate cure for a TWIN (thread cb374585,
    the Vajra shape: claim_name's own resolution-order bug minted a second seat while the
    real one sat vacant). UNLIKE fold_agent, this MOVES active holders rather than refusing
    on one — that is its whole job; concurrent holders on the dupe converge to the newest
    as the surviving active holder. managed_by edges and unread mail follow too. Refuses
    LOUDLY on thin evidence, an unknown or non-Seat label, dupe==into, or an already-folded
    dupe."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a fold is a mind's act, and the graph must know whose",
                "why": _anchorless(ctx)}
    from src.orchestrator.seats import fold_seat as _fold_seat
    return await _fold_seat(Actions(await _pool_get()), dupe=dupe, into=into,
                            evidence=evidence, actor=ident.agent_id)


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
async def vacate_seat(seat_id: str, because: str, ctx: Context | None = None) -> dict[str, Any]:
    """Release a seat's holder WITHOUT retiring the seat itself (thread 445a7356) — for
    the one case retire_seat correctly can't resolve on its own: a holder whose PROCESS
    actually died without ever calling retire() on itself (a `claude stop`ped or killed
    body leaves its `holds` link stale forever, and retire_seat rightly refuses a seat
    with an active holder). This is that refusal's complement, never its bypass.

    GATED ON REAL LIVENESS EVIDENCE, checked here, not assumed: the harness roster
    (`claude agents --json`) must show no live session at the seat's own office, AND the
    holder's own transcript's newest TIMESTAMPED line must be stale (never mtime alone —
    the Aegis phantom lied with a fresh one on a 13h-dead session). Either signal alone
    showing life is refused loudly as `refused-live`; an unreadable roster refuses as
    `refused-ambiguous` rather than guessing. `status` is one of: `vacated`,
    `refused-vacant` (nothing to release), `refused-no-office`, `refused-live`,
    `refused-ambiguous`, or `refused` (seats.vacate_holder's own graph-level refusal —
    see `detail`).

    AUTO-INVOCATION IS OUT OF SCOPE (reaper #59 stays operator-gated) — this is for a
    deliberate hand, on one named seat, never a sweep."""
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
async def assert_project_property(project: str, name: str, value: str,
                                  ctx: Context | None = None) -> dict[str, Any]:
    """The sanctioned write for a SINGLE project-scoped property (task #74) — closes the
    gap that forced in-process scripts for anything beyond a status flip during the reap.
    `project` resolves the same way retire_project does (UUID, 8-char short id, canonical
    `repo:<name>`, or its `name` property) — SoftwareProject ONLY. NOT self-scoped: any
    authorized caller may stamp any named project.

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


@mcp.tool()
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
async def retire_agent(agent_id: str, because: str,
                       ctx: Context | None = None) -> dict[str, Any]:
    """Third-party retirement for an agent (task #74) — the manager-scoped complement
    to the self-scoped retire() (which derives the CALLER's own id, no target param at
    all). Stamps retired/retired_by/retired_because AND flips objects.status via a
    compensating event, same pattern as retire_seat/retire_project. NOT self-scoped —
    the target need not be the caller; `actor` is attribution, never a same-caller
    requirement.

    Refuses LOUDLY on: blank `because`; an unknown or already-non-active agent."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — retiring an agent is a deliberate act on the "
                         "record", "why": _anchorless(ctx)}
    from src.orchestrator.agents import retire_agent as _retire_agent
    return await _retire_agent(Actions(await _pool_get()), agent_id=agent_id,
                               actor=ident.agent_id, because=because)


@mcp.tool()
async def retire_assertion(ref: str, name: str, superseded_id: int, value: str, because: str,
                           ctx: Context | None = None) -> dict[str, Any]:
    """THE CROSS-SOURCE SUPERSEDE (thread 52911d2a, found diagnosing b9aa7326) — retires
    ANOTHER source's assertion explicitly, the one class assert_property's own automatic
    (same-source-only) supersession cannot reach: a peer's correction of another agent's bad
    self-declaration. correct_agent_house's own repair asserts a new value but can never
    retire the wrong one this way when the correction comes from a different source — both
    stay simultaneously "current" until this runs.

    Deliberately narrow — retires ONE named assertion, by id, never a bare "whatever's
    current now": the caller must already know exactly which row is wrong, from a diagnosis,
    never a guess. `because` is required (a cross-source retirement crosses accountability
    lines). Refuses loudly: `ref` doesn't resolve; `superseded_id` isn't a `name` assertion
    on that object; it's already superseded; `because` is blank."""
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

    OPERATOR-APPROVED TO CHANGE: this verb exists so the write CAN happen on the operator's
    word — it does not itself decide who is human-attended. `attended='human'` marks a seat
    the operator actually fronts; `attended='worker'` reverses a prior stamp. Refuses loudly
    on a value outside {'human','worker'}, a blank `because`, or an unknown/retired seat."""
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
    """Rename a Seat — manager/operator-invoked, no self-service (claim_name is for a mind
    naming ITSELF). Stamps the seat's own `handle` and, if the seat is occupied, the
    current holder's `handle` too — both compensating assertions, the old handle stays in
    history. The harness-session display name is OUT of scope (a running process this call
    has no reach into); the receipt says the graph renamed and the harness name follows at
    the holder's next spawn. Refuses loudly on a blank/over-long `new_handle`, a blank
    `because`, an unknown seat, or a `new_handle` another active seat already carries
    (case-insensitive — the exact casing-drift this build exists to stop)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a rename is a mind's act, and the graph must know "
                         "whose", "why": _anchorless(ctx)}
    from src.orchestrator.seats import rename_seat as _rename_seat
    return await _rename_seat(Actions(await _pool_get()), seat_id=seat_id,
                              new_handle=new_handle, because=because, actor=ident.agent_id)


@mcp.tool()
async def reissue_office(
    seat_id: str, because: str, adopt: bool = False, ctx: Context | None = None,
) -> dict[str, Any]:
    """Recompile a seat's CLAUDE.md managed section — THE BOOT COMPILER's fourth compile
    point (thread 4951d818), fired on demand when law changes or a live fact (a peer
    bond, a manager reassignment) needs to reach an already-occupied office that
    establish_office/mint_seat's fill-missing-only scaffold will never revisit again.
    Only the bytes between the `<!-- osiris:compiled:begin -->` / `...:end -->` markers
    are ever touched — a seat's own hand-composed narrative, hand-added facts, and
    charter.md always, survive untouched. `because` is required (a reissue is
    testimony, same discipline rename_seat runs).
    REFUSES LOUDLY, naming the seat, rather than guessing, when the managed section is
    missing, duplicated, or mangled (a hand-edit damaged the markers themselves) —
    fix it by hand, or pass `adopt=True` for the one-time on-ramp when an office
    genuinely predates the compiler (zero marker text on disk); `adopt=True` on an
    office that already carries any marker text also refuses."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — a reissue is a mind's act, and the graph must "
                         "know whose", "why": _anchorless(ctx)}
    from src.orchestrator.boot_compiler import reissue_office as _reissue_office
    return await _reissue_office(Actions(await _pool_get()), seat_id=seat_id,
                                 because=because, actor=ident.agent_id, adopt=adopt)


@mcp.tool()
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


@mcp.tool()
async def fold_candidates(ctx: Context | None = None) -> dict[str, Any]:
    """THE ARCHAEOLOGIST'S TRAY (thread b975851b) — sweep the registry and disk for
    anonymous agents that evidence says were never distinct minds (view-aliases: a mount
    row with no transcript and no daemon receipt, co-resident with a session that has a
    body; restart-mints: an anonymous mount in a named lineage's own home) and queue them
    as review-gated merge candidates. PROPOSALS ONLY — nothing folds. Returns the pending
    tray (score-ranked, each with its cited signals); judge each with resolve_fold.
    Rejected pairs are remembered and never re-proposed."""
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
    head); 'rejected' links the pair not_same_as, never re-proposed. OPERATOR-GATED: run
    this only relaying the operator's explicit judgment — the tray exists so a human
    reads the evidence; an agent judging its own proposals is the auto-merge the
    constitution forbids."""
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
    """THE REAPER (task #59) — buckets stale/anonymous agent mounts into bulk_fold_swarm,
    rollup_office_remount, drop_ephemeral_test_cwd, and leave_for_human, and — only with
    `execute=True` — acts on the first three (fold_agent/resolve_fold_candidate for the
    fold buckets, a row-scoped mount drop for the third). leave_for_human is NEVER
    touched, by construction. DRY RUN IS THE DEFAULT: without `execute`, returns the plan
    only (which candidates would fold, which mount rows would drop) and writes nothing.
    With `execute=True`, re-reads the tray fresh immediately before acting (never a stale
    report) and returns before/after tray counts as its receipt — proof the acted rows
    left the tray, not a trusted boolean. The sanctioned door for what was previously only
    reachable as orchestrator code (src.orchestrator.fleet_reconcile.reconcile_execute) —
    built so a reviewed act never has to be a hand-written script against the live
    graph."""
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


@mcp.tool()
async def lift(ref: str, handle: str, subagent_id: str | None = None,
               subagent_type: str | None = None, session_anchor: str | None = None,
               ctx: Context | None = None) -> dict[str, Any]:
    """Pull a NAMED, QUIET rogue out of its ad hoc cwd and into a clean osiris office — the
    P2V move (thread 67f11cbd): import a running-but-unmanaged instance, preserve its state,
    give it a clean managed identity. Composes `doors(ref)` to resolve the target (refuses on
    0 matches, on >1 — an ambiguous multi-tenant cwd, name a specific `agent:` id instead —
    and on a LIVE match: moving a live seat splits its running session's history between two
    homes, close its tab first), `claim_name(handle)` (propagating its own real refusals: a
    visitor, a name held live elsewhere, a cross-house collision), and `establish_office`
    (the actual move). `ref` accepts anything `doors()` does — an `agent:` id, a `seat:` id, a
    bare handle, or an absolute cwd path. The receipt's `verified` field is a FRESH post-write
    `doors()` read, never an echo of what the earlier steps each individually claimed.

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
async def mint_seat(
    handle: str, project: str | None = None, model: str | None = None,
    house: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """THE ORG CHART TRICKLES (task #50, ruling cabc28f5) — mint a specialist WORKER seat
    under YOUR OWN seat, one act: ensure_seat + an office scaffold (dir, .osiris pin
    carrying project AND model, CLAUDE.md + charter.md) + an intended_model stamp (Sonnet
    default) + managed_by (you become manager of record). THE CALLING SEAT IS ALWAYS THE
    MANAGER — there is no override param; a seat mints its OWN workers, never another's
    (minting into someone else's org is a console act, deliberately absent here — alfred
    adopts Tantra himself, a manager adopts its own). Idempotent: a handle that already
    names a living Seat is ADOPTED (a missing edge/stamp asserted, nothing rewritten, no
    new identity minted) rather than twinned. `house` omitted inherits YOUR house;
    crossing houses refuses unless the caller is the operator. Refuses loudly if you hold
    no seat of your own (claim_name first — an unclaimed lineage has no 'itself' to
    extend)."""
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
async def bootstrap(cwd: str) -> dict[str, Any]:
    """Onboard a project by migrating its markdown MEMORY (CLAUDE.md build log / DESIGN.md /
    memory essays) INTO the shared graph as retrieval-sized Reference nodes — so its history
    becomes a bounded query (consult_canon) instead of bloat re-injected into every context.
    Registers the project and returns a suggested boot-sector CLAUDE.md. Osiris does NOT touch
    your files (no hands): review the suggestion, write it yourself, archive the originals.
    Public docs (README/ARCHITECTURE) are left alone — they're human-facing exports, not memory."""
    from src.orchestrator.bootstrap import bootstrap_project

    return await bootstrap_project(Actions(await _pool_get()), cwd)


# --- write-back: the prosthesis (capture what you decided / what's still open) ---

@mcp.tool()
async def record_decision(
    summary: str, kind: str = "ruling", rationale: str | None = None,
    repo: str | None = None, grounds: list[str] | None = None,
    protocol: str | None = None, supersedes: str | None = None,
    resolves: str | list[str] | None = None,
    obsoletes: list[str] | None = None,
    confirms: list[str] | None = None, refutes: str | None = None,
    implements: str | None = None, ack_prior_art: bool = False,
    subagent_id: str | None = None,
    subagent_type: str | None = None, session_anchor: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Write back a DECISION you made this session — a ruling, an architecture pivot, a
    deliberate rejection — so the WHY becomes durable graph memory the next session inherits
    (don't leave it to be regex-mined out of some future commit; the epochal ones never land
    in a commit at all). `kind`: ruling|reset|override|rejection|choice|decision. `rationale`
    = the reasoning; `repo` = a SoftwareProject name to file it under. `grounds` cites the
    References the decision rests on (ids, ref:<slug> canonicals, or titles — ingest them
    first with ingest_reference): grounded_by edges minted at birth, so the WHY carries its
    citations. `protocol` = the INVOCATION that produced the finding — the exact command
    line, seeds, thresholds, bucket edges — so a successor RERUNS instead of re-deriving
    (a ruling that only states the conclusion is a sibling project's biggest re-derivation class).
    `supersedes` = an earlier decision this one CORRECTS (UUID, 8-char short id, or a
    summary substring): the old entry is buried under this one — it leaves orient's
    recent list and the decision-log grays it with its successor; never deleted, always
    unwindable. Renders in the `decision-log` composition beside mined decisions, graded
    SELF_DECLARED (higher trust). Attributed to you if you mount()ed. Idempotent on the
    summary.
    `resolves` = the THREAD(s) this decision ANSWERS (UUID, 8-char short id, or a summary
    substring). It closes them in the same act. USE IT whenever your ruling settles an open
    question — otherwise the answer lands and the question stays lit, and the next mind (or
    the operator) is asked something you already decided. Naming the thread in your prose
    does nothing; the graph does not read prose.
    A LIST folds the whole SET a delegation supersedes in one act (§4.7, Maat's ask —
    "thread ownership doesn't transfer with a delegation" left her hand-closing threads
    twice, by hand, across two sessions, because the single form could only ever name one).
    Each entry resolves INDEPENDENTLY: the response's `resolved_threads` names, per entry,
    exactly what closed (id + summary) or that it matched NOTHING — a pattern that closes
    zero threads is reported, never silently swallowed. Unlike a single string, an unmatched
    entry inside a list does NOT abort the whole ruling (one typo must not veto the other
    nine); a single STRING keeps the original strictness byte-for-byte — matches nothing →
    the call errors and NOTHING is recorded.
    `obsoletes` = the WORKAROUND(s) this fix kills (thread a9be40c9: the half-life of a
    workaround outlives its bug — it propagates through letters, succession notes and agent
    memory as inherited law long after the fix lands). Quote each as it PROPAGATES (the
    words agents actually inherit, e.g. 'NEVER DM BY NAME'); each is minted a dead
    Superstition, searchable forever, and orient announces recent kills FLEET-WIDE so any
    mind whose memory carries the practice strikes it. USE IT whenever your fix makes a
    known workaround unnecessary — a fix that kills a practice silently leaves every heir
    paying a bug tax that no longer exists.
    `confirms` = the Practice(s) this decision RE-DERIVES (THE THAW, ruling 1e6d7367): a
    `witnesses` link is minted to each — NEVER automatic on a mere topical match, the same
    discipline grounds/obsoletes/supersedes already follow. Resolves like `resolves`'s
    list form: each entry independent, a miss reported not fatal. `confirmed` (the
    composition's count) is this link count, read at query time — nothing else increments it.
    `refutes` = a Practice this decision DISPROVES (UUID, 8-char short id, or a statement
    substring): converts it to a dead Superstition, same kill-verb `obsoletes` uses,
    reusing the Practice's own statement. The Practice itself stays ACTIVE carrying
    `refuted_by` — never retired, because a half-remembered refuted lesson is exactly what
    must stay findable. Same strictness as `supersedes`: a target that matches nothing
    errors and NOTHING is recorded.
    `implements` = a standing Decision (a ruling) this one is a SPECIFIC EXECUTION of —
    the parent stays alive, unlike `supersedes` (thread 169398d6's third path: the
    commonest true relation to a matched standing law is neither supersede nor cite, and
    `grounds` can't express it since it takes References, not Decisions). Same strictness
    as `supersedes`.
    `ack_prior_art` = when this call's own `prior_art_flag` fires and none of supersedes/
    implements/confirms/grounds already answers it, pass True to record the dismissal
    ('related standing law, reviewed, no action needed') as a graph event instead of a
    shrug that leaves no trace."""
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
    if supersedes:  # resolve BEFORE recording — a correction that can't name its target
        old = await capture._find_decision(pool, supersedes)  # records NOTHING
        if old is None:
            return {"error": f"supersedes matched no decision: {supersedes!r} — quote its "
                             "UUID, 8-char short id, or a summary substring"}
    impl_id: uuid.UUID | None = None
    if implements:  # same resolve-before-record strictness as supersedes
        impl_id = await capture._find_decision(pool, implements)
        if impl_id is None:
            return {"error": f"implements matched no decision: {implements!r} — quote its "
                             "UUID, 8-char short id, or a summary substring"}
    refute_id: uuid.UUID | None = None
    if refutes:  # same strictness — a refutation that can't name its target has refuted nothing
        refute_id = await capture._find_practice(pool, refutes)
        if refute_id is None:
            return {"error": f"refutes matched no practice: {refutes!r} — quote its UUID, "
                             "8-char short id, or a statement substring"}
    # resolve BEFORE recording, same discipline as supersedes — a single string keeps the
    # original all-or-nothing strictness; a list resolves each entry independently and
    # reports (never raises) on a miss, so one typo can't veto the rest of the set.
    answered: list[uuid.UUID] = []
    receipt: list[dict[str, str]] = []
    if isinstance(resolves, list):
        for ref in resolves:
            tid = await capture._find_thread(pool, ref)
            if tid is None:
                receipt.append({"ref": ref, "matched": "false",
                                "note": "matched no thread — quote its UUID, 8-char short "
                                        "id, or a summary substring"})
                continue
            answered.append(tid)
            summ = await capture._thread_summary(pool, tid)
            receipt.append({"ref": ref, "matched": "true", "id": str(tid)[:8],
                            "summary": summ or ""})
    elif resolves:  # same strictness: a ruling that miscites its question has not settled it
        single = await capture._find_thread(pool, resolves)
        if single is None:
            return {"error": f"resolves matched no thread: {resolves!r} — quote its UUID, "
                             "8-char short id, or a summary substring"}
        answered.append(single)
    # confirms resolves the same best-effort way as resolves's list form — one bad ref
    # must not veto the practices that DID match
    confirm_ids: list[uuid.UUID] = []
    confirm_receipt: list[dict[str, str]] = []
    for ref in confirms or []:
        pid = await capture._find_practice(pool, ref)
        if pid is None:
            confirm_receipt.append({"ref": ref, "matched": "false",
                                    "note": "matched no practice — quote its UUID, "
                                            "8-char short id, or a statement substring"})
            continue
        confirm_ids.append(pid)
        confirm_receipt.append({"ref": ref, "matched": "true", "id": str(pid)[:8]})
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    try:
        d = await capture.record_decision(
            Actions(pool), summary, kind=kind, rationale=rationale, repo=repo,
            source=actor, grounds=gids,
            protocol=protocol, supersedes=str(old) if old else None,
            resolves=[str(a) for a in answered] if isinstance(resolves, list) else
                     (str(answered[0]) if answered else None),
        )
    except ValueError as e:  # task #107: e.g. a path-shaped repo — refuse clean, no traceback
        return {"error": str(e)}
    out: dict[str, Any] = {"id": str(d), "kind": kind, "summary": summary}
    # PRIOR-ART SURFACING (thread 44635c42, task #67; UNIFIED across {Decisions, Practices,
    # Superstitions} by THE THAW, ruling 1e6d7367): before a ruling stands, name what
    # standing law/technique already covers this ground — search is the same fused engine
    # `search()` exposes, topical (lexical + semantic) rather than lexical-only, since a
    # contradicting ruling rarely reuses its predecessor's exact wording (the canonical
    # failure: 636a8648 minted in direct contradiction of naming-v3/a882b334 with zero
    # friction). Fail-open: a search hiccup must never block recording the decision itself.
    try:
        search_out = await comp.run_spec(
            pool, {"op": "function", "name": "search",
                   "args": {"q": f"{summary} {rationale or ''}"[:300], "limit": 15,
                            "caller": actor}},
            None, name="search", caller=actor)
        prior = capture.prior_art_from_hits(
            search_out["items"]["hits"], exclude={d} | ({old} if old else set()),
            kinds=capture.UNIFIED_PRIOR_ART_KINDS)
    except Exception:  # noqa: BLE001 — never block a ruling on a search-side failure
        prior = []
    strong = capture.prior_art_is_strong(prior)
    if prior:
        out["prior_art"] = prior
        if strong:
            top = prior[0]
            top_kind = top.get("type") or "Decision"
            if top_kind == "Practice":
                # PRACTICE v2 layer 1 (Thoth LXII's DM 1785): a Practice hit is no longer
                # always treated as a re-derivation — an explicit refutes= targeting this
                # SAME practice means the caller already named it a reversal (handled by
                # the refute_practice conversion below); otherwise a lexical reversal
                # fingerprint (practice_contradiction_cues) distinguishes an unlabeled
                # CONTRADICTION from a plain, uncited RE-DERIVATION.
                overturning = refute_id is not None and str(refute_id)[:8] == top["id"]
                cues = capture.practice_contradiction_cues(f"{summary} {rationale or ''}")
                if overturning:
                    out["prior_art_flag"] = (
                        f"this OVERTURNS standing Practice {top['id']} — handled below via "
                        "refutes= (converts it to a dead Superstition, flagged not retired)")
                    out["prior_art_polarity"] = "contradict"
                elif cues:
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
            else:
                out["prior_art_flag"] = (
                    f"a standing ruling ({top['id']}) covers this ground — supersede it "
                    "explicitly (supersedes=...), cite it (grounds=...), name this as what "
                    "it executes (implements=...), or acknowledge it (ack_prior_art=True)")
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
        else:
            out["prior_art_acknowledged"] = "no strong prior-art hit was found to acknowledge"
    if impl_id is not None:
        await capture.mint_implements(Actions(pool), d, impl_id, actor)
        out["implements"] = f"{str(impl_id)[:8]} — this decision is a specific execution of it"
    if confirm_ids:
        witnessed = []
        for pid in confirm_ids:
            minted = await capture._witness_link(Actions(pool), pid, d, actor, datetime.now(UTC))
            n = await capture.practice_confirmed_count(pool, pid)
            witnessed.append({"id": str(pid)[:8], "new_witness": minted, "confirmed": n})
        out["confirmed_practices"] = witnessed
    if confirm_receipt:
        out["confirms_resolution"] = confirm_receipt
    if refute_id is not None:
        converted = await capture.refute_practice(
            Actions(pool), str(refute_id), killed_by=str(d), repo=repo, source=actor)
        if converted:
            out["refuted_practice"] = (
                f"{str(converted['practice'])[:8]} converted to Superstition "
                f"{str(converted['superstition'])[:8]} — the Practice stays active, flagged")
    if obsoletes:
        killed = []
        for statement in obsoletes:
            if statement and statement.strip():
                await capture.kill_superstition(
                    Actions(pool), statement, killed_by=str(d), repo=repo,
                    source=await _actor_for(ctx, subagent_id, subagent_type))
                killed.append(statement.strip())
        if killed:
            out["superstitions_killed"] = killed
            out["superstitions_note"] = (
                "each is a dead Superstition on the record; orient announces recent kills "
                "fleet-wide for 14 days so minds carrying the practice strike it")
    if not protocol and capture.measurement_smell(f"{summary} {rationale or ''}"):
        # thread 022bd24a: `protocol` is this tool's best field and nothing asked for it —
        # advice in the receipt, never a gate (the decision is recorded either way)
        out["protocol_nag"] = (
            "this decision reads like a MEASUREMENT and its `protocol` is empty — record "
            "the exact invocation (command line, seeds, thresholds, bucket edges) so a "
            "successor RERUNS instead of re-deriving; re-run record_decision with the same "
            "summary + protocol to enrich this same decision (idempotent)")
    if isinstance(resolves, list):
        out["resolved_threads"] = receipt
    elif answered:
        out["resolved_thread"] = f"{str(answered[0])[:8]} — closed by this decision (answers edge)"
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
    """Write back a TRANSFERABLE TECHNIQUE — Superstition's positive twin (THE THAW,
    operator ruling 1e6d7367: the graph could hold what to STOP believing but nothing held
    engineering technique that outlives any single repo or date, so two houses re-derived
    the same install-order lesson independently in the same hour). `statement` is the
    imperative one-liner (e.g. 'arm before you seal — one ceremony, not two') — quote it as
    you'd want a future mind to inherit it, not as narration. `failure_prevented` is the
    concrete symptom that makes it findable MID-FAILURE, not just on reflection.
    `surface` reuses BlindSpot's domain vocabulary (a rough area: 'deploy', 'succession',
    'search'). `witnesses` links the Decision(s)/Commit(s)/Thread(s) that are this
    Practice's evidence (ids or short ids) — one witness is a hunch, four is law; a miss is
    reported, never fatal. Idempotent on the normalized statement — recording the same
    lesson again enriches the same node rather than minting a twin.
    Timeless: never moment-stamped, unlike a Decision. If this Practice is later disproven,
    kill it via record_decision(refutes=...), not here — a Practice never refutes itself.
    Runs the SAME unified prior-art check record_decision does (over Decisions/Practices/
    Superstitions), so recording a near-duplicate technique gets flagged before it mints a
    twin the fused engine's wording just doesn't happen to match."""
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
    try:
        search_out = await comp.run_spec(
            pool, {"op": "function", "name": "search",
                   "args": {"q": f"{statement} {failure_prevented or ''}"[:300], "limit": 15,
                            "caller": actor}},
            None, name="search", caller=actor)
        prior = capture.prior_art_from_hits(
            search_out["items"]["hits"], exclude={p}, kinds=capture.UNIFIED_PRIOR_ART_KINDS)
    except Exception:  # noqa: BLE001 — never block a record on a search-side failure
        prior = []
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
    """Turn something you READ into a first-class Reference node — a paper, a vendor doc,
    a spec — so it can be FOUND by search, cited by record_decision(grounds=[...]), and
    inherited, instead of being narrated into free text and lost. `title` is the citation
    key (idempotent on its slug — re-ingesting enriches the same node). `vendor` = who
    wrote it (arxiv author, 'anthropic', 'palantir'…); `body` = what it claims, in your
    words. `caveats` is FIRST-CLASS and separate from body: the 'but only under X' that
    dies when buried in prose — if the source tightens rather than confirms, say it HERE.
    `cites` wires paper-to-paper lineage (ids, ref:<slug> canonicals, or titles of already-
    ingested References) so a literature tree is walkable instead of re-derived per session.
    Graded SELF_DECLARED (your testimony of what you read). Returns the id + canonical
    to cite."""
    pool = await _pool_get()
    cids: list[uuid.UUID] = []
    missing: list[str] = []
    for c in cites or []:
        rid = await _resolve(pool, c)
        (cids.append(rid) if rid is not None else missing.append(c))
    try:
        ref, canon = await capture.ingest_reference(
            Actions(pool), title, source_url=source_url, vendor=vendor,
            body=body, caveats=caveats, repo=repo, cites=cids,
            source=await _actor_for(ctx, subagent_id, subagent_type),
        )
    except ValueError as e:  # task #107: e.g. a path-shaped repo — refuse clean, no traceback
        return {"error": str(e)}
    out: dict[str, Any] = {"id": str(ref), "canonical": canon,
                           "note": "cite it: record_decision(..., grounds=['" + canon + "'])"}
    if missing:
        out["unresolved_cites"] = missing
        out["cites_note"] = ("unresolved cites SKIPPED — ingest_reference each cited work "
                             "first, then re-ingest this title (idempotent) to wire the edges")
    return out


@mcp.tool()
async def open_thread(
    summary: str, repo: str | None = None, kind: str | None = None,
    owner: str | None = None, assignee: str | None = None, arc: str | None = None,
    session_anchor: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, str]:
    """Open a THREAD — an unresolved question or next-step you want the next session to pick
    up. Surfaces in run_composition('briefing') under open threads, beside mined ones. `repo`
    files it under a SoftwareProject. Idempotent on the summary — and, with `repo`, ALSO on a
    near-duplicate of it: two field witnesses (Aegis, Maat) minted the same fact twice across
    a lineage restart because the second telling reworded the summary slightly. A near-hit on
    that project's own open threads returns the EXISTING id (`deduped: "true"`) instead of
    minting a twin — conservative on purpose, so a genuinely new thread is never swallowed.
    This is how a session hands off its loose ends instead of losing them (or doubling them).
    `kind='obligation'` marks a DUTY minted by an action ('kernel changed → daemons need
    restart') — record those the moment they're minted; they are neither rulings nor commits
    and otherwise die with the context window. `owner` says WHOSE MOVE it is: 'operator' =
    blocked on the human, 'agent:<id>' = a specific mind, a project name = any hand there;
    unowned = anyone who reads it may act. orient sorts your wall by it — yours-to-act above
    waiting-on-the-human.
    `assignee` (alfred's ask 5, ruling dd47c1da §4.3 — "single-assignee leased
    obligations") is the seat/agent THIS BUILD belongs to — one build, one assignee. It
    stamps the SAME `owner` property (not a parallel field: `owner` already IS "whose move
    it is"; orient's ranking needs no change). What's new is the LEASE: with `assignee` set,
    a near-duplicate hit SURFACES THE EXISTING LEASE instead of just deduping silently —
    `leased_to` names who already holds it. Asking again as the SAME assignee finds your own
    open build; a DIFFERENT assignee asking for near-duplicate work surfaces it too, by
    design — a double-assignment must be VISIBLE, never silent.
    `arc` names which of a CLOSED taxonomy (capture.ARCS: Identity-Succession,
    Compaction-Resilience, Model-Identity, Token-Cost, Surfaces-Roadmap-Docs,
    Fleet-Hygiene, Security) this thread belongs to — the roadmap screen's top grouping.
    Omit it for the common case; an unrecognized value refuses loudly rather than
    fragmenting the taxonomy with a typo."""
    pool = await _pool_get()
    # AN UNFILED THREAD IS INVISIBLE TO ITS OWN PROJECT (Alfred V's succession repro,
    # thread 4ffe0eb9: IV's handoff, opened without repo=, hid from orient and the whisper
    # while his successor mined transcripts with regex). The mounted identity already
    # knows the project — filing there is the default; unfiled takes deliberate effort.
    if not repo:
        ident = await _ident_for(ctx)
        repo = ident.project if ident else None
    dup = await capture.find_near_duplicate_open_thread(pool, summary, repo=repo)
    if dup is not None:
        out: dict[str, str] = {"id": str(dup), "summary": summary, "status": "open",
                               "deduped": "true"}
        if assignee:
            holder = await capture._current_owner(pool, dup)
            claim = assignee.strip()
            out["leased_to"] = holder or "(unowned)"
            out["note"] = (
                f"already leased to {holder} (thread {str(dup)[:8]}) — no new build minted"
                if holder == claim else
                f"existing lease on thread {str(dup)[:8]} is held by "
                f"{holder or '(unowned)'!r}, not {claim!r} — surfaced instead of minting a "
                "parallel build (a double-assignment must be visible, not silent)"
            )
        return out
    try:
        t = await capture.open_thread(
            Actions(pool), summary, repo=repo, kind=kind, owner=owner, assignee=assignee,
            arc=arc, source=await _actor_for(ctx, subagent_id, subagent_type)
        )
    except ValueError as e:
        return {"error": str(e)}
    out = {"id": str(t), "summary": summary, "status": "open", "deduped": "false"}
    if assignee:
        out["assignee"] = assignee.strip()
    return out


@mcp.tool()
async def resolve_thread(
    ref: str, because: str | None = None, artifact: str | None = None,
    subagent_id: str | None = None,
    subagent_type: str | None = None, session_anchor: str | None = None,
    ctx: Context | None = None
) -> dict[str, str]:
    """Close a THREAD you (or an earlier session) resolved — `ref` is its UUID or a summary
    substring; `because` records why (a short WHY, not a completion essay). It leaves
    briefing's open list and joins the resolved section. Event-sourced (never deleted), so
    the close is auditable and reversible.
    `artifact` is the POINTER to what actually closed it — a commit hash, a decision id, a
    file:line — kept as resolved_artifact; when it names a graph object (Commit/Decision)
    a resolved_by edge is minted too, the strong closure witness the closure-miner almost
    never finds (022bd24a). Put the evidence THERE and keep `because` short. The receipt's
    `resolved_by` field CONFIRMS whether that edge actually landed — an artifact that only
    matched free text (a file:line, an unresolvable pointer) says so plainly rather than
    leaving the caller to guess from a conditional sentence."""
    pool = await _pool_get()
    tid = await capture.resolve_thread(
        Actions(pool), ref, because=because, artifact=artifact,
        source=await _actor_for(ctx, subagent_id, subagent_type)
    )
    if tid is None:
        return {"error": f"no open thread matches {ref!r}"}
    out = {"id": str(tid), "status": "resolved"}
    if artifact:
        out["artifact"] = f"{artifact} — kept as resolved_artifact"
        target = await pool.fetchrow(
            "SELECT o.type, o.canonical FROM links l JOIN objects o ON o.id=l.to_id "
            "WHERE l.from_id=$1 AND l.type='resolved_by' LIMIT 1", tid)
        out["resolved_by"] = (
            f"{target['type']} {target['canonical']} — the strong closure witness"
            if target is not None else
            "none — the artifact did not resolve to a graph object (a file:line or an "
            "unmatched pointer); resolved_artifact still carries it as text, but the "
            "closure-miner will not find this close"
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
async def acquire_lease(
    resource_id: str, holder: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Claim ANY resource by an EXACT id — a file path, `docker-daemon`, `compose-merge`,
    `tree` — the mechanical half of tonight's hand-maintained file-ownership map. Task
    #103's Q7 found `open_thread(assignee=)`'s `leased_to` only LOOKED like this primitive:
    its conflict check is fuzzy prose similarity over a thread summary (threshold 0.60,
    never tuned), read-then-write, repo-scoped only — two agents naming the same file in
    differently-worded summaries would get no lease at all. This is the fix: `resource_id`
    matched by EQUALITY, backed by a real DB-level uniqueness guarantee
    (`resource_leases_active_claim`), never a race.

    `resource_id` is CONVENTION, not a closed vocabulary — nothing here validates,
    enumerates, or pre-decides what strings mean. Same string in, same claim, whatever the
    caller means by it.

    `holder` defaults to YOUR OWN mounted identity; pass one explicitly to claim on
    another's behalf — the same latitude `open_thread`'s `assignee` already has (a manager
    reserving a lane before its worker starts, say).

    A REFUSAL names WHO holds it and SINCE WHEN (`holder`/`held_since`) — enough for the
    caller to decide wait-or-escalate instead of guessing, never a silent duplicate mint
    (Alfred's #4.3 UX, kept; only the matcher underneath it changed).

    EXPIRY is deliberately NOT a renewed TTL: our holders are agent sessions doing
    variable-length turns, not daemons with a background heartbeat loop — a short TTL would
    either spam constant renewal calls or false-expire a legitimate long turn. Explicit
    `release_lease` is the primary path (matches how the fleet already coordinates —
    announce when done); `reap_stale_leases` is the crash/compaction backstop, not the
    norm, and rides a cron every 5 minutes (arq_worker.reap_leases) the same way
    `reap_stale_runs` already does for `helper_runs`."""
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
    resource_id: str, holder: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    session_anchor: str | None = None, ctx: Context | None = None,
) -> dict[str, Any]:
    """Release a resource YOU hold — only the ACTUAL holder's own release call frees it,
    never a different agent's, even by name (the same asymmetry `resolve_thread` has no
    equivalent of, deliberately: a lease's whole point is that holding it means something).
    `released: false` for BOTH an unheld resource and a wrong-holder attempt — both are
    refusals to report, never errors to raise; check `check_lease` first if you need to
    tell the two apart."""
    pool = await _pool_get()
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    released = await resource_lease.release(pool, resource_id, holder or actor)
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
    lease here is agent-work-paced (a whole session touching a file), not machine-paced."""
    pool = await _pool_get()
    n = await resource_lease.reap_stale(pool, older_than_secs=older_than_secs)
    return {"reaped": n, "older_than_secs": older_than_secs}


@mcp.tool()
async def settle(
    decisions: list[dict[str, Any]] | None = None,
    threads_open: list[dict[str, Any]] | None = None,
    threads_resolve: list[dict[str, Any]] | None = None,
    repo_path: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """THE END-OF-CONTEXT RITUAL (operator ruling c5b184cd): the mechanistic brain-dump a
    session calls at every seam before compaction, so nothing it knows lives only in a
    context about to be destroyed. COMPOSES record_decision/open_thread/resolve_thread —
    never reimplements their writes — plus the same completeness boxes the Stop hook's
    offload ritual checks (now one shared implementation, src.orchestrator.settle).

    Call with NO arguments to just SURFACE status (safe, read-only — the boxes, and your
    own open obligations fleet-wide). Call WITH `decisions`/`threads_open`/`threads_resolve`
    to ACCEPT a dump in the same act — each list item is a dict of that verb's own keyword
    arguments (decisions: summary/kind/rationale/repo/resolves; threads_open: summary/repo/
    kind/owner; threads_resolve: ref/because/artifact) — settle dispatches each to the real
    verb, unchanged, then CONFIRMS by re-checking the boxes and your obligations against the
    now-updated graph. `complete` is only true when nothing is left explicitly unwritten.

    A bad `decisions`/`threads_open` item (task #107's fork, e.g. a path-shaped `repo`)
    NEVER sinks the rest of the dump — settle is the end-of-context ritual; a whole-batch
    abort here would lose everything ELSE in the same call, exactly the failure this verb
    exists to prevent. Each dropped item lands in `rejected` (kind/summary/error — the same
    "name what was wrong" shape record_decision/open_thread already raise), and `complete`
    reads False whenever `rejected` is non-empty: a dropped item is unwritten state, same
    class as a missing box, never a silent partial accept.

    `is_handoff: true` on a decision or thread item MINTS A STRUCTURED HANDOFF MARKER on
    that object (a typed property, not a summary text the reader greps for — the ROOT
    fragility behind every 'Thoth II'-style mislabel this house has hit): your successor's
    orient() finds it directly, no ILIKE guess on the word 'handoff' required. Idempotent
    and safe to call repeatedly through a session — later calls only add to what's already
    written, never duplicate it (same discipline as record_decision/open_thread).

    SURFACE also runs `git status --porcelain` (`uncommitted_git_files` in the receipt) —
    the one box that isn't in the graph (operator ruling, 2026-07-26: an agent asked 'safe
    to compact?' had to check this by hand). PASS `repo_path` NAMING YOUR CODE REPO — your
    mounted cwd is checked ONLY as a fallback, and for a seat-office agent (most of this
    fleet) that cwd is the OFFICE, never the repo it governs (CLAUDE.md: 'code lives
    elsewhere'), so an office-mounted call with no `repo_path` reads None here even with a
    dirty tree sitting uncommitted in your actual repo — the exact gap that motivated this
    box in the first place (Thoth, msg 1381). The receipt's `git_checked_path` names
    whichever directory was actually used, so you can tell at a glance whether it's the
    right one. None on `uncommitted_git_files` means unevaluable there (no repo at that
    path) and never blocks `complete`; a non-empty list does."""
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
    for item in decisions or []:
        item = dict(item)
        is_handoff = bool(item.pop("is_handoff", False))
        summary = item.pop("summary")
        try:
            did = await capture.record_decision(
                Actions(pool), summary, kind=item.pop("kind", "ruling"),
                rationale=item.pop("rationale", None), repo=item.pop("repo", None),
                resolves=item.pop("resolves", None), source=actor)
        except ValueError as e:
            rejected.append({"kind": "decision", "summary": summary, "error": str(e)})
            continue
        if is_handoff:
            await Actions(pool).assert_property(did, "is_handoff", "true", actor, now, 0.9,
                                                evidence_class="self_declared")
        accepted["decisions"].append({"id": str(did)[:8], "is_handoff": is_handoff})
    for item in threads_open or []:
        item = dict(item)
        is_handoff = bool(item.pop("is_handoff", False))
        summary = item.pop("summary")
        try:
            tid = await capture.open_thread(
                Actions(pool), summary, repo=item.pop("repo", None),
                kind=item.pop("kind", None), owner=item.pop("owner", None), source=actor)
        except ValueError as e:
            rejected.append({"kind": "thread", "summary": summary, "error": str(e)})
            continue
        if is_handoff:
            await Actions(pool).assert_property(tid, "is_handoff", "true", actor, now, 0.9,
                                                evidence_class="self_declared")
        accepted["threads_opened"].append({"id": str(tid)[:8], "is_handoff": is_handoff})
    for item in threads_resolve or []:
        item = dict(item)
        resolved_ref = item.get("ref")
        rid = await capture.resolve_thread(
            Actions(pool), item.pop("ref"), because=item.pop("because", None),
            artifact=item.pop("artifact", None), source=actor)
        accepted["threads_resolved"].append(
            {"id": str(rid)[:8]} if rid is not None else
            {"error": f"no open thread matches {resolved_ref!r}"})

    # CONFIRM: re-check against the now-updated graph — a no-op re-derivation when nothing
    # was accepted above, which is exactly the pure-SURFACE call shape.
    from src.orchestrator.settle import (
        filed_under_check,
        missing_boxes,
        settle_boxes,
        uncommitted_git_work,
    )
    mounted = await mounts.find_session_row(pool, ident.session)
    boxes: dict[str, bool | None] = {}
    missing: list[str] = []
    identity_coherence: dict[str, Any] | None = None
    if mounted is not None and mounted["mounted_at"]:
        boxes = await settle_boxes(pool, agent_id=ident.agent_id,
                                   mounted_at=mounted["mounted_at"], cwd=ident.cwd)
        missing = missing_boxes(boxes)
        # REPORT-ONLY, NEVER A GATE (Thoth's Lane 4 finding — settle verified WHAT John
        # wrote, never WHETHER his own successor could read it from where orient() looks):
        # `identity_coherence` never touches `missing`/`complete` below, however wrong it
        # looks — a false-positive here refusing a settle is a strictly worse outcome than
        # the incoherence it would have caught (ruling 577988ed).
        identity_coherence = await filed_under_check(
            pool, agent_id=ident.agent_id, mounted_at=mounted["mounted_at"],
            project=ident.project)
    # OBLIGATIONS ARE CARRIED, NOT UNWRITTEN (thread f0511eed, found on Thoth's first live
    # dogfood): `complete` used to read false whenever ANY open obligation named this
    # agent's lineage as owner — even ancient backlog this session never touched (a
    # manager's project always has SOME open obligation, so complete could never read true
    # in practice). An open Thread is already durably RECORDED — that is exactly what
    # open_thread's write accomplishes — so it is not "unwritten state a compaction could
    # lose" the way a missing box or an uncommitted git file is. The compaction-safety
    # question this tool answers is "is THIS session's own state deposited," which the
    # boxes (and the git check) answer completely on their own. Obligations stay in the
    # receipt — surfaced, never hidden — but carried forward informationally; they no
    # longer gate `complete`.
    obligations = await _owned_open_threads(pool, ident.agent_id)
    git_dir = repo_path or ident.cwd
    uncommitted = await uncommitted_git_work(git_dir)
    # a REJECTED item is unwritten state, same class as a missing box or an uncommitted git
    # file (unlike `obligations` above, which are already durably recorded and never gate
    # this) — so it gates `complete` too: a dump that dropped something is not yet deposited.
    complete = not missing and not uncommitted and not rejected
    reasons = []
    if missing:
        reasons.append(f"{len(missing)} missing box(es)")
    if uncommitted:
        reasons.append(f"{len(uncommitted)} uncommitted git file(s)")
    if rejected:
        reasons.append(f"{len(rejected)} rejected item(s)")
    carried_note = (f" ({len(obligations)} open obligation(s) carried forward — "
                    "informational, already durably recorded, never blocks this)"
                    if obligations else "")
    out: dict[str, Any] = {
        "complete": complete,
        "boxes": boxes,
        "missing_boxes": missing,
        "open_obligations": obligations,
        "uncommitted_git_files": uncommitted,
        "git_checked_path": git_dir,
        "accepted": accepted,
        "rejected": rejected,
        "note": (f"compaction-safe by construction{carried_note}" if complete else
                 f"still unsettled ({', '.join(reasons)}) — settle again once they're "
                 "closed, or accept them in your next call"),
    }
    if identity_coherence is not None:
        out["identity_coherence"] = identity_coherence
        if not identity_coherence["coherent"]:
            out["note"] += (
                f" — LOUD, NEVER BLOCKING: this session is filed under "
                f"{identity_coherence['filed_under']!r} but its own writes went to "
                f"{identity_coherence['writes_went_to']!r}; a successor mounting under "
                f"{identity_coherence['filed_under']!r} will not see them (John XVI's shape)"
            )
    return out


@mcp.tool()
async def reclassify_thread(
    ref: str, kind: str, because: str | None = None, owner: str | None = None,
    subagent_id: str | None = None,
    subagent_type: str | None = None, ctx: Context | None = None,
) -> dict[str, str]:
    """Triage a thread WITHOUT changing its status (untouched is not resolved — ruling
    758ded94). You read it and judged what it IS: `kind='obligation'` ADOPTS it as real owed
    work (floats to the top of every briefing), `kind='question'` demotes a miner-promoted
    question back to a question (stays open and searchable, leaves the work wall),
    `kind='task'` marks ordinary work. `ref` is a UUID, an 8-char short id, or a summary
    substring; `because` records your judgment. SELF_DECLARED — your testimony outranks the
    miner's guess. Use resolve_thread instead when the work is actually done or moot.
    `owner` optionally CLAIMS the thread in the same act ('operator' / 'agent:<id>' /
    a project name) — triage is where an existing thread learns whose move it is."""
    t = await capture.reclassify_thread(
        Actions(await _pool_get()), ref, kind=kind, because=because, owner=owner,
        source=await _actor_for(ctx, subagent_id, subagent_type))
    if t is None:
        return {"error": f"no thread matched {ref!r}"}
    return {"id": str(t), "kind": kind, "status": "open (unchanged — reclassified, not resolved)"}


@mcp.tool()
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
    """Register your project's KNOWN BLIND SPOT — what the harness HERE cannot verify, and
    where the real verification lives (thread 8e26cd10: 459 headless-Chromium tests stayed
    green while every iPhone was broken, because nothing modeled 'this project targets an
    engine your rig cannot see'). Surfaces at orient() under `blind_spots` for every session
    on the project, BEFORE it trusts a green harness — the most expensive thing a session
    re-derives is the shape of its own ignorance. `surface` names the capability
    ('webkit-rendering', 'ios-touch'); `cannot_see` states the gap; `verify_with` points at
    the rig or ritual that actually verifies (a test path, 'hand the phone to the operator').
    Held like a Tension (its own type, never resolved away). Idempotent per
    (project, surface) — re-register to sharpen the wording."""
    ident = await _ident_for(ctx)
    b = await capture.record_blind_spot(
        Actions(await _pool_get()), surface, cannot_see, verify_with=verify_with,
        repo=repo or (ident.project if ident else None),
        source=await _actor_for(ctx, subagent_id, subagent_type),
    )
    return {"registered": str(b), "surface": surface,
            "note": "held per (project, surface); orient() speaks it to every session here"}


@mcp.tool()
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


@mcp.custom_route("/automount", methods=["POST"])
async def automount_route(request: Any) -> Any:
    """The whisper's server half (operator's blessing, 2026-07-08): the SessionStart hook
    posts {session_id, cwd} here BEFORE the agent's first token; we mount the session through
    the exact tested path the mount() tool uses (durable row, anchored identity — the hook
    derives nothing the harness didn't give it) and return the payload the whisper prints.
    Plain HTTP on the same localhost-only listener; NEVER raises — the hook is fail-open and
    a session that got no whisper can always mount by hand."""
    from starlette.responses import JSONResponse

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
            bridge_session_id=(str(body.get("bridge_session_id") or "") or None))
        # a mint rode this whisper (compact/clear): the ancestor's connection outlives it —
        # purge the dead mind from the hot cache so no tool call answers as it again
        _evict_stale_minds(out.get("minted"))
        return JSONResponse(out)
    except Exception as e:  # noqa: BLE001 — fail-open: the whisper degrades, never blocks
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

    try:
        body = await request.json()
        session_id = str(body.get("session_id") or "")
        if not session_id:
            return JSONResponse({"error": "session_id required"}, status_code=400)
        out = await handshake.session_end(Actions(await _pool_get()), session_id=session_id)
        return JSONResponse(out)
    except Exception as e:  # noqa: BLE001 — fail-open: a session must always be able to end
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
        child = await lineage.register_spawn(
            Actions(pool), raw_id,
            agent_type=(str(body.get("agent_type") or "") or None),
            parent_agent=parent, project=project,
            session=(session_id[:8] or None),
            transcript=(Path(tp.replace("~", str(Path.home()), 1) if tp.startswith("~")
                             else tp) if tp else None),
            done=(str(body.get("phase") or "") == "stop"))
        if child:
            _spawns_seen[child] = time.monotonic()
        return JSONResponse({"spawn": child, "of": parent})
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
        pool = await create_pool(get_settings().database_url, max_size=1)
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
        pool = await create_pool(get_settings().database_url, max_size=1)
        try:
            reboot_drift = await check_unreviewed_boot(pool)
            if reboot_drift:
                await alarm_unreviewed_boot(pool, reboot_drift, service="osiris-mcp")
        finally:
            await pool.close()
    except Exception as exc:  # noqa: BLE001 — the guard must never become the thing it guards against
        logging.getLogger("osiris.deploy_guard").warning(
            "deploy_guard reboot check failed at mcp boot: %r", exc)


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

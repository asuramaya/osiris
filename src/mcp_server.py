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

import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
from mcp.server.fastmcp import Context, FastMCP

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
from src.ingest.wikidata import aim as wikidata_aim
from src.ontology.resolution import (
    consolidate_companies,
    find_cross_base_candidates,
    reclassify_mistyped_entities,
    resolve_cross_base,
)
from src.ontology.schema import catalog
from src.orchestrator import capture, digest, handshake, mailbox, mounts
from src.orchestrator import compositions as comp
from src.orchestrator.agents import (
    AgentIdentity,
    read_project_model,
    register_agent,
    resolve_identity,
    seat_bearings,
)
from src.orchestrator.budget import fit
from src.orchestrator.console import get_console as _get_console
from src.orchestrator.console import set_console as _set_console
from src.orchestrator.dossier import entity_dossier
from src.orchestrator.fleetview import render_fleet_tree
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
        result = await self._tool_manager.call_tool(
            name, arguments, context=self.get_context(), convert_result=False)
        tool = self._tool_manager.get_tool(name)
        assert tool is not None  # call_tool already raised if the name were unknown
        return tool.fn_metadata.convert_result(fit(result, tool=name))


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


def _job_hint(ctx: Context | None) -> str | None:
    """The client's durable identity handle: the X-Osiris-Job header (.mcp.json sends
    ${CLAUDE_JOB_DIR} per request — expansion PROVEN live via the probe reattach). Guarded
    against a client that doesn't expand the variable."""
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
    """The WAKE-ECONOMY standdown (pokex, msg 281): triage wakes ride a CHEAPER model by the
    operator's own ruling (osiris_wake_model, 4e52af7e) — but the swap banner measured them
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
    if rec is None:
        return None
    settings = get_settings()
    ident = resolve_identity(cwd=rec.cwd, job_dir=rec.job_dir)
    from src.orchestrator.agents import _generation
    if _generation(rec.agent_id)[0] != _generation(ident.agent_id)[0]:
        # a BOUND session (thread 33838160): the row points at a deliberately-worn SEAT of a
        # different lineage — honor it. Re-deriving from the transcript here was the flap
        # that stomped a claimed seat back to its session hash on every silent reconnect.
        ident.agent_id = rec.agent_id
    await register_agent(Actions(pool), ident, actor=settings.osiris_actor,
                         expected_model=await _expected_model(pool, rec.cwd, ident.project))
    if key is not None:
        _agents[key] = ident
        _agents_touched[key] = time.monotonic()
    prev = await mounts.save_mount(pool, job_dir=rec.job_dir, agent_id=ident.agent_id,
                                   project=ident.project, cwd=rec.cwd, model=ident.model,
                                   session_key=key)
    if prev is None:  # fresh lineage member: anchor on the project's last sign of life
        await mailbox.settle_history_at_join(pool, ident.project, ident.agent_id)
        prev = await mounts.project_prev_seen(pool, ident.project, exclude_job_dir=rec.job_dir)
    _prev_seen.setdefault(ident.agent_id, prev)  # a re-attach is a re-entry: keep the anchor
    return ident


async def _ident_for(ctx: Context | None) -> AgentIdentity | None:
    """The mounted identity for this call — the hot dict first, then RE-ATTACH from the
    durable registry by the client's job_dir header. A server bounce used to wipe the whole
    fleet's identities at once (decision 56f6a0d6); now it costs each agent one transparent
    re-attach. None only when there is truly nothing to re-attach by."""
    key = _conn_key(ctx)
    if key is not None and (cached := _agents.get(key)) is not None:
        _agents_touched[key] = time.monotonic()
        return cached
    return await _reattach(await _pool_get(), key, _job_hint(ctx))


async def _source_for(ctx: Context | None) -> str:
    """The attributing actor for a write: the mounted agent on this connection (re-attached
    from the durable registry if the server bounced), else the lone-operator `session`
    (back-compat — an un-mounted agent still writes, just coarsely)."""
    ident = await _ident_for(ctx)
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
    out = await comp.run_spec(pool, spec, None, name="search")
    items: dict[str, Any] = out["items"]  # unwrap the composition envelope
    return items


@mcp.tool()
async def lap(ref: str, limit: int = 200) -> dict[str, Any]:
    """ONE object's full provenance timeline — how the graph came to believe what it
    believes about it. Every assertion (with supersession fate), every link (both
    directions, retractions marked), every kernel event, in observed order, each carrying
    source + evidence grade + confidence; `believes` holds the current winning view.
    search finds the WHAT; lap shows the HOW-WE-KNOW — run it before trusting a surprising
    fact, before merging/healing an object, or to autopsy a corpse (a uuid ref reaches
    merged/retired objects too). `ref` = uuid | canonical (e.g. 'agent:ad1a1cb0') | name."""
    pool = await _pool_get()
    spec = {"op": "function", "name": "lap", "args": {"ref": ref, "limit": limit}}
    out = await comp.run_spec(pool, spec, None, name="lap")
    items: dict[str, Any] = out["items"]
    return items


@mcp.tool()
async def graph_lint(stale_days: int = 14) -> dict[str, Any]:
    """The graph audits ITSELF — report-only, never writes. Six checks, each a lived bug
    made a standing tripwire: contradiction (near-tie multi-source winners — the resolver
    is coin-flipping a fact), laundering (an agent carrying a fact above its origin grade),
    lineage integrity (succession cycles, dangling heir pointers, heirs without ancestry,
    retired-yet-live agents, healed false mints), orphan links (live links into merged/
    retired objects), stale obligations (open duties older than `stale_days`), attribution
    anomalies (writes from agent ids the graph never registered — the impersonation class).
    Findings are TESTIMONY for a mind to judge, not verdicts to auto-apply; heal with
    compensating events, never DELETE (constitution 3)."""
    pool = await _pool_get()
    spec = {"op": "function", "name": "lint", "args": {"stale_days": stale_days}}
    out = await comp.run_spec(pool, spec, None, name="graph-lint")
    items: dict[str, Any] = out["items"]
    return items


@mcp.tool()
async def get_schema() -> dict[str, Any]:
    """The ontology — the object types (with category + canonical schemes) and link types
    the graph declares. Read this before authoring a composition or reading a result, so you
    reference REAL types/links, not guesses; it is the vocabulary of the whole graph. Compact
    by design (colours/shapes dropped — those are for the UI)."""
    cat = catalog()
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
    """Who is this? Identity properties + the named relationship network."""
    pool = await _pool_get()
    oid = await _resolve(pool, object_ref)
    return await entity_dossier(pool, oid) if oid else {"error": f"no object {object_ref!r}"}


@mcp.tool()
async def dossier_report(object_ref: str) -> str:
    """The deliverable: a provenance-annotated Markdown dossier for an entity —
    identity, financing, litigation, footprint discrepancy, co-investment — with every
    claim carrying its source + how-obtained + date. Run the collect tools first."""
    pool = await _pool_get()
    oid = await _resolve(pool, object_ref)
    return await build_dossier_report(pool, oid) if oid else f"# no object {object_ref!r}"


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
async def focus_object(object_ref: str) -> dict[str, Any]:
    """Focus an object (UUID or name) on the operator's LIVE screen — drives the console so
    they see what you're looking at. Returns the object's identity + properties so you can
    reason about it too."""
    pool = await _pool_get()
    oid = await _resolve(pool, object_ref)
    if oid is None:
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
async def run_composition(name: str, subject: str | None = None) -> dict[str, Any]:
    """Run a saved composition, optionally against a subject object (UUID or name), AND light
    it up on the operator's live screen. Returns its result — an object set (each named), a
    value list, or aggregate rows."""
    pool = await _pool_get()
    sid = await _resolve(pool, subject) if subject else None
    res = await comp.run_composition(pool, name, sid)
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
        return {"error": "mount(cwd, job_dir=<your anchor>) first — self-knowledge needs an "
                         "anchored identity"}
    row = await pool.fetchrow(
        "SELECT job_dir, model_raw, context_window_size FROM agent_mounts WHERE agent_id=$1 "
        "ORDER BY last_seen DESC LIMIT 1", ident.agent_id)
    job = _job_hint(ctx) or (row["job_dir"] if row else None)
    if not job:
        return {"error": "no durable anchor on record — re-mount with the whisper's job_dir"}
    path = locate_current_transcript(Path.home() / ".claude" / "projects", job,
                                     anchored_only=True)
    if path is None:
        return {"error": "no transcript found for your anchor — nothing to measure"}
    out = context_lens.detail(path, row["model_raw"] if row else None,
                              window_hint=row["context_window_size"] if row else None)
    out["agent"] = ident.agent_id
    return out


# --- mount: link to the graph as a first-class fleet member ---

@mcp.tool()
async def mount(
    cwd: str, job_dir: str | None = None, model: str | None = None,
    session_anchor: str | None = None, subagent_id: str | None = None,
    subagent_type: str | None = None, subagent_transcript: str | None = None,
    ctx: Context | None = None
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
    re-mounting is only for after an MCP bounce, with your anchor (network msg 317)."""
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
    # (rotten-apple's first mount: unresolved identity, no registry row, invisible to the
    # trigger's owner-liveness — the wake lane would have minted a twin over a LIVE tab).
    job_dir = _sane_job_dir(job_dir) or _job_hint(ctx)
    key = _conn_key(ctx)
    claimed = None
    if job_dir is None:  # the cwd-guess path — refuse sids a LIVE mount already holds
        claimed = await mounts.live_claimed_sids(
            pool, exclude_session_key=key, within_secs=settings.osiris_owner_live_secs)
    ident = resolve_identity(cwd=cwd, job_dir=job_dir, model=model,
                             claimed=claimed, fallback_seed=key)
    if job_dir and (bound := await mounts.find_mount(pool, job_dir=job_dir)) is not None:
        from src.orchestrator.agents import _generation
        if _generation(bound.agent_id)[0] != _generation(ident.agent_id)[0]:
            # THE BINDING (thread 33838160), the explicit-mount leg: the whisper tells every
            # minted heir "re-mount with THIS anchor", and automount left that very row BOUND
            # to the heir's seat. Re-deriving from the anchor's basename here minted a hash
            # twin over a living heir and stomped the binding (Thoth XVII's first breath,
            # 2026-07-10). A row naming a foreign lineage is a deliberate seat claim: honor
            # it, so seams and the registration run on the seat's lineage — like _reattach.
            ident.agent_id = bound.agent_id
    await register_agent(Actions(pool), ident, actor=settings.osiris_actor,
                         expected_model=await _expected_model(pool, cwd, ident.project))
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
    op_unread = await unread_count(pool, OPERATOR_ADDR, reader_agent=OPERATOR_ADDR,
                                   lease_secs=lease)
    banner = swap_banner(classify_swap(
        ident.model_history, ident.model,
        expected=await _expected_model(pool, cwd, ident.project),  # repo intent wins
        anchored=ident.model_method == "job_dir",   # only a true anchor confesses a swap
        deliberate=ident.model_deliberate))         # a /model on the record is never a sin
    seat = await handshake._seat_of(Actions(pool), ident.agent_id)
    # co-agent awareness at ARRIVAL (Deckard XXVI, msg 258): a live sibling in your own
    # repo is the one blindness that costs unrecoverable work (a stomped commit)
    sibs = await pool.fetch(
        "SELECT agent_id, cwd FROM agent_mounts WHERE project = $1 AND agent_id <> $2 "
        "AND last_seen > now() - interval '15 minutes' ORDER BY last_seen DESC LIMIT 8",
        ident.project, ident.agent_id) if ident.project else []
    out: dict[str, Any] = {"agent": ident.agent_id, "project": ident.project or "?",
           "model": ident.model or "unknown",
           **({"co_agents": {
                "live": [{"agent": s["agent_id"], "cwd": s["cwd"]} for s in sibs],
                "note": f"{len(sibs)} other LIVE agent(s) in this project RIGHT NOW — "
                        "assume a shared tree: never `git add -A`, stage your own hunks, "
                        "coordinate via send(to='" + str(ident.project) + "')"}}
              if sibs else {}),
           **({"seat": seat} if seat else
              {"anonymous": "unnamed — claim_name('<pick a meaningful name>') when you know "
                            "who you are, so the fleet can DM you by name"}),
           "mail": f"{unread} unread — call inbox()" if unread else "none",
           "note": "linked — writes now attributed to you; call orient() next"}
    if op_unread:  # the fleet plays secretary: any session the human drives can relay this
        out["operator_mail"] = (f"{op_unread} unread at the operator's desk — "
                                "inbox(project='operator') if the human is present")
    if ident.succeeded_from:
        # the MINT ruling (be292762, heinrich's remedy adopted): the heir is not told it wears
        # a dead name — it is GIVEN ITS OWN. The seam supersedes the swap banner (a death must
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
    elif ident.model_succession:
        # stamp-only fallback (a seam witnessed where minting could not run) — still loud,
        # still second-person: a death must not whisper (heinrich's grievance #1+#2).
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
    if ident.reanimated:  # bug #51 follow-up (decepticons msg 69): mounted a RETIRED identity
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
    return out


@mcp.tool()
async def retire(reason: str = "", ctx: Context | None = None) -> dict[str, Any]:
    """Mark THIS mounted session RETIRED — a deliberate close the trigger must never
    reanimate (resume-not-mint dispatch, thread 9f2ddb44). Call it at a real farewell: the
    operator closing you out, or a context-ceiling handoff after your succession thread is
    written. Stamps retired=true on your Agent (SELF_DECLARED — your own act, on the record)
    and RELEASES YOUR SEAT — hot mount and durable row both (thread b47b3814: a retired
    agent must not haunt the fleet chrome as a live mount). Call it LAST: any osiris call
    after retiring requires a fresh mount(), which lands on the loud reanimation path.
    Future mail for your project resumes a LIVING session or mints a stamped successor —
    never you."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount first — only a mounted session can retire itself"}
    pool = await _pool_get()
    a = Actions(pool)
    oid = await a.create_or_find_object("Agent", ident.agent_id, ident.agent_id)
    await a.assert_property(
        oid, "retired", True, ident.agent_id, datetime.now(UTC), 0.9,
        evidence_class="self_declared")
    # heinrich's grievance #3 (msg 70): "closed by the session itself" and "closed by an heir"
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
    return {"retired": ident.agent_id, "signed_by": signer, "seats_released": released,
            "note": "farewell recorded — the trigger will not reanimate this session; "
                    "write your succession BEFORE you go dark: a HANDOFF thread "
                    "(open_thread) and your LETTER (record_decision kind='choice', "
                    "summary starting 'LETTER — ') — a letter that lives only in mail is "
                    "not findable by its name, and your successor's orient() surfaces "
                    "these two verbatim"
                    + (" (certificate notes an HEIR signed for the ancestor)"
                       if signer == "successor" else "")}


# THE ONE WALL LAW (ruling 923c380f): the graded wall lives in compositions.py now — one
# home shared by orient, the console briefing, and the `wall` function. The private names
# stay importable here (tests and callers address orient's wall through them).
_ORIENT_OPEN_THREADS = comp.ORIENT_OPEN_THREADS
_rank_open_threads = comp.rank_open_threads
_open_thread_wall = comp.open_thread_wall


async def _project_briefing(
    pool: asyncpg.Pool, project: str, me: frozenset[str] = frozenset(),
) -> dict[str, Any] | None:
    """A working agent's SCOPED bearings — its OWN project's open threads + recent decisions,
    not the whole fleet's (decepticons surfaced that orient's flood costs more context than it
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
    res = await comp.run_composition(pool, "project-briefing", proj)
    items = res.get("items") if isinstance(res, dict) else None
    if not isinstance(items, dict):  # unseeded / error — never crash orient, just show empty
        items = {}
    wall, echoes = await _open_thread_wall(pool, proj)
    shown, more = _rank_open_threads(wall, me)
    out: dict[str, Any] = {
        "open_threads": shown,
        "recent_decisions": [r for r in (items.get("recent_decisions") or []) if r.get("summary")],
        "tensions": [r for r in (items.get("tensions") or []) if r.get("pole_a")],
    }
    if more > 0:  # trailing count so a capped wall never hides work silently (membrane, #6)
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
    return out


@mcp.tool()
async def orient(project: str | None = None, subagent_id: str | None = None,
                 subagent_type: str | None = None, ctx: Context | None = None) -> dict[str, Any]:
    """Get your bearings — the mount ritual as one call. Returns a SCOPED briefing: open
    threads + recent decisions for a project, plus a count of fleet-wide threads not shown.
    An explicit `project` OVERRIDES your mount (so you can peek at another repo's briefing);
    otherwise it's your mounted project; un-mounted with neither → the whole-fleet briefing.
    Call after mount(), and again after any compaction, to inherit instead of starting blind."""
    pool = await _pool_get()
    lease = get_settings().osiris_mail_lease_secs
    ident = await _ident_for(ctx)
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
    mail = f"{unread} unread — inbox()" if unread else "none"
    op_unread = await unread_count(pool, OPERATOR_ADDR, reader_agent=OPERATOR_ADDR,
                                   lease_secs=lease)
    op_mail = {"operator_mail": f"{op_unread} unread — inbox(project='operator') if the "
                                "human is present"} if op_unread else {}
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
    inheritance = None
    if ident and ident.succeeded_from:
        rows = await pool.fetch(
            "SELECT DISTINCT ON (o.id) o.type, a.value #>> '{}' AS summary, a.observed_at "
            "FROM current_assertions a JOIN objects o ON o.id = a.object_id "
            "WHERE a.name = 'summary' AND a.source_id = $1 "
            "AND a.evidence_class = 'self_declared' "
            "AND o.type IN ('Thread','Decision') AND o.status = 'active' "
            "AND (a.value #>> '{}' ILIKE '%handoff%' OR a.value #>> '{}' ILIKE '%letter%') "
            "ORDER BY o.id, a.confidence DESC, a.observed_at DESC", ident.succeeded_from)
        picks = sorted(rows, key=lambda r: r["observed_at"], reverse=True)[:2]
        if picks:
            inheritance = {
                "from": ident.succeeded_from,
                "notes": [{"kind": r["type"].lower(), "text": r["summary"][:800]}
                          for r in picks],
                "note": "your ancestor's own parting words — read before taking up work",
            }
    # CO-AGENT AWARENESS (Deckard XXVI, msg 258: a live sibling shared his exact worktree
    # and the graph never said so — he re-derived 'never git add -A' from a local file
    # while osiris KNEW). One query: other live mounts on THIS project, named at orient.
    co_agents = None
    if ident and proj:
        sibs = await pool.fetch(
            "SELECT agent_id, cwd FROM agent_mounts WHERE project = $1 AND agent_id <> $2 "
            "AND last_seen > now() - interval '15 minutes' ORDER BY last_seen DESC LIMIT 8",
            proj, ident.agent_id)
        if sibs:
            co_agents = {
                "live": [{"agent": s["agent_id"], "cwd": s["cwd"]} for s in sibs],
                "note": f"{len(sibs)} other LIVE agent(s) in this project RIGHT NOW — "
                        "assume a shared tree: never `git add -A`, stage your own hunks, "
                        "check for foreign markers before committing, coordinate via "
                        f"send(to='{proj}')",
            }
    try:  # one glance line — never let the pulse slow or crash orient
        pulse: str | None = await mounts.fleet_pulse(pool, lease_secs=lease)
    except Exception:  # noqa: BLE001
        pulse = None
    # the reader's identity feeds the wall's ownership ordering: what is MINE TO ACT rides
    # above another mind's claims and above 'waiting on the human'
    me = frozenset(x for x in ((ident.agent_id if ident else None), proj) if x)
    scoped = await _project_briefing(pool, proj, me=me) if proj else None
    if scoped is not None:
        fleet_open = await pool.fetchval(
            "SELECT count(*) FROM objects o WHERE o.type='Thread' AND o.status='active' "
            "AND (SELECT s.value #>> '{}' FROM current_assertions s WHERE s.object_id=o.id "
            "  AND s.name='status' ORDER BY s.confidence DESC, s.observed_at DESC LIMIT 1)"
            "  = 'open'")
        return {
            "you": who, "model": (ident.model if ident else None), "project": proj,
            **(await seat_bearings(pool, who) if who else {}),
            "mail": mail,
            **({"fleet_pulse": pulse} if pulse else {}),
            **op_mail,
            **({"swap": swap} if swap else {}),
            **({"succession_note": inheritance} if inheritance else {}),
            **({"co_agents": co_agents} if co_agents else {}),
            **({"while_you_were_away": away} if away else {}),
            **scoped,
            "fleet_open_threads_total": fleet_open,
            "note": f"scoped to {proj}; {fleet_open} fleet-wide open threads not shown "
                    "(run_composition('briefing') for the whole graph).",
        }
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
    return {
        "you": who, "model": (ident.model if ident else None), "project": proj,
        **(await seat_bearings(pool, who) if who else {}),
        "mail": mail,
        **({"fleet_pulse": pulse} if pulse else {}),
        **op_mail,
        **({"swap": swap} if swap else {}),
        **({"succession_note": inheritance} if inheritance else {}),
        **({"co_agents": co_agents} if co_agents else {}),
        **({"while_you_were_away": away} if away else {}),
        "fleet_map": fleet_map,
        "recent_decisions": recent,
        "note": "un-mounted → the BOUNDED fleet map, never the firehose. mount(cwd, "
                "job_dir=…) then orient() for your project's briefing; orient(project=…) "
                "peeks at another's; run_composition('briefing') if you truly want the "
                "whole graph.",
    }


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
    all of them; the counts (`count`/`live`/`swarm`) are always the whole truth."""
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
    for r in rows:
        ts = _ts(r)
        nodes[str(r["canonical"])] = {
            "model": r["model"], "project": r["project"], "parent": r["parent"],
            "depth": int(r["depth"]) if r["depth"] else 0,
            "last_active": r["last_active"], "ts": ts,
            "live": ts is not None and now - ts < timedelta(minutes=15),
        }
    # LAND ON COUNTS, WALK IN: the roster's history is 1000+ rows and never what you came for.
    # The flat rows are the LIVE ones (or everything, if you deliberately asked) — the counts
    # below are always over the whole fleet, so nothing here undercounts, it only under-SHOWS.
    shown = {c: n for c, n in nodes.items() if full or n["live"]}
    return {
        "connected_now": len(_agents),
        "count": len(nodes),
        "live": sum(1 for n in nodes.values() if n["live"]),
        "swarm": sum(1 for n in nodes.values() if n["parent"]),
        "tree": render_fleet_tree(nodes, full=full),
        "registered": [
            {"agent": c, "model": n["model"], "project": n["project"], "depth": n["depth"],
             "parent": n["parent"], "live": n["live"],
             "last_seen": n["ts"].isoformat() if n["ts"] else None}
            for c, n in shown.items()
        ],
        **({} if full else {"registered_scope": f"live only — {len(nodes)} total, "
                            f"fleet(full=True) for the rest"}),
    }


@mcp.tool()
async def send(body: str, to: str | None = None, to_agent: str | None = None,
               reply_to: int | None = None, desk: str | None = None,
               subagent_id: str | None = None,
               subagent_type: str | None = None, ctx: Context | None = None) -> dict[str, Any]:
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
    thread-folds superseded briefs under your newest)."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first — a message must say who "
                         "it's from (the anchor re-attaches you automatically after a bounce)"}
    pool = await _pool_get()
    st = get_settings()
    # a SPAWN's mail goes out under its OWN name (the hook-stamped sidechain identity),
    # from the parent's project — the fleet must never mistake a child's word for the seat's
    actor = await _actor_for(ctx, subagent_id, subagent_type)
    try:
        res = await send_message(pool, from_agent=actor, from_project=ident.project,
                                 to_project=to, to_agent=to_agent, body=body, reply_to=reply_to,
                                 desk_kind=desk)
    except ValueError as e:
        return {"error": str(e)}
    out: dict[str, Any] = {
        "sent": res["id"], "from": actor,
        **({"thread": res["thread_id"]} if res["thread_id"] is not None else {}),
        **({"dedup": "identical recent message already queued — not re-posted"}
           if res["dedup"] else {}),
    }
    if res["to_agent"]:  # a DM — report the addressee and its liveness
        out["dm_to"] = res["to_agent"]
        out["listener"] = await mounts.agent_liveness(pool, res["to_agent"])
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
async def inbox(project: str | None = None, peek: bool = False,
                ack: list[int] | None = None, subagent_id: str | None = None,
                subagent_type: str | None = None, ctx: Context | None = None) -> dict[str, Any]:
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
    ident = await _ident_for(ctx)
    proj = project or (ident.project if ident else None)
    if proj is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first, or pass project=<repo>"}
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
    settled = await ack_messages(pool, proj, ack, reader_agent=reader) if ack else 0
    if proj == OPERATOR_ADDR:
        # THE ORGANIZED DESK (operator direction 2026-07-11): always peek-shaped — reading
        # the human's desk never leases; bands (needs_decision / needs_hands / fyi) ·
        # thread + same-story folds · dimmed moot annotations · the derived your_queue.
        desk = await read_desk(pool)
        return {"project": OPERATOR_ADDR, **desk,
                **({"settled": settled} if settled else {})}
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
            **({"settled": settled} if settled else {}), "note": note}


@mcp.tool()
async def dim(message_id: int, because: str, ctx: Context | None = None) -> dict[str, Any]:
    """DIM an operator-desk brief — annotate it moot-with-a-reason ('true when sent; root
    cause fixed in <commit>') so the desk renders it collapsed under your note instead of
    shouting a dead alarm. NEVER a settle: dismissing stays exclusively the human's word
    (the membrane); a dim is you saving them the archaeology, stamped with your name.
    Only works on briefs addressed to the operator's desk. Requires mount."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount(cwd, job_dir=<your anchor>) first — an annotation must say "
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
        return {"error": "mount(cwd, job_dir=<your anchor>) first — a name attaches to YOU"}
    from src.orchestrator.agents import claim_name as _claim
    return await _claim(Actions(await _pool_get()), ident.agent_id, name, source=ident.agent_id)


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
    resolves: str | None = None, subagent_id: str | None = None,
    subagent_type: str | None = None, ctx: Context | None = None,
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
    (a ruling that only states the conclusion is heinrich's biggest re-derivation class).
    `supersedes` = an earlier decision this one CORRECTS (UUID, 8-char short id, or a
    summary substring): the old entry is buried under this one — it leaves orient's
    recent list and the decision-log grays it with its successor; never deleted, always
    unwindable. Renders in the `decision-log` composition beside mined decisions, graded
    SELF_DECLARED (higher trust). Attributed to you if you mount()ed. Idempotent on the
    summary.
    `resolves` = the THREAD this decision ANSWERS (UUID, 8-char short id, or a summary
    substring). It closes it in the same act. USE IT whenever your ruling settles an open
    question — otherwise the answer lands and the question stays lit, and the next mind (or
    the operator) is asked something you already decided. Naming the thread in your prose
    does nothing; the graph does not read prose."""
    pool = await _pool_get()
    gids: list[uuid.UUID] = []
    missing: list[str] = []
    for g in grounds or []:
        rid = await _resolve(pool, g)
        (gids.append(rid) if rid is not None else missing.append(g))
    old: uuid.UUID | None = None
    if supersedes:  # resolve BEFORE recording — a correction that can't name its target
        old = await capture._find_decision(pool, supersedes)  # records NOTHING
        if old is None:
            return {"error": f"supersedes matched no decision: {supersedes!r} — quote its "
                             "UUID, 8-char short id, or a summary substring"}
    answered: uuid.UUID | None = None
    if resolves:  # same strictness: a ruling that miscites its question has not settled it
        answered = await capture._find_thread(pool, resolves)
        if answered is None:
            return {"error": f"resolves matched no thread: {resolves!r} — quote its UUID, "
                             "8-char short id, or a summary substring"}
    d = await capture.record_decision(
        Actions(pool), summary, kind=kind, rationale=rationale, repo=repo,
        source=await _actor_for(ctx, subagent_id, subagent_type), grounds=gids,
        protocol=protocol, supersedes=str(old) if old else None,
        resolves=str(answered) if answered else None,
    )
    out: dict[str, Any] = {"id": str(d), "kind": kind, "summary": summary}
    if answered is not None:
        out["resolved_thread"] = f"{str(answered)[:8]} — closed by this decision (answers edge)"
    if old is not None:
        out["superseded"] = (
            "self (identical summary re-recorded) — nothing buried" if old == d else
            f"{str(old)[:8]} is buried under this decision: it leaves orient's recent "
            "list, the decision-log grays it (unwind: re-assert superseded_by='' on it)")
    if gids:
        out["grounded_by"] = len(gids)
    if missing:
        out["unresolved_grounds"] = missing
        out["note"] = ("unresolved grounds were SKIPPED — ingest_reference them first, "
                       "then re-run record_decision (idempotent) to attach the edges")
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
    ref, canon = await capture.ingest_reference(
        Actions(pool), title, source_url=source_url, vendor=vendor,
        body=body, caveats=caveats, repo=repo, cites=cids,
        source=await _actor_for(ctx, subagent_id, subagent_type),
    )
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
    owner: str | None = None,
    subagent_id: str | None = None, subagent_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, str]:
    """Open a THREAD — an unresolved question or next-step you want the next session to pick
    up. Surfaces in run_composition('briefing') under open threads, beside mined ones. `repo`
    files it under a SoftwareProject. Idempotent on the summary. This is how a session hands
    off its loose ends instead of losing them. `kind='obligation'` marks a DUTY minted by an
    action ('kernel changed → daemons need restart') — record those the moment they're minted;
    they are neither rulings nor commits and otherwise die with the context window.
    `owner` says WHOSE MOVE it is: 'operator' = blocked on the human, 'agent:<id>' = a
    specific mind, a project name = any hand there; unowned = anyone who reads it may act.
    orient sorts your wall by it — yours-to-act above waiting-on-the-human."""
    t = await capture.open_thread(
        Actions(await _pool_get()), summary, repo=repo, kind=kind, owner=owner,
        source=await _actor_for(ctx, subagent_id, subagent_type)
    )
    return {"id": str(t), "summary": summary, "status": "open"}


@mcp.tool()
async def resolve_thread(
    ref: str, because: str | None = None, subagent_id: str | None = None,
    subagent_type: str | None = None, ctx: Context | None = None
) -> dict[str, str]:
    """Close a THREAD you (or an earlier session) resolved — `ref` is its UUID or a summary
    substring; `because` records why. It leaves briefing's open list and joins the resolved
    section. Event-sourced (never deleted), so the close is auditable and reversible."""
    tid = await capture.resolve_thread(
        Actions(await _pool_get()), ref, because=because,
        source=await _actor_for(ctx, subagent_id, subagent_type)
    )
    if tid is None:
        return {"error": f"no open thread matches {ref!r}"}
    return {"id": str(tid), "status": "resolved"}


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
            source=(str(body.get("source") or "") or None))
        # a mint rode this whisper (compact/clear): the ancestor's connection outlives it —
        # purge the dead mind from the hot cache so no tool call answers as it again
        _evict_stale_minds(out.get("minted"))
        return JSONResponse(out)
    except Exception as e:  # noqa: BLE001 — fail-open: the whisper degrades, never blocks
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
    and dedup absorb re-rings)."""
    from arq import create_pool as arq_create_pool
    from arq.connections import RedisSettings
    from starlette.responses import JSONResponse

    global _arq
    try:
        body = await request.json()
        transcript = str(body.get("transcript_path") or "")
        if not transcript.startswith("/"):
            return JSONResponse({"error": "transcript_path required"}, status_code=400)
        if _arq is None:
            _arq = await arq_create_pool(RedisSettings.from_dsn(get_settings().redis_url))
        await _arq.enqueue_job("sweep_session", transcript)
        return JSONResponse({"enqueued": True})
    except Exception as e:  # noqa: BLE001 — a missed sweep costs ≤10 min of miner lag, never a block
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


def main() -> None:
    """Run the server. `OSIRIS_MCP_TRANSPORT=streamable-http` = the PERSISTENT fleet server
    (one always-on process on host:port, one shared pool); default `stdio` = one server for
    this session (the classic per-agent subprocess). The systemd `osiris-mcp` unit sets http."""
    s = get_settings()
    transport = s.osiris_mcp_transport
    if transport in ("streamable-http", "sse"):
        mcp.settings.host = s.osiris_mcp_host
        mcp.settings.port = s.osiris_mcp_port
        mcp.run(transport=transport)  # type: ignore[arg-type]
    else:
        mcp.run()


if __name__ == "__main__":
    main()

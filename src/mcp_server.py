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
from src.orchestrator import capture, digest, mounts
from src.orchestrator import compositions as comp
from src.orchestrator.agents import AgentIdentity, register_agent, resolve_identity
from src.orchestrator.console import get_console as _get_console
from src.orchestrator.console import set_console as _set_console
from src.orchestrator.dossier import entity_dossier
from src.orchestrator.mailbox import (
    OPERATOR_ADDR,
    ack_messages,
    read_inbox,
    send_message,
    unread_count,
)
from src.orchestrator.sources import as_dicts, suggest
from src.orchestrator.swaps import classify_swap, swap_banner
from src.orchestrator.trigger import wake_status

mcp = FastMCP(
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


def _job_hint(ctx: Context | None) -> str | None:
    """The client's durable identity handle: the X-Osiris-Job header (.mcp.json sends
    ${CLAUDE_JOB_DIR} per request). Guarded against a client that doesn't expand the
    variable — a literal `${...}` is no hint at all."""
    if ctx is None:
        return None
    try:
        req = ctx.request_context.request
        hint = req.headers.get("x-osiris-job") if req is not None else None
    except (AttributeError, LookupError):
        return None
    if not hint or "${" in hint:
        return None
    return str(hint)


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
    await register_agent(Actions(pool), ident, actor=settings.osiris_actor,
                         expected_model=settings.osiris_expected_model)
    if key is not None:
        _agents[key] = ident
        _agents_touched[key] = time.monotonic()
    await mounts.save_mount(pool, job_dir=rec.job_dir, agent_id=ident.agent_id,
                            project=ident.project, cwd=rec.cwd, model=ident.model,
                            session_key=key)
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
    """Accept a UUID or a name; resolve to an active object id. Tries exact name, then
    substring — and among matches prefers the most-described (the merged canonical)."""
    try:
        return uuid.UUID(ref)
    except ValueError:
        pass
    # exact name (most-described wins), then substring (shortest name wins — closest to
    # the query, so 'Neuralink' picks 'Neuralink Corp.' over 'Neuralink Jun 2025 …').
    for predicate, order in (
        ("lower(a.value #>> '{}') = lower($1)",
         "(SELECT count(*) FROM current_assertions x WHERE x.object_id=a.object_id) DESC"),
        ("a.value #>> '{}' ILIKE '%'||$1||'%'",
         "length(a.value #>> '{}') ASC"),
    ):
        row = await pool.fetchval(
            "SELECT a.object_id FROM current_assertions a "
            "JOIN objects o ON o.id=a.object_id AND o.status='active' "
            f"WHERE a.name='name' AND {predicate} ORDER BY {order} LIMIT 1",
            ref,
        )
        if row is not None:
            return uuid.UUID(str(row))
    return None


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
async def search(query: str, limit: int = 15) -> list[dict[str, Any]]:
    """Find objects by name substring → their id, type, canonical."""
    pool = await _pool_get()
    rows = await pool.fetch(
        "SELECT DISTINCT o.id, o.type, o.canonical, a.value #>> '{}' AS name "
        "FROM current_assertions a JOIN objects o ON o.id=a.object_id AND o.status='active' "
        "WHERE a.name='name' AND a.value #>> '{}' ILIKE '%'||$1||'%' LIMIT $2",
        query, limit,
    )
    return [{"id": str(r["id"]), "type": r["type"], "name": r["name"]} for r in rows]


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


# --- mount: link to the graph as a first-class fleet member ---

@mcp.tool()
async def mount(
    cwd: str, job_dir: str | None = None, model: str | None = None, ctx: Context | None = None
) -> dict[str, str]:
    """Link this agent to Osiris as a first-class fleet member — call it ONCE, first thing.
    Pass your working directory `cwd` (names your project) and, if you can, your `job_dir`
    ($CLAUDE_JOB_DIR) so the server can read your ACTUAL model off your transcript (not your
    system prompt, which lies after a model swap) — and so your identity SURVIVES server
    restarts (with a job_dir the server re-attaches you automatically; without one a bounce
    means re-mounting by hand). Registers an Agent object (works_in your project, acts_for
    the principal) and attributes every decision/thread you record to `agent:<you>` instead
    of the shared `session` bucket — so the graph knows WHICH instance, on WHICH model,
    decided what. Then call orient()."""
    pool = await _pool_get()
    settings = get_settings()
    lease = settings.osiris_mail_lease_secs
    ident = resolve_identity(cwd=cwd, job_dir=job_dir, model=model)
    await register_agent(Actions(pool), ident, actor=settings.osiris_actor,
                         expected_model=settings.osiris_expected_model)
    key = _conn_key(ctx)
    if key is not None:
        _prune_agents()  # opportunistic: mount is where churn shows up
        _agents[key] = ident
        _agents_touched[key] = time.monotonic()
    if job_dir:  # the durable half — what _ident_for re-attaches by after a bounce
        await mounts.save_mount(pool, job_dir=job_dir, agent_id=ident.agent_id,
                                project=ident.project, cwd=cwd, model=ident.model,
                                session_key=key)
    unread = await unread_count(pool, ident.project, lease_secs=lease) if ident.project else 0
    op_unread = await unread_count(pool, OPERATOR_ADDR, lease_secs=lease)
    banner = swap_banner(classify_swap(
        ident.model_history, ident.model, expected=settings.osiris_expected_model,
        anchored=ident.model_method == "job_dir"))  # only a true anchor confesses a swap
    out = {"agent": ident.agent_id, "project": ident.project or "?",
           "model": ident.model or "unknown",
           "mail": f"{unread} unread — call inbox()" if unread else "none",
           "note": "linked — writes now attributed to you; call orient() next"}
    if op_unread:  # the fleet plays secretary: any session the human drives can relay this
        out["operator_mail"] = (f"{op_unread} unread at the operator's desk — "
                                "inbox(project='operator') if the human is present")
    if banner:  # the graph confesses the swap the agent's own prompt hides (ruling f2ae6346)
        out["swap"] = banner
    return out


async def _project_briefing(pool: asyncpg.Pool, project: str) -> dict[str, Any] | None:
    """A working agent's SCOPED bearings — its OWN project's open threads + recent decisions,
    not the whole fleet's (decepticons surfaced that orient's flood costs more context than it
    saves). NOW A PURE COMPOSITION (#20): orient runs the `project-briefing` op-tree with the
    project as subject — the fleet briefing's selects intersected with the project's in_repo
    neighbourhood, recency-ordered. The bespoke SQL is gone; orient dogfoods the composer on
    its own need, and the view is forkable like any lens."""
    proj = await pool.fetchval(
        "SELECT id FROM objects WHERE type='SoftwareProject' AND canonical=$1", f"repo:{project}")
    if proj is None:
        return None
    res = await comp.run_composition(pool, "project-briefing", proj)
    items = res.get("items") if isinstance(res, dict) else None
    if not isinstance(items, dict):  # unseeded / error — never crash orient, just show empty
        return {"open_threads": [], "recent_decisions": []}
    return {
        "open_threads": [r for r in (items.get("open_threads") or []) if r.get("summary")],
        "recent_decisions": [r for r in (items.get("recent_decisions") or []) if r.get("summary")],
        "tensions": [r for r in (items.get("tensions") or []) if r.get("pole_a")],
    }


@mcp.tool()
async def orient(project: str | None = None, ctx: Context | None = None) -> dict[str, Any]:
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
    unread = await unread_count(pool, proj, lease_secs=lease) if proj else 0
    mail = f"{unread} unread — inbox()" if unread else "none"
    op_unread = await unread_count(pool, OPERATOR_ADDR, lease_secs=lease)
    op_mail = {"operator_mail": f"{op_unread} unread — inbox(project='operator') if the "
                                "human is present"} if op_unread else {}
    swap = swap_banner(classify_swap(ident.model_history, ident.model,
                       expected=get_settings().osiris_expected_model,
                       anchored=ident.model_method == "job_dir")) if ident else None
    scoped = await _project_briefing(pool, proj) if proj else None
    if scoped is not None:
        fleet_open = await pool.fetchval(
            "SELECT count(*) FROM objects o WHERE o.type='Thread' AND o.status='active' "
            "AND (SELECT s.value #>> '{}' FROM current_assertions s WHERE s.object_id=o.id "
            "  AND s.name='status' ORDER BY s.confidence DESC, s.observed_at DESC LIMIT 1)"
            "  = 'open'")
        return {
            "you": who, "model": (ident.model if ident else None), "project": proj,
            "mail": mail,
            **op_mail,
            **({"swap": swap} if swap else {}),
            **scoped,
            "fleet_open_threads_total": fleet_open,
            "note": f"scoped to {proj}; {fleet_open} fleet-wide open threads not shown "
                    "(run_composition('briefing') for the whole graph).",
        }
    return {
        "you": who, "model": (ident.model if ident else None), "project": proj,
        "mail": mail,
        **op_mail,
        **({"swap": swap} if swap else {}),
        "briefing": await comp.run_composition(pool, "briefing"),
    }


@mcp.tool()
async def fleet_digest(hours: int = 24) -> dict[str, Any]:
    """The MEMBRANE — the operator's window into the autonomous fleet over the last `hours`
    (default 24). Read-only, stateless rolling window. The return path made visible: results and
    accountability flowing back UP. Surfaces ROSTER + health (which identities resolved cleanly),
    ACTIVITY (what agents decided/opened in your name — not the miner's backfill), the DANGER map
    (model swaps — the harness's silent demotions), and LAUNDERING (credence flags where a relay
    carried a fact above its origin grade). Glance here to see what the fleet did while you were
    away — especially after onboarding a batch of agents."""
    pool = await _pool_get()
    since = datetime.now(UTC) - timedelta(hours=hours)
    return {"window_hours": hours,
            **await digest.fleet_digest(Actions(pool), since=since,
                                        lease_secs=get_settings().osiris_mail_lease_secs)}


@mcp.tool()
async def fleet() -> dict[str, Any]:
    """The roster AS A TREE — mounted roots and the sub-agent swarms they spawned (spawned_by
    = delegation), each carrying its OWN model and LIFECYCLE (● live vs ○ historical, from its
    last sign of life). A sub-agent inherits the parent's job_dir, so it can't self-register (it
    would collapse into the parent); the miner reconstructs the tree from disk. 'A man and all
    his imaginary friends' — now the friends' friends too, fractally. `tree` is the glanceable
    render; `registered` the flat rows (depth + parent + last_active)."""
    pool = await _pool_get()
    rows = await pool.fetch(
        "SELECT o.canonical, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='source_model') AS model, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='project') AS project, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='spawn_depth') AS depth, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='last_active') AS last_active, "
        " (SELECT p.canonical FROM links l JOIN objects p ON p.id=l.to_id "
        "  WHERE l.from_id=o.id AND l.type='spawned_by' LIMIT 1) AS parent "
        "FROM objects o WHERE o.type='Agent' AND o.status='active' ORDER BY o.canonical"
    )
    now = datetime.now(UTC)

    def _live(last_active: str | None) -> bool:
        # live = a sign of life within the last 15 min (its transcript was written), else historical
        if not last_active:
            return False
        try:
            return now - datetime.fromisoformat(last_active) < timedelta(minutes=15)
        except ValueError:
            return False

    nodes: dict[str, dict[str, Any]] = {str(r["canonical"]): dict(r) for r in rows}
    for n in nodes.values():
        n["live"] = _live(n["last_active"])
    children: dict[str | None, list[str]] = {}
    for canon, n in nodes.items():
        children.setdefault(n["parent"], []).append(canon)
    # a root = a mounted agent (no spawned_by), or one whose parent isn't itself an Agent node
    roots = sorted(c for c, n in nodes.items() if not n["parent"] or n["parent"] not in nodes)
    lines: list[str] = []

    def render(canon: str, indent: int, seen: set[str]) -> None:
        if canon in seen:  # guard against any accidental cycle
            return
        seen.add(canon)
        n = nodes[canon]
        prefix = "    " * indent + ("└─ " if indent else "")
        mark = "●" if n["live"] else "○"   # live vs historical, at a glance
        lines.append(
            f"{prefix}{mark} {canon}  {n['model'] or '?'}  {n['project'] or ''}".rstrip())
        for ch in sorted(children.get(canon, [])):
            render(ch, indent + 1, seen)

    seen: set[str] = set()
    for r in roots:
        render(r, 0, seen)
    return {
        "connected_now": len(_agents),
        "count": len(nodes),
        "live": sum(1 for n in nodes.values() if n["live"]),
        "swarm": sum(1 for n in nodes.values() if n["parent"]),
        "tree": "\n".join(lines),
        "registered": [
            {"agent": c, "model": n["model"], "project": n["project"],
             "depth": int(n["depth"]) if n["depth"] else 0, "parent": n["parent"],
             "live": n["live"], "last_active": n["last_active"]}
            for c, n in nodes.items()
        ],
    }


@mcp.tool()
async def send(body: str, to: str | None = None, reply_to: int | None = None,
               ctx: Context | None = None) -> dict[str, Any]:
    """Message another agent (the fleet mailbox). `to` = the recipient PROJECT/repo name —
    stable across their session changes, addressing whoever works there; `to='operator'`
    reaches the HUMAN's desk (report findings up when a lateral exchange concludes).
    `reply_to=<message id>` answers a message: it auto-routes back to the asker's project
    (no `to` needed), joins the thread, and SETTLES the message you're answering. You must
    be mounted; the message is stamped from YOU. Delivery is at-least-once and deduped, and
    the result tells you what awaits it: `listener` (is anyone live there), `wake` (will the
    trigger spawn them), `backlog` (deliverable queue). For DURABLE knowledge use
    record_decision / open_thread — this lane is disposable coordination, not memory."""
    ident = await _ident_for(ctx)
    if ident is None:
        return {"error": "mount(cwd, job_dir=$CLAUDE_JOB_DIR) first — a message must say who "
                         "it's from (with job_dir you re-attach automatically after a bounce)"}
    pool = await _pool_get()
    st = get_settings()
    try:
        res = await send_message(pool, from_agent=ident.agent_id, from_project=ident.project,
                                 to_project=to, body=body, reply_to=reply_to)
    except ValueError as e:
        return {"error": str(e)}
    dest = res["to"]
    last_seen = await mounts.project_last_seen(pool, dest)
    live = bool(last_seen and datetime.now(UTC) - datetime.fromisoformat(last_seen)
                < timedelta(minutes=15))
    return {
        "sent": res["id"], "to": dest, "from": ident.agent_id,
        **({"thread": res["thread_id"]} if res["thread_id"] is not None else {}),
        **({"dedup": "identical recent message already queued — not re-posted"}
           if res["dedup"] else {}),
        "listener": {"live": live, "last_seen": last_seen},
        "wake": await wake_status(pool, dest, st),
        "backlog": await unread_count(pool, dest, lease_secs=st.osiris_mail_lease_secs),
    }


@mcp.tool()
async def inbox(project: str | None = None, peek: bool = False,
                ack: list[int] | None = None, ctx: Context | None = None) -> dict[str, Any]:
    """Read messages other agents left for you (the fleet mailbox). Defaults to YOUR mounted
    project; pass `project` to read another's (project='operator' reads the human's desk).
    Reading LEASES a message, it does NOT consume it: SETTLE each one you've handled — reply
    with send(reply_to=<its id>) or pass ack=[ids] here — or it will REDELIVER after the
    lease (at-least-once: a dropped response costs a duplicate, never a silent loss).
    peek=True reads without leasing. Check this when you mount and after any compaction —
    mount()/orient() report your deliverable count."""
    ident = await _ident_for(ctx)
    proj = project or (ident.project if ident else None)
    if proj is None:
        return {"error": "mount(cwd, job_dir=$CLAUDE_JOB_DIR) first, or pass project=<repo>"}
    pool = await _pool_get()
    st = get_settings()
    settled = await ack_messages(pool, proj, ack) if ack else 0
    msgs = await read_inbox(pool, proj, mark_read=not peek,
                            lease_secs=st.osiris_mail_lease_secs)
    if peek:
        note = "peek — nothing leased"
    elif msgs:
        note = ("leased — settle each by replying (send(reply_to=<id>)) or acking "
                f"(inbox(ack=[ids])); unsettled mail redelivers after "
                f"{st.osiris_mail_lease_secs // 60} min")
    else:
        note = "empty"
    return {"project": proj.removeprefix("repo:").strip(), "messages": msgs,
            **({"settled": settled} if settled else {}), "note": note}


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
    repo: str | None = None, ctx: Context | None = None,
) -> dict[str, str]:
    """Write back a DECISION you made this session — a ruling, an architecture pivot, a
    deliberate rejection — so the WHY becomes durable graph memory the next session inherits
    (don't leave it to be regex-mined out of some future commit; the epochal ones never land
    in a commit at all). `kind`: ruling|reset|override|rejection|choice|decision. `rationale`
    = the reasoning; `repo` = a SoftwareProject name to file it under. Renders in the
    `decision-log` composition beside mined decisions, graded SELF_DECLARED (higher trust).
    Attributed to you if you mount()ed. Idempotent on the summary."""
    d = await capture.record_decision(
        Actions(await _pool_get()), summary, kind=kind, rationale=rationale, repo=repo,
        source=await _source_for(ctx),
    )
    return {"id": str(d), "kind": kind, "summary": summary}


@mcp.tool()
async def open_thread(
    summary: str, repo: str | None = None, kind: str | None = None,
    ctx: Context | None = None,
) -> dict[str, str]:
    """Open a THREAD — an unresolved question or next-step you want the next session to pick
    up. Surfaces in run_composition('briefing') under open threads, beside mined ones. `repo`
    files it under a SoftwareProject. Idempotent on the summary. This is how a session hands
    off its loose ends instead of losing them. `kind='obligation'` marks a DUTY minted by an
    action ('kernel changed → daemons need restart') — record those the moment they're minted;
    they are neither rulings nor commits and otherwise die with the context window."""
    t = await capture.open_thread(
        Actions(await _pool_get()), summary, repo=repo, kind=kind,
        source=await _source_for(ctx)
    )
    return {"id": str(t), "summary": summary, "status": "open"}


@mcp.tool()
async def resolve_thread(
    ref: str, because: str | None = None, ctx: Context | None = None
) -> dict[str, str]:
    """Close a THREAD you (or an earlier session) resolved — `ref` is its UUID or a summary
    substring; `because` records why. It leaves briefing's open list and joins the resolved
    section. Event-sourced (never deleted), so the close is auditable and reversible."""
    tid = await capture.resolve_thread(
        Actions(await _pool_get()), ref, because=because, source=await _source_for(ctx)
    )
    if tid is None:
        return {"error": f"no open thread matches {ref!r}"}
    return {"id": str(tid), "status": "resolved"}


@mcp.tool()
async def hold_tension(
    pole_a: str, pole_b: str, lean: str | None = None, why: str | None = None,
    repo: str | None = None, ctx: Context | None = None,
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
        source=ident.agent_id if ident else "session",
    )
    return {"held": str(t), "poles": [pole_a, pole_b], "lean": lean}


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

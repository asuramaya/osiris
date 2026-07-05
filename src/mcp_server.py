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

import uuid
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
from src.orchestrator import capture
from src.orchestrator import compositions as comp
from src.orchestrator.agents import AgentIdentity, register_agent, resolve_identity
from src.orchestrator.console import get_console as _get_console
from src.orchestrator.console import set_console as _set_console
from src.orchestrator.dossier import entity_dossier
from src.orchestrator.mailbox import read_inbox, send_message, unread_count
from src.orchestrator.sources import as_dicts, suggest

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
        "push: you perceive mail only when you check, so glance when you arrive). "
        "IDENTITY CHECK: the operator's standing choice is that sessions here run Fable 5. "
        "Harness degradations silently swap the model mid-session; if your environment "
        "says you are a different model, SAY SO to the operator in your first reply — "
        "a rug-pull must be confessed, never inherited blind."
    ),
)
_pool: asyncpg.Pool | None = None


# The fleet registry: each connected agent's identity, keyed by its connection. On the
# shared server every agent writes through ONE process, so without this their writes
# collapse into the single `session` source. `mount` populates this; the capture tools
# read it so each write is attributed to `agent:<session>`. In-memory (rebuilt on mount
# after a restart); the Agent OBJECTS live durably in the graph.
_agents: dict[int, AgentIdentity] = {}


def _conn_key(ctx: Context | None) -> int | None:
    """A stable per-connection key: the id() of the persistent ServerSession object (one
    per client connection for the connection's lifetime). None under stdio / no context."""
    try:
        return id(ctx.request_context.session) if ctx is not None else None
    except (AttributeError, LookupError):
        return None


def _source_for(ctx: Context | None) -> str:
    """The attributing actor for a write: the mounted agent on this connection, else the
    lone-operator `session` (back-compat — an un-mounted agent still writes, just coarsely)."""
    key = _conn_key(ctx)
    ident = _agents.get(key) if key is not None else None
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
async def consult_canon(query: str = "") -> dict[str, Any]:
    """Consult the design canon — Palantir's Object Set / Ontology / Action models + Notion's
    databases / relations-rollups / UI-UX, plus Osiris's own docs, all ingested as References.
    'Cite, don't re-derive': given a topic, a module path, or a design word, returns the
    matching canon SECTIONS (the source + the module each grounds). Call this BEFORE
    re-deriving a problem these companies already solved — the closed op set & "no generic
    join", aggregation caps, the kinetic write path, the renderer's view rules, calm UI.
    Empty query → the whole canon index."""
    pool = await _pool_get()
    spec = {"op": "function", "name": "canon", "args": {"q": query}}
    return await comp.run_spec(pool, spec, None, name="design-canon")


# --- mount: link to the graph as a first-class fleet member ---

@mcp.tool()
async def mount(
    cwd: str, job_dir: str | None = None, model: str | None = None, ctx: Context | None = None
) -> dict[str, str]:
    """Link this agent to Osiris as a first-class fleet member — call it ONCE, first thing.
    Pass your working directory `cwd` (names your project) and, if you can, your `job_dir`
    ($CLAUDE_JOB_DIR) so the server can read your ACTUAL model off your transcript (not your
    system prompt, which lies after a model swap). Registers an Agent object (works_in your
    project, acts_for the principal) and, for the rest of this connection, attributes every
    decision/thread you record to `agent:<you>` instead of the shared `session` bucket — so
    the graph knows WHICH instance, on WHICH model, decided what. Then call orient()."""
    pool = await _pool_get()
    ident = resolve_identity(cwd=cwd, job_dir=job_dir, model=model)
    await register_agent(Actions(pool), ident, actor=get_settings().osiris_actor)
    key = _conn_key(ctx)
    if key is not None:
        _agents[key] = ident
    unread = await unread_count(pool, ident.project) if ident.project else 0
    return {"agent": ident.agent_id, "project": ident.project or "?",
            "model": ident.model or "unknown",
            "mail": f"{unread} unread — call inbox()" if unread else "none",
            "note": "linked — writes now attributed to you; call orient() next"}


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
    }


@mcp.tool()
async def orient(project: str | None = None, ctx: Context | None = None) -> dict[str, Any]:
    """Get your bearings — the mount ritual as one call. If you're mounted (or pass a
    `project`), returns a SCOPED briefing: YOUR project's open threads + recent decisions,
    plus a count of fleet-wide threads not shown. Un-mounted with no project falls back to
    the whole-fleet briefing. Call after mount(), and again after any compaction, to inherit
    instead of starting blind."""
    pool = await _pool_get()
    key = _conn_key(ctx)
    ident = _agents.get(key) if key is not None else None
    proj = ident.project if ident else project
    who = ident.agent_id if ident else "session (un-mounted — call mount(cwd) first)"
    unread = await unread_count(pool, proj) if proj else 0
    mail = f"{unread} unread — inbox()" if unread else "none"
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
            **scoped,
            "fleet_open_threads_total": fleet_open,
            "note": f"scoped to {proj}; {fleet_open} fleet-wide open threads not shown "
                    "(run_composition('briefing') for the whole graph).",
        }
    return {
        "you": who, "model": (ident.model if ident else None), "project": proj,
        "mail": mail,
        "briefing": await comp.run_composition(pool, "briefing"),
    }


@mcp.tool()
async def fleet() -> dict[str, Any]:
    """The roster — every agent registered in the shared graph, its model + project, most
    recent first, plus how many are connected right now. This is the fleet made visible:
    'a man and all his imaginary friends.' Use it to see who else is working where."""
    pool = await _pool_get()
    rows = await pool.fetch(
        "SELECT o.canonical, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='source_model') AS model, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='project') AS project "
        "FROM objects o WHERE o.type='Agent' AND o.status='active' ORDER BY o.id DESC"
    )
    return {
        "connected_now": len(_agents),
        "registered": [
            {"agent": r["canonical"], "model": r["model"], "project": r["project"]}
            for r in rows
        ],
    }


@mcp.tool()
async def send(to: str, body: str, ctx: Context | None = None) -> dict[str, Any]:
    """Message another agent (the fleet mailbox). `to` = the recipient PROJECT/repo name —
    stable across their session changes, addressing whoever works there. You must be mounted;
    the message is stamped from YOU. PULL, not push: they read it on their next mount/orient
    (Osiris never interrupts a live agent). For DURABLE knowledge use record_decision /
    open_thread — this lane is disposable coordination, not memory."""
    key = _conn_key(ctx)
    ident = _agents.get(key) if key is not None else None
    if ident is None:
        return {"error": "mount(cwd) first — a message must say who it's from"}
    mid = await send_message(await _pool_get(), from_agent=ident.agent_id,
                             from_project=ident.project, to_project=to, body=body)
    return {"sent": mid, "to": to.removeprefix("repo:").strip(), "from": ident.agent_id}


@mcp.tool()
async def inbox(project: str | None = None, peek: bool = False,
                ctx: Context | None = None) -> dict[str, Any]:
    """Read messages other agents left for you (the fleet mailbox). Defaults to YOUR mounted
    project; pass `project` to read another's. Reading MARKS them read (mailbox semantics)
    unless peek=True. Check this when you mount and after any compaction — mount()/orient()
    report your unread count."""
    key = _conn_key(ctx)
    ident = _agents.get(key) if key is not None else None
    proj = project or (ident.project if ident else None)
    if proj is None:
        return {"error": "mount(cwd) first, or pass project=<repo>"}
    msgs = await read_inbox(await _pool_get(), proj, mark_read=not peek)
    where = "peek — left unread" if peek else ("marked read" if msgs else "empty")
    return {"project": proj.removeprefix("repo:").strip(), "unread": msgs, "note": where}


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
        source=_source_for(ctx),
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
        Actions(await _pool_get()), summary, repo=repo, kind=kind, source=_source_for(ctx)
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
        Actions(await _pool_get()), ref, because=because, source=_source_for(ctx)
    )
    if tid is None:
        return {"error": f"no open thread matches {ref!r}"}
    return {"id": str(tid), "status": "resolved"}


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

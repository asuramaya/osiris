"""/desk /mail /fleet — the chrome OPENED: clickable, self-refreshing lenses (operator
direction, 2026-07-11: "make it clickable and openable... without having to call the agent,
closer to real time").

Same constitution as /membrane: a read-only LENS, no framework, no state, no writes — the
console can crash without touching the truth. What's new is the SHAPE: each surface is its
own page, long bodies open natively (<details> — the browser is the click handler), and a
~15-line poller re-fetches the page body (?partial=1) every 4 seconds, restoring whichever
cards the operator had open. Near-real-time without websockets: the graph is on localhost,
a 4s poll of one bounded query costs nothing and survives everything.

Renderers are PURE (fixture-fed in tests); the routes in app.py feed them the live graph.
"""
from __future__ import annotations

import html
import re
from typing import Any

import asyncpg

from src.orchestrator.agents import seat_label

_CSS = """
body{background:#0d1117;color:#c9d1d9;font:14px/1.5 ui-monospace,monospace;margin:2rem auto;
     max-width:72rem;padding:0 1rem}
h1{font-size:1.1rem;letter-spacing:.2em;text-transform:uppercase;color:#8b949e}
h2{font-size:.9rem;letter-spacing:.15em;text-transform:uppercase;color:#8b949e;
   border-bottom:1px solid #21262d;padding-bottom:.3rem;margin-top:2.2rem}
table{border-collapse:collapse;width:100%;margin:.6rem 0}
td,th{text-align:left;padding:.25rem .6rem .25rem 0;vertical-align:top}
th{color:#484f58;font-weight:normal}
tr{border-bottom:1px solid #161b22}
.dim{color:#484f58}.red{color:#f85149}.green{color:#3fb950}.amber{color:#d29922}
.strip{display:flex;gap:1.6rem;margin:1rem 0;flex-wrap:wrap}
.strip a{color:#c9d1d9;text-decoration:none;border-bottom:1px dotted #30363d}
.strip b{font-weight:600}
.strip.ambient{border-top:1px solid #21262d;padding-top:.7rem;margin-top:0}
.strip-label{font-size:.75rem;letter-spacing:.15em;text-transform:uppercase;color:#484f58;
             margin:1rem 0 -.4rem}
a{color:#58a6ff;text-decoration:none}
.body{white-space:pre-wrap;color:#8b949e;max-width:60rem}
"""


def _e(v: Any) -> str:
    return html.escape(str(v if v is not None else ""))


def _age(secs: float | None) -> str:
    """A coarse human age for the watermark's own freshness ('3h', '2d', 'just now')."""
    if secs is None:
        return "unknown"
    secs = int(secs)
    if secs < 90:
        return "just now"
    if secs < 5400:
        return f"{round(secs / 60)}m"
    if secs < 172800:
        return f"{round(secs / 3600)}h"
    return f"{round(secs / 86400)}d"


_CHROME_CSS = _CSS + """
.nav{display:flex;gap:1.4rem;margin:.4rem 0 1.4rem;flex-wrap:wrap}
.nav a{color:#8b949e;text-decoration:none;letter-spacing:.15em;text-transform:uppercase;
       font-size:.85rem;border-bottom:2px solid transparent;padding-bottom:.2rem}
.nav a.on{color:#c9d1d9;border-bottom-color:#58a6ff}
.card{border:1px solid #21262d;border-radius:6px;padding:.6rem .9rem;margin:.5rem 0;
      background:#11161d}
.card summary{cursor:pointer;list-style:none;display:flex;gap:.8rem;align-items:baseline}
.card summary::before{content:"▸";color:#484f58;flex:none}
.card details[open] summary::before,.card[open] summary::before{content:"▾"}
.band{margin-top:1.6rem}
.hdr-decision{color:#f85149}.hdr-hands{color:#d29922}.hdr-fyi{color:#8b949e}
.who{color:#58a6ff}.ts{color:#484f58;font-size:.85rem}
.fold{color:#484f58;font-size:.85rem;margin:.2rem 0 0 1.2rem}
.queue li{margin:.25rem 0}
.pill{border:1px solid #30363d;border-radius:10px;padding:0 .5rem;color:#8b949e;
      font-size:.8rem}
.live{color:#3fb950}
.counts{display:flex;gap:1rem;align-items:center;margin:.2rem 0 1rem;flex-wrap:wrap}
.owe{border:1px solid #f85149;border-radius:6px;padding:.35rem .8rem;color:#f85149;
     letter-spacing:.1em;font-size:.85rem}
.owe b{font-size:1.3rem;color:#ff7b72}
.owe.clear{border-color:#3fb950;color:#3fb950}.owe.clear b{color:#3fb950}
.lett{border:1px solid #30363d;border-radius:6px;padding:.35rem .8rem;color:#8b949e;
      font-size:.85rem}
.proj{border:1px solid #21262d;border-left:3px solid #30363d;border-radius:6px;
      padding:.6rem .9rem;margin:.5rem 0;background:#11161d}
.proj[open]{border-left-color:#58a6ff}
.proj.crit{border-left-color:#f85149}
.proj summary{cursor:pointer;list-style:none;display:flex;gap:.7rem;align-items:baseline}
.proj summary::before{content:"▸";color:#484f58;flex:none}
.proj[open] summary::before{content:"▾"}
.roster{border-collapse:collapse;width:100%;max-width:44rem;margin-top:.4rem}
.roster td{padding:.5rem .8rem;border-top:1px solid #21262d}
.roster tr:hover td{background:#11161d}
.roster tr.crit td:first-child{border-left:3px solid #f85149;padding-left:.5rem}
.roster a{color:#58a6ff;text-decoration:none;font-size:1.05rem}
.roster a:hover{text-decoration:underline}
.roster .num{text-align:right;color:#c9d1d9;white-space:nowrap}
.roster .ts{text-align:right;white-space:nowrap}
.debt{margin:.5rem 0;padding:.4rem 0;border-top:1px solid #1b2027}
.debt code{color:#8b949e;font-size:.8rem}
.verbs{display:inline-flex;gap:.35rem;margin-left:.5rem}
button[data-act]{background:#161b22;border:1px solid #30363d;color:#8b949e;cursor:pointer;
      border-radius:5px;padding:.1rem .55rem;font:inherit;font-size:.78rem}
button[data-act]:hover{border-color:#58a6ff;color:#c9d1d9}
button[data-verb="resolve"]:hover{border-color:#3fb950;color:#3fb950}
button[data-verb="assign"]:hover{border-color:#d29922;color:#d29922}
button[data-act="settle"]:hover,button[data-act="settle-all"]:hover{
      border-color:#3fb950;color:#3fb950}
button[disabled]{opacity:.4;cursor:default}
"""

# THE DESK GREW HANDS (operator, 2026-07-11: "i sit on a scary red 11 desk without a good way
# to resolve these debts per thread or per project, so it snowballs into infinity"). One
# delegated handler; every button is the OPERATOR'S OWN CLICK posting through the Actions
# waist (ruling 923c380f — reads stay free, only explicit acts write, signed analyst:operator).
# After the write it just calls tick(): the 4s poller re-renders from the graph, so the page
# never holds state and can never disagree with the record.
_ACTIONS = """<script>
document.addEventListener('click',async e=>{
  const b=e.target.closest('button[data-act],button[data-action]');if(!b)return;
  e.preventDefault();e.stopPropagation();
  const J={'content-type':'application/json'};
  b.disabled=true;const was=b.textContent;b.textContent='…';
  try{
    if(b.dataset.action){
      // the generic action-binding path (ruling c5b184cd, thread d56e7073/#44) — the row's
      // OWN args, resolved server-side by row_action, echoed back verbatim; this handler
      // never constructs args itself.
      await fetch('/act',{method:'POST',headers:J,body:JSON.stringify(
        {action:b.dataset.action,args:JSON.parse(b.dataset.args||'{}')})});
    }else if(b.dataset.act==='settle'){
      await fetch('/desk/settle',{method:'POST',headers:J,body:JSON.stringify(
        {ids:b.dataset.ids.split(',').map(Number)})});
    }else{
      const p={ids:b.dataset.id.split(','),verb:b.dataset.verb,
               because:b.dataset.because||'operator triage from the desk'};
      if(b.dataset.owner)p.owner=b.dataset.owner;
      if(b.dataset.days)p.days=+b.dataset.days;
      await fetch('/threads/triage',{method:'POST',headers:J,body:JSON.stringify(p)});
    }
  }catch(err){b.textContent=was;b.disabled=false;return}
  tick();
});
</script>"""

# the poller: re-fetch this page's body every 4s (paused when the tab is hidden), swap it
# in, and RE-OPEN whatever cards the operator was reading — a refresh must never close the
# drawer in their hand.
_POLLER = """<script>
const c=document.getElementById('c');
async function tick(){
  if(document.hidden)return;
  try{
    const u=new URL(location);u.searchParams.set('partial','1');
    const r=await fetch(u);
    if(!r.ok)return;
    const open=[...c.querySelectorAll('details[open]')].map(d=>d.id);
    c.innerHTML=await r.text();
    open.forEach(i=>{const d=document.getElementById(i);if(d)d.open=true});
    const t=document.getElementById('ts');if(t)t.textContent=new Date().toLocaleTimeString();
  }catch(e){}
}
setInterval(tick,4000);
</script>"""

# "docs" (/canon) RETIRED 2026-07-30, task #96 — the route was a pure pass-through to the
# "docs" composition, which lives in /ui. render_composition and its whole _comp_* chain
# retired with it (task #96, second cut) — a second generic composition renderer in
# Python, duplicating osiris.js client-side; no capability lost, a duplicate died.
_TABS = (("inbox", "/"), ("desk", "/desk"), ("mail", "/mail"), ("fleet", "/fleet"),
         ("overhead", "/overhead"))


def page(title: str, active: str, inner: str, *, actions: bool = False) -> str:
    """The shell: nav + the poll-swapped content div. Everything inside #c must render
    identically when served as ?partial=1 — the poller depends on it.

    `actions=True` arms the click handler (the desk only). The label tells the truth about
    which it is: a page that can write must never present itself as a read-only lens."""
    nav = "".join(
        f'<a href="{href}" class="{"on" if name == active else ""}">{name}</a>'
        for name, href in _TABS)
    mode = "your clicks write (signed operator)" if actions else "read-only"
    return (
        '<!doctype html><meta charset="utf-8">'
        f"<title>osiris {_e(title)}</title><style>{_CHROME_CSS}</style>"
        f'<h1>◈ {_e(title)}</h1><div class="nav">{nav}'
        f'<span class="ts">updated <span id="ts">just now</span> · refreshes 4s · '
        f"{mode}</span></div>"
        f'<div id="c">{inner}</div>{_POLLER}{_ACTIONS if actions else ""}'
    )


def _card(dom_id: str, head: str, body: str, sub: str = "") -> str:
    """One clickable card: closed = the head line; open = the full body. Stable dom ids let
    the poller re-open what the operator was reading."""
    return (
        f'<details class="card" id="{_e(dom_id)}"><summary>{head}</summary>'
        f'<div class="body">{_e(body)}</div>{sub}</details>'
    )


# ── /desk ────────────────────────────────────────────────────────────────────────────────

# The bands are no longer the desk's TOP-LEVEL shape (they still are in read_desk, and still
# are for AGENTS reading via inbox — a mind wants "what must the human decide"). The HUMAN's
# page groups by project instead: decision+hands become a project's `asks`, fyi becomes the
# letters bin. Same record, two lenses.


def _brief_card(m: dict[str, Any]) -> str:
    head = (f'<span class="who">{_e(m.get("from_project") or "?")}</span>'
            f'<span class="dim">{_e(m.get("from"))}</span>'
            f'<span class="ts">{_e((m.get("when") or "")[:16])}</span>'
            f'<span>{_e((m.get("body") or "")[:110])}</span>')
    subs = []
    ss = m.get("same_story")
    if ss:
        also = ", ".join(f'{_e(a["project"])} ({a["id"]})' for a in ss["also"])
        subs.append(f'<div class="fold">×{ss["count"]} same story — also: {also}</div>')
    tf = m.get("thread_folded")
    if tf:
        subs.append(f'<div class="fold">supersedes {tf["count"]} earlier in this thread '
                    f'({", ".join(str(i) for i in tf["ids"])})</div>')
    return _card(f'm{m["id"]}', head, m.get("body") or "", "".join(subs))


def _verbs(t: dict[str, Any], project: str) -> str:
    """The FOUR DOORS off a debt. Before these, a thread on the operator's desk had two exits
    — he did it, or it rotted — which is why his desk snowballed. `not mine` is the one that
    was missing: it hands the duty back to the project that owes it (owner=<project>), where
    orient() puts it on THAT project's wall at its next mount. No dispatcher; the graph is
    the dispatcher."""
    tid = _e(t["id"])
    hand_to = _e(project) if project and project != "—" else ""
    back = (f'<button data-act="triage" data-verb="assign" data-id="{tid}" '
            f'data-owner="{hand_to}" data-because="operator: not mine — {hand_to} owns this" '
            f'title="hand it back to {hand_to} — stays open, stops being yours"'
            f'>not mine</button>' if hand_to else "")
    return (
        f'<span class="verbs">'
        f'<button data-act="triage" data-verb="resolve" data-id="{tid}" '
        f'data-because="operator: done" title="close it — done">done</button>'
        f'{back}'
        f'<button data-act="triage" data-verb="defer" data-id="{tid}" data-days="30" '
        f'data-because="operator: not now" title="mine, but not now — back in 30 days"'
        f'>later</button>'
        f"</span>")


def _settle(ids: list[Any], label: str = "settle") -> str:
    return (f'<button data-act="settle" data-ids="{",".join(str(i) for i in ids)}">'
            f"{_e(label)}</button>")


def _counts(desk: dict[str, Any]) -> str:
    owed, letters = desk.get("owed", 0), desk.get("letters", 0)
    lett_ids = [m["id"] for m in (desk.get("fyi") or [])]
    return (
        f'<div class="counts"><span class="owe{" clear" if not owed else ""}">'
        f"YOU OWE <b>{owed}</b></span>"
        f'<span class="lett">letters <b>{letters}</b> '
        f'<span class="dim">— no debt attached</span> '
        + (_settle(lett_ids, f"clear all {letters}") if lett_ids else "")
        + "</span></div>")


def render_desk(desk: dict[str, Any]) -> str:
    """THE ROSTER (operator, 2026-07-11: "the desk is better off as a per-project thing, like
    the mail. the overwhelming kill here is that i get flooded with my entire fleet worth of
    backlog on one tab").

    So the desk LANDS ON COUNTS, never on contents: one line per project — what you owe it,
    who asked, how long it has been rotting. You pick ONE and walk in. The whole fleet's
    backlog on a single scroll was itself the thing that made the desk unusable; a landing
    page that shows everything shows nothing."""
    out: list[str] = [_counts(desk)]
    projects = desk.get("by_project") or []
    if projects:
        out.append('<div class="band"><h2>your desk '
                   '<span class="dim">(one project at a sitting — click to walk in)</span>'
                   "</h2><table class=\"roster\">")
        for p in projects:
            debts, asks = p.get("debts") or [], p.get("asks") or []
            name = p["project"]
            out.append(
                f'<tr class="{"crit" if p.get("critical") else ""}">'
                f'<td><a href="/desk?p={_e(name)}">{_e(name)}</a>'
                + (' <span class="hdr-decision">🚨</span>' if p.get("critical") else "")
                + f'</td><td class="num">{len(debts) or ""}'
                + ('<span class="dim"> owed</span>' if debts else "")
                + f'</td><td class="num">{len(asks) or ""}'
                + ('<span class="dim"> asked</span>' if asks else "")
                + f'</td><td class="ts">{_e(_age(p.get("oldest_secs")))}'
                  "</td></tr>")
        out.append("</table></div>")
    else:
        out.append('<p class="dim">desk clear — nothing owed, nobody waiting.</p>')
    out.append(_guesses_band(desk))
    out.append(_letters_band(desk))
    out.append(_dimmed_band(desk))
    return "".join(out)


def _guesses_band(desk: dict[str, Any]) -> str:
    """WHAT THE MINER THINKS YOU OWE — folded, grey, and never in the red number.

    An LLM read a conversation and inferred a duty for the human. Nobody asked him. Five of
    the six debts on this desk were exactly that, and two were provably false the moment anyone
    checked. They are kept — the miner really did overhear something, and some of these are
    real — but a guess that wears the same colour as a deliberate ask spends the only currency
    the red number has. So: shown, with the same four doors, so a true one can be acted on and
    a false one killed in a click."""
    guesses = (desk.get("miner_guesses") or {}).get("threads") or []
    if not guesses:
        return ""
    rows = [f'<div class="debt"><code>{_e(t["id"])}</code> '
            f'<span class="dim">{_e(t["project"])}</span> {_e(t["summary"])}'
            + _verbs(t, t["project"]) + "</div>" for t in guesses]
    return ('<details class="band" id="guesses"><summary class="hdr-fyi">the miner thinks you '
            f'may owe <span class="pill">{len(guesses)}</span> '
            '<span class="dim">(inferred from overheard talk — nobody asked you; '
            "not counted as debt)</span></summary>" + "".join(rows) + "</details>")


def render_desk_project(desk: dict[str, Any], project: str) -> str:
    """ONE PROJECT, walked into: its debts (each with the four doors) and the briefs that
    asked — together, because that is the unit of a sitting."""
    p = next((x for x in (desk.get("by_project") or []) if x["project"] == project), None)
    back = '<p><a href="/desk">← all projects</a></p>'
    if p is None:
        return (back + f'<p class="dim">nothing owed to <b>{_e(project)}</b> — '
                       "cleared, or never was.</p>")
    debts, asks = p.get("debts") or [], p.get("asks") or []
    out = [back, f'<div class="band"><h2><span class="who">{_e(project)}</span> '
                 + ('<span class="hdr-decision">🚨</span> ' if p.get("critical") else "")
                 + f'<span class="pill">{len(debts)} owed</span> '
                   f'<span class="pill">{len(asks)} asked</span></h2>']
    for t in debts:
        out.append(f'<div class="debt"><code>{_e(t["id"])}</code> {_e(t["summary"])}'
                   + (' <span class="pill">obligation</span>'
                      if t.get("kind") == "obligation" else "")
                   + _verbs(t, project) + "</div>")
    for m in asks:
        out.append(f'<div class="debt">{_brief_card(m)}{_settle([m["id"]])}</div>')
    out.append("</div>")
    return "".join(out)


def _letters_band(desk: dict[str, Any]) -> str:
    """Reports and eulogies. They owe nothing, so they never touch the roster — they sit
    FOLDED at the bottom and clear in one click."""
    letters = desk.get("fyi") or []
    if not letters:
        return ""
    rows = []
    for m in letters:
        ids = [m["id"], *[a["id"] for a in (m.get("same_story") or {}).get("also", [])],
               *(m.get("thread_folded") or {}).get("ids", [])]
        rows.append(f'<div class="debt">{_brief_card(m)}{_settle(ids)}</div>')
    return ('<details class="band" id="letters"><summary class="hdr-fyi">letters '
            f'<span class="pill">{len(letters)}</span> '
            '<span class="dim">(reports and eulogies — nothing owed)</span></summary>'
            + "".join(rows) + "</details>")


def _dimmed_band(desk: dict[str, Any]) -> str:
    """An agent judged these moot and said WHY — but a dim is an annotation, never a settle
    (the membrane). They stay, folded, with the human's own dismiss."""
    dimmed = desk.get("dimmed") or []
    if not dimmed:
        return ""
    rows = []
    for d in dimmed:
        head = (f'<span class="who">{_e(d.get("project") or "?")}</span>'
                f'<span class="dim">{_e(d.get("headline") or "")}</span>')
        rows.append('<div class="debt">'
                    + _card(f'dim{d["id"]}', head,
                            f'moot ({_e(d.get("by"))}): {d.get("moot") or ""}')
                    + _settle([d["id"]]) + "</div>")
    return ('<details class="band" id="dimmedband"><summary class="hdr-fyi">dimmed '
            f'<span class="pill">{len(dimmed)}</span> '
            '<span class="dim">(an agent called these moot — yours to dismiss)</span>'
            "</summary>" + _settle([d["id"] for d in dimmed], f"clear all {len(dimmed)}")
            + "".join(rows) + "</details>")


# ── /mail ────────────────────────────────────────────────────────────────────────────────

async def mail_overview(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Every mailbox with traffic, busiest-latest first. Unsettled = no intended recipient
    has settled it (the same notion the wake dispatch uses).

    LANES WEAR THEIR SOUL'S NAME (operator, 2026-07-17: 'more mailboxes than agents — who
    do they belong to?'): a '@agent:<hash>' lane is an ADDRESS, and a long-lived seat
    leaves one per generation. Each agent lane resolves through the living head and wears
    the seat's label; a SUPERSEDED lane with nothing unsettled is history and folds into
    one counted line (`historical` on the synthetic row). A superseded lane still holding
    unsettled mail stays visible and says so — that is a sweep waiting to happen, never
    something to hide. Rooms (project boxes) render exactly as before."""
    from src.orchestrator.agents import seat_label
    from src.orchestrator.folds import living_head

    rows = await pool.fetch(
        "SELECT COALESCE(m.to_project, '@' || m.to_agent) AS box, count(*) AS msgs, "
        " count(*) FILTER (WHERE m.read_at IS NULL AND NOT EXISTS "
        "   (SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
        "    AND r.read_at IS NOT NULL)) AS unsettled, "
        " max(m.created_at) AS last_at "
        "FROM fleet_messages m GROUP BY 1 ORDER BY max(m.created_at) DESC LIMIT 60")
    groups: dict[str, dict[str, Any]] = {}

    def group(name: str) -> dict[str, Any]:
        g = groups.get(name)
        if g is None:
            g = groups[name] = {"project": name, "room": None, "souls": [],
                                "last_at": None}
        return g

    souls: dict[str, dict[str, Any]] = {}
    for r in rows:
        d = dict(r)
        box = str(d["box"])
        head: str | None = None
        if box.startswith("@seat:"):
            holder = await pool.fetchval(
                "SELECT hf.canonical FROM links l "
                "JOIN objects hf ON hf.id=l.from_id JOIN objects ht ON ht.id=l.to_id "
                "WHERE l.type='holds' AND ht.canonical=$1 "
                "AND (l.valid_until IS NULL OR l.valid_until > now()) "
                "ORDER BY l.first_seen DESC NULLS LAST LIMIT 1", box[1:])
            if holder:
                head = await living_head(pool, str(holder))
        elif box.startswith("@agent:"):
            head = await living_head(pool, box[1:])
        if head is None:
            if box.startswith("@"):
                group("unhoused")["souls"].append(d)   # an address nobody claims
            else:
                group(box)["room"] = d                 # a project's group chat
            continue
        # ONE SOUL, ONE ROW, UNDER ITS HOUSE: every lane of a soul — each generation's
        # address, each seat it holds — merges into a single line under the living head,
        # nested under the soul's project (agent mailboxes belong to projects)
        s = souls.get(head)
        if s is None:
            srow = await pool.fetchrow(
                "SELECT max(a.value #>> '{}') FILTER (WHERE a.name='handle') AS handle, "
                "       max(a.value #>> '{}') FILTER (WHERE a.name='seat_generation') AS g, "
                "       max(a.value #>> '{}') FILTER (WHERE a.name='project') AS project "
                "FROM current_assertions a JOIN objects o ON o.id=a.object_id "
                "WHERE o.canonical=$1", head)
            handle = srow["handle"] if srow else None
            gen = (int(srow["g"]) if srow and srow["g"] and str(srow["g"]).isdigit()
                   else None)
            s = souls[head] = {
                "box": "@" + head, "msgs": 0, "unsettled": 0, "last_at": None,
                "soul": seat_label(head, handle, gen) if handle else None,
            }
            group(str(srow["project"]) if srow and srow["project"] else "unhoused")[
                "souls"].append(s)
        s["msgs"] += int(d["msgs"] or 0)
        s["unsettled"] += int(d["unsettled"] or 0)
        s["last_at"] = max((t for t in (s["last_at"], d["last_at"]) if t is not None),
                           default=None)
    for g in groups.values():
        stamps = [x["last_at"] for x in ([g["room"]] if g["room"] else []) + g["souls"]
                  if x and x["last_at"] is not None]
        g["last_at"] = max(stamps, default=None)
        g["souls"].sort(key=lambda s: (s["last_at"] is not None, s["last_at"]),
                        reverse=True)
    return sorted(groups.values(),
                  key=lambda g: (g["last_at"] is not None, g["last_at"]), reverse=True)


async def mail_threads(pool: asyncpg.Pool, box: str) -> list[dict[str, Any]]:
    """One mailbox's conversations, newest thread first, messages oldest-first within.
    A '@agent:...' box is the DM lane; a bare name is a project's group chat (both
    directions — the conversation, not just the inbound half)."""
    if box.startswith("@agent:"):
        # THE SOUL'S WHOLE CONVERSATION: every generation's address plus every seat the
        # lineage ever held — the overview folds lanes by soul, so its one row must open
        # onto everything that row counted
        from src.orchestrator.agents import _generation
        cond = ("((m.to_agent = $1 OR m.to_agent LIKE $1 || '-%') "
                " OR (m.from_agent = $1 OR m.from_agent LIKE $1 || '-%') "
                " OR m.to_agent IN (SELECT ht.canonical FROM links l "
                "     JOIN objects hf ON hf.id=l.from_id "
                "     JOIN objects ht ON ht.id=l.to_id "
                "     WHERE l.type='holds' AND ht.type='Seat' "
                "     AND (hf.canonical=$1 OR hf.canonical LIKE $1 || '-%')))")
        arg = _generation(box[1:])[0]
    elif box.startswith("@"):
        cond, arg = "(m.to_agent=$1 OR m.from_agent=$1)", box[1:]
    else:
        cond, arg = "(m.to_project=$1 OR m.from_project=$1)", box
    rows = await pool.fetch(
        f"SELECT m.id, m.from_agent, m.from_project, m.to_project, m.to_agent, m.body, "
        f" m.created_at, COALESCE(m.thread_id, m.id) AS thread, "
        f" (m.read_at IS NOT NULL OR EXISTS (SELECT 1 FROM message_recipients r "
        f"   WHERE r.message_id=m.id AND r.read_at IS NOT NULL)) AS settled "
        f"FROM fleet_messages m WHERE {cond} "
        f"ORDER BY m.created_at DESC LIMIT 300", arg)
    threads: dict[int, dict[str, Any]] = {}
    for r in rows:
        t = threads.setdefault(int(r["thread"]), {"thread": int(r["thread"]), "msgs": []})
        t["msgs"].append(dict(r))
    for t in threads.values():
        t["msgs"].sort(key=lambda m: m["created_at"])
        t["last_at"] = t["msgs"][-1]["created_at"]
        t["unsettled"] = sum(1 for m in t["msgs"] if not m["settled"])
        t["between"] = sorted({m["from_project"] or m["from_agent"] for m in t["msgs"]})
    return sorted(threads.values(), key=lambda t: t["last_at"], reverse=True)


def render_mail_overview(groups: list[dict[str, Any]]) -> str:
    """PROJECT → AGENT (operator, 2026-07-17: 'agent mailboxes belong to projects, so it
    nests nicely'): each project's group chat leads its block, the house's souls indent
    beneath it, and a room-less project (or a house-less soul) still gets its header."""
    def unsettled_cell(b: dict[str, Any]) -> str:
        if b["unsettled"]:
            return f'<span class="amber">{b["unsettled"]} unsettled</span>'
        return '<span class="dim">settled</span>'

    def cells(b: dict[str, Any]) -> str:
        return (f'<td>{b["msgs"]} msgs</td><td>{unsettled_cell(b)}</td>'
                f'<td class="ts">{_e(str(b["last_at"])[:16])}</td>')

    def soul_cell(b: dict[str, Any]) -> str:
        name = f"@{b['soul']}" if b.get("soul") else str(b["box"])
        cell = f'<a href="/mail?box={_e(str(b["box"]))}">{_e(name)}</a>'
        if b.get("soul"):
            cell += f' <span class="dim">{_e(str(b["box"])[1:])}</span>'
        return cell

    parts: list[str] = []
    for g in groups:
        room = g.get("room")
        if room:
            parts.append(
                f'<tr><td><a href="/mail?box={_e(str(room["box"]))}">'
                f'{_e(str(room["box"]))}</a></td>{cells(room)}</tr>')
        else:
            parts.append(f'<tr><td class="dim">{_e(str(g["project"]))}</td>'
                         "<td></td><td></td><td></td></tr>")
        for s in g["souls"]:
            parts.append(
                f'<tr><td style="padding-left:1.6em">{soul_cell(s)}</td>{cells(s)}</tr>')
    rows = "".join(parts) or '<tr><td class="dim">no mail anywhere</td></tr>'
    return f"<h2>mailboxes</h2><table>{rows}</table>"


def render_mail_box(box: str, threads: list[dict[str, Any]]) -> str:
    out = [f'<h2>{_e(box)} <a class="pill" href="/mail">all boxes</a></h2>']
    if not threads:
        out.append('<p class="dim">no mail</p>')
    for t in threads:
        head = (f'<span class="who">#{t["thread"]}</span>'
                f'<span class="dim">{_e(" ↔ ".join(t["between"]))}</span>'
                f'<span>{t["msgs"].__len__()} msgs'
                + (f' · <span class="amber">{t["unsettled"]} unsettled</span>'
                   if t["unsettled"] else "")
                + f'</span><span class="ts">{_e(str(t["last_at"])[:16])}</span>')
        body_html = "".join(
            f'<p><span class="who">{_e(m["from_agent"])}</span> '
            f'<span class="dim">({_e(m["from_project"] or "?")}'
            + (f' → {_e(m["to_agent"])}' if m["to_agent"] else "")
            + f')</span> <span class="ts">{_e(str(m["created_at"])[:16])}</span>'
            + ("" if m["settled"] else ' <span class="amber">unsettled</span>')
            + f'<div class="body">{_e(m["body"])}</div></p>'
            for m in t["msgs"])
        out.append(f'<details class="card" id="t{t["thread"]}"><summary>{head}</summary>'
                   f"{body_html}</details>")
    return "".join(out)


# ── /fleet ───────────────────────────────────────────────────────────────────────────────

def _row_rank(m: dict[str, Any]) -> tuple[int, float]:
    """Which of one agent's mount rows testifies for the CARD (the fold below): a row an
    agent's own MCP connection touched (sid:) outranks an untouched whisper row, which
    outranks a mere window (view-of:/resume-of: — a tab's alias of a session that lives
    elsewhere). Ties go to the fresher row. The alias is never the witness: it carries
    the model/cwd stamped at ITS birth, stale the moment the real session swaps."""
    key = m.get("session_key") or ""
    if key.startswith("sid:"):
        k = 0
    elif key.startswith(("view-of:", "resume-of:")):
        k = 2
    else:
        k = 1
    return (k, float(m["age_secs"] or 1e9))


async def fleet_data(pool: asyncpg.Pool, *, wake_budget: int = 0) -> dict[str, Any]:
    rows = [dict(r) for r in await pool.fetch(
        "SELECT m.agent_id, m.project, m.model, m.cwd, m.last_seen, m.session_key, "
        " m.job_dir, extract(epoch FROM (now() - m.last_seen)) AS age_secs, "
        " (SELECT a.value #>> '{}' FROM current_assertions a "
        "   JOIN objects o ON o.id=a.object_id "
        "   WHERE o.canonical=m.agent_id AND a.name='handle' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS handle, "
        " (SELECT a.value #>> '{}' FROM current_assertions a "
        "   JOIN objects o ON o.id=a.object_id "
        "   WHERE o.canonical=m.agent_id AND a.name='seat_generation' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS seat_gen, "
        # SEATED (operator, 2026-07-17: 'fleet 5 but there are only 3 agents up') — the
        # visitor gate's own discriminator, read not enforced: a row whose agent id is just
        # the session id echoed back, with no active object behind it, is a stranger's door
        # (a bg-pty host, a spare, a passing whisper) and must never count as fleet.
        " (substring(m.agent_id from 7 for 8) IS DISTINCT FROM "
        "    substring(split_part(coalesce(m.session_key,''), ':', 2) from 1 for 8) "
        "  OR EXISTS (SELECT 1 FROM objects o WHERE o.canonical=m.agent_id "
        "       AND o.status='active')) AS seated "
        "FROM agent_mounts m ORDER BY m.last_seen DESC LIMIT 240")]
    for m in rows:
        gen = int(m["seat_gen"]) if m.get("seat_gen") else None
        m["seat"] = seat_label(m["agent_id"], m["handle"], gen)
        m["live"] = (m["age_secs"] or 1e9) < 900
    # THE FOLD (operator ruling, 2026-07-16: "the agent hash should be a row"): one soul,
    # one line. An agent legitimately holds MANY mount rows — its durable anchor, a tab
    # viewing it, a resume sibling — and rendering rows drew the same seat twice ("why is
    # there 2 thoth XL agents"). Group by agent: the realest row testifies for the card,
    # the soul is as alive as its freshest body, and ×N confesses the extra bodies.
    # THE SOUL is the LINEAGE, not the generation (operator, 2026-07-16: "metron ix,
    # viii, vii all show up as separate seats, but the mental model says the ancestors
    # are superseded"): the freshest generation is the face and every superseded one
    # folds UNDER it as a past life — never beside it. The NAME is the soul when one
    # exists (a restart can re-mint the id base mid-lineage — Metron IX rode a new base
    # while VIII and VII kept the old — but the seat's generations tick with the HANDLE,
    # and 'one day there will be a thoth 400' counts by name, not by hash); the hex base
    # groups only the anonymous.
    def _soul_key(m: dict[str, Any]) -> str:
        if m.get("handle"):
            return "seat:" + str(m["handle"]).lower()
        mt = re.match(r"^agent:[0-9a-f]{8}", m.get("agent_id") or "")
        return mt.group(0) if mt else (m.get("agent_id") or "?")

    souls: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for m in rows:
        souls.setdefault(_soul_key(m), {}).setdefault(m["agent_id"], []).append(m)
    mounts = []
    for gens in souls.values():
        def _grp_age(gid: str, _g: dict[str, list[dict[str, Any]]] = gens) -> float:
            return min(float(g["age_secs"] or 1e9) for g in _g[gid])
        head_id = min(gens, key=_grp_age)
        head = sorted(gens[head_id], key=_row_rank)
        best = dict(head[0])
        allrows = [g for grp in gens.values() for g in grp]
        best["live"] = any(g["live"] for g in allrows)
        best["seated"] = any(g.get("seated") for g in allrows)
        best["age_secs"] = min(float(g["age_secs"] or 1e9) for g in allrows)
        best["sessions"] = len(allrows)
        # the head's doors, realest first — each carrying what the renderer EXPLAINS
        best["doors"] = [{"session_key": g.get("session_key"), "job_dir": g.get("job_dir"),
                          "age_secs": g["age_secs"], "live": g["live"]} for g in head]
        # superseded generations, freshest first: past lives of the same seat
        best["ancestors"] = sorted(
            ({"seat": grp[0].get("seat") or gid, "age_secs": _grp_age(gid),
              "doors": len(grp)}
             for gid, grp in gens.items() if gid != head_id),
            key=lambda a: a["age_secs"])
        # a named lineage keeps its face even when the head row itself carries no label
        if not best.get("seat"):
            for a in best["ancestors"]:
                if str(a["seat"]).startswith("agent:"):
                    continue
                best["seat"] = a["seat"]
                break
        mounts.append(best)
    mounts.sort(key=lambda m: float(m["age_secs"] or 1e9))
    mounts = mounts[:60]
    wakes = [dict(r) for r in await pool.fetch(
        "SELECT to_project, from_agent, message_id, mode, woke_at FROM agent_wakes "
        "ORDER BY woke_at DESC LIMIT 30")]
    hourly = await pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE woke_at > now() - interval '1 hour'")
    return {"mounts": mounts, "wakes": wakes,
            "wakes_hour": int(hourly or 0), "wake_budget": wake_budget}


def _door_label(d: dict[str, Any]) -> str:
    """One door, one short line (operator, 2026-07-16, second pass: the plain words
    taught the model; now the verbosity goes — 'tab → <sid>' says it all)."""
    key = d.get("session_key") or ""
    sid = (d.get("job_dir") or "").rsplit("/", 1)[-1] or "?"
    when = _age(d.get("age_secs"))
    if key.startswith("view-of:"):
        return f"tab → {key.removeprefix('view-of:')} — {when}"
    if key.startswith("resume-of:"):
        return f"resume → {key.removeprefix('resume-of:')} — {when}"
    return f"session {sid} — {when}"


def _fleet_row(m: dict[str, Any]) -> str:
    dot = ('<span class="live">●</span> ' if m["live"] else '<span class="dim">○</span> ')
    name = _e(m["seat"] or m["agent_id"])
    doors = m.get("doors") or []
    ancestors = m.get("ancestors") or []
    # LEAN LABELS (operator, 2026-07-16, third pass): '1 agent' is the fold's invariant —
    # never said; the life number is already the roman in the name — never repeated. Only
    # the doors earn a marker. The seat's true depth (from the graph, never the registry
    # window) lives inside the unfold.
    gen = int(m["seat_gen"]) if m.get("seat_gen") else None
    lives_total = (gen - 1) if gen and gen > 1 else len(ancestors)
    if len(doors) > 1 or ancestors:
        marker = (f' <span class="dim">— {len(doors)} doors</span>'
                  if len(doors) > 1 else "")
        inner = "<br>".join("· " + _e(_door_label(d)) for d in doors)
        if ancestors:
            head_line = (f"{lives_total} earlier li{'ves' if lives_total != 1 else 'fe'} "
                         f"· {len(ancestors)} in window:")
            past = "<br>".join(
                f'· {_e(str(a["seat"]))} — {_e(_age(a["age_secs"]))}' for a in ancestors)
            inner = (inner + "<br>" if inner else "") + _e(head_line) + "<br>" + past
        # a STABLE id per soul: the poller restores open folds by id, so an unkeyed
        # details snapped shut on every refresh (operator: 'hard to nav')
        did = "f-" + re.sub(r"[^a-zA-Z0-9]", "", m.get("agent_id") or name)
        name_cell = (f'<details id="{did}"><summary>{dot}{name}{marker}</summary>'
                     f'<div class="dim">{inner}</div></details>')
    else:
        name_cell = f"{dot}{name}"
    return (
        f"<tr><td>{name_cell}</td>"
        f'<td><a href="/mail?box={_e(m["project"] or "?")}">{_e(m["project"] or "?")}</a></td>'
        f'<td class="dim">{_e((m["model"] or "?").removeprefix("claude-"))}</td>'
        f'<td class="dim">{_e(m["cwd"] or "")}</td>'
        f'<td class="ts">{_e(_age(m["age_secs"]))}</td></tr>')


def render_fleet(data: dict[str, Any]) -> str:
    mounts = data["mounts"]
    # the fleet number counts SEATED minds only (operator, 2026-07-17: 'fleet 5 but there
    # are only 3 agents up') — a live stranger's session (a bg-pty host, a spare) is real,
    # so it is confessed beside the number, never counted inside it.
    live_n = sum(1 for m in mounts if m["live"] and m.get("seated", True))
    vis_n = sum(1 for m in mounts if m["live"] and not m.get("seated", True))
    budget = (f' / {data["wake_budget"]}' if data.get("wake_budget") else "")
    # SOULS FIRST, DOORS NEVER (operator, 2026-07-16: "the 3x ra and 3x thoth is still
    # there and very confusing"): a soul's extra mount rows are doorways — tabs, resumes —
    # and a lens does not count plumbing, so the ×N marker is gone. Below the souls, the
    # UNRECONCILED sink into one collapsed line: they are the fold tray's backlog, named
    # as such, not paraded as peers of the named.
    named = [m for m in mounts if m.get("seat")]
    anon = [m for m in mounts if not m.get("seat")]
    vis = f" · {vis_n} visitor{'s' if vis_n != 1 else ''}" if vis_n else ""
    out = [f'<h2>seats <span class="pill">{live_n} live{vis} · '
           f'{len(named)} soul{"s" if len(named) != 1 else ""} · '
           f'{len(anon)} unreconciled</span> '
           f'<span class="pill">wakes {data["wakes_hour"]}{budget}/h</span></h2>',
           # CONFESSED, NOT VERIFIED (door census item 2, Thoth msg 5772/5741, thread
           # 2c3c2b9a): ● here means agent_mounts.last_seen touched within 15 minutes —
           # the SAME cache every other liveness read in this house draws from, never
           # cross-checked against the harness+/proc registry_census authority. An
           # operator reading this dot had no way to know that until now; it is an
           # awareness signal, not a verified fact, the same law render_overhead's own
           # "the page says so rather than estimating" caption already follows.
           '<p class="dim">● = touched agent_mounts within 15 min (a cache reading), '
           "not a harness/proc-verified fact — a fresh row can still be a phantom, and a "
           "stale one can still be a live body the cache hasn't heard from yet.</p>"]
    rows = "".join(_fleet_row(m) for m in named) or (
        '<tr><td class="dim">nothing mounted</td></tr>')
    out.append(f"<table>{rows}</table>")
    if anon:
        arows = "".join(_fleet_row(m) for m in anon)
        out.append(
            f'<details id="f-unreconciled"><summary class="dim">{len(anon)} '
            "unreconciled session(s) — fold proposals wait in the tray</summary>"
            f"<table>{arows}</table></details>")
    wrows = "".join(
        f'<tr><td class="ts">{_e(str(w["woke_at"])[:16])}</td>'
        f'<td>{_e(w["to_project"])}</td><td class="dim">{_e(w["mode"] or "?")}</td>'
        f'<td class="dim">by {_e(w["from_agent"] or "?")} (msg {_e(w["message_id"])})</td>'
        "</tr>"
        for w in data["wakes"]) or '<tr><td class="dim">no wakes yet</td></tr>'
    out.append(f"<h2>wake ledger</h2><table>{wrows}</table>")
    return "".join(out)


# ── /overhead ────────────────────────────────────────────────────────────────────────────

def _fmt_tok(n: int) -> str:
    """neo's token formatter: 1.2M reads, 1234567 doesn't."""
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))


def render_overhead(data: dict[str, Any], telemetry: dict[str, Any] | None = None) -> str:
    """THE OVERHEAD LENS (neo's eye, task #34): what the harness itself costs — the
    hidden channels beside every visible window, the cache-vs-fresh split, the
    system-reminder drip, the compaction churn. Read from the transcript store, which
    the observer's backfill keeps ~10 min current; a session the store hasn't eaten
    isn't here, and the page says so rather than estimating."""
    t = data["totals"]
    out = [
        f'<h2>harness overhead <span class="pill">{t["sessions"]} sessions · '
        f'{t["channel_files"]} hidden channels</span> '
        f'<span class="pill">{_fmt_tok(t["total_tokens"])} tokens · '
        f'{t["hidden_pct"]}% hidden · {t["cache_read_pct"]}% cache-read</span> '
        f'<span class="pill">{t["reminders"]} reminders · '
        f'{t["compactions"]} compactions</span></h2>',
        '<p class="dim">what the harness itself costs: subagent sidechains the screen '
        "never shows, cache re-reads vs fresh computation, system-reminder injections, "
        "compaction churn. tokens are API-reported from the transcripts the store has "
        "eaten; a session with no reported usage falls back to on-disk bytes (basis "
        "column names which). the ancestor lens: github.com/asuramaya/neo.</p>",
    ]
    def _size_cell(s: dict[str, Any]) -> str:
        if s["basis"] == "tokens":
            return _fmt_tok(s["total_tokens"])
        return f'{s["bytes"] / 1024:.0f}KB <span class="dim">(bytes)</span>'

    hrows = "".join(
        f'<tr><td>{_e(s["anchor_sid"])}</td>'
        f'<td><a href="/mail?box={_e(s["project"] or "?")}">'
        f'{_e(s["project"] or "?")}</a></td>'
        f"<td>{_size_cell(s)}</td>"
        f'<td>{s["hidden_pct"]}%</td><td class="dim">{s["multiplier"]}×</td>'
        f'<td class="dim">{s["cache_read_pct"]}%</td>'
        f'<td class="dim">{s["channel_files"]}</td>'
        f'<td class="dim">{s["reminders"]}</td>'
        f'<td class="dim">{s["compactions"]}</td></tr>'
        for s in data["top"]) or '<tr><td class="dim">the store has eaten nothing yet</td></tr>'
    out.append(
        "<h2>top sessions</h2><table>"
        "<tr><th>session</th><th>project</th><th>total</th><th>hidden</th>"
        "<th>×</th><th>cache</th><th>channels</th><th>reminders</th>"
        f"<th>compactions</th></tr>{hrows}</table>")
    if telemetry:
        t2 = telemetry
        span = ""
        if t2.get("oldest") and t2.get("newest"):
            span = f' · {str(t2["oldest"])[:10]} → {str(t2["newest"])[:10]}'
        tops = ", ".join(
            f'{_e(e["event"].removeprefix("tengu_"))} ×{e["n"]}'
            for e in t2.get("top_events", []))
        out.append(
            f'<h2>retained telemetry <span class="pill">{t2["events"]} events · '
            f'{t2["files"]} files · {t2["bytes"] / 1024:.0f}KB</span> '
            f'<span class="pill">{t2["sessions"]} sessions · '
            f'{t2["devices"]} device id{"s" if t2["devices"] != 1 else ""}</span></h2>'
            f'<p class="dim">failed-to-send telemetry Claude Code keeps at '
            f"~/.claude/telemetry — nothing reads or prunes it; this is what it "
            f"remembers about you{_e(span)}. top events: {tops or '—'}.</p>")
    return "".join(out)

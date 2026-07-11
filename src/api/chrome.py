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

from typing import Any

import asyncpg

from src.api.membrane import _CSS, _age, _e
from src.orchestrator.agents import seat_label
from src.orchestrator.mailbox import DESK_KINDS

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
"""

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

_TABS = (("desk", "/desk"), ("mail", "/mail"), ("fleet", "/fleet"), ("membrane", "/membrane"))


def page(title: str, active: str, inner: str) -> str:
    """The shell: nav + the poll-swapped content div. Everything inside #c must render
    identically when served as ?partial=1 — the poller depends on it."""
    nav = "".join(
        f'<a href="{href}" class="{"on" if name == active else ""}">{name}</a>'
        for name, href in _TABS)
    return (
        '<!doctype html><meta charset="utf-8">'
        f"<title>osiris {_e(title)}</title><style>{_CHROME_CSS}</style>"
        f'<h1>◈ {_e(title)}</h1><div class="nav">{nav}'
        f'<span class="ts">updated <span id="ts">just now</span> · refreshes 4s · '
        "read-only</span></div>"
        f'<div id="c">{inner}</div>{_POLLER}'
    )


def _card(dom_id: str, head: str, body: str, sub: str = "") -> str:
    """One clickable card: closed = the head line; open = the full body. Stable dom ids let
    the poller re-open what the operator was reading."""
    return (
        f'<details class="card" id="{_e(dom_id)}"><summary>{head}</summary>'
        f'<div class="body">{_e(body)}</div>{sub}</details>'
    )


# ── /desk ────────────────────────────────────────────────────────────────────────────────

_BAND_HDRS = {"decision": ("needs your decision", "hdr-decision"),
              "hands": ("blocked on your hands", "hdr-hands"),
              "fyi": ("fyi", "hdr-fyi")}
_BAND_KEYS = {"decision": "needs_decision", "hands": "needs_hands", "fyi": "fyi"}


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


def render_desk(desk: dict[str, Any]) -> str:
    """The organized desk (mailbox.read_desk's shape) as clickable bands."""
    out: list[str] = []
    for kind in DESK_KINDS:
        cards = desk.get(_BAND_KEYS[kind]) or []
        title, cls = _BAND_HDRS[kind]
        out.append(f'<div class="band"><h2 class="{cls}">{title} '
                   f'<span class="pill">{len(cards)}</span></h2>')
        out.append("".join(_brief_card(m) for m in cards)
                   or '<p class="dim">nothing here</p>')
        out.append("</div>")
    dimmed = desk.get("dimmed") or []
    if dimmed:
        out.append('<div class="band"><h2 class="hdr-fyi">dimmed '
                   f'<span class="pill">{len(dimmed)}</span></h2>')
        for d in dimmed:
            head = (f'<span class="who">{_e(d.get("project") or "?")}</span>'
                    f'<span class="dim">{_e(d.get("headline") or "")}</span>')
            out.append(_card(f'dim{d["id"]}', head,
                             f'moot ({_e(d.get("by"))}): {d.get("moot") or ""}'))
        out.append("</div>")
    q = (desk.get("your_queue") or {}).get("threads") or []
    if q:
        items = "".join(
            f'<li><span class="dim">{_e(t["id"])}</span> {_e(t["summary"])}'
            + (' <span class="pill">obligation</span>' if t.get("kind") == "obligation"
               else "") + "</li>" for t in q)
        out.append('<div class="band"><h2>your queue '
                   '<span class="dim">(owner=operator threads, live from the graph)</span>'
                   f'</h2><ul class="queue">{items}</ul></div>')
    return "".join(out)


# ── /mail ────────────────────────────────────────────────────────────────────────────────

async def mail_overview(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Every mailbox with traffic, busiest-latest first. Unsettled = no intended recipient
    has settled it (the same notion the wake dispatch uses)."""
    rows = await pool.fetch(
        "SELECT COALESCE(m.to_project, '@' || m.to_agent) AS box, count(*) AS msgs, "
        " count(*) FILTER (WHERE m.read_at IS NULL AND NOT EXISTS "
        "   (SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
        "    AND r.read_at IS NOT NULL)) AS unsettled, "
        " max(m.created_at) AS last_at "
        "FROM fleet_messages m GROUP BY 1 ORDER BY max(m.created_at) DESC LIMIT 60")
    return [dict(r) for r in rows]


async def mail_threads(pool: asyncpg.Pool, box: str) -> list[dict[str, Any]]:
    """One mailbox's conversations, newest thread first, messages oldest-first within.
    A '@agent:...' box is the DM lane; a bare name is a project's group chat (both
    directions — the conversation, not just the inbound half)."""
    if box.startswith("@"):
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


def render_mail_overview(boxes: list[dict[str, Any]]) -> str:
    def unsettled_cell(b: dict[str, Any]) -> str:
        if b["unsettled"]:
            return f'<span class="amber">{b["unsettled"]} unsettled</span>'
        return '<span class="dim">settled</span>'
    rows = "".join(
        f'<tr><td><a href="/mail?box={_e(b["box"])}">{_e(b["box"])}</a></td>'
        f'<td>{b["msgs"]} msgs</td>'
        f"<td>{unsettled_cell(b)}</td>"
        f'<td class="ts">{_e(str(b["last_at"])[:16])}</td></tr>'
        for b in boxes) or '<tr><td class="dim">no mail anywhere</td></tr>'
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

async def fleet_data(pool: asyncpg.Pool, *, wake_budget: int = 0) -> dict[str, Any]:
    mounts = [dict(r) for r in await pool.fetch(
        "SELECT m.agent_id, m.project, m.model, m.cwd, m.last_seen, "
        " extract(epoch FROM (now() - m.last_seen)) AS age_secs, "
        " (SELECT a.value #>> '{}' FROM current_assertions a "
        "   JOIN objects o ON o.id=a.object_id "
        "   WHERE o.canonical=m.agent_id AND a.name='handle' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS handle "
        "FROM agent_mounts m ORDER BY m.last_seen DESC LIMIT 60")]
    for m in mounts:
        m["seat"] = seat_label(m["agent_id"], m["handle"])
        m["live"] = (m["age_secs"] or 1e9) < 900
    wakes = [dict(r) for r in await pool.fetch(
        "SELECT to_project, from_agent, message_id, mode, woke_at FROM agent_wakes "
        "ORDER BY woke_at DESC LIMIT 30")]
    hourly = await pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE woke_at > now() - interval '1 hour'")
    return {"mounts": mounts, "wakes": wakes,
            "wakes_hour": int(hourly or 0), "wake_budget": wake_budget}


def render_fleet(data: dict[str, Any]) -> str:
    mounts = data["mounts"]
    live_n = sum(1 for m in mounts if m["live"])
    budget = (f' / {data["wake_budget"]}' if data.get("wake_budget") else "")
    out = [f'<h2>seats <span class="pill">{live_n} live · {len(mounts)} mounted</span> '
           f'<span class="pill">wakes {data["wakes_hour"]}{budget}/h</span></h2>']
    rows = "".join(
        "<tr><td>" + ('<span class="live">●</span> ' if m["live"]
                      else '<span class="dim">○</span> ')
        + f'{_e(m["seat"] or m["agent_id"])}</td>'
        f'<td><a href="/mail?box={_e(m["project"] or "?")}">{_e(m["project"] or "?")}</a></td>'
        f'<td class="dim">{_e((m["model"] or "?").removeprefix("claude-"))}</td>'
        f'<td class="dim">{_e(m["cwd"] or "")}</td>'
        f'<td class="ts">{_e(_age(m["age_secs"]))}</td></tr>'
        for m in mounts) or '<tr><td class="dim">nothing mounted</td></tr>'
    out.append(f"<table>{rows}</table>")
    wrows = "".join(
        f'<tr><td class="ts">{_e(str(w["woke_at"])[:16])}</td>'
        f'<td>{_e(w["to_project"])}</td><td class="dim">{_e(w["mode"] or "?")}</td>'
        f'<td class="dim">by {_e(w["from_agent"] or "?")} (msg {_e(w["message_id"])})</td>'
        "</tr>"
        for w in data["wakes"]) or '<tr><td class="dim">no wakes yet</td></tr>'
    out.append(f"<h2>wake ledger</h2><table>{wrows}</table>")
    return "".join(out)

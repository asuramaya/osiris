"""/membrane — the upward lane as a page: the statusline's click-through (Lane A+, 9d2aaf4d).

The chrome shows counts; this is where the counts open. One server-rendered, read-only page
over fleet_digest + the wake ledger, with ANCHORED sections the statusline deep-links into
(#desk #conversations #fleet #wakes #activity). Deliberately a LENS, not the composer: no
JS framework, no state, no writes (the operator-inbox read is a peek), auto-refresh 30s.
The composer shell (Lane B) stays held by operator ruling 450caf7b — this page is the
smallest thing that makes the membrane walkable, and it dies happily when P5 supersedes it.
"""
from __future__ import annotations

import html
from typing import Any

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
a{color:#58a6ff;text-decoration:none}
.body{white-space:pre-wrap;color:#8b949e;max-width:60rem}
"""


def _e(v: Any) -> str:
    return html.escape(str(v if v is not None else ""))


def _row(*cells: str) -> str:
    return "<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>"


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


def _footer(dg: dict[str, Any]) -> str:
    """The window line: 'new since <watermark> (age)' in watermark mode, else a plain window.
    Tolerant of a digest with no watermark block (an older shape)."""
    wm = dg.get("watermark") or {}
    since = _e(dg["since"][:16])
    if wm.get("mode") == "watermark":
        if wm.get("value"):
            lead = (f'new since <b>{_e(wm["value"][:16])}</b> '
                    f'<span class="dim">· watermark {_e(_age(wm.get("age_secs")))} old</span>')
        else:
            lead = 'new in the <b>last 24h</b> <span class="dim">· no watermark set yet</span>'
    else:
        lead = f"window {since} → now"
    return (f'<p class="dim">read-only lens · auto-refreshes 30s · {lead} '
            '<span class="dim">· the page never advances the watermark</span></p>')


def render_membrane(dg: dict[str, Any], wakes: list[dict[str, Any]]) -> str:
    """The whole page from a fleet_digest dict + the wake-ledger tail. Pure — tests feed it
    fixtures; the route feeds it the live graph."""
    s = dg["summary"]
    desk = dg["operator_inbox"]
    strip = (
        f'<div class="strip">'
        f'<span><a href="#desk"><b class="{"red" if desk["unread"] else "dim"}">desk '
        f'{desk["unread"]}</b></a></span>'
        f'<span><a href="#conversations"><b>threads {s["conversations"]}</b></a></span>'
        f'<span><a href="#fleet"><b>fleet {s["agents"]}</b></a>'
        f' <span class="dim">({s["unresolved"]} unresolved · {s["swapped"]} swapped)</span></span>'
        f'<span><a href="#wakes"><b>wakes {len(wakes)}</b></a></span>'
        f'<span><a href="#activity"><b>activity {s["activity"]}</b></a></span>'
        f'<span class="{"red" if s["laundering"] else "dim"}">laundering {s["laundering"]}</span>'
        f"</div>"
    )
    desk_rows = "".join(
        _row(f'<span class="dim">{_e(m["when"][:16])}</span>',
             f'{_e(m["from_project"] or "?")} <span class="dim">{_e(m["from"])}</span>',
             f'<div class="body">{_e(m["body"])}</div>')
        for m in desk["latest"]
    ) or _row('<span class="dim">nothing waiting</span>', "", "")
    conv_rows = "".join(
        _row(f'#{_e(c["thread"])}',
             _e(" ↔ ".join(c["between"])),
             f'{c["msgs"]} msg{"s" if c["msgs"] != 1 else ""}'
             + (f' · <span class="amber">{c["unsettled"]} unsettled</span>'
                if c["unsettled"] else ""),
             f'<span class="dim">{_e(c["last_at"][:16])}</span>',
             f'<div class="body">{_e(c["last"]["from"] + ": " + c["last"]["body"])}'
             "</div>" if c["last"] else "")
        for c in dg["conversations"]
    ) or _row('<span class="dim">no threads in window</span>', "", "", "", "")
    roster_rows = "".join(
        _row(_e(r["agent"]),
             _e(r["project"] or "?"),
             _e((r["model"] or "?").removeprefix("claude-")),
             ('<span class="green">resolved</span>' if r["resolved"]
              else '<span class="red">unresolved</span>'),
             f'<span class="red">{_e(r["swapped"])}</span>' if r["swapped"] else "")
        for r in dg["roster"]
    )
    wake_rows = "".join(
        _row(f'<span class="dim">{_e(str(w["woke_at"])[:16])}</span>',
             _e(w["to_project"]),
             f'<span class="dim">by {_e(w["from_agent"] or "?")} '
             f'(msg {_e(w["message_id"])})</span>')
        for w in wakes
    ) or _row('<span class="dim">no wakes yet</span>', "", "")
    act_rows = "".join(
        _row(f'<span class="dim">{_e(a["at"][:16])}</span>',
             f'{_e(a["type"])} <span class="dim">{_e(a["agent"])}</span>',
             f'<div class="body">{_e(a["summary"])}</div>')
        for a in dg["activity"]
    )
    laund = ""
    if dg["laundering"]:
        laund_rows = "".join(
            _row(_e(x["name"]), _e(x["value"]),
                 f'<span class="red">{_e(", ".join(x["laundering_sources"]))}</span>')
            for x in dg["laundering"])
        laund = f'<h2 id="laundering">laundering</h2><table>{laund_rows}</table>'
    return (
        '<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="30">'
        f"<title>osiris membrane</title><style>{_CSS}</style>"
        f"<h1>◈ membrane</h1>{strip}"
        f'<h2 id="desk">operator desk</h2><table>{desk_rows}</table>'
        f'<h2 id="conversations">conversations</h2><table>{conv_rows}</table>'
        f'<h2 id="fleet">fleet</h2><table>{roster_rows}</table>'
        f'<h2 id="wakes">wake ledger</h2><table>{wake_rows}</table>'
        f"{laund}"
        f'<h2 id="activity">activity</h2><table>{act_rows}</table>'
        f"{_footer(dg)}"
    )

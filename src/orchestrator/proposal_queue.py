"""One-pass judging of the miner's proposals.

A Proposal is a miner's suggestion (this object belongs in that project) that a person
accepts or rejects. This module is the list and the batch: `pending()` returns the live
proposals as short, labelled rows, and `judge()` accepts or rejects any set of them. It
writes nothing of its own: every decision goes through `proposals.accept` /
`proposals.reject` (via the same `proposal` action the MCP tool and `osiris proposal` use),
so the audit trail, the evidence grade and the refusals are exactly theirs.

Each proposal is shown and addressed by a short STABLE handle: the first hex digits of its
own id (at least five, longer only when two pending ones would collide). It never changes
when other proposals are judged, so a command can only ever land on the proposal it names.
(An earlier version numbered the rows 1..n; accepting one renumbered the rest, and a second
command meant for one proposal rejected another.) Rows are ordered by expiry, soonest first.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import asyncpg

from src.orchestrator.proposals import _proposal_row

_TITLE_MAX = 60


def _label(value: object, limit: int = _TITLE_MAX) -> str:
    flat = " ".join(str(value or "").split())
    return flat if len(flat) <= limit else flat[: limit - 1].rstrip() + "…"


async def _name_of(pool: asyncpg.Pool, object_id: object) -> tuple[str, str, str]:
    """(type, title, canonical) of an object: the first of name, summary, statement that it
    carries, else its canonical."""
    row = await pool.fetchrow("SELECT type, canonical FROM objects WHERE id = $1::uuid",
                              str(object_id))
    if row is None:
        return "", str(object_id), str(object_id)
    title = await pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id = $1::uuid "
        "AND a.name IN ('name', 'summary', 'statement') "
        "ORDER BY CASE a.name WHEN 'name' THEN 0 WHEN 'summary' THEN 1 ELSE 2 END, "
        "a.confidence DESC, a.observed_at DESC, a.id DESC LIMIT 1", str(object_id))
    return row["type"], _label(title or row["canonical"]), row["canonical"]


def _as_dict(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            out = json.loads(value)
        except ValueError:
            return {}
        return out if isinstance(out, dict) else {}
    return {}


_MIN_HANDLE = 5


def _hex_of(ref: str) -> str:
    """The id inside a proposal ref, dashes and prefix removed, lower case."""
    return ref.split(":", 1)[-1].replace("-", "").lower()


def _handles(refs: list[str]) -> list[str]:
    """The shortest prefix of each ref's id, at least five digits, that no other listed
    ref shares."""
    ids = [_hex_of(r) for r in refs]
    out: list[str] = []
    for mine in ids:
        n = _MIN_HANDLE
        while n < len(mine) and any(o != mine and o.startswith(mine[:n]) for o in ids):
            n += 1
        out.append(mine[:n])
    return out


async def resolve(pool: asyncpg.Pool, words: list[str],
                  items: list[dict[str, Any]]) -> tuple[list[str], str]:
    """Words to proposal refs: `all` (every pending one), a handle, or a full ref. Returns
    (refs, complaint). A handle is looked up among EVERY proposal, so a judged one is found
    and refused by the real verb rather than reported as unknown; one that matches several
    is refused naming them; nothing is acted on until every word resolves."""
    refs: list[str] = []
    for word in words:
        if word.lower() == "all":
            refs.extend(it["ref"] for it in items)
            continue
        digits = _hex_of(word)
        if len(digits) < 4 or any(c not in "0123456789abcdef" for c in digits):
            return [], (f"'{word}' is not a proposal id: ids look like "
                        f"{items[0]['handle'] if items else '8b75b'} (see osiris proposals).")
        rows = await pool.fetch(
            "SELECT canonical FROM objects WHERE type = 'Proposal' AND "
            "replace(replace(canonical, 'proposal:', ''), '-', '') LIKE $1 || '%'", digits)
        if not rows:
            return [], f"no proposal starts with {digits}."
        if len(rows) > 1:
            names = ", ".join(_hex_of(r["canonical"])[:8] for r in rows[:4])
            return [], f"{digits} matches {len(rows)} proposals ({names}); type more of it."
        if rows[0]["canonical"] not in refs:
            refs.append(rows[0]["canonical"])
    return refs, ""


async def describe(pool: asyncpg.Pool, ref: str) -> dict[str, Any] | None:
    """Title and target of any proposal, judged or not (for the line that reports it)."""
    row = await _proposal_row(pool, ref)
    if row is None:
        return None
    cand = _as_dict(row["candidate"])
    if cand.get("kind") == "link":
        _t, title, _c = await _name_of(pool, cand.get("from_id"))
        _t2, target, _c2 = await _name_of(pool, cand.get("to_id"))
        return {"title": title, "target": target.removeprefix("repo:"),
                "status": row["status"]}
    return {"title": _label(cand.get("canonical")), "target": "(new)",
            "status": row["status"]}


async def reopen_one(pool: asyncpg.Pool, actor: str, ref: str, reason: str,
                     *, ruling: str | None = None) -> dict[str, Any]:
    """Undo a mistaken rejection through `proposals.reopen`: {"ref","ok","error"?}."""
    from src.actions.core import Actions
    from src.orchestrator.proposals import reopen

    res = await reopen(Actions(pool), proposal=ref, reason=reason, actor=actor,
                       ruling=ruling)
    return {"ref": ref, "ok": "error" not in res,
            **({"error": str(res["error"])} if "error" in res else {})}


async def pending(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Live proposals (status 'proposed', not yet past their expiry), soonest to expire
    first, each with the stable handle it is shown under and everything a person needs to judge
    it: the thing being filed, where the miner would file it, and the signal it went on."""
    now = datetime.now(UTC)
    refs = await pool.fetch(
        "SELECT o.canonical FROM objects o JOIN current_assertions s "
        "ON s.object_id = o.id AND s.name = 'status' "
        "WHERE o.type = 'Proposal' AND s.value #>> '{}' = 'proposed'")
    rows: list[dict[str, Any]] = []
    for r in refs:
        row = await _proposal_row(pool, r["canonical"])
        if row is None or row["status"] != "proposed" or not row["expires_at"]:
            continue
        expires = datetime.fromisoformat(row["expires_at"])
        if expires <= now:
            continue
        cand = _as_dict(row["candidate"])
        if cand.get("kind") == "link":
            otype, title, _canon = await _name_of(pool, cand.get("from_id"))
            _ttype, target, _tcanon = await _name_of(pool, cand.get("to_id"))
            target = target.removeprefix("repo:")
            what = cand.get("link_type", "link")
        else:
            otype, title = str(cand.get("type", "")), _label(cand.get("canonical"))
            target, what = "(new)", "create"
        miner = await pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id = $1 "
            "AND a.name = 'miner' ORDER BY a.observed_at DESC, a.id DESC LIMIT 1", row["id"])
        rows.append({
            "ref": r["canonical"], "title": title, "type": otype, "target": target,
            "link": what, "signal": cand.get("signal") or "", "miner": miner or "",
            "owner": row["owner"] if isinstance(row["owner"], str) else "",
            "expires_at": expires.isoformat(), "days_left": max((expires - now).days, 0)})
    rows.sort(key=lambda x: (x["expires_at"], x["ref"]))
    for item, handle in zip(rows, _handles([r["ref"] for r in rows]), strict=True):
        item["handle"] = handle
    return rows


async def judge(
    pool: asyncpg.Pool, actor: str, decision: str, refs: list[str], *, reason: str = "",
) -> list[dict[str, Any]]:
    """Accept or reject each ref through the existing `proposal` action, one result per
    ref in order: {"ref", "ok", "error"?}. A refusal on one (already judged, expired) never
    stops the others."""
    from src.mcp_server import _proposal_action_impl

    if decision not in ("accept", "reject"):
        return [{"ref": r, "ok": False, "error": f"unknown decision {decision!r}"}
                for r in refs]
    out: list[dict[str, Any]] = []
    for ref in refs:
        res = await _proposal_action_impl(
            pool, actor, decision, proposal_ref=ref,
            reason=reason or "not a match (judged in one pass)")
        if "error" in res:
            out.append({"ref": ref, "ok": False, "error": str(res["error"])})
        else:
            out.append({"ref": ref, "ok": True})
    return out


# --- what a person reads and types ---------------------------------------------------------

def _plural(n: int, word: str) -> str:
    return f"{n} {word}" if n == 1 else f"{n} {word}s"


def list_lines(items: list[dict[str, Any]], *, verbose: bool = False) -> list[str]:
    """The list: a header, one line each (handle, what, where, signal), and one line saying
    what to type, written with real handles from the list."""
    if not items:
        return ["osiris proposals: nothing waiting."]
    soonest = items[0]["days_left"]
    when = "today" if soonest == 0 else f"in {_plural(soonest, 'day')}"
    lines = [f"osiris proposals: {len(items)} waiting; the first lapses {when}."]
    width = max(len(it["handle"]) for it in items)
    for it in items:
        line = f" {it['handle']:<{width}}  {it['title']}  ->  {it['target']}"
        if it["signal"]:
            line += f"  ({it['signal']})"
        if verbose:
            line += (f"  [{it['type'] or 'object'} {it['link']}; {it['miner'] or 'miner'}; "
                     f"lapses {it['expires_at'][:10]}; {it['ref']}]")
        lines.append(line)
    h = [it["handle"] for it in items]
    second = h[1] if len(h) > 1 else h[0]
    lines.append(f"Judge them: osiris proposals accept {h[0]}  |  reject {second}  |  "
                 "accept all")
    return lines


def result_lines(titles: dict[str, dict[str, Any]], refs: list[str],
                 results: list[dict[str, Any]], verb: str) -> list[str]:
    """One line per decision, then a summary. `verb` is the past tense ('accepted',
    'rejected', 'reopened'); `titles` maps ref to its title/target when known."""
    lines: list[str] = []
    done = 0
    for ref, res in zip(refs, results, strict=True):
        handle = _hex_of(ref)[:_MIN_HANDLE]
        info = titles.get(ref)
        if res["ok"]:
            done += 1
            lines.append(f" {verb} {handle}: {info['title']}  ->  {info['target']}"
                         if info else f" {verb} {handle}")
        else:
            lines.append(f" refused {handle}: {res['error'].split(': ')[-1].split('. ')[0]}")
    failed = len(results) - done
    lines.append(f"osiris proposals: {done} {verb}"
                 + (f", {failed} refused." if failed else "."))
    return lines

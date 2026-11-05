"""One-pass judging of the miner's proposals.

A Proposal is a miner's suggestion (this object belongs in that project) that a person
accepts or rejects. Judging them one at a time through the `proposal` verb meant a ref to
copy and a call to make per item, and eight sat unjudged. This module is the list and the
batch: `pending()` returns the live proposals as short, numbered, human-labelled rows, and
`judge()` accepts or rejects any set of them. It writes nothing of its own: every decision
goes through `proposals.accept` / `proposals.reject` (via the same `proposal` action the
MCP tool and `osiris proposal` use), so the audit trail, the evidence grade and the
refusals are exactly theirs.

Numbering is by expiry, soonest first, so the one that will lapse next is always number 1
and a newly mined proposal joins the end. The CLI shows numbers; the desk and `judge()` act
on the stable proposal ref, so a list that changes between looking and clicking can never
hit the wrong one.
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
        "a.confidence DESC, a.observed_at DESC LIMIT 1", str(object_id))
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


async def pending(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Live proposals (status 'proposed', not yet past their expiry), soonest to expire
    first, each with the number it is shown under and everything a person needs to judge
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
            "AND a.name = 'miner' ORDER BY a.observed_at DESC LIMIT 1", row["id"])
        rows.append({
            "ref": r["canonical"], "title": title, "type": otype, "target": target,
            "link": what, "signal": cand.get("signal") or "", "miner": miner or "",
            "owner": row["owner"] if isinstance(row["owner"], str) else "",
            "expires_at": expires.isoformat(), "days_left": max((expires - now).days, 0)})
    rows.sort(key=lambda x: (x["expires_at"], x["ref"]))
    for i, item in enumerate(rows, 1):
        item["number"] = i
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

def parse_numbers(words: list[str], count: int) -> list[int] | str:
    """`1 3 4`, `1,3`, `2-4` and `all` as a list of numbers within 1..count, in the order
    given and without repeats; or a one-line complaint when a word is not a number in range.
    Nothing is acted on until every word parses."""
    picked: list[int] = []
    for word in words:
        if word.lower() == "all":
            picked.extend(range(1, count + 1))
            continue
        for part in word.split(","):
            if not part:
                continue
            try:
                if "-" in part:
                    lo, hi = (int(x) for x in part.split("-", 1))
                    nums = list(range(lo, hi + 1))
                else:
                    nums = [int(part)]
            except ValueError:
                return f"'{part}' is not a number from the list (or 'all')."
            for n in nums:
                if not 1 <= n <= count:
                    return (f"there is no number {n}: the list runs 1 to {count}."
                            if count else "nothing is waiting.")
                picked.append(n)
    ordered: list[int] = []
    for n in picked:
        if n not in ordered:
            ordered.append(n)
    return ordered


def _plural(n: int, word: str) -> str:
    return f"{n} {word}" if n == 1 else f"{n} {word}s"


def list_lines(items: list[dict[str, Any]], *, verbose: bool = False) -> list[str]:
    """The list: a header, one line each, one line saying what to type."""
    if not items:
        return ["osiris proposals: nothing waiting."]
    soonest = items[0]["days_left"]
    when = "today" if soonest == 0 else f"in {_plural(soonest, 'day')}"
    lines = [f"osiris proposals: {len(items)} waiting; the first lapses {when}."]
    width = len(str(len(items)))
    for it in items:
        line = f" {it['number']:>{width}}  {it['title']}  ->  {it['target']}"
        if it["signal"]:
            line += f"  ({it['signal']})"
        if verbose:
            line += (f"  [{it['type'] or 'object'} {it['link']}; {it['miner'] or 'miner'}; "
                     f"lapses {it['expires_at'][:10]}; {it['ref']}]")
        lines.append(line)
    lines.append("Judge them: osiris proposals accept 1 3  |  reject 2  |  accept all")
    return lines


def result_lines(items: list[dict[str, Any]], numbers: list[int],
                 results: list[dict[str, Any]], decision: str) -> list[str]:
    """One line per decision, then a summary. `numbers` and `results` run in step."""
    by_number = {it["number"]: it for it in items}
    verb = "accepted" if decision == "accept" else "rejected"
    lines: list[str] = []
    done = 0
    for n, res in zip(numbers, results, strict=True):
        it = by_number[n]
        if res["ok"]:
            done += 1
            lines.append(f" {verb} {n}: {it['title']}  ->  {it['target']}")
        else:
            lines.append(f" refused {n}: {res['error'].split(': ')[-1].split('. ')[0]}")
    failed = len(results) - done
    lines.append(f"osiris proposals: {done} {verb}"
                 + (f", {failed} refused." if failed else "."))
    return lines

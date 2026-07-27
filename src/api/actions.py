"""ACTION_VERBS — the declarative write registry (ruling c5b184cd, thread d56e7073/#44, the
composition abstraction's WRITE leg). A composition row can carry `{"action":<name>,
"args":{...}}` (see `compositions._table`'s `row_action`); the generic renderer turns that
into a button, and one click POSTs `{action, args}` to `/act` (app.py). This module is what
`/act` looks the action name up in.

Grounded in the two write routes that already do this correctly today, just per-route-
hardcoded — `/threads/triage` and `/desk/settle` (app.py) — this generalizes their exact
shape rather than inventing one:

  - AUTHORITY FROM THE SURFACE, NEVER A CLIENT-SUPPLIED FIELD. Every adapter below hardcodes
    `source="analyst:operator"` itself; nothing in `args` is ever read as "who's acting."
  - NEVER RE-IMPLEMENTS A GUARD. Each adapter calls the REAL verb (capture.py/mailbox.py)
    plainly — it is pure dispatch, reading only the specific `args` keys it names, exactly
    the same explicit-getter discipline `/threads/triage` already uses (never `**args`
    forwarded blindly; an unread key is silently ignored, never an injection surface).
  - IDEMPOTENT BY THE VERB'S OWN CONSTRUCTION, not a mechanism built here. resolve_thread/
    assign_thread/reclassify_thread/ack_messages are event-sourced — re-asserting the same
    value twice is a no-op, not a second effect. The client's own disable-on-click (chrome.py's
    `_ACTIONS` JS) is the UI-level safeguard against a double-fire during one request; no new
    generic dedup token is built until a real action verb needs one.

A registry entry is a function `(pool, args) -> receipt`, not the raw graph verb directly —
the raw verbs take heterogeneous shapes (`Actions` vs a bare pool, `ref` vs `ids`, one
`because` vs none); each adapter here is the thin, explicit seam that normalizes ONE verb to
the same call shape `/act` needs, and nothing more."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import asyncpg

ActionVerb = Callable[[asyncpg.Pool, dict[str, Any]], Awaitable[dict[str, Any]]]

_ACTOR = "analyst:operator"  # the exact attribution /threads/triage and /desk/settle already use


async def _act_resolve(pool: asyncpg.Pool, args: dict[str, Any]) -> dict[str, Any]:
    from src.actions.core import Actions
    from src.orchestrator.capture import resolve_thread

    ref = str(args.get("ref") or "")
    if not ref:
        return {"error": "resolve_thread needs ref"}
    tid = await resolve_thread(Actions(pool), ref, because=args.get("because"),
                               source=_ACTOR)
    return {"ok": tid is not None, "id": str(tid) if tid else None}


async def _act_assign(pool: asyncpg.Pool, args: dict[str, Any]) -> dict[str, Any]:
    from src.actions.core import Actions
    from src.orchestrator.capture import assign_thread

    ref, owner = str(args.get("ref") or ""), str(args.get("owner") or "")
    if not ref or not owner:
        return {"error": "assign_thread needs ref and owner"}
    tid = await assign_thread(Actions(pool), ref, owner=owner, because=args.get("because"),
                              source=_ACTOR)
    return {"ok": tid is not None, "id": str(tid) if tid else None}


async def _act_defer(pool: asyncpg.Pool, args: dict[str, Any]) -> dict[str, Any]:
    from src.actions.core import Actions
    from src.orchestrator.capture import defer_thread

    ref = str(args.get("ref") or "")
    if not ref:
        return {"error": "defer_thread needs ref"}
    days = int(args.get("days") or 30)
    tid = await defer_thread(Actions(pool), ref, days=days, because=args.get("because"),
                             source=_ACTOR)
    return {"ok": tid is not None, "id": str(tid) if tid else None}


async def _act_reclassify(pool: asyncpg.Pool, args: dict[str, Any]) -> dict[str, Any]:
    from src.actions.core import Actions
    from src.orchestrator.capture import reclassify_thread

    ref, kind = str(args.get("ref") or ""), str(args.get("kind") or "")
    if not ref or not kind:
        return {"error": "reclassify_thread needs ref and kind"}
    tid = await reclassify_thread(Actions(pool), ref, kind=kind, because=args.get("because"),
                                  source=_ACTOR)
    return {"ok": tid is not None, "id": str(tid) if tid else None}


async def _act_settle(pool: asyncpg.Pool, args: dict[str, Any]) -> dict[str, Any]:
    from src.orchestrator.mailbox import OPERATOR_ADDR, ack_messages

    raw = args.get("ids")
    ids = [int(i) for i in raw] if isinstance(raw, list) else (
        [int(raw)] if raw is not None else [])
    if not ids:
        return {"error": "settle needs ids"}
    out = await ack_messages(pool, OPERATOR_ADDR, ids, reader_agent=OPERATOR_ADDR)
    return {"settled": len(out["settled"]), "skipped": out.get("skipped", {})}


ACTION_VERBS: dict[str, ActionVerb] = {
    "resolve_thread": _act_resolve,
    "assign_thread": _act_assign,
    "defer_thread": _act_defer,
    "reclassify_thread": _act_reclassify,
    "settle": _act_settle,
}

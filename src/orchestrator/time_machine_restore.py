"""TIME MACHINE, stage 3: restore ONE item from a past moment into the live graph.

Never by overwriting history. The live graph only ever grows: the restore goes through the
same write verbs every other writer uses (`assert_property`, `create_link`), inside one
transaction, and leaves a note on the object saying where it came from.

WHAT IS RESTORED. The object's assertions that were CURRENT at the moment (one per
property and source), and its links that were active then. The superseded history stays in
the moment (it is in the backup); live readers never use it. Identity is (type, canonical):
an object that exists live is added to under its own uuid, one that does not is created
through the normal find-or-create.

THE PLAN COMES FIRST (`plan_restore`, writes nothing). Each assertion lands in one bucket:
  copy       no live value for that property from that source: it is simply added.
  skip       live already holds the same value from that source.
  conflict   the operator picks (default: keep live). Two kinds:
               same_source_changed  live holds a DIFFERENT value from the same source;
                                    restoring would supersede it (as a new write).
               tie                  a live value from another source has the same confidence
                                    and observed_at but a different value. A restored row gets
                                    a NEW, higher id, and the id is the final tiebreak of
                                    "which value is current", so it would silently win the
                                    tie. It is never appended without a choice.
A copy that would outrank the live displayed value is marked `becomes_displayed`.
Properties live has but the moment did not are listed as `live_only` and never touched.
A link is copied only when both ends exist live (an end that was merged follows its head);
the rest are skipped as "endpoint absent", never created as stubs.

APPLY (`apply_restore`) needs the digest of the plan the operator saw: if the live graph moved
since, it refuses and the plan is shown again. Nothing to copy writes nothing, so a repeat
is a no-op."""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from src.orchestrator import time_machine_browse as browse

PROVENANCE_NAME = "restored_from"
PROVENANCE_SOURCE = "time_machine"
PROVENANCE_CONFIDENCE = 0.9
PROVENANCE_EVIDENCE = "self_declared"
LINKS_MAX = 500
_NEVER_COPIED = frozenset({PROVENANCE_NAME})
CONFLICT_CHOICES = ("keep", "restore")


class RestoreError(RuntimeError):
    """A refusal the console can show as is."""


def _jv(text: str) -> Any:
    return json.loads(text)


def _same_confidence(a: float, b: float) -> bool:
    return abs(float(a) - float(b)) < 1e-6


def _iso(when: datetime | None) -> str | None:
    return when.isoformat() if when else None


async def _live_object_id(actions: Any, type_: str, canonical: str) -> uuid.UUID | None:
    row = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type=$1 AND canonical=$2", type_, canonical)
    if row is None:
        return None
    return uuid.UUID(str(await actions.resolve_object_id(row)))


async def _moment_object(moment: Any, ref: str) -> Any:
    found = await browse.resolve_ref(moment, ref)
    if not found:
        raise RestoreError(f"nothing in that moment matches {ref!r}")
    if len(found) > 1:
        raise RestoreError(f"{ref!r} is ambiguous in that moment ({len(found)} objects); "
                           "give a longer id")
    return found[0]


def _classify_assertions(rows: list[Any], live: list[Any]) -> dict[str, Any]:
    copy: list[dict[str, Any]] = []
    skip: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    for a in rows:
        if a["name"] in _NEVER_COPIED:
            continue
        value = _jv(a["vtext"])
        item = {
            "key": f"a:{a['id']}", "name": a["name"], "value": _capped(value),
            "source_id": a["source_id"], "confidence": float(a["confidence"]),
            "observed_at": _iso(a["observed_at"]),
        }
        same_name = [lv for lv in live if lv["name"] == a["name"]]
        same_source = [lv for lv in same_name if lv["source_id"] == a["source_id"]]
        if any(_jv(lv["vtext"]) == value for lv in same_source):
            skip.append({**item, "reason": "live already holds this value from this source"})
            continue
        if same_source:
            cur = same_source[0]
            conflicts.append({**item, "kind": "same_source_changed", "live": _live_view(cur),
                              "choices": list(CONFLICT_CHOICES), "default": "keep"})
            continue
        ties = [lv for lv in same_name
                if _same_confidence(lv["confidence"], a["confidence"])
                and lv["observed_at"] == a["observed_at"] and _jv(lv["vtext"]) != value]
        if ties:
            conflicts.append({**item, "kind": "tie", "live": _live_view(ties[0]),
                              "choices": list(CONFLICT_CHOICES), "default": "keep"})
            continue
        copy.append({**item, "effect": _effect(a, same_name)})
    return {"copy": copy, "skip": skip, "conflicts": conflicts}


def _effect(a: Any, same_name: list[Any]) -> str:
    if not same_name:
        return "new_property"
    top = max(same_name, key=lambda lv: (float(lv["confidence"]), lv["observed_at"], lv["id"]))
    mine = (float(a["confidence"]), a["observed_at"])
    return "becomes_displayed" if mine >= (float(top["confidence"]), top["observed_at"]) \
        else "alongside"


def _live_view(lv: Any) -> dict[str, Any]:
    return {"id": int(lv["id"]), "value": _capped(_jv(lv["vtext"])),
            "source_id": lv["source_id"], "confidence": float(lv["confidence"]),
            "observed_at": _iso(lv["observed_at"])}


def _capped(value: Any) -> Any:
    return browse.text_capped(value)


async def _resolve_end(actions: Any, type_: str, canonical: str) -> tuple[uuid.UUID | None, bool]:
    """The live object for a moment endpoint, and whether a merge was followed to reach it."""
    row = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type=$1 AND canonical=$2", type_, canonical)
    if row is None:
        return None, False
    head = uuid.UUID(str(await actions.resolve_object_id(row)))
    return head, head != row


async def _plan_links(moment: Any, actions: Any, mo: Any, live_id: uuid.UUID | None
                      ) -> dict[str, Any]:
    rows = await moment.fetch(
        "SELECT l.id, l.type, l.properties::text AS ptext, l.source_id, l.confidence, "
        "l.first_seen, l.evidence_class, l.evidence_uri, l.evidence_sha256, l.from_id, "
        "l.to_id, f.type AS ft, f.canonical AS fc, t.type AS tt, t.canonical AS tc "
        "FROM links l JOIN objects f ON f.id = l.from_id JOIN objects t ON t.id = l.to_id "
        "WHERE (l.from_id = $1 OR l.to_id = $1) AND l.valid_until IS NULL "
        "ORDER BY l.id LIMIT $2", mo["id"], LINKS_MAX + 1)
    truncated = len(rows) > LINKS_MAX
    copy: list[dict[str, Any]] = []
    skip: list[dict[str, Any]] = []
    for r in rows[:LINKS_MAX]:
        out = r["from_id"] == mo["id"]
        other = (r["tt"], r["tc"]) if out else (r["ft"], r["fc"])
        item = {"key": f"l:{r['id']}", "type": r["type"],
                "direction": "out" if out else "in",
                "other": {"type": other[0], "canonical": other[1]},
                "source_id": r["source_id"], "confidence": float(r["confidence"])}
        if r["from_id"] == r["to_id"]:
            other_id: uuid.UUID | None = live_id
            followed = False
        else:
            other_id, followed = await _resolve_end(actions, other[0], other[1])
        if other_id is None:
            skip.append({**item, "reason": "endpoint absent live"})
            continue
        if followed:
            item["note"] = "that end was merged live; the link goes to its current head"
        if live_id is not None:
            a, b = (live_id, other_id) if out else (other_id, live_id)
            have = await actions.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3 "
                "AND (valid_until IS NULL OR valid_until > now()) LIMIT 1", a, b, r["type"])
            if have:
                skip.append({**item, "reason": "an active live link already exists"})
                continue
        item["_row"] = r
        item["_other"] = other_id
        copy.append(item)
    return {"copy": copy, "skip": skip, "truncated": truncated}


def _public(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: v for k, v in i.items() if not k.startswith("_")} for i in items]


async def plan_restore(moment: Any, actions: Any, ref: str, *,
                       moment_label: str = "") -> dict[str, Any]:
    """What restoring `ref` from the throwaway `moment` into the live graph WOULD do.
    Writes nothing. `digest` identifies exactly this plan for `apply_restore`."""
    plan, _ = await _plan(moment, actions, ref, moment_label)
    return plan


async def _plan(moment: Any, actions: Any, ref: str, moment_label: str
                ) -> tuple[dict[str, Any], dict[str, Any]]:
    mo = await _moment_object(moment, ref)
    live_id = await _live_object_id(actions, mo["type"], mo["canonical"])
    rows = await moment.fetch(
        "SELECT id, name, value::text AS vtext, source_id, confidence, observed_at, "
        "evidence_class, evidence_uri, evidence_sha256 FROM current_assertions "
        "WHERE object_id = $1 ORDER BY id", mo["id"])
    live = [] if live_id is None else list(await actions.pool.fetch(
        "SELECT id, name, value::text AS vtext, source_id, confidence, observed_at "
        "FROM current_assertions WHERE object_id = $1", live_id))
    buckets = _classify_assertions(list(rows), live)
    names_then = {r["name"] for r in rows}
    live_only = sorted({lv["name"] for lv in live} - names_then - _NEVER_COPIED)
    links = await _plan_links(moment, actions, mo, live_id)
    digest_src = json.dumps({
        "object": [mo["type"], mo["canonical"], str(live_id)],
        "copy": [i["key"] for i in buckets["copy"]],
        "conflicts": [[i["key"], i["live"]["id"]] for i in buckets["conflicts"]],
        "links": [i["key"] for i in links["copy"]],
    }, sort_keys=True)
    plan = {
        "moment": moment_label,
        "object": {"type": mo["type"], "canonical": mo["canonical"],
                   "moment_id": str(mo["id"]), "moment_status": mo["status"],
                   "live_id": str(live_id) if live_id else None,
                   "action": "add_to_existing" if live_id else "create"},
        "assertions": {"copy": buckets["copy"], "skip": buckets["skip"],
                       "conflicts": buckets["conflicts"]},
        "links": {"copy": _public(links["copy"]), "skip": links["skip"],
                  "truncated": links["truncated"]},
        "live_only": live_only,
        "counts": {"copy": len(buckets["copy"]), "skip": len(buckets["skip"]),
                   "conflicts": len(buckets["conflicts"]),
                   "links_copy": len(links["copy"]), "links_skip": len(links["skip"])},
        "digest": hashlib.sha256(digest_src.encode()).hexdigest()[:16],
    }
    return plan, {"mo": mo, "rows": {f"a:{r['id']}": r for r in rows}, "links": links}


async def apply_restore(
    moment: Any, actions: Any, ref: str, *, expect_digest: str, actor: str,
    moment_label: str = "", resolutions: dict[str, str] | None = None, note: str = "",
    link_keys: list[str] | None = None,
) -> dict[str, Any]:
    """Do what the plan with `expect_digest` showed. `resolutions` maps a conflict's key to
    "restore" or "keep"; anything not named is kept. `link_keys` names the planned links to
    add (None adds every planned link, [] adds none): a link live retired on purpose is the
    operator's to bring back or leave. Returns a receipt."""
    resolutions = resolutions or {}
    bad = {k: v for k, v in resolutions.items() if v not in CONFLICT_CHOICES}
    if bad:
        raise RestoreError(f"unknown choice(s) {bad}; use one of {CONFLICT_CHOICES}")
    plan, ctx = await _plan(moment, actions, ref, moment_label)
    if plan["digest"] != expect_digest:
        raise RestoreError("the live graph changed since that plan was shown; "
                           "review the new plan and confirm again")
    conflict_keys = {c["key"] for c in plan["assertions"]["conflicts"]}
    unknown = set(resolutions) - conflict_keys
    if unknown:
        raise RestoreError(f"no such conflict in this plan: {sorted(unknown)}")
    chosen_conflicts = [k for k in conflict_keys if resolutions.get(k) == "restore"]
    a_keys = [c["key"] for c in plan["assertions"]["copy"]] + sorted(chosen_conflicts)
    link_items = ctx["links"]["copy"]
    if link_keys is not None:
        wanted = set(link_keys)
        unknown_links = wanted - {i["key"] for i in link_items}
        if unknown_links:
            raise RestoreError(f"no such planned link: {sorted(unknown_links)}")
        link_items = [i for i in link_items if i["key"] in wanted]
    kept = sorted(k for k in conflict_keys if k not in chosen_conflicts)
    if not a_keys and not link_items and plan["object"]["action"] == "add_to_existing":
        return {"changed": False, "object_id": plan["object"]["live_id"],
                "assertions_copied": 0, "links_copied": 0, "conflicts_kept": kept,
                "digest": plan["digest"]}
    mo = ctx["mo"]
    now = datetime.now(UTC)
    async with actions.atomic() as tx:
        oid = await _live_object_id(actions, mo["type"], mo["canonical"])
        if oid is None:
            oid = await tx.create_or_find_object(mo["type"], mo["canonical"], actor)
        copied_a: list[int] = []
        for key in a_keys:
            r = ctx["rows"][key]
            await tx.assert_property(
                oid, r["name"], _jv(r["vtext"]), r["source_id"], r["observed_at"],
                float(r["confidence"]), evidence_uri=r["evidence_uri"],
                evidence_sha256=r["evidence_sha256"], evidence_class=r["evidence_class"],
                actor=actor)
            copied_a.append(int(r["id"]))
        copied_l: list[int] = []
        for item in link_items:
            r = item["_row"]
            a, b = (oid, item["_other"]) if item["direction"] == "out" \
                else (item["_other"], oid)
            await tx.create_link(
                a, b, r["type"], r["source_id"], r["first_seen"] or now,
                float(r["confidence"]), properties=_jv(r["ptext"]),
                evidence_uri=r["evidence_uri"], evidence_sha256=r["evidence_sha256"],
                evidence_class=r["evidence_class"], actor=actor)
            copied_l.append(int(r["id"]))
        provenance = {
            "moment": moment_label, "moment_object_id": str(mo["id"]), "restored_at": _iso(now),
            "restored_by": actor, "note": note, "assertion_ids_in_moment": copied_a,
            "link_ids_in_moment": copied_l, "conflicts_kept_live": kept,
        }
        await tx.assert_property(
            oid, PROVENANCE_NAME, provenance, PROVENANCE_SOURCE, now,
            PROVENANCE_CONFIDENCE, evidence_class=PROVENANCE_EVIDENCE, actor=actor)
        await tx.record_object_event(oid, "restore", provenance, actor)
    return {"changed": True, "object_id": str(oid), "assertions_copied": len(copied_a),
            "links_copied": len(copied_l), "conflicts_kept": kept, "digest": plan["digest"],
            "created": plan["object"]["action"] == "create"}

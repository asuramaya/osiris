"""MIGRATION 0060, THE THREE CLASSIFICATION LAWS (thread 0af7b202, decision 0d863363's
own "ships mechanically, never a coordinator's hand pass" mandate, #203 to zero):

  (1) OWNER LAW: an owner is a Seat's own canonical or the literal 'operator', nothing
      else. Every open Thread's owner is resolved through `resolve_owner_seat`
      (owner_normalization.py, shared with thread b5ae6773's write-time refusal gate —
      one function, never two copies deriving the same rules twice); an empty owner with
      a `repo` on record resolves via `_coordinating_seat_for_project` directly (`resolve_
      owner_seat` short-circuits an empty string to None before ever reaching its own
      project-coordinator rung). A row NOTHING resolves for (no repo to fall back
      through, or a repo whose own coordinator can't be found either) is left untouched
      -- the law's own text says so explicitly ("never a fold thread"): unlike migration
      0059, this NEVER mints a surfacing Thread for an unresolvable owner.
  (2) KIND LAW: a derived thread is never an obligation -- an already-kinded thread whose
      `summary` carries evidence_class='derived' but kind='obligation' is reclassified to
      'finding' (the law's own stated mapping for a derived thread with no better
      information). A KINDLESS thread (no current `kind` assertion at all -- the same
      population graph_lint's own `kindless-open-thread` check flags) gets a kind from
      its own summary's evidence_class: 'derived' -> 'finding', anything else
      (self_declared, or any other class -- there is no third rule to reach for) ->
      'task'.
  (3) EXPIRY: a derived thread (summary evidence_class='derived'), still open, older than
      30 days (the object's own `created_at` -- the same "observed_at of the thread"
      proxy migration 0058 already established), with no `cites` or `noted_in` link
      touching it in EITHER direction, closes via the real `resolve_thread` door (status
      ='resolved', matching every other closure in this graph -- 'closed' is not a status
      this kernel uses anywhere) with because='expired unclaimed'. Compensating, nothing
      deleted.

Idempotent by construction: the owner/kind writes are ordinary `assert_property` compensating
assertions (a genuinely unchanged re-run costs nothing meaningful -- assert_property's own
same-value-same-source skip, migration 0059's own precedent); expiry's own `resolve_thread`
is safe to re-call on an already-resolved thread by design (its own docstring: re-resolving
is allowed on purpose, not refused) and a resolved thread no longer matches this migration's
own `status='open'` population filter on a second run regardless.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

MIGRATION_SOURCE = "migration:0060_classification_laws"
_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)
_EXPIRY_DAYS = 30
_EXPIRED_BECAUSE = "expired unclaimed"

_OPEN_THREADS_SQL = """
    SELECT o.id, o.canonical, o.created_at,
        (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id
         AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)
        AS owner,
        (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id
         AND a.name='repo' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)
        AS repo,
        (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id
         AND a.name='kind' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)
        AS kind,
        (SELECT a.evidence_class FROM current_assertions a WHERE a.object_id=o.id
         AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)
        AS summary_evidence_class
    FROM objects o
    WHERE o.type='Thread' AND o.status='active'
      AND EXISTS (SELECT 1 FROM current_assertions ca WHERE ca.object_id=o.id
                  AND ca.name='status' AND ca.value #>> '{}' = 'open')
"""


def _kind_for_evidence(evidence_class: str | None) -> str:
    """The law's own two-case mapping. Anything that isn't literally 'derived' -- self_
    declared, or any other class this graph might carry -- has no third rule to reach
    for, so it takes the same door self_declared does: 'task'."""
    return "finding" if evidence_class == "derived" else "task"


async def plan_migration_0060(pool: asyncpg.Pool) -> dict[str, Any]:
    """DRY RUN -- never writes. Every open Thread classified against all three laws at
    once (one scan, one row set)."""
    from src.orchestrator.owner_normalization import (
        _coordinating_seat_for_project,
        resolve_owner_seat,
    )

    rows = await pool.fetch(_OPEN_THREADS_SQL)
    now = datetime.now(UTC)
    owner_resolved: list[dict[str, Any]] = []
    owner_unresolvable: list[dict[str, Any]] = []
    kind_assigned: list[dict[str, Any]] = []
    kind_reclassified: list[dict[str, Any]] = []
    to_expire: list[dict[str, Any]] = []

    for row in rows:
        canonical, repo = row["canonical"], row["repo"]
        owner = (row["owner"] or "").strip()

        # (1) OWNER LAW
        if owner:
            new_owner = await resolve_owner_seat(pool, owner, project=repo)
        elif repo:
            new_owner, _reason = await _coordinating_seat_for_project(pool, repo)
        else:
            new_owner = None
        already_compliant = owner == "operator" or owner.startswith("seat:")
        if new_owner is not None and new_owner != owner:
            owner_resolved.append({"id": row["id"], "thread": canonical,
                                   "current_owner": row["owner"], "new_owner": new_owner})
        elif new_owner is None and not already_compliant:
            owner_unresolvable.append({"thread": canonical, "current_owner": row["owner"],
                                       "repo": repo})

        # (2) KIND LAW
        ec = row["summary_evidence_class"]
        if row["kind"] is None:
            kind_assigned.append({"id": row["id"], "thread": canonical,
                                  "new_kind": _kind_for_evidence(ec)})
        elif row["kind"] == "obligation" and ec == "derived":
            kind_reclassified.append({"id": row["id"], "thread": canonical,
                                      "new_kind": "finding"})

        # (3) EXPIRY
        if (ec == "derived" and row["created_at"] < now - timedelta(days=_EXPIRY_DAYS)):
            has_activity = await pool.fetchval(
                "SELECT 1 FROM links WHERE (from_id=$1 OR to_id=$1) "
                "AND type IN ('cites','noted_in') LIMIT 1", row["id"])
            if not has_activity:
                to_expire.append({"id": row["id"], "thread": canonical})

    return {
        "owner_resolved": owner_resolved, "owner_unresolvable": owner_unresolvable,
        "kind_assigned": kind_assigned, "kind_reclassified": kind_reclassified,
        "to_expire": to_expire, "threads_scanned": len(rows),
    }


async def apply_migration_0060(actions: Actions) -> dict[str, Any]:
    """Applies `plan_migration_0060`'s own plan. Owner/kind: compensating `assert_
    property` writes from `MIGRATION_SOURCE`. Expiry: the real `resolve_thread` door,
    never a hand-written status assertion -- matches every other closure in this graph
    exactly (resolved_in/resolved_because, the closed_by witness edge)."""
    from src.orchestrator.capture import resolve_thread

    now = datetime.now(UTC)
    plan = await plan_migration_0060(actions.pool)

    for entry in plan["owner_resolved"]:
        await actions.assert_property(
            entry["id"], "owner", entry["new_owner"], MIGRATION_SOURCE, now, _CONF,
            evidence_class=_EC)
    for entry in plan["kind_assigned"] + plan["kind_reclassified"]:
        await actions.assert_property(
            entry["id"], "kind", entry["new_kind"], MIGRATION_SOURCE, now, _CONF,
            evidence_class=_EC)
    for entry in plan["to_expire"]:
        await resolve_thread(actions, entry["thread"], because=_EXPIRED_BECAUSE,
                             source=MIGRATION_SOURCE)

    return {
        "owners_resolved": len(plan["owner_resolved"]),
        "owners_left_unresolvable": len(plan["owner_unresolvable"]),
        "kinds_assigned": len(plan["kind_assigned"]),
        "kinds_reclassified": len(plan["kind_reclassified"]),
        "expired": len(plan["to_expire"]),
        "threads_scanned": plan["threads_scanned"],
    }

"""retire_assertion — the cross-source supersede (thread 52911d2a, found diagnosing
b9aa7326): assert_property's own supersession is scoped to the SAME source only ("other
sources' values coexist as the multi-source set") — by design, for legitimate multi-source
corroboration. It leaves exactly one class unreachable: a peer's CORRECTION of another
agent's bad self-declaration can never retire it. Khnum's own correct_agent_house call on
agent:ad1a1cb0-g40-xxiv proved this live (decision d28d1459): the right value ("58") landed
from a different source, the wrong one ("2", self-declared) stayed current, both
simultaneously "current" per current_assertions' own definition (every row nothing else's
supersedes points at) — a reader without an exact ORDER BY confidence DESC, observed_at DESC
LIMIT 1 could still surface the wrong one.

Deliberately NARROW, not a general edit/delete escape hatch: it retires ONE named
assertion, by id, on a caller-named (object, name) — never a bare "whatever's current now",
so a caller must already know exactly which row is wrong (from a diagnosis, never a guess).
`because` is required: a cross-source retirement crosses accountability lines, so the
justification is not optional the way assert_property's own routine supersession isn't."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.actions.core import ActionError, Actions
from src.orchestrator.compositions import resolve_ref
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for


async def retire_assertion(
    actions: Actions, *, ref: str, name: str, superseded_id: int, value: Any,
    because: str, actor: str,
) -> dict[str, Any]:
    """Retire assertion `superseded_id` on the object `ref` resolves to (any form
    resolve_ref accepts — UUID, short-id, canonical, or name), asserting `value` as the new
    current fact from `actor`. Refuses LOUDLY (an error dict, nothing written) when:
    `because` is blank; `ref` doesn't resolve; `superseded_id` isn't a `name` assertion on
    that object; it's already superseded by something else."""
    because = (because or "").strip()
    if not because:
        return {"error": "because is required — a cross-source retirement must carry its "
                         "justification, not just a value"}
    name = (name or "").strip()
    if not name:
        return {"error": "name is required"}
    pool = actions.pool
    object_id = await resolve_ref(pool, ref)
    if object_id is None:
        return {"error": f"no object matches {ref!r}"}
    target = await pool.fetchrow(
        "SELECT id, value, source_id FROM assertions WHERE id=$1 AND object_id=$2 AND name=$3",
        superseded_id, object_id, name)
    if target is None:
        return {"error": f"assertion {superseded_id} is not a {name!r} assertion on "
                         f"{ref!r} — check the id and the property name"}
    already = await pool.fetchval(
        "SELECT 1 FROM assertions WHERE supersedes=$1", superseded_id)
    if already:
        return {"error": f"assertion {superseded_id} is already superseded — nothing to "
                         "retire"}
    now = datetime.now(UTC)
    ec = EvidenceClass.SELF_DECLARED
    try:
        new_id = await actions.supersede_assertion(
            object_id, name, superseded_id, value, actor, now, confidence_for(ec), because,
            evidence_class=ec.value, actor=actor)
    except ActionError as exc:
        return {"error": str(exc)}
    return {
        "retired": {"id": superseded_id, "value": target["value"], "source": target["source_id"]},
        "now_current": {"id": new_id, "value": value, "source": actor},
        "because": because,
    }

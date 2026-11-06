"""A heir minted onto a numeral that was healed moments ago is born un-flagged.

The 2026-09-23 specimen (generation xxvii): a debounce healed the numeral (false_mint +
retired + the ancestor's pointer unwound, one transaction), and 14 seconds later the next
mint adopted that same numeral, as it is meant to inside the debounce window. The heal's
stamps stayed on the now-real heir, so the detector opened a "half-healed phantom" that no
heal could complete, and the heir read as a retired identity on its next registration."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from src.actions.core import Actions
from src.orchestrator.agents import mint_heir
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for


async def _prop(actions: Actions, canonical: str, name: str) -> str | None:
    return await actions.pool.fetchval(  # type: ignore[no-any-return]
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name=$2 ORDER BY a.confidence DESC, a.observed_at DESC "
        "LIMIT 1", canonical, name)


async def _healed_numeral(actions: Actions, ancestor: str, at: datetime) -> str:
    """The ancestor and the numeral after it, the latter healed (flagged false_mint and
    retired) at `at`, the way the debounce leaves it."""
    anc_oid = await actions.create_or_find_object("Agent", ancestor, "test")
    heir = ancestor + "-ii"
    heir_oid = await actions.create_or_find_object("Agent", heir, "test")
    do = EvidenceClass.DIRECT_OBSERVATION
    for k, v in (("false_mint", True), ("retired", True), ("retired_by", "seam-debounce")):
        await actions.assert_property(heir_oid, k, v, "seam-debounce", at,
                                      confidence_for(do), evidence_class=do.value)
    del anc_oid
    return heir


async def test_a_numeral_healed_seconds_ago_is_adopted_un_flagged(actions: Actions) -> None:
    ancestor = "agent:adopt0001"
    heir = await _healed_numeral(actions, ancestor, datetime.now(UTC) - timedelta(seconds=14))
    anc_oid = await actions.create_or_find_object("Agent", ancestor, "test")

    got, got_oid = await mint_heir(actions, ancestor, anc_oid, because="compaction",
                                   succession=None)

    assert got == heir  # inside the window the numeral is the transition's own
    assert await _prop(actions, heir, "false_mint") == "false"
    assert await _prop(actions, heir, "retired") == "false"
    assert "adopted by a real heir" in str(await _prop(actions, heir, "reinstated_because"))
    assert await _prop(actions, heir, "succeeded_from") == ancestor
    assert await _prop(actions, ancestor, "succeeded_by") == heir
    del got_oid


async def test_a_numeral_healed_long_ago_is_still_stepped_past_and_stays_flagged(
    actions: Actions,
) -> None:
    ancestor = "agent:adopt0002"
    old = await _healed_numeral(actions, ancestor, datetime.now(UTC) - timedelta(hours=20))
    anc_oid = await actions.create_or_find_object("Agent", ancestor, "test")

    got, _oid = await mint_heir(actions, ancestor, anc_oid, because="compaction",
                                succession=None)

    assert got != old and got.startswith(ancestor + "-")
    assert await _prop(actions, old, "false_mint") == "true"   # a closed one-way route
    assert await _prop(actions, old, "retired") == "true"
    assert await _prop(actions, got, "false_mint") in (None, "false")


async def test_an_ordinary_mint_writes_no_reinstatement(actions: Actions) -> None:
    ancestor = "agent:adopt0003"
    anc_oid = await actions.create_or_find_object("Agent", ancestor, "test")
    got, _oid = await mint_heir(actions, ancestor, anc_oid, because="compaction",
                                succession=None)
    assert await _prop(actions, got, "reinstated_because") is None
    assert await _prop(actions, got, "false_mint") is None


async def test_the_husk_heal_is_one_transaction_a_failed_unwind_leaves_no_flag(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The flag stamps and the pointer unwind land together or not at all: an interruption
    between them is what used to leave a flagged phantom still pointed at by its ancestor."""
    from src.orchestrator.heal import heal_husks

    from tests.test_onboarding_seams import FABLE, OPUS, _acts

    a = await actions.create_or_find_object("Agent", "agent:ea570099", "test")
    await _acts(actions, "agent:ea570099")
    _, ii_oid = await mint_heir(actions, "agent:ea570099", a, because="compaction",
                                succession=None)
    await mint_heir(actions, "agent:ea570099-ii", ii_oid, because="live-swap",
                    succession=f"{OPUS} → {FABLE}")

    real = Actions.assert_property

    async def _flaky(self: Actions, oid: object, name: str, value: object, *args: object,
                     **kw: object) -> object:
        if name == "succeeded_by" and value == "":
            raise RuntimeError("interrupted before the unwind")
        return await real(self, oid, name, value, *args, **kw)  # type: ignore[arg-type]

    monkeypatch.setattr(Actions, "assert_property", _flaky)
    with pytest.raises(RuntimeError, match="interrupted"):
        await heal_husks(actions, ["agent:ea570099-ii", "agent:ea570099-iii"], apply=True)
    monkeypatch.setattr(Actions, "assert_property", real)

    for husk in ("agent:ea570099-ii", "agent:ea570099-iii"):
        assert await _prop(actions, husk, "false_mint") is None, "a flag landed without its unwind"
    assert await _prop(actions, "agent:ea570099", "succeeded_by") == "agent:ea570099-ii"

    out = await heal_husks(actions, ["agent:ea570099-ii", "agent:ea570099-iii"], apply=True)
    assert out["applied"] is True
    assert await _prop(actions, "agent:ea570099-iii", "false_mint") == "true"
    assert await _prop(actions, "agent:ea570099", "succeeded_by") in ("", None)

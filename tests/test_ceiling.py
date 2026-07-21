"""THE DAILY CEILING — what Osiris may SPEND before it stops.

    "nobody will touch this if it burns."                            — the operator, 2026-07-13

Every catastrophe in this system's life was a spend catastrophe: a miner that walked every
transcript forever, a trigger that minted 463 real Claude sessions on abandoned projects, a worker
that wedged itself with ten `claude -p` children. In every case NOBODY WAS COUNTING.

The adversary already had a YIELD floor ("is this producer any good?"). Nothing anywhere answered
the only question the operator actually asked: CAN HE AFFORD IT?

The two tests that matter most here are test_the_gate_CAN_OPEN (Thoth XXVIII shipped one that
could not — "a gate that can never open is a kill switch wearing a gate's clothes") and
test_an_UNPRICED_call_is_not_a_FREE_call (an invisible spend is worse than a large one, and
scoring it $0 is precisely how the ghost farm ran for a week).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from src.actions.core import Actions
from src.ingest.providers import Usage, spend_is_metered
from src.ingest.usage import record_usage
from src.orchestrator.ceiling import DEFAULT_DAILY_USD, ceiling, may_spend


async def _spend(actions: Actions, usd: float | None, *, ago_h: float = 0.0) -> None:
    await record_usage(
        actions.pool, purpose="session-adversary",
        usage=Usage(model="claude-haiku-4-5-20251001", input_tokens=10, output_tokens=100,
                    cache_read_tokens=0, cache_creation_tokens=0, cost_usd=usd),
        ran_at=datetime.now(UTC) - timedelta(hours=ago_h) if ago_h else None)


async def test_an_empty_ledger_may_spend(actions: Actions) -> None:
    ok, why = await may_spend(actions.pool)
    assert ok
    assert "$0.00" in why


async def test_the_ceiling_STOPS_a_runaway(actions: Actions) -> None:
    for _ in range(12):
        await _spend(actions, 1.00)
    ok, why = await may_spend(actions.pool, cap=10.0)
    assert not ok
    assert "CEILING REACHED" in why and "$12.00" in why


async def test_the_gate_CAN_OPEN(actions: Actions) -> None:
    """THE MISTAKE I REFUSE TO REPEAT. Thoth XXVIII shipped a licence gate that was a PERMANENT
    LOCKOUT: it judged a new producer on the OLD one's yield, over rows the new one could never
    have written, so the number it demanded could not be raised BY ANY ACTION THE PRODUCER COULD
    TAKE. He deployed it, ran it, and it refused. "A GATE THAT CAN NEVER OPEN IS A KILL SWITCH
    WEARING A GATE'S CLOTHES."

    This ceiling reads a ROLLING WINDOW over spend that actually happened. It therefore DRAINS on
    its own, with no intervention, no reset job, and no state to wedge in. Yesterday's blowout
    does not sentence you to a dead system today.
    """
    for _ in range(12):
        await _spend(actions, 1.00, ago_h=30)      # a blowout — but it was YESTERDAY
    ok, why = await may_spend(actions.pool, cap=10.0)
    assert ok, "the window did not roll — this gate is a lockout"
    assert "$0.00" in why


async def test_an_UNPRICED_call_is_not_a_FREE_call(actions: Actions) -> None:
    """AN INVISIBLE SPEND IS WORSE THAN A LARGE ONE, because the ceiling waves it through.

    That is not hypothetical: it is 463 wakes. The spawner pointed the CLI's stdout at /dev/null,
    so the vendor's own price for each session went in the bin, and every one of them landed in
    the ledger with cost_usd = NULL. Quietly summing those as $0 would let a fortune pass a gate
    built to stop exactly that. So blindness is counted SEPARATELY and reported LOUDLY, and it is
    never allowed to masquerade as thrift.
    """
    await _spend(actions, None)
    await _spend(actions, None)
    await _spend(actions, 1.00)
    c = await ceiling(actions.pool, cap=10.0)
    assert c.spent == 1.00, "an unpriced call must not be summed as zero"
    assert c.blind == 2
    _, why = await may_spend(actions.pool, cap=10.0)
    assert "NO PRICE" in why and "INVISIBLE" in why
    assert "cannot price itself may not spend" in why


def test_spend_is_metered_only_on_the_KEYED_api_backend() -> None:
    """THE CEILING IS LIVE ONLY WHEN A DOLLAR IS ACTUALLY CHARGED (Thoth LIII 2026-07-21).

    On the local Claude CLI (a subscription) the envelope's total_cost_usd is a notional number
    the vendor prints, not a debit — so metering it is a category error. Metered is true only on
    the keyed API path, which is exactly the decision llm_provider() already makes from config;
    spend_is_metered asks it rather than re-guessing."""
    api = SimpleNamespace(osiris_extract_provider="anthropic",
                          osiris_claude_binary="claude", anthropic_api_key="sk-x")
    assert spend_is_metered(api) is True

    keyless = SimpleNamespace(osiris_extract_provider="anthropic",
                              osiris_claude_binary="/no/such/claude", anthropic_api_key="")
    assert spend_is_metered(keyless) is False   # 'anthropic' with no key resolves to no backend


async def test_a_SUBSCRIPTION_makes_the_gate_INERT(actions: Actions) -> None:
    """THE '$12/$10' FALSE STOP, REMOVED. On a subscription the CLI cost is notional, not a
    charge, so a ledger far over any cap must STILL pass — the gate was halting real work on
    imaginary money. may_spend(metered=False) never refuses and says why; and the SAME ledger
    with metered=True still stops, so the gate itself is intact, only correctly scoped."""
    for _ in range(50):
        await _spend(actions, 10.00)                 # $500 of NOTIONAL cost — never charged
    ok, why = await may_spend(actions.pool, cap=10.0, metered=False)
    assert ok, "the ceiling false-stopped on money that was never charged"
    assert "subscription" in why and "not billed" in why

    stopped, _ = await may_spend(actions.pool, cap=10.0, metered=True)
    assert not stopped, "the billed-path gate must still bite over the same ledger"


async def test_a_cap_of_zero_is_an_HONEST_kill_switch(actions: Actions) -> None:
    """It is allowed to be a kill switch. It is not allowed to PRETEND to be a gate."""
    ok, why = await may_spend(actions.pool, cap=0.0)
    assert not ok
    assert "kill switch, and it is named as one" in why


async def test_a_NEGATIVE_cap_is_the_operators_deliberate_no_net(actions: Actions) -> None:
    """The operator may choose to run uncapped. He may not do it BY ACCIDENT — an unset config
    lands on DEFAULT_DAILY_USD, never on infinity."""
    for _ in range(50):
        await _spend(actions, 10.00)
    ok, why = await may_spend(actions.pool, cap=-1.0)
    assert ok
    assert "NO CEILING" in why and "explicit choice" in why
    assert DEFAULT_DAILY_USD > 0, "the DEFAULT must never be 'unlimited'"


async def test_the_ledger_is_dated_by_the_EVENT_not_the_BOOKKEEPING(actions: Actions) -> None:
    """A BACKFILL MUST NOT LOOK LIKE A SPREE.

    The wake meter read 257 historical wakes off disk in ONE PASS, and record_usage defaulted to
    now() — filing a week of spending under a single day. Nobody noticed, because nobody was
    counting. The moment a daily ceiling reads this table that becomes fatal: it would refuse to
    spend a cent on a day that had actually cost nothing, starving a producer over an accountant's
    clerical error.
    """
    for _ in range(20):
        await _spend(actions, 1.00, ago_h=48)     # metered TODAY, but SPENT two days ago
    c = await ceiling(actions.pool, cap=10.0)
    assert c.spent == 0.0, "a backfill was counted as today's spend"
    assert (await may_spend(actions.pool, cap=10.0))[0]


async def test_the_refusal_EXPLAINS_ITSELF(actions: Actions) -> None:
    """A refusal that cannot say why gets overridden by the next person in a hurry, and then it
    protects nobody. Never a bare boolean."""
    for _ in range(12):
        await _spend(actions, 1.00)
    ok, why = await may_spend(actions.pool, cap=10.0)
    assert not ok
    assert "$12.00" in why and "$10.00" in why and "24h" in why

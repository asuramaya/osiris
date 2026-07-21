"""THE DAILY CEILING — what Osiris may SPEND before it stops.

(Not to be confused with budget.py, which bounds how many CHARACTERS a tool may hand back. That
one protects the reader's context; this one protects the operator's card.)

    "my plan is about to reset, not catastrophic but still damning for the application,
     nobody will touch this if it burns."                            — the operator, 2026-07-13

Every catastrophe in this system's life has been a SPEND catastrophe wearing a memory costume: a
miner that walked every transcript forever; a trigger that minted 463 real Claude sessions on
projects nobody had opened in days; a worker that wedged itself with ten 290MB `claude -p`
children. In every single case the same thing was true — NOBODY WAS COUNTING.

The adversary already has a YIELD FLOOR (dispose.licence: "is this producer any good?"). That is
a different question from the only one the operator actually asked, which is: CAN HE AFFORD IT? A
producer can be excellent and still ruinous. Nothing anywhere said "Osiris may spend $X today and
then it stops." This is that.

═══ IT GATES ON MEASURED DOLLARS, NEVER ON A GUESS.

`cost_usd` was never a price table — it is `total_cost_usd`, printed by the CLI in its own output
envelope. The vendor tells us what each call cost, for free, on every call. That is why the
miner's $40.49 is exact to the cent, and it is why this gate needs no rate card, no price feed,
and no maintenance: it cannot drift, because it never estimates.

I tried the other way first. Fitting a rate card against 1,376 rows of real billing gave a 0.1%
residual and A NEGATIVE PRICE PER INPUT TOKEN (−$1,457/Mtok). The fit was excellent; the model was
nonsense. A GOOD FIT IS NOT A CORRECT MODEL — and a low residual is the most persuasive costume an
inference can wear while impersonating a fact.

    A PRODUCER THAT CANNOT PRICE ITSELF MAY NOT SPEND.

Hence `blind`. A call recorded with a NULL cost is not a cheap call — it is an INVISIBLE one, and
that is strictly worse, because the ceiling cannot see it and will wave a fortune straight
through. The ghost farm was 463 spawns of exactly that kind. Blindness is reported LOUDLY, on its
own, and is never quietly counted as zero.

═══ AND IT MUST BE ABLE TO OPEN.

Thoth XXVIII shipped a licence gate that could never pass: it judged a new producer on the OLD
one's yield, over rows the new one could not have written. "A GATE THAT CAN NEVER OPEN IS A KILL
SWITCH WEARING A GATE'S CLOTHES." So this ceiling is a ROLLING window over spend that actually
happened — it drains as the clock moves, it has no state to wedge in, and it cannot lock the
system out forever. cap = 0 means STOP (an honest kill switch, named as one); cap < 0 means
UNLIMITED (the operator's deliberate choice to run without a net).
"""

from __future__ import annotations

from dataclasses import dataclass

import asyncpg

# What Osiris has visibly cost, PER DAY, across its entire life (2026-07-05 .. 07-13):
#   $1.47  $4.80  $3.05  $3.59  $2.52  $7.38  $11.89  $3.74      median ~$3.60, peak $11.89
# Nobody had ever run that query. A $10 ceiling is therefore "a busy day, and no worse": it would
# not have blocked a single day of honest work in this system's history, and it would have caught
# every runaway long before it became a story. A DEFAULT, not a doctrine — the operator's to
# raise, lower, or switch off entirely.
DEFAULT_DAILY_USD = 10.0

WINDOW_HOURS = 24


@dataclass(frozen=True)
class Ceiling:
    """What the gate knows right now — and, in `blind`, what it CANNOT know and says so."""

    cap: float          # < 0 = no ceiling (deliberate); 0 = stopped; > 0 = the daily allowance
    spent: float        # MEASURED dollars in the window — the vendor's own figure, never a guess
    blind: int          # calls in the window that recorded NO price: spend nobody can see

    @property
    def unlimited(self) -> bool:
        return self.cap < 0

    @property
    def remaining(self) -> float:
        return float("inf") if self.unlimited else max(0.0, self.cap - self.spent)

    @property
    def may_spend(self) -> bool:
        return self.unlimited or self.spent < self.cap

    def why(self) -> str:
        """One line a human can act on. NEVER a bare boolean: a refusal that cannot explain
        itself gets overridden by the next person in a hurry, and then it protects nobody."""
        if self.unlimited:
            out = f"NO CEILING (cap < 0 — the operator's explicit choice). ${self.spent:.2f} spent"
        elif self.cap == 0:
            out = "STOPPED — the ceiling is 0. That is a kill switch, and it is named as one"
        elif self.may_spend:
            out = (f"${self.spent:.2f} of ${self.cap:.2f} spent in {WINDOW_HOURS}h — "
                   f"${self.remaining:.2f} left")
        else:
            out = (f"CEILING REACHED — ${self.spent:.2f} of ${self.cap:.2f} in {WINDOW_HOURS}h. "
                   f"Osiris stops spending until the window rolls forward")
        if self.blind:
            # NOT folded into `spent`. An unpriced call is not a cheap call, it is an unseen one,
            # and silently scoring it $0 is precisely how the ghost farm ran for a week.
            out += (f" · ⚠ {self.blind} call(s) recorded NO PRICE — that spend is INVISIBLE to "
                    f"this ceiling. A producer that cannot price itself may not spend")
        return out


async def ceiling(pool: asyncpg.Pool, *, cap: float = DEFAULT_DAILY_USD) -> Ceiling:
    """Read the ceiling off the ledger. A rolling window, so it always drains and never wedges."""
    row = await pool.fetchrow(
        "SELECT coalesce(sum(cost_usd), 0) AS spent, "
        "       count(*) FILTER (WHERE cost_usd IS NULL) AS blind "
        "FROM llm_usage WHERE ran_at > now() - make_interval(hours => $1)", WINDOW_HOURS)
    return Ceiling(cap=cap, spent=float(row["spent"] or 0.0), blind=int(row["blind"] or 0))


async def may_spend(
    pool: asyncpg.Pool, *, cap: float = DEFAULT_DAILY_USD, metered: bool = True,
) -> tuple[bool, str]:
    """THE GATE. Call it before any paid inference; obey what it says.

    `metered` says whether that inference is BILLED PER CALL — the keyed API path (ask
    providers.spend_is_metered(); it reads the live backend). On a SUBSCRIPTION (the local Claude
    CLI) the vendor's `total_cost_usd` is a notional figure, not a debit, so summing it and gating
    on it stops real work on imaginary money. When the spend is NOT metered this gate is INERT: it
    never refuses, and it says so. It defaults True so a caller that forgets fails toward
    enforcement, never toward an unbounded spree — the historically safe direction.

    FAILS OPEN on an unreadable ledger, and that is deliberate. If Postgres is down, Osiris has no
    graph to write to and no work worth doing — the ceiling is not what is protecting anyone in
    that moment, while a gate that SLAMS on its own read error is a system that bricks itself over
    a hiccup. The bound that actually matters is on the ledger being WRITTEN, not on it being
    readable, and an unpriced producer is refused AT THE PRODUCER — a place no database outage can
    reach.
    """
    if not metered:
        return True, ("subscription — inference runs on the local Claude CLI, not billed per "
                      "call; the dollar ceiling does not apply (spend_is_metered=False)")
    try:
        c = await ceiling(pool, cap=cap)
    except Exception:  # noqa: BLE001 — an unreadable ledger must never brick the fleet
        return True, "ceiling UNKNOWN (the ledger could not be read) — proceeding, and saying so"
    return c.may_spend, c.why()

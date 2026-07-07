"""Swap-detector — the silent demotion made a first-class event (ruling f2ae6346).

The fable harness demotes fable→opus SILENTLY when it senses danger. Observation (reading the
model off the transcript) captures the model that ANSWERED — but is blind to the SWAP ITSELF: we
record the effect, never the event. The swap is visible only two ways, and this classifies both:

  * DIVERGENCE FROM INTENT — the observed model != the operator's standing choice (`expected`). A
    session that STARTED already-demoted (its whole transcript is opus, no transition) shows ONLY
    here — the case the miner's within-session detector misses entirely.
  * WITHIN-SESSION TRANSITION — >1 model across the transcript: the model flipped mid-run, a warm
    rug-pull the running agent can't feel (its system prompt keeps asserting the old identity).

Either is a DANGER-SENSE TRIPWIRE — an opus response in a fable seat means the harness got nervous
*here*. This classifier is pure (testable in isolation); mount()/orient() surface its banner as the
confession backstop the cold-boot ritual can never be, and register_agent stamps it on the Agent.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class SwapVerdict:
    """A model-swap reading. `swapped` is the aggregate tripwire — either divergence from the
    operator's intent or a within-session transition means a silent demotion occurred."""

    observed: str | None       # the model that actually ran (the latest assistant turn)
    expected: str              # the operator's standing choice (the intent)
    history: tuple[str, ...]   # distinct models across the transcript, first-seen order
    diverged_from_intent: bool  # observed != expected: running a model the operator didn't choose
    within_session: bool        # the transcript holds >1 model: it flipped mid-run
    from_model: str | None      # the swap's source (history[0], or the intent for a cold demotion)
    to_model: str | None        # the swap's destination (the observed model)

    @property
    def swapped(self) -> bool:
        return self.diverged_from_intent or self.within_session


def classify_swap(
    history: Sequence[str], observed: str | None, *, expected: str
) -> SwapVerdict:
    """Read both swap signals off a session's model history + its current (observed) model,
    against the operator's `expected` standing choice. Pure — the IO (probing the transcript for
    `history`/`observed`) happens upstream in resolve_identity / the miner."""
    hist = tuple(history)
    within = len(hist) > 1
    diverged = observed is not None and observed != expected
    from_model: str | None = None
    to_model: str | None = None
    if within:
        from_model, to_model = hist[0], hist[-1]
    elif diverged:
        # a COLD demotion: no transition on record, so the swap predates the transcript — the
        # intent is the only witness to where it started.
        from_model, to_model = expected, observed
    return SwapVerdict(
        observed=observed, expected=expected, history=hist,
        diverged_from_intent=diverged, within_session=within,
        from_model=from_model, to_model=to_model,
    )


def swap_banner(v: SwapVerdict) -> str | None:
    """A one-line confession prompt for mount()/orient(), or None when nothing swapped. The
    running agent can't feel the swap; this is the graph telling it what its own prompt hides."""
    if not v.swapped:
        return None
    if v.within_session:
        chain = " → ".join(v.history)
        return (f"⚠ warm model swap THIS session ({chain}) — the harness demoted mid-run; "
                "a danger-sense tripwire. Confess it to the operator.")
    return (f"⚠ model divergence: intended {v.expected}, running {v.observed} — a silent demotion "
            "(the harness swapped before this session's first turn). Confess it to the operator.")

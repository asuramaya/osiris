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
        # origin → CURRENT (the observed/latest turn), NOT the last-SEEN-distinct model: under
        # oscillation (opus→fable→opus) the last-seen-distinct is fable while the current is opus,
        # so hist[-1] lied. `observed` is always the truth of "what is running now".
        from_model, to_model = hist[0], observed
    elif diverged:
        # a COLD demotion: no transition on record, so the swap predates the transcript — the
        # intent is the only witness to where it started.
        from_model, to_model = expected, observed
    return SwapVerdict(
        observed=observed, expected=expected, history=hist,
        diverged_from_intent=diverged, within_session=within,
        from_model=from_model, to_model=to_model,
    )


def swap_marker(v: SwapVerdict) -> str:
    """The honest one-line marker (the model_swapped stamp). Under OSCILLATION a session flips back
    and forth, so a single `a → b` arrow lies — it printed 'opus → fable' when the CURRENT model
    was opus. The within-session form names the models SEEN and the current one; the cold-demotion
    form (no in-session transition) keeps the clean intent → observed arrow."""
    if v.within_session:
        return f"{' ↔ '.join(v.history)} (now {v.observed})"
    return f"{v.from_model} → {v.to_model}"


def swap_banner(v: SwapVerdict) -> str | None:
    """A one-line confession prompt for mount()/orient(), or None when nothing swapped. The
    running agent can't feel the swap; this is the graph telling it what its own prompt hides. A
    within-session swap ALSO names the intent when the current model diverges from it — otherwise
    the compound case ('flipped mid-run AND currently off-intent') hides that you're not running
    what the operator chose."""
    if not v.swapped:
        return None
    cur = v.observed or "unknown"
    if v.within_session:
        intent = (f" — NOT your intended {v.expected}" if v.diverged_from_intent
                  else f" (back on your intended {v.expected})")
        return (f"⚠ warm model swap THIS session — seen [{', '.join(v.history)}], currently "
                f"{cur}{intent}. The harness swapped mid-run (a danger-sense tripwire); confess "
                "it to the operator.")
    return (f"⚠ model divergence: intended {v.expected}, running {cur} — a silent demotion "
            "(the harness swapped before this session's first turn). Confess it to the operator.")

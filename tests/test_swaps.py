"""Swap-detector — the silent fable→opus demotion (on danger-sense) made a first-class event.

Observation captures the model that ANSWERED but is blind to the swap itself; classify_swap reads
both signals off the history + observed model — divergence from the operator's intent, and a
within-session transition. Pure, so these are pure. (ruling f2ae6346)
"""
from __future__ import annotations

from src.orchestrator.swaps import classify_swap, swap_banner, swap_marker

FABLE = "claude-fable-5"
OPUS = "claude-opus-4-8"


def test_no_swap_when_observed_matches_intent() -> None:
    v = classify_swap([FABLE], FABLE, expected=FABLE)
    assert v.swapped is False
    assert v.diverged_from_intent is False and v.within_session is False
    assert swap_banner(v) is None


def test_within_session_transition_is_a_swap() -> None:
    v = classify_swap([FABLE, OPUS], OPUS, expected=FABLE)
    assert v.within_session is True and v.swapped is True
    assert v.from_model == FABLE and v.to_model == OPUS  # origin → CURRENT (observed)
    banner = swap_banner(v)
    assert banner is not None
    assert "warm model swap" in banner and FABLE in banner and f"currently {OPUS}" in banner


def test_oscillation_is_labelled_by_current_not_last_seen() -> None:
    # the dogfooding bug: a session that went opus→fable→opus is CURRENTLY opus. The old label
    # (history[0] → history[-1]) printed 'opus → fable'; now to_model tracks the observed current.
    v = classify_swap([OPUS, FABLE], OPUS, expected=FABLE)
    assert v.within_session is True and v.swapped is True
    assert v.to_model == OPUS                          # the CURRENT model, not the last-seen fable
    assert swap_marker(v) == f"{OPUS} ↔ {FABLE} (now {OPUS})"
    banner = swap_banner(v)
    assert banner is not None and f"currently {OPUS}" in banner
    assert f"{OPUS} → {FABLE}" not in banner           # never the misleading one-way arrow


def test_cold_demotion_diverges_from_intent_without_a_transition() -> None:
    # the whole transcript is opus — the swap predates it, so there's no in-session transition,
    # only divergence from the intended fable. This is the case _record_swap misses entirely.
    v = classify_swap([OPUS], OPUS, expected=FABLE)
    assert v.within_session is False
    assert v.diverged_from_intent is True and v.swapped is True
    assert v.from_model == FABLE and v.to_model == OPUS  # intent is the only witness to the source
    banner = swap_banner(v)
    assert banner is not None
    assert "divergence" in banner and "silent demotion" in banner


def test_no_history_still_flags_divergence() -> None:
    # the cwd-fallback path yields no history; divergence still fires on observed vs expected
    v = classify_swap([], OPUS, expected=FABLE)
    assert v.diverged_from_intent is True and v.within_session is False and v.swapped is True


def test_unknown_observed_is_not_a_false_swap() -> None:
    # nothing observed (no transcript) → we cannot claim a swap
    v = classify_swap([], None, expected=FABLE)
    assert v.swapped is False and swap_banner(v) is None

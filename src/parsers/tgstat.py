"""tgstat-style Telegram channel behavior parser (windowed, DESIGN §11).

Per window it APPENDS an ObservedData (a distinct per-window object = immutable
evidence) and re-asserts a rolling behavioral assessment as a property on a
STABLE Campaign object — so the existing within-source supersession keeps only
the latest judgment current while every window's evidence is retained.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.parsers.base import InputObject, LinkSpec, ObjectSpec, ParseResult, TargetRef


def parse_tgstat_behavior(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    channel = input_object.canonical
    window_start = str(response.get("window_start", ""))

    # APPEND: one ObservedData per window (distinct canonical => new object)
    observation = f"tgstat-window:{channel}:{window_start}"
    result.objects.append(
        ObjectSpec(
            type="ObservedData",
            canonical=observation,
            confidence=0.9,
            properties={
                "window_start": window_start,
                "post_count": response.get("post_count"),
                "forward_count": response.get("forward_count"),
            },
            evidence=response,
        )
    )

    # SUPERSEDE: rolling assessment on a stable Campaign object. Re-asserting the
    # same (object, name, source) each window auto-supersedes the prior window.
    campaign = f"campaign:tgstat:{channel}"
    result.objects.append(
        ObjectSpec(
            type="Campaign",
            canonical=campaign,
            confidence=0.5,
            properties={"behavior_confidence": response.get("campaign_confidence", 0.4)},
        )
    )
    result.links.append(
        LinkSpec(TargetRef(ref=campaign), TargetRef(ref=observation), "has_observation", 0.9)
    )
    result.links.append(
        LinkSpec(TargetRef(ref=campaign), TargetRef(input=True), "targets", 0.5)
    )
    return result

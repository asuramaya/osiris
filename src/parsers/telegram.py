"""Telegram channel profile parser (gated helper).

Runs on the result the analyst's browser posts back after visiting the channel
(Telegram throttles/needs a session, so this is tier=gated). Asserts profile
properties on the TelegramChannel and records the scrape as ObservedData — the
custom object exports later as STIX observed-data.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.parsers.base import InputObject, LinkSpec, ObjectSpec, ParseResult, TargetRef


def parse_telegram_channel(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    channel = input_object.canonical

    result.objects.append(
        ObjectSpec(
            type="TelegramChannel",
            canonical=channel,
            confidence=0.8,
            properties={
                "title": response.get("title"),
                "subscribers": response.get("subscribers"),
                "description": response.get("description"),
            },
        )
    )
    snapshot = f"tg-snapshot:{channel}"
    result.objects.append(
        ObjectSpec(
            type="ObservedData",
            canonical=snapshot,
            confidence=0.99,
            properties={"source": "telegram", "channel": channel},
            evidence=response,
        )
    )
    result.links.append(
        LinkSpec(
            from_ref=TargetRef(ref=channel),
            to_ref=TargetRef(ref=snapshot),
            type="has_observation",
            confidence=0.9,
        )
    )
    return result

"""No-op parser for suggest-tier (osint4all augmentation) helpers.

Suggest helpers are link-outs the analyst opens — there's no structured response
to parse. If a handoff is posted back anyway, we emit nothing; the analyst
promotes findings explicitly via intake/federate (ruling #6, human-gated).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.parsers.base import InputObject, ParseResult


def parse_suggest_noop(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    return ParseResult(observed_at=datetime.now(UTC))

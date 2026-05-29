"""Local derivation connectors — pure transforms, no network.

These exist so a derivation can run through the normal helper/cascade machinery
(claim, audit, cascade). The connector just echoes the selector; the parser does
the transform.
"""

from __future__ import annotations

from typing import Any

from src.parsers.base import InputObject


async def echo(input_object: InputObject) -> dict[str, Any]:
    return {"canonical": input_object.canonical, "type": input_object.type}

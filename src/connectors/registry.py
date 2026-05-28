"""Connector registry: helper id -> async fetch function.

The connector is the *network seam*: given the input object, it returns the raw
source response the parser will interpret. Keeping it behind a registry lets the
cascade dispatch helpers generically, and lets tests inject deterministic
responses without hitting the network (the same boundary fixtures stub).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from src.connectors.crtsh import fetch_crtsh
from src.parsers.base import InputObject

Connector = Callable[[InputObject], Awaitable[dict[str, Any]]]

CONNECTORS: dict[str, Connector] = {
    "crtsh_subdomains": fetch_crtsh,
    # threatfox is request/response with args -> wired when its tier/cascade lands
}


def get_connector(helper_id: str) -> Connector:
    try:
        return CONNECTORS[helper_id]
    except KeyError:
        raise KeyError(f"no connector registered for {helper_id!r}") from None

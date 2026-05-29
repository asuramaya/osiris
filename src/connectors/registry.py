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
from src.connectors.derive import echo
from src.connectors.github import fetch_github_user
from src.connectors.github_deep import fetch_github_deep
from src.connectors.gravatar import fetch_gravatar
from src.connectors.phone import fetch_phone_meta
from src.connectors.urlfetch import fetch_webpage
from src.connectors.username_enum import enumerate_username
from src.parsers.base import InputObject

Connector = Callable[[InputObject], Awaitable[dict[str, Any]]]

CONNECTORS: dict[str, Connector] = {
    "crtsh_subdomains": fetch_crtsh,
    "gravatar_email": fetch_gravatar,
    "email_handles": echo,           # pure local derivation
    "username_accounts": enumerate_username,
    "github_user": fetch_github_user,  # keyless api.github.com public profile
    "github_deep": fetch_github_deep,  # README + repos + commit emails (anchor pivot)
    "url_accounts": echo,            # pure regex derivation
    "url_fetch": fetch_webpage,      # keyless page fetch (rel=me, mailto, profiles)
    "phone_meta": fetch_phone_meta,  # offline libphonenumber, no network/key
    # threatfox is request/response with args -> wired when its tier/cascade lands
}


def get_connector(helper_id: str) -> Connector:
    try:
        return CONNECTORS[helper_id]
    except KeyError:
        raise KeyError(f"no connector registered for {helper_id!r}") from None

"""classify() — detect the ontology type of arbitrary input (DESIGN §6).

Phase-1 of the regex -> local-ML -> LLM ladder (ruling #9): regex covers the
common selectors deterministically; ambiguous/novel input falls through to
'Phrase' (a dorking seed) until the ML/LLM tiers are wired. Ordering matters —
e.g. URL and Email are checked before bare Domain.
"""

from __future__ import annotations

import ipaddress
import re

_RE = {
    "url": re.compile(r"^https?://", re.I),
    "md5": re.compile(r"^[a-f0-9]{32}$", re.I),
    "sha1": re.compile(r"^[a-f0-9]{40}$", re.I),
    "sha256": re.compile(r"^[a-f0-9]{64}$", re.I),
    "btc": re.compile(r"^(bc1[a-z0-9]{20,}|[13][a-km-zA-HJ-NP-Z1-9]{25,34})$"),
    "tg": re.compile(r"^@[A-Za-z0-9_]{4,32}$"),
    "domain": re.compile(r"^(?=.{1,253}$)([a-z0-9-]{1,63}\.)+[a-z]{2,}$", re.I),
    "email": re.compile(r"^[^@\s]+@(?=.{1,253}$)([a-z0-9-]{1,63}\.)+[a-z]{2,}$", re.I),
    "phone": re.compile(r"^\+?[0-9][0-9 ().\-]{6,}$"),
    "username": re.compile(r"^[A-Za-z0-9_.]{3,40}$"),
}


def classify(raw: str) -> str:
    """Return the ontology type for a raw input blob (best-effort regex)."""
    s = raw.strip()
    if not s:
        return "Phrase"
    if _RE["url"].match(s):
        return "URL"
    if _RE["email"].match(s):
        return "Email"
    try:
        ip = ipaddress.ip_address(s)
        return "IPv6" if ip.version == 6 else "IPv4"
    except ValueError:
        pass
    if _RE["tg"].match(s):
        return "TelegramChannel"
    if _RE["sha256"].match(s) or _RE["sha1"].match(s) or _RE["md5"].match(s):
        return "FileHash"
    if _RE["btc"].match(s):
        return "BTCAddress"
    if _RE["domain"].match(s):
        return "Domain"
    if _RE["phone"].match(s) and sum(c.isdigit() for c in s) >= 7:
        return "Phone"
    if _RE["username"].match(s):
        return "Username"
    return "Phrase"  # free text -> a dorking seed

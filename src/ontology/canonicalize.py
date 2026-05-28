"""Deterministic canonicalization per type (DESIGN §10.1).

Same canonical form => same object (auto-merge happens for free via the
UNIQUE(type, canonical) constraint + create_or_find). Canonicalization is
one-way and lossy, so callers preserve the as-observed value as a separate
assertion. Person has no natural key — it is never canonicalized here (ruling
#3); its ER is probabilistic, in resolution.py.
"""

from __future__ import annotations

import ipaddress
import re

_GMAIL_DOMAINS = {"gmail.com", "googlemail.com"}
_NON_DIGITS = re.compile(r"\D")


def _email(raw: str) -> str:
    raw = raw.strip().lower()
    if "@" not in raw:
        return raw
    local, _, domain = raw.rpartition("@")
    domain = domain.strip(".")
    if domain in _GMAIL_DOMAINS:
        local = local.split("+", 1)[0].replace(".", "")
        domain = "gmail.com"
    else:
        local = local.split("+", 1)[0]
    return f"{local}@{domain}"


def _phone(raw: str) -> str:
    # Best-effort E.164. NB: without a region a national number can't be fully
    # normalized — we keep an explicit '+' when present and strip formatting.
    has_plus = raw.strip().startswith("+")
    digits = _NON_DIGITS.sub("", raw)
    return f"+{digits}" if has_plus else digits


def _domain(raw: str) -> str:
    d = raw.strip().lower().rstrip(".").removeprefix("*.")
    try:
        return d.encode("idna").decode("ascii")  # IDN -> punycode
    except (UnicodeError, ValueError):
        return d


def _ip(raw: str) -> str:
    try:
        return str(ipaddress.ip_address(raw.strip()))
    except ValueError:
        return raw.strip()


def canonicalize(type_: str, raw: str) -> str:
    """Normalize a raw observable into its canonical (dedupe) key for its type."""
    match type_:
        case "Email":
            return _email(raw)
        case "Phone":
            return _phone(raw)
        case "Domain":
            return _domain(raw)
        case "IPv4" | "IPv6" | "IPAddress":
            return _ip(raw)
        case "Account" | "Username":
            return raw.strip().lower()  # (platform, handle) tuples arrive pre-joined
        case "BTCAddress" | "BitcoinAddress":
            return raw.strip()  # case-sensitive encodings — leave as-is
        case _:
            return raw.strip()

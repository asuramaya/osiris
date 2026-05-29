"""Email -> candidate Username pivot.

A bare email rarely has a public search footprint, but the handle behind it
does. Deriving Username objects from the local part lets the cascade pivot into
username search/dorking + osint4all username sources — the productive path.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.parsers.base import InputObject, LinkSpec, ObjectSpec, ParseResult, TargetRef

# Common first names / generic mailbox words have no discriminating power as a
# handle — enumerating "hector" or "info" just surfaces strangers. We skip them
# as derived seeds (the distinctive part of an address is what's worth crawling).
_COMMON_HANDLES: frozenset[str] = frozenset({
    # generic mailbox locals
    "info", "admin", "contact", "hello", "hi", "mail", "team", "support", "sales",
    "office", "help", "no-reply", "noreply", "webmaster", "abuse", "postmaster",
    # very common first names (top given names — high collision, low signal)
    "james", "john", "robert", "michael", "william", "david", "richard", "joseph",
    "thomas", "charles", "daniel", "matthew", "anthony", "mark", "paul", "steven",
    "andrew", "kenneth", "george", "joshua", "kevin", "brian", "edward", "ronald",
    "hector", "carlos", "luis", "juan", "jose", "miguel", "antonio", "manuel",
    "mary", "patricia", "jennifer", "linda", "elizabeth", "susan", "jessica",
    "sarah", "karen", "nancy", "lisa", "maria", "ana", "laura", "sofia",
    "alex", "sam", "max", "chris", "mike", "dan", "tom", "ben", "joe", "nick",
})


def _common(handle: str) -> bool:
    return handle in _COMMON_HANDLES or len(handle) <= 2


def parse_email_handles(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    local = input_object.canonical.split("@", 1)[0].lower()

    variants: list[str] = []
    for v in (local, local.replace(".", ""), local.replace(".", "_"), local.split(".", 1)[0]):
        if v and v not in variants and not _common(v):
            variants.append(v)

    for handle in variants:
        result.objects.append(
            ObjectSpec(
                type="Username",
                canonical=handle,
                confidence=0.5,  # candidate — same local part, not proven the same person
                properties={"derived_from": input_object.canonical},
            )
        )
        result.links.append(
            LinkSpec(TargetRef(input=True), TargetRef(ref=handle), "derived_handle", 0.5)
        )
    return result

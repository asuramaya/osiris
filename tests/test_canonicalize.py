from __future__ import annotations

import uuid

from src.actions.core import Actions
from src.ontology.canonicalize import canonicalize
from src.ontology.intake import intake


def test_email_canonicalization() -> None:
    # predictable: lowercase + googlemail->gmail domain alias only; local part kept
    assert canonicalize("Email", "Priya.Kowalski42@Gmail.com") == "priya.kowalski42@gmail.com"
    assert canonicalize("Email", "a.b.c@googlemail.com") == "a.b.c@gmail.com"


def test_email_preserves_dots_and_subaddressing() -> None:
    # dots and +subaddressing are significant — never stripped, any provider.
    # Regression: sanitization used to mangle these (gmail dot-stripping included).
    assert canonicalize("Email", "First.Last+osint@Corp.COM") == "first.last+osint@corp.com"
    assert canonicalize("Email", "priya.kowalski42@gmail.com") == "priya.kowalski42@gmail.com"
    assert canonicalize("Email", "o'brien@example.org") == "o'brien@example.org"


def test_domain_and_ip_canonicalization() -> None:
    assert canonicalize("Domain", "*.Example.COM.") == "example.com"
    assert canonicalize("IPv4", "192.168.1.1") == "192.168.1.1"
    # IPv6 compresses to its canonical form
    assert canonicalize("IPv6", "2001:0DB8:0000:0000:0000:0000:0000:0001") == "2001:db8::1"


def test_phone_best_effort_e164() -> None:
    assert canonicalize("Phone", "+1 (415) 555-0100") == "+14155550100"
    assert canonicalize("Phone", "0612345678") == "0612345678"  # no region -> kept as-is


def test_btc_is_case_preserving() -> None:
    addr = "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"
    assert canonicalize("BTCAddress", addr) == addr


async def test_intake_deterministic_auto_merge(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    # case-folding still dedups (dots are now preserved, so they must match)
    a = await intake(actions, "Email", "Priya.KOWALSKI42@Gmail.com", "analyst:test", cid)
    b = await intake(actions, "Email", "priya.kowalski42@gmail.com", "analyst:test", cid)
    assert a == b  # two raw forms collapse to one object

    # only one object, and the original form is preserved as evidence
    assert await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Email'") == 1
    observed = {
        r["value"]
        for r in await actions.pool.fetch(
            "SELECT value #>> '{}' AS value FROM current_assertions "
            "WHERE object_id=$1 AND name='observed_value'",
            a,
        )
    }
    assert "Priya.KOWALSKI42@Gmail.com" in observed

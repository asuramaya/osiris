"""Phone-number enrichment connector — fully offline, keyless, no antibot.

Uses Google's libphonenumber metadata (the `phonenumbers` port) to derive a
number's region, carrier, line type and canonical formats locally. No network,
no API key, no rate limit — the cleanest possible "open source" enrichment and
the anchor that lets the rest of the footprint crawl dork the number in the
formats people actually post it in.
"""

from __future__ import annotations

from typing import Any

import phonenumbers
from phonenumbers import PhoneNumberType as _T

from src.parsers.base import InputObject

_LINE_TYPES = {
    _T.MOBILE: "mobile",
    _T.FIXED_LINE: "fixed_line",
    _T.FIXED_LINE_OR_MOBILE: "fixed_or_mobile",
    _T.VOIP: "voip",
    _T.TOLL_FREE: "toll_free",
    _T.PREMIUM_RATE: "premium_rate",
}


async def fetch_phone_meta(input_object: InputObject) -> dict[str, Any]:
    """Parse the canonical (E.164-ish) number locally and return its metadata.

    Returns {"valid": False} on an unparseable / non-E.164 number rather than
    raising, so a bare national number that never got a region just yields no
    enrichment instead of crashing the cascade.
    """
    # LAZY, ON PURPOSE (thread e6fd3772 piece 2, measured): `phonenumbers.geocoder`/`carrier`
    # each drag in their own bundled geodata tables (data1.py alone is multi-megabyte
    # source, slow to parse and heavy to hold resident) the instant they're imported —
    # cost paid at MODULE IMPORT, not at first real lookup, if hoisted to the top of this
    # file. This connector is only reached from `src.connectors.registry`'s own eager,
    # module-level CONNECTORS dict — so a top-level import here meant every process that
    # ever imports the registry (including osiris-mcp's read-only `orient()` path, via
    # `monitor.scheduled_jobs()`'s lazy `from src.workers.arq_worker import WorkerSettings`)
    # paid the full geodata cost just to REGISTER this function, never mind call it. A
    # traced live specimen: orient() blocked 15s+ and grew RSS by several hundred MB on
    # the first call in a fresh process, entirely inside this import chain.
    from phonenumbers import carrier, geocoder

    raw = input_object.canonical
    try:
        num = phonenumbers.parse(raw, None)  # None region => must be E.164 (has +)
    except phonenumbers.NumberParseException:
        return {"valid": False, "input": raw}
    if not phonenumbers.is_valid_number(num):
        return {"valid": False, "input": raw}

    region = geocoder.description_for_number(num, "en")
    country = phonenumbers.region_code_for_number(num)
    carr = carrier.name_for_number(num, "en")
    line = _LINE_TYPES.get(phonenumbers.number_type(num), "unknown")
    return {
        "valid": True,
        "country": country,                       # ISO region, e.g. "US"
        "region": region or None,                 # geocoded area, e.g. "California"
        "carrier": carr or None,
        "line_type": line,
        "e164": phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164),
        "international": phonenumbers.format_number(
            num, phonenumbers.PhoneNumberFormat.INTERNATIONAL
        ),
        "national": phonenumbers.format_number(
            num, phonenumbers.PhoneNumberFormat.NATIONAL
        ),
    }

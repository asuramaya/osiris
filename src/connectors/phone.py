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
from phonenumbers import carrier, geocoder

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

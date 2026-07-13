from __future__ import annotations

import uuid

from src.connectors.phone import fetch_phone_meta
from src.parsers.accounts import parse_url_accounts, parse_username_accounts
from src.parsers.base import InputObject
from src.parsers.phone import parse_phone_meta


def _inp(type_: str, canon: str) -> InputObject:
    return InputObject(id=str(uuid.uuid4()), type=type_, canonical=canon)


def test_username_enum_to_accounts() -> None:
    response = {"username": "asuramaya", "accounts": [
        {"platform": "github", "url": "https://github.com/asuramaya", "username": "asuramaya"},
        {"platform": "keybase", "url": "https://keybase.io/asuramaya", "username": "asuramaya"},
    ]}
    r = parse_username_accounts(response, _inp("Username", "asuramaya"))
    accts = {o.canonical for o in r.objects if o.type == "Account"}
    assert accts == {"github:asuramaya", "keybase:asuramaya"}
    assert all(link.type == "has_account" for link in r.links)


def test_url_to_account_recognizes_profiles() -> None:
    cases = {
        "https://github.com/asuramaya": "github:asuramaya",
        "https://www.linkedin.com/in/priya-kowalski-583920147": "linkedin:priya-kowalski-583920147",
        "https://x.com/asuramaya_hq": "twitter:asuramaya_hq",
        "https://www.youtube.com/@asuramaya": "youtube:asuramaya",
    }
    for url, expected in cases.items():
        r = parse_url_accounts({}, _inp("URL", url))
        assert [o.canonical for o in r.objects] == [expected], url


def test_url_account_ignores_non_profile_urls() -> None:
    # a repo path or article is not a profile -> no account derived
    r = parse_url_accounts({}, _inp("URL", "https://github.com/someuser/SomeRepo"))
    assert r.objects == []
    r2 = parse_url_accounts({}, _inp("URL", "https://zenodo.org/record/2403053/files/article.pdf"))
    assert r2.objects == []


def test_url_to_account_recognizes_more_social_profiles() -> None:
    cases = {
        "https://www.tiktok.com/@asuramaya": "tiktok:asuramaya",
        "https://soundcloud.com/asuramaya": "soundcloud:asuramaya",
        "https://about.me/asuramaya": "about.me:asuramaya",
        "https://lobste.rs/~asuramaya": "lobsters:asuramaya",
        "https://asuramaya.tumblr.com": "tumblr:asuramaya",
        "https://www.last.fm/user/asuramaya": "lastfm:asuramaya",
        "https://mastodon.social/@asuramaya": "mastodon:asuramaya",
    }
    for url, expected in cases.items():
        r = parse_url_accounts({}, _inp("URL", url))
        assert [o.canonical for o in r.objects] == [expected], url


async def test_phone_enrichment_offline() -> None:
    # libphonenumber metadata is fully offline/keyless — deterministic in tests
    io = _inp("Phone", "+14155552671")
    meta = await fetch_phone_meta(io)
    assert meta["valid"] is True
    assert meta["country"] == "US"
    assert meta["national"] == "(415) 555-2671"

    r = parse_phone_meta(meta, io)
    phone = [o for o in r.objects if o.type == "Phone"]
    assert phone and phone[0].properties["country"] == "US"
    # human-formatted variants are seeded as search Phrases
    variants = {o.canonical for o in r.objects if o.type == "Phrase"}
    assert "(415) 555-2671" in variants
    assert all(link.type == "search_variant" for link in r.links)


def test_url_account_rejects_platform_reserved_pages() -> None:
    # github's own nav/footer pages must not become fake "accounts"
    for url in ("https://github.com/about", "https://github.com/features",
                "https://github.com/pricing", "https://twitter.com/home"):
        assert parse_url_accounts({}, _inp("URL", url)).objects == [], url
    # a real single-segment profile still resolves
    real = parse_url_accounts({}, _inp("URL", "https://github.com/asuramaya"))
    assert real.objects[0].canonical == "github:asuramaya"


def test_email_handles_skips_common_names() -> None:
    from src.parsers.handles import parse_email_handles
    # "hector" is a common first name -> no discriminating power, not enumerated
    r = parse_email_handles({}, _inp("Email", "priya@kowalski.dev"))
    assert {o.canonical for o in r.objects} == set()
    # a distinctive local part still derives handles
    r2 = parse_email_handles({}, _inp("Email", "dakota.jm@gmail.com"))
    assert "asuramaya" in {o.canonical for o in r2.objects}


async def test_phone_enrichment_rejects_invalid() -> None:
    io = _inp("Phone", "12345")
    meta = await fetch_phone_meta(io)
    assert meta["valid"] is False
    assert parse_phone_meta(meta, io).objects == []

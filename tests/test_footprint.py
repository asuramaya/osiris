from __future__ import annotations

import uuid

from src.parsers.accounts import parse_url_accounts, parse_username_accounts
from src.parsers.base import InputObject


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
    r = parse_url_accounts({}, _inp("URL", "https://github.com/asuramaya/MadApes.ai"))
    assert r.objects == []
    r2 = parse_url_accounts({}, _inp("URL", "https://zenodo.org/record/2403053/files/article.pdf"))
    assert r2.objects == []

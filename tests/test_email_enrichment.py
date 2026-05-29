from __future__ import annotations

import uuid

from src.parsers.base import InputObject
from src.parsers.gravatar import parse_gravatar
from src.parsers.handles import parse_email_handles


def _email(canon: str) -> InputObject:
    return InputObject(id=str(uuid.uuid4()), type="Email", canonical=canon)


def test_email_handles_pivot() -> None:
    r = parse_email_handles({}, _email("dakota.jm@gmail.com"))
    handles = {o.canonical for o in r.objects if o.type == "Username"}
    # local part + dot-stripped + underscore + first-segment variants
    assert handles == {"asuramaya.hq", "asuramayahq", "asuramaya_hq", "asuramaya"}
    assert all(link.type == "derived_handle" for link in r.links)


def test_gravatar_absent_is_empty() -> None:
    r = parse_gravatar({"found": False, "hash": "abc"}, _email("x@y.com"))
    assert r.objects == []


def test_gravatar_profile_to_person_and_accounts() -> None:
    response = {
        "found": True, "hash": "deadbeef",
        "entry": [{
            "displayName": "Hector",
            "currentLocation": "Lima",
            "accounts": [
                {"shortname": "github", "username": "asuramaya", "url": "https://github.com/asuramaya"},
                {"shortname": "twitter", "display": "@asuramaya_hq", "url": "https://x.com/asuramaya_hq"},
            ],
        }],
    }
    r = parse_gravatar(response, _email("dakota.jm@gmail.com"))
    person = [o for o in r.objects if o.type == "Person"]
    accounts = [o for o in r.objects if o.type == "Account"]
    assert len(person) == 1 and person[0].properties["name"] == "Hector"
    assert {a.properties["platform"] for a in accounts} == {"github", "twitter"}
    assert any(link.type == "has_profile" for link in r.links)
    assert sum(1 for link in r.links if link.type == "has_account") == 2

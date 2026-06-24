from __future__ import annotations

import uuid

from src.parsers.base import EvidenceClass, InputObject
from src.parsers.github_deep import parse_github_deep


def _acc(canon: str) -> InputObject:
    return InputObject(id=str(uuid.uuid4()), type="Account", canonical=canon)


def test_github_deep_extracts_declared_links_domains_and_commit_email() -> None:
    readme = (
        "I build things. Links: [website](https://asuramaya.com/) · "
        "[GitHub](https://github.com/asuramaya) · "
        "[LinkedIn](https://www.linkedin.com/in/priya-kowalski-583920147). "
        "Also on [SoundCloud](https://soundcloud.com/wrenaudio7). "
        "Reach me at contact@asuramaya.com"
    )
    resp = {
        "found": True, "login": "asuramaya",
        "profile": {"name": "architect of illusions", "twitter_username": "asuramaya_hq",
                    "blog": "asuramaya.com", "bio": "researcher"},
        "readme": readme,
        "homepages": ["https://madapesai.com", "https://chronohorn.com"],
        "commit_emails": {"dakota.jm@gmail.com": 400,
                          "69973947+asuramaya@users.noreply.github.com": 100},
    }
    r = parse_github_deep(resp, _acc("github:asuramaya"))
    objs = {(o.type, o.canonical): o for o in r.objects}
    links = {(lk.to_ref.ref, lk.type) for lk in r.links}

    # self-declared profile accounts (high trust)
    assert (
        objs[("Account", "linkedin:priya-kowalski-583920147")].evidence_class
        is EvidenceClass.SELF_DECLARED
    )
    assert ("Account", "soundcloud:wrenaudio7") in objs
    assert ("Account", "twitter:asuramaya_hq") in objs
    assert ("linkedin:priya-kowalski-583920147", "declares") in links
    # the github account itself is not re-emitted as a separate declared account
    assert ("github:asuramaya", "declares") not in links
    # owned sites -> URL + Domain
    assert ("URL", "https://madapesai.com") in objs
    assert ("Domain", "madapesai.com") in objs
    assert ("Domain", "chronohorn.com") in objs
    # the real committing email (strongest tie); github noreply is dropped
    assert (
        objs[("Email", "dakota.jm@gmail.com")].evidence_class
        is EvidenceClass.SELF_DECLARED
    )
    assert ("dakota.jm@gmail.com", "committed_as") in links
    assert not any("noreply" in canon for (_t, canon) in objs)
    # mailto / contact email from the README
    assert ("Email", "contact@asuramaya.com") in objs


def test_github_deep_noop_on_non_github_account() -> None:
    assert parse_github_deep({"found": False}, _acc("twitter:foo")).objects == []

from __future__ import annotations

import uuid

from src.parsers.base import InputObject
from src.parsers.webpage import parse_webpage


def _inp(url: str) -> InputObject:
    return InputObject(id=str(uuid.uuid4()), type="URL", canonical=url)


def test_parse_webpage_extracts_identity_signals() -> None:
    html = """
    <html><head><title>  Hector — home  </title></head><body>
      <a rel="me" href="https://github.com/asuramaya">github</a>
      <a rel="me noopener" href="https://mastodon.social/@asuramaya">fedi</a>
      <a href="mailto:priya@kowalski.dev?subject=hi">email me</a>
      <a href="https://www.linkedin.com/in/priya-kowalski">linkedin</a>
      <a href="https://example.com/some/article">a random link</a>
    </body></html>
    """
    r = parse_webpage({"fetched": True, "url": "https://asuramaya.com", "html": html}, _inp("https://asuramaya.com"))
    objs = {(o.type, o.canonical): o for o in r.objects}
    links = {(o.type, o.canonical): None for o in r.objects}  # noqa: F841

    # rel=me github profile -> Account at 0.8
    assert objs[("Account", "github:asuramaya")].confidence == 0.8
    # rel=me mastodon -> Account at 0.8 (generic /@handle pattern)
    assert ("Account", "mastodon:asuramaya") in objs
    # mailto -> Email at 0.7
    assert objs[("Email", "priya@kowalski.dev")].confidence == 0.7
    # plain profile link (not rel=me) -> Account at 0.5
    assert objs[("Account", "linkedin:priya-kowalski")].confidence == 0.5
    # the random non-profile link is NOT emitted
    assert ("URL", "https://example.com/some/article") not in objs
    # page title labels the fetched URL
    title_obj = objs[("URL", "https://asuramaya.com")]
    assert title_obj.properties["page_title"] == "Hector — home"

    rel_me_links = [link for link in r.links if link.type == "rel_me"]
    assert len(rel_me_links) == 2


def test_parse_webpage_noop_when_not_fetched() -> None:
    assert parse_webpage({"fetched": False, "reason": "not-html"}, _inp("https://x")).objects == []

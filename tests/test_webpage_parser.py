from __future__ import annotations

import uuid

from src.parsers.base import EvidenceClass, InputObject
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

    # rel=me github profile -> self-declared identity link
    assert objs[("Account", "github:asuramaya")].evidence_class is EvidenceClass.SELF_DECLARED
    # rel=me mastodon -> Account (generic /@handle pattern), self-declared
    assert ("Account", "mastodon:asuramaya") in objs
    # mailto -> self-declared contact email
    assert objs[("Email", "priya@kowalski.dev")].evidence_class is EvidenceClass.SELF_DECLARED
    # plain profile link (not rel=me) -> observed, but a weaker signal (not an anchor)
    assert (
        objs[("Account", "linkedin:priya-kowalski")].evidence_class
        is EvidenceClass.DIRECT_OBSERVATION
    )
    # the random non-profile link is NOT emitted
    assert ("URL", "https://example.com/some/article") not in objs
    # page title labels the fetched URL
    title_obj = objs[("URL", "https://asuramaya.com")]
    assert title_obj.properties["page_title"] == "Hector — home"

    rel_me_links = [link for link in r.links if link.type == "rel_me"]
    assert len(rel_me_links) == 2


def test_parse_webpage_mines_plaintext_contact_email() -> None:
    # the high-value case the bridge surfaced: a contact email in visible TEXT, not a
    # mailto: anchor. Conservative parser missed it; mining catches it, graded by domain.
    html = """
    <html><head><title>Voltara Devices</title></head><body>
      <span class="elementor-icon-list-text">contact@voltara.example</span>
      <p>partner: ops@thirdparty.example</p>
      <script>var x = "noise@analytics.example";</script>
    </body></html>
    """
    r = parse_webpage(
        {"fetched": True, "url": "http://voltara.example/", "html": html}, _inp("http://voltara.example")
    )
    objs = {(o.type, o.canonical): o for o in r.objects}

    # same-domain email == the entity's own contact -> DIRECT_OBSERVATION
    assert objs[("Email", "contact@voltara.example")].evidence_class is EvidenceClass.DIRECT_OBSERVATION
    # off-domain email merely co-occurs
    assert objs[("Email", "ops@thirdparty.example")].evidence_class is EvidenceClass.CO_OCCURRENCE
    # text inside <script> is NOT mined
    assert ("Email", "noise@analytics.example") not in objs


def test_parse_webpage_noop_when_not_fetched() -> None:
    assert parse_webpage({"fetched": False, "reason": "not-html"}, _inp("https://x")).objects == []

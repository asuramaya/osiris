from __future__ import annotations

import uuid

from src.parsers.base import EvidenceClass, InputObject
from src.parsers.searxng import parse_searxng_results
from src.parsers.snippets import extract_selectors


def test_extract_selectors_finds_anchored_types() -> None:
    text = (
        "Contact jane.doe@work.com or ping @jane_doe on socials. "
        "Profile https://github.com/janedoe — call +1 (415) 555-2671."
    )
    found = dict(extract_selectors(text))
    assert found["Email"] == "jane.doe@work.com"
    assert found["Username"] == "jane_doe"
    assert found["URL"] == "https://github.com/janedoe"
    assert found["Phone"] == "+14155552671"  # canonicalized


def test_extract_selectors_rejects_bare_words_and_short_runs() -> None:
    # bare words are NOT usernames (only @handles are); short digit runs aren't phones
    pairs = extract_selectors("the quick brown fox jumped 12345 times in 2024")
    assert all(t != "Username" for t, _ in pairs)
    assert all(t != "Phone" for t, _ in pairs)


def test_snippet_mining_emits_speculative_co_occurs() -> None:
    inp = InputObject(id=str(uuid.uuid4()), type="Email", canonical="priya@kowalski.dev")
    response = {
        "selector": "priya@kowalski.dev",
        "dork_results": [
            {"query": '"priya@kowalski.dev"', "results": [
                {"url": "https://blog.example/post", "title": "Hector (@asuramaya)",
                 "content": "reach me at priya@kowalski.dev or github.com/asuramaya",
                 "engine": "ddg"},
            ]},
        ],
    }
    result = parse_searxng_results(response, inp)
    mined = {
        (o.type, o.canonical)
        for o in result.objects
        if o.evidence_class is EvidenceClass.CO_OCCURRENCE
    }
    # @handle and a co-occurring URL are mined; the seed email itself is skipped
    assert ("Username", "asuramaya") in mined
    assert ("Email", "priya@kowalski.dev") not in mined  # the seed is not re-emitted
    assert all(
        link.type == "co_occurs" for link in result.links
        if link.to_ref.ref in {c for _, c in mined}
    )

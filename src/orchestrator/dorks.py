"""Dork generation (DESIGN §8) — intent-keyed query families per selector type.

Rather than bespoke scrapers per source (fragile, rate-limited, CAPTCHA-walled),
the broad collection path is search-engine dorking through a meta-search engine:
expand a selector into a ranked set of dork queries, run the top-K, and pull the
aggregated results into the graph. Families are intent-keyed and site-agnostic;
substitution adapts them to the selector.
"""

from __future__ import annotations

# Per-type dork sets, most-specific first. {q} is the canonical selector.
_FAMILIES: dict[str, list[str]] = {
    "Email": [
        '"{q}"',
        '"{q}" (filetype:env OR filetype:sql OR filetype:log)',
        '"{q}" (site:pastebin.com OR site:ghostbin.com OR site:rentry.co OR site:paste.ee)',
        '"{q}" (site:github.com OR site:gitlab.com)',
    ],
    "Domain": [
        'site:{q}',
        '"{q}" (filetype:pdf OR filetype:xlsx) (confidential OR internal)',
        '"{q}" (site:github.com OR site:gitlab.com OR site:bitbucket.org)',
        '"{q}" (site:s3.amazonaws.com OR site:storage.googleapis.com '
        'OR site:blob.core.windows.net)',
    ],
    "Username": [
        '"{q}"',
        '"{q}" (inurl:profile OR inurl:user OR inurl:u/)',
        'intext:"{q}" (site:github.com OR site:gitlab.com OR site:keybase.io)',
    ],
    "IPv4": ['"{q}"', '"{q}" (site:shodan.io OR site:censys.io OR site:viz.greynoise.io)'],
    "Person": ['"{q}"', '"{q}" (linkedin OR twitter OR facebook)'],
    "Phrase": ["{q}", '"{q}" (filetype:pdf OR filetype:docx)'],
    "Phone": [
        '"{q}"',
        '"{q}" (site:truecaller.com OR site:sync.me OR site:whocalld.com)',
        '"{q}" (contact OR whatsapp OR telegram OR signal)',
    ],
}
_DEFAULT = ['"{q}"']


def generate_dorks(type_: str, canonical: str, *, limit: int = 4) -> list[str]:
    """Ranked dork queries for a selector, top-`limit` first (the long tail eats
    rate/human budget, so callers run only the head)."""
    families = _FAMILIES.get(type_, _DEFAULT)
    return [tpl.replace("{q}", canonical) for tpl in families[:limit]]

"""Parser registry: manifest `parser:` name -> parser function.

One helper = one manifest + one parser function (DESIGN §16). Register new
parsers here so the runner can resolve them by the name in the manifest.
"""

from __future__ import annotations

from src.parsers.base import Parser
from src.parsers.crtsh import parse_crtsh
from src.parsers.searxng import parse_searxng_results
from src.parsers.suggest import parse_suggest_noop
from src.parsers.telegram import parse_telegram_channel
from src.parsers.tgstat import parse_tgstat_behavior
from src.parsers.threatfox import parse_threatfox_iocs

PARSERS: dict[str, Parser] = {
    "threatfox_malware_iocs": parse_threatfox_iocs,
    "crtsh_subdomains": parse_crtsh,
    "telegram_channel_profile": parse_telegram_channel,
    "tgstat_channel_behavior": parse_tgstat_behavior,
    "suggest_noop": parse_suggest_noop,
    "searxng_results": parse_searxng_results,
}


def get_parser(name: str) -> Parser:
    try:
        return PARSERS[name]
    except KeyError:
        raise KeyError(f"no parser registered for {name!r}") from None

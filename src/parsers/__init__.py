"""Parser registry: manifest `parser:` name -> parser function.

One helper = one manifest + one parser function (DESIGN §16). Register new
parsers here so the runner can resolve them by the name in the manifest.
"""

from __future__ import annotations

from src.parsers.base import Parser
from src.parsers.threatfox import parse_threatfox_iocs

PARSERS: dict[str, Parser] = {
    "threatfox_malware_iocs": parse_threatfox_iocs,
}


def get_parser(name: str) -> Parser:
    try:
        return PARSERS[name]
    except KeyError:
        raise KeyError(f"no parser registered for {name!r}") from None

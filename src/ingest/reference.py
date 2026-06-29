"""Reference ingest — the design canon as project memory.

Notion + Palantir's models (and Osiris's own docs) become `Reference` objects in the graph,
so the design knowledge that shapes the front end is itself queryable, sourced project memory.
This closes the self-referential loop the operator asked for: build the front FROM the canon,
with the canon living in the substrate next to the commits and threads that implement it.

A vendor doc is graded AUTHORITATIVE_API (a published canonical model); our own docs are
SELF_DECLARED. `python -m src.ingest.reference` ingests docs/reference/ + the own docs and
wires the `cites` edges COMPOSER.md already declares.
"""
from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_HEADER = re.compile(r"<!--\s*(.*?)\s*-->", re.S)
# the own docs to ingest (SELF_DECLARED) and the vendor refs COMPOSER.md cites
_OWN_DOCS = ("docs/COMPOSER.md", "ARCHITECTURE.md", "docs/REFERENCE.md")


def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


# file I/O stays in sync helpers — the async functions do DB work only (ASYNC240)
def _read(path: str) -> str:
    return Path(path).read_text()


def _md_files(directory: str) -> list[str]:
    return [str(p) for p in sorted(Path(directory).glob("*.md"))]


def _existing(paths: tuple[str, ...]) -> list[str]:
    return [p for p in paths if Path(p).exists()]


def parse_doc(text: str) -> dict[str, str]:
    """Pull the leading `<!-- source: … | vendor: … | topic: … -->` header (if any) + the
    title (first H1) + the body. Pure; tolerant of docs with no header (our own)."""
    meta: dict[str, str] = {}
    m = _HEADER.match(text.lstrip())
    if m:
        for part in m.group(1).split("|"):
            if ":" in part:
                k, v = part.split(":", 1)
                meta[k.strip()] = v.strip()
    body = _HEADER.sub("", text, count=1).strip()
    title_m = re.search(r"^#\s+(.+)$", body, re.M)
    meta["title"] = title_m.group(1).strip() if title_m else "(untitled)"
    meta["body"] = body
    return meta


async def ingest_reference_doc(
    actions: Actions, path: str, *, case_id: uuid.UUID | None = None
) -> dict[str, Any]:
    """Ingest one markdown doc as a `Reference` object (canonical `ref:<stem-slug>`),
    idempotent on the canonical. Vendor → AUTHORITATIVE_API, own → SELF_DECLARED."""
    doc = parse_doc(_read(path))
    vendor = doc.get("vendor", "osiris")
    ec = EvidenceClass.AUTHORITATIVE_API if vendor != "osiris" else EvidenceClass.SELF_DECLARED
    conf = confidence_for(ec)
    source_id = f"ref:{vendor}"
    now = datetime.now(UTC)
    canon = f"ref:{_slug(Path(path).stem)}"
    ref = await actions.create_or_find_object("Reference", canon, source_id, case_id)
    await actions.assert_property(ref, "name", doc["title"], source_id, now, conf,
                                  case_id=case_id, evidence_class=ec.value)
    for name in ("vendor", "source", "topic", "body"):
        value = vendor if name == "vendor" else doc.get(name, "")
        if value:
            prop = "source_url" if name == "source" else name
            await actions.assert_property(ref, prop, value, source_id, now, conf,
                                          case_id=case_id, evidence_class=ec.value)
    return {"id": ref, "canonical": canon, "title": doc["title"], "vendor": vendor}


async def ingest_reference_dir(
    actions: Actions, directory: str = "docs/reference", *, case_id: uuid.UUID | None = None
) -> list[dict[str, Any]]:
    """Ingest every markdown doc in a directory (sorted, deterministic)."""
    out = []
    for p in _md_files(directory):
        out.append(await ingest_reference_doc(actions, p, case_id=case_id))
    return out


async def ingest_canon(actions: Actions, *, case_id: uuid.UUID | None = None) -> dict[str, Any]:
    """Ingest the vendor canon (docs/reference/) + our own docs, then wire the `cites` edges
    COMPOSER.md declares (it descends its op vocabulary from the Palantir/Notion refs)."""
    vendor_refs = await ingest_reference_dir(actions, case_id=case_id)
    own = [await ingest_reference_doc(actions, p, case_id=case_id)
           for p in _existing(_OWN_DOCS)]
    # the true self-referential link: COMPOSER cites the canon it was grounded in
    composer = next((o for o in own if o["canonical"] == "ref:composer"), None)
    cites = 0
    if composer is not None:
        now = datetime.now(UTC)
        for v in vendor_refs:
            await actions.create_link(composer["id"], v["id"], "cites", "ref:osiris", now,
                                      confidence_for(EvidenceClass.SELF_DECLARED),
                                      case_id=case_id,
                                      evidence_class=EvidenceClass.SELF_DECLARED.value)
            cites += 1
    return {"vendor": len(vendor_refs), "own": len(own), "cites": cites}


def main() -> None:  # pragma: no cover - CLI
    import asyncio

    async def run() -> None:
        pool = await create_pool(get_settings().database_url)
        try:
            print(await ingest_canon(Actions(pool)))
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":  # pragma: no cover
    main()

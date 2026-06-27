"""Compose — document → sourced lead, done right (cron Phase 5).

This is the whole point of the persistence ladder: a cron that *watches* a document
source, *AI-extracts* each new document into graded entities, *resolves* them against
what the graph already knows, and lets the subscription evaluator fire a *sourced
lead*. It composes Phase 3 (the delta watcher) and Phase 4 (the universal extractor)
with the kernel's resolution — no new collection primitive, just the pipeline.

Every part is injected (delta puller, document fetch, LLM) so the pipeline is hermetic
and source-agnostic; a real source supplies the three callables. Two cursors keep the
document watch independent of the simple WatchItem tick (`docsource:<id>`).
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from src.actions.core import Actions
from src.ingest.extract import LLMClient, extract_document
from src.ontology.resolution import find_cross_base_candidates
from src.orchestrator.monitor import get_cursor, set_cursor

logger = logging.getLogger("osiris.compose")


@dataclass
class DocRef:
    """A document the watcher found. `doc_id` dedups, `date` is the cursor field,
    `text` may already be carried by the delta (else `fetch` is called)."""

    doc_id: str
    date: str
    url: str | None = None
    text: str | None = None


# the source-specific halves, all injected:
#   delta:  (cursor) -> (new document refs, advanced cursor)
#   fetch:  (DocRef) -> the document text
DocDelta = Callable[[str | None], Awaitable[tuple[list[DocRef], str]]]
DocFetch = Callable[[DocRef], Awaitable[str]]


async def watch_extract_tick(
    actions: Actions,
    source_id: str,
    delta: DocDelta,
    fetch: DocFetch,
    llm: LLMClient,
    *,
    case_id: uuid.UUID | None = None,
    resolve: bool = True,
) -> dict[str, Any]:
    """One composed tick: pull new documents past the cursor, AI-extract each into
    graded entities, resolve cross-base, advance the cursor. The extracted nodes write
    the outbox, so a saved subscription fires the lead. Returns roll-up counts.

    A single bad document (fetch error, garbage text) is logged and skipped — it must
    never abort the cron or lose the cursor for the documents that did parse."""
    pool = actions.pool
    cursor = await get_cursor(pool, f"docsource:{source_id}")
    docs, new_cursor = await delta(cursor)

    documents = entities = relationships = 0
    for doc in docs:
        try:
            text = doc.text if doc.text is not None else await fetch(doc)
            if not text:
                continue
            counts = await extract_document(
                actions, text, llm, case_id=case_id, source_id=f"extract:{source_id}"
            )
        except Exception as exc:  # one bad doc can't sink the tick
            logger.warning("extract of %s failed: %r", doc.doc_id, exc)
            continue
        documents += 1
        entities += counts["entities"]
        relationships += counts["relationships"]

    candidates = await find_cross_base_candidates(pool) if resolve and documents else 0
    await set_cursor(pool, f"docsource:{source_id}", new_cursor)
    return {
        "documents": documents,
        "entities": entities,
        "relationships": relationships,
        "merge_candidates": candidates,
    }

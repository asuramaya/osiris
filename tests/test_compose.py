"""Phase 5 — compose: document → sourced lead, done right.

The whole persistence ladder in one pipeline: a watcher pulls new documents past a
cursor, the universal extractor turns each into graded entities, resolution links
them to what the graph already knows, and the subscription evaluator fires a sourced
lead. All halves injected (delta, fetch, LLM) — hermetic, source-agnostic.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from src.actions.core import Actions
from src.orchestrator.compose import DocRef, watch_extract_tick
from src.orchestrator.compositions import save_watch
from src.orchestrator.monitor import evaluate_watches, get_cursor

NOW = datetime(2026, 6, 26, tzinfo=UTC)

# what the LLM returns for the (canned) filing document
_EXTRACTION = """
{"entities":[
   {"name":"Neuralink Corp.","type":"Organization","properties":{"role":"issuer"}},
   {"name":"Jared Birchall","type":"Person","properties":{"title":"director"}}
 ],
 "relationships":[{"from":"Jared Birchall","to":"Neuralink Corp.","type":"officer_of"}]}
"""


class _FakeLLM:
    async def complete(
        self, *, system: str, prompt: str, model: str, max_tokens: int = 2048
    ) -> str:
        return _EXTRACTION


def _delta_returning(refs: list[DocRef], cursor: str):
    async def delta(_prev: str | None) -> tuple[list[DocRef], str]:
        return refs, cursor
    return delta


async def _fetch(doc: DocRef) -> str:
    return f"filing body for {doc.doc_id}"


async def test_document_to_sourced_lead_end_to_end(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    # a federated record already in the graph (edgar) the extraction should resolve to
    fed = await actions.create_or_find_object("Organization", "cik:0001708503", "edgar", cid)
    await actions.assert_property(fed, "name", "Neuralink Corp.", "edgar", NOW, 0.85)

    # an analyst's saved watch: tell me when a new doc names a Neuralink officer/entity
    # (set-entry: fires when a new object whose name mentions Neuralink is created)
    await save_watch(
        actions.pool, "neuralink mentions", None,
        [{"property": "name", "op": "contains", "value": "neuralink"}],
    )

    refs = [DocRef(doc_id="acc-1", date="2026-06-25", text="(carried text)"),
            DocRef(doc_id="acc-2", date="2026-06-26")]  # text fetched
    delta = _delta_returning(refs, cursor="2026-06-26")

    counts = await watch_extract_tick(
        actions, "form_d_docs", delta, _fetch, _FakeLLM(), case_id=cid
    )
    assert counts["documents"] == 2
    assert counts["entities"] == 4   # 2 entities per doc (same canonicals, deduped)
    assert counts["relationships"] == 2
    # RESOLVE: the extracted 'Neuralink Corp.' cross-base-matches the federated cik:
    assert counts["merge_candidates"] >= 1
    assert await get_cursor(actions.pool, "docsource:form_d_docs") == "2026-06-26"

    # the extraction is graded DERIVED (a lead to verify, not an authoritative fact)
    ec = await actions.pool.fetchval(
        "SELECT evidence_class FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='extracted-org:neuralink-corp' AND a.name='name'"
    )
    assert ec == "derived"

    # THE LEAD: the saved watch fires a sourced alert on the extracted Neuralink entity
    fired = await evaluate_watches(actions.pool)
    assert fired >= 1
    hit = await actions.pool.fetchval(
        "SELECT o.canonical FROM alerts a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical LIKE 'extracted-%' LIMIT 1"
    )
    assert hit == "extracted-org:neuralink-corp"


async def test_compose_ocrs_a_scanned_document(
    actions: Actions, case_id: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A scanned source (image bytes) is OCR'd via the vision seam before extraction —
    the county-notice case. The GPU is an API key; here a fake stands in for it."""
    cid = uuid.UUID(case_id)
    ocred = {"called": False}

    class _FakeVision:
        async def to_text(self, *, image: bytes, media_type: str, model: str,
                          instruction: str = "") -> str:
            ocred["called"] = True
            return "transcribed page mentioning Neuralink Corp."

    monkeypatch.setattr("src.ingest.providers.vision_provider", lambda: _FakeVision())

    async def img_delta(_c: str | None) -> tuple[list[DocRef], str]:
        return [DocRef(doc_id="scan-1", date="2026-06-27", media_type="image/png")], "2026-06-27"

    async def img_fetch(_d: DocRef) -> bytes:
        return b"\x89PNG fake-scan-bytes"

    counts = await watch_extract_tick(
        actions, "scans", img_delta, img_fetch, _FakeLLM(), case_id=cid, resolve=False
    )
    assert ocred["called"] is True            # the scan went through OCR
    assert counts["documents"] == 1 and counts["entities"] >= 1


async def test_compose_advances_cursor_and_is_quiet_with_no_new_docs(
    actions: Actions, case_id: str
) -> None:
    cid = uuid.UUID(case_id)
    delta = _delta_returning([], cursor="2026-06-26")  # nothing new
    counts = await watch_extract_tick(actions, "src", delta, _fetch, _FakeLLM(), case_id=cid)
    assert counts == {"documents": 0, "entities": 0, "relationships": 0, "merge_candidates": 0}
    assert await get_cursor(actions.pool, "docsource:src") == "2026-06-26"


async def test_compose_skips_a_bad_document_without_aborting(
    actions: Actions, case_id: str
) -> None:
    """A fetch that raises on one doc must not lose the cursor or the good docs."""
    cid = uuid.UUID(case_id)
    refs = [DocRef(doc_id="good", date="2026-06-25", text="ok"),
            DocRef(doc_id="bad", date="2026-06-26")]

    async def flaky_fetch(doc: DocRef) -> str:
        if doc.doc_id == "bad":
            raise RuntimeError("fetch 500")
        return "good body"

    counts = await watch_extract_tick(
        actions, "src", _delta_returning(refs, "2026-06-26"), flaky_fetch, _FakeLLM(),
        case_id=cid, resolve=False,
    )
    assert counts["documents"] == 1  # only the good doc parsed
    assert await get_cursor(actions.pool, "docsource:src") == "2026-06-26"  # cursor advanced

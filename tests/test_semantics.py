"""The semantic layer + the max-level search doors (operator ruling a0cfcca1).

The embedder is a SEAM: these tests inject a fake (3-axis keyword vectors) — CI never
downloads a model. What they prove: the backfill is incremental by hash watermark and
forgets inactive objects; cosine candidates respect the floor; the semantic door surfaces
a lexically-disjoint hit end-to-end with honest via/telemetry; the trigram door forgives
typos; fusion dedupes by object with 'both' marked; and a closed door degrades to pure
lexical search with nothing false said.
"""
from __future__ import annotations

import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
import pytest_asyncio
from src.actions.core import Actions
from src.orchestrator import semantics
from src.orchestrator.compositions import _fuse_ranked, run_spec
from src.parsers.base import EvidenceClass

NOW = datetime(2026, 7, 11, tzinfo=UTC)
_SD = EvidenceClass.SELF_DECLARED.value

# keyword → axis: texts sharing an axis are "about the same thing" regardless of wording
_AXES = {"swap": 0, "demotion": 0, "downgrade": 0,
         "mail": 1, "inbox": 1, "settle": 1,
         "backup": 2, "rsync": 2}


class FakeEmbedder:
    model = "fake-3d"

    async def embed(self, texts: list[str]) -> list[list[float]]:
        out = []
        for t in texts:
            v = [0.0, 0.0, 0.0]
            for w, axis in _AXES.items():
                if w in t.lower():
                    v[axis] += 1.0
            out.append(v)
        return out


@pytest_asyncio.fixture
async def fake_embedder() -> AsyncIterator[FakeEmbedder]:
    fe = FakeEmbedder()
    semantics.set_embedder_for_tests(fe)
    yield fe
    semantics.set_embedder_for_tests(None)  # leave the door force-closed for other tests


# --- Model2VecEmbedder's own load must be BOUNDED and STICKY (task #149, Imhotep's 300s
# record_decision timeouts, thread 9f08b027): a hung first-load never raises, so no
# try/except anywhere in the search pipeline could ever have caught it — only a timeout
# can. Measured live before this fix: StaticModel.from_pretrained() exceeded 120s with no
# exception at all. These tests never touch model2vec or the network — they prove the
# wrapper's own timeout/latch behavior in isolation. ---


async def test_model2vec_embedder_embed_bounds_a_hung_load(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(semantics, "_LOAD_TIMEOUT_S", 0.05)
    embedder = semantics.Model2VecEmbedder("fake/hangs")
    monkeypatch.setattr(embedder, "_load", lambda: time.sleep(2))

    t0 = time.monotonic()
    with pytest.raises(TimeoutError):
        await embedder.embed(["anything"])
    assert time.monotonic() - t0 < 1.0  # bounded near 0.05s, not the simulated 2s hang
    assert embedder._load_failed is True


async def test_model2vec_embedder_load_refuses_fast_once_latched() -> None:
    """The specimen this guards: three record_decision calls in a row each independently
    hung — this is what stops call 2 and 3 from re-running the same doomed fetch, WITHIN
    the cooldown window (see the self-heal tests below for what happens past it)."""
    embedder = semantics.Model2VecEmbedder("fake/model")
    embedder._load_failed = True
    embedder._failed_at = time.monotonic()
    with pytest.raises(RuntimeError, match="previously failed to load"):
        embedder._load()


# --- the latch self-heals past a cooldown (thread 5cd49217, Thoth DM 5287): #149's original
# design stayed closed for the whole process life — the operator's own "no babysitting"
# complaint once the root cause (HF_HUB_OFFLINE missing on osiris-worker's unit) turned out
# to be transient/fixable without a restart. ---

async def test_model2vec_embedder_latch_reopens_after_the_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(semantics, "_RETRY_COOLDOWN_S", 0.05)
    embedder = semantics.Model2VecEmbedder("fake/model")
    embedder._load_failed = True
    embedder._failed_at = time.monotonic() - 1.0  # well past the (shrunk) cooldown
    embedder._m = "already-loaded-stand-in"  # skip the real model2vec import path
    assert embedder._load() == "already-loaded-stand-in"
    assert embedder._load_failed is False


async def test_model2vec_embedder_latch_stays_shut_inside_the_cooldown() -> None:
    embedder = semantics.Model2VecEmbedder("fake/model")
    embedder._load_failed = True
    embedder._failed_at = time.monotonic()  # just failed, well inside the window
    with pytest.raises(RuntimeError, match="previously failed to load"):
        embedder._load()


async def test_embed_pass_files_an_alarm_on_a_load_failure(actions: Actions) -> None:
    """thread 5cd49217 (Thoth DM 5287): embed_pass's own generic except-block used to
    log-and-swallow a load failure with nothing else watching — this proves the new alarm
    wiring lands the confession where smoke.embed_health can find it."""
    from src.orchestrator.capture import EMBED_ALARM_SURFACE
    from src.workers.arq_worker import embed_pass

    await _decision(actions, "decision:embed-pass-alarm-test", "something worth embedding")

    class _RaisingEmbedder:
        model = "fake/raises"

        async def embed(self, texts: list[str]) -> list[list[float]]:
            raise RuntimeError("simulated: model2vec previously failed to load")

    semantics.set_embedder_for_tests(_RaisingEmbedder())
    try:
        ctx = {"cascade": SimpleNamespace(actions=actions)}
        n = await embed_pass(ctx)
    finally:
        semantics.set_embedder_for_tests(None)
    assert n == 0
    obj = await actions.pool.fetchval(
        "SELECT o.id FROM objects o WHERE o.type='BlindSpot' AND EXISTS "
        "(SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        " AND a.name='surface' AND a.value #>> '{}' = $1)", EMBED_ALARM_SURFACE)
    assert obj is not None


async def _decision(actions: Actions, canonical: str, summary: str) -> None:
    d = await actions.create_or_find_object("Decision", canonical, "agent:a")
    await actions.assert_property(d, "summary", summary, "agent:a", NOW, 0.9,
                                  evidence_class=_SD)


async def _search(actions: Actions, q: str) -> dict:
    spec = {"op": "function", "name": "search", "args": {"q": q}}
    return (await run_spec(actions.pool, spec, None, name="search"))["items"]


async def test_backfill_is_incremental_and_forgets_the_dead(
    actions: Actions, fake_embedder: FakeEmbedder
) -> None:
    # drain whatever the persistent Type catalog (task #97) already needs embedded —
    # search_vectors resets every test but the Type objects themselves don't, so
    # they legitimately look "new" to the very first backfill call of any test that
    # hasn't already indexed them; not what THIS test is about
    baseline = await semantics.embed_backfill(actions.pool, fake_embedder)
    await _decision(actions, "decision:swap", "the warm swap demotion ruling stands")
    await _decision(actions, "decision:mail", "settle every inbox mail before dark")
    r1 = await semantics.embed_backfill(actions.pool, fake_embedder)
    assert r1["embedded"] == 2 and r1["dropped"] == 0
    # unchanged graph → the hash watermark makes the pass free
    r2 = await semantics.embed_backfill(actions.pool, fake_embedder)
    assert r2["embedded"] == 0
    # a reworded winner re-embeds exactly that row
    await _decision(actions, "decision:swap", "the warm swap demotion ruling was amended")
    r3 = await semantics.embed_backfill(actions.pool, fake_embedder)
    assert r3["embedded"] == 1
    # an object leaving 'active' is forgotten by the index (the graph resolved it away)
    await actions.pool.execute(
        "UPDATE objects SET status='merged' WHERE canonical='decision:mail'")
    r4 = await semantics.embed_backfill(actions.pool, fake_embedder)
    assert r4["dropped"] == 1
    n = await actions.pool.fetchval("SELECT count(*) FROM search_vectors")
    assert n == baseline["embedded"] + 1


async def test_semantic_candidates_rank_by_meaning_and_respect_the_floor(
    actions: Actions, fake_embedder: FakeEmbedder
) -> None:
    await _decision(actions, "decision:swap", "the warm swap demotion ruling stands")
    await _decision(actions, "decision:mail", "settle every inbox mail before dark")
    await semantics.embed_backfill(actions.pool, fake_embedder)
    hits = await semantics.semantic_candidates(actions.pool, fake_embedder,
                                               "model downgrade confession")
    assert hits and hits[0]["cos"] > 0.9  # same axis → near-1 cosine
    top = await actions.pool.fetchval(
        "SELECT canonical FROM objects WHERE id=$1", hits[0]["object_id"])
    assert top == "decision:swap"
    # an axis nothing was written about: cosine 0 for every doc → the floor keeps it empty
    assert await semantics.semantic_candidates(
        actions.pool, fake_embedder, "backup rsync landmine") == []


async def test_search_semantic_door_end_to_end(
    actions: Actions, fake_embedder: FakeEmbedder
) -> None:
    """A lexically-DISJOINT query surfaces the meaning-neighbor: 'model downgrade
    confession' shares zero words with 'the warm swap demotion ruling stands' — every
    lexical door misses, the semantic door answers, and the telemetry says so."""
    await _decision(actions, "decision:swap", "the warm swap demotion ruling stands")
    await semantics.embed_backfill(actions.pool, fake_embedder)
    out = await _search(actions, "model downgrade confession")
    assert out["hits"], "the semantic door should have answered"
    h = out["hits"][0]
    assert h["canonical"] == "decision:swap" and h["via"] == "semantic"
    assert h["grade"] == "self_declared" and h["source"] == "agent:a"  # testimony intact
    row = await actions.pool.fetchrow(
        "SELECT semantic, fuzzy, relaxed FROM search_log ORDER BY id DESC LIMIT 1")
    assert row["semantic"] is True and row["fuzzy"] is False


async def test_search_degrades_honestly_when_the_door_is_closed(actions: Actions) -> None:
    semantics.set_embedder_for_tests(None)
    await _decision(actions, "decision:swap", "the warm swap demotion ruling stands")
    out = await _search(actions, "warm swap ruling")
    assert out["hits"] and out["hits"][0]["via"] == "lexical"
    assert (await actions.pool.fetchval(
        "SELECT semantic FROM search_log ORDER BY id DESC LIMIT 1")) is False


async def test_search_trigram_door_forgives_typos(actions: Actions) -> None:
    """'compositon breifings' matches no tsquery ever — the trigram door is strict-AND
    with typo tolerance, and the note confesses which door answered."""
    semantics.set_embedder_for_tests(None)
    await _decision(actions, "decision:lens", "the composition lens renders briefings")
    out = await _search(actions, "compositon breifings")
    assert out["hits"] and out["hits"][0]["canonical"] == "decision:lens"
    assert out["hits"][0]["via"] == "fuzzy"
    assert "spelling-tolerant" in out["note"]
    row = await actions.pool.fetchrow(
        "SELECT fuzzy, semantic FROM search_log ORDER BY id DESC LIMIT 1")
    assert row["fuzzy"] is True and row["semantic"] is False
    # a word that fuzzily matches NOTHING keeps the AND-semantics: no hit invented
    assert (await _search(actions, "compositon zzqqxxyy"))["hits"] == []


def test_fuse_ranked_dedupes_and_marks_both() -> None:
    lex = [{"id": "A", "via": "lexical"}, {"id": "B", "via": "lexical"}]
    sem = [{"id": "B", "via": "semantic"}, {"id": "C", "via": "semantic"}]
    out = _fuse_ranked(lex, sem, 10)
    assert [h["id"] for h in out] == ["B", "A", "C"]  # found by both doors → first
    by_id = {h["id"]: h for h in out}
    assert by_id["B"]["via"] == "both"
    assert by_id["A"]["via"] == "lexical" and by_id["C"]["via"] == "semantic"
    assert len(_fuse_ranked(lex, sem, 1)) == 1  # the limit is honored post-fusion

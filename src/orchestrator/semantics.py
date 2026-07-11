"""The semantic layer — local static embeddings behind a seam, cosine over a cached matrix.

Search's lexical doors (FTS, OR-relaxation, trigram) match WORDS; this layer matches
MEANING — "model downgrade" finds the warm-swap rulings that never say "downgrade". The
operator's ruling (a0cfcca1) un-parked embeddings; the constraint that shaped this design:
the box's inference is the local Claude CLI, which has NO embeddings endpoint, and keyless
is a feature — so the embedder is a LOCAL static model (model2vec: a distilled lookup
table, pure CPU numpy, ~30MB, no GPU, no key, no server). The engine still never runs a
GPU (providers.py's law) — a static embedding is arithmetic, not inference.

Right-sizing: the searchable corpus is ~6.5k (object, field) texts. Vectors live in
search_vectors as real[] (no pgvector in the image, none needed) and are brute-force
cosined over an in-process cache — microseconds at this scale, refreshed when the table's
fingerprint moves. Backfill is incremental by construction: each row carries the md5 of
the text it embedded; unchanged text is never re-embedded (the watermark discipline).

Degradation is graceful and HONEST: no model on disk / import failure → embedder() is
None → fn_search runs its lexical doors only and says nothing false. Tests inject a fake
via `set_embedder_for_tests` — CI never downloads a model.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from typing import Any, Protocol

import asyncpg

from src.config.settings import get_settings

# the searchable fields — mirrors fn_search's lexical door, so the two doors index the
# same rooms (a body lands via Reference ingest; first 2k chars carry a section's gist)
_FIELDS = ("name", "summary", "rationale", "body")
_MAX_CHARS = 2000
_BATCH = 256


class EmbedClient(Protocol):
    """The embedding seam — mirrors providers.py's LLMClient discipline: config picks the
    backend, tests inject fakes, callers never import a model library."""

    model: str

    async def embed(self, texts: list[str]) -> list[list[float]]: ...


class Model2VecEmbedder:
    """Static-embedding backend (minishlab/model2vec). Lazy: the model loads on FIRST
    embed (never at import — CI installs the package but must never touch the network).
    Load failure raises; resolve_embedder turns that into a clean None ONCE."""

    def __init__(self, model_name: str) -> None:
        self.model = model_name
        self._m: Any = None

    def _load(self) -> Any:
        if self._m is None:
            from model2vec import StaticModel  # import here: the dep is optional at runtime

            self._m = StaticModel.from_pretrained(self.model)
        return self._m

    async def embed(self, texts: list[str]) -> list[list[float]]:
        def _run() -> list[list[float]]:
            m = self._load()
            return [v.tolist() for v in m.encode(texts)]
        return await asyncio.to_thread(_run)


_resolved: tuple[EmbedClient | None] | None = None  # 1-tuple: "resolved, possibly to None"
_override: tuple[EmbedClient | None] | None = None


def set_embedder_for_tests(client: EmbedClient | None) -> None:
    """Inject a fake (or force None). Pass through resolve caching so tests are hermetic."""
    global _override, _resolved
    _override = (client,)
    _resolved = None
    _matrix_cache.clear()


def resolve_embedder() -> EmbedClient | None:
    """The configured embedder, or None (semantic door closed, lexical doors unaffected).
    Resolution is cached — a missing model is discovered once, not per search."""
    global _resolved
    if _override is not None:
        return _override[0]
    if _resolved is None:
        s = get_settings()
        client: EmbedClient | None = None
        if s.osiris_embed_provider in ("auto", "model2vec"):
            try:
                import model2vec  # noqa: F401 — probe only; the model itself loads lazily

                client = Model2VecEmbedder(s.osiris_embed_model)
            except ImportError:
                client = None
        _resolved = (client,)
    return _resolved[0]


def _hash(text: str) -> str:
    return hashlib.md5(text[:_MAX_CHARS].encode()).hexdigest()


async def embed_backfill(
    pool: asyncpg.Pool, embedder: EmbedClient, *, batch: int = _BATCH,
) -> dict[str, int]:
    """One incremental pass: embed every searchable winner text whose hash isn't already
    vectorized under this model, and drop vectors whose object went inactive (merged /
    retracted — the index must forget what the graph resolved away). Idempotent; the
    hash watermark makes a no-change pass free."""
    rows = await pool.fetch(
        "SELECT DISTINCT ON (o.id, a.name) o.id AS object_id, a.name AS field, "
        " a.value #>> '{}' AS text "
        "FROM current_assertions a JOIN objects o ON o.id = a.object_id "
        "WHERE o.status='active' AND a.name = ANY($1::text[]) "
        "  AND length(a.value #>> '{}') > 12 "
        "ORDER BY o.id, a.name, a.confidence DESC, a.observed_at DESC",
        list(_FIELDS))
    have = {(r["object_id"], r["field"]): r["text_hash"] for r in await pool.fetch(
        "SELECT object_id, field, text_hash FROM search_vectors WHERE model=$1",
        embedder.model)}
    todo = [r for r in rows
            if have.get((r["object_id"], r["field"])) != _hash(r["text"] or "")]
    embedded = 0
    for i in range(0, len(todo), batch):
        chunk = todo[i:i + batch]
        vecs = await embedder.embed([(r["text"] or "")[:_MAX_CHARS] for r in chunk])
        await pool.executemany(
            "INSERT INTO search_vectors (object_id, field, model, text_hash, vec) "
            "VALUES ($1,$2,$3,$4,$5) "
            "ON CONFLICT (object_id, field) DO UPDATE SET "
            " model=$3, text_hash=$4, vec=$5, embedded_at=now()",
            [(r["object_id"], r["field"], embedder.model, _hash(r["text"] or ""), v)
             for r, v in zip(chunk, vecs, strict=True)])
        embedded += len(chunk)
    dropped = await pool.fetchval(
        "WITH gone AS (DELETE FROM search_vectors sv USING objects o "
        " WHERE o.id=sv.object_id AND o.status <> 'active' RETURNING 1) "
        "SELECT count(*) FROM gone")
    if _matrix_cache and (embedded or dropped):
        _matrix_cache.clear()  # same-process searches see the new index immediately
    return {"embedded": embedded, "dropped": int(dropped or 0), "corpus": len(rows)}


# --- query side: the cached matrix + brute-force cosine ---------------------------------

_matrix_cache: dict[str, Any] = {}  # fingerprint → (ids, fields, numpy matrix)
_CACHE_TTL = 120.0


async def _matrix(pool: asyncpg.Pool, model: str) -> tuple[list[Any], list[str], Any] | None:
    """The whole vector index in memory (~7MB at current scale), keyed by a cheap DB
    fingerprint so a stale cache survives at most one backfill OR the TTL."""
    import numpy as np

    fp_row = await pool.fetchrow(
        "SELECT count(*) AS n, max(embedded_at) AS latest FROM search_vectors WHERE model=$1",
        model)
    if not fp_row or not fp_row["n"]:
        return None
    fp = f"{model}:{fp_row['n']}:{fp_row['latest']}"
    hit = _matrix_cache.get("v")
    if hit and hit[0] == fp and time.monotonic() - hit[1] < _CACHE_TTL:
        cached: tuple[list[Any], list[str], Any] = hit[2]
        return cached
    rows = await pool.fetch(
        "SELECT object_id, field, vec FROM search_vectors WHERE model=$1", model)
    ids = [r["object_id"] for r in rows]
    fields = [r["field"] for r in rows]
    mat = np.asarray([r["vec"] for r in rows], dtype=np.float32)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    packed = (ids, fields, mat / norms)
    _matrix_cache["v"] = (fp, time.monotonic(), packed)
    return packed


async def semantic_candidates(
    pool: asyncpg.Pool, embedder: EmbedClient, q: str, *, k: int = 30,
    floor: float = 0.35,
) -> list[dict[str, Any]]:
    """Top-k (object_id, field, cosine) for a query — the semantic door's raw candidates.
    `floor` keeps garbage out: below it a nearest neighbor is noise wearing a rank. Errors
    degrade to [] — the lexical doors must never pay for a semantic failure."""
    import numpy as np

    try:
        packed = await _matrix(pool, embedder.model)
        if packed is None:
            return []
        ids, fields, mat = packed
        (qv,) = await embedder.embed([q[:_MAX_CHARS]])
        v = np.asarray(qv, dtype=np.float32)
        n = float(np.linalg.norm(v))
        if not n:
            return []
        sims = mat @ (v / n)
        order = np.argsort(-sims)[:k]
        return [{"object_id": ids[i], "field": fields[i], "cos": float(sims[i])}
                for i in order if float(sims[i]) >= floor]
    except Exception:  # noqa: BLE001 — the semantic door fails closed, never loudly
        return []

"""The semantic layer: local static embeddings behind a small interface, cosine over a
cached matrix.

Search's lexical routes (FTS, OR-relaxation, trigram) match WORDS; this layer matches
MEANING: "model downgrade" finds the warm-swap rulings that never say "downgrade". A
prior decision approved un-parking embeddings, under a constraint that shaped this design:
the box's inference is the local Claude CLI, which has NO embeddings endpoint, and staying
keyless is a feature. So the embedder is a LOCAL static model (model2vec: a distilled
lookup table, pure CPU numpy, ~30MB, no GPU, no key, no server). The engine still never
runs a GPU (per providers.py's rule); a static embedding is arithmetic, not inference.

Right-sizing: the searchable corpus is ~6.5k (object, field) texts. Vectors live in
search_vectors as real[] (no pgvector in the image, none needed) and are brute-force
cosined over an in-process cache: microseconds at this scale, refreshed when the table's
fingerprint moves. Backfill is incremental by construction: each row carries the md5 of
the text it embedded; unchanged text is never re-embedded (the watermark discipline).

Degradation is graceful and HONEST: no model on disk / import failure means embedder()
returns None, and fn_search runs its lexical routes only and says nothing false. A THIRD
failure mode: the first load can HANG rather than error, since StaticModel.from_pretrained()
reaches HF's CDN with no local cache, and no try/except ever catches that, because a hang
never raises. Model2VecEmbedder bounds that load with a timeout and latches the failure
(never re-attempted mid-process), so this route closes within seconds instead of the
caller waiting out an external 300s timeout with no diagnosis. Tests inject a fake via
`set_embedder_for_tests`; CI never downloads a model.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import time
from typing import Any, Protocol

import asyncpg

from src.config.settings import get_settings

# the searchable fields: mirrors fn_search's lexical route, so the two routes index the
# same content (a document lands via Reference ingest; first 2k chars carry a section's gist)
_FIELDS = ("name", "summary", "rationale", "body")
_MAX_CHARS = 2000
_BATCH = 256


class EmbedClient(Protocol):
    """The embedding interface: mirrors providers.py's LLMClient discipline: config picks
    the backend, tests inject fakes, callers never import a model library."""

    model: str

    async def embed(self, texts: list[str]) -> list[list[float]]: ...


_LOAD_TIMEOUT_S = 8.0  # a ~30MB local model loads in low single digits on a healthy
# network. This bounds a HUNG first-load, previously observed as 300s record_decision
# timeouts: StaticModel.from_pretrained() reaches out to HF's CDN for
# the actual weight files, and that fetch can hang with NO exception raised at all
# (measured live: it exceeded 120s with zero output past the file-listing step). Every
# existing "fail closed" guard in this module and in fn_search's semantic route
# (semantic_candidates's own `except Exception: return []`) only helps once something
# actually RAISES: a hang is silence, not an error, and silence was never caught. This is
# a distinct class of defect: a mechanism that cannot tell "no" from "I don't know
# yet" is not the same defect as "the answer is slow"; the fix is a bound on I-don't-know,
# not a retry on no.


_RETRY_COOLDOWN_S = 300.0  # the original latch never
# re-opened itself: a genuinely transient cause (the HF CDN round-trip this class of
# timeout was root-caused to) stayed closed for that process's WHOLE remaining life, which
# ran counter to the goal of requiring no manual babysitting. HALF-OPEN after a cooldown:
# consistency over opportunism still holds WITHIN a burst (no caller mid-cooldown ever
# re-hangs on the same doomed fetch), but past the cooldown exactly one caller gets to try
# again: a fresh timeout re-latches for another full window, a real recovery just works, no
# restart required. embed_pass's own 10-minute cron grid means a 5-minute cooldown costs at
# most one extra silently-skipped tick, never a stampede.


def local_model_dir(model: str) -> str | None:
    """Where the model already sits in the local Hugging Face cache, or None. Looks at disk
    only: `local_files_only=True` means the hub is never contacted, not even for the
    revision lookup that a plain `from_pretrained` makes on every load."""
    try:
        from huggingface_hub import snapshot_download

        return str(snapshot_download(model, local_files_only=True))
    except Exception:  # noqa: BLE001 - absent, partial or unreadable cache all read as "not here"
        return None


def fetch_model(model: str) -> str:
    """Download the model into the local cache. The ONLY network step: run it from the
    background embed pass or by hand, never from a request. Returns the local directory."""
    from huggingface_hub import snapshot_download

    return str(snapshot_download(model))


class Model2VecEmbedder:
    """Static-embedding backend (minishlab/model2vec). Lazy: the model loads on FIRST
    embed (never at import: CI installs the package but must never touch the network).
    Load failure raises; resolve_embedder turns that into a clean None ONCE.

    THE LOAD ITSELF IS BOUNDED AND STICKY-WITH-COOLDOWN: `_load_failed` latches the
    moment a load times out, so a caller making three calls in a row fails fast on calls 2
    and 3 instead of re-attempting, and re-hanging, the same doomed fetch each time. The
    underlying thread is NOT killed on timeout (Python cannot forcibly kill a thread); it
    may still finish loading later in the background, but this embedder never trusts that
    late result while still inside a cooldown window; past it, `_load` tries once more on
    its own (see `_RETRY_COOLDOWN_S`'s own note): self-healing, not a human noticing and
    restarting a daemon. `last_error`/`failed_at` are read by `smoke.embed_health` (kept as
    plain attributes rather than a graph write here: this module has no `Actions`, only a
    bare pool; the graph-alarm side lives at the caller, `embed_pass`)."""

    def __init__(self, model_name: str) -> None:
        self.model = model_name
        self._m: Any = None
        self._load_failed = False
        self._failed_at: float | None = None
        self.last_error: str | None = None

    def _load(self) -> Any:
        if self._load_failed:
            if (self._failed_at is not None
                    and time.monotonic() - self._failed_at < _RETRY_COOLDOWN_S):
                raise RuntimeError(f"{self.model} previously failed to load (timed out); "
                                   "the semantic route stays closed for "
                                   f"{_RETRY_COOLDOWN_S:.0f}s, then retries itself once")
            self._load_failed = False  # cooldown elapsed: HALF-OPEN, one real attempt below
        if self._m is None:
            from model2vec import StaticModel  # import here: the dep is optional at runtime

            # Offline, always: a request must never make a network call. A model that is
            # not in the local cache closes the semantic route (the lexical legs carry the
            # search) until the background embed pass has fetched it.
            where = local_model_dir(self.model)
            if where is None:
                self.last_error = f"{self.model} is not in the local cache (offline)"
                raise RuntimeError(self.last_error)
            self._m = StaticModel.from_pretrained(where, force_download=False)
        return self._m

    async def embed(self, texts: list[str]) -> list[list[float]]:
        def _run() -> list[list[float]]:
            m = self._load()
            return [v.tolist() for v in m.encode(texts)]
        try:
            return await asyncio.wait_for(asyncio.to_thread(_run), timeout=_LOAD_TIMEOUT_S)
        except TimeoutError:
            self._load_failed = True
            self._failed_at = time.monotonic()
            self.last_error = f"timed out after {_LOAD_TIMEOUT_S:.0f}s"
            raise


_resolved: tuple[EmbedClient | None] | None = None  # 1-tuple: "resolved, possibly to None"
_override: tuple[EmbedClient | None] | None = None


def set_embedder_for_tests(client: EmbedClient | None) -> None:
    """Inject a fake (or force None). Pass through resolve caching so tests are hermetic."""
    global _override, _resolved
    _override = (client,)
    _resolved = None
    _matrix_cache.clear()


def resolve_embedder() -> EmbedClient | None:
    """The configured embedder, or None (semantic route closed, lexical routes unaffected).
    Resolution is cached: a missing model is discovered once, not per search."""
    global _resolved
    if _override is not None:
        return _override[0]
    if _resolved is None:
        s = get_settings()
        client: EmbedClient | None = None
        if s.osiris_embed_provider in ("auto", "model2vec"):
            try:
                import model2vec  # noqa: F401 -- probe only; the model itself loads lazily

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
    retracted, the index must forget what the graph resolved away). Idempotent; the
    hash watermark makes a no-change pass free.

    THE CORPUS QUERY USED TO FETCH EVERY WINNER (a past worker boot spike):
    all ~218k current-assertion winners came back to Python every pass, then were diffed
    against search_vectors' hashes in a dict: a steady-state (nothing changed) pass still
    paid the full fetch. `left(text, N)` + `md5()` mirror `_hash` exactly (Postgres md5()
    and Python's hashlib.md5().hexdigest() agree on identical UTF-8 bytes), so the
    unchanged/changed diff runs server-side in the CTE join below and only the rows that
    actually need re-embedding cross the wire."""
    todo = await pool.fetch(
        "WITH winners AS ( "
        " SELECT DISTINCT ON (o.id, a.name) o.id AS object_id, a.name AS field, "
        "  a.value #>> '{}' AS text "
        " FROM current_assertions a JOIN objects o ON o.id = a.object_id "
        " WHERE o.status='active' AND a.name = ANY($1::text[]) "
        "   AND length(a.value #>> '{}') > 12 "
        " ORDER BY o.id, a.name, a.confidence DESC, a.observed_at DESC "
        ") "
        "SELECT w.object_id, w.field, w.text FROM winners w "
        "LEFT JOIN search_vectors sv ON sv.object_id=w.object_id AND sv.field=w.field "
        " AND sv.model=$2 "
        "WHERE sv.text_hash IS DISTINCT FROM md5(left(w.text, $3))",
        list(_FIELDS), embedder.model, _MAX_CHARS)
    corpus = await pool.fetchval(
        "SELECT count(*) FROM ( "
        " SELECT DISTINCT ON (o.id, a.name) 1 "
        " FROM current_assertions a JOIN objects o ON o.id = a.object_id "
        " WHERE o.status='active' AND a.name = ANY($1::text[]) "
        "   AND length(a.value #>> '{}') > 12 "
        " ORDER BY o.id, a.name "
        ") w",
        list(_FIELDS))
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
    return {"embedded": embedded, "dropped": int(dropped or 0), "corpus": int(corpus or 0)}


# --- query side: the cached matrix + brute-force cosine ---------------------------------

_matrix_cache: dict[str, Any] = {}  # fingerprint → (ids, fields, numpy matrix)


async def _matrix(pool: asyncpg.Pool, model: str) -> tuple[list[Any], list[str], Any] | None:
    """The whole vector index in memory (~7MB at current scale), keyed by a cheap DB
    fingerprint (row count + freshest embedded_at), refreshed when THAT moves, exactly
    as this module's own docstring says, never on a wall clock.

    THE 120s-TTL MEMORY BUG (from an operator report of high memory use on 2026-09-06):
    a caller-side `time.monotonic() - hit[1] < 120.0` check used to force a
    full rebuild whenever 120s had elapsed since the last one, even when the fingerprint
    (queried fresh on every call, above) hadn't moved at all. `embed_backfill`'s own
    explicit `_matrix_cache.clear()` on write already invalidates same-process staleness
    the instant new vectors land, and the fingerprint query (cheap: one count+max) already
    catches EVERY case, same-process or cross-process (the arq worker's own embed pass),
    the moment it runs, so the TTL was pure waste, not a safety net: a hot record_decision/
    open_thread path (both run prior-art's semantic search) fetched all ~39,097 rows as
    Python-list-valued asyncpg Records, transiently allocating hundreds of MB before
    np.asarray ever discarded them, on a clock unrelated to whether anything changed. The
    fix is not a smaller TTL, it is removing the wall-clock condition entirely: the
    fingerprint alone is already authoritative."""
    fp_row = await pool.fetchrow(
        "SELECT count(*) AS n, max(embedded_at) AS latest FROM search_vectors WHERE model=$1",
        model)
    if not fp_row or not fp_row["n"]:
        return None
    fp = f"{model}:{fp_row['n']}:{fp_row['latest']}"
    hit = _matrix_cache.get("v")
    if hit and hit[0] == fp:
        cached: tuple[list[Any], list[str], Any] = hit[1]
        return cached
    # Built in slices: converting 39k Python float lists to numpy holds the GIL, so a
    # worker thread would not free the loop; instead each slice is fetched, converted and
    # followed by a yield, so no one step blocks the loop for more than a slice (the single
    # fetch + convert blocked it ~1.6 s).
    import numpy as np

    ids: list[Any] = []
    fields: list[str] = []
    parts: list[Any] = []
    async with pool.acquire() as conn, conn.transaction():
        cur = await conn.cursor(
            "SELECT object_id, field, vec FROM search_vectors WHERE model=$1", model)
        while chunk := await cur.fetch(1000):
            ids.extend(r["object_id"] for r in chunk)
            fields.extend(r["field"] for r in chunk)
            parts.append(np.asarray([r["vec"] for r in chunk], dtype=np.float32))
            await asyncio.sleep(0)
    mat = np.concatenate(parts)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    packed = (ids, fields, mat / norms)
    _matrix_cache["v"] = (fp, packed)
    return packed


def _top_k(mat: Any, qv: list[float], k: int, floor: float) -> list[tuple[int, float]]:
    """The k nearest rows to the query by cosine, as (row index, cosine), above `floor`."""
    import numpy as np

    v = np.asarray(qv, dtype=np.float32)
    n = float(np.linalg.norm(v))
    if not n:
        return []
    sims = mat @ (v / n)
    order = np.argsort(-sims)[:k]
    return [(int(i), float(sims[i])) for i in order if float(sims[i]) >= floor]


_warm_task: asyncio.Task[bool] | None = None


async def warm(pool: asyncpg.Pool) -> bool:
    """Load the model and build the vector matrix now. True when the semantic route is
    ready; False (never an exception) when the model is not cached or the index is empty."""
    try:
        embedder = resolve_embedder()
        if embedder is None:
            return False
        await embedder.embed(["warm"])
        return await _matrix(pool, embedder.model) is not None
    except Exception:  # noqa: BLE001 - warming is an optimisation, never a failure
        return False


def warm_in_background(pool: asyncpg.Pool) -> None:
    """Warm once per process, off the request path, so the first search after a restart
    pays for neither the model load nor the matrix build. A test run starts no background
    work of its own."""
    global _warm_task
    if _warm_task is not None or "PYTEST_CURRENT_TEST" in os.environ:
        return
    _warm_task = asyncio.get_running_loop().create_task(warm(pool))


async def semantic_candidates(
    pool: asyncpg.Pool, embedder: EmbedClient, q: str, *, k: int = 30,
    floor: float = 0.35,
) -> list[dict[str, Any]]:
    """Top-k (object_id, field, cosine) for a query: the semantic route's raw candidates.
    `floor` keeps garbage out: below it a nearest neighbor is noise wearing a rank. Errors
    degrade to []; the lexical routes must never pay for a semantic failure."""
    try:
        packed = await _matrix(pool, embedder.model)
        if packed is None:
            return []
        ids, fields, mat = packed
        (qv,) = await embedder.embed([q[:_MAX_CHARS]])
        top = await asyncio.to_thread(_top_k, mat, qv, k, floor)
        return [{"object_id": ids[i], "field": fields[i], "cos": cos} for i, cos in top]
    except Exception:  # noqa: BLE001 -- the semantic route fails closed, never loudly
        return []

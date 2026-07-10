"""search v2 — the FTS engine (rung 1, thread 0deaec4f).

The old search read only the `name` property; these tests prove the rooms are searchable
(summaries/rationales), that the ranking inherits the evidence ladder, that every hit
carries testimony, and that the misses log records what the graph failed to answer.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.compositions import run_spec
from src.parsers.base import EvidenceClass

NOW = datetime(2026, 7, 7, tzinfo=UTC)


async def _search(actions: Actions, q: str, caller: str | None = None) -> dict:
    spec = {"op": "function", "name": "search", "args": {"q": q, "caller": caller}}
    out = await run_spec(actions.pool, spec, None, name="search")
    return out["items"]  # the composition envelope wraps the function's return


async def _decision(actions: Actions, canonical: str, summary: str, source: str,
                    ec: str, conf: float = 0.9) -> None:
    d = await actions.create_or_find_object("Decision", canonical, source)
    await actions.assert_property(d, "summary", summary, source, NOW, conf, evidence_class=ec)


async def test_search_reads_the_rooms_not_the_door_plaques(actions: Actions) -> None:
    """A decision whose SUMMARY says 'credence clamp' must be findable by those words —
    the exact miss the old name-only search shipped with."""
    await _decision(actions, "decision:clamp", "the credence clamp ships tonight",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    out = await _search(actions, "credence clamp")
    assert len(out["hits"]) == 1
    h = out["hits"][0]
    assert h["canonical"] == "decision:clamp" and h["field"] == "summary"
    # testimony: source, grade, when, snippet — the hit says WHY it surfaced
    assert h["source"] == "agent:a" and h["grade"] == "self_declared"
    assert "clamp" in h["snippet"].lower() and h["when"].startswith("2026-07")


async def test_ranking_inherits_the_evidence_ladder(actions: Actions) -> None:
    """Equal textual relevance → the deliberate ruling outranks the mined echo."""
    await _decision(actions, "decision:ruled", "wake economics uses haiku triage",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    await _decision(actions, "decision:mined", "wake economics uses haiku triage",
                    "agent:b", EvidenceClass.DERIVED.value, conf=0.4)
    out = await _search(actions, "wake economics haiku")
    assert [h["canonical"] for h in out["hits"]] == ["decision:ruled", "decision:mined"]
    assert out["hits"][0]["rank"] > out["hits"][1]["rank"]


async def test_stemming_and_phrases_work(actions: Actions) -> None:
    await _decision(actions, "decision:stem", "successor identities are minted with lineage",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    assert len((await _search(actions, "minting successors"))["hits"]) == 1  # stems match
    assert len((await _search(actions, '"minted with lineage"'))["hits"]) == 1  # phrase
    assert (await _search(actions, '"lineage with minted"'))["hits"] == []  # order matters


async def test_a_keyword_bag_relaxes_to_any_term(actions: Actions) -> None:
    """The false-empty repro (agent e46a657e-ii, msg 124): websearch ANDs every term, so
    'Hector background skills experience projects' needs all five words in ONE document —
    zero by construction while the graph is full. A plain multi-word bag that strict-AND
    can't satisfy relaxes to ANY-term, best-covered first; explicit syntax (quotes,
    operators) is never second-guessed."""
    await _decision(actions, "decision:ga", "the gestalt head-pointer ships",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    await _decision(actions, "decision:tr", "training the reinforcement loop on pytorch",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    # strict-AND would need one doc holding all of these — instead both docs surface
    out = await _search(actions, "gestalt accessibility pointer hands-free training")
    got = {h["canonical"] for h in out["hits"]}
    assert {"decision:ga", "decision:tr"} <= got
    assert "ANY term" in out["note"]
    # a quoted phrase is the asker's own syntax — no relaxation behind their back
    assert (await _search(actions, '"gestalt training loop"'))["hits"] == []
    # the log records the ORIGINAL query with the relaxed outcome (telemetry stays honest)
    row = await actions.pool.fetchrow(
        "SELECT hits FROM search_log WHERE query='gestalt accessibility pointer hands-free "
        "training'")
    assert row is not None and row["hits"] >= 2


async def test_the_misses_log_records_recall_failures(actions: Actions) -> None:
    """Every call logs; the zero-hit rate is the embeddings tripwire — measured, not vibed."""
    await _decision(actions, "decision:x", "the membrane holds",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    await _search(actions, "membrane", caller="agent:tester")
    await _search(actions, "quantum bagels", caller="agent:tester")
    rows = await actions.pool.fetch("SELECT query, caller, hits FROM search_log ORDER BY id")
    assert [(r["query"], r["hits"]) for r in rows] == [("membrane", 1), ("quantum bagels", 0)]
    assert all(r["caller"] == "agent:tester" for r in rows)
    # and the digest surfaces the telemetry
    from datetime import timedelta

    from src.orchestrator.digest import fleet_digest
    dg = await fleet_digest(actions, since=NOW - timedelta(days=1))
    assert dg["retrieval"]["queries"] == 2 and dg["retrieval"]["zero_hits"] == 1
    assert dg["retrieval"]["top_missed"][0]["query"] == "quantum bagels"


async def test_one_row_per_object_best_witness(actions: Actions) -> None:
    """Two sources co-asserting one decision's summary → ONE hit (the multi-source set must
    not double-list), carrying the better witness."""
    await _decision(actions, "decision:co", "adopt the desk fold",
                    "agent:weak", EvidenceClass.DERIVED.value, conf=0.4)
    await _decision(actions, "decision:co", "adopt the desk fold",
                    "agent:strong", EvidenceClass.SELF_DECLARED.value)
    out = await _search(actions, "desk fold")
    assert len(out["hits"]) == 1
    assert out["hits"][0]["source"] == "agent:strong"


async def test_hardening_stopwords_never_poison_the_misses_log(actions: Actions) -> None:
    """'the of and' parses to an EMPTY tsquery — zero hits by construction, not a recall
    failure. It must return a note and stay OUT of search_log (audit finding #4)."""
    out = await _search(actions, "the of and")
    assert out["hits"] == [] and "stopwords" in out["note"]
    assert await actions.pool.fetchval("SELECT count(*) FROM search_log") == 0


async def test_hardening_limit_and_length_clamps(actions: Actions) -> None:
    """limit=-5 was a PG error; a 50KB paste is not a query (audit findings #2/#3)."""
    await _decision(actions, "decision:c", "clamp the inputs",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    spec = {"op": "function", "name": "search", "args": {"q": "clamp inputs", "limit": -5}}
    out = (await run_spec(actions.pool, spec, None, name="search"))["items"]
    assert len(out["hits"]) == 1  # clamped to >=1, no error
    long_q = "clamp " * 200  # ~1.2KB → truncated to 300 chars, still searches
    out2 = await _search(actions, long_q)
    assert len(out2["hits"]) == 1
    logged = await actions.pool.fetchval(
        "SELECT max(length(query)) FROM search_log")
    assert logged <= 300  # the log can't be ballooned by paste-bombs


async def test_hardening_log_retention_prunes_ancient_rows(actions: Actions) -> None:
    """search_log keeps 90 days — the telemetry must not grow forever (audit finding #5)."""
    await actions.pool.execute(
        "INSERT INTO search_log (query, hits, searched_at) "
        "VALUES ('ancient', 0, now() - interval '200 days')")
    await _decision(actions, "decision:r", "retention works",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    await _search(actions, "retention")
    rows = await actions.pool.fetch("SELECT query FROM search_log ORDER BY id")
    assert [r["query"] for r in rows] == ["retention"]  # the ancient row is gone


async def test_relaxed_flag_lands_in_the_telemetry(actions: Actions) -> None:
    """Zero-hits retired as the embeddings tripwire (ruling 40e68cb1); the next honest
    trigger is relaxed-hit QUALITY — so the log must remember which searches only
    survived on the ANY-term fallback."""
    now = datetime.now(UTC)
    d = await actions.create_or_find_object("Decision", "decision:relaxtest", "session")
    await actions.assert_property(d, "summary", "the wake economics ruling landed",
                                  "session", now, 0.9, evidence_class="self_declared")
    from src.orchestrator.compositions import _fn_search

    # strict: all terms present in one doc
    await _fn_search(actions.pool, None, {"q": "wake economics", "caller": "t"})
    # bag: no single doc holds all terms -> survives only relaxed
    await _fn_search(actions.pool, None,
                     {"q": "wake zzeconomics haiku triage nonexistent", "caller": "t"})
    rows = await actions.pool.fetch(
        "SELECT query, hits, relaxed FROM search_log ORDER BY id")
    assert [bool(r["relaxed"]) for r in rows] == [False, True]
    assert rows[1]["hits"] > 0  # it did survive — on relaxation

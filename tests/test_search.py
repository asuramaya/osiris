"""search v2 — the FTS engine (rung 1, thread 0deaec4f).

The old search read only the `name` property; these tests prove the rooms are searchable
(summaries/rationales), that the ranking inherits the evidence ladder, that every hit
carries testimony, and that the misses log records what the graph failed to answer.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import open_thread, record_practice
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


async def test_a_bare_hex_fragment_opens_the_id_door(actions: Actions) -> None:
    """Soundwave (msg 244): cross-referencing rulings by id was 'all manual memory'. A
    bare hex fragment is an ID, not vocabulary — prefix lookup answers directly."""
    await _decision(actions, "decision:iddoor", "the id door answers by prefix",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    oid = await actions.pool.fetchval(
        "SELECT id::text FROM objects WHERE canonical='decision:iddoor'")
    out = await _search(actions, oid[:8])
    assert out["hits"] and out["hits"][0]["canonical"] == "decision:iddoor"
    assert out["hits"][0]["via"] == "id" and "id door" in out["hits"][0]["snippet"]
    assert "id-fragment" in out["note"]
    # ...and ordinary words are never mistaken for ids ('decide' is not hex) — this
    # is specifically about the ID-FRAGMENT door, not about zero hits overall: the
    # catalog (task #97) legitimately seeds a real, searchable link type named
    # "decided_in", so a substring match on "decide" is correct search behavior,
    # not the bug this test guards against
    decide_hits = (await _search(actions, "decide"))["hits"]
    assert not any(h.get("via") == "id" for h in decide_hits)


async def test_a_practice_hit_carries_its_statement_not_a_raw_hash(actions: Actions) -> None:
    """Task #97 workstream 3 (ruling 52daab71): the reported bug verbatim — a Practice
    has none of name/title/summary (only statement/failure_prevented/surface), so a
    search hit's own `canonical` is all a raw hash gives you. Matched via lexical
    search on the statement text (not the id door), the hit must still carry a real
    label."""
    await record_practice(actions, "measure it yourself, don't trust an inherited "
                          "number", failure_prevented="a wrong verb count shipped")
    out = await _search(actions, "inherited number")
    assert out["hits"] and out["hits"][0]["type"] == "Practice"
    h = out["hits"][0]
    assert h["label"] == "measure it yourself, don't trust an inherited number"
    assert h["label_source"] == "chain"
    assert h["display_label"]  # disambiguated form is always populated too


async def test_the_id_door_labels_its_answer_too(actions: Actions) -> None:
    """The single-bare-hex-token path returns early (a separate code path from the
    lexical/fused hits below) — it must not skip label attachment just because it
    skips everything else."""
    pid = await record_practice(actions, "never hand-write kernel SQL, use the MCP surface")
    oid = await actions.pool.fetchval(
        "SELECT id::text FROM objects WHERE id=$1", pid)
    out = await _search(actions, oid[:8])
    assert out["hits"] and out["hits"][0]["via"] == "id"
    assert out["hits"][0]["label"] == "never hand-write kernel SQL, use the MCP surface"


async def test_the_id_door_also_matches_a_threads_canonical_short_hash(actions: Actions) -> None:
    """Alfred V's repro (thread 4ffe0eb9): a Thread's natural handle is its CANONICAL
    short hash (thread:23423ff856ab — a sha1 of the summary, minted by open_thread), not
    the underlying object's UUID — two different hashes name one thread, and a holder may
    quote either. Both must resolve."""
    tid = await open_thread(actions, "Alfred IV's succession handoff, unfiled and unmissable")
    canonical = await actions.pool.fetchval(
        "SELECT canonical FROM objects WHERE id=$1", tid)
    short_hash = canonical.split(":", 1)[1]  # the sha1[:12] tail, NOT the object's uuid

    by_canon = await _search(actions, short_hash)
    assert by_canon["hits"] and by_canon["hits"][0]["canonical"] == canonical
    assert by_canon["hits"][0]["via"] == "id"

    by_uuid = await _search(actions, str(tid)[:8])
    assert by_uuid["hits"] and by_uuid["hits"][0]["canonical"] == canonical


async def test_id_door_fires_on_a_token_inside_a_longer_query(actions: Actions) -> None:
    """A quoted id doesn't have to be the WHOLE query — 'dd27f61f succession torch' must
    still surface the thread the token names, merged above the ordinary text hits, while
    the pure-FTS path for queries with no id token never regresses."""
    tid = await open_thread(actions, "the succession torch passes at the seam")
    canonical = await actions.pool.fetchval(
        "SELECT canonical FROM objects WHERE id=$1", tid)

    out = await _search(actions, f"{str(tid)[:8]} succession torch")
    assert out["hits"][0]["canonical"] == canonical
    assert out["hits"][0]["via"] == "id"
    assert "id-fragment" not in out.get("note", "")  # not the bare-query early-return shape

    # a plain multi-word query with no id token still finds it by text, unregressed
    out2 = await _search(actions, "succession torch")
    assert out2["hits"] and out2["hits"][0]["canonical"] == canonical
    assert out2["hits"][0]["via"] != "id"


async def test_a_keyword_bag_relaxes_to_any_term(actions: Actions) -> None:
    """The false-empty repro (agent e46a657e-ii, msg 124): websearch ANDs every term, so
    'Hector background skills experience projects' needs all five words in ONE document —
    zero by construction while the graph is full. A plain multi-word bag that strict-AND
    can't satisfy relaxes to ANY-term, best-covered first; explicit syntax (quotes,
    operators) is never second-guessed."""
    await _decision(actions, "decision:ga", "the sibling-six head-pointer ships",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    await _decision(actions, "decision:tr", "training the reinforcement loop on pytorch",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    # strict-AND would need one doc holding all of these — instead both docs surface
    out = await _search(actions, "sibling-six accessibility pointer hands-free training")
    got = {h["canonical"] for h in out["hits"]}
    assert {"decision:ga", "decision:tr"} <= got
    assert "ANY term" in out["note"]
    # a quoted phrase is the asker's own syntax — no relaxation behind their back
    assert (await _search(actions, '"sibling-six training loop"'))["hits"] == []
    # the log records the ORIGINAL query with the relaxed outcome (telemetry stays honest)
    row = await actions.pool.fetchrow(
        "SELECT hits FROM search_log WHERE query='sibling-six accessibility pointer hands-free "
        "training'")
    assert row is not None and row["hits"] >= 2


async def test_relaxation_ranks_rarity_over_ubiquity(actions: Actions) -> None:
    """Thread 15b976ce (hector-vector msg 237): the ANY-term retry ranked flat, so a
    common word outranked a distinctive one on every multi-term bag. The relaxed leg now
    scores by summed idf — the one document holding the RARE word must beat every
    document that merely repeats the ubiquitous one."""
    # 'harness' is everywhere; 'chronohorn' lives in exactly one document
    for i in range(6):
        await _decision(actions, f"decision:common-{i}",
                        f"the harness harness harness note number {i}",
                        "agent:a", EvidenceClass.SELF_DECLARED.value)
    await _decision(actions, "decision:rare",
                    "chronohorn ships its first clock",
                    "agent:a", EvidenceClass.SELF_DECLARED.value)
    # strict-AND needs both words in one doc → nothing; the bag relaxes
    out = await _search(actions, "chronohorn harness")
    assert "ANY term" in out.get("note", "")
    assert out["hits"][0]["canonical"] == "decision:rare"  # rarity outranks repetition
    got = {h["canonical"] for h in out["hits"]}
    assert any(c.startswith("decision:common-") for c in got)  # the common hits still come


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


async def test_a_superseded_decision_is_flagged_never_hidden(actions: Actions) -> None:
    """The supersedes verb reaches search (dd04d7dd): a buried ruling still surfaces (the
    record forgets nothing) but carries a superseded flag naming its successor — a skimmer
    is never handed a corrected hypothesis as live testimony."""
    from src.orchestrator.capture import record_decision

    old = await record_decision(actions, "the flaky test is a timezone bug",
                                kind="ruling", source="agent:a")
    await record_decision(actions, "the flaky test is a race in the fixture, not timezones",
                          kind="ruling", source="agent:a", supersedes=str(old))
    out = await _search(actions, "flaky test")
    flags = {h["canonical"]: h.get("superseded") for h in out["hits"]}
    assert len(out["hits"]) == 2
    buried = [v for v in flags.values() if v]
    assert len(buried) == 1 and "read the successor" in buried[0]


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

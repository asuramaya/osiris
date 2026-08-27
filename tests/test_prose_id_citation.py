"""PROSE-ID -> EDGE (task #189's derivation lane, Thoth's dispatch msg 5865/5878,
Seshat's measurement: 37.5% of active osiris Decision+Thread objects carry at least one
recoverable citation, zero same-type collisions at 8-hex length). Two things proven
here: the extraction/resolution mechanism in isolation, and the real wiring through
record_decision/open_thread/acknowledge_prior_art."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator import capture

# --- extraction: the qualifier-word discipline -------------------------------------

def test_cited_object_refs_requires_the_qualifier_word_immediately_before_the_id() -> None:
    refs = capture._cited_object_refs(
        "per ruling d68c57e5, and obligation 7bde8729 is now closed")
    assert refs == [("Decision", "d68c57e5"), ("Thread", "7bde8729")]


def test_cited_object_refs_ignores_a_bare_id_with_no_qualifier() -> None:
    """The exact negative control Seshat's own measurement used: an 8-hex string quoted
    for some other reason, never preceded by one of the qualifier words, must not mint."""
    refs = capture._cited_object_refs("see d68c57e5 for context")
    assert refs == []


def test_cited_object_refs_dedupes_the_same_pair_across_texts() -> None:
    refs = capture._cited_object_refs("ruling d68c57e5 set this", "per ruling d68c57e5")
    assert refs == [("Decision", "d68c57e5")]


def test_cited_object_refs_recognizes_all_four_qualifiers() -> None:
    refs = capture._cited_object_refs(
        "decision 11111111, decisions 22222222, thread 33333333, threads 44444444")
    assert refs == [
        ("Decision", "11111111"), ("Decision", "22222222"),
        ("Thread", "33333333"), ("Thread", "44444444"),
    ]


# --- resolution: UUID prefix, not canonical, never a guess across type -------------

async def test_resolve_cited_object_matches_the_uuid_prefix_not_the_canonical(
    actions: Actions,
) -> None:
    """Seshat's own catch (msg 5878): the house's 8-char short id is a prefix of the
    object's UUID, not its `_canon()` hash — building against the wrong scheme
    undercounted 27x before she found it."""
    d = await capture.record_decision(actions, "a decision to be cited by uuid prefix")
    short_id = str(d)[:8]
    hit, reason = await capture._resolve_cited_object(actions.pool, "Decision", short_id)
    assert hit == d
    assert reason is None


async def test_resolve_cited_object_skips_and_names_a_real_type_mismatch(
    actions: Actions,
) -> None:
    """A citation whose qualifier claimed the wrong type resolves to nothing under that
    type, but the code checks the OTHER type too so the skip reason names what actually
    happened — never a silent guess, per Thoth's explicit instruction."""
    t = await capture.open_thread(actions, "a thread wrongly cited as a decision")
    short_id = str(t)[:8]
    hit, reason = await capture._resolve_cited_object(actions.pool, "Decision", short_id)
    assert hit is None
    assert reason is not None and "resolves to a Thread instead" in reason


async def test_resolve_cited_object_skips_when_nothing_matches_at_all(
    actions: Actions,
) -> None:
    hit, reason = await capture._resolve_cited_object(actions.pool, "Decision", "deadbeef")
    assert hit is None
    assert reason is not None and "not found" in reason


# --- the real wiring: record_decision / open_thread ---------------------------------

async def test_record_decision_mints_a_cites_edge_from_its_own_prose(
    actions: Actions,
) -> None:
    parent = await capture.record_decision(actions, "a standing ruling worth citing")
    short_id = str(parent)[:8]
    child = await capture.record_decision(
        actions, f"a follow-up per ruling {short_id}, same session")
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='cites'", child, parent)
    assert exists


async def test_record_decision_cites_is_idempotent_on_a_repeat_write(
    actions: Actions,
) -> None:
    parent = await capture.record_decision(actions, "a ruling cited twice")
    short_id = str(parent)[:8]
    summary = f"a decision citing ruling {short_id} more than once"
    await capture.record_decision(actions, summary)
    await capture.record_decision(actions, summary)  # idempotent re-record, same hash
    child = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='Decision' AND canonical=$1",
        capture._canon("decision", summary))
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='cites'",
        child, parent)
    assert n == 1


async def test_open_thread_mints_a_cites_edge_from_its_summary(actions: Actions) -> None:
    decision = await capture.record_decision(actions, "a decision a thread will cite")
    short_id = str(decision)[:8]
    thread = await capture.open_thread(
        actions, f"a stale row that references decision {short_id}")
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='cites'",
        thread, decision)
    assert exists


async def test_record_decision_never_mints_from_an_unqualified_bare_id(
    actions: Actions,
) -> None:
    parent = await capture.record_decision(actions, "a ruling never actually cited")
    short_id = str(parent)[:8]
    child = await capture.record_decision(
        actions, f"a decision that merely mentions {short_id} in passing, no qualifier")
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='cites'", child, parent)
    assert not exists


async def test_record_decision_records_a_skip_reason_for_an_unresolvable_citation(
    actions: Actions,
) -> None:
    d = await capture.record_decision(
        actions, "a decision citing ruling deadbeef, which does not exist")
    skips = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='prose_citation_skips'", d)
    assert skips is not None and any(s["ref"] == "decision deadbeef" for s in skips)


# --- self-referential flagging (Thoth's condition, same discipline as the #189 hatch) -

async def test_cites_link_flags_a_same_source_citation_as_self_referential(
    actions: Actions,
) -> None:
    parent = await capture.record_decision(
        actions, "a ruling by one lineage", source="agent:same-lineage")
    short_id = str(parent)[:8]
    child = await capture.record_decision(
        actions, f"a follow-up per ruling {short_id}, same author",
        source="agent:same-lineage")
    props = await actions.pool.fetchval(
        "SELECT properties FROM links WHERE from_id=$1 AND to_id=$2 AND type='cites'",
        child, parent)
    assert props["self_referential"] is True


async def test_cites_link_flags_a_different_source_citation_as_cross_author(
    actions: Actions,
) -> None:
    parent = await capture.record_decision(
        actions, "a ruling by one author", source="agent:author-a")
    short_id = str(parent)[:8]
    child = await capture.record_decision(
        actions, f"an independent follow-up per ruling {short_id}",
        source="agent:author-b")
    props = await actions.pool.fetchval(
        "SELECT properties FROM links WHERE from_id=$1 AND to_id=$2 AND type='cites'",
        child, parent)
    assert props["self_referential"] is False


# --- acknowledge_prior_art promotion -------------------------------------------------

async def test_acknowledge_prior_art_mints_a_cites_edge_never_self_referential(
    actions: Actions,
) -> None:
    other = await capture.record_decision(actions, "a standing decision surfaced as prior art")
    d = await capture.record_decision(actions, "a fresh decision acknowledging it")
    await capture.acknowledge_prior_art(actions, d, str(other), "session")
    exists = await actions.pool.fetchval(
        "SELECT properties FROM links WHERE from_id=$1 AND to_id=$2 AND type='cites'",
        d, other)
    assert exists is not None and exists["self_referential"] is False
    # the property write survives too — additive, not a replacement
    prop = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='prior_art_acknowledged'", d)
    assert prop == str(other)


async def test_mint_cites_never_mints_a_self_loop(actions: Actions) -> None:
    d = await capture.record_decision(actions, "a decision that can never cite itself")
    minted = await capture.mint_cites(actions, d, d, "session", self_referential=True)
    assert minted is False

from __future__ import annotations

import re as _re
from datetime import UTC, datetime

import pytest
from src.actions.core import ActionError, Actions

NOW = datetime(2026, 5, 27, tzinfo=UTC)


async def test_create_or_find_is_idempotent(actions: Actions, case_id: str) -> None:
    a = await actions.create_or_find_object("Email", "alice@example.com", "analyst:test", case_id)
    b = await actions.create_or_find_object("Email", "alice@example.com", "analyst:test", case_id)
    assert a == b

    # exactly one object, one create event, one object_created outbox, one create_object audit
    # (excluding the session-persistent Type catalog, task #97 — not this test's business)
    assert await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type <> 'Type'") == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE event_type='create'"
    ) == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM outbox WHERE event_type='object_created'"
    ) == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM audit_log WHERE action='create_object'"
    ) == 1
    # case membership recorded once
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM case_objects WHERE object_id=$1", a
    ) == 1


async def test_assert_property_supersedes_within_source_but_keeps_set(
    actions: Actions, case_id: str
) -> None:
    obj = await actions.create_or_find_object("Domain", "corp.com", "analyst:test", case_id)

    # same source asserts twice -> the second supersedes the first
    first = await actions.assert_property(obj, "registrar", "GoDaddy", "helper:rdap", NOW, 0.9)
    second = await actions.assert_property(obj, "registrar", "Namecheap", "helper:rdap", NOW, 0.9)
    superseded_by = await actions.pool.fetchval(
        "SELECT supersedes FROM assertions WHERE id=$1", second
    )
    assert superseded_by == first

    # within-source current value is just the latest
    vals = await actions.current_values(obj, "registrar")
    assert [v["value"] for v in vals] == ["Namecheap"]

    # a *different* source's value coexists -> multi-source set (ruling #2)
    await actions.assert_property(obj, "registrar", "MarkMonitor", "helper:whois", NOW, 0.7)
    vals = await actions.current_values(obj, "registrar")
    assert {v["value"] for v in vals} == {"Namecheap", "MarkMonitor"}


async def test_assert_property_flips_is_current_on_the_exact_row_it_supersedes(
    actions: Actions, case_id: str,
) -> None:
    """migration 0047/thread 2a280e07: is_current is the maintained flag current_assertions'
    view now reads instead of re-deriving the anti-join on every call. Same-source supersede
    must flip false on the superseded row only — never the other source's row (#102's own
    coexistence rule, unaffected by this being a flag instead of a live NOT EXISTS)."""
    obj = await actions.create_or_find_object("Domain", "corp.com", "analyst:test", case_id)
    first = await actions.assert_property(obj, "registrar", "GoDaddy", "helper:rdap", NOW, 0.9)
    other_source = await actions.assert_property(
        obj, "registrar", "MarkMonitor", "helper:whois", NOW, 0.7)
    second = await actions.assert_property(obj, "registrar", "Namecheap", "helper:rdap", NOW, 0.9)

    rows = {r["id"]: r["is_current"] for r in await actions.pool.fetch(
        "SELECT id, is_current FROM assertions WHERE id = ANY($1::bigint[])",
        [first, other_source, second])}
    assert rows == {first: False, other_source: True, second: True}


# ═══ the is_current invariant (thread 09bde57e, piece c): every write path that sets
# assertions.supersedes flips the target's is_current=false in the SAME transaction, so no
# LIVE write can ever leave a fresh stale flag. khepri's own specimen (2696774 supersedes
# 2676719, is_current still true on the superseded row) and the 123,914-row population it
# led to measuring (d8225e71) are BOTH from before this flip discipline existed — a
# migration-time backfill-completeness gap, not a hole in the two paths below. This suite
# proves the two paths hold the invariant live, and that nothing else in the codebase writes
# `supersedes` without them. ═══


def test_static_check_only_two_sites_write_the_supersedes_column() -> None:
    """A THIRD insert path into assertions.supersedes, added later without this discipline,
    is exactly how a fresh stale-flag population gets born again. Grep the whole src tree —
    if this ever finds a third site, the invariant needs a new flip, not a wider allowlist."""
    import pathlib

    sites: list[str] = []
    for path in pathlib.Path("src").rglob("*.py"):
        src = path.read_text()
        for m in _re.finditer(r"INSERT INTO assertions\b", src):
            # column list may be split across adjacent string literals (line-wrapped
            # SQL) — look for `supersedes` anywhere before the matching VALUES clause
            window = src[m.start():m.start() + 400]
            values_at = window.find("VALUES")
            column_text = window[:values_at] if values_at != -1 else window
            if _re.search(r"\bsupersedes\b", column_text):
                sites.append(f"{path}:{src.count(chr(10), 0, m.start()) + 1}")
    assert len(sites) == 2, (
        f"expected exactly assert_property + supersede_assertion, found {sites!r} — a new "
        "site must flip is_current in the SAME transaction as its INSERT, per this file's "
        "own two precedents, before it's added to this allowlist")
    assert all(s.startswith("src/actions/core.py:") for s in sites)


async def test_contract_assert_property_leaves_zero_stale_flags(
    actions: Actions, case_id: str,
) -> None:
    """End-to-end: after a same-source supersede, stale_current_flags (the thread 09bde57e
    read door) must report zero for this row — the flip is real, not just is_current itself
    correct in isolation."""
    from src.orchestrator.retirement import stale_current_flags

    obj = await actions.create_or_find_object("Domain", "flip1.com", "analyst:test", case_id)
    first = await actions.assert_property(obj, "registrar", "GoDaddy", "helper:rdap", NOW, 0.9)
    await actions.assert_property(obj, "registrar", "Namecheap", "helper:rdap", NOW, 0.9)

    out = await stale_current_flags(actions)
    assert first not in {s["stale_id"] for s in out["sample"]}
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM assertions a JOIN assertions s ON s.supersedes=a.id "
        "WHERE a.id=$1 AND a.is_current", first) == 0


async def test_contract_supersede_assertion_leaves_zero_stale_flags(
    actions: Actions, case_id: str,
) -> None:
    """Same contract, the cross-source path (supersede_assertion) — the other of exactly two
    live writers of assertions.supersedes."""
    from src.orchestrator.retirement import stale_current_flags

    obj = await actions.create_or_find_object("Domain", "flip2.com", "analyst:test", case_id)
    wrong = await actions.assert_property(obj, "registrar", "GoDaddy", "helper:self", NOW, 0.9)
    await actions.assert_property(obj, "registrar", "Namecheap", "helper:peer", NOW, 0.9)
    await actions.supersede_assertion(
        obj, "registrar", wrong, "Namecheap", "helper:correction", NOW, 0.9,
        "peer correction", evidence_class="self_declared")

    out = await stale_current_flags(actions)
    assert wrong not in {s["stale_id"] for s in out["sample"]}


async def test_repair_stale_current_flags_heals_a_raw_written_gap(
    actions: Actions, case_id: str,
) -> None:
    """The Actions-layer repair (thread 09bde57e, piece d) against exactly the shape a
    pre-flip-discipline write (or any write outside the two allowlisted sites) leaves
    behind: a real supersedes FK, is_current never flipped. Batched, idempotent — a second
    call against an already-clean population repairs nothing."""
    obj = await actions.create_or_find_object("Domain", "stale-repair.com", "analyst:test", case_id)
    stale_id = await actions.assert_property(obj, "registrar", "GoDaddy", "helper:self", NOW, 0.9)
    await actions.pool.execute(
        "INSERT INTO assertions (object_id, name, value, source_id, observed_at, confidence, "
        "supersedes, evidence_class) VALUES ($1,'registrar','\"Namecheap\"','helper:revert', "
        "$2, 0.9, $3, 'self_declared')", obj, NOW, stale_id)
    assert await actions.pool.fetchval(
        "SELECT is_current FROM assertions WHERE id=$1", stale_id) is True

    repaired = await actions.repair_stale_current_flags(limit=500, actor="analyst:test")
    assert stale_id in repaired
    assert await actions.pool.fetchval(
        "SELECT is_current FROM assertions WHERE id=$1", stale_id) is False
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM audit_log WHERE action='repair_stale_current_flags'") == 1

    # idempotent — nothing left to repair on a repeat call
    again = await actions.repair_stale_current_flags(limit=500, actor="analyst:test")
    assert stale_id not in again


async def test_repair_stale_current_flags_respects_the_batch_limit(
    actions: Actions, case_id: str,
) -> None:
    obj = await actions.create_or_find_object("Domain", "stale-batch.com", "analyst:test", case_id)
    stale_ids = []
    for i in range(3):
        sid = await actions.assert_property(obj, f"prop{i}", "old", f"helper:src{i}", NOW, 0.9)
        stale_ids.append(sid)
        await actions.pool.execute(
            "INSERT INTO assertions (object_id, name, value, source_id, observed_at, "
            "confidence, supersedes, evidence_class) VALUES "
            f"($1,'prop{i}','\"new\"','helper:revert', $2, 0.9, $3, 'self_declared')",
            obj, NOW, sid)

    repaired = await actions.repair_stale_current_flags(limit=1, actor="analyst:test")
    assert len(repaired) == 1
    still_stale = await actions.pool.fetchval(
        "SELECT count(*) FROM assertions a JOIN assertions s ON s.supersedes=a.id "
        "WHERE a.is_current AND a.id = ANY($1::bigint[])", stale_ids)
    assert still_stale == 2


async def test_create_link(actions: Actions, case_id: str) -> None:
    a = await actions.create_or_find_object("Email", "a@x.com", "analyst:test", case_id)
    b = await actions.create_or_find_object("Account", "github:a", "analyst:test", case_id)
    link_id = await actions.create_link(a, b, "registered_with", "helper:holehe", NOW, 0.8)
    assert link_id > 0

    row = await actions.pool.fetchrow("SELECT * FROM links WHERE id=$1", link_id)
    assert row["type"] == "registered_with"
    assert row["first_seen"] == row["last_seen"] == NOW
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM outbox WHERE event_type='link_created'"
    ) == 1


async def test_create_or_find_object_accretes_an_undeclared_type(
    actions: Actions, case_id: str
) -> None:
    """Task #97 workstream 2 — the accretion hook: an undeclared type SELF-DECLARES as a
    stub instead of merely warning. The write always succeeds even in the default
    warn-only runtime mode, and a real Type object materializes for it."""
    from src.ontology.catalog import is_known_object_type, set_strict

    set_strict(False)
    try:
        obj = await actions.create_or_find_object(
            "NeverDeclaredWidget", "widget-1", "analyst:test", case_id)
    finally:
        set_strict(True)
    assert obj is not None
    assert await is_known_object_type(actions.pool, "NeverDeclaredWidget")


async def test_create_or_find_object_still_raises_in_strict_mode(
    actions: Actions, case_id: str
) -> None:
    """Strict mode ALWAYS wins over accretion — it exists so CI catches a real typo as
    a hard failure, and silently minting a stub past that would defeat the point."""
    from src.ontology.catalog import UnknownTypeError, is_known_object_type

    with pytest.raises(UnknownTypeError):
        await actions.create_or_find_object("ShouldRaiseWidget", "widget-2", "analyst:test",
                                            case_id)
    assert not await is_known_object_type(actions.pool, "ShouldRaiseWidget")


async def test_create_link_accretes_an_undeclared_link_type(
    actions: Actions, case_id: str
) -> None:
    from src.ontology.catalog import is_known_link_type, set_strict

    a = await actions.create_or_find_object("Email", "a2@x.com", "analyst:test", case_id)
    b = await actions.create_or_find_object("Account", "github:a2", "analyst:test", case_id)
    set_strict(False)
    try:
        link_id = await actions.create_link(a, b, "never_declared_rel", "helper:test", NOW, 0.8)
    finally:
        set_strict(True)
    assert link_id > 0
    assert await is_known_link_type(actions.pool, "never_declared_rel")


async def test_merge_objects_is_event_sourced_and_resolves(
    actions: Actions, case_id: str
) -> None:
    winner = await actions.create_or_find_object("Person", "p-winner", "analyst:test", case_id)
    loser = await actions.create_or_find_object("Person", "p-loser", "analyst:test", case_id)

    await actions.merge_objects(winner, loser, "same DOB + email", "analyst:test", case_id)

    # projection updated
    row = await actions.pool.fetchrow("SELECT status, merged_into FROM objects WHERE id=$1", loser)
    assert row["status"] == "merged"
    assert row["merged_into"] == winner
    # identity resolves loser -> winner
    assert await actions.resolve_object_id(loser) == winner
    # event recorded (source of truth) + same_as link + audit + outbox
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE event_type='merge' AND object_id=$1", winner
    ) == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='same_as'", loser, winner
    ) == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM outbox WHERE event_type='object_merged'"
    ) == 1


async def test_merge_guards(actions: Actions, case_id: str) -> None:
    a = await actions.create_or_find_object("Person", "g-a", "analyst:test", case_id)
    with pytest.raises(ActionError):
        await actions.merge_objects(a, a, "self", "analyst:test", case_id)

    b = await actions.create_or_find_object("Person", "g-b", "analyst:test", case_id)
    await actions.merge_objects(a, b, "ok", "analyst:test", case_id)
    with pytest.raises(ActionError):  # b already merged
        await actions.merge_objects(a, b, "again", "analyst:test", case_id)


async def test_split_object_records_lineage(actions: Actions, case_id: str) -> None:
    obj = await actions.create_or_find_object("Person", "conflated", "analyst:test", case_id)
    spec = {"parts": [{"type": "Person", "canonical": "real-1"},
                      {"type": "Person", "canonical": "real-2"}]}
    new_ids = await actions.split_object(obj, spec, "two people conflated", "analyst:test", case_id)

    assert len(new_ids) == 2
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE event_type='split' AND object_id=$1", obj
    ) == 2
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM audit_log WHERE action='split_object'"
    ) == 1


async def test_tag_object_is_additive(actions: Actions, case_id: str) -> None:
    obj = await actions.create_or_find_object("Domain", "evil.tld", "analyst:test", case_id)
    await actions.tag_object(obj, "c2", "case", "analyst:test", case_id)
    await actions.tag_object(obj, "phishing", "case", "analyst:test", case_id)

    tags = {t["value"]["tag"] for t in await actions.current_values(obj, "tag")}
    assert tags == {"c2", "phishing"}
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM audit_log WHERE action='tag_object'"
    ) == 2


async def test_find_never_returns_a_corpse(actions: Actions, case_id: str) -> None:
    """THE CORPSE GATE (task #29): a canonical held by a MERGED object resolves to its
    living head — 2410 in_repo/works_in links landed on merged repo:osiris because
    find-or-create returned whatever row owned the name, status unread."""
    corpse = await actions.create_or_find_object(
        "SoftwareProject", "repo:corpse-gate", "analyst:test", case_id)
    head = await actions.create_or_find_object(
        "SoftwareProject", "repo:corpse-gate-head", "analyst:test", case_id)
    await actions.merge_objects(head, corpse, "one project", "analyst:test", case_id)
    found = await actions.create_or_find_object(
        "SoftwareProject", "repo:corpse-gate", "analyst:test", case_id)
    assert found == head  # the canonical's owner is merged — the find walks to the head


async def test_unmerge_restores_the_loser(actions: Actions, case_id: str) -> None:
    """The compensating act: unmerge writes an event, restores the projection, and
    leaves the original merge event + same_as link as witnesses of the era."""
    w = await actions.create_or_find_object("Person", "un-w", "analyst:test", case_id)
    l_ = await actions.create_or_find_object("Person", "un-l", "analyst:test", case_id)
    await actions.merge_objects(w, l_, "wrong direction", "analyst:test", case_id)
    await actions.unmerge_objects(l_, "the direction was backwards", "analyst:test", case_id)
    row = await actions.pool.fetchrow(
        "SELECT status, merged_into FROM objects WHERE id=$1", l_)
    assert row["status"] == "active" and row["merged_into"] is None
    # the era's witnesses survive: merge event + same_as link + the new unmerge event
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE event_type='merge' AND related_id=$1", l_) == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE event_type='unmerge' AND object_id=$1", l_) == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='same_as'", l_) == 1
    # and the pair can now merge the RIGHT way
    await actions.merge_objects(l_, w, "right direction", "analyst:test", case_id)
    assert await actions.resolve_object_id(w) == l_


async def test_unmerge_guards(actions: Actions, case_id: str) -> None:
    a = await actions.create_or_find_object("Person", "un-g", "analyst:test", case_id)
    with pytest.raises(ActionError):  # not merged
        await actions.unmerge_objects(a, "nothing to undo", "analyst:test", case_id)


async def test_supersede_assertion_retires_a_different_sources_row(
    actions: Actions, case_id: str,
) -> None:
    """The cross-source supersede (thread 52911d2a) — assert_property's own within-source
    rule leaves two different sources' rows coexisting; supersede_assertion is the one
    legitimate way to retire one of them explicitly, by id."""
    obj = await actions.create_or_find_object("Domain", "corp.com", "analyst:test", case_id)
    wrong = await actions.assert_property(obj, "registrar", "GoDaddy", "helper:self", NOW, 0.9)
    await actions.assert_property(obj, "registrar", "Namecheap", "helper:peer", NOW, 0.9)
    vals = await actions.current_values(obj, "registrar")
    assert {v["value"] for v in vals} == {"GoDaddy", "Namecheap"}  # both current, cross-source

    new_id = await actions.supersede_assertion(
        obj, "registrar", wrong, "Namecheap", "helper:correction", NOW, 0.9,
        "peer correction, verified live", evidence_class="self_declared")

    superseded_by = await actions.pool.fetchval(
        "SELECT supersedes FROM assertions WHERE id=$1", new_id)
    assert superseded_by == wrong
    vals = await actions.current_values(obj, "registrar")
    assert {v["value"] for v in vals} == {"Namecheap"}  # the wrong row is gone from current
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM audit_log WHERE action='supersede_assertion'") == 1
    # migration 0047/thread 2a280e07: the cross-source retirement flips is_current too,
    # same discipline as assert_property's own same-source flip
    assert await actions.pool.fetchval(
        "SELECT is_current FROM assertions WHERE id=$1", wrong) is False
    assert await actions.pool.fetchval(
        "SELECT is_current FROM assertions WHERE id=$1", new_id) is True


async def test_supersede_assertion_guards(actions: Actions, case_id: str) -> None:
    obj = await actions.create_or_find_object("Domain", "corp.com", "analyst:test", case_id)
    other = await actions.create_or_find_object("Domain", "other.com", "analyst:test", case_id)
    row_id = await actions.assert_property(obj, "registrar", "GoDaddy", "helper:self", NOW, 0.9)

    with pytest.raises(ActionError):  # wrong object
        await actions.supersede_assertion(
            other, "registrar", row_id, "X", "helper:x", NOW, 0.9, "wrong object")
    with pytest.raises(ActionError):  # wrong name
        await actions.supersede_assertion(
            obj, "not-registrar", row_id, "X", "helper:x", NOW, 0.9, "wrong name")
    with pytest.raises(ActionError):  # unknown id
        await actions.supersede_assertion(
            obj, "registrar", 999999999, "X", "helper:x", NOW, 0.9, "unknown id")

    await actions.supersede_assertion(
        obj, "registrar", row_id, "Namecheap", "helper:peer", NOW, 0.9, "correction")
    with pytest.raises(ActionError):  # already superseded — never twice
        await actions.supersede_assertion(
            obj, "registrar", row_id, "MarkMonitor", "helper:another", NOW, 0.9, "twice")

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from src.actions.core import ActionError, Actions

NOW = datetime(2026, 5, 27, tzinfo=UTC)


async def test_create_or_find_is_idempotent(actions: Actions, case_id: str) -> None:
    a = await actions.create_or_find_object("Email", "alice@example.com", "analyst:test", case_id)
    b = await actions.create_or_find_object("Email", "alice@example.com", "analyst:test", case_id)
    assert a == b

    # exactly one object, one create event, one object_created outbox, one create_object audit
    assert await actions.pool.fetchval("SELECT count(*) FROM objects") == 1
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

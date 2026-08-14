"""PEER_OF (ruling d74492ee, spec e6636c7e) — a symmetric Seat<->Seat bond. These witness
the two verbs' full refusal shape and the SYMMETRIC read every legibility surface depends on:
peer_of_seat must answer the same regardless of which side peer_seats happened to mint as
`from_id`, and unpeer must heal the bond regardless of which order it's asked to release it in.
"""
from __future__ import annotations

import asyncio

from src.actions.core import Actions


async def _seat(actions: Actions, canon: str) -> None:
    await actions.create_or_find_object("Seat", canon, "test")


async def test_peer_seats_creates_a_symmetric_bond(actions: Actions) -> None:
    from src.orchestrator.seats import peer_of_seat, peer_seats

    await _seat(actions, "seat:pr1aaaaa")
    await _seat(actions, "seat:pr1bbbbb")

    out = await peer_seats(actions, "seat:pr1aaaaa", "seat:pr1bbbbb",
                           because="the reconciliation, msg 1770", actor="test")

    assert out == {"peered": ["seat:pr1aaaaa", "seat:pr1bbbbb"],
                   "because": "the reconciliation, msg 1770"}
    # symmetric: the read answers correctly from EITHER seat, regardless of which one
    # actually landed as the link's own from_id
    assert await peer_of_seat(actions.pool, "seat:pr1aaaaa") == "seat:pr1bbbbb"
    assert await peer_of_seat(actions.pool, "seat:pr1bbbbb") == "seat:pr1aaaaa"


async def test_peer_seats_refuses_blank_because(actions: Actions) -> None:
    from src.orchestrator.seats import peer_seats

    await _seat(actions, "seat:pr2aaaaa")
    await _seat(actions, "seat:pr2bbbbb")

    out = await peer_seats(actions, "seat:pr2aaaaa", "seat:pr2bbbbb", because="  ",
                           actor="test")
    assert "because is required" in out["error"]


async def test_peer_seats_refuses_an_unknown_seat(actions: Actions) -> None:
    from src.orchestrator.seats import peer_seats

    await _seat(actions, "seat:pr3aaaaa")

    out = await peer_seats(actions, "seat:pr3aaaaa", "seat:pr3nosuch", because="test",
                           actor="test")
    assert "no such active seat" in out["error"] and "pr3nosuch" in out["error"]


async def test_peer_seats_refuses_an_inactive_seat(actions: Actions) -> None:
    from src.orchestrator.seats import peer_seats

    await _seat(actions, "seat:pr4aaaaa")
    seat_b = await actions.create_or_find_object("Seat", "seat:pr4bbbbb", "test")
    await actions.set_status(seat_b, "retired", "unrelated retirement", "test")

    out = await peer_seats(actions, "seat:pr4aaaaa", "seat:pr4bbbbb", because="test",
                           actor="test")
    assert "no such active seat" in out["error"] and "pr4bbbbb" in out["error"]


async def test_peer_seats_refuses_pairing_a_seat_with_itself(actions: Actions) -> None:
    from src.orchestrator.seats import peer_seats

    await _seat(actions, "seat:pr5aaaaa")

    out = await peer_seats(actions, "seat:pr5aaaaa", "seat:pr5aaaaa", because="test",
                           actor="test")
    assert "cannot be peered with itself" in out["error"]


async def test_peer_seats_refuses_when_seat_a_already_has_a_peer(actions: Actions) -> None:
    from src.orchestrator.seats import peer_seats

    await _seat(actions, "seat:pr6aaaaa")
    await _seat(actions, "seat:pr6bbbbb")
    await _seat(actions, "seat:pr6ccccc")
    await peer_seats(actions, "seat:pr6aaaaa", "seat:pr6bbbbb", because="first pairing",
                     actor="test")

    out = await peer_seats(actions, "seat:pr6aaaaa", "seat:pr6ccccc", because="second try",
                           actor="test")
    assert "already has a peer" in out["error"] and "seat:pr6bbbbb" in out["error"]
    assert "pairs only" in out["error"]


async def test_peer_seats_refuses_when_seat_b_already_has_a_peer(actions: Actions) -> None:
    """The symmetric half of the precondition: an existing bond blocks a THIRD seat from
    pairing with either side, not just the side named `seat_a` at mint time."""
    from src.orchestrator.seats import peer_seats

    await _seat(actions, "seat:pr7aaaaa")
    await _seat(actions, "seat:pr7bbbbb")
    await _seat(actions, "seat:pr7ccccc")
    await peer_seats(actions, "seat:pr7aaaaa", "seat:pr7bbbbb", because="first pairing",
                     actor="test")

    out = await peer_seats(actions, "seat:pr7ccccc", "seat:pr7bbbbb", because="second try",
                           actor="test")
    assert "already has a peer" in out["error"] and "seat:pr7aaaaa" in out["error"]


async def test_unpeer_heals_the_bond_read_from_either_direction(actions: Actions) -> None:
    from src.orchestrator.seats import peer_of_seat, peer_seats, unpeer

    await _seat(actions, "seat:pr8aaaaa")
    await _seat(actions, "seat:pr8bbbbb")
    await peer_seats(actions, "seat:pr8aaaaa", "seat:pr8bbbbb", because="paired",
                     actor="test")

    # released in the REVERSE order from how it was minted — the bond is symmetric, so
    # this must still find and heal it
    out = await unpeer(actions, "seat:pr8bbbbb", "seat:pr8aaaaa", because="reconciled apart",
                       actor="test")

    assert out == {"unpeered": ["seat:pr8bbbbb", "seat:pr8aaaaa"], "because": "reconciled apart"}
    assert await peer_of_seat(actions.pool, "seat:pr8aaaaa") is None
    assert await peer_of_seat(actions.pool, "seat:pr8bbbbb") is None
    # each side's own record carries the reason, symmetrically
    for canon in ("seat:pr8aaaaa", "seat:pr8bbbbb"):
        val = await actions.pool.fetchval(
            "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
            "ON a.object_id=o.id AND a.name='unpeer_because' WHERE o.canonical=$1", canon)
        assert val == "reconciled apart"
    # healed, not merely one-sided — a fresh pairing is free to form again
    again = await peer_seats(actions, "seat:pr8aaaaa", "seat:pr8bbbbb", because="re-paired",
                             actor="test")
    assert "peered" in again


async def test_unpeer_refuses_blank_because(actions: Actions) -> None:
    from src.orchestrator.seats import peer_seats, unpeer

    await _seat(actions, "seat:pr9aaaaa")
    await _seat(actions, "seat:pr9bbbbb")
    await peer_seats(actions, "seat:pr9aaaaa", "seat:pr9bbbbb", because="paired", actor="test")

    out = await unpeer(actions, "seat:pr9aaaaa", "seat:pr9bbbbb", because=" ", actor="test")
    assert "because is required" in out["error"]


async def test_unpeer_refuses_when_not_peered(actions: Actions) -> None:
    from src.orchestrator.seats import unpeer

    await _seat(actions, "seat:praaaaaa")
    await _seat(actions, "seat:prbbbbbb")

    out = await unpeer(actions, "seat:praaaaaa", "seat:prbbbbbb", because="test", actor="test")
    assert "are not peered" in out["error"]


async def test_peer_of_seat_is_none_for_an_unpeered_seat(actions: Actions) -> None:
    from src.orchestrator.seats import peer_of_seat

    await _seat(actions, "seat:prcccccc")
    assert await peer_of_seat(actions.pool, "seat:prcccccc") is None
    assert await peer_of_seat(actions.pool, "seat:no-such-seat") is None


async def test_peer_seats_serializes_two_concurrent_calls_sharing_a_seat(
    actions: Actions,
) -> None:
    """Task #76 item 1: peer_seats' own "already has a peer" check and its create_link were
    two round-trips with no lock between them — two concurrent callers both trying to pair a
    THIRD seat onto the same shared seat could each pass the check before either wrote,
    leaving that seat with two active peer_of edges. Fire both at once; exactly one must
    land, the other must see the fresh bond and refuse — never both succeeding."""
    from src.orchestrator.seats import peer_of_seat, peer_seats

    await _seat(actions, "seat:prlockaa")
    await _seat(actions, "seat:prlockbb")
    await _seat(actions, "seat:prlockcc")

    results = await asyncio.gather(
        peer_seats(actions, "seat:prlockaa", "seat:prlockbb", because="racer 1",
                   actor="test"),
        peer_seats(actions, "seat:prlockcc", "seat:prlockbb", because="racer 2",
                   actor="test"),
    )

    successes = [r for r in results if "peered" in r]
    failures = [r for r in results if "error" in r]
    assert len(successes) == 1, results
    assert len(failures) == 1, results
    assert "already has a peer" in failures[0]["error"]
    # the graph agrees with whichever racer actually won — no double bond either way
    winner_peer = "seat:prlockaa" if successes[0]["peered"][0] == "seat:prlockaa" \
        else "seat:prlockcc"
    assert await peer_of_seat(actions.pool, "seat:prlockbb") == winner_peer


async def test_hold_action_mints_an_owned_severity_hold_thread(actions: Actions) -> None:
    """Task #76 item 4a: the recorded half of a mutual HOLD reuses open_thread's ordinary
    obligation shape — owner=held (whose move it is), severity='hold' (filterable, never
    a text match), the hold's own fields stamped on the same thread object."""
    from src.orchestrator.seats import hold_action, peer_seats

    await _seat(actions, "seat:hd1aaaaa")
    await _seat(actions, "seat:hd1bbbbb")
    await peer_seats(actions, "seat:hd1aaaaa", "seat:hd1bbbbb", because="paired",
                     actor="test")

    out = await hold_action(actions, "seat:hd1aaaaa", "seat:hd1bbbbb",
                            act="deleting the shared checkout", because="risky, unreviewed",
                            hours=6, actor="test")

    assert out["holder"] == "seat:hd1aaaaa" and out["held_seat"] == "seat:hd1bbbbb"
    assert out["act"] == "deleting the shared checkout"
    for name, expected in (
        ("owner", "seat:hd1bbbbb"), ("severity", "hold"),
        ("hold_holder", "seat:hd1aaaaa"), ("hold_held", "seat:hd1bbbbb"),
        ("hold_act", "deleting the shared checkout"),
        ("hold_because", "risky, unreviewed"), ("hold_deadline", out["deadline"]),
    ):
        val = await actions.pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a "
            "WHERE a.object_id::text=$1 AND a.name=$2", out["held"], name)
        assert val == expected, name


async def test_hold_action_refuses_blank_act_or_because(actions: Actions) -> None:
    from src.orchestrator.seats import hold_action, peer_seats

    await _seat(actions, "seat:hd2aaaaa")
    await _seat(actions, "seat:hd2bbbbb")
    await peer_seats(actions, "seat:hd2aaaaa", "seat:hd2bbbbb", because="paired",
                     actor="test")

    out = await hold_action(actions, "seat:hd2aaaaa", "seat:hd2bbbbb", act="  ",
                            because="test", actor="test")
    assert "act is required" in out["error"]
    out = await hold_action(actions, "seat:hd2aaaaa", "seat:hd2bbbbb", act="test",
                            because=" ", actor="test")
    assert "because is required" in out["error"]


async def test_hold_action_refuses_non_positive_hours(actions: Actions) -> None:
    from src.orchestrator.seats import hold_action, peer_seats

    await _seat(actions, "seat:hd3aaaaa")
    await _seat(actions, "seat:hd3bbbbb")
    await peer_seats(actions, "seat:hd3aaaaa", "seat:hd3bbbbb", because="paired",
                     actor="test")

    out = await hold_action(actions, "seat:hd3aaaaa", "seat:hd3bbbbb", act="test",
                            because="test", hours=0, actor="test")
    assert "hours must be positive" in out["error"]


async def test_hold_action_refuses_a_seat_holding_itself(actions: Actions) -> None:
    from src.orchestrator.seats import hold_action

    await _seat(actions, "seat:hd4aaaaa")

    out = await hold_action(actions, "seat:hd4aaaaa", "seat:hd4aaaaa", act="test",
                            because="test", actor="test")
    assert "cannot hold its own act" in out["error"]


async def test_hold_action_refuses_an_unknown_seat(actions: Actions) -> None:
    from src.orchestrator.seats import hold_action

    await _seat(actions, "seat:hd5aaaaa")

    out = await hold_action(actions, "seat:hd5aaaaa", "seat:hd5nosuch", act="test",
                            because="test", actor="test")
    assert "no such active seat" in out["error"] and "hd5nosuch" in out["error"]


async def test_hold_action_refuses_a_non_peer(actions: Actions) -> None:
    """v1's hold is a peer's own power — a seat with no active peer_of bond to the other
    side cannot hold its act, even if both seats otherwise exist and are active."""
    from src.orchestrator.seats import hold_action

    await _seat(actions, "seat:hd6aaaaa")
    await _seat(actions, "seat:hd6bbbbb")

    out = await hold_action(actions, "seat:hd6aaaaa", "seat:hd6bbbbb", act="test",
                            because="test", actor="test")
    assert "not an active peer_of pair" in out["error"]


async def test_hold_action_is_resolved_by_the_ordinary_resolve_thread_verb(
    actions: Actions,
) -> None:
    """No new resolve path — the hold IS a Thread, so resolve_thread heals it exactly like
    any other obligation."""
    from src.orchestrator.seats import hold_action, peer_seats

    await _seat(actions, "seat:hd7aaaaa")
    await _seat(actions, "seat:hd7bbbbb")
    await peer_seats(actions, "seat:hd7aaaaa", "seat:hd7bbbbb", because="paired",
                     actor="test")
    out = await hold_action(actions, "seat:hd7aaaaa", "seat:hd7bbbbb", act="test act",
                            because="test", actor="test")

    from src.orchestrator.capture import resolve_thread

    resolved = await resolve_thread(actions, out["held"], because="respected the hold",
                                    source="test")
    assert resolved is not None
    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "WHERE a.object_id::text=$1 AND a.name='status'", out["held"])
    assert status == "resolved"

"""PEER_OF (ruling d74492ee, spec e6636c7e) — a symmetric Seat<->Seat bond. These witness
the two verbs' full refusal shape and the SYMMETRIC read every legibility surface depends on:
peer_of_seat must answer the same regardless of which side peer_seats happened to mint as
`from_id`, and unpeer must heal the bond regardless of which order it's asked to release it in.
"""
from __future__ import annotations

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

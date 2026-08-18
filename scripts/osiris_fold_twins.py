"""FOLD THE TWINS — the forks that were already seated twice, before we knew what a fork was.

`claude --fork-session --resume` gives one running conversation a NEW session id, and every one
of those got its own seat (see src/orchestrator/forks.py for the autopsy, ruling 7cbc2f98). The
plumbing is fixed going forward; this is the sediment.

WHAT IT DOES, AND WHAT IT REFUSES TO DO. It re-points the fork's MOUNT ROW at the seat the mind
already had, so every future call on that anchor lands on the real agent. The mount registry is a
lookup table — a projection of "who is at this anchor" — and correcting it is a correction, not
an erasure.

IT DELETES NOTHING, AND IT REWRITES NO AUTHORSHIP. The twin agent really did write what it wrote
(Anubis XII's own bug report came from his twin), and every object it authored keeps naming it.
The Agent object is left exactly as it stands. What changes is only the REGISTRY: the anchor now
resolves to the real seat, the twin's UNREAD mail is carried across to that seat (fixing the
mailbox while destroying the mail would be a fine joke to play once), and no row is left claiming
a heartbeat for a mind that is already counted elsewhere. The record stops over-counting minds; it
does not un-say anything anyone said.

AND IT ONLY TOUCHES THE ANONYMOUS ONES. A twin that CLAIMED A NAME is testimony — a deliberate act
of identity by a living mind — and folding it would be an inference overruling a declaration,
which is this codebase's named disease. Those are reported and left alone: an identity merge is
the operator's call, always (constitution 1).

DRY RUN IS THE DEFAULT. A janitor that cannot be rehearsed is a shredder — and this one's first
rehearsal proposed folding a live agent into his own grandfather, which is the only reason he
still exists.

    uv run python scripts/osiris_fold_twins.py            # show me
    uv run python scripts/osiris_fold_twins.py --apply    # do it
"""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

import asyncpg
from src.orchestrator.forks import _find, resolve_parent, sid_of


async def survey(pool: asyncpg.Pool, root: Path) -> list[dict[str, str]]:
    """Every mount row that is a fork landing in a DIFFERENT LINEAGE than its ancestor.

    A FORK IS NOT A TWIN, and the first cut of this script did not know the difference — it
    proposed folding Thoth XXIX into Thoth XXVI, HIS OWN ANCESTOR, and folded phanspeed's -ii
    into its -v while folding the -v back into the -ii. It would have collapsed the fleet's
    generation chains into their own grandparents: an identity merge, performed by a machine,
    on a guess. The rehearsal is the only reason it did not happen.

    THE DISTINCTION:
      SAME lineage  (ad1a1cb0-xxvi -> ad1a1cb0-xxix)  a GENERATION. The mind died at a compact
                    and its heir was minted deliberately, by rite. The succession machinery
                    already handled it. There is nothing here to fix and everything to lose.
      OTHER lineage (d6b28b9c      -> a8c15486-xii)   a TWIN. One mind, seated twice under two
                    unrelated names, because nothing told the graph the fork was him.

    Every fork looks like the first. Only the second is the bug.
    """
    from src.orchestrator.agents import _generation

    rows = await pool.fetch(
        "SELECT job_dir, agent_id, project FROM agent_mounts ORDER BY mounted_at")
    seat_by_sid = {Path(r["job_dir"]).name: r["agent_id"] for r in rows}
    out: list[dict[str, str]] = []
    for r in rows:
        sid = Path(r["job_dir"]).name
        path = await asyncio.to_thread(_find, root, sid)
        if path is None:
            continue                                  # transcript gone: nothing to read, no claim
        seen = {sid}
        cur, real = path, None
        for _ in range(16):
            parent = await resolve_parent(pool, cur, root=root)
            if not parent or parent in seen:
                break
            seen.add(parent)
            owner = seat_by_sid.get(parent)
            if owner and _generation(owner)[0] != _generation(r["agent_id"])[0]:
                real = owner                          # a DIFFERENT lineage: this is the twin
                break
            if owner:
                break                                 # same lineage: a generation. Leave it alone.
            nxt = await asyncio.to_thread(_find, root, parent)
            if nxt is None:
                break
            cur = nxt
        if not real:
            continue
        # THE LICENCE, AND IT IS NARROW ON PURPOSE (constitution 1: identity merges are
        # review-gated, ALWAYS). A twin that never claimed a NAME is the machine's own
        # droppings — an anonymous row automount minted, that no mind ever inhabited — and
        # the janitor's whole warrant is that it may retract what the MACHINE wrote.
        #
        # A NAME IS TESTIMONY. Halcyon I forked off Ferryman IV and CLAIMED A NEW HOUSE;
        # Soundwave and Ra forked and re-claimed THEIR OWN, starting a second generation
        # counter (which is exactly where the skipped numerals came from). Every one of those
        # is a deliberate act of identity by a living mind, and folding it would be an
        # inference overruling a declaration — the named disease of this codebase, committed
        # by the very tool built to cure it. Those go to the operator. They do not go to me.
        # ORDERED, because a seat can carry more than one `handle` assertion (a rename leaves the
        # old one standing) and an unordered LIMIT 1 answers a DIFFERENT NAME on different runs —
        # it called the same agent "Ra" once and "Ptah" the next. A guess that changes its mind
        # between rehearsal and apply is not a rehearsal at all.
        handle = await pool.fetchval(
            "SELECT a.value#>>'{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
            "WHERE o.canonical=$1 AND a.name='handle' "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", r["agent_id"])
        out.append({"job_dir": r["job_dir"], "twin": r["agent_id"], "seat": real,
                    "project": r["project"] or "?", "sid": sid_of(path),
                    "handle": str(handle or "")})
    return out


async def fold(pool: asyncpg.Pool, twins: list[dict[str, str]]) -> None:
    """Re-point the anchor at the real seat, carry the twin's UNREAD MAIL across, and stop
    counting it among the living.

    THE MAIL LEG IS NOT OPTIONAL. The moment an anchor re-points, a letter addressed to the twin
    is addressed to a name nobody answers to any more — so folding a twin WITHOUT moving its
    unread mail would DESTROY the mail in the act of fixing the mailbox. (I nearly did exactly
    that: my own reply to Anubis is addressed to his twin, because his twin is what the bug
    forced him to write from.) Read mail is left where it is: it was delivered, and the record of
    who read what is history, not plumbing.

    `last_seen = NULL` is the point of the whole sweep — a twin with a fresh heartbeat is LIVE by
    every test the fleet has, which is how a DM lands in a seat nobody reads and how a co-agent
    warning cries wolf on an uncontended tree.

    NOTHING IS DELETED. The twin Agent keeps every object it authored (it really did write them);
    it simply stops being counted as a separate mind.
    """
    for t in twins:
        await pool.execute("UPDATE agent_mounts SET agent_id=$1 WHERE job_dir=$2",
                           t["seat"], t["job_dir"])
        await pool.execute(
            "UPDATE fleet_messages SET to_agent=$1 WHERE to_agent=$2 AND read_at IS NULL",
            t["seat"], t["twin"])
        await pool.execute(
            "UPDATE message_recipients SET agent_id=$1 WHERE agent_id=$2 "
            "AND NOT EXISTS (SELECT 1 FROM message_recipients m2 "
            "                WHERE m2.message_id=message_recipients.message_id "
            "                  AND m2.agent_id=$1)", t["seat"], t["twin"])
        await pool.execute(
            "UPDATE agent_mounts SET last_seen=NULL WHERE agent_id=$1", t["twin"])


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write (default: rehearse)")
    args = ap.parse_args()
    root = Path.home() / ".claude/projects"
    pool = await asyncpg.create_pool(
        os.environ["DATABASE_URL"],
        server_settings={"application_name": "osiris-script:fold-twins"})
    assert pool is not None
    found = await survey(pool, root)
    anon = [t for t in found if not t["handle"]]
    named = [t for t in found if t["handle"]]

    print(f"{len(found)} seat(s) are a FORK of a mind that already had one.\n")
    print(f"ANONYMOUS — the machine's own droppings; mine to sweep ({len(anon)}):")
    for t in sorted(anon, key=lambda x: x["project"]):
        print(f"  [{t['project']:14s}] {t['sid']}  {t['twin']:22s} -> {t['seat']}")
    if not anon:
        print("  (none)")

    print(f"\nNAMED — TESTIMONY. A mind claimed this. REVIEW-GATED, NOT MINE ({len(named)}):")
    for t in sorted(named, key=lambda x: x["project"]):
        print(f"  [{t['project']:14s}] {t['sid']}  {t['twin']:22s} ({t['handle']})"
              f"  is a fork of {t['seat']}")
    if not named:
        print("  (none)")

    if args.apply and anon:
        await fold(pool, anon)
        print(f"\nFOLDED {len(anon)} anonymous twin(s): anchors re-pointed, unread mail carried "
              f"across, no longer counted among the living. Nothing deleted.")
        print(f"LEFT {len(named)} named seat(s) UNTOUCHED — an identity merge is the operator's "
              f"call, always.")
    elif args.apply:
        print("\nNothing anonymous to fold.")
    else:
        print("\nDRY RUN — nothing written. Re-run with --apply.")
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

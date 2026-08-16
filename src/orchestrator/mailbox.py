"""The fleet mailbox — group-chat + DM over the shared memory graph (thread 6fa9791d).

The fleet co-writes MEMORY; this is the directed CHANNEL. Two shapes:
  * a BROADCAST to a project (to_agent NULL) — the group chat: every agent working that project
    sees it and settles it INDEPENDENTLY (per-recipient read state), so two co-located agents
    (handlingtheloop ux + engine, one dir) both see the conversation;
  * a DM to a specific agent (to_agent set) — only that agent sees it.

PULL, never push (Osiris has no hands): a recipient reads its inbox when it next mounts/orients.
Messages are ephemeral coordination, not durable memory, so they live in their own table, never
the event-sourced graph — for durable knowledge, agents use record_decision/open_thread.

Delivery is AT-LEAST-ONCE per recipient (decision 56f6a0d6): reading LEASES a message FOR THAT
READER (a row in message_recipients) rather than consuming it; an ACK settles it. Settling three
ways — replying (send(reply_to=id): replying proves perception), an explicit inbox(ack=[ids]), or
never (it redelivers after the lease, flagged `redelivered`). Each reader's lease/settle is its
own; the message itself is never consumed, so a broadcast survives being read by one agent.

`operator` is a reserved reader — the human's desk (no repo, never woken), just another agent_id
in message_recipients. reply_to/thread_id are the request→reply lane: a reply to a DM routes back
to the sender as a DM; a reply to a broadcast routes to the thread's project.
"""
from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

import asyncpg

# The human's desk. Not a repo (the trigger never wakes it); read via inbox(project='operator').
OPERATOR_ADDR = "operator"

# THE LIVE-HOLDER EXTENSION (grievance survey 2026-07-11, two witnesses — Anubis VIII
# msg 236, Soundwave msg 244: a message redelivered while the analysis it minted was still
# computing). A lease held by a reader whose mount is LIVE stretches to the hold-grace hour
# — the holder is demonstrably present, at-least-once needs no duplicate yet. A holder gone
# stale (died, idled out) redelivers at the plain lease exactly as before. MUST match the
# stop hook's STOP_GRACE_SECS (scripts/osiris_stophook.py): if the two windows disagree,
# the hook nags about mail the inbox refuses to show.
_HOLD_GRACE_SECS = 3600

# Deliverable TO A GIVEN READER: addressed to it (a DM to_agent=me, or a broadcast to my project
# THAT I DID NOT WRITE), not settled by me, and not under MY live lease (live = my mount
# breathes; see the live-holder extension above). `r` is my message_recipients row (LEFT JOINed).
# `m.read_at IS NULL` honors the LEGACY per-message settle: messages settled under the old
# single-reader model (pre-0021) carry fleet_messages.read_at and are globally suppressed so
# history doesn't resurface; new messages never set it (per-recipient state only).
# THE SELF-ECHO (Metron V, msgs 444/446, six blocked turns in one night): a project broadcast
# fanned out to its own author, so every send() came back as unread mail, the blocking stop
# hook fired on it, and the author paid a full read to discover its own voice. Worse than
# noise: the author's reflexive self-ack marked the broadcast SETTLED, which silenced the
# wake for the real recipient. An agent's outbox is not its mail — the author is excluded
# from its own broadcast's fan-out (the DM path always had this predicate).
# A SEAT ADDRESS IS READ BY ITS CURRENT HOLDER (Phase B2, ruling 5cef856b): mail stored
# with to_agent='seat:<id>' is deliverable to whichever mind ACTIVELY holds that Seat at
# read time — the address never dies with a mind, so mint_heir's estate-transfer UPDATE is
# simply unnecessary for it (the heir holds the seat, therefore the heir matches; nothing
# to re-address). Agent-id DMs keep the exact-id match and the estate transfer, unchanged.
_READER_HOLDS_ADDR = (
    "(m.to_agent LIKE 'seat:%' AND EXISTS (SELECT 1 FROM links hl "
    "  JOIN objects hf ON hf.id=hl.from_id JOIN objects ht ON ht.id=hl.to_id "
    "  WHERE hf.canonical = $agent AND ht.canonical = m.to_agent AND hl.type='holds' "
    "  AND (hl.valid_until IS NULL OR hl.valid_until > now())))"
)

# THE ROLLUP (operator, 2026-07-17: 'mail addressed to dead agents should roll up to the
# current live agent'): a reader drains its whole LINEAGE's lanes — an exact-id DM parked
# on any generation of the reader's own base is deliverable to whoever wears the name at
# read time. Estate transfers and sweeps still converge the lanes; the read no longer
# WAITS for them (Atlas's split: the statusline counted a DM on -ii while the freshly
# minted -iii read an empty inbox — one soul, two answers).
#
# THE SETTLE-STATE ROLLUP (threads af911f47/00378259, "A FORK MINTS A FRESH INBOX",
# Thoth's word 2026-07-29, DM 1856 — Option A): delivery already rolls up the whole
# lineage (above); settle-state used to check ONLY the exact reader's own message_
# recipients row (`r.read_at`, `r` LEFT JOINed on `r.agent_id = $agent`). mint_heir
# (agents.py's estate transfer) already copies message_recipients rows forward on every
# TRUE succession — this only closes the gap for an identity that reaches a shared
# lineage base-prefix WITHOUT going through mint_heir (a true fork, or a fresh body
# landing on an existing handle): the NOT EXISTS below asks "has ANY generation of MY
# OWN lineage already settled this", the same base-prefix pattern delivery already
# trusts to decide WHOSE mail this is — restoring symmetry (delivery already has a
# proactive copy AND a live-query fallback; settle-state had only the first). This is a
# CONSCIOUSLY ACCEPTED trust boundary, not a new one: a coincidental base-prefix match
# with no real succession behind it would inherit settled-state it never earned, exactly
# as it already inherits DELIVERY it never earned via the clause above — same risk,
# already live, now made consistent rather than introduced. Scoped to lineage-matched
# mail ONLY: the broadcast/project-wide clause (to_project=$project) is untouched, so a
# genuinely new seat in an old project still sees every broadcast nobody in ITS OWN
# lineage has settled — standing debt stays standing for a reader who never inherited
# any history to begin with. `r` (the exact-agent LEFT JOIN) still governs the
# delivered_at/lease/grace bookkeeping below UNCHANGED — only the read/settled
# determination rolls up; per-reader delivery tracking stays per-reader on purpose.
_DELIVERABLE_TO_READER = (
    "((m.to_agent = $agent) "
    " OR (m.to_agent = $lineage OR m.to_agent LIKE $lineage || '-%') "
    " OR " + _READER_HOLDS_ADDR + " "
    " OR (m.to_project = $project AND m.to_agent IS NULL AND m.from_agent <> $agent)) "
    "AND m.read_at IS NULL "
    "AND NOT EXISTS (SELECT 1 FROM message_recipients r3 WHERE r3.message_id = m.id "
    "  AND (r3.agent_id = $agent OR r3.agent_id = $lineage "
    "       OR r3.agent_id LIKE $lineage || '-%') "
    "  AND r3.read_at IS NOT NULL) "
    "AND (r.delivered_at IS NULL "
    "     OR r.delivered_at < now() - make_interval(secs => $grace) "
    "     OR (r.delivered_at < now() - make_interval(secs => $lease) "
    "         AND NOT EXISTS (SELECT 1 FROM agent_mounts lm WHERE lm.agent_id = $agent "
    "             AND lm.last_seen > now() - make_interval(secs => $lease))))"
)


async def _addressed_to_me(pool: asyncpg.Pool, to_agent: str | None, me: str) -> bool:
    """Was a DM address MINE — my exact id, or a seat I actively hold? The Python-side twin
    of the SQL predicate above, for reply routing and the reply-settles-the-referenced rule."""
    if to_agent is None:
        return False
    if to_agent == me:
        return True
    if to_agent.startswith("seat:"):
        from src.orchestrator.seats import holds
        return await holds(pool, me, to_agent)
    # the rollup's twin: an address anywhere in MY lineage is mine
    from src.orchestrator.agents import _generation
    base = _generation(me)[0]
    return to_agent == base or to_agent.startswith(base + "-")


def _norm(project: str) -> str:
    """A project address, with the optional `repo:` canonical prefix stripped."""
    return project.removeprefix("repo:").strip()


async def _dm_ineligibility(pool: asyncpg.Pool, agent_id: str) -> str | None:
    """WHY may this id not receive a DM — or None if it may. The resolver-eligibility law
    (thread 21596481): a retired or false-mint agent is never a DM target — mail parked
    on a phantom lane is a loss wearing a delivery receipt. 'unknown' = no active Agent
    object (ineligible for the RESOLVER lanes, but a direct DM to an id the graph merely
    hasn't met yet stays deliverable — registration can lag a living mind)."""
    row = await pool.fetchrow(
        "SELECT "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='retired' ORDER BY a.confidence DESC, a.observed_at DESC "
        "   LIMIT 1) AS retired, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='false_mint' ORDER BY a.confidence DESC, a.observed_at DESC "
        "   LIMIT 1) AS false_mint "
        "FROM objects o WHERE o.canonical=$1 AND o.type='Agent' AND o.status='active'",
        agent_id)
    if row is None:
        return "unknown"
    if str(row["retired"]).lower() == "true":
        return "retired"
    if str(row["false_mint"]).lower() == "true":
        return "false_mint"
    return None


async def _dm_eligible(pool: asyncpg.Pool, agent_id: str) -> bool:
    """The boolean face of _dm_ineligibility, for the resolver lanes (reply routing):
    there, an id the graph doesn't know IS ineligible — a resolver must never park mail
    on a lane it cannot vouch for."""
    return await _dm_ineligibility(pool, agent_id) is None


async def settle_history_at_join(
    pool: asyncpg.Pool, project: str | None, agent_id: str
) -> int:
    """A JOINER inherits the project's collective settle-state (the zombie-count fix,
    2026-07-09: a wake-minted osiris tab counted 5 broadcasts its sibling had already
    settled). Joining a group chat does not make the room's handled history your unread:
    any broadcast some OTHER reader already settled is stamped read for the newcomer at
    join. Broadcasts NOBODY settled stay deliverable — mail-at-birth still greets a fresh
    session and the wake pipeline still finds its cause. Live members are untouched: their
    per-reader unread keeps working message by message. Returns rows stamped."""
    if not project:
        return 0
    res = await pool.execute(
        "INSERT INTO message_recipients (message_id, agent_id, delivered_at, read_at) "
        "SELECT m.id, $2, now(), now() FROM fleet_messages m "
        "WHERE m.to_project = $1 AND m.to_agent IS NULL "
        "AND (m.read_at IS NOT NULL OR EXISTS (SELECT 1 FROM message_recipients r2 "
        "  WHERE r2.message_id = m.id AND r2.read_at IS NOT NULL)) "
        "ON CONFLICT (message_id, agent_id) DO NOTHING", _norm(project), agent_id)
    try:
        return int(res.split()[-1])
    except (ValueError, IndexError):
        return 0


async def send_message(
    pool: asyncpg.Pool, *, from_agent: str, from_project: str | None,
    to_project: str | None = None, to_agent: str | None = None, body: str,
    reply_to: int | None = None, dedup_window_secs: int = 600,
    desk_kind: str | None = None, grade: str | None = None,
    require_seat: bool = False, threads: list[str] | None = None,
) -> dict[str, Any]:
    """Post a BROADCAST (to_project) or a DM (to_agent). An EXPLICIT `to_project` must name a
    project SOMEONE HAS ACTUALLY MOUNTED UNDER (`agent_mounts.project`) or the reserved
    `OPERATOR_ADDR` — refuses otherwise, naming the string tried, with a courtesy hint if it
    happens to match a live seat/agent name ("did you mean to_agent=?"), never auto-
    substituting the parameter. Without this, any string reached `to_project` unchecked —
    including a seat handle typed into `to=` by mistake — and filed mail into a "project" no
    inbox() call would ever scope to, silently, reported as sent (shape 3 of #117; obligation
    45e52530).
    With `reply_to` and no explicit address,
    it routes by channel: a reply to a DM goes back to that sender as a DM; a reply to YOUR OWN
    outgoing DM continues that same DM with its original recipient (thread 7d670c74); a reply to
    a broadcast routes to the referenced message's project (replying to YOUR OWN broadcast routes
    ONWARD to its recipient — the desk supersession lane), joining the thread and settling the
    referenced message for the replier (replying proves perception). An identical (sender,
    recipient, body) within the dedup window returns the EXISTING id. Raises ValueError on an
    unknown reply_to or
    an unroutable message. `desk_kind` is the sender's own triage of an operator brief
    ('decision' | 'hands' | 'fyi') — which band of the desk it belongs to. `grade` is the
    SENDER'S OWN triage of what this message wants from its reader (thread f9449d8d):
    'ask' (needs a reply or an act) | 'fyi' (a notice; an ack settles it). None is honest
    ignorance — ungraded mail renders exactly as before, never guessed into a band.

    THE ECHO (ruling dd47c1da: "a build order resolved silently to an id, unverified"). A DM
    addressed by RAW agent id skips resolve_handle's live-seat resolution entirely — the exact
    gap alfred's dispatch fell into. So every DM's result now carries `seat` (the addressee's
    claimed handle, e.g. "Soundwave XI", or None for an anonymous agent) and `lineage_head`
    (where the addressed id's OWN succession chain currently ends) — a dispatcher reading the
    receipt can see whether the id it named is still who it thinks it is, without this
    function ever silently redirecting the address (an explicit id remains an act of intent,
    same as resolve_seat's grave rule). `require_seat=True` refuses to send at all when the
    resolved target carries no claimed handle — no message row is written; the ValueError
    names what was tried and what it resolved to.

    REPLY ROUTING FOLLOWS THE LINEAGE (Thoth msg 3880/3882, 2026-08-09): an IMPLICIT
    reply_to address (no explicit to_agent=/to=) resolves to the referenced message's
    sender's/recipient's current LINEAGE HEAD, not the raw id stamped on that older
    message — measured live, 90 of 1727 reply DMs across a month had already been
    superseded by the time the reply was sent. `redirected_from` names the pre-redirect id
    when this fires, never silent. This does NOT apply to explicit to_agent=<raw id>
    addressing, which keeps the grave rule above unchanged — naming a specific generation's
    id by hand remains an act of intent reply_to's implicit routing never has.

    THE ADDRESSING REFUSAL (rulings 1a64ae9a/aee67e6d, DM 2360 — John XV/XVI, resolved
    live): a DM addressed by NAME whose unique Seat exists but whose only active holder is
    marked retired/false_mint used to fall through resolve_seat's own un-seated-lineage
    fallback and land on a DIFFERENT Agent object's stale `handle` assertion — a dead
    generation, addressed with the same confidence as a real resolution (both `dm_to` and
    `lineage_head` agreed, wrongly, since both came from the same broken walk — no
    receipt-side check catches this). `seat_holder_ineligible` (seats.py) is checked BEFORE
    resolve_seat runs and refuses outright when it fires — no message row is written, same
    as `require_seat`'s refusal above.

    OWNERSHIP STAMPED AT DISPATCH, NOT REMEMBERED (Phase 1c, decision 79533336): `threads`
    names EXISTING Thread objects (uuid / `thread:<12hex>` / short-id prefix) this DM
    ASSIGNS to its addressee — a TRANSFER, re-pointing each Thread's `owner` assertion to
    the resolved `to_agent`, using the exact mechanism `reclassify_thread` already exposes
    for the human-triaged case. NO PROSE INFERENCE, EVER (cb38d922 already measured that
    channel and found it unreliable: 232 decision-prose thread mentions, 25 edged, a
    three-way ambiguity no query can separate) — only a ref named HERE, explicitly, by the
    sender, is ever touched; nothing is scraped from `body`. Each ref must resolve to
    EXACTLY ONE Thread or this call refuses outright (RefAmbiguous propagates as a
    ValueError) — named-enough-to-identify and named-enough-to-auto-stamp are different
    bars (the live specimen: `resolves="28842543"` matched 2 Thread objects by short-id
    prefix the same night this shipped). Requires a resolved single addressee (`to_agent`
    or a name that resolves to one) — ownership transfer has nowhere to land on a
    broadcast. New work with no prior thread needs nothing here: open_thread(assignee=...)
    already covers dispatch-time minting on the RECIPIENT's own end."""
    if desk_kind is not None and desk_kind not in DESK_KINDS:
        raise ValueError(f"desk_kind must be one of {DESK_KINDS}")
    if grade is not None and grade not in MAIL_GRADES:
        raise ValueError(f"grade must be one of {MAIL_GRADES}")
    requested = to_agent  # what the caller actually named, before name→id resolution overwrites it
    ref = None
    if reply_to is not None:
        ref = await pool.fetchrow(
            "SELECT id, from_agent, from_project, to_project, to_agent, thread_id "
            "FROM fleet_messages WHERE id=$1", reply_to)
        if ref is None:
            raise ValueError(f"reply_to message {reply_to} does not exist")
    if to_agent and to_agent.startswith("seat:"):
        # a DM addressed to a RAW SEAT id — an act of intent, accepted only for a living Seat
        # (never invented; a typo'd seat address must fail loudly, not park mail in a void)
        exists = await pool.fetchval(
            "SELECT 1 FROM objects WHERE canonical=$1 AND type='Seat' AND status='active'",
            to_agent)
        if not exists:
            raise ValueError(f"no such seat: '{to_agent}' — check fleet() or address by name")
    elif to_agent and not to_agent.startswith("agent:"):
        # a DM addressed by HUMAN NAME: when the name resolves through a BINDING (Phase B2,
        # 5cef856b), the SEAT is the stored address — it survives every succession, so the
        # mail reaches whoever holds the seat at READ time, not whoever held it at send time
        # (the old snapshot semantics' documented in-flight edge, now closed for bound
        # seats). An unbound name keeps the snapshot: the live holder's id, exactly as
        # before (ruling 1e02e069).
        from src.orchestrator.seats import seat_holder_ineligible
        ineligible = await seat_holder_ineligible(pool, to_agent)
        if ineligible is not None:
            # THE REFUSAL BELONGS HERE, IN THE RESOLUTION (rulings 1a64ae9a/aee67e6d, DM
            # 2360): resolve_seat's own un-seated-lineage fallback cannot tell "no seat at
            # all" apart from "a seat exists but its only holder is marked" — it treats both
            # as license to search EVERY Agent object's raw handle assertion instead, which
            # can and did find a dead generation (John XV) and address it with the same
            # confidence as a real resolution. Checking BEFORE calling resolve_seat, not
            # after, is the only place a fix here can land: both dm_to and lineage_head are
            # computed from that same fallback walk once it has run, so they agree with each
            # other while being wrong together — no post-hoc receipt check can catch this.
            raise ValueError(f"undeliverable: {ineligible} — refusing rather than guessing; "
                             "check fleet() for who actually holds the seat now, or address "
                             "the seat directly (to_agent='seat:<id>') once a new holder "
                             "claims it.")
        from src.actions.core import Actions
        from src.orchestrator.agents import resolve_seat
        resolved = await resolve_seat(Actions(pool), to_agent)
        if resolved["agent"] is None:
            raise ValueError(f"no agent named '{to_agent}' — check the name or DM by agent id")
        to_agent = resolved.get("seat_id") or resolved["agent"]
    via_reply_routing = False  # set True only where `to_a` is copied from `ref`, below —
    # never for explicit to_agent=/to=, whose staleness stays an act of intent (see the
    # REPLY ROUTING FOLLOWS THE LINEAGE comment further down)
    if to_agent or to_project:  # explicit addressing wins
        to_a = to_agent
        to_p = _norm(to_project) if to_project else None
        if to_p and to_a is None and to_p != OPERATOR_ADDR:
            # THE PHANTOM BROADCAST (obligation 45e52530, shape 3 of #117 — Thoth's own
            # three dispatches vanished 15 minutes this way): to_project reached here with
            # NO existence check at all, so ANY string could be written — including a seat
            # handle typed into `to=` by mistake — filing mail into a "project" no inbox()
            # call will ever scope to. `agent_mounts.project` is the operational truth of
            # "someone has actually been here" (the same surface fleet() reads); the Agent
            # object's own `project` property is a different question (a claim about
            # identity, not operational history) and can drift from it — using that instead
            # would let mail land where no inbox() scopes to, today's bug with a longer fuse.
            known = await pool.fetchval(
                "SELECT 1 FROM agent_mounts WHERE project = $1 LIMIT 1", to_p)
            if not known:
                assert to_project is not None  # to_p is truthy only when to_project was
                hint = ""
                from src.actions.core import Actions
                from src.orchestrator.agents import resolve_seat
                from src.orchestrator.seats import seat_holder_ineligible
                # Don't suggest a hint that's guaranteed to hit the SAME refusal: if
                # to_project's unique seat has only ineligible holders, addressing it as
                # to_agent=<name> below would refuse via this exact guard (task #142 punch-
                # list item 3) — no point pointing the caller at a door that won't open.
                if await seat_holder_ineligible(pool, to_project) is None:
                    maybe = await resolve_seat(Actions(pool), to_project)
                    if maybe["agent"] is not None:
                        hint = f" — did you mean to_agent={to_project!r} instead of to=?"
                raise ValueError(
                    f"no such project: {to_project!r} — nobody has ever mounted there, so "
                    f"no inbox() call would ever see this broadcast{hint}")
    elif ref is not None and await _addressed_to_me(pool, ref["to_agent"], from_agent):
        to_a, to_p = ref["from_agent"], ref["from_project"]  # a DM to me → DM back to its sender
        via_reply_routing = True
    elif (ref is not None and ref["to_agent"] is not None
          and await _addressed_to_me(pool, ref["from_agent"], from_agent)):
        # REPLYING TO MY OWN DM CONTINUES THAT SAME DM (thread 7d670c74, 2026-08-03): `ref`
        # here is a message I SENT, not one sent to me — the case below (the "own message"
        # branch) was built for replying to my own BROADCAST, the desk supersession lane,
        # where to_project names where it should route onward. A DM's to_project is normally
        # NULL, so falling into that branch either raised a misleading "no recipient" error
        # or — if the original send had also carried an explicit to_project alongside
        # to_agent — silently rebroadcast the reply to that project instead of continuing the
        # conversation with the person actually being talked to (live specimen: a DM to Thoth,
        # replied-to via its own id, landed as a stalled project broadcast nobody was
        # watching). Continuing the SAME DM is the only sensible reading of "replying to my
        # own outgoing message" when that message was itself a DM.
        to_a, to_p = ref["to_agent"], ref["to_project"]
        via_reply_routing = True
    elif ref is not None:  # a broadcast/own message → project routing (supersession lane)
        own = from_project and _norm(ref["from_project"] or "") == _norm(from_project)
        if own:
            to_a, to_p = None, _norm(ref["to_project"] or "")
        else:
            # THE CROSS-PROJECT RETURN GOES TO THE SEAT (Werner's leak, 2026-07-16): a reply
            # to a FOREIGN project's broadcast used to return to that project's whole ROOM —
            # gestalt's audit reports, addressed to 'whoever commissioned this', broadcast
            # into every bytebye reader's inbox because the commissioner's house was the only
            # return address the pre-seat mailbox had. The seat IS the address now (B2,
            # 5cef856b): a seat-bound asker gets the reply as a seat DM — durable across
            # succession, invisible to housemates. An unbound asker keeps the room return
            # (a DM to a transient dead id would strand the mail; the room at least reaches
            # the house — exactly the old law, unchanged).
            from src.orchestrator.seats import held_seat
            bound = await held_seat(pool, ref["from_agent"])
            if bound:
                to_a, to_p = bound["seat_id"], None
            else:
                # MAIL FOLLOWS THE MIND, NOT THE ROOM (operator ruling 2026-07-19, thread
                # 07d64473: Atlas IV asked from the xxit room he was visiting; the reply
                # landed on Metron, the room's resident — 'metron got the mail that was
                # for you, thats a bug'). An unbound asker the GRAPH KNOWS gets the reply
                # as a DM to their living head — since the lineage rollup (one soul, one
                # inbox) a head DM is deliverable at read time across successions, so the
                # old stranding fear only holds for ids the graph never registered. Those
                # keep the room return (it at least reaches the house), as does a head the
                # eligibility law refuses (retired/false_mint — never a DM target).
                from src.orchestrator.folds import living_head
                head: str | None = None
                if str(ref["from_agent"] or "").startswith("agent:"):
                    cand = await living_head(pool, ref["from_agent"])
                    if cand and await _dm_eligible(pool, cand):
                        head = cand
                if head:
                    to_a, to_p = head, None
                else:
                    to_a, to_p = None, _norm(ref["from_project"] or "")
    else:
        to_a = to_p = None
    if not to_a and not to_p:
        raise ValueError("no recipient: pass to=<project>, to_agent=<agent>, or reply_to a "
                         "message whose sender is addressable")
    # A FOLDED LABEL IS A FORWARDING ORDER, NOT A GRAVE (the reconciliation primitive,
    # thread b975851b): a DM to an agent id whose object is status='merged' can never be
    # read under that label — parking mail there re-creates the orphan machine the fold
    # exists to end. This is NOT the silent redirection the grave rule forbids: the fold
    # was review-gated (the operator signed it), and the receipt confesses the forward.
    folded_from: str | None = None
    if to_a and to_a.startswith("agent:"):
        from src.orchestrator.folds import canonical_agent, living_head
        canon = await canonical_agent(pool, to_a)
        if canon != to_a:
            folded_from, to_a = to_a, await living_head(pool, canon)
    # REPLY ROUTING FOLLOWS THE LINEAGE, NOT THE STAMP (Thoth msg 3880/3882, 2026-08-09):
    # the two `via_reply_routing` branches above used to copy ref["from_agent"]/
    # ref["to_agent"] VERBATIM — the raw id stamped on the REFERENCED message at ITS send
    # time. That id can be superseded (mint_heir / a compaction succession) between the
    # original message and this reply — measured live against fleet_messages: 90 of 1727
    # reply DMs (5.2%, msg 188 through msg 3884, spanning the whole month, not a papercut)
    # landed on a `to_agent` already superseded before the reply was even sent.
    # `lineage_head` is computed a few lines below for every agent-id `to_a` regardless —
    # it was ALWAYS being fetched, for the eligibility gate and the receipt echo, just
    # never fed back into what actually got WRITTEN as the delivery target. Scoped to
    # `via_reply_routing` ONLY: explicit to_agent=<raw id> addressing keeps its existing,
    # deliberately-unredirected law (test_a_raw_id_send_reveals_a_stale_generation_via_
    # lineage_head, tests/test_mailbox.py) — a human who names a specific ancestor id may
    # mean THAT generation, an act of intent this function must never second-guess.
    # reply_to has no such excuse: nobody CHOOSES who a reply goes back to, so "back to
    # whoever I'm actually talking to now" is the only sensible reading — the same reading
    # `_addressed_to_me`'s own lineage rollup already gives the PERCEPTION side of this
    # exact question. The receipt NAMES the redirect (`redirected_from`) rather than
    # silently rerouting, same discipline `folded_from` above already keeps for a merge.
    # A dead/retired/false_mint head is NOT a new case to handle here — the eligibility
    # gate just below already runs on `lineage_head(to_a)` regardless of how `to_a` was
    # set, so it already refused delivery to a since-retired head before this change (the
    # stale `to_agent` this fix corrects was never what protected against that grave; it
    # only ever protected the receipt's `lineage_head` echo, which was already computed
    # correctly). This redirect and that gate are independent fixes for independent halves
    # of the same underlying gap.
    redirected_from: str | None = None
    if via_reply_routing and to_a and to_a.startswith("agent:"):
        from src.orchestrator.agents import lineage_head
        head = await lineage_head(pool, to_a)
        if head != to_a:
            redirected_from, to_a = to_a, head
    # THE ECHO + THE GATE (dd47c1da) — resolved BEFORE any write, so a require_seat refusal
    # never leaves a row behind. `to_a` may be a name already resolved above, OR a raw agent id
    # that skipped resolution entirely (alfred's gap): either way, lineage_head reveals whether
    # this id is still its lineage's newest generation, and agent_seat reveals whether it is a
    # claimed seat at all — a dispatcher gets the truth, this function never guesses for it.
    seat: str | None = None
    lineage: str | None = None
    holder: str | None = None
    if to_a and to_a.startswith("seat:"):
        # a SEAT address: the receipt names the seat's handle and its CURRENT holder (whose
        # lineage head is the live truth a dispatcher wants); a vacant seat is not a grave —
        # the mail waits for the next holder, and the receipt says holder=None honestly.
        from src.orchestrator.agents import lineage_head
        from src.orchestrator.seats import seat_receipt
        sr = await seat_receipt(pool, to_a)
        seat = (sr or {}).get("handle")
        holder = (sr or {}).get("holder")
        lineage = await lineage_head(pool, holder) if holder else None
        if require_seat and sr is None:
            raise ValueError(f"require_seat: '{requested or to_a}' is not a living Seat — "
                             "refusing to dispatch blind.")
    elif to_a:
        from src.orchestrator.agents import agent_seat, lineage_head
        lineage = await lineage_head(pool, to_a)
        # THE ELIGIBILITY LAW ON THE DIRECT LANE (thread 21596481; the msg-192 fixture —
        # a DM routed to a retired phantom lane): when the lineage's newest generation is
        # KNOWN-dead (retired / false_mint), the mail can never be read under any of its
        # addresses — fail LOUDLY, naming who was found and why, instead of parking it.
        # An id the graph merely hasn't met stays deliverable (registration can lag).
        if lineage:
            why = await _dm_ineligibility(pool, lineage)
            if why in ("retired", "false_mint"):
                raise ValueError(
                    f"undeliverable: '{requested or to_a}' resolves to {lineage}, whose "
                    f"lineage head is {why} — no eligible living head exists, so this DM "
                    "would park on a phantom lane forever. Check fleet() for who actually "
                    "lives, or broadcast to the project room instead.")
        seat = await agent_seat(pool, to_a)
        if require_seat and seat is None:
            raise ValueError(
                f"require_seat: '{requested or to_a}' resolved to {to_a}, which holds no "
                "CLAIMED seat (no handle asserted) — refusing to dispatch blind. Check "
                "fleet() for who actually holds a seat, or pass require_seat=False to send "
                "anyway.")
    # OWNERSHIP STAMPED AT DISPATCH (Phase 1c): resolved BEFORE any write, same law as the
    # require_seat gate just above — an ambiguous or unknown thread ref must refuse loudly,
    # never park a message whose ownership claim silently didn't land.
    resolved_threads: list[Any] = []
    if threads:
        if not to_a:
            raise ValueError("threads=…: ownership transfer needs a single resolved "
                             "addressee (to_agent=…) — a broadcast has no one to hand it to")
        from src.orchestrator.capture import RefAmbiguous, _find_thread
        for tref in threads:
            try:
                tid = await _find_thread(pool, tref, require_identifier=True)
            except RefAmbiguous as exc:
                raise ValueError(
                    f"threads=…: {exc} — refusing to guess which Thread {tref!r} means "
                    "(named-enough-to-identify is not named-enough-to-auto-stamp)") from exc
            if tid is None:
                raise ValueError(f"threads=…: no Thread matches {tref!r} — check the short "
                                 "id, or open it first")
            resolved_threads.append(tid)
    thread = (ref["thread_id"] or ref["id"]) if ref is not None else None
    # the reply IS the ack — settle the referenced message for the replier, if it was addressed
    # to them (a DM to me, or a broadcast to my project)
    if ref is not None and (
        await _addressed_to_me(pool, ref["to_agent"], from_agent)
        or (ref["to_agent"] is None and from_project
            and _norm(ref["to_project"] or "") == _norm(from_project))
    ):
        await pool.execute(
            "INSERT INTO message_recipients (message_id, agent_id, read_at) VALUES ($1,$2,now()) "
            "ON CONFLICT (message_id, agent_id) DO UPDATE SET read_at=COALESCE("
            "message_recipients.read_at, now())", ref["id"], from_agent)
    async def _stamp_threads() -> list[str]:
        """The transfer itself — re-point each resolved Thread's `owner` to the resolved
        addressee, same mechanism `reclassify_thread` uses (assert_property, self-declared,
        the sender's own act). Idempotent by construction (within-source supersession), so
        a dedup'd retry re-stamping the same value is harmless, never a second fact."""
        if not resolved_threads:
            return []
        from src.actions.core import Actions
        from src.parsers.base import EvidenceClass
        from src.parsers.evidence import confidence_for

        a = Actions(pool)
        observed = datetime.now(UTC)
        conf = confidence_for(EvidenceClass.SELF_DECLARED)
        for tid in resolved_threads:
            await a.assert_property(tid, "owner", to_a, from_agent, observed, conf,
                                    evidence_class=EvidenceClass.SELF_DECLARED.value)
        return [str(t) for t in resolved_threads]

    dup = await pool.fetchrow(
        "SELECT id, thread_id FROM fleet_messages WHERE from_agent=$1 "
        "AND to_project IS NOT DISTINCT FROM $2 AND to_agent IS NOT DISTINCT FROM $3 "
        "AND md5(body)=md5($4) AND created_at > now() - make_interval(secs => $5) "
        "ORDER BY id DESC LIMIT 1", from_agent, to_p, to_a, body, dedup_window_secs)
    if dup is not None:
        stamped = await _stamp_threads()
        return {"id": dup["id"], "to": to_p, "to_agent": to_a,
                "thread_id": dup["thread_id"], "dedup": True,
                **({"seat": seat, "lineage_head": lineage} if to_a else {}),
                **({"holder": holder} if to_a and to_a.startswith("seat:") else {}),
                **({"folded_from": folded_from} if folded_from else {}),
                **({"redirected_from": redirected_from} if redirected_from else {}),
                **({"threads_stamped": stamped} if stamped else {})}
    mid = await pool.fetchval(
        "INSERT INTO fleet_messages (from_agent, from_project, to_project, to_agent, body, "
        "reply_to, thread_id, desk_kind, grade) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) "
        "RETURNING id",
        from_agent, from_project, to_p, to_a, body, reply_to, thread, desk_kind, grade)
    stamped = await _stamp_threads()
    return {"id": mid, "to": to_p, "to_agent": to_a, "thread_id": thread, "dedup": False,
            **({"seat": seat, "lineage_head": lineage} if to_a else {}),
            **({"holder": holder} if to_a and to_a.startswith("seat:") else {}),
            **({"folded_from": folded_from} if folded_from else {}),
            **({"redirected_from": redirected_from} if redirected_from else {}),
            **({"threads_stamped": stamped} if stamped else {})}


async def unread_count(
    pool: asyncpg.Pool, reader_project: str, *, reader_agent: str, lease_secs: int = 900,
    grade: str | None = None,
) -> int:
    """How many DELIVERABLE messages await this reader — broadcasts to its project + DMs to it,
    unsettled and not under its own live lease. The number mount()/orient() surface.
    `grade` narrows to one band ('ask' | 'fyi') — the count that leads with what is ACTIONABLE
    (thread f9449d8d: a mailbox that cries wolf gets skimmed, and a skimmed mailbox is lost)."""
    from src.orchestrator.agents import _generation

    q = ("SELECT count(*) FROM fleet_messages m "
         "LEFT JOIN message_recipients r ON r.message_id=m.id AND r.agent_id=$agent "
         "WHERE " + _DELIVERABLE_TO_READER + (" AND m.grade = $6" if grade else ""))
    q = (q.replace("$agent", "$1").replace("$project", "$2").replace("$lease", "$3")
         .replace("$grace", "$4").replace("$lineage", "$5"))
    args = [reader_agent, _norm(reader_project), lease_secs, _HOLD_GRACE_SECS,
            _generation(reader_agent)[0]]
    if grade:
        args.append(grade)
    return await pool.fetchval(q, *args)  # type: ignore[no-any-return]


async def unread_split(
    pool: asyncpg.Pool, reader_project: str | None, *, reader_agent: str | None,
    lease_secs: int = 900,
) -> dict[str, int]:
    """{mail, dm} — unread_count's number SPLIT by lane (broadcasts to the project vs DMs
    to the reader), same `_DELIVERABLE_TO_READER` predicate, so the statusline's two
    segments always sum to what mount()/orient() report (operator ruling 2026-07-19: the
    statusline carried its own stale COPY of this predicate — no lineage rollup, no hold
    grace — and its mail number quietly diverged from orient's).

    An IDENTITY-LESS reader (no mount row yet) has no receipts, so per-reader semantics
    would re-count the project's whole settled history; it falls back to PROJECT-OPEN
    semantics — broadcasts NOBODY settled, dm 0 — exactly the statusline's old honest
    fallback, now housed with the authority."""
    from src.orchestrator.agents import _generation

    if not reader_agent:
        if not reader_project:
            return {"mail": 0, "dm": 0}
        n = await pool.fetchval(
            "SELECT count(*) FROM fleet_messages m "
            "WHERE m.to_project=$1 AND m.to_agent IS NULL AND m.read_at IS NULL "
            "AND NOT EXISTS (SELECT 1 FROM message_recipients r2 WHERE r2.message_id=m.id "
            "  AND r2.read_at IS NOT NULL)", _norm(reader_project))
        return {"mail": int(n or 0), "dm": 0}
    q = ("SELECT count(*) FILTER (WHERE m.to_agent IS NULL) AS mail, "
         "       count(*) FILTER (WHERE m.to_agent IS NOT NULL) AS dm "
         "FROM fleet_messages m "
         "LEFT JOIN message_recipients r ON r.message_id=m.id AND r.agent_id=$agent "
         "WHERE " + _DELIVERABLE_TO_READER)
    q = (q.replace("$agent", "$1").replace("$project", "$2").replace("$lease", "$3")
         .replace("$grace", "$4").replace("$lineage", "$5"))
    row = await pool.fetchrow(
        q, reader_agent, _norm(reader_project or ""), lease_secs, _HOLD_GRACE_SECS,
        _generation(reader_agent)[0])
    return {"mail": int(row["mail"] or 0), "dm": int(row["dm"] or 0)}


# A BRIEF THE DESK STILL SHOWS — the one predicate behind every briefs number (operator
# ruling 2026-07-19, the chrome/harness disagreement): an unread, un-dismissed operator
# brief that is NOT moot-dimmed and IS its thread's lead (the desk thread-folds — an
# earlier brief superseded by a newer one in its thread rides under it and is not a
# second debt; the count must fold exactly as the page folds). Placeholders $op / the
# alias m are fixed; callers add their own scoping clauses.
_DESK_BRIEF_ROW = (
    "m.to_project=$op AND m.to_agent IS NULL AND m.read_at IS NULL "
    "AND m.moot_note IS NULL "
    "AND NOT EXISTS (SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
    "  AND r.agent_id=$op AND r.read_at IS NOT NULL) "
    "AND NOT EXISTS (SELECT 1 FROM fleet_messages nl WHERE "
    "  COALESCE(nl.thread_id, nl.id) = COALESCE(m.thread_id, m.id) AND nl.id > m.id "
    "  AND nl.to_project=$op AND nl.to_agent IS NULL AND nl.read_at IS NULL "
    "  AND NOT EXISTS (SELECT 1 FROM message_recipients r2 WHERE r2.message_id=nl.id "
    "    AND r2.agent_id=$op AND r2.read_at IS NOT NULL))"
)


async def desk_briefs_total(pool: asyncpg.Pool) -> int:
    """The whole desk's undismissed briefs — the fleet pulse's number, counted with the
    SAME fold the desk page renders (lead-per-thread, moot-dimmed excluded)."""
    q = ("SELECT count(*) FROM fleet_messages m WHERE "
         + _DESK_BRIEF_ROW).replace("$op", "$1")
    return await pool.fetchval(q, OPERATOR_ADDR)  # type: ignore[no-any-return]


async def desk_briefs_from(pool: asyncpg.Pool, agent_id: str | None) -> int:
    """The DESK, scoped to ONE seat (operator ruling, 2026-07-16: 'desk should not be
    globally scoped... only scope what is for or from the agent'): unread operator-desk
    briefs sent by THIS agent's lineage — the agent's own words still awaiting the human's
    eye, never the fleet-wide backlog (a number identical in every chrome informs nobody).
    An identity-less or non-lineage caller scores 0 — nothing of theirs can be waiting.
    Same fold as the page and the pulse (_DESK_BRIEF_ROW), plus the lineage scope."""
    mt = re.match(r"^agent:[0-9a-f]{8}", agent_id or "")
    if not mt:
        return 0
    base = mt.group(0)
    q = ("SELECT count(*) FROM fleet_messages m WHERE " + _DESK_BRIEF_ROW
         + " AND (m.from_agent = $2 OR m.from_agent LIKE $2 || '-%')").replace("$op", "$1")
    return await pool.fetchval(q, OPERATOR_ADDR, base)  # type: ignore[no-any-return]


async def read_inbox(
    pool: asyncpg.Pool, reader_project: str, *, reader_agent: str, mark_read: bool = True,
    limit: int = 50, lease_secs: int = 900,
) -> list[dict[str, Any]]:
    """This reader's deliverable messages, oldest first (broadcasts to its project + DMs to it).
    Reading LEASES them FOR THIS READER (a message_recipients row) — settle each by replying or
    acking; an unsettled message redelivers after the lease, flagged `redelivered`. mark_read=
    False is a pure peek. A broadcast read by one agent stays visible to the others — each has
    its own lease/settle."""
    proj = _norm(reader_project)
    from src.orchestrator.agents import _generation

    q = ("SELECT m.id, m.from_agent, m.from_project, m.to_agent, m.body, m.created_at, "
         "m.reply_to, m.thread_id, m.grade, m.prior_art, COALESCE(r.deliveries,0) "
         "AS deliveries "
         "FROM fleet_messages m "
         "LEFT JOIN message_recipients r ON r.message_id=m.id AND r.agent_id=$agent "
         "WHERE " + _DELIVERABLE_TO_READER + " ORDER BY m.created_at LIMIT $limit")
    q = (q.replace("$agent", "$1").replace("$project", "$2")
         .replace("$lease", "$3").replace("$grace", "$4").replace("$limit", "$5")
         .replace("$lineage", "$6"))
    rows = await pool.fetch(q, reader_agent, proj, lease_secs, _HOLD_GRACE_SECS, limit,
                            _generation(reader_agent)[0])
    if mark_read and rows:
        await pool.executemany(
            "INSERT INTO message_recipients (message_id, agent_id, delivered_at, deliveries) "
            "VALUES ($1,$2,now(),1) ON CONFLICT (message_id, agent_id) DO UPDATE SET "
            "delivered_at=now(), deliveries=message_recipients.deliveries+1",
            [(r["id"], reader_agent) for r in rows])
    return [
        {"id": r["id"], "from": r["from_agent"], "from_project": r["from_project"],
         "body": r["body"], "when": r["created_at"].isoformat(),
         "thread": r["thread_id"] or r["id"],
         **({"dm": True} if r["to_agent"] is not None else {}),
         **({"grade": r["grade"]} if r["grade"] else {}),
         **({"reply_to": r["reply_to"]} if r["reply_to"] is not None else {}),
         **({"redelivered": True} if r["deliveries"] > 0 else {}),
         # THE READ-SIDE PRIOR-ART HOP (obligation a6198075): computed ONCE at send()
         # time (see mcp_server.py's send()), never re-run here — a reader sees the same
         # hits the sender already saw, no second search on the read path.
         **({"prior_art": r["prior_art"]} if r["prior_art"] else {})}
        for r in rows
    ]


async def in_flight(
    pool: asyncpg.Pool, reader_project: str, *, reader_agent: str, lease_secs: int = 900
) -> list[dict[str, Any]]:
    """The group's in-flight view: shared BROADCAST mail to this reader's project that ANOTHER
    agent currently holds under a live lease (unsettled). 'mail 0' must never silently mean 'a
    sibling is answering the group thread right now' (msg-78 lesson)."""
    rows = await pool.fetch(
        "SELECT m.id, m.from_project, r.agent_id AS leased_by, m.thread_id, "
        " extract(epoch FROM (now() - r.delivered_at)) AS held_secs "
        "FROM fleet_messages m JOIN message_recipients r ON r.message_id=m.id "
        "WHERE m.to_project=$1 AND m.to_agent IS NULL AND r.agent_id <> $2 "
        "AND r.read_at IS NULL AND r.delivered_at IS NOT NULL "
        "AND r.delivered_at >= now() - make_interval(secs => $3) "
        "ORDER BY r.delivered_at", _norm(reader_project), reader_agent, lease_secs)
    return [
        {"id": r["id"], "from_project": r["from_project"], "leased_by": r["leased_by"],
         "thread": r["thread_id"] or r["id"], "held_for_secs": int(r["held_secs"] or 0)}
        for r in rows
    ]


async def ack_messages(
    pool: asyncpg.Pool, reader_project: str, ids: list[int], *, reader_agent: str
) -> dict[str, Any]:
    """Settle messages FOR THIS READER — a DM to it OR TO ANY GENERATION OF ITS LINEAGE
    (the rollup: what a reader may READ it may SETTLE — Alfred's fixture, msg 666: four
    DMs addressed to his -iii were readable by -iv but the ack's exact-id match no-oped
    silently, so twice-acked mail redelivered forever), a seat it actively holds, or a
    broadcast to its project. Returns {settled: [ids], skipped: {id: why}} — an ack that
    did nothing must SAY SO AND SAY WHY; an empty response indistinguishable from success
    is how the same mail gets acked three times. Idempotent: re-acking the settled lands
    in skipped as 'already settled by you'. Another reader acking the same broadcast
    settles only ITS own copy."""
    from src.orchestrator.agents import _generation

    base = _generation(reader_agent)[0]
    rows = await pool.fetch(
        "INSERT INTO message_recipients (message_id, agent_id, read_at) "
        "SELECT m.id, $3, now() FROM fleet_messages m "
        "WHERE m.id = ANY($1::bigint[]) "
        "  AND ((m.to_agent = $3) "
        # THE ROLLUP (kept in step with _DELIVERABLE_TO_READER and _addressed_to_me — the
        # read and reply-settle sides learned it long before this ack side did)
        "   OR (m.to_agent = $4 OR m.to_agent LIKE $4 || '-%') "
        # ...or a SEAT the acking mind actively holds (Phase B2): settling seat mail is the
        # holder's right exactly as reading it is
        "   OR (m.to_agent LIKE 'seat:%' AND EXISTS (SELECT 1 FROM links hl "
        "     JOIN objects hf ON hf.id=hl.from_id JOIN objects ht ON ht.id=hl.to_id "
        "     WHERE hf.canonical = $3 AND ht.canonical = m.to_agent AND hl.type='holds' "
        "     AND (hl.valid_until IS NULL OR hl.valid_until > now()))) "
        "   OR (m.to_project = $2 AND m.to_agent IS NULL)) "
        "ON CONFLICT (message_id, agent_id) DO UPDATE SET read_at=COALESCE("
        "message_recipients.read_at, now()) "
        "WHERE message_recipients.read_at IS NULL RETURNING message_id",
        ids, _norm(reader_project), reader_agent, base)
    settled = sorted(int(r["message_id"]) for r in rows)
    skipped: dict[int, str] = {}
    missing = [i for i in ids if i not in set(settled)]
    if missing:
        info = await pool.fetch(
            "SELECT m.id, m.to_agent, m.to_project, (r.read_at IS NOT NULL) AS already "
            "FROM fleet_messages m LEFT JOIN message_recipients r "
            "  ON r.message_id=m.id AND r.agent_id=$2 "
            "WHERE m.id = ANY($1::bigint[])", missing, reader_agent)
        known = {int(r["id"]): r for r in info}
        for i in missing:
            r = known.get(i)
            if r is None:
                skipped[i] = "unknown id"
            elif r["already"]:
                skipped[i] = "already settled by you"
            else:
                skipped[i] = (f"not addressed to you (to_agent={r['to_agent']}, "
                              f"to_project={r['to_project']})")
    return {"settled": settled, "skipped": skipped}


async def project_deliverable_count(
    pool: asyncpg.Pool, project: str, *, lease_secs: int = 900
) -> int:
    """For the wake dispatch: messages to this project (broadcasts + DMs to its agents) that NO
    intended recipient has settled yet. Once anyone reads a broadcast the wake stops, but the
    other agents still see it in their own inboxes — the wake ensures SOMEONE looks, without
    re-firing per sibling. (Agent-precise waking is a later phase; this is the safe project
    signal.)"""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT count(*) FROM fleet_messages m WHERE "
        "(m.to_project=$1 AND m.to_agent IS NULL) AND m.read_at IS NULL "
        "AND NOT EXISTS (SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
        "  AND r.read_at IS NOT NULL) "
        "AND (NOT EXISTS (SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
        "  AND r.delivered_at >= now() - make_interval(secs => $2))) "
        # the live-holder extension: never WAKE a sibling for a message a live mind is
        # already holding (grievance msg 244: duplicate reads while the grid computed)
        "AND NOT EXISTS (SELECT 1 FROM message_recipients r "
        "  JOIN agent_mounts lm ON lm.agent_id = r.agent_id "
        "  WHERE r.message_id = m.id "
        "  AND r.delivered_at >= now() - make_interval(secs => $3) "
        "  AND lm.last_seen > now() - make_interval(secs => $2))",
        _norm(project), lease_secs, _HOLD_GRACE_SECS)


# ── THE DESK (operator direction, 2026-07-11: "my desk is full — fix it") ────────────────
# The desk drowned in SHAPE, not volume: one fleet-wide condition reported five times, moot
# alarms lingering because only the human may dismiss, CRITICAL interleaved with FYI, and
# blockers re-stated in prose that the thread wall already carries. Four organs answer it:
# BANDS (sender-declared desk_kind, heuristic fallback) · THREAD+SAME-STORY FOLDS (newest
# per thread; pg_trgm clusters near-duplicates within a band) · DIM (an agent may annotate
# a brief moot with a reason — never settle; the membrane holds) · YOUR-QUEUE (derived live
# from owner='operator' threads — the graph is the record, the desk stops double-billing).

DESK_KINDS = ("decision", "hands", "fyi")

# The PROJECT-mail analogue of the desk bands (thread f9449d8d): the sender's own triage of
# what the message wants from its reader. 'ask' needs a reply or an act; 'fyi' is a notice an
# ack settles. NULL stays honest ignorance — never heuristically guessed, unlike desk briefs,
# because a wrong "needs nothing" on a duty-bearing letter silences it.
MAIL_GRADES = ("ask", "fyi")

# same-story threshold: pg_trgm similarity MEASURED on the live desk (2026-07-11): the
# model-divergence five pair at 0.32–0.44; the worst FALSE pair (two long technical briefs
# sharing engine vocabulary, 300 vs 237) at 0.294. 0.30 splits them — thin, so the sender
# guard below carries half the load: same-story means one condition SEVERAL witnesses
# reported, so two briefs from ONE sender never same-story fold (their lane is the
# thread-fold via reply_to, which the wake prompt teaches).
_SAME_STORY_SIM = 0.30


def classify_brief(body: str, desk_kind: str | None) -> str:
    """The band a brief belongs to. The SENDER's declaration wins; unclassified (legacy)
    briefs are banded by heuristic — biased UPWARD (a CRITICAL misfiled under fyi costs
    more than an FYI misfiled under decision)."""
    if desk_kind in DESK_KINDS:
        return desk_kind
    low = (body or "").lower()
    if any(t in low for t in ("🚨", "critical", "needs your decision", "judgment call",
                              "decide", "ruling needed", "your call")):
        return "decision"
    if any(t in low for t in ("needs your hands", "blocked on you", "physically",
                              "authorization", "authorize", "refill", "your word")):
        return "hands"
    return "fyi"


async def dim_brief(
    pool: asyncpg.Pool, message_id: int, *, because: str, by: str,
) -> dict[str, Any]:
    """DIM an operator-desk brief: annotate it moot-with-a-reason so the desk renders it
    collapsed under the note. NEVER a settle — dismissing stays exclusively the human's
    (the membrane); a dim is an agent saving the human the archaeology ("true when sent,
    moot now: root cause fixed in <commit>"). Stamped who + when — an annotation is
    testimony. Re-dimming overwrites (last honest word wins; the row keeps one note)."""
    row = await pool.fetchrow(
        "UPDATE fleet_messages SET moot_note=$2, moot_by=$3, moot_at=now() "
        "WHERE id=$1 AND to_project=$4 RETURNING id", message_id, because, by, OPERATOR_ADDR)
    if row is None:
        raise ValueError(f"message {message_id} is not an operator-desk brief — dim only "
                         "applies to the human's desk")
    return {"dimmed": message_id, "because": because, "by": by,
            "note": "annotated, NOT settled — the brief stays the operator's to dismiss"}


async def _same_story_clusters(
    pool: asyncpg.Pool, cards: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Greedy trigram clustering WITHIN one band: five agents reporting one fleet-wide
    condition become one card carrying the newest telling + who else said it. One SQL
    round-trip for all pairs; tiny N (a desk, not a corpus)."""
    if len(cards) < 2:
        return cards
    ids = [c["id"] for c in cards]
    bodies = [c["body"] for c in cards]
    sender = {c["id"]: c["from"] for c in cards}
    pairs = [p for p in await pool.fetch(
        "WITH b AS (SELECT unnest($1::bigint[]) AS id, unnest($2::text[]) AS body) "
        "SELECT a.id AS ida, c.id AS idb FROM b a JOIN b c ON a.id < c.id "
        "WHERE similarity(a.body, c.body) > $3", ids, bodies, _SAME_STORY_SIM)
        # several WITNESSES, one condition: a single sender's repeats are thread-fold's lane
        if sender[p["ida"]] != sender[p["idb"]]]
    parent: dict[int, int] = {i: i for i in ids}

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for p in pairs:
        ra, rb = find(p["ida"]), find(p["idb"])
        if ra != rb:
            parent[rb] = ra
    groups: dict[int, list[dict[str, Any]]] = {}
    for c in cards:
        groups.setdefault(find(c["id"]), []).append(c)
    out = []
    for members in groups.values():
        members.sort(key=lambda c: c["when"], reverse=True)  # newest telling leads
        lead = members[0]
        if len(members) > 1:
            lead = {**lead, "same_story": {
                "count": len(members),
                "also": [{"id": m["id"], "from": m["from"], "project": m["from_project"]}
                         for m in members[1:]],
                "note": "near-identical briefs folded — one condition, several witnesses; "
                        "the ids above settle with this one"}}
        out.append(lead)
    out.sort(key=lambda c: c["when"], reverse=True)
    return out


async def _operator_queue(pool: asyncpg.Pool, limit: int = 100) -> list[dict[str, Any]]:
    """The standing YOUR-QUEUE: open threads whose owner is the human (owner='operator' —
    the tags shipped 90b2832). Derived LIVE from the graph, so briefs can point at a thread
    instead of re-stating the blocker in prose that instantly goes stale.

    Carries the PROJECT (the in_repo edge) so the desk can group by it — the operator works
    his debts a project at a time, not a band at a time ("no good way to resolve these debts
    per thread or per project, so it snowballs into infinity", 2026-07-11).

    DEFERRED debts are hidden until their date (capture.defer_thread). Fix at the lens: the
    thread stays open and owned in the record; only its visibility moves.

    ASKED vs GUESSED — each row carries the GRADE of the mind that placed it (2026-07-12, the
    operator: "according to the desk, this session owes 6, accurate or bug?" — bug, all six).
    FIVE of those six were DERIVED: the miner read a conversation, inferred "the human must do
    X", and minted a duty nobody ever asked him for — "check the mid-flight 121G rsync" (none
    had run in a day), "process 17 non-auto-mergeable decisions" (the queue held one, already
    resolved). Evidence-graded ingest puts DERIVED at the BOTTOM, and yet a DERIVED guess was
    landing on the human's queue with the same weight, the same red number and the same
    permanence as a deliberate ask. LEXICAL SIMILARITY MAY ASK BUT NEVER ASSERT, one level up:
    THE MINER MAY NOTICE, BUT MUST NEVER OBLIGE. So the grade rides along and the lens splits
    on it — the record keeps every guess, the RED NUMBER counts only what a mind actually asked.
    """
    rows = await pool.fetch(
        "SELECT o.id, o.created_at, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS s, "
        " (SELECT a.evidence_class FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS ec, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='kind' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS k, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='deferred_until' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS d, "
        " (SELECT replace(p.canonical,'repo:','') FROM links l JOIN objects p ON p.id=l.to_id "
        "   WHERE l.from_id=o.id AND l.type='in_repo' AND p.type='SoftwareProject' "
        "   ORDER BY l.created_at DESC LIMIT 1) AS proj "
        "FROM objects o "
        "WHERE o.type='Thread' AND o.status='active' "
        "AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "  = 'operator' "
        "AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='status' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open')='open' "
        "ORDER BY (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='kind' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "  IS DISTINCT FROM 'obligation', o.created_at DESC LIMIT $1", limit)
    today = datetime.now(UTC).date().isoformat()
    return [{"id": str(r["id"])[:8], "summary": (r["s"] or "")[:200],
             "project": r["proj"] or "—", "born": r["created_at"].isoformat(),
             "guessed": r["ec"] == "derived",  # the miner INFERRED this duty; nobody asked
             **({"kind": r["k"]} if r["k"] else {})}
            for r in rows if r["s"] and not (r["d"] and r["d"] > today)]


async def read_desk(pool: asyncpg.Pool, *, limit: int = 100) -> dict[str, Any]:
    """The ORGANIZED desk — always a peek (reading the human's desk never leases; settling
    is only ever the human's explicit word via ack). Bands ordered by what the human must
    see first; within a band: thread-folded (newest per thread — the supersession lane the
    wake prompt teaches), then same-story clustered. Dimmed briefs collapse to one line
    each at the bottom — annotated moot by an agent, still the human's to dismiss."""
    rows = await pool.fetch(
        "SELECT m.id, m.from_agent, m.from_project, m.body, m.created_at, m.thread_id, "
        " m.desk_kind, m.moot_note, m.moot_by "
        "FROM fleet_messages m WHERE m.to_project=$1 AND m.read_at IS NULL "
        "AND NOT EXISTS (SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
        "  AND r.agent_id=$1 AND r.read_at IS NOT NULL) "
        "ORDER BY m.created_at DESC LIMIT $2", OPERATOR_ADDR, limit)
    # thread-fold: the newest brief in a thread speaks for it; earlier ones ride under it
    by_thread: dict[int, list[Any]] = {}
    for r in rows:  # rows arrive newest-first
        by_thread.setdefault(r["thread_id"] or r["id"], []).append(r)
    cards, dimmed = [], []
    for members in by_thread.values():
        lead, earlier = members[0], members[1:]
        card: dict[str, Any] = {
            "id": lead["id"], "from": lead["from_agent"], "from_project": lead["from_project"],
            "body": lead["body"], "when": lead["created_at"].isoformat()}
        if earlier:
            card["thread_folded"] = {
                "count": len(earlier), "ids": [e["id"] for e in earlier],
                "note": "earlier briefs in this thread — superseded by the one above"}
        if lead["moot_note"]:
            dimmed.append({"id": card["id"], "from": card["from"],
                           "project": card["from_project"],
                           "headline": (lead["body"] or "")[:100],
                           "moot": lead["moot_note"], "by": lead["moot_by"]})
            continue
        card["band"] = classify_brief(lead["body"], lead["desk_kind"])
        cards.append(card)
    bands: dict[str, list[dict[str, Any]]] = {k: [] for k in DESK_KINDS}
    for c in cards:
        bands[c.pop("band")].append(c)
    for k in DESK_KINDS:  # same-story folding never crosses a band
        bands[k] = await _same_story_clusters(pool, bands[k])
    everything = await _operator_queue(pool)
    # THE RED NUMBER IS A PROMISE. A debt is a duty a MIND deliberately placed on him; a
    # miner's inference is a SUGGESTION and must never wear the same colour. A count he cannot
    # trust is a count he learns to ignore — which is how the desk reached a scary red 11.
    queue = [t for t in everything if not t["guessed"]]
    guessed = [t for t in everything if t["guessed"]]
    # the COUNT comes from the authority, never from len(a-capped-display-fetch) — the
    # chrome and the statusline must show the same red number (vitals, 2026-07-19)
    from src.orchestrator.vitals import operator_debts
    owed = (await operator_debts(pool))["owed"]
    return {
        "owed": owed,
        "letters": len(bands["fyi"]),
        "needs_decision": bands["decision"],
        "needs_hands": bands["hands"],
        "fyi": bands["fyi"],
        **({"dimmed": dimmed} if dimmed else {}),
        **({"your_queue": {
            "threads": queue,
            "note": "open threads a mind deliberately put on YOU (owner='operator', "
                    "SELF_DECLARED) — the canonical waiting-on-your-hands list"}} if queue else {}),
        **({"miner_guesses": {
            "threads": guessed,
            "note": f"{len(guessed)} duties the MINER inferred you owe, from overhearing "
                    "conversation. Nobody asked you. They are NOT counted in `owed` and never "
                    "go red — read them, then resolve or assign the ones that are real"}}
           if guessed else {}),
        "by_project": _group_by_project(queue, bands["decision"] + bands["hands"]),
        "note": "peek — nothing leased; settle only at your word (inbox(project='operator', "
                "ack=[ids])). Folded ids settle with their lead card. THE COUNT THAT MATTERS "
                "is `owed` (debts a mind actually asked of you); `letters` carry no debt and "
                "clear in bulk; `miner_guesses` are inferences, not obligations.",
    }


def _group_by_project(
    debts: list[dict[str, Any]], asks: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """THE DESK AS A WORKSPACE, not a notification list (operator, 2026-07-11: "no good way to
    resolve these debts per thread or per project, so it snowballs into infinity"). One card
    per project carrying BOTH what he owes it (threads) and who asked (the decision/hands
    briefs) — because he clears a project at a sitting, not a band. Letters (fyi) are excluded
    on purpose: they carry no debt and must never crowd the ledger.

    Ordered by weight: most debts first, then most asks — the project that needs him loudest
    is the one he opens."""
    projects: dict[str, dict[str, Any]] = {}

    def slot(name: str) -> dict[str, Any]:
        return projects.setdefault(name, {"project": name, "debts": [], "asks": []})

    for d in debts:
        slot(d.get("project") or "—")["debts"].append(d)
    for a in asks:
        slot(a.get("from_project") or "—")["asks"].append(a)
    out = list(projects.values())
    for p in out:
        p["owed"] = len(p["debts"])
        p["critical"] = any("🚨" in (a.get("body") or "") or "CRITICAL" in (a.get("body") or "")
                            for a in p["asks"])
        # the oldest thing this project is waiting on — the roster's rot signal. Both forms:
        # the stamp (for a mind reading via inbox) and the age in seconds (for the lens, which
        # is pure and must not know what `now` is).
        stamps = ([d["born"] for d in p["debts"] if d.get("born")]
                  + [a["when"] for a in p["asks"] if a.get("when")])
        p["oldest"] = min(stamps) if stamps else None
        p["oldest_secs"] = ((datetime.now(UTC) - datetime.fromisoformat(p["oldest"]))
                            .total_seconds() if p["oldest"] else None)
    out.sort(key=lambda p: (-int(p["critical"]), -p["owed"], -len(p["asks"]), p["project"]))
    return out

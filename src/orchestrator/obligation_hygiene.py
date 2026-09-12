"""OBLIGATION HYGIENE — the no-regrow rule (dispatch #204's own follow-on, decision
a44ab697161a proposing N1=14/N2=14; OPERATOR RULED TIGHTER, 2026-09-05 ~04:40Z, relayed
Thoth DM 7161): N1 = 7 idle days -> a DM nudge to the obligation's own owner. N2 = +7 more
days of continued silence past that nudge -> a STALE-CANDIDATE marker plus a desk brief,
surfaced for a human. NEVER AUTO-RESOLVED at either stage — this mechanism only nudges and
surfaces, it never closes, reclassifies away from 'open', or judges a thread dead on its
own authority.

THE SHAPE mirrors phantom_fold_reap.py's own two-phase discipline (a pure `_dry_run`
report, then a separately-gated `_execute`) rather than reinventing it, and reuses
open_thread_wall's own `last_touched`/`owner` reads (compositions.py) — the SAME
authoritative clock (`assertions.evidence_class='self_declared'`, max `observed_at`) that
already answers "has a MIND touched this" everywhere else in the fleet.

IDLE, exactly as proposed and ruled on: a thread's own `last_touched` is at least N1 days
old, AND the owner has made no self_declared graph write ANYWHERE in that window — an
owner who is visibly alive and working the graph gets the benefit of the doubt even before
they get to this particular thread. Both halves must hold; neither alone is idle.

TWO DURABLE MARKERS, both assertions on the thread itself (name `hygiene_stage`, values
'nudged' / 'stale_candidate'; `hygiene_nudged_at` records when N1 fired) — written at
`EvidenceClass.DERIVED`, DELIBERATELY NOT 'self_declared', so this sweep's own writes can
never count as the "touch" that resets a thread's own idle clock or fools
open_thread_wall's untouched/echo split. A thread genuinely re-annotated by a mind after a
nudge resets the clock and starts a fresh N1 window, exactly as if never nudged.

THE OWNER-ADDRESS FALLBACK (owner='operator' when the owner is a project name or no live
agent, exactly as ruled): the nudge always tries a DM to the declared owner first — a
project-name owner, or one `send_message` cannot resolve to a live agent, routes to the
operator's desk instead. `send_message`'s own ValueError on an unresolvable `to_agent` IS
that "no live agent" signal; this module never re-derives seat liveness a second way.

SCHEDULED LEG stays a normal cron switch (`osiris_obligation_hygiene_enabled`) — but per
the operator's own explicit instruction ("land it with the flag ON"), this ships TRUE by
default rather than the dark-by-default convention every sibling scheduled writer in this
house otherwise follows (fleet_reconcile, phantom_heal, phantom_fold_reap, landing_audit,
tree_ingest_alarm) — a deliberate, named exception, not an oversight."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.config.settings import Settings, get_settings
from src.parsers.base import EvidenceClass

N1_IDLE_DAYS = 7
N2_SILENCE_DAYS = 7

_HYGIENE_EC = EvidenceClass.DERIVED.value
_SANCTIONED_HYGIENE_ACTOR = "cron:obligation_hygiene_heartbeat"

# THE SAME summary-display COALESCE every wall/roadmap query uses (compositions.py's own
# _SUMMARY_DISPLAY_SQL) — a corrected summary wins over the original by default.
_SUMMARY_SQL = (
    "COALESCE("
    "(SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
    " AND a.name='corrected_summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), "
    "(SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
    " AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1))"
)

_KIND_SQL = (
    "(SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
    " AND a.name='kind' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)"
)
_STATUS_SQL = (
    "COALESCE((SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
    " AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open')"
)


async def _open_obligation_rows(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Every OPEN kind='obligation' Thread fleet-wide, with `owner`, `last_touched` (the
    freshest self_declared assertion's observed_at — open_thread_wall's own clock), and
    this sweep's own prior markers read back (`hygiene_stage`/`hygiene_nudged_at`) so a
    tick is idempotent against its own last tick.

    `contested`/`summary_age_days` (fix (c), Metron's mechanism report, mail 8890/8921/
    8922): the nudge that quotes `summary` back at the owner must never present a
    disputed headline as settled fact — `contested` names the dispute, `summary_age_days`
    (how long the CURRENT summary text has stood, from its own last touch to now) lets
    the nudge say "unchanged for N days" instead of implying it was just observed.

    `answered_by` (wave 18 item 1, thread 898840dc, "the stale nudge quotes it"): every
    live `answers` edge already landed on this row (`capture.thread_answering_decisions`,
    the SAME batched read `recall()` uses for its own `bears_on_from`) — a row Seshat's
    own sweep found FOUR of six stale board rows already carried, unrouted, before
    mint_bears_on existed at all. Never suppresses or reclassifies the nudge; only lets
    it say "already answered by decision X" instead of blindly asking the owner to
    re-measure something someone already did."""
    from src.orchestrator.capture import (
        CONTESTED_SQL,
        LAST_SUMMARY_TOUCH_SQL,
        thread_answering_decisions,
    )

    rows = await pool.fetch(
        "SELECT o.id, o.created_at, "
        f" {_SUMMARY_SQL} AS summary, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS owner, "
        " (SELECT max(sa.observed_at) FROM assertions sa WHERE sa.object_id=o.id "
        "   AND sa.evidence_class='self_declared') AS last_touched, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='hygiene_stage' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS hygiene_stage, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='hygiene_nudged_at' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS hygiene_nudged_at, "
        f" {CONTESTED_SQL} AS contested, "
        f" {LAST_SUMMARY_TOUCH_SQL} AS last_summary_touch "
        "FROM objects o "
        f"WHERE o.type='Thread' AND o.merged_into IS NULL AND o.status='active' "
        f"  AND {_STATUS_SQL}='open' AND {_KIND_SQL}='obligation'")
    answers_by_thread = await thread_answering_decisions(pool, [r["id"] for r in rows])
    return [{**dict(r), "answered_by": answers_by_thread.get(r["id"], [])} for r in rows]


async def _owner_active_since(pool: asyncpg.Pool, owner: str, since: datetime) -> bool:
    """Has this owner made ANY self_declared graph write, anywhere, since `since`? — the
    idle definition's own "no graph write by its owner in the window" half, deliberately
    fleet-wide rather than scoped to one thread. Case-insensitive exact match on
    `assertions.source_id`, the same forgiving match rank_open_threads already applies to
    a bare seat handle."""
    return bool(await pool.fetchval(
        "SELECT 1 FROM assertions WHERE evidence_class='self_declared' "
        "  AND lower(source_id)=lower($1) AND observed_at > $2 LIMIT 1", owner, since))


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _quote_summary(item: dict[str, Any]) -> str:
    """Fix (c), Metron's mechanism report (mail 8890/8921/8922): the nudge quotes a
    summary WITH its age and, when disputed, says so — never as though it were current,
    unverified fact. `summary_age_days` is None only when the thread's own created_at
    (this function's last-resort clock) is somehow absent; the quote degrades gracefully
    rather than raising.

    ANSWERED-BY, APPENDED (wave 18 item 1, thread 898840dc, "the stale nudge quotes it"):
    when `mint_bears_on` has already routed a fresh Decision onto this exact row, the
    nudge says so and quotes it — never suppressing or softening the nudge itself (the
    row is still open and still needs a human act), only sparing the owner a redundant
    re-measurement of something someone already found."""
    age = item.get("summary_age_days")
    aged = f"{item['summary']!r}, unchanged for {age} day(s)" if age is not None \
        else f"{item['summary']!r}"
    if item.get("contested"):
        aged = f"{aged} — CONTESTED: a newer note disputes this summary, unresolved"
    answers = item.get("answered_by") or []
    if answers:
        quoted = "; ".join(f"{a['id']} ({a['summary']!r})" for a in answers)
        aged = f"{aged} — ALREADY ANSWERED by {len(answers)} decision(s): {quoted}"
    return aged


async def hygiene_dry_run(pool: asyncpg.Pool, *, now: datetime | None = None) -> dict[str, Any]:
    """THE REPORT — every open obligation Thread, bucketed into would_nudge /
    would_stale_candidate / no_action with the reason named. Writes NOTHING."""
    now = now or datetime.now(UTC)
    n1_cutoff = now - timedelta(days=N1_IDLE_DAYS)
    rows = await _open_obligation_rows(pool)
    buckets: dict[str, list[dict[str, Any]]] = {
        "would_nudge": [], "would_stale_candidate": [], "no_action": [],
    }
    for r in rows:
        last_touched = r["last_touched"] or r["created_at"]
        owner = (r["owner"] or "").strip() or None
        stage = r["hygiene_stage"]
        nudged_at = _parse_dt(r["hygiene_nudged_at"])
        summary_touch = r["last_summary_touch"] or r["created_at"]
        item: dict[str, Any] = {
            "thread_id": str(r["id"]), "owner": owner, "summary": r["summary"],
            "last_touched": last_touched.isoformat() if last_touched else None,
            "stage": stage, "contested": bool(r["contested"]),
            "summary_age_days": (now - summary_touch).days if summary_touch else None,
            "answered_by": r["answered_by"],
        }

        if stage == "stale_candidate":
            buckets["no_action"].append({
                **item, "reason": "already a stale-candidate — never re-classified, "
                                   "never auto-resolved"})
            continue

        if stage == "nudged" and nudged_at is not None and last_touched <= nudged_at:
            # No touch on the thread since its own nudge — check the N2 silence window.
            if now - nudged_at >= timedelta(days=N2_SILENCE_DAYS):
                owner_silent = True
                if owner:
                    owner_silent = not await _owner_active_since(pool, owner, nudged_at)
                if owner_silent:
                    buckets["would_stale_candidate"].append(item)
                    continue
            buckets["no_action"].append({
                **item, "reason": "nudged — awaiting the N2 window or the owner's own "
                                   "activity"})
            continue

        # stage is None, OR 'nudged' but genuinely touched since (last_touched > nudged_at)
        # — a real touch resets the clock exactly as if never nudged.
        if now - last_touched >= timedelta(days=N1_IDLE_DAYS):
            owner_silent = True
            if owner:
                owner_silent = not await _owner_active_since(pool, owner, n1_cutoff)
            if owner_silent:
                buckets["would_nudge"].append(item)
                continue
        buckets["no_action"].append({**item, "reason": "not idle"})

    return {
        "buckets": buckets, "counts": {k: len(v) for k, v in buckets.items()},
        "generated_at": now.isoformat(),
    }


async def _project_name_for(pool: asyncpg.Pool, owner: str) -> str | None:
    """The bare project name `owner` names, or None when it isn't one — a SoftwareProject
    lookup, case-insensitive, with or without the `repo:` canonical prefix."""
    if owner.lower() == "operator":
        return None
    row = await pool.fetchval(
        "SELECT replace(canonical,'repo:','') FROM objects WHERE type='SoftwareProject' AND "
        "(lower(canonical)=lower($1) OR lower(canonical)=lower('repo:'||$1)) LIMIT 1", owner)
    return str(row) if row else None


async def _is_exactly_live(pool: asyncpg.Pool, agent_id: str) -> bool:
    """`mounts.agent_liveness_exact`'s own bool shape — no lineage-wide LIKE broadening
    across a base id's generations. A DM to a specific `agent:<id>` addresses THAT
    generation's own mailbox literally (send_message's own grave rule: an explicit id is
    an act of intent, never silently redirected) — a live SUCCESSOR does not make an
    ANCESTOR's own address live, only lineage_head's forward walk finds the successor
    worth redirecting to. Delegates outright (thread 7dd09031) rather than re-deriving
    the same query a second time — this WAS a hand-rolled duplicate of
    `agent_liveness_exact`, carrying the same stale `current_assertions.last_active`
    defect that function has since dropped; calling it directly means this can never
    drift from that fix again."""
    from src.orchestrator.mounts import agent_liveness_exact

    return bool((await agent_liveness_exact(pool, agent_id))["live"])


async def resolve_owner_target(pool: asyncpg.Pool, owner: str | None) -> dict[str, Any]:
    """THE OWNER-RESOLUTION LADDER (operator "one more round" 2026-09-05, relayed Thoth DM
    7391 — the first firing's own live proof found 128 of 151 nudges landing on the
    operator's desk because owners are project names or dead agents, "that makes the desk
    the pile"): a rung is tried before falling to the desk, never instead of trying at all.

    Rung 0 — AN OWNER STRING IS A SEAT BEFORE IT IS A REPO (Thoth ruling, msg 7425,
    correcting decision 1014b74804da's "44 no-match = charter gap": 42 of those 44 turned
    out to be bare seat handles — 'Thoth', 'seshat', 'imhotep' — not project names at all;
    a sample thread owned by 'Thoth' was a literal duty for whoever holds that seat, not a
    project awaiting a claimant). Checked FIRST, before the project-name rung, because a
    seat's own bare handle and a project's bare name are indistinguishable strings — a
    project-name lookup that happens to hit a stale/retired SoftwareProject sharing the
    same spelling would otherwise pre-empt this every time. Case-insensitive match against
    a live Seat's `handle`; the seat's own current holder, gated by the SAME exact-liveness
    test rung 2 uses (`_is_exactly_live`, not `seat_occupancy`'s lineage-broadened
    `agent_liveness`) — a DM addresses a literal agent id, so what matters is whether that
    exact holder is live right now, not whether the seat's lineage survives under some
    other generation.

    Rung 1 — owner is a PROJECT NAME: that project's live seat head — `roster(repo=...)`'s
    own single unambiguous match, occupied right now; when charter and pin disagree but one
    seat manages the other (`agreement='governed'`), the MANAGING seat, never the managed
    one; when N>2 seats all match and it's their own HOUSE's home repo
    (`agreement='shared-house'`, roster's own third case, Thoth ruling msg 7425 — every
    worker legitimately charters/pins the house's own repo, that's the normal shape, not a
    conflict), the house's manager seat. A genuine 2-seat `conflict` gets one more look,
    narrowly (operator ruling, decision 2ee59140, msg 7428) — NEVER folded into roster's
    own `governed` split, which stays charter-manages-pin-specific and tested against the
    reverse direction: if one matched seat MANAGES the other regardless of which via-signal
    each carries, the manager, same role as `governed`; if the two are an active `peer_of`
    pair, whichever peer is live, BOTH when both are (a peer pair is shared ownership, never
    a coin flip — `target` is then a list, and `_nudge_owner` sends the same nudge to each).
    Anything else stays a genuine `conflict`, falls through, never guessed at.

    Rung 2 — owner is a DEAD/RETIRED AGENT id: its own lineage head (`lineage_head`'s
    forward `succeeded_by` walk — the SAME authority `send_message`'s reply-routing already
    trusts for "where does this soul live now"), but ONLY if that HEAD's own exact address
    is currently live — deliberately NOT `agent_liveness` (its own lineage-wide LIKE
    broadening treats a live successor as proof the ANCESTOR's own literal mailbox is
    "live" too, which is backwards for a DM: `send_message` addresses an `agent:<id>`
    literally, the grave rule, so what matters is whether HEAD's own exact address has a
    body reading it, not whether the soul survives under some other numeral). A lineage
    that ends in another corpse is not a rung, it's the same fall-through.

    Every fall-through carries a `reason` naming exactly which rung failed and why — the
    desk brief for a fallback quotes it verbatim, per the dispatch's own instruction ("the
    desk brief for those carries the reason"). Pure and read-only: sends nothing, so a
    caller (real send, or a dry-run report) can call this as many times as it likes without
    ever double-nudging a real owner."""
    from src.orchestrator.agents import lineage_head, resolve_seat
    from src.orchestrator.mailbox import _dm_eligible
    from src.orchestrator.seats import (
        manager_of_seat,
        peer_of_seat,
        roster,
        seat_holder_ineligible,
        seat_occupancy,
    )

    target = (owner or "").strip()
    if not target or target.lower() == "operator":
        return {"channel": "desk", "target": None, "reason": "unowned, or owner=operator"}

    seat_id = await pool.fetchval(
        "SELECT o.canonical FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='Seat' AND o.status='active' AND a.name='handle' "
        "AND lower(a.value #>> '{}') = lower($1) LIMIT 1", target)
    if seat_id is not None:
        occ = await seat_occupancy(pool, seat_id)
        holder = occ.get("holder")
        if holder is not None:
            head = await lineage_head(pool, holder)
            if await _is_exactly_live(pool, head) and await _dm_eligible(pool, head):
                return {"channel": "dm", "target": head, "reason": None}
            reason = f"seat {target!r} ({seat_id}) has no live holder right now"
        else:
            reason = f"seat {target!r} ({seat_id}) is vacant — never held"
        return {"channel": "desk", "target": None, "reason": reason}

    project = await _project_name_for(pool, target)
    if project is not None:
        out = await roster(pool, repo=project)
        agreement, matches = out.get("agreement"), out.get("matches") or []

        if agreement == "conflict" and len(matches) == 2:
            # TWO RESOLUTIONS ROSTER ITSELF NEVER CLASSIFIES (operator ruling, decision
            # 2ee59140, relayed Thoth msg 7428) — narrowly scoped to THIS ladder, never
            # folded into roster's own `governed`/`conflict` split (that split is
            # deliberately charter-manages-pin-specific, tested against the reverse
            # direction — see test_roster_repo_lookup_stays_conflict_when_the_manager_edge_
            # points_the_other_way; widening it there would silently flip that boundary):
            #
            # PEER CHECKED FIRST (Ptah/Ra's own live shape caught this): a peer_of bond is
            # the FRESH, deliberate signal this exact ruling minted for this exact pair; an
            # older, unrelated managed_by edge between the same two seats (an ordinary org
            # fact, not a statement about who owns THIS repo) must never silently outrank
            # it — checking manager first found Ptah already managed_by Ra from some
            # earlier, unrelated org fact and would have nudged neither seat as a "cold
            # manager" even though the operator explicitly ruled this pair PEERS, not
            # manager/managed, for rotten-apple specifically.
            #
            # (a) the two matched seats are an active peer_of pair (rotten-apple: Ptah/Ra;
            # xxit: deckard/metron) — never a conflict once peered: whichever peer is live,
            # BOTH when both are (a peer pair is recognized as shared ownership, not two
            # rivals — silently picking one over the other would be exactly the guess this
            # ladder refuses to make everywhere else).
            #
            # (b) one matched seat MANAGES the other, regardless of which via-signal each
            # carries (mudra's own shape: vajra matches via both charter+pin, alfred via
            # charter only, but alfred already manages vajra) — prefer the manager, same
            # role `governed` gives the charter-seat.
            seat_a, seat_b = matches[0]["seat"], matches[1]["seat"]
            if await peer_of_seat(pool, seat_a) == seat_b:
                live = [m for m in matches
                        if m.get("occupancy") == "occupied" and m.get("holder")]
                if live:
                    target_out: str | list[str] = (
                        str(live[0]["holder"]) if len(live) == 1
                        else [str(m["holder"]) for m in live])
                    return {"channel": "dm", "target": target_out, "reason": None}
                return {"channel": "desk", "target": None,
                        "reason": f"peer pair for project {project!r} "
                                  f"({seat_a}, {seat_b}) — neither peer is live"}
            manager_seat = (
                seat_b if await manager_of_seat(pool, seat_a) == seat_b else
                seat_a if await manager_of_seat(pool, seat_b) == seat_a else None)
            if manager_seat is not None:
                chosen = next(m for m in matches if m["seat"] == manager_seat)
                if chosen.get("occupancy") == "occupied" and chosen.get("holder"):
                    return {"channel": "dm", "target": str(chosen["holder"]), "reason": None}
                return {"channel": "desk", "target": None,
                        "reason": f"no live seat for project {project!r} "
                                  f"(seat {chosen['seat']} is {chosen.get('occupancy')})"}

        chosen = None
        if agreement == "governed":
            chosen = next((m for m in matches if "charter" in m["via"]), None)
        elif agreement == "shared-house":
            manager_seat = out.get("manager")
            chosen = next((m for m in matches if m["seat"] == manager_seat), None)
        elif agreement == "single-match":
            chosen = matches[0]
        if chosen and chosen.get("occupancy") == "occupied" and chosen.get("holder"):
            return {"channel": "dm", "target": str(chosen["holder"]), "reason": None}
        if agreement == "no-match":
            reason = f"no seat's charter or pin names project {project!r}"
        elif agreement == "conflict":
            seats = ", ".join(m["seat"] for m in matches)
            reason = (f"ambiguous: {len(matches)} seats claim project {project!r} "
                      f"({seats}), no governed manager to prefer")
        elif chosen is not None:
            reason = (f"no live seat for project {project!r} "
                      f"(seat {chosen['seat']} is {chosen.get('occupancy')})")
        else:
            reason = f"no live seat for project {project!r}"
        return {"channel": "desk", "target": None, "reason": reason}

    if target.startswith("agent:"):
        head = await lineage_head(pool, target)
        if await _is_exactly_live(pool, head) and await _dm_eligible(pool, head):
            return {"channel": "dm", "target": head, "reason": None}
        reason = (f"no live successor for {target!r}" if head == target else
                  f"lineage head {head!r} of {target!r} is not currently live either")
        return {"channel": "desk", "target": None, "reason": reason}

    if target.startswith("seat:"):
        exists = await pool.fetchval(
            "SELECT 1 FROM objects WHERE canonical=$1 AND type='Seat' AND status='active'",
            target)
        if not exists:
            return {"channel": "desk", "target": None, "reason": f"no such seat: {target!r}"}
        occ = await seat_occupancy(pool, target)
        if (occ["state"] == "occupied" and occ["holder"]
                and await _dm_eligible(pool, occ["holder"])):
            return {"channel": "dm", "target": str(occ["holder"]), "reason": None}
        return {"channel": "desk", "target": None,
                "reason": f"seat {target!r} is {occ['state']}"}

    # a bare handle/name — resolve_seat's own territory, the SAME live-holder-at-read-time
    # resolution send_message itself uses for a plain to_agent= name.
    ineligible = await seat_holder_ineligible(pool, target)
    if ineligible is not None:
        return {"channel": "desk", "target": None, "reason": ineligible}
    resolved = await resolve_seat(Actions(pool), target)
    agent = resolved.get("agent")
    if agent is None:
        return {"channel": "desk", "target": None, "reason": f"no agent named {target!r}"}
    return {"channel": "dm", "target": resolved.get("seat_id") or agent, "reason": None}


async def _nudge_owner(
    pool: asyncpg.Pool, owner: str | None, *, actor: str, body: str,
) -> dict[str, Any]:
    """Resolves via the owner-resolution ladder (`resolve_owner_target`), then sends
    against that verdict — a DM when a rung resolved (ONE send, except a live peer_of pair
    where BOTH peers are live: `target` is then a list and each gets the SAME nudge, per
    the operator's own ruling that a peer pair is shared ownership, never a coin flip), the
    operator's desk (with the failing rung's own reason appended) otherwise. A race between
    resolution and send (the target goes cold in between) still falls back to the desk
    rather than losing the nudge outright."""
    from src.orchestrator.mailbox import send_message

    verdict = await resolve_owner_target(pool, owner)
    if verdict["channel"] == "dm":
        targets = verdict["target"] if isinstance(verdict["target"], list) else [verdict["target"]]
        try:
            sent = [await send_message(
                pool, from_agent=actor, from_project="osiris", to_agent=t,
                body=body, grade="ask") for t in targets]
            return sent[0] if len(sent) == 1 else {"sent_to": targets, "receipts": sent}
        except ValueError as exc:
            verdict = {"channel": "desk", "target": None, "reason": str(exc)}
    return await send_message(
        pool, from_agent=actor, from_project="osiris", to_project="operator",
        body=f"{body}\n\n(owner-address fallback: {verdict['reason']})", desk_kind="fyi")


async def hygiene_execute(
    actions: Actions, *, actor: str = _SANCTIONED_HYGIENE_ACTOR, execute: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """THE ACTING HALF. DRY RUN IS THE DEFAULT (`execute=False`) — re-reads the tray itself
    via `hygiene_dry_run` (never trusts a caller-supplied stale report), returning the exact
    plan without writing anything.

    would_nudge: a DM (or desk brief, per the owner-address fallback) plus the two durable
    markers (`hygiene_nudged_at`, `hygiene_stage='nudged'`) — written even if the mail send
    itself fails, so a mail hiccup never masks the tick's own idle finding; the failed send
    is reported inline on the row instead.

    would_stale_candidate: `hygiene_stage='stale_candidate'` plus one desk `fyi` brief.
    NEVER a status change on the thread, never a resolve — the record's own law here is
    identical to phantom_fold_reap's: a single row's mail hiccup is caught and reported
    inline rather than aborting the batch."""
    now = now or datetime.now(UTC)
    report = await hygiene_dry_run(actions.pool, now=now)
    plan: dict[str, Any] = {
        "would_nudge": [dict(r) for r in report["buckets"]["would_nudge"]],
        "would_stale_candidate": [dict(r) for r in report["buckets"]["would_stale_candidate"]],
        "execute": execute,
    }
    if not execute:
        plan["note"] = "PLAN ONLY — call with execute=True to write. Nothing touched."
        return plan

    nudged: list[dict[str, Any]] = []
    for item in plan["would_nudge"]:
        tid = uuid.UUID(item["thread_id"])
        body = (
            f"OBLIGATION HYGIENE NUDGE — thread {item['thread_id'][:8]} has been idle "
            f"{N1_IDLE_DAYS}+ days ({_quote_summary(item)}). Touch it (annotate/resolve/"
            f"reclassify) or it becomes a STALE-CANDIDATE on the operator's desk after "
            f"{N2_SILENCE_DAYS} more days of silence. Never auto-resolved.")
        try:
            sent = await _nudge_owner(actions.pool, item["owner"], actor=actor, body=body)
        except Exception as exc:  # noqa: BLE001 — a mail hiccup must not skip the marker
            sent = {"error": f"{type(exc).__name__}: {exc}"}
        await actions.assert_property(
            tid, "hygiene_nudged_at", now.isoformat(), actor, now, 0.9,
            evidence_class=_HYGIENE_EC)
        await actions.assert_property(
            tid, "hygiene_stage", "nudged", actor, now, 0.9, evidence_class=_HYGIENE_EC)
        nudged.append({**item, "sent": sent})

    staled: list[dict[str, Any]] = []
    for item in plan["would_stale_candidate"]:
        tid = uuid.UUID(item["thread_id"])
        await actions.assert_property(
            tid, "hygiene_stage", "stale_candidate", actor, now, 0.9,
            evidence_class=_HYGIENE_EC)
        body = (
            f"OBLIGATION STALE-CANDIDATE — thread {item['thread_id'][:8]} "
            f"(owner={item['owner']!r}, {_quote_summary(item)}) drew a nudge and "
            f"{N2_SILENCE_DAYS}+ more days of silence since. NEVER auto-resolved — a "
            f"human call on whether it's still real.")
        try:
            from src.orchestrator.mailbox import send_message
            sent = await send_message(
                actions.pool, from_agent=actor, from_project="osiris",
                to_project="operator", body=body, desk_kind="fyi")
        except Exception as exc:  # noqa: BLE001 — the marker must land even if mail fails
            sent = {"error": f"{type(exc).__name__}: {exc}"}
        staled.append({**item, "desk_brief": sent})

    plan.update({
        "nudged": nudged, "staled": staled,
        "note": "EXECUTED — markers and mail attempted for every row named above.",
    })
    return plan


async def hygiene_status(pool: asyncpg.Pool) -> dict[str, Any]:
    """counts per stage per project — 'none' (never nudged), 'nudged', 'stale_candidate' —
    across every open kind='obligation' Thread fleet-wide. An unfiled obligation (no
    in_repo link) counts under '(unfiled)'. Read-only, no scheduling side effect."""
    rows = await pool.fetch(
        "SELECT COALESCE(p.canonical, '(unfiled)') AS project, "
        " COALESCE((SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='hygiene_stage' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), "
        "   'none') AS stage, "
        " count(*) AS n "
        "FROM objects o "
        "LEFT JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
        "  AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "LEFT JOIN objects p ON p.id=l.to_id "
        f"WHERE o.type='Thread' AND o.merged_into IS NULL AND o.status='active' "
        f"  AND {_STATUS_SQL}='open' AND {_KIND_SQL}='obligation' "
        "GROUP BY 1, 2 ORDER BY 1, 2")
    out: dict[str, dict[str, int]] = {}
    for r in rows:
        out.setdefault(r["project"], {})[r["stage"]] = r["n"]
    return {"counts_by_project": out, "generated_at": datetime.now(UTC).isoformat()}


async def obligation_hygiene_scheduled_tick(
    actions: Actions, *, settings: Settings | None = None, now: datetime | None = None,
) -> dict[str, Any]:
    """THE SCHEDULED LEG's own tick — `arq_worker.obligation_hygiene_heartbeat` calls this
    unconditionally, the same thin-shim shape every other scheduled writer in this house
    uses. OFF unless `osiris_obligation_hygiene_enabled` — but per the operator's own
    explicit instruction this ships TRUE by default, a named exception to every sibling
    switch's dark-by-default convention."""
    st = settings or get_settings()
    if not st.osiris_obligation_hygiene_enabled:
        return {"enabled": False, "nudged": [], "staled": [],
                "note": "the sweep's scheduled leg is dark "
                        "(osiris_obligation_hygiene_enabled=0)"}
    out = await hygiene_execute(actions, actor=_SANCTIONED_HYGIENE_ACTOR, execute=True,
                                now=now)
    return {"enabled": True, **out}

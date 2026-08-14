"""THE OFFICE CEREMONY — one act moves a seat into its Osiris-owned home.

The seat-offices ruling (ed5f5ce2): agents sit at ~/.osiris/seats/<handle>/, code stays in
the repos they GOVERN — the sit-place is Osiris's, stable forever, and the fragile-gitignore
class (agent state inside code repos) ends seat by seat. alfred's office, the first, was
hand-assembled across four separate acts (mkdir, .osiris, CLAUDE.md, rebind-extract); the
rollout to his chartered children must be ONE CALL with one receipt, or every transition
re-derives the ceremony from a transcript.

The primitive composes what already exists rather than re-owning any of it: the standing
orders are written here (the one genuinely new artifact — a per-seat boot sector), then
`rebind_seat(extract=True)` carries everything else (the .osiris pin, the lineage's mount
rows, the transcripts with their re-addressing, the Seat object's anchor).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.orchestrator.mounts import rebind_seat

_DEFAULT_OFFICE_ROOT = Path.home() / ".osiris" / "seats"


def is_bare_office_root(cwd: str | Path | None) -> bool:
    """True only for the exact seat-office CONTAINER (~/.osiris/seats) — the parent of every
    seat, never a project of its own (ruling 577988ed). ONE shared check so every cwd→project
    fold (agents.resolve_identity, census.live_bodies) applies the SAME guard instead of
    drifting copies (msg 1888: census.live_bodies was missing this and minted a phantom
    "seats" project row from a live process sitting at the bare root).

    Deliberately narrower than "any office subdirectory": a real seat's own office dir IS an
    ordinary basename guess here BY DESIGN — see
    test_resolve_identity_never_invents_a_project_from_the_bare_office_root. A seated agent's
    project resolving to its seat's HOUSE instead of its handle is the DB-backed resolver's
    job (seats.resolve_project), not this pure, cwd-only guard's."""
    return cwd is not None and Path(cwd) == _DEFAULT_OFFICE_ROOT


_WORKTREE_MARKER = "/.claude/worktrees/"


def _infer_tree_kind(path: str) -> str | None:
    """office | worktree | repo | container from PATH SHAPE alone, ruling 719ed5b1's
    kind key — no graph query, no I/O beyond a `.git` existence check. `container` has its
    own exact-match caller (`is_bare_office_root`) and `office` is decided by the caller
    (a seat's `anchor_cwd`, never inferred from shape); this only ever returns worktree,
    repo, or None for anything else. None is deliberate, not a bug: a directory whose shape
    matches neither a worktree path nor a real repo root gets no `kind` proposal — the
    migration this feeds names it as a gap rather than guessing (Thoth's own constraint,
    msg 3919: 'a pin that confidently states a wrong seat is worse than a bare pin')."""
    if _WORKTREE_MARKER in path.replace("\\", "/"):
        return "worktree"
    if (Path(path) / ".git").exists():
        return "repo"
    return None


def _dir_exists(path: str | None) -> bool:
    """A plain sync wrapper so `plan_pin_migration` (async) never calls a blocking Path
    method inline (ASYNC240) — the same convention `seats.roster`'s own `_dir_exists`
    already keeps, kept local here rather than importing a private helper cross-module."""
    return path is not None and Path(path).is_dir()


async def plan_pin_migration(pool: asyncpg.Pool) -> dict[str, Any]:
    """DRY RUN ONLY — never writes a byte (ruling 719ed5b1's five-key schema; Thoth's own two
    added constraints, msg 3919). (1) A gap the graph cannot answer confidently is NAMED and
    left unwritten, never guessed into a declaration — a pin that confidently states a wrong
    seat is worse than a bare one, because the whole point of this build is to make the pin
    outrank inference. (2) Every path's diff is computed and returned here, in full, BEFORE a
    single file changes — 35 seats' identity files is the largest on-disk write this house has
    made in one act, and there is no undo but git, which does not cover `~/.osiris`.

    Walks `roster()`'s own seat rows — the graph's EXISTING single source for handle/house/
    anchor_cwd/tree_cwd, invents no new resolution path — and for every real directory
    (`anchor_cwd`, and `tree_cwd` when it names a distinct, existing path) proposes:
      seat  — the row's own handle. Skipped entirely for a seat with no handle on record.
      house — `derive_house`'s own answer. None IS the honest "I don't know" (already built
              into that function's cycle/hop-limit handling, ruling ff6148b0) — reported as a
              gap, never defaulted to anything.
      kind  — "office" for `anchor_cwd`; `_infer_tree_kind`'s path-shape read for `tree_cwd`.

    A path CLAIMED BY MORE THAN ONE SEAT (two rows naming the same anchor_cwd or tree_cwd —
    should not happen, asserted rather than assumed) drops `seat` for that path and reports
    the conflict instead of picking one; `house`/`kind` still propose if every claimant agrees
    on them, since those aren't identity-bearing the way `seat` is.

    `project`/`model` are untouched entirely — this plans only the three new keys, and only
    ever proposes ADDING/CORRECTING them (never invents or removes anything else in a pin,
    matching every existing writer's own preserve-what-I-don't-own discipline, `_write_osiris_
    file`'s own convention). `changes` is the actual diff (current != proposed, so a pin
    already correct proposes nothing there — idempotent by construction, the same no-churn
    discipline `_write_model_pin_sync` already keeps for `model`).

    THE PIN SCHEMA'S OWN CONTRACT, STATED EXPLICITLY (decisions 126210f0/23b667d0, task
    #152's own khepri mistake): `project` HOLDS A SoftwareProject'S CANONICAL SUFFIX,
    NEVER A DISPLAY NAME. This was never written down before, and that silence is exactly
    what let a "corrected" pin regress — `rename_project` changes only the `name` property,
    the canonical stays fixed forever, and every mint/lookup path outside a narrow
    diagnostic read (`register_agent`'s/`mint_heir`'s own `_resolve_or_mint_project`,
    `f"repo:{project}"`, literal) treats a pin that doesn't match the canonical as grounds
    to MINT A BRAND NEW OBJECT — not to look the project up by its current name. A seat
    whose project was renamed keeps the OLD canonical string in its pin, forever; only the
    project's own `name` property changes. `roster()`'s own `pin.name_resolution` field
    (seats.py) is diagnostic-only for exactly this shape — it never implies a pin should be
    rewritten to a display name, only reports when one already, mistakenly, is."""
    from src.orchestrator.agents import read_house_label, read_seat_handle, read_tree_kind
    from src.orchestrator.seats import roster

    data = await roster(pool)
    claims: dict[str, list[tuple[str, str]]] = {}       # path -> [(seat_id, handle), ...]
    kind_of: dict[str, str | None] = {}                  # path -> proposed kind
    house_of_path: dict[str, list[str | None]] = {}      # path -> every claimant's house answer

    for row in data["seats"]:
        handle = row["handle"]
        if not handle:
            continue  # no handle on record — nothing to declare `seat` as, anywhere
        seat_id = row["seat"]
        house = row["house"]
        anchor, tree = row["anchor_cwd"], row["tree_cwd"]
        for path, is_office in ((anchor, True), (tree, False) if tree != anchor else (None, False)):
            if not _dir_exists(path):
                continue
            assert path is not None  # narrowed by _dir_exists above; mypy can't see through it
            claims.setdefault(path, []).append((seat_id, handle))
            kind_of[path] = "office" if is_office else _infer_tree_kind(path)
            house_of_path.setdefault(path, []).append(house)

    plan: list[dict[str, Any]] = []
    for path, claimants in sorted(claims.items()):
        distinct_handles = {h for _sid, h in claimants}
        current = {"house": read_house_label(path), "seat": read_seat_handle(path),
                   "kind": read_tree_kind(path)}
        proposed: dict[str, str] = {}
        unknown: list[str] = []
        if len(distinct_handles) > 1:
            unknown.append(f"seat: conflicting claims from {sorted(distinct_handles)} — "
                           "writing nothing")
        else:
            proposed["seat"] = next(iter(distinct_handles))
        houses = {h for h in house_of_path[path] if h}
        if len(houses) == 1:
            proposed["house"] = next(iter(houses))
        elif len(houses) > 1:
            unknown.append(f"house: claimants disagree ({sorted(houses)}) — writing nothing")
        else:
            unknown.append("house: derive_house found nothing (unmanaged head with no "
                           "stamp, or a managed_by cycle) — writing nothing")
        kind = kind_of.get(path)
        if kind:
            proposed["kind"] = kind
        else:
            unknown.append("kind: path shape matches neither office, worktree, nor repo — "
                           "writing nothing")
        changes = {k: v for k, v in proposed.items() if current.get(k) != v}
        if changes or unknown:
            plan.append({"path": path, "current": current, "proposed": proposed,
                         "changes": changes, "unknown": unknown})
    return {
        "plan": plan, "seats_scanned": len(data["seats"]), "paths_with_changes_or_gaps": len(plan),
        "caveats": [
            "READ-ONLY: no file is touched by this function. A separate writer verb applies "
            "`changes` one path at a time, only after this plan is reviewed.",
            "kind is derived from PATH SHAPE alone, never the graph — a directory that "
            "doesn't clearly read as office/worktree/repo gets no kind proposal, not a guess.",
            *data["caveats"],
        ],
    }


def write_pin_additions(path: str, proposed: dict[str, str]) -> dict[str, Any]:
    """THE WRITER — ruling 719ed5b1's five-key schema, applying one `plan_pin_migration` entry's
    `changes` at a time (Thoth's own staged rollout, msg 3929: her office alone first, then the
    rest only after it lands clean — this function is what both stages call, never a bulk
    driver that hides the boundary between them). Three constraints, msg 3929, none negotiable:

    (1) ADDITIVE ONLY. Appends a key ONLY when `path/.osiris` does not already declare it —
    never rewrites, reorders, or reformats an existing line, even to correct a value that
    disagrees with `proposed` (that disagreement is `plan_pin_migration`'s own `changes` dict
    to surface; resolving it by silent overwrite is exactly the drift-vs-truth conflation this
    whole build exists to end). Re-reads the file itself at write time rather than trusting a
    caller's possibly-stale plan snapshot — a stale DIAGNOSIS is a lesser bug than a stale CURE.

    (2) IDEMPOTENT, PROVEN BY TEST, NOT ASSUMED. A key already present — from an earlier call
    to this function, a hand edit, or any other writer — is skipped, so two calls with the same
    `proposed` leave the file byte-identical after the second (`written: False`).

    (3) REVERSIBLE. `~/.osiris` carries no git history and no undo but the one built here:
    before the FIRST byte changes, the file's exact current bytes (or the empty string, if it
    doesn't exist yet) are copied to `path/.osiris.bak` — overwritten on every real write, so
    it always holds "immediately before the most recent touch" — and never written on a no-op
    call. `revert_pin_write` restores from it.

    Refuses (an error dict, nothing written) when the file exists but is not valid TOML —
    appending onto a broken file would make a bad file worse and harder to diagnose, not better.

    Returns `written` (bool), `added` (the keys actually appended, a strict subset of
    `proposed`), `skipped` (keys already present, left untouched), and `backup` (only when a
    write happened)."""
    import tomllib

    p = Path(path) / ".osiris"
    existing_text = p.read_text() if p.is_file() else ""
    try:
        existing = tomllib.loads(existing_text) if existing_text else {}
    except (tomllib.TOMLDecodeError, ValueError) as exc:
        return {"error": f"{p} is not valid TOML ({type(exc).__name__}: {exc}) — refusing to "
                         "append onto a broken file"}

    to_add = {k: v for k, v in proposed.items() if k not in existing}
    skipped = sorted(set(proposed) - set(to_add))
    if not to_add:
        return {"written": False, "added": [], "skipped": skipped, "path": str(p)}

    backup = p.with_name(".osiris.bak")
    backup.write_text(existing_text)
    new_lines = [f"{k} = {json.dumps(v)}" for k, v in sorted(to_add.items())]
    sep = "\n" if existing_text and not existing_text.endswith("\n") else ""
    p.write_text(existing_text + sep + "\n".join(new_lines) + "\n")
    return {"written": True, "added": sorted(to_add), "skipped": skipped,
            "path": str(p), "backup": str(backup)}


def correct_pin_value(path: str, key: str, value: str, *, reason: str) -> dict[str, Any]:
    """THE NAMED EXCEPTION TO write_pin_additions' ADDITIVE-ONLY LAW (ruling 719ed5b1/msg
    3929) — NOT a change to that function, NOT a bulk driver, and NOT interchangeable with
    it. write_pin_additions refuses to overwrite an existing key so that a disagreement
    between a declared pin and reality stays VISIBLE as `plan_pin_migration`'s own diagnosed
    `changes`, never silently resolved by a migration tool guessing at intent. This function
    exists for the opposite, narrower situation: a SPECIFIC, ALREADY-DIAGNOSED, individually
    authorized correction (task #152's khepri repair — a rename the graph itself already
    confirmed, decision 6602d39d/188df76a-class findings) where silence would be the actual
    dishonesty, not the cure. Every call is a deliberate, one-seat-at-a-time act with a
    `reason` that MUST land in the caller's own decision record — this function does not
    itself write to the graph, it only makes the correction auditable at the call site.

    Same backup discipline as write_pin_additions (`.osiris.bak` captures the exact pre-write
    bytes, overwritten each real write): a caller who mis-corrects can revert_pin_write same
    as any other write here. Refuses on invalid TOML, exactly like write_pin_additions, for
    the same reason (a bad file made worse is never a fix). Refuses if `key` is not already
    present — this function corrects an EXISTING declaration, it does not mint a new one;
    write_pin_additions is the tool for a genuinely missing key, and calling this for that
    case would blur the two verbs' distinct audit trails."""
    import tomllib

    p = Path(path) / ".osiris"
    existing_text = p.read_text() if p.is_file() else ""
    try:
        existing = tomllib.loads(existing_text) if existing_text else {}
    except (tomllib.TOMLDecodeError, ValueError) as exc:
        return {"error": f"{p} is not valid TOML ({type(exc).__name__}: {exc}) — refusing to "
                         "correct a broken file"}
    if key not in existing:
        return {"error": f"{key!r} is not declared in {p} — correct_pin_value only rewrites "
                         "an EXISTING key; use write_pin_additions to add a missing one"}
    old_value = existing[key]
    if old_value == value:
        return {"written": False, "old_value": old_value, "path": str(p)}
    if not reason.strip():
        return {"error": "a correction with no reason is exactly the silent overwrite "
                         "719ed5b1 rules against — refusing"}

    backup = p.with_name(".osiris.bak")
    backup.write_text(existing_text)
    lines = existing_text.splitlines(keepends=True)
    rewritten = False
    prefix = f"{key} ="
    for i, line in enumerate(lines):
        if line.split("=", 1)[0].strip() == key:
            eol = "\n" if line.endswith("\n") else ""
            lines[i] = f"{prefix} {json.dumps(value)}{eol}"
            rewritten = True
            break
    if not rewritten:  # defensive — tomllib parsed the key but the line-scan missed it
        return {"error": f"{key!r} parsed by tomllib but its line could not be located in "
                         f"{p} — refusing rather than guessing at the file's shape"}
    p.write_text("".join(lines))
    return {"written": True, "old_value": old_value, "new_value": value,
            "reason": reason, "path": str(p), "backup": str(backup)}


def revert_pin_write(path: str) -> dict[str, Any]:
    """The reversibility half of `write_pin_additions`'s constraint 3: restore `path/.osiris`
    from the backup it took immediately before its most recent real write. Refuses (an error
    dict, nothing touched) when no backup exists — never invents a prior state to revert to.
    An EMPTY backup means the file didn't exist before that write: revert DELETES the current
    file, restoring true absence, rather than leaving a stray empty `.osiris` behind."""
    p = Path(path) / ".osiris"
    backup = p.with_name(".osiris.bak")
    if not backup.is_file():
        return {"error": f"no backup at {backup} — nothing to revert to"}
    content = backup.read_text()
    if content:
        p.write_text(content)
    elif p.exists():
        p.unlink()
    return {"reverted": True, "path": str(p), "from_backup": str(backup)}


# THE PEER ADDENDUM (ruling d74492ee, spec e6636c7e — LEGIBILITY leg 2, seats.py): rendered
# INTO house_law.md's `{peer_block}` slot (boot_compiler.compile_managed_body) only when the
# seat carries an active peer_of edge at establish_office's OWN call time (never at
# mintseat.py's fresh-mint scaffold — a brand-new seat cannot yet have a peer to declare).
# The default "\n" reproduces the template's original single blank line between the charter
# section and "## How to work..." exactly (see the empty-vs-populated arithmetic in
# _peer_addendum's own docstring); a populated block adds its own leading/trailing blank
# lines so the surrounding sections never collide. THE BOOT COMPILER (thread 4951d818) closed
# the v1.1 gap this comment used to name here: reissue_office recomputes this live on an
# already-occupied seat, same as establish_office always has for a fresh one.
def _peer_addendum(peer_seat: str, peer_handle: str | None) -> str:
    """The `## Peer` section's full text, INCLUDING its own leading `\\n` (one blank line
    after the charter block) and trailing `\\n\\n` (one blank line before "## How to work").
    An unpeered seat never calls this — its caller passes the bare `"\\n"` default instead,
    which reproduces the template's ORIGINAL spacing (one literal `\\n` already sits before
    the `{peer_addendum}` slot in the template; this default's own single `\\n` supplies the
    second, together forming the one blank line the un-addended template always had)."""
    who = peer_handle or peer_seat
    return (
        "\n## Peer\n"
        f"You are peered with **{who}** (`{peer_seat}`) — a symmetric bond (ruling "
        "d74492ee, spec e6636c7e), not a chain of command. It carries real law:\n"
        "- **Two-tier decisions**: an ordinary act either of you takes ALONE binds the "
        "pair — tell your peer, don't wait for sign-off. Extraordinary acts (schema "
        "changes, external commitments, scope changes, spending) need BOTH your names.\n"
        "- **Domain split**: a co-owned PROJECT is never a co-owned TASK — every item has "
        "exactly one accountable peer.\n"
        "- **Mutual hold**: either of you may say HOLD on an irreversible act. Respect it; "
        "resolve within one exchange or escalate to the operator's desk.\n"
        "- **Fiduciary disclosure**: surface in-scope findings and risks to your peer "
        "proactively — silence is a violation.\n"
        "- **Review cadence**: exchange a structured status and review each other's work "
        "at every settle.\n"
        "- **The ledger**: keep small reciprocal obligations deliberately OPEN — a zeroed "
        "ledger is a dead bond.\n"
        "- **Anti-sycophancy**: never change a position without citing a reason not "
        "already on the table; two round-trips without convergence means "
        "decide-by-domain-owner or escalate.\n"
        f"send(to_agent='{who}') reaches them.\n\n"
    )


# THE CHARTER FILE (d80621a7 piece 3, alfred's alfred-seat-charter.md pattern graduating
# to convention): the seat's own LIVE-STATE scratchpad, distinct from CLAUDE.md's standing
# orders (identity, ritual — rarely rewritten) and distinct from the graph (typed, durable,
# but not where a mid-thought working note belongs). This is the OFFLOAD TARGET the stop-
# hook ritual (queue item 4) will enforce writing to above a context threshold — a session
# that dies mid-turn leaves its heir this file, not a blank page.
_CHARTER_TEMPLATE = """\
# {handle}'s charter

This file is **{handle}'s own live-state scratchpad** — write here AS YOU GO, not only at
a seam. It is the OFFLOAD TARGET when context runs high (the stop-hook ritual enforces
writing here before a quiet stop is allowed): a session that dies mid-turn leaves its heir
this file, never a blank page. Durable typed facts still belong in the graph
(`record_decision` / `open_thread` / `resolve_thread`) — this file is for the WHY and the
IN-PROGRESS state a typed object can't hold on its own.

## Current work
(what you're doing right now — the thread that would be lost if this session ended mid-turn)

## Key ids
(threads, decisions, commits worth a successor's first glance — short ids, one line each)

## Working notes
(anything else worth carrying forward that doesn't fit the graph's typed objects)
"""

# THE ONE UNDECLARED SENTINEL (task #157 piece 1, operator's own words "fix the slop"): the
# GRAPH declaration (a Seat's own `governs` edges, charter_of's own read) never had a single
# word for "nothing there yet" — mint_seat's receipt said nothing at all about it while this
# ceremony, one door over, already spoke plainly. Centralized so every caller that reports
# charter state says the SAME thing, not a copy that can drift the moment one side is edited.
_CHARTER_UNDECLARED = "UNDECLARED — the standing orders instruct the seat to declare"


async def _handle_of(pool: asyncpg.Pool, agent_id: str) -> str | None:
    """The lineage's claimed handle (freshest generation's assertion), or None — an office
    is NAMED for its seat, so an anonymous lineage has nothing to name one after."""
    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id "
        "WHERE a.name='handle' AND o.type='Agent' "
        "AND (o.canonical=$1 OR o.canonical LIKE $1 || '-%') "
        "ORDER BY a.observed_at DESC LIMIT 1", base)


async def _establish_pure_seat_office(
    actions: Actions, *, seat_id: str, actor: str | None,
    office_root: Path | None, projects_root: Path | None, claude_json: Path | None,
) -> dict[str, Any]:
    """The office ceremony for a seat with NO Agent lineage at all (thread 236d3940) — every
    fact comes off the Seat record alone, since there is no claimed occupant to resolve a
    handle/house/deed through. `seat_facts` already derives house the same way the
    agent-lineage path does (`derive_house`, ruling ff6148b0); the charter is the SEAT's own
    (ruling 1db1ff41 — `governs` is keyed on the seat, not any occupant), so it reads
    straight off `seat_id`, occupied or not; `file_office_deed` is skipped outright — it
    is Agent-only by construction (handshake.py), and there is nothing to deed until an
    actual agent launches and claims this seat."""
    from src.orchestrator.charter import charter_of
    from src.orchestrator.seats import peer_of_seat, seat_facts

    facts = await seat_facts(actions.pool, seat_id)
    handle = facts["handle"]
    if not handle:
        return {"error": f"{seat_id} has no handle on record — an office is named for its "
                         "seat's handle, and this one has none"}
    house = facts["house"]
    if not house:
        return {"error": f"{seat_id} has no derivable house — nothing to pin at an office"}
    root = office_root or _DEFAULT_OFFICE_ROOT
    office = root / handle.lower()
    office.mkdir(parents=True, exist_ok=True)
    seat_line = f" — durable identity `{seat_id}`."
    repos = await charter_of(actions.pool, seat_id)
    charter_block = (
        "You govern: " + ", ".join(f"`{r}`" for r in repos) + "." if repos else
        "Your charter was never formally declared — it lives only in prose. First act: "
        "`charter(repos=[...])` naming the repos you actually govern. A house is what a "
        "seat GOVERNS, not where it sits.")
    peer_addendum = "\n"
    peer_seat = await peer_of_seat(actions.pool, seat_id)
    if peer_seat is not None:
        peer_handle = await actions.pool.fetchval(
            "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
            "ON a.object_id=o.id AND a.name='handle' WHERE o.canonical=$1 "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", peer_seat)
        peer_addendum = _peer_addendum(peer_seat, peer_handle)
    orders = office / "CLAUDE.md"
    if orders.exists():
        orders_state = "left in place — the office already has standing orders"
    else:
        from src.orchestrator.boot_compiler import (
            compile_managed_body,
            template_version,
            wrap_managed,
        )

        body = await compile_managed_body(
            actions, seat_id=seat_id, handle=handle, house=house, office=str(office),
            seat_line=seat_line, charter_block=charter_block, peer_block=peer_addendum)
        orders.write_text(wrap_managed(body, template_version()))
        orders_state = "written"
    charter_file = office / "charter.md"
    if charter_file.exists():
        charter_file_state = "left in place — the seat's own live state, never overwritten"
    else:
        charter_file.write_text(_CHARTER_TEMPLATE.format(handle=handle))
        charter_file_state = "written"
    rebind = await rebind_seat(
        actions, seat_or_agent=seat_id, new_cwd=str(office), actor=actor,
        projects_root=projects_root, claude_json=claude_json, extract=True)
    return {
        "office": str(office), "handle": handle, "house": house,
        "office_deed": "n/a — no claimed occupant yet to deed an office to",
        "seat": seat_id,
        "charter": repos or _CHARTER_UNDECLARED,
        "standing_orders": orders_state,
        "charter_file": charter_file_state,
        "rebind": rebind,
        "launch": f"cd {office} && claude   (or claude --resume there)",
        "note": f"{handle}'s office stands at {office} — no agent has ever claimed this "
                "seat, so this ceremony ran off the Seat record alone; the first launch at "
                "this office is what claims it",
    }


async def establish_office(
    actions: Actions, *, seat_or_agent: str, actor: str | None = None,
    office_root: Path | None = None, projects_root: Path | None = None,
    claude_json: Path | None = None,
) -> dict[str, Any]:
    """The whole ceremony, one receipt: resolve the seat, write its standing orders
    (never clobbering — an occupied office's orders may be hand-tuned), then
    `rebind_seat(extract=True)` into ~/.osiris/seats/<handle>/. Refuses loudly on an
    unknown seat and on an ANONYMOUS lineage (claim_name first — an office is named for
    its seat). Idempotent: re-running converges on the same office.

    THE PURE SEAT PATH (thread 236d3940, mirroring rebind_seat's 3ae57d36 fix): a seat
    minted by mint_seat/ensure_seat but never claim_name'd by any agent (grantprobe's real
    shape) used to resolve to NO agent at all here — 'grantprobe' matches neither a claimed
    handle nor an Agent canonical — so this refused outright before an office could ever be
    built for the house's next never-yet-launched worker. An explicit SEAT identifier (its
    own canonical, or its `handle` property) is now checked directly whenever agent
    resolution supplies nothing, and a hit there builds the office off the Seat record
    alone: `seat_facts` supplies handle/house (house already DERIVED there), charter is read
    through the seat's occupant if it has ever had one (usually none), and `file_office_deed`
    is skipped entirely (it is Agent-only by construction — nothing to deed an office to
    until someone actually claims this seat by launching in it)."""
    from src.orchestrator.agents import house_of, resolve_handle
    from src.orchestrator.seats import held_seat

    seat_or_agent = (seat_or_agent or "").strip()
    agent_id = await resolve_handle(actions, seat_or_agent) if seat_or_agent else None
    if agent_id is None and seat_or_agent:
        exists = await actions.pool.fetchval(
            "SELECT 1 FROM objects WHERE canonical=$1 AND type='Agent' AND status='active'",
            seat_or_agent)
        agent_id = seat_or_agent if exists else None
    direct_seat_id: str | None = None
    if seat_or_agent:
        direct_seat_id = await actions.pool.fetchval(
            "SELECT o.canonical FROM objects o WHERE o.type='Seat' AND o.status='active' "
            "AND (o.canonical=$1 OR EXISTS (SELECT 1 FROM current_assertions a "
            "WHERE a.object_id=o.id AND a.name='handle' AND a.value #>> '{}' = $1))",
            seat_or_agent)
    if agent_id is None and direct_seat_id is None:
        return {"error": f"no such seat or agent: {seat_or_agent!r} — an office ceremony "
                         "never invents its occupant"}
    if agent_id is None:
        # direct_seat_id is guaranteed set here (the refusal above already ruled out both
        # being None) — mirrors rebind_seat's own PURE SEAT PATH assert exactly.
        assert direct_seat_id is not None
        return await _establish_pure_seat_office(
            actions, seat_id=direct_seat_id, actor=actor, office_root=office_root,
            projects_root=projects_root, claude_json=claude_json)
    handle = await _handle_of(actions.pool, agent_id)
    if not handle:
        return {"error": f"{agent_id} has never claimed a name — an office is named for its "
                         "seat. claim_name first, then establish the office"}
    house = await house_of(actions.pool, agent_id)
    if not house:
        return {"error": f"{agent_id} has no durable project label — it has never been "
                         "mounted in a project, so there is no house to pin at an office"}
    # A LIVE SEAT IS NEVER MOVED (the rollout guard): extraction relocates the lineage's
    # transcripts, and a running harness process appends to its own by path — moving it
    # mid-tab splits the session's history between two slugs. The ceremony waits for a
    # quiet seat (lineage-wide: a live heir blocks moving the base); close the tab,
    # establish, relaunch at the office.
    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    fresh = await actions.pool.fetchval(
        "SELECT max(last_seen) FROM agent_mounts "
        "WHERE (agent_id=$1 OR agent_id LIKE $1 || '-%') "
        "AND last_seen > now() - interval '15 minutes'", base)
    if fresh is not None:
        return {"error": f"{handle} ({agent_id}) is LIVE right now (last seen "
                         f"{fresh.isoformat()}) — moving a live seat splits its running "
                         "session's history between two homes. Close its tab first, "
                         "then establish; it wakes up in the office"}
    root = office_root or _DEFAULT_OFFICE_ROOT
    office = root / handle.lower()
    office.mkdir(parents=True, exist_ok=True)
    bound = await held_seat(actions.pool, agent_id)
    seat_line = (f" — durable identity `{bound['seat_id']}`." if bound else
                 " — not yet seated: your next claim binds you (the on-ramp).")
    # THE CHARTER IS THE SEAT'S (ruling 1db1ff41) — not the lineage's: reads through the
    # SAME `bound` this function already resolved for seat_line, one line up, no second
    # lookup and no lineage-string walk. An agent not yet seated has no charter to read.
    from src.orchestrator.charter import charter_of

    repos = await charter_of(actions.pool, bound["seat_id"]) if bound else []
    charter_block = (
        "You govern: " + ", ".join(f"`{r}`" for r in repos) + "." if repos else
        "Your charter was never formally declared — it lives only in prose. First act: "
        "`charter(repos=[...])` naming the repos you actually govern. A house is what a "
        "seat GOVERNS, not where it sits.")
    # THE PEER ADDENDUM (ruling d74492ee, spec e6636c7e): computed LIVE, like seat_line
    # and charter_block above it — a peer bonded after a fresh mint's scaffold still shows
    # up here, the one place that reads the graph instead of a fixed mint-time default.
    peer_addendum = "\n"
    if bound is not None:
        from src.orchestrator.seats import peer_of_seat

        peer_seat = await peer_of_seat(actions.pool, bound["seat_id"])
        if peer_seat is not None:
            peer_handle = await actions.pool.fetchval(
                "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
                "ON a.object_id=o.id AND a.name='handle' WHERE o.canonical=$1 "
                "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", peer_seat)
            peer_addendum = _peer_addendum(peer_seat, peer_handle)
    orders = office / "CLAUDE.md"
    if orders.exists():
        orders_state = "left in place — the office already has standing orders"
    else:
        from src.orchestrator.boot_compiler import (
            compile_managed_body,
            template_version,
            wrap_managed,
        )

        body = await compile_managed_body(
            actions, seat_id=bound["seat_id"] if bound else None, handle=handle,
            house=house, office=str(office), seat_line=seat_line,
            charter_block=charter_block, peer_block=peer_addendum)
        orders.write_text(wrap_managed(body, template_version()))
        orders_state = "written"
    # THE CHARTER FILE, never clobbered (d80621a7 piece 3): an occupied office's charter is
    # the seat's own hand-maintained live state — alfred's stays his, exactly like CLAUDE.md.
    charter_file = office / "charter.md"
    if charter_file.exists():
        charter_file_state = "left in place — the seat's own live state, never overwritten"
    else:
        charter_file.write_text(_CHARTER_TEMPLATE.format(handle=handle))
        charter_file_state = "written"
    rebind = await rebind_seat(
        actions, seat_or_agent=agent_id, new_cwd=str(office), actor=actor,
        projects_root=projects_root, claude_json=claude_json, extract=True)
    # THE DEED (a2d06410): the ceremony records office ownership in the GRAPH — the
    # fourth door must survive the seat's death, and mount rows don't (SessionEnd
    # releases them; Ra's ended lineage held none, so every fresh launch at his own
    # office minted a stranger). The deed is what office_seat reads first.
    from src.orchestrator.handshake import file_office_deed

    deeded = await file_office_deed(
        actions, agent_id=agent_id, cwd=str(office),
        actor=actor or "ceremony:establish-office", office_root=root)
    return {
        "office": str(office), "handle": handle, "house": house,
        "office_deed": "filed" if deeded else "already on the lineage's record",
        "seat": bound["seat_id"] if bound else None,
        "charter": repos or _CHARTER_UNDECLARED,
        "standing_orders": orders_state,
        "charter_file": charter_file_state,
        "rebind": rebind,
        "launch": f"cd {office} && claude   (or claude --resume there)",
        "note": f"{handle}'s office stands at {office} — the whisper will mount house "
                f"{house} from the pin; transcripts moved are re-addressed so resume "
                "works in place",
    }

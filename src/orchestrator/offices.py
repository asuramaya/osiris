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

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.orchestrator.mounts import rebind_seat

_OFFICE_ROOT_ENV = "OSIRIS_OFFICE_ROOT"


def _default_office_root() -> Path:
    """THE OFFICE-SCAFFOLDING ROOT, RE-READ ON EVERY CALL — never frozen at import time
    (Thoth's wave 9 lane, msg 6089): a bare module-level constant is captured once, by
    whichever call site imported it first, and stays that value for the life of the
    process — a test-only monkeypatch of one module's own attribute never reaches a
    SIBLING module that did `from offices import _DEFAULT_OFFICE_ROOT` at its own
    top level (mintseat.py, agents.py both did exactly this) and is holding its own
    separate, already-frozen copy of the same name.

    Reading `OSIRIS_OFFICE_ROOT` fresh here instead closes that gap for every caller in
    this tree, present or future, without each one remembering to thread `office_root`
    through by hand: tests/conftest.py sets this env var once, before any test runs, and
    every scaffold/sweep call in the whole process — no matter which module resolved it,
    no matter whether that call site passed its own explicit `office_root` (which still
    takes precedence; this is only the FALLBACK) — lands in the same throwaway directory
    instead of the real `~/.osiris/seats/` (the climintworker1/inferredworker1 shape,
    decision 5d97b750/f642a1e6: two real office directories scaffolded onto the real disk
    by unmocked test runs, the second one recreated DURING a live authorised deletion).

    Unset (the real launch path — cli.py, mcp_server.py, a live agent's own mount — never
    sets this) falls through to the real seats root, exactly as before; establish_office's
    production write is untouched."""
    override = os.environ.get(_OFFICE_ROOT_ENV)
    return Path(override) if override else Path.home() / ".osiris" / "seats"


async def seat_office_target(
    pool: asyncpg.Pool, seat_id: str, *, office_root: Path | None = None,
) -> str | None:
    """THE ANCHOR INVARIANT'S OWN ADDRESS (ruling 23771416, msg 6584): `<office_root>/
    <handle>` — DERIVED, never observed, never read off a row that might drift again.
    `heal_seat_anchor` (identity_heal.py) computes this to know what a seat's `anchor_cwd`
    SHOULD read; a resume materializer needing a target slug to emit into (thread d161a156,
    the operator's own "materialize, don't hunt" ruling) needs the identical derivation —
    one function, not two independently-typed copies that could disagree the way
    `anchor_cwd` and `tree_cwd` themselves once did.

    Returns None when the seat has no handle on record — nothing to derive an office path
    from; a caller decides for itself what "no target" means (heal_seat_anchor refuses,
    a materializer should refuse the same way rather than guess a slug)."""
    handle = await pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id="
        "  (SELECT id FROM objects WHERE canonical=$1 AND type='Seat' AND status='active') "
        "AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        seat_id)
    if not handle:
        return None
    root = office_root or _default_office_root()
    return str(root / handle.lower())


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
    return cwd is not None and Path(cwd) == _default_office_root()


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


def _pin_backup_path(p: Path) -> Path:
    """Where a `.osiris` pin's own backup belongs — NEVER inside a tracked git working tree
    (obligation 27ae4f89). A SEAT-OFFICE pin (~/.osiris/seats/<handle>, ruling ed5f5ce2)
    already sits outside version control — its backup stays beside it, unchanged, exactly
    the original behavior. A REPO-side pin lives INSIDE a git working tree, so a backup
    beside it is a tracked-file hazard: nothing stops a caller's own `git add -A` or
    `git commit -a` from picking up an untracked, unignored `.osiris.bak` sitting right next
    to a real `.osiris` pin. For that case the backup goes inside the repo's OWN `.git`
    metadata instead — the real directory for an ordinary repo root, or the worktree's own
    PRIVATE gitdir (resolved from the `gitdir: <path>` one-line gitlink file every worktree
    checkout carries in place of a real `.git` directory) for a worktree — never staged by
    any git command, because git never walks its own `.git` contents as working-tree files.
    `revert_pin_write` calls this SAME function to find what `write_pin_additions`/
    `correct_pin_value` actually wrote, so the undo keeps working regardless of which branch
    fired at write time."""
    git_path = p.parent / ".git"
    if git_path.is_dir():
        return git_path / "osiris-pin.bak"
    if git_path.is_file():
        line = git_path.read_text().strip()
        if line.startswith("gitdir:"):
            real = Path(line.split(":", 1)[1].strip())
            if real.is_dir():
                return real / "osiris-pin.bak"
    return p.with_name(".osiris.bak")


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
    `proposed`), `skipped` (keys already present, left untouched), `discarded` (the write-
    boundary honesty rule, decision beb046cfbdf9/42176e16: the subset of `skipped` whose
    ALREADY-PRESENT value actually DIFFERS from what `proposed` asked for — additive-only
    means this function will never resolve that disagreement itself, but it must say the
    disagreement exists rather than let `written: False` read identically to "already
    correct." Alfred's own scenario, obligation 71f637e8: rerun a pin migration, get
    byte-identical `written: False` across 31 files, wrongly conclude the fleet is
    normalized when some of those files were left holding the WRONG value on purpose,
    unannounced), and `backup` (only when a write happened)."""
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
    from src.orchestrator.capture import discarded_on_noop

    discarded = discarded_on_noop({k: proposed[k] for k in skipped}, existing)
    if not to_add:
        return {"written": False, "added": [], "skipped": skipped, "discarded": discarded,
                "path": str(p)}

    backup = _pin_backup_path(p)
    backup.parent.mkdir(parents=True, exist_ok=True)
    backup.write_text(existing_text)
    new_lines = [f"{k} = {json.dumps(v)}" for k, v in sorted(to_add.items())]
    sep = "\n" if existing_text and not existing_text.endswith("\n") else ""
    p.write_text(existing_text + sep + "\n".join(new_lines) + "\n")
    return {"written": True, "added": sorted(to_add), "skipped": skipped,
            "discarded": discarded,
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

    backup = _pin_backup_path(p)
    backup.parent.mkdir(parents=True, exist_ok=True)
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


async def correct_own_pin_value(
    pool: asyncpg.Pool, agent_id: str, key: str, value: str, *, reason: str,
    office_root: Path | None = None, workspace_root: Path | None = None,
) -> dict[str, Any]:
    """THE SELF-SCOPED DOOR onto `correct_pin_value` (msg 4761, obligation 114f7ac9): the raw
    function takes an arbitrary filesystem `path`, which is exactly the wrong shape for a
    seat-facing surface — an MCP caller has no path to hand it that isn't either a guess or a
    trust exercise. This composes `correct_pin_value` with `held_seat` (the SAME lookup
    `correct_house` uses) so a caller names only WHAT to correct, never WHERE: the seat's own
    office, resolved off its `handle`, exactly like `establish_office`/`write_model_pin`
    already do (`_default_office_root() / handle.lower()`) — never `identity.cwd`, which can be
    a shared root other seats climb to (#146's lesson), and never a directory-basename guess
    (13af22fc's phantom `repo:seats` defect came from exactly that shape).

    Refuses on a caller holding no seat — a pin correction is a seat's own act, never
    performed on another's behalf and never inferred. `reason` stays required and non-empty;
    enforced by `correct_pin_value` itself, unchanged here. `office_root` exists only as a
    test seam, same convention as `establish_office`.

    THE SECOND COPY (ruling b30e2b38, the Jesus/Godel live specimen): `rebind_seat` writes
    its own courtesy `.osiris` at the seat's ANCHOR path — a second, independent pin copy
    this function used to never reach, so a fully-correct transition (fold + rebind +
    THIS call) still left the anchor copy reading the pre-transition project forever,
    with no sanctioned door onto it at all. Still self-scoped, never a caller-supplied
    path: the anchor is read fresh off the SAME held seat's own `anchor_cwd` property —
    the seat's other self-owned location, not an arbitrary one. Corrected WHEN IT EXISTS,
    DECLARES `key` ALREADY, AND DIFFERS FROM THE OFFICE PATH; skipped silently (never an
    error) when it's the same directory as the office (no second copy to diverge) or has
    no `.osiris` of its own yet. Reported separately under `anchor` so a caller can see
    whether the second copy was touched, left alone, or doesn't apply — never folded into
    the office result, which could otherwise mask a partial correction as a full one.

    THE THIRD COPY (thread 6483/6504, Thoth's own scoping and ruling: the workspace is
    real, deliberate infrastructure, not an undeclared scratch dir — decision 87457dc1,
    the operator's own correction that jesus/chad are real seats mid-arc, not accidents):
    `mint_seat`/`found_seat` scaffold a WORKSPACE alongside the office, its own directory,
    its own `.osiris` pin (`sweep_seat_workspace`'s own docstring names the convention,
    `Path.home() / "code" / handle.lower()`, `path=` overridden at mint time) — a location
    this function still never reached even after the anchor extension above, live-verified
    still stale at the moment this was written. SAME PATTERN, a third time, not a new
    design: self-scoped (the convention path, never caller-supplied — `workspace_root` is
    a test seam only, matching `sweep_seat_workspace`'s own), corrected ONLY when it
    exists, already declares `key`, and differs from BOTH the office AND the (possibly
    already-corrected) anchor path — never a double-write when a seat's anchor happens to
    equal its workspace default. Reported under `workspace`, same shape as `anchor`. Best-
    effort like `sweep_seat_workspace`'s own default: a seat minted with an explicit
    custom `path=` is not covered by this guess, the same accepted gap that verb's own
    docstring names — the existence+declares-key+differs guard means a wrong guess here
    writes nothing, it simply finds no matching file to correct."""
    from src.orchestrator.seats import held_seat

    bound = await held_seat(pool, agent_id)
    if bound is None:
        return {"error": f"{agent_id} holds no seat — correcting a pin is a seat's own act, "
                         "never done on another's behalf"}
    handle = bound["handle"].lower()
    root = office_root or _default_office_root()
    office = root / handle
    result = correct_pin_value(str(office), key, value, reason=reason)
    if not result.get("error"):
        result["seat_id"] = bound["seat_id"]
    touched = {_resolved(office)}
    anchor_cwd = await pool.fetchval(
        "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
        "ON a.object_id=o.id AND a.name='anchor_cwd' WHERE o.canonical=$1 "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", bound["seat_id"])
    if anchor_cwd and _resolved(Path(anchor_cwd)) not in touched and \
            (Path(anchor_cwd) / ".osiris").is_file():
        anchor_result = correct_pin_value(anchor_cwd, key, value, reason=reason)
        result["anchor"] = anchor_result if anchor_result.get("error") else {
            "path": anchor_result["path"], "corrected": True}
        touched.add(_resolved(Path(anchor_cwd)))
    workspace = (workspace_root or (Path.home() / "code")) / handle
    if _resolved(workspace) not in touched and (workspace / ".osiris").is_file():
        workspace_result = correct_pin_value(str(workspace), key, value, reason=reason)
        result["workspace"] = workspace_result if workspace_result.get("error") else {
            "path": workspace_result["path"], "corrected": True}
    return result


def _resolved(p: Path) -> Path:
    """Sync helper (ASYNC240, this codebase's own ruff gate) — `Path.resolve()` stays out
    of correct_own_pin_value/revert_own_pin_write's own async bodies."""
    return p.resolve()


def revert_pin_write(path: str) -> dict[str, Any]:
    """The reversibility half of `write_pin_additions`'s constraint 3: restore `path/.osiris`
    from the backup it took immediately before its most recent real write. Refuses (an error
    dict, nothing touched) when no backup exists — never invents a prior state to revert to.
    An EMPTY backup means the file didn't exist before that write: revert DELETES the current
    file, restoring true absence, rather than leaving a stray empty `.osiris` behind."""
    p = Path(path) / ".osiris"
    backup = _pin_backup_path(p)
    if not backup.is_file():
        return {"error": f"no backup at {backup} — nothing to revert to"}
    content = backup.read_text()
    if content:
        p.write_text(content)
    elif p.exists():
        p.unlink()
    return {"reverted": True, "path": str(p), "from_backup": str(backup)}


async def revert_own_pin_write(
    pool: asyncpg.Pool, agent_id: str, *, office_root: Path | None = None,
    workspace_root: Path | None = None,
) -> dict[str, Any]:
    """THE SELF-SCOPED DOOR onto `revert_pin_write` (ruling b30e2b38): built the same day
    its absence was found live — a seat that follows the rules into a bad pin state had
    no sanctioned way back out. `revert_pin_write` (above) has existed, tested, since
    write_pin_additions's own constraint 3; it simply had no MCP surface a seat could
    reach on its own behalf, the same unreached-not-unbuilt shape this house kept hitting
    tonight. Composes `held_seat`, identical resolution to `correct_own_pin_value` — a
    caller names nothing but its own act, never a path.

    ALL THREE COPIES, SYMMETRIC WITH `correct_own_pin_value`'s OWN EXTENSION (thread
    6483/6504's workspace-copy addendum included — a write with no matching undo would
    break the "same backup discipline" promise that function's own docstring makes):
    reverts the office first, then the seat's own current `anchor_cwd` copy, then the
    `~/code/<handle>` workspace convention — each WHEN a backup exists there too
    (silently skipped, never an error, when there isn't one to revert: that copy may
    never have been corrected at all, or may be the same directory as one already
    reverted). Each copy's own receipt lands separately (`office`/`anchor`/`workspace`)
    so a caller can see exactly which copies actually moved."""
    from src.orchestrator.seats import held_seat

    bound = await held_seat(pool, agent_id)
    if bound is None:
        return {"error": f"{agent_id} holds no seat — reverting a pin is a seat's own act, "
                         "never done on another's behalf"}
    handle = bound["handle"].lower()
    root = office_root or _default_office_root()
    office = root / handle
    office_result = revert_pin_write(str(office))
    out: dict[str, Any] = {"seat_id": bound["seat_id"], "office": office_result}
    touched = {_resolved(office)}
    anchor_cwd = await pool.fetchval(
        "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
        "ON a.object_id=o.id AND a.name='anchor_cwd' WHERE o.canonical=$1 "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", bound["seat_id"])
    if anchor_cwd and _resolved(Path(anchor_cwd)) not in touched and \
            _pin_backup_path(Path(anchor_cwd) / ".osiris").is_file():
        out["anchor"] = revert_pin_write(anchor_cwd)
        touched.add(_resolved(Path(anchor_cwd)))
    workspace = (workspace_root or (Path.home() / "code")) / handle
    if _resolved(workspace) not in touched and \
            _pin_backup_path(workspace / ".osiris").is_file():
        out["workspace"] = revert_pin_write(str(workspace))
    if office_result.get("error"):
        out["error"] = office_result["error"]
    return out


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
async def self_heal_project_pin(pool: asyncpg.Pool, agent_id: str, cwd: str) -> dict[str, Any]:
    """MECHANISM (1) of ruling fe8ec7ff — the operator's own standard (decision df646654):
    "give each agent independence and infrastructure to fix their own problems... patch
    osiris so the problems don't even happen in the first place." No human blesses a value
    the graph already holds unambiguously; nobody escalates for the ordinary case.

    Called at mount, before any pin banner renders. A no-op (`{"state": "n/a"}`) unless the
    pin is genuinely unset (no `.osiris`, or one that never declares `project`) AND readable
    (a broken file or a missing cwd are `project_pin_banner`'s real errors, untouched here).

    THE RULE, verbatim: write `project` into the seat's OWN pin only when THREE independent
    graph signals — `governs` (this seat's own charter), `works_in` (this agent's own active
    project link), and the anchor directory's basename resolving to a real, active
    SoftwareProject — ALL exist and ALL agree on the same one project. Any signal absent, or
    any two disagreeing, and the write is refused: `{"state": "unset", "reason": "..."}`
    names exactly which signals fired and which didn't, so unset stays a valid, auditable
    state rather than a silent gap. A caller with no held seat gets the same honest refusal
    — self-healing is a seat's own act, same law `correct_own_pin_value` already keeps.

    The write itself goes through `write_pin_additions` unchanged (additive-only, backup-
    first, idempotent) — this function only ever supplies ITS OWN candidate value to that
    door, never bypasses it. `revert_pin_write` undoes it exactly as it would any other
    write there."""
    from src.orchestrator.agents import read_project_pin
    from src.orchestrator.charter import charter_of
    from src.orchestrator.seats import held_seat

    pin_read = read_project_pin(cwd)
    if pin_read.error or pin_read.cwd_missing or pin_read.value is not None:
        return {"state": "n/a"}  # a real error, or already declared — not this mechanism's case

    bound = await held_seat(pool, agent_id)
    if bound is None:
        return {"state": "unset", "reason": "no seat held — self-healing is a seat's own "
                                            "act; nothing to reconcile against"}
    seat_id = bound["seat_id"]

    governs = await charter_of(pool, seat_id)
    governs_vote = governs[0] if len(governs) == 1 else None

    works_in_rows = await pool.fetch(
        "SELECT DISTINCT p.canonical FROM links l "
        "JOIN objects a ON a.id=l.from_id AND a.type='Agent' AND a.canonical=$1 "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
        "WHERE l.type='works_in' AND (l.valid_until IS NULL OR l.valid_until > now())",
        agent_id)
    works_in_names = {r["canonical"].removeprefix("repo:") for r in works_in_rows}
    works_in_vote = next(iter(works_in_names)) if len(works_in_names) == 1 else None

    basename = Path(cwd).name
    basename_is_real_project = await pool.fetchval(
        "SELECT 1 FROM objects WHERE type='SoftwareProject' AND canonical=$1 "
        "AND status='active'", f"repo:{basename}")
    anchor_vote = basename if basename_is_real_project else None

    votes = {governs_vote, works_in_vote, anchor_vote}
    if len(votes) != 1 or None in votes:
        return {"state": "unset", "reason": (
            f"governs={governs_vote!r} works_in={works_in_vote!r} "
            f"anchor_cwd={anchor_vote!r} — not all three agree; leaving unset, valid")}

    assert governs_vote is not None  # narrowed by the `None in votes` check above
    candidate = governs_vote  # == works_in_vote == anchor_vote, asserted above
    write = write_pin_additions(cwd, {"project": candidate})
    if write.get("error") or not write.get("written"):
        return {"state": "unset", "reason": f"self-heal attempted but did not write: {write}"}
    return {"state": "self-healed", "project": candidate,
            "evidence": "governs + works_in + anchor_cwd all agree", "write": write}


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
#
# THE VERB, NAMED IN THE SENTINEL ITSELF (operator's self-chartering ruling, "each agent
# should be able to own it and handle it on their own" — msg 4378): the ORIGINAL text sent a
# reader back to "the standing orders" (CLAUDE.md, read once at mint/boot) instead of naming
# the call on THE ONE SURFACE a seat actually re-reads every session, orient()'s own live
# payload (mcp_server.py, 26 of 33 active seats measured reading `charter` absent from their
# own orient() — task #157's own live count). An office whose CLAUDE.md was "left in place"
# (compile_managed_body's own branch — an existing office's standing orders are never
# recompiled) can go an entire reign without that file crossing a session's eyes again; a
# sentinel that only points at it, rather than carrying the verb itself, is a dead end for
# exactly the seat it's meant to move. Plain text, no harness assumed — `charter(repos=[...])`
# is the same MCP tool name on every surface, Claude Code or otherwise.
_CHARTER_UNDECLARED = ("UNDECLARED — call charter(repos=[...]) naming the repos you govern "
                       "(the standing orders say more, but this is the one line every "
                       "session sees)")


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
    root = office_root or _default_office_root()
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
        projects_root=projects_root, claude_json=claude_json, extract=True,
        office_root=root)
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
    agents_json: Any = None, read_exe: Any = None, read_cwd: Any = None,
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
    #
    # A NINTH SPECIMEN OF THE SAME ATLAS SHAPE, found live while fixing door census item 4
    # (Thoth msg 5772/5741, thread 2c3c2b9a): a fresh/refreshing agent_mounts row alone
    # used to be enough to refuse this whole ceremony — even with no harness-confirmed
    # body behind it. This door was not in the original count; found because it was
    # masking doors.py's own `_record` fix inside lift()'s own call chain (establish_office
    # runs its own, separate liveness check AFTER lift()'s pre-claim check already passed).
    # establish_office is a rare, deliberate ceremony (never a hot per-mount path), so the
    # same registry_census cross-check is affordable here too.
    from src.orchestrator.agents import _generation
    from src.orchestrator.mounts import registry_census

    base = _generation(agent_id)[0]
    fresh_rows = await actions.pool.fetch(
        "SELECT agent_id, last_seen FROM agent_mounts "
        "WHERE (agent_id=$1 OR agent_id LIKE $1 || '-%') "
        "AND last_seen > now() - interval '15 minutes' "
        "ORDER BY last_seen DESC", base)
    confirmed_fresh = None
    if fresh_rows:
        census = await registry_census(
            actions.pool, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
        matched_ids = {m.get("agent_id") for m in census.get("matched", [])}
        confirmed_fresh = next(
            (r for r in fresh_rows if str(r["agent_id"]) in matched_ids), None)
    if confirmed_fresh is not None:
        return {"error": f"{handle} ({agent_id}) is LIVE right now (last seen "
                         f"{confirmed_fresh['last_seen'].isoformat()}) — moving a live "
                         "seat splits its running session's history between two homes. "
                         "Close its tab first, then establish; it wakes up in the office"}
    root = office_root or _default_office_root()
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
        projects_root=projects_root, claude_json=claude_json, extract=True,
        office_root=root)
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


_SWEEP_HEAL_WAIT_SECS = 90  # wave6probe's own measured respawn window (~1 min, decision
# 4ca39589/ruling 457d5e96) plus margin — the interval a supervised harness daemon needs to
# silently re-resume a killed session onto a fresh pid; a clean read taken before this has
# elapsed is not evidence of nothing, it is evidence of "not yet"


async def _live_body_at_office(
    pool: asyncpg.Pool, office: Path, *,
    agents_json: Any = None, read_exe: Any = None, read_cwd: Any = None,
) -> dict[str, Any] | None:
    """One registry_census read, matched against `office` by EITHER cwd the census carries
    (harness-reported `harness_cwd` or /proc-confirmed `proc_cwd` — a body can disagree with
    itself mid-move, so both are checked) over `verified` (every /proc-confirmed live body,
    matched-to-a-graph-row or not — `rowless` bodies are exactly the population house law
    #178 warns never to treat as absent just because agent_mounts missed them). A blind
    census (`blind: true`, the harness read itself failed) is NEVER read as "nothing live" —
    it refuses the same as a real hit, one instant's silence is not proof of an empty room."""
    from src.orchestrator.mounts import registry_census

    census = await registry_census(
        pool, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
    if census.get("blind"):
        return {"blind": True}
    office_str = str(office)
    for body in census.get("verified", []):
        body_dict: dict[str, Any] = body
        for key in ("harness_cwd", "proc_cwd"):
            cwd = body_dict.get(key)
            if cwd and (cwd == office_str or str(cwd).startswith(office_str + "/")):
                return body_dict
    return None


async def sweep_retired_office(
    pool: asyncpg.Pool, *, handle: str, dry_run: bool = True, because: str | None = None,
    office_root: Path | None = None, sleep: Any = None,
    agents_json: Any = None, read_exe: Any = None, read_cwd: Any = None,
) -> dict[str, Any]:
    """THE MISSING DISK HALF of seat cleanup (Thoth's msg 6026/6035 lane, wave6probe's own
    finding): retire_seat/vacate_holder are graph-only by design — neither touches the
    office directory establish_office scaffolded, so a retired seat's `~/.osiris/seats/
    <handle>/` sits on disk forever, a complete-looking office belonging to nobody. This is
    the deliberately SEPARATE verb that closes that gap — never folded into retire_seat
    (every existing caller relies on its graph-only contract, and a seat can legitimately
    be retired while its files are kept for archival/audit) or vacate_holder (that releases
    a STILL-REUSABLE seat; deleting the office under something that may be relaunched into
    the same directory would be actively wrong).

    REFUSES, per directory, rather than guessing, on:
    - no office directory at the resolved path — nothing to sweep;
    - more than one Seat object shares this handle (any status) — ambiguous, never guesses
      which one owns this directory;
    - a matching Seat exists and is NOT retired (active/unknown status) — this verb only
      ever touches a graph-retired seat's office, or an office with NO Seat row at all
      (the climintworker1/inferredworker1 shape: pure test-run filesystem debris, never a
      real seat, confirmed independently across 4+ prior generations);
    - a matching Seat carries an active `holds` link despite its retired status — a shape
      that should never exist and is not this verb's business to untangle;
    - a live body's cwd resolves inside the office RIGHT NOW, per registry_census;
    - a live body's cwd resolves inside the office after waiting `_SWEEP_HEAL_WAIT_SECS` —
      wave6probe's own lesson (decision 4ca39589/ruling 457d5e96): a supervised harness
      daemon can silently re-resume a killed session onto a fresh pid within about a
      minute, so a single instant's clean read is not proof of an empty office. THE GUARD
      IS TWO READS, NEVER ONE, exactly the discipline that build demanded.

    `dry_run=True` (the default) reports `would-delete` with every entry under the office,
    never removes anything. `dry_run=False` is the OPERATOR'S OWN CALL (his words, msg
    6049: "all holds on me approved, take care of them") — requires `because`, runs the
    EXACT SAME per-directory guard (no re-derivation, no separate execute-only code path
    that could drift from what the dry-run actually checked), and on a clean pass removes
    the office with `shutil.rmtree` before returning `status: "deleted"` with the entries
    that were actually removed. Every refusal above applies identically in execute mode —
    a directory that refuses is left untouched, exactly as it would be under dry-run,
    which is what makes the two modes trustworthy: dry-run predicts precisely what execute
    does, never an approximation of it."""
    if not dry_run and not (because or "").strip():
        return {"error": "because is required to execute — a filesystem delete is not "
                         "self-justifying the way a dry-run report is"}
    handle = (handle or "").strip()
    if not handle:
        return {"error": "a handle is required"}
    root = office_root or _default_office_root()
    office = root / handle.lower()
    # CONTAINMENT, AND IT IS THE ONE GUARD THIS VERB WAS MISSING (Thoth LXXXIX, wave 8
    # merge review). Every other refusal below interrogates THE SEAT — is it retired, is
    # it ambiguous, does it hold, is a body live in it. Not one of them interrogates THE
    # PATH, and `handle` is a caller-supplied string that goes straight into a `/` join:
    # handle='../../code/osiris/docs' resolves clean out of the office root, matches no
    # Seat row, carries no holder, has no live body inside it — so it sails past all five
    # seat guards and reaches shutil.rmtree with a real source directory in hand. Verified
    # by hand before this line existed. It was harmless while dry_run was the only wired
    # mode and became an arbitrary-directory delete the instant the execute path landed:
    # A GUARD THAT CHECKS THE SUBJECT IS NOT A GUARD ON THE OBJECT.
    #
    # The invariant is the one establish_office itself scaffolds — an office is a DIRECT
    # CHILD of the office root, never a descendant, never a sibling reached by traversal.
    # Compared after resolve() on both sides so symlinks and .. are collapsed first.
    #
    # The three other `root / handle.lower()` sites in this module (correct_office_pin,
    # establish_office_for_seat, establish_office) are NOT patched here and that is
    # deliberate, not an oversight: each takes its handle from the GRAPH (held_seat /
    # seat_facts), never from a caller argument, and each mkdirs rather than deletes. If a
    # handle ever becomes caller-supplied at one of those, it needs this same check.
    resolved_root = root.resolve()
    if office.resolve().parent != resolved_root:
        return {"error": f"handle {handle!r} does not name an office directly under "
                         f"{resolved_root} — it resolves to {office.resolve()}, outside "
                         "the office root. Refusing: this verb deletes, and a handle is "
                         "a seat's name, never a path"}
    if not office.is_dir():
        return {"error": f"no office directory at {office} — nothing to sweep"}

    rows = await pool.fetch(
        "SELECT o.id, o.canonical, o.status FROM objects o WHERE o.type='Seat' "
        "AND lower(COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='handle' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '')) = lower($1) "
        "ORDER BY o.created_at", handle)
    if len(rows) > 1:
        return {"error": f"{len(rows)} Seat objects (any status) match handle {handle!r} — "
                         "ambiguous, refuses rather than guessing which one owns this office",
                "office": str(office), "seats": [r["canonical"] for r in rows]}
    seat_id = rows[0]["canonical"] if rows else None
    seat_status = rows[0]["status"] if rows else None
    if seat_id is not None and seat_status != "retired":
        return {"error": f"{handle} is a graph Seat ({seat_id}) with status={seat_status!r} "
                         "— sweep_retired_office only touches a RETIRED seat's office, or "
                         "an office with no matching Seat row at all",
                "office": str(office), "seat": seat_id, "seat_status": seat_status}
    if seat_id is not None:
        holder = await pool.fetchval(
            "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
            "WHERE l.to_id=$1 AND l.type='holds' "
            "AND (l.valid_until IS NULL OR l.valid_until > now()) LIMIT 1", rows[0]["id"])
        if holder:
            return {"error": f"{handle} is retired but carries an active holder ({holder}) "
                             "— a shape that should never exist; refusing rather than "
                             "resolving it silently",
                    "office": str(office), "seat": seat_id}

    first = await _live_body_at_office(
        pool, office, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
    if first is not None:
        detail = ("the harness registry read itself failed (blind census) — never read as "
                  "'nothing live'" if first.get("blind") else
                  f"a live body's cwd resolves inside this office right now (pid "
                  f"{first.get('pid')})")
        return {"status": "refused-live-body", "office": str(office), "seat": seat_id,
                "detail": detail}
    _sleep = sleep or asyncio.sleep
    await _sleep(_SWEEP_HEAL_WAIT_SECS)
    second = await _live_body_at_office(
        pool, office, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
    if second is not None:
        detail = ("the harness registry read itself failed on the heal-interval re-check "
                  "(blind census) — never read as 'nothing live'" if second.get("blind") else
                  f"clean at the first read, but a live body appeared after the "
                  f"{_SWEEP_HEAL_WAIT_SECS}s heal-interval wait (pid {second.get('pid')}) — "
                  "exactly the daemon-respawn race wave6probe reproduced")
        return {"status": "refused-live-body-after-heal-wait", "office": str(office),
                "seat": seat_id, "detail": detail}

    entries = sorted(str(p.relative_to(office)) for p in office.rglob("*"))
    if dry_run:
        return {"status": "would-delete", "dry_run": True, "office": str(office),
                "seat": seat_id, "seat_status": seat_status, "entry_count": len(entries),
                "entries": entries, **({"because": because.strip()} if because else {})}
    import shutil
    shutil.rmtree(office)
    return {"status": "deleted", "dry_run": False, "office": str(office),
            "seat": seat_id, "seat_status": seat_status, "entry_count": len(entries),
            "entries": entries, "because": (because or "").strip()}


async def sweep_seat_workspace(
    pool: asyncpg.Pool, *, handle: str, dry_run: bool = True, because: str | None = None,
    workspace_root: Path | None = None, sleep: Any = None,
    agents_json: Any = None, read_exe: Any = None, read_cwd: Any = None,
) -> dict[str, Any]:
    """THE WORKSPACE HALF sweep_retired_office never covered (thread 6272, Thoth's own
    lane, the operator's "jesus manages the godel project, chad manages the cdking"
    correction that killed the accident premise this pair was first scoped under):
    mint_seat/found_seat scaffold TWO directories per seat, the office
    (`~/.osiris/seats/<handle>/`, an `.osiris` pin) AND the workspace (`~/code/<handle>/`
    by convention, `path=` overridden at mint time, its OWN `.osiris` pin) — sweep_
    retired_office only ever reached the first. A retired seat's workspace sits on disk
    forever exactly the way its office used to, and needed the identical guard shape, not
    a generalization of the office function (deliberately a SEPARATE function, same
    reasoning sweep_retired_office's own docstring gives for staying separate from
    retire_seat/vacate_holder: each caller relies on a narrow, specific contract).

    `workspace_root` defaults to `Path.home() / "code"`, the documented mint-time default
    (mintseat.py's own `workspace = Path.home() / "code" / handle.lower()` when no `path=`
    was given) — the SAME best-effort convention sweep_retired_office's own `office_root`
    resolution already leans on for the office half; a seat minted with an explicit custom
    `path=` needs a caller-supplied `workspace_root` naming that real parent directory
    instead, exactly as a test override does for the office side.

    EVERY GUARD IS THE OFFICE FUNCTION'S OWN, reapplied to the workspace path, not
    reinvented: the containment check (`workspace.resolve().parent == workspace_root.
    resolve()` — a handle is a seat's name, never a path, so `../../` never sails past the
    other five seat guards the way it did before sweep_retired_office's own containment fix
    landed), the ambiguous-Seat refusal, the retired-or-no-Seat-row gate, the active-holder
    refusal even on a nominally-retired seat, and the DOUBLE live-body check (immediate
    plus a `_SWEEP_HEAL_WAIT_SECS` heal-wait re-check, wave6probe's own measured daemon-
    respawn race) before `shutil.rmtree` is ever reached. `dry_run=True` (default) reports
    `would-delete`; `dry_run=False` is operator-gated (`because` required), same law every
    repair verb here follows."""
    if not dry_run and not (because or "").strip():
        return {"error": "because is required to execute — a filesystem delete is not "
                         "self-justifying the way a dry-run report is"}
    handle = (handle or "").strip()
    if not handle:
        return {"error": "a handle is required"}
    root = workspace_root or (Path.home() / "code")
    workspace = root / handle.lower()
    resolved_root = root.resolve()
    if workspace.resolve().parent != resolved_root:
        return {"error": f"handle {handle!r} does not name a workspace directly under "
                         f"{resolved_root} — it resolves to {workspace.resolve()}, outside "
                         "the workspace root. Refusing: this verb deletes, and a handle is "
                         "a seat's name, never a path"}
    if not workspace.is_dir():
        return {"error": f"no workspace directory at {workspace} — nothing to sweep"}

    rows = await pool.fetch(
        "SELECT o.id, o.canonical, o.status FROM objects o WHERE o.type='Seat' "
        "AND lower(COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='handle' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '')) = lower($1) "
        "ORDER BY o.created_at", handle)
    if len(rows) > 1:
        return {"error": f"{len(rows)} Seat objects (any status) match handle {handle!r} — "
                         "ambiguous, refuses rather than guessing which one owns this "
                         "workspace",
                "workspace": str(workspace), "seats": [r["canonical"] for r in rows]}
    seat_id = rows[0]["canonical"] if rows else None
    seat_status = rows[0]["status"] if rows else None
    if seat_id is not None and seat_status != "retired":
        return {"error": f"{handle} is a graph Seat ({seat_id}) with status={seat_status!r} "
                         "— sweep_seat_workspace only touches a RETIRED seat's workspace, "
                         "or a workspace with no matching Seat row at all",
                "workspace": str(workspace), "seat": seat_id, "seat_status": seat_status}
    if seat_id is not None:
        holder = await pool.fetchval(
            "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
            "WHERE l.to_id=$1 AND l.type='holds' "
            "AND (l.valid_until IS NULL OR l.valid_until > now()) LIMIT 1", rows[0]["id"])
        if holder:
            return {"error": f"{handle} is retired but carries an active holder ({holder}) "
                             "— a shape that should never exist; refusing rather than "
                             "resolving it silently",
                    "workspace": str(workspace), "seat": seat_id}

    first = await _live_body_at_office(
        pool, workspace, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
    if first is not None:
        detail = ("the harness registry read itself failed (blind census) — never read as "
                  "'nothing live'" if first.get("blind") else
                  f"a live body's cwd resolves inside this workspace right now (pid "
                  f"{first.get('pid')})")
        return {"status": "refused-live-body", "workspace": str(workspace), "seat": seat_id,
                "detail": detail}
    _sleep = sleep or asyncio.sleep
    await _sleep(_SWEEP_HEAL_WAIT_SECS)
    second = await _live_body_at_office(
        pool, workspace, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
    if second is not None:
        detail = ("the harness registry read itself failed on the heal-interval re-check "
                  "(blind census) — never read as 'nothing live'" if second.get("blind") else
                  f"clean at the first read, but a live body appeared after the "
                  f"{_SWEEP_HEAL_WAIT_SECS}s heal-interval wait (pid {second.get('pid')}) — "
                  "exactly the daemon-respawn race wave6probe reproduced")
        return {"status": "refused-live-body-after-heal-wait", "workspace": str(workspace),
                "seat": seat_id, "detail": detail}

    entries = sorted(str(p.relative_to(workspace)) for p in workspace.rglob("*"))
    if dry_run:
        return {"status": "would-delete", "dry_run": True, "workspace": str(workspace),
                "seat": seat_id, "seat_status": seat_status, "entry_count": len(entries),
                "entries": entries, **({"because": because.strip()} if because else {})}
    import shutil
    shutil.rmtree(workspace)
    return {"status": "deleted", "dry_run": False, "workspace": str(workspace),
            "seat": seat_id, "seat_status": seat_status, "entry_count": len(entries),
            "entries": entries, "because": (because or "").strip()}

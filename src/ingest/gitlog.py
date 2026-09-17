"""Git-history ingest — Osiris tracking its own genesis (and any repository).

Proof that the engine is a GENERAL substrate, not OSINT-only: a git history is just
another structured source. The same collector pattern every federator uses — a source →
graded objects/links through the Actions waist — maps a repo's commits, developers, and
the history DAG into the entity graph. So Osiris can model its own development, the first
commit onward. The git log is the authoritative record (facts land AUTHORITATIVE_API),
and the commit date is the observed-at clock (time-travel the graph by commit).

  SoftwareProject ──in_repo── Commit ──authored_by── Person(dev)
                              Commit ──follows──────► parent Commit
                              Commit ──committed_by── Agent (the live generation that ran
                                                       it, additional to authored_by)

Run: `python -m src.ingest.gitlog [path] [limit]`.
"""

from __future__ import annotations

import re
import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SOURCE = "git"
_EC = EvidenceClass.AUTHORITATIVE_API.value
_CONF = confidence_for(EvidenceClass.AUTHORITATIVE_API)
# unit-separator delimited fields, record-separated — robust against newlines in subjects.
# %b (body) carries the rationale: each commit message IS a decision record, so the body is
# project memory, not noise.
_FMT = "%H%x1f%an%x1f%ae%x1f%aI%x1f%P%x1f%s%x1f%b%x1e"

# Conventional Commits: `type(scope)!: summary`. The type+scope make a commit groupable
# ("what changed in the composer?"), so the changelog is a query, not a string-scan.
_CONVENTIONAL = re.compile(r"^(?P<type>[a-z]+)(?:\((?P<scope>[^)]+)\))?!?:\s*(?P<summary>.+)$")


@dataclass
class Commit:
    sha: str
    author_name: str
    author_email: str
    date: str  # ISO 8601 with offset
    parents: list[str] = field(default_factory=list)
    subject: str = ""
    body: str = ""


# Machine trailers git appends to a body — provenance, NOT rationale. The body is memory
# (decision-mining, recall, and cross-repo derivation all read the `rationale` property), and
# these lines poison it: the Co-Authored-By / *-Session trailers made "claude" / "anthropic" /
# "noreply" the top cross-repo "concern" in all seven repos. Allow-listed keys only — a plain
# "Note:" or "Fixes:" body line is never treated as a trailer.
_TRAILER = re.compile(
    r"^\s*(?:co-authored-by|signed-off-by|[\w-]*-session)\s*:|^\s*🤖?\s*generated with\b", re.I)


def strip_trailers(body: str) -> str:
    """A commit body with its machine trailer lines removed — the human rationale only. Pure."""
    return "\n".join(ln for ln in body.splitlines() if not _TRAILER.match(ln)).strip()


def parse_subject(subject: str) -> dict[str, str]:
    """Pull the Conventional-Commit type/scope/summary from a subject line (empty dict if
    it isn't conventional). These become queryable Commit properties."""
    m = _CONVENTIONAL.match(subject.strip())
    if not m:
        return {}
    return {k: v for k, v in {
        "change_type": m.group("type"), "scope": m.group("scope"),
        "summary": m.group("summary"),
    }.items() if v}


def parse_git_log(raw: str) -> list[Commit]:
    """Pure: a delimited `git log` dump → commits (genesis first if --reverse)."""
    out: list[Commit] = []
    for record in raw.split("\x1e"):
        rec = record.strip("\n")
        if not rec.strip():
            continue
        f = rec.split("\x1f")
        if len(f) < 6:
            continue
        body = f[6].strip() if len(f) > 6 else ""
        out.append(Commit(f[0], f[1], f[2], f[3],
                          f[4].split() if f[4].strip() else [], f[5], body))
    return out


def _git(path: str, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", path, *args], capture_output=True, text=True, check=True
    ).stdout


def read_commits(path: str, *, limit: int | None = None) -> list[Commit]:
    """Read a repo's history, genesis first. `limit` takes the most-recent N (git -n).

    `--all` (thread b4297a47/1c8e3907, ingest registration phase 2's own measured
    constraint 3): a bare `git log` walks only the current checked-out branch (HEAD),
    so a repo with live topic/worktree branches — 17 live worktree-agent-* branches
    proved it — never gets every commit ingested even when the repo itself IS
    tracked. Safe to always include: `follows` links are derived from each commit's
    own parent SHA (`c.parents` below), never from this log's line order, and every
    object here is find-or-create on sha/canonical — a commit reachable from two
    branches is ingested once regardless of how many roots `--all` walks it from."""
    args = ["log", "--all", "--reverse", f"--pretty=format:{_FMT}"]
    if limit:
        args += ["-n", str(limit)]
    return parse_git_log(_git(path, *args))


def _dev_canonical(c: Commit) -> str:
    return f"dev:{(c.author_email or c.author_name).strip().lower()}"


def _identity_name(dev_canonical: str) -> str:
    """GIT IDENTITIES WEARING THE WRONG NAME (thread 0be2f790's own operator-finding
    follow-up, Thoth DM 10711, ruling 9d64cb25): a dev: identity's own `name` is
    derived ONCE from the identity itself — the email local part, literally whatever
    precedes '@' in the canonical's own `dev:<email>` — never from whichever commit
    author happened to write it last. Stable by construction: a pure function of the
    (immutable) canonical, so it never varies run to run regardless of which of the many
    callers (gitlog's own CLI, pulse's watch loop, tree_ingest's own one-source-id-per-
    agent-worktree calls, each a DIFFERENT source_id under the SAME shared ingest_repo)
    triggered this ingest, or what that particular commit's author name string said.
    A `dev:<name>` canonical with no `@` at all (the `_dev_canonical` fallback for a
    commit with no author email) has no "local part" to strip — the whole thing is the
    identity, unchanged. A `machine:<email>` canonical (MACHINE GIT IDENTITIES ARE NOT
    PEOPLE, ruling edb6b0fc) is the same derivation over the same shape — one prefix or
    the other, never both, so stripping either leaves the identity untouched."""
    email = dev_canonical.removeprefix("dev:").removeprefix("machine:")
    return email.split("@", 1)[0]


# MACHINE GIT IDENTITIES ARE NOT PEOPLE (thread 2619f011, ruling edb6b0fc): a bare local
# or noreply-style host is the tell — never the local part alone, since a human genuinely
# named after a repo on a real mail provider must never misroute.
_MACHINE_SHAPED_DOMAINS = {"local", "localhost"}


def _is_machine_shaped_domain(domain: str) -> bool:
    d = domain.lower()
    return d in _MACHINE_SHAPED_DOMAINS or "noreply" in d


def _machine_canonical(email: str) -> str:
    return f"machine:{email.strip().lower()}"


async def _matching_ingested_project(actions: Actions, local_part: str) -> uuid.UUID | None:
    """The other half of the heuristic (ruling edb6b0fc): the local part must equal an
    ALREADY-INGESTED repo's own canonical name, never just any string that happens to
    look machine-shaped — `dev:ballgem@local` routes to MachineIdentity specifically
    because repo:ballgem already exists as a SoftwareProject."""
    return await actions.pool.fetchval(  # type: ignore[no-any-return]
        "SELECT id FROM objects WHERE type='SoftwareProject' AND canonical=$1",
        f"repo:{local_part}")


async def declare_machine_identity(
    actions: Actions, *, email: str, project: str, because: str, actor: str,
) -> dict[str, Any]:
    """THE declare-machine-identity DOOR (ruling edb6b0fc): covers what the ingest
    heuristic (_matching_ingested_project + _is_machine_shaped_domain) misses — a bot
    committing from a real-looking domain, or a local part that doesn't happen to match
    any repo name. Manual, so it demands a written `because`, never silent. Mints/finds
    the MachineIdentity, bridges any pre-existing dev:<email> Person via same_as (never
    deleted, never retyped), and mints committer_for with `since` = now (this door has
    no commit history of its own to date it by, unlike the ingest heuristic's own first-
    seen date). Idempotent: re-declaring the same (email, project) is a no-op past the
    first call, same dedup discipline as ingest_repo's own links."""
    if not because.strip():
        return {"error": "declare-machine-identity requires a written `because`"}
    email = email.strip().lower()
    local_part, _, domain = email.partition("@")
    if not local_part or not domain:
        return {"error": f"not an email address: {email!r}"}
    project_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='SoftwareProject' AND canonical=$1",
        f"repo:{project}")
    if project_id is None:
        return {"error": f"no such SoftwareProject: repo:{project!r}"}

    now = datetime.now(UTC)
    conf = confidence_for(EvidenceClass.SELF_DECLARED)
    ec = EvidenceClass.SELF_DECLARED.value
    machine_canonical = _machine_canonical(email)
    machine_id = await actions.create_or_find_object("MachineIdentity", machine_canonical, actor)
    await actions.assert_property(machine_id, "name", _identity_name(machine_canonical),
                                  actor, now, conf, evidence_class=ec)
    await actions.assert_property(machine_id, "declared_because", because, actor, now, conf,
                                  evidence_class=ec)

    bridged_person: str | None = None
    legacy_canonical = f"dev:{email}"
    legacy_person_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='Person' AND canonical=$1", legacy_canonical)
    if legacy_person_id is not None:
        already = await actions.pool.fetchval(
            "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='same_as'",
            legacy_person_id, machine_id)
        if not already:
            await actions.create_link(legacy_person_id, machine_id, "same_as", actor, now,
                                      conf, evidence_class=ec)
        bridged_person = legacy_canonical

    minted_committer_for = False
    committer_exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='committer_for'",
        machine_id, project_id)
    if not committer_exists:
        await actions.create_link(machine_id, project_id, "committer_for", actor, now, conf,
                                  evidence_class=ec, properties={"since": now.isoformat()})
        minted_committer_for = True

    return {
        "machine_identity": machine_canonical,
        "project": f"repo:{project}",
        "bridged_person": bridged_person,
        "minted_committer_for": minted_committer_for,
    }


async def _seat_holder_at(pool: Any, *, seat_id: str, at: datetime) -> str | None:
    """Which Agent generation held `seat_id` at time `at` — the seat's own `holds` link
    history (bind_holder's own convention: the prior holder's link heals by `valid_until`,
    never deleted, so the full holder history stays walkable), time-windowed. None when no
    holder's window covers `at` — a commit older than the seat's first holder, or one that
    falls in a gap nothing bound — never guesses to the nearest one either side."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id "
        "WHERE t.canonical=$1 AND l.type='holds' "
        "AND l.first_seen <= $2 AND (l.valid_until IS NULL OR l.valid_until > $2) "
        "LIMIT 1", seat_id, at)


async def _worktree_seat(pool: Any, *, worktree_path: str) -> str | None:
    """The seat bound to this commit's own worktree — `tree_seat_hint` (the same
    mechanical-mount primitive `mount()` already trusts, never a string-guess off the
    directory name), resolved to a single unambiguous Seat id via `seats_by_handle`. None
    when no seat is bound here, or (should never happen for a real `tree_cwd` binding, but
    never guessed through) more than one carries the same handle."""
    from src.orchestrator.seats import seats_by_handle, tree_seat_hint

    handle = await tree_seat_hint(pool, cwd=worktree_path)
    if handle is None:
        return None
    seats = await seats_by_handle(pool, handle)
    if len(seats) != 1:
        return None
    return seats[0]


async def resolve_committed_by(
    pool: Any, *, worktree_path: str, author_date: datetime,
) -> str | None:
    """WAVE 27 COMMITS ATTRIBUTED TO AGENT IDENTITIES (ruling 4cf5e4b3/b8fb26494e0e,
    amended by decision 830a6c0a: the Claude-Session trailer is a claude.ai WEB session id,
    a different namespace from the local job_dir/anchor_sid sids this house's own
    provenance actually tracks — it resolves nothing on its own). Primary signal, and in
    this build the ONLY one: `_worktree_seat` crossed with WHICH GENERATION held that seat
    at the commit's own author time (`_seat_holder_at`, the holds link's own history). None
    on any miss — never guesses. A per-commit convenience; `ingest_repo` resolves the seat
    ONCE per run instead (the worktree is fixed for the whole call) and calls
    `_seat_holder_at` directly per commit.

    SCOPED OUT OF THIS BUILD, named rather than rushed in (see the tip that shipped this):
    the ruling also names an ingest-actor fallback for when no worktree is bound, and
    Claude-Session-trailer disambiguation for when the worktree-handle signal and the
    ingest actor disagree (a successor ingesting commits its own ancestor authored, the
    exact BUG 4 shape). Both need a bounded-scan-of-candidates'-own-transcripts primitive
    this build does not build — left as a named follow-up rather than a rushed one."""
    seat_id = await _worktree_seat(pool, worktree_path=worktree_path)
    if seat_id is None:
        return None
    return await _seat_holder_at(pool, seat_id=seat_id, at=author_date)


async def ingest_repo(
    actions: Actions, path: str = ".", *, limit: int | None = None,
    source_id: str = _SOURCE, case_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    """Ingest a repository's history into the entity graph. Idempotent (find-or-create
    on the commit sha / dev email), so re-running just adds new commits.

    COMMITTED_BY (WAVE 27, ruling 4cf5e4b3/b8fb26494e0e): each commit's own author time
    is crossed against `toplevel`'s bound seat (`resolve_committed_by`, resolved ONCE per
    run since the worktree is fixed for the whole call) to mint an ADDITIONAL committed_by
    Agent link beside authored_by — never a replacement, never asserted when the resolver
    can't name a live holder for that exact instant."""
    toplevel = _git(path, "rev-parse", "--show-toplevel").strip()
    name = Path(toplevel).name
    commits = read_commits(path, limit=limit)
    latest = (
        datetime.fromisoformat(commits[-1].date) if commits else datetime.now(UTC)
    )
    committed_by_seat = await _worktree_seat(actions.pool, worktree_path=toplevel)

    repo = await actions.create_or_find_object(
        "SoftwareProject", f"repo:{name}", source_id, case_id
    )
    await actions.assert_property(repo, "name", name, source_id, latest, _CONF,
                                  case_id=case_id, evidence_class=_EC)

    # create_link is a plain append, so re-ingesting a repo (the normal way to pick up new
    # commits) would DUPLICATE every structural edge. Objects dedup on canonical, but the
    # authored_by/in_repo/follows/committer_for/same_as links don't — dedup them so a
    # re-ingest is truly idempotent. committer_for/same_as (ruling edb6b0fc) join the same
    # set: a re-ingest must never re-bridge or re-link what a prior run already minted.
    existing = {(r["from_id"], r["to_id"], r["type"]) for r in await actions.pool.fetch(
        "SELECT from_id, to_id, type FROM links "
        "WHERE type IN ('authored_by', 'in_repo', 'follows', 'committer_for', 'same_as', "
        "'committed_by')")}

    async def _link(frm: uuid.UUID, to: uuid.UUID, typ: str, observed: datetime, *,
                    properties: dict[str, Any] | None = None) -> None:
        if (frm, to, typ) in existing:
            return
        await actions.create_link(frm, to, typ, source_id, observed, _CONF,
                                  case_id=case_id, evidence_class=_EC, properties=properties)
        existing.add((frm, to, typ))

    # MACHINE GIT IDENTITIES ARE NOT PEOPLE (ruling edb6b0fc): the (local_part, domain) ->
    # matched-project lookup is memoized per run — the DB round trip in
    # _matching_ingested_project only ever fires for a machine-shaped domain (rare), and
    # every commit from the same author asks the identical question.
    _machine_project_cache: dict[str, uuid.UUID | None] = {}

    async def _machine_project_for(email: str) -> uuid.UUID | None:
        local_part, _, domain = email.partition("@")
        if not local_part or not domain or not _is_machine_shaped_domain(domain):
            return None
        if local_part not in _machine_project_cache:
            _machine_project_cache[local_part] = await _matching_ingested_project(
                actions, local_part)
        return _machine_project_cache[local_part]

    # A DEV'S EMAIL/ALIAS SET IS CHECKED ONCE PER RUN, NOT REASSERTED PER COMMIT (operator
    # ruling, thread 2a280e07, mail 9240 — "fix the sources"): the naive per-commit assert
    # reasserted on EVERY commit by the same author, live-measured at 166,786/166,782 rows
    # for one Person — this repo's own git history is exactly the 8-12-minute-cron source
    # Thoth's dispatch named. `dev_info` accumulates, per dev canonical, EVERY distinct
    # author_name actually seen across this run's whole walk (not just the last commit) —
    # GIT IDENTITIES WEARING THE WRONG NAME (thread 0be2f790's own operator-finding
    # follow-up, Thoth DM 10711, ruling 9d64cb25) replaced the old "last commit's
    # author name becomes `name`" policy (which let whichever of many source_id-per-
    # agent-worktree ingest runs happened to write most recently silently flip a Person's
    # own displayed name — the operator's own git identity flipping to an agent's name was
    # exactly this) with a policy that never lets ANY commit author name touch `name` at
    # all: `_identity_name` derives it once from the (stable) canonical itself, and every
    # author name actually seen rides along as an `author_alias` instead — never lost,
    # never mistaken for the identity's own chosen name.
    dev_info: dict[str, dict[str, Any]] = {}
    for c in commits:
        observed = datetime.fromisoformat(c.date)
        short = c.sha[:12]

        # MACHINE GIT IDENTITIES ARE NOT PEOPLE (ruling edb6b0fc): route to MachineIdentity
        # at mint time when the heuristic matches, never to Person — objects.type is
        # immutable, so getting this right at first mint is the only way to avoid a
        # same_as bridge later. Falls back to the existing Person routing otherwise.
        machine_project_id = (
            await _machine_project_for(c.author_email) if c.author_email else None)
        if machine_project_id is not None:
            key = _machine_canonical(c.author_email)
            dev = await actions.create_or_find_object("MachineIdentity", key, source_id, case_id)
        else:
            key = _dev_canonical(c)
            dev = await actions.create_or_find_object("Person", key, source_id, case_id)
        info = dev_info.setdefault(
            key, {"id": dev, "names": set(), "email": None, "machine_project_id": None})
        info["machine_project_id"] = machine_project_id or info["machine_project_id"]
        info["names"].add(c.author_name)
        info["email"] = c.author_email or info["email"]
        info["observed"] = observed
        info.setdefault("first_seen", observed)

        cm = await actions.create_or_find_object("Commit", f"commit:{short}", source_id, case_id)
        await actions.assert_property(cm, "subject", c.subject, source_id, observed, _CONF,
                                      case_id=case_id, evidence_class=_EC)
        await actions.assert_property(cm, "authored_date", c.date, source_id, observed, _CONF,
                                      case_id=case_id, evidence_class=_EC)
        # the structure that turns the log into queryable memory: type/scope (groupable) +
        # the rationale body (why, not just what).
        for prop, value in parse_subject(c.subject).items():  # not `name` — it shadows the repo
            await actions.assert_property(cm, prop, value, source_id, observed, _CONF,
                                          case_id=case_id, evidence_class=_EC)
        rationale = strip_trailers(c.body)
        if rationale:
            await actions.assert_property(cm, "rationale", rationale, source_id, observed, _CONF,
                                          case_id=case_id, evidence_class=_EC)
        if not c.parents:  # the first commit — the genesis
            await actions.assert_property(cm, "genesis", "true", source_id, observed, _CONF,
                                          case_id=case_id, evidence_class=_EC)

        await _link(cm, dev, "authored_by", observed)
        await _link(cm, repo, "in_repo", observed)
        if committed_by_seat is not None:
            holder = await _seat_holder_at(actions.pool, seat_id=committed_by_seat, at=observed)
            if holder is not None:
                agent = await actions.create_or_find_object(
                    "Agent", holder, source_id, case_id)
                await _link(cm, agent, "committed_by", observed)
        for parent in c.parents:
            par = await actions.create_or_find_object(
                "Commit", f"commit:{parent[:12]}", source_id, case_id
            )
            await _link(cm, par, "follows", observed)

    for key, info in dev_info.items():
        dev, observed, author_email = info["id"], info["observed"], info["email"]
        identity_name = _identity_name(key)
        current_name = await actions.pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a "
            "WHERE a.object_id=$1 AND a.name='name' AND a.source_id=$2 LIMIT 1",
            dev, source_id)
        if current_name != identity_name:
            await actions.assert_property(dev, "name", identity_name, source_id, observed,
                                          _CONF, case_id=case_id, evidence_class=_EC)
        # GIT IDENTITIES WEARING THE WRONG NAME (Thoth DM 10711, ruling 9d64cb25):
        # every author_name actually seen for this dev this run, other than the identity-
        # derived name itself, rides along as `author_alias` — additive across runs (merged
        # with whatever this SAME source already recorded), never overwritten, never
        # mistaken for the identity's own chosen name.
        # case-insensitive compare: `identity_name` is always lowercase (`_dev_canonical`
        # lowercases the whole canonical), but a commit author's own display name carries
        # its natural casing — "Ada" must not count as an alias of "ada" just because the
        # email-derived identity name is lowercase; "Ada Lovelace" genuinely is a different
        # name and still does.
        seen_aliases = {n for n in info["names"] if n and n.lower() != identity_name}
        if seen_aliases:
            existing_alias_value = await actions.pool.fetchval(
                "SELECT a.value FROM current_assertions a "
                "WHERE a.object_id=$1 AND a.name='author_alias' AND a.source_id=$2 LIMIT 1",
                dev, source_id)
            existing_aliases = set(existing_alias_value) if existing_alias_value else set()
            merged = sorted(existing_aliases | seen_aliases)
            if merged != sorted(existing_aliases):
                await actions.assert_property(
                    dev, "author_alias", merged, source_id, observed, _CONF,
                    case_id=case_id, evidence_class=_EC)
        if author_email:
            current_email = await actions.pool.fetchval(
                "SELECT a.value #>> '{}' FROM current_assertions a "
                "WHERE a.object_id=$1 AND a.name='email' AND a.source_id=$2 LIMIT 1",
                dev, source_id)
            if current_email != author_email:
                await actions.assert_property(
                    dev, "email", author_email, source_id, observed, _CONF,
                    case_id=case_id, evidence_class=_EC)

        # MACHINE GIT IDENTITIES ARE NOT PEOPLE (ruling edb6b0fc): the standing
        # committer_for edge, `since` = the first commit this identity was seen for this
        # project THIS run (commits arrive genesis-first, so the first one encountered is
        # already the earliest — set once, never revised by a later re-ingest that only
        # ever sees newer history). A Person minted under the old, pre-heuristic routing
        # for the SAME email is bridged with same_as (loser -> winner), never deleted or
        # retyped — the object may not exist at all if this identity was always routed
        # correctly, which is the ordinary case going forward.
        machine_project_id = info.get("machine_project_id")
        if machine_project_id is not None:
            await _link(dev, machine_project_id, "committer_for", info["first_seen"],
                       properties={"since": info["first_seen"].isoformat()})
            if author_email:
                legacy_person_id = await actions.pool.fetchval(
                    "SELECT id FROM objects WHERE type='Person' AND canonical=$1",
                    f"dev:{author_email.strip().lower()}")
                if legacy_person_id is not None:
                    await _link(legacy_person_id, dev, "same_as", observed)

    return {"repo": name, "commits": len(commits), "developers": len(dev_info)}


def main() -> None:  # pragma: no cover - CLI
    import asyncio
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else "."
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None

    async def run() -> None:
        pool = await create_pool(
            get_settings().database_url, application_name="osiris-script:ingest-gitlog")
        try:
            print(await ingest_repo(Actions(pool), path, limit=limit))
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":  # pragma: no cover
    main()

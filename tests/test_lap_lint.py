"""rungs 2+3 — the lap lens and the graph lint (campaign 5c57f54d).

lap: ONE object's provenance timeline — assertions with their supersession fate, links in
both directions, kernel events, and the current winning view: how the graph came to
believe a thing. lint: the graph auditing ITSELF — report-only findings, each check born
from a lived bug (the impersonation class, coin-flip winners, rotting duties).
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from src.actions.core import Actions
from src.orchestrator import mounts
from src.orchestrator.compositions import resolve_ref, run_spec
from src.parsers.base import EvidenceClass

NOW = datetime(2026, 7, 9, tzinfo=UTC)
_SD = EvidenceClass.SELF_DECLARED.value


async def _fn(actions: Actions, name: str, args: dict[str, Any]) -> dict[str, Any]:
    out = await run_spec(actions.pool, {"op": "function", "name": name, "args": args},
                         None, name=name)
    items: dict[str, Any] = out["items"]  # the composition envelope wraps the return
    return items


def _by_check(out: dict[str, Any], check: str) -> list[dict[str, Any]]:
    return [f for f in out["findings"] if f["check"] == check]


# --- lap ---------------------------------------------------------------------


async def test_lap_shows_how_the_graph_came_to_believe(actions: Actions) -> None:
    """The whole lens in one object: a superseded assertion keeps its place in the timeline
    (marked), the winner lands in `believes`, links carry their direction, and the entries
    read in observed order."""
    t = "agent:teller"
    a = await actions.create_or_find_object("Person", "person:probe", t)
    b = await actions.create_or_find_object("Organization", "org:probe", t)
    await actions.assert_property(a, "name", "Probe One", t, NOW, 0.9, evidence_class=_SD)
    await actions.assert_property(a, "name", "Probe Prime", t, NOW + timedelta(hours=1),
                                  0.9, evidence_class=_SD)  # within-source supersession
    await actions.create_link(a, b, "member_of", t, NOW, 0.9, evidence_class=_SD)
    out = await _fn(actions, "lap", {"ref": "person:probe"})
    assert out["object"]["canonical"] == "person:probe" and out["object"]["type"] == "Person"
    assert out["believes"]["name"] == "Probe Prime"
    names = [e for e in out["timeline"] if e.get("field") == "name"]
    assert names[0]["value"] == "Probe One" and names[0]["superseded"] is True
    assert names[1]["value"] == "Probe Prime" and "superseded" not in names[1]
    assert names[0]["source"] == t and names[0]["grade"] == "self_declared"
    link = next(e for e in out["timeline"] if e["kind"] == "link-out")
    assert link["link"] == "member_of" and link["other"] == "org:probe"
    ats = [str(e["at"]) for e in out["timeline"]]
    assert ats == sorted(ats)  # observed order — a timeline, not a bag
    assert out["counts"]["superseded"] == 1
    # the other end sees the same edge, inbound
    other = await _fn(actions, "lap", {"ref": "org:probe"})
    assert any(e["kind"] == "link-in" and e["other"] == "person:probe"
               for e in other["timeline"])


async def test_lap_resolves_names_and_reports_absence(actions: Actions) -> None:
    t = "agent:teller"
    a = await actions.create_or_find_object("Person", "person:named", t)
    await actions.assert_property(a, "name", "Hannah Arendt", t, NOW, 0.9,
                                  evidence_class=_SD)
    assert await resolve_ref(actions.pool, "Hannah Arendt") == a  # exact name
    assert await resolve_ref(actions.pool, "person:named") == a  # canonical
    assert await resolve_ref(actions.pool, str(a)[:8]) == a  # short-id prefix (task #64)
    out = await _fn(actions, "lap", {"ref": "Arendt"})  # substring
    assert out["object"]["canonical"] == "person:named"
    assert "nothing matches" in (await _fn(actions, "lap", {"ref": "zz-never-zz"}))["note"]


async def test_resolve_ref_short_id_never_shadows_a_real_canonical_or_name(
    actions: Actions,
) -> None:
    """task #64 (ruling ad19a779) — the short-id leg mirrors capture._find_thread/
    _find_decision's own convention exactly, widened to any object type: a
    table/Function-sourced row's own "id" column (_col_value's 8-char short-id special
    case) now resolves through dossier()/focus_object(), not just recall(). A ref that
    ISN'T hex-shaped (a real canonical, a real name) is completely untouched by the new
    leg — it never even runs the short-id query."""
    a = await actions.create_or_find_object("Person", "person:short-a", "t")
    b = await actions.create_or_find_object("Person", "person:short-b", "t")
    assert await resolve_ref(actions.pool, str(a)[:8]) == a
    assert await resolve_ref(actions.pool, str(b)[:8]) == b
    assert await resolve_ref(actions.pool, "not-hex-at-all") is None
    assert await resolve_ref(actions.pool, "repo:osiris-does-not-exist") is None
    assert "ONE object" in (await _fn(actions, "lap", {}))["note"]  # no ref, no subject


async def test_lap_caps_honestly(actions: Actions) -> None:
    """A trimmed timeline SAYS it trimmed — counts hold the true totals (no silent caps)."""
    t = "agent:teller"
    a = await actions.create_or_find_object("Person", "person:busy", t)
    for i in range(6):
        await actions.assert_property(a, f"p{i}", f"v{i}", t, NOW + timedelta(minutes=i),
                                      0.9, evidence_class=_SD)
    out = await _fn(actions, "lap", {"ref": "person:busy", "limit": 2})
    assert len(out["timeline"]) == 2
    assert out["counts"]["assertions"] == 6
    assert "dropped" in out["note"]
    # the newest survive the trim — the shown tail IS the untrimmed timeline's tail
    full = await _fn(actions, "lap", {"ref": "person:busy", "limit": 1000})
    assert "note" not in full and out["timeline"] == full["timeline"][-2:]


# --- lint --------------------------------------------------------------------


async def test_lint_catches_the_lineage_sins(actions: Actions) -> None:
    """Cycles, dangling heir pointers, heirs without ancestry, retired-yet-live, and the
    healed false mints — the succession invariants of ruling a882b334, as tripwires."""
    t = "agent:teller"
    x = await actions.create_or_find_object("Agent", "agent:aaaa0001", t)
    y = await actions.create_or_find_object("Agent", "agent:bbbb0002", t)
    await actions.assert_property(x, "succeeded_by", "agent:bbbb0002", t, NOW, 0.9,
                                  evidence_class=_SD)
    await actions.assert_property(y, "succeeded_by", "agent:aaaa0001", t, NOW, 0.9,
                                  evidence_class=_SD)  # a ring of heirs
    z = await actions.create_or_find_object("Agent", "agent:cccc0003", t)
    await actions.assert_property(z, "succeeded_by", "agent:dddd0404", t, NOW, 0.9,
                                  evidence_class=_SD)  # points into the void
    await actions.create_or_find_object("Agent", "agent:eeee0005-ii", t)  # no succeeded_from
    r = await actions.create_or_find_object("Agent", "agent:ffff0006", t)
    await actions.assert_property(r, "retired", True, t, NOW, 0.9, evidence_class=_SD)
    await mounts.save_mount(actions.pool, job_dir="/x/jobs/ffff0006",
                            agent_id="agent:ffff0006", project="p", cwd="/w",
                            model=None, session_key=None)  # ...yet freshly seen
    fm = await actions.create_or_find_object("Agent", "agent:0000dead", t)
    await actions.assert_property(fm, "false_mint", True, t, NOW, 0.9, evidence_class=_SD)
    out = await _fn(actions, "lint", {})
    cycle = _by_check(out, "lineage-cycle")
    assert len(cycle) == 1 and "agent:aaaa0001" in cycle[0]["detail"]
    dangle = _by_check(out, "lineage-dangling")
    assert len(dangle) == 1 and dangle[0]["subject"] == "agent:cccc0003"
    assert [f["subject"] for f in _by_check(out, "orphan-heir")] == ["agent:eeee0005-ii"]
    retired = _by_check(out, "retired-live")
    assert len(retired) == 1 and retired[0]["subject"] == "agent:ffff0006"
    assert retired[0]["severity"] == "error"
    assert [f["subject"] for f in _by_check(out, "false-mint")] == ["agent:0000dead"]


async def test_lint_walks_through_a_historical_generation(actions: Actions) -> None:
    """An archived (historical) heir is ANCESTRY, not absence (task #20, 2026-07-19: four
    bases whose -ii heirs had been archived read as 'dangling' for two sessions). The walk
    must not flag a pointer at a historical object — and must CONTINUE through it, so a
    genuine void pointer deeper in the chain is still found and blamed on its true holder."""
    t = "agent:teller"
    base = await actions.create_or_find_object("Agent", "agent:aaaa1111", t)
    mid = await actions.create_or_find_object("Agent", "agent:aaaa1111-ii", t)
    await actions.assert_property(base, "succeeded_by", "agent:aaaa1111-ii", t, NOW, 0.9,
                                  evidence_class=_SD)
    await actions.assert_property(mid, "succeeded_from", "agent:aaaa1111", t, NOW, 0.9,
                                  evidence_class=_SD)
    await actions.assert_property(mid, "succeeded_by", "agent:aaaa1111-gone", t, NOW, 0.9,
                                  evidence_class=_SD)   # the void, PAST the archive
    await actions.pool.execute(
        "UPDATE objects SET status='historical' WHERE id=$1", mid)
    out = await _fn(actions, "lint", {})
    dangle = {f["subject"]: f["detail"] for f in _by_check(out, "lineage-dangling")}
    assert "agent:aaaa1111" not in dangle          # a historical heir is not a void
    assert "agent:aaaa1111-ii" in dangle           # the walk continued and found the void
    assert "agent:aaaa1111-gone" in dangle["agent:aaaa1111-ii"]


async def test_lint_surfaces_coin_flip_winners(actions: Actions) -> None:
    """Two sources, same fact, different values, near-tie confidence: the resolver is
    deciding on recency alone — surfaced as a contradiction, never resolved."""
    t = "agent:teller"
    c = await actions.create_or_find_object("Organization", "org:tie", t)
    await actions.assert_property(c, "hq", "Berlin", "agent:one", NOW, 0.9,
                                  evidence_class=_SD)
    await actions.assert_property(c, "hq", "Munich", "agent:two",
                                  NOW + timedelta(minutes=1), 0.9, evidence_class=_SD)
    await actions.assert_property(c, "hq", "Berlin", "agent:one",
                                  NOW + timedelta(minutes=2), 0.9,
                                  evidence_class=_SD)  # re-affirmed; still a tie
    out = await _fn(actions, "lint", {})
    con = _by_check(out, "contradiction")
    assert len(con) == 1
    assert con[0]["subject"] == "org:tie" and con[0]["field"] == "hq"
    assert "Berlin" in con[0]["detail"] and "Munich" in con[0]["detail"]
    # a decisive gap is NOT a contradiction
    await actions.assert_property(c, "hq", "Berlin", "agent:one",
                                  NOW + timedelta(minutes=3), 0.99, evidence_class=_SD)
    out2 = await _fn(actions, "lint", {})
    assert _by_check(out2, "contradiction") == []


async def test_lint_status_lifecycle_is_not_a_war(actions: Actions) -> None:
    """The first live run's lesson (23 findings, zero real): open→resolved from another
    hand is the state machine WORKING — never a contradiction. The one true failure mode —
    an 'open' NEWER than a different source's 'resolved' — is its own error check."""
    t = "agent:teller"
    ok = await actions.create_or_find_object("Thread", "thread:lifecycle", t)
    await actions.assert_property(ok, "status", "open", "agent:opener", NOW, 0.9,
                                  evidence_class=_SD)
    await actions.assert_property(ok, "status", "resolved", "agent:closer",
                                  NOW + timedelta(hours=1), 0.9, evidence_class=_SD)
    out = await _fn(actions, "lint", {})
    assert _by_check(out, "contradiction") == []        # a transition, not a tie
    assert _by_check(out, "status-regression") == []
    # ...but a REGRESSION — re-opened by recency over a deliberate close — is an error
    bad = await actions.create_or_find_object("Thread", "thread:regressed", t)
    await actions.assert_property(bad, "summary", "the overridden close", t, NOW, 0.9,
                                  evidence_class=_SD)
    await actions.assert_property(bad, "status", "resolved", "agent:closer", NOW, 0.9,
                                  evidence_class=_SD)
    await actions.assert_property(bad, "status", "open", "agent:necromancer",
                                  NOW + timedelta(hours=2), 0.9, evidence_class=_SD)
    out2 = await _fn(actions, "lint", {})
    reg = _by_check(out2, "status-regression")
    assert len(reg) == 1 and reg[0]["subject"] == "thread:regressed"
    assert reg[0]["severity"] == "error"
    assert "agent:necromancer" in reg[0]["detail"] and "agent:closer" in reg[0]["detail"]
    # re-resolving heals the finding, and a LOW-confidence re-open (the miner's DERIVED
    # echo, newer still) does NOT re-flag: it lacks the confidence to override the close
    await actions.assert_property(bad, "status", "resolved", "agent:closer",
                                  NOW + timedelta(hours=3), 0.9, evidence_class=_SD)
    await actions.assert_property(bad, "status", "open", "session-miner",
                                  NOW + timedelta(hours=4), 0.4, evidence_class="derived")
    out3 = await _fn(actions, "lint", {})
    assert all(f["subject"] != "thread:regressed"
               for f in _by_check(out3, "status-regression"))


async def test_lint_double_resolution_is_corroboration(actions: Actions) -> None:
    """Operator ruling 64adf08a (the 94ddca1f adjudication): two hands both closing the
    same thread — status agrees, only resolved_in/resolved_because differ — is TWO
    WITNESSES attesting one fact, never a contradiction. Keep both; the lint stays quiet.
    A genuine non-lifecycle tie on the same object still flags."""
    t = "agent:teller"
    th = await actions.create_or_find_object("Thread", "thread:twice-blessed", t)
    await actions.assert_property(th, "summary", "image the matched organ pair", t, NOW, 0.9,
                                  evidence_class=_SD)
    await actions.assert_property(th, "status", "open", t, NOW, 0.9, evidence_class=_SD)
    for src, because, dt in (("agent:doer", "done in commit 2b3fe10", 1),
                             ("session", "verified overnight by successor", 12)):
        when = NOW + timedelta(hours=dt)
        await actions.assert_property(th, "status", "resolved", src, when, 0.9,
                                      evidence_class=_SD)
        await actions.assert_property(th, "resolved_in", src, src, when, 0.9,
                                      evidence_class=_SD)
        await actions.assert_property(th, "resolved_because", because, src, when, 0.9,
                                      evidence_class=_SD)
    out = await _fn(actions, "lint", {})
    assert _by_check(out, "contradiction") == []         # corroboration, not a war
    assert _by_check(out, "status-regression") == []     # and no false regression either
    # the exclusion is the lifecycle FAMILY only — a real tie elsewhere still surfaces
    await actions.assert_property(th, "owner", "alice", "agent:one", NOW, 0.9,
                                  evidence_class=_SD)
    await actions.assert_property(th, "owner", "bob", "agent:two",
                                  NOW + timedelta(minutes=1), 0.9, evidence_class=_SD)
    out2 = await _fn(actions, "lint", {})
    con = _by_check(out2, "contradiction")
    assert len(con) == 1 and con[0]["field"] == "owner"


async def test_lint_orphan_links_stale_duties_and_ghosts(actions: Actions) -> None:
    t = "agent:teller"
    # a live link into a retired corpse
    alive = await actions.create_or_find_object("Person", "person:alive", t)
    corpse = await actions.create_or_find_object("Organization", "org:corpse", t)
    await actions.create_link(alive, corpse, "member_of", t, NOW, 0.9, evidence_class=_SD)
    await actions.pool.execute(
        "UPDATE objects SET status='retired' WHERE id=$1", corpse)
    # an obligation left open past its patience
    th = await actions.create_or_find_object("Thread", "thread:old-duty", t)
    await actions.assert_property(th, "status", "open", t, NOW, 0.9, evidence_class=_SD)
    await actions.assert_property(th, "kind", "obligation", t, NOW, 0.9, evidence_class=_SD)
    await actions.assert_property(th, "summary", "rotting duty", t, NOW, 0.9,
                                  evidence_class=_SD)
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '30 days' WHERE id=$1", th)
    # ...and a MERGE: the loser's old edge stays visible as consolidation debt, but the
    # merge's own same_as marker is the mechanism, never a finding
    winner = await actions.create_or_find_object("Organization", "org:winner", t)
    loser = await actions.create_or_find_object("Organization", "org:loser", t)
    await actions.create_link(alive, loser, "member_of", t, NOW, 0.9, evidence_class=_SD)
    await actions.merge_objects(winner, loser, "same org, two filings", t)
    out = await _fn(actions, "lint", {"stale_days": 14})
    orphan = _by_check(out, "orphan-link")
    subjects = [f["subject"] for f in orphan]
    assert any("org:corpse" in s for s in subjects)
    assert any("org:loser" in s and "member_of" in s for s in subjects)  # the debt, metered
    assert not any("same_as" in s for s in subjects)                     # the marker, excluded
    assert all(f["severity"] == "info" for f in orphan)                  # history, not damage
    assert "resolve-on-read" in orphan[0]["detail"]
    stale = _by_check(out, "stale-obligation")
    assert len(stale) == 1 and stale[0]["age_days"] >= 29
    assert "rotting duty" in stale[0]["detail"]
    # every write above was stamped by unregistered agent sources — the ghosts show up
    ghosts = {f["subject"] for f in _by_check(out, "attribution")}
    assert "agent:teller" in ghosts
    # ...and registering the face clears it
    await actions.create_or_find_object("Agent", "agent:teller", t)
    out2 = await _fn(actions, "lint", {})
    assert "agent:teller" not in {f["subject"] for f in _by_check(out2, "attribution")}


async def test_lint_attribution_sees_through_a_relay_annotation(actions: Actions) -> None:
    """A registered writer that suffixes its id with a parenthetical provenance note is
    NOT an impersonator (task #21, 2026-07-19: 338 of XLIV's relay writes — source_id
    'agent:... (relaying operator bulk ruling ...)' — read as an unregistered ghost for
    two sessions). The id is judged; the note rides along. A bare unregistered id still
    flags."""
    t = "agent:teller"
    await actions.create_or_find_object("Agent", "agent:teller", t)
    subj = await actions.create_or_find_object("Organization", "org:relayed", t)
    await actions.assert_property(
        subj, "hq", "Berlin", "agent:teller (relaying operator ruling, test fixture)",
        NOW, 0.9, evidence_class=_SD)
    await actions.assert_property(
        subj, "founded", "1999", "agent:00nobody0 (relaying nothing real)", NOW, 0.9,
        evidence_class=_SD)
    out = await _fn(actions, "lint", {})
    ghosts = {f["subject"] for f in _by_check(out, "attribution")}
    assert not any(s.startswith("agent:teller") for s in ghosts)   # registered + annotated
    assert "agent:00nobody0 (relaying nothing real)" in ghosts     # annotation ≠ amnesty


async def test_lint_deals_rot_candidates_but_never_resolves(actions: Actions) -> None:
    """Two witnesses (Metron IV, Soundwave): open threads whose repo's LATER commits share
    their vocabulary are probably done — the lint deals them as 'confirm?' candidates.
    Report-only: the thread's status is untouched (758ded94 — testimony, never lint)."""
    from src.orchestrator.capture import open_thread

    t = "agent:teller"
    tid = await open_thread(
        actions, "wire the satellite dispatcher into the vantage scheduler queue",
        repo="rotproj")
    assert tid is not None
    # a LATER commit in the same repo carries the thread's distinctive vocabulary
    repo = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:rotproj'")
    c = await actions.create_or_find_object("Commit", "commit:abc123rot", t)
    await actions.assert_property(
        c, "summary", "feat: satellite dispatcher wired into the vantage scheduler",
        t, NOW + timedelta(days=2), 0.9, evidence_class="authoritative_api")
    await actions.create_link(c, repo, "in_repo", t, NOW + timedelta(days=2), 0.9)
    out = await _fn(actions, "lint", {})
    rot = _by_check(out, "rot-candidate")
    assert len(rot) == 1 and rot[0]["subject"] == str(tid)
    assert "probably resolved, confirm?" in rot[0]["detail"]
    assert "commit:abc123rot" in rot[0]["detail"]
    # the record is UNTOUCHED — the lint dealt a card, it did not play a verb
    st = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='status' "
        "ORDER BY confidence DESC, observed_at DESC LIMIT 1", tid)
    assert st == "open"


async def test_lint_is_report_only_and_a_clean_graph_says_so(actions: Actions) -> None:
    """Rule #7 in test form: the lint must not write a single row — a linter that healed
    would be a loop pathology. And silence must be legible: clean checks are NAMED."""
    before = await actions.pool.fetchval("SELECT count(*) FROM assertions")
    out = await _fn(actions, "lint", {})
    after = await actions.pool.fetchval("SELECT count(*) FROM assertions")
    assert before == after
    events = await actions.pool.fetchval("SELECT count(*) FROM object_events")
    await _fn(actions, "lint", {})
    assert await actions.pool.fetchval("SELECT count(*) FROM object_events") == events
    assert out["findings"] == []
    assert set(out["clean"]) >= {"contradiction", "status-regression", "laundering",
                                 "lineage-cycle", "orphan-link", "stale-obligation",
                                 "attribution"}
    assert "report-only" in out["discipline"]


async def test_lint_flags_the_phantom_twin_at_an_office(actions: Actions) -> None:
    """PHANTOM-TWIN (90f0cb3a's residue): an anonymous, un-spawned, un-seated agent
    mounted at a Seat's office beside a different holder lineage — the shape a bridged
    resume mints when its receipts are missing. Flagged, never guessed at: seating is
    deliberate or it is nothing."""
    t = "agent:teller"
    office = "/w/twin-office"
    seat = await actions.create_or_find_object("Seat", "seat:0ffab001", t)
    await actions.assert_property(seat, "anchor_cwd", office, t, NOW, 0.9,
                                  evidence_class=_SD)
    holder = await actions.create_or_find_object("Agent", "agent:ab1e0001", t)
    await actions.create_link(holder, seat, "holds", t, NOW, 0.9, evidence_class=_SD)
    # the holder's own seated row at the office — never a twin (seat-bound)
    await mounts.save_mount(actions.pool, job_dir="/x/jobs/ab1e0001",
                            agent_id="agent:ab1e0001", project="p", cwd=office,
                            model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET seat_id='seat:0ffab001' WHERE agent_id='agent:ab1e0001'")
    # THE SUSPECT: anonymous, un-spawned, un-seated, at the office, foreign lineage
    await actions.create_or_find_object("Agent", "agent:facade01", t)
    await mounts.save_mount(actions.pool, job_dir="/x/jobs/facade01",
                            agent_id="agent:facade01", project="p", cwd=office,
                            model=None, session_key="whisper:facade01")
    # a NAMED agent at the office — a deliberate presence, not a phantom
    named = await actions.create_or_find_object("Agent", "agent:0c0ffee1", t)
    await actions.assert_property(named, "handle", "Visitor", t, NOW, 0.9,
                                  evidence_class=_SD)
    await mounts.save_mount(actions.pool, job_dir="/x/jobs/0c0ffee1",
                            agent_id="agent:0c0ffee1", project="p", cwd=office,
                            model=None, session_key=None)

    out = await _fn(actions, "lint", {})

    twins = _by_check(out, "phantom-twin")
    assert [f["subject"] for f in twins] == ["agent:facade01"]
    assert twins[0]["severity"] == "warn"
    assert "agent:ab1e0001" in twins[0]["detail"]        # names whose office it haunts
    assert "never auto-merge" in twins[0]["detail"]      # constitution #1 in the finding


async def test_lint_flags_a_parallel_life(actions: Actions) -> None:
    """PARALLEL-LIVES (thread 4bcd6541, invariant 3 of the guarantee): a generation
    minted while a DIFFERENT door of its own lineage still pulsed — the predecessor was
    not dead. The stamp is written AT the mint (mint_heir; rows are hot state and the
    pulse is gone by lint time); the lint reads the stamps and testifies. The phantom
    generations g40-v/vi would each have tripped this within a minute."""
    t = "agent:teller"
    heir = await actions.create_or_find_object("Agent", "agent:para0001-ii", t)
    await actions.assert_property(heir, "minted_because", "compaction", t, NOW, 0.9,
                                  evidence_class=_SD)
    await actions.assert_property(heir, "predecessor_last_seen", NOW.isoformat(), t,
                                  NOW, 0.9, evidence_class=_SD)
    await actions.assert_property(heir, "parallel_pulse_door", "a7e60257", t, NOW, 0.9,
                                  evidence_class=_SD)
    # a CLEAN heir — stamped pulse, no parallel door: a legitimate seam, never flagged
    clean = await actions.create_or_find_object("Agent", "agent:c1ean001-ii", t)
    await actions.assert_property(clean, "minted_because", "compaction", t, NOW, 0.9,
                                  evidence_class=_SD)
    await actions.assert_property(clean, "predecessor_last_seen", NOW.isoformat(), t,
                                  NOW, 0.9, evidence_class=_SD)

    out = await _fn(actions, "lint", {})

    par = _by_check(out, "parallel-lives")
    assert [f["subject"] for f in par] == ["agent:para0001-ii"]
    assert par[0]["severity"] == "warn"
    assert "a7e60257" in par[0]["detail"]                # names the live door
    assert "fold by hand" in par[0]["detail"]            # testimony, never a verdict

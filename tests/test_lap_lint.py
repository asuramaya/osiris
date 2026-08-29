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
    # false_mint but NO mount row — the ordinary healed-phantom case, never this check's own
    assert _by_check(out, "false-mint-live") == []


async def test_lint_flags_a_false_minted_generation_with_a_live_mount(
    actions: Actions,
) -> None:
    """THE HALCYON RULE (obligation 6b1efacb, 2026-08-18): a generation carrying
    false_mint=true with a LIVE mount is a DISTINCT, more alarming signal than an ordinary
    retirement racing a slow mount cleanup — a genuinely live body may be wearing a
    phantom-folded face. Named separately from `retired-live` so a human reading the
    report sees the repair door (reinstate_generation) directly, not a generic warning."""
    t = "agent:teller"
    victim = await actions.create_or_find_object("Agent", "agent:b4251601", t)
    await actions.assert_property(victim, "false_mint", True, t, NOW, 0.9, evidence_class=_SD)
    await actions.assert_property(victim, "retired", True, t, NOW, 0.9, evidence_class=_SD)
    await mounts.save_mount(actions.pool, job_dir="/x/jobs/b4251601",
                            agent_id="agent:b4251601", project="p", cwd="/w",
                            model=None, session_key=None)
    out = await _fn(actions, "lint", {})
    findings = _by_check(out, "false-mint-live")
    assert len(findings) == 1 and findings[0]["subject"] == "agent:b4251601"
    assert findings[0]["severity"] == "error"
    assert "reinstate_generation" in findings[0]["detail"]


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


async def test_lint_contradiction_sees_past_a_rank_3_rival_hidden_by_agreeing_top_2(
    actions: Actions,
) -> None:
    """The check's mechanism (row_number() hard-joined at rn=1 AND rn=2) only ever compares
    the TOP TWO ranked rows for an (object, field) pair — rows ranked 3+ are computed and
    then discarded, never compared to anything. Proven concretely on repo:bytebye/name: 19
    rival rows sat invisible at rn=3+ because rn=1 and rn=2 happened to already agree on the
    same value. Reproduce the minimal shape: two sources CORROBORATE the winning value (so
    it occupies both rn=1 and rn=2, and the old `w.v IS DISTINCT FROM r.v` check finds them
    equal and stops looking) while a THIRD source disputes it at a confidence within eps of
    the winner. To be precise about which claim this indicts: the RESOLVER's own supersession
    still correctly serves the corroborated value — that mechanism is not in question. What's
    wrong is the AUDITOR's completeness claim — it must still see a live, close-confidence
    rival that the winner's own corroboration happens to be hiding from it."""
    t = "agent:teller"
    c = await actions.create_or_find_object("Organization", "org:tri", t)
    await actions.assert_property(c, "hq", "Berlin", "agent:one", NOW, 0.9, evidence_class=_SD)
    await actions.assert_property(c, "hq", "Berlin", "agent:two", NOW + timedelta(minutes=1),
                                  0.9, evidence_class=_SD)   # corroborates -- ties rn=1/rn=2
    await actions.assert_property(c, "hq", "Munich", "agent:three", NOW + timedelta(minutes=2),
                                  0.87, evidence_class=_SD)  # rn=3 under the old ranking, but
                                                              # within eps of the winner
    out = await _fn(actions, "lint", {})
    con = _by_check(out, "contradiction")
    assert len(con) == 1
    assert con[0]["subject"] == "org:tri" and con[0]["field"] == "hq"
    assert "Berlin" in con[0]["detail"] and "Munich" in con[0]["detail"]


async def test_lint_contradiction_excludes_a_non_active_subject(actions: Actions) -> None:
    """thread 4a7da43a/12a210ab (reap Stage 1b, 2026-07-28): a merged/historical/archived
    object's internal coin-flips are history, not live ambiguity — nothing in the read-path
    (lineage_head resolves merged_into before ever touching a loser's own properties) ever
    surfaces them. Prove the exclusion is doing real work: the SAME tie flags while active,
    and goes quiet the moment status flips away from active."""
    t = "agent:teller"
    c = await actions.create_or_find_object("Organization", "org:grave", t)
    await actions.assert_property(c, "hq", "Berlin", "agent:one", NOW, 0.9, evidence_class=_SD)
    await actions.assert_property(c, "hq", "Munich", "agent:two", NOW + timedelta(minutes=1),
                                  0.9, evidence_class=_SD)
    out = await _fn(actions, "lint", {})
    assert len(_by_check(out, "contradiction")) == 1
    await actions.pool.execute("UPDATE objects SET status='merged' WHERE id=$1", c)
    out2 = await _fn(actions, "lint", {})
    assert _by_check(out2, "contradiction") == []


async def test_lint_contradiction_excludes_a_healed_debounce_guess(actions: Actions) -> None:
    """thread 4a7da43a/12a210ab: seam-debounce and husk-heal both write succeeded_by=''
    at debounce/heal time as a "no successor seen yet" placeholder; once a real generation
    self-declares succeeded_from back at the predecessor, the guess is permanently stale but
    never a live dispute — resolver noise from a known automated observer, not a coin-flip a
    mind needs to referee (walked and verified live against lineage_head: decision c41f74a6).
    The exclusion is narrow: a genuine tie on succeeded_by from two OTHER real sources still
    flags, and an empty value from a source OTHER than the two known debouncers still flags."""
    t = "agent:teller"
    a = await actions.create_or_find_object("Agent", "agent:debounced0001", t)
    await actions.assert_property(a, "succeeded_by", "", "seam-debounce", NOW, 0.6,
                                  evidence_class="direct_observation")
    await actions.assert_property(a, "succeeded_by", "agent:debounced0001-ii",
                                  "agent:debounced0001-ii", NOW + timedelta(minutes=1), 0.6,
                                  evidence_class="direct_observation")
    b = await actions.create_or_find_object("Agent", "agent:healed0002", t)
    await actions.assert_property(b, "succeeded_by", "", "husk-heal", NOW, 0.6,
                                  evidence_class="direct_observation")
    await actions.assert_property(b, "succeeded_by", "agent:healed0002-ii",
                                  "agent:healed0002-ii", NOW + timedelta(minutes=1), 0.6,
                                  evidence_class="direct_observation")
    out = await _fn(actions, "lint", {})
    con = _by_check(out, "contradiction")
    assert not any(f["subject"] in ("agent:debounced0001", "agent:healed0002") for f in con)


async def test_lint_contradiction_excludes_is_handoff_retirement(actions: Actions) -> None:
    """thread 6027 (Thoth's "504 contradictions are probably one bug" dispatch): is_handoff
    joins the lifecycle family (`status`/`resolved_in`/`resolved_because`) already excluded
    above — record_decision stamps 'true' at mint, `_retire_stale_handoffs` retires it by
    asserting 'false' at the SAME fixed confidence later. Measured live: 295/295 real
    findings had the 'false' winner strictly newer than the 'true' rival, zero reverse, zero
    ties — a designed two-state lifecycle, not a live dispute. Prove the exclusion is narrow:
    a genuine tie on some OTHER field for the same object still flags."""
    t = "agent:teller"
    d = await actions.create_or_find_object("Decision", "decision:handoff0001", t)
    await actions.assert_property(d, "is_handoff", "true", "agent:ancestor", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(d, "is_handoff", "false", "agent:successor",
                                  NOW + timedelta(minutes=5), 0.9, evidence_class="self_declared")
    # an unrelated field on the SAME object still flags if it genuinely ties
    await actions.assert_property(d, "summary", "Reign A", "agent:ancestor", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(d, "summary", "Reign B", "agent:successor",
                                  NOW + timedelta(minutes=1), 0.9, evidence_class="self_declared")
    out = await _fn(actions, "lint", {})
    con = _by_check(out, "contradiction")
    assert not any(f["field"] == "is_handoff" for f in con)
    assert any(f["field"] == "summary" and f["subject"] == "decision:handoff0001" for f in con)
    # a real tie on succeeded_by (neither source is a known debouncer, neither value empty)
    # still flags — the exclusion never widens into "succeeded_by is exempt"
    d = await actions.create_or_find_object("Agent", "agent:disputed0003", t)
    await actions.assert_property(d, "succeeded_by", "agent:disputed0003-ii", "agent:one",
                                  NOW, 0.9, evidence_class=_SD)
    await actions.assert_property(d, "succeeded_by", "agent:disputed0003-iii", "agent:two",
                                  NOW + timedelta(minutes=1), 0.9, evidence_class=_SD)
    out2 = await _fn(actions, "lint", {})
    disputed = _by_check(out2, "contradiction")
    assert any(f["subject"] == "agent:disputed0003" for f in disputed)
    # an empty rival from an UNKNOWN source is not the debounce/heal case — still flags
    e = await actions.create_or_find_object("Agent", "agent:strayempty0004", t)
    await actions.assert_property(e, "succeeded_by", "", "some-other-source", NOW, 0.6,
                                  evidence_class="direct_observation")
    await actions.assert_property(e, "succeeded_by", "agent:strayempty0004-ii",
                                  "agent:strayempty0004-ii", NOW + timedelta(minutes=1), 0.6,
                                  evidence_class="direct_observation")
    out3 = await _fn(actions, "lint", {})
    assert any(f["subject"] == "agent:strayempty0004" for f in _by_check(out3, "contradiction"))


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


# ═══ check/limit/offset (task #74, thread 12a210ab leg 1) — the 50-row hard cap had no
# pagination and no way to isolate one check's full findings without hand-writing this
# tool's own SQL, the exact pain the reap hit needing all 19 contradiction + 24 false-mint
# rows.

async def test_lint_check_filter_lists_only_that_checks_findings(actions: Actions) -> None:
    t = "agent:teller"
    c = await actions.create_or_find_object("Organization", "org:tie2", t)
    await actions.assert_property(c, "hq", "Berlin", "agent:one", NOW, 0.9, evidence_class=_SD)
    await actions.assert_property(c, "hq", "Munich", "agent:two", NOW + timedelta(minutes=1),
                                  0.9, evidence_class=_SD)
    th = await actions.create_or_find_object("Thread", "thread:filter-duty", t)
    await actions.assert_property(th, "status", "open", t, NOW, 0.9, evidence_class=_SD)
    await actions.assert_property(th, "kind", "obligation", t, NOW, 0.9, evidence_class=_SD)
    await actions.assert_property(th, "summary", "filter test duty", t, NOW, 0.9,
                                  evidence_class=_SD)
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '30 days' WHERE id=$1", th)

    out = await _fn(actions, "lint", {"check": "contradiction"})
    assert {f["check"] for f in out["findings"]} == {"contradiction"}
    assert len(out["findings"]) == 1
    # every OTHER check's true total is still reported — just not listed
    assert out["counts"]["stale-obligation"] >= 1
    assert not any(f["check"] == "stale-obligation" for f in out["findings"])


async def test_lint_check_filter_unknown_check_returns_nothing(actions: Actions) -> None:
    t = "agent:teller"
    c = await actions.create_or_find_object("Organization", "org:tie3", t)
    await actions.assert_property(c, "hq", "Berlin", "agent:one", NOW, 0.9, evidence_class=_SD)
    await actions.assert_property(c, "hq", "Munich", "agent:two", NOW + timedelta(minutes=1),
                                  0.9, evidence_class=_SD)
    out = await _fn(actions, "lint", {"check": "not-a-real-check"})
    assert out["findings"] == []
    assert out["counts"]["contradiction"] == 1  # still counted, just not listed


async def test_lint_unfiltered_call_is_unchanged(actions: Actions) -> None:
    """`check=None` (the default) stays behavior-identical to the pre-existing 50-cap —
    no regression for every caller that never asked for a check filter."""
    t = "agent:teller"
    for i in range(3):
        c = await actions.create_or_find_object("Organization", f"org:many{i}", t)
        await actions.assert_property(c, "hq", "A", "agent:one", NOW, 0.9, evidence_class=_SD)
        await actions.assert_property(c, "hq", "B", "agent:two", NOW + timedelta(minutes=1),
                                      0.9, evidence_class=_SD)
    out = await _fn(actions, "lint", {})
    assert "capped" not in out
    assert len(_by_check(out, "contradiction")) == 3


async def test_lint_check_filter_paginates_beyond_the_50_cap(actions: Actions) -> None:
    """The exact reap pain: more than 50 findings on one check. Unfiltered stays capped
    at 50 with a `capped`/`note` receipt; `check=` lists the FULL set uncapped, and
    `limit`/`offset` page through it."""
    t = "agent:teller"
    for i in range(55):
        c = await actions.create_or_find_object("Organization", f"org:bulk{i:03d}", t)
        await actions.assert_property(c, "hq", "A", "agent:one", NOW, 0.9, evidence_class=_SD)
        await actions.assert_property(c, "hq", "B", "agent:two", NOW + timedelta(minutes=1),
                                      0.9, evidence_class=_SD)
    unfiltered = await _fn(actions, "lint", {})
    assert len(_by_check(unfiltered, "contradiction")) == 50
    assert unfiltered["capped"]["contradiction"] == 5

    full = await _fn(actions, "lint", {"check": "contradiction"})
    assert len(full["findings"]) == 55
    assert "capped" not in full

    paged = await _fn(actions, "lint", {"check": "contradiction", "limit": 10, "offset": 20})
    assert len(paged["findings"]) == 10
    assert paged["capped"]["contradiction"] == 25  # 55 - 20 - 10
    assert "25" in paged["note"]


async def test_lint_orphan_link_check_filter_beyond_its_own_sql_cap(actions: Actions) -> None:
    """orphan-link's own SQL pre-limits its fetch to _LINT_CAP — unlike every other check,
    which fetches its FULL row set and only caps at the display layer. Prove the check
    filter actually raises the SQL-level fetch too, not just the display slice."""
    t = "agent:teller"
    corpse = await actions.create_or_find_object("Organization", "org:manycorpse", t)
    await actions.pool.execute("UPDATE objects SET status='retired' WHERE id=$1", corpse)
    for i in range(55):
        alive = await actions.create_or_find_object("Person", f"person:orphan{i:03d}", t)
        await actions.create_link(alive, corpse, "member_of", t,
                                  NOW + timedelta(seconds=i), 0.9, evidence_class=_SD)
    unfiltered = await _fn(actions, "lint", {})
    assert len(_by_check(unfiltered, "orphan-link")) == 50
    assert unfiltered["counts"]["orphan-link"] == 55

    full = await _fn(actions, "lint", {"check": "orphan-link"})
    assert len(full["findings"]) == 55


async def test_lint_orphan_link_pagination_survives_the_5000_row_hard_cap(
    actions: Actions,
) -> None:
    """Thread 187323d9 / decision 6647fcd5 (Thoth DM 3143), the live specimen: orphan-link's
    own SQL fetch used to hard-cap at min(offset+limit, 5000) regardless of the real
    population — so ANY offset at or past ~5000 silently returned an EMPTY findings list
    while `note`/`capped` still reported a genuine positive remainder (graph_lint(offset=
    5000) and (offset=10600) both returned [] against a real 10,637-row population, both
    claiming a positive remainder). Blindness rendered as silence, never a refusal.
    Reproduced with bulk SQL, not a per-row ORM loop — 5000+ rows one at a time is minutes,
    not seconds."""
    t = "agent:teller"
    corpse = await actions.create_or_find_object("Organization", "org:hugecorpse", t)
    await actions.pool.execute("UPDATE objects SET status='retired' WHERE id=$1", corpse)
    n = 5010
    await actions.pool.execute(
        "WITH new_objs AS (INSERT INTO objects (type, canonical, status) "
        "  SELECT 'Person', 'person:hugebulk'||g, 'active' FROM generate_series(1, $1) g "
        "  RETURNING id) "
        "INSERT INTO links (from_id, to_id, type, source_id, confidence, "
        "  first_seen, last_seen) "
        "SELECT id, $2, 'member_of', $3, 0.9, $4, $4 FROM new_objs",
        n, corpse, t, NOW)

    out = await _fn(actions, "lint", {"check": "orphan-link", "offset": 5000, "limit": 5})
    assert out["counts"]["orphan-link"] == n
    assert len(out["findings"]) == 5  # real rows past the old 5000-row hard fetch cap
    remaining = n - 5000 - 5
    assert out["capped"]["orphan-link"] == remaining
    assert out["note"] is not None and str(remaining) in out["note"]
    # decision 6647fcd5's third defect ("count and listable population disagree") was the
    # same root cause as the pagination bug above, not an independent one — `counts` and
    # the fetched population now agree because the fetch reaches the true total either way.
    assert out["counts"]["orphan-link"] == n


async def test_lint_counts_carries_a_severity_split(actions: Actions) -> None:
    """Thread 187323d9's first defect: `counts` alone mixed info-grade metered history
    (orphan-link) with warn-grade damage (contradiction) in one undifferentiated list — a
    reader trusting it at face value overstated real debt 54x, live. `severity` (per-check)
    and `counts_by_severity` (the rollup) fix that without changing `counts`'s own shape —
    every existing caller reading `counts[check]` as a plain int is unaffected."""
    t = "agent:teller"
    corpse = await actions.create_or_find_object("Organization", "org:sevcorpse", t)
    await actions.pool.execute("UPDATE objects SET status='retired' WHERE id=$1", corpse)
    alive = await actions.create_or_find_object("Person", "person:sevorphan", t)
    await actions.create_link(alive, corpse, "member_of", t, NOW, 0.9, evidence_class=_SD)
    c = await actions.create_or_find_object("Organization", "org:sevcontra", t)
    await actions.assert_property(c, "hq", "A", "agent:one", NOW, 0.9, evidence_class=_SD)
    await actions.assert_property(c, "hq", "B", "agent:two", NOW + timedelta(minutes=1),
                                  0.9, evidence_class=_SD)

    out = await _fn(actions, "lint", {})
    assert out["severity"]["orphan-link"] == "info"
    assert out["severity"]["contradiction"] == "warn"
    assert out["counts"]["orphan-link"] == 1  # counts's own shape is unchanged — a plain int
    assert out["counts_by_severity"]["info"] >= 1
    assert out["counts_by_severity"]["warn"] >= 1


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


async def test_lint_flags_a_live_agent_with_duplicate_works_in_edges(actions: Actions) -> None:
    """DUPLICATE-WORKS-IN (thread 8640a625, John XVII's own specimen): a currently-live
    agent carrying two simultaneously-live works_in edges — orient() resolves through
    exactly one, so the duplicate can hide a live lineage's own threads/decisions from
    itself. Scoped to LIVE agents only (via agent_mounts, the same liveness window
    phantom-twin already uses) — a dead generation's own leftover duplicate is thread
    20af2c95's separate, still-open concern, not this check's."""
    t = "agent:teller"
    live = await actions.create_or_find_object("Agent", "agent:dup1live0", t)
    stale = await actions.create_or_find_object("SoftwareProject", "repo:dup1stale", t)
    keep = await actions.create_or_find_object("SoftwareProject", "repo:dup1keep", t)
    await actions.create_link(live, stale, "works_in", t, NOW, 0.9, evidence_class=_SD)
    await actions.create_link(live, keep, "works_in", t, NOW, 0.9, evidence_class=_SD)
    await mounts.save_mount(actions.pool, job_dir="/x/jobs/dup1live0",
                            agent_id="agent:dup1live0", project="dup1keep", cwd="/w/dup1",
                            model=None, session_key=None)

    out = await _fn(actions, "lint", {})

    dup = _by_check(out, "duplicate-works-in")
    assert [f["subject"] for f in dup] == ["agent:dup1live0"]
    assert dup[0]["severity"] == "warn"
    assert "repo:dup1keep" in dup[0]["detail"] and "repo:dup1stale" in dup[0]["detail"]
    assert "invalidate_works_in" in dup[0]["detail"]


async def test_lint_does_not_flag_a_dead_generations_duplicate_works_in(
    actions: Actions,
) -> None:
    """The historical-noise exclusion: the same duplicate shape on an agent with NO live
    mount stays silent here — nobody's orient() is resolving through it right now, and
    that larger pile is thread 20af2c95's own separate concern."""
    t = "agent:teller"
    dead = await actions.create_or_find_object("Agent", "agent:dup2dead0", t)
    p1 = await actions.create_or_find_object("SoftwareProject", "repo:dup2p1", t)
    p2 = await actions.create_or_find_object("SoftwareProject", "repo:dup2p2", t)
    await actions.create_link(dead, p1, "works_in", t, NOW, 0.9, evidence_class=_SD)
    await actions.create_link(dead, p2, "works_in", t, NOW, 0.9, evidence_class=_SD)
    # never mounted at all — no agent_mounts row for this agent

    out = await _fn(actions, "lint", {})

    assert _by_check(out, "duplicate-works-in") == []


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


async def test_lint_flags_a_silent_peer_pair(actions: Actions) -> None:
    """PEER-SILENT (task #76 item 2, spec e6636c7e): an active peer_of pair with no DM
    ever seen between either side's holders is flagged as a proxy for the fiduciary-
    disclosure duty ("silence is a violation") — testimony, not proof a finding was
    withheld."""
    from src.orchestrator.seats import bind_holder, peer_seats

    t = "agent:teller"
    await actions.create_or_find_object("Seat", "seat:ps1aaaaa", t)
    await actions.create_or_find_object("Seat", "seat:ps1bbbbb", t)
    await bind_holder(actions, seat_id="seat:ps1aaaaa", agent_id="agent:ps1holda")
    await bind_holder(actions, seat_id="seat:ps1bbbbb", agent_id="agent:ps1holdb")
    await peer_seats(actions, "seat:ps1aaaaa", "seat:ps1bbbbb", because="the pairing",
                     actor=t)

    out = await _fn(actions, "lint", {"stale_days": 14})

    silent = _by_check(out, "peer-silent")
    assert [f["subject"] for f in silent] == ["seat:ps1aaaaa <-> seat:ps1bbbbb"]
    assert silent[0]["severity"] == "warn"
    assert "no direct mail" in silent[0]["detail"]
    assert "the pairing" in silent[0]["detail"]


async def test_lint_does_not_flag_a_peer_pair_that_talked_recently(
    actions: Actions,
) -> None:
    """A DM between the two holders, inside the window, clears the pair — direct contact
    is the signal this proxy actually measures."""
    from src.orchestrator.seats import bind_holder, peer_seats

    t = "agent:teller"
    await actions.create_or_find_object("Seat", "seat:ps2aaaaa", t)
    await actions.create_or_find_object("Seat", "seat:ps2bbbbb", t)
    await bind_holder(actions, seat_id="seat:ps2aaaaa", agent_id="agent:ps2holda")
    await bind_holder(actions, seat_id="seat:ps2bbbbb", agent_id="agent:ps2holdb")
    await peer_seats(actions, "seat:ps2aaaaa", "seat:ps2bbbbb", because="the pairing",
                     actor=t)
    await actions.pool.execute(
        "INSERT INTO fleet_messages (from_agent, to_agent, body, created_at) "
        "VALUES ($1, $2, $3, now())", "agent:ps2holda", "agent:ps2holdb", "checking in")

    out = await _fn(actions, "lint", {"stale_days": 14})

    assert _by_check(out, "peer-silent") == []


async def test_lint_flags_a_peer_pair_whose_only_contact_is_stale(actions: Actions) -> None:
    """Contact that happened, but outside the window, still reads as silence right now —
    `stale_days` is the same rolling-window law stale-obligation already applies."""
    from src.orchestrator.seats import bind_holder, peer_seats

    t = "agent:teller"
    await actions.create_or_find_object("Seat", "seat:ps3aaaaa", t)
    await actions.create_or_find_object("Seat", "seat:ps3bbbbb", t)
    await bind_holder(actions, seat_id="seat:ps3aaaaa", agent_id="agent:ps3holda")
    await bind_holder(actions, seat_id="seat:ps3bbbbb", agent_id="agent:ps3holdb")
    await peer_seats(actions, "seat:ps3aaaaa", "seat:ps3bbbbb", because="the pairing",
                     actor=t)
    stale_at = datetime.now(UTC) - timedelta(days=30)
    await actions.pool.execute(
        "INSERT INTO fleet_messages (from_agent, to_agent, body, created_at) "
        "VALUES ($1, $2, $3, $4)", "agent:ps3holda", "agent:ps3holdb", "long ago", stale_at)

    out = await _fn(actions, "lint", {"stale_days": 14})

    silent = _by_check(out, "peer-silent")
    assert [f["subject"] for f in silent] == ["seat:ps3aaaaa <-> seat:ps3bbbbb"]
    assert "last direct mail" in silent[0]["detail"]


async def test_lint_peer_silent_counts_seat_addressed_mail_as_contact(
    actions: Actions,
) -> None:
    """A DM addressed to the SEAT itself (`to_agent='seat:...'`, holds-resolved to
    whichever generation is live) still counts as contact — it reaches the same peer
    regardless of which generation happens to be holding at read time."""
    from src.orchestrator.seats import bind_holder, peer_seats

    t = "agent:teller"
    await actions.create_or_find_object("Seat", "seat:ps4aaaaa", t)
    await actions.create_or_find_object("Seat", "seat:ps4bbbbb", t)
    await bind_holder(actions, seat_id="seat:ps4aaaaa", agent_id="agent:ps4holda")
    await bind_holder(actions, seat_id="seat:ps4bbbbb", agent_id="agent:ps4holdb")
    await peer_seats(actions, "seat:ps4aaaaa", "seat:ps4bbbbb", because="the pairing",
                     actor=t)
    await actions.pool.execute(
        "INSERT INTO fleet_messages (from_agent, to_agent, body, created_at) "
        "VALUES ($1, $2, $3, now())", "agent:ps4holda", "seat:ps4bbbbb", "checking in")

    out = await _fn(actions, "lint", {"stale_days": 14})

    assert _by_check(out, "peer-silent") == []


async def test_lint_peer_silent_ignores_a_project_broadcast(actions: Actions) -> None:
    """A broadcast (`to_agent` NULL, `to_project` set) is NOT counted as disclosure —
    neither peer need have actually read it, so crediting it would hide real silence."""
    from src.orchestrator.seats import bind_holder, peer_seats

    t = "agent:teller"
    await actions.create_or_find_object("Seat", "seat:ps5aaaaa", t)
    await actions.create_or_find_object("Seat", "seat:ps5bbbbb", t)
    await bind_holder(actions, seat_id="seat:ps5aaaaa", agent_id="agent:ps5holda")
    await bind_holder(actions, seat_id="seat:ps5bbbbb", agent_id="agent:ps5holdb")
    await peer_seats(actions, "seat:ps5aaaaa", "seat:ps5bbbbb", because="the pairing",
                     actor=t)
    await actions.pool.execute(
        "INSERT INTO fleet_messages (from_agent, to_project, body, created_at) "
        "VALUES ($1, $2, $3, now())", "agent:ps5holda", "osiris", "broadcast, not a DM")

    out = await _fn(actions, "lint", {"stale_days": 14})

    silent = _by_check(out, "peer-silent")
    assert [f["subject"] for f in silent] == ["seat:ps5aaaaa <-> seat:ps5bbbbb"]


async def _hold_thread(
    actions: Actions, *, holder: str, held: str, act: str, deadline: datetime,
) -> str:
    """Mirrors hold_action()'s own written shape by hand — this branch predates item 4a's
    merge, so there's no hold_action() import to reuse; the lint check only ever reads the
    property names, never the verb that wrote them."""
    from src.orchestrator.capture import open_thread

    t = await open_thread(actions, f"HOLD by {holder} on {held}'s act ({act}): test",
                          kind="obligation", owner=held, severity="hold", source="test")
    for name, value in (
        ("hold_holder", holder), ("hold_held", held), ("hold_act", act),
        ("hold_because", "test"), ("hold_deadline", deadline.isoformat()),
    ):
        await actions.assert_property(t, name, value, "test", NOW, 0.9, evidence_class=_SD)
    return str(t)


async def test_lint_flags_a_hold_past_its_deadline(actions: Actions) -> None:
    """HELD-PAST-DEADLINE (task #76 item 4b): a mutual HOLD's time-box, expired, with no
    resolve_thread call yet — the spec's auto-escalation half, surfaced as testimony
    rather than pushed anywhere, matching Thoth's own ruling (lint, not a daemon)."""
    past = NOW - timedelta(hours=1)
    await _hold_thread(actions, holder="seat:hp1aaaaa", held="seat:hp1bbbbb",
                       act="deleting the shared checkout", deadline=past)

    out = await _fn(actions, "lint", {})

    held = _by_check(out, "held-past-deadline")
    assert [f["subject"] for f in held] == [
        "seat:hp1aaaaa holding seat:hp1bbbbb's act (deleting the shared checkout)"]
    assert held[0]["severity"] == "warn"
    assert past.isoformat() in held[0]["detail"]


async def test_lint_does_not_flag_a_hold_still_inside_its_window(actions: Actions) -> None:
    # NOW is a fixed HISTORICAL fake date (this file's own convention) — the deadline
    # comparison is against the database's real `now()`, so "still inside its window"
    # needs a genuinely future wall-clock timestamp, not NOW + an offset.
    future = datetime.now(UTC) + timedelta(hours=1)
    await _hold_thread(actions, holder="seat:hp2aaaaa", held="seat:hp2bbbbb", act="test",
                       deadline=future)

    out = await _fn(actions, "lint", {})

    assert _by_check(out, "held-past-deadline") == []


async def test_lint_does_not_flag_a_resolved_hold_past_its_deadline(
    actions: Actions,
) -> None:
    """Resolved is not held — the same law every other obligation follows."""
    from src.orchestrator.capture import resolve_thread

    past = NOW - timedelta(hours=1)
    tid = await _hold_thread(actions, holder="seat:hp3aaaaa", held="seat:hp3bbbbb",
                             act="test", deadline=past)
    await resolve_thread(actions, tid, because="respected the hold", source="test")

    out = await _fn(actions, "lint", {})

    assert _by_check(out, "held-past-deadline") == []


async def test_lint_does_not_flag_an_ordinary_obligation_with_no_severity(
    actions: Actions,
) -> None:
    """An obligation with no `severity='hold'` at all is a different check's business
    (stale-obligation) — this one is scoped to holds specifically, never every open
    thread."""
    from src.orchestrator.capture import open_thread

    await open_thread(actions, "an ordinary obligation, unrelated to any hold",
                      kind="obligation", owner="seat:hp4aaaaa", source="test")

    out = await _fn(actions, "lint", {})

    assert _by_check(out, "held-past-deadline") == []


async def test_lint_flags_a_stale_off_head_link(actions: Actions) -> None:
    """STALE-OFF-HEAD-LINK (thread 20af2c95, Thoth DM 5341 — recurrence detection): the
    write-side fix (mint_heir invalidating a predecessor's works_in onto its heir) landed
    2026-08-04, but nothing watches for the CLASS recurring. An ancestor whose lineage has
    since minted a living heir, yet still carries its own live works_in edge (simulating
    debt from BEFORE the write-side fix, or a future regression of it), is flagged —
    backfill_agent_project_links is the repair, this check only counts."""
    t = "agent:teller"
    ancestor = await actions.create_or_find_object("Agent", "agent:staleoh01", t)
    heir = await actions.create_or_find_object("Agent", "agent:staleoh01-ii", t)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:staleoh1", t)
    await actions.assert_property(ancestor, "succeeded_by", "agent:staleoh01-ii", t, NOW, 0.9,
                                  evidence_class=_SD)
    await actions.create_link(heir, ancestor, "succeeded_from", t, NOW, 0.9, evidence_class=_SD)
    await actions.create_link(ancestor, proj, "works_in", t, NOW, 0.9, evidence_class=_SD)

    out = await _fn(actions, "lint", {})

    stale = _by_check(out, "stale-off-head-link")
    assert [f["subject"] for f in stale] == ["agent:staleoh01"]
    assert stale[0]["severity"] == "warn"
    assert "repo:staleoh1" in stale[0]["detail"]
    assert "agent:staleoh01-ii" in stale[0]["detail"]
    assert "backfill_agent_project_links" in stale[0]["detail"]


async def test_lint_does_not_flag_the_living_heads_own_works_in(actions: Actions) -> None:
    """The living head's OWN works_in edge is exactly the correct, current state — never
    flagged by the same check that catches its ancestor's stale leftover."""
    t = "agent:teller"
    ancestor = await actions.create_or_find_object("Agent", "agent:staleoh02", t)
    heir = await actions.create_or_find_object("Agent", "agent:staleoh02-ii", t)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:staleoh2", t)
    await actions.assert_property(ancestor, "succeeded_by", "agent:staleoh02-ii", t, NOW, 0.9,
                                  evidence_class=_SD)
    await actions.create_link(heir, ancestor, "succeeded_from", t, NOW, 0.9, evidence_class=_SD)
    await actions.create_link(heir, proj, "works_in", t, NOW, 0.9, evidence_class=_SD)

    out = await _fn(actions, "lint", {})

    assert [f["subject"] for f in _by_check(out, "stale-off-head-link")
            if f["subject"] == "agent:staleoh02"] == []


async def test_lint_flags_a_stale_current_flag(actions: Actions) -> None:
    """STALE-CURRENT-FLAG (thread 09bde57e, Thoth DM 5341 — recurrence detection): khepri's
    own specimen — a real `supersedes` FK exists but `is_current` was never flipped false
    on the row it supersedes (a migration-0047 backfill gap), so current_assertions still
    lists the superseded value. repair_stale_current_flags is the batched repair; this
    check only counts."""
    t = "agent:teller"
    obj = await actions.create_or_find_object("Person", "person:stalecur1", t)
    old_id = await actions.pool.fetchval(
        "INSERT INTO assertions (object_id, name, value, source_id, observed_at, "
        " confidence, evidence_class, is_current) "
        "VALUES ($1, 'name', '\"old\"'::jsonb, $2, $3, 0.9, 'self_declared', true) "
        "RETURNING id", obj, t, NOW)
    await actions.pool.execute(
        "INSERT INTO assertions (object_id, name, value, source_id, observed_at, "
        " confidence, evidence_class, is_current, supersedes) "
        "VALUES ($1, 'name', '\"new\"'::jsonb, $2, $3, 0.9, 'self_declared', true, $4)",
        obj, t, NOW + timedelta(hours=1), old_id)

    out = await _fn(actions, "lint", {})

    stale = _by_check(out, "stale-current-flag")
    assert [f["subject"] for f in stale] == [str(obj)]
    assert stale[0]["severity"] == "warn"
    assert str(old_id) in stale[0]["detail"]
    assert "repair_stale_current_flags" in stale[0]["detail"]


async def test_lint_does_not_flag_a_correctly_flipped_supersession(actions: Actions) -> None:
    """The ordinary, correct case — the superseded row's own is_current was properly
    flipped false — must never be flagged; this check is for the ANOMALY only."""
    t = "agent:teller"
    obj = await actions.create_or_find_object("Person", "person:stalecur2", t)
    await actions.assert_property(obj, "name", "old", t, NOW, 0.9, evidence_class=_SD)
    await actions.assert_property(obj, "name", "new", t, NOW + timedelta(hours=1), 0.9,
                                  evidence_class=_SD)

    out = await _fn(actions, "lint", {})

    assert [f for f in _by_check(out, "stale-current-flag")
            if f["subject"] == str(obj)] == []

"""P2 of the composer — the opinionated read-models leave the engine as FUNCTIONS.

coinvest / subject_report / screen_network are not pure op-trees: their precision lives
in domain logic the closed ops can't express (merge-aware cluster resolution, a platform-
degree filter, multi-signal fuzzy matching). That is exactly Palantir's split — a small
closed op set PLUS a Function escape hatch. So the eviction keeps every drop of the
analytics: the logic is registered as a named Function, and a forkable composition merely
REFERENCES it ({"op":"function","name":...}). The proof each must pass: running the
composition is byte-equal to calling the bespoke read-model directly (opinion moved, did
not degrade).
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from src.actions.core import Actions
from src.ingest.opensanctions import ingest_ftm
from src.ontology.resolution import screen_network
from src.orchestrator.coinvest import coinvestment_ties
from src.orchestrator.compositions import (
    list_functions,
    run_composition,
    run_spec,
    save_composition,
    seed_default_compositions,
)
from src.orchestrator.frontier import subject_report
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

NOW = datetime(2026, 6, 28, tzinfo=UTC)


# --- coinvest ---------------------------------------------------------------

async def _spv(actions: Actions, spv: str, operator: str, target: uuid.UUID) -> None:
    s = await actions.create_or_find_object("Organization", spv, "edgar")
    op = await actions.create_or_find_object("Person", operator, "edgar")
    await actions.assert_property(op, "name", operator.split(":")[-1], "edgar", NOW, 0.85)
    await actions.create_link(s, op, "officer", "edgar", NOW, 0.85)
    await actions.create_link(s, target, "raises_for", "edgar", NOW, 0.35)


async def test_coinvest_function_is_byte_equal(actions: Actions) -> None:
    neuralink = await actions.create_or_find_object("Organization", "company:n", "edgar")
    await actions.assert_property(neuralink, "name", "Neuralink", "edgar", NOW, 0.85)
    openai = await actions.create_or_find_object("Organization", "company:o", "edgar")
    await actions.assert_property(openai, "name", "OpenAI", "edgar", NOW, 0.85)
    await _spv(actions, "org:s1", "sec-person:Alpha Ventures", neuralink)
    await _spv(actions, "org:s2", "sec-person:Alpha Ventures", openai)
    await _spv(actions, "org:m1", "sec-person:Beta Capital", neuralink)
    await _spv(actions, "org:m2", "sec-person:Beta Capital", openai)

    await seed_default_compositions(actions.pool)
    direct = await coinvestment_ties(actions.pool, neuralink)
    via = await run_composition(actions.pool, "co-investment-ties", neuralink)
    assert via["kind"] == "data"
    assert via["items"] == direct           # the precise tie list, unchanged
    assert direct[0]["company"] == "OpenAI"  # and it's the REAL (non-degraded) analytic
    assert direct[0]["shared_operators"] == 2


# --- screen_network ---------------------------------------------------------

async def test_screen_function_is_byte_equal(actions: Actions) -> None:
    await ingest_ftm(actions, [{"id": "W1", "schema": "Person", "properties": {
        "name": ["Dmitry Dirtyhands"], "topics": ["sanction"]}}])
    company = await actions.create_or_find_object("Organization", "cik:42", "edgar")
    await actions.assert_property(company, "name", "Target Corp", "edgar", NOW, 0.85)
    spv = await actions.create_or_find_object("Organization", "cik:99", "edgar")
    await actions.assert_property(spv, "name", "Feeder SPV LP", "edgar", NOW, 0.85)
    await actions.create_link(spv, company, "raises_for", "edgar", NOW, 0.35)
    dirty = await actions.create_or_find_object("Person", "sec-person:dirty", "edgar")
    await actions.assert_property(dirty, "name", "Dmitry Dirtyhands", "edgar", NOW, 0.85)
    await actions.create_link(spv, dirty, "officer", "edgar", NOW, 0.85)

    await seed_default_compositions(actions.pool)
    direct = await screen_network(actions.pool, company)
    via = await run_composition(actions.pool, "screen-financing-network", company)
    assert via["items"] == direct
    assert direct[0]["member_name"] == "Dmitry Dirtyhands"  # the dirty operator survives


# --- subject_report (the subject is the CASE id here) -----------------------

async def test_subject_report_function_is_byte_equal(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    seed = await actions.create_or_find_object("Username", "asuramaya", "analyst:test", cid)
    acct = await actions.create_or_find_object("Account", "soundcloud:wrenaudio7", "helper", cid)
    await actions.pool.execute(
        "UPDATE case_objects SET hop_distance=1 WHERE case_id=$1 AND object_id=$2", cid, acct)
    await actions.create_link(seed, acct, "declares", "helper", NOW,
                              confidence_for(EvidenceClass.SELF_DECLARED), case_id=cid,
                              evidence_class=EvidenceClass.SELF_DECLARED.value)

    await seed_default_compositions(actions.pool)
    direct = await subject_report(actions.pool, cid)
    via = await run_composition(actions.pool, "who-is-this", cid)
    assert via["items"] == direct
    assert {f["canonical"] for f in direct["verified"]} >= {"asuramaya", "soundcloud:wrenaudio7"}


# --- the registry + guards --------------------------------------------------

async def test_function_registry_is_listable(actions: Actions) -> None:
    # `briefing`/`decisions` are NOT here — they decomposed into op-trees (see the tests below).
    assert list_functions() == ["canon", "coinvest", "echoes", "family",
                                "family_drift", "lap", "lint", "portfolio", "project",
                                "pulse", "screen_network", "search", "subject_report",
                                "wall"]


async def test_briefing_is_a_sections_op_tree(actions: Actions) -> None:
    """The eviction proof: `briefing` is no longer a hand-written Function — it's a `sections`
    op-tree (select→table per section) the user owns. Same orientation read-model (open threads,
    recent work, self-healed threads), no bespoke SQL. A "briefing" is a PAGE OF COMPOSITIONS."""
    cm = await actions.create_or_find_object("Commit", "commit:aa", "git")
    await actions.assert_property(cm, "summary", "ship the thing", "git", NOW, 0.85)
    await actions.assert_property(cm, "scope", "ui", "git", NOW, 0.85)
    await actions.assert_property(cm, "authored_date", "2026-06-29T00:00:00+00:00",
                                  "git", NOW, 0.85)
    th = await actions.create_or_find_object("Thread", "thread:1", "git-memory")
    await actions.assert_property(th, "summary", "THE WALL: needs portal access", "git-memory",
                                 NOW, 0.4)
    await actions.assert_property(th, "status", "open", "git-memory", NOW, 0.4)
    # a thread a later commit already closed — must self-heal OUT of the open list and INTO
    # the resolved section, carrying the provenance of why it was closed.
    done = await actions.create_or_find_object("Thread", "thread:2", "git-memory")
    await actions.assert_property(done, "summary", "NEXT: build the renderer", "git-memory",
                                  NOW, 0.4)
    await actions.assert_property(done, "status", "resolved", "git-memory", NOW, 0.4)
    await actions.assert_property(done, "resolved_in", "commit:zz", "git-memory", NOW, 0.4)
    await actions.assert_property(done, "resolved_because", "renderer, generic", "git-memory",
                                  NOW, 0.4)

    await seed_default_compositions(actions.pool)      # `briefing` is now a seeded sections tree
    assert "briefing" not in list_functions()          # the hand-written Function is gone
    # runs with NO subject (it briefs the project, not an entity)
    res = await run_composition(actions.pool, "briefing")
    assert res["kind"] == "data"
    wall = next(v for k, v in res["items"].items() if "wall" in k)
    recent = next(v for k, v in res["items"].items() if "Recent work" in k)
    healed = next(v for k, v in res["items"].items() if "Resolved" in k)
    # the open section is the GRADED wall (ruling 923c380f): the untouched kindless thread
    # counts in the pile, never as a raw row; the resolved one is in neither
    assert wall["totals"]["open"] == 1 and wall["totals"]["pile"] == 1
    assert not any("renderer" in t["summary"] for t in wall["top_of_wall"])
    assert any(r["change"] == "ship the thing" and r["scope"] == "ui" for r in recent)
    assert any(h["by"] == "commit:zz" and "renderer" in h["because"] for h in healed)


async def test_sections_op_stacks_mixed_body_kinds(actions: Actions) -> None:
    """The `sections` primitive: a page of compositions. Each body is its own op-tree and is
    packaged the way a top-level composition would be — an `objects` body becomes labelled rows,
    a `table` body stays rows — so a briefing/dossier is stackable primitives, not coded output."""
    d = await actions.create_or_find_object("Decision", "decision:1", "mine")
    await actions.assert_property(d, "summary", "keyless by design", "mine", NOW, 0.85)
    c = await actions.create_or_find_object("Commit", "commit:x", "git")
    await actions.assert_property(c, "summary", "wire the sections op", "git", NOW, 0.85)

    res = await run_spec(actions.pool, {"op": "sections", "sections": [
        {"title": "Decisions", "body": {"op": "select", "object_type": "Decision"}},
        {"title": "Commits", "body": {"op": "table",
                                      "from": {"op": "select", "object_type": "Commit"},
                                      "columns": [{"name": "msg", "property": "summary"}]}},
    ]}, None)
    assert res["kind"] == "data"
    # objects body → labelled rows (object_items shape); table body → the plain rollup rows
    assert res["items"]["Decisions"][0]["label"] == "keyless by design"
    assert res["items"]["Commits"][0]["msg"] == "wire the sections op"


async def test_rollup_show_original_plucks_a_single_relation(actions: Actions) -> None:
    """`of:"first"` (Notion's show-original): a table rollup over a single relation plucks the
    related object's value — a property AND an object column (`canonical`). This is the enabler
    for a decision showing its `decided_in` commit's hash + date without abusing max(). No
    dedicated Function needed: the linked commit is named by a pure op-tree."""
    d = await actions.create_or_find_object("Decision", "decision:keyless", "mine")
    await actions.assert_property(d, "summary", "keyless by design", "mine", NOW, 0.85)
    c = await actions.create_or_find_object("Commit", "commit:abc123", "git")
    await actions.assert_property(c, "authored_date", "2026-06-30T12:00:00+00:00", "git", NOW, 0.9)
    await actions.create_link(d, c, "decided_in", "mine", NOW, 0.9)

    res = await run_spec(actions.pool, {"op": "table",
        "from": {"op": "select", "object_type": "Decision"},
        "columns": [
            {"name": "decision", "property": "summary"},
            {"name": "in", "rollup": {"direction": "out", "link_type": "decided_in",
                                      "of": "first", "property": "canonical"}},
            {"name": "when", "rollup": {"direction": "out", "link_type": "decided_in",
                                        "of": "first", "property": "authored_date"}},
        ]}, None)
    row = res["items"][0]
    assert row["decision"] == "keyless by design"
    assert row["in"] == "commit:abc123"                     # object column, plucked (show-original)
    assert row["when"] == "2026-06-30T12:00:00+00:00"       # a property of the single relation


async def test_unknown_function_and_missing_subject_raise(actions: Actions) -> None:
    await save_composition(actions.pool, "bogus", {"op": "function", "name": "nope"})
    with pytest.raises(ValueError, match="unknown function"):
        await run_composition(actions.pool, "bogus", uuid.uuid4())
    await save_composition(actions.pool, "needs-subj", {"op": "function", "name": "coinvest"})
    with pytest.raises(ValueError, match="requires a subject"):
        await run_composition(actions.pool, "needs-subj", None)


# --- canon: consult the design canon (cite, don't re-derive) ----------------

async def _ref(actions: Actions, canon: str, title: str, vendor: str, grounds: str,
               body: str) -> None:
    r = await actions.create_or_find_object("Reference", canon, f"ref:{vendor}")
    for name, value in (("name", title), ("vendor", vendor), ("grounds", grounds),
                        ("body", body)):
        await actions.assert_property(r, name, value, f"ref:{vendor}", NOW, 0.85)


async def test_canon_retrieves_ranked_sections(actions: Actions) -> None:
    """The keystone: a design query returns the matching canon SECTIONS, ranked, each carrying
    the module it grounds — what a designer calls BEFORE re-deriving a solved problem."""
    await _ref(actions, "ref:palantir-object-sets", "Object Sets", "palantir",
               "src/orchestrator/compositions.py",
               "# Object Sets\n\nThe closed op vocabulary.\n\n## Operations\nfilter, traverse, "
               "aggregate; relating two sets is set algebra — never a join.\n\n## Functions\n"
               "the escape hatch for anything the ops can't express.")
    await _ref(actions, "ref:notion-uiux", "Calm UI", "notion", "src/ui/static/index.html",
               "# Calm UI\n\nintro.\n\n## Progressive disclosure\nhide complexity until asked.")

    # query by a design word → the object-sets doc ranks top, its section is returned
    res = await run_spec(actions.pool, {"op": "function", "name": "canon",
                                        "args": {"q": "join"}}, None, name="design-canon")
    assert res["kind"] == "data"
    hits = next(iter(res["items"].values()))
    assert hits and hits[0]["reference"] == "Object Sets"
    assert any("never a join" in h["text"] for h in hits)
    assert hits[0]["grounds"] == "src/orchestrator/compositions.py"   # carries what it grounds

    # query by a MODULE PATH (the grounds field) → surfaces that doc even with no body match
    bymod = await run_spec(actions.pool, {"op": "function", "name": "canon",
                                          "args": {"q": "index.html"}}, None)
    modhits = next(iter(bymod["items"].values()))
    assert modhits and all(h["reference"] == "Calm UI" for h in modhits)

    # empty query → the canon INDEX: one overview row per reference, subject-free (no raise)
    idx = await run_spec(actions.pool, {"op": "function", "name": "canon", "args": {}}, None)
    idxhits = next(iter(idx["items"].values()))
    assert len(idxhits) == 2 and {h["reference"] for h in idxhits} == {"Object Sets", "Calm UI"}


async def test_canon_recall_is_keyword_ranked_and_project_scoped(actions: Actions) -> None:
    """Bug #1 (Heinrich's first session): the migration's RECALL path. bootstrap ingests project
    history as ref:<project>-* Reference nodes and promises consult_canon retrieves it — but
    _fn_canon matched the query as a CONTIGUOUS substring, so a natural multi-keyword query
    returned EMPTY, silently breaking 'history becomes a bounded query'. Now it is keyword-ranked
    and scoped to the caller's history + the shared design canon (no cross-project bleed)."""
    src = "ref:heinrich"
    h = await actions.create_or_find_object("Reference", "ref:heinrich-history-homing", src)
    for name, val in (("name", "Homing instinct study"), ("topic", "heinrich-history"),
                      ("body", "## Findings\nthe homing displacement held above baseline across "
                               "seeds.")):
        await actions.assert_property(h, name, val, src, NOW, 0.9)
    # a shared design-canon ref (vendor-tagged → visible to everyone)
    await _ref(actions, "ref:palantir-os", "Object Sets", "palantir", "src/x.py",
               "# Object Sets\n\nthe closed op set, never a generic join.")
    # ANOTHER project's unvendored history — must be scoped OUT for a heinrich caller
    o = await actions.create_or_find_object("Reference", "ref:osiris-history-base", "ref:osiris")
    for name, val in (("name", "Osiris baseline"),
                      ("body", "## note\nthe osiris baseline displacement metric.")):
        await actions.assert_property(o, name, val, "ref:osiris", NOW, 0.9)

    # the multi-keyword query that used to return EMPTY now recalls the project history
    res = await run_spec(actions.pool, {"op": "function", "name": "canon", "args":
                         {"q": "frozen homing displacement baseline", "project": "heinrich"}}, None)
    refs = {r["reference"] for r in next(iter(res["items"].values()))}
    assert "Homing instinct study" in refs        # keyword recall works (was empty before)
    assert "Osiris baseline" not in refs          # another project's history is scoped OUT

    # the shared design canon stays reachable under a project scope
    canon = await run_spec(actions.pool, {"op": "function", "name": "canon", "args":
                           {"q": "generic join", "project": "heinrich"}}, None)
    assert "Object Sets" in {r["reference"] for r in next(iter(canon["items"].values()))}


async def test_canon_ranks_reordered_and_partial_multiword_queries(actions: Actions) -> None:
    """The tokenizer's core guarantee: scoring sums INDEPENDENT per-token hits, so a query
    matches regardless of token ORDER and with no contiguous phrase — the exact failure (a
    whole-string match) that returned empty for natural queries. A reordered query and a
    strict-subset query both recall the section; single-token ranking is unchanged."""
    await _ref(actions, "ref:palantir-object-sets", "Object Sets", "palantir", "src/x.py",
               "# Object Sets\n\n## Traversal\nfilter then traverse the object set to a "
               "neighboring set; relating two sets is set algebra.")

    # tokens in a DIFFERENT order than the document, none contiguous with the doc phrasing
    reordered = await run_spec(actions.pool, {"op": "function", "name": "canon",
                               "args": {"q": "traverse filter algebra"}}, None)
    assert "Object Sets" in {r["reference"] for r in next(iter(reordered["items"].values()))}

    # a strict subset of the tokens still matches — scoring is additive, not all-or-none
    partial = await run_spec(actions.pool, {"op": "function", "name": "canon",
                             "args": {"q": "algebra traverse"}}, None)
    assert "Object Sets" in {r["reference"] for r in next(iter(partial["items"].values()))}

    # single-token behavior is unchanged (the pre-tokenization contract still holds)
    single = await run_spec(actions.pool, {"op": "function", "name": "canon",
                            "args": {"q": "algebra"}}, None)
    assert "Object Sets" in {r["reference"] for r in next(iter(single["items"].values()))}


async def test_canon_is_subject_free_and_seeded(actions: Actions) -> None:
    """`canon` is registered + subject-free (a design question, not an entity), and the
    `design-canon` view is a default composition the operator can switch to."""
    assert "canon" in list_functions()
    await seed_default_compositions(actions.pool)
    res = await run_composition(actions.pool, "design-canon")     # NO subject → must not raise
    assert res["kind"] == "data"


# --- family consistency audit (the developer persona, multi-repo) ------------

async def _file(actions: Actions, repo: uuid.UUID, rname: str, path: str, role: str) -> None:
    f = await actions.create_or_find_object("File", f"file:{rname}/{path}", "git-tree")
    await actions.assert_property(f, "name", path, "git-tree", NOW, 0.9)
    if role:
        await actions.assert_property(f, "role", role, "git-tree", NOW, 0.9)
    await actions.create_link(f, repo, "in_repo", "git-tree", NOW, 0.9)


async def test_family_consistency_audit(actions: Actions) -> None:
    """Block files by ROLE across the family; a role present in some-but-not-all is the
    inconsistency. Here kast lacks CI that phanspeed has — exactly what an audit should flag."""
    a = await actions.create_or_find_object("SoftwareProject", "repo:phanspeed", "git")
    await actions.assert_property(a, "name", "phanspeed", "git", NOW, 0.9)
    b = await actions.create_or_find_object("SoftwareProject", "repo:kast", "git")
    await actions.assert_property(b, "name", "kast", "git", NOW, 0.9)
    await _file(actions, a, "phanspeed", "LICENSE", "license")
    await _file(actions, a, "phanspeed", "README.md", "readme")
    await _file(actions, a, "phanspeed", ".github/workflows/ci.yml", "ci")
    await _file(actions, b, "kast", "LICENSE", "license")
    await _file(actions, b, "kast", "README.md", "readme")          # kast has NO ci

    res = await run_spec(actions.pool, {"op": "function", "name": "family", "args": {}}, None)
    assert res["kind"] == "data"
    rows = next(iter(res["items"].values()))
    by = {r["role"]: r for r in rows}
    assert by["ci"]["consistent"] is False and "kast" in by["ci"]["missing"]   # the finding
    assert by["license"]["consistent"] is True and by["readme"]["consistent"] is True
    assert rows[0]["role"] == "ci"                                  # inconsistencies sort first
    # scoping to one repo (or none) can't audit a family
    solo = await run_spec(actions.pool, {"op": "function", "name": "family",
                                         "args": {"repos": ["phanspeed"]}}, None)
    assert "need ≥2 repos" in str(next(iter(solo["items"].values())))


async def test_family_content_drift(actions: Actions) -> None:
    """Content drift: two repos both HAVE a license (presence-consistent) but of different
    TYPES — the deeper audit flags it. A shared .gitignore that diverged is flagged too; an
    identical one is not."""
    a = await actions.create_or_find_object("SoftwareProject", "repo:phanspeed", "git")
    await actions.assert_property(a, "name", "phanspeed", "git", NOW, 0.9)
    b = await actions.create_or_find_object("SoftwareProject", "repo:coldspot", "git")
    await actions.assert_property(b, "name", "coldspot", "git", NOW, 0.9)

    async def lic(repo: uuid.UUID, rn: str, ltype: str) -> None:
        f = await actions.create_or_find_object("File", f"file:{rn}/LICENSE", "git-tree")
        await actions.assert_property(f, "role", "license", "git-tree", NOW, 0.9)
        await actions.assert_property(f, "content_hash", f"h-{ltype}", "git-tree", NOW, 0.9)
        await actions.assert_property(f, "license_type", ltype, "git-tree", NOW, 0.9)
        await actions.create_link(f, repo, "in_repo", "git-tree", NOW, 0.9)

    async def gi(repo: uuid.UUID, rn: str, h: str) -> None:
        f = await actions.create_or_find_object("File", f"file:{rn}/.gitignore", "git-tree")
        await actions.assert_property(f, "role", "gitignore", "git-tree", NOW, 0.9)
        await actions.assert_property(f, "content_hash", h, "git-tree", NOW, 0.9)
        await actions.create_link(f, repo, "in_repo", "git-tree", NOW, 0.9)

    await lic(a, "phanspeed", "MIT")
    await lic(b, "coldspot", "Apache-2.0")        # same role, different license TYPE → drift
    await gi(a, "phanspeed", "same")
    await gi(b, "coldspot", "same")               # identical .gitignore → no drift

    res = await run_spec(actions.pool, {"op": "function", "name": "family_drift",
                                        "args": {}}, None)
    rows = next(iter(res["items"].values()))
    by = {r["role"]: r for r in rows}
    assert by["license"]["drift"] is True and "MIT" in by["license"]["detail"]
    assert by["gitignore"]["drift"] is False
    assert rows[0]["role"] == "license"           # drift findings sort first


# --- decomposition: the `table` op evicts the hardcoded `projects` Function ---

async def _repo(actions: Actions, canon: str, name: str, *, commits: int, day0: int) -> uuid.UUID:
    repo = await actions.create_or_find_object("SoftwareProject", canon, "git")
    await actions.assert_property(repo, "name", name, "git", NOW, 0.9)
    for i in range(commits):
        c = await actions.create_or_find_object("Commit", f"commit:{name}{i}", "git")
        await actions.assert_property(c, "authored_date", f"2026-06-{day0+i:02d}T00:00:00+00:00",
                                      "git", NOW, 0.9)
        await actions.create_link(c, repo, "in_repo", "git", NOW, 0.9)
    return repo


async def test_projects_decomposed_to_a_table_op(actions: Actions) -> None:
    """The first eviction: `projects` is no longer a hardcoded Function — it's a pure `table`
    op-tree the user owns (select repos → rollup columns → order). Same rows, no Python. This
    is what 'everything is composed' means: a per-object table with rollup-over-link columns
    (Notion's database+rollups) is now a PRIMITIVE, not bespoke code."""
    osiris = await _repo(actions, "repo:osiris", "osiris", commits=3, day0=20)   # newest: 06-22
    await _repo(actions, "repo:kast", "kast", commits=1, day0=20)                # 06-20
    f = await actions.create_or_find_object("File", "file:osiris/LICENSE", "git-tree")
    await actions.create_link(f, osiris, "in_repo", "git-tree", NOW, 0.9)        # a files rollup

    await seed_default_compositions(actions.pool)
    res = await run_composition(actions.pool, "projects")               # the op-tree, no subject
    assert res["kind"] == "rows"                                        # a table, not Function data
    by = {r["project"]: r for r in res["items"]}
    assert by["osiris"]["commits"] == 3 and by["kast"]["commits"] == 1  # count rollup over in_repo
    assert by["osiris"]["files"] == 1 and by["kast"]["files"] == 0      # a second rollup, typed
    assert res["items"][0]["project"] == "osiris"                       # order by last_touched desc
    assert "projects" not in list_functions()                          # the slop is gone

"""NAVIGABLE SPACE, THE READING LAYER (ruling c5953bb1, operator's word on his own
screenshot: clusters far apart, huge cross-cluster bundles). Live numbers Thoth measured
(mail 10595): repo:osiris degree 20,352, principal:analyst:operator degree 18,472,
dev:asuramaya 9,462 -- every object in the graph draws a spoke to one of a few shared
hubs via a MEMBERSHIP/IDENTITY edge (in_repo, acts_for, works_in, spawned_by,
authored_by...), and when the layout heartbeat's intra-project relax pulled on those
edges too, that spoke became a literal spring pulling every object toward the hub
regardless of which project it actually lives in -- the bundle.

STRUCTURAL vs SEMANTIC is the fix: a structural edge says WHO BELONGS WHERE (identity,
membership, authorship, dispatch) -- real, load-bearing, but never a claim about the
object's own CONTENT, and drawing a spring along it is exactly what produced the
cross-cluster bundle. A semantic edge is an actual claim about content (this cites that,
this supersedes that, this is upstream of that) -- the only kind of edge
graph_layout.py's intra-project relax should ever pull on.

ONE shared table, agreed by DM with Seshat (mail 10604) before this was committed:
graph_layout.py's own relax-exclusion reads it, graph_stream.py's wire header
serializes it (`link_type_class`, index-aligned to `edge_types`) so the renderer reads
the classification off the wire rather than hardcoding a second copy client-side.

TOTAL, NEVER PARTIAL: `link_class()` always returns a value (an unclassified/extension
type defaults to "semantic" -- the safe default, since a wrongly-semantic structural
edge just behaves like it did before this fix, never invisibly bundling something new).
But `test_link_classes.py`'s own population test holds this house to a HIGHER bar than
the safe default: every link type this codebase's OWN write paths (`create_link`/
`_link_once` call sites, scanned live) can ever actually mint must appear in one of the
two EXPLICIT sets below, not silently fall through to the default -- "no unclassified"
per the ruling's own acceptance line.
"""
from __future__ import annotations

STRUCTURAL_LINK_TYPES: frozenset[str] = frozenset({
    # membership / identity -- an object's own place in the fleet, not a claim about
    # what it says or means
    "in_repo", "works_in", "acts_for", "authored_by", "spawned_by",
    # WHICH LIVE GENERATION ran a commit (ruling 4cf5e4b3/b8fb26494e0e) -- additional to
    # authored_by, same identity/attribution shape, not a content claim.
    "committed_by",
    # a MachineIdentity's own standing relationship to the SoftwareProject it commits
    # for (ruling edb6b0fc) -- same membership/identity shape as authored_by/in_repo
    # just above, not a claim about content.
    "committer_for",
    # dispatch / addressing -- who a message or broadcast reaches, not content
    "sent_by", "addressed_to", "broadcast_to", "replies_to", "in_thread",
    "holds",
    # governance / lineage-of-OFFICE (never lineage-of-FACT, see succeeded_from below)
    "managed_by", "governs", "succeeds_seat", "forked_from", "worktree_of",
    # OSINT-domain membership (Person member_of Organization) -- was previously
    # unclassified by omission (THE PHYSICS LAYOUT, Thoth mail 11047): the same
    # membership/identity shape as in_repo/works_in above, not a content claim.
    "member_of",
    # THE ASSERTION LINKS MIGRATION (DRAWING THE WHOLE GRAPH, thread 325ef660):
    # recorded_by (Decision/Thread -> the Agent whose source_id minted it, an
    # authorship/attribution edge, same shape as authored_by above, not a claim
    # about content), owned_by (Thread -> its own owner, a standing-responsibility
    # edge, same membership/identity shape as managed_by), admitted_by (Thread ->
    # the Agent who admitted it, a dispatch/attribution edge), vendor_of
    # (Reference -> its vendor, a membership/identity edge, same shape as
    # committer_for). NOT acknowledges (Thoth mail 11448): Decision.prior_art_
    # acknowledged's own confirmation already mints a real `cites` edge
    # (acknowledge_prior_art's own docstring) -- a distinct type would be
    # redundant, dropped from the migration entirely, never classified here.
    "recorded_by", "owned_by", "admitted_by", "vendor_of",
})

# every OTHER link type this codebase's write paths actually mint today (scanned via
# `create_link`/`_link_once`/`_link` call sites -- test_link_classes.py's own
# population test re-derives this list live and fails if a new one appears
# unclassified) -- listed explicitly so a reviewer can see the actual triage, not
# inferred from "not structural."
SEMANTIC_LINK_TYPES_KNOWN: frozenset[str] = frozenset({
    "follows", "cites", "possible_upstream", "informs",
    "resolved_by", "closed_by", "decided_in", "answers", "grounded_by", "witnesses",
    "refuted_by", "killed_by", "implements", "rediscovers", "narrows", "peer_of",
    "produced", "derived_from", "authorized_by", "evaluated_by", "ruled_by", "revises",
    "mentions", "noted_in", "not_same_as",
    # same_as (identity merge, loser -> winner, e.g. the MachineIdentity same_as
    # bridge, ruling edb6b0fc): a real claim about content ("this IS that"), same
    # class as its own negation not_same_as just above -- not membership.
    "same_as",
    # lineage-of-FACT (succession/authorship across agent generations): a real claim
    # ("this session's own words descend from that one's"), and per Thoth's own mail
    # 10595 listed as semantic, not structural -- pulling on it doesn't bundle by
    # PROJECT the way a membership edge does.
    "succeeded_from",
    # OSINT-domain entity relationships (case/investigation objects, a different
    # population from the fleet's own Agent/Thread/Decision graph this ruling is
    # actually about) -- genuine CONTENT claims about an entity (who controls it, what
    # it owns, who sponsors it), never fleet membership, so semantic is the correct
    # classification, not merely the safe default.
    "controlled_by", "has_account", "has_domain", "has_email", "has_url",
    "investigator", "litigation", "officer", "raises_for", "site", "sponsors",
    "transacted_with",
    # THE ASSERTION LINKS MIGRATION (DRAWING THE WHOLE GRAPH, thread 325ef660):
    # supersedes (Decision -> the Decision it supersedes) is a real claim about
    # content ("this replaces that"), the same shape as revises/refuted_by above
    # -- already walked as a path edge by the client (space.js's own
    # PATH_EDGE_TYPES), unclassified here only by omission until now.
    "supersedes",
})


# THE PHYSICS LAYOUT (operator ruling d7d55257, Thoth mail 11047): the SPATIAL-
# CONTAINMENT subset of STRUCTURAL_LINK_TYPES -- an edge saying "this object's home
# IS that container" (a project, a seat, a person, a parent agent), as opposed to a
# structural edge that is real and load-bearing but not about WHERE an object lives
# (dispatch/addressing, governance/lineage-of-office, authorship, commit attribution).
# Only this subset drives gravity toward a container's own earned centroid in the
# physics layout; every other STRUCTURAL type stays excluded from the layout
# entirely, exactly as before this ruling -- a subset, never an independent bucket.
CONTAINER_LINK_TYPES: frozenset[str] = frozenset({
    "in_repo", "works_in", "acts_for", "spawned_by", "holds", "member_of",
})


def link_class(link_type: str) -> str:
    """"container", "structural", or "semantic" -- total, never raises. "container" is
    a FLAG NESTED INSIDE the structural class: every container type is also in
    STRUCTURAL_LINK_TYPES (test_container_types_are_a_subset_of_structural), so any
    existing caller checking membership in that frozenset directly (graph_layout.py's
    relax-exclusion) is unaffected by this three-way split -- only a caller reading
    the STRING value this function (or the wire's `link_type_class`) returns needs to
    know about the new third value. An unrecognized/extension type defaults to
    "semantic" (the safe default: behaves exactly as it did before this fix, never a
    silent new source of bundling) -- but see the module docstring for why the
    actually-used population is held to a stricter bar than this default."""
    if link_type in CONTAINER_LINK_TYPES:
        return "container"
    if link_type in STRUCTURAL_LINK_TYPES:
        return "structural"
    return "semantic"

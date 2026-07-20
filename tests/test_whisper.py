"""The whisper's render step — the three defects from Ra IV's field report (msg 301), fixed
in one commit: inline-the-fold (a3a3d512), the false-seam hedge (e9104869), and
succession-by-query (d80621a7 piece 4). render_whisper() is pure, so each is provable
against a canned automount() payload — no server, no clock, no network."""
from __future__ import annotations

from scripts.osiris_whisper import render_whisper


def _base(**extra: object) -> dict:
    out = {"agent": "agent:abc12345", "project": "osiris", "model": "claude-sonnet-5",
           "resolved": True, "mail": 0, "mail_asks": 0, "desk": 0, "seat": None, "thin": False,
           "job_dir": "/home/asuramaya/.claude/jobs/abc12345"}
    out.update(extra)
    return out


def test_inline_the_fold_shows_top_obligations_with_short_ids() -> None:
    """thread a3a3d512: a resumed mind shouldn't need a full orient() round-trip just to
    see what it already owes — the top of the SAME ranked wall inlines right here."""
    out = _base(obligations=[
        {"id": "12a58447", "summary": "HANDOFF — Thoth L to LI", "kind": "obligation"},
        {"id": "9dc3ce8b", "summary": "READ-SIDE ADOPTION OF THE VISIT CLASS"},
        {"id": "879c97b9", "summary": "FIRST-CLASS WALK-IN"},
    ])
    text = render_whisper(out, cwd="/home/asuramaya/.osiris/seats/seshat", env_job="")
    assert "Top of your project's wall" in text
    assert "[12a58447] HANDOFF — Thoth L to LI" in text
    assert "[9dc3ce8b]" in text and "[879c97b9]" in text
    assert "orient() for the rest" in text


def test_no_obligations_means_no_wall_line() -> None:
    out = _base()  # no "obligations" key at all
    text = render_whisper(out, cwd="/x", env_job="")
    assert "Top of your project's wall" not in text


def test_false_seam_hedges_when_deliberateness_is_unconfirmed() -> None:
    """thread e9104869: automount's registration path never reads the transcript for a
    deliberate /model marker (only the heartbeat swap-detector does), so it must never
    assert a confession it cannot ground — hedge, and point at mount() to verify."""
    out = _base(swap="claude-fable-5 → claude-haiku-4-5")
    text = render_whisper(out, cwd="/x", env_job="")
    assert "Possible model seam" in text
    assert "unconfirmed" in text
    assert "mount() may resolve it" in text
    # never a bare imperative to confess — that's the exact bug being fixed
    assert "confess it to the operator in your first reply" not in text.lower()


def test_a_witnessed_operator_swap_still_speaks_plainly() -> None:
    """The grounded case (the transcript DID witness a deliberate /model) must keep its
    full confidence — the hedge is for the UNCONFIRMED case only, never a blanket softening."""
    out = _base(swap="claude-fable-5 → claude-opus-4-8 [operator /model]")
    text = render_whisper(out, cwd="/x", env_job="")
    assert "the OPERATOR's own deliberate choice" in text
    assert "no confession owed" in text
    assert "Possible model seam" not in text
    assert "unconfirmed" not in text


def test_succession_pointer_resolves_by_query_not_a_copied_id() -> None:
    """d80621a7 piece 4: point a freshly minted heir at the charter file and the newest
    OPEN OBLIGATION the SERVER just resolved by query (owner+kind, newest at read
    time) — never an id baked into this script."""
    out = _base(minted="agent:ad1a1cb0-g40-xiii", succession={
        "charter_file": "/home/asuramaya/.osiris/seats/thoth/CLAUDE.md",
        "thread_id": "12a58447",
        "thread_summary": "HANDOFF — Thoth L to LI",
    })
    text = render_whisper(out, cwd="/x", env_job="")
    assert "MINTED as this lineage's successor" in text
    assert "Your charter file is /home/asuramaya/.osiris/seats/thoth/CLAUDE.md" in text
    assert "[12a58447] HANDOFF — Thoth L to LI" in text
    assert "then orient()" in text


def test_succession_pointer_never_overclaims_the_kind_of_thread_it_found() -> None:
    """Thoth LI's amend (msg 861): the query finds the newest OPEN OBLIGATION, not
    specifically a handoff — a live repro called a census obligation 'the newest
    succession thread', steering a fresh heir at zero context into the wrong thread. The
    label must never claim more than the query witnessed."""
    out = _base(minted="agent:ad1a1cb0-g40-xiii", succession={
        "thread_id": "9dc3ce8b",
        "thread_summary": "READ-SIDE ADOPTION OF THE VISIT CLASS",
    })
    text = render_whisper(out, cwd="/x", env_job="")
    assert "The newest open obligation your project owns is [9dc3ce8b]" in text
    assert "succession thread" not in text.lower()


def test_succession_pointer_falls_back_when_the_server_found_neither() -> None:
    """A mint with no resolvable charter/thread must never promise a field the server
    didn't find — it falls back to the old pointer rather than printing an empty claim."""
    out = _base(minted="agent:ad1a1cb0-g40-xiii")  # no "succession" key
    text = render_whisper(out, cwd="/x", env_job="")
    assert "MINTED as this lineage's successor" in text
    assert "read orient()'s open threads for the succession note" in text
    assert "charter file" not in text.lower()

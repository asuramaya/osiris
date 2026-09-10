"""Static-source guards for console.js routes that have no browser test coverage.

tagIt() posted to /objects/:id/tag instead of the real /objects/:id/tags route, was fixed once
on a branch (khnum-land-console, 1867a5c), then silently lost when a legitimate full rewrite
(9e2226b) superseded that branch without carrying the one-line fix forward — untested, so
nothing caught it live again for three days (thread 98344adb). This file exists so the same
route can't regress invisibly a second time.
"""
from __future__ import annotations

from pathlib import Path

_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()


def test_tag_it_posts_to_the_real_tags_route() -> None:
    assert "/objects/' + id + '/tags'" in _JS
    assert "/objects/' + id + '/tag'" not in _JS


# #92's tail (Thoth dispatch 9257 piece 1): the Mailbox surface used to fetch /pulse, which
# never carried a `messages` array (src/api/app.py's pulse_route returns
# {line, live, owed, briefs, wakes, spend} only) — renderMailbox() always rendered a false
# "no JSON mail-list route exists yet" wall. Ported onto the REAL mail read: the "mail" saved
# composition (MAIL_OVERVIEW, compositions.py — chrome.mail_overview's own fold-aware room/soul
# read), run through the same generic Osiris.renderResult() pipeline every other composition
# surface uses, with each row's "run:mail_threads" drill-in (test_row_action_ui.py's own
# documented gap: "the page shell, index.html, owns actually running the Function... untested
# here") finally caught by a scoped `osiris:run` listener.


def test_mailbox_no_longer_reads_the_messageless_pulse_route() -> None:
    assert "fetch('/pulse')" not in _JS.split("// ── Mailbox")[1].split("// ── ")[0]
    assert "no JSON mail-list route exists yet" not in _JS


def test_render_mailbox_runs_the_mail_composition() -> None:
    assert "async function renderMailbox() { await runMailboxComposition('mail', {}); }" in _JS


def test_mailbox_composition_runner_hits_the_saved_composition_route_with_no_args() -> None:
    assert "'/compositions/' + encodeURIComponent(name) + '/run'" in _JS


def test_mailbox_composition_runner_hits_run_spec_for_a_function_drill_in() -> None:
    assert "'/compositions/run-spec'" in _JS
    assert "op: 'function', name: name, args: args" in _JS


def test_mailbox_listens_for_the_osiris_run_navigation_event_scoped_to_its_own_surface() -> None:
    assert "document.addEventListener('osiris:run'" in _JS
    listener = _JS.split("document.addEventListener('osiris:run'", 1)[1][:300]
    assert "if (ACTIVE_SURFACE !== 'mailbox') return;" in listener
    assert "runMailboxComposition(e.detail.name, e.detail.args || {});" in listener

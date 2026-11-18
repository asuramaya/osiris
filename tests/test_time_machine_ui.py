"""The console's Time Machine screen (src/ui/static/timemachine.js): its pure helpers run under
node, and its source is checked for the rules that matter: nothing is written except by Apply,
nothing runs on load but a status read, every server string is escaped, and the plan the operator
sees is the plan that is applied (its digest and choices travel together)."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_JS = (_STATIC / "timemachine.js").read_text()
_CONSOLE = (_STATIC / "console.js").read_text()
_INDEX = (_STATIC / "index.html").read_text()


def _node(expr: str) -> object:
    script = (f"const a = require({json.dumps(str(_STATIC / 'timemachine.js'))});"
              f"process.stdout.write(JSON.stringify({expr}));")
    out = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=20)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_it_is_loaded_after_athome_and_lives_in_the_backups_section() -> None:
    assert _INDEX.index("/ui/athome.js") < _INDEX.index("/ui/timemachine.js")
    body = _CONSOLE.split("async function renderSettingsSectionOffload() {", 1)[1][:2400]
    assert "tmSlotHtml()" in body and "tmInit()" in body
    assert "label: 'Browse a past moment…'" in _CONSOLE
    assert "openSettings('backups')" in _CONSOLE.split("label: 'Browse a past moment…'", 1)[1][:300]


def test_it_talks_to_the_time_machine_routes() -> None:
    for path in ("'/time-machine/moments'", "'/time-machine/status'", "'/time-machine/start'",
                 "'/time-machine/extend'", "'/time-machine/teardown'",
                 "'/time-machine/browse/types'", "'/time-machine/browse/objects?limit='",
                 "'/time-machine/browse/object/'", "'/time-machine/browse/sessions?limit=50'",
                 "'/time-machine/plan'", "'/time-machine/apply'"):
        assert path in _JS, path


def test_nothing_is_written_on_load_and_apply_is_the_only_restore() -> None:
    init = _JS.split("function tmInit() {", 1)[1].split("\n}\n", 1)[0]
    assert "'POST'" not in init and "tmRefreshStatus()" in init and "tmShowList" not in init
    for fn in ("async function tmRefreshStatus", "async function tmShowList",
               "async function tmLoadItems", "async function tmOpenItem"):
        body = _JS.split(fn, 1)[1].split("\n}\n", 1)[0]
        assert "'POST'" not in body, fn
    assert _JS.count("'/time-machine/apply', 'POST'") == 1
    apply_body = _JS.split("async function tmApply() {", 1)[1].split("\n}\n", 1)[0]
    assert "digest: TM.plan.digest" in apply_body and "tmChoices(box)" in apply_body


def test_polling_runs_only_while_a_moment_is_opening_and_stops_when_the_screen_is_gone() -> None:
    body = _JS.split("function tmSchedulePoll() {", 1)[1].split("\n}\n", 1)[0]
    assert "if (!$('tm-root')) return;" in body
    # every working state is polled (the NAS kinds also go through "pulling"), not a fixed list
    assert "st && st !== 'failed'" in body and "4000" in body
    assert "'starting' || st === 'restoring'" not in body


def test_a_pulling_moment_shows_its_progress_text_and_a_cancel() -> None:
    html = str(_node("a.tmStatusHtml({state:'pulling',state_text:'pulling base 2.1/3.8 GB',"
                     "seconds_left:7000,moment:{label:'NAS dump'}})"))
    assert "pulling base 2.1/3.8 GB" in html and "Cancel" in html


def test_the_time_box_minimum_rounds_up_and_the_maximum_down() -> None:
    at = "'2026-11-17T10:00:30Z'"
    assert _node(f"a.tmLocalBox({at}, true) > a.tmLocalBox({at})")
    whole = "'2026-11-17T10:00:00Z'"
    assert _node(f"a.tmLocalBox({whole}, true) === a.tmLocalBox({whole})")


def test_a_moment_that_needs_a_time_gets_a_picker_and_the_time_goes_up_in_utc() -> None:
    html = str(_node("a.tmMomentHtml({id:'pitr-b',kind:'pitr',label:'Point in time',"
                     "when:'2026-11-17T12:00:00Z',window_from:'2026-11-10T00:00:00Z',"
                     "window_to:'2026-11-17T12:00:00Z',pick_time:true,available:true,"
                     "size_bytes:3e10},0)"))
    assert 'type="datetime-local"' in html and "Open this moment" in html
    assert "30.0 GB" in html
    assert _node("a.tmUtc('2026-11-17T12:00')") .__str__().endswith("Z")  # type: ignore[union-attr]
    plain = str(_node("a.tmMomentHtml({id:'d1',kind:'dump',label:'dump 2026-11-17 10:30',"
                      "when:'2026-11-17T10:30:00Z',available:true},1)"))
    assert "datetime-local" not in plain


def test_an_unavailable_moment_says_why_and_cannot_be_opened() -> None:
    html = str(_node("a.tmMomentHtml({id:'r1',kind:'restic',label:'NAS dump',"
                     "when:'2026-10-01T00:00:00Z',available:false,"
                     "reason:'Not on the home network.'},2)"))
    assert "Not available: Not on the home network." in html
    assert "disabled" in html


def test_server_text_is_escaped() -> None:
    html = str(_node("a.tmMomentHtml({id:'x',kind:'dump',label:'<img src=x onerror=1>',"
                     "when:'2026-11-17T10:30:00Z',available:true},0)"))
    assert "<img" not in html and "&lt;img" in html
    plan = str(_node("a.tmPlanHtml({object:{type:'Thread',canonical:'<b>x</b>',action:'create'},"
                     "assertions:{copy:[{name:'n',value:'<script>',effect:'new_property'}],"
                     "skip:[],conflicts:[]},links:{copy:[],skip:[]},counts:{},digest:'d'})"))
    assert "<script>" not in plan and "&lt;script&gt;" in plan


def test_the_status_line_shows_the_servers_progress_text_verbatim() -> None:
    html = str(_node("a.tmStatusHtml({state:'restoring',state_text:'replaying: reached Oct 30 "
                     "11:02 of Oct 30 14:00 UTC',moment:{label:'Point in time'}})"))
    assert "replaying: reached Oct 30 11:02 of Oct 30 14:00 UTC" in html
    ready = str(_node("a.tmStatusHtml({state:'ready',seconds_left:5400,extensions:0,"
                      "moment:{label:'dump'}})"))
    assert "1 h 30 min left" in ready and "Add two hours" in ready
    used = str(_node("a.tmStatusHtml({state:'ready',seconds_left:600,extensions:1,"
                     "moment:{label:'d'}})"))
    assert "Add two hours" not in used


def test_the_plan_shows_the_merge_note_and_conflicts_default_to_keep() -> None:
    plan = json.dumps({
        "moment": "dump 2026-11-17", "digest": "abc123",
        "object": {"type": "Thread", "canonical": "t1", "action": "add_to_existing",
                   "note": "That item was merged into X since then, so this adds to it."},
        "assertions": {
            "copy": [{"key": "a:1", "name": "summary", "value": "old",
                      "effect": "becomes_displayed"}],
            "skip": [{"key": "a:2"}],
            "conflicts": [{"key": "a:3", "name": "status", "value": "open", "kind": "tie",
                           "live": {"value": "closed"}, "choices": ["keep", "restore"],
                           "default": "keep"}]},
        "links": {"copy": [{"key": "l:9", "type": "in_repo", "direction": "out",
                            "other": {"type": "Repo", "canonical": "osiris"}}], "skip": []},
        "live_only": ["owner"], "counts": {}})
    html = str(_node(f"a.tmPlanHtml({plan})"))
    assert "That item was merged into X since then, so this adds to it." in html
    assert 'data-conflict="a:3"' in html and 'value="keep" checked' in html
    assert 'data-link="l:9" checked' in html and "would become the value shown" in html
    assert "Only live, never touched: owner" in html
    assert 'id="tm-apply" disabled' not in html
    empty = str(_node("a.tmPlanHtml({object:{type:'T',canonical:'c',action:'add_to_existing'},"
                      "assertions:{copy:[],skip:[],conflicts:[]},links:{copy:[],skip:[]},digest:'d'})"))
    assert 'id="tm-apply" disabled' in empty


def test_choices_are_read_from_the_screen_not_assumed() -> None:
    src = _JS.split("function tmChoices(rootEl) {", 1)[1].split("\n}\n", 1)[0]
    assert "input[type=radio]:checked" in src and "input[data-link]" in src

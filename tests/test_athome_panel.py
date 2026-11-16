"""The console's At home panel (src/ui/static/athome.js): its pure helpers run under node, and its
routes, wiring and the rules about a recovery code are checked against the source."""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

_STATIC = Path(__file__).resolve().parent.parent / "src" / "ui" / "static"
_JS = (_STATIC / "athome.js").read_text()
_CODE = "\n".join(line for line in _JS.splitlines() if not line.lstrip().startswith("//"))
_CONSOLE = (_STATIC / "console.js").read_text()
_INDEX = (_STATIC / "index.html").read_text()

pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="needs node")


def _node(expr: str) -> object:
    script = (f"const a = require({json.dumps(str(_STATIC / 'athome.js'))});"
              f"process.stdout.write(JSON.stringify({expr}));")
    out = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=20)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_the_panel_is_loaded_after_the_console_and_reachable_from_the_palette() -> None:
    assert _INDEX.index("/ui/console.js") < _INDEX.index("/ui/athome.js")
    assert "label: 'At home…'" in _CONSOLE and "renderAtHomePanel()" in _CONSOLE


def test_it_talks_only_to_the_at_home_routes() -> None:
    for path in ("'/at-home'", "'/at-home/envelope'", "'/at-home/restore-test'",
                 "'/at-home/envelope/prove'", "'/at-home/envelope/prove-browser'",
                 "'/at-home/envelope/add-code'", "'/at-home/envelope/confirm-code'",
                 "'/at-home/envelope/cancel-code'", "'/at-home/envelope/add-key'",
                 "'/at-home/nas/' + which"):
        assert path in _JS, path
    assert "/at-home/envelope/challenge?recipient_id=" in _JS


def test_a_recovery_code_is_never_stored_and_never_outlives_its_screen() -> None:
    assert "localStorage" not in _CODE and "sessionStorage" not in _CODE
    assert "ATHOME_CODE_SHOWN" in _JS and "beforeunload" in _JS
    # leaving the surface clears it, and both buttons clear it
    assert "athomeClearCode()" in _CONSOLE.split("async function switchSurface")[1][:400]
    for fn in ("athomeConfirmCode", "athomeCancelCode"):
        body = _JS.split(f"async function {fn}")[1].split("\n}\n")[0]
        assert "athomeClearCode()" in body, fn
    # removing a code is behind a tick box, adding a key too, and the first copy
    assert "athome-cancel-confirm" in _JS and "athome-addkey-confirm" in _JS
    assert "athome-nas-confirm" in _JS and 'id="athome-nas-run" disabled' in _JS


def test_no_browser_dialogs_and_no_banned_words() -> None:
    for banned in ("alert(", "confirm(", "prompt(", "—", "receipt", "dual-door"):
        assert banned not in _CODE, banned


def test_statuses_read_in_plain_words() -> None:
    assert _node("[a.athomeStatusLabel('done'), a.athomeStatusLabel('waiting'),"
                 " a.athomeStatusLabel('blocked'), a.athomeStatusLabel('optional')]") == [
        "Done", "Waiting on you", "Waiting for an earlier step", "Only if you want it"]


def test_a_code_is_grouped_for_copying_by_hand() -> None:
    assert _node("a.athomeGroupCode('ABCDEFGHIJKL')") == "ABCD EFGH IJKL"
    assert _node("a.athomeGroupCode('ABCD-EFGH-IJKL')") == "ABCD-EFGH-IJKL"


def test_stream_lines_arrive_in_chunks_and_the_tail_waits_for_the_next() -> None:
    out = _node("a.athomeParseLines('{\"n\":1,\"text\":\"a\"}\\n{\"n\":2,\"te')")
    assert out == {"lines": [{"n": 1, "text": "a"}], "rest": '{"n":2,"te'}
    whole = _node("a.athomeParseLines('{\"n\":2,\"text\":\"b\"}\\n')")
    assert whole == {"lines": [{"n": 2, "text": "b"}], "rest": ""}


def test_the_checklist_keeps_the_cli_order_and_escapes_what_it_shows() -> None:
    html = _node("a.athomeChecklistHtml({items: [{key:'k',order:2,title:'<b>Second</b>',"
                 "status:'waiting',why:'w',command:'run \"x\"',then:null},"
                 "{key:'j',order:1,title:'First',status:'done',why:'ok',command:null}]})")
    assert isinstance(html, str)
    assert "&lt;b&gt;Second&lt;/b&gt;" in html and "<b>Second</b>" not in html
    assert "&quot;x&quot;" in html  # the copy button's attribute is quoted safely


def test_keys_show_proof_dates_and_a_prove_button_only_where_it_can_run() -> None:
    html = _node("a.athomeKeysHtml({recipients: [{id:'r1',kind:'fido2-prf',label:'Blue key',"
                 "proved_at:'2026-11-01T00:00:00Z',opened_at:null,can_prove:'server-touch'},"
                 "{id:'r2',kind:'recovery-code',label:'Paper',proved_at:null,opened_at:null,"
                 "can_prove:'no',hint:'Type it at the terminal'}],"
                 " warnings:['One key was never proved.']})")
    assert isinstance(html, str)
    assert "Security key" in html and "Recovery code" in html and "never" in html
    assert html.count("Prove this key") == 1 and "Type it at the terminal" in html
    assert "One key was never proved." in html


def test_an_unreadable_envelope_and_a_never_run_restore_test_say_so() -> None:
    assert "could not be read" in str(_node("a.athomeKeysHtml({error:'no vault'})"))
    assert "not been run yet" in str(_node("a.athomeRestoreHtml({ok:null})"))
    passed = _node("a.athomeRestoreHtml({ok:true,when:'2026-11-02T00:00:00Z',files_checked:3})")
    assert "Passed" in str(passed)

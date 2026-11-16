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


def test_it_talks_to_the_key_and_at_home_routes() -> None:
    for path in ("'/soul-key/at-home'", "'/soul-key/envelope'", "'/at-home/restore-test'",
                 "'/soul-key/envelope/add-code'", "'/soul-key/envelope/remove'",
                 "'/soul-key/envelope/add-key/begin'", "'/soul-key/envelope/add-key/finish'",
                 "'/at-home/nas/' + which"):
        assert path in _JS, path
    assert "'/soul-key/envelope/' + encodeURIComponent(id) + '/challenge'" in _JS
    assert "'/soul-key/envelope/' + encodeURIComponent(id) + '/drill'" in _JS


def test_a_key_is_used_only_from_its_own_address_with_user_verification_required() -> None:
    assert "athomeHostOk(rpId)" in _JS and "Open the console at" in _JS
    assert _JS.count("userVerification: 'required'") >= 3  # prove, create, and the second ask
    # the secret a key returns is dropped right after its one request
    assert "prfBytes.fill(0)" in _JS and "body.prf = null" in _JS
    assert "console.log" not in _CODE


def test_a_recovery_code_is_never_stored_and_never_outlives_its_screen() -> None:
    assert "localStorage" not in _CODE and "sessionStorage" not in _CODE
    assert "ATHOME_CODE_SHOWN" in _JS and "beforeunload" in _JS
    # leaving the surface clears it, and both buttons clear it
    assert "athomeClearCode()" in _CONSOLE.split("async function switchSurface")[1][:400]
    assert "athomeClearCode()" in _JS.split("function athomeConfirmCode")[1].split("\n}\n")[0]
    assert "athomeClearCode()" in _JS.split("async function athomeCancelCode")[1].split("\n}\n")[0]
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


def test_the_lists_wrap_instead_of_being_cut_off_by_the_console_tables() -> None:
    assert "ee-table" not in _CODE and "<table" not in _CODE
    assert "overflow-wrap:anywhere" in _JS


def test_a_note_about_one_key_is_not_repeated_above_that_keys_own_row() -> None:
    html = _node("a.athomeKeysHtml({recipients: [{id:'r0',kind:'fido2-prf',label:'Key',"
                 "rp_id:'localhost',verdict:'unproven'}], check:{problems:[],"
                 "warnings:[\"r0: never proved to open the envelope\","
                 " 'Only one hardware key.']}})")
    assert isinstance(html, str)
    assert "r0: never proved" not in html and "Only one hardware key." in html


def test_the_checklist_keeps_the_cli_order_and_escapes_what_it_shows() -> None:
    html = _node("a.athomeChecklistHtml({items: [{key:'k',order:2,title:'<b>Second</b>',"
                 "status:'waiting',why:'w',command:'run \"x\"',then:null},"
                 "{key:'j',order:1,title:'First',status:'done',why:'ok',command:null}]})")
    assert isinstance(html, str)
    assert "&lt;b&gt;Second&lt;/b&gt;" in html and "<b>Second</b>" not in html
    assert "&quot;x&quot;" in html  # the copy button's attribute is quoted safely


def test_keys_show_proof_dates_and_the_right_button_for_each_kind() -> None:
    html = _node("a.athomeKeysHtml({recipients: [{id:'r1',kind:'fido2-prf',label:'Blue key',"
                 "rp_id:'localhost',last_proved_at:'2026-11-01T00:00:00Z',last_opened_at:null,"
                 "verdict:'stale'},"
                 "{id:'r2',kind:'recovery-code',label:'Paper',last_proved_at:null,"
                 "last_opened_at:null,verdict:'unproven'}],"
                 " check:{problems:[],warnings:['One key was never proved.']}})")
    assert isinstance(html, str)
    assert "Security key" in html and "Recovery code" in html and "never" in html
    assert html.count("Prove this key") == 1 and html.count("Prove this code") == 1
    assert "Not proved in a while" in html and "Never proved" in html
    assert "One key was never proved." in html
    assert "Opens only when the console is at <code>localhost</code>" in html


def test_an_unreadable_envelope_and_a_never_run_restore_test_say_so() -> None:
    assert "could not be read" in str(_node("a.athomeKeysHtml({error:'no vault'})"))
    assert "not been run yet" in str(_node("a.athomeRestoreHtml({ok:null})"))
    passed = _node("a.athomeRestoreHtml({ok:true,when:'2026-11-02T00:00:00Z',files_checked:3})")
    assert "Passed" in str(passed)


def test_a_403_names_the_wrong_computer_and_nothing_is_cached() -> None:
    assert "res.status === 403" in _JS
    assert "only answer on the computer that runs the console" in _JS
    assert "cache: 'no-store'" in _JS
    assert "Nothing was tested." in _JS  # needs_operator_key is not a failed proof


# --- the panel does the work: an action per item, the command only as a detail ----------------

def _items(*pairs: tuple[str, str]) -> str:
    rows = ",".join(f"{{key:'{k}',order:1,title:'T',status:'{s}',why:'w',command:'osiris x'}}"
                    for k, s in pairs)
    return f"a.athomeChecklistHtml({{items:[{rows}]}})"


def test_each_open_item_has_a_button_that_does_it_and_the_command_is_a_small_detail() -> None:
    html = str(_node(_items(("envelope_drill", "waiting"), ("envelope_code", "waiting"),
                            ("envelope_key", "waiting"), ("nas_offload", "waiting"),
                            ("rebuild_probe", "optional"), ("plaintext_copy", "blocked"))))
    for label in ("Prove a key now", "Add a recovery code", "Add a second key", "Check",
                  "Make the first copy", "Run the probe"):
        assert label in html, label
    assert 'id="athome-plain"' in html
    assert html.count("or run in a terminal") == 6
    assert html.count("<details") == 6  # the command is never the primary control


def test_a_finished_item_has_no_button() -> None:
    html = str(_node(_items(("nas_offload", "done"), ("envelope_code", "done"))))
    for label in ("Check", "Make the first copy", "Add a recovery code"):
        assert label not in html, label  # only the small Copy button remains


def test_the_old_plaintext_copy_offers_three_choices_and_delete_needs_the_typed_phrase() -> None:
    html = str(_node("a.athomePlainHtml({exists:true,size_bytes:280e9,choice:null,"
                     "delete_phrase:'delete 280 GB of history',encrypt_state:null})"))
    assert "Keep it" in html
    assert "Encrypt it into the backup, then remove the plaintext" in html
    assert "Delete it" in html and "delete 280 GB of history" in html
    assert 'id="athome-plain-delete" disabled' in html  # off until the phrase is typed
    assert "I am on the home network" in html


def test_a_move_in_progress_can_be_followed_after_a_reload_and_a_stopped_one_carried_on() -> None:
    running = str(_node("a.athomePlainHtml({exists:true,size_bytes:1e9,encrypt_state:'running',"
                        "covered_files:3,total_files:10})"))
    assert "Follow it" in running and "3 of 10 files" in running
    assert "Delete it" not in running  # no second action while it runs
    stopped = str(_node("a.athomePlainHtml({exists:true,size_bytes:1e9,encrypt_state:'stopped'})"))
    assert "Carry on" in stopped


def test_nothing_starts_by_itself_and_the_destructive_ones_are_confirmed() -> None:
    for fn in ("async function athomeLoadAll", "async function renderAtHomePanel",
               "async function athomePlainLoad"):
        body = _JS.split(fn)[1].split("\n}\n")[0]
        assert "'POST'" not in body and "athomeStream(" not in body, fn
    choose = _JS.split("async function athomePlainChoose")[1].split("\n}\n")[0]
    assert "payload.confirm" in choose and "athome-plain-home" in choose
    assert "athomeGB(" in _JS  # the size shown is the server's, in plain gigabytes

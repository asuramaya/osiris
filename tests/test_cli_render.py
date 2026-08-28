"""Tests for the CLI's human renderer (src/cli_render.py).

THE PROPERTY THAT MATTERS MOST HERE IS NOT PRETTINESS, IT IS THAT NOTHING IS LOST. A
renderer that silently drops a field is worse than json.dumps, because the reader cannot
tell. Several tests below assert presence of every key rather than any particular layout,
so a future restyle stays free while the honesty guarantee stays nailed down.
"""

from __future__ import annotations

import io

import pytest
from src import cli_render as r

# --- colour gating -------------------------------------------------------------------------

def test_no_color_env_beats_a_real_tty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NO_COLOR", "1")

    class FakeTTY(io.StringIO):
        def isatty(self) -> bool:
            return True

    assert r.supports_color(FakeTTY()) is False


def test_dumb_terminal_beats_force_color(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both are set; TERM=dumb is checked first ON PURPOSE — a terminal that cannot render
    escapes must win over a caller merely asking for colour."""
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setenv("TERM", "dumb")
    monkeypatch.setenv("FORCE_COLOR", "1")
    assert r.supports_color(io.StringIO()) is False


def test_a_plain_pipe_gets_no_escapes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    monkeypatch.setenv("TERM", "xterm")
    buf = io.StringIO()
    r.emit({"status": "ok", "n": 3}, as_json=False, stream=buf)
    assert "\x1b" not in buf.getvalue()
    assert "ok" in buf.getvalue()


# --- the human/machine split ------------------------------------------------------------

def test_json_mode_is_compact_and_byte_exact() -> None:
    """The agent-facing win: one line, no indent. Asserted as STRICTLY SHORTER than the
    indent=2 dump it replaced, because 'fewer tokens' was the whole point."""
    import json as _json
    data = {"seats": [{"handle": "Seshat", "house": "osiris"}], "total": 1}
    buf = io.StringIO()
    r.emit(data, as_json=True, stream=buf)
    out = buf.getvalue()
    assert out.count("\n") == 1
    assert _json.loads(out) == data
    assert len(out) < len(_json.dumps(data, indent=2))


def test_human_mode_never_claims_to_be_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NO_COLOR", "1")
    buf = io.StringIO()
    r.emit({"a": 1}, as_json=False, stream=buf)
    with pytest.raises(ValueError):
        __import__("json").loads(buf.getvalue())


# --- nothing is lost ---------------------------------------------------------------------

def test_every_scalar_key_survives_the_render(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NO_COLOR", "1")
    data = {f"field_{i}": f"value_{i}" for i in range(12)}
    buf = io.StringIO()
    r.emit(data, as_json=False, stream=buf, title="t")
    text = buf.getvalue()
    for k in data:
        assert k in text, f"{k} vanished from the human render"


def test_a_row_with_an_extra_column_does_not_lose_it() -> None:
    """Columns are the UNION of every row's keys, not the first row's — the bug a
    first-row-wins implementation would ship and nobody would notice until a real
    heterogeneous result arrived."""
    rows = [{"a": 1, "b": 2}, {"a": 3, "b": 4, "surprise": 5}]
    lines = r.table(rows, r.Paint(False), 120)
    assert any("surprise" in ln for ln in lines)
    assert any("5" in ln for ln in lines)


def test_empty_collection_is_reported_not_omitted(monkeypatch: pytest.MonkeyPatch) -> None:
    """'measured, and it is zero' and 'missing' are different facts and must not render the
    same way."""
    monkeypatch.setenv("NO_COLOR", "1")
    buf = io.StringIO()
    r.emit({"eligible": [], "scanned": 41}, as_json=False, stream=buf)
    text = buf.getvalue()
    assert "eligible" in text
    assert "(none)" in text


# --- layout correctness ------------------------------------------------------------------

def test_colour_does_not_shear_column_alignment() -> None:
    """Escape sequences occupy zero columns. If _visible_len were wrong every coloured cell
    would over-pad and the table would stagger — invisible in a diff, obvious on screen."""
    rows = [{"status": "ok", "name": "aaa"}, {"status": "failed", "name": "b"}]
    plain = r.table(rows, r.Paint(False), 120)
    fancy = r.table(rows, r.Paint(True), 120)
    strip = lambda s: "".join(  # noqa: E731
        part.split("m", 1)[-1] if i else part
        for i, part in enumerate(s.split("\x1b[")))
    assert [strip(x) for x in fancy] == plain


def test_a_table_too_wide_becomes_records_rather_than_mush() -> None:
    """Twelve columns in 60 chars cannot give any column a usable width. The fallback keeps
    the values readable instead of truncating all of them to 'seat:…'."""
    rows = [{f"c{i}": f"value{i}" for i in range(12)}]
    lines = r.table(rows, r.Paint(False), 60)
    text = "\n".join(lines)
    for i in range(12):
        assert f"value{i}" in text, f"c{i} was shredded instead of laid out"


def test_paths_keep_their_tail_not_their_head() -> None:
    """Head-truncation makes every seat under ~/.osiris/seats/ identical. The distinguishing
    segment is the LAST one, so that is what must survive."""
    p = "/home/asuramaya/.osiris/seats/seshat"
    out = r.shorten_path(p, 20)
    assert len(out) <= 20
    assert out.endswith("seshat")


def test_shorten_path_leaves_a_short_path_alone() -> None:
    assert r.shorten_path("/etc/hosts", 40) == "/etc/hosts"


def test_non_path_strings_are_not_path_shortened() -> None:
    assert r._looks_like_path("agent:c38f8f3b-g40") is False
    assert r._looks_like_path("/home/x/y") is True


def test_truncation_never_leaves_a_dangling_escape() -> None:
    """A cut inside a coloured run must still emit a reset, or the stain runs to the end of
    the terminal for everything printed afterwards."""
    coloured = r.Paint(True).good("a-very-long-green-value-here")
    cut = r._truncate(coloured, 8)
    assert cut.endswith("\x1b[0m")


# --- verdict colouring is conservative -----------------------------------------------------

def test_status_words_colour_only_on_a_whole_value_match() -> None:
    """'ok' inside a path or an id is not a verdict. Substring matching here would paint
    arbitrary identifiers green and quietly assert something untrue about them."""
    paint = r.Paint(True)
    assert "\x1b[32m" in r.fmt_value("ok", paint)
    assert "\x1b[32m" not in r.fmt_value("/home/tokyo/ok-ish-path", paint)
    assert "\x1b[32m" not in r.fmt_value("agent:ok9f2a", paint)


def test_none_and_false_are_distinguishable() -> None:
    """A missing value and a measured false are different facts; #142's whole lesson."""
    paint = r.Paint(False)
    assert r.fmt_value(None, paint) != r.fmt_value(False, paint)

"""Light, dark and follow-the-device for the console (operator ruling on the light theme).

Proofs: the choice persists and applies before first paint (theme.js run under node with a
fake window), the two copies of the light token set stay identical, every colour in the
stylesheet, the page style blocks and the console's inline styles is a token (so a theme
cannot miss a surface), both themes define the same tokens, and the text and accent tokens
hold readable contrast on their grounds in both themes.
"""
from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

import pytest

_STATIC = Path(__file__).resolve().parent.parent / "src" / "ui" / "static"
_CSS = (_STATIC / "osiris.css").read_text()
_CONSOLE = (_STATIC / "console.js").read_text()
_PAGES = {n: (_STATIC / n).read_text() for n in ("index.html", "space.html")}


# --- block extraction ---------------------------------------------------------------------
def _block(start: str) -> str:
    i = _CSS.index(start)
    j = _CSS.index("{", i)
    depth, k = 0, j
    while True:
        if _CSS[k] == "{":
            depth += 1
        elif _CSS[k] == "}":
            depth -= 1
            if depth == 0:
                return _CSS[j + 1:k]
        k += 1


def _tokens(body: str) -> dict[str, str]:
    return {m.group(1): m.group(2).strip()
            for m in re.finditer(r"(--[a-z0-9-]+)\s*:\s*([^;]+);", body)}


_DARK = _tokens(_block(":root {"))
_LIGHT_ATTR = _tokens(_block(':root[data-theme="light"]'))
_LIGHT_MEDIA = _tokens(_block(':root:not([data-theme="dark"])'))


def test_the_two_light_copies_are_identical() -> None:
    assert _LIGHT_ATTR == _LIGHT_MEDIA
    assert _LIGHT_ATTR["color-scheme"] if "color-scheme" in _LIGHT_ATTR else True


def test_light_and_dark_define_the_same_tokens() -> None:
    dark = {k for k in _DARK if k not in ("--font-sans", "--font-mono")}
    assert dark == set(_LIGHT_ATTR), (dark ^ set(_LIGHT_ATTR))


def test_system_rule_yields_to_a_pinned_dark_choice() -> None:
    assert "@media (prefers-color-scheme: light)" in _CSS
    assert ':root:not([data-theme="dark"])' in _CSS
    assert "color-scheme: light;" in _block(':root[data-theme="light"]')
    assert "color-scheme: dark;" in _block(":root {")


# --- token coverage -----------------------------------------------------------------------
_LITERAL = re.compile(r"#[0-9a-fA-F]{3,8}\b|rgba?\(\s*\d")


def _outside_theme_blocks(css: str) -> str:
    # drop the three token blocks, then comments
    for start in (":root {", ':root:not([data-theme="dark"])', ':root[data-theme="light"]'):
        body = _block(start)
        css = css.replace(body, "", 1)
    return re.sub(r"/\*.*?\*/", "", css, flags=re.S)


def test_stylesheet_names_no_literal_colour_outside_the_tokens() -> None:
    hits = _LITERAL.findall(_outside_theme_blocks(_CSS))
    assert hits == [], hits


@pytest.mark.parametrize("name", sorted(_PAGES))
def test_page_style_blocks_use_tokens(name: str) -> None:
    styles = "".join(re.findall(r"<style>(.*?)</style>", _PAGES[name], flags=re.S))
    assert _LITERAL.findall(styles) == []


def test_console_inline_styles_use_tokens() -> None:
    # colours written into style="..." strings; the data colour for an unknown object type
    # (#6e7681) is a fallback for server-supplied type colours, not a theme surface.
    inline = re.findall(r"(?:color|background)\s*:\s*(#[0-9a-fA-F]{3,8})\b", _CONSOLE)
    assert [c for c in inline if c.lower() != "#6e7681"] == []


def test_every_var_in_use_is_defined() -> None:
    used = set(re.findall(r"var\((--[a-z0-9-]+)", _CSS + "".join(_PAGES.values()) + _CONSOLE))
    # tokens set per-element by script rather than by the theme
    per_element = {"--rw", "--lw", "--orange"}
    missing = {t for t in used if t not in _DARK and t not in per_element}
    assert missing == set(), missing


# --- contrast -----------------------------------------------------------------------------
def _rgb(v: str) -> tuple[float, float, float]:
    v = v.strip()
    if v.startswith("#"):
        h = v[1:]
        if len(h) == 3:
            h = "".join(c * 2 for c in h)
        return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))  # type: ignore[return-value]
    m = re.match(r"rgba?\(([^)]*)\)", v)
    assert m, v
    parts = [float(p) for p in re.split(r"[ ,/]+", m.group(1).strip())[:3]]
    return tuple(parts)  # type: ignore[return-value]


def _lum(c: tuple[float, float, float]) -> float:
    def f(x: float) -> float:
        x /= 255
        return x / 12.92 if x <= 0.03928 else ((x + 0.055) / 1.055) ** 2.4
    r, g, b = (f(x) for x in c)
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def _ratio(a: tuple[float, float, float], b: tuple[float, float, float]) -> float:
    la, lb = _lum(a), _lum(b)
    hi, lo = max(la, lb), min(la, lb)
    return (hi + 0.05) / (lo + 0.05)


def _over(fg: tuple[float, float, float], bg: tuple[float, float, float], a: float
          ) -> tuple[float, float, float]:
    return tuple(f * a + g * (1 - a) for f, g in zip(fg, bg, strict=True))  # type: ignore[return-value]


_THEMES = {"dark": _DARK, "light": _LIGHT_ATTR}


@pytest.mark.parametrize("theme", sorted(_THEMES))
def test_text_tokens_hold_contrast_on_every_ground(theme: str) -> None:
    t = _THEMES[theme]
    for ground in ("--bg", "--panel", "--panel2", "--chip"):
        g = _rgb(t[ground])
        for fg, floor in (("--text", 7.0), ("--text-strong", 7.0), ("--muted", 4.5),
                          ("--blue", 4.5), ("--accent", 3.0), ("--green", 3.0),
                          ("--amber", 3.0), ("--err", 3.0)):
            r = _ratio(_rgb(t[fg]), g)
            assert r >= floor, f"{theme}: {fg} on {ground} is {r:.2f}, needs {floor}"


@pytest.mark.parametrize("theme", sorted(_THEMES))
def test_faint_text_is_still_legible(theme: str) -> None:
    t = _THEMES[theme]
    for ground in ("--bg", "--panel"):
        assert _ratio(_rgb(t["--faint"]), _rgb(t[ground])) >= 2.9


@pytest.mark.parametrize("theme", sorted(_THEMES))
def test_grade_chips_read_on_their_tint(theme: str) -> None:
    t = _THEMES[theme]
    panel = _rgb(t["--panel"])
    pairs = {"--grade-self": "--green-rgb", "--grade-api": "--blue-rgb",
             "--grade-obs": "--purple-rgb", "--grade-corr": "--amber-rgb"}
    for text, tint in pairs.items():
        bg = _over(tuple(float(x) for x in t[tint].split()), panel, 0.15)  # type: ignore[arg-type]
        r = _ratio(_rgb(t[text]), bg)
        assert r >= 4.5, f"{theme}: {text} on its tint is {r:.2f}"
    assert _ratio(_rgb(t["--danger-text"]), _rgb(t["--danger-bg"])) >= 4.5


@pytest.mark.parametrize("theme", sorted(_THEMES))
def test_graph_label_tokens_read(theme: str) -> None:
    t = _THEMES[theme]
    for text, bg in (("--label-project", "--label-project-bg"),
                     ("--label-community", "--label-community-bg")):
        # a label sits on the canvas colour with its own translucent chip
        canvas = _rgb(t["--panel"])
        m = re.match(r"rgba\(([^)]*)\)", t[bg])
        assert m
        r_, g_, b_, a_ = (float(x) for x in m.group(1).split(","))
        chip = _over((r_, g_, b_), canvas, a_)
        assert _ratio(_rgb(t[text]), chip) >= 4.5, (theme, text)
    assert _ratio(_rgb(t["--label-text"]), _rgb(t["--panel"])) >= 4.5


# --- the choice: persists, applies before paint, follows the device -------------------------
_HARNESS = Path(__file__).resolve().parent / "theme_harness.js"


@pytest.fixture(scope="module")
def theme_run() -> dict[str, dict[str, str | None]]:
    r = subprocess.run(["node", str(_HARNESS), str(_STATIC / "theme.js")],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)  # type: ignore[no-any-return]


def test_no_choice_means_follow_the_device(theme_run: dict) -> None:
    assert theme_run["fresh"] == {"attr": None, "mode": "system", "eff": "dark"}
    assert theme_run["freshLightOs"] == {"attr": None, "eff": "light"}


def test_a_choice_is_remembered_and_pins_the_page(theme_run: dict) -> None:
    assert theme_run["pinDark"] == {"attr": "dark", "stored": "dark", "eff": "dark"}


def test_a_saved_choice_applies_on_load(theme_run: dict) -> None:
    assert theme_run["restored"] == {"attr": "light", "eff": "light", "mode": "light"}


def test_system_clears_the_saved_choice(theme_run: dict) -> None:
    assert theme_run["backToSystem"] == {"attr": None, "stored": None, "mode": "system"}


def test_an_unknown_value_is_never_stored_or_applied(theme_run: dict) -> None:
    assert theme_run["junkIgnored"] == {"attr": None, "mode": "system"}
    assert theme_run["bogusStored"] == {"mode": "system", "attr": None}


def test_a_change_is_broadcast_for_the_graph(theme_run: dict) -> None:
    assert theme_run["event"] == {"mode": "light", "effective": "light"}


def test_blocked_storage_still_applies_for_this_page(theme_run: dict) -> None:
    assert theme_run["blockedStorage"] == {"mode": "light", "attr": "light"}


@pytest.mark.parametrize("name,src", [("index.html", "/ui/theme.js"), ("space.html", "theme.js")])
def test_pages_load_the_theme_before_the_stylesheet(name: str, src: str) -> None:
    html = _PAGES[name]
    tag = f'<script src="{src}"></script>'
    assert tag in html
    assert html.index(tag) < html.index('<link rel="stylesheet"')
    assert 'name="color-scheme"' in html


def test_settings_offers_the_three_choices() -> None:
    assert "settingsSectionShell('appearance', 'Appearance')" in _CONSOLE
    assert "THEME_CHOICES = [['system', 'Match this device']," in _CONSOLE
    assert "['light', 'Light'], ['dark', 'Dark']]" in _CONSOLE
    assert "OsirisTheme.set(mode)" in _CONSOLE


def test_the_graph_follows_the_theme() -> None:
    space = (_STATIC / "space.js").read_text()
    assert 'addEventListener("osiris-theme"' in space
    assert "uLight" in space and "uPaper" in space
    assert "themeIsLight()" in space

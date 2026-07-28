"""THE ONLY CODE THAT TOUCHES JINJA (task #71). Every other module in this subpackage
builds a typed Block tree (blocks.py) and hands it here; nothing else imports jinja2 or
the templates directory. `render_page` and `render_block` are the two entry points — a
builder (inbox.py) or a route (app.py) never formats HTML by hand."""
from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from src.api.inbox.blocks import ITEM_KIND_GLYPH, Block, Page

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATES_DIR)),
    autoescape=select_autoescape(["html", "j2"]),
    trim_blocks=True,
    lstrip_blocks=True,
)
_env.globals["ITEM_KIND_GLYPH"] = ITEM_KIND_GLYPH
_catalog = _env.get_template("catalog.html.j2")
_shell = _env.get_template("shell.html.j2")


def render_block(block: Block) -> str:
    """Any single Block (of the eight sanctioned kinds) to its HTML fragment — the
    fragment SSE patches send, via catalog.html.j2's own dispatcher macro."""
    return str(_catalog.module.render_block(block))  # type: ignore[attr-defined]


def render_page(page: Page) -> str:
    """A full Page (its regions, recursively) wrapped in the frozen page shell —
    what GET / returns whole."""
    body_html = str(_catalog.module.render_page(page))  # type: ignore[attr-defined]
    return _shell.render(title=page.title, body_html=body_html)

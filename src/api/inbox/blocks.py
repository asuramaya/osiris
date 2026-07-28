"""THE FROZEN BLOCK VOCABULARY — task #71 slice one, gated by Thoth (msg 1818, replying to
the plan+vocabulary-v0 proposal in msg 1815). This closed vocabulary IS the UI: every
surface in this subpackage (and, eventually, agent-composed panes elsewhere) builds a TREE
of these types; render.py is the ONLY code allowed to turn a tree into HTML, via
templates/catalog.html.j2 (one macro per `kind`). A new `kind` here is the one sanctioned
door for a new component — anywhere else, reaching for raw HTML is the slop channel this
architecture exists to close STRUCTURALLY (a mypy error), not by review discipline alone.

THREE RULINGS FROM THE GATE (msg 1818), each load-bearing here:
1. Button is a DELIBERATE 13th component beyond design-layer's original closed 12
   (masthead/queue-row/strip-row/status/data-table/kv/section/answer/log-entry/note/
   kbd-hint/empty) — approved because slice one needs it (approve/edit/reject/reply, the
   three-interrupt-type pattern, research-prior-art.md mechanism 6). `style='quiet'` is
   the default; `style='primary'` marks the SINGLE most-likely action and at most one may
   appear per ActionRow (enforced below, not left to catalog discipline alone); no third
   style exists.
2. Badge with `tone` in {ok, wait, fail} IS design-layer's `status` component wearing this
   vocabulary's naming — same rendering (glyph+word+desaturated hue, the ONLY colored
   thing on the page). `tone='neutral'` is a plain monochrome count/label. catalog.html.j2
   documents this equivalence explicitly so the two vocabularies (Python blocks, CSS
   components) read as ONE system, never two.
3. InboxItem.item_kind (notify/question/review) is a DIFFERENT AXIS from Badge's status
   tones — rendered as a MONOCHROME mono-face glyph (ITEM_KIND_GLYPH below), never
   colored, never sharing a slot with a status Badge on the same row. Color stays
   reserved exclusively for status tones.
"""
from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator


class Text(BaseModel):
    kind: Literal["text"] = "text"
    body: str
    # body = serif, authored prose (dossier/answer copy); muted = de-emphasized secondary
    # line; mono = operational/provenance text (timestamps, ids) — the type-face IS the
    # provenance signal (design-layer: serif=authored/settled, mono=operational/verify-me)
    style: Literal["body", "muted", "mono"] = "body"


class Badge(BaseModel):
    kind: Literal["badge"] = "badge"
    label: str
    # neutral = monochrome count/label, no color. ok/wait/fail ARE the `status` CSS
    # component (glyph+word+desaturated hue) wearing this name — see module docstring
    # ruling 2.
    tone: Literal["neutral", "ok", "wait", "fail"] = "neutral"


class Button(BaseModel):
    kind: Literal["button"] = "button"
    label: str
    action: str  # the POST /inbox/{id}/{action} verb this button fires
    style: Literal["primary", "quiet"] = "quiet"


class ActionRow(BaseModel):
    kind: Literal["action_row"] = "action_row"
    buttons: list[Button]

    @model_validator(mode="after")
    def _at_most_one_primary(self) -> ActionRow:
        primaries = sum(1 for b in self.buttons if b.style == "primary")
        if primaries > 1:
            raise ValueError(
                f"at most one 'primary' Button per ActionRow (msg 1818) — got {primaries}")
        return self


# notify/question/review is a DIFFERENT axis from Badge's status tones — a monochrome
# mono-face glyph, never colored (ruling 3 above). Approved verbatim, msg 1818.
ITEM_KIND_GLYPH: dict[str, str] = {"notify": "ℹ", "question": "?", "review": "⟲"}


class InboxItem(BaseModel):
    kind: Literal["inbox_item"] = "inbox_item"
    id: str  # the POST /inbox/{id}/{action} target every Button in `actions` fires against
    item_kind: Literal["notify", "question", "review"]
    title: str
    detail: Text | None = None
    age: str  # pre-formatted short age ("3d ago") per design-layer's dense-table rules
    actions: ActionRow | None = None


class InboxList(BaseModel):
    kind: Literal["inbox_list"] = "inbox_list"
    items: list[InboxItem]
    # the `empty` component (design-layer's 12th) — agents/builders never improvise this
    # string ad hoc; it is named here, once, as part of the frozen vocabulary
    empty_label: str = "Inbox clear."


class Region(BaseModel):
    kind: Literal["region"] = "region"
    # shell.html.j2's four named regions (approved, msg 1818) — aside is reserved for the
    # fleet-strip module (ruling 0b3dd431's immediate second builder), empty in slice one
    name: Literal["masthead", "main", "aside", "footer"]
    children: list[Block]


class Page(BaseModel):
    kind: Literal["page"] = "page"
    title: str
    regions: list[Region]


Block = Annotated[
    Text | Badge | Button | ActionRow | InboxItem | InboxList | Region | Page,
    Field(discriminator="kind"),
]

Region.model_rebuild()

# every sanctioned `kind` value — the lint gate (test_inbox_catalog.py) diffs this against
# catalog.html.j2's own macro names so the two vocabularies can never quietly drift apart
KIND_NAMES: frozenset[str] = frozenset(
    {"text", "badge", "button", "action_row", "inbox_item", "inbox_list", "region", "page"})

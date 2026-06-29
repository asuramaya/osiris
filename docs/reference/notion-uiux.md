<!-- source: https://raw.studio/blog/how-notion-ux-converts-100-million-users/ + https://dashibase.com/blog/notion-ui/ + notion.com/help | vendor: notion | topic: ui-ux complexity management -->
# Notion — carrying capability without the UI collapsing

The north star for Osiris's UI/UX pass. Notion holds an enormous feature surface (docs +
databases + relations + rollups + automations) yet *opens calm* — whitespace and a cursor,
not a wall of panels. Osiris has the opposite problem today: a dense fixed 3-pane console
shows **everything, all controls, all the time**, and it is collapsing under its own weight
(an overloaded inspector, a global toolbar, a hash-list sidebar, no command surface). This is
how Notion avoids that, and what to copy.

## The mechanisms (how Notion stays calm)

1. **Calm by default.** First paint is whitespace + typography + a cursor — *no* buttons
   competing for attention, no crowded dashboard. Users judge an interface in ~50ms; Notion
   spends that on calm. Content first; chrome second.
2. **Progressive disclosure.** Reveal complexity only as the user acts (hover, click, scroll,
   `/`). Hick's Law: fewer visible choices = faster decisions. Advanced power stays hidden
   until summoned — it's *there*, not *shown*.
3. **Hover-reveal controls.** A block's handle (⋮⋮) and `+` appear on hover, not persistently.
   The page is clean until you reach for a control.
4. **Contextual placement.** App/nav settings in the left sidebar; page settings upper-right;
   block actions *by the block*. Everything is where you'd reach for it — no menu-hunting,
   no global toolbar of everything.
5. **The slash menu (`/`).** ONE on-demand command surface, at the cursor, categorized
   (blocks / databases / inline / embeds), type-to-filter. It replaces toolbars — every action
   lives here, summoned where you are, dismissed when done.
6. **Command palette (`⌘K`).** Global navigation + actions by keyboard, so there's no need for
   visible top menus. Keyboard-driven momentum (don't make me hunt with the mouse).
7. **One primitive (the block).** Everything is a block; every page and database is the *same*
   shape. You **compose, you don't configure**. Consistency removes decisions.
8. **Constrained customization ("can't make it ugly").** Restricted fonts/widths/styling →
   visual consistency for free; the user never has to make a design decision.
9. **Peek / side-peek.** Open an item in a panel *without losing your place* — context
   preserved, not a navigation away.
10. **In-place manipulation.** Drag-and-drop, inline edit — no modal dialogs that hide the work.
11. **Templates.** Pre-structured starting points beat the blank page.

## Where Osiris collapses today → the Notion fix

| Osiris strain (now) | Notion mechanism |
|---|---|
| Fixed 3-pane console shows compositions + object-set + board + inspector **always**, dense from first paint | **Calm by default** + **progressive disclosure** — collapse what isn't in use; open quiet |
| The inspector is overloaded — merge-review **and** object detail **and** props **and** rels crammed in one narrow rail | **Peek (one thing at a time)** + **contextual placement** — review is its own surface; the inspector is just the focused object |
| A global toolbar (Fit / Re-layout / Clear / Search-around / Expand / Save-as) always visible | **Slash menu / ⌘K** — one command surface; actions summoned, not parked on screen |
| Scattered buttons, mouse-hunting, no keyboard path | **⌘K palette** + keyboard momentum |
| Object-set is a flat dense list of `commit:hash…` with no hierarchy | **Hover-reveal** + typographic hierarchy + collapse |
| Per-node / per-row controls are absent or always-on | **Hover-reveal controls** |
| The renderer already picks the view by shape (good) | **Constrained "can't make it ugly"** — keep leaning on this; the stress test guards it |

## The design rules for the pass (testable north-star)

1. **Open calm.** The console's first paint is the focused subject (or a brief), not three
   dense panels. Empty state is whitespace + one prompt, never a wall.
2. **One command surface.** A `⌘K` / `/` palette is the home of every action (run a lens,
   focus, search-around, expand, save, switch room). The global toolbar shrinks toward zero.
3. **Disclose on demand.** Panels and controls collapse by default and reveal on hover/click.
   Nothing is shown that isn't being used right now.
4. **One panel, one job.** The inspector shows the focused object — nothing else. Review,
   compose, and settings are their own summoned surfaces (peek), not co-tenants of the rail.
5. **Contextual, not global.** Board actions live by the board; object actions in the
   inspector; nav in the sidebar. Kill the everything-toolbar.
6. **Consistency over configuration.** Every object renders in the one card shape (schema-
   driven, already true); every result through the one shape-aware renderer. The user composes
   meaning; they never style.
7. **Keep it un-ugly by construction.** Constrained spacing/typography/width; the render
   stress test (`tests/test_ui_render.py`) is the guardrail that this can't regress.

The test of the pass: a newcomer opens Osiris and sees something *calm and legible*, and a
power user can reach *every* capability from one keystroke — the Notion bargain, on the
entity-graph substrate.

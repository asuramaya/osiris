"""THE SLASH-FILE SIZE DIET (dispatch e6585927, msg 7882 item 3, operator's word
2026-09-06): every `commands/*.md` file is paid IN FULL on every invocation of that slash
command — a prompt cost independent of what the caller actually needs this turn. `/seat`
was the worst offender (12.9 KB of per-verb manual pasted into the prompt regardless of
which one verb was being run) and is now a bare subcommand list pointing at
`describe('seat')`/`describe('seat:<verb>')` for the full text — the same move
`_NAG_CATALOG` already made for advisory nags. This test is the ratchet: it runs against
the real, live `commands/` directory (never a hand-copied byte count) so a future slash
file that grows past the bar fails here, not in a review nobody happened to notice on.
"""
from __future__ import annotations

from scripts.commands_status import REPO_ROOT

SIZE_CEILING_BYTES = 1024


def test_every_slash_command_file_is_under_the_size_ceiling() -> None:
    over = []
    for path in sorted((REPO_ROOT / "commands").glob("*.md")):
        size = path.stat().st_size
        if size >= SIZE_CEILING_BYTES:
            over.append((path.name, size))
    assert not over, (
        f"these slash command files are at or over the {SIZE_CEILING_BYTES}-byte ceiling "
        f"(paid on every invocation, regardless of what the caller needs this turn): "
        f"{over} — move the long-form prose into describe() (the _NAG_CATALOG/_SEAT_MANUAL "
        "convention), never just re-wrap the same words tighter forever")

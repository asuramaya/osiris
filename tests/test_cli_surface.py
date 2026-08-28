"""THE CLI'S FRONT DOOR MUST NOT LIE ABOUT ITSELF.

Two promises the top-level help makes to a human, both previously unguarded, both found
broken or nearly-broken by the 2026-08-28 surface audit:

  1. "COMMANDS, GROUPED BY WHAT YOU'RE TRYING TO DO" read as a complete map and was not —
     roster, retention, fold-project and rematerialize existed as subcommands and appeared
     nowhere in it. `roster` is the worst of those: #140 shipped it as the "who owns this
     seat" read verb and the guide never named it, which is this house's recurring
     shipped-and-never-adopted failure showing up in the help text itself.
  2. "Run `osiris <command> --help` for ... a worked example." This one turned out TRUE for
     all 21 commands when measured correctly — an earlier count that said four were missing
     had matched only lines beginning "example:" and missed "example, count only:" and its
     siblings. The guard is here anyway, because the promise is only worth making if
     something enforces it on command 22.

Both tests read the REAL parser, never a hand-maintained list, so a new subcommand cannot
satisfy them by being added to a duplicate roll-call somewhere.
"""

from __future__ import annotations

import argparse

import pytest
from src.cli import _build_parser as build_parser


def _subparsers(parser: argparse.ArgumentParser) -> dict[str, argparse.ArgumentParser]:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return dict(action.choices)
    raise AssertionError("the osiris parser has no subcommands — did build_parser change?")


def test_every_subcommand_appears_in_the_grouped_guide() -> None:
    """The guide is a MAP, and a map that omits four of twenty-one destinations is worse
    than no map: the reader has no way to know it is partial."""
    parser = build_parser()
    names = set(_subparsers(parser))
    guide = parser.description or ""
    body = guide.split("COMMANDS, GROUPED BY WHAT YOU'RE TRYING TO DO:", 1)
    assert len(body) == 2, "the grouped guide section is gone from the top-level help"
    missing = sorted(n for n in names if n not in body[1])
    assert not missing, (
        f"these subcommands exist but appear nowhere in the grouped guide: {missing}. "
        "Add them to a group — a human reading that list treats it as complete.")


def test_every_subcommand_carries_a_worked_example() -> None:
    """The top-level help promises one per command. Enforced here so command 22 cannot
    quietly break the promise."""
    without = sorted(name for name, sub in _subparsers(build_parser()).items()
                     if "example" not in (sub.epilog or "").lower())
    assert not without, (
        f"these subcommands have no worked example in their epilog: {without}. "
        "The top-level help promises every command has one.")


@pytest.mark.parametrize("command", ["fleet", "roster", "unmerge", "retention"])
def test_read_verbs_offer_the_machine_door(command: str) -> None:
    """Every verb that used to print `json.dumps(..., indent=2)` at a human now renders for
    the human AND keeps --json for a script or an agent. Neither audience is served the
    other's format, and the flag must not silently disappear in a refactor."""
    sub = _subparsers(build_parser())[command]
    flags = {opt for action in sub._actions for opt in action.option_strings}
    assert "--json" in flags, f"osiris {command} lost its --json escape hatch"

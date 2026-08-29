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
import inspect

from src import cli as cli_mod
from src.cli import _build_parser as build_parser


def _cmd_function(name: str) -> object:
    """The REAL command function for a subcommand, by the naming convention every
    subcommand in this file already follows (`fold-project` -> `cmd_fold_project`) — never
    a hand-maintained name->function table."""
    fn = getattr(cli_mod, f"cmd_{name.replace('-', '_')}", None)
    assert fn is not None, (
        f"osiris {name} has no cmd_{name.replace('-', '_')} — naming convention drift")
    return fn


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


def test_every_command_that_renders_structured_output_offers_the_machine_door() -> None:
    """DERIVED FROM THE REAL IMPLEMENTATION, never a hand-maintained list of read verbs
    (2026-08-28 CLI-surface audit, thread 00913be9's own follow-up): the earlier version of
    this test named `["fleet", "roster", "unmerge", "retention"]` by hand — the exact shape
    this module's own docstring warns against ("never a hand-maintained list, so a new
    subcommand cannot satisfy them by being added to a duplicate roll-call somewhere"), and
    it silently missed `desk`/`show` the moment they shipped. Any command whose own source
    calls `render.emit` is showing a human a structure, so cli_render.py's own promise
    (a human render AND `--json` for nothing extra) must hold for it — checked by reading
    `inspect.getsource` on the REAL `cmd_*` function, not a name a person remembered to
    add. A future command that starts emitting structured output is covered for free; one
    that means to offer `--json` but drops the flag in a refactor is caught the same way."""
    missing = []
    for name, sub in _subparsers(build_parser()).items():
        fn = _cmd_function(name)
        if "render.emit(" not in inspect.getsource(fn):  # type: ignore[arg-type]
            continue
        flags = {opt for action in sub._actions for opt in action.option_strings}
        if "--json" not in flags:
            missing.append(name)
    assert not missing, (
        f"these commands render structured output via cli_render.emit but offer no "
        f"--json escape hatch: {missing}")


# --- the stop door (2026-08-28) ------------------------------------------------------------

def test_launch_has_an_inverse_on_the_same_surface() -> None:
    """THE ASYMMETRY THIS CLOSED: `osiris launch` had a terminal door since #72 and stop had
    none, so a human could START a body from the shell and had no way to END one from it.
    Every exit was a raw kill by hand — untracked, unaudited, and exactly the "dead ends and
    corpses" the operator named. A lifecycle with only one door is not a lifecycle."""
    names = set(_subparsers(build_parser()))
    assert "launch" in names
    assert "stop" in names, "launch exists with no inverse on the same surface"


def test_stop_calls_the_same_function_the_mcp_tool_does() -> None:
    """One implementation, two doors. If the CLI grew its own copy of the stop logic, the
    two surfaces would drift the way #135's bootstrap path did — caught there by the parity
    gate, prevented here by construction."""
    import inspect

    from src.cli import cmd_stop
    assert "stop_seat" in inspect.getsource(cmd_stop)


def test_stop_help_states_the_no_live_body_exit_contract() -> None:
    """A teardown loop must not go red because the thing it was cleaning up was already
    gone. That contract only helps if it is written where the person scripting it looks."""
    sub = _subparsers(build_parser())["stop"]
    blurb = f"{sub.description or ''} {sub.epilog or ''}".lower()
    assert "no-live-body" in blurb
    assert "0" in blurb

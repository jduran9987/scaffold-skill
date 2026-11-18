"""Actions the skill can run, each a subcommand of the command-line entry point."""

import argparse
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Action:
    """A subcommand of the skill.

    Attributes:
        name: Subcommand name, such as `scaffold`.
        help: One-line description shown in the command's help.
        add_arguments: Adds the action's arguments to its subcommand parser.
        run: Runs the action with the parsed arguments and returns the exit code.
    """

    name: str
    help: str
    add_arguments: Callable[[argparse.ArgumentParser], None]
    run: Callable[[argparse.Namespace], int]

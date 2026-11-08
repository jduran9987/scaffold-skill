"""Command line for v1: routes a subcommand to one of v1's actions."""

import argparse
from collections.abc import Sequence

from sp_dynamo_firehose_s3.v1.actions import (
    Action,
    create_stream,
    generate_yaml,
    scaffold,
)

ACTIONS: dict[str, Action] = {
    action.name: action
    for action in [scaffold.ACTION, generate_yaml.ACTION, create_stream.ACTION]
}


def build_parser() -> argparse.ArgumentParser:
    """Builds the parser with one subcommand per action.

    Returns:
        The command-line parser.
    """
    parser = argparse.ArgumentParser(
        prog="generate",
        description="Run an action for the DynamoDB, Firehose, S3 pipeline pattern.",
    )
    subparsers = parser.add_subparsers(dest="action", required=True, metavar="action")
    for action in ACTIONS.values():
        action.add_arguments(subparsers.add_parser(action.name, help=action.help))
    return parser


def main(argv: Sequence[str]) -> int:
    """Runs the v1 action named on the command line.

    Args:
        argv: Arguments after the program name, starting with the action.

    Returns:
        The process exit code.
    """
    args = build_parser().parse_args(argv)
    return ACTIONS[args.action].run(args)

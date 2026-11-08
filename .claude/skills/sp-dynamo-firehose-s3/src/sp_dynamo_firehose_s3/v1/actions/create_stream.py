"""The `create-stream` action: streams a DynamoDB table to S3 through Firehose."""

import argparse

from sp_dynamo_firehose_s3.v1.actions import Action
from sp_dynamo_firehose_s3.v1.streaming import create_stream


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Adds the create-stream action's arguments.

    Args:
        parser: The action's subcommand parser.
    """
    parser.add_argument("table", help="DynamoDB table to stream.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Only report what exists and what would be created.",
    )


def run(args: argparse.Namespace) -> int:
    """Checks the table's streams and creates the missing ones unless checking.

    The whole report goes to standard output, so it reads in one piece.

    Args:
        args: Parsed arguments for the action.

    Returns:
        The process exit code.
    """
    report = create_stream(args.table, args.check)
    print(report.text, end="")
    return 0 if report.ok else 1


ACTION = Action(
    name="create-stream",
    help="Stream a DynamoDB table to S3 through Kinesis and Firehose.",
    add_arguments=add_arguments,
    run=run,
)

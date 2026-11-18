"""Command-line arguments shared by the pipeline's jobs."""

import argparse
from collections.abc import Sequence


def parse_args(
    description: str, argv: Sequence[str] | None = None
) -> argparse.Namespace:
    """Parses a job's command-line arguments.

    Args:
        description: What the job does, shown in its help text.
        argv: Arguments to parse. Defaults to the process arguments.

    Returns:
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--env", required=True, help="Deployment environment.")
    return parser.parse_args(argv)

"""The `scaffold` action: generates a pipeline project from a v1 YAML config."""

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from sp_dynamo_firehose_s3.v1.actions import Action
from sp_dynamo_firehose_s3.v1.generator import ProjectInitError, generate


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Adds the scaffold action's arguments.

    Args:
        parser: The action's subcommand parser.
    """
    parser.add_argument("config_path", type=Path, help="Path to the pipeline YAML.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        help=(
            "Project directory. Defaults to ./<projects_dir>/<metadata.name>/, "
            "with projects_dir set in defaults.toml."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing output files.",
    )


def load_config(config_path: Path) -> dict[str, Any]:
    """Loads a pipeline YAML file.

    Args:
        config_path: Path to the pipeline YAML.

    Returns:
        The parsed YAML mapping.

    Raises:
        ValueError: If the top level of the file is not a mapping.
    """
    raw_config = yaml.safe_load(config_path.read_text())
    if not isinstance(raw_config, dict):
        raise ValueError(f"Expected a YAML mapping in {config_path}")
    return raw_config


def run(args: argparse.Namespace) -> int:
    """Generates the pipeline project from a YAML config.

    Args:
        args: Parsed arguments for the action.

    Returns:
        The process exit code.
    """
    try:
        paths = generate(load_config(args.config_path), args.output_dir, args.force)
    except (
        OSError,
        ValueError,
        ValidationError,
        yaml.YAMLError,
        ProjectInitError,
    ) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    for path in paths:
        print(f"Generated {path}")
    return 0


ACTION = Action(
    name="scaffold",
    help="Generate a pipeline project from a YAML config.",
    add_arguments=add_arguments,
    run=run,
)

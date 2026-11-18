"""Command-line entry point that hands the arguments to the config's version.

This is the only code shared by all versions. When the action's first argument is
an existing pipeline YAML, it reads the file's `version` key and passes the
arguments, unchanged, to that version's `main`. Otherwise, such as for an action
that creates the YAML or takes a table name, the latest version handles the
arguments. Each version owns its actions, argument parsing, and everything else.
"""

import sys
from collections.abc import Callable, Sequence
from pathlib import Path

import yaml

from sp_dynamo_firehose_s3 import v1

YAML_SUFFIXES = frozenset({".yaml", ".yml"})
VERSIONS: dict[str, Callable[[Sequence[str]], int]] = {"v1": v1.main}
LATEST_VERSION = "v1"


def read_version(config_path: Path) -> str:
    """Reads the version from a pipeline YAML file.

    Args:
        config_path: Path to the pipeline YAML.

    Returns:
        The version, such as `v1`.

    Raises:
        ValueError: If the file is not a YAML mapping or its version is not
            supported.
    """
    raw_config = yaml.safe_load(config_path.read_text())
    if not isinstance(raw_config, dict):
        raise ValueError(f"Expected a YAML mapping in {config_path}")
    version = str(raw_config.get("version"))
    if version not in VERSIONS:
        supported = ", ".join(sorted(VERSIONS))
        raise ValueError(f"Unsupported version: {version}. Supported: {supported}")
    return version


def main(argv: Sequence[str] | None = None) -> int:
    """Runs the action with the version named in its YAML config.

    Without an existing `.yaml` or `.yml` file as the first argument, such as for
    `--help`, an action that creates the YAML, or one that takes a table name, the
    latest version handles the arguments.

    Args:
        argv: Arguments after the program name. Defaults to the process arguments.

    Returns:
        The process exit code.
    """
    args = list(sys.argv[1:] if argv is None else argv)
    if (
        len(args) < 2
        or args[0].startswith("-")
        or args[1].startswith("-")
        or Path(args[1]).suffix not in YAML_SUFFIXES
        or not Path(args[1]).is_file()
    ):
        return VERSIONS[LATEST_VERSION](args)
    try:
        version = read_version(Path(args[1]))
    except (OSError, ValueError, yaml.YAMLError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return VERSIONS[version](args)

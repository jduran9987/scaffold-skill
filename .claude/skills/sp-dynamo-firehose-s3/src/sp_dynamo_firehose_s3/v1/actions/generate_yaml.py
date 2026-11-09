"""The `generate-yaml` action: writes a v1 pipeline YAML to fill in.

The YAML for table `BO.Accounts` is written to `pipelines/bo-accounts.yaml`, with
the directory set by `cli.pipelines_dir` in `defaults.toml`, and its `schema`
section is inferred from the table.
"""

import argparse
from collections import Counter
from enum import StrEnum
from pathlib import Path

from sp_dynamo_firehose_s3.v1.actions import Action
from sp_dynamo_firehose_s3.v1.aws import AwsError, AwsErrorKind
from sp_dynamo_firehose_s3.v1.defaults import DEFAULTS
from sp_dynamo_firehose_s3.v1.fields import DYNAMO_TABLE_NAME_PATTERN
from sp_dynamo_firehose_s3.v1.generator import hyphenate, render_config_template
from sp_dynamo_firehose_s3.v1.report import format_report, format_result
from sp_dynamo_firehose_s3.v1.schema_inference import (
    InferredSchema,
    SchemaInferenceError,
    infer_table_schema,
)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Adds the generate-yaml action's arguments.

    Args:
        parser: The action's subcommand parser.
    """
    parser.add_argument(
        "table",
        help=(
            "DynamoDB table to infer the schema from. The YAML is written to "
            "<pipelines_dir>/<table, hyphenated>.yaml, with pipelines_dir set in "
            "defaults.toml."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the file if it exists.",
    )


class Outcome(StrEnum):
    """How a run ended, other than an AWS CLI failure."""

    GENERATED = "GENERATED"
    INVALID_TABLE_NAME = "INVALID_TABLE_NAME"
    FILE_EXISTS = "FILE_EXISTS"
    TABLE_NOT_FOUND = "TABLE_NOT_FOUND"
    KEY_NOT_USABLE = "KEY_NOT_USABLE"
    WRITE_FAILED = "WRITE_FAILED"


# The explanation and next step for each outcome. Values come only from the run.
OUTCOME_TEXT: dict[Outcome, tuple[str, str]] = {
    Outcome.GENERATED: (
        "Wrote {path} with every supported field and {columns} columns inferred "
        "from DynamoDB table {table}.{duplicates}",
        "Fill in the fields marked `# Required field, please fill.`, including "
        "each column's targetType, then run scaffold {path}.",
    ),
    Outcome.INVALID_TABLE_NAME: (
        "{table} is not a DynamoDB table name. Nothing was written.",
        "Use the table's exact name: 3 to 255 letters, digits, underscores, "
        "dashes, or dots.",
    ),
    Outcome.FILE_EXISTS: (
        "{path} already exists. Nothing was written.",
        "Rerun with --force to overwrite it.",
    ),
    Outcome.TABLE_NOT_FOUND: (
        "DynamoDB table {table} does not exist in {region} for the AWS CLI "
        "profile {profile}. Nothing was written.",
        "Check the table name, or aws_region and aws_profile in defaults.toml, "
        "then rerun.",
    ),
    Outcome.KEY_NOT_USABLE: (
        "{reason}. Nothing was written.",
        "This table's schema cannot be inferred by the skill.",
    ),
    Outcome.WRITE_FAILED: (
        "{path} could not be written: {reason}.",
        "Check the path, then rerun.",
    ),
}


def duplicate_names(schema: InferredSchema) -> list[str]:
    """Finds column names that more than one attribute path got.

    Args:
        schema: The inferred schema.

    Returns:
        The repeated names, sorted.
    """
    counts = Counter(column.name for column in schema.columns)
    return sorted(name for name, count in counts.items() if count > 1)


def config_path_for(table: str) -> Path:
    """Builds the path of a table's pipeline YAML.

    Args:
        table: DynamoDB table name, such as `BO.Accounts`.

    Returns:
        `<pipelines_dir>/<table, hyphenated>.yaml`, such as
        `pipelines/bo-accounts.yaml`.
    """
    return Path(DEFAULTS.cli.pipelines_dir) / f"{hyphenate(table)}.yaml"


def write_config_template(table: str, force: bool) -> tuple[Outcome, dict]:
    """Writes the table's pipeline YAML with a schema inferred from DynamoDB.

    Nothing is written if the file exists without `force` or the schema cannot
    be inferred.

    Args:
        table: DynamoDB table name.
        force: Whether to overwrite an existing file.

    Returns:
        The outcome and the values its text is filled in with.

    Raises:
        AwsError: If an AWS CLI command fails for a reason other than a missing
            table.
    """
    config_path = config_path_for(table)
    facts: dict[str, object] = {
        "path": config_path,
        "table": table,
        "profile": DEFAULTS.cli.aws_profile,
        "region": DEFAULTS.cli.aws_region,
    }
    if not DYNAMO_TABLE_NAME_PATTERN.fullmatch(table):
        return Outcome.INVALID_TABLE_NAME, facts
    if config_path.exists() and not force:
        return Outcome.FILE_EXISTS, facts
    try:
        schema = infer_table_schema(table)
    except AwsError as error:
        if error.kind is AwsErrorKind.NOT_FOUND:
            return Outcome.TABLE_NOT_FOUND, facts
        raise
    except SchemaInferenceError as error:
        return Outcome.KEY_NOT_USABLE, facts | {"reason": error}
    try:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(render_config_template(schema, table))
    except OSError as error:
        return Outcome.WRITE_FAILED, facts | {"reason": error.strerror or error}
    duplicates = duplicate_names(schema)
    note = (
        f" Columns named {', '.join(duplicates)} come from more than one path; "
        "rename them."
        if duplicates
        else ""
    )
    return Outcome.GENERATED, facts | {
        "columns": len(schema.columns),
        "duplicates": note,
    }


def run(args: argparse.Namespace) -> int:
    """Writes a pipeline YAML with every supported field to fill in.

    The whole report goes to standard output, so it reads in one piece.

    Args:
        args: Parsed arguments for the action.

    Returns:
        The process exit code.
    """
    title = f"generate-yaml {args.table}"
    try:
        outcome, facts = write_config_template(args.table, args.force)
    except AwsError as error:
        result = format_result(error.kind.value, error.message(), error.next_step())
        print(format_report(title, [], result), end="")
        return 1
    message, next_step = OUTCOME_TEXT[outcome]
    result = format_result(outcome, message.format(**facts), next_step.format(**facts))
    print(format_report(title, [], result), end="")
    return 0 if outcome is Outcome.GENERATED else 1


ACTION = Action(
    name="generate-yaml",
    help="Write a pipeline YAML with every supported field to fill in.",
    add_arguments=add_arguments,
    run=run,
)

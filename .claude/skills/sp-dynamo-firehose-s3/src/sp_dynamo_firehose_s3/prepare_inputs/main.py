"""The prepare-inputs command's entry point.

`run` builds and checks the configuration, reads the table's keys and a sample of
its items, flattens them into columns, and writes the columns for scaffold. Every
run prints one fixed report. A run that succeeds first prints the columns, each
with the Spark types it can be cast to, for the agent only.
"""

import json
import sys
from collections.abc import Sequence

from sp_dynamo_firehose_s3.aws import AwsError, AwsErrorKind
from sp_dynamo_firehose_s3.prepare_inputs.columns import (
    Columns,
    Stop,
    describe_keys,
    infer_columns,
    scan_items,
    write_columns,
)
from sp_dynamo_firehose_s3.prepare_inputs.config import (
    Config,
    ConfigError,
    build_config,
)
from sp_dynamo_firehose_s3.prepare_inputs.report import Code, Report, build_report

COMMAND = "prepare-inputs"
COLUMNS_HEADING = "## Columns (for the agent)"

AWS_CODES: dict[AwsErrorKind, Code] = {
    AwsErrorKind.CLI_MISSING: Code.AWS_CLI_MISSING,
    AwsErrorKind.CREDENTIALS: Code.AWS_CREDENTIALS,
    AwsErrorKind.ACCESS_DENIED: Code.AWS_ACCESS_DENIED,
    AwsErrorKind.NOT_FOUND: Code.AWS_ERROR,
    AwsErrorKind.OTHER: Code.AWS_ERROR,
}


def agent_data(config: Config, columns: Columns) -> str:
    """Formats what the agent reads to pick each column's Spark type.

    Args:
        config: The run's configuration.
        columns: Everything found in the table.

    Returns:
        One JSON object per column, with the Spark types it can be cast to,
        under a fixed heading.
    """
    allowed_types = config.schema_inference.allowed_types
    lines = [COLUMNS_HEADING]
    for column in columns.columns:
        shown = {
            "name": column.name,
            "dynamo_type": column.dynamo_type,
            "key": column.key,
            "examples": list(column.examples),
            "allowed_types": allowed_types.for_type(column.dynamo_type),
        }
        lines.append(json.dumps(shown, ensure_ascii=False))
    return "\n".join(lines) + "\n"


def run(args: Sequence[str]) -> Report:
    """Runs prepare-inputs and builds its report.

    Args:
        args: Arguments after the command: the session ID, then the table name.

    Returns:
        The report.
    """
    if len(args) != 2:
        return build_report(COMMAND, Code.USAGE, count=len(args))
    session_id, table = args
    title = f"{COMMAND} {table}"

    try:
        config = build_config(table, session_id)
    except ConfigError as error:
        return build_report(title, Code.CONFIG_INVALID, problems=error.problems)

    facts = {
        "table": config.dynamo_table_name,
        "profile": config.aws.profile,
        "region": config.aws.region,
    }
    try:
        columns = infer_columns(describe_keys(config), scan_items(config))
        write_columns(config, columns)
    except Stop as stop:
        return build_report(title, stop.code, **facts, **stop.facts)
    except AwsError as error:
        return build_report(
            title,
            AWS_CODES[error.kind],
            **facts,
            operation=error.operation,
            aws_message=error.aws_message,
        )

    return build_report(
        title,
        Code.COLUMNS_FOUND,
        agent_data(config, columns),
        **facts,
        columns=len(columns.columns),
        items=columns.item_count,
        skipped=", ".join(columns.skipped) or "none",
    )


def main() -> int:
    """Runs prepare-inputs with the process arguments and prints its output.

    Returns:
        0 if the run succeeded, otherwise 1.
    """
    report = run(sys.argv[1:])
    print(report.output, end="")
    return 0 if report.ok else 1

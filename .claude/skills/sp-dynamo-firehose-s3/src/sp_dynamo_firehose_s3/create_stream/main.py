"""The create-stream command's entry point.

`run` builds and checks the configuration, then runs the steps in order: the
DynamoDB table, the Kinesis stream, the table's streaming to it, and the
Firehose stream. Every run prints one fixed report.
"""

import sys
import time
from collections.abc import Sequence

from sp_dynamo_firehose_s3.aws import AwsError, AwsErrorKind
from sp_dynamo_firehose_s3.create_stream.config import Config, ConfigError, build_config
from sp_dynamo_firehose_s3.create_stream.report import Code, Report, build_report
from sp_dynamo_firehose_s3.create_stream.steps import (
    Resource,
    Status,
    Stop,
    check_no_other_stream,
    check_table,
    ensure_firehose_stream,
    ensure_kinesis_stream,
    ensure_streaming,
    resource_labels,
)

COMMAND = "create-stream"

AWS_CODES: dict[AwsErrorKind, Code] = {
    AwsErrorKind.CLI_MISSING: Code.AWS_CLI_MISSING,
    AwsErrorKind.CREDENTIALS: Code.AWS_CREDENTIALS,
    AwsErrorKind.ACCESS_DENIED: Code.AWS_ACCESS_DENIED,
    AwsErrorKind.NOT_FOUND: Code.AWS_ERROR,
    AwsErrorKind.OTHER: Code.AWS_ERROR,
}


def config_facts(config: Config) -> dict[str, object]:
    """Collects the configuration values report text can mention.

    Args:
        config: The run's configuration.

    Returns:
        The values, by the names report text uses.
    """
    return {
        "table": config.dynamo_table_name,
        "kinesis": config.kinesis.name,
        "firehose": config.firehose.name,
        "bucket_arn": config.firehose.bucket_arn,
        "profile": config.aws.profile,
        "region": config.aws.region,
        "timeout": config.wait.timeout_seconds,
    }


def resource_lines(config: Config, statuses: dict[Resource, Status]) -> list[str]:
    """Builds the report's line for each resource.

    Args:
        config: The run's configuration.
        statuses: What the run found or did for each resource.

    Returns:
        One numbered line per resource, in step order.
    """
    labels = resource_labels(config)
    return [
        f"{resource.value}. {labels[resource]}: {statuses[resource]}"
        for resource in Resource
    ]


def run(args: Sequence[str]) -> Report:
    """Runs create-stream and builds its report.

    Args:
        args: Arguments after the command, which must be one table name.

    Returns:
        The report.
    """
    title = " ".join([COMMAND, *args])
    if len(args) != 1:
        return build_report(title, Code.USAGE, count=len(args))

    try:
        config = build_config(args[0])
    except ConfigError as error:
        return build_report(title, Code.CONFIG_INVALID, problems=error.problems)

    facts = config_facts(config)
    statuses = dict.fromkeys(Resource, Status.NOT_CHECKED)
    current = Resource.TABLE
    try:
        statuses[Resource.TABLE] = check_table(config)
        current = Resource.STREAMING
        check_no_other_stream(config)
        deadline = time.monotonic() + config.wait.timeout_seconds

        current = Resource.KINESIS
        kinesis_status, stream_arn = ensure_kinesis_stream(config, deadline)
        statuses[Resource.KINESIS] = kinesis_status

        current = Resource.STREAMING
        statuses[Resource.STREAMING] = ensure_streaming(config, stream_arn, deadline)

        current = Resource.FIREHOSE
        statuses[Resource.FIREHOSE] = ensure_firehose_stream(
            config, stream_arn, deadline
        )
    except Stop as stop:
        statuses[current] = stop.resource_status
        lines = resource_lines(config, statuses)
        return build_report(title, stop.code, lines, **facts, **stop.facts)
    except AwsError as error:
        statuses[current] = Status.FAILED
        lines = resource_lines(config, statuses)
        return build_report(
            title,
            AWS_CODES[error.kind],
            lines,
            **facts,
            operation=error.operation,
            aws_message=error.aws_message,
        )

    created = Status.CREATED in statuses.values()
    code = Code.CREATED if created else Code.ALREADY_EXISTED
    return build_report(title, code, resource_lines(config, statuses), **facts)


def main() -> int:
    """Runs create-stream with the process arguments and prints its report.

    Returns:
        0 if the run succeeded, otherwise 1.
    """
    report = run(sys.argv[1:])
    print(report.text, end="")
    return 0 if report.ok else 1

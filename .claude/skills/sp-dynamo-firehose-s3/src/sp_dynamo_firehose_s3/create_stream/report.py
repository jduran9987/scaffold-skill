"""Result codes for create-stream and the fixed text for each."""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from sp_dynamo_firehose_s3.result import format_report


class Code(StrEnum):
    """How a create-stream run ended."""

    CREATED = "CREATED"
    ALREADY_EXISTED = "ALREADY_EXISTED"
    USAGE = "USAGE"
    CONFIG_INVALID = "CONFIG_INVALID"
    TABLE_NOT_FOUND = "TABLE_NOT_FOUND"
    TABLE_STREAMS_ELSEWHERE = "TABLE_STREAMS_ELSEWHERE"
    KINESIS_FAILED = "KINESIS_FAILED"
    STREAMING_FAILED = "STREAMING_FAILED"
    FIREHOSE_FAILED = "FIREHOSE_FAILED"
    TIMEOUT = "TIMEOUT"
    AWS_CLI_MISSING = "AWS_CLI_MISSING"
    AWS_CREDENTIALS = "AWS_CREDENTIALS"
    AWS_ACCESS_DENIED = "AWS_ACCESS_DENIED"
    AWS_ERROR = "AWS_ERROR"


SUCCESS_CODES = frozenset({Code.CREATED, Code.ALREADY_EXISTED})

CONNECTED = (
    "DynamoDB table {table} streams to Kinesis stream {kinesis}, which Firehose "
    "stream {firehose} delivers to {bucket_arn}."
)

# The message and next step for each code. `{...}` fields are filled in from the
# run's facts; the values themselves are never formatted again.
TEXT: dict[Code, tuple[str, str]] = {
    Code.CREATED: (f"Every resource is active. {CONNECTED}", "None."),
    Code.ALREADY_EXISTED: (
        f"Every resource already existed and is active. {CONNECTED}",
        "None.",
    ),
    Code.USAGE: (
        "create-stream takes exactly one argument, the DynamoDB table name, "
        "but got {count}.",
        "Rerun the skill.",
    ),
    Code.CONFIG_INVALID: (
        "The table name or inputs.toml is invalid: {problems}.",
        "Fix what is listed, then rerun the skill.",
    ),
    Code.TABLE_NOT_FOUND: (
        "DynamoDB table {table} does not exist in {region} for AWS CLI profile "
        "{profile}.",
        "Check the table name, or aws.profile and aws.region in inputs.toml, then "
        "rerun the skill.",
    ),
    Code.TABLE_STREAMS_ELSEWHERE: (
        "DynamoDB table {table} already streams to {other_stream} (status "
        "{other_status}). A table can stream to only one Kinesis stream, and "
        "create-stream uses Kinesis stream {kinesis}. Nothing was created.",
        "Disable the table's streaming to {other_stream} yourself, then rerun "
        "the skill.",
    ),
    Code.KINESIS_FAILED: (
        "Kinesis stream {kinesis} has status {status}, so it cannot be used.",
        "Check Kinesis stream {kinesis} in AWS, then rerun the skill.",
    ),
    Code.STREAMING_FAILED: (
        "Streaming from DynamoDB table {table} to Kinesis stream {kinesis} has "
        "status {status}. {details}",
        "Fix the cause, then rerun the skill.",
    ),
    Code.FIREHOSE_FAILED: (
        "Firehose stream {firehose} has status {status}. {details}",
        "Delete Firehose stream {firehose} yourself, fix the cause, then rerun "
        "the skill.",
    ),
    Code.TIMEOUT: (
        "{resource} was still {status} after waiting {timeout} seconds in total.",
        "Rerun the skill. It skips what exists and waits again.",
    ),
    Code.AWS_CLI_MISSING: (
        "The AWS CLI is not installed or not on PATH.",
        "Install the AWS CLI, then rerun the skill.",
    ),
    Code.AWS_CREDENTIALS: (
        "AWS CLI profile {profile} has no valid credentials, or its session "
        "expired. AWS said: {aws_message}",
        "Run `aws sso login --profile {profile}` yourself, then rerun the skill.",
    ),
    Code.AWS_ACCESS_DENIED: (
        "AWS CLI profile {profile} is not allowed to run {operation}. AWS said: "
        "{aws_message}",
        "Get that permission for the profile, then rerun the skill.",
    ),
    Code.AWS_ERROR: (
        "{operation} failed. AWS said: {aws_message}",
        "Fix the cause AWS gave, then rerun the skill.",
    ),
}


@dataclass(frozen=True, slots=True)
class Report:
    """The result of a run.

    Attributes:
        code: How the run ended.
        text: The full report to print.
    """

    code: Code
    text: str

    @property
    def ok(self) -> bool:
        """Whether the run succeeded."""
        return self.code in SUCCESS_CODES


def build_report(
    title: str, code: Code, lines: Sequence[str] = (), **facts: object
) -> Report:
    """Builds the report for a run that ended with a code.

    Args:
        title: First line, the command that ran.
        code: How the run ended.
        lines: Detail lines, one per resource, or none.
        **facts: Values the code's text is filled in with.

    Returns:
        The report.
    """
    values = {key: str(value) for key, value in facts.items()}
    message, next_step = TEXT[code]
    text = format_report(
        title,
        lines,
        code,
        " ".join(message.format(**values).split()),
        next_step.format(**values),
    )
    return Report(code, text)

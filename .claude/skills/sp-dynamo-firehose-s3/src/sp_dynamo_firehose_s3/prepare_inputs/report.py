"""Result codes for prepare-inputs and the fixed text for each."""

from dataclasses import dataclass
from enum import StrEnum

from sp_dynamo_firehose_s3.result import format_report

# Heading of the section the agent shows the user. Everything printed before it
# is for the agent only.
REPORT_HEADING = "## Report"


class Code(StrEnum):
    """How a prepare-inputs run ended."""

    COLUMNS_FOUND = "COLUMNS_FOUND"
    USAGE = "USAGE"
    CONFIG_INVALID = "CONFIG_INVALID"
    TABLE_NOT_FOUND = "TABLE_NOT_FOUND"
    KEY_NAME_UNSUPPORTED = "KEY_NAME_UNSUPPORTED"
    COLUMN_NAME_CLASH = "COLUMN_NAME_CLASH"
    WRITE_FAILED = "WRITE_FAILED"
    AWS_CLI_MISSING = "AWS_CLI_MISSING"
    AWS_CREDENTIALS = "AWS_CREDENTIALS"
    AWS_ACCESS_DENIED = "AWS_ACCESS_DENIED"
    AWS_ERROR = "AWS_ERROR"


# The message and next step for each code. `{...}` fields are filled in from the
# run's facts; the values themselves are never formatted again.
TEXT: dict[Code, tuple[str, str]] = {
    Code.COLUMNS_FOUND: (
        "Columns found in DynamoDB table {table}: {columns}. Items sampled: "
        "{items}. Attributes left out because their names cannot be column "
        "names: {skipped}.",
        "None.",
    ),
    Code.USAGE: (
        "prepare-inputs takes exactly two arguments, the session ID and the "
        "DynamoDB table name, but got {count}.",
        "Rerun the skill.",
    ),
    Code.CONFIG_INVALID: (
        "The table name, session ID, or inputs.toml is invalid: {problems}.",
        "Fix what is listed, then rerun the skill.",
    ),
    Code.TABLE_NOT_FOUND: (
        "DynamoDB table {table} does not exist in {region} for AWS CLI profile "
        "{profile}.",
        "Check the table name, or aws.profile and aws.region in inputs.toml, then "
        "rerun the skill.",
    ),
    Code.KEY_NAME_UNSUPPORTED: (
        "Key attribute {attribute} of DynamoDB table {table} cannot be a column "
        "name. A column name must start with a letter and contain only letters, "
        "digits, and underscores.",
        "None. The skill cannot build a pipeline for this table.",
    ),
    Code.COLUMN_NAME_CLASH: (
        "Attributes of DynamoDB table {table} would become the same column: {clashes}.",
        "Rename the attributes in DynamoDB so their column names differ, then "
        "rerun the skill.",
    ),
    Code.WRITE_FAILED: (
        "The columns could not be written to {path}: {reason}.",
        "Fix the cause, then rerun the skill.",
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
        text: The report the agent shows the user.
        agent_data: What only the agent reads, printed before the report. Empty
            unless the run succeeded.
    """

    code: Code
    text: str
    agent_data: str = ""

    @property
    def ok(self) -> bool:
        """Whether the run succeeded."""
        return self.code is Code.COLUMNS_FOUND

    @property
    def output(self) -> str:
        """Everything the command prints: the agent's data, then the report."""
        return f"{self.agent_data}{REPORT_HEADING}\n{self.text}"


def build_report(
    title: str, code: Code, agent_data: str = "", **facts: object
) -> Report:
    """Builds the report for a run that ended with a code.

    Args:
        title: First line, the command that ran.
        code: How the run ended.
        agent_data: What only the agent reads, or nothing.
        **facts: Values the code's text is filled in with.

    Returns:
        The report.
    """
    values = {key: str(value) for key, value in facts.items()}
    message, next_step = TEXT[code]
    text = format_report(
        title,
        (),
        code,
        " ".join(message.format(**values).split()),
        next_step.format(**values),
    )
    return Report(code, text, agent_data)

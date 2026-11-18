"""Result codes for scaffold and the fixed text for each."""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from sp_dynamo_firehose_s3.result import format_report


class Code(StrEnum):
    """How a scaffold run ended."""

    GENERATED = "GENERATED"
    USAGE = "USAGE"
    CONFIG_INVALID = "CONFIG_INVALID"
    WRITE_FAILED = "WRITE_FAILED"


# The message and next step for each code. `{...}` fields are filled in from the
# run's facts; the values themselves are never formatted again.
TEXT: dict[Code, tuple[str, str]] = {
    Code.GENERATED: (
        "Generated the Databricks project for DynamoDB table {table} in {directory}.",
        "None.",
    ),
    Code.USAGE: (
        "scaffold takes exactly three arguments, the session ID, the DynamoDB "
        "table name, and the project folder, but got {count}.",
        "Rerun the skill.",
    ),
    Code.CONFIG_INVALID: (
        "The inputs are invalid: {problems}.",
        "Fix inputs.toml if a problem is listed there, then rerun the skill.",
    ),
    Code.WRITE_FAILED: (
        "The project could not be written to {path}: {reason}.",
        "Fix the cause, then rerun the skill.",
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
        return self.code is Code.GENERATED


def build_report(
    title: str, code: Code, lines: Sequence[str] = (), **facts: object
) -> Report:
    """Builds the report for a run that ended with a code.

    Args:
        title: First line, the command that ran.
        code: How the run ended.
        lines: Detail lines, one per file written, or none.
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

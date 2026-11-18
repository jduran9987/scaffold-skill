"""Result codes for check-project and the fixed text for each."""

from dataclasses import dataclass
from enum import StrEnum

from sp_dynamo_firehose_s3.result import format_report


class Code(StrEnum):
    """How a check-project run ended."""

    PROJECT_NEW = "PROJECT_NEW"
    PROJECT_EXISTS = "PROJECT_EXISTS"
    USAGE = "USAGE"
    CONFIG_INVALID = "CONFIG_INVALID"


# The check ran, whether or not the project exists.
SUCCESS_CODES = frozenset({Code.PROJECT_NEW, Code.PROJECT_EXISTS})

# The message and next step for each code. `{...}` fields are filled in from the
# run's facts; the values themselves are never formatted again.
TEXT: dict[Code, tuple[str, str]] = {
    Code.PROJECT_NEW: ("Project {project} does not exist yet.", "None."),
    # The message is the question the agent asks the user, word for word.
    Code.PROJECT_EXISTS: (
        "Project {project} already exists. Continuing will overwrite the "
        "existing project. Do you want to continue?",
        "Answer the question.",
    ),
    Code.USAGE: (
        "check-project takes exactly two arguments, the DynamoDB table name and "
        "the project folder, but got {count}.",
        "Rerun the skill.",
    ),
    Code.CONFIG_INVALID: (
        "The table name, project folder, or inputs.toml is invalid: {problems}.",
        "Fix what is listed, then rerun the skill.",
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
        """Whether the check ran."""
        return self.code in SUCCESS_CODES


def build_report(title: str, code: Code, **facts: object) -> Report:
    """Builds the report for a run that ended with a code.

    Args:
        title: First line, the command that ran.
        code: How the run ended.
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
    return Report(code, text)

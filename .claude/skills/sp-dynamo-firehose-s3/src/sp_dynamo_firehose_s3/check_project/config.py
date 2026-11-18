"""Builds and checks every value check-project uses, in one place.

`build_config` takes the DynamoDB table name the user gave and the Claude Code
project folder, derives the table names, reads `[project]` from `inputs.toml`,
fills in the table name placeholders, and checks everything at once with one
pydantic model. Any problem raises one `ConfigError` that lists every problem.
Other sections are left to the actions that use them.

The file is never changed. The built values exist only in memory for the run
that built them.
"""

import re
import tomllib
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, ValidationError

INPUTS_PATH = Path(__file__).parent.parent / "inputs.toml"
# The table name placeholders check-project fills in an inputs.toml value.
PLACEHOLDER = re.compile(r"\{(dynamo_table_name|table_name|firehose_table_name)\}")

# A run of capitals followed by a capitalized word: `HTTPLogs` -> `HTTP_Logs`.
ACRONYM_BOUNDARY = re.compile(r"([A-Z]+)([A-Z][a-z])")
# A lowercase letter or digit followed by a capital: `suspenseAccounts`.
WORD_BOUNDARY = re.compile(r"([a-z0-9])([A-Z])")
NOT_ALPHANUMERIC = re.compile(r"[^A-Za-z0-9]+")

NonEmptyStr = Annotated[str, Field(min_length=1)]


class ConfigError(Exception):
    """Raised when the configuration cannot be built.

    Attributes:
        problems: Every problem found, joined by `; `.
    """

    def __init__(self, problems: str) -> None:
        """Records the problems.

        Args:
            problems: Every problem found, joined by `; `.
        """
        self.problems = problems
        super().__init__(problems)


class Section(BaseModel):
    """Base for every section: strict types, no unknown keys, read-only."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class ProjectConfig(Section):
    """The generated project."""

    projects_dir: NonEmptyStr
    python_version: NonEmptyStr
    uv_build_version: NonEmptyStr


class Config(BaseModel):
    """Every value check-project uses: its arguments and `[project]`.

    Sections of `inputs.toml` that other actions use are ignored.
    """

    model_config = ConfigDict(strict=True, extra="ignore", frozen=True)

    # DynamoDB's table name rules.
    dynamo_table_name: Annotated[
        str, Field(min_length=3, max_length=255, pattern=r"^[A-Za-z0-9_.-]+$")
    ]
    # Names the project folder.
    table_name: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]*$")]
    # The Claude Code project folder the project is written under.
    project_dir: Annotated[str, Field(pattern=r"^/")]
    project: ProjectConfig

    @property
    def project_path(self) -> Path:
        """The folder scaffold writes the project to."""
        return Path(self.project_dir) / self.project.projects_dir / self.table_name


def to_snake_case(name: str) -> str:
    """Converts a DynamoDB table name to snake_case.

    Must match scaffold's `to_snake_case`, which names the folder this command
    checks.

    Args:
        name: A DynamoDB table name, such as `bo.suspenseAccounts`.

    Returns:
        The snake_case name, such as `bo_suspense_accounts`. Empty if the name
        has no letters or digits.
    """
    words = WORD_BOUNDARY.sub(r"\1_\2", ACRONYM_BOUNDARY.sub(r"\1_\2", name))
    return NOT_ALPHANUMERIC.sub("_", words).strip("_").lower()


def fill_placeholders(
    table: dict[str, object], names: dict[str, str]
) -> dict[str, object]:
    """Replaces every placeholder in every string of a parsed TOML table.

    Only the exact placeholders in `PLACEHOLDER` are replaced, in one pass, so
    other braces are left unchanged, and a replaced value is never replaced
    again. Nested tables are filled in too.

    Args:
        table: A parsed TOML table.
        names: The value for each placeholder name, such as `table_name`.

    Returns:
        A copy of the table with the placeholders replaced.
    """
    filled: dict[str, object] = {}
    for key, value in table.items():
        if isinstance(value, dict):
            filled[key] = fill_placeholders(value, names)
        elif isinstance(value, str):
            filled[key] = PLACEHOLDER.sub(lambda match: names[match[1]], value)
        else:
            filled[key] = value
    return filled


def describe_errors(error: ValidationError) -> str:
    """Lists every problem in a validation error in a fixed form.

    Args:
        error: The validation error.

    Returns:
        One `<key>: <problem>` entry per problem, joined by `; `.
    """
    problems = []
    for item in error.errors(include_url=False):
        key = ".".join(str(part) for part in item["loc"])
        problems.append(f"{key}: {item['msg']}" if key else item["msg"])
    return "; ".join(problems)


def build_config(dynamo_table_name: str, project_dir: str) -> Config:
    """Builds and checks every value from the arguments and `inputs.toml`.

    Args:
        dynamo_table_name: The table name exactly as the user gave it.
        project_dir: The Claude Code project folder.

    Returns:
        The checked configuration.

    Raises:
        ConfigError: If `inputs.toml` cannot be read or parsed, or any value,
            including either argument, is invalid.
    """
    try:
        inputs = tomllib.loads(INPUTS_PATH.read_text())
    except OSError as error:
        raise ConfigError(f"{INPUTS_PATH}: {error.strerror or error}") from error
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f"{INPUTS_PATH}: is not valid TOML: {error}") from error
    names = {
        "dynamo_table_name": dynamo_table_name,
        "table_name": to_snake_case(dynamo_table_name),
        "firehose_table_name": dynamo_table_name.replace(".", "-"),
    }
    # The arguments come last, so the file cannot override them.
    raw = {
        **fill_placeholders(inputs, names),
        **names,
        "project_dir": project_dir,
    }
    try:
        return Config.model_validate(raw)
    except ValidationError as error:
        raise ConfigError(describe_errors(error)) from error

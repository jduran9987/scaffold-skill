"""Builds and checks every value prepare-inputs uses, in one place.

`build_config` takes the Claude Code session ID and the DynamoDB table name the
user gave, reads the sections of `inputs.toml` prepare-inputs uses (`[aws]`,
`[schema_inference]`), fills in `{dynamo_table_name}`, and checks everything at
once with one pydantic model. Any problem, in the arguments or in the file,
raises one `ConfigError` that lists every problem. Other sections are left to the
actions that use them.

The file is never changed. The built values exist only in memory for the run
that built them.
"""

import re
import tomllib
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, ValidationError

INPUTS_PATH = Path(__file__).parent.parent / "inputs.toml"
# The placeholder prepare-inputs fills in an inputs.toml value.
PLACEHOLDER = re.compile(r"\{(dynamo_table_name)\}")

NonEmptyStr = Annotated[str, Field(min_length=1)]
PositiveInt = Annotated[int, Field(ge=1)]
SparkTypes = Annotated[list[NonEmptyStr], Field(min_length=1)]
# Claude Code session IDs are lowercase UUIDs.
SESSION_ID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"


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


class AwsConfig(Section):
    """The AWS CLI profile and region every AWS command uses."""

    profile: NonEmptyStr
    region: NonEmptyStr


class AllowedTypesConfig(Section):
    """The Spark types a column of each DynamoDB type can be cast to.

    Maps are flattened into their attributes, so no column has type `M`, and a
    column that is NULL in every sampled item has type `S`.
    """

    S: SparkTypes
    N: SparkTypes
    BOOL: SparkTypes
    B: SparkTypes
    L: SparkTypes
    SS: SparkTypes
    NS: SparkTypes
    BS: SparkTypes

    def for_type(self, dynamo_type: str) -> list[str]:
        """Lists the Spark types a column of a DynamoDB type can be cast to.

        Args:
            dynamo_type: A DynamoDB type code other than `M` or `NULL`, such as
                `S`.

        Returns:
            The allowed Spark types, in file order.
        """
        return getattr(self, dynamo_type)


class SchemaInferenceConfig(Section):
    """How prepare-inputs infers the table's columns."""

    sample_size: PositiveInt
    allowed_types: AllowedTypesConfig


class Config(BaseModel):
    """Every value prepare-inputs uses: its arguments and its sections.

    Sections of `inputs.toml` that other actions use are ignored.
    """

    model_config = ConfigDict(strict=True, extra="ignore", frozen=True)

    # DynamoDB's table name rules.
    dynamo_table_name: Annotated[
        str, Field(min_length=3, max_length=255, pattern=r"^[A-Za-z0-9_.-]+$")
    ]
    # Names the folder the columns are written to.
    session_id: Annotated[str, Field(pattern=SESSION_ID_PATTERN)]
    aws: AwsConfig
    schema_inference: SchemaInferenceConfig


def fill_placeholders(
    table: dict[str, object], names: dict[str, str]
) -> dict[str, object]:
    """Replaces every placeholder in every string of a parsed TOML table.

    Only the exact placeholders in `PLACEHOLDER` are replaced, in one pass, so
    a replaced value is never replaced again. Nested tables are filled in too.

    Args:
        table: A parsed TOML table.
        names: The value for each placeholder name, such as `dynamo_table_name`.

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


def build_config(dynamo_table_name: str, session_id: str) -> Config:
    """Builds and checks every value from the arguments and `inputs.toml`.

    Args:
        dynamo_table_name: The table name exactly as the user gave it.
        session_id: The Claude Code session ID.

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
    names = {"dynamo_table_name": dynamo_table_name}
    # The arguments come last, so the file cannot override them.
    raw = {**fill_placeholders(inputs, names), **names, "session_id": session_id}
    try:
        return Config.model_validate(raw)
    except ValidationError as error:
        raise ConfigError(describe_errors(error)) from error

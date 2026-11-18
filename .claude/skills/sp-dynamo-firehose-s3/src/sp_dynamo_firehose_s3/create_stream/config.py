"""Builds and checks every value create-stream uses, in one place.

`build_config` takes the DynamoDB table name the user gave, reads the sections of
`inputs.toml` create-stream uses (`[aws]`, `[kinesis]`, `[firehose]`, `[wait]`),
fills in `{dynamo_table_name}`, and checks everything at once with one pydantic
model. Any problem, in the table name or in the file, raises one `ConfigError`
that lists every problem. Other sections are left to the actions that use them.

The file is never changed. The built values exist only in memory for the run
that built them.
"""

import re
import tomllib
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

INPUTS_PATH = Path(__file__).parent.parent / "inputs.toml"
# The placeholder create-stream fills in an inputs.toml value.
PLACEHOLDER = re.compile(r"\{(dynamo_table_name)\}")
KINESIS_NAME_SUFFIX = "-stream"
FIREHOSE_NAME_SUFFIX = "-firehose"
# Writes each record under a folder named after its table, with dots replaced by
# dashes. scaffold's `{firehose_table_name}` and bronze's source path rely on it.
METADATA_EXTRACTION_QUERY = r'{tableName:limit(1;.tableName | gsub("\\.", "-"; "-"))}'
# A fixed folder, then the table's folder, as bronze's source path expects.
PREFIX_PATTERN = r"^[^!{}]+/!\{partitionKeyFromQuery:tableName\}/"

NonEmptyStr = Annotated[str, Field(min_length=1)]
PositiveInt = Annotated[int, Field(ge=1)]


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


class KinesisConfig(Section):
    """The Kinesis data stream the DynamoDB table streams to."""

    name: Annotated[str, Field(min_length=1, max_length=128)]
    shard_count: PositiveInt


class FirehoseConfig(Section):
    """The Firehose stream that reads the Kinesis stream and writes to S3."""

    name: Annotated[str, Field(min_length=1, max_length=64)]
    role_arn: Annotated[str, Field(pattern=r"^arn:aws:iam::\d{12}:role/.+$")]
    bucket_arn: Annotated[str, Field(pattern=r"^arn:aws:s3:::[^/]+$")]
    prefix: Annotated[str, Field(pattern=PREFIX_PATTERN)]
    error_output_prefix: NonEmptyStr
    # Bronze reads only gzipped files (`*.gz`).
    compression_format: Literal["GZIP"]
    # Dynamic partitioning needs 64 to 128 MB and 60 to 900 seconds.
    buffer_size_mb: Annotated[int, Field(ge=64, le=128)]
    buffer_interval_seconds: Annotated[int, Field(ge=60, le=900)]
    metadata_extraction_query: Literal[METADATA_EXTRACTION_QUERY]
    json_parsing_engine: Literal["JQ-1.6"]
    partition_retry_seconds: Annotated[int, Field(ge=0, le=7200)]
    # Bronze reads one JSON record per line.
    record_delimiter: Literal[r"\n"]


class WaitConfig(Section):
    """How create-stream waits for resources to become active."""

    poll_interval_seconds: PositiveInt
    # A run must report a timeout before Claude Code's 10-minute command limit.
    timeout_seconds: Annotated[int, Field(ge=1, le=540)]


class Config(BaseModel):
    """Every value create-stream uses: the table name and its sections.

    Sections of `inputs.toml` that other actions use are ignored.
    """

    model_config = ConfigDict(strict=True, extra="ignore", frozen=True)

    # DynamoDB's table name rules.
    dynamo_table_name: Annotated[
        str, Field(min_length=3, max_length=255, pattern=r"^[A-Za-z0-9_.-]+$")
    ]
    aws: AwsConfig
    kinesis: KinesisConfig
    firehose: FirehoseConfig
    wait: WaitConfig

    @model_validator(mode="after")
    def names_follow_the_table(self) -> Self:
        """Checks the stream names are the ones derived from the table name.

        Returns:
            The config.

        Raises:
            ValueError: If either stream name is not the derived one.
        """
        kinesis_name = f"{self.dynamo_table_name}{KINESIS_NAME_SUFFIX}"
        firehose_name = f"{self.dynamo_table_name}{FIREHOSE_NAME_SUFFIX}"
        problems = []
        if self.kinesis.name != kinesis_name:
            problems.append(
                f'kinesis.name is "{self.kinesis.name}" but must be "{kinesis_name}"'
            )
        if self.firehose.name != firehose_name:
            problems.append(
                f'firehose.name is "{self.firehose.name}" but must be "{firehose_name}"'
            )
        if problems:
            raise ValueError("; ".join(problems))
        return self


def fill_placeholders(
    table: dict[str, object], names: dict[str, str]
) -> dict[str, object]:
    """Replaces every placeholder in every string of a parsed TOML table.

    Only the exact placeholders in `PLACEHOLDER` are replaced, in one pass, so
    Firehose's `!{...}` syntax and the JQ query's braces are left unchanged, and
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


def build_config(dynamo_table_name: str) -> Config:
    """Builds and checks every value from the table name and `inputs.toml`.

    Args:
        dynamo_table_name: The table name exactly as the user gave it.

    Returns:
        The checked configuration.

    Raises:
        ConfigError: If `inputs.toml` cannot be read or parsed, or any value,
            including the table name, is invalid.
    """
    try:
        inputs = tomllib.loads(INPUTS_PATH.read_text())
    except OSError as error:
        raise ConfigError(f"{INPUTS_PATH}: {error.strerror or error}") from error
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f"{INPUTS_PATH}: is not valid TOML: {error}") from error
    names = {"dynamo_table_name": dynamo_table_name}
    # The table name comes last, so the file cannot override it.
    raw = {**fill_placeholders(inputs, names), **names}
    try:
        return Config.model_validate(raw)
    except ValidationError as error:
        raise ConfigError(describe_errors(error)) from error

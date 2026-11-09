"""Field types, shared constants, and base models for the v1 pipeline YAML.

Kept apart from `config.py` so the defaults file can be validated with the same
types the config models use.
"""

import re
from contextvars import ContextVar
from typing import Annotated, Literal, NamedTuple

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)
from pydantic.alias_generators import to_camel

# Becomes the target's name, such as prod.
ENV_PLACEHOLDER = "{env}"
# Becomes the pipeline's `metadata.name` when a pipeline YAML is validated.
NAME_PLACEHOLDER = "{name}"
# Becomes `metadata.name` with hyphens replaced by underscores, for table names.
TABLE_PLACEHOLDER = "{table}"
PLACEHOLDERS = (ENV_PLACEHOLDER, NAME_PLACEHOLDER, TABLE_PLACEHOLDER)
# The `metadata.name` of the pipeline YAML being validated, if any.
PIPELINE_NAME: ContextVar[str | None] = ContextVar("pipeline_name", default=None)
PLACEHOLDER_PATTERN = re.compile(r"\{[^{}]*\}")


def check_placeholders(value: str) -> str:
    """Rejects placeholders other than `{env}`, `{name}`, and `{table}`.

    Args:
        value: Path or table name from the YAML or defaults file.

    Returns:
        The value unchanged.

    Raises:
        ValueError: If the value contains any other `{...}` placeholder.
    """
    others = sorted(set(PLACEHOLDER_PATTERN.findall(value)) - set(PLACEHOLDERS))
    if others:
        raise ValueError(
            f"only the {', '.join(PLACEHOLDERS)} placeholders are supported, "
            f"not: {', '.join(others)}"
        )
    return value


def resolve_name(value: object) -> object:
    """Replaces `{name}` and `{table}` using the YAML being validated.

    `{name}` becomes its `metadata.name`, and `{table}` the same name with
    hyphens replaced by underscores.

    Args:
        value: A string, or a list or mapping holding strings. Mapping keys are
            not changed.

    Returns:
        The value with both placeholders replaced in every string, or the value
        unchanged if no pipeline YAML is being validated.
    """
    name = PIPELINE_NAME.get()
    if name is None:
        return value
    if isinstance(value, str):
        return value.replace(NAME_PLACEHOLDER, name).replace(
            TABLE_PLACEHOLDER, name.replace("-", "_")
        )
    if isinstance(value, list):
        return [resolve_name(item) for item in value]
    if isinstance(value, dict):
        return {key: resolve_name(item) for key, item in value.items()}
    return value


def resolve_env(value: object, target: str) -> object:
    """Replaces `{env}` with the target's name in a setting's value.

    Args:
        value: A string, a list of strings, or any other value.
        target: Target name, such as `prod`.

    Returns:
        The value with `{env}` replaced in every string, or the value unchanged
        if it holds no strings.

    Raises:
        ValueError: If a string contains any other `{...}` placeholder.
    """
    if isinstance(value, str):
        return check_placeholders(value).replace(ENV_PLACEHOLDER, target)
    if isinstance(value, list):
        return [resolve_env(item, target) for item in value]
    return value


S3Path = Annotated[
    str,
    StringConstraints(pattern=r"^s3://\S+$"),
    AfterValidator(check_placeholders),
]
TableName = Annotated[
    str,
    StringConstraints(pattern=r"^[^.\s]+\.[^.\s]+\.[^.\s]+$"),
    AfterValidator(check_placeholders),
]
# DynamoDB table names.
DYNAMO_TABLE_NAME_PATTERN = re.compile(r"[A-Za-z0-9_.-]{3,255}")
PIPELINE_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_-]*")
PipelineName = Annotated[
    str, StringConstraints(pattern=rf"^{PIPELINE_NAME_PATTERN.pattern}$")
]
ColumnName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")]
# Configured columns cannot start with an underscore, which is kept for metadata.
ConfiguredColumnName = Annotated[
    str, StringConstraints(pattern=r"^[A-Za-z][A-Za-z0-9_]*$")
]
AttributePath = Annotated[
    str, StringConstraints(pattern=r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*$")
]
NonEmptyString = Annotated[str, StringConstraints(min_length=1)]
# The only deployment targets. Each becomes a bundle target and an `{env}` value.
TARGET_NAMES = ("dev", "qa", "uat", "prod")
TargetName = Literal["dev", "qa", "uat", "prod"]
# Permission level the run-as service principal gets on the bundle's resources.
ServicePrincipalPermission = Literal["can_manage", "can_run"]
# Bundle permission level for each kind of grant.
PERMISSION_LEVELS = {
    "can_manage": "CAN_MANAGE",
    "can_run": "CAN_RUN",
    "can_view": "CAN_VIEW",
}
GroupNames = list[NonEmptyString]


def check_group_levels(
    manage_groups: list[str], run_groups: list[str], view_groups: list[str]
) -> None:
    """Checks that each group gets only one permission level.

    Args:
        manage_groups: Groups with CAN_MANAGE.
        run_groups: Groups with CAN_RUN.
        view_groups: Groups with CAN_VIEW.

    Raises:
        ValueError: If a group is listed more than once, in one list or across
            lists.
    """
    groups = [*manage_groups, *run_groups, *view_groups]
    repeated = sorted({group for group in groups if groups.count(group) > 1})
    if repeated:
        raise ValueError(
            "each group can be listed once across manage, run, and view groups, "
            f"not: {', '.join(repeated)}"
        )


# The only target that sends job and task failure notifications.
NOTIFICATION_TARGET = "prod"
# Quoted in YAML, so 3.10 is not read as the number 3.1.
PythonVersion = Annotated[str, StringConstraints(pattern=r"^3\.\d+$")]
# A version range with a lower and an upper bound, such as ">=0.13.7,<0.14.0".
UvBuildVersion = Annotated[
    str, StringConstraints(pattern=r"^>=\d+\.\d+\.\d+,<\d+\.\d+\.\d+$")
]
# Job tags are forwarded to the cluster as AWS tags, so they follow AWS tag rules.
# Keys must also be plain YAML scalars because they are rendered unquoted.
TagKey = Annotated[
    str,
    StringConstraints(max_length=127, pattern=r"^[A-Za-z][A-Za-z0-9_.:/=+@-]*$"),
]
TAG_VALUE_PATTERN = re.compile(r"[A-Za-z0-9 _.:/=+@-]*")
MAX_TAG_VALUE_LENGTH = 255


def check_tag_value(value: str) -> str:
    """Checks a tag value against AWS tag rules for every target.

    `{env}` is allowed and becomes the target's name when the bundle deploys,
    so the value is checked with each target's name in its place. `{name}` and
    `{table}` are allowed in the defaults file and are replaced before a
    pipeline YAML's tags are checked, so they are checked here as a one-letter
    name.

    Args:
        value: Tag value from the YAML or defaults file.

    Returns:
        The value unchanged.

    Raises:
        ValueError: If, for any target, the value has characters AWS tags do not
            allow or is longer than 255 characters.
    """
    for name in TARGET_NAMES:
        resolved = value.replace(ENV_PLACEHOLDER, name)
        for placeholder in (NAME_PLACEHOLDER, TABLE_PLACEHOLDER):
            resolved = resolved.replace(placeholder, "a")
        if not TAG_VALUE_PATTERN.fullmatch(resolved):
            raise ValueError(
                "tag values can only contain letters, digits, spaces, "
                f"_ . : / = + @ -, and the {', '.join(PLACEHOLDERS)} placeholders"
            )
        if len(resolved) > MAX_TAG_VALUE_LENGTH:
            raise ValueError(
                f"tag values must be at most {MAX_TAG_VALUE_LENGTH} characters "
                f"with {ENV_PLACEHOLDER} replaced by the target's name"
            )
    return value


TagValue = Annotated[str, AfterValidator(check_tag_value)]
# Unquoted, YAML reads these as booleans or null instead of strings.
YAML_RESERVED_WORDS = frozenset(
    {"true", "false", "null", "yes", "no", "on", "off", "y", "n"}
)
MAX_JOB_TAGS = 25
EmailAddress = Annotated[str, StringConstraints(pattern=r"^[^@\s]+@[^@\s]+$")]
WorkspaceHost = Annotated[str, StringConstraints(pattern=r"^https://\S+$")]
AwsRegion = Annotated[str, StringConstraints(pattern=r"^[a-z]{2}(-[a-z]+)+-\d$")]
IamRoleArn = Annotated[
    str, StringConstraints(pattern=r"^arn:aws[a-z-]*:iam::\d{12}:role/\S+$")
]
S3BucketArn = Annotated[
    str,
    StringConstraints(
        pattern=r"^arn:aws[a-z-]*:s3:::[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$"
    ),
]
NonEmptyStrings = Annotated[list[NonEmptyString], Field(min_length=1)]
WebhookIds = NonEmptyStrings
EmailAddresses = Annotated[list[EmailAddress], Field(min_length=1)]
NUMERIC_TYPE_PATTERN = (
    r"tinyint|smallint|int|bigint|float|double|decimal\(\s*\d+\s*,\s*\d+\s*\)"
)
TEXT_TYPE_PATTERN = r"string|boolean|date|timestamp|timestamp_ntz"
SCALAR_TYPE_PATTERN = rf"{NUMERIC_TYPE_PATTERN}|{TEXT_TYPE_PATTERN}"
TargetType = Annotated[
    str,
    StringConstraints(
        pattern=rf"^({SCALAR_TYPE_PATTERN}|variant|array<({SCALAR_TYPE_PATTERN})>)$"
    ),
]
DynamoType = Literal["S", "N", "B", "BOOL", "NULL", "M", "L", "SS", "NS", "BS"]
# DynamoDB key attributes can only be strings, numbers, or binary.
KEY_DYNAMO_TYPES = frozenset({"S", "N", "B"})
# Target types each DynamoDB type can be cast to, and how to describe them.
# Binary values are base64 text in the stream, so they are kept as strings.
TARGET_TYPE_RULES: dict[str, tuple[str, str]] = {
    "S": (rf"{SCALAR_TYPE_PATTERN}", "a scalar type"),
    "N": (rf"{NUMERIC_TYPE_PATTERN}|string", "a numeric type or string"),
    "B": (r"string", "string"),
    "BOOL": (r"boolean|string", "boolean or string"),
    "NULL": (r"boolean", "boolean"),
    "M": (r"string|variant", "string or variant"),
    "L": (r"string|variant", "string or variant"),
    "SS": (rf"array<({SCALAR_TYPE_PATTERN})>", "an array of a scalar type"),
    "NS": (
        rf"array<({NUMERIC_TYPE_PATTERN}|string)>",
        "an array of a numeric type or string",
    ),
    "BS": (r"array<string>", "array<string>"),
}

# A Quartz cron field: digits, names such as MON or JAN, and * ? / , - # L W.
QUARTZ_FIELD_PATTERN = r"[0-9A-Za-z*?/,#-]+"


# Metadata columns the silver template adds to every row.
class MetadataColumn(NamedTuple):
    """A column a job adds, with the type and comment its table's DDL gives it.

    Attributes:
        name: Column name.
        type: Spark SQL type.
        comment: Column description, the same in every pipeline.
    """

    name: str
    type: str
    comment: str


# Bronze stores each source record whole in this column.
BRONZE_DATA_COLUMN = MetadataColumn(
    "data",
    "variant",
    "DynamoDB change event as delivered by Firehose from the table's Kinesis stream.",
)
BRONZE_METADATA_COLUMNS = (
    MetadataColumn(
        "_ingested_at", "timestamp", "Time the bronze job loaded the record."
    ),
    MetadataColumn(
        "_file_path", "string", "S3 path of the Firehose file the record came from."
    ),
    MetadataColumn(
        "_file_modification_time",
        "timestamp",
        "Time the Firehose file the record came from was last modified in S3.",
    ),
)
SILVER_EVENT_COLUMNS = (
    MetadataColumn(
        "_event_name", "string", "DynamoDB change type: INSERT, MODIFY, or REMOVE."
    ),
    MetadataColumn("_event_id", "string", "Unique ID of the DynamoDB change event."),
    MetadataColumn(
        "_event_timestamp",
        "timestamp",
        "Approximate time DynamoDB made the change, from the event's "
        "ApproximateCreationDateTime.",
    ),
)
# Silver and gold add these after the configured columns.
SILVER_METADATA = SILVER_EVENT_COLUMNS + BRONZE_METADATA_COLUMNS
SILVER_METADATA_COLUMNS = frozenset(column.name for column in SILVER_METADATA)


class ConfigModel(BaseModel):
    """Base model that treats keys set to null as left out."""

    @model_validator(mode="before")
    @classmethod
    def drop_null_keys(cls, data: object) -> object:
        """Removes keys whose value is null so they validate as missing.

        Args:
            data: Raw input for the model.

        Returns:
            The input without null values, or the input unchanged if it is not a
            mapping.
        """
        if isinstance(data, dict):
            return {key: value for key, value in data.items() if value is not None}
        return data


class StrictModel(ConfigModel):
    """Base model that reads camelCase keys and rejects unknown keys.

    Values are not converted between types, so a string is never read as a boolean.
    """

    model_config = ConfigDict(alias_generator=to_camel, extra="forbid", strict=True)

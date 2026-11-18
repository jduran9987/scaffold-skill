"""Builds and checks every value scaffold uses, in one place.

`build_config` takes scaffold's arguments and the agent's answers, and combines
them with:

- the sections of `inputs.toml` scaffold uses (`[schema_inference]`,
  `[project]`, `[bundle]`, `[job]`), with the table name placeholders
  (`{dynamo_table_name}`, `{table_name}`, `{firehose_table_name}`) filled in;
- the columns prepare-inputs wrote to `columns.json` in the session's folder.

Everything is checked at once with one pydantic model. Any problem, in the
arguments, the answers, `columns.json`, or `inputs.toml`, raises one
`ConfigError` that lists every problem.

`inputs.toml` is never changed. The built values exist only in memory for the
run that built them.
"""

import json
import re
import shutil
import tempfile
import tomllib
from pathlib import Path
from typing import Annotated, Literal, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)

INPUTS_PATH = Path(__file__).parent.parent / "inputs.toml"
# One folder per Claude Code session holds the files passed between actions.
SESSIONS_DIR = Path(tempfile.gettempdir()) / "sp-dynamo-firehose-s3"
COLUMNS_FILE = "columns.json"
# The table name placeholders scaffold fills in an inputs.toml value.
PLACEHOLDER = re.compile(r"\{(dynamo_table_name|table_name|firehose_table_name)\}")
# Claude Code session IDs are lowercase UUIDs.
SESSION_ID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)

# A run of capitals followed by a capitalized word: `HTTPLogs` -> `HTTP_Logs`.
ACRONYM_BOUNDARY = re.compile(r"([A-Z]+)([A-Z][a-z])")
# A lowercase letter or digit followed by a capital: `suspenseAccounts`.
WORD_BOUNDARY = re.compile(r"([a-z0-9])([A-Z])")
NOT_ALPHANUMERIC = re.compile(r"[^A-Za-z0-9]+")

# Quartz cron fields in order: name, lowest and highest number, and names.
MONTHS = ("JAN", "FEB", "MAR", "APR", "MAY", "JUN")
MONTHS += ("JUL", "AUG", "SEP", "OCT", "NOV", "DEC")
WEEKDAYS = ("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT")
CRON_FIELDS: tuple[tuple[str, int, int, tuple[str, ...]], ...] = (
    ("seconds", 0, 59, ()),
    ("minutes", 0, 59, ()),
    ("hours", 0, 23, ()),
    ("day of month", 1, 31, ()),
    ("month", 1, 12, MONTHS),
    ("day of week", 1, 7, WEEKDAYS),
    ("year", 1970, 2099, ()),
)
DAY_OF_MONTH = 3
DAY_OF_WEEK = 5
NO_SPECIFIC_VALUE = "?"
# Day of month only: the last day (`L`), some days before it (`L-3`), the last
# weekday (`LW`), or the weekday nearest a day (`15W`).
DAY_OF_MONTH_SPECIAL = re.compile(
    r"L(-([1-9]|[12][0-9]|30))?|LW|([1-9]|[12][0-9]|3[01])W"
)
# Day of week only: Saturday (`L`), the last given weekday of the month (`6L`),
# or the nth given weekday of the month (`6#3`).
DAY_OF_WEEK_SPECIAL = re.compile(r"L|[1-7](L|#[1-5])")

NonEmptyStr = Annotated[str, Field(min_length=1)]
PositiveInt = Annotated[int, Field(ge=1)]
SparkTypes = Annotated[list[NonEmptyStr], Field(min_length=1)]
DynamoType = Literal["S", "N", "BOOL", "B", "L", "SS", "NS", "BS"]
S3Folder = Annotated[str, Field(pattern=r"^s3://.+/$")]
# catalog.schema.table
TableName = Annotated[str, Field(pattern=r"^[^.\s]+\.[^.\s]+\.[^.\s]+$")]
# A Spark processing-time trigger, such as `1 minute` or `30 seconds`.
TriggerInterval = Annotated[
    str, Field(pattern=r"^[1-9][0-9]* (second|minute|hour|day)s?$")
]
# The columns the silver job adds after the table's own, in the order it writes
# them. gold's upsert_latest_key must be one of these or a table column.
SILVER_METADATA_NAMES = (
    "_event_name",
    "_event_id",
    "_event_timestamp",
    "_ingested_at",
    "_file_path",
    "_file_modification_time",
)


def check_timezone(value: str) -> str:
    """Checks a time zone is a known IANA name, as Databricks schedules need.

    Args:
        value: The time zone, such as `UTC` or `America/New_York`.

    Returns:
        The time zone unchanged.

    Raises:
        ValueError: If the time zone is not known.
    """
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError) as error:
        raise ValueError(f'"{value}" is not a known time zone') from error
    return value


TimeZone = Annotated[str, AfterValidator(check_timezone)]


def check_cron_value(token: str, low: int, high: int, names: tuple[str, ...]) -> bool:
    """Checks one value of a Quartz cron field.

    Args:
        token: A number, or a name such as `MON`.
        low: Lowest number the field allows.
        high: Highest number the field allows.
        names: Names the field allows, such as month names.

    Returns:
        Whether the value is valid.
    """
    if token.isdigit():
        return low <= int(token) <= high
    return token.upper() in names


def check_cron_item(item: str, low: int, high: int, names: tuple[str, ...]) -> bool:
    """Checks one comma-separated item of a Quartz cron field.

    An item is `*`, a value, or a range such as `MON-FRI`, optionally followed by
    a step such as `/15`.

    Args:
        item: The item.
        low: Lowest number the field allows.
        high: Highest number the field allows.
        names: Names the field allows.

    Returns:
        Whether the item is valid.
    """
    base, slash, step = item.partition("/")
    if slash and not (step.isdigit() and int(step) >= 1):
        return False
    if base == "*":
        return True
    start, dash, end = base.partition("-")
    if dash:
        return check_cron_value(start, low, high, names) and check_cron_value(
            end, low, high, names
        )
    return check_cron_value(base, low, high, names)


def check_quartz_cron(value: str) -> str:
    """Checks a Quartz cron expression, the schedule format Databricks jobs use.

    Args:
        value: The expression, such as `0 0 6 * * ?`.

    Returns:
        The expression unchanged.

    Raises:
        ValueError: If the expression does not have 6 or 7 fields, a field is not
            valid, or not exactly one of the two day fields is `?`.
    """
    fields = value.split()
    if len(fields) not in (6, 7):
        raise ValueError(
            f"must have 6 or 7 fields separated by spaces, not {len(fields)}"
        )
    problems = []
    for index, field in enumerate(fields):
        name, low, high, names = CRON_FIELDS[index]
        if field == NO_SPECIFIC_VALUE and index in (DAY_OF_MONTH, DAY_OF_WEEK):
            continue
        if index == DAY_OF_MONTH and DAY_OF_MONTH_SPECIAL.fullmatch(field):
            continue
        if index == DAY_OF_WEEK and DAY_OF_WEEK_SPECIAL.fullmatch(field):
            continue
        items = field.split(",")
        if not all(check_cron_item(item, low, high, names) for item in items):
            problems.append(f'the {name} field "{field}" is not valid')
    no_day_of_month = fields[DAY_OF_MONTH] == NO_SPECIFIC_VALUE
    no_day_of_week = fields[DAY_OF_WEEK] == NO_SPECIFIC_VALUE
    if no_day_of_month == no_day_of_week:
        problems.append(
            "exactly one of the day of month and day of week fields must be ?"
        )
    if problems:
        raise ValueError("; ".join(problems))
    return value


QuartzCron = Annotated[str, AfterValidator(check_quartz_cron)]


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
    """The Spark types a column can be cast to."""

    sample_size: PositiveInt
    allowed_types: AllowedTypesConfig


class ProjectConfig(Section):
    """The generated project."""

    projects_dir: NonEmptyStr
    python_version: NonEmptyStr
    uv_build_version: NonEmptyStr


class SharedTargetConfig(Section):
    """Defaults shared by every target."""

    service_principal: NonEmptyStr


class TargetConfig(Section):
    """One target's workspace settings."""

    host: Annotated[str, Field(pattern=r"^https://.+$")]
    compute_policy: NonEmptyStr
    # Workers on the job cluster; the cluster policy limits the range.
    num_workers: PositiveInt
    # The permission levels a Databricks bundle accepts.
    service_principal_permissions: Literal["can_manage", "can_run", "can_view"]
    manage_groups: list[NonEmptyStr]
    run_groups: list[NonEmptyStr]
    view_groups: list[NonEmptyStr]
    # Overrides the shared service principal; None when left out.
    service_principal: NonEmptyStr | None = None


class TargetsConfig(Section):
    """Every target's workspace settings. All four are required."""

    dev: TargetConfig
    qa: TargetConfig
    uat: TargetConfig
    prod: TargetConfig


class BundleConfig(Section):
    """The Databricks bundle's notifications, tags, and targets."""

    on_job_failure_webhook_notification_ids: list[NonEmptyStr]
    on_job_failure_emails: list[NonEmptyStr]
    on_task_failure_webhook_notification_ids: list[NonEmptyStr]
    on_task_failure_emails: list[NonEmptyStr]
    tags: dict[NonEmptyStr, str]
    target: SharedTargetConfig
    targets: TargetsConfig


class LayerConfig(Section):
    """One layer's checkpoint path and target table. Both may contain `{env}`."""

    checkpoint_path: S3Folder
    target_table: TableName


class BronzeConfig(LayerConfig):
    """The bronze layer's settings."""

    # The S3 folder Firehose writes the table's records under.
    source_path: S3Folder


class GoldConfig(LayerConfig):
    """The gold layer's settings."""

    upsert_latest_key: NonEmptyStr


class JobConfig(Section):
    """The jobs' settings that are not the user's answers, and their layers."""

    timezone: TimeZone
    trigger_interval: TriggerInterval
    bronze: BronzeConfig
    silver: LayerConfig
    gold: GoldConfig

    @model_validator(mode="after")
    def layers_are_separate(self) -> Self:
        """Checks no two layers share a checkpoint path or a target table.

        Returns:
            The job settings.

        Raises:
            ValueError: If two layers share either.
        """
        layers = {"bronze": self.bronze, "silver": self.silver, "gold": self.gold}
        problems = []
        for field in ("checkpoint_path", "target_table"):
            values = [getattr(layer, field) for layer in layers.values()]
            if len(set(values)) < len(values):
                problems.append(f"each layer needs its own {field}")
        if problems:
            raise ValueError("; ".join(problems))
        return self


class SourceColumn(Section):
    """A column prepare-inputs found in the table."""

    name: NonEmptyStr
    path: NonEmptyStr
    dynamo_type: DynamoType
    key: bool


class SourceConfig(Section):
    """What prepare-inputs wrote to `columns.json`."""

    dynamo_table_name: NonEmptyStr
    columns: Annotated[list[SourceColumn], Field(min_length=1)]


class Answers(Section):
    """The agent's answers: the column types it picked and the user's trigger."""

    # The Spark type for each column, by column name.
    types: dict[NonEmptyStr, NonEmptyStr]
    execution_mode: Literal["batch", "continuous"]
    # Required for batch jobs; null for continuous ones.
    batch_schedule: QuartzCron | None

    @model_validator(mode="after")
    def schedule_follows_the_mode(self) -> Self:
        """Checks only batch jobs have a schedule.

        Returns:
            The answers.

        Raises:
            ValueError: If a batch job has no schedule or a continuous job has
                one.
        """
        if self.execution_mode == "batch" and self.batch_schedule is None:
            raise ValueError("batch_schedule is required when execution_mode is batch")
        if self.execution_mode == "continuous" and self.batch_schedule is not None:
            raise ValueError(
                "batch_schedule must be null when execution_mode is continuous"
            )
        return self


class Column(BaseModel):
    """A column with the Spark type the agent picked for it."""

    model_config = ConfigDict(frozen=True)

    name: str
    path: str
    dynamo_type: str
    key: bool
    spark_type: str


class Config(BaseModel):
    """Every value scaffold uses.

    Sections of `inputs.toml` that other actions use are ignored.
    """

    model_config = ConfigDict(strict=True, extra="ignore", frozen=True)

    # DynamoDB's table name rules.
    dynamo_table_name: Annotated[
        str, Field(min_length=3, max_length=255, pattern=r"^[A-Za-z0-9_.-]+$")
    ]
    # Names the bundle, the Python package, and the project folder.
    table_name: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]*$")]
    firehose_table_name: NonEmptyStr
    session_id: Annotated[str, Field(pattern=SESSION_ID.pattern)]
    # The Claude Code project folder the project is written under.
    project_dir: Annotated[str, Field(pattern=r"^/")]
    schema_inference: SchemaInferenceConfig
    project: ProjectConfig
    bundle: BundleConfig
    job: JobConfig
    source: SourceConfig
    answers: Answers

    @model_validator(mode="after")
    def answers_match_the_columns(self) -> Self:
        """Checks the columns are this table's, and each has an allowed type.

        Returns:
            The config.

        Raises:
            ValueError: If `columns.json` is for another table, a column has no
                type, a type is given for a column that does not exist, a type
                is not allowed for the column's DynamoDB type, or gold's
                upsert_latest_key is not a silver column.
        """
        problems = []
        if self.source.dynamo_table_name != self.dynamo_table_name:
            problems.append(
                f'{COLUMNS_FILE} is for table "{self.source.dynamo_table_name}", '
                f'not "{self.dynamo_table_name}"'
            )
        types = self.answers.types
        names = [column.name for column in self.source.columns]
        missing = [name for name in names if name not in types]
        unknown = sorted(set(types) - set(names))
        if missing:
            problems.append(f"answers.types has no type for {', '.join(missing)}")
        if unknown:
            problems.append(
                f"answers.types has types for columns that do not exist: "
                f"{', '.join(unknown)}"
            )
        latest_key = self.job.gold.upsert_latest_key
        if latest_key not in [*names, *SILVER_METADATA_NAMES]:
            problems.append(
                f'job.gold.upsert_latest_key is "{latest_key}", which is not a '
                "silver column"
            )
        allowed_types = self.schema_inference.allowed_types
        for column in self.source.columns:
            spark_type = types.get(column.name)
            allowed = allowed_types.for_type(column.dynamo_type)
            if spark_type is not None and spark_type not in allowed:
                problems.append(
                    f'answers.types.{column.name} is "{spark_type}", but a '
                    f"{column.dynamo_type} column can be {', '.join(allowed)}"
                )
        if problems:
            raise ValueError("; ".join(problems))
        return self

    @property
    def columns(self) -> list[Column]:
        """The table's columns, each with the Spark type the agent picked.

        Returns:
            Key columns first, then the rest by path, as prepare-inputs wrote
            them.
        """
        return [
            Column(
                name=column.name,
                path=column.path,
                dynamo_type=column.dynamo_type,
                key=column.key,
                spark_type=self.answers.types[column.name],
            )
            for column in self.source.columns
        ]

    @property
    def project_path(self) -> Path:
        """The folder the project is written to."""
        return Path(self.project_dir) / self.project.projects_dir / self.table_name


def to_snake_case(name: str) -> str:
    """Converts a DynamoDB table name to snake_case.

    Splits camelCase words, replaces every run of other characters with one
    underscore, trims underscores from both ends, and lowercases the result.

    Args:
        name: A DynamoDB table name, such as `bo.suspenseAccounts`.

    Returns:
        The snake_case name, such as `bo_suspense_accounts`. Empty if the name
        has no letters or digits.
    """
    words = WORD_BOUNDARY.sub(r"\1_\2", ACRONYM_BOUNDARY.sub(r"\1_\2", name))
    return NOT_ALPHANUMERIC.sub("_", words).strip("_").lower()


def to_firehose_name(name: str) -> str:
    """Converts a DynamoDB table name the way Firehose names its folder.

    Must match `firehose.metadata_extraction_query` in `inputs.toml`, which
    create-stream gives Firehose and which replaces every dot in the record's
    table name with a dash.

    Args:
        name: A DynamoDB table name, such as `bo.suspenseAccounts`.

    Returns:
        The folder name, such as `bo-suspenseAccounts`.
    """
    return name.replace(".", "-")


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


def describe_errors(error: ValidationError, skipped: set[str]) -> list[str]:
    """Lists every problem in a validation error in a fixed form.

    Args:
        error: The validation error.
        skipped: Top-level keys whose problems are left out.

    Returns:
        One `<key>: <problem>` entry per problem.
    """
    problems = []
    for item in error.errors(include_url=False):
        if item["loc"] and item["loc"][0] in skipped:
            continue
        key = ".".join(str(part) for part in item["loc"])
        problems.append(f"{key}: {item['msg']}" if key else item["msg"])
    return problems


def read_source(session_id: str) -> tuple[object, str | None]:
    """Reads the columns prepare-inputs wrote for the session.

    Args:
        session_id: The Claude Code session ID.

    Returns:
        The parsed file, or None, and the problem reading it, or None.
    """
    if not SESSION_ID.fullmatch(session_id):
        # The session ID's own problem is reported; no file is read for it.
        return None, None
    path = SESSIONS_DIR / session_id / COLUMNS_FILE
    try:
        return json.loads(path.read_text()), None
    except OSError as error:
        return None, f"source: {path}: {error.strerror or error}"
    except json.JSONDecodeError as error:
        return None, f"source: {path}: is not valid JSON: {error}"


def remove_session(session_id: str) -> None:
    """Deletes the session's folder, if the session ID is valid.

    Args:
        session_id: The Claude Code session ID.
    """
    if SESSION_ID.fullmatch(session_id):
        shutil.rmtree(SESSIONS_DIR / session_id, ignore_errors=True)


def parse_answers(answers: str) -> tuple[object, str | None]:
    """Parses the agent's answers.

    Args:
        answers: The answers as JSON text.

    Returns:
        The parsed answers, or None, and the problem parsing them, or None.
    """
    try:
        return json.loads(answers), None
    except json.JSONDecodeError as error:
        return None, f"answers: is not valid JSON: {error}"


def build_config(
    dynamo_table_name: str, session_id: str, project_dir: str, answers: str
) -> Config:
    """Builds and checks every value scaffold uses.

    Args:
        dynamo_table_name: The table name exactly as the user gave it.
        session_id: The Claude Code session ID.
        project_dir: The Claude Code project folder.
        answers: The agent's answers as JSON text.

    Returns:
        The checked configuration.

    Raises:
        ConfigError: If `inputs.toml`, `columns.json`, or the answers cannot be
            read or parsed, or any value is invalid.
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
        "firehose_table_name": to_firehose_name(dynamo_table_name),
    }
    source, source_problem = read_source(session_id)
    parsed_answers, answers_problem = parse_answers(answers)
    # The arguments come last, so the file cannot override them.
    raw = {
        **fill_placeholders(inputs, names),
        **names,
        "session_id": session_id,
        "project_dir": project_dir,
        "source": source,
        "answers": parsed_answers,
    }
    read_problems = [
        problem for problem in (source_problem, answers_problem) if problem
    ]
    # A file that could not be read has its own problem listed, so the errors
    # that follow from its missing value are left out.
    unread = set()
    if source_problem or not SESSION_ID.fullmatch(session_id):
        unread.add("source")
    if answers_problem:
        unread.add("answers")
    try:
        return Config.model_validate(raw)
    except ValidationError as error:
        problems = read_problems + describe_errors(error, unread)
        raise ConfigError("; ".join(problems)) from error

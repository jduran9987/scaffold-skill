"""Pydantic models for the v1 pipeline YAML.

Every section is fully defined, so unknown keys anywhere are rejected. Values are
validated strictly: YAML types must match the field, so `"true"` or `1` is not
accepted as a boolean.

A key set to null is treated as left out: optional keys take their default and
required keys fail as missing.
"""

import re
from typing import Any, Literal, Self

from pydantic import Field, ModelWrapValidatorHandler, model_validator

from sp_dynamo_firehose_s3.v1.defaults import DEFAULTS
from sp_dynamo_firehose_s3.v1.fields import (
    KEY_DYNAMO_TYPES,
    MAX_JOB_TAGS,
    NOTIFICATION_TARGET,
    PERMISSION_LEVELS,
    PIPELINE_NAME,
    PIPELINE_NAME_PATTERN,
    QUARTZ_FIELD_PATTERN,
    SILVER_METADATA_COLUMNS,
    TARGET_NAMES,
    TARGET_TYPE_RULES,
    YAML_RESERVED_WORDS,
    AttributePath,
    ColumnName,
    ConfiguredColumnName,
    DynamoType,
    EmailAddresses,
    GroupNames,
    NonEmptyString,
    PipelineName,
    PythonVersion,
    S3Path,
    ServicePrincipalPermission,
    StrictModel,
    TableName,
    TagKey,
    TagValue,
    TargetName,
    TargetType,
    UvBuildVersion,
    WebhookIds,
    WorkspaceHost,
    check_group_levels,
    resolve_env,
    resolve_name,
)


class Metadata(StrictModel):
    """Pipeline metadata.

    Attributes:
        name: Pipeline name, used as the default output directory.
        description: What the pipeline does. Starts the job description.
        python_version: Python version of the generated project, such as "3.12".
            Defaults to `metadata.python_version` in `defaults.toml`.
        uv_build_version: Version range of the `uv_build` backend the generated
            project builds with. Defaults to `metadata.uv_build_version` in
            `defaults.toml`.
    """

    name: PipelineName
    description: NonEmptyString
    python_version: PythonVersion = DEFAULTS.metadata.python_version
    uv_build_version: UvBuildVersion = DEFAULTS.metadata.uv_build_version


def check_quartz_cron(expression: str) -> None:
    """Checks that a schedule is a Quartz cron expression, as Databricks expects.

    Quartz uses six or seven fields: seconds, minutes, hours, day of month, month,
    day of week, and an optional year. Exactly one of the two day fields must be
    `?`.

    Args:
        expression: Cron expression from the YAML.

    Raises:
        ValueError: If the expression does not have Quartz's shape.
    """
    fields = expression.split()
    if len(fields) not in (6, 7) or not all(
        re.fullmatch(QUARTZ_FIELD_PATTERN, field) for field in fields
    ):
        raise ValueError(
            "job.batchSchedule must be a Quartz cron expression with six or seven "
            f'fields, such as "0 0 2 * * ?", not "{expression}"'
        )
    if (fields[3] == "?") == (fields[5] == "?"):
        raise ValueError(
            "job.batchSchedule must set exactly one of day of month (field 4) and "
            f'day of week (field 6) to "?", not "{expression}"'
        )


def parse_target_entries(entries: object) -> dict[str, object]:
    """Merges the YAML's single-key target entries into one mapping.

    Args:
        entries: Value of `bundle.targets`.

    Returns:
        Settings keyed by target name.

    Raises:
        ValueError: If the value is not a list of single-key mappings, a name is
            not a supported target, or a name repeats.
    """
    if not isinstance(entries, list):
        raise ValueError("bundle.targets must be a list of single-key mappings")
    targets: dict[str, object] = {}
    for entry in entries:
        if not isinstance(entry, dict) or len(entry) != 1:
            raise ValueError("each bundle.targets entry must have exactly one target")
        [(name, settings)] = entry.items()
        if name not in TARGET_NAMES:
            raise ValueError(
                f"bundle.targets names must be one of {', '.join(TARGET_NAMES)}, "
                f"not: {name}"
            )
        if name in targets:
            raise ValueError("bundle.targets names must not repeat")
        targets[name] = settings
    return targets


# Target settings only the notification target accepts. They have no defaults:
# a notification left out of the YAML is not sent.
NOTIFICATION_KEYS = (
    "onJobFailureWebhookNotificationIds",
    "onJobFailureEmails",
    "onTaskFailureWebhookNotificationIds",
    "onTaskFailureEmails",
)


def fill_target_defaults(name: str, settings: object) -> object:
    """Adds the target's defaults from `defaults.toml` to its settings.

    Args:
        name: Name of the deployment target.
        settings: Settings from the YAML, if any.

    Returns:
        The settings with every missing key set to the target's default, or the
        value unchanged if it is not a mapping. `{env}` in every value is replaced
        by the target's name. Notifications are never filled in.

    Raises:
        ValueError: If a target other than prod sets notifications, or a value
            contains a placeholder other than `{env}`.
    """
    if settings is None:
        settings = {}
    if not isinstance(settings, dict):
        return settings
    defaults = DEFAULTS.bundle.for_target(name)
    filled = {key: value for key, value in settings.items() if value is not None}
    filled.setdefault("host", defaults.host)
    filled.setdefault("computePolicy", defaults.compute_policy)
    filled.setdefault("servicePrincipal", defaults.service_principal)
    filled.setdefault(
        "servicePrincipalPermissions", defaults.service_principal_permissions
    )
    filled.setdefault("manageGroups", list(defaults.manage_groups))
    filled.setdefault("runGroups", list(defaults.run_groups))
    filled.setdefault("viewGroups", list(defaults.view_groups))
    filled = {
        key: resolve_env(resolve_name(value), name) for key, value in filled.items()
    }
    if name != NOTIFICATION_TARGET:
        rejected = [key for key in NOTIFICATION_KEYS if key in filled]
        if rejected:
            raise ValueError(
                f"bundle.targets.{name} cannot set {', '.join(rejected)}: "
                f"notifications are only sent by the {NOTIFICATION_TARGET} target"
            )
    return filled


class Target(StrictModel):
    """Settings for one deployment target.

    Attributes:
        host: URL of the target's workspace.
        compute_policy: ID of the cluster policy the tasks' compute uses.
        service_principal: Display name of the service principal the job runs as.
        service_principal_permissions: Level the service principal gets on the
            bundle's resources: `can_manage` or `can_run`.
        manage_groups: Groups with CAN_MANAGE.
        run_groups: Groups with CAN_RUN.
        view_groups: Groups with CAN_VIEW.
        on_job_failure_webhook_notification_ids: IDs of the notification
            destinations the job notifies when a run fails.
        on_job_failure_emails: Addresses the job emails when a run fails.
        on_task_failure_webhook_notification_ids: IDs of the notification
            destinations each task notifies when it fails.
        on_task_failure_emails: Addresses each task emails when it fails.

    Notifications can only be set for prod and are not sent when left out.
    Every other setting the YAML leaves out comes from `defaults.toml`, applied
    by `Bundle` before validation.
    """

    host: WorkspaceHost
    compute_policy: NonEmptyString
    service_principal: NonEmptyString
    service_principal_permissions: ServicePrincipalPermission
    manage_groups: GroupNames
    run_groups: GroupNames
    view_groups: GroupNames
    on_job_failure_webhook_notification_ids: WebhookIds | None = None
    on_job_failure_emails: EmailAddresses | None = None
    on_task_failure_webhook_notification_ids: WebhookIds | None = None
    on_task_failure_emails: EmailAddresses | None = None

    @model_validator(mode="after")
    def check_groups(self) -> Self:
        """Allows each group at only one permission level.

        Returns:
            The validated model.
        """
        check_group_levels(self.manage_groups, self.run_groups, self.view_groups)
        return self

    @property
    def service_principal_level(self) -> str:
        """The bundle permission level of the run-as service principal.

        Returns:
            `CAN_MANAGE` or `CAN_RUN`.
        """
        return PERMISSION_LEVELS[self.service_principal_permissions]

    def group_levels(self) -> list[tuple[str, str]]:
        """Lists each group with its bundle permission level.

        Returns:
            Pairs of group name and level, manage groups first, then run and view
            groups.
        """
        return [
            *((group, PERMISSION_LEVELS["can_manage"]) for group in self.manage_groups),
            *((group, PERMISSION_LEVELS["can_run"]) for group in self.run_groups),
            *((group, PERMISSION_LEVELS["can_view"]) for group in self.view_groups),
        ]


class Bundle(StrictModel):
    """Settings for the Declarative Automation Bundle.

    Attributes:
        tags: Tags added to the job, at most 25.
        targets: Settings for each deployment target. Every supported target is
            always present; targets left out of the YAML use the defaults.
    """

    tags: dict[TagKey, TagValue] = Field(
        default_factory=lambda: resolve_name(dict(DEFAULTS.bundle.tags)),
        max_length=MAX_JOB_TAGS,
    )
    targets: dict[TargetName, Target]

    @model_validator(mode="before")
    @classmethod
    def fill_targets(cls, data: object) -> object:
        """Fills in every supported target, applying defaults to missing settings.

        Args:
            data: Raw input for the model.

        Returns:
            The input with `targets` as a mapping of every supported target, in
            the order dev, qa, uat, prod.
        """
        if not isinstance(data, dict):
            return data
        entries = data.get("targets")
        supplied = parse_target_entries([] if entries is None else entries)
        targets = {
            name: fill_target_defaults(name, supplied.get(name))
            for name in TARGET_NAMES
        }
        return {**data, "targets": targets}

    @model_validator(mode="after")
    def check_tag_keys(self) -> Self:
        """Rejects tag keys that YAML would not read as strings.

        Returns:
            The validated model.

        Raises:
            ValueError: If a tag key is a YAML boolean or null word.
        """
        reserved = [key for key in self.tags if key.lower() in YAML_RESERVED_WORDS]
        if reserved:
            raise ValueError(
                f"bundle.tags keys are reserved YAML words: {', '.join(reserved)}"
            )
        return self


def default_or_required(value: str) -> Any:  # noqa: ANN401 - pydantic types Field as Any
    """Builds a field that defaults to a value from `defaults.toml`, if set.

    Args:
        value: The default, or an empty string for none.

    Returns:
        A field with the default, with `{name}` replaced, or a required field if
        the value is empty.
    """
    return Field(default_factory=lambda: resolve_name(value)) if value else Field()


class BronzeLayer(StrictModel):
    """Settings for the bronze job.

    Attributes:
        source_path: S3 URL to read from. May contain `{env}`.
        checkpoint_path: S3 URL of the streaming checkpoint. May contain `{env}`.
            Defaults to `job.bronze.checkpoint_path` in `defaults.toml`, if set.
        target_table: Three-part target table name. May contain `{env}`.
            Defaults to `job.bronze.target_table` in `defaults.toml`, if set.
        trigger_interval: Processing-time interval. Used only in continuous mode,
            where it defaults to `job.trigger_interval` in
            `defaults.toml`. Not allowed in batch mode.
    """

    source_path: S3Path
    checkpoint_path: S3Path = default_or_required(DEFAULTS.job.bronze.checkpoint_path)
    target_table: TableName = default_or_required(DEFAULTS.job.bronze.target_table)
    trigger_interval: str | None = None


class SilverLayer(StrictModel):
    """Settings for the silver job. The source is the bronze target table.

    Attributes:
        checkpoint_path: S3 URL of the streaming checkpoint. May contain `{env}`.
            Defaults to `job.silver.checkpoint_path` in `defaults.toml`, if set.
        target_table: Three-part target table name. May contain `{env}`.
            Defaults to `job.silver.target_table` in `defaults.toml`, if set.
        trigger_interval: Processing-time interval. Used only in continuous mode,
            where it defaults to `job.trigger_interval` in
            `defaults.toml`. Not allowed in batch mode.
    """

    checkpoint_path: S3Path = default_or_required(DEFAULTS.job.silver.checkpoint_path)
    target_table: TableName = default_or_required(DEFAULTS.job.silver.target_table)
    trigger_interval: str | None = None


class GoldLayer(StrictModel):
    """Settings for the gold job. The source is the silver target table.

    Attributes:
        upsert_latest_key: Silver column that orders records for the same primary
            key. The record with the highest value is kept. Defaults to
            `job.gold.upsert_latest_key` in `defaults.toml`.
        checkpoint_path: S3 URL of the streaming checkpoint. May contain `{env}`.
            Defaults to `job.gold.checkpoint_path` in `defaults.toml`, if set.
        target_table: Three-part target table name. May contain `{env}`.
            Defaults to `job.gold.target_table` in `defaults.toml`, if set.
        trigger_interval: Processing-time interval. Used only in continuous mode,
            where it defaults to `job.trigger_interval` in
            `defaults.toml`. Not allowed in batch mode.
    """

    upsert_latest_key: ColumnName = DEFAULTS.job.gold.upsert_latest_key
    checkpoint_path: S3Path = default_or_required(DEFAULTS.job.gold.checkpoint_path)
    target_table: TableName = default_or_required(DEFAULTS.job.gold.target_table)
    trigger_interval: str | None = None


class Job(StrictModel):
    """Settings for the pipeline's Databricks job.

    Attributes:
        execution_mode: Whether the job runs continuously or as a batch. Defaults
            to `job.execution_mode` in `defaults.toml`.
        batch_schedule: Quartz cron expression for when the batch job runs, in
            UTC. Required in batch mode and not allowed in continuous mode.
        bronze: Settings for the bronze job.
        silver: Settings for the silver job, if the pipeline has one.
        gold: Settings for the gold job, if the pipeline has one.
    """

    execution_mode: Literal["continuous", "batch"] = DEFAULTS.job.execution_mode
    batch_schedule: str | None = None
    bronze: BronzeLayer
    silver: SilverLayer | None = None
    gold: GoldLayer | None = None

    @model_validator(mode="after")
    def check_trigger_intervals(self) -> Self:
        """Defaults trigger intervals in continuous mode and rejects them in batch.

        Returns:
            The validated model, with every continuous layer's interval set.

        Raises:
            ValueError: If a layer sets an interval in batch mode.
        """
        layers = {"bronze": self.bronze, "silver": self.silver, "gold": self.gold}
        for layer_name, layer in layers.items():
            if layer is None:
                continue
            if self.execution_mode == "continuous" and layer.trigger_interval is None:
                layer.trigger_interval = DEFAULTS.job.trigger_interval
            if self.execution_mode == "batch" and layer.trigger_interval is not None:
                raise ValueError(
                    f"{layer_name}.triggerInterval is not allowed in batch mode"
                )
        return self

    @model_validator(mode="after")
    def check_batch_schedule(self) -> Self:
        """Requires a valid batch schedule in batch mode and rejects it otherwise.

        Returns:
            The validated model.

        Raises:
            ValueError: If the schedule is missing in batch mode, set in continuous
                mode, or not a Quartz cron expression.
        """
        if self.execution_mode == "continuous":
            if self.batch_schedule is not None:
                raise ValueError("job.batchSchedule is not allowed in continuous mode")
            return self
        if self.batch_schedule is None:
            raise ValueError("job.batchSchedule is required in batch mode")
        check_quartz_cron(self.batch_schedule)
        return self


class Column(StrictModel):
    """A column to extract from a DynamoDB change event.

    Attributes:
        name: Column name in the silver table.
        description: Description of the column.
        dynamo_type: DynamoDB type code of the attribute.
        target_type: Spark SQL type to cast the value to. Must suit the DynamoDB
            type; maps and lists keep DynamoDB's typed JSON.
        path: Dot-separated attribute path as it appears in the DynamoDB payload,
            including the `M` level of nested maps.
        key: Whether the attribute is part of the DynamoDB table's key.
    """

    name: ConfiguredColumnName
    description: NonEmptyString | None = None
    dynamo_type: DynamoType
    target_type: TargetType
    path: AttributePath
    key: bool = False

    @model_validator(mode="after")
    def check_column(self) -> Self:
        """Checks the types and key settings.

        Returns:
            The validated model.

        Raises:
            ValueError: If the target type does not suit the DynamoDB type, or a
                key attribute is nested or not a string, number, or binary.
        """
        pattern, allowed = TARGET_TYPE_RULES[self.dynamo_type]
        if not re.fullmatch(pattern, self.target_type):
            raise ValueError(
                f"column {self.name}: targetType for dynamoType {self.dynamo_type} "
                f"must be {allowed}, not {self.target_type}"
            )
        if self.key and "." in self.path:
            raise ValueError(f"key column {self.name} must have a top-level path")
        if self.key and self.dynamo_type not in KEY_DYNAMO_TYPES:
            raise ValueError(
                f"key column {self.name} must have dynamoType S, N, or B, "
                f"not {self.dynamo_type}"
            )
        return self


class TableSchema(StrictModel):
    """Columns of the silver table.

    Attributes:
        columns: Columns to extract, in table order.
        primary_keys: Names of the columns that identify a row.
    """

    columns: list[Column] = Field(min_length=1)
    primary_keys: list[ColumnName] = Field(min_length=1)

    @model_validator(mode="after")
    def check_names(self) -> Self:
        """Checks column names and primary keys.

        Returns:
            The validated model.

        Raises:
            ValueError: If a name is duplicated or a primary key does not name a
                column.
        """
        names = [column.name for column in self.columns]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"duplicate column names: {', '.join(duplicates)}")
        unknown = sorted(set(self.primary_keys) - set(names))
        if unknown:
            raise ValueError(f"primary keys are not columns: {', '.join(unknown)}")
        if len(set(self.primary_keys)) != len(self.primary_keys):
            raise ValueError("primary keys must not repeat")
        return self


class PipelineConfig(StrictModel):
    """Top-level v1 pipeline config.

    Attributes:
        version: Config version.
        metadata: Pipeline metadata.
        bundle: Settings for the Declarative Automation Bundle. Defaults apply
            to every setting left out.
        job: Settings for the pipeline's Databricks job.
        table_schema: Columns of the silver table. Required when the pipeline has
            a silver job.
    """

    version: Literal["v1"]
    metadata: Metadata
    bundle: Bundle = Field(default_factory=Bundle)
    job: Job
    table_schema: TableSchema | None = Field(default=None, alias="schema")

    @model_validator(mode="wrap")
    @classmethod
    def resolve_pipeline_name(
        cls, data: object, handler: ModelWrapValidatorHandler[Self]
    ) -> Self:
        """Replaces `{name}` and `{table}` in the YAML and its defaults.

        `{name}` becomes `metadata.name`, and `{table}` the same name with
        hyphens replaced by underscores. Both are replaced in every string of the
        YAML, and in every default from `defaults.toml` applied while the YAML is
        validated.

        Args:
            data: Raw input for the model.
            handler: Validates the input.

        Returns:
            The validated model.
        """
        metadata = data.get("metadata") if isinstance(data, dict) else None
        name = metadata.get("name") if isinstance(metadata, dict) else None
        if not isinstance(name, str) or not PIPELINE_NAME_PATTERN.fullmatch(name):
            return handler(data)
        token = PIPELINE_NAME.set(name)
        try:
            return handler(resolve_name(data))
        finally:
            PIPELINE_NAME.reset(token)

    @model_validator(mode="after")
    def check_layers(self) -> Self:
        """Checks that each layer has what it reads from.

        Returns:
            The validated model.

        Raises:
            ValueError: If the silver job has no schema, the gold job has no silver
                job, or the gold upsert key is not a silver column.
        """
        if self.job.silver is not None and self.table_schema is None:
            raise ValueError("schema is required when job.silver is set")
        gold = self.job.gold
        if gold is None:
            return self
        if self.job.silver is None or self.table_schema is None:
            raise ValueError("job.silver is required when job.gold is set")
        silver_columns = {column.name for column in self.table_schema.columns}
        if gold.upsert_latest_key not in silver_columns | SILVER_METADATA_COLUMNS:
            raise ValueError(
                f"job.gold.upsertLatestKey is not a silver column: "
                f"{gold.upsert_latest_key}"
            )
        return self

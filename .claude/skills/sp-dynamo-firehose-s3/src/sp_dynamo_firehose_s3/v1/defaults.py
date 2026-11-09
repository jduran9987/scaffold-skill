"""Loads the v1 defaults from `defaults.toml`.

The file sets the default for every optional pipeline YAML setting, the skill's
command options, and the AWS resources `create-stream` creates. It is validated
with the same field types as the pipeline YAML, so an invalid default fails when
the skill starts. Set the
`SP_DYNAMO_FIREHOSE_S3_V1_DEFAULTS` environment variable to load another file.
"""

import os
import tomllib
from importlib import resources
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from sp_dynamo_firehose_s3.v1.fields import (
    MAX_JOB_TAGS,
    TARGET_NAMES,
    TARGET_PLACEHOLDER,
    YAML_RESERVED_WORDS,
    AwsRegion,
    ColumnName,
    EmailAddresses,
    GroupNames,
    IamRoleArn,
    NonEmptyString,
    PythonVersion,
    S3BucketArn,
    S3Path,
    ServicePrincipalPermission,
    TableName,
    TagKey,
    TagValue,
    TargetName,
    UvBuildVersion,
    WebhookIds,
    WorkspaceHost,
    check_group_levels,
)

DEFAULTS_ENV_VAR = "SP_DYNAMO_FIREHOSE_S3_V1_DEFAULTS"
DEFAULTS_FILE = "defaults.toml"


class DefaultsError(ValueError):
    """Raised when the defaults file cannot be read or is invalid."""


class DefaultsModel(BaseModel):
    """Base model for the defaults file.

    Keys are snake_case, unknown keys are rejected, and values are not converted
    between types.
    """

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class CliDefaults(DefaultsModel):
    """Defaults for the skill's commands.

    Attributes:
        aws_profile: AWS CLI profile every AWS command uses.
        aws_region: AWS region every AWS command uses.
        sample_size: Items scanned to infer a schema.
        pipelines_dir: Directory `generate-yaml` writes pipeline YAMLs to.
        projects_dir: Directory `scaffold` writes projects under.
    """

    aws_profile: NonEmptyString
    aws_region: AwsRegion
    sample_size: Annotated[int, Field(gt=0)]
    pipelines_dir: NonEmptyString
    projects_dir: NonEmptyString


class MetadataDefaults(DefaultsModel):
    """Defaults for the `metadata` section.

    Attributes:
        python_version: Python version of generated projects.
        uv_build_version: Version range of the `uv_build` backend.
    """

    python_version: PythonVersion
    uv_build_version: UvBuildVersion


class TargetDefaults(DefaultsModel):
    """Defaults for one deployment target, with the shared defaults applied.

    Attributes:
        host: URL of the target's workspace.
        compute_policy: ID of the cluster policy the tasks' compute uses.
        service_principal: Run-as service principal.
        service_principal_permissions: Level the service principal gets:
            `can_manage` or `can_run`.
        manage_groups: Groups with CAN_MANAGE.
        run_groups: Groups with CAN_RUN.
        view_groups: Groups with CAN_VIEW.
    """

    host: WorkspaceHost
    compute_policy: NonEmptyString
    service_principal: NonEmptyString
    service_principal_permissions: ServicePrincipalPermission
    manage_groups: GroupNames
    run_groups: GroupNames
    view_groups: GroupNames


class SharedTargetDefaults(DefaultsModel):
    """Defaults shared by every target.

    Attributes:
        service_principal: Run-as service principal. `{target}` is replaced by the
            target's name.
    """

    service_principal: NonEmptyString


class TargetOverrides(DefaultsModel):
    """Settings for one target's workspace.

    Attributes:
        host: URL of the target's workspace.
        compute_policy: ID of the cluster policy the tasks' compute uses in the
            target's workspace.
        service_principal_permissions: Level the service principal gets:
            `can_manage` or `can_run`.
        manage_groups: Groups with CAN_MANAGE.
        run_groups: Groups with CAN_RUN.
        view_groups: Groups with CAN_VIEW.
        service_principal: Run-as service principal. Overrides the shared default.
    """

    host: WorkspaceHost
    compute_policy: NonEmptyString
    service_principal_permissions: ServicePrincipalPermission
    manage_groups: GroupNames
    run_groups: GroupNames
    view_groups: GroupNames
    service_principal: NonEmptyString | None = None

    @model_validator(mode="after")
    def check_groups(self) -> Self:
        """Allows each group at only one permission level.

        Returns:
            The validated model.
        """
        check_group_levels(self.manage_groups, self.run_groups, self.view_groups)
        return self


class BundleDefaults(DefaultsModel):
    """Defaults for the `bundle` section.

    The notification values only pre-fill the prod target in YAMLs that
    `generate-yaml` writes. Unset ones are left out of those YAMLs.

    Attributes:
        on_job_failure_webhook_notification_ids: Destinations the job notifies when
            a run fails.
        on_job_failure_emails: Addresses the job emails when a run fails.
        on_task_failure_webhook_notification_ids: Destinations each task notifies
            when it fails.
        on_task_failure_emails: Addresses each task emails when it fails.
        tags: Tags added to the job.
        target: Defaults shared by every target.
        targets: Each target's workspace settings and overrides. Every target
            must be present.
    """

    on_job_failure_webhook_notification_ids: WebhookIds | None = None
    on_job_failure_emails: EmailAddresses | None = None
    on_task_failure_webhook_notification_ids: WebhookIds | None = None
    on_task_failure_emails: EmailAddresses | None = None
    tags: Annotated[dict[TagKey, TagValue], Field(max_length=MAX_JOB_TAGS)]
    target: SharedTargetDefaults
    targets: dict[TargetName, TargetOverrides]

    @model_validator(mode="after")
    def check_every_target(self) -> Self:
        """Requires settings for every target, since each has its own workspace.

        Returns:
            The validated model.

        Raises:
            ValueError: If a target has no `[bundle.targets.<name>]` table.
        """
        missing = [name for name in TARGET_NAMES if name not in self.targets]
        if missing:
            tables = ", ".join(f"[bundle.targets.{name}]" for name in missing)
            raise ValueError(f"missing {tables} with host and compute_policy")
        return self

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
                f"tags keys are reserved YAML words: {', '.join(reserved)}"
            )
        return self

    def for_target(self, name: str) -> TargetDefaults:
        """Builds the defaults for one target.

        Args:
            name: Target name, such as `dev`.

        Returns:
            The target's workspace settings with the shared defaults for every
            setting it does not override, and `{target}` in the service principal
            and group names replaced by the name.
        """
        overrides = self.targets[name]  # type: ignore[index]
        values = self.target.model_dump() | overrides.model_dump(exclude_none=True)
        values["service_principal"] = values["service_principal"].replace(
            TARGET_PLACEHOLDER, name
        )
        for key in ("manage_groups", "run_groups", "view_groups"):
            values[key] = [
                group.replace(TARGET_PLACEHOLDER, name) for group in values[key]
            ]
        return TargetDefaults.model_validate(values)


class LayerDefaults(DefaultsModel):
    """Defaults for one layer of the `job` section. Empty means no default.

    Attributes:
        checkpoint_path: S3 URL of the layer's streaming checkpoint.
        target_table: Three-part name of the layer's target table.
    """

    checkpoint_path: S3Path | Literal[""]
    target_table: TableName | Literal[""]


class JobDefaults(DefaultsModel):
    """Defaults for the `job` section.

    Attributes:
        execution_mode: Whether jobs run continuously or as a batch.
        trigger_interval: Processing-time interval in continuous mode.
        upsert_latest_key: Silver column gold uses to keep the latest record.
        bronze: Defaults for the bronze layer.
        silver: Defaults for the silver layer.
        gold: Defaults for the gold layer.
    """

    execution_mode: Literal["continuous", "batch"]
    trigger_interval: NonEmptyString
    upsert_latest_key: ColumnName
    bronze: LayerDefaults
    silver: LayerDefaults
    gold: LayerDefaults


class StreamDefaults(DefaultsModel):
    """Defaults for the resources `create-stream` creates.

    Attributes:
        shard_count: Shards of the Kinesis data stream.
        firehose_role_arn: Existing IAM role Firehose uses to read the stream and
            write to the bucket.
        bucket_arn: S3 bucket Firehose writes to.
        buffer_size_mb: Data Firehose buffers before writing a file.
        buffer_interval_seconds: Time Firehose buffers before writing a file.
        partition_retry_seconds: Time Firehose retries delivery to a partition.
        poll_interval_seconds: Time between checks while a resource activates.
        wait_timeout_seconds: Total time one run waits for resources to become
            active. At most 540, under Claude Code's 10-minute command limit.
    """

    shard_count: Annotated[int, Field(gt=0)]
    firehose_role_arn: IamRoleArn
    bucket_arn: S3BucketArn
    buffer_size_mb: Annotated[int, Field(ge=64, le=128)]
    buffer_interval_seconds: Annotated[int, Field(ge=60, le=900)]
    partition_retry_seconds: Annotated[int, Field(ge=0, le=7200)]
    poll_interval_seconds: Annotated[int, Field(gt=0)]
    wait_timeout_seconds: Annotated[int, Field(gt=0, le=540)]


class Defaults(DefaultsModel):
    """Every default in `defaults.toml`.

    Attributes:
        cli: Defaults for the skill's commands.
        metadata: Defaults for the `metadata` section.
        bundle: Defaults for the `bundle` section.
        job: Defaults for the `job` section.
        stream: Defaults for the resources `create-stream` creates.
    """

    cli: CliDefaults
    metadata: MetadataDefaults
    bundle: BundleDefaults
    job: JobDefaults
    stream: StreamDefaults


def read_defaults_text() -> tuple[str, str]:
    """Reads the defaults file.

    Returns:
        The file's location, for error messages, and its contents.
    """
    override = os.environ.get(DEFAULTS_ENV_VAR)
    if override:
        return override, Path(override).read_text()
    packaged = resources.files(__package__).joinpath(DEFAULTS_FILE)
    return str(packaged), packaged.read_text()


def load_defaults() -> Defaults:
    """Loads and validates the defaults file.

    Returns:
        The validated defaults.

    Raises:
        DefaultsError: If the file cannot be read or has invalid values.
    """
    location = DEFAULTS_ENV_VAR
    try:
        location, text = read_defaults_text()
        return Defaults.model_validate(tomllib.loads(text))
    except (OSError, tomllib.TOMLDecodeError, ValidationError) as error:
        raise DefaultsError(f"Invalid defaults file {location}: {error}") from error


DEFAULTS = load_defaults()

__all__ = ["DEFAULTS", "TARGET_NAMES", "Defaults", "DefaultsError", "load_defaults"]

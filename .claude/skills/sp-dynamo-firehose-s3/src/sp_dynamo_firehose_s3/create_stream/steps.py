"""One function per resource: check it exists, create it if not, wait for it.

Each step ends in one of three ways:

- It returns the resource's status, `EXISTS` or `CREATED`.
- It raises `Stop` with a result code, for an outcome with fixed text.
- It lets an `AwsError` pass up, for an AWS failure with no special handling.

Existing resources are never changed and their settings are never compared:
a resource that exists by name is used as it is. Every step waits until its
resource is active, whether the step created it or found it still being set up.
"""

import json
import time
from collections.abc import Callable
from enum import IntEnum, StrEnum
from typing import Any

from sp_dynamo_firehose_s3.aws import AwsError, AwsErrorKind, run_aws
from sp_dynamo_firehose_s3.create_stream.config import Config
from sp_dynamo_firehose_s3.create_stream.report import Code

KINESIS_READY = frozenset({"ACTIVE", "UPDATING"})
KINESIS_PENDING = frozenset({"CREATING"})
# A table destination with one of these statuses counts as streaming.
STREAMING_ENABLED = frozenset({"ENABLING", "ACTIVE", "UPDATING"})
STREAMING_READY = frozenset({"ACTIVE", "UPDATING"})
STREAMING_PENDING = frozenset({"ENABLING"})
# AWS can enable streaming only while every other destination has one of these.
STREAMING_OFF = frozenset({"DISABLED", "ENABLE_FAILED"})
FIREHOSE_READY = frozenset({"ACTIVE"})
FIREHOSE_PENDING = frozenset({"CREATING"})
# Shown when a resource disappears while waiting for it.
MISSING = "MISSING"


class Resource(IntEnum):
    """The resources, in the order the steps handle them."""

    TABLE = 1
    KINESIS = 2
    STREAMING = 3
    FIREHOSE = 4


class Status(StrEnum):
    """What a run found or did for one resource, as the report shows it."""

    NOT_CHECKED = "not checked"
    EXISTS = "exists"
    CREATED = "created"
    MISSING = "missing"
    NOT_ACTIVE = "not active yet"
    CONFLICT = "conflict"
    FAILED = "failed"


class Stop(Exception):
    """Ends a run with a result code that has fixed text.

    Attributes:
        code: How the run ended.
        resource_status: The status to show for the resource the step handles.
        facts: Values the code's text is filled in with.
    """

    def __init__(self, code: Code, resource_status: Status, **facts: object) -> None:
        """Records the outcome.

        Args:
            code: How the run ended.
            resource_status: The status to show for the resource.
            **facts: Values the code's text is filled in with.
        """
        super().__init__(code)
        self.code = code
        self.resource_status = resource_status
        self.facts = facts


def resource_labels(config: Config) -> dict[Resource, str]:
    """Names each resource the way the report shows it.

    Args:
        config: The run's configuration.

    Returns:
        One label per resource.
    """
    table, kinesis = config.dynamo_table_name, config.kinesis.name
    return {
        Resource.TABLE: f"DynamoDB table {table}",
        Resource.KINESIS: f"Kinesis stream {kinesis}",
        Resource.STREAMING: f"Streaming from table {table} to Kinesis stream {kinesis}",
        Resource.FIREHOSE: f"Firehose stream {config.firehose.name}",
    }


def wait_until_ready(
    config: Config,
    deadline: float,
    resource: Resource,
    read_status: Callable[[], tuple[str, str]],
    ready: frozenset[str],
    pending: frozenset[str],
    failed: Code,
) -> None:
    """Waits until a resource is active, within the run's deadline.

    Args:
        config: The run's configuration.
        deadline: `time.monotonic()` value after which the run stops waiting.
        resource: The resource being waited for.
        read_status: Returns the resource's AWS status and any failure details.
        ready: Statuses that mean the resource is active.
        pending: Statuses that mean it is still being set up.
        failed: Code for any other status.

    Raises:
        Stop: If the resource reaches another status, or the deadline passes.
    """
    while True:
        status, details = read_status()
        if status in ready:
            return
        if status not in pending:
            raise Stop(failed, Status.FAILED, status=status, details=details)
        if time.monotonic() >= deadline:
            raise Stop(
                Code.TIMEOUT,
                Status.NOT_ACTIVE,
                resource=resource_labels(config)[resource],
                status=status,
            )
        time.sleep(config.wait.poll_interval_seconds)


# 1. DynamoDB table


def check_table(config: Config) -> Status:
    """Checks the DynamoDB table exists. The table is never created.

    Args:
        config: The run's configuration.

    Returns:
        `EXISTS`.

    Raises:
        Stop: If the table does not exist.
    """
    try:
        run_aws(
            config.aws.profile,
            config.aws.region,
            "dynamodb",
            ["describe-table", "--table-name", config.dynamo_table_name],
        )
    except AwsError as error:
        if error.kind is AwsErrorKind.NOT_FOUND:
            raise Stop(Code.TABLE_NOT_FOUND, Status.MISSING) from error
        raise
    return Status.EXISTS


# 1b. The table does not stream to another Kinesis stream


def table_destinations(config: Config) -> list[dict[str, Any]]:
    """Lists the table's Kinesis streaming destinations.

    Args:
        config: The run's configuration.

    Returns:
        Each destination's description, with its `StreamArn` and
        `DestinationStatus`.
    """
    result = run_aws(
        config.aws.profile,
        config.aws.region,
        "dynamodb",
        [
            "describe-kinesis-streaming-destination",
            "--table-name",
            config.dynamo_table_name,
        ],
    )
    return result.get("KinesisDataStreamDestinations", [])


def check_no_other_stream(config: Config) -> None:
    """Checks the table does not stream to a Kinesis stream other than its own.

    A table can stream to only one Kinesis stream at a time, so this runs
    before anything is created. Streams are compared by the name in their ARN,
    because the table's own stream may not exist yet.

    Args:
        config: The run's configuration.

    Raises:
        Stop: If the table streams, or is starting or stopping streaming, to
            another Kinesis stream.
    """
    for destination in table_destinations(config):
        arn = destination.get("StreamArn", "")
        status = destination.get("DestinationStatus", "")
        if arn.rsplit("/", 1)[-1] != config.kinesis.name and status not in (
            STREAMING_OFF
        ):
            raise Stop(
                Code.TABLE_STREAMS_ELSEWHERE,
                Status.CONFLICT,
                other_stream=arn,
                other_status=status,
            )


# 2. Kinesis stream


def describe_kinesis_stream(config: Config) -> dict[str, Any] | None:
    """Describes the Kinesis stream.

    Args:
        config: The run's configuration.

    Returns:
        The stream's `StreamDescriptionSummary`, or None if it does not exist.
    """
    try:
        result = run_aws(
            config.aws.profile,
            config.aws.region,
            "kinesis",
            ["describe-stream-summary", "--stream-name", config.kinesis.name],
        )
    except AwsError as error:
        if error.kind is AwsErrorKind.NOT_FOUND:
            return None
        raise
    return result["StreamDescriptionSummary"]


def ensure_kinesis_stream(config: Config, deadline: float) -> tuple[Status, str]:
    """Creates the Kinesis stream if it does not exist, then waits for it.

    Args:
        config: The run's configuration.
        deadline: `time.monotonic()` value after which the run stops waiting.

    Returns:
        `EXISTS` or `CREATED`, and the stream's ARN.

    Raises:
        Stop: If the stream cannot be used, or the deadline passes.
    """
    status = Status.EXISTS
    if describe_kinesis_stream(config) is None:
        run_aws(
            config.aws.profile,
            config.aws.region,
            "kinesis",
            [
                "create-stream",
                "--stream-name",
                config.kinesis.name,
                "--shard-count",
                str(config.kinesis.shard_count),
            ],
        )
        status = Status.CREATED

    def read_status() -> tuple[str, str]:
        stream = describe_kinesis_stream(config) or {}
        return stream.get("StreamStatus", MISSING), ""

    wait_until_ready(
        config,
        deadline,
        Resource.KINESIS,
        read_status,
        KINESIS_READY,
        KINESIS_PENDING,
        Code.KINESIS_FAILED,
    )
    stream = describe_kinesis_stream(config)
    if stream is None:
        raise Stop(Code.KINESIS_FAILED, Status.FAILED, status=MISSING)
    return status, stream["StreamARN"]


# 3. Streaming from the table to the Kinesis stream


def streaming_destination(config: Config, stream_arn: str) -> dict[str, Any] | None:
    """Finds the table's streaming destination for the Kinesis stream.

    Args:
        config: The run's configuration.
        stream_arn: The Kinesis stream's ARN.

    Returns:
        The destination, or None if the table never streamed to the stream.
    """
    for destination in table_destinations(config):
        if destination.get("StreamArn") == stream_arn:
            return destination
    return None


def ensure_streaming(config: Config, stream_arn: str, deadline: float) -> Status:
    """Enables streaming from the table to the Kinesis stream if needed, then waits.

    Args:
        config: The run's configuration.
        stream_arn: The Kinesis stream's ARN.
        deadline: `time.monotonic()` value after which the run stops waiting.

    Returns:
        `EXISTS` or `CREATED`.

    Raises:
        Stop: If streaming fails, or the deadline passes.
    """
    status = Status.EXISTS
    destination = streaming_destination(config, stream_arn) or {}
    if destination.get("DestinationStatus") not in STREAMING_ENABLED:
        run_aws(
            config.aws.profile,
            config.aws.region,
            "dynamodb",
            [
                "enable-kinesis-streaming-destination",
                "--table-name",
                config.dynamo_table_name,
                "--stream-arn",
                stream_arn,
            ],
        )
        status = Status.CREATED

    def read_status() -> tuple[str, str]:
        found = streaming_destination(config, stream_arn) or {}
        return (
            found.get("DestinationStatus", MISSING),
            found.get("DestinationStatusDescription", ""),
        )

    wait_until_ready(
        config,
        deadline,
        Resource.STREAMING,
        read_status,
        STREAMING_READY,
        STREAMING_PENDING,
        Code.STREAMING_FAILED,
    )
    return status


# 4. Firehose stream


def describe_firehose_stream(config: Config) -> dict[str, Any] | None:
    """Describes the Firehose stream.

    Args:
        config: The run's configuration.

    Returns:
        The stream's `DeliveryStreamDescription`, or None if it does not exist.
    """
    try:
        result = run_aws(
            config.aws.profile,
            config.aws.region,
            "firehose",
            [
                "describe-delivery-stream",
                "--delivery-stream-name",
                config.firehose.name,
            ],
        )
    except AwsError as error:
        if error.kind is AwsErrorKind.NOT_FOUND:
            return None
        raise
    return result["DeliveryStreamDescription"]


def firehose_settings(config: Config, stream_arn: str) -> dict[str, Any]:
    """Builds the `create-delivery-stream` input from the configuration.

    Args:
        config: The run's configuration.
        stream_arn: The Kinesis stream's ARN, the Firehose stream's source.

    Returns:
        The input for `aws firehose create-delivery-stream --cli-input-json`.
    """
    firehose = config.firehose
    return {
        "DeliveryStreamName": firehose.name,
        "DeliveryStreamType": "KinesisStreamAsSource",
        "KinesisStreamSourceConfiguration": {
            "KinesisStreamARN": stream_arn,
            "RoleARN": firehose.role_arn,
        },
        "ExtendedS3DestinationConfiguration": {
            "RoleARN": firehose.role_arn,
            "BucketARN": firehose.bucket_arn,
            "Prefix": firehose.prefix,
            "ErrorOutputPrefix": firehose.error_output_prefix,
            "BufferingHints": {
                "SizeInMBs": firehose.buffer_size_mb,
                "IntervalInSeconds": firehose.buffer_interval_seconds,
            },
            "CompressionFormat": firehose.compression_format,
            "DynamicPartitioningConfiguration": {
                "Enabled": True,
                "RetryOptions": {"DurationInSeconds": firehose.partition_retry_seconds},
            },
            "ProcessingConfiguration": {
                "Enabled": True,
                "Processors": [
                    {
                        "Type": "MetadataExtraction",
                        "Parameters": [
                            {
                                "ParameterName": "MetadataExtractionQuery",
                                "ParameterValue": firehose.metadata_extraction_query,
                            },
                            {
                                "ParameterName": "JsonParsingEngine",
                                "ParameterValue": firehose.json_parsing_engine,
                            },
                        ],
                    },
                    {
                        "Type": "AppendDelimiterToRecord",
                        "Parameters": [
                            {
                                "ParameterName": "Delimiter",
                                "ParameterValue": firehose.record_delimiter,
                            }
                        ],
                    },
                ],
            },
        },
    }


def ensure_firehose_stream(config: Config, stream_arn: str, deadline: float) -> Status:
    """Creates the Firehose stream if it does not exist, then waits for it.

    Args:
        config: The run's configuration.
        stream_arn: The Kinesis stream's ARN, the Firehose stream's source.
        deadline: `time.monotonic()` value after which the run stops waiting.

    Returns:
        `EXISTS` or `CREATED`.

    Raises:
        Stop: If the stream fails, or the deadline passes.
    """
    status = Status.EXISTS
    if describe_firehose_stream(config) is None:
        settings = json.dumps(firehose_settings(config, stream_arn))
        run_aws(
            config.aws.profile,
            config.aws.region,
            "firehose",
            ["create-delivery-stream", "--cli-input-json", settings],
        )
        status = Status.CREATED

    def read_status() -> tuple[str, str]:
        stream = describe_firehose_stream(config) or {}
        failure = stream.get("FailureDescription") or {}
        return stream.get("DeliveryStreamStatus", MISSING), failure.get("Details", "")

    wait_until_ready(
        config,
        deadline,
        Resource.FIREHOSE,
        read_status,
        FIREHOSE_READY,
        FIREHOSE_PENDING,
        Code.FIREHOSE_FAILED,
    )
    return status

"""Creates the DynamoDB to Kinesis to Firehose to S3 resources for a table.

A run has two phases. The check phase reads every resource and looks for anything
that would block the setup, such as the table already streaming to another Kinesis
stream. If it finds one, the run stops before creating anything. The create phase
then creates only the missing resources, in order, waiting for each to become
active, and a final check confirms everything is active and connected. Existing
resources are never changed.

Every run ends with one of a fixed set of outcomes, each with a fixed explanation
and next step, so the output is the same for the same AWS state.
"""

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import IntEnum, StrEnum
from typing import Any

from sp_dynamo_firehose_s3.v1.aws import AwsError, AwsErrorKind, run_aws
from sp_dynamo_firehose_s3.v1.defaults import DEFAULTS
from sp_dynamo_firehose_s3.v1.fields import DYNAMO_TABLE_NAME_PATTERN
from sp_dynamo_firehose_s3.v1.report import format_report, format_result

DELIVERY_STREAM_SUFFIX = "-firehose"
MAX_DELIVERY_STREAM_NAME_LENGTH = 64
S3_PREFIX_ROOT = "firehose-stream"
# Firehose partitions by the record's table name, with dots replaced by dashes.
METADATA_EXTRACTION_QUERY = r'{tableName:limit(1;.tableName | gsub("\\.", "-"; "-"))}'
JQ_ENGINE = "JQ-1.6"
# Firehose reads the two characters `\n` as a newline.
RECORD_DELIMITER = r"\n"

KINESIS_READY = frozenset({"ACTIVE", "UPDATING"})
KINESIS_PENDING = frozenset({"CREATING"})
DESTINATION_READY = frozenset({"ACTIVE", "UPDATING"})
DESTINATION_PENDING = frozenset({"ENABLING"})
DESTINATION_BUSY = frozenset({"DISABLING"})
FIREHOSE_READY = frozenset({"ACTIVE"})
FIREHOSE_PENDING = frozenset({"CREATING"})
FIREHOSE_UNUSABLE = frozenset({"CREATING_FAILED", "DELETING", "DELETING_FAILED"})


class Step(IntEnum):
    """The resources, in the order they are checked and created."""

    TABLE = 0
    KINESIS = 1
    STREAMING = 2
    FIREHOSE = 3


class StepStatus(StrEnum):
    """What a run found or did for one resource."""

    NOT_CHECKED = "not checked"
    EXISTS = "exists"
    MISSING = "missing"
    WOULD_CREATE = "would be created"
    CREATED = "created"
    PENDING = "still being created"
    CONFLICT = "conflict"
    FAILED = "failed"


class Outcome(StrEnum):
    """How a run ended, other than an AWS CLI failure."""

    CREATED = "CREATED"
    ALREADY_EXISTED = "ALREADY_EXISTED"
    READY = "READY"
    WOULD_CREATE = "WOULD_CREATE"
    IN_PROGRESS = "IN_PROGRESS"
    INVALID_TABLE_NAME = "INVALID_TABLE_NAME"
    TABLE_NOT_FOUND = "TABLE_NOT_FOUND"
    TABLE_STREAMS_ELSEWHERE = "TABLE_STREAMS_ELSEWHERE"
    TABLE_STREAMING_BUSY = "TABLE_STREAMING_BUSY"
    KINESIS_STREAM_DELETING = "KINESIS_STREAM_DELETING"
    FIREHOSE_WIRED_ELSEWHERE = "FIREHOSE_WIRED_ELSEWHERE"
    FIREHOSE_UNUSABLE = "FIREHOSE_UNUSABLE"
    FIREHOSE_CONFIG_REJECTED = "FIREHOSE_CONFIG_REJECTED"
    STREAMING_FAILED = "STREAMING_FAILED"
    FIREHOSE_FAILED = "FIREHOSE_FAILED"
    TIMEOUT = "TIMEOUT"
    NOT_CONFIRMED = "NOT_CONFIRMED"


SUCCESS_OUTCOMES = frozenset(
    {
        Outcome.CREATED,
        Outcome.ALREADY_EXISTED,
        Outcome.READY,
        Outcome.WOULD_CREATE,
        Outcome.IN_PROGRESS,
    }
)
CONNECTED = (
    "DynamoDB table {table} streams to Kinesis stream {table}, which Firehose "
    "stream {firehose} delivers to {bucket_arn}."
)
SET_SOURCE_PATH = "Set job.bronze.sourcePath to {source_path}."
RERUN = "Fix the cause, then rerun create-stream {table}."
# The explanation and next step for each outcome. Values come only from the run.
OUTCOME_TEXT: dict[Outcome, tuple[str, str]] = {
    Outcome.CREATED: (
        f"Every resource is active and connected. {CONNECTED}",
        SET_SOURCE_PATH,
    ),
    Outcome.ALREADY_EXISTED: (
        f"Every resource already existed and is active and connected. {CONNECTED}",
        SET_SOURCE_PATH,
    ),
    Outcome.READY: (
        f"Every resource exists and is active and connected. {CONNECTED}",
        SET_SOURCE_PATH,
    ),
    Outcome.WOULD_CREATE: (
        "Nothing blocks the setup. create-stream would create: {missing}.",
        "Run create-stream {table} to create them.",
    ),
    Outcome.IN_PROGRESS: (
        "Every resource exists, but {resource} is still {status}.",
        "Rerun create-stream {table} --check in a few minutes.",
    ),
    Outcome.INVALID_TABLE_NAME: (
        "{table} cannot name the streams: {reason}.",
        "Check the table name, then rerun.",
    ),
    Outcome.TABLE_NOT_FOUND: (
        "DynamoDB table {table} does not exist in {region} for the AWS CLI "
        "profile {profile}.",
        "Check the table name, or aws_region and aws_profile in defaults.toml, "
        "then rerun.",
    ),
    Outcome.TABLE_STREAMS_ELSEWHERE: (
        "DynamoDB table {table} already streams to {other_stream} (status "
        "{other_status}). DynamoDB cannot stream the table to Kinesis stream "
        "{table} while that is enabled.",
        "Keep the existing setup, or disable the table's streaming to "
        "{other_stream} yourself, then rerun.",
    ),
    Outcome.TABLE_STREAMING_BUSY: (
        "DynamoDB table {table}'s streaming to {other_stream} is {other_status}.",
        "Wait a few minutes, then rerun.",
    ),
    Outcome.KINESIS_STREAM_DELETING: (
        "Kinesis stream {table} is being deleted.",
        "Wait until it is gone, then rerun.",
    ),
    Outcome.FIREHOSE_WIRED_ELSEWHERE: (
        "Firehose stream {firehose} already exists but reads {firehose_source} and "
        "writes to {firehose_bucket}, instead of reading Kinesis stream {table} "
        "and writing to {bucket_arn}.",
        "Delete or rename Firehose stream {firehose} yourself, or change "
        "bucket_arn in defaults.toml, then rerun.",
    ),
    Outcome.FIREHOSE_UNUSABLE: (
        "Firehose stream {firehose} exists with status {status}. {details}",
        "Delete Firehose stream {firehose} yourself, then rerun.",
    ),
    Outcome.FIREHOSE_CONFIG_REJECTED: (
        "AWS rejected the Firehose stream's settings. AWS said: {aws_message}",
        "Check firehose_role_arn ({role_arn}) and bucket_arn ({bucket_arn}) in "
        "defaults.toml. The role must exist, trust firehose.amazonaws.com, read "
        "Kinesis stream {table}, and write to the bucket. Then rerun.",
    ),
    Outcome.STREAMING_FAILED: (
        "Streaming from DynamoDB table {table} to Kinesis stream {table} ended "
        "with status {status}. {details}",
        RERUN,
    ),
    Outcome.FIREHOSE_FAILED: (
        "Creating Firehose stream {firehose} ended with status {status}. {details}",
        "Delete Firehose stream {firehose} yourself, fix the cause, then rerun.",
    ),
    Outcome.TIMEOUT: (
        "{resource} was still {status} after waiting {timeout} seconds.",
        "Rerun create-stream {table}. It skips what exists and waits again.",
    ),
    Outcome.NOT_CONFIRMED: (
        "The final check found {resource} with status {status}.",
        "Run create-stream {table} --check to see the current state.",
    ),
}


class Stop(Exception):
    """Ends a run with an outcome.

    Attributes:
        outcome: How the run ended.
        step: The resource the outcome is about.
        step_status: The status to show for that resource.
        facts: Values the outcome's text is filled in with.
    """

    def __init__(
        self, outcome: Outcome, step: Step, step_status: StepStatus, **facts: object
    ) -> None:
        """Records the outcome.

        Args:
            outcome: How the run ended.
            step: The resource the outcome is about.
            step_status: The status to show for that resource.
            **facts: Values the outcome's text is filled in with.
        """
        super().__init__(outcome)
        self.outcome = outcome
        self.step = step
        self.step_status = step_status
        self.facts = {key: str(value) for key, value in facts.items()}


@dataclass(frozen=True, slots=True)
class Snapshot:
    """The resources as the AWS CLI described them.

    Attributes:
        stream: The Kinesis stream's summary, or None if it does not exist.
        destinations: The table's Kinesis streaming destinations.
        firehose: The Firehose stream's description, or None if it does not exist.
    """

    stream: dict[str, Any] | None
    destinations: list[dict[str, Any]]
    firehose: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class RunReport:
    """The result of a run.

    Attributes:
        code: Result code, an `Outcome` or `AwsErrorKind` value.
        ok: Whether the run succeeded.
        text: The full report to print.
    """

    code: str
    ok: bool
    text: str


def delivery_stream_name(table: str) -> str:
    """Names the table's Firehose stream.

    Args:
        table: DynamoDB table name.

    Returns:
        The Firehose stream name, `<table>-firehose`.
    """
    return f"{table}{DELIVERY_STREAM_SUFFIX}"


def table_name_problem(table: str) -> str | None:
    """Checks the table name can name the Kinesis and Firehose streams.

    Args:
        table: DynamoDB table name.

    Returns:
        Why the name cannot be used, or None if it can.
    """
    if not DYNAMO_TABLE_NAME_PATTERN.fullmatch(table):
        return "use 3 to 255 letters, digits, underscores, dashes, or dots"
    name = delivery_stream_name(table)
    if len(name) > MAX_DELIVERY_STREAM_NAME_LENGTH:
        return (
            f"Firehose stream name {name} is longer than "
            f"{MAX_DELIVERY_STREAM_NAME_LENGTH} characters"
        )
    return None


def bronze_source_path(table: str) -> str:
    """Builds the S3 path Firehose writes the table's records under.

    Args:
        table: DynamoDB table name.

    Returns:
        The path to use as the pipeline YAML's `job.bronze.sourcePath`.
    """
    bucket = DEFAULTS.stream.bucket_arn.rsplit(":", 1)[-1]
    return f"s3://{bucket}/{S3_PREFIX_ROOT}/{table.replace('.', '-')}/"


def build_delivery_stream_config(table: str, stream_arn: str) -> dict[str, Any]:
    """Builds the `create-delivery-stream` input for the table's Firehose stream.

    The stream reads the Kinesis stream, partitions records by table name and
    hour, compresses them with GZIP, and writes one record per line.

    Args:
        table: DynamoDB table name.
        stream_arn: ARN of the table's Kinesis stream.

    Returns:
        The input for `aws firehose create-delivery-stream --cli-input-json`.
    """
    stream = DEFAULTS.stream
    hour = "!{timestamp:yyyy/MM/dd/HH}"
    return {
        "DeliveryStreamName": delivery_stream_name(table),
        "DeliveryStreamType": "KinesisStreamAsSource",
        "KinesisStreamSourceConfiguration": {
            "KinesisStreamARN": stream_arn,
            "RoleARN": stream.firehose_role_arn,
        },
        "ExtendedS3DestinationConfiguration": {
            "BucketARN": stream.bucket_arn,
            "Prefix": f"{S3_PREFIX_ROOT}/!{{partitionKeyFromQuery:tableName}}/{hour}/",
            "ErrorOutputPrefix": (
                f"{S3_PREFIX_ROOT}/failures/{table}/"
                f"!{{firehose:error-output-type}}/{hour}/"
            ),
            "BufferingHints": {
                "SizeInMBs": stream.buffer_size_mb,
                "IntervalInSeconds": stream.buffer_interval_seconds,
            },
            "CompressionFormat": "GZIP",
            "DynamicPartitioningConfiguration": {
                "Enabled": True,
                "RetryOptions": {"DurationInSeconds": stream.partition_retry_seconds},
            },
            "ProcessingConfiguration": {
                "Enabled": True,
                "Processors": [
                    {
                        "Type": "MetadataExtraction",
                        "Parameters": [
                            {
                                "ParameterName": "MetadataExtractionQuery",
                                "ParameterValue": METADATA_EXTRACTION_QUERY,
                            },
                            {
                                "ParameterName": "JsonParsingEngine",
                                "ParameterValue": JQ_ENGINE,
                            },
                        ],
                    },
                    {
                        "Type": "AppendDelimiterToRecord",
                        "Parameters": [
                            {
                                "ParameterName": "Delimiter",
                                "ParameterValue": RECORD_DELIMITER,
                            }
                        ],
                    },
                ],
            },
            "RoleARN": stream.firehose_role_arn,
        },
    }


def describe_kinesis_stream(table: str) -> dict[str, Any] | None:
    """Describes the table's Kinesis stream.

    Args:
        table: DynamoDB table name, which is also the stream name.

    Returns:
        The stream's `StreamDescriptionSummary`, or None if it does not exist.
    """
    try:
        result = run_aws("kinesis", ["describe-stream-summary", "--stream-name", table])
    except AwsError as error:
        if error.kind is AwsErrorKind.NOT_FOUND:
            return None
        raise
    return result["StreamDescriptionSummary"]


def describe_destinations(table: str) -> list[dict[str, Any]]:
    """Lists the table's Kinesis streaming destinations.

    Args:
        table: DynamoDB table name.

    Returns:
        Each destination's description.
    """
    result = run_aws(
        "dynamodb", ["describe-kinesis-streaming-destination", "--table-name", table]
    )
    return result.get("KinesisDataStreamDestinations", [])


def describe_delivery_stream(table: str) -> dict[str, Any] | None:
    """Describes the table's Firehose stream.

    Args:
        table: DynamoDB table name.

    Returns:
        The stream's `DeliveryStreamDescription`, or None if it does not exist.
    """
    name = delivery_stream_name(table)
    try:
        result = run_aws(
            "firehose", ["describe-delivery-stream", "--delivery-stream-name", name]
        )
    except AwsError as error:
        if error.kind is AwsErrorKind.NOT_FOUND:
            return None
        raise
    return result["DeliveryStreamDescription"]


def firehose_wiring(description: dict[str, Any]) -> tuple[str, list[str]]:
    """Reads which Kinesis stream a Firehose stream reads and which buckets it writes.

    Args:
        description: The Firehose stream's `DeliveryStreamDescription`.

    Returns:
        The source stream's ARN, empty if it has none, and the bucket ARNs.
    """
    source = description.get("Source", {}).get("KinesisStreamSourceDescription", {})
    buckets = [
        destination.get("ExtendedS3DestinationDescription", {}).get("BucketARN", "")
        for destination in description.get("Destinations", [])
    ]
    return source.get("KinesisStreamARN", ""), buckets


def firehose_failure(description: dict[str, Any]) -> str:
    """Reads why a Firehose stream failed.

    Args:
        description: The Firehose stream's `DeliveryStreamDescription`.

    Returns:
        The failure details, or an empty string.
    """
    return (description.get("FailureDescription") or {}).get("Details", "")


class CreateStreamRun:
    """One run of `create-stream` for a table."""

    def __init__(self, table: str, check_only: bool) -> None:
        """Prepares the run.

        Args:
            table: DynamoDB table name.
            check_only: Whether to only check, creating nothing.
        """
        self.table = table
        self.check_only = check_only
        self.firehose = delivery_stream_name(table)
        self.labels = {
            Step.TABLE: f"DynamoDB table {table}",
            Step.KINESIS: f"Kinesis stream {table}",
            Step.STREAMING: f"Streaming from table {table} to Kinesis stream {table}",
            Step.FIREHOSE: f"Firehose stream {self.firehose}",
        }
        self.statuses = dict.fromkeys(Step, StepStatus.NOT_CHECKED)
        self.created: set[Step] = set()
        self.other_conflicts: list[Stop] = []
        self.current = Step.TABLE
        self.deadline = 0.0
        self.facts: dict[str, str] = {
            "table": table,
            "firehose": self.firehose,
            "profile": DEFAULTS.cli.aws_profile,
            "region": DEFAULTS.cli.aws_region,
            "bucket_arn": DEFAULTS.stream.bucket_arn,
            "role_arn": DEFAULTS.stream.firehose_role_arn,
            "source_path": bronze_source_path(table),
            "timeout": str(DEFAULTS.stream.wait_timeout_seconds),
        }

    def is_table_stream(self, arn: str, snapshot: Snapshot) -> bool:
        """Checks whether an ARN names the table's Kinesis stream.

        Args:
            arn: A Kinesis stream ARN.
            snapshot: The resources as last described.

        Returns:
            Whether the ARN is the table's stream, by exact ARN when the stream
            exists and by region and name otherwise.
        """
        if snapshot.stream is not None:
            return arn == snapshot.stream["StreamARN"]
        parts = arn.split(":")
        return (
            len(parts) == 6
            and parts[2] == "kinesis"
            and parts[3] == DEFAULTS.cli.aws_region
            and parts[5] == f"stream/{self.table}"
        )

    def table_destination(self, snapshot: Snapshot) -> dict[str, Any] | None:
        """Finds the table's destination for its own Kinesis stream.

        Args:
            snapshot: The resources as last described.

        Returns:
            The destination, or None if the table never streamed to it.
        """
        for destination in snapshot.destinations:
            if self.is_table_stream(destination["StreamArn"], snapshot):
                return destination
        return None

    def inspect(self) -> Snapshot:
        """Describes every resource and records whether each exists.

        Returns:
            The resources as described.

        Raises:
            Stop: If the DynamoDB table does not exist.
        """
        self.current = Step.TABLE
        try:
            run_aws("dynamodb", ["describe-table", "--table-name", self.table])
        except AwsError as error:
            if error.kind is AwsErrorKind.NOT_FOUND:
                raise Stop(
                    Outcome.TABLE_NOT_FOUND, Step.TABLE, StepStatus.MISSING
                ) from error
            raise
        self.statuses[Step.TABLE] = StepStatus.EXISTS

        self.current = Step.KINESIS
        stream = describe_kinesis_stream(self.table)
        self.current = Step.STREAMING
        destinations = describe_destinations(self.table)
        self.current = Step.FIREHOSE
        firehose = describe_delivery_stream(self.table)
        snapshot = Snapshot(stream, destinations, firehose)

        destination = self.table_destination(snapshot)
        streaming = destination is not None and destination["DestinationStatus"] in (
            DESTINATION_READY | DESTINATION_PENDING
        )
        for step, exists in (
            (Step.KINESIS, stream is not None),
            (Step.STREAMING, streaming),
            (Step.FIREHOSE, firehose is not None),
        ):
            self.statuses[step] = StepStatus.EXISTS if exists else StepStatus.MISSING
        return snapshot

    def find_conflicts(self, snapshot: Snapshot) -> list[Stop]:
        """Finds everything that blocks the setup and marks those resources.

        Args:
            snapshot: The resources as last described.

        Returns:
            One stop per blocked resource, in step order.
        """
        conflicts: list[Stop] = []
        if (
            snapshot.stream is not None
            and snapshot.stream["StreamStatus"] == "DELETING"
        ):
            conflicts.append(
                Stop(Outcome.KINESIS_STREAM_DELETING, Step.KINESIS, StepStatus.CONFLICT)
            )
        for destination in snapshot.destinations:
            arn, status = destination["StreamArn"], destination["DestinationStatus"]
            own = self.is_table_stream(arn, snapshot)
            if status in DESTINATION_BUSY:
                outcome = Outcome.TABLE_STREAMING_BUSY
            elif not own and status in DESTINATION_READY | DESTINATION_PENDING:
                outcome = Outcome.TABLE_STREAMS_ELSEWHERE
            else:
                continue
            conflicts.append(
                Stop(
                    outcome,
                    Step.STREAMING,
                    StepStatus.CONFLICT,
                    other_stream=arn,
                    other_status=status,
                )
            )
            break
        if snapshot.firehose is not None:
            status = snapshot.firehose["DeliveryStreamStatus"]
            source, buckets = firehose_wiring(snapshot.firehose)
            if status in FIREHOSE_UNUSABLE:
                conflicts.append(
                    Stop(
                        Outcome.FIREHOSE_UNUSABLE,
                        Step.FIREHOSE,
                        StepStatus.CONFLICT,
                        status=status,
                        details=firehose_failure(snapshot.firehose),
                    )
                )
            elif (
                not self.is_table_stream(source, snapshot)
                or DEFAULTS.stream.bucket_arn not in buckets
            ):
                conflicts.append(
                    Stop(
                        Outcome.FIREHOSE_WIRED_ELSEWHERE,
                        Step.FIREHOSE,
                        StepStatus.CONFLICT,
                        firehose_source=source or "no Kinesis stream",
                        firehose_bucket=", ".join(buckets) or "no bucket",
                    )
                )
        for conflict in conflicts:
            self.statuses[conflict.step] = conflict.step_status
        return conflicts

    def pending_resource(self, snapshot: Snapshot) -> tuple[Step, str] | None:
        """Finds the first resource that exists but is not active yet.

        Args:
            snapshot: The resources as last described.

        Returns:
            The resource and its status, or None if every resource is active.
        """
        destination = self.table_destination(snapshot) or {}
        checks = (
            (Step.KINESIS, (snapshot.stream or {}).get("StreamStatus"), KINESIS_READY),
            (Step.STREAMING, destination.get("DestinationStatus"), DESTINATION_READY),
            (
                Step.FIREHOSE,
                (snapshot.firehose or {}).get("DeliveryStreamStatus"),
                FIREHOSE_READY,
            ),
        )
        for step, status, ready in checks:
            if status not in ready:
                return step, str(status or "MISSING")
        return None

    def wait(
        self,
        step: Step,
        describe: Callable[[], tuple[str, str]],
        ready: frozenset[str],
        pending: frozenset[str],
        failed: Outcome,
    ) -> None:
        """Waits until a resource is active, within the run's time limit.

        Args:
            step: The resource.
            describe: Returns the resource's status and any failure details.
            ready: Statuses that mean the resource is active.
            pending: Statuses that mean it is still being set up.
            failed: Outcome if it reaches any other status.

        Raises:
            Stop: If the resource fails or the run's time limit passes.
        """
        while True:
            status, details = describe()
            if status in ready:
                return
            if status not in pending:
                raise Stop(
                    failed, step, StepStatus.FAILED, status=status, details=details
                )
            if time.monotonic() >= self.deadline:
                raise Stop(
                    Outcome.TIMEOUT,
                    step,
                    StepStatus.PENDING,
                    resource=self.labels[step],
                    status=status,
                )
            time.sleep(DEFAULTS.stream.poll_interval_seconds)

    def kinesis_status(self) -> tuple[str, str]:
        """Reads the Kinesis stream's status.

        Returns:
            The status and no details.
        """
        stream = describe_kinesis_stream(self.table)
        return (stream or {}).get("StreamStatus", "MISSING"), ""

    def streaming_status(self) -> tuple[str, str]:
        """Reads the status of the table's streaming to its Kinesis stream.

        Returns:
            The status and its description.
        """
        snapshot = Snapshot(
            describe_kinesis_stream(self.table), describe_destinations(self.table), None
        )
        destination = self.table_destination(snapshot) or {}
        return (
            destination.get("DestinationStatus", "MISSING"),
            destination.get("DestinationStatusDescription", ""),
        )

    def firehose_status(self) -> tuple[str, str]:
        """Reads the Firehose stream's status.

        Returns:
            The status and any failure details.
        """
        description = describe_delivery_stream(self.table) or {}
        return (
            description.get("DeliveryStreamStatus", "MISSING"),
            firehose_failure(description),
        )

    def create_missing(self) -> None:
        """Creates each missing resource in order and waits for each.

        Raises:
            Stop: If a resource fails or the run's time limit passes.
        """
        self.deadline = time.monotonic() + DEFAULTS.stream.wait_timeout_seconds

        self.current = Step.KINESIS
        if self.statuses[Step.KINESIS] is StepStatus.MISSING:
            shards = str(DEFAULTS.stream.shard_count)
            run_aws(
                "kinesis",
                ["create-stream", "--stream-name", self.table, "--shard-count", shards],
            )
            self.statuses[Step.KINESIS] = StepStatus.CREATED
            self.created.add(Step.KINESIS)
        self.wait(
            Step.KINESIS,
            self.kinesis_status,
            KINESIS_READY,
            KINESIS_PENDING,
            Outcome.KINESIS_STREAM_DELETING,
        )
        stream_arn = (describe_kinesis_stream(self.table) or {})["StreamARN"]

        self.current = Step.STREAMING
        if self.statuses[Step.STREAMING] is StepStatus.MISSING:
            run_aws(
                "dynamodb",
                [
                    "enable-kinesis-streaming-destination",
                    "--table-name",
                    self.table,
                    "--stream-arn",
                    stream_arn,
                ],
            )
            self.statuses[Step.STREAMING] = StepStatus.CREATED
            self.created.add(Step.STREAMING)
        self.wait(
            Step.STREAMING,
            self.streaming_status,
            DESTINATION_READY,
            DESTINATION_PENDING,
            Outcome.STREAMING_FAILED,
        )

        self.current = Step.FIREHOSE
        if self.statuses[Step.FIREHOSE] is StepStatus.MISSING:
            config = json.dumps(build_delivery_stream_config(self.table, stream_arn))
            try:
                run_aws(
                    "firehose", ["create-delivery-stream", "--cli-input-json", config]
                )
            except AwsError as error:
                if error.kind is AwsErrorKind.INVALID_ARGUMENT:
                    raise Stop(
                        Outcome.FIREHOSE_CONFIG_REJECTED,
                        Step.FIREHOSE,
                        StepStatus.FAILED,
                        aws_message=error.aws_message,
                    ) from error
                raise
            self.statuses[Step.FIREHOSE] = StepStatus.CREATED
            self.created.add(Step.FIREHOSE)
        self.wait(
            Step.FIREHOSE,
            self.firehose_status,
            FIREHOSE_READY,
            FIREHOSE_PENDING,
            Outcome.FIREHOSE_FAILED,
        )

    def run(self) -> Outcome:
        """Checks every resource, then creates what is missing unless checking.

        Returns:
            The successful outcome.

        Raises:
            Stop: If the run ends with any other outcome.
        """
        problem = table_name_problem(self.table)
        if problem is not None:
            raise Stop(
                Outcome.INVALID_TABLE_NAME,
                Step.TABLE,
                StepStatus.FAILED,
                reason=problem,
            )
        snapshot = self.inspect()
        conflicts = self.find_conflicts(snapshot)
        if conflicts:
            self.other_conflicts = conflicts[1:]
            raise conflicts[0]

        missing = [step for step in Step if self.statuses[step] is StepStatus.MISSING]
        if self.check_only:
            for step in missing:
                self.statuses[step] = StepStatus.WOULD_CREATE
            if missing:
                self.facts["missing"] = ", ".join(self.labels[step] for step in missing)
                return Outcome.WOULD_CREATE
            pending = self.pending_resource(snapshot)
            if pending is not None:
                self.facts["resource"] = self.labels[pending[0]]
                self.facts["status"] = pending[1]
                return Outcome.IN_PROGRESS
            return Outcome.READY

        self.create_missing()
        snapshot = self.inspect()
        for step in self.created:
            self.statuses[step] = StepStatus.CREATED
        pending = self.pending_resource(snapshot)
        if self.find_conflicts(snapshot) or pending is not None:
            step, status = pending or (Step.FIREHOSE, "connected elsewhere")
            raise Stop(
                Outcome.NOT_CONFIRMED,
                step,
                StepStatus.FAILED,
                resource=self.labels[step],
                status=status,
            )
        return Outcome.CREATED if self.created else Outcome.ALREADY_EXISTED

    def execute(self) -> RunReport:
        """Runs and builds the report, whatever the outcome.

        Returns:
            The report.
        """
        try:
            code = self.run()
            message, next_step = OUTCOME_TEXT[code]
        except Stop as stop:
            self.statuses[stop.step] = stop.step_status
            self.facts |= stop.facts
            code = stop.outcome
            message, next_step = OUTCOME_TEXT[code]
            for other in self.other_conflicts:
                other_message = OUTCOME_TEXT[other.outcome][0]
                message += " Also blocking: " + other_message.format(
                    **(self.facts | other.facts)
                ).replace("{", "{{").replace("}", "}}")
        except AwsError as error:
            self.statuses[self.current] = StepStatus.FAILED
            return self.report(
                error.kind.value, False, error.message(), error.next_step()
            )
        return self.report(
            code,
            code in SUCCESS_OUTCOMES,
            message.format(**self.facts),
            next_step.format(**self.facts),
        )

    def report(self, code: str, ok: bool, message: str, next_step: str) -> RunReport:
        """Builds the report.

        Args:
            code: Result code.
            ok: Whether the run succeeded.
            message: What happened.
            next_step: What the user should do next.

        Returns:
            The report.
        """
        lines = [
            f"{number}. {self.labels[step]}: {self.statuses[step]}"
            for number, step in enumerate(Step, start=1)
        ]
        created = [self.labels[step] for step in Step if step in self.created]
        lines.append(f"Created: {', '.join(created) if created else 'nothing'}")
        title = f"create-stream {self.table}{' --check' if self.check_only else ''}"
        result = format_result(code, " ".join(message.split()), next_step)
        return RunReport(code, ok, format_report(title, lines, result))


def create_stream(table: str, check_only: bool = False) -> RunReport:
    """Checks the table's streams and creates the missing ones unless checking.

    Args:
        table: DynamoDB table name.
        check_only: Whether to only check, creating nothing.

    Returns:
        The report of the run.
    """
    return CreateStreamRun(table, check_only).execute()

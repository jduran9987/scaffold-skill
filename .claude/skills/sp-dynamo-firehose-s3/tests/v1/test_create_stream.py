import json
import subprocess
from pathlib import Path

import pytest

from sp_dynamo_firehose_s3.v1 import aws, main, streaming
from sp_dynamo_firehose_s3.v1.aws import AwsErrorKind
from sp_dynamo_firehose_s3.v1.streaming import (
    OUTCOME_TEXT,
    Outcome,
    bronze_source_path,
    build_delivery_stream_config,
    create_stream,
)

FIXTURES = Path(__file__).parent / "fixtures"
TABLE = "orders"
STREAM_ARN = "arn:aws:kinesis:us-east-1:123456789012:stream/orders"
OTHER_ARN = "arn:aws:kinesis:us-east-1:123456789012:stream/other"
BUCKET_ARN = "arn:aws:s3:::dev-drivewealth-kinesis"
NOT_FOUND = "An error occurred (ResourceNotFoundException): not found"
CREATES = {"create-stream", "enable-kinesis-streaming-destination"} | {
    "create-delivery-stream"
}


class FakeAws:
    """A stateful AWS CLI. Each resource reports its statuses in turn, then stays
    on the last one. `errors` maps an operation to the stderr it fails with."""

    def __init__(self):
        self.calls = []
        self.tables = {TABLE}
        self.stream = None
        self.destinations = {}
        self.firehose = None
        self.firehose_config = None
        self.pending_polls = 1
        self.final = {"stream": "ACTIVE", "destination": "ACTIVE", "firehose": "ACTIVE"}
        self.errors = {}

    @staticmethod
    def next_status(statuses):
        return statuses.pop(0) if len(statuses) > 1 else statuses[0]

    def run(self, command, **kwargs):
        service, operation = command[1], command[2]
        self.calls.append(operation)
        if operation in self.errors:
            return subprocess.CompletedProcess(
                command, 254, stderr=self.errors[operation]
            )
        options = dict(zip(command[3::2], command[4::2], strict=False))
        handler = getattr(self, f"{service}_{operation.replace('-', '_')}")
        try:
            output = handler(options)
        except LookupError:
            return subprocess.CompletedProcess(command, 254, stderr=NOT_FOUND)
        stdout = "" if output is None else json.dumps(output)
        return subprocess.CompletedProcess(command, 0, stdout=stdout)

    def dynamodb_describe_table(self, options):
        if options["--table-name"] not in self.tables:
            raise LookupError
        return {"Table": {"TableName": options["--table-name"]}}

    def kinesis_describe_stream_summary(self, options):
        if self.stream is None:
            raise LookupError
        return {
            "StreamDescriptionSummary": {
                "StreamARN": STREAM_ARN,
                "StreamStatus": self.next_status(self.stream),
            }
        }

    def kinesis_create_stream(self, options):
        assert options["--shard-count"] == "3"
        self.stream = ["CREATING"] * self.pending_polls + [self.final["stream"]]

    def dynamodb_describe_kinesis_streaming_destination(self, options):
        return {
            "KinesisDataStreamDestinations": [
                {
                    "StreamArn": arn,
                    "DestinationStatus": self.next_status(statuses),
                    "DestinationStatusDescription": "described",
                }
                for arn, statuses in self.destinations.items()
            ]
        }

    def dynamodb_enable_kinesis_streaming_destination(self, options):
        assert options["--stream-arn"] == STREAM_ARN
        final = self.final["destination"]
        self.destinations[STREAM_ARN] = ["ENABLING"] * self.pending_polls + [final]

    def firehose_describe_delivery_stream(self, options):
        assert options["--delivery-stream-name"] == "orders-firehose"
        if self.firehose is None:
            raise LookupError
        source_arn, bucket_arn, statuses = self.firehose
        return {
            "DeliveryStreamDescription": {
                "DeliveryStreamStatus": self.next_status(statuses),
                "FailureDescription": {"Details": "role cannot be assumed"},
                "Source": {
                    "KinesisStreamSourceDescription": {"KinesisStreamARN": source_arn}
                },
                "Destinations": [
                    {"ExtendedS3DestinationDescription": {"BucketARN": bucket_arn}}
                ],
            }
        }

    def firehose_create_delivery_stream(self, options):
        self.firehose_config = json.loads(options["--cli-input-json"])
        statuses = ["CREATING"] * self.pending_polls + [self.final["firehose"]]
        self.firehose = (STREAM_ARN, BUCKET_ARN, statuses)
        return {"DeliveryStreamARN": "arn:firehose"}


@pytest.fixture
def fake(monkeypatch):
    fake = FakeAws()
    monkeypatch.setattr(aws, "subprocess", fake)
    monkeypatch.setattr(streaming.time, "sleep", lambda seconds: None)
    return fake


def existing(fake):
    fake.stream = ["ACTIVE"]
    fake.destinations = {STREAM_ARN: ["ACTIVE"]}
    fake.firehose = (STREAM_ARN, BUCKET_ARN, ["ACTIVE"])


def creates(fake):
    return [call for call in fake.calls if call in CREATES]


def test_every_outcome_has_text():
    assert set(OUTCOME_TEXT) == set(Outcome)


def test_firehose_config_matches_the_reference_config():
    expected = json.loads((FIXTURES / "expected" / "firehose.json").read_text())

    assert build_delivery_stream_config(TABLE, STREAM_ARN) == expected


def test_full_report_when_everything_is_created(fake):
    report = create_stream(TABLE)

    assert report.ok
    assert creates(fake) == sorted(
        CREATES,
        key=[
            "create-stream",
            "enable-kinesis-streaming-destination",
            "create-delivery-stream",
        ].index,
    )
    assert fake.firehose_config == build_delivery_stream_config(TABLE, STREAM_ARN)
    assert report.text == (
        "create-stream orders\n"
        "  1. DynamoDB table orders: exists\n"
        "  2. Kinesis stream orders: created\n"
        "  3. Streaming from table orders to Kinesis stream orders: created\n"
        "  4. Firehose stream orders-firehose: created\n"
        "  Created: Kinesis stream orders, Streaming from table orders to Kinesis "
        "stream orders, Firehose stream orders-firehose\n"
        "Result: CREATED\n"
        "  Every resource is active and connected. DynamoDB table orders streams "
        "to Kinesis stream orders, which Firehose stream orders-firehose delivers "
        f"to {BUCKET_ARN}.\n"
        "Next step: Set job.bronze.sourcePath to "
        "s3://dev-drivewealth-kinesis/firehose-stream/orders/.\n"
    )


def test_full_report_when_the_table_streams_elsewhere(fake):
    fake.destinations = {OTHER_ARN: ["ACTIVE"]}
    fake.firehose = (OTHER_ARN, "arn:aws:s3:::other", ["ACTIVE"])

    report = create_stream(TABLE)

    assert not report.ok
    assert creates(fake) == []
    assert report.text == (
        "create-stream orders\n"
        "  1. DynamoDB table orders: exists\n"
        "  2. Kinesis stream orders: missing\n"
        "  3. Streaming from table orders to Kinesis stream orders: conflict\n"
        "  4. Firehose stream orders-firehose: conflict\n"
        "  Created: nothing\n"
        "Result: TABLE_STREAMS_ELSEWHERE\n"
        f"  DynamoDB table orders already streams to {OTHER_ARN} (status ACTIVE). "
        "DynamoDB cannot stream the table to Kinesis stream orders while that is "
        "enabled. Also blocking: Firehose stream orders-firehose already exists "
        f"but reads {OTHER_ARN} and writes to arn:aws:s3:::other, instead of "
        f"reading Kinesis stream orders and writing to {BUCKET_ARN}.\n"
        "Next step: Keep the existing setup, or disable the table's streaming to "
        f"{OTHER_ARN} yourself, then rerun.\n"
    )


def test_existing_resources_are_skipped(fake):
    existing(fake)

    report = create_stream(TABLE)

    assert report.code == Outcome.ALREADY_EXISTED
    assert creates(fake) == []
    assert "Created: nothing" in report.text


def test_resumes_after_the_resources_that_exist(fake):
    fake.stream = ["ACTIVE"]

    report = create_stream(TABLE)

    assert report.code == Outcome.CREATED
    assert creates(fake) == [
        "enable-kinesis-streaming-destination",
        "create-delivery-stream",
    ]
    assert "2. Kinesis stream orders: exists" in report.text


def test_disabled_streaming_is_enabled_again(fake):
    fake.stream = ["ACTIVE"]
    fake.destinations = {STREAM_ARN: ["DISABLED"]}

    assert create_stream(TABLE).code == Outcome.CREATED
    assert "enable-kinesis-streaming-destination" in fake.calls


def test_disabled_streaming_to_another_stream_does_not_block(fake):
    fake.destinations = {OTHER_ARN: ["DISABLED"]}

    assert create_stream(TABLE).code == Outcome.CREATED


@pytest.mark.parametrize(
    ("setup", "code"),
    [
        (lambda f: setattr(f, "tables", set()), Outcome.TABLE_NOT_FOUND),
        (
            lambda f: setattr(f, "destinations", {OTHER_ARN: ["ENABLING"]}),
            Outcome.TABLE_STREAMS_ELSEWHERE,
        ),
        (
            lambda f: setattr(f, "destinations", {STREAM_ARN: ["DISABLING"]}),
            Outcome.TABLE_STREAMING_BUSY,
        ),
        (lambda f: setattr(f, "stream", ["DELETING"]), Outcome.KINESIS_STREAM_DELETING),
        (
            lambda f: setattr(
                f, "firehose", (STREAM_ARN, "arn:aws:s3:::x", ["ACTIVE"])
            ),
            Outcome.FIREHOSE_WIRED_ELSEWHERE,
        ),
        (
            lambda f: setattr(f, "firehose", (OTHER_ARN, BUCKET_ARN, ["ACTIVE"])),
            Outcome.FIREHOSE_WIRED_ELSEWHERE,
        ),
        (
            lambda f: setattr(
                f, "firehose", (STREAM_ARN, BUCKET_ARN, ["CREATING_FAILED"])
            ),
            Outcome.FIREHOSE_UNUSABLE,
        ),
    ],
)
def test_blocking_states_stop_before_creating_anything(fake, setup, code):
    setup(fake)

    report = create_stream(TABLE)

    assert report.code == code
    assert not report.ok
    assert creates(fake) == []
    assert "Created: nothing" in report.text


@pytest.mark.parametrize(
    ("resource", "final", "code"),
    [
        ("stream", "DELETING", Outcome.KINESIS_STREAM_DELETING),
        ("destination", "ENABLE_FAILED", Outcome.STREAMING_FAILED),
        ("firehose", "CREATING_FAILED", Outcome.FIREHOSE_FAILED),
    ],
)
def test_resources_that_fail_while_being_created(fake, resource, final, code):
    fake.final[resource] = final

    report = create_stream(TABLE)

    assert report.code == code
    assert final in report.text


def test_firehose_details_are_reported(fake):
    fake.final["firehose"] = "CREATING_FAILED"

    assert "role cannot be assumed" in create_stream(TABLE).text


def test_rejected_firehose_settings_point_to_the_defaults(fake):
    fake.errors["create-delivery-stream"] = (
        "An error occurred (InvalidArgumentException): Role is not valid"
    )

    report = create_stream(TABLE)

    assert report.code == Outcome.FIREHOSE_CONFIG_REJECTED
    assert "Role is not valid" in report.text
    assert "firehose_role_arn (arn:aws:iam::123456789012:role/" in report.text
    assert "4. Firehose stream orders-firehose: failed" in report.text
    assert "Created: Kinesis stream orders, Streaming" in report.text


def test_waits_for_resources_that_are_still_being_created(fake):
    fake.pending_polls = 3

    assert create_stream(TABLE).code == Outcome.CREATED
    assert fake.calls.count("describe-stream-summary") >= 4


def test_times_out_and_suggests_a_rerun(fake, monkeypatch):
    fake.stream = ["CREATING"]
    clock = iter(range(0, 10_000, 100))
    monkeypatch.setattr(streaming.time, "monotonic", lambda: next(clock))

    report = create_stream(TABLE)

    assert report.code == Outcome.TIMEOUT
    assert "2. Kinesis stream orders: still being created" in report.text
    assert "was still CREATING after waiting 540 seconds" in report.text
    assert "Rerun create-stream orders" in report.text


@pytest.mark.parametrize(
    ("stderr", "code"),
    [
        (
            "Error loading SSO Token: Token for default does not exist",
            "AWS_CREDENTIALS",
        ),
        ("Unable to locate credentials.", "AWS_CREDENTIALS"),
        ("The config profile (default) could not be found", "AWS_CREDENTIALS"),
        ("An error occurred (ExpiredTokenException): expired", "AWS_CREDENTIALS"),
        ("An error occurred (AccessDeniedException): no", "AWS_ACCESS_DENIED"),
        ("An error occurred (LimitExceededException): shards", "AWS_ERROR"),
    ],
)
def test_aws_failures_have_fixed_codes(fake, stderr, code):
    fake.errors["create-stream"] = stderr

    report = create_stream(TABLE)

    assert report.code == code
    assert "2. Kinesis stream orders: failed" in report.text
    assert f"Result: {code}\n" in report.text


def test_expired_credentials_say_how_to_sign_in(fake):
    fake.errors["describe-table"] = "Unable to locate credentials."

    report = create_stream(TABLE)

    assert report.code == AwsErrorKind.CREDENTIALS
    assert "1. DynamoDB table orders: failed" in report.text
    assert "Run `aws sso login --profile default` yourself" in report.text


def test_missing_aws_cli(fake, monkeypatch):
    def missing(command, **kwargs):
        raise FileNotFoundError("aws")

    monkeypatch.setattr(fake, "run", missing)

    report = create_stream(TABLE)

    assert report.code == "AWS_CLI_MISSING"
    assert "The AWS CLI is not installed or not on PATH." in report.text


def test_check_reports_what_would_be_created(fake):
    report = create_stream(TABLE, check_only=True)

    assert report.code == Outcome.WOULD_CREATE
    assert creates(fake) == []
    assert "2. Kinesis stream orders: would be created" in report.text
    assert report.text.startswith("create-stream orders --check\n")


def test_check_reports_ready(fake):
    existing(fake)

    report = create_stream(TABLE, check_only=True)

    assert report.code == Outcome.READY
    assert "Set job.bronze.sourcePath to" in report.text


def test_check_reports_resources_still_being_created(fake):
    existing(fake)
    fake.firehose = (STREAM_ARN, BUCKET_ARN, ["CREATING"])

    report = create_stream(TABLE, check_only=True)

    assert report.code == Outcome.IN_PROGRESS
    assert "Firehose stream orders-firehose is still CREATING" in report.text


def test_check_reports_conflicts(fake):
    fake.destinations = {OTHER_ARN: ["ACTIVE"]}

    assert create_stream(TABLE, check_only=True).code == Outcome.TABLE_STREAMS_ELSEWHERE


@pytest.mark.parametrize("table", ["ab", "a b c", "t" * 56])
def test_invalid_table_names_are_rejected_before_any_aws_call(fake, table):
    report = create_stream(table)

    assert report.code == Outcome.INVALID_TABLE_NAME
    assert fake.calls == []


def test_dots_in_the_table_name_become_dashes_in_the_source_path():
    assert bronze_source_path("my.orders").endswith("/firehose-stream/my-orders/")


def test_action_prints_the_report_to_stdout(fake, capsys):
    assert main(["create-stream", TABLE, "--check"]) == 0
    output = capsys.readouterr()
    assert output.out.endswith("Next step: Run create-stream orders to create them.\n")
    assert output.err == ""

    fake.tables = set()
    assert main(["create-stream", TABLE]) == 1
    assert "Result: TABLE_NOT_FOUND\n" in capsys.readouterr().out

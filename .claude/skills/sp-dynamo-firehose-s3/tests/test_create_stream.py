import json
import subprocess

import pytest

from sp_dynamo_firehose_s3.create_stream.main import run
from sp_dynamo_firehose_s3.create_stream.report import Code

STREAM_ARN = "arn:aws:kinesis:us-east-1:123456789012:stream/orders-stream"
OTHER_ARN = "arn:aws:kinesis:us-east-1:123456789012:stream/other"
NOT_FOUND = "An error occurred (ResourceNotFoundException): not found"

LINES_ALL_EXIST = (
    "  1. DynamoDB table orders: exists\n"
    "  2. Kinesis stream orders-stream: exists\n"
    "  3. Streaming from table orders to Kinesis stream orders-stream: exists\n"
    "  4. Firehose stream orders-firehose: exists\n"
)
CONNECTED = (
    "DynamoDB table orders streams to Kinesis stream orders-stream, which Firehose "
    "stream orders-firehose delivers to arn:aws:s3:::dev-datalake-7472."
)


def table():
    return {"Table": {"TableName": "orders"}}


def kinesis(status):
    return {
        "StreamDescriptionSummary": {"StreamStatus": status, "StreamARN": STREAM_ARN}
    }


def destinations(*pairs):
    return {
        "KinesisDataStreamDestinations": [
            {
                "StreamArn": arn,
                "DestinationStatus": status,
                "DestinationStatusDescription": description,
            }
            for arn, status, description in pairs
        ]
    }


def firehose(status, details=""):
    description = {"DeliveryStreamStatus": status}
    if details:
        description["FailureDescription"] = {"Details": details}
    return {"DeliveryStreamDescription": description}


@pytest.fixture
def all_exist(fake_aws):
    fake_aws.respond("dynamodb", "describe-table", table())
    fake_aws.respond("kinesis", "describe-stream-summary", kinesis("ACTIVE"))
    fake_aws.respond(
        "dynamodb",
        "describe-kinesis-streaming-destination",
        destinations((STREAM_ARN, "ACTIVE", "")),
    )
    fake_aws.respond("firehose", "describe-delivery-stream", firehose("ACTIVE"))
    return fake_aws


@pytest.fixture
def none_exist(fake_aws):
    fake_aws.respond("dynamodb", "describe-table", table())
    fake_aws.respond(
        "kinesis",
        "describe-stream-summary",
        NOT_FOUND,
        kinesis("CREATING"),
        kinesis("ACTIVE"),
    )
    fake_aws.respond("kinesis", "create-stream", {})
    fake_aws.respond(
        "dynamodb",
        "describe-kinesis-streaming-destination",
        destinations(),  # the conflict check
        destinations(),  # the existence check
        destinations((STREAM_ARN, "ENABLING", "")),
        destinations((STREAM_ARN, "ACTIVE", "")),
    )
    fake_aws.respond(
        "dynamodb",
        "enable-kinesis-streaming-destination",
        {"DestinationStatus": "ENABLING"},
    )
    fake_aws.respond(
        "firehose",
        "describe-delivery-stream",
        NOT_FOUND,
        firehose("CREATING"),
        firehose("ACTIVE"),
    )
    fake_aws.respond("firehose", "create-delivery-stream", {"DeliveryStreamARN": "x"})
    return fake_aws


# Before any AWS call


def test_no_argument(fake_aws):
    report = run([])
    assert report.code is Code.USAGE
    assert report.text == (
        "create-stream\n"
        "Result: USAGE\n"
        "  create-stream takes exactly one argument, the DynamoDB table name, "
        "but got 0.\n"
        "Next step: Rerun the skill.\n"
    )
    assert fake_aws.calls == []


def test_two_arguments():
    report = run(["orders", "extra"])
    assert report.code is Code.USAGE
    assert report.text.startswith("create-stream orders extra\nResult: USAGE\n")


def test_invalid_table_name(fake_aws):
    report = run(["it's orders"])
    assert report.code is Code.CONFIG_INVALID
    assert report.text == (
        "create-stream it's orders\n"
        "Result: CONFIG_INVALID\n"
        "  The table name or inputs.toml is invalid: dynamo_table_name: String "
        "should match pattern '^[A-Za-z0-9_.-]+$'.\n"
        "Next step: Fix what is listed, then rerun the skill.\n"
    )
    assert fake_aws.calls == []


def test_invalid_inputs(tmp_path, monkeypatch, test_inputs):
    path = tmp_path / "inputs.toml"
    path.write_text(
        test_inputs.read_text().replace("shard_count = 3", "shard_count = 0")
    )
    monkeypatch.setattr("sp_dynamo_firehose_s3.create_stream.config.INPUTS_PATH", path)
    report = run(["orders"])
    assert report.code is Code.CONFIG_INVALID
    assert "kinesis.shard_count: Input should be greater than or equal to 1" in (
        report.text
    )


# Success


def test_already_existed(all_exist):
    report = run(["orders"])
    assert report.code is Code.ALREADY_EXISTED
    assert report.ok
    assert report.text == (
        "create-stream orders\n"
        f"{LINES_ALL_EXIST}"
        "Result: ALREADY_EXISTED\n"
        f"  Every resource already existed and is active. {CONNECTED}\n"
        "Next step: None.\n"
    )
    assert all_exist.operations() == [
        "dynamodb describe-table",
        "dynamodb describe-kinesis-streaming-destination",
        "kinesis describe-stream-summary",
        "kinesis describe-stream-summary",
        "kinesis describe-stream-summary",
        "dynamodb describe-kinesis-streaming-destination",
        "dynamodb describe-kinesis-streaming-destination",
        "firehose describe-delivery-stream",
        "firehose describe-delivery-stream",
    ]


def test_created(none_exist, clock):
    report = run(["orders"])
    assert report.code is Code.CREATED
    assert report.ok
    assert report.text == (
        "create-stream orders\n"
        "  1. DynamoDB table orders: exists\n"
        "  2. Kinesis stream orders-stream: created\n"
        "  3. Streaming from table orders to Kinesis stream orders-stream: created\n"
        "  4. Firehose stream orders-firehose: created\n"
        "Result: CREATED\n"
        f"  Every resource is active. {CONNECTED}\n"
        "Next step: None.\n"
    )
    assert [op for op in none_exist.operations() if "describe" not in op] == [
        "kinesis create-stream",
        "dynamodb enable-kinesis-streaming-destination",
        "firehose create-delivery-stream",
    ]
    assert clock.sleeps == [10, 10, 10]


def test_create_commands(none_exist):
    run(["orders"])
    assert none_exist.argument("kinesis create-stream", "--stream-name") == (
        "orders-stream"
    )
    assert none_exist.argument("kinesis create-stream", "--shard-count") == "3"
    assert (
        none_exist.argument(
            "dynamodb enable-kinesis-streaming-destination", "--stream-arn"
        )
        == STREAM_ARN
    )
    settings = json.loads(
        none_exist.argument("firehose create-delivery-stream", "--cli-input-json")
    )
    assert settings["DeliveryStreamName"] == "orders-firehose"
    assert settings["KinesisStreamSourceConfiguration"]["KinesisStreamARN"] == (
        STREAM_ARN
    )
    destination = settings["ExtendedS3DestinationConfiguration"]
    assert destination["BucketARN"] == "arn:aws:s3:::dev-datalake-7472"
    assert destination["Prefix"] == (
        "firehose-stream/!{partitionKeyFromQuery:tableName}/!{timestamp:yyyy/MM/dd/HH}/"
    )
    assert destination["ErrorOutputPrefix"] == (
        "firehose-stream/failures/orders/!{firehose:error-output-type}/"
        "!{timestamp:yyyy/MM/dd/HH}/"
    )
    assert destination["CompressionFormat"] == "GZIP"
    assert destination["BufferingHints"] == {"SizeInMBs": 128, "IntervalInSeconds": 300}


def test_table_streams_to_another_stream(all_exist):
    all_exist.respond(
        "dynamodb",
        "describe-kinesis-streaming-destination",
        destinations((OTHER_ARN, "ACTIVE", "")),
    )
    report = run(["orders"])
    assert report.code is Code.TABLE_STREAMS_ELSEWHERE
    assert not report.ok
    assert report.text == (
        "create-stream orders\n"
        "  1. DynamoDB table orders: exists\n"
        "  2. Kinesis stream orders-stream: not checked\n"
        "  3. Streaming from table orders to Kinesis stream orders-stream: "
        "conflict\n"
        "  4. Firehose stream orders-firehose: not checked\n"
        "Result: TABLE_STREAMS_ELSEWHERE\n"
        f"  DynamoDB table orders already streams to {OTHER_ARN} (status ACTIVE). "
        "A table can stream to only one Kinesis stream, and create-stream uses "
        "Kinesis stream orders-stream. Nothing was created.\n"
        f"Next step: Disable the table's streaming to {OTHER_ARN} yourself, then "
        "rerun the skill.\n"
    )
    assert all_exist.operations() == [
        "dynamodb describe-table",
        "dynamodb describe-kinesis-streaming-destination",
    ]


@pytest.mark.parametrize("status", ["ENABLING", "UPDATING", "DISABLING"])
def test_other_stream_in_any_active_state_is_a_conflict(all_exist, status):
    all_exist.respond(
        "dynamodb",
        "describe-kinesis-streaming-destination",
        destinations((OTHER_ARN, status, "")),
    )
    assert run(["orders"]).code is Code.TABLE_STREAMS_ELSEWHERE


@pytest.mark.parametrize("status", ["DISABLED", "ENABLE_FAILED"])
def test_other_stream_that_is_off_is_not_a_conflict(all_exist, status):
    all_exist.respond(
        "dynamodb",
        "describe-kinesis-streaming-destination",
        destinations((OTHER_ARN, status, "")),  # the conflict check
        destinations((OTHER_ARN, status, "")),  # the existence check
        destinations((OTHER_ARN, status, ""), (STREAM_ARN, "ACTIVE", "")),
    )
    all_exist.respond(
        "dynamodb",
        "enable-kinesis-streaming-destination",
        {"DestinationStatus": "ENABLING"},
    )
    report = run(["orders"])
    assert report.code is Code.CREATED
    assert (
        "3. Streaming from table orders to Kinesis stream orders-stream: created"
        in report.text
    )


def test_disabled_streaming_is_enabled_again(all_exist):
    all_exist.respond(
        "dynamodb",
        "describe-kinesis-streaming-destination",
        destinations((STREAM_ARN, "DISABLED", "")),  # the conflict check
        destinations((STREAM_ARN, "DISABLED", "")),  # the existence check
        destinations((STREAM_ARN, "ACTIVE", "")),
    )
    all_exist.respond(
        "dynamodb",
        "enable-kinesis-streaming-destination",
        {"DestinationStatus": "ENABLING"},
    )
    assert run(["orders"]).code is Code.CREATED


def test_existing_resource_still_being_created_is_waited_for(all_exist, clock):
    all_exist.respond(
        "kinesis",
        "describe-stream-summary",
        kinesis("CREATING"),  # the existence check
        kinesis("CREATING"),  # the first wait read
        kinesis("ACTIVE"),
    )
    report = run(["orders"])
    assert report.code is Code.ALREADY_EXISTED
    assert clock.sleeps == [10]


def test_same_state_gives_the_same_report(all_exist):
    assert run(["orders"]).text == run(["orders"]).text


# Failures


def test_table_not_found(fake_aws):
    fake_aws.respond("dynamodb", "describe-table", NOT_FOUND)
    report = run(["orders"])
    assert report.code is Code.TABLE_NOT_FOUND
    assert not report.ok
    assert report.text == (
        "create-stream orders\n"
        "  1. DynamoDB table orders: missing\n"
        "  2. Kinesis stream orders-stream: not checked\n"
        "  3. Streaming from table orders to Kinesis stream orders-stream: "
        "not checked\n"
        "  4. Firehose stream orders-firehose: not checked\n"
        "Result: TABLE_NOT_FOUND\n"
        "  DynamoDB table orders does not exist in us-east-1 for AWS CLI profile "
        "dev-main.\n"
        "Next step: Check the table name, or aws.profile and aws.region in "
        "inputs.toml, then rerun the skill.\n"
    )
    assert fake_aws.operations() == ["dynamodb describe-table"]


def test_expired_credentials(fake_aws):
    fake_aws.respond("dynamodb", "describe-table", "Error loading SSO Token: expired")
    report = run(["orders"])
    assert report.code is Code.AWS_CREDENTIALS
    assert report.text == (
        "create-stream orders\n"
        "  1. DynamoDB table orders: failed\n"
        "  2. Kinesis stream orders-stream: not checked\n"
        "  3. Streaming from table orders to Kinesis stream orders-stream: "
        "not checked\n"
        "  4. Firehose stream orders-firehose: not checked\n"
        "Result: AWS_CREDENTIALS\n"
        "  AWS CLI profile dev-main has no valid credentials, or its session "
        "expired. AWS said: Error loading SSO Token: expired\n"
        "Next step: Run `aws sso login --profile dev-main` yourself, then rerun "
        "the skill.\n"
    )


def test_cli_missing(fake_aws):
    fake_aws.respond("dynamodb", "describe-table", FileNotFoundError("aws"))
    assert run(["orders"]).code is Code.AWS_CLI_MISSING


def test_access_denied_while_creating(none_exist):
    none_exist.respond(
        "kinesis", "create-stream", "User is not authorized to perform: CreateStream"
    )
    report = run(["orders"])
    assert report.code is Code.AWS_ACCESS_DENIED
    assert "  2. Kinesis stream orders-stream: failed\n" in report.text
    assert "not allowed to run aws kinesis create-stream" in report.text


def test_aws_rejects_enabling_streaming(none_exist):
    none_exist.respond(
        "dynamodb",
        "enable-kinesis-streaming-destination",
        "An error occurred (ValidationException): Table already has a destination",
    )
    report = run(["orders"])
    assert report.code is Code.AWS_ERROR
    assert "  2. Kinesis stream orders-stream: created\n" in report.text
    assert (
        "  3. Streaming from table orders to Kinesis stream orders-stream: failed\n"
        in report.text
    )
    assert "  4. Firehose stream orders-firehose: not checked\n" in report.text
    assert (
        "aws dynamodb enable-kinesis-streaming-destination failed. AWS said: An "
        "error occurred (ValidationException): Table already has a destination"
        in report.text
    )


def test_kinesis_stream_being_deleted(all_exist):
    all_exist.respond("kinesis", "describe-stream-summary", kinesis("DELETING"))
    report = run(["orders"])
    assert report.code is Code.KINESIS_FAILED
    assert "  2. Kinesis stream orders-stream: failed\n" in report.text
    assert (
        "Kinesis stream orders-stream has status DELETING, so it cannot be used."
        in (report.text)
    )


def test_streaming_fails(all_exist):
    all_exist.respond(
        "dynamodb",
        "describe-kinesis-streaming-destination",
        destinations(),  # the conflict check
        destinations(),  # the existence check
        destinations((STREAM_ARN, "ENABLE_FAILED", "Stream is not active")),
    )
    all_exist.respond(
        "dynamodb",
        "enable-kinesis-streaming-destination",
        {"DestinationStatus": "ENABLING"},
    )
    report = run(["orders"])
    assert report.code is Code.STREAMING_FAILED
    assert (
        "  Streaming from DynamoDB table orders to Kinesis stream orders-stream "
        "has status ENABLE_FAILED. Stream is not active\n" in report.text
    )


def test_firehose_fails(all_exist):
    all_exist.respond(
        "firehose",
        "describe-delivery-stream",
        firehose("CREATING_FAILED", "Role cannot be assumed"),
    )
    report = run(["orders"])
    assert report.code is Code.FIREHOSE_FAILED
    assert "  4. Firehose stream orders-firehose: failed\n" in report.text
    assert (
        "  Firehose stream orders-firehose has status CREATING_FAILED. Role cannot "
        "be assumed\n" in report.text
    )


def test_timeout(all_exist, clock):
    all_exist.respond("kinesis", "describe-stream-summary", kinesis("CREATING"))
    report = run(["orders"])
    assert report.code is Code.TIMEOUT
    assert report.text == (
        "create-stream orders\n"
        "  1. DynamoDB table orders: exists\n"
        "  2. Kinesis stream orders-stream: not active yet\n"
        "  3. Streaming from table orders to Kinesis stream orders-stream: "
        "not checked\n"
        "  4. Firehose stream orders-firehose: not checked\n"
        "Result: TIMEOUT\n"
        "  Kinesis stream orders-stream was still CREATING after waiting 540 "
        "seconds in total.\n"
        "Next step: Rerun the skill. It skips what exists and waits again.\n"
    )
    assert clock.now == 540


def test_deadline_is_shared_by_every_wait(all_exist, clock):
    all_exist.respond(
        "kinesis",
        "describe-stream-summary",
        *[kinesis("CREATING")] * 30,
        kinesis("ACTIVE"),
    )
    all_exist.respond("firehose", "describe-delivery-stream", firehose("CREATING"))
    report = run(["orders"])
    assert report.code is Code.TIMEOUT
    assert "Firehose stream orders-firehose was still CREATING" in report.text
    assert clock.now == 540


# The real command


def test_entry_point():
    # No arguments, so the command reports USAGE before reading any file.
    result = subprocess.run(
        ["create-stream"], capture_output=True, text=True, check=False
    )
    assert result.returncode == 1
    assert result.stdout == run([]).text
    assert result.stderr == ""

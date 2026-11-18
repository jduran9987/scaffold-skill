import pytest

from sp_dynamo_firehose_s3.aws import AwsError, AwsErrorKind, run_aws

PROFILE = "dev-main"
REGION = "us-east-1"


def test_command_uses_the_profile_and_region(fake_aws):
    fake_aws.respond("kinesis", "describe-stream-summary", {"ok": True})
    run_aws(
        PROFILE,
        REGION,
        "kinesis",
        ["describe-stream-summary", "--stream-name", "orders"],
    )
    assert fake_aws.calls == [
        [
            "aws",
            "kinesis",
            "describe-stream-summary",
            "--stream-name",
            "orders",
            "--output",
            "json",
            "--profile",
            "dev-main",
            "--region",
            "us-east-1",
        ]
    ]


def test_parses_json_output(fake_aws):
    fake_aws.respond("dynamodb", "describe-table", {"Table": {"TableName": "orders"}})
    assert run_aws(PROFILE, REGION, "dynamodb", ["describe-table"]) == {
        "Table": {"TableName": "orders"}
    }


def test_empty_output(fake_aws):
    fake_aws.respond("dynamodb", "describe-table", {})
    assert run_aws(PROFILE, REGION, "dynamodb", ["describe-table"]) == {}


@pytest.mark.parametrize(
    ("stderr", "kind"),
    [
        ("Error loading SSO Token: Token has expired", AwsErrorKind.CREDENTIALS),
        ("Unable to locate credentials.", AwsErrorKind.CREDENTIALS),
        ("The config profile (dev-main) could not be found", AwsErrorKind.CREDENTIALS),
        (
            "An error occurred (AccessDeniedException): User is not authorized",
            AwsErrorKind.ACCESS_DENIED,
        ),
        (
            "An error occurred (ResourceNotFoundException): Table not found",
            AwsErrorKind.NOT_FOUND,
        ),
        ("An error occurred (ValidationException): bad", AwsErrorKind.OTHER),
    ],
)
def test_errors_are_classified(fake_aws, stderr, kind):
    fake_aws.respond("dynamodb", "describe-table", stderr)
    with pytest.raises(AwsError) as raised:
        run_aws(PROFILE, REGION, "dynamodb", ["describe-table"])
    assert raised.value.kind is kind
    assert raised.value.operation == "aws dynamodb describe-table"
    assert raised.value.aws_message == stderr


def test_error_message_is_one_line(fake_aws):
    fake_aws.respond("dynamodb", "describe-table", "line one\n  line two\n")
    with pytest.raises(AwsError) as raised:
        run_aws(PROFILE, REGION, "dynamodb", ["describe-table"])
    assert raised.value.aws_message == "line one line two"


def test_cli_missing(fake_aws):
    fake_aws.respond("dynamodb", "describe-table", FileNotFoundError("aws"))
    with pytest.raises(AwsError) as raised:
        run_aws(PROFILE, REGION, "dynamodb", ["describe-table"])
    assert raised.value.kind is AwsErrorKind.CLI_MISSING

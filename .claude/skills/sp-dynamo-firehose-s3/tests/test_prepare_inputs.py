import json
import subprocess

import pytest

from sp_dynamo_firehose_s3.prepare_inputs.main import run
from sp_dynamo_firehose_s3.prepare_inputs.report import Code

SESSION_ID = "e5d4c591-1e54-431d-ba66-6522f39f0a15"
NOT_FOUND = "An error occurred (ResourceNotFoundException): not found"
S_TYPES = (
    '["string", "boolean", "int", "bigint", "double", "decimal(22,2)", "date", '
    '"timestamp"]'
)


@pytest.fixture(autouse=True)
def sessions_dir(tmp_path, monkeypatch):
    """Writes session folders under tmp_path instead of the system temp folder."""
    path = tmp_path / "sessions"
    monkeypatch.setattr(
        "sp_dynamo_firehose_s3.prepare_inputs.columns.SESSIONS_DIR", path
    )
    return path


def table(*keys):
    """A describe-table result. Each key is (name, key type, attribute type)."""
    return {
        "Table": {
            "KeySchema": [
                {"AttributeName": name, "KeyType": key_type}
                for name, key_type, _ in keys
            ],
            "AttributeDefinitions": [
                {"AttributeName": name, "AttributeType": attribute_type}
                for name, _, attribute_type in keys
            ],
        }
    }


def respond(fake_aws, items, keys=(("accountId", "HASH", "S"),)):
    fake_aws.respond("dynamodb", "describe-table", table(*keys))
    fake_aws.respond("dynamodb", "scan", {"Items": items})


def columns_section(output):
    """The column objects the agent reads, by name."""
    lines = output.split("\n")
    start = lines.index("## Columns (for the agent)") + 1
    end = lines.index("## Report")
    return {column["name"]: column for column in map(json.loads, lines[start:end])}


def written(sessions_dir):
    return json.loads((sessions_dir / SESSION_ID / "columns.json").read_text())


ITEMS = [
    {
        "accountId": {"S": "ACC-1001"},
        "balance": {"N": "0"},
        "openedAt": {"S": "2024-03-01T12:00:00Z"},
        "owner": {
            "M": {
                "fullName": {"S": "Jane Doe"},
                "homeAddress": {"M": {"zipCode": {"S": "10001"}}},
            }
        },
        "tags": {"SS": ["vip", "legacy"]},
        "notes": {"L": [{"S": "a"}, {"N": "1"}, {"M": {"x": {"BOOL": True}}}]},
        "active": {"BOOL": True},
        "bad-name": {"S": "left out"},
    },
    {
        "accountId": {"S": "ACC-1002"},
        "balance": {"N": "1250.75"},
        "openedAt": {"NULL": True},
        "owner": {"M": {"fullName": {"S": "John Roe"}, "1st": {"S": "x"}}},
        "active": {"BOOL": False},
    },
]


# Before any AWS call


@pytest.mark.parametrize("args", [[], [SESSION_ID], [SESSION_ID, "orders", "x"]])
def test_wrong_number_of_arguments(fake_aws, args):
    report = run(args)
    assert report.code is Code.USAGE
    assert report.output == (
        "## Report\n"
        "prepare-inputs\n"
        "Result: USAGE\n"
        "  prepare-inputs takes exactly two arguments, the session ID and the "
        f"DynamoDB table name, but got {len(args)}.\n"
        "Next step: Rerun the skill.\n"
    )
    assert fake_aws.calls == []


def test_invalid_session_id(fake_aws, sessions_dir):
    report = run(["${CLAUDE_SESSION_ID}", "orders"])
    assert report.code is Code.CONFIG_INVALID
    assert report.text == (
        "prepare-inputs orders\n"
        "Result: CONFIG_INVALID\n"
        "  The table name, session ID, or inputs.toml is invalid: session_id: "
        "String should match pattern "
        "'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'.\n"
        "Next step: Fix what is listed, then rerun the skill.\n"
    )
    assert fake_aws.calls == []
    assert not sessions_dir.exists()


def test_invalid_table_name(fake_aws):
    report = run([SESSION_ID, "ab"])
    assert report.code is Code.CONFIG_INVALID
    assert "dynamo_table_name: String should have at least 3 characters" in (
        report.text
    )
    assert fake_aws.calls == []


# Reading the table


def test_reads_the_keys_then_a_sample(fake_aws):
    respond(fake_aws, ITEMS)
    run([SESSION_ID, "orders"])
    assert fake_aws.operations() == ["dynamodb describe-table", "dynamodb scan"]
    assert fake_aws.argument("dynamodb describe-table", "--table-name") == "orders"
    assert fake_aws.argument("dynamodb scan", "--table-name") == "orders"
    assert fake_aws.argument("dynamodb scan", "--max-items") == "100"


def test_columns_found(fake_aws, sessions_dir):
    respond(fake_aws, ITEMS)
    report = run([SESSION_ID, "orders"])
    assert report.code is Code.COLUMNS_FOUND
    assert report.ok
    assert report.output == (
        "## Columns (for the agent)\n"
        '{"name": "account_id", "dynamo_type": "S", "key": true, '
        '"examples": ["ACC-1001", "ACC-1002"], '
        f'"allowed_types": {S_TYPES}}}\n'
        '{"name": "active", "dynamo_type": "BOOL", "key": false, '
        '"examples": ["true", "false"], "allowed_types": ["boolean", "string"]}\n'
        '{"name": "balance", "dynamo_type": "N", "key": false, '
        '"examples": ["0", "1250.75"], "allowed_types": '
        '["int", "bigint", "double", "decimal(22,2)", "string"]}\n'
        '{"name": "notes", "dynamo_type": "L", "key": false, '
        '"examples": ["[\\"a\\",\\"1\\",{\\"x\\":true}]"], '
        '"allowed_types": ["string"]}\n'
        '{"name": "opened_at", "dynamo_type": "S", "key": false, '
        f'"examples": ["2024-03-01T12:00:00Z"], "allowed_types": {S_TYPES}}}\n'
        '{"name": "owner__full_name", "dynamo_type": "S", "key": false, '
        f'"examples": ["Jane Doe", "John Roe"], "allowed_types": {S_TYPES}}}\n'
        '{"name": "owner__home_address__zip_code", "dynamo_type": "S", '
        f'"key": false, "examples": ["10001"], "allowed_types": {S_TYPES}}}\n'
        '{"name": "tags", "dynamo_type": "SS", "key": false, '
        '"examples": ["[\\"vip\\",\\"legacy\\"]"], '
        '"allowed_types": ["array<string>"]}\n'
        "## Report\n"
        "prepare-inputs orders\n"
        "Result: COLUMNS_FOUND\n"
        "  Columns found in DynamoDB table orders: 8. Items sampled: 2. "
        "Attributes left out because their names cannot be column names: "
        "bad-name, owner.1st.\n"
        "Next step: None.\n"
    )


def test_columns_are_written_without_examples(fake_aws, sessions_dir):
    respond(fake_aws, ITEMS)
    run([SESSION_ID, "orders"])
    data = written(sessions_dir)
    assert data["dynamo_table_name"] == "orders"
    assert data["columns"][0] == {
        "name": "account_id",
        "path": "accountId",
        "dynamo_type": "S",
        "key": True,
    }
    assert {
        "name": "owner__home_address__zip_code",
        "path": "owner.M.homeAddress.M.zipCode",
        "dynamo_type": "S",
        "key": False,
    } in data["columns"]
    assert [column["name"] for column in data["columns"]] == list(
        columns_section(run([SESSION_ID, "orders"]).output)
    )


def test_same_table_gives_the_same_output(fake_aws, sessions_dir):
    respond(fake_aws, ITEMS)
    first = run([SESSION_ID, "orders"]).output
    first_file = written(sessions_dir)
    assert run([SESSION_ID, "orders"]).output == first
    assert written(sessions_dir) == first_file


def test_empty_table_gives_the_key_columns(fake_aws, sessions_dir):
    respond(
        fake_aws,
        [],
        keys=(("openedAt", "RANGE", "N"), ("accountId", "HASH", "S")),
    )
    report = run([SESSION_ID, "orders"])
    assert report.code is Code.COLUMNS_FOUND
    columns = columns_section(report.output)
    assert list(columns) == ["account_id", "opened_at"]
    assert columns["opened_at"] == {
        "name": "opened_at",
        "dynamo_type": "N",
        "key": True,
        "examples": [],
        "allowed_types": ["int", "bigint", "double", "decimal(22,2)", "string"],
    }
    assert "Items sampled: 0." in report.text
    assert "column names: none." in report.text
    assert [column["key"] for column in written(sessions_dir)["columns"]] == [
        True,
        True,
    ]


def test_key_columns_come_first(fake_aws):
    respond(
        fake_aws,
        [{"zId": {"S": "z"}, "aSort": {"N": "1"}, "aaa": {"S": "x"}}],
        keys=(("zId", "HASH", "S"), ("aSort", "RANGE", "N")),
    )
    columns = columns_section(run([SESSION_ID, "orders"]).output)
    assert list(columns) == ["z_id", "a_sort", "aaa"]


# Types and examples


def test_type_is_the_most_common_ignoring_null(fake_aws):
    respond(
        fake_aws,
        [
            {"accountId": {"S": "1"}, "a": {"NULL": True}, "b": {"NULL": True}},
            {"accountId": {"S": "2"}, "a": {"NULL": True}, "b": {"NULL": True}},
            {"accountId": {"S": "3"}, "a": {"N": "5"}},
            {"accountId": {"S": "4"}, "a": {"S": "x"}},
        ],
    )
    columns = columns_section(run([SESSION_ID, "orders"]).output)
    # N and S tie, so the alphabetically first wins; NULL is ignored.
    assert columns["a"]["dynamo_type"] == "N"
    assert columns["a"]["examples"] == ["5"]
    # A column that is only ever NULL is read as a string.
    assert columns["b"]["dynamo_type"] == "S"
    assert columns["b"]["examples"] == []
    assert columns["b"]["allowed_types"][0] == "string"


def test_up_to_three_different_examples(fake_aws):
    values = ["1", "1", "2", "3", "4"]
    respond(
        fake_aws,
        [{"accountId": {"S": "k"}, "n": {"N": value}} for value in values],
    )
    columns = columns_section(run([SESSION_ID, "orders"]).output)
    assert columns["n"]["examples"] == ["1", "2", "3"]


def test_long_examples_are_cut(fake_aws):
    respond(fake_aws, [{"accountId": {"S": "k"}, "text": {"S": "x" * 50}}])
    columns = columns_section(run([SESSION_ID, "orders"]).output)
    assert columns["text"]["examples"] == ["x" * 39 + "…"]


# Names that cannot be columns


def test_key_name_unsupported(fake_aws, sessions_dir):
    respond(fake_aws, ITEMS, keys=(("account-id", "HASH", "S"),))
    report = run([SESSION_ID, "orders"])
    assert report.code is Code.KEY_NAME_UNSUPPORTED
    assert report.output == (
        "## Report\n"
        "prepare-inputs orders\n"
        "Result: KEY_NAME_UNSUPPORTED\n"
        "  Key attribute account-id of DynamoDB table orders cannot be a column "
        "name. A column name must start with a letter and contain only letters, "
        "digits, and underscores.\n"
        "Next step: None. The skill cannot build a pipeline for this table.\n"
    )
    assert not sessions_dir.exists()


def test_column_name_clash(fake_aws, sessions_dir):
    respond(
        fake_aws,
        [
            {
                "accountId": {"S": "k"},
                "account_id": {"S": "x"},
                "zipCode": {"S": "1"},
                "zip_code": {"S": "2"},
                "a": {"M": {"bC": {"S": "3"}}},
                "a__b_c": {"S": "4"},
            }
        ],
    )
    report = run([SESSION_ID, "orders"])
    assert report.code is Code.COLUMN_NAME_CLASH
    assert report.text == (
        "prepare-inputs orders\n"
        "Result: COLUMN_NAME_CLASH\n"
        "  Attributes of DynamoDB table orders would become the same column: "
        "accountId, account_id would all be account_id; a.bC, a__b_c would all "
        "be a__b_c; zipCode, zip_code would all be zip_code.\n"
        "Next step: Rename the attributes in DynamoDB so their column names "
        "differ, then rerun the skill.\n"
    )
    assert not sessions_dir.exists()


# Failures


def test_table_not_found(fake_aws, sessions_dir):
    fake_aws.respond("dynamodb", "describe-table", NOT_FOUND)
    report = run([SESSION_ID, "orders"])
    assert report.code is Code.TABLE_NOT_FOUND
    assert report.output == (
        "## Report\n"
        "prepare-inputs orders\n"
        "Result: TABLE_NOT_FOUND\n"
        "  DynamoDB table orders does not exist in us-east-1 for AWS CLI profile "
        "dev-main.\n"
        "Next step: Check the table name, or aws.profile and aws.region in "
        "inputs.toml, then rerun the skill.\n"
    )
    assert not sessions_dir.exists()


def test_table_deleted_before_the_scan(fake_aws):
    fake_aws.respond("dynamodb", "describe-table", table(("accountId", "HASH", "S")))
    fake_aws.respond("dynamodb", "scan", NOT_FOUND)
    assert run([SESSION_ID, "orders"]).code is Code.TABLE_NOT_FOUND


@pytest.mark.parametrize(
    ("stderr", "code", "next_step"),
    [
        (
            "Error loading SSO Token: expired",
            Code.AWS_CREDENTIALS,
            "Run `aws sso login --profile dev-main` yourself, then rerun the skill.",
        ),
        (
            "An error occurred (AccessDeniedException): no",
            Code.AWS_ACCESS_DENIED,
            "Get that permission for the profile, then rerun the skill.",
        ),
        (
            "An error occurred (ThrottlingException): slow down",
            Code.AWS_ERROR,
            "Fix the cause AWS gave, then rerun the skill.",
        ),
    ],
)
def test_aws_errors(fake_aws, sessions_dir, stderr, code, next_step):
    fake_aws.respond("dynamodb", "describe-table", table(("accountId", "HASH", "S")))
    fake_aws.respond("dynamodb", "scan", stderr)
    report = run([SESSION_ID, "orders"])
    assert report.code is code
    assert report.output.startswith("## Report\n")
    assert "aws dynamodb scan" in report.text or code is Code.AWS_CREDENTIALS
    assert report.text.endswith(f"Next step: {next_step}\n")
    assert not sessions_dir.exists()


def test_aws_cli_missing(fake_aws):
    fake_aws.respond("dynamodb", "describe-table", FileNotFoundError("aws"))
    assert run([SESSION_ID, "orders"]).code is Code.AWS_CLI_MISSING


def test_write_failed(fake_aws, sessions_dir):
    sessions_dir.write_text("not a folder")
    respond(fake_aws, ITEMS)
    report = run([SESSION_ID, "orders"])
    assert report.code is Code.WRITE_FAILED
    assert report.output.startswith("## Report\n")
    assert (
        f"The columns could not be written to "
        f"{sessions_dir / SESSION_ID / 'columns.json'}: "
    ) in report.text


# The real command


def test_entry_point():
    # One argument, so the command reports USAGE before reading any file.
    result = subprocess.run(
        ["prepare-inputs", SESSION_ID],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert result.stdout == run([SESSION_ID]).output
    assert result.stderr == ""

from pathlib import Path

import pytest

from sp_dynamo_firehose_s3.create_stream.config import (
    ConfigError,
    build_config,
)

INPUTS = (Path(__file__).parent / "fixtures" / "inputs.toml").read_text()


@pytest.fixture
def inputs_path(tmp_path, monkeypatch):
    """Points build_config at a file in tmp_path instead of the tests' copy."""
    path = tmp_path / "inputs.toml"
    monkeypatch.setattr("sp_dynamo_firehose_s3.create_stream.config.INPUTS_PATH", path)
    return path


def problems_for(path, old, new, table="bo.suspenseAccounts"):
    assert old in INPUTS, f"{old!r} is not in the tests' inputs.toml"
    path.write_text(INPUTS.replace(old, new, 1))
    with pytest.raises(ConfigError) as raised:
        build_config(table)
    return raised.value.problems


def problems_for_table(table):
    with pytest.raises(ConfigError) as raised:
        build_config(table)
    return raised.value.problems


# Building from the shipped inputs


def test_shipped_inputs():
    config = build_config("orders")
    assert config.dynamo_table_name == "orders"
    assert config.aws.profile == "dev-main"
    assert config.aws.region == "us-east-1"
    assert config.kinesis.shard_count == 3
    assert config.wait.timeout_seconds == 540


def test_names_are_filled_in():
    config = build_config("bo.suspenseAccounts")
    assert config.kinesis.name == "bo.suspenseAccounts-stream"
    assert config.firehose.name == "bo.suspenseAccounts-firehose"
    assert config.firehose.error_output_prefix == (
        "firehose-stream/failures/bo.suspenseAccounts/"
        "!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd/HH}/"
    )


def test_placeholders_are_filled_in_one_pass(inputs_path):
    inputs_path.write_text(
        INPUTS.replace('profile = "dev-main"', 'profile = "{dynamo_table_name}"')
    )
    # A name that looks like a placeholder is inserted as is, never expanded.
    with pytest.raises(ConfigError):
        build_config("{dynamo_table_name}")
    assert build_config("orders").aws.profile == "orders"


def test_firehose_syntax_is_left_unchanged():
    firehose = build_config("orders").firehose
    assert firehose.prefix == (
        "firehose-stream/!{partitionKeyFromQuery:tableName}/!{timestamp:yyyy/MM/dd/HH}/"
    )
    assert firehose.metadata_extraction_query == (
        r'{tableName:limit(1;.tableName | gsub("\\.", "-"; "-"))}'
    )
    assert firehose.record_delimiter == r"\n"


def test_file_is_not_changed(test_inputs):
    build_config("orders")
    assert test_inputs.read_text() == INPUTS


def test_same_config_for_the_same_table():
    assert build_config("orders") == build_config("orders")


def test_config_is_immutable():
    config = build_config("orders")
    with pytest.raises(ValueError):
        config.dynamo_table_name = "other"


def test_other_actions_sections_are_ignored(inputs_path):
    inputs_path.write_text(INPUTS[: INPUTS.index("\n[schema_inference]\n")])
    assert build_config("orders").kinesis.name == "orders-stream"


# Table name rules


@pytest.mark.parametrize("table", ["abc", "a.b", "A-1", "x_y", "a" * 55])
def test_valid_table_names(table):
    assert build_config(table).dynamo_table_name == table


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        ("ab", "dynamo_table_name: String should have at least 3 characters"),
        (
            "it's orders",
            "dynamo_table_name: String should match pattern '^[A-Za-z0-9_.-]+$'",
        ),
        ("a" * 56, "firehose.name: String should have at most 64 characters"),
    ],
)
def test_invalid_table_names(table, expected):
    assert problems_for_table(table) == expected


def test_every_table_name_problem_is_listed():
    assert problems_for_table("a b" * 100) == (
        "dynamo_table_name: String should have at most 255 characters; "
        "kinesis.name: String should have at most 128 characters; "
        "firehose.name: String should have at most 64 characters"
    )


# Stream names must follow the table name


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        (
            'name = "{dynamo_table_name}-stream"\n',
            'name = "orders"\n',
            'Value error, kinesis.name is "orders" but must be '
            '"bo.suspenseAccounts-stream"',
        ),
        (
            'name = "{dynamo_table_name}-firehose"',
            'name = "{dynamo_table_name}-fh"',
            'Value error, firehose.name is "bo.suspenseAccounts-fh" but must be '
            '"bo.suspenseAccounts-firehose"',
        ),
    ],
)
def test_stream_names_must_follow_the_table(inputs_path, old, new, expected):
    assert problems_for(inputs_path, old, new) == expected


# Invalid inputs.toml values


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        (
            "timeout_seconds = 540",
            "timeout_seconds = 541",
            "wait.timeout_seconds: Input should be less than or equal to 540",
        ),
        (
            "shard_count = 3",
            'shard_count = "3"',
            "kinesis.shard_count: Input should be a valid integer",
        ),
        (
            "shard_count = 3",
            "shard_count = 0",
            "kinesis.shard_count: Input should be greater than or equal to 1",
        ),
        (
            "buffer_size_mb = 128",
            "buffer_size_mb = 32",
            "firehose.buffer_size_mb: Input should be greater than or equal to 64",
        ),
        (
            'profile = "dev-main"',
            'profile = ""',
            "aws.profile: String should have at least 1 character",
        ),
        (
            'bucket_arn = "arn:aws:s3:::dev-datalake-7472"',
            'bucket_arn = "dev-datalake-7472"',
            "firehose.bucket_arn: String should match pattern '^arn:aws:s3:::[^/]+$'",
        ),
        (
            'region = "us-east-1"\n',
            'region = "us-east-1"\nextra = 1\n',
            "aws.extra: Extra inputs are not permitted",
        ),
        ('region = "us-east-1"\n', "", "aws.region: Field required"),
        ("\n[wait]\n", "\n[waits]\n", "wait: Field required"),
    ],
)
def test_invalid_inputs(inputs_path, old, new, expected):
    assert problems_for(inputs_path, old, new) == expected


def test_table_name_and_inputs_problems_are_listed_together(inputs_path):
    assert problems_for(inputs_path, "shard_count = 3", "shard_count = 0", "ab") == (
        "dynamo_table_name: String should have at least 3 characters; "
        "kinesis.shard_count: Input should be greater than or equal to 1"
    )


def test_file_cannot_override_the_table_name(inputs_path):
    inputs_path.write_text('dynamo_table_name = "other"\n' + INPUTS)
    assert build_config("orders").dynamo_table_name == "orders"


def test_missing_file(inputs_path):
    assert problems_for_table("orders") == (f"{inputs_path}: No such file or directory")


def test_invalid_toml(inputs_path):
    inputs_path.write_text("[aws\n")
    assert problems_for_table("orders").startswith(
        f"{inputs_path}: is not valid TOML: "
    )


# Values the generated pipeline depends on


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        (
            'compression_format = "GZIP"',
            'compression_format = "ZIP"',
            "firehose.compression_format: Input should be 'GZIP'",
        ),
        (
            "record_delimiter = '\\n'",
            "record_delimiter = ','",
            "firehose.record_delimiter: Input should be '\\\\n'",
        ),
        (
            'gsub("\\\\.", "-"; "-")',
            'gsub("\\\\.", "_"; "-")',
            "firehose.metadata_extraction_query: Input should be "
            '\'{tableName:limit(1;.tableName | gsub("\\\\\\\\.", "-"; "-"))}\'',
        ),
        (
            'prefix = "firehose-stream/!{partitionKeyFromQuery:tableName}/',
            'prefix = "firehose-stream/!{timestamp:yyyy}/',
            "firehose.prefix: String should match pattern "
            "'^[^!{}]+/!\\{partitionKeyFromQuery:tableName\\}/'",
        ),
    ],
)
def test_values_the_pipeline_depends_on(inputs_path, old, new, expected):
    assert problems_for(inputs_path, old, new) == expected

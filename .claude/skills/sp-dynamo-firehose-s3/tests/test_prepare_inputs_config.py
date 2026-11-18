from pathlib import Path

import pytest

from sp_dynamo_firehose_s3.prepare_inputs.config import (
    ConfigError,
    build_config,
)

INPUTS = (Path(__file__).parent / "fixtures" / "inputs.toml").read_text()
SESSION_ID = "e5d4c591-1e54-431d-ba66-6522f39f0a15"


@pytest.fixture
def inputs_path(tmp_path, monkeypatch):
    """Points build_config at a file in tmp_path instead of the tests' copy."""
    path = tmp_path / "inputs.toml"
    monkeypatch.setattr("sp_dynamo_firehose_s3.prepare_inputs.config.INPUTS_PATH", path)
    return path


def problems_for(path, old, new, table="bo.suspenseAccounts"):
    assert old in INPUTS, f"{old!r} is not in the tests' inputs.toml"
    path.write_text(INPUTS.replace(old, new, 1))
    with pytest.raises(ConfigError) as raised:
        build_config(table, SESSION_ID)
    return raised.value.problems


def problems_for_table(table):
    with pytest.raises(ConfigError) as raised:
        build_config(table, SESSION_ID)
    return raised.value.problems


def test_shipped_inputs():
    config = build_config("bo.suspenseAccounts", SESSION_ID)
    assert config.dynamo_table_name == "bo.suspenseAccounts"
    assert config.aws.profile == "dev-main"
    assert config.aws.region == "us-east-1"
    assert config.schema_inference.sample_size == 100
    assert config.schema_inference.allowed_types.for_type("N") == [
        "int",
        "bigint",
        "double",
        "decimal(22,2)",
        "string",
    ]


def test_other_actions_sections_are_ignored(inputs_path):
    start = INPUTS.index("\n[kinesis]\n")
    end = INPUTS.index("\n[schema_inference]\n")
    inputs_path.write_text(INPUTS[:start] + INPUTS[end : INPUTS.index("\n[project]\n")])
    assert build_config("orders", SESSION_ID).schema_inference.sample_size == 100


def test_config_is_immutable():
    config = build_config("orders", SESSION_ID)
    with pytest.raises(ValueError):
        config.dynamo_table_name = "other"


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        ("ab", "dynamo_table_name: String should have at least 3 characters"),
        (
            "it's orders",
            "dynamo_table_name: String should match pattern '^[A-Za-z0-9_.-]+$'",
        ),
    ],
)
def test_invalid_table_names(table, expected):
    assert problems_for_table(table) == expected


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        (
            "sample_size = 100",
            "sample_size = 0",
            "schema_inference.sample_size: Input should be greater than or equal to 1",
        ),
        (
            "sample_size = 100",
            'sample_size = "100"',
            "schema_inference.sample_size: Input should be a valid integer",
        ),
        ('region = "us-east-1"\n', "", "aws.region: Field required"),
        (
            'BS = ["array<string>"]\n',
            "",
            "schema_inference.allowed_types.BS: Field required",
        ),
        (
            'BS = ["array<string>"]\n',
            "BS = []\n",
            "schema_inference.allowed_types.BS: List should have at least 1 item "
            "after validation, not 0",
        ),
        (
            'region = "us-east-1"\n',
            'region = "us-east-1"\nextra = 1\n',
            "aws.extra: Extra inputs are not permitted",
        ),
    ],
)
def test_invalid_inputs(inputs_path, old, new, expected):
    assert problems_for(inputs_path, old, new) == expected


def test_file_cannot_override_the_table_name(inputs_path):
    inputs_path.write_text('dynamo_table_name = "other"\n' + INPUTS)
    assert build_config("orders", SESSION_ID).dynamo_table_name == "orders"


def test_missing_file(inputs_path):
    assert problems_for_table("orders") == (f"{inputs_path}: No such file or directory")


def test_invalid_toml(inputs_path):
    inputs_path.write_text("[aws\n")
    assert problems_for_table("orders").startswith(
        f"{inputs_path}: is not valid TOML: "
    )

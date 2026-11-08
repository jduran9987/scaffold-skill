import subprocess
from pathlib import Path

import pytest
import yaml

from sp_dynamo_firehose_s3.v1 import generate, main
from sp_dynamo_firehose_s3.v1 import schema_inference as inference
from sp_dynamo_firehose_s3.v1.aws import AwsError
from sp_dynamo_firehose_s3.v1.schema_inference import (
    InferredColumn,
    SchemaInferenceError,
    infer_schema,
    to_snake_case,
)

FIXTURES = Path(__file__).parent / "fixtures"
KEYS = [("id", "N"), ("createdAt", "S")]


def order(order_id, **attributes):
    return {
        "id": {"N": str(order_id)},
        "createdAt": {"S": "2026-11-05T10:00:00Z"},
        **attributes,
    }


ADDRESS = {
    "M": {
        "city": {"S": "Austin"},
        "zipCode": {"S": "78701"},
        "geo": {"M": {"lat": {"N": "30.2"}, "lng": {"N": "-97.7"}}},
    }
}
ITEMS = [
    order(1, amount={"N": "12.50"}, status={"S": "PENDING"}, addressDetails=ADDRESS),
    order(2, amount={"N": "3"}, status={"NULL": True}, labels={"SS": ["a"]}),
    order(3, amount={"S": "n/a"}, notes={"NULL": True}, isGift={"BOOL": True}),
]


def columns_by_name(schema):
    return {column.name: column for column in schema.columns}


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("id", "id"),
        ("createdAt", "created_at"),
        ("zipCode", "zip_code"),
        ("orderID", "order_id"),
        ("HTTPStatusCode", "http_status_code"),
        ("already_snake", "already_snake"),
        ("line2Address", "line2_address"),
    ],
)
def test_to_snake_case(name, expected):
    assert to_snake_case(name) == expected


def test_keys_come_first_and_are_primary_keys():
    schema = infer_schema(KEYS, ITEMS)

    assert schema.columns[:2] == (
        InferredColumn("id", "N", "id", key=True),
        InferredColumn("created_at", "S", "createdAt", key=True),
    )
    assert schema.primary_keys == ("id", "created_at")
    assert all(not column.key for column in schema.columns[2:])


def test_table_without_sort_key():
    schema = infer_schema([("id", "N")], ITEMS)

    assert schema.primary_keys == ("id",)
    assert columns_by_name(schema)["created_at"].key is False


def test_maps_are_flattened_at_every_depth():
    columns = columns_by_name(infer_schema(KEYS, ITEMS))

    assert columns["address_details__zip_code"] == InferredColumn(
        "address_details__zip_code", "S", "addressDetails.M.zipCode", key=False
    )
    assert columns["address_details__geo__lat"].path == "addressDetails.M.geo.M.lat"
    assert all(column.dynamo_type != "M" for column in columns.values())
    assert "address_details" not in columns


def test_columns_are_the_union_of_attributes_sorted_by_path():
    schema = infer_schema(KEYS, ITEMS)

    assert [column.path for column in schema.columns[2:]] == [
        "addressDetails.M.city",
        "addressDetails.M.geo.M.lat",
        "addressDetails.M.geo.M.lng",
        "addressDetails.M.zipCode",
        "amount",
        "isGift",
        "labels",
        "notes",
        "status",
    ]


def test_most_common_type_wins_and_null_is_ignored():
    columns = columns_by_name(infer_schema(KEYS, ITEMS))

    assert columns["amount"].dynamo_type == "N"
    assert columns["status"].dynamo_type == "S"
    assert columns["notes"].dynamo_type == "NULL"
    assert columns["labels"].dynamo_type == "SS"
    assert columns["is_gift"].dynamo_type == "BOOL"


def test_type_ties_are_broken_alphabetically():
    items = [order(1, code={"S": "a"}), order(2, code={"N": "1"})]

    assert columns_by_name(infer_schema(KEYS, items))["code"].dynamo_type == "N"


def test_attributes_with_unusable_names_are_left_out():
    items = [
        order(
            1,
            **{
                "order-id": {"S": "x"},
                "first name": {"S": "x"},
                "1stItem": {"S": "x"},
                "_private": {"S": "x"},
                "bad-map": {"M": {"city": {"S": "x"}}},
                "meta": {"M": {"ok": {"S": "x"}, "not.ok": {"S": "x"}}},
            },
        )
    ]

    names = list(columns_by_name(infer_schema(KEYS, items)))

    assert names == ["id", "created_at", "meta__ok"]


def test_name_collisions_keep_every_path():
    items = [order(1, zipCode={"S": "a"}, zip_code={"N": "1"})]

    columns = infer_schema(KEYS, items).columns[2:]

    assert columns == (
        InferredColumn("zip_code", "S", "zipCode", key=False),
        InferredColumn("zip_code", "N", "zip_code", key=False),
    )


def test_empty_maps_add_no_columns():
    items = [order(1, extras={"M": {}})]

    assert list(columns_by_name(infer_schema(KEYS, items))) == ["id", "created_at"]


def test_unusable_key_name_fails():
    with pytest.raises(SchemaInferenceError, match="order-id"):
        infer_schema([("order-id", "S")], ITEMS)


def test_inference_is_deterministic():
    assert infer_schema(KEYS, ITEMS) == infer_schema(KEYS, list(reversed(ITEMS)))


def test_aws_commands_use_the_table_and_defaults_profile_and_sample_size(fake_aws):
    inference.infer_table_schema("orders")

    assert fake_aws.calls == [
        [
            "aws",
            "dynamodb",
            "describe-table",
            "--table-name",
            "orders",
            "--output",
            "json",
            "--profile",
            "default",
            "--region",
            "us-east-1",
        ],
        [
            "aws",
            "dynamodb",
            "scan",
            "--table-name",
            "orders",
            "--max-items",
            "100",
            "--output",
            "json",
            "--profile",
            "default",
            "--region",
            "us-east-1",
        ],
    ]


def test_aws_failure_is_reported(fake_aws):
    fake_aws.result = subprocess.CompletedProcess(
        [], 255, stderr="The SSO session has expired"
    )

    with pytest.raises(AwsError, match="SSO session has expired"):
        inference.infer_table_schema("orders")


def test_missing_aws_cli_is_reported(fake_aws):
    fake_aws.result = FileNotFoundError("aws")

    with pytest.raises(AwsError, match="AWS CLI is not installed"):
        inference.infer_table_schema("orders")


def test_action_reads_the_table_named_after_the_file(tmp_path, fake_aws):
    assert main(["generate-yaml", str(tmp_path / "pipelines" / "orders.yaml")]) == 0

    assert {call[4] for call in fake_aws.calls} == {"orders"}


def test_action_writes_the_inferred_schema(tmp_path):
    path = tmp_path / "orders.yaml"

    assert main(["generate-yaml", str(path)]) == 0

    source = path.read_text()
    schema = yaml.safe_load(source)["schema"]
    assert schema["primaryKeys"] == ["id", "created_at"]
    assert schema["columns"][0] == {
        "name": "id",
        "description": "Please fill or remove.",
        "dynamoType": "N",
        "targetType": "",
        "path": "id",
        "key": True,
    }
    assert '      description: "Please fill or remove."\n' in source
    assert '      targetType: ""  # Required field, please fill.\n' in source
    notes = next(c for c in schema["columns"] if c["name"] == "notes")
    assert notes["dynamoType"] == "NULL"


@pytest.mark.parametrize("option", [["--profile", "dev"], ["--infer-schema", "t"]])
def test_removed_options_are_rejected(tmp_path, option):
    with pytest.raises(SystemExit):
        main(["generate-yaml", str(tmp_path / "o.yaml"), *option])


def test_aws_failure_writes_nothing(tmp_path, fake_aws, capsys):
    fake_aws.result = subprocess.CompletedProcess([], 254, stderr="Table not found")
    path = tmp_path / "orders.yaml"

    assert main(["generate-yaml", str(path)]) == 1

    output = capsys.readouterr().out
    assert "Result: AWS_ERROR\n" in output
    assert "Table not found" in output
    assert not path.exists()


def test_filled_inferred_yaml_scaffolds(tmp_path):
    path = tmp_path / "orders.yaml"
    main(["generate-yaml", str(path)])
    config = yaml.safe_load(path.read_text())
    orders = yaml.safe_load((FIXTURES / "orders.yaml").read_text())
    config["metadata"].update(name="orders", description="Orders.")
    for layer in ("bronze", "silver", "gold"):
        for key in ("sourcePath", "checkpointPath", "targetTable"):
            if key in config["job"][layer]:
                config["job"][layer][key] = orders["job"][layer][key]
    target_types = {
        "N": "decimal(22, 2)",
        "S": "string",
        "BOOL": "boolean",
        "NULL": "boolean",
        "SS": "array<string>",
    }
    for column in config["schema"]["columns"]:
        column["targetType"] = target_types[column["dynamoType"]]
        del column["description"]

    assert generate(config, tmp_path / "project")

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from sp_dynamo_firehose_s3.v1.config import PipelineConfig
from sp_dynamo_firehose_s3.v1.fields import SILVER_METADATA
from sp_dynamo_firehose_s3.v1.generator import render_files, sql_string, sql_table

FIXTURES = Path(__file__).parent / "fixtures"
TARGETS = ("dev", "qa", "uat", "prod")
LAYERS = ("bronze", "silver", "gold")


@pytest.fixture
def raw_config():
    return yaml.safe_load((FIXTURES / "orders.yaml").read_text())


def ddl(raw_config):
    files = render_files(PipelineConfig.model_validate(raw_config))
    return {path: text for path, text in files.items() if path.startswith("ddl/")}


def test_one_file_per_layer_per_target(raw_config):
    assert sorted(ddl(raw_config)) == sorted(
        f"ddl/{target}/{layer}.sql" for target in TARGETS for layer in LAYERS
    )


@pytest.mark.parametrize("layer", LAYERS)
def test_dev_ddl_matches_expected_file(raw_config, layer):
    expected = (FIXTURES / "expected" / "ddl" / f"{layer}.sql").read_text()

    assert ddl(raw_config)[f"ddl/dev/{layer}.sql"] == expected


@pytest.mark.parametrize("target", TARGETS)
@pytest.mark.parametrize("layer", LAYERS)
def test_each_target_differs_only_in_its_name(raw_config, target, layer):
    files = ddl(raw_config)
    dev = files[f"ddl/dev/{layer}.sql"]

    assert files[f"ddl/{target}/{layer}.sql"] == dev.replace(
        "`dev`.", f"`{target}`."
    ).replace("for the dev target", f"for the {target} target")


def test_table_name_without_env_placeholder_is_used_as_is(raw_config):
    raw_config["job"]["gold"]["targetTable"] = "shared.gold.orders"

    files = ddl(raw_config)

    assert all(
        "CREATE TABLE IF NOT EXISTS `shared`.`gold`.`orders` (" in files[path]
        for path in files
        if path.endswith("gold.sql")
    )


def test_columns_without_description_have_no_comment(raw_config):
    del raw_config["schema"]["columns"][3]["description"]

    silver = ddl(raw_config)["ddl/dev/silver.sql"]

    assert "  `status` STRING,\n" in silver
    assert '  `amount` DECIMAL(22, 2) COMMENT "Order amount in dollars.",\n' in silver


def test_comments_use_double_quotes_and_escape_them(raw_config):
    raw_config["schema"]["columns"][3]["description"] = 'Customer\'s "status" \\ flag.'

    silver = ddl(raw_config)["ddl/dev/silver.sql"]

    assert 'COMMENT "Customer\'s \\"status\\" \\\\ flag."' in silver
    assert "COMMENT '" not in silver


@pytest.mark.parametrize("target", TARGETS)
def test_table_names_use_the_target_name(raw_config, target):
    files = ddl(raw_config)

    for layer in LAYERS:
        assert (
            f"CREATE TABLE IF NOT EXISTS `{target}`.`{layer}`.`orders` ("
            in files[f"ddl/{target}/{layer}.sql"]
        )
        assert "{" not in files[f"ddl/{target}/{layer}.sql"]


@pytest.mark.parametrize(
    ("layer", "key", "value"),
    [
        ("bronze", "targetTable", "{dev}.bronze.orders"),
        ("silver", "checkpointPath", "s3://{environment}-lake/cp/"),
        ("bronze", "sourcePath", "s3://{env}-lake/{tbl}/"),
    ],
)
def test_placeholders_other_than_env_are_rejected(raw_config, layer, key, value):
    raw_config["job"][layer][key] = value

    with pytest.raises(
        ValidationError, match="only the {env}, {name}, {table} placeholders"
    ):
        PipelineConfig.model_validate(raw_config)


def test_metadata_column_comments_never_change(raw_config):
    other = yaml.safe_load((FIXTURES / "orders.yaml").read_text())
    other["metadata"]["description"] = "Something else."
    for column in other["schema"]["columns"]:
        column.pop("description", None)

    for config in (raw_config, other):
        gold = ddl(config)["ddl/dev/gold.sql"]
        for column in SILVER_METADATA:
            assert (
                f"`{column.name}` {column.type.upper()} COMMENT "
                f"{sql_string(column.comment)}"
            ) in gold


def test_silver_and_gold_have_the_same_columns(raw_config):
    files = ddl(raw_config)

    def columns(text):
        return [line for line in text.splitlines() if line.startswith("  `")]

    assert columns(files["ddl/dev/silver.sql"]) == columns(files["ddl/dev/gold.sql"])


def test_only_the_configured_layers_get_ddl(raw_config):
    del raw_config["job"]["gold"]

    assert not any(path.endswith("gold.sql") for path in ddl(raw_config))


def test_table_name_parts_are_quoted():
    assert sql_table("my-catalog.bronze.orders") == "`my-catalog`.`bronze`.`orders`"

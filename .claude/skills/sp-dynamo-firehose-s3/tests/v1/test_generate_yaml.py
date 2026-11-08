import json
import re
import subprocess
from copy import deepcopy
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from sp_dynamo_firehose_s3.v1 import generate, generator, main
from sp_dynamo_firehose_s3.v1.config import check_quartz_cron
from sp_dynamo_firehose_s3.v1.defaults import DEFAULTS, LayerDefaults
from sp_dynamo_firehose_s3.v1.generator import (
    SAMPLE_BATCH_SCHEDULE,
    default_pipeline_name,
    render_config_template,
)

FIXTURES = Path(__file__).parent / "fixtures"
REQUIRED = "# Required field, please fill."
BATCH_ONLY = "# This field only applies when executionMode is batch."


@pytest.fixture
def template(orders_schema):
    return render_config_template(orders_schema, "orders")


@pytest.fixture
def config(template):
    return yaml.safe_load(template)


def fill_required(config):
    orders = yaml.safe_load((FIXTURES / "orders.yaml").read_text())
    config = deepcopy(config)
    config["metadata"].update(
        name=orders["metadata"]["name"],
        description=orders["metadata"]["description"],
    )
    for layer in ("bronze", "silver", "gold"):
        for key in ("sourcePath", "checkpointPath", "targetTable"):
            if key in config["job"][layer]:
                config["job"][layer][key] = orders["job"][layer][key]
    config["schema"] = orders["schema"]
    return config


def test_template_matches_expected_file(template):
    assert template == (FIXTURES / "expected" / "pipeline.yaml").read_text()


def test_template_is_deterministic(orders_schema, template):
    assert render_config_template(orders_schema, "orders") == template


def test_required_fields_are_empty_and_marked(template, config):
    lines = [line for line in template.splitlines() if REQUIRED in line]
    keys = [line.split(":")[0].strip() for line in lines]

    assert keys == [
        "description",
        "sourcePath",
        "checkpointPath",
        "targetTable",
        "checkpointPath",
        "targetTable",
        "checkpointPath",
        "targetTable",
        *["targetType"] * len(config["schema"]["columns"]),
    ]
    assert all(re.search(r': ""  #', line) for line in lines)


def test_template_uses_continuous_mode_with_batch_fields_commented_out(
    template, config
):
    assert config["job"]["executionMode"] == "continuous"
    assert f'  # batchSchedule: "0 0 2 * * ?"  {BATCH_ONLY}\n' in template
    assert "batchSchedule" not in config["job"]


def test_sample_batch_schedule_is_valid():
    check_quartz_cron(SAMPLE_BATCH_SCHEDULE)  # raises if invalid


def test_template_has_the_inferred_schema(orders_schema, config):
    columns = config["schema"]["columns"]

    assert [column["path"] for column in columns] == [
        column.path for column in orders_schema.columns
    ]
    assert config["schema"]["primaryKeys"] == list(orders_schema.primary_keys)


def test_filled_template_scaffolds_the_same_project_as_orders(tmp_path, config):
    orders = yaml.safe_load((FIXTURES / "orders.yaml").read_text())
    expected = {
        path.relative_to(tmp_path / "orders"): path.read_text()
        for path in generate(orders, tmp_path / "orders")
    }

    filled = fill_required(config)
    actual = {
        path.relative_to(tmp_path / "filled"): path.read_text()
        for path in generate(filled, tmp_path / "filled")
    }

    assert actual == expected


def test_template_works_in_batch_mode_once_batch_fields_are_set(tmp_path, template):
    source = template.replace('executionMode: "continuous"', 'executionMode: "batch"')
    source = source.replace("  # batchSchedule:", "  batchSchedule:")
    source = re.sub(r"\n    triggerInterval: .*", "", source)

    generate(fill_required(yaml.safe_load(source)), tmp_path)


def test_unfilled_template_fails_on_required_fields(tmp_path, config):
    config["schema"] = yaml.safe_load((FIXTURES / "orders.yaml").read_text())["schema"]

    with pytest.raises(ValidationError) as error_info:
        generate(config, tmp_path)

    message = str(error_info.value)
    for field in ("metadata.description", "bronze.sourcePath", "gold.targetTable"):
        assert field in message


def test_action_writes_the_template(tmp_path, capsys, template):
    path = tmp_path / "pipelines" / "orders.yaml"

    assert main(["generate-yaml", str(path)]) == 0

    assert path.read_text() == template
    assert capsys.readouterr().out == (
        f"generate-yaml {path}\n"
        "Result: GENERATED\n"
        f"  Wrote {path} with every supported field and 11 columns inferred from "
        "DynamoDB table orders.\n"
        "Next step: Fill in the fields marked `# Required field, please fill.`, "
        f"including each column's targetType, then run scaffold {path}.\n"
    )


def test_action_does_not_overwrite_without_force(tmp_path, capsys):
    path = tmp_path / "customers.yaml"
    path.write_text("custom")

    assert main(["generate-yaml", str(path)]) == 1

    assert path.read_text() == "custom"
    output = capsys.readouterr().out
    assert "Result: FILE_EXISTS\n" in output
    assert "Next step: Rerun with --force to overwrite it.\n" in output


def test_action_overwrites_with_force(tmp_path, template):
    path = tmp_path / "orders.yaml"
    path.write_text("custom")

    assert main(["generate-yaml", str(path), "--force"]) == 0

    assert path.read_text() == template


def test_duplicate_column_names_are_called_out(tmp_path, fake_aws, capsys):
    items = fake_aws_items_with_collision()
    fake_aws.result = None
    path = tmp_path / "orders.yaml"
    original = fake_aws.run

    def run(command, **kwargs):
        if command[2] == "scan":
            return subprocess.CompletedProcess(
                command, 0, stdout=json.dumps({"Items": items})
            )
        return original(command, **kwargs)

    fake_aws.run = run

    assert main(["generate-yaml", str(path)]) == 0
    assert (
        "Columns named zip_code come from more than one path; rename them."
        in capsys.readouterr().out
    )


def fake_aws_items_with_collision():
    return [
        {
            "id": {"N": "1"},
            "createdAt": {"S": "x"},
            "zipCode": {"S": "1"},
            "zip_code": {"S": "2"},
        }
    ]


def test_missing_table_has_a_fixed_result(tmp_path, fake_aws, capsys):
    fake_aws.result = subprocess.CompletedProcess(
        [], 254, stderr="An error occurred (ResourceNotFoundException): not found"
    )

    assert main(["generate-yaml", str(tmp_path / "missing.yaml")]) == 1
    output = capsys.readouterr().out
    assert "Result: TABLE_NOT_FOUND\n" in output
    assert "DynamoDB table missing does not exist in us-east-1" in output


@pytest.mark.parametrize(
    ("table", "name"),
    [
        ("orders", "orders"),
        ("bo.accounts", "bo-accounts"),
        ("bo_accounts", "bo-accounts"),
        ("Customer.Orders-V2", "customer-orders-v2"),
        ("a..b", "a--b"),
        ("1orders", ""),
        ("_orders", ""),
    ],
)
def test_pipeline_name_comes_from_the_table_name(table, name):
    assert default_pipeline_name(table) == name


def test_name_is_filled_from_the_file_name(orders_schema):
    source = render_config_template(orders_schema, "bo.accounts")

    assert '  name: "bo-accounts"\n' in source


def test_name_that_cannot_be_derived_is_required(orders_schema):
    source = render_config_template(orders_schema, "1orders")

    assert f'  name: ""  {REQUIRED}\n' in source


def test_layer_defaults_fill_the_template(orders_schema, monkeypatch):
    layer = LayerDefaults(
        checkpoint_path="s3://{env}-lake/cp/", target_table="{env}.bronze.t"
    )
    job = DEFAULTS.job.model_copy(update={"bronze": layer})
    monkeypatch.setattr(generator, "DEFAULTS", DEFAULTS.model_copy(update={"job": job}))

    config = yaml.safe_load(render_config_template(orders_schema, "orders"))

    assert config["job"]["bronze"]["checkpointPath"] == "s3://{env}-lake/cp/"
    assert config["job"]["bronze"]["targetTable"] == "{env}.bronze.t"
    assert config["job"]["silver"]["targetTable"] == ""

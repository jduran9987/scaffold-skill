import re
from pathlib import Path

import pytest
import yaml

from sp_dynamo_firehose_s3.v1 import generate
from sp_dynamo_firehose_s3.v1.config import PipelineConfig
from sp_dynamo_firehose_s3.v1.generator import LAYER_SUMMARIES, render_files

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def raw_config():
    return yaml.safe_load((FIXTURES / "orders.yaml").read_text())


def readme(raw_config):
    return render_files(PipelineConfig.model_validate(raw_config))["README.md"]


def section(text, title):
    return text.split(f"## {title}\n", 1)[1].split("\n## ", 1)[0]


def column_rows(text):
    return [line for line in text.splitlines() if line.startswith("| `")]


def test_readme_matches_expected_file(raw_config):
    assert readme(raw_config) == (FIXTURES / "expected" / "README.md").read_text()


def test_layer_summaries_are_the_same_in_every_project(raw_config):
    other = yaml.safe_load((FIXTURES / "orders.yaml").read_text())
    other["metadata"].update(name="payments", description="Payments.")
    other["schema"]["columns"] = other["schema"]["columns"][:2]
    other["job"]["executionMode"] = "batch"
    other["job"]["batchSchedule"] = "0 0 2 * * ?"
    for layer in ("bronze", "silver", "gold"):
        del other["job"][layer]["triggerInterval"]

    for config in (raw_config, other):
        text = readme(config)
        for layer, summary in LAYER_SUMMARIES.items():
            assert section(text, layer.capitalize()).startswith(f"\n{summary}\n")


def test_each_section_lists_the_table_for_every_target(raw_config):
    gold = section(readme(raw_config), "Gold")

    for target in ("dev", "qa", "uat", "prod"):
        assert f"| {target} | `{target}.gold.orders` |" in gold
    assert "{env}" not in readme(raw_config)


def test_columns_match_the_ddl(raw_config):
    files = render_files(PipelineConfig.model_validate(raw_config))

    for layer in ("bronze", "silver", "gold"):
        ddl_columns = re.findall(r"^  `([^`]+)`", files[f"ddl/dev/{layer}.sql"], re.M)
        rows = column_rows(section(files["README.md"], layer.capitalize()))
        assert [row.split("`")[1] for row in rows] == ddl_columns


def test_descriptions_come_from_the_schema_or_are_blank(raw_config):
    del raw_config["schema"]["columns"][3]["description"]

    silver = section(readme(raw_config), "Silver")

    assert "| `amount` | DECIMAL(22, 2) | Order amount in dollars. |" in silver
    assert "| `status` | STRING |  |" in silver


def test_pipes_in_descriptions_do_not_break_the_table(raw_config):
    raw_config["schema"]["columns"][3]["description"] = "PENDING | SHIPPED"

    assert "| `status` | STRING | PENDING \\| SHIPPED |" in readme(raw_config)


def test_only_configured_layers_get_a_section(raw_config):
    del raw_config["job"]["gold"]

    text = readme(raw_config)

    assert "## Silver" in text
    assert "## Gold" not in text


def test_readme_replaces_the_empty_one_from_uv_init(raw_config, tmp_path):
    generate(raw_config, tmp_path)

    assert (tmp_path / "README.md").read_text().startswith("# orders\n")

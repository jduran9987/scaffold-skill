import subprocess
import sys
import tomllib
from copy import deepcopy
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from sp_dynamo_firehose_s3.v1 import ProjectInitError, generate, generator

FIXTURES = Path(__file__).parent / "fixtures"
JOB_FILES = ["bronze.py", "gold.py", "silver.py"]
SHARED_FILES = ["cli.py", "env.py", "logs.py"]
PACKAGE_DIR = "src/orders"
INIT_FILE = f"{PACKAGE_DIR}/__init__.py"
JOB_RESOURCE = "resource/orders.job.yml"
BUNDLE_FILES = ["databricks.yml", JOB_RESOURCE]
TARGETS = ["dev", "qa", "uat", "prod"]


def ddl_files(*layers):
    return [f"ddl/{target}/{layer}.sql" for target in TARGETS for layer in layers]


DDL_FILES = ddl_files("bronze", "silver", "gold")
ALL_FILES = sorted(
    [
        *BUNDLE_FILES,
        *DDL_FILES,
        "README.md",
        INIT_FILE,
        *(f"{PACKAGE_DIR}/{name}" for name in [*SHARED_FILES, *JOB_FILES]),
        "pyproject.toml",
    ]
)


def job_path(name):
    return f"{PACKAGE_DIR}/{name}"


@pytest.fixture
def raw_config():
    return yaml.safe_load((FIXTURES / "orders.yaml").read_text())


def to_batch(config):
    config = deepcopy(config)
    config["job"]["executionMode"] = "batch"
    config["job"]["batchSchedule"] = "0 0 2 * * ?"
    for layer in ("bronze", "silver", "gold"):
        if layer in config["job"]:
            del config["job"][layer]["triggerInterval"]
    return config


def generated_sources(raw_config, output_dir):
    return {
        path.relative_to(output_dir).as_posix(): path.read_text()
        for path in generate(raw_config, output_dir)
    }


@pytest.mark.parametrize("name", [*SHARED_FILES, *JOB_FILES])
def test_continuous_matches_expected_file(raw_config, tmp_path, name):
    expected = (FIXTURES / "expected" / name).read_text()
    assert generated_sources(raw_config, tmp_path)[job_path(name)] == expected


@pytest.mark.parametrize("name", JOB_FILES)
def test_batch_renders_only_the_batch_trigger(raw_config, tmp_path, name):
    source = generated_sources(to_batch(raw_config), tmp_path)[job_path(name)]

    assert ".trigger(availableNow=True)" in source
    assert "processingTime" not in source
    assert "TRIGGER_INTERVAL" not in source


@pytest.mark.parametrize("batch", [False, True])
def test_output_is_valid_formatted_python(raw_config, tmp_path, batch):
    config = to_batch(raw_config) if batch else raw_config

    sources = generated_sources(config, tmp_path)

    assert sorted(sources) == ALL_FILES
    for name in [*SHARED_FILES, *JOB_FILES]:
        source = sources[job_path(name)]
        compile(source, name, "exec")
        assert all(len(line) <= 88 for line in source.splitlines())
    subprocess.run(
        [
            sys.executable,
            "-m",
            "ruff",
            "format",
            "--check",
            "--isolated",
            "--line-length",
            "88",
            str(tmp_path),
        ],
        check=True,
    )


@pytest.mark.parametrize("name", BUNDLE_FILES)
def test_bundle_matches_expected_file(raw_config, tmp_path, name):
    expected = (FIXTURES / "expected" / name).read_text()
    assert generated_sources(raw_config, tmp_path)[name] == expected


def load_job(raw_config, tmp_path):
    resource = yaml.safe_load(generated_sources(raw_config, tmp_path)[JOB_RESOURCE])
    return resource["resources"]["jobs"]["orders"]


def test_job_settings_come_from_yaml(raw_config, tmp_path):
    raw_config["bundle"]["tags"] = {"team": "data"}

    job = load_job(raw_config, tmp_path)

    assert job["tags"] == {"team": "data"}
    assert job["job_clusters"][0]["new_cluster"]["policy_id"] == (
        "${var.compute_policy_id}"
    )
    assert [task["python_wheel_task"]["entry_point"] for task in job["tasks"]] == [
        "bronze",
        "silver",
        "gold",
    ]


def test_job_description_starts_with_metadata_description(raw_config, tmp_path):
    raw_config["metadata"]["description"] = "Loads things."
    del raw_config["job"]["gold"]

    description = load_job(raw_config, tmp_path)["description"]

    assert description.startswith("Loads things. The bronze task ")
    assert "silver task" in description
    assert "gold task" not in description


def test_tags_have_a_default(raw_config, tmp_path):
    del raw_config["bundle"]["tags"]

    assert load_job(raw_config, tmp_path)["tags"] == {
        "project": "orders",
        "SOURCE": "dynamo",
    }


def test_empty_tags_are_left_out(raw_config, tmp_path):
    raw_config["bundle"]["tags"] = {}

    assert "tags" not in load_job(raw_config, tmp_path)


def test_tag_keys_are_unquoted(raw_config, tmp_path):
    raw_config["bundle"]["tags"] = {"SOURCE": "dynamo", "cost:center": "a b/c"}

    source = generated_sources(raw_config, tmp_path)[JOB_RESOURCE]

    assert '        SOURCE: "dynamo"\n' in source
    assert '        cost:center: "a b/c"\n' in source
    job = load_job(raw_config, tmp_path / "again")
    assert job["tags"] == {"SOURCE": "dynamo", "cost:center": "a b/c"}


def test_target_placeholder_in_tags_becomes_the_bundle_target(raw_config, tmp_path):
    raw_config["bundle"]["tags"] = {"environment": "{target}", "app": "orders-{target}"}

    source = generated_sources(raw_config, tmp_path)[JOB_RESOURCE]

    assert '        environment: "${bundle.target}"\n' in source
    assert '        app: "orders-${bundle.target}"\n' in source


def test_tag_value_with_target_fits_for_every_target(raw_config, tmp_path):
    raw_config["bundle"]["tags"] = {"team": "v" * 251 + "{target}"}

    job = load_job(raw_config, tmp_path)

    assert job["tags"] == {"team": "v" * 251 + "${bundle.target}"}


@pytest.mark.parametrize(
    "tags",
    [
        {"true": "x"},
        {"No": "x"},
        {"1team": "x"},
        {"team name": "x"},
        {"#team": "x"},
        {"team": 1},
        {"team": None},
        {"team": "a#b"},
        {"k" * 128: "x"},
        {"team": "v" * 256},
        {"team": "v" * 252 + "{target}"},
        {"env": "{env}"},
        {"env": "{Target}"},
        {"env": "${bundle.target}"},
        {f"tag{i}": "x" for i in range(26)},
        [{"team": "x"}],
    ],
)
def test_invalid_tags_are_rejected(raw_config, tmp_path, tags):
    raw_config["bundle"]["tags"] = tags

    with pytest.raises(ValidationError, match="tags"):
        generate(raw_config, tmp_path)


def test_continuous_job_runs_tasks_together(raw_config, tmp_path):
    job = load_job(raw_config, tmp_path)

    assert job["continuous"] == {
        "pause_status": "PAUSED",
        "task_retry_mode": "ON_FAILURE",
    }
    assert "schedule" not in job
    assert all("depends_on" not in task for task in job["tasks"])


def test_batch_job_runs_tasks_in_order(raw_config, tmp_path):
    job = load_job(to_batch(raw_config), tmp_path)

    assert "continuous" not in job
    assert job["schedule"] == {
        "quartz_cron_expression": "0 0 2 * * ?",
        "timezone_id": "UTC",
        "pause_status": "PAUSED",
    }
    assert [task.get("depends_on") for task in job["tasks"]] == [
        None,
        [{"task_key": "bronze"}],
        [{"task_key": "silver"}],
    ]


def test_hyphenated_name_uses_underscores_in_job_keys(raw_config, tmp_path):
    raw_config["metadata"]["name"] = "customer-events"

    sources = generated_sources(raw_config, tmp_path)
    resource = yaml.safe_load(sources["resource/customer-events.job.yml"])
    job = resource["resources"]["jobs"]["customer_events"]

    assert job["name"] == "[${bundle.target}] customer-events"
    assert job["job_clusters"][0]["job_cluster_key"] == "customer_events_cluster"
    assert job["tasks"][0]["python_wheel_task"]["package_name"] == "customer_events"


def test_bundle_names_come_from_metadata(raw_config, tmp_path):
    raw_config["metadata"]["name"] = "customer-events"

    bundle = yaml.safe_load(generated_sources(raw_config, tmp_path)["databricks.yml"])

    assert bundle["bundle"] == {"name": "customer-events", "engine": "direct"}
    assert bundle["artifacts"] == {
        "customer_events_wheel": {
            "type": "whl",
            "build": "uv build --wheel",
            "path": ".",
        }
    }


def dev_target(raw_config):
    return raw_config["bundle"]["targets"][0]["dev"]


def test_targets_come_from_bundle(raw_config, tmp_path):
    bundle = yaml.safe_load(generated_sources(raw_config, tmp_path)["databricks.yml"])

    assert list(bundle["variables"]) == [
        "environment",
        "compute_policy_id",
        "run-as",
        "job_failure_webhooks",
        "job_failure_emails",
        "task_failure_webhooks",
        "task_failure_emails",
    ]
    assert list(bundle["targets"]) == ["dev", "qa", "uat", "prod"]
    prod = bundle["targets"]["prod"]
    assert prod["variables"] == {
        "environment": "prod",
        "compute_policy_id": "PRODPOLICY",
        "run-as": {"lookup": {"service_principal": "sp_ingestion_prod"}},
        "job_failure_webhooks": [{"id": "XXXXXXXXX"}],
        "job_failure_emails": ["jduran9987@gmail.com"],
        "task_failure_webhooks": [{"id": "XXXXXXXXX"}],
        "task_failure_emails": ["jduran9987@gmail.com"],
    }
    assert prod["run_as"] == {"service_principal_name": "${var.run-as}"}
    assert prod["workspace"]["root_path"] == (
        "/Workspace/Users/${var.run-as}/.bundle/${bundle.name}/${bundle.target}"
    )


def test_permissions_come_from_the_target(raw_config, tmp_path):
    dev_target(raw_config).update(
        servicePrincipalPermissions="can_run",
        manageGroups=["leads"],
        runGroups=[],
        viewGroups=["analysts", "auditors"],
    )

    bundle = yaml.safe_load(generated_sources(raw_config, tmp_path)["databricks.yml"])

    assert bundle["targets"]["dev"]["permissions"] == [
        {"service_principal_name": "${var.run-as}", "level": "CAN_RUN"},
        {"group_name": "leads", "level": "CAN_MANAGE"},
        {"group_name": "analysts", "level": "CAN_VIEW"},
        {"group_name": "auditors", "level": "CAN_VIEW"},
    ]


def test_permissions_default_per_target(raw_config, tmp_path):
    for key in (
        "servicePrincipalPermissions",
        "manageGroups",
        "runGroups",
        "viewGroups",
    ):
        del dev_target(raw_config)[key]
    dev_target(raw_config)["servicePrincipal"] = "sp_custom"

    bundle = yaml.safe_load(generated_sources(raw_config, tmp_path)["databricks.yml"])

    dev = bundle["targets"]["dev"]
    assert dev["variables"]["run-as"] == {"lookup": {"service_principal": "sp_custom"}}
    assert dev["permissions"] == [
        {"service_principal_name": "${var.run-as}", "level": "CAN_MANAGE"},
        {"group_name": "data_engineers", "level": "CAN_MANAGE"},
        {"group_name": "admins", "level": "CAN_MANAGE"},
        {"group_name": "data_analysts", "level": "CAN_RUN"},
    ]


def test_service_principal_is_the_only_grant_without_groups(raw_config, tmp_path):
    dev_target(raw_config).update(manageGroups=[], runGroups=[], viewGroups=[])

    bundle = yaml.safe_load(generated_sources(raw_config, tmp_path)["databricks.yml"])

    assert bundle["targets"]["dev"]["permissions"] == [
        {"service_principal_name": "${var.run-as}", "level": "CAN_MANAGE"}
    ]


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda c: c["bundle"].update(targets={"dev": {}}), "list of single-key"),
        (
            lambda c: c["bundle"]["targets"].append({"staging": {}}),
            "must be one of dev, qa, uat, prod, not: staging",
        ),
        (lambda c: c["bundle"]["targets"].append({"dev": "x"}), "must not repeat"),
        (lambda c: c["bundle"]["targets"].__setitem__(0, {"dev": "x"}), "targets.dev"),
        (
            lambda c: c["bundle"]["targets"].append(
                deepcopy(c["bundle"]["targets"][0])
            ),
            "must not repeat",
        ),
        (
            lambda c: c["bundle"]["targets"][0].update(
                qa2=deepcopy(c["bundle"]["targets"][1]["qa"])
            ),
            "exactly one target",
        ),
        (
            lambda c: c["bundle"]["targets"][0].update(
                Dev=c["bundle"]["targets"][0].pop("dev")
            ),
            "targets",
        ),
        (lambda c: dev_target(c).update(host="http://x.com"), "host"),
        (lambda c: c["bundle"].update(tags=[{"a": "b"}]), "tags"),
        (lambda c: c["bundle"].update(computePolicy="x"), "computePolicy"),
        (lambda c: dev_target(c).update(computePolicy=""), "computePolicy"),
        (lambda c: dev_target(c).update(compute_policy="x"), "compute_policy"),
        (
            lambda c: dev_target(c).update(servicePrincipalPermissions="can_view"),
            "servicePrincipalPermissions",
        ),
        (
            lambda c: dev_target(c).update(servicePrincipalPermissions="CAN_MANAGE"),
            "servicePrincipalPermissions",
        ),
        (
            lambda c: dev_target(c).update(viewGroups=["data_analysts"]),
            "listed once across manage, run, and view groups, not: data_analysts",
        ),
        (
            lambda c: dev_target(c).update(runGroups=["g", "g"]),
            "not: g",
        ),
        (lambda c: dev_target(c).update(viewGroups=[""]), "viewGroups"),
        (lambda c: dev_target(c).update(permissions={}), "permissions"),
    ],
)
def test_invalid_bundle_settings_are_rejected(raw_config, tmp_path, change, message):
    change(raw_config)

    with pytest.raises(ValidationError, match=message):
        generate(raw_config, tmp_path)


def test_silver_columns_come_from_schema(raw_config, tmp_path):
    namespace = {}
    source = generated_sources(raw_config, tmp_path)[job_path("silver.py")]
    start = source.index("@dataclass(frozen=True, slots=True)\nclass ColumnSpec")
    end = source.index("\nVARIANT_COLUMN")
    exec("from dataclasses import dataclass\n" + source[start:end], namespace)

    columns = namespace["COLUMNS"]
    assert [column.name for column in columns] == [
        column["name"] for column in raw_config["schema"]["columns"]
    ]
    assert [column.name for column in columns if column.key] == ["id", "created_at"]
    assert namespace["SOURCE_TABLE"] == raw_config["job"]["bronze"]["targetTable"]
    city = next(column for column in columns if column.name == "address__city")
    assert (city.dynamo_type, city.target_type, city.path) == (
        "S",
        "string",
        "addressDetails.M.city",
    )


def test_silver_is_optional(raw_config, tmp_path):
    del raw_config["job"]["silver"]
    del raw_config["job"]["gold"]
    del raw_config["schema"]

    names = list(generated_sources(raw_config, tmp_path))

    assert names == [
        *BUNDLE_FILES,
        *ddl_files("bronze"),
        "README.md",
        INIT_FILE,
        *map(job_path, SHARED_FILES),
        job_path("bronze.py"),
        "pyproject.toml",
    ]


def test_gold_is_optional(raw_config, tmp_path):
    del raw_config["job"]["gold"]

    names = list(generated_sources(raw_config, tmp_path))

    assert names == [
        *BUNDLE_FILES,
        *ddl_files("bronze", "silver"),
        "README.md",
        INIT_FILE,
        *map(job_path, SHARED_FILES),
        job_path("bronze.py"),
        job_path("silver.py"),
        "pyproject.toml",
    ]


def test_gold_settings_come_from_yaml(raw_config, tmp_path):
    raw_config["schema"]["primaryKeys"] = ["id"]
    raw_config["job"]["gold"]["upsertLatestKey"] = "_ingested_at"

    source = generated_sources(raw_config, tmp_path)[job_path("gold.py")]
    start = source.index("SOURCE_TABLE =")
    end = source.index("\nROW_NUMBER_COLUMN")
    namespace = {}
    exec(source[start:end], namespace)

    assert namespace["SOURCE_TABLE"] == raw_config["job"]["silver"]["targetTable"]
    assert namespace["TARGET_TABLE"] == raw_config["job"]["gold"]["targetTable"]
    assert namespace["PRIMARY_KEYS"] == ("id",)
    assert namespace["UPSERT_LATEST_KEY"] == "_ingested_at"


def test_silver_requires_schema(raw_config, tmp_path):
    del raw_config["schema"]

    with pytest.raises(ValidationError, match="schema is required"):
        generate(raw_config, tmp_path)


def test_values_with_quotes_stay_valid_python(raw_config, tmp_path):
    raw_config["job"]["bronze"]["sourcePath"] = 's3://bucket/a"b\\c/'

    source = generated_sources(raw_config, tmp_path)[job_path("bronze.py")]

    namespace = {}
    line = next(ln for ln in source.splitlines() if ln.startswith("SOURCE_PATH"))
    exec(line, namespace)
    assert namespace["SOURCE_PATH"] == 's3://bucket/a"b\\c/'


@pytest.mark.parametrize("layer", ["bronze", "silver", "gold"])
def test_continuous_trigger_interval_defaults_to_one_minute(
    raw_config, tmp_path, layer
):
    del raw_config["job"][layer]["triggerInterval"]

    source = generated_sources(raw_config, tmp_path)[job_path(f"{layer}.py")]

    assert 'TRIGGER_INTERVAL = "1 minute"' in source


@pytest.mark.parametrize("layer", ["bronze", "silver", "gold"])
def test_continuous_trigger_interval_comes_from_yaml(raw_config, tmp_path, layer):
    raw_config["job"][layer]["triggerInterval"] = "5 minutes"

    source = generated_sources(raw_config, tmp_path)[job_path(f"{layer}.py")]

    assert 'TRIGGER_INTERVAL = "5 minutes"' in source


def test_execution_mode_defaults_to_continuous(raw_config, tmp_path):
    expected = generated_sources(raw_config, tmp_path / "full")
    del raw_config["job"]["executionMode"]
    for layer in ("bronze", "silver", "gold"):
        del raw_config["job"][layer]["triggerInterval"]

    assert generated_sources(raw_config, tmp_path / "defaults") == expected


def test_upsert_latest_key_defaults_to_event_timestamp(raw_config, tmp_path):
    del raw_config["job"]["gold"]["upsertLatestKey"]

    source = generated_sources(raw_config, tmp_path)[job_path("gold.py")]

    assert 'UPSERT_LATEST_KEY = "_event_timestamp"' in source


@pytest.mark.parametrize(
    ("layer", "key"),
    [
        ("bronze", "sourcePath"),
        ("bronze", "checkpointPath"),
        ("bronze", "targetTable"),
        ("silver", "checkpointPath"),
        ("silver", "targetTable"),
        ("gold", "checkpointPath"),
        ("gold", "targetTable"),
    ],
)
def test_layer_paths_and_tables_are_required(raw_config, tmp_path, layer, key):
    del raw_config["job"][layer][key]

    with pytest.raises(ValidationError, match=f"{layer}.{key}"):
        generate(raw_config, tmp_path)


@pytest.mark.parametrize("layer", ["bronze", "silver", "gold"])
def test_batch_rejects_trigger_interval(raw_config, tmp_path, layer):
    config = to_batch(raw_config)
    config["job"][layer]["triggerInterval"] = "1 minute"

    with pytest.raises(ValidationError, match=f"{layer}.triggerInterval is not"):
        generate(config, tmp_path)


@pytest.mark.parametrize(
    ("section", "key", "value"),
    [
        ("job", "executionMode", "streaming"),
        ("job", "platinum", {}),
        ("gold", "checkpointPath", "/not/s3"),
        ("gold", "targetTable", "two.parts"),
        ("gold", "sourceTable", "a.b.c"),
        ("gold", "upsertLatestKey", "bad-name"),
        ("bronze", "sourcePath", "/not/s3"),
        ("bronze", "checkpointPath", "/not/s3"),
        ("bronze", "targetTable", "two.parts"),
        ("bronze", "unknownField", "x"),
        ("silver", "checkpointPath", "/not/s3"),
        ("silver", "targetTable", "two.parts"),
        ("silver", "sourceTable", "a.b.c"),
        ("metadata", "name", "../escape"),
        ("column", "dynamoType", "M"),
        ("column", "targetType", "int); DROP TABLE x; --"),
        ("column", "path", "address'Details"),
        ("column", "name", "bad-name"),
        ("column", "unknownField", "x"),
        ("key_column", "path", "nested.M.id"),
    ],
)
def test_rejects_invalid_values(raw_config, tmp_path, section, key, value):
    targets = {
        "job": raw_config["job"],
        "bronze": raw_config["job"]["bronze"],
        "silver": raw_config["job"]["silver"],
        "gold": raw_config["job"]["gold"],
        "metadata": raw_config["metadata"],
        "column": raw_config["schema"]["columns"][2],
        "key_column": raw_config["schema"]["columns"][0],
    }
    targets[section][key] = value

    with pytest.raises(ValidationError):
        generate(raw_config, tmp_path / "out")
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda s: s["columns"].append(dict(s["columns"][0])), "duplicate column"),
        (lambda s: s["columns"][2].update(name="_ingested_at"), "should match"),
        (lambda s: s["primaryKeys"].append("missing"), "primary keys are not"),
        (lambda s: s["primaryKeys"].append("id"), "must not repeat"),
        (lambda s: s.update(columns=[]), "at least 1"),
    ],
)
def test_rejects_invalid_schema(raw_config, tmp_path, change, message):
    change(raw_config["schema"])

    with pytest.raises(ValidationError, match=message):
        generate(raw_config, tmp_path)


def test_gold_requires_silver(raw_config, tmp_path):
    del raw_config["job"]["silver"]

    with pytest.raises(ValidationError, match="job.silver is required"):
        generate(raw_config, tmp_path)


@pytest.mark.parametrize("key", ["_event_timestamp", "_ingested_at", "amount"])
def test_gold_upsert_key_can_be_any_silver_column(raw_config, tmp_path, key):
    raw_config["job"]["gold"]["upsertLatestKey"] = key

    assert len(generate(raw_config, tmp_path)) == len(ALL_FILES)


def test_gold_upsert_key_must_be_a_silver_column(raw_config, tmp_path):
    raw_config["job"]["gold"]["upsertLatestKey"] = "missing"

    with pytest.raises(ValidationError, match="not a silver column"):
        generate(raw_config, tmp_path)


@pytest.mark.parametrize("target_type", ["decimal(22, 2)", "decimal(10,0)", "bigint"])
def test_accepts_supported_target_types(raw_config, tmp_path, target_type):
    raw_config["schema"]["columns"][2]["targetType"] = target_type

    assert len(generate(raw_config, tmp_path)) == len(ALL_FILES)


def test_does_not_overwrite_any_file_without_force(raw_config, tmp_path):
    silver_path = tmp_path / job_path("silver.py")
    silver_path.parent.mkdir(parents=True)
    silver_path.write_text("custom")

    with pytest.raises(FileExistsError, match="silver.py"):
        generate(raw_config, tmp_path)
    assert silver_path.read_text() == "custom"
    assert not (tmp_path / job_path("bronze.py")).exists()
    assert not (tmp_path / "pyproject.toml").exists()


def test_force_overwrites(raw_config, tmp_path):
    generate(raw_config, tmp_path)
    for name in JOB_FILES:
        (tmp_path / job_path(name)).write_text("custom")

    generate(raw_config, tmp_path, force=True)

    assert all(
        "custom" not in (tmp_path / job_path(name)).read_text() for name in JOB_FILES
    )


def test_default_output_is_under_dabs(raw_config, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    paths = generate(raw_config)

    project_dir = tmp_path / "dabs" / "orders"
    assert sorted(paths) == sorted(project_dir / name for name in ALL_FILES)


def test_project_is_a_uv_package_with_job_entry_points(raw_config, tmp_path):
    raw_config["metadata"]["pythonVersion"] = "3.13"
    del raw_config["job"]["gold"]

    sources = generated_sources(raw_config, tmp_path)

    project = tomllib.loads(sources["pyproject.toml"])["project"]
    assert project["name"] == "orders"
    assert project["description"] == "Pipeline to ingest orders."
    assert project["requires-python"] == ">=3.13"
    assert project["scripts"] == {
        "bronze": "orders.bronze:main",
        "silver": "orders.silver:main",
    }
    assert (tmp_path / ".python-version").read_text().strip() == "3.13"
    assert "def main" not in sources[INIT_FILE]
    assert [path.name for path in (tmp_path / "src").iterdir()] == ["orders"]


def test_rerun_keeps_existing_project_and_resets_scripts(raw_config, tmp_path):
    generate(raw_config, tmp_path)
    project_file = tmp_path / "pyproject.toml"
    edited = project_file.read_text().replace(
        "dependencies = []", 'dependencies = ["requests"]'
    )
    project_file.write_text(edited.replace('gold = "orders.gold:main"', ""))
    del raw_config["job"]["gold"]

    generate(raw_config, tmp_path, force=True)

    project = tomllib.loads(project_file.read_text())["project"]
    assert project["dependencies"] == ["requests"]
    assert project["scripts"] == {
        "bronze": "orders.bronze:main",
        "silver": "orders.silver:main",
    }


def test_hyphenated_name_uses_underscored_package(raw_config, tmp_path):
    raw_config["metadata"]["name"] = "customer-events"

    sources = generated_sources(raw_config, tmp_path)

    assert "src/customer_events/bronze.py" in sources
    scripts = tomllib.loads(sources["pyproject.toml"])["project"]["scripts"]
    assert scripts["bronze"] == "customer_events.bronze:main"


def test_uv_init_failure_is_reported(raw_config, tmp_path, monkeypatch):
    def fail(command, **kwargs):
        return subprocess.CompletedProcess(command, 2, stderr="boom")

    monkeypatch.setattr(generator.subprocess, "run", fail)

    with pytest.raises(ProjectInitError, match="uv init failed: boom"):
        generate(raw_config, tmp_path)


@pytest.mark.parametrize("version", [3.12, "3", "python3.12"])
def test_python_version_must_be_a_quoted_version(raw_config, tmp_path, version):
    raw_config["metadata"]["pythonVersion"] = version

    with pytest.raises(ValidationError, match="pythonVersion"):
        generate(raw_config, tmp_path)


def test_python_version_defaults_to_3_12(raw_config, tmp_path):
    del raw_config["metadata"]["pythonVersion"]

    sources = generated_sources(raw_config, tmp_path)

    project = tomllib.loads(sources["pyproject.toml"])["project"]
    assert project["requires-python"] == ">=3.12"


@pytest.mark.parametrize("field", ["name", "description"])
@pytest.mark.parametrize("value", [None, "", 123])
def test_metadata_name_and_description_are_required_strings(
    raw_config, tmp_path, field, value
):
    raw_config["metadata"][field] = value

    with pytest.raises(ValidationError, match=f"metadata.{field}"):
        generate(raw_config, tmp_path)


@pytest.mark.parametrize("field", ["name", "description"])
def test_metadata_name_and_description_cannot_be_omitted(raw_config, tmp_path, field):
    del raw_config["metadata"][field]

    with pytest.raises(ValidationError, match=f"metadata.{field}"):
        generate(raw_config, tmp_path)


def test_uv_build_version_sets_build_backend(raw_config, tmp_path):
    raw_config["metadata"]["uvBuildVersion"] = ">=0.12.0,<0.13.0"

    sources = generated_sources(raw_config, tmp_path)

    assert tomllib.loads(sources["pyproject.toml"])["build-system"] == {
        "requires": ["uv_build>=0.12.0,<0.13.0"],
        "build-backend": "uv_build",
    }


def test_uv_build_version_has_a_default(raw_config, tmp_path):
    del raw_config["metadata"]["uvBuildVersion"]

    sources = generated_sources(raw_config, tmp_path)

    build_system = tomllib.loads(sources["pyproject.toml"])["build-system"]
    assert build_system["requires"] == ["uv_build>=0.13.7,<0.14.0"]


@pytest.mark.parametrize(
    "version", ["0.13.7", ">=0.13.7", "<0.14.0", ">=0.13,<0.14", 0.13]
)
def test_uv_build_version_must_be_a_bounded_range(raw_config, tmp_path, version):
    raw_config["metadata"]["uvBuildVersion"] = version

    with pytest.raises(ValidationError, match="uvBuildVersion"):
        generate(raw_config, tmp_path)


JOB_KEYS = ("onJobFailureWebhookNotificationIds", "onJobFailureEmails")
TASK_KEYS = ("onTaskFailureWebhookNotificationIds", "onTaskFailureEmails")


def prod_target(raw_config):
    return raw_config["bundle"]["targets"][3]["prod"]


def load_targets(raw_config, tmp_path):
    sources = generated_sources(raw_config, tmp_path)
    return yaml.safe_load(sources["databricks.yml"])["targets"]


def test_job_notifications_are_set_on_prod(raw_config, tmp_path):
    prod_target(raw_config)[JOB_KEYS[0]] = ["j1", "j2"]
    prod_target(raw_config)[JOB_KEYS[1]] = ["a@example.com"]

    sources = generated_sources(raw_config, tmp_path)
    prod = yaml.safe_load(sources["databricks.yml"])["targets"]["prod"]["variables"]
    job = yaml.safe_load(sources[JOB_RESOURCE])["resources"]["jobs"]["orders"]

    assert prod["job_failure_webhooks"] == [{"id": "j1"}, {"id": "j2"}]
    assert prod["job_failure_emails"] == ["a@example.com"]
    assert job["webhook_notifications"] == {"on_failure": "${var.job_failure_webhooks}"}
    assert job["email_notifications"] == {"on_failure": "${var.job_failure_emails}"}
    assert job["notification_settings"] == {"no_alert_for_skipped_runs": False}


def drop_notifications(raw_config):
    for key in JOB_KEYS + TASK_KEYS:
        del prod_target(raw_config)[key]


def test_notifications_left_out_are_not_configured(raw_config, tmp_path):
    drop_notifications(raw_config)

    sources = generated_sources(raw_config, tmp_path)
    bundle = yaml.safe_load(sources["databricks.yml"])
    job = yaml.safe_load(sources[JOB_RESOURCE])["resources"]["jobs"]["orders"]

    assert list(bundle["variables"]) == ["environment", "compute_policy_id", "run-as"]
    for target in bundle["targets"].values():
        assert list(target["variables"]) == [
            "environment",
            "compute_policy_id",
            "run-as",
        ]
    for key in (
        "email_notifications",
        "webhook_notifications",
        "notification_settings",
    ):
        assert key not in job
        for task in job["tasks"]:
            assert key not in task


def test_null_notifications_are_not_configured(raw_config, tmp_path):
    expected = generated_sources(raw_config, tmp_path / "full")
    for key in JOB_KEYS + TASK_KEYS:
        prod_target(raw_config)[key] = None
    nulls = generated_sources(raw_config, tmp_path / "nulls")
    drop_notifications(raw_config)

    assert nulls == generated_sources(raw_config, tmp_path / "left-out")
    assert nulls != expected


def test_only_the_notifications_set_are_configured(raw_config, tmp_path):
    drop_notifications(raw_config)
    prod_target(raw_config)["onJobFailureEmails"] = ["a@example.com"]
    prod_target(raw_config)["onTaskFailureWebhookNotificationIds"] = ["w1"]

    sources = generated_sources(raw_config, tmp_path)
    bundle = yaml.safe_load(sources["databricks.yml"])
    job = yaml.safe_load(sources[JOB_RESOURCE])["resources"]["jobs"]["orders"]

    assert list(bundle["variables"])[3:] == [
        "job_failure_emails",
        "task_failure_webhooks",
    ]
    assert bundle["targets"]["prod"]["variables"]["job_failure_emails"] == [
        "a@example.com"
    ]
    assert job["email_notifications"] == {"on_failure": "${var.job_failure_emails}"}
    assert "webhook_notifications" not in job
    assert job["notification_settings"] == {"no_alert_for_skipped_runs": False}
    for task in job["tasks"]:
        assert task["webhook_notifications"] == {
            "on_failure": "${var.task_failure_webhooks}"
        }
        assert "email_notifications" not in task
        assert task["notification_settings"]["alert_on_last_attempt"] is True


def test_task_notifications_are_set_on_prod(raw_config, tmp_path):
    prod_target(raw_config)[TASK_KEYS[0]] = ["w1", "w2"]
    prod_target(raw_config)[TASK_KEYS[1]] = ["prod@example.com"]

    sources = generated_sources(raw_config, tmp_path)
    targets = yaml.safe_load(sources["databricks.yml"])["targets"]
    job = yaml.safe_load(sources[JOB_RESOURCE])["resources"]["jobs"]["orders"]

    assert targets["prod"]["variables"]["task_failure_webhooks"] == [
        {"id": "w1"},
        {"id": "w2"},
    ]
    assert targets["prod"]["variables"]["task_failure_emails"] == ["prod@example.com"]
    for task in job["tasks"]:
        assert task["webhook_notifications"] == {
            "on_failure": "${var.task_failure_webhooks}"
        }
        assert task["email_notifications"] == {
            "on_failure": "${var.task_failure_emails}"
        }
        assert task["notification_settings"]["alert_on_last_attempt"] is True


def test_only_prod_sends_notifications(raw_config, tmp_path):
    bundle = yaml.safe_load(generated_sources(raw_config, tmp_path)["databricks.yml"])

    for name in ("job_failure_webhooks", "job_failure_emails"):
        assert bundle["variables"][name]["default"] == []
    for name in ("task_failure_webhooks", "task_failure_emails"):
        assert bundle["variables"][name]["default"] == []
    for target in ("dev", "qa", "uat"):
        variables = bundle["targets"][target]["variables"]
        assert not any(key.endswith("_failure_webhooks") for key in variables)
        assert not any(key.endswith("_failure_emails") for key in variables)


@pytest.mark.parametrize("key", JOB_KEYS)
def test_notifications_outside_targets_are_rejected(raw_config, tmp_path, key):
    raw_config["bundle"][key] = ["x@example.com"] if key.endswith("Emails") else ["w1"]

    with pytest.raises(ValidationError, match=key):
        generate(raw_config, tmp_path)
    assert not tmp_path.joinpath("pyproject.toml").exists()


@pytest.mark.parametrize("target", [0, 1, 2])
@pytest.mark.parametrize("key", JOB_KEYS + TASK_KEYS)
def test_notifications_on_non_prod_targets_are_rejected(
    raw_config, tmp_path, target, key
):
    [(name, settings)] = raw_config["bundle"]["targets"][target].items()
    settings[key] = ["x@example.com"] if key.endswith("Emails") else ["w1"]

    with pytest.raises(
        ValidationError, match=rf"bundle\.targets\.{name} cannot set {key}"
    ):
        generate(raw_config, tmp_path)
    assert not tmp_path.joinpath("pyproject.toml").exists()


def test_null_notifications_on_non_prod_targets_are_left_out(raw_config, tmp_path):
    expected = generated_sources(raw_config, tmp_path / "full")
    for key in JOB_KEYS + TASK_KEYS:
        dev_target(raw_config)[key] = None

    assert generated_sources(raw_config, tmp_path / "nulls") == expected


@pytest.mark.parametrize("level", ["job", "task"])
@pytest.mark.parametrize(
    ("index", "value"),
    [
        (0, []),
        (0, [""]),
        (0, "w1"),
        (1, []),
        (1, ["not-an-email"]),
        (1, "a@example.com"),
    ],
)
def test_invalid_notification_values_are_rejected(
    raw_config, tmp_path, level, index, value
):
    section = prod_target(raw_config)
    keys = JOB_KEYS if level == "job" else TASK_KEYS
    section[keys[index]] = value

    with pytest.raises(ValidationError, match=keys[index]):
        generate(raw_config, tmp_path)


def test_null_optional_keys_take_their_defaults(raw_config, tmp_path):
    expected = generated_sources(raw_config, tmp_path / "full")
    raw_config["metadata"]["pythonVersion"] = None
    raw_config["metadata"]["uvBuildVersion"] = None
    raw_config["bundle"]["tags"] = None
    dev_target(raw_config)["viewGroups"] = None
    dev_target(raw_config)["host"] = None
    raw_config["bundle"]["targets"][1] = {"qa": None}

    assert generated_sources(raw_config, tmp_path / "nulls") == expected


@pytest.mark.parametrize(
    "change",
    [
        lambda c: c["metadata"].update(name=None),
        lambda c: c["metadata"].update(description=None),
        lambda c: c["job"]["bronze"].update(targetTable=None),
        lambda c: c["job"]["bronze"].update(sourcePath=None),
    ],
)
def test_null_required_keys_are_missing(raw_config, tmp_path, change):
    change(raw_config)

    with pytest.raises(ValidationError, match="Field required"):
        generate(raw_config, tmp_path)


def test_bundle_is_optional_and_defaults_to_the_sample(raw_config, tmp_path):
    drop_notifications(raw_config)
    expected = generated_sources(raw_config, tmp_path / "full")
    del raw_config["bundle"]

    assert generated_sources(raw_config, tmp_path / "default") == expected


def test_targets_left_out_use_their_defaults(raw_config, tmp_path):
    drop_notifications(raw_config)
    expected = generated_sources(raw_config, tmp_path / "full")
    raw_config["bundle"]["targets"] = [raw_config["bundle"]["targets"][2]]

    assert generated_sources(raw_config, tmp_path / "partial") == expected


def test_targets_are_always_rendered_in_order(raw_config, tmp_path):
    raw_config["bundle"]["targets"] = [{"prod": {}}, {"dev": {}}]

    bundle = yaml.safe_load(generated_sources(raw_config, tmp_path)["databricks.yml"])

    assert list(bundle["targets"]) == ["dev", "qa", "uat", "prod"]
    assert bundle["targets"]["prod"]["run_as"] == {
        "service_principal_name": "${var.run-as}"
    }
    assert bundle["targets"]["prod"]["variables"]["run-as"] == {
        "lookup": {"service_principal": "sp_ingestion_prod"}
    }


SECTIONS = {
    "top level": lambda c: c,
    "metadata": lambda c: c["metadata"],
    "bundle": lambda c: c["bundle"],
    "target": dev_target,
    "job": lambda c: c["job"],
    "bronze": lambda c: c["job"]["bronze"],
    "silver": lambda c: c["job"]["silver"],
    "gold": lambda c: c["job"]["gold"],
    "schema": lambda c: c["schema"],
    "column": lambda c: c["schema"]["columns"][0],
}


@pytest.mark.parametrize("section", SECTIONS)
def test_unknown_keys_are_rejected_in_every_section(raw_config, tmp_path, section):
    SECTIONS[section](raw_config)["unknownKey"] = "x"

    with pytest.raises(ValidationError, match="unknownKey"):
        generate(raw_config, tmp_path)
    assert not tmp_path.joinpath("pyproject.toml").exists()


@pytest.mark.parametrize(
    "change",
    [
        lambda c: c["schema"]["columns"][0].update(key="true"),
        lambda c: c["schema"]["columns"][0].update(key=1),
        lambda c: c["schema"]["columns"][0].update(key="yes"),
        lambda c: c["metadata"].update(name=1),
        lambda c: c["metadata"].update(description=["x"]),
        lambda c: c["job"].update(executionMode=True),
        lambda c: c["job"]["bronze"].update(triggerInterval=60),
        lambda c: c["bundle"].update(tags={"a": 1}),
        lambda c: prod_target(c).update(onJobFailureEmails="a@example.com"),
        lambda c: dev_target(c).update(computePolicy=123),
        lambda c: dev_target(c).update(runGroups="g"),
        lambda c: c["schema"].update(primaryKeys="id"),
        lambda c: c["schema"]["columns"][0].update(description=5),
        lambda c: c.update(job=["bronze"]),
    ],
)
def test_wrong_types_are_rejected(raw_config, tmp_path, change):
    change(raw_config)

    with pytest.raises(ValidationError):
        generate(raw_config, tmp_path)


def load_silver_columns(raw_config, tmp_path):
    namespace = {}
    source = generated_sources(raw_config, tmp_path)[job_path("silver.py")]
    start = source.index("@dataclass(frozen=True, slots=True)\nclass ColumnSpec")
    end = source.index("\nVARIANT_COLUMN")
    exec("from dataclasses import dataclass\n" + source[start:end], namespace)
    return {column.name: column for column in namespace["COLUMNS"]}


def test_column_path_is_required(raw_config, tmp_path):
    del raw_config["schema"]["columns"][3]["path"]

    with pytest.raises(ValidationError, match=r"columns\.3\.path"):
        generate(raw_config, tmp_path)


def test_null_column_path_counts_as_missing(raw_config, tmp_path):
    raw_config["schema"]["columns"][3]["path"] = None

    with pytest.raises(ValidationError, match="Field required"):
        generate(raw_config, tmp_path)


def test_column_description_is_optional(raw_config, tmp_path):
    for column in raw_config["schema"]["columns"]:
        del column["description"]

    assert len(load_silver_columns(raw_config, tmp_path)) == 7


@pytest.mark.parametrize(
    ("dynamo_type", "target_type"),
    [
        ("S", "string"),
        ("S", "timestamp"),
        ("N", "decimal(10, 2)"),
        ("N", "string"),
        ("B", "string"),
        ("BOOL", "boolean"),
        ("NULL", "boolean"),
        ("M", "variant"),
        ("M", "string"),
        ("L", "variant"),
        ("SS", "array<string>"),
        ("NS", "array<bigint>"),
        ("BS", "array<string>"),
    ],
)
def test_every_dynamo_type_has_a_target_type(
    raw_config, tmp_path, dynamo_type, target_type
):
    raw_config["schema"]["columns"][3].update(
        dynamoType=dynamo_type, targetType=target_type
    )

    status = load_silver_columns(raw_config, tmp_path)["status"]

    assert (status.dynamo_type, status.target_type) == (dynamo_type, target_type)


@pytest.mark.parametrize(
    ("dynamo_type", "target_type"),
    [
        ("S", "variant"),
        ("S", "array<string>"),
        ("N", "boolean"),
        ("B", "binary"),
        ("BOOL", "int"),
        ("NULL", "string"),
        ("M", "int"),
        ("L", "array<string>"),
        ("SS", "string"),
        ("NS", "array<boolean>"),
        ("BS", "binary"),
    ],
)
def test_target_type_must_suit_dynamo_type(
    raw_config, tmp_path, dynamo_type, target_type
):
    raw_config["schema"]["columns"][3].update(
        dynamoType=dynamo_type, targetType=target_type
    )

    with pytest.raises(ValidationError, match="targetType"):
        generate(raw_config, tmp_path)


@pytest.mark.parametrize("dynamo_type", ["s", "SET", "MAP", "X"])
def test_unknown_dynamo_types_are_rejected(raw_config, tmp_path, dynamo_type):
    raw_config["schema"]["columns"][3]["dynamoType"] = dynamo_type

    with pytest.raises(ValidationError, match="dynamoType"):
        generate(raw_config, tmp_path)


@pytest.mark.parametrize("dynamo_type", ["BOOL", "M", "SS"])
def test_key_columns_must_be_string_number_or_binary(raw_config, tmp_path, dynamo_type):
    raw_config["schema"]["columns"][0].update(
        dynamoType=dynamo_type,
        targetType={"BOOL": "boolean", "M": "string", "SS": "array<string>"}[
            dynamo_type
        ],
    )

    with pytest.raises(ValidationError, match="must have dynamoType S, N, or B"):
        generate(raw_config, tmp_path)


@pytest.mark.parametrize("key", ["name", "dynamoType", "targetType"])
def test_column_fields_are_required(raw_config, tmp_path, key):
    del raw_config["schema"]["columns"][3][key]

    with pytest.raises(ValidationError, match=key):
        generate(raw_config, tmp_path)


@pytest.mark.parametrize("key", ["columns", "primaryKeys"])
def test_schema_columns_and_primary_keys_are_required(raw_config, tmp_path, key):
    del raw_config["schema"][key]

    with pytest.raises(ValidationError, match=key):
        generate(raw_config, tmp_path)


def test_batch_schedule_is_required_in_batch_mode(raw_config, tmp_path):
    config = to_batch(raw_config)
    del config["job"]["batchSchedule"]

    with pytest.raises(ValidationError, match="batchSchedule is required"):
        generate(config, tmp_path)


def test_batch_schedule_is_not_allowed_in_continuous_mode(raw_config, tmp_path):
    raw_config["job"]["batchSchedule"] = "0 0 2 * * ?"

    with pytest.raises(ValidationError, match="batchSchedule is not allowed"):
        generate(raw_config, tmp_path)


@pytest.mark.parametrize(
    "schedule",
    ["0 0 2 * * ?", "0 */15 * * * ?", "0 30 6 ? * MON-FRI", "0 0 0 1 * ? 2027"],
)
def test_batch_schedule_accepts_quartz_cron(raw_config, tmp_path, schedule):
    config = to_batch(raw_config)
    config["job"]["batchSchedule"] = schedule

    job = load_job(config, tmp_path)

    assert job["schedule"]["quartz_cron_expression"] == schedule


@pytest.mark.parametrize(
    ("schedule", "message"),
    [
        ("0 2 * * *", "six or seven fields"),
        ("0 0 2 * * ? 2027 1", "six or seven fields"),
        ("0 0 2 * * ? ;", "six or seven fields"),
        ("", "six or seven fields"),
        ("0 0 2 * * *", "exactly one of day of month"),
        ("0 0 2 ? * ?", "exactly one of day of month"),
        (2, "batchSchedule"),
    ],
)
def test_invalid_batch_schedules_are_rejected(raw_config, tmp_path, schedule, message):
    config = to_batch(raw_config)
    config["job"]["batchSchedule"] = schedule

    with pytest.raises(ValidationError, match=message):
        generate(config, tmp_path)

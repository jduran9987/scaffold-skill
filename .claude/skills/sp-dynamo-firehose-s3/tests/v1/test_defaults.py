import os
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
import yaml

from sp_dynamo_firehose_s3.v1 import defaults as defaults_module
from sp_dynamo_firehose_s3.v1.defaults import (
    DEFAULTS_ENV_VAR,
    DefaultsError,
    load_defaults,
)

FIXTURES = Path(__file__).parent / "fixtures"
PACKAGED = (
    Path(defaults_module.__file__).parent / defaults_module.DEFAULTS_FILE
).read_text()
TEST_COPY = (FIXTURES / "defaults.toml").read_text()


def load_from(tmp_path, monkeypatch, text):
    path = tmp_path / "defaults.toml"
    path.write_text(text)
    monkeypatch.setenv(DEFAULTS_ENV_VAR, str(path))
    return load_defaults()


def test_packaged_defaults_are_valid(monkeypatch):
    monkeypatch.delenv(DEFAULTS_ENV_VAR)

    assert load_defaults().cli.aws_profile


def test_service_principal_placeholder_is_replaced(tmp_path, monkeypatch):
    defaults = load_from(tmp_path, monkeypatch, TEST_COPY)

    assert defaults.bundle.for_target("uat").service_principal == "sp_ingestion_uat"


def test_env_placeholder_in_group_names_is_replaced(tmp_path, monkeypatch):
    text = TEST_COPY.replace(
        'manage_groups = ["data_engineers", "admins"]',
        'manage_groups = ["data_engineers_{env}", "admins"]',
    ).replace('run_groups = ["data_analysts"]', 'run_groups = ["{env}_readers"]')

    defaults = load_from(tmp_path, monkeypatch, text)

    qa = defaults.bundle.for_target("qa")
    assert qa.manage_groups == ["data_engineers_qa", "admins"]
    assert qa.run_groups == ["qa_readers"]


def test_env_placeholder_in_tags_is_kept(tmp_path, monkeypatch):
    text = TEST_COPY.replace('SOURCE = "dynamo"', 'environment = "{env}"')

    defaults = load_from(tmp_path, monkeypatch, text)

    assert defaults.bundle.tags["environment"] == "{env}"


def test_notification_defaults_are_optional(tmp_path, monkeypatch):
    lines = [
        line
        for line in TEST_COPY.splitlines()
        if not line.startswith(("on_job_failure", "on_task_failure"))
    ]

    defaults = load_from(tmp_path, monkeypatch, "\n".join(lines) + "\n")

    assert defaults.bundle.on_job_failure_emails is None
    assert defaults.bundle.on_task_failure_webhook_notification_ids is None


def test_target_overrides_apply_to_one_target(tmp_path, monkeypatch):
    text = TEST_COPY.replace(
        "[bundle.targets.prod]\n",
        '[bundle.targets.prod]\nservice_principal = "sp_prod_owner"\n',
    )

    defaults = load_from(tmp_path, monkeypatch, text)

    assert defaults.bundle.for_target("prod").service_principal == "sp_prod_owner"
    assert defaults.bundle.for_target("dev").service_principal == "sp_ingestion_dev"


PROD_PERMISSIONS = (
    'compute_policy = "PRODPOLICY"\n'
    'service_principal_permissions = "can_manage"\n'
    'manage_groups = ["data_engineers", "admins"]\n'
    'run_groups = ["data_analysts"]\n'
    "view_groups = []\n"
)


def test_every_target_needs_its_own_table(tmp_path, monkeypatch):
    start = TEST_COPY.index("[bundle.targets.qa]\n")
    end = TEST_COPY.index("[bundle.targets.uat]\n")

    with pytest.raises(DefaultsError, match=r"missing \[bundle\.targets\.qa\]"):
        load_from(tmp_path, monkeypatch, TEST_COPY[:start] + TEST_COPY[end:])


def test_each_target_has_its_own_permissions(tmp_path, monkeypatch):
    assert PROD_PERMISSIONS in TEST_COPY
    text = TEST_COPY.replace(
        PROD_PERMISSIONS,
        'compute_policy = "PRODPOLICY"\n'
        'service_principal_permissions = "can_run"\n'
        'manage_groups = ["prod_admins"]\n'
        "run_groups = []\n"
        'view_groups = ["business_users"]\n',
    )

    defaults = load_from(tmp_path, monkeypatch, text)

    prod = defaults.bundle.for_target("prod")
    assert prod.service_principal_permissions == "can_run"
    assert (prod.manage_groups, prod.run_groups, prod.view_groups) == (
        ["prod_admins"],
        [],
        ["business_users"],
    )
    dev = defaults.bundle.for_target("dev")
    assert dev.service_principal_permissions == "can_manage"
    assert dev.manage_groups == ["data_engineers", "admins"]


def test_each_target_has_its_own_workspace(tmp_path, monkeypatch):
    defaults = load_from(tmp_path, monkeypatch, TEST_COPY)

    for name in ("dev", "qa", "uat", "prod"):
        target = defaults.bundle.for_target(name)
        assert target.host == f"https://{name}-workspace.cloud.databricks.com"
        assert target.compute_policy == f"{name.upper()}POLICY"


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ('aws_profile = "default"', 'aws_profile = ""', "aws_profile"),
        ("sample_size = 100", "sample_size = 0", "sample_size"),
        ("sample_size = 100", 'sample_size = "100"', "sample_size"),
        ('python_version = "3.12"', "python_version = 3.12", "python_version"),
        ('execution_mode = "continuous"', 'execution_mode = "stream"', "execution"),
        ('host = "https://', 'host = "http://', "host"),
        (
            'on_job_failure_emails = ["jduran9987@gmail.com"]',
            'on_job_failure_emails = ["not-an-email"]',
            "emails",
        ),
        ("[cli]", "[cli]\nunknown = 1", "unknown"),
        (
            "manage_groups = ",
            'on_task_failure_emails = ["a@example.com"]\nmanage_groups = ',
            "on_task_failure_emails",
        ),
        ('project = "orders"', 'true = "orders"', "reserved YAML words"),
        ("[job]", "[job]\n[bundle.targets.staging]", "staging"),
        ("[bundle.targets.qa]\n", "[bundle.targets.qa_old]\n", "qa_old"),
        (
            'service_principal = "sp_ingestion_{env}"',
            'service_principal = "sp_ingestion_{target}"',
            r"only the \{env\}, \{name\}, \{table\} placeholders",
        ),
        (
            'SOURCE = "dynamo"',
            'environment = "{target}"',
            "tag values",
        ),
        (
            'trigger_interval = "1 minute"\n',
            'trigger_interval = "1 minute"\nupsert_latest_key = "_event_timestamp"\n',
            "upsert_latest_key",
        ),
        (
            'QAPOLICY"\nservice_principal_permissions = "can_manage"',
            'QAPOLICY"',
            "service_principal_permissions",
        ),
        (
            'QAPOLICY"\nservice_principal_permissions = "can_manage"',
            'QAPOLICY"\nservice_principal_permissions = "can_view"',
            "service_principal_permissions",
        ),
        (
            "view_groups = []\n\n[bundle.targets.qa]",
            "\n[bundle.targets.qa]",
            "view_groups",
        ),
        (
            "view_groups = []\n\n[bundle.targets.qa]",
            'view_groups = ["admins"]\n\n[bundle.targets.qa]',
            "not: admins",
        ),
        (
            "[bundle.target]\n",
            '[bundle.target]\nmanage_groups = ["g"]\n',
            "manage_groups",
        ),
        (
            'compute_policy = "UATPOLICY"\n',
            "",
            "compute_policy",
        ),
        (
            "[bundle.target]\n",
            '[bundle.target]\nhost = "https://shared.cloud.databricks.com"\n',
            "host",
        ),
        ("[cli]", "[cli", "Invalid defaults file"),
        (
            '[job.bronze]\ncheckpoint_path = ""',
            '[job.bronze]\ncheckpoint_path = "bucket/cp/"',
            "checkpoint_path",
        ),
        (
            '[job.gold]\ncheckpoint_path = ""\ntarget_table = ""',
            '[job.gold]\ncheckpoint_path = ""\ntarget_table = "orders"',
            "target_table",
        ),
        ("[job.silver]", "[job.silver]\nsource_path = 1", "source_path"),
        ("[job.gold]", "[job.platinum]\n[job.gold]", "platinum"),
        (
            '[job.silver]\ncheckpoint_path = ""',
            '[job.silver]\ncheckpoint_path = "s3://{dev}-lake/cp/"',
            "only the {env}, {name}, {table} placeholders",
        ),
        ('aws_region = "us-east-1"', 'aws_region = "us east"', "aws_region"),
        ("buffer_size_mb = 128", "buffer_size_mb = 32", "buffer_size_mb"),
        ("shard_count = 3", "shard_count = 0", "shard_count"),
        (
            'bucket_arn = "arn:aws:s3:::dev-drivewealth-kinesis"',
            'bucket_arn = "dev-drivewealth-kinesis"',
            "bucket_arn",
        ),
        (
            'role_arn = "arn:aws:iam::123456789012:role/',
            'role_arn = "arn:aws:iam::1234:role/',
            "firehose_role_arn",
        ),
    ],
)
def test_invalid_defaults_are_rejected(tmp_path, monkeypatch, old, new, message):
    assert old in TEST_COPY

    with pytest.raises(DefaultsError, match=message):
        load_from(tmp_path, monkeypatch, TEST_COPY.replace(old, new, 1))


def test_missing_file_is_reported(tmp_path, monkeypatch):
    monkeypatch.setenv(DEFAULTS_ENV_VAR, str(tmp_path / "missing.toml"))

    with pytest.raises(DefaultsError, match="missing.toml"):
        load_defaults()


def test_missing_section_is_rejected(tmp_path, monkeypatch):
    text = TEST_COPY[: TEST_COPY.index("[job]")]

    with pytest.raises(DefaultsError, match="job"):
        load_from(tmp_path, monkeypatch, text)


def test_test_copy_matches_packaged_keys():
    assert tomllib.loads(TEST_COPY).keys() == tomllib.loads(PACKAGED).keys()


CUSTOM = (
    TEST_COPY.replace('aws_profile = "default"', 'aws_profile = "data-team"')
    .replace('pipelines_dir = "pipelines"', 'pipelines_dir = "yamls"')
    .replace('projects_dir = "dabs"', 'projects_dir = "bundles"')
    .replace('python_version = "3.12"', 'python_version = "3.13"')
    .replace(
        'service_principal = "sp_ingestion_{env}"',
        'service_principal = "sp_{env}_etl"',
    )
    .replace('trigger_interval = "1 minute"', 'trigger_interval = "5 minutes"')
    .replace(
        'host = "https://prod-workspace.cloud.databricks.com"',
        'host = "https://prod.example.com"',
    )
)
MINIMAL_YAML = """\
version: v1
metadata:
  name: events
  description: Events.
job:
  bronze:
    sourcePath: "s3://bucket/data/"
    checkpointPath: "s3://bucket/bronze/"
    targetTable: "dev.bronze.events"
"""


def run_skill(tmp_path, defaults_text, *args, path=None):
    defaults_path = tmp_path / "custom.toml"
    defaults_path.write_text(defaults_text)
    if path is None:
        path = f"{FIXTURES / 'bin'}{os.pathsep}{os.environ['PATH']}"
    env = os.environ | {DEFAULTS_ENV_VAR: str(defaults_path), "PATH": path}
    code = "import sys; from sp_dynamo_firehose_s3.router import main; sys.exit(main())"
    return subprocess.run(
        [sys.executable, "-c", code, *args],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_changed_defaults_flow_into_scaffold(tmp_path):
    (tmp_path / "events.yaml").write_text(MINIMAL_YAML)

    result = run_skill(tmp_path, CUSTOM, "scaffold", "events.yaml")

    assert result.returncode == 0, result.stderr
    project = tmp_path / "bundles" / "events"
    targets = yaml.safe_load((project / "databricks.yml").read_text())["targets"]
    assert targets["prod"]["workspace"]["host"] == "https://prod.example.com"
    assert targets["dev"]["workspace"]["host"] != "https://prod.example.com"
    assert targets["qa"]["variables"]["run-as"] == {
        "lookup": {"service_principal": "sp_qa_etl"}
    }
    pyproject = tomllib.loads((project / "pyproject.toml").read_text())
    assert pyproject["project"]["requires-python"] == ">=3.13"
    bronze = (project / "src" / "events" / "bronze.py").read_text()
    assert 'TRIGGER_INTERVAL = "5 minutes"' in bronze


def test_changed_defaults_flow_into_generate_yaml(tmp_path):
    result = run_skill(tmp_path, CUSTOM, "generate-yaml", "new.table")

    assert result.returncode == 0, result.stderr
    config = yaml.safe_load((tmp_path / "yamls" / "new-table.yaml").read_text())
    assert config["metadata"]["name"] == "new-table"
    assert config["metadata"]["pythonVersion"] == "3.13"
    targets = {
        name: t for entry in config["bundle"]["targets"] for name, t in entry.items()
    }
    assert targets["prod"]["host"] == "https://prod.example.com"
    assert targets["dev"]["servicePrincipal"] == "sp_dev_etl"
    assert config["job"]["bronze"]["triggerInterval"] == "5 minutes"


def test_batch_default_changes_which_fields_are_commented_out(tmp_path):
    text = TEST_COPY.replace(
        'execution_mode = "continuous"', 'execution_mode = "batch"'
    )

    result = run_skill(tmp_path, text, "generate-yaml", "orders")

    assert result.returncode == 0, result.stderr
    source = (tmp_path / "pipelines" / "orders.yaml").read_text()
    assert '  batchSchedule: ""  # Required field, please fill.\n' in source
    assert "0 0 2 * * ?" not in source
    assert '    # triggerInterval: "1 minute"' in source
    assert yaml.safe_load(source)["job"]["executionMode"] == "batch"


def test_invalid_defaults_stop_the_skill(tmp_path):
    result = run_skill(
        tmp_path, TEST_COPY.replace("sample_size = 100", "sample_size = -1"), "--help"
    )

    assert result.returncode != 0
    assert "Invalid defaults file" in result.stderr


def test_generate_yaml_runs_the_aws_cli_with_the_defaults_profile(tmp_path):
    log = tmp_path / "aws.log"
    wrapper = tmp_path / "bin" / "aws"
    wrapper.parent.mkdir()
    wrapper.write_text(f'#!/bin/sh\necho "$@" >> {log}\nexec {FIXTURES}/bin/aws "$@"\n')
    wrapper.chmod(0o755)

    result = run_skill(
        tmp_path,
        CUSTOM,
        "generate-yaml",
        "orders",
        path=f"{wrapper.parent}{os.pathsep}{os.environ['PATH']}",
    )

    assert result.returncode == 0, result.stderr
    calls = log.read_text().splitlines()
    assert [call.split()[1] for call in calls] == ["describe-table", "scan"]
    assert all("--table-name orders" in call for call in calls)
    assert all("--profile data-team --region us-east-1" in call for call in calls)


def test_generate_yaml_without_the_aws_cli_fails(tmp_path):
    result = run_skill(tmp_path, TEST_COPY, "generate-yaml", "orders", path="")

    assert result.returncode == 1
    assert "Result: AWS_CLI_MISSING\n" in result.stdout
    assert "AWS CLI is not installed" in result.stdout
    assert not (tmp_path / "pipelines").exists()


def test_tests_use_the_test_copy_of_the_defaults():
    assert defaults_module.read_defaults_text()[1] == TEST_COPY


BRONZE_EMPTY = '[job.bronze]\ncheckpoint_path = ""\ntarget_table = ""'
BRONZE_SET = (
    '[job.bronze]\ncheckpoint_path = "s3://{env}-lake/events/bronze/"\n'
    'target_table = "{env}.bronze.events"'
)
BRONZE_ONLY_YAML = """\
version: v1
metadata:
  name: events
  description: Events.
job:
  bronze:
    sourcePath: "s3://bucket/data/"
"""


def test_layer_defaults_fill_fields_the_yaml_leaves_out(tmp_path):
    assert BRONZE_EMPTY in TEST_COPY
    (tmp_path / "events.yaml").write_text(BRONZE_ONLY_YAML)

    result = run_skill(
        tmp_path, TEST_COPY.replace(BRONZE_EMPTY, BRONZE_SET), "scaffold", "events.yaml"
    )

    assert result.returncode == 0, result.stderr
    project = tmp_path / "dabs" / "events"
    bronze = (project / "src" / "events" / "bronze.py").read_text()
    assert 'CHECKPOINT_PATH = "s3://{env}-lake/events/bronze/"' in bronze
    assert 'TARGET_TABLE = "{env}.bronze.events"' in bronze
    ddl = (project / "ddl" / "qa" / "bronze.sql").read_text()
    assert "CREATE TABLE IF NOT EXISTS `qa`.`bronze`.`events` (" in ddl


def test_yaml_values_override_layer_defaults(tmp_path):
    (tmp_path / "events.yaml").write_text(MINIMAL_YAML)

    result = run_skill(
        tmp_path, TEST_COPY.replace(BRONZE_EMPTY, BRONZE_SET), "scaffold", "events.yaml"
    )

    assert result.returncode == 0, result.stderr
    bronze = (tmp_path / "dabs" / "events" / "src" / "events" / "bronze.py").read_text()
    assert 'TARGET_TABLE = "dev.bronze.events"' in bronze


def test_empty_layer_defaults_keep_the_fields_required(tmp_path):
    (tmp_path / "events.yaml").write_text(BRONZE_ONLY_YAML)

    result = run_skill(tmp_path, TEST_COPY, "scaffold", "events.yaml")

    assert result.returncode == 1
    assert "job.bronze.checkpointPath" in result.stderr
    assert "job.bronze.targetTable" in result.stderr

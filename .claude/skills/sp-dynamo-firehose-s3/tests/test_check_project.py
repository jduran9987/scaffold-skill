import subprocess

import pytest

from sp_dynamo_firehose_s3.check_project.config import to_snake_case
from sp_dynamo_firehose_s3.check_project.main import run
from sp_dynamo_firehose_s3.check_project.report import Code
from sp_dynamo_firehose_s3.scaffold import config as scaffold_config

TABLE = "bo.suspenseAccounts"


@pytest.fixture
def project_dir(tmp_path):
    return tmp_path / "project"


def test_project_new(project_dir):
    report = run([TABLE, str(project_dir)])
    assert report.code is Code.PROJECT_NEW
    assert report.ok
    assert report.text == (
        "check-project bo.suspenseAccounts\n"
        "Result: PROJECT_NEW\n"
        "  Project bo_suspense_accounts does not exist yet.\n"
        "Next step: None.\n"
    )


def test_project_exists(project_dir):
    (project_dir / "bundles" / "bo_suspense_accounts").mkdir(parents=True)
    report = run([TABLE, str(project_dir)])
    assert report.code is Code.PROJECT_EXISTS
    assert report.ok
    assert report.text == (
        "check-project bo.suspenseAccounts\n"
        "Result: PROJECT_EXISTS\n"
        "  Project bo_suspense_accounts already exists. Continuing will overwrite "
        "the existing project. Do you want to continue?\n"
        "Next step: Answer the question.\n"
    )


def test_writes_nothing(project_dir):
    run([TABLE, str(project_dir)])
    assert not project_dir.exists()


@pytest.mark.parametrize("args", [[], [TABLE], [TABLE, "/project", "x"]])
def test_wrong_number_of_arguments(args):
    report = run(args)
    assert report.code is Code.USAGE
    assert report.text == (
        "check-project\n"
        "Result: USAGE\n"
        "  check-project takes exactly two arguments, the DynamoDB table name and "
        f"the project folder, but got {len(args)}.\n"
        "Next step: Rerun the skill.\n"
    )


@pytest.mark.parametrize(
    ("table", "project_dir", "expected"),
    [
        (
            "ab",
            "/project",
            "dynamo_table_name: String should have at least 3 characters",
        ),
        (
            "2024orders",
            "/project",
            "table_name: String should match pattern '^[a-z][a-z0-9_]*$'",
        ),
        ("orders", "project", "project_dir: String should match pattern '^/'"),
    ],
)
def test_config_invalid(table, project_dir, expected):
    report = run([table, project_dir])
    assert report.code is Code.CONFIG_INVALID
    assert (
        f"The table name, project folder, or inputs.toml is invalid: {expected}."
    ) in report.text


@pytest.mark.parametrize(
    "name",
    ["bo.suspenseAccounts", "orders", "bo-HTTPLogs.v2", "Orders__2024", "v2Data"],
)
def test_folder_name_matches_scaffold(name):
    # check-project must look at the folder scaffold writes to.
    assert to_snake_case(name) == scaffold_config.to_snake_case(name)


def test_entry_point():
    # One argument, so the command reports USAGE before reading any file.
    result = subprocess.run(
        ["check-project", TABLE], capture_output=True, text=True, check=False
    )
    assert result.returncode == 1
    assert result.stdout == run([TABLE]).text
    assert result.stderr == ""

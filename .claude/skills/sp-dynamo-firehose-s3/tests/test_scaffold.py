"""End-to-end tests of scaffold, with byte-exact expected files.

To regenerate the expected files after an intended change, run
`UPDATE_EXPECTED=1 uv run pytest tests/test_scaffold.py`, then review the diff.
"""

import json
import os
import subprocess
from pathlib import Path

import pytest

from sp_dynamo_firehose_s3.scaffold.main import run
from sp_dynamo_firehose_s3.scaffold.report import Code

EXPECTED = Path(__file__).parent / "fixtures" / "scaffold"
UPDATE = os.environ.get("UPDATE_EXPECTED") == "1"
SESSION_ID = "e5d4c591-1e54-431d-ba66-6522f39f0a15"
TABLE = "bo.suspenseAccounts"
COLUMNS = [
    {"name": "account_id", "path": "accountId", "dynamo_type": "S", "key": True},
    {"name": "opened_at", "path": "openedAt", "dynamo_type": "S", "key": True},
    {"name": "balance", "path": "balance", "dynamo_type": "N", "key": False},
    {
        "name": "owner__full_name",
        "path": "owner.M.fullName",
        "dynamo_type": "S",
        "key": False,
    },
]
BATCH = {
    "types": {
        "account_id": "string",
        "opened_at": "timestamp",
        "balance": "decimal(22,2)",
        "owner__full_name": "string",
    },
    "execution_mode": "batch",
    "batch_schedule": "0 0 6 * * ?",
}
CONTINUOUS = {**BATCH, "execution_mode": "continuous", "batch_schedule": None}
PACKAGE = "src/bo_suspense_accounts"
# Files whose contents depend on the execution mode.
MODE_FILES = [
    "resource/bo_suspense_accounts.job.yml",
    f"{PACKAGE}/bronze.py",
    f"{PACKAGE}/silver.py",
    f"{PACKAGE}/gold.py",
]
FILES = [
    "pyproject.toml",
    ".python-version",
    "databricks.yml",
    "resource/bo_suspense_accounts.job.yml",
    f"{PACKAGE}/__init__.py",
    f"{PACKAGE}/cli.py",
    f"{PACKAGE}/env.py",
    f"{PACKAGE}/logs.py",
    f"{PACKAGE}/bronze.py",
    f"{PACKAGE}/silver.py",
    f"{PACKAGE}/gold.py",
    *[
        f"ddl/{target}/{layer}.sql"
        for target in ("dev", "qa", "uat", "prod")
        for layer in ("bronze", "silver", "gold")
    ],
    "README.md",
]


@pytest.fixture(autouse=True)
def sessions_dir(tmp_path, monkeypatch):
    """Keeps session folders under tmp_path instead of the system temp folder."""
    path = tmp_path / "sessions"
    monkeypatch.setattr("sp_dynamo_firehose_s3.scaffold.config.SESSIONS_DIR", path)
    write_columns(path)
    return path


@pytest.fixture
def project_dir(tmp_path):
    path = tmp_path / "project"
    path.mkdir()
    return path


def write_columns(sessions_dir, table=TABLE, columns=COLUMNS):
    folder = sessions_dir / SESSION_ID
    folder.mkdir(parents=True, exist_ok=True)
    data = {"dynamo_table_name": table, "columns": columns}
    (folder / "columns.json").write_text(json.dumps(data, indent=2) + "\n")


def scaffold(project_dir, answers=BATCH, session_id=SESSION_ID):
    return run([session_id, TABLE, str(project_dir)], json.dumps(answers))


def output_dir(project_dir):
    return project_dir / "bundles" / "bo_suspense_accounts"


def check_expected(actual: Path, expected: Path):
    if UPDATE:
        expected.parent.mkdir(parents=True, exist_ok=True)
        expected.write_text(actual.read_text())
    assert actual.read_text() == expected.read_text(), actual.name


# Generating


def test_generated(project_dir, sessions_dir):
    report = scaffold(project_dir)
    directory = output_dir(project_dir)
    assert report.code is Code.GENERATED
    assert report.text == (
        "scaffold bo.suspenseAccounts\n"
        + "".join(f"  {name}\n" for name in FILES)
        + "Result: GENERATED\n"
        + "  Generated the Databricks project for DynamoDB table bo.suspenseAccounts "
        + f"in {directory}.\n"
        + "Next step: None.\n"
    )
    assert sorted(
        str(path.relative_to(directory))
        for path in directory.rglob("*")
        if path.is_file()
    ) == sorted(FILES)


@pytest.mark.parametrize("name", FILES)
def test_batch_files(project_dir, name):
    scaffold(project_dir)
    check_expected(output_dir(project_dir) / name, EXPECTED / "batch" / name)


@pytest.mark.parametrize("name", MODE_FILES)
def test_continuous_files(project_dir, name):
    scaffold(project_dir, CONTINUOUS)
    check_expected(output_dir(project_dir) / name, EXPECTED / "continuous" / name)


def test_continuous_changes_only_the_mode_files(project_dir, sessions_dir, tmp_path):
    scaffold(project_dir, CONTINUOUS)
    other = tmp_path / "other"
    other.mkdir()
    write_columns(sessions_dir)
    scaffold(other, BATCH)
    for name in sorted(set(FILES) - set(MODE_FILES)):
        assert (output_dir(project_dir) / name).read_text() == (
            output_dir(other) / name
        ).read_text()


def test_same_inputs_give_the_same_files(project_dir, sessions_dir):
    scaffold(project_dir)
    first = {name: (output_dir(project_dir) / name).read_bytes() for name in FILES}
    write_columns(sessions_dir)
    scaffold(project_dir)
    assert {
        name: (output_dir(project_dir) / name).read_bytes() for name in FILES
    } == first


def test_existing_files_are_replaced(project_dir):
    directory = output_dir(project_dir)
    (directory / "resource").mkdir(parents=True)
    (directory / "databricks.yml").write_text("old")
    (directory / "notes.md").write_text("mine")
    assert scaffold(project_dir).code is Code.GENERATED
    assert (directory / "databricks.yml").read_text() != "old"
    assert (directory / "notes.md").read_text() == "mine"


def test_partition_key_only_table(project_dir, sessions_dir):
    # A table with no sort key: the partition key alone is gold's primary key,
    # used both to keep the latest change per key and to match in the MERGE.
    columns = [column for column in COLUMNS if column["name"] != "opened_at"]
    write_columns(sessions_dir, columns=columns)
    types = {name: kind for name, kind in BATCH["types"].items() if name != "opened_at"}
    assert scaffold(project_dir, {**BATCH, "types": types}).code is Code.GENERATED
    gold = (output_dir(project_dir) / PACKAGE / "gold.py").read_text()
    assert 'PRIMARY_KEYS = (\n    "account_id",\n)\n' in gold
    assert "Window.partitionBy(*PRIMARY_KEYS)" in gold
    assert "for key in PRIMARY_KEYS" in gold
    silver = (output_dir(project_dir) / PACKAGE / "silver.py").read_text()
    assert silver.count("key=True") == 1


# The session folder


def test_session_folder_is_deleted_after_success(project_dir, sessions_dir):
    scaffold(project_dir)
    assert not (sessions_dir / SESSION_ID).exists()


def test_session_folder_is_deleted_after_a_failure(project_dir, sessions_dir):
    report = scaffold(project_dir, {**BATCH, "batch_schedule": "0 6 * * *"})
    assert report.code is Code.CONFIG_INVALID
    assert not (sessions_dir / SESSION_ID).exists()
    assert not (project_dir / "bundles").exists()


def test_invalid_session_id_deletes_nothing(project_dir, sessions_dir):
    report = scaffold(project_dir, session_id="..")
    assert report.code is Code.CONFIG_INVALID
    assert (sessions_dir / SESSION_ID / "columns.json").exists()


# Failures


@pytest.mark.parametrize(
    "args",
    [[], [SESSION_ID, TABLE], [SESSION_ID, TABLE, "/project", "x"]],
)
def test_wrong_number_of_arguments(sessions_dir, args):
    report = run(args, "{}")
    assert report.code is Code.USAGE
    assert report.text == (
        "scaffold\n"
        "Result: USAGE\n"
        "  scaffold takes exactly three arguments, the session ID, the DynamoDB "
        f"table name, and the project folder, but got {len(args)}.\n"
        "Next step: Rerun the skill.\n"
    )
    assert (sessions_dir / SESSION_ID).exists()


def test_config_invalid(project_dir):
    answers = {**BATCH, "types": {**BATCH["types"], "balance": "date"}}
    report = scaffold(project_dir, answers)
    assert report.code is Code.CONFIG_INVALID
    assert report.text == (
        "scaffold bo.suspenseAccounts\n"
        "Result: CONFIG_INVALID\n"
        '  The inputs are invalid: Value error, answers.types.balance is "date", '
        "but a N column can be int, bigint, double, decimal(22,2), string.\n"
        "Next step: Fix inputs.toml if a problem is listed there, then rerun the "
        "skill.\n"
    )


def test_write_failed(project_dir):
    (project_dir / "bundles").write_text("not a folder")
    report = scaffold(project_dir)
    assert report.code is Code.WRITE_FAILED
    assert report.text.startswith(
        "scaffold bo.suspenseAccounts\nResult: WRITE_FAILED\n"
        f"  The project could not be written to {project_dir / 'bundles'}"
    )


# The real command


def test_entry_point(project_dir, tmp_path):
    result = subprocess.run(
        ["scaffold", SESSION_ID, TABLE],
        input="{}",
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert result.stdout == run([SESSION_ID, TABLE], "{}").text
    assert result.stderr == ""

"""Checks the shipped inputs.toml without depending on any of its values.

Its values differ between projects, so these tests check only that every action
accepts it, which checks each value's type and limits, and that its sections and
keys match the tests' copy.
"""

import json
import tomllib
from pathlib import Path

import pytest

import sp_dynamo_firehose_s3
from sp_dynamo_firehose_s3.check_project import config as check_project_config
from sp_dynamo_firehose_s3.create_stream import config as create_stream_config
from sp_dynamo_firehose_s3.prepare_inputs import config as prepare_inputs_config
from sp_dynamo_firehose_s3.scaffold import config as scaffold_config

SHIPPED = Path(sp_dynamo_firehose_s3.__file__).parent / "inputs.toml"
TEST_COPY = Path(__file__).parent / "fixtures" / "inputs.toml"
SESSION_ID = "e5d4c591-1e54-431d-ba66-6522f39f0a15"
TABLE = "bo.suspenseAccounts"
# Keys a project may add or leave out: its own job tags (under `bundle.tags`), and
# a target's service principal override.
OPTIONAL_KEYS = {
    f"bundle.targets.{target}.service_principal"
    for target in ("dev", "qa", "uat", "prod")
}


@pytest.fixture(autouse=True)
def shipped_inputs(monkeypatch):
    """Points every action at the shipped inputs.toml."""
    for module in (
        check_project_config,
        create_stream_config,
        prepare_inputs_config,
        scaffold_config,
    ):
        monkeypatch.setattr(module, "INPUTS_PATH", SHIPPED)


def keys(table, prefix=""):
    """Lists every key in a parsed TOML table as a dotted path."""
    found = set()
    for key, value in table.items():
        path = f"{prefix}{key}"
        if isinstance(value, dict):
            found |= keys(value, f"{path}.")
        else:
            found.add(path)
    return found


def comparable(path):
    """Keys of one file, without those a project may add or leave out."""
    return {
        key
        for key in keys(tomllib.loads(path.read_text()))
        if not key.startswith("bundle.tags.") and key not in OPTIONAL_KEYS
    }


def test_same_keys_as_the_tests_copy():
    assert comparable(SHIPPED) == comparable(TEST_COPY)


def test_check_project_accepts_it():
    check_project_config.build_config(TABLE, "/project")


def test_create_stream_accepts_it():
    create_stream_config.build_config(TABLE)


def test_prepare_inputs_accepts_it():
    prepare_inputs_config.build_config(TABLE, SESSION_ID)


def test_scaffold_accepts_it(tmp_path, monkeypatch):
    monkeypatch.setattr(scaffold_config, "SESSIONS_DIR", tmp_path)
    folder = tmp_path / SESSION_ID
    folder.mkdir()
    column = {
        "name": "account_id",
        "path": "accountId",
        "dynamo_type": "S",
        "key": True,
    }
    source = {"dynamo_table_name": TABLE, "columns": [column]}
    (folder / "columns.json").write_text(json.dumps(source))
    answers = {
        "types": {"account_id": "string"},
        "execution_mode": "continuous",
        "batch_schedule": None,
    }
    scaffold_config.build_config(TABLE, SESSION_ID, "/project", json.dumps(answers))

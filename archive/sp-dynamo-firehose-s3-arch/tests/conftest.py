"""Shared test setup.

Loads the test copy of the v1 defaults, so edits to `defaults.toml` do not change
what the tests expect, and replaces the AWS CLI with canned DynamoDB responses, so
no test reads a real table.
"""

import json
import os
import subprocess
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "v1" / "fixtures"
# Set before importing the package, which loads the defaults on import.
os.environ["SP_DYNAMO_FIREHOSE_S3_V1_DEFAULTS"] = str(FIXTURES / "defaults.toml")

from sp_dynamo_firehose_s3.v1 import aws, schema_inference  # noqa: E402

DYNAMODB_RESPONSES = json.loads((FIXTURES / "dynamodb.json").read_text())


class FakeSubprocess:
    """Stands in for `subprocess` in the AWS module."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.result: subprocess.CompletedProcess[str] | Exception | None = None

    def run(self, command: list[str], **kwargs: object):
        self.calls.append(command)
        if isinstance(self.result, Exception):
            raise self.result
        if self.result is not None:
            return self.result
        output = json.dumps(DYNAMODB_RESPONSES[command[2]])
        return subprocess.CompletedProcess(command, 0, stdout=output)


@pytest.fixture(autouse=True)
def fake_aws(monkeypatch: pytest.MonkeyPatch) -> FakeSubprocess:
    """Replaces the AWS CLI for every test. Only the AWS module sees the fake."""
    fake = FakeSubprocess()
    monkeypatch.setattr(aws, "subprocess", fake)
    return fake


@pytest.fixture
def orders_schema() -> schema_inference.InferredSchema:
    """The schema inferred from the canned DynamoDB responses."""
    return schema_inference.infer_table_schema("orders")

"""Test inputs and fakes for the AWS CLI and the clock.

Every test reads `tests/fixtures/inputs.toml` instead of the shipped
`inputs.toml`, so editing the shipped file never changes a test's result. Only
`test_inputs.py` reads the shipped file.

No test calls AWS or sleeps. A test that reaches AWS must say what each command
returns with `fake_aws.respond`; any other AWS command fails the test.
"""

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

TEST_INPUTS = Path(__file__).parent / "fixtures" / "inputs.toml"
ACTIONS = ("check_project", "create_stream", "prepare_inputs", "scaffold")


class FakeAws:
    """Stands in for `subprocess.run` inside `aws.py`.

    Each `(service, operation)` has a list of results, returned in order. The
    last result repeats once the others are used up. A result is a dict (printed
    as JSON with exit code 0), a string (printed to stderr with exit code 255),
    or an exception (raised).
    """

    def __init__(self):
        self.responses = {}
        self.calls = []

    def respond(self, service, operation, *results):
        self.responses[(service, operation)] = list(results)

    def __call__(self, command, **kwargs):
        self.calls.append(command)
        key = (command[1], command[2])
        if key not in self.responses:
            raise AssertionError(f"Unexpected AWS command: {' '.join(command)}")
        results = self.responses[key]
        result = results.pop(0) if len(results) > 1 else results[0]
        if isinstance(result, BaseException):
            raise result
        if isinstance(result, str):
            return subprocess.CompletedProcess(command, 255, "", result)
        return subprocess.CompletedProcess(command, 0, json.dumps(result), "")

    def operations(self):
        return [f"{command[1]} {command[2]}" for command in self.calls]

    def argument(self, operation, flag):
        for command in self.calls:
            if f"{command[1]} {command[2]}" == operation:
                return command[command.index(flag) + 1]
        raise AssertionError(f"{operation} was not called")


class FakeClock:
    """Stands in for `time` in the create-stream modules; sleeping is instant."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture(autouse=True)
def test_inputs(monkeypatch):
    """Points every action at the tests' copy of inputs.toml."""
    for action in ACTIONS:
        monkeypatch.setattr(
            f"sp_dynamo_firehose_s3.{action}.config.INPUTS_PATH", TEST_INPUTS
        )
    return TEST_INPUTS


@pytest.fixture(autouse=True)
def fake_aws(monkeypatch):
    fake = FakeAws()
    monkeypatch.setattr(
        "sp_dynamo_firehose_s3.aws.subprocess", SimpleNamespace(run=fake)
    )
    return fake


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    fake = FakeClock()
    for module in ("main", "steps"):
        monkeypatch.setattr(f"sp_dynamo_firehose_s3.create_stream.{module}.time", fake)
    return fake

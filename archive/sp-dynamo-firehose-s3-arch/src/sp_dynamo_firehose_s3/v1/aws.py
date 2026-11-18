"""Runs AWS CLI commands with the profile and region from `defaults.toml`.

Credentials come from the local AWS CLI profile, whether it uses SSO, static keys,
or an assumed role. The skill never signs in on the user's behalf. Every failure is
sorted into a fixed kind with a fixed explanation, so callers report it the same
way every time.
"""

import json
import subprocess
from collections.abc import Sequence
from enum import StrEnum
from typing import Any

from sp_dynamo_firehose_s3.v1.defaults import DEFAULTS


class AwsErrorKind(StrEnum):
    """Why an AWS CLI command failed. Each value is also a result code."""

    CLI_MISSING = "AWS_CLI_MISSING"
    CREDENTIALS = "AWS_CREDENTIALS"
    ACCESS_DENIED = "AWS_ACCESS_DENIED"
    NOT_FOUND = "AWS_NOT_FOUND"
    INVALID_ARGUMENT = "AWS_INVALID_ARGUMENT"
    OTHER = "AWS_ERROR"


# Text in the AWS CLI's error output that identifies each kind, checked in order.
ERROR_MARKERS: tuple[tuple[AwsErrorKind, tuple[str, ...]], ...] = (
    (
        AwsErrorKind.CREDENTIALS,
        (
            "Unable to locate credentials",
            "ExpiredToken",
            "Token has expired",
            "token is expired",
            "SSO session",
            "Error loading SSO Token",
            "Error when retrieving token from sso",
            "UnrecognizedClientException",
            "InvalidClientTokenId",
            "security token included in the request is",
            "The config profile",
        ),
    ),
    (AwsErrorKind.ACCESS_DENIED, ("AccessDenied", "is not authorized to perform")),
    (AwsErrorKind.NOT_FOUND, ("ResourceNotFoundException",)),
    (
        AwsErrorKind.INVALID_ARGUMENT,
        ("InvalidArgumentException", "ValidationException"),
    ),
)

ERROR_TEXT: dict[AwsErrorKind, tuple[str, str]] = {
    AwsErrorKind.CLI_MISSING: (
        "The AWS CLI is not installed or not on PATH.",
        "Install the AWS CLI and configure the {profile} profile, then rerun.",
    ),
    AwsErrorKind.CREDENTIALS: (
        "The AWS CLI profile {profile} has no valid credentials, or its session "
        "expired. AWS said: {aws_message}",
        "Run `aws sso login --profile {profile}` yourself (or refresh the "
        "profile's credentials), then rerun.",
    ),
    AwsErrorKind.ACCESS_DENIED: (
        "The AWS CLI profile {profile} is not allowed to run {operation}. AWS "
        "said: {aws_message}",
        "Get that permission for the profile, then rerun.",
    ),
    AwsErrorKind.NOT_FOUND: (
        "{operation} failed because a resource does not exist. AWS said: {aws_message}",
        "Fix the cause AWS gave, then rerun.",
    ),
    AwsErrorKind.INVALID_ARGUMENT: (
        "AWS rejected {operation}. AWS said: {aws_message}",
        "Fix the cause AWS gave, then rerun.",
    ),
    AwsErrorKind.OTHER: (
        "{operation} failed. AWS said: {aws_message}",
        "Fix the cause AWS gave, then rerun.",
    ),
}


class AwsError(RuntimeError):
    """Raised when the AWS CLI is missing or a command fails.

    Attributes:
        kind: Why the command failed.
        operation: The command, such as `aws kinesis create-stream`.
        aws_message: The AWS CLI's error output, on one line.
    """

    def __init__(self, kind: AwsErrorKind, operation: str, aws_message: str) -> None:
        """Records why the command failed.

        Args:
            kind: Why the command failed.
            operation: The command, such as `aws kinesis create-stream`.
            aws_message: The AWS CLI's error output, on one line.
        """
        self.kind = kind
        self.operation = operation
        self.aws_message = aws_message
        super().__init__(self.message())

    def facts(self) -> dict[str, str]:
        """Builds the values the error's explanation is filled in with.

        Returns:
            The profile, region, operation, and AWS message.
        """
        return {
            "profile": DEFAULTS.cli.aws_profile,
            "region": DEFAULTS.cli.aws_region,
            "operation": self.operation,
            "aws_message": self.aws_message,
        }

    def message(self) -> str:
        """Explains the error.

        Returns:
            The fixed explanation for the error's kind.
        """
        return ERROR_TEXT[self.kind][0].format(**self.facts())

    def next_step(self) -> str:
        """Says what the user should do about the error.

        Returns:
            The fixed next step for the error's kind.
        """
        return ERROR_TEXT[self.kind][1].format(**self.facts())


def classify(stderr: str) -> AwsErrorKind:
    """Sorts an AWS CLI error into a kind.

    Args:
        stderr: The AWS CLI's error output.

    Returns:
        The first kind whose markers appear in the output, or `OTHER`.
    """
    for kind, markers in ERROR_MARKERS:
        if any(marker in stderr for marker in markers):
            return kind
    return AwsErrorKind.OTHER


def run_aws(service: str, arguments: Sequence[str]) -> dict[str, Any]:
    """Runs an AWS CLI command and parses its JSON output.

    The command uses `cli.aws_profile` and `cli.aws_region` from `defaults.toml`.

    Args:
        service: AWS CLI service, such as `dynamodb`.
        arguments: Arguments after the service, starting with the operation.

    Returns:
        The command's parsed output, or an empty mapping if it printed nothing.

    Raises:
        AwsError: If the AWS CLI is missing or the command fails.
    """
    operation = f"aws {service} {arguments[0]}"
    command = [
        "aws",
        service,
        *arguments,
        "--output",
        "json",
        "--profile",
        DEFAULTS.cli.aws_profile,
        "--region",
        DEFAULTS.cli.aws_region,
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
    except FileNotFoundError as error:
        raise AwsError(AwsErrorKind.CLI_MISSING, operation, "") from error
    if result.returncode != 0:
        aws_message = " ".join(result.stderr.split())
        raise AwsError(classify(result.stderr), operation, aws_message)
    output = json.loads(result.stdout) if result.stdout.strip() else None
    return output or {}

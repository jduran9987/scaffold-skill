"""Runs AWS CLI commands with the configured profile and region.

Credentials come from the local AWS CLI profile, whether it uses SSO, static keys,
or an assumed role. The skill never signs in on the user's behalf. Every failure
is sorted into a fixed kind, so callers handle and report it the same way every
time.
"""

import json
import subprocess
from collections.abc import Sequence
from enum import StrEnum
from typing import Any


class AwsErrorKind(StrEnum):
    """Why an AWS CLI command failed."""

    CLI_MISSING = "CLI_MISSING"
    CREDENTIALS = "CREDENTIALS"
    ACCESS_DENIED = "ACCESS_DENIED"
    NOT_FOUND = "NOT_FOUND"
    OTHER = "OTHER"


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
)


class AwsError(Exception):
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
        super().__init__(f"{operation}: {kind}: {aws_message}")


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


def run_aws(
    profile: str, region: str, service: str, arguments: Sequence[str]
) -> dict[str, Any]:
    """Runs one AWS CLI command and parses its JSON output.

    Args:
        profile: AWS CLI profile to use.
        region: AWS region to use.
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
        profile,
        "--region",
        region,
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
    except FileNotFoundError as error:
        raise AwsError(AwsErrorKind.CLI_MISSING, operation, "") from error
    if result.returncode != 0:
        raise AwsError(
            classify(result.stderr), operation, " ".join(result.stderr.split())
        )
    return json.loads(result.stdout) if result.stdout.strip() else {}

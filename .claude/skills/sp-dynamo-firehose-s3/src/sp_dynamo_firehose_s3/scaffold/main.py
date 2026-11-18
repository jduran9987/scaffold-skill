"""The scaffold command's entry point.

`run` builds and checks the configuration from the arguments, the agent's
answers on standard input, `columns.json`, and `inputs.toml`, then renders the
project's files and writes them, replacing any that exist. Every run prints one
fixed report, and every run with a valid session ID deletes the session's folder
when it ends, whatever the result.
"""

import sys
from collections.abc import Sequence

from sp_dynamo_firehose_s3.scaffold.config import (
    ConfigError,
    build_config,
    remove_session,
)
from sp_dynamo_firehose_s3.scaffold.render import render_files, write_files
from sp_dynamo_firehose_s3.scaffold.report import Code, Report, build_report

COMMAND = "scaffold"


def generate(table: str, session_id: str, project_dir: str, answers: str) -> Report:
    """Builds the configuration, then renders and writes the project.

    Args:
        table: The DynamoDB table name exactly as the user gave it.
        session_id: The Claude Code session ID.
        project_dir: The Claude Code project folder.
        answers: The agent's answers as JSON text.

    Returns:
        The report.
    """
    title = f"{COMMAND} {table}"
    try:
        config = build_config(table, session_id, project_dir, answers)
    except ConfigError as error:
        return build_report(title, Code.CONFIG_INVALID, problems=error.problems)

    directory = config.project_path
    try:
        paths = write_files(directory, render_files(config))
    except OSError as error:
        return build_report(
            title,
            Code.WRITE_FAILED,
            path=error.filename or directory,
            reason=error.strerror or error,
        )
    lines = [str(path.relative_to(directory)) for path in paths]
    return build_report(
        title,
        Code.GENERATED,
        lines,
        table=config.dynamo_table_name,
        directory=directory,
    )


def run(args: Sequence[str], answers: str) -> Report:
    """Runs scaffold and builds its report.

    Args:
        args: Arguments after the command: the session ID, the table name, then
            the project folder.
        answers: The agent's answers as JSON text.

    Returns:
        The report.
    """
    if len(args) != 3:
        return build_report(COMMAND, Code.USAGE, count=len(args))
    session_id, table, project_dir = args
    try:
        return generate(table, session_id, project_dir, answers)
    finally:
        remove_session(session_id)


def main() -> int:
    """Runs scaffold with the process arguments and standard input.

    Returns:
        0 if the run succeeded, otherwise 1.
    """
    report = run(sys.argv[1:], sys.stdin.read())
    print(report.text, end="")
    return 0 if report.ok else 1

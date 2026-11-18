"""The check-project command's entry point.

`run` works out the folder scaffold writes the table's project to and reports
whether it exists, so the agent can ask the user before anything else runs. It
writes nothing. Every run prints one fixed report.
"""

import sys
from collections.abc import Sequence

from sp_dynamo_firehose_s3.check_project.config import ConfigError, build_config
from sp_dynamo_firehose_s3.check_project.report import Code, Report, build_report

COMMAND = "check-project"


def run(args: Sequence[str]) -> Report:
    """Runs check-project and builds its report.

    Args:
        args: Arguments after the command: the table name, then the project
            folder.

    Returns:
        The report: `PROJECT_EXISTS`, whose message is the question the agent
        asks the user, or `PROJECT_NEW`.
    """
    if len(args) != 2:
        return build_report(COMMAND, Code.USAGE, count=len(args))
    table, project_dir = args
    title = f"{COMMAND} {table}"
    try:
        config = build_config(table, project_dir)
    except ConfigError as error:
        return build_report(title, Code.CONFIG_INVALID, problems=error.problems)
    exists = config.project_path.exists()
    code = Code.PROJECT_EXISTS if exists else Code.PROJECT_NEW
    return build_report(title, code, project=config.table_name)


def main() -> int:
    """Runs check-project with the process arguments and prints its report.

    Returns:
        0 if the check ran, whether or not the project exists, otherwise 1.
    """
    report = run(sys.argv[1:])
    print(report.text, end="")
    return 0 if report.ok else 1

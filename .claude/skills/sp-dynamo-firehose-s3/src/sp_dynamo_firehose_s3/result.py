"""Formats the fixed report every action prints.

Every report has the same layout, so the agent relays it without interpreting
free-form output:

    <command that ran>
      <one line per detail, if any>
    Result: <CODE>
      <what happened>
    Next step: <what to do>
"""

from collections.abc import Sequence


def format_report(
    title: str, lines: Sequence[str], code: str, message: str, next_step: str
) -> str:
    """Formats a full report.

    Args:
        title: First line, the command that ran.
        lines: Detail lines shown under the title, such as one per resource.
        code: Result code, such as `CREATED`.
        message: What happened.
        next_step: What to do next.

    Returns:
        The report, ending with a newline.
    """
    body = "".join(f"  {line}\n" for line in lines)
    return f"{title}\n{body}Result: {code}\n  {message}\nNext step: {next_step}\n"

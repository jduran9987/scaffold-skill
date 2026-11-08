"""Formats the fixed result block every action prints.

Every run ends with the same layout, so the person reading it, or Claude relaying
it, never has to interpret free-form output:

    Result: <CODE>
      <what happened>
    Next step: <what to do>
"""

from collections.abc import Sequence


def format_result(code: str, message: str, next_step: str) -> str:
    """Formats the result block.

    Args:
        code: Result code, such as `CREATED` or `AWS_CREDENTIALS`.
        message: What happened.
        next_step: What the user should do next.

    Returns:
        The block's lines, joined by newlines.
    """
    return f"Result: {code}\n  {message}\nNext step: {next_step}"


def format_report(title: str, lines: Sequence[str], result: str) -> str:
    """Formats a full report: a title, indented detail lines, and the result.

    Args:
        title: First line, such as the command that ran.
        lines: Detail lines shown under the title.
        result: The result block from `format_result`.

    Returns:
        The report, ending with a newline.
    """
    body = "".join(f"  {line}\n" for line in lines)
    return f"{title}\n{body}{result}\n"

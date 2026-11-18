"""Turns a DynamoDB table into the columns the pipeline reads.

The table's keys come from `aws dynamodb describe-table`, and its other
attributes from the first `schema_inference.sample_size` items of a scan. Maps
are flattened at every depth, so each leaf attribute becomes its own column.
Lists and sets stay one column each.

The columns are written to `columns.json` in the session's folder, under the
system temp folder, where scaffold reads them. Example values are kept only in
memory, for the agent to pick each column's Spark type.
"""

import json
import re
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sp_dynamo_firehose_s3.aws import AwsError, AwsErrorKind, run_aws
from sp_dynamo_firehose_s3.prepare_inputs.config import Config
from sp_dynamo_firehose_s3.prepare_inputs.report import Code

# One folder per Claude Code session holds the files passed between actions.
SESSIONS_DIR = Path(tempfile.gettempdir()) / "sp-dynamo-firehose-s3"
COLUMNS_FILE = "columns.json"

MAP_TYPE = "M"
LIST_TYPE = "L"
NULL_TYPE = "NULL"
# The type of a column that is NULL in every sampled item, so it is read as a
# string once it has values.
ALL_NULL_TYPE = "S"
# Types whose value is shown as is; every other type is shown as compact JSON.
TEXT_TYPES = frozenset({"S", "N", "B"})
NAME_SEPARATOR = "__"
# Attribute names a column name can represent. Others are left out.
ATTRIBUTE_NAME = re.compile(r"[A-Za-z][A-Za-z0-9_]*")
# A run of capitals followed by a capitalized word: `orderIDList` -> `orderID_List`.
ACRONYM_BOUNDARY = re.compile(r"([A-Z]+)([A-Z][a-z])")
# A lowercase letter or digit followed by a capital: `zipCode`.
WORD_BOUNDARY = re.compile(r"([a-z0-9])([A-Z])")
# Different example values kept per column, and the length each is cut to.
EXAMPLE_COUNT = 3
EXAMPLE_LENGTH = 40
ELLIPSIS = "…"


class Stop(Exception):
    """Raised when a run cannot continue and must report a code.

    Attributes:
        code: The result code to report.
        facts: Values the code's text is filled in with.
    """

    def __init__(self, code: Code, **facts: object) -> None:
        """Records the code and facts.

        Args:
            code: The result code to report.
            **facts: Values the code's text is filled in with.
        """
        self.code = code
        self.facts = facts
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class Leaf:
    """One attribute that is not a map, found in one item.

    Attributes:
        path: Path in DynamoDB's typed JSON, with each map's `M` level, such as
            `owner.M.zipCode`.
        names: The attribute names along the path, such as `("owner", "zipCode")`.
        dynamo_type: DynamoDB type code, such as `S`.
        value: The value under the type code.
    """

    path: str
    names: tuple[str, ...]
    dynamo_type: str
    value: Any


@dataclass(frozen=True, slots=True)
class Column:
    """A column the pipeline reads.

    Attributes:
        name: Each attribute name on the path in snake_case, joined with `__`.
        path: Path in DynamoDB's typed JSON, with each map's `M` level.
        dynamo_type: The most common DynamoDB type code seen, ignoring NULL, or
            `S` if every value seen was NULL.
        key: Whether the column is the table's partition or sort key.
        examples: Up to `EXAMPLE_COUNT` different values of that type, in scan
            order, each cut to `EXAMPLE_LENGTH` characters.
    """

    name: str
    path: str
    dynamo_type: str
    key: bool
    examples: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Columns:
    """Everything found in the table.

    Attributes:
        columns: Key columns first, partition before sort, then the rest by path.
        item_count: How many items the sample had.
        skipped: Dotted paths of attributes left out because their names cannot
            be column names, sorted.
    """

    columns: tuple[Column, ...]
    item_count: int
    skipped: tuple[str, ...]


def to_snake_case(name: str) -> str:
    """Converts an attribute name to snake_case.

    Args:
        name: An attribute name, such as `zipCode` or `orderID`.

    Returns:
        The name in snake_case, such as `zip_code` or `order_id`.
    """
    words = WORD_BOUNDARY.sub(r"\1_\2", ACRONYM_BOUNDARY.sub(r"\1_\2", name))
    return words.lower()


def describe_keys(config: Config) -> list[tuple[str, str]]:
    """Reads the table's key attributes.

    Args:
        config: The run's configuration.

    Returns:
        The partition key and, if any, the sort key, each as its attribute name
        and type code.

    Raises:
        Stop: If the table does not exist.
    """
    try:
        result = run_aws(
            config.aws.profile,
            config.aws.region,
            "dynamodb",
            ["describe-table", "--table-name", config.dynamo_table_name],
        )
    except AwsError as error:
        if error.kind is AwsErrorKind.NOT_FOUND:
            raise Stop(Code.TABLE_NOT_FOUND) from error
        raise
    table = result["Table"]
    types = {
        definition["AttributeName"]: definition["AttributeType"]
        for definition in table["AttributeDefinitions"]
    }
    order = {"HASH": 0, "RANGE": 1}
    keys = sorted(table["KeySchema"], key=lambda key: order[key["KeyType"]])
    return [(key["AttributeName"], types[key["AttributeName"]]) for key in keys]


def scan_items(config: Config) -> list[dict[str, Any]]:
    """Reads the first items of a scan of the table.

    Args:
        config: The run's configuration.

    Returns:
        Up to `schema_inference.sample_size` items, in DynamoDB's typed JSON.

    Raises:
        Stop: If the table does not exist.
    """
    try:
        result = run_aws(
            config.aws.profile,
            config.aws.region,
            "dynamodb",
            [
                "scan",
                "--table-name",
                config.dynamo_table_name,
                "--max-items",
                str(config.schema_inference.sample_size),
            ],
        )
    except AwsError as error:
        if error.kind is AwsErrorKind.NOT_FOUND:
            raise Stop(Code.TABLE_NOT_FOUND) from error
        raise
    return result.get("Items", [])


def flatten_item(
    attributes: Mapping[str, Any],
    leaves: list[Leaf],
    skipped: set[str],
    path: str = "",
    names: tuple[str, ...] = (),
) -> None:
    """Collects every attribute of an item that is not a map, flattening maps.

    Attributes whose names a column name cannot represent are left out, along
    with everything nested under them.

    Args:
        attributes: Typed attributes of an item or map.
        leaves: Collects each attribute that is not a map.
        skipped: Collects the dotted path of each attribute left out.
        path: Path of the enclosing map, including its `M` level.
        names: Attribute names of the enclosing maps.
    """
    for attribute_name, typed_value in attributes.items():
        attribute_names = (*names, attribute_name)
        if not ATTRIBUTE_NAME.fullmatch(attribute_name):
            skipped.add(".".join(attribute_names))
            continue
        [(dynamo_type, value)] = typed_value.items()
        attribute_path = f"{path}.{attribute_name}" if path else attribute_name
        if dynamo_type == MAP_TYPE:
            flatten_item(
                value, leaves, skipped, f"{attribute_path}.{MAP_TYPE}", attribute_names
            )
        else:
            leaves.append(Leaf(attribute_path, attribute_names, dynamo_type, value))


def to_plain(typed_value: Mapping[str, Any]) -> object:
    """Removes DynamoDB's type codes from a typed value, at every depth.

    Args:
        typed_value: A value in DynamoDB's typed JSON, such as `{"S": "a"}`.

    Returns:
        The value without type codes. Numbers stay strings, as DynamoDB sends
        them.
    """
    [(dynamo_type, value)] = typed_value.items()
    if dynamo_type == MAP_TYPE:
        return {name: to_plain(item) for name, item in value.items()}
    if dynamo_type == LIST_TYPE:
        return [to_plain(item) for item in value]
    if dynamo_type == NULL_TYPE:
        return None
    return value


def show_value(dynamo_type: str, value: object) -> str:
    """Shows a value as an example for the agent.

    Args:
        dynamo_type: DynamoDB type code of the value.
        value: The value under the type code.

    Returns:
        The value as text, cut to `EXAMPLE_LENGTH` characters.
    """
    if dynamo_type in TEXT_TYPES:
        shown = str(value)
    else:
        shown = json.dumps(
            to_plain({dynamo_type: value}), ensure_ascii=False, separators=(",", ":")
        )
    if len(shown) <= EXAMPLE_LENGTH:
        return shown
    return shown[: EXAMPLE_LENGTH - len(ELLIPSIS)] + ELLIPSIS


def pick_type(type_counts: Counter[str]) -> str:
    """Picks the most common type, ignoring NULL.

    Args:
        type_counts: How often each type code was seen.

    Returns:
        The most common type code other than NULL. Ties go to the alphabetically
        first code. `S` if NULL is the only type, so the column is a string.
    """
    counts = Counter({code: n for code, n in type_counts.items() if code != NULL_TYPE})
    if not counts:
        return ALL_NULL_TYPE
    return min(counts, key=lambda code: (-counts[code], code))


def check_names_differ(columns: Sequence[Column]) -> None:
    """Checks no two columns have the same name.

    Args:
        columns: Every column.

    Raises:
        Stop: If attributes such as `zipCode` and `zip_code` would become the
            same column.
    """
    paths: dict[str, list[str]] = {}
    for column in columns:
        dotted = column.path.replace(f".{MAP_TYPE}.", ".")
        paths.setdefault(column.name, []).append(dotted)
    clashes = [
        f"{', '.join(attributes)} would all be {name}"
        for name, attributes in paths.items()
        if len(attributes) > 1
    ]
    if clashes:
        raise Stop(Code.COLUMN_NAME_CLASH, clashes="; ".join(clashes))


def infer_columns(
    keys: Sequence[tuple[str, str]], items: Sequence[Mapping[str, Any]]
) -> Columns:
    """Infers the columns from the table's keys and a sample of its items.

    Args:
        keys: The partition key and, if any, the sort key, each as its attribute
            name and type code.
        items: Sample items in DynamoDB's typed JSON. May be empty, which gives
            the key columns only.

    Returns:
        The columns, with key columns typed by the table's definition.

    Raises:
        Stop: If a key attribute's name cannot be a column name, or two
            attributes would become the same column.
    """
    for name, _ in keys:
        if not ATTRIBUTE_NAME.fullmatch(name):
            raise Stop(Code.KEY_NAME_UNSUPPORTED, attribute=name)

    type_counts: dict[str, Counter[str]] = {}
    names: dict[str, tuple[str, ...]] = {}
    examples: dict[tuple[str, str], list[str]] = {}
    skipped: set[str] = set()
    for item in items:
        leaves: list[Leaf] = []
        flatten_item(item, leaves, skipped)
        for leaf in leaves:
            type_counts.setdefault(leaf.path, Counter())[leaf.dynamo_type] += 1
            names[leaf.path] = leaf.names
            if leaf.dynamo_type == NULL_TYPE:
                continue
            seen = examples.setdefault((leaf.path, leaf.dynamo_type), [])
            shown = show_value(leaf.dynamo_type, leaf.value)
            if shown not in seen and len(seen) < EXAMPLE_COUNT:
                seen.append(shown)

    key_names = {name for name, _ in keys}
    columns = [
        Column(
            to_snake_case(name),
            name,
            dynamo_type,
            True,
            tuple(examples.get((name, dynamo_type), ())),
        )
        for name, dynamo_type in keys
    ]
    for path in sorted(type_counts.keys() - key_names):
        dynamo_type = pick_type(type_counts[path])
        columns.append(
            Column(
                NAME_SEPARATOR.join(to_snake_case(name) for name in names[path]),
                path,
                dynamo_type,
                False,
                tuple(examples.get((path, dynamo_type), ())),
            )
        )
    check_names_differ(columns)
    return Columns(tuple(columns), len(items), tuple(sorted(skipped)))


def write_columns(config: Config, columns: Columns) -> None:
    """Writes the columns to `columns.json` in the session's folder.

    Example values are not written.

    Args:
        config: The run's configuration.
        columns: Everything found in the table.

    Raises:
        Stop: If the file cannot be written.
    """
    path = SESSIONS_DIR / config.session_id / COLUMNS_FILE
    data = {
        "dynamo_table_name": config.dynamo_table_name,
        "columns": [
            {
                "name": column.name,
                "path": column.path,
                "dynamo_type": column.dynamo_type,
                "key": column.key,
            }
            for column in columns.columns
        ],
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2) + "\n")
    except OSError as error:
        raise Stop(
            Code.WRITE_FAILED, path=path, reason=error.strerror or error
        ) from error

"""Infers a pipeline YAML's `schema` section from a DynamoDB table.

The table's key schema comes from `aws dynamodb describe-table`, and its attributes
come from a sample of items read with `aws dynamodb scan`. Maps are flattened at
every depth, so each leaf attribute becomes its own column.
"""

import re
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from sp_dynamo_firehose_s3.v1.aws import run_aws
from sp_dynamo_firehose_s3.v1.defaults import DEFAULTS

MAP_TYPE = "M"
NULL_TYPE = "NULL"
NAME_SEPARATOR = "__"
# Attribute names a column path and name can represent. Others are left out.
ATTRIBUTE_NAME_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_]*")


class SchemaInferenceError(RuntimeError):
    """Raised when the table's keys cannot become columns."""


@dataclass(frozen=True, slots=True)
class InferredColumn:
    """A column inferred from the table.

    Attributes:
        name: Column name: each path level in snake_case, joined with `__`.
        dynamo_type: Most common DynamoDB type code seen for the attribute.
        path: Attribute path as it appears in the DynamoDB payload.
        key: Whether the attribute is the table's partition or sort key.
    """

    name: str
    dynamo_type: str
    path: str
    key: bool


@dataclass(frozen=True, slots=True)
class InferredSchema:
    """The inferred `schema` section.

    Attributes:
        columns: Key columns first, partition before sort, then the rest by path.
        primary_keys: Column names of the partition key and, if any, sort key.
    """

    columns: tuple[InferredColumn, ...]
    primary_keys: tuple[str, ...]


def to_snake_case(name: str) -> str:
    """Converts an attribute name to snake_case.

    Args:
        name: Attribute name, such as `zipCode` or `orderID`.

    Returns:
        The name in snake_case, such as `zip_code` or `order_id`.
    """
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


def describe_keys(table: str) -> list[tuple[str, str]]:
    """Reads the table's key attributes.

    Args:
        table: DynamoDB table name.

    Returns:
        The partition key and, if any, the sort key, each as its attribute name and
        type code.
    """
    description = run_aws("dynamodb", ["describe-table", "--table-name", table])
    table_description = description["Table"]
    types = {
        definition["AttributeName"]: definition["AttributeType"]
        for definition in table_description["AttributeDefinitions"]
    }
    key_order = {"HASH": 0, "RANGE": 1}
    key_schema = sorted(
        table_description["KeySchema"], key=lambda key: key_order[key["KeyType"]]
    )
    return [(key["AttributeName"], types[key["AttributeName"]]) for key in key_schema]


def scan_items(table: str) -> list[dict[str, Any]]:
    """Reads a sample of the table's items.

    Args:
        table: DynamoDB table name.

    Returns:
        Up to `cli.sample_size` items, from `defaults.toml`, in DynamoDB's typed
        JSON.
    """
    sample_size = str(DEFAULTS.cli.sample_size)
    result = run_aws(
        "dynamodb", ["scan", "--table-name", table, "--max-items", sample_size]
    )
    return result.get("Items", [])


def walk_attributes(
    attributes: Mapping[str, Any], path: str = "", names: tuple[str, ...] = ()
) -> Iterator[tuple[str, tuple[str, ...], str]]:
    """Yields every leaf attribute in an item, flattening maps.

    Attributes whose names a path or column name cannot represent are skipped,
    along with everything nested under them.

    Args:
        attributes: Typed attributes of an item or map.
        path: Path of the enclosing map, including its `M` level.
        names: Snake-case names of the enclosing attributes.

    Yields:
        Each leaf attribute's path, its name parts, and its type code.
    """
    for attribute_name, typed_value in attributes.items():
        if not ATTRIBUTE_NAME_PATTERN.fullmatch(attribute_name):
            continue
        [(type_code, value)] = typed_value.items()
        attribute_path = f"{path}.{attribute_name}" if path else attribute_name
        attribute_names = (*names, to_snake_case(attribute_name))
        if type_code == MAP_TYPE:
            yield from walk_attributes(
                value, f"{attribute_path}.{MAP_TYPE}", attribute_names
            )
        else:
            yield attribute_path, attribute_names, type_code


def pick_type(type_counts: Counter[str]) -> str:
    """Picks the most common type, ignoring NULL unless it is the only type.

    Args:
        type_counts: How often each type code was seen.

    Returns:
        The most common type code. Ties go to the alphabetically first code.
    """
    counts = Counter({code: n for code, n in type_counts.items() if code != NULL_TYPE})
    if not counts:
        return NULL_TYPE
    return min(counts, key=lambda code: (-counts[code], code))


def infer_schema(
    keys: Sequence[tuple[str, str]], items: Sequence[Mapping[str, Any]]
) -> InferredSchema:
    """Infers the schema from the table's keys and a sample of its items.

    Args:
        keys: The partition key and, if any, the sort key, each as its attribute
            name and type code.
        items: Sample items in DynamoDB's typed JSON.

    Returns:
        The inferred columns and primary keys. Attributes that would get the same
        column name, such as `zipCode` and `zip_code`, each keep their own column,
        so the author can rename them.

    Raises:
        SchemaInferenceError: If a key attribute's name cannot be a column name.
    """
    for name, _ in keys:
        if not ATTRIBUTE_NAME_PATTERN.fullmatch(name):
            raise SchemaInferenceError(
                f"Key attribute {name} cannot be used as a column name or path"
            )
    key_names = {name for name, _ in keys}
    type_counts: dict[str, Counter[str]] = {}
    name_parts: dict[str, tuple[str, ...]] = {}
    for item in items:
        for path, names, type_code in walk_attributes(item):
            if path in key_names:
                continue
            type_counts.setdefault(path, Counter())[type_code] += 1
            name_parts[path] = names

    columns = [
        InferredColumn(to_snake_case(name), type_code, name, key=True)
        for name, type_code in keys
    ]
    for path in sorted(type_counts):
        name = NAME_SEPARATOR.join(name_parts[path])
        columns.append(InferredColumn(name, pick_type(type_counts[path]), path, False))

    primary_keys = tuple(to_snake_case(name) for name, _ in keys)
    return InferredSchema(columns=tuple(columns), primary_keys=primary_keys)


def infer_table_schema(table: str) -> InferredSchema:
    """Infers the schema of a DynamoDB table with the AWS CLI.

    Args:
        table: DynamoDB table name.

    Returns:
        The inferred schema.
    """
    return infer_schema(describe_keys(table), scan_items(table))

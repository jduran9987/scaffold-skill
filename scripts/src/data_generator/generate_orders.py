"""Generates insert, modify, and delete activity on the scaffoldskill DynamoDB table.

Each change produces a stream event, which gives the pipeline realistic data to
ingest. Rows to modify and delete are chosen at random from the rows that exist
before any new rows are inserted. About 35% of inserted and modified rows are
missing one or more of the optional fields.

Typical usage:

    uv run generate-orders --new-rows 10 --modify-rows 3 --delete-rows 2
"""

from __future__ import annotations

import argparse
import random
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import boto3

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table

Key = dict[str, Any]

ADDRESSES = (
    ("Austin", "TX", "78701"),
    ("Denver", "CO", "80202"),
    ("Seattle", "WA", "98101"),
    ("Boston", "MA", "02108"),
    ("Chicago", "IL", "60601"),
)
STATUSES = ("PENDING", "SHIPPED", "DELIVERED", "CANCELLED")
MAX_ID = 2**31 - 1

OPTIONAL_FIELDS = ("amount", "status", "addressDetails")
# Share of inserted or modified rows missing at least one optional field.
MISSING_FIELDS_RATE = 0.35

KEY_NAMES = {"#id": "id", "#createdAt": "createdAt"}


def non_negative_int(value: str) -> int:
    """Parses a non-negative integer argument.

    Args:
        value: Raw argument value.

    Returns:
        The parsed integer.

    Raises:
        argparse.ArgumentTypeError: If the value is not a non-negative integer.
    """
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError(f"must be 0 or greater, got {value}")
    return number


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parses command-line arguments.

    Args:
        argv: Arguments to parse. Defaults to the process arguments.

    Returns:
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Generate DynamoDB order changes.")
    parser.add_argument(
        "--new-rows", type=non_negative_int, default=0, help="Rows to insert."
    )
    parser.add_argument(
        "--modify-rows",
        type=non_negative_int,
        default=0,
        help="Existing rows to modify. Capped at the number of rows.",
    )
    parser.add_argument(
        "--delete-rows",
        type=non_negative_int,
        default=0,
        help="Existing rows to delete. Capped at the number of rows.",
    )
    parser.add_argument("--table-name", default="scaffoldskill", help="Table name.")
    parser.add_argument("--profile", help="AWS profile. Defaults to the default.")
    return parser.parse_args(argv)


def random_address() -> dict[str, str]:
    """Picks a random address.

    Returns:
        A nested address map.
    """
    city, state, zip_code = random.choice(ADDRESSES)
    return {"city": city, "state": state, "zipCode": zip_code}


def random_amount() -> Decimal:
    """Picks a random order amount.

    Returns:
        An amount with two decimal places.
    """
    return Decimal(random.randint(100, 100_000)) / 100


def random_optional_fields() -> dict[str, Any]:
    """Builds the optional fields, leaving some out at the configured rate.

    Returns:
        The optional fields. At the rate set by `MISSING_FIELDS_RATE`, one or more
        randomly chosen fields are left out.
    """
    fields = {
        "amount": random_amount(),
        "status": random.choice(STATUSES),
        "addressDetails": random_address(),
    }
    if random.random() < MISSING_FIELDS_RATE:
        omitted_count = random.randint(1, len(OPTIONAL_FIELDS))
        for name in random.sample(OPTIONAL_FIELDS, omitted_count):
            del fields[name]
    return fields


def build_order() -> dict[str, Any]:
    """Builds a new order item.

    Returns:
        An item with a random ID, the current time as its sort key, and random
        optional fields.
    """
    created_at = datetime.now(UTC).isoformat(timespec="milliseconds")
    return {
        "id": random.randint(1, MAX_ID),
        "createdAt": created_at.replace("+00:00", "Z"),
        **random_optional_fields(),
    }


def build_update(fields: dict[str, Any]) -> dict[str, Any]:
    """Builds the arguments that update an order's optional fields.

    Fields present in `fields` are set, and the rest are removed from the item.
    The update fails if the order no longer exists.

    Args:
        fields: Optional fields to set.

    Returns:
        Keyword arguments for `Table.update_item`, without the key.
    """
    names = {"#id": "id"}
    values: dict[str, Any] = {}
    set_parts: list[str] = []
    remove_parts: list[str] = []
    for index, name in enumerate(OPTIONAL_FIELDS):
        names[f"#f{index}"] = name
        if name in fields:
            values[f":v{index}"] = fields[name]
            set_parts.append(f"#f{index} = :v{index}")
        else:
            remove_parts.append(f"#f{index}")

    clauses = []
    if set_parts:
        clauses.append(f"SET {', '.join(set_parts)}")
    if remove_parts:
        clauses.append(f"REMOVE {', '.join(remove_parts)}")

    update: dict[str, Any] = {
        "UpdateExpression": " ".join(clauses),
        "ConditionExpression": "attribute_exists(#id)",
        "ExpressionAttributeNames": names,
    }
    if values:
        update["ExpressionAttributeValues"] = values
    return update


def fetch_keys(table: Table) -> list[Key]:
    """Reads the primary key of every item in the table.

    Args:
        table: DynamoDB table resource.

    Returns:
        The key of each item.
    """
    scan_args = {
        "ProjectionExpression": "#id, #createdAt",
        "ExpressionAttributeNames": KEY_NAMES,
    }
    keys: list[Key] = []
    while True:
        response = table.scan(**scan_args)
        keys.extend(response["Items"])
        if "LastEvaluatedKey" not in response:
            return keys
        scan_args["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def insert_rows(table: Table, count: int) -> int:
    """Inserts new orders.

    A write that would replace an existing item is retried with a new ID, so
    every write produces an insert event rather than a modify event.

    Args:
        table: DynamoDB table resource.
        count: Number of orders to insert.

    Returns:
        The number of inserted orders.
    """
    conflict = table.meta.client.exceptions.ConditionalCheckFailedException
    inserted = 0
    while inserted < count:
        try:
            table.put_item(
                Item=build_order(),
                ConditionExpression="attribute_not_exists(#id)",
                ExpressionAttributeNames={"#id": "id"},
            )
        except conflict:
            continue
        inserted += 1
    return inserted


def modify_rows(table: Table, keys: Sequence[Key], count: int) -> int:
    """Replaces the optional fields of randomly chosen orders.

    Fields left out by `random_optional_fields` are removed from the order. An
    order deleted since the keys were read is skipped instead of recreated.

    Args:
        table: DynamoDB table resource.
        keys: Keys of the existing orders.
        count: Number of orders to modify, capped at the number of keys.

    Returns:
        The number of modified orders.
    """
    conflict = table.meta.client.exceptions.ConditionalCheckFailedException
    modified = 0
    for key in random.sample(list(keys), min(count, len(keys))):
        try:
            table.update_item(Key=key, **build_update(random_optional_fields()))
        except conflict:
            continue
        modified += 1
    return modified


def delete_rows(table: Table, keys: Sequence[Key], count: int) -> int:
    """Deletes randomly chosen orders.

    Args:
        table: DynamoDB table resource.
        keys: Keys of the existing orders.
        count: Number of orders to delete, capped at the number of keys.

    Returns:
        The number of deleted orders.
    """
    chosen = random.sample(list(keys), min(count, len(keys)))
    with table.batch_writer() as batch:
        for key in chosen:
            batch.delete_item(Key=key)
    return len(chosen)


def main(argv: Sequence[str] | None = None) -> None:
    """Applies the requested inserts, modifies, and deletes.

    Args:
        argv: Arguments to parse. Defaults to the process arguments.
    """
    args = parse_args(argv)
    session = boto3.Session(profile_name=args.profile)
    table = session.resource("dynamodb").Table(args.table_name)

    keys = fetch_keys(table)
    inserted = insert_rows(table, args.new_rows)
    modified = modify_rows(table, keys, args.modify_rows)
    deleted = delete_rows(table, keys, args.delete_rows)

    print(
        f"Table {args.table_name}: {len(keys)} existing rows, "
        f"{inserted} inserted, {modified} modified, {deleted} deleted"
    )


if __name__ == "__main__":
    main()

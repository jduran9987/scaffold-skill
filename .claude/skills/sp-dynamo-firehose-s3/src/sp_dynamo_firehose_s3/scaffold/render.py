"""Renders the generated project's files from the templates and writes them.

Every file is rendered from a Jinja template with `StrictUndefined`, so the same
config always gives byte-identical files.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

from jinja2 import Environment, PackageLoader, StrictUndefined

from sp_dynamo_firehose_s3.scaffold.config import Config

TARGET_NAMES = ("dev", "qa", "uat", "prod")
# The only target that sends failure notifications.
NOTIFICATION_TARGET = "prod"
# The job's tasks, in the order data flows through them. Each has a module and
# an entry point of the same name.
TASKS = ("bronze", "silver", "gold")
ENV_PLACEHOLDER = "{env}"
# The bundle fills this in with the target's name when it deploys.
BUNDLE_TARGET = "${bundle.target}"
GROUP_LEVELS = (
    ("manage_groups", "CAN_MANAGE"),
    ("run_groups", "CAN_RUN"),
    ("view_groups", "CAN_VIEW"),
)
PROJECT_DESCRIPTION = "Replication Job For The {table} Dynamo Table"
DDL_DIR = "ddl"
# What each layer does, the same in every project.
LAYER_SUMMARIES = {
    "bronze": (
        "Loads each record Firehose writes to S3, unchanged, into a single VARIANT "
        "column, along with when it was loaded and which file it came from. One "
        "row per record. Each run reads only files it has not read before."
    ),
    "silver": (
        "Reads new bronze rows and flattens each DynamoDB change event into typed "
        "columns. One row per change. Inserts and modifies take their values from "
        "the item after the change, and removes from the item before it. Key "
        "columns always come from the event's keys."
    ),
    "gold": (
        "Merges silver changes into one row per primary key, keeping the most "
        "recent change. A newer change replaces the stored row, and an older one "
        "is ignored. Items deleted in DynamoDB remain as rows whose `_event_name` "
        "is `REMOVE`."
    ),
}


class TableColumn(NamedTuple):
    """A column of a layer's table, as its DDL and the README show it.

    Attributes:
        name: Column name.
        type: Spark SQL type.
        comment: What the column holds. Empty for the table's own columns, which
            have no description.
    """

    name: str
    type: str
    comment: str = ""


# Bronze stores each source record whole in this column.
BRONZE_DATA_COLUMN = TableColumn(
    "data",
    "variant",
    "DynamoDB change event as delivered by Firehose from the table's Kinesis stream.",
)
BRONZE_METADATA_COLUMNS = (
    TableColumn("_ingested_at", "timestamp", "Time the bronze job loaded the record."),
    TableColumn(
        "_file_path", "string", "S3 path of the Firehose file the record came from."
    ),
    TableColumn(
        "_file_modification_time",
        "timestamp",
        "Time the Firehose file the record came from was last modified in S3.",
    ),
)
# Silver and gold add these after the table's own columns.
SILVER_METADATA_COLUMNS = (
    TableColumn(
        "_event_name", "string", "DynamoDB change type: INSERT, MODIFY, or REMOVE."
    ),
    TableColumn("_event_id", "string", "Unique ID of the DynamoDB change event."),
    TableColumn(
        "_event_timestamp",
        "timestamp",
        "Approximate time DynamoDB made the change, from the event's "
        "ApproximateCreationDateTime.",
    ),
    *BRONZE_METADATA_COLUMNS,
)


@dataclass(frozen=True, slots=True)
class Target:
    """One target's settings, with `{env}` replaced by the target's name.

    Attributes:
        name: The target's name, such as `prod`.
        host: Workspace URL.
        compute_policy: Cluster policy ID.
        num_workers: Workers on the job cluster.
        service_principal: Name of the service principal the job runs as.
        service_principal_level: Its permission level, such as `CAN_MANAGE`.
        group_levels: Each group and its permission level, managers first.
        notifies: Whether the target sends failure notifications.
    """

    name: str
    host: str
    compute_policy: str
    num_workers: int
    service_principal: str
    service_principal_level: str
    group_levels: tuple[tuple[str, str], ...]
    notifies: bool


def list_targets(config: Config) -> list[Target]:
    """Lists every target's settings.

    Args:
        config: The run's configuration.

    Returns:
        One entry per target, in the order dev, qa, uat, prod.
    """
    targets = []
    for name in TARGET_NAMES:
        target = getattr(config.bundle.targets, name)
        service_principal = (
            target.service_principal or config.bundle.target.service_principal
        )
        targets.append(
            Target(
                name=name,
                host=target.host,
                compute_policy=target.compute_policy,
                num_workers=target.num_workers,
                service_principal=service_principal.replace(ENV_PLACEHOLDER, name),
                service_principal_level=target.service_principal_permissions.upper(),
                group_levels=tuple(
                    (group, level)
                    for field, level in GROUP_LEVELS
                    for group in getattr(target, field)
                ),
                notifies=name == NOTIFICATION_TARGET,
            )
        )
    return targets


def list_table_columns(config: Config, layer: str) -> list[TableColumn]:
    """Lists a layer's table columns in the order its job writes them.

    Args:
        config: The run's configuration.
        layer: `bronze`, `silver`, or `gold`.

    Returns:
        Bronze's data and load metadata columns, or, for silver and gold, the
        table's own columns followed by the event and load metadata columns.
    """
    if layer == "bronze":
        return [BRONZE_DATA_COLUMN, *BRONZE_METADATA_COLUMNS]
    own = [TableColumn(column.name, column.spark_type) for column in config.columns]
    return [*own, *SILVER_METADATA_COLUMNS]


def resolve_env(value: str, target: str) -> str:
    """Replaces `{env}` in a value with a target's name.

    Args:
        value: A value such as a table name.
        target: The target's name, such as `prod`.

    Returns:
        The value for that target.
    """
    return value.replace(ENV_PLACEHOLDER, target)


def sql_string(value: str) -> str:
    """Renders a value as a double-quoted Spark SQL string literal.

    Backslashes and double quotes are escaped with backslashes, which is how
    Spark SQL escapes inside string literals.

    Args:
        value: Text of the literal.

    Returns:
        The value in double quotes.
    """
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def sql_identifier(name: str) -> str:
    """Quotes a name for Spark SQL.

    Args:
        name: Column, catalog, schema, or table name.

    Returns:
        The name in backticks, with any backtick doubled.
    """
    return "`" + name.replace("`", "``") + "`"


def sql_table(name: str) -> str:
    """Quotes each part of a `catalog.schema.table` name for Spark SQL.

    Args:
        name: The table name.

    Returns:
        Each part quoted, joined by dots.
    """
    return ".".join(sql_identifier(part) for part in name.split("."))


def md_cell(value: str) -> str:
    """Makes a value safe inside a Markdown table cell.

    Args:
        value: Cell text.

    Returns:
        The text on one line, with pipes escaped.
    """
    return " ".join(value.split()).replace("|", "\\|")


def bundle_target(value: str) -> str:
    """Replaces `{env}` with the bundle's target substitution.

    Args:
        value: A value such as a job tag.

    Returns:
        The value with each `{env}` replaced by `${bundle.target}`.
    """
    return value.replace(ENV_PLACEHOLDER, BUNDLE_TARGET)


def build_environment() -> Environment:
    """Builds the Jinja environment for scaffold's templates.

    Returns:
        An environment that fails on undefined variables. Its `py_string`,
        `toml_string`, `yaml_string`, and `sql_string` filters render a value as
        a quoted string; `sql_identifier` and `sql_table` quote names for Spark
        SQL; `md_cell` makes text safe in a Markdown table; and `bundle_target`
        replaces `{env}` with `${bundle.target}`.
    """
    environment = Environment(
        loader=PackageLoader("sp_dynamo_firehose_s3.scaffold", "templates"),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
        autoescape=False,
    )
    # JSON strings are valid Python, TOML basic, and YAML double-quoted strings.
    environment.filters["py_string"] = json.dumps
    environment.filters["toml_string"] = json.dumps
    environment.filters["yaml_string"] = json.dumps
    environment.filters["sql_string"] = sql_string
    environment.filters["sql_identifier"] = sql_identifier
    environment.filters["sql_table"] = sql_table
    environment.filters["md_cell"] = md_cell
    environment.filters["bundle_target"] = bundle_target
    return environment


def render_files(config: Config) -> dict[str, str]:
    """Renders every file of the project.

    Args:
        config: The run's configuration.

    Returns:
        File contents keyed by path relative to the project folder, in a fixed
        order: the uv project files, the bundle, the job code, each layer's DDL
        for each target, and the README.
    """
    environment = build_environment()
    package = config.table_name
    job = config.job
    layers = {"bronze": job.bronze, "silver": job.silver, "gold": job.gold}
    context = {
        "config": config,
        "package": package,
        "tasks": TASKS,
        "targets": list_targets(config),
        "description": PROJECT_DESCRIPTION.format(table=config.dynamo_table_name),
        "job": job,
        "answers": config.answers,
        "bronze": job.bronze,
        "silver": job.silver,
        "gold": job.gold,
        "columns": config.columns,
        "primary_keys": [column.name for column in config.columns if column.key],
    }

    def render(template: str, **extra: object) -> str:
        """Renders one template with the shared context."""
        return environment.get_template(template).render(context, **extra)

    package_dir = f"src/{package}"
    files = {
        "pyproject.toml": render("pyproject.toml.j2"),
        ".python-version": render("python-version.j2"),
        "databricks.yml": render("databricks.yml.j2"),
        f"resource/{package}.job.yml": render("job.yml.j2"),
        f"{package_dir}/__init__.py": render("init.py.j2"),
        f"{package_dir}/cli.py": render("cli.py.j2"),
        f"{package_dir}/env.py": render("env.py.j2"),
        f"{package_dir}/logs.py": render("logs.py.j2"),
    }
    for task in TASKS:
        files[f"{package_dir}/{task}.py"] = render(f"{task}.py.j2")
    for target in TARGET_NAMES:
        for layer, settings in layers.items():
            files[f"{DDL_DIR}/{target}/{layer}.sql"] = render(
                "ddl.sql.j2",
                layer=layer,
                target=target,
                table=resolve_env(settings.target_table, target),
                columns=list_table_columns(config, layer),
            )
    sections = [
        {
            "title": layer.capitalize(),
            "summary": LAYER_SUMMARIES[layer],
            "tables": [
                (target, resolve_env(settings.target_table, target))
                for target in TARGET_NAMES
            ],
            "columns": list_table_columns(config, layer),
        }
        for layer, settings in layers.items()
    ]
    files["README.md"] = render("README.md.j2", layers=sections)
    return files


def write_files(directory: Path, files: dict[str, str]) -> list[Path]:
    """Writes the files, replacing any that exist.

    Args:
        directory: The project folder.
        files: File contents keyed by path relative to the project folder.

    Returns:
        The paths written, in the files' order.

    Raises:
        OSError: If a folder or file cannot be written.
    """
    paths = []
    for name, contents in files.items():
        path = directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)
        paths.append(path)
    return paths

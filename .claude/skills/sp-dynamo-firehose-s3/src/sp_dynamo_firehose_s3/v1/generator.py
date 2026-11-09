"""Renders the v1 pipeline templates from a validated config."""

import json
import re
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import tomlkit
from jinja2 import Environment, PackageLoader, StrictUndefined

from sp_dynamo_firehose_s3.v1.config import Bundle, Metadata, PipelineConfig
from sp_dynamo_firehose_s3.v1.defaults import DEFAULTS
from sp_dynamo_firehose_s3.v1.fields import (
    BRONZE_DATA_COLUMN,
    BRONZE_METADATA_COLUMNS,
    ENV_PLACEHOLDER,
    NOTIFICATION_TARGET,
    PIPELINE_NAME_PATTERN,
    SILVER_METADATA,
    TARGET_NAMES,
    MetadataColumn,
)
from sp_dynamo_firehose_s3.v1.schema_inference import InferredSchema

PROJECT_FILE = "pyproject.toml"

# Describes each layer's responsibility in the job description. Kept free of
# pipeline details so every pipeline can share it.
LAYER_DESCRIPTIONS = {
    "bronze": (
        "The bronze task appends raw source records to a Delta table as they "
        "arrive, along with ingestion and source file metadata."
    ),
    "silver": (
        "The silver task flattens bronze records into typed columns, one row per "
        "change event."
    ),
    "gold": (
        "The gold task merges each change into a Delta table that keeps the "
        "latest version of every primary key."
    ),
}


def build_environment() -> Environment:
    """Builds the Jinja environment for the v1 templates.

    Returns:
        An environment that fails on undefined variables. Its `py_string` and
        `yaml_string` filters render a value as a Python or YAML string literal.
    """
    environment = Environment(
        loader=PackageLoader("sp_dynamo_firehose_s3.v1", "templates"),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    environment.filters["py_string"] = json.dumps
    environment.filters["yaml_string"] = json.dumps
    environment.filters["sql_string"] = sql_string
    environment.filters["sql_identifier"] = sql_identifier
    environment.filters["sql_table"] = sql_table
    environment.filters["md_cell"] = md_cell
    return environment


def md_cell(value: str) -> str:
    """Makes a value safe inside a Markdown table cell.

    Args:
        value: Cell text.

    Returns:
        The text on one line, with pipes escaped.
    """
    return " ".join(value.split()).replace("|", "\\|")


def sql_string(value: str) -> str:
    """Renders a value as a double-quoted Spark SQL string literal.

    Double quotes leave apostrophes in descriptions unescaped. Backslashes and
    double quotes are escaped with backslashes, which is how Spark SQL escapes
    inside string literals.

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
    """Quotes each part of a fully qualified table name for Spark SQL.

    Args:
        name: Table name in `catalog.schema.table` form.

    Returns:
        Each part quoted, joined by dots.
    """
    return ".".join(sql_identifier(part) for part in name.split("."))


class ProjectInitError(RuntimeError):
    """Raised when `uv init` cannot create the project."""


def to_identifier(name: str) -> str:
    """Converts a pipeline name to a Python and bundle resource identifier.

    Args:
        name: Pipeline name, which may contain hyphens.

    Returns:
        The name with hyphens replaced by underscores.
    """
    return name.replace("-", "_")


def list_task_keys(config: PipelineConfig) -> list[str]:
    """Lists the job's tasks in the order data flows through them.

    Args:
        config: Validated pipeline config.

    Returns:
        `bronze`, followed by `silver` and `gold` when the config has them.
    """
    job = config.job
    layers = {"bronze": job.bronze, "silver": job.silver, "gold": job.gold}
    return [name for name, layer in layers.items() if layer is not None]


def build_job_description(config: PipelineConfig, task_keys: list[str]) -> str:
    """Builds the job description from the pipeline and its layers.

    Args:
        config: Validated pipeline config.
        task_keys: The job's tasks.

    Returns:
        The pipeline description followed by each task's responsibility.
    """
    sentences = [config.metadata.description.strip()]
    sentences += [LAYER_DESCRIPTIONS[task_key] for task_key in task_keys]
    return " ".join(sentences)


def render_bundle_files(
    environment: Environment, config: PipelineConfig
) -> dict[str, str]:
    """Renders the bundle configuration and job resource.

    Args:
        environment: Jinja environment with the v1 templates.
        config: Validated pipeline config.

    Returns:
        The `databricks.yml` and `resource/<name>.job.yml` contents keyed by path.
    """
    resource_name = to_identifier(config.metadata.name)
    task_keys = list_task_keys(config)
    return {
        "databricks.yml": environment.get_template("databricks.yml.j2").render(
            metadata=config.metadata,
            bundle=config.bundle,
            notification_target=NOTIFICATION_TARGET,
        ),
        f"resource/{config.metadata.name}.job.yml": environment.get_template(
            "job.yml.j2"
        ).render(
            metadata=config.metadata,
            bundle=config.bundle,
            notification_target=NOTIFICATION_TARGET,
            job=config.job,
            job_key=resource_name,
            cluster_key=f"{resource_name}_cluster",
            package_name=resource_name,
            task_keys=task_keys,
            description=build_job_description(config, task_keys),
        ),
    }


TABLE_DESCRIPTIONS = {
    "bronze": (
        "Raw DynamoDB change events delivered by Firehose, one row per record, "
        "with ingestion and source file metadata."
    ),
    "silver": (
        "DynamoDB change events flattened into typed columns, one row per change."
    ),
    "gold": "The latest version of every primary key, merged from silver.",
}
DDL_DIR = "ddl"
PROJECT_README = "README.md"
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


def list_table_columns(config: PipelineConfig, layer: str) -> list[MetadataColumn]:
    """Lists a layer's table columns in the order its job writes them.

    Args:
        config: Validated pipeline config.
        layer: `bronze`, `silver`, or `gold`.

    Returns:
        Bronze's data and load metadata columns, or, for silver and gold, the
        configured columns followed by the event and load metadata columns.
        Configured columns have a comment only if they have a description.
    """
    if layer == "bronze" or config.table_schema is None:
        return [BRONZE_DATA_COLUMN, *BRONZE_METADATA_COLUMNS]
    configured = [
        MetadataColumn(column.name, column.target_type, column.description or "")
        for column in config.table_schema.columns
    ]
    return [*configured, *SILVER_METADATA]


def render_ddl_files(
    environment: Environment, config: PipelineConfig
) -> dict[str, str]:
    """Renders each layer's table DDL for each target.

    Args:
        environment: Jinja environment for the v1 templates.
        config: Validated pipeline config.

    Returns:
        File contents keyed by `ddl/<target>/<layer>.sql`, for every target and
        every layer the job has.
    """
    template = environment.get_template("ddl.sql.j2")
    layers = {
        "bronze": config.job.bronze,
        "silver": config.job.silver,
        "gold": config.job.gold,
    }
    description = config.metadata.description.strip()
    files = {}
    for target in TARGET_NAMES:
        for layer in list_task_keys(config):
            files[f"{DDL_DIR}/{target}/{layer}.sql"] = template.render(
                layer=layer,
                pipeline=config.metadata.name,
                env=target,
                table=resolve_table(layers[layer].target_table, target),
                columns=list_table_columns(config, layer),
                comment=f"{description} {TABLE_DESCRIPTIONS[layer]}",
            )
    return files


def resolve_table(table: str, target: str) -> str:
    """Replaces `{env}` in a table name with a target's name.

    Args:
        table: Table name from the config.
        target: Target name, such as `prod`.

    Returns:
        The target's table name.
    """
    return table.replace(ENV_PLACEHOLDER, target)


def render_project_readme(environment: Environment, config: PipelineConfig) -> str:
    """Renders the project's README: one section per layer with its table.

    Args:
        environment: Jinja environment for the v1 templates.
        config: Validated pipeline config.

    Returns:
        The README contents.
    """
    layers = {
        "bronze": config.job.bronze,
        "silver": config.job.silver,
        "gold": config.job.gold,
    }
    sections = [
        {
            "title": layer.capitalize(),
            "summary": LAYER_SUMMARIES[layer],
            "tables": [
                (target, resolve_table(layers[layer].target_table, target))
                for target in TARGET_NAMES
            ],
            "columns": list_table_columns(config, layer),
        }
        for layer in list_task_keys(config)
    ]
    return environment.get_template("README.md.j2").render(
        pipeline=config.metadata.name,
        description=config.metadata.description.strip(),
        layers=sections,
    )


def render_files(config: PipelineConfig) -> dict[str, str]:
    """Renders every job the config defines.

    Args:
        config: Validated pipeline config.

    Returns:
        File contents keyed by path relative to the project directory: the bundle's
        `databricks.yml`, its job resource, and the package with the job code and
        the logging and argument modules the jobs share. The silver and gold jobs
        are included only when the config has those layers. Each layer's table
        DDL is included for every target, and the project's README describes each
        layer's table.
    """
    environment = build_environment()
    job = config.job
    package = to_identifier(config.metadata.name)
    package_dir = f"src/{package}"

    def render(template: str, **context: object) -> str:
        """Renders a package template with the package name available."""
        return environment.get_template(template).render(package=package, **context)

    files = render_bundle_files(environment, config)
    files |= render_ddl_files(environment, config)
    files[PROJECT_README] = render_project_readme(environment, config)
    files |= {
        f"{package_dir}/__init__.py": (
            f'"""Databricks jobs for the {config.metadata.name} pipeline."""\n'
        ),
        f"{package_dir}/cli.py": render("cli.py.j2"),
        f"{package_dir}/env.py": render("env.py.j2"),
        f"{package_dir}/logs.py": render("logs.py.j2"),
        f"{package_dir}/bronze.py": render("bronze.py.j2", job=job, bronze=job.bronze),
    }
    if job.silver is not None and config.table_schema is not None:
        files[f"{package_dir}/silver.py"] = render(
            "silver.py.j2",
            job=job,
            bronze=job.bronze,
            silver=job.silver,
            columns=config.table_schema.columns,
        )
    if job.gold is not None and job.silver is not None and config.table_schema:
        files[f"{package_dir}/gold.py"] = render(
            "gold.py.j2",
            job=job,
            silver=job.silver,
            gold=job.gold,
            primary_keys=config.table_schema.primary_keys,
        )
    return files


def init_project(directory: Path, metadata: Metadata) -> None:
    """Creates the uv project unless it already exists.

    Args:
        directory: Project directory.
        metadata: Pipeline metadata with the project's name, description, and
            Python version.

    Raises:
        ProjectInitError: If `uv init` fails.
    """
    if (directory / PROJECT_FILE).exists():
        return
    command = [
        "uv",
        "init",
        str(directory),
        "--name",
        metadata.name,
        "--package",
        "--vcs",
        "none",
        "--no-workspace",
        "--python",
        metadata.python_version,
        "--description",
        metadata.description,
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise ProjectInitError(f"uv init failed: {result.stderr.strip()}")


def configure_project(
    project_file: Path, metadata: Metadata, task_keys: Sequence[str]
) -> None:
    """Sets the project's build backend and its scripts to the job's entry points.

    Other settings, such as dependencies, are kept.

    Args:
        project_file: Path to the project's `pyproject.toml`.
        metadata: Pipeline metadata with the project's name and `uv_build` version
            range.
        task_keys: The job's tasks, each with a module of the same name.

    Raises:
        ValueError: If the file has no `[project]` table.
    """
    document = tomlkit.parse(project_file.read_text())
    project = document.get("project")
    if not isinstance(project, dict):
        raise ValueError(f"{project_file} has no [project] table")

    package = to_identifier(metadata.name)
    scripts = tomlkit.table()
    for task_key in task_keys:
        scripts[task_key] = f"{package}.{task_key}:main"
    project["scripts"] = scripts

    build_system = tomlkit.table()
    build_system["requires"] = [f"uv_build{metadata.uv_build_version}"]
    build_system["build-backend"] = "uv_build"
    document["build-system"] = build_system

    project_file.write_text(tomlkit.dumps(document))


REQUIRED_COMMENT = "Required field, please fill."
BATCH_ONLY_COMMENT = "This field only applies when executionMode is batch."
CONTINUOUS_ONLY_COMMENT = "This field only applies when executionMode is continuous."
DESCRIPTION_PLACEHOLDER = "Please fill or remove."
# Example shown on the commented-out batchSchedule line: daily at 02:00 UTC.
SAMPLE_BATCH_SCHEDULE = "0 0 2 * * ?"


def hyphenate(table: str) -> str:
    """Lowercases a table name and turns every other character into a hyphen.

    Args:
        table: DynamoDB table name, such as `BO.Accounts`.

    Returns:
        The hyphenated name, such as `bo-accounts`.
    """
    return re.sub(r"[^A-Za-z0-9]", "-", table).lower()


def default_pipeline_name(table: str) -> str:
    """Derives the pipeline name from the DynamoDB table name.

    Every character that is not a letter or digit becomes a hyphen, and letters
    are lowercased, so `bo.accounts` becomes `bo-accounts`.

    Args:
        table: DynamoDB table name.

    Returns:
        The pipeline name, or an empty string if the result is not a valid
        pipeline name, such as one that starts with a digit or hyphen.
    """
    name = hyphenate(table)
    return name if PIPELINE_NAME_PATTERN.fullmatch(name) else ""


def render_config_template(schema: InferredSchema, table: str) -> str:
    """Renders a pipeline YAML with every supported field.

    Fields with defaults show their default values from `defaults.toml`, and
    `metadata.name` is derived from the table name. Required fields without
    defaults are empty and marked for the user to fill. Fields that only apply to
    the other execution mode are commented out.

    Args:
        schema: Schema inferred from the DynamoDB table.
        table: DynamoDB table name.

    Returns:
        The YAML contents.
    """
    return (
        build_environment()
        .get_template("pipeline.yaml.j2")
        .render(
            name=default_pipeline_name(table),
            bundle=Bundle(),
            notification_target=NOTIFICATION_TARGET,
            notification_defaults=DEFAULTS.bundle,
            python_version=DEFAULTS.metadata.python_version,
            uv_build_version=DEFAULTS.metadata.uv_build_version,
            execution_mode=DEFAULTS.job.execution_mode,
            trigger_interval=DEFAULTS.job.trigger_interval,
            upsert_latest_key=DEFAULTS.job.upsert_latest_key,
            layers={
                "bronze": DEFAULTS.job.bronze,
                "silver": DEFAULTS.job.silver,
                "gold": DEFAULTS.job.gold,
            },
            required=REQUIRED_COMMENT,
            batch_only=BATCH_ONLY_COMMENT,
            sample_batch_schedule=SAMPLE_BATCH_SCHEDULE,
            continuous_only=CONTINUOUS_ONLY_COMMENT,
            description_placeholder=DESCRIPTION_PLACEHOLDER,
            schema=schema,
        )
    )


def generate(
    raw_config: Mapping[str, Any],
    output_dir: Path | None = None,
    force: bool = False,
) -> list[Path]:
    """Validates a config, creates its uv project, and writes its pipeline files.

    The project is created with `uv init` unless its `pyproject.toml` exists. The
    generator then writes its files and sets the project's build backend and its
    scripts to the job's entry points. Nothing is created if validation fails or,
    without `force`, if any generated file already exists.

    Args:
        raw_config: Parsed pipeline YAML.
        output_dir: Project directory. Defaults to
            `./<cli.projects_dir>/<metadata.name>/`, with `cli.projects_dir` from
            `defaults.toml`.
        force: Whether to overwrite existing files.

    Returns:
        The paths of the written files, ending with the updated `pyproject.toml`.

    Raises:
        FileExistsError: If a generated file exists and `force` is false.
    """
    config = PipelineConfig.model_validate(raw_config)
    files = render_files(config)

    directory = (
        output_dir or Path.cwd() / DEFAULTS.cli.projects_dir / config.metadata.name
    )
    paths = {directory / name: source for name, source in files.items()}
    existing = [str(path) for path in paths if path.exists()]
    if existing and not force:
        raise FileExistsError(f"Output files already exist: {', '.join(existing)}")

    init_project(directory, config.metadata)
    for path, source in paths.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
    project_file = directory / PROJECT_FILE
    configure_project(project_file, config.metadata, list_task_keys(config))
    return [*paths, project_file]

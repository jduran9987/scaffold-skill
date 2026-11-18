import json
from pathlib import Path

import pytest

from sp_dynamo_firehose_s3.scaffold.config import (
    SILVER_METADATA_NAMES,
    ConfigError,
    build_config,
    check_quartz_cron,
    to_firehose_name,
    to_snake_case,
)
from sp_dynamo_firehose_s3.scaffold.render import SILVER_METADATA_COLUMNS

INPUTS = (Path(__file__).parent / "fixtures" / "inputs.toml").read_text()
SESSION_ID = "e5d4c591-1e54-431d-ba66-6522f39f0a15"
PROJECT_DIR = "/work/project"
COLUMNS = [
    {"name": "account_id", "path": "accountId", "dynamo_type": "S", "key": True},
    {"name": "balance", "path": "balance", "dynamo_type": "N", "key": False},
    {"name": "tags", "path": "tags", "dynamo_type": "SS", "key": False},
]
ANSWERS = {
    "types": {
        "account_id": "string",
        "balance": "decimal(22,2)",
        "tags": "array<string>",
    },
    "execution_mode": "batch",
    "batch_schedule": "0 0 6 * * ?",
}


@pytest.fixture(autouse=True)
def sessions_dir(tmp_path, monkeypatch):
    """Reads session folders under tmp_path instead of the system temp folder."""
    path = tmp_path / "sessions"
    monkeypatch.setattr("sp_dynamo_firehose_s3.scaffold.config.SESSIONS_DIR", path)
    return path


@pytest.fixture
def inputs_path(tmp_path, monkeypatch):
    """Points build_config at a file in tmp_path instead of the tests' copy."""
    path = tmp_path / "inputs.toml"
    monkeypatch.setattr("sp_dynamo_firehose_s3.scaffold.config.INPUTS_PATH", path)
    return path


def write_columns(sessions_dir, table, columns=COLUMNS):
    folder = sessions_dir / SESSION_ID
    folder.mkdir(parents=True, exist_ok=True)
    data = {"dynamo_table_name": table, "columns": columns}
    (folder / "columns.json").write_text(json.dumps(data))


@pytest.fixture
def build(sessions_dir):
    """Builds the config after writing columns.json for the table."""

    def build(table="bo.suspenseAccounts", answers=ANSWERS, columns=COLUMNS):
        write_columns(sessions_dir, table, columns)
        text = answers if isinstance(answers, str) else json.dumps(answers)
        return build_config(table, SESSION_ID, PROJECT_DIR, text)

    return build


@pytest.fixture
def problems(build):
    """Lists the problems building the config raises."""

    def problems(**kwargs):
        with pytest.raises(ConfigError) as raised:
            build(**kwargs)
        return raised.value.problems

    return problems


@pytest.fixture
def problems_for(inputs_path, problems):
    """Lists the problems after replacing text in the shipped inputs.toml."""

    def problems_for(old, new, **kwargs):
        assert old in INPUTS, f"{old!r} is not in the tests' inputs.toml"
        inputs_path.write_text(INPUTS.replace(old, new, 1))
        return problems(**kwargs)

    return problems_for


def with_answers(**changes):
    return {**ANSWERS, **changes}


# Derived table names


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("bo.suspenseAccounts", "bo_suspense_accounts"),
        ("orders", "orders"),
        ("bo-HTTPLogs.v2", "bo_http_logs_v2"),
        ("Orders__2024", "orders_2024"),
        ("already_snake", "already_snake"),
        ("HTTPServer", "http_server"),
        ("v2Data", "v2_data"),
        ("HTTP2", "http2"),
        ("_.leading-and-trailing._", "leading_and_trailing"),
        ("ALLCAPS", "allcaps"),
        ("...", ""),
    ],
)
def test_to_snake_case(name, expected):
    assert to_snake_case(name) == expected


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("bo.suspenseAccounts", "bo-suspenseAccounts"),
        ("a.b.c", "a-b-c"),
        ("orders", "orders"),
        ("my_table-1", "my_table-1"),
    ],
)
def test_to_firehose_name(name, expected):
    assert to_firehose_name(name) == expected


def test_table_names(build):
    config = build()
    assert config.dynamo_table_name == "bo.suspenseAccounts"
    assert config.table_name == "bo_suspense_accounts"
    assert config.firehose_table_name == "bo-suspenseAccounts"


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        ("ab", "dynamo_table_name: String should have at least 3 characters"),
        ("2024orders", "table_name: String should match pattern '^[a-z][a-z0-9_]*$'"),
    ],
)
def test_invalid_table_names(problems, table, expected):
    assert problems(table=table) == expected


def test_table_name_without_letters_or_digits(problems):
    target_table = "String should match pattern '^[^.\\s]+\\.[^.\\s]+\\.[^.\\s]+$'"
    assert problems(table="...") == (
        "table_name: String should match pattern '^[a-z][a-z0-9_]*$'; "
        f"job.bronze.target_table: {target_table}; "
        f"job.silver.target_table: {target_table}; "
        f"job.gold.target_table: {target_table}"
    )


# Building from the shipped inputs


def test_shipped_inputs(build):
    config = build()
    assert config.schema_inference.allowed_types.for_type("BOOL") == [
        "boolean",
        "string",
    ]
    assert config.project.projects_dir == "bundles"
    assert config.bundle.tags == {"project": "bo_suspense_accounts", "SOURCE": "dynamo"}
    assert config.bundle.targets.prod.service_principal is None
    assert config.job.gold.upsert_latest_key == "_event_timestamp"
    assert config.job.timezone == "UTC"


def test_project_path(build):
    assert build().project_path == Path("/work/project/bundles/bo_suspense_accounts")


def test_layer_defaults(build):
    job = build().job
    assert job.bronze.source_path == (
        "s3://{env}-datalake-7472/firehose-stream/bo-suspenseAccounts/"
    )
    assert job.bronze.checkpoint_path == (
        "s3://{env}-datalake-7472/checkpoints/bo_suspense_accounts/bronze/"
    )
    assert job.bronze.target_table == "{env}.bronze.bo_suspense_accounts"
    assert job.silver.target_table == "{env}.silver.bo_suspense_accounts"
    assert job.gold.checkpoint_path == (
        "s3://{env}-datalake-7472/checkpoints/bo_suspense_accounts/gold/"
    )


def test_dynamo_table_name_placeholder_is_filled_in(inputs_path, build):
    inputs_path.write_text(
        INPUTS.replace('project = "{table_name}"', 'project = "{dynamo_table_name}"', 1)
    )
    assert build().bundle.tags["project"] == "bo.suspenseAccounts"


def test_env_is_left_as_is(build):
    assert build().bundle.target.service_principal == "sp_ingestion_{env}"


def test_other_actions_sections_are_ignored(inputs_path, build):
    start = INPUTS.index("\n[aws]\n")
    end = INPUTS.index("\n[schema_inference]\n")
    inputs_path.write_text(INPUTS[:start] + INPUTS[end:])
    assert build().table_name == "bo_suspense_accounts"


def test_config_is_immutable(build):
    config = build()
    with pytest.raises(ValueError):
        config.table_name = "other"


def test_file_cannot_override_the_arguments(inputs_path, build):
    inputs_path.write_text('table_name = "other"\nsession_id = "x"\n' + INPUTS)
    config = build()
    assert config.table_name == "bo_suspense_accounts"
    assert config.session_id == SESSION_ID


# Invalid inputs.toml values


def test_every_target_is_required(inputs_path, problems):
    start = INPUTS.index("[bundle.targets.uat]")
    end = INPUTS.index("[bundle.targets.prod]")
    inputs_path.write_text(INPUTS[:start] + INPUTS[end:])
    assert problems() == "bundle.targets.uat: Field required"


def test_target_can_override_the_service_principal(inputs_path, build):
    inputs_path.write_text(
        INPUTS.replace(
            "[bundle.targets.prod]\n",
            '[bundle.targets.prod]\nservice_principal = "sp"\n',
        )
    )
    assert build().bundle.targets.prod.service_principal == "sp"


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        (
            'service_principal_permissions = "can_manage"',
            'service_principal_permissions = "owner"',
            "bundle.targets.dev.service_principal_permissions: Input should be "
            "'can_manage', 'can_run' or 'can_view'",
        ),
        (
            'host = "https://dbc-f18f3dd9-412f.cloud.databricks.com"',
            'host = "dbc-f18f3dd9-412f.cloud.databricks.com"',
            "bundle.targets.dev.host: String should match pattern '^https://.+$'",
        ),
        (
            'upsert_latest_key = "_event_timestamp"',
            'upsert_latest_key = ""',
            "job.gold.upsert_latest_key: String should have at least 1 character",
        ),
        (
            'source_path = "s3://{env}-datalake-7472/',
            'source_path = "{env}-datalake-7472/',
            "job.bronze.source_path: String should match pattern '^s3://.+/$'",
        ),
        (
            'checkpoint_path = "s3://{env}-datalake-7472/checkpoints/{table_name}/'
            'silver/"',
            'checkpoint_path = ""',
            "job.silver.checkpoint_path: String should match pattern '^s3://.+/$'",
        ),
        (
            'target_table = "{env}.gold.{table_name}"',
            'target_table = "gold.{table_name}"',
            "job.gold.target_table: String should match pattern "
            "'^[^.\\s]+\\.[^.\\s]+\\.[^.\\s]+$'",
        ),
        ('timezone = "UTC"\n', "", "job.timezone: Field required"),
        (
            "num_workers = 1\n",
            "num_workers = 0\n",
            "bundle.targets.dev.num_workers: Input should be greater than or equal "
            "to 1",
        ),
        ("num_workers = 1\n", "", "bundle.targets.dev.num_workers: Field required"),
        (
            'BOOL = ["boolean", "string"]\n',
            "",
            "schema_inference.allowed_types.BOOL: Field required",
        ),
    ],
)
def test_invalid_inputs(problems_for, old, new, expected):
    assert problems_for(old, new) == expected


def test_missing_inputs_file(inputs_path, problems):
    assert problems() == f"{inputs_path}: No such file or directory"


def test_invalid_toml(inputs_path, problems):
    inputs_path.write_text("[aws\n")
    assert problems().startswith(f"{inputs_path}: is not valid TOML: ")


# Arguments


def test_columns_with_their_types(build):
    columns = build().columns
    assert [(column.name, column.spark_type, column.key) for column in columns] == [
        ("account_id", "string", True),
        ("balance", "decimal(22,2)", False),
        ("tags", "array<string>", False),
    ]
    assert columns[0].path == "accountId"


def test_invalid_session_id(sessions_dir):
    with pytest.raises(ConfigError) as raised:
        build_config("orders", "../escape", PROJECT_DIR, json.dumps(ANSWERS))
    assert raised.value.problems == (
        "session_id: String should match pattern "
        "'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'"
    )


def test_project_dir_must_be_absolute(sessions_dir):
    write_columns(sessions_dir, "orders")
    with pytest.raises(ConfigError) as raised:
        build_config("orders", SESSION_ID, "work", json.dumps(ANSWERS))
    assert raised.value.problems == "project_dir: String should match pattern '^/'"


# columns.json


def test_missing_columns_file(sessions_dir):
    with pytest.raises(ConfigError) as raised:
        build_config("orders", SESSION_ID, PROJECT_DIR, json.dumps(ANSWERS))
    path = sessions_dir / SESSION_ID / "columns.json"
    assert raised.value.problems == f"source: {path}: No such file or directory"


def test_invalid_columns_file(sessions_dir):
    folder = sessions_dir / SESSION_ID
    folder.mkdir(parents=True)
    (folder / "columns.json").write_text("{")
    with pytest.raises(ConfigError) as raised:
        build_config("orders", SESSION_ID, PROJECT_DIR, json.dumps(ANSWERS))
    assert raised.value.problems.startswith(
        f"source: {folder / 'columns.json'}: is not valid JSON: "
    )


def test_columns_file_for_another_table(sessions_dir):
    write_columns(sessions_dir, "other")
    with pytest.raises(ConfigError) as raised:
        build_config("orders", SESSION_ID, PROJECT_DIR, json.dumps(ANSWERS))
    assert raised.value.problems == (
        'Value error, columns.json is for table "other", not "orders"'
    )


# Answers


def test_answers_are_not_json(problems):
    assert problems(answers="types: x").startswith("answers: is not valid JSON: ")


def test_answers_must_be_an_object(problems):
    assert problems(answers="null") == (
        "answers: Input should be a valid dictionary or instance of Answers"
    )


def test_continuous(build):
    config = build(
        answers=with_answers(execution_mode="continuous", batch_schedule=None)
    )
    assert config.answers.execution_mode == "continuous"
    assert config.answers.batch_schedule is None


@pytest.mark.parametrize(
    ("answers", "expected"),
    [
        (
            with_answers(execution_mode="streaming"),
            "answers.execution_mode: Input should be 'batch' or 'continuous'",
        ),
        (
            with_answers(batch_schedule=None),
            "answers: Value error, batch_schedule is required when execution_mode "
            "is batch",
        ),
        (
            with_answers(execution_mode="continuous"),
            "answers: Value error, batch_schedule must be null when execution_mode "
            "is continuous",
        ),
        (
            with_answers(batch_schedule="0 6 * * *"),
            "answers.batch_schedule: Value error, must have 6 or 7 fields "
            "separated by spaces, not 5",
        ),
        (
            {key: value for key, value in ANSWERS.items() if key != "batch_schedule"},
            "answers.batch_schedule: Field required",
        ),
        (
            with_answers(extra=1),
            "answers.extra: Extra inputs are not permitted",
        ),
    ],
)
def test_invalid_trigger(problems, answers, expected):
    assert problems(answers=answers) == expected


def test_every_type_problem_is_listed(problems):
    answers = with_answers(
        types={"account_id": "timestamp", "balance": "date", "unknown": "string"}
    )
    assert problems(answers=answers) == (
        "Value error, answers.types has no type for tags; answers.types has types "
        "for columns that do not exist: unknown; answers.types.balance is "
        '"date", but a N column can be int, bigint, double, decimal(22,2), string'
    )


def test_answers_and_inputs_problems_are_listed_together(problems_for):
    assert problems_for(
        'timezone = "UTC"\n', "", answers=with_answers(execution_mode="x")
    ) == (
        "job.timezone: Field required; answers.execution_mode: Input should be "
        "'batch' or 'continuous'"
    )


# Quartz cron expressions


@pytest.mark.parametrize(
    "expression",
    [
        "0 0 6 * * ?",
        "0 0 6 ? * MON-FRI",
        "0 */15 * * * ?",
        "0 0 12 1,15 * ?",
        "0 30 2 L * ?",
        "0 0 0 LW * ?",
        "0 0 0 15W * ?",
        "0 0 0 L-3 * ?",
        "0 0 8 ? * 6#3",
        "0 0 8 ? * 6L",
        "0 0 0 ? JAN-MAR SUN 2030",
        "0 0 0 * * ?  ",
    ],
)
def test_valid_cron(expression):
    assert check_quartz_cron(expression) == expression


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("0 6 * * *", "must have 6 or 7 fields separated by spaces, not 5"),
        ("0 0 6 * * ? 2030 x", "must have 6 or 7 fields separated by spaces, not 8"),
        (
            "0 0 6 * * *",
            "exactly one of the day of month and day of week fields must be ?",
        ),
        (
            "0 0 6 ? * ?",
            "exactly one of the day of month and day of week fields must be ?",
        ),
        ("60 0 6 * * ?", 'the seconds field "60" is not valid'),
        ("0 0 24 * * ?", 'the hours field "24" is not valid'),
        ("0 0 6 32 * ?", 'the day of month field "32" is not valid'),
        ("0 0 6 ? 13 MON", 'the month field "13" is not valid'),
        ("0 0 6 ? * FUN", 'the day of week field "FUN" is not valid'),
        ("0 0 6 ? * 8", 'the day of week field "8" is not valid'),
        ("0 */0 6 * * ?", 'the minutes field "*/0" is not valid'),
        ("0 0 6,, * * ?", 'the hours field "6,," is not valid'),
        ("? 0 6 * * ?", 'the seconds field "?" is not valid'),
        ("0 0 6 * * ? 1969", 'the year field "1969" is not valid'),
    ],
)
def test_invalid_cron(expression, expected):
    with pytest.raises(ValueError) as raised:
        check_quartz_cron(expression)
    assert str(raised.value) == expected


# Values the generated pipeline depends on


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        (
            'trigger_interval = "1 minute"',
            'trigger_interval = "1min"',
            "job.trigger_interval: String should match pattern "
            "'^[1-9][0-9]* (second|minute|hour|day)s?$'",
        ),
        (
            'timezone = "UTC"',
            'timezone = "Mars/Base"',
            'job.timezone: Value error, "Mars/Base" is not a known time zone',
        ),
        (
            "checkpoints/{table_name}/silver/",
            "checkpoints/{table_name}/bronze/",
            "job: Value error, each layer needs its own checkpoint_path",
        ),
        (
            'target_table = "{env}.gold.{table_name}"',
            'target_table = "{env}.silver.{table_name}"',
            "job: Value error, each layer needs its own target_table",
        ),
        (
            'upsert_latest_key = "_event_timestamp"',
            'upsert_latest_key = "modified"',
            'Value error, job.gold.upsert_latest_key is "modified", which is not '
            "a silver column",
        ),
    ],
)
def test_values_the_pipeline_depends_on(problems_for, old, new, expected):
    assert problems_for(old, new) == expected


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ('trigger_interval = "1 minute"', 'trigger_interval = "30 seconds"'),
        ('timezone = "UTC"', 'timezone = "America/New_York"'),
        ('upsert_latest_key = "_event_timestamp"', 'upsert_latest_key = "balance"'),
    ],
)
def test_other_values_are_accepted(inputs_path, build, old, new):
    assert old in INPUTS
    inputs_path.write_text(INPUTS.replace(old, new, 1))
    build()


def test_silver_metadata_names_match_the_rendered_columns():
    assert SILVER_METADATA_NAMES == tuple(
        column.name for column in SILVER_METADATA_COLUMNS
    )

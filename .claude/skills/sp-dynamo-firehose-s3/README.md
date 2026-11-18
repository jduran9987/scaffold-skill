# sp-dynamo-firehose-s3

Claude Code skill that sets up a Databricks pipeline for a DynamoDB table whose
changes flow through Kinesis and Firehose to S3. `SKILL.md` is the agent's
procedure; this file is the reference.

## Flow

Invoke with `/sp-dynamo-firehose-s3`. The agent asks the table name, checks
whether its project exists (asking whether to overwrite it if so), asks the
trigger and schedule, then runs three commands in order. create-stream ends the
run only on
`TABLE_NOT_FOUND`; any other result continues. prepare-inputs continues only on
`COLUMNS_FOUND`.

| Step | Agent does | Shown to the user |
|---|---|---|
| 1 | Asks the DynamoDB table name (free text) | The question |
| 2 | Runs `check-project`. If the project exists, asks whether to overwrite it, and stops on No | The question, only if the project exists |
| 3 | Asks the trigger: `batch` or `continuous` | The question |
| 4 | Asks a Databricks Quartz cron expression, for batch only | The question |
| 5 | Runs `create-stream`; reruns once on `TIMEOUT` | Its report |
| 6 | Runs `prepare-inputs` | Its `## Report` section only |
| 7 | Picks a Spark type per column from fixed rules in `SKILL.md` | Nothing |
| 8 | Runs `scaffold` | Its report |

## Commands

Run as `uv run --project <skill dir> <command> <arguments>`.

| Command | Arguments | Reads | Writes |
|---|---|---|---|
| `check-project` | table name, project folder | `[project]` | nothing |
| `create-stream` | table name | `[aws]`, `[kinesis]`, `[firehose]`, `[wait]` | AWS resources |
| `prepare-inputs` | session ID, table name | `[aws]`, `[schema_inference]` | `columns.json` |
| `scaffold` | session ID, table name, project folder; answers on stdin | `[schema_inference]`, `[project]`, `[bundle]`, `[job]`, `columns.json` | the project |

- **Table name:** passed exactly as typed. Each command checks it against
  DynamoDB's rules.
- **Session ID:** `${CLAUDE_SESSION_ID}`, a lowercase UUID. It names the folder
  `<system temp>/sp-dynamo-firehose-s3/<session ID>/` that holds `columns.json`.
  scaffold deletes the folder when it ends, whatever the result.
- **Project folder:** `${CLAUDE_PROJECT_DIR}`. The project is written to
  `<project folder>/<projects_dir>/<table_name>/`. check-project reports
  whether that folder exists. scaffold replaces the project's files and leaves
  other files in the folder alone.
- **Answers:** JSON, for example:

  ```json
  {"types": {"account_id": "string", "balance": "decimal(22,2)"},
   "execution_mode": "batch", "batch_schedule": "0 0 6 * * ?"}
  ```

  `batch_schedule` is `null` for `continuous`.

Every command prints one report and exits 0 on success, 1 otherwise:

```
<command> <table name>
  <detail lines, if any>
Result: <CODE>
  <what happened>
Next step: <what to do>
```

## check-project

Works out the project folder from the table name and `project.projects_dir`, and
reports whether it exists. Writes nothing.

| Code | Meaning |
|---|---|
| `PROJECT_NEW` | The project folder does not exist |
| `PROJECT_EXISTS` | The project folder exists. The message is the question the agent asks. |
| `USAGE` | Not exactly two arguments |
| `CONFIG_INVALID` | The table name, project folder, or `[project]` is invalid |

## create-stream

Creates, for table `<t>`, only what is missing, in order, and never changes what
exists:

1. Checks table `<t>` exists, and that it streams to no Kinesis stream other than
   `<t>-stream`. Nothing is created if either check fails.
2. Kinesis stream `<t>-stream`, with `kinesis.shard_count` shards.
3. Streaming from table `<t>` to `<t>-stream`.
4. Firehose stream `<t>-firehose`, reading `<t>-stream` and writing to
   `firehose.bucket_arn` with dynamic partitioning by table name.

Each step waits until its resource is active. All waits share one
`wait.timeout_seconds` limit, at most 540.

| Code | Meaning |
|---|---|
| `CREATED` | Every resource is active; at least one was created |
| `ALREADY_EXISTED` | Every resource already existed and is active |
| `USAGE` | Not exactly one argument |
| `CONFIG_INVALID` | The table name or `inputs.toml` is invalid |
| `TABLE_NOT_FOUND` | The table does not exist for the profile and region |
| `TABLE_STREAMS_ELSEWHERE` | The table already streams to another Kinesis stream |
| `KINESIS_FAILED` | The Kinesis stream has an unusable status |
| `STREAMING_FAILED` | Streaming to the Kinesis stream failed to enable |
| `FIREHOSE_FAILED` | The Firehose stream failed to create |
| `TIMEOUT` | A resource was not active within the time limit |
| `AWS_CLI_MISSING` | The AWS CLI is not installed |
| `AWS_CREDENTIALS` | The profile has no valid credentials |
| `AWS_ACCESS_DENIED` | The profile lacks a permission |
| `AWS_ERROR` | Any other AWS error |

## prepare-inputs

Reads the table's keys with `describe-table` and the first
`schema_inference.sample_size` items with `scan`, then flattens them:

| Rule | Example |
|---|---|
| Maps are flattened at every depth | `owner.address.city` → `owner__address__city` |
| Names are snake_case, joined by `__` | `owner.zipCode` → `owner__zip_code` |
| Lists and sets stay one column | `tags` (`SS`) → `tags` |
| The type is the most common one seen, ignoring NULL | `S` ×98, `NULL` ×2 → `S` |
| A column NULL in every item is `S` | `notes` (`NULL` ×100) → `S` |
| Attribute names that cannot be column names are left out | `my-attr`, `1st` |
| Key columns come first, partition then sort; the rest by path | |
| An empty table gives the key columns only | |

On success, the output starts with `## Columns (for the agent)`: one JSON object
per column with `name`, `dynamo_type`, `key`, up to 3 different example values
(each cut to 40 characters), and its `allowed_types`. `## Report` follows.
`columns.json` holds name, path, DynamoDB type, and key, with no examples.

| Code | Meaning |
|---|---|
| `COLUMNS_FOUND` | Columns written; the report lists attributes left out |
| `USAGE` | Not exactly two arguments |
| `CONFIG_INVALID` | The table name, session ID, or `inputs.toml` is invalid |
| `TABLE_NOT_FOUND` | The table does not exist for the profile and region |
| `KEY_NAME_UNSUPPORTED` | A key attribute's name cannot be a column name |
| `COLUMN_NAME_CLASH` | Two attributes would become the same column |
| `WRITE_FAILED` | `columns.json` could not be written |
| `AWS_CLI_MISSING`, `AWS_CREDENTIALS`, `AWS_ACCESS_DENIED`, `AWS_ERROR` | As for create-stream |

## scaffold

Checks everything in one model, then renders the project from templates:

- `inputs.toml`'s scaffold sections, with the checkpoint paths and target tables
  required.
- `columns.json` exists and is for this table.
- `types` has exactly one type per column, each in `allowed_types` for that
  column's DynamoDB type.
- `execution_mode` is `batch` or `continuous`; `batch_schedule` is a valid Quartz
  cron expression for batch and `null` for continuous.
- `table_name` is a valid Python package name.

| Code | Meaning |
|---|---|
| `GENERATED` | The project was written; the report lists every file |
| `USAGE` | Not exactly three arguments |
| `CONFIG_INVALID` | Any input is invalid; every problem is listed |
| `WRITE_FAILED` | A file could not be written |

### Generated project

```
<projects_dir>/<table_name>/
  pyproject.toml              uv project, uv_build backend, bronze/silver/gold scripts
  .python-version
  README.md                   each layer's tables and columns
  databricks.yml              bundle with dev, qa, uat, prod targets
  resource/<table_name>.job.yml
  src/<table_name>/           __init__, cli, env, logs, bronze, silver, gold
  ddl/<target>/<layer>.sql    one CREATE TABLE per target and layer
```

| Layer | Job |
|---|---|
| bronze | Auto Loader reads the Firehose files into one `VARIANT` column, with load metadata |
| silver | Flattens each change event into the typed columns, one row per change |
| gold | Keeps the latest change per primary key and merges it into the table |

- **Gold's primary key** is the table's key columns: the partition key, plus the
  sort key if there is one. Within each microbatch, gold keeps the row with the
  latest `job.gold.upsert_latest_key` per primary key, then merges on the
  primary key.
- **Batch** jobs run on `batch_schedule` in `job.timezone`, with the tasks in
  order. **Continuous** jobs run the three tasks side by side, each triggering
  every `job.trigger_interval`.
- **The job cluster** uses the target's `compute_policy`, with
  `apply_policy_default_values`, and the target's `num_workers`.
- **`{env}`** in paths and table names becomes the target's name when the job
  runs.
- **Failure notifications** are set by the prod target only.
- **Comments in the DDL** are on the job-added columns only.

## inputs.toml

Holds every value the user is not asked for. Each value is described in the
file's comments. Values are checked by type, pattern, and AWS limit; only these
are fixed, because AWS or the generated pipeline depends on them:

| Value | Must be | Depends on it |
|---|---|---|
| `firehose.compression_format` | `GZIP` | bronze reads only `*.gz` files |
| `firehose.record_delimiter` | `\n` | bronze reads one JSON record per line |
| `firehose.metadata_extraction_query` | The shipped query | `{firehose_table_name}` and bronze's source path |
| `firehose.prefix` | A folder, then `!{partitionKeyFromQuery:tableName}/` | bronze's `source_path` |
| `firehose.json_parsing_engine` | `JQ-1.6` | AWS |
| `kinesis.name`, `firehose.name` | `{dynamo_table_name}-stream`, `{dynamo_table_name}-firehose` | create-stream |
| `bundle.targets.*.service_principal_permissions` | `can_manage`, `can_run`, or `can_view` | Databricks bundles |
| `job.trigger_interval` | A Spark trigger, such as `1 minute` | the jobs' continuous trigger |
| `job.timezone` | An IANA time zone | the job schedule |
| `job.*.checkpoint_path`, `job.*.target_table` | Different for each layer | the jobs |
| `job.gold.upsert_latest_key` | A silver column | gold's dedup and merge |

| Placeholder | Becomes | Filled by |
|---|---|---|
| `{dynamo_table_name}` | The table name as typed: `bo.suspenseAccounts` | Every action |
| `{table_name}` | snake_case: `bo_suspense_accounts` | scaffold |
| `{firehose_table_name}` | Dots replaced by dashes: `bo-suspenseAccounts` | scaffold |
| `{env}` | The target's name, when the generated job runs | The generated project |
| `!{...}` | Firehose's own syntax, passed to AWS unchanged | Firehose |

## Development

Run from this folder:

```bash
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
UPDATE_EXPECTED=1 uv run pytest tests/test_scaffold.py
```

The last command regenerates the byte-exact expected project in
`tests/fixtures/scaffold/` after an intended change. Review the diff before
committing.

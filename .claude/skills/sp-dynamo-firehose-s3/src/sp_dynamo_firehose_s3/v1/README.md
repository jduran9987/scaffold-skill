# sp-dynamo-firehose-s3 (v1)

- [Overview](#overview)
- [Skill](#skill)
  - [create-stream](#create-stream)
  - [generate-yaml](#generate-yaml)
  - [scaffold](#scaffold)
- [YAML reference](#yaml-reference)
- [Architecture](#architecture)
- [Defaults](#defaults)

## Overview

A Claude Code skill that builds Databricks pipelines for data flowing from DynamoDB
through Kinesis and Firehose to S3.

```
DynamoDB ──► Kinesis ──► Firehose ──► S3 ──► bronze ──► silver ──► gold
└──────────── create-stream ────────────┘   └──────── scaffold ───────┘
```

| Action | Purpose |
|---|---|
| `create-stream` | Provisions the Kinesis stream, the table's streaming to it, and the Firehose stream, without running AWS commands by hand. |
| `generate-yaml` | Writes a pipeline YAML, with the schema inferred from the DynamoDB table, that needs only a light review and fill-in. |
| `scaffold` | Creates a complete replication project from the YAML: bundle, job, job code, and table DDL. |

Value:

- Fast, reliable generation of standardized code and projects.
- Minimal human review: output comes from predefined templates and code, not from
  the agent writing code.
- Deterministic: the same inputs always produce the same output.

Typical flow:

```
/sp-dynamo-firehose-s3 create-stream orders
/sp-dynamo-firehose-s3 generate-yaml orders
# review and fill in pipelines/orders.yaml
/sp-dynamo-firehose-s3 scaffold pipelines/orders.yaml
```

## Skill

Invoke as `/sp-dynamo-firehose-s3 <action> <args>`. The same actions run without
Claude:

```bash
uv run --project .claude/skills/sp-dynamo-firehose-s3 generate <action> <args>
```

Requirements:

- `uv`
- The AWS CLI, with a configured profile (SSO, static keys, or assumed role), for
  `create-stream` and `generate-yaml`. The profile and region come from
  [`defaults.toml`](#defaults). The skill never signs in; on expired credentials,
  run `aws sso login --profile <profile>` yourself.

`create-stream` and `generate-yaml` end every run with a fixed result block,
which Claude relays as printed:

```
Result: <CODE>
  <what happened>
Next step: <what to do>
```

Both can also end with these AWS codes: `AWS_CLI_MISSING`, `AWS_CREDENTIALS`,
`AWS_ACCESS_DENIED`, `AWS_NOT_FOUND`, `AWS_INVALID_ARGUMENT`, `AWS_ERROR`.

---

### create-stream

```
create-stream <dynamodb-table> [--check]
```

| Input | Description |
|---|---|
| `<dynamodb-table>` | Existing DynamoDB table to stream. |
| `--check` | Report what exists and what would be created. Creates nothing. |

Resources, in order:

| # | Resource | Name |
|---|---|---|
| 1 | DynamoDB table | `<table>` (must exist, never created) |
| 2 | Kinesis data stream | `<table>` |
| 3 | Table streaming to the Kinesis stream | — |
| 4 | Firehose stream, writing GZIP JSON to S3 | `<table>-firehose` |

- Firehose writes records under
  `s3://<bucket>/firehose-stream/<table>/yyyy/MM/dd/HH/`, with dots in the table
  name replaced by dashes. Failed records go under
  `firehose-stream/failures/<table>/`.
- A successful run prints the S3 path to use as `job.bronze.sourcePath`.
- The Firehose IAM role and the S3 bucket must already exist. Both are set in
  `defaults.toml`.

Behavior when resources exist:

| State | Behavior |
|---|---|
| Resource missing | Created, then waited on until active. |
| Resource exists and is connected correctly | Skipped. |
| Resource still being created | Waited on. |
| Table already streams to a different Kinesis stream | Blocked. Nothing is created. |
| Firehose stream exists but reads another stream or writes another bucket | Blocked. Nothing is created. |
| Firehose stream exists in a failed or deleting state | Blocked. Nothing is created. |
| Kinesis stream is being deleted | Blocked. Nothing is created. |
| Run fails partway | Created resources are kept. Rerunning resumes at the failed step. |

- All resources are checked before anything is created.
- Existing resources are never changed or deleted.
- A run waits at most a fixed total time (set in `defaults.toml`), then ends with
  `TIMEOUT`. Rerunning continues waiting.

Example:

```
create-stream orders
  1. DynamoDB table orders: exists
  2. Kinesis stream orders: created
  3. Streaming from table orders to Kinesis stream orders: created
  4. Firehose stream orders-firehose: created
  Created: Kinesis stream orders, Streaming from table orders to Kinesis stream orders, Firehose stream orders-firehose
Result: CREATED
  Every resource is active and connected. DynamoDB table orders streams to Kinesis stream orders, which Firehose stream orders-firehose delivers to arn:aws:s3:::my-bucket.
Next step: Set job.bronze.sourcePath to s3://my-bucket/firehose-stream/orders/.
```

Result codes:

| Code | Meaning |
|---|---|
| `CREATED` | At least one resource was created. All are active and connected. |
| `ALREADY_EXISTED` | Every resource already existed. All are active and connected. |
| `READY` | `--check`: everything exists and is active. |
| `WOULD_CREATE` | `--check`: nothing blocks the setup. Lists what would be created. |
| `IN_PROGRESS` | `--check`: everything exists, but something is still being created. |
| `INVALID_TABLE_NAME` | The name cannot name the streams. |
| `TABLE_NOT_FOUND` | The DynamoDB table does not exist in the configured region. |
| `TABLE_STREAMS_ELSEWHERE` | The table already streams to another Kinesis stream. |
| `TABLE_STREAMING_BUSY` | The table's streaming is being disabled. |
| `KINESIS_STREAM_DELETING` | The Kinesis stream is being deleted. |
| `FIREHOSE_WIRED_ELSEWHERE` | The Firehose stream reads another stream or writes another bucket. |
| `FIREHOSE_UNUSABLE` | The Firehose stream exists in a failed or deleting state. |
| `FIREHOSE_CONFIG_REJECTED` | AWS rejected the Firehose settings, usually the role or bucket. |
| `STREAMING_FAILED` | Enabling the table's streaming failed. |
| `FIREHOSE_FAILED` | Creating the Firehose stream failed. |
| `TIMEOUT` | A resource was not active within the wait limit. |
| `NOT_CONFIRMED` | The final check found a resource not active or not connected. |

---

### generate-yaml

```
generate-yaml <dynamodb-table> [--force]
```

| Input | Description |
|---|---|
| `<dynamodb-table>` | Existing DynamoDB table to infer the schema from. Its exact name. |
| `--force` | Overwrite the YAML if it exists. |

> **The table must exist.** If no table with that name exists in the configured
> region, the run ends with `TABLE_NOT_FOUND` and writes nothing.

Output path: `./pipelines/<table>.yaml`, with the table name lowercased and every
character that is not a letter or digit replaced by a hyphen. The `pipelines`
directory is set in `defaults.toml` and is created if it does not exist.

| Table | YAML |
|---|---|
| `orders` | `pipelines/orders.yaml` |
| `bo.accounts` | `pipelines/bo-accounts.yaml` |
| `BO_Accounts` | `pipelines/bo-accounts.yaml` |

What it writes:

- Every supported field. Fields with defaults show their default values.
- Required fields without a value are `""` and marked
  `# Required field, please fill.`
- `metadata.name` is the same hyphenated table name (`bo.accounts` →
  `bo-accounts`). If that is not a valid name, such as one starting with a digit,
  it is left `""` and marked required.
- Fields for the execution mode that is not the default are commented out.
  `batchSchedule` shows a sample Quartz expression.
- A `schema` section inferred from the table.

Schema inference:

| Rule | Example |
|---|---|
| Reads the table's keys and its first N items (N set in `defaults.toml`). | — |
| Key columns come first, partition key then sort key, and are the `primaryKeys`. | `id`, `created_at` |
| Nested maps are flattened at every depth. | `addressDetails.M.zipCode` |
| Column names are each path level in snake_case, joined by `__`. | `address_details__zip_code` |
| `dynamoType` is the most common type seen, ignoring `NULL`. | `N` |
| Attributes whose names are not `[A-Za-z][A-Za-z0-9_]*` are left out. | `order-id` |
| Attributes that produce the same name each keep a column. | `zipCode`, `zip_code` → two `zip_code` columns |
| `targetType` is left `""` for the developer. | — |
| `description` is `"Please fill or remove."` | — |

Duplicate column names must be renamed before `scaffold`; the result message lists
them.

Result codes:

| Code | Meaning |
|---|---|
| `GENERATED` | The file was written. |
| `INVALID_TABLE_NAME` | The argument is not a DynamoDB table name. Nothing was written. |
| `FILE_EXISTS` | The file exists. Nothing was written. Rerun with `--force` to overwrite. |
| `TABLE_NOT_FOUND` | The DynamoDB table does not exist in the configured region. Nothing was written. |
| `KEY_NOT_USABLE` | A key attribute's name cannot be a column. Nothing was written. |
| `WRITE_FAILED` | The file could not be written. |

---

### scaffold

```
scaffold <path-to-yaml> [--output-dir <path>] [--force]
```

| Input | Description |
|---|---|
| `<path-to-yaml>` | A filled-in pipeline YAML. See [YAML reference](#yaml-reference). |
| `--output-dir` | Project directory. Defaults to `./dabs/<metadata.name>/`. The `dabs` directory is set in `defaults.toml`. |
| `--force` | Overwrite existing generated files. |

- The YAML is fully validated before anything is written. On a validation error,
  nothing is generated and every invalid field is listed.
- Existing generated files are not overwritten without `--force`.
- An existing `pyproject.toml` is kept: only its scripts and build backend are set.

Generated project (`orders`, all layers):

```
dabs/orders/
├── databricks.yml               # bundle: targets dev, qa, uat, prod
├── resource/orders.job.yml      # one job with bronze, silver, gold tasks
├── pyproject.toml               # uv package, one script per task
├── README.md                    # each layer: what it does, its tables and columns
├── src/orders/
│   ├── bronze.py                # S3 files → bronze table
│   ├── silver.py                # bronze → typed columns, one row per change
│   ├── gold.py                  # silver → latest row per primary key (MERGE)
│   ├── cli.py, env.py, logs.py  # shared by the jobs
│   └── __init__.py
└── ddl/
    ├── dev/{bronze,silver,gold}.sql
    ├── qa/…  uat/…  prod/…      # one CREATE TABLE per layer per target
```

- `silver.py` and `gold.py`, and their DDL, are generated only when the YAML has
  those layers.
- The package name is `metadata.name` with hyphens replaced by underscores.
- The jobs never create their tables. Run the DDL for a target before its first
  job run.

Tables:

| Layer | Columns |
|---|---|
| bronze | `data` (VARIANT, the raw record), `_ingested_at`, `_file_path`, `_file_modification_time` |
| silver | Each `schema.columns` entry, then `_event_name`, `_event_id`, `_event_timestamp`, `_ingested_at`, `_file_path`, `_file_modification_time` |
| gold | Same as silver. One row per primary key. |

- Column comments come from each column's `description`. Columns without one get no
  comment.
- Metadata columns (prefixed `_`, and `data`) always get the same comments.
- Each table's comment is `metadata.description` followed by a fixed sentence for
  the layer.

Project README:

- One section per layer: a fixed description of the layer (the same in every
  project), the layer's table for each target, and its columns with type and
  description.
- Column descriptions match the DDL comments: from `schema.columns[].description`,
  blank when not set, and fixed for metadata columns.

## YAML reference

General rules:

- `version: v1` is required and selects this version of the skill.
- Unknown keys are rejected at every level.
- Types are strict: values are not converted. Quote strings YAML would read as
  numbers or booleans, such as `pythonVersion: "3.12"` and `dynamoType: "NULL"`.
- A key set to `null` is the same as leaving it out.
- `{env}` in paths and table names is replaced by the target name (`dev`, `qa`,
  `uat`, `prod`) when the job runs and in each target's DDL. It is the only
  placeholder: any other `{...}`, such as `{dev}`, is rejected.

Column key: **Req** = required. **Default** = "defaults.toml" when a default exists
there.

### `metadata`

| Field | Type | Req | Default | Description |
|---|---|---|---|---|
| `name` | string: `[a-z][a-z0-9_-]*` | Yes | — | Pipeline name. Names the project directory, package, and job. `generate-yaml` fills it from the table name. |
| `description` | string | Yes | — | What the pipeline does. Starts the job and table descriptions. |
| `pythonVersion` | string: `"3.N"` | No | defaults.toml | Python version of the generated project. |
| `uvBuildVersion` | string: `">=X.Y.Z,<X.Y.Z"` | No | defaults.toml | Version range of the `uv_build` backend. |

### `bundle`

The whole section is optional.

| Field | Type | Req | Default | Description |
|---|---|---|---|---|
| `onJobFailureWebhookNotificationIds` | list of strings, at least 1 | No | defaults.toml | Notification destination IDs the job notifies on failure. |
| `onJobFailureEmails` | list of emails, at least 1 | No | defaults.toml | Addresses the job emails on failure. |
| `tags` | map of string to string, at most 25 | No | defaults.toml | Job tags. Keys start with a letter and are not YAML words like `true` or `no`. |
| `targets` | list of single-key maps | No | defaults.toml | Per-target settings. See below. |

`bundle.targets`:

- Each entry is `- <target>: {settings}`. Targets are `dev`, `qa`, `uat`, `prod`,
  each at most once.
- All four targets are always generated. Targets and settings left out use the
  defaults.

```yaml
bundle:
  targets:
    - prod:
        host: "https://prod-workspace.cloud.databricks.com"
```

| Field | Type | Req | Default | Description |
|---|---|---|---|---|
| `host` | string: `https://…` | No | defaults.toml | Workspace URL. |
| `servicePrincipal` | string | No | defaults.toml | Display name of the service principal the job runs as. |
| `computePolicy` | string | No | defaults.toml | Cluster policy ID the job's compute uses. |
| `onTaskFailureWebhookNotificationIds` | list of strings, at least 1 | No | defaults.toml | Notification destination IDs each task notifies on failure. |
| `onTaskFailureEmails` | list of emails, at least 1 | No | defaults.toml | Addresses each task emails on failure. |
| `permissions` | map | No | defaults.toml | Job permissions. When set, it replaces the default permissions. |

`bundle.targets.<target>.permissions`:

| Field | Type | Description |
|---|---|---|
| `canManage.servicePrincipal` | list of strings | Service principals with CAN_MANAGE. Only the target's `servicePrincipal` is allowed. |
| `canManage.groups` | list of strings | Groups with CAN_MANAGE. |
| `canRun.servicePrincipal` | list of strings | Service principals with CAN_RUN. Only the target's `servicePrincipal` is allowed. |
| `canRun.groups` | list of strings | Groups with CAN_RUN. |

The default permissions give CAN_MANAGE to the target's `servicePrincipal` and
the default manage groups, and CAN_RUN to the default run groups.

### `job`

| Field | Type | Req | Default | Description |
|---|---|---|---|---|
| `executionMode` | `"continuous"` or `"batch"` | No | defaults.toml | How the job runs. |
| `batchSchedule` | string: Quartz cron | Batch only | — | When the batch job runs, in UTC. |
| `bronze` | map | Yes | — | Bronze layer settings. |
| `silver` | map | No | — | Silver layer settings. Requires `schema`. |
| `gold` | map | No | — | Gold layer settings. Requires `silver`. |

Execution mode dependencies:

| Field | `continuous` | `batch` |
|---|---|---|
| `batchSchedule` | Not allowed | Required |
| `<layer>.triggerInterval` | Optional, defaults from defaults.toml | Not allowed |
| Job | Runs continuously, created paused | Runs on `batchSchedule`, created paused. Tasks run bronze → silver → gold. |

`batchSchedule` is a Quartz expression with 6 or 7 fields (seconds, minutes, hours,
day of month, month, day of week, optional year). Exactly one of day of month and
day of week must be `?`. Example: `"0 0 2 * * ?"` (daily at 02:00 UTC).

`job.bronze`:

| Field | Type | Req | Default | Description |
|---|---|---|---|---|
| `sourcePath` | string: `s3://…` | Yes | — | Where Firehose writes. `create-stream` prints this path. |
| `checkpointPath` | string: `s3://…` | Yes, unless a default is set | defaults.toml | Streaming checkpoint. |
| `targetTable` | string: `catalog.schema.table` | Yes, unless a default is set | defaults.toml | Bronze table. |
| `triggerInterval` | string, such as `"1 minute"` | Continuous only | defaults.toml | Processing interval. |

`job.silver` (reads the bronze table):

| Field | Type | Req | Default | Description |
|---|---|---|---|---|
| `checkpointPath` | string: `s3://…` | Yes, unless a default is set | defaults.toml | Streaming checkpoint. |
| `targetTable` | string: `catalog.schema.table` | Yes, unless a default is set | defaults.toml | Silver table. |
| `triggerInterval` | string | Continuous only | defaults.toml | Processing interval. |

`job.gold` (reads the silver table):

| Field | Type | Req | Default | Description |
|---|---|---|---|---|
| `upsertLatestKey` | string: column name | No | defaults.toml | Column that orders changes to the same primary key; the highest value is kept. Must be a `schema` column or a silver metadata column, such as `_event_timestamp`. |
| `checkpointPath` | string: `s3://…` | Yes, unless a default is set | defaults.toml | Streaming checkpoint. |
| `targetTable` | string: `catalog.schema.table` | Yes, unless a default is set | defaults.toml | Gold table. |
| `triggerInterval` | string | Continuous only | defaults.toml | Processing interval. |

### `schema`

Required when `job.silver` is set.

| Field | Type | Req | Default | Description |
|---|---|---|---|---|
| `columns` | list, at least 1 | Yes | — | Silver and gold columns, in table order. |
| `primaryKeys` | list of column names, at least 1 | Yes | — | Columns that identify a row. Gold keeps one row per key. No repeats. |

`schema.columns[]`:

| Field | Type | Req | Default | Description |
|---|---|---|---|---|
| `name` | string: `[A-Za-z][A-Za-z0-9_]*` | Yes | — | Column name. Unique. `generate-yaml` fills it from `path`: each level in snake_case, joined by `__` (`addressDetails.M.zipCode` → `address_details__zip_code`). |
| `description` | string | No | — | Column comment in the DDL. |
| `dynamoType` | `S` `N` `B` `BOOL` `NULL` `M` `L` `SS` `NS` `BS` | Yes | — | DynamoDB type of the attribute. |
| `targetType` | string: Spark SQL type | Yes | — | Type the value is cast to. Must suit `dynamoType` (table below). |
| `path` | string: dot path | Yes | — | Attribute path in the DynamoDB item. Nested maps include the `M` level, such as `addressDetails.M.zipCode`. |
| `key` | boolean | No | `false` | Whether the attribute is part of the table's key. Key columns are read from the event's keys, so they are present on deletes. |

Key columns must have a top-level `path` and `dynamoType` `S`, `N`, or `B`.

`targetType` by `dynamoType` (scalar = `tinyint`, `smallint`, `int`, `bigint`,
`float`, `double`, `decimal(p, s)`, `string`, `boolean`, `date`, `timestamp`,
`timestamp_ntz`):

| `dynamoType` | Allowed `targetType` |
|---|---|
| `S` | any scalar |
| `N` | numeric scalar or `string` |
| `B` | `string` |
| `BOOL` | `boolean` or `string` |
| `NULL` | `boolean` |
| `M`, `L` | `string` or `variant` (DynamoDB's typed JSON) |
| `SS` | `array<scalar>` |
| `NS` | `array<numeric scalar or string>` |
| `BS` | `array<string>` |

### Example

```yaml
version: v1

metadata:
  name: orders
  description: Pipeline to ingest orders.

job:
  executionMode: "continuous"
  bronze:
    sourcePath: "s3://{env}-lake/firehose-stream/orders/"
    checkpointPath: "s3://{env}-lake/orders/bronze/checkpoint/"
    targetTable: "{env}.bronze.orders"
  silver:
    checkpointPath: "s3://{env}-lake/orders/silver/checkpoint/"
    targetTable: "{env}.silver.orders"
  gold:
    checkpointPath: "s3://{env}-lake/orders/gold/checkpoint/"
    targetTable: "{env}.gold.orders"

schema:
  columns:
    - name: id
      description: "Order identifier."
      dynamoType: N
      targetType: int
      path: id
      key: true
    - name: created_at
      dynamoType: S
      targetType: timestamp
      path: createdAt
      key: true
    - name: address__zip_code
      dynamoType: S
      targetType: string
      path: addressDetails.M.zipCode
  primaryKeys:
    - id
    - created_at
```

## Architecture

```
/sp-dynamo-firehose-s3 <action> <args>
        │
        ▼
SKILL.md ── maps the action to one command; Claude runs it once and relays the output
        │
        ▼
router ── reads `version` from the YAML (or uses the latest version) and hands off
        │
        ▼
v1/ ── actions ─┬─ validate inputs ─► render templates ─► write files
                └─ AWS CLI ─► check state ─► create what is missing ─► report
```

Principles:

| Principle | How it shows up |
|---|---|
| Code, not the agent, produces output | Claude only runs the command and shows the result. It does not write, edit, or infer files, schemas, or AWS commands. |
| Deterministic output | Files are rendered from templates. The same YAML, AWS state, and defaults produce identical output. |
| Validate before acting | Inputs are validated against strict models before any file is written or AWS resource is created. |
| One source of defaults | Every default comes from `defaults.toml`, applied the same way by every action. |
| Fixed outcomes | Every way an action can end has a code and fixed wording. AWS errors are sorted into fixed kinds. |
| Check, then create | AWS state is read in full before changes. Existing resources are never modified. Reruns resume. |
| Isolated versions | Everything for a version lives in its own directory: actions, models, templates, defaults, and this README. Versions share no code; only a thin router sits outside them. |
| Generated code stays separate from setup | Generated jobs never create tables or infrastructure. DDL and `create-stream` handle that. |

Layout:

| Path | Contents |
|---|---|
| `SKILL.md` (skill root) | Instructions Claude follows: action usage and how to relay results. |
| `router.py` (package root) | Picks the version. The only code outside a version directory. |
| `v1/actions/` | One module per action: arguments, run, result. |
| `v1/templates/` | Templates for every generated file. |
| `v1/defaults.toml` | Defaults for this version. |
| `tests/v1/` (skill root) | Tests, with expected-output fixtures and a fake AWS CLI. |

## Defaults

[`defaults.toml`](defaults.toml) holds every default this version uses.

- Edit it to change the defaults for every pipeline and every action.
- It covers optional YAML fields, the values `generate-yaml` writes, command
  options, and the AWS settings `create-stream` and `generate-yaml` use.
- A YAML value always overrides the default.
- An empty value (`""`) means no default: the field stays required.
- The file is validated with the same rules as the YAML every time the skill
  runs. An invalid file stops the skill with `Invalid defaults file <path>: …`
  before anything is generated or created.
- Set `SP_DYNAMO_FIREHOSE_S3_V1_DEFAULTS` to a path to use a different file.

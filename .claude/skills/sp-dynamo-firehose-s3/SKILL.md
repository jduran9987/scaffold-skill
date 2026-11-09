---
name: sp-dynamo-firehose-s3
description: Runs actions for Databricks pipelines whose data flows from DynamoDB through Firehose to S3, such as creating the Kinesis and Firehose streams for a table, writing a pipeline YAML to fill in, and scaffolding a pipeline project from it. Use when working on a pipeline for this source.
argument-hint: "<action> <args>"
disable-model-invocation: true
allowed-tools: Bash(uv run --project *)
---

# DynamoDB, Firehose, S3 pipelines

Requires `uv`. Defaults for every optional YAML setting and command option are in
`${CLAUDE_SKILL_DIR}/src/sp_dynamo_firehose_s3/v1/defaults.toml`. The first word of
the arguments is the action. Run it with:

```bash
uv run --project "${CLAUDE_SKILL_DIR}" generate $ARGUMENTS
```

If the action is missing or not listed below, tell the user the available actions
instead of running anything.

Run the command once, with a Bash timeout of 600000 ms. `generate-yaml` and
`create-stream` print a report that ends with a fixed result block:

```
Result: <CODE>
  <what happened>
Next step: <what to do>
```

Show the report to the user exactly as printed, in a code block, and add nothing
else: no diagnosis, no summary of your own, and no other advice than its
`Next step`. Do not run any other command to investigate, check, or fix the
result, including AWS CLI commands, and do not rerun the action unless the user
asks. The report already contains every check the skill makes.

## Actions

### scaffold

Usage: `scaffold <path-to-yaml> [--output-dir <path>] [--force]`

Generates a pipeline project. The generator owns the output, so do not write or edit
pipeline code by hand.

1. Run the command above.

   It creates a uv project at `./dabs/<metadata.name>/` (or `--output-dir <path>`)
   with `uv init` unless one exists. It then writes the bundle's `databricks.yml`,
   its job resource `resource/<metadata.name>.job.yml`, and the job code in
   `src/<package>/`: `bronze.py`, plus `silver.py` and `gold.py` when the YAML has
   `job.silver` and `job.gold`. It sets the project's scripts to one entry point
   per job.

   It also writes the project's `README.md`, with each layer's description,
   tables, and columns, and each layer's table DDL for every target, as
   `ddl/<target>/<layer>.sql` (`dev`, `qa`, `uat`, `prod`). The jobs never create
   their tables, so these must run before each job's first run. Column comments
   come from the schema's `description` fields, and metadata columns always get
   the same comments. Do not run the DDL unless the user asks.

2. If it succeeds, report the generated file paths.

3. If it fails, report the error as printed. Do not change the YAML unless the user
   asks.

4. If an output file already exists, ask the user before rerunning with `--force`,
   which overwrites it.

### generate-yaml

Usage: `generate-yaml <dynamodb-table> [--force]`

Writes `./pipelines/<table>.yaml`, with the table name lowercased and every
character that is not a letter or digit replaced by a hyphen (`bo.accounts`
becomes `pipelines/bo-accounts.yaml`). The YAML has every supported field,
including a `schema` section inferred from the table, for the user to fill in
before running `scaffold`. Pass the table name exactly as the user gives it. The command
writes the whole file, so do not write, complete, or edit the YAML yourself, and
do not suggest a schema from your own knowledge of the table.

Fields with defaults in the defaults file are filled with them, including each
layer's `checkpointPath` and `targetTable` when those defaults are set.
`metadata.name` is the same hyphenated name, so `bo.accounts` gets
`bo-accounts`. Required
fields without a value are `""` and marked `# Required field, please fill.`

Result codes: `GENERATED`, `INVALID_TABLE_NAME`, `FILE_EXISTS`, `TABLE_NOT_FOUND`, `KEY_NOT_USABLE`,
`WRITE_FAILED`, and the AWS codes below. Only `GENERATED` writes the file.

For `FILE_EXISTS`, ask the user before rerunning with `--force`, which overwrites
the file.

### create-stream

Usage: `create-stream <dynamodb-table> [--check]`

Streams a DynamoDB table to S3: a Kinesis data stream named after the table, the
table's streaming to it, and a Firehose stream `<table>-firehose` that writes to
the bucket set in the defaults file.

The command first checks every resource. If anything blocks the setup, such as
the table already streaming to another Kinesis stream, it stops before creating
anything. Otherwise it creates only what is missing, waits for each resource to
become active, and confirms everything is connected. Existing resources are never
changed. With `--check`, it only reports what exists and what it would create.
Use `--check` whenever the user asks whether the streams exist or are ready.

Result codes:

- Success: `CREATED`, `ALREADY_EXISTED`, and with `--check`, `READY`,
  `WOULD_CREATE`, `IN_PROGRESS`.
- Blocked before creating anything: `INVALID_TABLE_NAME`, `TABLE_NOT_FOUND`,
  `TABLE_STREAMS_ELSEWHERE`, `TABLE_STREAMING_BUSY`, `KINESIS_STREAM_DELETING`,
  `FIREHOSE_WIRED_ELSEWHERE`, `FIREHOSE_UNUSABLE`.
- Failed while creating: `FIREHOSE_CONFIG_REJECTED`, `STREAMING_FAILED`,
  `FIREHOSE_FAILED`, `TIMEOUT`, `NOT_CONFIRMED`, and the AWS codes below. The
  report's `Created:` line lists what was created before the failure, and a rerun
  resumes from there.

### AWS result codes

Both actions that call AWS can end with `AWS_CLI_MISSING`, `AWS_CREDENTIALS`,
`AWS_ACCESS_DENIED`, `AWS_NOT_FOUND`, `AWS_INVALID_ARGUMENT`, or `AWS_ERROR`. For
`AWS_CREDENTIALS`, the next step tells the user to run `aws sso login`. Never run
it yourself.

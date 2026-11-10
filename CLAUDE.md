# scaffold-skill

Claude Code skills that generate Databricks pipeline projects. The one skill so
far, `sp-dynamo-firehose-s3`, handles DynamoDB → Kinesis → Firehose → S3 sources:

- `create-stream <table>`: creates the AWS streaming resources for a table.
- `generate-yaml <table>`: writes a pipeline YAML with a schema inferred from it.
- `scaffold <yaml>`: generates a Declarative Automation Bundle project with bronze,
  silver, and gold jobs, DDL, and a README.

The full reference (actions, YAML fields, architecture, defaults) is
`.claude/skills/sp-dynamo-firehose-s3/src/sp_dynamo_firehose_s3/v1/README.md`.
Read it before changing behavior.

## Philosophy: Claude routes, code generates

Output must be as close to deterministic as possible. Claude is a router that
picks an action and runs coded logic, not a code generator.

- IMPORTANT: Never write or edit generated pipeline code, DDL, or pipeline YAML by
  hand, and never improvise AWS commands. Change the templates or Python instead.
- The same YAML and defaults always produce byte-identical files: Jinja
  templates with `StrictUndefined`, no timestamps, no ordering that varies.
- Every outcome has a canned report ending in a fixed block:
  `Result: <CODE>` / what happened / `Next step:`. A new failure mode gets a new
  result code and fixed text, never free-form output.
- `SKILL.md` tells Claude to run the command once and show the report exactly as
  printed, adding nothing. Keep `SKILL.md` short and procedural; logic belongs
  in code.
- Every AWS call is coded ahead of time in `v1/aws.py` and `v1/streaming.py`, with
  its errors classified into result codes.

## Layout

```
.claude/skills/sp-dynamo-firehose-s3/
  SKILL.md                      # Claude's procedure: run once, relay the report
  pyproject.toml                # entry point `generate` = router:main
  src/sp_dynamo_firehose_s3/
    router.py                   # only code shared across versions
    v1/                         # everything for version v1
      actions/                  # one module per action
      config.py, fields.py      # pydantic models for the pipeline YAML
      defaults.py, defaults.toml
      generator.py, templates/  # Jinja rendering of the generated project
      aws.py, streaming.py      # AWS CLI wrapper and create-stream flow
      report.py                 # result block formatting
      README.md                 # v1 reference
  tests/v1/fixtures/            # test defaults, fake AWS CLI, expected outputs
scripts/                        # data generator for the `scaffoldskill` test table
dabs/                           # default scaffold output (`dabs/<metadata.name>/`)
```

## Versioning

- A pipeline YAML declares `version: v1`. `router.py` reads it and passes the
  arguments, unchanged, to that version's `main`. Actions without a YAML
  (`generate-yaml`, `create-stream`) go to the latest version.
- Each version is fully isolated: its own actions, argument parsing, models,
  defaults, templates, README, and tests. YOU MUST NOT share code between
  versions; only the thin router lives outside `vN/`. This way an existing
  YAML always generates the same output, whatever later versions change.
- v1 is still in development, so changes go into v1 directly (after asking
  about breaking ones).

## Config rules

- Pipeline YAML: pydantic strict mode, camelCase keys, unknown keys rejected,
  no type coercion, and `null` means the key was left out.
- `defaults.toml` (snake_case) is the single source of defaults. It is
  validated with the same types as the YAML when the skill starts, and
  `generate-yaml` writes its values into new YAMLs.
- Placeholders: `{env}` (target name), `{name}` (`metadata.name`), `{table}`
  (`metadata.name` with `-` → `_`). Any other `{...}` is rejected.
- Targets are always `dev`, `qa`, `uat`, `prod`. Workspace settings are per
  target in `[bundle.targets.<target>]`. Failure notifications are prod-only and
  opt-in.
- Hyphens are allowed in `metadata.name`. Use `{table}` for table names.

## AWS

- Every command goes through `aws.run_aws`, with `--profile` and `--region` from
  `defaults.toml`. Credentials come from local AWS profiles.
- IMPORTANT: Never run `aws sso login`. Credential errors return
  `AWS_CREDENTIALS`, whose next step tells the user to run it.
- `create-stream` checks every resource first and stops on any conflict before
  creating anything. It then creates only what is missing, waits, and confirms.
  It never modifies existing resources. The total wait is at most 540 s, to stay
  under Claude Code's 10-minute Bash limit.

## Commands

Run from `.claude/skills/sp-dynamo-firehose-s3/`:

```bash
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
uv run generate <action> <args>
```

- Tests use `tests/v1/fixtures/defaults.toml` (set via
  `SP_DYNAMO_FIREHOSE_S3_V1_DEFAULTS` in `conftest.py` before import), never the
  shipped file. Keep its keys in sync with `v1/defaults.toml` (a test checks only
  the top-level sections).
- AWS is faked: `conftest.py` patches `aws.subprocess`, and
  `tests/v1/fixtures/bin/aws` is a fake CLI for subprocess tests.
- `tests/v1/fixtures/expected/` holds byte-exact expected outputs. When output
  changes on purpose, regenerate them and review the diff.

## Code style

- Python 3.12+, type annotations everywhere, Google-style docstrings, 88-char
  lines, ruff rules from `pyproject.toml`.
- uv for everything; `uv_build` is the build backend for the skill and for
  generated projects.

## Working agreements

- Ask before breaking changes (YAML or defaults layout, build backend, removing
  working commands), and say what breaks.
- Push back on anti-patterns instead of implementing them.
- Each behavior change also updates `v1/README.md`, `SKILL.md` if affected, the
  test copy of the defaults, and the expected fixtures.
- Docs are for technical readers: dry, concise, tables and bullets, examples. They
  describe only the current state, with no history, rationale, or plans.
- Databricks testing is deferred until the project is done. Local Spark + Delta
  checks are fine.

## Open items

- An invalid `defaults.toml` prints a Python error instead of a result block.
- Not yet requested: primary key constraints and `CLUSTER BY` on gold tables.
- End-to-end test in Databricks.

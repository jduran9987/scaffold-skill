# scaffold-skill

Claude Code skills that generate Databricks pipeline projects. The one skill so
far, `sp-dynamo-firehose-s3`, handles DynamoDB → Kinesis → Firehose → S3 sources.
The user invokes it and the agent runs these steps in order, one at a time:

| Step | Command | What it does |
|---|---|---|
| Table name | none | Asks the DynamoDB table name |
| 1 | `check-project <table> <project dir>` | Reports whether the table's project exists; if so, the agent asks whether to overwrite it, and No stops the run |
| Trigger | none | Asks `batch` or `continuous`, and a cron schedule if batch |
| 2 | `create-stream <table>` | Creates the Kinesis stream, the table's streaming to it, and the Firehose stream. Only `TABLE_NOT_FOUND` stops the run. |
| 3 | `prepare-inputs <session> <table>` | Scans and flattens the table into columns for the agent and for scaffold. Anything but `COLUMNS_FOUND` stops the run. |
| Types | none | The agent picks a Spark type per column from each column's allowed types |
| 4 | `scaffold <session> <table> <project dir> < answers` | Generates a Declarative Automation Bundle project with bronze, silver, and gold jobs, DDL, and a README |

`SKILL.md` is the agent's procedure. The skill's `README.md` is the reference:
commands, result codes, flattening rules, and the generated project. Read it
before changing behavior. `inputs.toml` holds every value the user is not asked
for, with comments describing each.

## Philosophy: Claude routes, code generates

Output must be as close to deterministic as possible. Claude asks fixed
questions, runs coded commands, and relays their reports. It is not a code
generator.

- IMPORTANT: Never write or edit generated pipeline code, DDL, or bundle files by
  hand, and never improvise AWS commands. Change the templates or Python instead.
- The same inputs and answers always produce byte-identical files: Jinja templates
  with `StrictUndefined`, no timestamps, no ordering that varies.
- Every outcome has a canned report ending in a fixed block:
  `Result: <CODE>` / what happened / `Next step:`. A new failure mode gets a new
  result code and fixed text, never free-form output.
- `SKILL.md` gives every question word for word, every command, and every status
  message, and tells Claude to show each report exactly as printed, adding
  nothing. Keep it short and procedural; logic belongs in code.
- The agent's only judgment call is each column's Spark type, made from fixed
  rules in `SKILL.md` and checked by scaffold against `allowed_types`.

## Layout

```
.claude/skills/sp-dynamo-firehose-s3/
  SKILL.md                       # the agent's procedure
  pyproject.toml                 # entry points create-stream, prepare-inputs, scaffold
  src/sp_dynamo_firehose_s3/
    inputs.toml                  # every value the user is not asked for
    aws.py                       # AWS CLI wrapper; errors sorted into fixed kinds
    result.py                    # report layout shared by every action
    check_project/               # config.py, report.py, main.py
    create_stream/               # config.py, steps.py, report.py, main.py
    prepare_inputs/              # config.py, columns.py, report.py, main.py
    scaffold/                    # config.py, render.py, report.py, main.py, templates/
  tests/                         # one file per action, plus one per action's config
    test_inputs.py               # the only test of the shipped inputs.toml
    fixtures/inputs.toml         # the tests' copy of inputs.toml
    fixtures/scaffold/           # byte-exact expected project files
archive/                         # the previous version, for reference only; never edit
scripts/                         # data generator for the `scaffoldskill` test table
bundles/                         # default scaffold output (`bundles/<table_name>/`)
```

## Actions are isolated

- Each action has its own config model, report codes, and logic. Actions never
  import from each other. Only `aws.py` and `result.py` are shared, and neither
  describes any action's inputs.
- Each command is its own action, with its own folder, even a small one such as
  check-project.
- Copying small pieces between actions (reading `inputs.toml`,
  `fill_placeholders`, `AwsConfig`, the session folder path, `to_snake_case`) is
  intended. Do not factor them into a shared module. Where two copies must agree,
  such as check-project's and scaffold's `to_snake_case`, a test compares them.
- Each action validates all of its inputs in one pydantic model, at one time, and
  reports every problem in one `CONFIG_INVALID` report. Do not add a separate,
  earlier check for a single value.
- create-stream's report is always shown, but only `TABLE_NOT_FOUND` ends the
  run; any other result continues to prepare-inputs. A prepare-inputs or
  scaffold failure ends the run. There are no retries for bad answers; the user
  reruns the skill. The one retry is create-stream's `TIMEOUT`, run once more.

## Handoff between actions

- prepare-inputs writes `columns.json` (name, path, DynamoDB type, key) to
  `<system temp>/sp-dynamo-firehose-s3/<session id>/`, where the session ID is
  `${CLAUDE_SESSION_ID}`. Users never see this folder.
- prepare-inputs prints `## Columns (for the agent)` (one JSON object per column,
  with examples and allowed types) before `## Report`. The agent shows only the
  report.
- The agent passes its types and the user's answers to scaffold as JSON on
  standard input. scaffold reads `columns.json`, checks everything, writes the
  project, and deletes the session folder whatever the result.
- The user interacts only through the agent's prompts. Their answers reach the
  commands in the answers JSON, never as command flags.

## Config rules

- Every model is pydantic strict: unknown keys rejected, no type coercion,
  read-only. Each action's top-level model ignores other actions' sections.
- `inputs.toml` is read, never written. Placeholders are filled in memory:
  - `{dynamo_table_name}`: the table name as typed. Filled by every action.
  - `{table_name}` (snake_case) and `{firehose_table_name}` (dots → dashes):
    filled by scaffold only.
  - `{env}`: left as is; the generated project replaces it per target.
  - `!{...}`: Firehose syntax, passed to AWS unchanged.
- Targets are always `dev`, `qa`, `uat`, `prod`. Failure notifications are sent
  by prod only.
- Generated projects are uv projects (`uv_build` backend) rendered from
  templates; `uv init` is not run.

## AWS

- Every command goes through `aws.run_aws`, with the profile and region from
  `inputs.toml`. Credentials come from local AWS profiles.
- IMPORTANT: Never run `aws sso login`. Credential errors return
  `AWS_CREDENTIALS`, whose next step tells the user to run it.
- Never run an action against real AWS without the user's permission; the user
  runs real tests.
- create-stream checks every resource first and stops on any conflict, such as
  the table already streaming elsewhere, before creating anything. It then
  creates only what is missing, waits, and confirms. It never modifies existing
  resources. The total wait is at most 540 s, to stay under Claude Code's
  10-minute Bash limit.

## Commands

Run from `.claude/skills/sp-dynamo-firehose-s3/`:

```bash
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
UPDATE_EXPECTED=1 uv run pytest tests/test_scaffold.py
```

- Tests read `tests/fixtures/inputs.toml`, never the shipped `inputs.toml`,
  whose values differ between projects. Only `tests/test_inputs.py` reads the
  shipped file, and only checks that every action accepts it (types and limits)
  and that its sections and keys match the copy. Keep the two files' keys in
  sync.
- AWS and the clock are faked in `tests/conftest.py`. Tests point files and
  folders (`INPUTS_PATH`, `SESSIONS_DIR`) at `tmp_path` with `monkeypatch`.
- `tests/fixtures/scaffold/` holds byte-exact expected output. When output
  changes on purpose, regenerate with `UPDATE_EXPECTED=1` and review the diff.

## Code style

- Python 3.12+, type annotations everywhere, Google-style docstrings, 88-char
  lines, ruff rules from `pyproject.toml`.
- uv for everything.
- No test-only parameters, hooks, or logic in production code. Tests use
  `monkeypatch` for files, subprocesses, and time.
- Keep code simple and organized above all; other people will maintain it.

## Working agreements

- Go slowly: agree on the design before writing much code.
- Ask before breaking changes, and say what breaks.
- Push back on anti-patterns instead of implementing them.
- Each behavior change also updates the skill's `README.md`, `SKILL.md` if
  affected, the comments in `inputs.toml`, the tests, and the expected fixtures.
- Docs and comments are for technical readers: dry, concise, tables and bullets,
  examples. They describe only the current state, with no history, rationale,
  or plans.
- Databricks testing is deferred until the project is done. Local Spark + Delta
  checks are fine.

## Open items

- End-to-end run of the skill against real AWS.
- End-to-end test in Databricks.

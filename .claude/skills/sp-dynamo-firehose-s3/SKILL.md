---
name: sp-dynamo-firehose-s3
description: Sets up a Databricks pipeline for a DynamoDB table whose data flows through Kinesis and Firehose to S3. Use when the user wants a pipeline for a DynamoDB table.
disable-model-invocation: true
---

# Skill Invocation Response Procedure

Run the steps below in order, one at a time. Do only what each step says. Never
show, quote, or describe a command, and never run any command this procedure
does not give, including `aws sso login` or any other AWS command.

## 1. Get the DynamoDB table name

Ignore any arguments given with the skill command, and always ask the user this
question exactly as written, and nothing else:

> What is the name of the DynamoDB table the pipeline reads? For example: bo.suspenseAccounts

Do not rephrase, shorten, or add to the question.

Pass the name to every later command exactly as the user typed it, with the same
case and punctuation, in place of `<table name>`. Do not correct, normalize, or
validate it, and do not work out any value from it, such as a snake_case table
name. The commands do that.

## 2. Check whether the project exists

Run this command once, with the Bash description `Checking the project`:

```bash
uv run --project "${CLAUDE_SKILL_DIR}" check-project '<table name>' "${CLAUDE_PROJECT_DIR}"
```

- `PROJECT_NEW`: show nothing, and continue to step 3.
- `PROJECT_EXISTS`: do not show the report. Ask the user the line under
  `Result: PROJECT_EXISTS`, word for word, with the AskUserQuestion tool, header
  `Overwrite`, and exactly two options: `Yes` and `No`. If the answer is `Yes`,
  continue to step 3. Otherwise, stop.
- Any other result: show the report exactly as printed, in one code block, add
  nothing else, and stop.

## 3. Get the trigger

Ask the user this question with the AskUserQuestion tool, exactly as written,
with exactly these two options and descriptions, and nothing else:

- Question: `How should this pipeline be triggered?`
- Header: `Trigger`
- Option `batch`: `Runs on a cron schedule.`
- Option `continuous`: `Runs all the time.`

Keep the answer exactly as the user gave it, even if they typed their own.

## 4. Get the batch schedule

Only if the answer to step 3 is `batch`, ask the user this question exactly as
written, and nothing else:

> Enter the batch schedule as a Databricks cron expression. For example: 0 0 6 * * ? runs every day at 6:00.

Keep the answer exactly as the user typed it. Do not correct, convert, or
validate it. scaffold does that.

## 5. Create the stream

First tell the user exactly this, and nothing else:

> Creating infrastructure, hang tight ...

Then run this command once, with a Bash timeout of 600000 ms and the Bash
description `Creating infrastructure`:

```bash
uv run --project "${CLAUDE_SKILL_DIR}" create-stream '<table name>'
```

When it finishes, show the report it printed exactly as printed, in one code
block, and add nothing else.

If the report's `Result:` is `TIMEOUT`, tell the user exactly this, and nothing
else:

> Still waiting on AWS, checking again ...

Then run the same command one more time and show its report the same way.

If the last report's `Result:` is `TABLE_NOT_FOUND`, stop. For any other result,
continue to step 6.

## 6. Read the table's columns

First tell the user exactly this, and nothing else:

> Reading the table's columns ...

Then run this command once, with the Bash description `Reading columns`:

```bash
uv run --project "${CLAUDE_SKILL_DIR}" prepare-inputs "${CLAUDE_SESSION_ID}" '<table name>'
```

The output has up to two sections:

- `## Columns (for the agent)`: one JSON object per column. It is for you only.
  Never show it, quote it, or describe it to the user.
- `## Report`: show the lines after this heading exactly as printed, in one code
  block, and add nothing else.

Continue to step 7 only if the report's `Result:` is `COLUMNS_FOUND`. Otherwise,
stop.

## 7. Pick each column's Spark type

Do this silently: tell the user nothing, and do not ask them to confirm.

For every column object from step 6, pick exactly one type from its
`allowed_types`, using its `name`, `dynamo_type`, and `examples`:

- Money (amounts, balances, prices, costs, fees, totals): `decimal(22,2)`.
- `S` holding dates such as `2024-03-01`: `date`.
- `S` holding dates with times such as `2024-03-01T12:00:00Z`: `timestamp`.
- `S` holding only `true` or `false`: `boolean`.
- Any other `S`, including IDs, codes, and numbers such as zip codes: `string`.
- `N` holding whole numbers that are not money: `bigint`.
- `N` holding fractions that are not money: `double`.
- `BOOL`: `boolean`.
- A column with no examples: the first of its `allowed_types`.
- Any other column has one allowed type: use it.

## 8. Generate the project

First tell the user exactly this, and nothing else:

> Generating the project ...

Then run this command once, with the Bash description `Generating the project`.
In the JSON, set:

- `types`: every column's `name` from step 6 mapped to the type picked in step 7.
- `execution_mode`: the answer to step 3.
- `batch_schedule`: the answer to step 4 as a JSON string, or `null` if step 4
  was skipped.

```bash
uv run --project "${CLAUDE_SKILL_DIR}" scaffold "${CLAUDE_SESSION_ID}" '<table name>' "${CLAUDE_PROJECT_DIR}" <<'ANSWERS'
{"types": {"<column name>": "<type>"}, "execution_mode": "<answer>", "batch_schedule": "<answer>"}
ANSWERS
```

When it finishes, show the report it printed exactly as printed, in one code
block, and add nothing else. Then stop, whatever the result.

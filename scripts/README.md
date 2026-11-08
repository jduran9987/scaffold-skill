# data-generator

Generates insert, modify, and delete activity on orders in the `scaffoldskill` DynamoDB
table, so the pipeline has change events to ingest.

## Usage

Uses your default AWS profile. From this directory:

```bash
uv run generate-orders --new-rows 10 --modify-rows 3 --delete-rows 2
```

| Argument | Default | Description |
|---|---|---|
| `--new-rows` | `0` | Rows to insert. |
| `--modify-rows` | `0` | Existing rows to modify. Capped at the number of rows. |
| `--delete-rows` | `0` | Existing rows to delete. Capped at the number of rows. |
| `--table-name` | `scaffoldskill` | Table to write to. |
| `--profile` | default profile | AWS profile to use. |

About 35% of inserted and modified rows are missing one or more optional fields.
A modify that leaves out a field removes it from the row.

## Schema

| Field | Type | Required | Key |
|---|---|---|---|
| `id` | Number (`N`) | Yes | Partition key |
| `createdAt` | String (`S`), ISO 8601 UTC | Yes | Sort key |
| `amount` | Number (`N`), two decimal places | No | |
| `status` | String (`S`): `PENDING`, `SHIPPED`, `DELIVERED`, `CANCELLED` | No | |
| `addressDetails` | Map (`M`) | No | |
| `addressDetails.city` | String (`S`) | When `addressDetails` is present | |
| `addressDetails.state` | String (`S`) | When `addressDetails` is present | |
| `addressDetails.zipCode` | String (`S`) | When `addressDetails` is present | |

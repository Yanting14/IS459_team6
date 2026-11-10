# Athena layer (Glue tables + demo queries)

The proposal promised a query layer over the pipeline's outputs. These files add it without changing the
dashboard: Glue tables over the curated and gold files in S3, a workgroup with a scan limit, and a few queries that
reproduce numbers the dashboard and docs already show. The dashboard keeps reading with DuckDB (fast, offline).

| File | What it does |
|---|---|
| `01_create_tables.sql` | database `is459_team6`, tables `transfers`, `transfer_labels`, `rings` over the curated zone |
| `02_demo_queries.sql` | four queries to show in the demo |
| `03_sharing_table.sql` | optional: the BQ2 comparison as a table, and its query |
| `run_sql.py` | runs a `.sql` file statement by statement and prints results and data scanned |

## One-time setup (Command Prompt, in the `dashboard` folder)

```
set AWS_PROFILE=team6
aws athena create-work-group --name is459-team6 --region us-east-1 --description "Team 6 queries, 10 GB scan limit per query" --configuration "ResultConfiguration={OutputLocation=s3://is459-team6-gold-207567757101/athena-results/},EnforceWorkGroupConfiguration=true,BytesScannedCutoffPerQuery=10737418240,PublishCloudWatchMetricsEnabled=true"
python athena\run_sql.py athena\01_create_tables.sql
```

The workgroup keeps query results under `athena-results/` in the gold bucket and stops any query that would scan more
than 10 GB. The whole transfers table is about 1.4 GB, so the limit is a safety net, not a constraint.

## Run the demo queries

```
python athena\run_sql.py athena\02_demo_queries.sql
python athena\run_sql.py athena\03_sharing_table.sql      (optional)
```

Each statement prints its result and how much data it scanned. What to expect (these are the numbers already in the
dashboard and docs; if Athena differs, something is wrong):

| Query | Expected |
|---|---|
| 1. Transfers per day | 16 Sep = 3,021,866; about 1.9M on a weekday |
| 2. Cross-bank laundering transfers | close to 99% cross-bank |
| 3. Rings by pattern | 8 pattern types |
| 4. Ring visibility | 2,756 rings; about 42.5% not fully visible to any single bank; best bank sees about 71% |
| Sharing (file 03) | bank view 165 of 320 (51.6% precision, 30.6% recall); shared view 210 of 320 (65.6%, 38.9%) |

## If something fails

- **AccessDenied** on `create-work-group`, creating the database or tables, or writing results: ask Person 6 to grant
  your user `athena:*`, the Glue actions to create and read databases, tables and partitions, `s3:PutObject` on the
  results prefix, and `s3:GetObject` / `s3:ListBucket` on the curated and gold prefixes. If the gold bucket refuses
  result files, ask for a different results location and change `OutputLocation` above.
- **A table returns 0 rows:** check the `LOCATION` in `01_create_tables.sql` against the real curated prefix.
- **`HIVE_BAD_DATA` or a type error:** send the full message; a column type needs adjusting.
- **Re-running** is safe: every statement uses `IF NOT EXISTS` or `OR REPLACE`.

## Notes

- Tables use partition projection on `date` (1 to 30 Sep 2022), so there is no crawler and no `MSCK REPAIR`.
- Athena charges about $5 per terabyte scanned, so these queries cost a fraction of a cent each.
- Person 6 can turn the SQL into Terraform (`aws_glue_catalog_database`, `aws_glue_catalog_table`,
  `aws_athena_workgroup`) if the team wants it in `infra/`.
- `tests/test_athena_sql.py` checks the splitter and runner with a stub client, and checks that queries 1, 2 and 4
  give the same numbers as the dashboard's own code on the sample data (run in DuckDB). It cannot call Athena.

# Athena layer (Glue tables + demo queries)

The proposal promised a query layer over the pipeline's outputs. These files are that layer: Glue tables over the
curated and gold files in S3, a workgroup with a scan limit, and a few queries that reproduce numbers the dashboard
and docs already show. **The dashboard reads through it** (`DASH_SOURCE=athena`): every table it shows is an Athena
query. DuckDB is used only by the offline sample mode.

| File | What it does |
|---|---|
| `01_create_tables.sql` | database `is459_team6`, tables `transfers`, `transfer_labels`, `rings` over the curated zone |
| `02_demo_queries.sql` | four queries to show in the demo |
| `03_sharing_table.sql` | the BQ2 comparison as a table (view `sharing_comparison`) and its query. The Sharing page reads it |
| `04_queue_table.sql` | the BQ1 review queue as a table (view `queue_accounts`). The queue and investigation pages read it |
| `run_sql.py` | runs a `.sql` file statement by statement and prints results and data scanned |

## One-time setup (Command Prompt, in the `dashboard` folder)

```
set AWS_PROFILE=team6
aws athena create-work-group --name is459-team6 --region us-east-1 --description "Team 6 queries, 10 GB scan limit per query" --configuration "ResultConfiguration={OutputLocation=s3://is459-team6-gold-207567757101/athena-results/},EnforceWorkGroupConfiguration=true,BytesScannedCutoffPerQuery=10737418240,PublishCloudWatchMetricsEnabled=true"
python athena\run_sql.py athena\01_create_tables.sql
python athena\run_sql.py athena\03_sharing_table.sql
python athena\run_sql.py athena\04_queue_table.sql
```

The workgroup keeps query results under `athena-results/` in the gold bucket and stops any query that would scan more
than 10 GB. The whole transfers table is about 1.4 GB, so the limit is a safety net, not a constraint.

## Run the demo queries

```
python athena\run_sql.py athena\02_demo_queries.sql
python athena\run_sql.py athena\03_sharing_table.sql
python athena\run_sql.py athena\04_queue_table.sql
```

Each statement prints its result and how much data it scanned. What to expect (these are the numbers already in the
dashboard and docs; if Athena differs, something is wrong):

| Query | Expected |
|---|---|
| 1. Transfers per day | 16 Sep = 3,021,866; about 1.9M on a weekday |
| 2. Cross-bank laundering transfers | close to 99% cross-bank |
| 3. Rings by pattern | 8 pattern types |
| 4. Ring visibility | 2,756 rings; about 42.5% not fully visible to any single bank; best bank sees about 71% |
| Queue (file 04) | five rows for 15 Sep, rank 1 first, reasons filled in |
| Sharing (file 03) | bank view 165 of 320 (51.6% precision, 30.6% recall); shared view 210 of 320 (65.6%, 38.9%) |

## Run the dashboard on Athena

```
set AWS_PROFILE=team6
set DASH_SOURCE=athena
streamlit run app.py
```

Each page click is one or two Athena queries (a few seconds); results are cached, so repeating a click is instant.
The sidebar shows how many queries ran and how much data they scanned. Settings (all optional):
`DASH_ATHENA_WORKGROUP`, `DASH_ATHENA_DATABASE`, `DASH_ATHENA_REGION`, `DASH_QUEUE_URI` (the queue run that
`04_queue_table.sql` points at; used for `ranking.json`), `DASH_RULES_URI`, `DASH_RULES_TUNED_URI`.

What does not go through SQL: `ranking.json` (one per queue day) and the three `metrics.json` files are small JSON
documents, not tables, so the dashboard fetches them directly from S3 with the same credentials.

## If something fails

- **AccessDenied** on `create-work-group`, creating the database or tables, or writing results: ask Person 6 to grant
  your user `athena:*`, the Glue actions to create and read databases, tables and partitions, `s3:PutObject` on the
  results prefix, and `s3:GetObject` / `s3:ListBucket` on the curated and gold prefixes. If the gold bucket refuses
  result files, ask for a different results location and change `OutputLocation` above.
- **A table returns 0 rows:** check the `LOCATION` in `01_create_tables.sql` against the real curated prefix.
- **`HIVE_BAD_DATA` or a type error:** send the full message; a column type needs adjusting.
- **Existing tables or a new pipeline run:** rerun `01_create_tables.sql` and `04_queue_table.sql` to apply
  the corrected partition templates (each ends with `/`). Their `ALTER TABLE` statements update existing
  table locations; `CREATE IF NOT EXISTS` alone does not. When switching runs, edit all `LOCATION` and
  `storage.location.template` values in both the CREATE and ALTER statements. Set `DASH_QUEUE_URI` to the
  same queue folder, then restart Streamlit to clear cached results. These changes do not move or delete S3 data.

## Notes

- Tables use partition projection on `date` (1 to 30 Sep 2022), so there is no crawler and no `MSCK REPAIR`.
- Athena charges about $5 per terabyte scanned, so these queries cost a fraction of a cent each.
- Person 6 can turn the SQL into Terraform (`aws_glue_catalog_database`, `aws_glue_catalog_table`,
  `aws_athena_workgroup`) if the team wants it in `infra/`.
- `tests/test_athena_sql.py` checks the splitter and runner with a stub client, and checks that queries 1, 2 and 4
  give the same numbers as the dashboard's own code on the sample data (run in DuckDB). It cannot call Athena.
- `tests/test_athena_backend.py` runs every dashboard page through the Athena code path against a fake Athena
  (`tests/fake_athena.py`, DuckDB over the sample data). That proves the pages and queries work together; only
  the real service can prove it accepts the SQL, so run the app once against it before the demo.
- When the demo is over, delete what was created by hand: `aws athena delete-work-group --work-group is459-team6
  --recursive-delete-option --region us-east-1` and `aws glue delete-database --name is459_team6 --region us-east-1`
  (this removes the tables and views; the data in S3 is untouched).
